/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package capacityledger

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// legacyEngineTypeLabelKey is the engine-type label the metrics extractor reads when
// plugins.EngineTypeLabelKey is absent.
const legacyEngineTypeLabelKey = "inference.networking.k8s.io/engine-type"

// endpoint is the ledger's state for one endpoint ID. It outlives a datalayer endpoint recreated
// under the same ID, so the leases of requests still running on it stay booked.
type endpoint struct {
	id types.NamespacedName
	// ep is the latest datalayer endpoint added under the ID. A delete event for a different
	// pointer is stale.
	ep   atomic.Pointer[fwkdl.Endpoint]
	acct account

	// decodeRate is output tokens per second one sequence generates, measured between streamed
	// chunks and, for requests that streamed none, at end of stream from the reported usage;
	// prefillRate is uncached prompt tokens per second, measured from dispatch to the first chunk.
	// Both impute the progress of requests that have streamed no chunk.
	decodeRate  rate
	prefillRate rate

	// lastScrape and lastEligibility are what the sampler last recorded; guarded by the ledger's
	// sampleMu.
	lastScrape      time.Time
	lastEligibility attrcapacity.Eligibility
}

func newEndpoint(id types.NamespacedName, ep fwkdl.Endpoint) *endpoint {
	e := &endpoint{id: id}
	e.ep.Store(&ep)
	return e
}

func (e *endpoint) datalayer() fwkdl.Endpoint { return *e.ep.Load() }

// scraped is the engine's reported use: memory from the usage fraction, step from one decode step
// per running request, slots from the running count. The step value is a floor, since the engine
// reports no pending prefill. ok is false when the engine reports no block count. vLLM reserves
// one null block and computes usage over the rest.
func scraped(m *fwkdl.Metrics, decodeStep int64) (use vec, memoryCapacity int64, ok bool) {
	if m == nil || m.CacheNumBlocks <= 1 || m.CacheBlockSize <= 0 {
		return vec{}, 0, false
	}
	memoryCapacity = int64(m.CacheNumBlocks) - 1
	use[axisMemory] = min(max(blocksUsed(m.KVCacheUsagePercent, memoryCapacity), 0), memoryCapacity)
	use[axisStep] = mulSat(int64(m.RunningRequestsSize), decodeStep)
	use[axisSlots] = int64(m.RunningRequestsSize)
	return use, memoryCapacity, true
}

// blocksUsed converts the engine's usage fraction back to a block count, rounding up. A product
// within floating-point error of a whole number is that number: vLLM computes the fraction from
// whole blocks.
func blocksUsed(fraction float64, capacity int64) int64 {
	v := fraction * float64(capacity)
	if r := math.Round(v); math.Abs(v-r) <= 1e-9*float64(capacity) {
		return int64(r)
	}
	return int64(math.Ceil(v))
}

// eligibility reports whether the accounting holds for the endpoint, checking causes in a fixed
// order so the reported reason is stable.
func eligibility(meta *fwkdl.EndpointMetadata, m *fwkdl.Metrics, obs engineObservation, cfg config,
	now time.Time) attrcapacity.Eligibility {
	if meta != nil && isDisaggregatedRole(meta.Labels[bylabel.RoleLabel]) {
		return attrcapacity.Disaggregated
	}
	if meta != nil && !isVLLM(meta.Labels) {
		return attrcapacity.EngineType
	}
	if m == nil || m.UpdateTime.IsZero() || now.Sub(m.UpdateTime) > cfg.stalenessThreshold {
		return attrcapacity.Stale
	}
	if m.CacheNumBlocks <= 1 || m.CacheBlockSize <= 0 {
		return attrcapacity.NoBlockCount
	}
	if obs.engines > 1 {
		return attrcapacity.MultiEngine
	}
	if obs.hybrid {
		return attrcapacity.HybridLayout
	}
	if obs.speculative && cfg.speculativeTokens == 0 {
		return attrcapacity.SpeculativeUnconfigured
	}
	return attrcapacity.Eligible
}

func isDisaggregatedRole(role string) bool {
	switch role {
	case bylabel.RolePrefill, bylabel.RoleDecode, bylabel.RoleEncode, bylabel.RoleEncodePrefill:
		return true
	}
	return false
}

// isVLLM treats an endpoint as vLLM when its engine-type label, under the metrics extractor's
// default label keys, is vllm, default, or absent. A metrics extractor configured with another
// label key or another default engine is not detected.
func isVLLM(labels map[string]string) bool {
	engine := labels[plugins.EngineTypeLabelKey]
	if engine == "" {
		engine = labels[legacyEngineTypeLabelKey]
	}
	return engine == "" || engine == "default" || engine == "vllm"
}

const (
	// rateWeight is the weight of each new observation in a rate's moving average; it smooths
	// chunk-to-chunk jitter while following a change in load within a few dozen observations.
	rateWeight = 0.2
	// rateExpiry is how long a rate stays usable without an observation (convention). An expired
	// rate falls back to the conservative defaults of imputedProgress.
	rateExpiry = 30 * time.Second
)

// rate is an exponentially weighted moving average of a per-sequence rate in units per second.
// Observations are timed when the ledger processes a chunk, which trails the chunk's arrival by
// the director's per-request queue delay.
type rate struct {
	mu           sync.Mutex
	value        float64
	lastObserved time.Time // zero until the first observation
}

func (r *rate) observe(units int64, elapsed time.Duration, now time.Time) {
	if units <= 0 || elapsed <= 0 {
		return
	}
	sample := float64(units) / elapsed.Seconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastObserved.IsZero() || now.Sub(r.lastObserved) > rateExpiry {
		r.value = sample
	} else {
		r.value += rateWeight * (sample - r.value)
	}
	r.lastObserved = now
}

// rateEstimate is a rate's value, and whether an unexpired observation produced it.
type rateEstimate struct {
	value float64
	ok    bool
}

func (r *rate) estimate(now time.Time) rateEstimate {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastObserved.IsZero() || now.Sub(r.lastObserved) > rateExpiry {
		return rateEstimate{}
	}
	return rateEstimate{value: r.value, ok: true}
}
