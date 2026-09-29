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
	"context"
	"slices"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// Extract tracks endpoint additions and deletions and installs the capacity view on each endpoint.
func (l *Ledger) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if event.Endpoint == nil || event.Endpoint.GetMetadata() == nil {
		return nil
	}
	id := event.Endpoint.GetMetadata().ID
	key := id.String()

	// Membership changes and series deletion hold sampleMu, taken before mu as sampling takes it, so
	// a sampling pass cannot record a series between the change and the deletion.
	l.sampleMu.Lock()
	defer l.sampleMu.Unlock()
	switch event.Type {
	case fwkdl.EventAddOrUpdate:
		l.mu.Lock()
		e, ok := l.endpoints[key]
		replaced := ok && e.datalayer() != event.Endpoint
		if !ok {
			e = newEndpoint(id, event.Endpoint)
			l.endpoints[key] = e
		}
		l.mu.Unlock()
		// An endpoint recreated under the same ID keeps its account, so requests still running on
		// the replaced endpoint stay booked until they end. Its series restart for the new endpoint.
		if replaced {
			ep := event.Endpoint
			e.ep.Store(&ep)
			l.metrics.deleteEndpoint(id)
			e.lastEligibility = ""
			e.lastScrape = time.Time{}
		}
		event.Endpoint.GetAttributes().Put(l.dk, &fwkdl.DynamicAttribute{
			Get: func() fwkdl.Cloneable { return l.view(e) },
		})
	case fwkdl.EventDelete:
		l.mu.Lock()
		e, ok := l.endpoints[key]
		// The datalayer delivers the same pointer for delete as for the add; a different pointer
		// is a stale delete for an endpoint that has been replaced.
		if !ok || e.datalayer() != event.Endpoint {
			l.mu.Unlock()
			if ok {
				log.FromContext(ctx).V(logutil.DEFAULT).Info("Ignoring stale delete for replaced endpoint", "endpoint", key)
			} else {
				log.FromContext(ctx).V(logutil.DEBUG).Info("Ignoring delete for unknown endpoint", "endpoint", key)
			}
			return nil
		}
		delete(l.endpoints, key)
		l.mu.Unlock()
		l.engines.forget(key)
		l.metrics.deleteEndpoint(id)
	}
	return nil
}

// PreRequest books a lease for the request on the primary profile's chosen endpoint. ctx is the
// request's stream context, which ends with the request; the lease is released then if no
// end-of-stream call released it first.
func (l *Ledger) PreRequest(ctx context.Context, request *fwksched.InferenceRequest,
	result *fwksched.SchedulingResult) error {
	if request == nil || result == nil {
		return nil
	}
	primary := result.ProfileResults[result.PrimaryProfileName]
	if primary == nil || len(primary.TargetEndpoints) == 0 {
		return nil
	}
	// A picker may return fallback endpoints after the first; the lease is booked on the first.
	target := primary.TargetEndpoints[0]
	if target == nil || target.GetMetadata() == nil {
		return nil
	}
	key := target.GetMetadata().ID.String()
	l.mu.Lock()
	e, ok := l.endpoints[key]
	l.mu.Unlock()
	if !ok {
		log.FromContext(ctx).V(logutil.DEBUG).Info("Not booking lease on unknown endpoint", "endpoint", key)
		return nil
	}

	prompts := promptLengths(request, int64(request.RequestSizeBytes))
	ls := &lease{ep: e, dispatchedAt: l.clock.Now(), state: residentState{
		prompts:        prompts,
		uncachedPrompt: uncachedPromptTokens(target, sum(prompts), l.prefixDK),
		n:              sequencesPerPrompt(request, max(l.cfg.slotsPerEndpoint/int64(len(prompts)), 1)),
		maxOutput:      maxOutputTokens(request, slices.Max(prompts), l.cfg.maxModelLen),
	}}
	ls.update(l.geometry(e), func() {})
	if prev, loaded := l.leases.Swap(request, ls); loaded {
		prev.(*lease).release()
	}
	ls.setBackstop(context.AfterFunc(ctx, func() { l.releaseLease(request, ls) }))
	return nil
}

// releaseLease releases ls if it is still the request's lease.
func (l *Ledger) releaseLease(request *fwksched.InferenceRequest, ls *lease) {
	if l.leases.CompareAndDelete(request, ls) {
		ls.release()
	}
}

// ResponseBody ages the request's lease as its response streams and releases it at end of stream.
func (l *Ledger) ResponseBody(_ context.Context, request *fwksched.InferenceRequest,
	resp *requestcontrol.Response, _ *fwkdl.EndpointMetadata) {
	if request == nil || resp == nil {
		return
	}
	if resp.EndOfStream {
		if v, ok := l.leases.LoadAndDelete(request); ok {
			l.learnAtEnd(v.(*lease), int64(resp.Usage.CompletionTokens))
			v.(*lease).release()
		}
		return
	}
	v, ok := l.leases.Load(request)
	if !ok {
		return
	}
	l.observeChunk(v.(*lease), int64(resp.StreamedEvents), int64(resp.Usage.CompletionTokens))
}

// minPrefillObservation is the fewest uncached prompt tokens whose time to first chunk measures the
// prefill rate (convention). Below it, the fixed per-request overhead dominates the measurement.
const minPrefillObservation = 128

// observeChunk advances a lease on a streamed chunk. Any chunk ends prefill. Each sequence's age is
// its share of the response's cumulative events, one decode step per event; cumulative usage, when
// the stream reports it, replaces the event count. The first chunk measures the endpoint's prefill
// rate; later chunks measure its decode rate.
func (l *Ledger) observeChunk(ls *lease, events, completionTokens int64) {
	now := l.clock.Now()
	g := l.geometry(ls.ep)
	var first bool
	var delta int64
	var sinceLast time.Duration
	var uncached int64
	ls.update(g, func() {
		first = !ls.sawChunk
		ls.sawChunk = true
		if !ls.lastChunk.IsZero() {
			sinceLast = now.Sub(ls.lastChunk)
		}
		ls.lastChunk = now
		ls.state.decoding = true
		uncached = ls.state.uncachedPrompt
		seqs := ls.state.sequences()
		age := ceilDiv(events, seqs) * g.decodeStep
		if completionTokens > 0 {
			age = ceilDiv(completionTokens, seqs)
		}
		age = min(age, ls.state.maxOutput)
		// The first chunk's age replaces any imputed age; after it, age never decreases.
		if !first {
			age = max(age, ls.state.age)
		}
		delta = max(age-ls.state.age, 0)
		ls.state.age = age
	})
	if first {
		if uncached >= minPrefillObservation {
			ls.ep.prefillRate.observe(uncached, now.Sub(ls.dispatchedAt), now)
		}
		return
	}
	if sinceLast > 0 {
		ls.ep.decodeRate.observe(delta, sinceLast, now)
		l.decodeRate.observe(delta, sinceLast, now)
	}
}

// learnAtEnd measures the decode rate from a request that streamed no chunk, from its reported
// completion tokens over its time since dispatch, less its prefill at the endpoint's prefill rate
// when one is measured.
func (l *Ledger) learnAtEnd(ls *lease, completionTokens int64) {
	if completionTokens <= 0 {
		return
	}
	ls.mu.Lock()
	sawChunk, uncached, seqs := ls.sawChunk, ls.state.uncachedPrompt, ls.state.sequences()
	ls.mu.Unlock()
	if sawChunk {
		return
	}
	now := l.clock.Now()
	decoding := now.Sub(ls.dispatchedAt)
	if prefill := ls.ep.prefillRate.estimate(now); prefill.ok && prefill.value > 0 {
		decoding -= time.Duration(float64(uncached) / prefill.value * float64(time.Second))
	}
	tokens := ceilDiv(completionTokens, seqs)
	ls.ep.decodeRate.observe(tokens, decoding, now)
	l.decodeRate.observe(tokens, decoding, now)
}

func ceilDiv(a, b int64) int64 {
	if b <= 0 || a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}
