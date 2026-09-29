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

// Package capacityledger books every placed request against the endpoint that serves it and
// publishes, per endpoint, the booked use, the engine's scraped use, and the capacity on each of
// three axes: KV cache blocks, tokens committed to the next step, and concurrent sequences.
package capacityledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	capacityledgerconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/capacityledger/constants"
	tokenproducer "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
)

// CapacityLedgerType is the plugin type of the capacity ledger.
const CapacityLedgerType = capacityledgerconstants.CapacityLedgerType

var (
	_ fwkplugin.ProducerPlugin             = &Ledger{}
	_ fwkplugin.ConsumerPlugin             = &Ledger{}
	_ fwkplugin.StateDumper                = &Ledger{}
	_ requestcontrol.PreRequest            = &Ledger{}
	_ requestcontrol.ResponseBodyProcessor = &Ledger{}
	_ fwkdl.EndpointExtractor              = (*Ledger)(nil)
	_ fwkdl.Registrant                     = &Ledger{}
	_ fwkfc.EndpointGate                   = &Ledger{}
	_ fwksched.Filter                      = &Ledger{}
)

// Ledger is the capacity ledger plugin. It adapts the framework's extension points to the
// per-endpoint accounts; the accounts hold the state.
type Ledger struct {
	typedName fwkplugin.TypedName
	cfg       config
	clock     clock.WithTicker
	dk        fwkplugin.DataKey
	prefixDK  fwkplugin.DataKey
	engines   *engineCounter
	metrics   *ledgerMetrics

	// mu guards endpoint membership.
	mu        sync.Mutex
	endpoints map[string]*endpoint
	// leases maps *fwksched.InferenceRequest to *lease, and reservations to *reservation. Requests
	// are keyed by pointer: the same request object reaches every extension point, and the request
	// ID comes from a header a client can set.
	leases       sync.Map
	reservations sync.Map
	// gated is set once flow control has consulted the ledger as its endpoint gate.
	gated atomic.Bool
	// decodeRate is the decode rate across every endpoint, used where an endpoint has none.
	decodeRate rate

	sampleInterval time.Duration
	// sampleMu serializes sampling passes with endpoint deletion, so a deleted endpoint's series
	// are not recreated.
	sampleMu sync.Mutex
}

// Factory creates a capacity ledger from its plugin parameters.
func Factory(name string, decoder *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, errors.New("handle is nil")
	}
	var api apiConfig
	if decoder != nil {
		if err := decoder.Decode(&api); err != nil {
			return nil, fmt.Errorf("failed to decode %s parameters: %w", CapacityLedgerType, err)
		}
	}
	ctx := handle.Context()
	if ctx == nil {
		return nil, errors.New("handle has no context")
	}
	cfg, err := newConfig(api, handle.MetricsStalenessThreshold())
	if err != nil {
		return nil, fmt.Errorf("invalid %s parameters: %w", CapacityLedgerType, err)
	}
	metrics, err := newLedgerMetrics(handle.Metrics(), name)
	if err != nil {
		return nil, err
	}
	sampleInterval := handle.RefreshMetricsInterval()
	if sampleInterval <= 0 {
		sampleInterval = defaultSampleInterval
	}
	l := newLedger(name, cfg, clock.RealClock{}, sampleInterval, metrics)
	go l.sampleLoop(ctx)
	return l, nil
}

func newLedger(name string, cfg config, clk clock.WithTicker, sampleInterval time.Duration,
	metrics *ledgerMetrics) *Ledger {
	l := &Ledger{
		typedName:      fwkplugin.TypedName{Type: CapacityLedgerType, Name: name},
		cfg:            cfg,
		clock:          clk,
		dk:             attrcapacity.EndpointCapacityDataKey.WithNonEmptyProducerName(name),
		prefixDK:       attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(cfg.prefixProducerName),
		metrics:        metrics,
		endpoints:      map[string]*endpoint{},
		sampleInterval: sampleInterval,
	}
	l.engines = newEngineCounter(name, l.markScrape)
	return l
}

// TypedName returns the plugin's type and name.
func (l *Ledger) TypedName() fwkplugin.TypedName { return l.typedName }

// Produces declares the per-endpoint capacity view.
func (l *Ledger) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{l.dk: attrcapacity.EndpointCapacity{}}
}

// Consumes declares the tokenized prompt, for exact prompt lengths, and the prefix-cache match,
// for a lease's uncached prompt.
func (l *Ledger) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{tokenproducer.TokenizedPromptDataKey: fwksched.TokenizedRequest{}},
		Optional: map[fwkplugin.DataKey]any{l.prefixDK: attrprefix.PrefixCacheMatchInfo{}},
	}
}

// RegisterDependencies subscribes the ledger to endpoint lifecycle events and its engine counter to
// the raw metrics scrape. The engine counter registers after the metrics extractor, so it runs
// after it on each scrape.
func (l *Ledger) RegisterDependencies(r fwkdl.Registrar) error {
	if err := r.Register(fwkdl.PendingRegistration{
		Owner:      l.typedName,
		SourceType: sourcenotifications.EndpointNotificationSourceType,
		Extractor:  l,
		DefaultSource: sourcenotifications.NewEndpointDataSource(
			sourcenotifications.EndpointNotificationSourceType, sourcenotifications.EndpointNotificationSourceType),
	}); err != nil {
		return err
	}
	return r.Register(fwkdl.PendingRegistration{
		Owner:      l.typedName,
		SourceType: sourcemetrics.MetricsDataSourceType,
		Extractor:  l.engines,
		IfMissing:  fwkdl.Warn,
	})
}

// geometry is the endpoint's current geometry.
func (l *Ledger) geometry(e *endpoint) geometry { return l.geometryFor(e.datalayer().GetMetrics()) }

// geometryFor is the geometry of an endpoint with metrics m. Block size is 0 until the endpoint is
// scraped.
func (l *Ledger) geometryFor(m *fwkdl.Metrics) geometry {
	g := geometry{stepBudget: l.cfg.stepTokenBudget, decodeStep: l.cfg.decodeStep()}
	if m != nil {
		g.blockSize = int64(m.CacheBlockSize)
	}
	return g
}

// snapshot is a consistent read of an endpoint's state. used excludes reservations.
type snapshot struct {
	acct     accountState
	scraped  vec
	capacity vec
	used     vec
	geo      geometry
	metrics  *fwkdl.Metrics
	elig     attrcapacity.Eligibility
}

func (l *Ledger) snapshot(e *endpoint) snapshot {
	ep := e.datalayer()
	s := snapshot{acct: e.acct.snapshot(), metrics: ep.GetMetrics()}
	s.geo = l.geometryFor(s.metrics)
	s.elig = eligibility(ep.GetMetadata(), s.metrics, l.engines.get(e.id.String()), l.cfg, l.clock.Now())
	s.capacity[axisStep] = l.cfg.stepTokenBudget
	s.capacity[axisSlots] = l.cfg.slotsPerEndpoint
	var scrapeTime time.Time
	if use, memory, ok := scraped(s.metrics, l.cfg.decodeStep()); ok {
		s.scraped = use
		s.capacity[axisMemory] = memory
		scrapeTime = s.metrics.UpdateTime
	}
	s.used = usedState(s.acct, s.scraped, scrapeTime)
	return s
}

// markScrape records the endpoint's bookings at the scrape the datalayer endpoint's metrics carry.
// The engine counter calls it after the metrics extractor has run for the same scrape; when a
// scrape is not recorded, the state rule falls back to the larger of booked and scraped.
func (l *Ledger) markScrape(ep fwkdl.Endpoint) {
	if ep == nil || ep.GetMetadata() == nil {
		return
	}
	m := ep.GetMetrics()
	if m == nil || m.UpdateTime.IsZero() {
		return
	}
	l.mu.Lock()
	e, ok := l.endpoints[ep.GetMetadata().ID.String()]
	l.mu.Unlock()
	if ok && e.datalayer() == ep {
		e.acct.markScrape(m.UpdateTime)
	}
}

// view builds the endpoint's published capacity view. Reservations are not published.
func (l *Ledger) view(e *endpoint) *attrcapacity.EndpointCapacity {
	s := l.snapshot(e)
	axisView := func(a axis) attrcapacity.Axis {
		return attrcapacity.Axis{Capacity: s.capacity[a], Booked: s.acct.booked[a], Scraped: s.scraped[a], Used: s.used[a]}
	}
	v := &attrcapacity.EndpointCapacity{
		Memory:      axisView(axisMemory),
		Step:        axisView(axisStep),
		Slots:       axisView(axisSlots),
		Eligibility: s.elig,
		Generation:  s.acct.generation,
	}
	if s.metrics != nil {
		v.BlockSize = int64(s.metrics.CacheBlockSize)
		v.ScrapeTime = s.metrics.UpdateTime
	}
	return v
}
