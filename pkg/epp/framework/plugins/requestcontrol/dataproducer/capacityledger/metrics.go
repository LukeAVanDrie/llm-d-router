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
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

const producerLabel = "producer_name"

var endpointLabels = []string{"endpoint_name", "namespace", producerLabel}

func withLabels(base []string, extra ...string) []string {
	return append(append([]string{}, base...), extra...)
}

// The vectors are shared across ledger instances; each instance curries producer_name.
var (
	bookedVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_booked",
		Help: metricsutil.HelpMsgWithStability(
			"The capacity ledger's booked use per endpoint and axis: memory in KV cache blocks, step in tokens committed to the next step, slots in sequences.",
			compbasemetrics.ALPHA),
	}, withLabels(endpointLabels, "axis"))

	capacityVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_capacity",
		Help: metricsutil.HelpMsgWithStability(
			"The capacity ledger's capacity per endpoint and axis, in the units of capacity_ledger_booked.",
			compbasemetrics.ALPHA),
	}, withLabels(endpointLabels, "axis"))

	eligibilityVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_endpoint_eligibility",
		Help: metricsutil.HelpMsgWithStability(
			"1 for the endpoint's current eligibility under the capacity ledger's accounting; exactly one series per endpoint is set.",
			compbasemetrics.ALPHA),
	}, withLabels(endpointLabels, "eligibility"))

	driftVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_drift_ratio",
		Help: metricsutil.HelpMsgWithStability(
			"Booked minus scraped over capacity, per endpoint at each new scrape; axis is memory or slots. Meaningful only with a single router replica.",
			compbasemetrics.ALPHA),
		Buckets: []float64{-1, -0.5, -0.25, -0.1, -0.05, -0.01, 0, 0.01, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{producerLabel, "axis"})

	bypassVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_bypass_excess",
		Help: metricsutil.HelpMsgWithStability(
			"The engine's running plus waiting count minus the capacity ledger's booked slots, per endpoint at each new scrape. Persistent positive values indicate traffic that did not pass this router. Meaningful only with a single router replica.",
			compbasemetrics.ALPHA),
		Buckets: []float64{-64, -16, -4, -1, 0, 1, 4, 16, 64},
	}, []string{producerLabel})

	unratedLeasesVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_unrated_leases",
		Help: metricsutil.HelpMsgWithStability(
			"Leases that have streamed no chunk and have no measured decode rate, so their output is not booked until the next scrape reflects it.",
			compbasemetrics.ALPHA),
	}, []string{producerLabel})

	reservationsVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_reservations",
		Help: metricsutil.HelpMsgWithStability(
			"Reservations the endpoint gate holds: gated requests not yet placed, refunded or reaped.",
			compbasemetrics.ALPHA),
	}, []string{producerLabel})

	gateChecksVec = newCounterVec("capacity_ledger_gate_checks_total",
		"Checks of tracked candidate endpoints by the endpoint gate; result is fits, the endpoint's ineligibility, the axis the request does not fit (memory, step, slots), or reserved-<axis> when it fits but for other requests' reservations.",
		"result")
	wouldHoldVec = newCounterVec("capacity_ledger_would_hold_total",
		"Gated requests that fit no candidate endpoint; the request dispatched without a reservation. cause is reserved (some candidate fits but for other requests' reservations), capacity (some candidate is over capacity) or stale (every accountable candidate has stale metrics).",
		"cause")
	unaccountedVec = newCounterVec("capacity_ledger_unaccounted_total",
		"Gated requests none of whose candidate endpoints the ledger can account for; the request dispatched without a reservation.")
	placementRacesVec = newCounterVec("capacity_ledger_placement_races_total",
		"Placements of gated requests that a check at placement would refuse; cause is recheck (no candidate fit with the exact prompt), unfit-placement (placed on an endpoint that did not fit while another did), or unfiltered (the ledger's filter did not run for the request).",
		"cause")
	reservationsReapedVec = newCounterVec("capacity_ledger_reservations_reaped_total",
		"Reservations released by the reservation TTL that were never bound to a request; nonzero indicates a caller that neither dispatched nor refunded them.")
	unreservedPlacementsVec = newCounterVec("capacity_ledger_unreserved_placements_total",
		"Placements without a reservation after the ledger has served as the endpoint gate.")

	fittingEndpointsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_gate_fitting_endpoints",
		Help:      metricsutil.HelpMsgWithStability("Candidate endpoints a gated request fits.", compbasemetrics.ALPHA),
		Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64},
	}, []string{producerLabel})
	placementLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_placement_latency_seconds",
		Help:      metricsutil.HelpMsgWithStability("Time from a gated request's reservation to its placement.", compbasemetrics.ALPHA),
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{producerLabel})
	reservationToLeaseVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      "capacity_ledger_reservation_to_lease_ratio",
		Help:      metricsutil.HelpMsgWithStability("A placed request's lease over its reservation at placement, per axis (memory or step); below 1 means the reservation overstated.", compbasemetrics.ALPHA),
		Buckets:   []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2},
	}, []string{producerLabel, "axis"})

	collectors = []prometheus.Collector{
		bookedVec, capacityVec, eligibilityVec, driftVec, bypassVec, unratedLeasesVec,
		reservationsVec, gateChecksVec, wouldHoldVec, unaccountedVec, placementRacesVec, reservationsReapedVec,
		unreservedPlacementsVec, fittingEndpointsVec, placementLatencyVec, reservationToLeaseVec,
	}
)

func newCounterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
		Name:      name,
		Help:      metricsutil.HelpMsgWithStability(help, compbasemetrics.ALPHA),
	}, withLabels([]string{producerLabel}, labels...))
}

// ledgerMetrics is one ledger instance's view of the shared vectors.
type ledgerMetrics struct {
	producer    string
	booked      *prometheus.GaugeVec
	capacity    *prometheus.GaugeVec
	eligibility *prometheus.GaugeVec
	drift       prometheus.ObserverVec
	bypass      prometheus.Observer
	// unratedLeases is set on each sampling pass.
	unratedLeases prometheus.Gauge

	reservations prometheus.Gauge
	// gateChecks holds one counter per possible result, resolved at construction so the dispatch
	// goroutine does not resolve labels.
	gateChecks           map[string]prometheus.Counter
	gateChecksVec        *prometheus.CounterVec
	wouldHold            map[holdCause]prometheus.Counter
	unaccounted          prometheus.Counter
	placementRaces       *prometheus.CounterVec
	reservationsReaped   prometheus.Counter
	unreservedPlacements prometheus.Counter
	fittingEndpoints     prometheus.Observer
	placementLatency     prometheus.Observer
	reservationToLease   map[axis]prometheus.Observer
}

// newLedgerMetrics registers the shared vectors, which is a no-op for a second instance, and
// curries them for this instance.
func newLedgerMetrics(registerer prometheus.Registerer, producer string) (*ledgerMetrics, error) {
	if registerer == nil {
		return nil, errors.New("capacity ledger metrics registerer is required")
	}
	for _, c := range collectors {
		if err := registerer.Register(c); err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) && already.ExistingCollector == c {
				continue
			}
			return nil, fmt.Errorf("register capacity ledger metric: %w", err)
		}
	}
	p := prometheus.Labels{producerLabel: producer}
	return &ledgerMetrics{
		producer:    producer,
		booked:      bookedVec.MustCurryWith(p),
		capacity:    capacityVec.MustCurryWith(p),
		eligibility: eligibilityVec.MustCurryWith(p),
		drift:       driftVec.MustCurryWith(p),
		bypass:      bypassVec.WithLabelValues(producer),

		unratedLeases: unratedLeasesVec.WithLabelValues(producer),

		reservations:  reservationsVec.WithLabelValues(producer),
		gateChecks:    gateCheckCounters(gateChecksVec.MustCurryWith(p)),
		gateChecksVec: gateChecksVec.MustCurryWith(p),
		wouldHold: map[holdCause]prometheus.Counter{
			holdReserved: wouldHoldVec.WithLabelValues(producer, string(holdReserved)),
			holdCapacity: wouldHoldVec.WithLabelValues(producer, string(holdCapacity)),
			holdStale:    wouldHoldVec.WithLabelValues(producer, string(holdStale)),
		},
		unaccounted:          unaccountedVec.WithLabelValues(producer),
		placementRaces:       placementRacesVec.MustCurryWith(p),
		reservationsReaped:   reservationsReapedVec.WithLabelValues(producer),
		unreservedPlacements: unreservedPlacementsVec.WithLabelValues(producer),
		fittingEndpoints:     fittingEndpointsVec.WithLabelValues(producer),
		placementLatency:     placementLatencyVec.WithLabelValues(producer),
		reservationToLease: map[axis]prometheus.Observer{
			axisMemory: reservationToLeaseVec.WithLabelValues(producer, axisMemory.String()),
			axisStep:   reservationToLeaseVec.WithLabelValues(producer, axisStep.String()),
		},
	}, nil
}

// gateCheckCounters resolves the counter of every result a gate check can report.
func gateCheckCounters(vec *prometheus.CounterVec) map[string]prometheus.Counter {
	ineligible := []attrcapacity.Eligibility{attrcapacity.Stale, attrcapacity.NoBlockCount,
		attrcapacity.MultiEngine, attrcapacity.HybridLayout, attrcapacity.SpeculativeUnconfigured,
		attrcapacity.EngineType, attrcapacity.Disaggregated}
	results := make([]string, 0, 1+len(ineligible)+2*int(numAxes))
	results = append(results, "fits")
	for _, e := range ineligible {
		results = append(results, string(e))
	}
	for a := range numAxes {
		results = append(results, a.String(), "reserved-"+a.String())
	}
	counters := make(map[string]prometheus.Counter, len(results))
	for _, r := range results {
		counters[r] = vec.WithLabelValues(r)
	}
	return counters
}

// gateCheck returns the counter for a gate check result.
func (m *ledgerMetrics) gateCheck(result string) prometheus.Counter {
	if c, ok := m.gateChecks[result]; ok {
		return c
	}
	return m.gateChecksVec.WithLabelValues(result)
}

// deleteEndpoint removes this instance's per-endpoint series for an endpoint. Partial-match deletion
// ignores curried labels, so it runs on the shared vectors with producer_name in the match.
func (m *ledgerMetrics) deleteEndpoint(id types.NamespacedName) {
	labels := prometheus.Labels{"endpoint_name": id.Name, "namespace": id.Namespace, producerLabel: m.producer}
	bookedVec.DeletePartialMatch(labels)
	capacityVec.DeletePartialMatch(labels)
	eligibilityVec.DeletePartialMatch(labels)
}
