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

	collectors = []prometheus.Collector{bookedVec, capacityVec, eligibilityVec, driftVec, bypassVec, unratedLeasesVec}
)

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
	}, nil
}

// deleteEndpoint removes this instance's per-endpoint series for an endpoint. Partial-match deletion
// ignores curried labels, so it runs on the shared vectors with producer_name in the match.
func (m *ledgerMetrics) deleteEndpoint(id types.NamespacedName) {
	labels := prometheus.Labels{"endpoint_name": id.Name, "namespace": id.Namespace, producerLabel: m.producer}
	bookedVec.DeletePartialMatch(labels)
	capacityVec.DeletePartialMatch(labels)
	eligibilityVec.DeletePartialMatch(labels)
}
