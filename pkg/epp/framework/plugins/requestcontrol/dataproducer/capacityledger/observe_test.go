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
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func driftCount(t *testing.T, producer, axis string) uint64 {
	t.Helper()
	obs, err := driftVec.GetMetricWithLabelValues(producer, axis)
	require.NoError(t, err)
	return histogramSamples(t, obs)
}

func histogramSamples(t *testing.T, obs prometheus.Observer) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, obs.(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}

func TestRecord(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	producer := l.typedName.Name
	ep := newTestEndpoint("a", 0.2, testStart)
	ep.UpdateMetrics(testMetrics(0.2, 3, 2, testStart))
	addEndpoint(t, l, ep)
	// With no rates the sample imputes no output: 100 + 1 = 101 tokens is 7 blocks.
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, true, ptr(int64(11))), resultFor(l, ep, 0, 0)))

	l.sample()
	require.InDelta(t, 7, promtestutil.ToFloat64(bookedVec.WithLabelValues("a", "ns", producer, "memory")), 0)
	require.InDelta(t, 100, promtestutil.ToFloat64(capacityVec.WithLabelValues("a", "ns", producer, "memory")), 0)
	require.InDelta(t, testStepBudget, promtestutil.ToFloat64(capacityVec.WithLabelValues("a", "ns", producer, "step")), 0)
	require.InDelta(t, 1, promtestutil.ToFloat64(eligibilityVec.WithLabelValues("a", "ns", producer, "eligible")), 0)
	require.Equal(t, uint64(1), driftCount(t, producer, "memory"))

	l.sample()
	require.Equal(t, uint64(1), driftCount(t, producer, "memory"), "no new scrape, no new observation")

	// The endpoint goes stale: exactly one eligibility series remains.
	clk.Step(3 * time.Second)
	l.sample()
	require.Equal(t, 1, countSeries(t, eligibilityVec, producer))
	require.InDelta(t, 1, promtestutil.ToFloat64(eligibilityVec.WithLabelValues("a", "ns", producer, "stale")), 0)

	require.NoError(t, l.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: ep}))
	require.Zero(t, countSeries(t, bookedVec, producer))
	require.Zero(t, countSeries(t, eligibilityVec, producer))
}

// countSeries counts a vector's series for one producer.
func countSeries(t *testing.T, v *prometheus.GaugeVec, producer string) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() { v.Collect(ch); close(ch) }()
	n := 0
	for m := range ch {
		pb := &dto.Metric{}
		require.NoError(t, m.Write(pb))
		for _, lp := range pb.GetLabel() {
			if lp.GetName() == producerLabel && lp.GetValue() == producer {
				n++
			}
		}
	}
	return n
}

func TestDumpState(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	addEndpoint(t, l, newTestEndpoint("a", 0.2, testStart))
	addEndpoint(t, l, newTestEndpoint("b", 0.5, testStart))
	raw, err := l.DumpState()
	require.NoError(t, err)
	var state ledgerState
	require.NoError(t, json.Unmarshal(raw, &state))
	require.Equal(t, 2, state.TotalEndpoints)
	require.Equal(t, "ns/b", state.Endpoints[0].Endpoint, "the endpoint with more used blocks first")
}
