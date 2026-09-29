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
	"math"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
)

func TestFootprint(t *testing.T) {
	g := geometry{blockSize: 16, stepBudget: 2048, decodeStep: 1}
	tests := []struct {
		name string
		r    residentState
		g    geometry
		want vec
	}{
		{name: "prefilling", r: residentState{prompts: []int64{100}, uncachedPrompt: 52, n: 1}, g: g,
			want: vec{7, 52, 1}},
		{name: "decoding", r: residentState{prompts: []int64{100}, uncachedPrompt: 52, n: 1, age: 20, decoding: true}, g: g,
			want: vec{8, 1, 1}},
		{name: "step capped at the budget", r: residentState{prompts: []int64{5000}, uncachedPrompt: 5000, n: 1}, g: g,
			want: vec{313, 2048, 1}},
		{name: "n sequences", r: residentState{prompts: []int64{100}, uncachedPrompt: 100, n: 4}, g: g,
			want: vec{28, 400, 4}},
		{name: "n sequences decoding with speculative tokens", r: residentState{prompts: []int64{100}, n: 4, decoding: true},
			g: geometry{blockSize: 16, stepBudget: 2048, decodeStep: 3}, want: vec{28, 12, 4}},
		{name: "two prompts", r: residentState{prompts: []int64{100, 50}, uncachedPrompt: 150, n: 1}, g: g,
			want: vec{11, 150, 2}},
		{name: "block size unknown", r: residentState{prompts: []int64{100}, uncachedPrompt: 100, n: 1},
			g: geometry{stepBudget: 2048, decodeStep: 1}, want: vec{0, 100, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, footprint(tc.r, tc.g))
		})
	}
}

func TestMaxOutputTokens(t *testing.T) {
	req := newTestRequest(100, true, nil)
	require.Equal(t, int64(3996), maxOutputTokens(req, 100, testMaxModelLen), "no client cap: bounded by the context")
	req = newTestRequest(100, true, ptr(int64(50)))
	require.Equal(t, int64(50), maxOutputTokens(req, 100, testMaxModelLen))
	req = newTestRequest(100, true, ptr(int64(8000)))
	require.Equal(t, int64(3996), maxOutputTokens(req, 100, testMaxModelLen))
}

func TestSequencesPerPrompt(t *testing.T) {
	req := newTestRequest(100, true, nil)
	require.Equal(t, int64(1), sequencesPerPrompt(req, 8))
	req.Body.Payload = fwkrh.PayloadMap{"n": float64(4)}
	require.Equal(t, int64(4), sequencesPerPrompt(req, 8))
	req.Body.Payload = fwkrh.PayloadMap{"n": "4"}
	require.Equal(t, int64(1), sequencesPerPrompt(req, 8), "a malformed n is ignored")
	req.Body.Payload = fwkrh.PayloadMap{"n": float64(1 << 62)}
	require.Equal(t, int64(8), sequencesPerPrompt(req, 8), "n is clamped to the limit")
}

func TestClientNClampedToSlots(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(10, true, nil)
	req.Body.Payload = fwkrh.PayloadMap{"n": float64(1_000_000)}
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	require.Equal(t, int64(testSlots), booked(e)[axisSlots])
}

func TestMultiSequenceRequestBooksPerSequence(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(100, true, nil)
	req.Body.Payload = fwkrh.PayloadMap{"n": float64(4)}
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	require.Equal(t, vec{28, 400, 4}, booked(e))

	// 8 events across 4 sequences are 2 tokens each: 100 + 2 + 1 = 103 tokens is 7 blocks each.
	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 8}, nil)
	require.Equal(t, vec{28, 4, 4}, booked(e))
}

func TestLeaseBookedBeforeFirstScrapeBooksBlocksAfter(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := fwkdl.NewEndpoint(newTestEndpoint("a", 0, testStart).GetMetadata(), &fwkdl.Metrics{})
	e := addEndpoint(t, l, ep)
	req := newTestRequest(100, true, ptr(int64(11)))
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	require.Equal(t, vec{0, 100, 1}, booked(e), "no block size before the first scrape")

	ep.UpdateMetrics(testMetrics(0, 0, 0, testStart))
	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 1}, nil)
	require.Equal(t, vec{7, 1, 1}, booked(e), "100 + 1 + 1 = 102 tokens is 7 blocks")
}

func TestStateRuleAddsBookingsSinceScrape(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.5, testStart)
	e := addEndpoint(t, l, ep)
	l.markScrape(ep)
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, true, nil), resultFor(l, ep, 0, 0)))
	require.Equal(t, int64(7), booked(e)[axisMemory])

	v := l.view(e)
	require.Equal(t, attrcapacity.Axis{Capacity: 100, Booked: 7, Scraped: 50, Used: 57}, v.Memory,
		"the scrape does not reflect the request placed after it")
}

func TestTinyPromptDoesNotMeasurePrefillRate(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(16, true, nil)
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	clk.Step(100 * time.Millisecond)
	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 1}, nil)
	require.False(t, e.prefillRate.estimate(clk.Now()).ok)
	require.Equal(t, int64(1), booked(e)[axisStep], "the chunk still ends prefill")
}

func TestBufferedStreamIsImputed(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	// A streaming request whose response arrives as one buffered body streams no chunks.
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, true, ptr(int64(300))), resultFor(l, ep, 0, 0)))
	l.sample()
	require.Equal(t, vec{7, 1, 1}, booked(e), "with no rates the request is prefilled and has no output")
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.unratedLeases), 0)

	e.decodeRate.observe(50, time.Second, clk.Now())
	clk.Step(time.Second)
	l.sample()
	require.Equal(t, vec{blocksFor(100+50+1, testBlockSize), 1, 1}, booked(e))
	require.Zero(t, promtestutil.ToFloat64(l.metrics.unratedLeases))
}

func TestPoolDecodeRateCoversEndpointsWithout(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	rated := newTestEndpoint("rated", 0, testStart)
	unrated := newTestEndpoint("unrated", 0, testStart)
	addEndpoint(t, l, rated)
	e := addEndpoint(t, l, unrated)
	// A streaming request on the rated endpoint measures 50 tokens/s.
	req := newTestRequest(16, true, nil)
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, rated, 0, 0)))
	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 1}, nil)
	clk.Step(time.Second)
	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 51}, nil)

	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, false, ptr(int64(300))), resultFor(l, unrated, 0, 0)))
	clk.Step(time.Second)
	l.sample()
	require.Equal(t, blocksFor(100+50+1, testBlockSize), booked(e)[axisMemory])
}

func TestDecodeRateLearnedAtEndOfStream(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(100, false, nil)
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	clk.Step(2 * time.Second)
	l.ResponseBody(context.Background(), req, &fwkrc.Response{EndOfStream: true, Usage: fwkrh.Usage{CompletionTokens: 80}}, nil)
	rate := e.decodeRate.estimate(clk.Now())
	require.True(t, rate.ok)
	require.InDelta(t, 40.0, rate.value, 1e-9, "80 tokens in 2s with no prefill rate")
	require.True(t, l.decodeRate.estimate(clk.Now()).ok, "the pool rate learns too")
}

func TestSaturatingArithmetic(t *testing.T) {
	require.Equal(t, int64(math.MaxInt64), mulSat(1<<62, 4))
	require.Equal(t, int64(12), mulSat(3, 4))
	require.Equal(t, int64(math.MaxInt64), addSat(math.MaxInt64-1, 2))
	r := residentState{prompts: []int64{100}, uncachedPrompt: 100, n: 1 << 62}
	w := footprint(r, geometry{blockSize: 16, stepBudget: 2048, decodeStep: 1})
	for a := range numAxes {
		require.GreaterOrEqual(t, w[a], int64(0), a.String())
	}
}

func TestBlocksUsed(t *testing.T) {
	for used := int64(0); used <= 100; used++ {
		require.Equal(t, used, blocksUsed(float64(used)/100, 100), used)
	}
	require.Equal(t, int64(30), blocksUsed(0.291, 100), "a fraction between blocks rounds up")
}

func TestFirstChunkReplacesImputedAge(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	e.decodeRate.observe(1000, time.Second, clk.Now())
	req := newTestRequest(100, true, ptr(int64(300)))
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	clk.Step(time.Second)
	l.sample()
	require.Equal(t, blocksFor(100+300+1, testBlockSize), booked(e)[axisMemory], "imputed at the maximum output")

	l.ResponseBody(context.Background(), req, &fwkrc.Response{StreamedEvents: 2}, nil)
	require.Equal(t, blocksFor(100+2+1, testBlockSize), booked(e)[axisMemory], "observed age replaces the imputed age")
	l.sample()
	require.Equal(t, blocksFor(100+2+1, testBlockSize), booked(e)[axisMemory], "a lease with chunks is not imputed")
}

func TestRateExpires(t *testing.T) {
	var r rate
	r.observe(100, time.Second, testStart)
	require.Equal(t, rateEstimate{value: 100, ok: true}, r.estimate(testStart.Add(rateExpiry)))
	require.False(t, r.estimate(testStart.Add(rateExpiry+time.Second)).ok)

	r.observe(10, time.Second, testStart.Add(time.Minute))
	require.Equal(t, rateEstimate{value: 10, ok: true}, r.estimate(testStart.Add(time.Minute)),
		"an observation after expiry replaces the value")
}
