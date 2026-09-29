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
	"math/rand"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
)

func fcRequest(ir *fwksched.InferenceRequest) fwkfc.FlowControlRequest {
	req := fwkfcmocks.NewMockFlowControlRequest(uint64(ir.RequestSizeBytes), ir.RequestID, fwkfc.FlowKey{ID: "f"})
	req.InferenceRequestV = ir
	return req
}

func reserved(e *endpoint) vec {
	return e.acct.snapshot().reserved
}

func TestCheck(t *testing.T) {
	base := snapshot{capacity: vec{100, 2048, 8}, elig: attrcapacity.Eligible}
	with := func(f func(*snapshot)) snapshot { s := base; f(&s); return s }
	tests := []struct {
		name  string
		s     snapshot
		w     vec
		own   vec
		want  string
		wantW vec
	}{
		{name: "fits", s: base, w: vec{10, 100, 1}, want: "fits", wantW: vec{10, 100, 1}},
		{name: "stale reads as full", s: with(func(s *snapshot) { s.elig = attrcapacity.Stale }), w: vec{1, 1, 1}, want: "stale"},
		{name: "ineligible", s: with(func(s *snapshot) { s.elig = attrcapacity.EngineType }), w: vec{1, 1, 1}, want: "engine-type"},
		{name: "memory", s: with(func(s *snapshot) { s.used = vec{95, 0, 0} }), w: vec{10, 1, 1}, want: "memory"},
		{name: "long prompt fits a busy step budget", s: with(func(s *snapshot) { s.used = vec{0, 2000, 1} }), w: vec{1, 2048, 1},
			want: "fits", wantW: vec{1, 48, 1}},
		{name: "step budget spent", s: with(func(s *snapshot) { s.used = vec{0, 2048, 1} }), w: vec{1, 100, 1}, want: "step"},
		{name: "slots", s: with(func(s *snapshot) { s.used = vec{0, 0, 8} }), w: vec{1, 1, 1}, want: "slots"},
		{name: "reserved", s: with(func(s *snapshot) { s.acct.reserved = vec{95, 0, 1} }), w: vec{10, 1, 1}, want: "reserved-memory"},
		{name: "step budget left after reservations", s: with(func(s *snapshot) { s.acct.reserved = vec{0, 2000, 1} }), w: vec{1, 2048, 1},
			want: "fits", wantW: vec{1, 48, 1}},
		{name: "own reservation excluded", s: with(func(s *snapshot) { s.acct.reserved = vec{95, 100, 1} }), w: vec{10, 1, 1},
			own: vec{95, 100, 1}, want: "fits", wantW: vec{10, 1, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, w := check(tc.s, tc.w, tc.own)
			require.Equal(t, tc.want, v.label())
			require.Equal(t, tc.wantW, w)
		})
	}
}

func TestDemandCappedAtCapacity(t *testing.T) {
	s := snapshot{capacity: vec{100, 2048, 8}, geo: geometry{blockSize: 16, stepBudget: 2048, decodeStep: 1}}
	one := func(prompt, n int64) residentState {
		return residentState{prompts: []int64{prompt}, uncachedPrompt: prompt, n: n}
	}
	require.Equal(t, vec{7, 100, 1}, demand(one(100, 1), s))
	require.Equal(t, vec{100, 2048, 1}, demand(one(1_000_000, 1), s),
		"an oversized request is capped so an empty endpoint can take it alone")
	require.Equal(t, vec{28, 400, 4}, demand(one(100, 4), s), "one charge per sequence")
}

func TestGateDemandEqualsLeaseFootprint(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.1, testStart)
	e := addEndpoint(t, l, ep)
	ir := newTestRequest(100, true, nil)
	ir.Body.Payload = fwkrh.PayloadMap{"n": float64(2)}
	d := l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{ep})
	require.NotNil(t, d.Reservation)
	atGate := reserved(e)
	require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, ep, 0, 0)))
	require.Equal(t, atGate, booked(e), "with no prefix match, the lease books what the gate reserved")
}

func TestGateReservesWhereTheRequestFits(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	roomy := addEndpoint(t, l, newTestEndpoint("roomy", 0.1, testStart))
	full := addEndpoint(t, l, newTestEndpoint("full", 0.99, testStart))
	ir := newTestRequest(100, true, nil)

	d := l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{roomy.datalayer(), full.datalayer()})
	require.NotNil(t, d.Reservation)
	require.Equal(t, vec{7, 100, 1}, reserved(roomy), "the request's demand is reserved where it fits")
	require.Equal(t, vec{}, reserved(full))

	d.Reservation.Refund()
	require.Equal(t, vec{}, reserved(roomy))
	d.Reservation.Refund()
	require.Equal(t, vec{}, reserved(roomy), "refund is idempotent")
}

func TestGateReservesOnOneEndpoint(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	// 80 free blocks each: two 57-block requests fit the pool, one on each endpoint.
	a := addEndpoint(t, l, newTestEndpoint("a", 0.2, testStart))
	b := addEndpoint(t, l, newTestEndpoint("b", 0.2, testStart))
	candidates := []fwkdl.Endpoint{a.datalayer(), b.datalayer()}
	require.NotNil(t, l.Gate(context.Background(), fcRequest(newTestRequest(900, true, nil)), candidates).Reservation)
	require.NotNil(t, l.Gate(context.Background(), fcRequest(newTestRequest(900, true, nil)), candidates).Reservation)
	require.Equal(t, int64(1), reserved(a)[axisSlots])
	require.Equal(t, int64(1), reserved(b)[axisSlots])
}

func TestGateWitnessHasTheMostHeadroom(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	busy := addEndpoint(t, l, newTestEndpoint("busy", 0.5, testStart))
	idle := addEndpoint(t, l, newTestEndpoint("idle", 0.1, testStart))
	d := l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{busy.datalayer(), idle.datalayer()})
	require.NotNil(t, d.Reservation)
	require.Equal(t, vec{}, reserved(busy))
	require.Equal(t, vec{7, 100, 1}, reserved(idle))
}

func TestGateCountsWouldHoldAndUnaccounted(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	full := addEndpoint(t, l, newTestEndpoint("full", 0.99, testStart))
	stale := addEndpoint(t, l, newTestEndpoint("stale", 0, testStart.Add(-time.Minute)))
	other := newTestEndpoint("other", 0, testStart)
	other.GetMetadata().Labels["llm-d.ai/engine-type"] = "sglang"
	ineligible := addEndpoint(t, l, other)

	d := l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{full.datalayer()})
	require.Nil(t, d.Reservation)
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.wouldHold[holdCapacity]), 0)

	d = l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{stale.datalayer()})
	require.Nil(t, d.Reservation)
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.wouldHold[holdStale]), 0, "a stale endpoint reads as full")

	d = l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{ineligible.datalayer()})
	require.Nil(t, d.Reservation, "no accountable candidate: the ledger does not count a hold")
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.unaccounted), 0)
}

func TestGateLongPromptFitsWhileAnotherPrefills(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	m := testMetrics(0, 0, 0, testStart)
	m.CacheNumBlocks = 1001
	ep := fwkdl.NewEndpoint(newTestEndpoint("a", 0, testStart).GetMetadata(), m)
	e := addEndpoint(t, l, ep)
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(1500, true, nil), resultFor(l, ep, 0, 0)))
	d := l.Gate(context.Background(), fcRequest(newTestRequest(2000, true, nil)), []fwkdl.Endpoint{ep})
	require.NotNil(t, d.Reservation, "vLLM prefills the prompt in the budget the prefilling request leaves")
	require.Equal(t, int64(testStepBudget-1500), reserved(e)[axisStep])
}

func TestGateReservationsDoNotOversubscribe(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	// 80 of 100 blocks free: room for one 60-block request, not two.
	e := addEndpoint(t, l, newTestEndpoint("a", 0.2, testStart))
	first := l.Gate(context.Background(), fcRequest(newTestRequest(900, true, nil)), []fwkdl.Endpoint{e.datalayer()})
	require.NotNil(t, first.Reservation)
	second := l.Gate(context.Background(), fcRequest(newTestRequest(900, true, nil)), []fwkdl.Endpoint{e.datalayer()})
	require.Nil(t, second.Reservation)
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.gateCheck("reserved-memory")), 0)
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.wouldHold[holdReserved]), 0)
}

func TestGateTwiceRefundsTheEarlierReservation(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	e := addEndpoint(t, l, newTestEndpoint("a", 0.2, testStart))
	ir := newTestRequest(900, true, nil)
	require.NotNil(t, l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{e.datalayer()}).Reservation)
	require.NotNil(t, l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{e.datalayer()}).Reservation,
		"a request is not checked against its own earlier reservation")
	require.Equal(t, int64(1), reserved(e)[axisSlots])
}

func TestReservationReleasedWhenRequestContextEnds(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	e := addEndpoint(t, l, newTestEndpoint("a", 0.1, testStart))
	d := l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{e.datalayer()})
	ctx, cancel := context.WithCancel(context.Background())
	d.Reservation.Dispatched(ctx)
	cancel()
	require.Eventually(t, func() bool { return reserved(e) == vec{} }, time.Second, time.Millisecond)
}

func TestReapOnlyUndispatchedReservations(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	e := addEndpoint(t, l, newTestEndpoint("a", 0.1, testStart))
	undispatched := l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{e.datalayer()})
	dispatched := l.Gate(context.Background(), fcRequest(newTestRequest(100, true, nil)), []fwkdl.Endpoint{e.datalayer()})
	dispatched.Reservation.Dispatched(context.Background())
	require.NotNil(t, undispatched.Reservation)

	clk.Step(2 * time.Second)
	l.sample()
	require.Equal(t, int64(1), reserved(e)[axisSlots], "a dispatched reservation waits for its request")
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.reservationsReaped), 0)
}

func TestPlacementConvertsReservation(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.1, testStart)
	e := addEndpoint(t, l, ep)
	ir := newTestRequest(100, true, nil)
	d := l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{ep})
	d.Reservation.Dispatched(context.Background())

	require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, ep, 0, 0)))
	require.Equal(t, vec{}, reserved(e))
	require.Equal(t, vec{7, 100, 1}, booked(e))
	d.Reservation.Refund()
	require.Equal(t, vec{7, 100, 1}, booked(e), "a refund after conversion is a no-op")
	require.Equal(t, vec{}, reserved(e))
	require.Equal(t, uint64(1), histogramSamples(t, l.metrics.reservationToLease[axisMemory]))
	require.Equal(t, uint64(1), histogramSamples(t, l.metrics.reservationToLease[axisStep]))
}

func raceCount(l *Ledger, cause raceCause) float64 {
	return promtestutil.ToFloat64(l.metrics.placementRaces.WithLabelValues(string(cause)))
}

func TestPlacementRaces(t *testing.T) {
	setup := func(t *testing.T) (*Ledger, *fwkdl.ModelServer, *fwkdl.ModelServer, *fwksched.InferenceRequest) {
		l, _ := newTestLedger(t, testAPIConfig())
		roomy := newTestEndpoint("roomy", 0.1, testStart)
		full := newTestEndpoint("full", 0.1, testStart)
		addEndpoint(t, l, roomy)
		addEndpoint(t, l, full)
		ir := newTestRequest(100, true, nil)
		require.NotNil(t, l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{roomy, full}).Reservation)
		// full fills up between the gate and placement.
		full.UpdateMetrics(testMetrics(0.99, 0, 0, testStart))
		return l, roomy, full, ir
	}

	t.Run("placed on another endpoint that fits", func(t *testing.T) {
		l, roomy, full, ir := setup(t)
		candidates := []fwksched.Endpoint{fwksched.NewEndpoint(roomy.GetMetadata(), roomy.GetMetrics(), nil), fwksched.NewEndpoint(full.GetMetadata(), full.GetMetrics(), nil)}
		out := l.Filter(context.Background(), ir, candidates)
		require.Equal(t, candidates, out, "the filter never removes an endpoint")
		require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, roomy, 0, 0)))
		for _, cause := range []raceCause{raceRecheck, raceUnfitPlacement, raceUnfiltered} {
			require.Zero(t, raceCount(l, cause), cause)
		}
	})
	t.Run("unfit placement", func(t *testing.T) {
		l, roomy, full, ir := setup(t)
		l.Filter(context.Background(), ir, []fwksched.Endpoint{fwksched.NewEndpoint(roomy.GetMetadata(), roomy.GetMetrics(), nil), fwksched.NewEndpoint(full.GetMetadata(), full.GetMetrics(), nil)})
		l.Filter(context.Background(), ir, []fwksched.Endpoint{fwksched.NewEndpoint(roomy.GetMetadata(), roomy.GetMetrics(), nil)}) // a second profile
		require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, full, 0, 0)))
		require.InDelta(t, 1, raceCount(l, raceUnfitPlacement), 0)
	})
	t.Run("recheck", func(t *testing.T) {
		l, _, full, ir := setup(t)
		l.Filter(context.Background(), ir, []fwksched.Endpoint{fwksched.NewEndpoint(full.GetMetadata(), full.GetMetrics(), nil)})
		require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, full, 0, 0)))
		require.InDelta(t, 1, raceCount(l, raceRecheck), 0)
		require.Zero(t, raceCount(l, raceUnfitPlacement), "a request counts under one cause")
	})
	t.Run("unfiltered", func(t *testing.T) {
		l, roomy, _, ir := setup(t)
		require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, roomy, 0, 0)))
		require.InDelta(t, 1, raceCount(l, raceUnfiltered), 0)
	})
}

func TestFilterMatchesReservationByEndpointID(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.1, testStart)
	addEndpoint(t, l, ep)
	ir := newTestRequest(100, true, nil)
	require.NotNil(t, l.Gate(context.Background(), fcRequest(ir), []fwkdl.Endpoint{ep}).Reservation)

	// The endpoint is recreated under the same ID between the gate and placement.
	replacement := newTestEndpoint("a", 0.1, testStart)
	addEndpoint(t, l, replacement)
	l.Filter(context.Background(), ir,
		[]fwksched.Endpoint{fwksched.NewEndpoint(replacement.GetMetadata(), replacement.GetMetrics(), nil)})
	require.NoError(t, l.PreRequest(context.Background(), ir, resultFor(l, replacement, 0, 0)))
	for _, cause := range []raceCause{raceRecheck, raceUnfitPlacement, raceUnfiltered} {
		require.Zero(t, raceCount(l, cause), cause)
	}
}

func TestUnreservedPlacementsCountedOnlyWhenGated(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.1, testStart)
	addEndpoint(t, l, ep)
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(10, true, nil), resultFor(l, ep, 0, 0)))
	require.Zero(t, promtestutil.ToFloat64(l.metrics.unreservedPlacements))

	l.Gate(context.Background(), fcRequest(newTestRequest(10, true, nil)), nil)
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(10, true, nil), resultFor(l, ep, 0, 0)))
	require.InDelta(t, 1, promtestutil.ToFloat64(l.metrics.unreservedPlacements), 0)
}

// TestRandomGatedLifecycleDrainsToZero gates, cancels, places and completes requests concurrently;
// once every request ends, no account books or reserves anything.
func TestRandomGatedLifecycleDrainsToZero(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	eps := []*fwkdl.ModelServer{newTestEndpoint("a", 0, testStart), newTestEndpoint("b", 0, testStart)}
	endpoints := make([]*endpoint, 0, len(eps))
	for _, ep := range eps {
		endpoints = append(endpoints, addEndpoint(t, l, ep))
	}
	candidates := []fwkdl.Endpoint{eps[0], eps[1]}

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(w)))
			for range 200 {
				ctx, cancel := context.WithCancel(context.Background())
				ir := newTestRequest(1+rng.Intn(400), true, nil)
				d := l.Gate(ctx, fcRequest(ir), candidates)
				if d.Reservation != nil {
					if rng.Intn(5) == 0 {
						d.Reservation.Refund() // lost the dispatch race
						cancel()
						continue
					}
					d.Reservation.Dispatched(ctx)
				}
				if rng.Intn(4) == 0 {
					cancel() // abandoned before placement
					continue
				}
				ep := eps[rng.Intn(len(eps))]
				l.Filter(ctx, ir, []fwksched.Endpoint{fwksched.NewEndpoint(ep.GetMetadata(), ep.GetMetrics(), nil)})
				if err := l.PreRequest(ctx, ir, resultFor(l, ep, 0, 0)); err != nil {
					t.Error(err)
				}
				l.ResponseBody(ctx, ir, &fwkrc.Response{StartOfStream: true, StreamedEvents: 1}, nil)
				l.ResponseBody(ctx, ir, &fwkrc.Response{EndOfStream: true}, nil)
				cancel()
			}
		})
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		for _, e := range endpoints {
			if booked(e) != (vec{}) || reserved(e) != (vec{}) {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)
}
