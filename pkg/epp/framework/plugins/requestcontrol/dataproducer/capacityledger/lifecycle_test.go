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

	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
)

func TestPreRequestBooksLease(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)

	// 3 of the 6 indexed prompt blocks are cached on the endpoint; 4 tokens lie beyond the index.
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, true, nil), resultFor(l, ep, 3, 6)))
	require.Equal(t, vec{
		axisMemory: 7,                     // the full prompt plus one output token: ceil(101/16)
		axisStep:   100 - 3*testBlockSize, // prefill is reduced by the prefix match; memory is not
		axisSlots:  1,
	}, booked(e))
}

func TestPreRequestUnknownEndpoint(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	req := newTestRequest(10, true, nil)
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, newTestEndpoint("a", 0, testStart), 0, 0)))
	_, ok := l.leases.Load(req)
	require.False(t, ok)
}

func TestPreRequestTwiceReplacesLease(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(100, true, nil)
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))
	require.Equal(t, vec{7, 100, 1}, booked(e))
}

func TestStreamingLifecycle(t *testing.T) {
	api := testAPIConfig()
	api.SpeculativeTokens = ptr(int64(2))
	l, clk := newTestLedger(t, api)
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(192, true, nil)
	ctx := context.Background()
	require.NoError(t, l.PreRequest(ctx, req, resultFor(l, ep, 0, 0)))
	// 192 + 1 + 2 speculative slots = 195 tokens is 13 blocks.
	require.Equal(t, vec{13, 192, 1}, booked(e))

	// The first chunk ends prefill without StartOfStream. One event is one decode step of 1+2
	// tokens: 192 + 3 + 3 = 198 tokens is 13 blocks.
	clk.Step(500 * time.Millisecond)
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 1}, nil)
	require.Equal(t, vec{13, 3, 1}, booked(e), "after the first chunk the request schedules 1+2 decode tokens per step")
	prefillRate := e.prefillRate.estimate(clk.Now())
	require.True(t, prefillRate.ok)
	require.InDelta(t, 384.0, prefillRate.value, 1e-9, "192 uncached tokens in half a second")

	// Cumulative 20 events are 60 tokens: 192 + 60 + 3 = 255 tokens is 16 blocks.
	clk.Step(time.Second)
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 20}, nil)
	require.Equal(t, int64(16), booked(e)[axisMemory])
	decodeRate := e.decodeRate.estimate(clk.Now())
	require.True(t, decodeRate.ok)
	require.InDelta(t, 57.0, decodeRate.value, 1e-9)

	// Cumulative usage replaces the event count: 192 + 100 + 3 = 295 tokens is 19 blocks.
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 21, Usage: fwkrh.Usage{CompletionTokens: 100}}, nil)
	require.Equal(t, int64(19), booked(e)[axisMemory])

	l.ResponseBody(ctx, req, &fwkrc.Response{EndOfStream: true}, nil)
	require.Equal(t, vec{}, booked(e))
	l.ResponseBody(ctx, req, &fwkrc.Response{EndOfStream: true}, nil)
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 50}, nil)
	require.Equal(t, vec{}, booked(e), "a repeated end of stream and a late chunk are no-ops")
}

func TestLeaseReleasedWhenRequestContextEnds(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	ctx, cancel := context.WithCancel(context.Background())
	req := newTestRequest(100, true, nil)
	require.NoError(t, l.PreRequest(ctx, req, resultFor(l, ep, 0, 0)))
	require.Equal(t, int64(1), booked(e)[axisSlots])

	cancel()
	require.Eventually(t, func() bool { return booked(e) == vec{} }, time.Second, time.Millisecond,
		"a request whose end of stream never arrives is released when its context ends")
	_, ok := l.leases.Load(req)
	require.False(t, ok)
}

func TestImputedProgress(t *testing.T) {
	tests := []struct {
		name         string
		elapsed      time.Duration
		prefillRate  float64
		decodeRate   float64
		haveRates    bool
		wantDecoding bool
		wantAge      int64
	}{
		{name: "no rates: prefill done, no output", elapsed: time.Second, wantDecoding: true},
		{name: "still prefilling", elapsed: 500 * time.Millisecond, prefillRate: 100, decodeRate: 10, haveRates: true},
		{name: "decoding", elapsed: 3 * time.Second, prefillRate: 100, decodeRate: 10, haveRates: true,
			wantDecoding: true, wantAge: 20},
		{name: "capped at the maximum", elapsed: time.Hour, prefillRate: 100, decodeRate: 10, haveRates: true,
			wantDecoding: true, wantAge: 300},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoding, age := imputedProgress(tc.elapsed, 100, 300,
				rateEstimate{tc.prefillRate, tc.haveRates}, rateEstimate{tc.decodeRate, tc.haveRates})
			require.Equal(t, tc.wantDecoding, decoding)
			require.Equal(t, tc.wantAge, age)
		})
	}
}

func TestNonStreamingLeaseAdvancesOnSample(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	e.prefillRate.observe(100, time.Second, clk.Now())
	e.decodeRate.observe(10, time.Second, clk.Now())
	req := newTestRequest(100, false, ptr(int64(300)))
	require.NoError(t, l.PreRequest(context.Background(), req, resultFor(l, ep, 0, 0)))

	l.sample()
	require.Equal(t, int64(100), booked(e)[axisStep], "prefilling at dispatch")
	clk.Step(3 * time.Second)
	l.sample()
	// Prefill took 1s; 2s of decode at 10 tokens/s is 20 tokens: 100 + 20 + 1 = 121 tokens is 8 blocks.
	require.Equal(t, vec{8, 1, 1}, booked(e))
}

func TestExtractLifecycle(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ctx := context.Background()
	ep := newTestEndpoint("a", 0, testStart)
	old := addEndpoint(t, l, ep)
	req := newTestRequest(100, true, nil)
	require.NoError(t, l.PreRequest(ctx, req, resultFor(l, ep, 0, 0)))

	// The endpoint is recreated under the same ID: the state, with its lease, carries over to the
	// new endpoint, and the delete for the old pointer is stale.
	replacement := newTestEndpoint("a", 0, testStart)
	kept := addEndpoint(t, l, replacement)
	require.Same(t, old, kept)
	require.Same(t, fwkdl.Endpoint(replacement), kept.datalayer())
	require.NoError(t, l.Extract(ctx, fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: ep}))
	require.Equal(t, vec{7, 100, 1}, booked(kept), "the running request stays booked")
	_, ok := replacement.GetAttributes().Get(l.dk)
	require.True(t, ok, "the view is installed on the new endpoint")

	l.ResponseBody(ctx, req, &fwkrc.Response{EndOfStream: true}, nil)
	require.Equal(t, vec{}, booked(kept))

	require.NoError(t, l.Extract(ctx, fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: replacement}))
	l.mu.Lock()
	require.Empty(t, l.endpoints)
	l.mu.Unlock()
}

// TestRandomLifecycleDrainsToZero interleaves placements, chunks, sampling, cancellations, endpoint
// churn and completions concurrently; once every request ends, no account holds anything.
func TestRandomLifecycleDrainsToZero(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	eps := []*fwkdl.ModelServer{newTestEndpoint("a", 0, testStart), newTestEndpoint("b", 0, testStart)}
	var mu sync.Mutex
	var endpoints []*endpoint
	for _, ep := range eps {
		endpoints = append(endpoints, addEndpoint(t, l, ep))
	}

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(w)))
			for range 200 {
				ep := eps[rng.Intn(len(eps))]
				ctx, cancel := context.WithCancel(context.Background())
				req := newTestRequest(1+rng.Intn(200), rng.Intn(4) != 0, ptr(int64(1+rng.Intn(100))))
				if err := l.PreRequest(ctx, req, resultFor(l, ep, rng.Intn(3), 3)); err != nil {
					t.Error(err)
					cancel()
					return
				}
				events := 0
				for i := range rng.Intn(5) {
					events += rng.Intn(20)
					l.ResponseBody(ctx, req, &fwkrc.Response{StartOfStream: i == 0, StreamedEvents: events}, nil)
				}
				if rng.Intn(10) == 0 {
					l.sample()
				}
				if rng.Intn(3) == 0 {
					cancel() // the end of stream never arrives
				} else {
					l.ResponseBody(ctx, req, &fwkrc.Response{EndOfStream: true}, nil)
					cancel()
				}
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			e := addEndpoint(t, l, newTestEndpoint("a", 0, testStart))
			mu.Lock()
			endpoints = append(endpoints, e)
			mu.Unlock()
		}
	})
	wg.Wait()

	require.Eventually(t, func() bool {
		count := 0
		l.leases.Range(func(_, _ any) bool { count++; return true })
		return count == 0
	}, time.Second, time.Millisecond)
	for _, e := range endpoints {
		require.Equal(t, vec{}, booked(e), e.id.String())
	}
}
