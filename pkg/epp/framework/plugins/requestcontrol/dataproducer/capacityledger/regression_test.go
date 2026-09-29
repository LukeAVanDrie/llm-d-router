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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

func TestDeleteKeepsOtherInstancesSeries(t *testing.T) {
	a, _ := newTestLedger(t, testAPIConfig())
	b, _ := newTestLedger(t, testAPIConfig())
	epA := newTestEndpoint("shared", 0.1, testStart)
	epB := newTestEndpoint("shared", 0.1, testStart)
	addEndpoint(t, a, epA)
	addEndpoint(t, b, epB)
	a.sample()
	b.sample()

	require.NoError(t, b.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: epB}))
	require.Zero(t, countSeries(t, bookedVec, b.typedName.Name))
	require.Equal(t, int(numAxes), countSeries(t, bookedVec, a.typedName.Name),
		"deleting an endpoint in one ledger leaves another ledger's series")
	require.Equal(t, 1, countSeries(t, eligibilityVec, a.typedName.Name))
}

func TestReplaceWhileSamplingKeepsOneEligibilitySeries(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	addEndpoint(t, l, newTestEndpoint("a", 0.1, testStart))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 200 {
			l.sample()
		}
	}()
	for range 200 {
		addEndpoint(t, l, newTestEndpoint("a", 0.1, testStart))
	}
	<-done
	l.sample()
	require.Equal(t, 1, countSeries(t, eligibilityVec, l.typedName.Name))
}

func TestNonStreamingBookingNeverShrinks(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	e.decodeRate.observe(100, time.Second, clk.Now())
	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, false, ptr(int64(300))), resultFor(l, ep, 0, 0)))
	clk.Step(2 * time.Second)
	l.sample()
	grown := booked(e)
	require.Equal(t, blocksFor(100+200+1, testBlockSize), grown[axisMemory], "2s at 100 tokens/s")

	// A new rate implies far less progress.
	for range 50 {
		e.decodeRate.observe(1, time.Second, clk.Now())
	}
	clk.Step(time.Second)
	l.sample()
	require.Equal(t, grown, booked(e), "a running request's booking does not shrink")
}

func TestUncachedPromptCountsCachedBlocksUnweighted(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	// The precise prefix cache weights lower-tier blocks below 1 in MatchBlocks; CachedBlockCount is
	// the literal count.
	attrs := fwkdl.NewAttributes()
	attrs.Put(l.prefixDK, attrprefix.NewPrefixCacheMatchInfo(1, 6, testBlockSize).WithCachedBlockCount(3))
	target := fwksched.NewEndpoint(ep.GetMetadata(), ep.GetMetrics(), attrs)
	result := &fwksched.SchedulingResult{PrimaryProfileName: "p",
		ProfileResults: map[string]*fwksched.ProfileRunResult{"p": {TargetEndpoints: []fwksched.Endpoint{target}}}}

	require.NoError(t, l.PreRequest(context.Background(), newTestRequest(100, true, nil), result))
	require.Equal(t, int64(100-3*testBlockSize), booked(e)[axisStep])
}

func TestDecodeRateUsesUsageDelta(t *testing.T) {
	l, clk := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	e := addEndpoint(t, l, ep)
	req := newTestRequest(16, true, nil)
	ctx := context.Background()
	require.NoError(t, l.PreRequest(ctx, req, resultFor(l, ep, 0, 0)))
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 1}, nil)
	clk.Step(time.Second)
	// 4 more events carry 19 more tokens by cumulative usage.
	l.ResponseBody(ctx, req, &fwkrc.Response{StreamedEvents: 5, Usage: fwkrh.Usage{CompletionTokens: 20}}, nil)
	rate := e.decodeRate.estimate(clk.Now())
	require.True(t, rate.ok)
	require.InDelta(t, 19.0, rate.value, 1e-9)
}

func TestDeleteForgetsEngineObservation(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0, testStart)
	addEndpoint(t, l, ep)
	l.engines.mu.Lock()
	l.engines.obs["ns/a"] = engineObservation{engines: 2}
	l.engines.mu.Unlock()
	require.NoError(t, l.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventDelete, Endpoint: ep}))
	require.Equal(t, engineObservation{}, l.engines.get("ns/a"))
}
