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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	testclock "k8s.io/utils/clock/testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

const (
	testBlockSize = 16
	// testNumBlocks is the reported block count; one is vLLM's null block.
	testNumBlocks   = 101
	testSlots       = 8
	testStepBudget  = 2048
	testMaxModelLen = 4096
)

var testStart = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func testAPIConfig() apiConfig {
	return apiConfig{SlotsPerEndpoint: ptr(int64(testSlots)), StepTokenBudget: ptr(int64(testStepBudget)),
		MaxModelLen: ptr(int64(testMaxModelLen))}
}

var ledgerSeq atomic.Int64

// newTestLedger returns a ledger on a fake clock with a producer name unique to this run, so tests
// and repeated runs do not share metric series.
func newTestLedger(t *testing.T, api apiConfig) (*Ledger, *testclock.FakeClock) {
	t.Helper()
	cfg, err := newConfig(api, 0)
	require.NoError(t, err)
	name := fmt.Sprintf("%s-%d", t.Name(), ledgerSeq.Add(1))
	m, err := newLedgerMetrics(prometheus.NewRegistry(), name)
	require.NoError(t, err)
	clk := testclock.NewFakeClock(testStart)
	return newLedger(name, cfg, clk, 50*time.Millisecond, m), clk
}

func testMetrics(usage float64, running, waiting int, scrapedAt time.Time) *fwkdl.Metrics {
	return &fwkdl.Metrics{
		CacheNumBlocks:      testNumBlocks,
		CacheBlockSize:      testBlockSize,
		KVCacheUsagePercent: usage,
		RunningRequestsSize: running,
		WaitingQueueSize:    waiting,
		UpdateTime:          scrapedAt,
	}
}

// newTestEndpoint returns a datalayer endpoint with fresh vLLM-shaped metrics.
func newTestEndpoint(name string, usage float64, scrapedAt time.Time) *fwkdl.ModelServer {
	return fwkdl.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "ns", Name: name}, Labels: map[string]string{}},
		testMetrics(usage, 0, 0, scrapedAt),
	)
}

func addEndpoint(t *testing.T, l *Ledger, ep fwkdl.Endpoint) *endpoint {
	t.Helper()
	require.NoError(t, l.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: ep}))
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.endpoints[ep.GetMetadata().ID.String()]
}

// newTestRequest returns a request with a tokenized prompt of the given length.
func newTestRequest(prompt int, stream bool, maxOutput *int64) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{
		RequestID: "req",
		Body: &fwkrh.InferenceRequestBody{
			TokenizedRequest: &fwkrh.TokenizedRequest{Prompts: []fwkrh.PromptTokens{{TokenIDs: make([]uint32, prompt)}}},
			Stream:           stream,
			MaxOutputTokens:  maxOutput,
		},
	}
}

// resultFor places a request on ep, with a prefix match of matchBlocks out of totalBlocks when
// totalBlocks is positive.
func resultFor(l *Ledger, ep fwkdl.Endpoint, matchBlocks, totalBlocks int) *fwksched.SchedulingResult {
	attrs := fwkdl.NewAttributes()
	if totalBlocks > 0 {
		attrs.Put(l.prefixDK, attrprefix.NewPrefixCacheMatchInfo(matchBlocks, totalBlocks, testBlockSize))
	}
	target := fwksched.NewEndpoint(ep.GetMetadata(), ep.GetMetrics(), attrs)
	return &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"default": {TargetEndpoints: []fwksched.Endpoint{target}},
		},
	}
}

func booked(e *endpoint) vec {
	return e.acct.snapshot().booked
}
