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

package tokendelay

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
)

func makeEndpoint(name string, qTokens, nRequests, uTokens int64, kvUsage float64, maxCap int, updated bool) fwksched.Endpoint {
	m := &fwkdl.Metrics{
		KVCacheUsagePercent:     kvUsage,
		KvCacheMaxTokenCapacity: maxCap,
	}
	if updated {
		m.UpdateTime = time.Now()
	}
	ep := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: name}},
		m,
		nil,
	)
	ep.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{
		Tokens:   qTokens,
		Requests: nRequests,
	})
	ep.Put(attrconcurrency.UncachedRequestTokensDataKey, &attrconcurrency.UncachedRequestTokens{
		Tokens: uTokens,
	})
	return ep
}

func TestTokenDelayScorer_FactoryAndConfig(t *testing.T) {
	tests := []struct {
		name      string
		rawConfig string
		wantErr   bool
	}{
		{
			name:      "default config succeeds",
			rawConfig: `{}`,
			wantErr:   false,
		},
		{
			name:      "custom producer config succeeds",
			rawConfig: `{"inFlightLoadProducerName": "custom"}`,
			wantErr:   false,
		},
		{
			name:      "unknown field rejected",
			rawConfig: `{"unknownField": true}`,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := TokenDelayScorerFactory("td-scorer", fwkplugin.StrictDecoder(json.RawMessage(tt.rawConfig)), nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, p)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, p)
			assert.Equal(t, fwkplugin.TypedName{Type: TokenDelayScorerType, Name: "td-scorer"}, p.TypedName())
		})
	}

	t.Run("nil decoder uses defaults", func(t *testing.T) {
		p, err := TokenDelayScorerFactory("td-nil", nil, nil)
		require.NoError(t, err)
		s := p.(*TokenDelayScorer)
		assert.Equal(t, fwksched.Distribution, s.Category())
	})
}

func TestTokenDelayScorer_Consumes(t *testing.T) {
	scorer := NewTokenDelayScorer(Config{InFlightLoadProducerName: "custom-ifl"})

	deps := scorer.Consumes()
	expectedIFL := attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName("custom-ifl")
	expectedUncached := attrconcurrency.UncachedRequestTokensDataKey.WithNonEmptyProducerName("custom-ifl")
	expectedKV := fwkplugin.NewDataKey(metrics.KVCacheUsagePercentKey, metrics.MetricsExtractorType)

	assert.Contains(t, deps.Required, expectedIFL)
	assert.Contains(t, deps.Required, expectedUncached)
	assert.Contains(t, deps.Optional, expectedKV)
}

func TestTokenDelayScorer_Score(t *testing.T) {
	ctx := context.Background()

	t.Run("empty endpoints returns empty map", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})
		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, nil)
		assert.Empty(t, scores)
	})

	t.Run("zero KV occupancy and zero concurrency (kappa = 1)", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// No updated metrics -> pi = 0, kappa = 1.
		// L_e = Q_e + u_e, maxLoad = 1000.
		ep1 := makeEndpoint("pod1", 0, 0, 250, 0, 0, false)   // L = 250 -> score = 0.75
		ep2 := makeEndpoint("pod2", 250, 0, 250, 0, 0, false) // L = 500 -> score = 0.50
		ep3 := makeEndpoint("pod3", 750, 0, 250, 0, 0, false) // L = 1000 -> score = 0.00

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2, ep3})
		assert.InDelta(t, 0.75, scores[ep1], 1e-6)
		assert.InDelta(t, 0.50, scores[ep2], 1e-6)
		assert.InDelta(t, 0.00, scores[ep3], 1e-6)
	})

	t.Run("Naor congestion multiplier amplifies cache-miss penalty (unweighted mean pi)", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// ep1 has KVCacheUsagePercent = 0.4, ep2 has 0.6 -> mean pi = 0.5.
		// kappa = 1 + 0.5 / (0.5)^2 = 3.0.
		// u_min = min(200, 1000) = 200.
		// ep1 (warm cache, busy queue): Q = 1000, u = 200 -> L_1 = 1000 + 200 + 2*(0) = 1200.
		// ep2 (cold cache, empty queue): Q = 0, u = 1000 -> L_2 = 0 + 1000 + 2*(800) = 2600.
		// maxLoad = 2600.
		ep1 := makeEndpoint("pod1", 1000, 0, 200, 0.4, 0, true)
		ep2 := makeEndpoint("pod2", 0, 0, 1000, 0.6, 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		assert.InDelta(t, 1.0-1200.0/2600.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
		assert.Greater(t, scores[ep1], scores[ep2], "warm pod with smaller effective delay should score higher")
	})

	t.Run("capacity-weighted pooled KV occupancy when KvCacheMaxTokenCapacity > 0", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// ep1: usage = 0.2, cap = 1000
		// ep2: usage = 0.8, cap = 3000
		// Capacity-weighted pi = (0.2*1000 + 0.8*3000) / 4000 = 2600 / 4000 = 0.65.
		// kappa = 1 + 0.65 / (0.35)^2 = 1 + 0.65 / 0.1225 = 6.3061224489795915.
		// u_min = 100.
		// ep1: Q = 0, u = 100 -> L_1 = 100
		// ep2: Q = 0, u = 500 -> L_2 = 500 + (kappa - 1) * 400 = 100 + kappa * 400 = 2622.4489795918366
		ep1 := makeEndpoint("pod1", 0, 0, 100, 0.2, 1000, true)
		ep2 := makeEndpoint("pod2", 0, 0, 500, 0.8, 3000, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		kappa := 1.0 + 0.65/(0.35*0.35)
		wantLoad2 := 100.0 + kappa*400.0
		assert.InDelta(t, 1.0-100.0/wantLoad2, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
	})

	t.Run("mixed KvCacheMaxTokenCapacity falls back to arithmetic mean across updated endpoints", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// ep1: updated, usage = 0.2, cap = 1000
		// ep2: updated, usage = 0.8, cap = 0 (missing capacity metric)
		// ep3: not updated, usage = 0.99, cap = 5000 (ignored in pi)
		// Falls back to arithmetic mean of updated endpoints (ep1, ep2): pi = (0.2 + 0.8) / 2 = 0.5 -> kappa = 3.0.
		ep1 := makeEndpoint("pod1", 0, 0, 100, 0.2, 1000, true)
		ep2 := makeEndpoint("pod2", 0, 0, 500, 0.8, 0, true)
		ep3 := makeEndpoint("pod3", 0, 0, 300, 0.99, 5000, false)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2, ep3})
		// u_min = 100, kappa = 3.0 -> L_1 = 100, L_2 = 1300, L_3 = 700, maxLoad = 1300.
		assert.InDelta(t, 1.0-100.0/1300.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
		assert.InDelta(t, 1.0-700.0/1300.0, scores[ep3], 1e-6)
	})

	t.Run("NaN and Inf KVCacheUsagePercent on updated endpoints are ignored without poisoning scores", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// ep1 has valid usage 0.5 (kappa = 3.0); ep2 has NaN; ep3 has +Inf.
		ep1 := makeEndpoint("pod1", 0, 0, 100, 0.5, 0, true)
		ep2 := makeEndpoint("pod2", 0, 0, 500, math.NaN(), 0, true)
		ep3 := makeEndpoint("pod3", 0, 0, 300, math.Inf(1), 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2, ep3})
		assert.False(t, math.IsNaN(scores[ep1]))
		assert.False(t, math.IsNaN(scores[ep2]))
		assert.False(t, math.IsNaN(scores[ep3]))
		assert.InDelta(t, 1.0-100.0/1300.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
		assert.InDelta(t, 1.0-700.0/1300.0, scores[ep3], 1e-6)
	})

	t.Run("high KV occupancy clamps pi to 0.999 and preserves strict ordering", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// All pods report KVCacheUsagePercent = 1.0 -> clamped to pi = 0.999, kappa = 999001.0.
		// u_min = 100.
		// ep1: u = 100 -> L_1 = 100
		// ep2: u = 200 -> L_2 = 100 + 999001*100 = 99900200
		// ep3: u = 300 -> L_3 = 100 + 999001*200 = 199800300 (> L_2)
		// maxLoad = L_3, so scores are strictly ordered: score(ep1) > score(ep2) > score(ep3) == 0.
		ep1 := makeEndpoint("pod1", 0, 0, 100, 1.0, 0, true)
		ep2 := makeEndpoint("pod2", 0, 0, 200, 1.0, 0, true)
		ep3 := makeEndpoint("pod3", 0, 0, 300, 1.0, 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2, ep3})
		assert.Greater(t, scores[ep1], scores[ep2])
		assert.Greater(t, scores[ep2], scores[ep3])
		assert.InDelta(t, 0.0, scores[ep3], 1e-9)
	})

	t.Run("concurrent decode-batch stall externality included by default", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// pi = 0.5 -> kappa = 3.0, kappa - 1 = 2.0.
		// u_min = 200.
		// ep1: Q = 100, n = 4, u = 200 -> L_1 = 100 + (1+4)*200 + 2*(0) = 1100.
		// ep2: Q = 100, n = 0, u = 400 -> L_2 = 100 + (1+0)*400 + 2*(200) = 900.
		// maxLoad = 1100.
		ep1 := makeEndpoint("pod1", 100, 4, 200, 0.5, 0, true)
		ep2 := makeEndpoint("pod2", 100, 0, 400, 0.5, 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		assert.InDelta(t, 0.0, scores[ep1], 1e-6)
		assert.InDelta(t, 1.0-900.0/1100.0, scores[ep2], 1e-6)
		assert.Greater(t, scores[ep2], scores[ep1])
	})

	t.Run("decode concurrency breaks ties when u_e == 0 with zero tunable weights", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// Remote prefill or full-prompt cache hit where u_e == 0 on all candidates:
		// ep0: Q = 0, n = 0, u = 0 -> stepTokens = 0, L_0 = 0 -> score = 1.0
		// ep1: Q = 0, n = 2, u = 0 -> stepTokens = 1, L_1 = 3 -> score = 1 - 3/7
		// ep2: Q = 0, n = 6, u = 0 -> stepTokens = 1, L_2 = 7 -> score = 0.0
		ep0 := makeEndpoint("pod0", 0, 0, 0, 0, 0, false)
		ep1 := makeEndpoint("pod1", 0, 2, 0, 0, 0, false)
		ep2 := makeEndpoint("pod2", 0, 6, 0, 0, 0, false)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep0, ep1, ep2})
		assert.InDelta(t, 1.0, scores[ep0], 1e-6)
		assert.InDelta(t, 1.0-3.0/7.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
	})

	t.Run("missing and typed-nil attributes degrade gracefully to score 1.0", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		epMissing := fwksched.NewEndpoint(
			&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "missing"}},
			&fwkdl.Metrics{},
			nil,
		)
		epTypedNil := fwksched.NewEndpoint(
			&fwkdl.EndpointMetadata{ID: types.NamespacedName{Namespace: "default", Name: "typed-nil"}},
			nil,
			nil,
		)
		var nilLoad *attrconcurrency.InFlightLoad
		var nilUncached *attrconcurrency.UncachedRequestTokens
		epTypedNil.Put(scorer.inFlightLoadDataKey, nilLoad)
		epTypedNil.Put(scorer.uncachedRequestTokensDataKey, nilUncached)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{epMissing, epTypedNil})
		assert.Equal(t, 1.0, scores[epMissing])
		assert.Equal(t, 1.0, scores[epTypedNil])
	})

	t.Run("ScopedEndpoint confinement allows all declared attribute reads", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		datalayer.RegisterScopeSpecs([]fwkplugin.Plugin{scorer})
		// ep1: Q=1000, n=1, u=200, pi=0.5 -> L_1 = 1000 + (1+1)*200 + 2*0 = 1400.
		// ep2: Q=0, n=0, u=1000, pi=0.5 -> L_2 = 0 + 1*1000 + 2*800 = 2600.
		ep1 := makeEndpoint("pod1", 1000, 1, 200, 0.5, 1000, true)
		ep2 := makeEndpoint("pod2", 0, 0, 1000, 0.5, 1000, true)

		scoped, violations := datalayer.Scope(log.FromContext(ctx), "Score", scorer, []fwksched.Endpoint{ep1, ep2})
		scores := datalayer.UnscopeScores(scorer.Score(ctx, &fwksched.InferenceRequest{}, scoped))
		require.NoError(t, violations.Write())
		assert.InDelta(t, 1.0-1400.0/2600.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
	})

	t.Run("nil endpoints and negative attribute values handled safely", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		epNeg := makeEndpoint("pod-neg", -100, -5, -200, -0.5, 0, true)
		epPos := makeEndpoint("pod-pos", 100, 1, 200, 0.5, 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{nil, epNeg, epPos})
		assert.Len(t, scores, 2)
		assert.Equal(t, 1.0, scores[epNeg])
		assert.InDelta(t, 0.0, scores[epPos], 1e-6)

		onlyNil := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{nil})
		assert.Empty(t, onlyNil)
	})

	t.Run("identical positive load across all endpoints yields tied 0.0 scores", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		ep1 := makeEndpoint("pod1", 500, 1, 200, 0.5, 0, true)
		ep2 := makeEndpoint("pod2", 500, 1, 200, 0.5, 0, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		assert.Equal(t, 0.0, scores[ep1])
		assert.Equal(t, 0.0, scores[ep2])
	})

	t.Run("per-pod KV occupancy pi_e breaks ties when Delta u_e == 0 across candidates", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// Cold arrival (u_1 = u_2 = 500 -> Delta u_e = 0) with tied compute delay (Q=0, n=0),
		// but ep1 has 20% KV occupancy while ep2 has 80% KV occupancy.
		ep1 := makeEndpoint("pod1", 0, 0, 500, 0.2, 4000, true)
		ep2 := makeEndpoint("pod2", 0, 0, 500, 0.8, 4000, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		assert.InDelta(t, 1.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)

		// When a third pod has higher compute delay (n=2), ep1 and ep2 still beat ep3
		// and ep1 strictly beats ep2 via the pi_e tie-breaker.
		ep3 := makeEndpoint("pod3", 0, 2, 500, 0.1, 4000, true)
		scores3 := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2, ep3})
		assert.Greater(t, scores3[ep1], scores3[ep2])
		assert.Greater(t, scores3[ep2], scores3[ep3])
		assert.InDelta(t, 0.0, scores3[ep3], 1e-6)
	})

	t.Run("in-flight tokens contribute to cluster pi when scraped KVCacheUsagePercent is zero", func(t *testing.T) {
		scorer := NewTokenDelayScorer(Config{})

		// Before Prometheus scrapes reflect an in-flight burst, scraped KVCacheUsagePercent == 0,
		// but ep1 and ep2 each have 500 in-flight tokens out of 1000 capacity -> effective pi = 0.5, kappa = 3.0.
		ep1 := makeEndpoint("pod1", 500, 0, 200, 0.0, 1000, true)
		ep2 := makeEndpoint("pod2", 500, 0, 600, 0.0, 1000, true)

		scores := scorer.Score(ctx, &fwksched.InferenceRequest{}, []fwksched.Endpoint{ep1, ep2})
		// L_1 = 500 + 200 + 2*0 = 700; L_2 = 500 + 600 + 2*400 = 1900.
		assert.InDelta(t, 1.0-700.0/1900.0, scores[ep1], 1e-6)
		assert.InDelta(t, 0.0, scores[ep2], 1e-6)
	})
}

func BenchmarkTokenDelayScorer_Score(b *testing.B) {
	numPods := 8
	endpoints := make([]fwksched.Endpoint, numPods)
	scorer := NewTokenDelayScorer(Config{})

	for i := range numPods {
		endpoints[i] = makeEndpoint(
			fmt.Sprintf("pod%d", i),
			int64(i*200),
			int64(i),
			int64(500+i*100),
			0.5,
			32768,
			true,
		)
	}

	req := &fwksched.InferenceRequest{}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = scorer.Score(ctx, req, endpoints)
	}
}
