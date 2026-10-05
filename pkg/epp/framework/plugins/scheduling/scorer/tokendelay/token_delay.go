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

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
)

const (
	// TokenDelayScorerType is the registered plugin type name for TokenDelayScorer.
	TokenDelayScorerType = "token-delay-scorer"

	maxCongestionOccupancy = 0.999 // Clamps 1 - pi >= 1e-3 to guard division by zero.
)

// Config holds the configuration for the TokenDelayScorer.
type Config struct {
	// InFlightLoadProducerName selects which in-flight load producer's attributes to read.
	InFlightLoadProducerName string `json:"inFlightLoadProducerName,omitempty"`
}

// compile-time type assertions
var (
	_ fwksched.Scorer          = &TokenDelayScorer{}
	_ fwkplugin.ConsumerPlugin = &TokenDelayScorer{}
)

// TokenDelayScorer scores candidate endpoints by minimizing expected token delay:
//
//	L_e(t, r) = Q_e(t) + (1 + n_e(t)) * stepTokens + (kappa(t) - 1) * (u_e(r) - u_min(r))
//
// where stepTokens is u_e(r), or 1 when u_e(r) == 0 and n_e(t) > 0.
type TokenDelayScorer struct {
	typedName                    fwkplugin.TypedName
	inFlightLoadDataKey          fwkplugin.DataKey
	uncachedRequestTokensDataKey fwkplugin.DataKey
}

// TokenDelayScorerFactory constructs a TokenDelayScorer from JSON configuration.
func TokenDelayScorerFactory(name string, params *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := Config{}
	if params != nil {
		if err := params.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal token delay scorer config: %w", err)
		}
	}
	return NewTokenDelayScorer(cfg).WithName(name), nil
}

// NewTokenDelayScorer returns a new TokenDelayScorer.
func NewTokenDelayScorer(cfg Config) *TokenDelayScorer {
	return &TokenDelayScorer{
		typedName:           fwkplugin.TypedName{Type: TokenDelayScorerType, Name: TokenDelayScorerType},
		inFlightLoadDataKey: attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName(cfg.InFlightLoadProducerName),
		uncachedRequestTokensDataKey: attrconcurrency.UncachedRequestTokensDataKey.WithNonEmptyProducerName(
			cfg.InFlightLoadProducerName,
		),
	}
}

// WithName sets the plugin instance name.
func (s *TokenDelayScorer) WithName(name string) *TokenDelayScorer {
	s.typedName.Name = name
	return s
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *TokenDelayScorer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

// Category returns the preference the scorer applies when scoring candidate endpoints.
func (s *TokenDelayScorer) Category() fwksched.ScorerCategory {
	return fwksched.Distribution
}

// Consumes declares the endpoint attributes and optional metrics read by this scorer.
func (s *TokenDelayScorer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{
			s.inFlightLoadDataKey:          attrconcurrency.InFlightLoad{},
			s.uncachedRequestTokensDataKey: attrconcurrency.UncachedRequestTokens{},
		},
		Optional: map[fwkplugin.DataKey]any{
			fwkplugin.NewDataKey(metrics.KVCacheUsagePercentKey, metrics.MetricsExtractorType): float64(0),
		},
	}
}

type endpointLoadState struct {
	qTokens     float64
	nRequests   float64
	uTokens     float64
	effectiveKV float64
}

// Score computes normalized delay scores in [0, 1] for candidate endpoints.
func (s *TokenDelayScorer) Score(ctx context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	if len(endpoints) == 0 {
		return scores
	}

	logger := log.FromContext(ctx)
	debugLogger := logger.V(logutil.DEBUG)
	debugEnabled := debugLogger.Enabled()

	states := make([]endpointLoadState, len(endpoints))
	uMin := math.Inf(1)
	uMax := math.Inf(-1)
	minEffectiveKV := math.Inf(1)
	maxEffectiveKV := math.Inf(-1)

	var (
		weightedUsageSum   float64
		totalCapacity      float64
		capacityCount      int
		unweightedUsageSum float64
		updatedCount       int
	)

	for i, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		var qTokens, nRequests, uTokens float64
		if val, ok := endpoint.Get(s.inFlightLoadDataKey); ok {
			if load, ok := val.(*attrconcurrency.InFlightLoad); ok && load != nil {
				qTokens = float64(max(load.Tokens, 0))
				nRequests = float64(max(load.Requests, 0))
			}
		}
		if val, ok := endpoint.Get(s.uncachedRequestTokensDataKey); ok {
			if uncached, ok := val.(*attrconcurrency.UncachedRequestTokens); ok && uncached != nil {
				uTokens = float64(max(uncached.Tokens, 0))
			}
		}
		if uTokens < uMin {
			uMin = uTokens
		}
		if uTokens > uMax {
			uMax = uTokens
		}

		effectiveKV := 0.0
		podMetrics := endpoint.GetMetrics()
		if podMetrics != nil && podMetrics.Updated() &&
			!math.IsNaN(podMetrics.KVCacheUsagePercent) && !math.IsInf(podMetrics.KVCacheUsagePercent, 0) {
			scrapedUsage := min(max(podMetrics.KVCacheUsagePercent, 0.0), 1.0)
			effectiveKV = scrapedUsage
			if podMetrics.KvCacheMaxTokenCapacity > 0 && qTokens > 0 {
				inFlightUsage := qTokens / float64(podMetrics.KvCacheMaxTokenCapacity)
				effectiveKV = min(scrapedUsage+inFlightUsage, 1.0)
			}
			clusterUsage := scrapedUsage
			if scrapedUsage == 0 && effectiveKV > 0 {
				clusterUsage = effectiveKV
			}
			updatedCount++
			unweightedUsageSum += clusterUsage
			if podMetrics.KvCacheMaxTokenCapacity > 0 {
				capTokens := float64(podMetrics.KvCacheMaxTokenCapacity)
				weightedUsageSum += clusterUsage * capTokens
				totalCapacity += capTokens
				capacityCount++
			}
		} else if podMetrics != nil && podMetrics.KvCacheMaxTokenCapacity > 0 && qTokens > 0 {
			effectiveKV = min(qTokens/float64(podMetrics.KvCacheMaxTokenCapacity), 1.0)
		}

		states[i] = endpointLoadState{
			qTokens:     qTokens,
			nRequests:   nRequests,
			uTokens:     uTokens,
			effectiveKV: effectiveKV,
		}
		if effectiveKV < minEffectiveKV {
			minEffectiveKV = effectiveKV
		}
		if effectiveKV > maxEffectiveKV {
			maxEffectiveKV = effectiveKV
		}
	}
	if math.IsInf(uMin, 1) {
		uMin = 0
		uMax = 0
	}
	if math.IsInf(minEffectiveKV, 1) {
		minEffectiveKV = 0
		maxEffectiveKV = 0
	}

	pi := 0.0
	if updatedCount > 0 {
		if capacityCount == updatedCount && totalCapacity > 0 {
			pi = weightedUsageSum / totalCapacity
		} else {
			pi = unweightedUsageSum / float64(updatedCount)
		}
	}
	pi = min(max(pi, 0.0), maxCongestionOccupancy)
	oneMinusPi := 1.0 - pi
	kappa := 1.0 + pi/(oneMinusPi*oneMinusPi)

	loads := make([]float64, len(endpoints))
	minLoad := math.Inf(1)
	maxLoad := 0.0
	for i, st := range states {
		if endpoints[i] == nil {
			continue
		}
		stepTokens := st.uTokens
		if stepTokens == 0 && st.nRequests > 0 {
			stepTokens = 1.0
		}
		compDelay := st.qTokens + (1.0+st.nRequests)*stepTokens
		load := compDelay + (kappa-1.0)*(st.uTokens-uMin)
		loads[i] = load
		if load < minLoad {
			minLoad = load
		}
		if load > maxLoad {
			maxLoad = load
		}
	}

	// When all candidates have identical uncached tokens (Delta u_e == 0) and differ in
	// effective per-pod KV occupancy pi_e(t), break compute-delay ties using pi_e(t).
	if uMax == uMin && maxEffectiveKV > minEffectiveKV {
		if maxLoad == minLoad {
			maxLoad = 0.0
			for i, st := range states {
				if endpoints[i] == nil {
					continue
				}
				load := st.effectiveKV - minEffectiveKV
				loads[i] = load
				if load > maxLoad {
					maxLoad = load
				}
			}
		} else {
			tieScale := 1e-3 * (maxLoad - minLoad) / (maxEffectiveKV - minEffectiveKV)
			maxLoad = 0.0
			for i, st := range states {
				if endpoints[i] == nil {
					continue
				}
				loads[i] += (st.effectiveKV - minEffectiveKV) * tieScale
				if loads[i] > maxLoad {
					maxLoad = loads[i]
				}
			}
		}
	}

	for i, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		load := loads[i]
		score := 1.0
		if maxLoad > 0 && load > 0 {
			score = 1.0 - (load / maxLoad)
		}
		scores[endpoint] = score
		if debugEnabled {
			endpointID := ""
			if md := endpoint.GetMetadata(); md != nil {
				endpointID = md.ID.String()
			}
			debugLogger.Info("TokenDelayScorer scoring",
				"endpoint", endpointID,
				"qTokens", states[i].qTokens,
				"nRequests", states[i].nRequests,
				"uTokens", states[i].uTokens,
				"effectiveKV", states[i].effectiveKV,
				"uMin", uMin,
				"pi", pi,
				"kappa", kappa,
				"tokenDelayLoad", load,
				"maxLoad", maxLoad,
				"score", score,
			)
		}
	}

	return scores
}
