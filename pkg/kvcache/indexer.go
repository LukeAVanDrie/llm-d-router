/*
Copyright 2025 The llm-d Authors.

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

package kvcache

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvcache/metrics"
)

// Config holds the configuration for the Indexer module.
// The configuration cover the different components found in the Indexer
// module.
type Config struct {
	KVBlockIndexConfig  *kvblock.IndexConfig    `json:"kvBlockIndexConfig"`
	KVBlockScorerConfig *KVBlockScorerConfig    // not exported
	BackendConfigs      []*KVCacheBackendConfig `json:"kvCacheBackendConfigs"`
}

// NewDefaultConfig returns a default configuration for the Indexer module.
func NewDefaultConfig() (*Config, error) {
	return &Config{
		KVBlockIndexConfig:  kvblock.DefaultIndexConfig(),
		KVBlockScorerConfig: DefaultKVBlockScorerConfig(),
		BackendConfigs:      DefaultKVCacheBackendConfig(),
	}, nil
}

// Indexer is a concrete implementation of the KVCacheIndex interface.
type Indexer struct {
	config *Config

	tokenProcessor kvblock.TokenProcessor // turns tokens to kv block keys
	kvBlockIndex   kvblock.Index          // looks up pods for block keys
	keyWalker      kvblock.KeyWalker      // kvBlockIndex's walk capability; nil without one
	tierWeights    map[string]float64     // device tier -> weight for the prefix matcher
	// recordHits enables the contiguous-chain hit metrics, under the same
	// option that instruments the index.
	recordHits bool
}

// NewKVCacheIndexer creates a KVCacheIndex given a Config. Callers tokenize
// externally and use the tokens-in API (Indexer.ScoreTokens).
func NewKVCacheIndexer(ctx context.Context, config *Config, tokenProcessor kvblock.TokenProcessor) (*Indexer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if tokenProcessor == nil {
		return nil, fmt.Errorf("tokenProcessor cannot be nil")
	}

	kvBlockIndex, err := kvblock.NewIndex(ctx, config.KVBlockIndexConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create RedisKVBlockIndexer: %w", err)
	}

	// Wrap index with tracing instrumentation.
	// When tracing is not configured, the tracer is a no-op implementation.
	kvBlockIndex = kvblock.NewTracedIndex(kvBlockIndex)

	// override backend configs with the ones from the config, if the defaults are not used.
	config.KVBlockScorerConfig.BackendConfigs = config.BackendConfigs
	if strategy := config.KVBlockScorerConfig.ScoringStrategy; strategy != LongestPrefixMatch {
		return nil, fmt.Errorf("unsupported scoring strategy: %s", strategy)
	}

	// A nil index config selects kvblock's defaults, metrics off included.
	recordHits := config.KVBlockIndexConfig != nil && config.KVBlockIndexConfig.EnableMetrics
	indexer := newIndexer(tokenProcessor, kvBlockIndex, config.BackendConfigs, recordHits)
	indexer.config = config
	return indexer, nil
}

// newIndexer wires an Indexer over kvBlockIndex, keeping its walk capability
// when it has one.
func newIndexer(tokenProcessor kvblock.TokenProcessor, kvBlockIndex kvblock.Index,
	backends []*KVCacheBackendConfig, recordHits bool,
) *Indexer {
	keyWalker, _ := kvBlockIndex.(kvblock.KeyWalker)
	return &Indexer{
		tokenProcessor: tokenProcessor,
		kvBlockIndex:   kvBlockIndex,
		keyWalker:      keyWalker,
		tierWeights:    tierWeightsFromBackends(backends),
		recordHits:     recordHits,
	}
}

// Run starts the indexer. Blocks until ctx is cancelled.
func (k *Indexer) Run(ctx context.Context) {
	<-ctx.Done()
}

// KVBlockIndex returns the kvblock.Index used by the Indexer.
func (k *Indexer) KVBlockIndex() kvblock.Index {
	return k.kvBlockIndex
}

// ComputeBlockKeysFromTokens computes the KV-block keys for a pre-tokenized
// prompt. Callers tokenize and truncate externally. extraFeatures provides
// per-block multimodal data that taints the hash; nil means text-only.
func (k *Indexer) ComputeBlockKeysFromTokens(ctx context.Context, tokens []uint32, modelName string,
	extraFeatures []*kvblock.BlockExtraFeatures,
) ([]kvblock.BlockHash, error) {
	blockKeys, err := k.tokenProcessor.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, modelName, extraFeatures)
	if err != nil {
		log.FromContext(ctx).V(logging.TRACE).WithName("kvcache.ComputeBlockKeysFromTokens").Error(err, "blockKey conversion failed")
		return nil, fmt.Errorf("blockKey conversion failed: %w", err)
	}
	if traceLogger := log.FromContext(ctx).V(logging.TRACE); traceLogger.Enabled() {
		traceLogger = traceLogger.WithName("kvcache.ComputeBlockKeysFromTokens")
		if len(blockKeys) == 0 {
			traceLogger.Info("no block keys found")
			return nil, nil
		}
		traceLogger.Info("computed block keys", "tokens", tokens, "block-keys", blockKeys)
	} else if len(blockKeys) == 0 {
		return nil, nil
	}

	return blockKeys, nil
}

// ScoreTokens computes pod scores for the given tokens and model: the
// weighted prefix match (PodMatch.WeightedScore) of each pod holding the
// first block key.
//
// extraFeatures provides per-block multimodal data that taints the hash;
// nil means text-only. podIdentifiers limits scoring to the given pod addresses.
// If empty, all pods are considered.
func (k *Indexer) ScoreTokens(
	ctx context.Context,
	tokens []uint32,
	modelName string,
	podIdentifiers []string,
	extraFeatures []*kvblock.BlockExtraFeatures,
) (map[string]float64, error) {
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "score_tokens", internalSpanStartOpts...)
	defer span.End()

	// Correlate the log lines below (block keys, pod scores) when the indexer is
	// driven directly. Reached through an EPP request the context is already
	// correlated at the entry point and this is a no-op.
	ctx = tracing.LoggerWithSpanContext(ctx, span)
	traceLogger := log.FromContext(ctx).V(logging.TRACE)
	traceEnabled := traceLogger.Enabled()
	if traceEnabled {
		traceLogger = traceLogger.WithName("kvcache.ScoreTokens")
	}

	blockKeys, err := k.tokenProcessor.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, modelName, extraFeatures)
	if err != nil {
		return nil, fmt.Errorf("blockKey conversion failed: %w", err)
	}

	if span.IsRecording() {
		span.SetAttributes(
			semconv.GenAIRequestModel(modelName),
			semconv.LLMDKVCachePodCount(len(podIdentifiers)),
			semconv.LLMDKVCacheTokenCount(len(tokens)),
			semconv.LLMDKVCacheBlockKeysCount(len(blockKeys)),
		)
	}

	if len(blockKeys) == 0 {
		if traceEnabled {
			traceLogger.Info("no block keys found, returning empty scores")
		}
		//nolint:nilnil // no need to return an error
		return nil, nil
	}
	if traceEnabled {
		traceLogger.Info("found tokens", "tokens", tokens, "block-keys", blockKeys)
	}

	var podFilter sets.Set[string]
	if len(podIdentifiers) > 0 {
		podFilter = sets.New(podIdentifiers...)
	}
	podScores, blocksFound, err := k.scoreBlockKeys(ctx, blockKeys, podFilter)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to match block keys: %w", err)
	}
	if traceEnabled {
		traceLogger.Info("matched block keys", "block-keys", blockKeys, "scores", podScores)
	}

	// Block-level hit telemetry: the longest contiguous prefix one candidate
	// holds, which is as far as a walk reads.
	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCacheBlockHitRatio(float64(blocksFound)/float64(len(blockKeys))),
			semconv.LLMDKVCacheBlocksFound(blocksFound),
		)
	}

	return podScores, nil
}

func (k *Indexer) scoreBlockKeys(ctx context.Context, keys []kvblock.BlockHash,
	podFilter sets.Set[string],
) (map[string]float64, int, error) {
	if len(keys) == 0 {
		return map[string]float64{}, 0, nil
	}

	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "match_block_keys", internalSpanStartOpts...)
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCachePrefixMatchKeyCount(len(keys)),
			semconv.LLMDKVCachePrefixMatchPodFilterCount(podFilter.Len()),
			semconv.LLMDKVCachePrefixMatchWalked(k.keyWalker != nil),
		)
	}

	var scores map[string]float64
	var blocksFound int
	var err error
	if k.keyWalker != nil {
		scores, blocksFound, err = scoreWalk(ctx, k.keyWalker, keys, k.tierWeights, podFilter)
	} else {
		scores, blocksFound, err = scoreLookup(ctx, k.kvBlockIndex, keys, k.tierWeights, podFilter)
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, 0, err
	}

	if k.recordHits {
		metrics.MaxPodHitCount.Add(float64(blocksFound))
		metrics.LookupHits.Add(float64(blocksFound))
	}
	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCachePrefixMatchPodsMatched(len(scores)),
			semconv.LLMDKVCachePrefixMatchLongestChain(blocksFound),
		)
	}
	return scores, blocksFound, nil
}
