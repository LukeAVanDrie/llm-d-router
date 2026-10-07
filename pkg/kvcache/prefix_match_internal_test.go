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

package kvcache

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// Cold request state must scale with the live candidates of a request, not
// with the ordinals the index has assigned to pods since cleared.
func TestMatchBlockKeysColdStateDoesNotTrackPodHistory(t *testing.T) {
	measure := func(churnPods int) uint64 {
		ctx := log.IntoContext(context.Background(), logr.Discard())
		index, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: 128, PodCacheSize: 128})
		require.NoError(t, err)
		indexer := newIndexer(nil, index, DefaultKVCacheBackendConfig(), true)

		keys := []kvblock.BlockHash{10, 20}
		for i := 0; i < 96; i++ {
			entry := []kvblock.PodEntry{{PodIdentifier: fmt.Sprintf("pod-live-%d", i), DeviceTier: "gpu"}}
			require.NoError(t, index.Add(ctx, nil, keys, entry))
		}
		for i := 0; i < churnPods; i++ {
			pod := fmt.Sprintf("pod-gone-%d", i)
			require.NoError(t, index.Add(ctx, nil, []kvblock.BlockHash{999}, []kvblock.PodEntry{{PodIdentifier: pod, DeviceTier: "gpu"}}))
			require.NoError(t, index.Clear(ctx, pod))
		}

		accumulatorPool = sync.Pool{New: func() any { return &prefixAccumulator{} }}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		matches, err := indexer.MatchBlockKeys(ctx, keys, nil)
		require.NoError(t, err)
		require.Len(t, matches, 96)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}

	measure(0) // warm lazy runtime and package initialization
	withoutChurn := measure(0)
	withChurn := measure(20_000)
	assert.Less(t, withChurn, withoutChurn+128*1024,
		"cold request state must scale with live candidates, not with pod ordinal history")
}

// Request state must not scale with the tier ordinals the index has assigned
// either: a live tier interned after thousands of others costs the same as
// the first.
func TestMatchBlockKeysColdStateDoesNotTrackTierHistory(t *testing.T) {
	measure := func(churnTiers int) uint64 {
		ctx := log.IntoContext(context.Background(), logr.Discard())
		index, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: 128, PodCacheSize: 128})
		require.NoError(t, err)
		indexer := newIndexer(nil, index, DefaultKVCacheBackendConfig(), true)

		for i := 0; i < churnTiers; i++ {
			entry := []kvblock.PodEntry{{PodIdentifier: "pod-gone", DeviceTier: fmt.Sprintf("tier-%d", i)}}
			require.NoError(t, index.Add(ctx, nil, []kvblock.BlockHash{999}, entry))
		}
		require.NoError(t, index.Clear(ctx, "pod-gone"))
		keys := []kvblock.BlockHash{10, 20}
		for i := 0; i < 8; i++ {
			entry := []kvblock.PodEntry{{PodIdentifier: fmt.Sprintf("pod-live-%d", i), DeviceTier: "live-tier"}}
			require.NoError(t, index.Add(ctx, nil, keys, entry))
		}

		accumulatorPool = sync.Pool{New: func() any { return &prefixAccumulator{} }}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		matches, err := indexer.MatchBlockKeys(ctx, keys, nil)
		require.NoError(t, err)
		require.Len(t, matches, 8)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}

	measure(0)
	withoutChurn := measure(0)
	withChurn := measure(3000)
	assert.Less(t, withChurn, withoutChurn+16*1024,
		"request state must scale with the tiers a request sees, not with tier ordinal history")
}

func TestPrefixAccumulatorSingleToMultiTierTransition(t *testing.T) {
	weights := map[string]float64{"gpu": 1.0, "cpu": 0.5}
	acc := acquireAccumulator(weights, nil, true)
	defer releaseAccumulator(acc)

	for i := range 4 {
		acc.recordLocalPod(uint32(i), fmt.Sprintf("pod-%d", i))
	}

	// pos 0: single-tier "gpu" with pods 0, 1, 2, 3 (enters singleTier = true)
	require.True(t, acc.walkSnapshot(0, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-0", DeviceTier: "gpu"}, PodOrdinal: 0, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-1", DeviceTier: "gpu"}, PodOrdinal: 1, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-2", DeviceTier: "gpu"}, PodOrdinal: 2, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-3", DeviceTier: "gpu"}, PodOrdinal: 3, TierOrdinal: 0},
	})))
	assert.True(t, acc.singleTier)

	// pos 1: single-tier "gpu" with pods 0, 1, 2 (pod-3 drops at firstDropPos = 1)
	require.True(t, acc.walkSnapshot(1, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-0", DeviceTier: "gpu"}, PodOrdinal: 0, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-1", DeviceTier: "gpu"}, PodOrdinal: 1, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-2", DeviceTier: "gpu"}, PodOrdinal: 2, TierOrdinal: 0},
	})))
	assert.True(t, acc.singleTier)

	// pos 2: single-tier "gpu" with pods 0, 1 (pod-2 drops into droppedMask at pos = 2)
	require.True(t, acc.walkSnapshot(2, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-0", DeviceTier: "gpu"}, PodOrdinal: 0, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-1", DeviceTier: "gpu"}, PodOrdinal: 1, TierOrdinal: 0},
	})))
	assert.True(t, acc.singleTier)

	// pos 3: multi-tier ("gpu" for pod-0, "cpu" for pod-1) triggers transitionToMultiTier()
	require.True(t, acc.walkSnapshot(3, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-0", DeviceTier: "gpu"}, PodOrdinal: 0, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-1", DeviceTier: "cpu"}, PodOrdinal: 1, TierOrdinal: 1},
	})))
	assert.False(t, acc.singleTier)

	got, longest := acc.result()
	assert.Equal(t, 4, longest)
	assert.Equal(t, PodMatch{WeightedScore: 4.0, MatchedBlocks: 4, ConfirmedBlocks: 4, BlocksByTier: map[string]int{"gpu": 4}}, got["pod-0"])
	assert.Equal(t, PodMatch{WeightedScore: 3.5, MatchedBlocks: 4, ConfirmedBlocks: 4, BlocksByTier: map[string]int{"gpu": 3}}, got["pod-1"])
	assert.Equal(t, PodMatch{WeightedScore: 2.0, MatchedBlocks: 2, ConfirmedBlocks: 2, BlocksByTier: map[string]int{"gpu": 2}}, got["pod-2"])
	assert.Equal(t, PodMatch{WeightedScore: 1.0, MatchedBlocks: 1, ConfirmedBlocks: 1, BlocksByTier: map[string]int{"gpu": 1}}, got["pod-3"])
}

func TestPrefixAccumulatorOverflowOrdinalsBeyond256(t *testing.T) {
	weights := map[string]float64{"gpu": 1.0, "cpu": 0.5}
	acc := acquireAccumulator(weights, nil, true)
	defer releaseAccumulator(acc)

	acc.recordLocalPod(300, "pod-300")
	acc.recordLocalPod(5000, "pod-5000")

	require.True(t, acc.walkSnapshot(0, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-300", DeviceTier: "gpu"}, PodOrdinal: 300, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-5000", DeviceTier: "gpu"}, PodOrdinal: 5000, TierOrdinal: 0},
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-5000", DeviceTier: "cpu"}, PodOrdinal: 5000, TierOrdinal: 1},
	})))
	require.True(t, acc.walkSnapshot(1, kvblock.BuildPodSnapshot([]kvblock.EntryRef{
		{PodEntry: kvblock.PodEntry{PodIdentifier: "pod-5000", DeviceTier: "cpu"}, PodOrdinal: 5000, TierOrdinal: 1},
	})))

	got, longest := acc.result()
	assert.Equal(t, 2, longest)
	assert.Equal(t, PodMatch{WeightedScore: 1.0, MatchedBlocks: 1, ConfirmedBlocks: 1, BlocksByTier: map[string]int{"gpu": 1}}, got["pod-300"])
	assert.Equal(t, PodMatch{WeightedScore: 1.5, MatchedBlocks: 2, ConfirmedBlocks: 2, BlocksByTier: map[string]int{"gpu": 1, "cpu": 2}}, got["pod-5000"])
}
