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

// PROTOTYPE (fork only): in-order Mooncake replay from an empty index,
// comparing ChainIndex with InMemoryIndex and counting cascades.

package kvcache_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/test/utils/mooncake"
)

func TestMooncakeChainReplayInOrder(t *testing.T) {
	r := mooncake.Load(t)
	ctx := benchContext()
	ref, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: 1 << 23, PodCacheSize: mooncake.Workers})
	if err != nil {
		t.Fatal(err)
	}
	chain := kvblock.NewChainIndex(kvblock.ChainIndexConfig{})
	refM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, ref, kvcache.DefaultKVCacheBackendConfig())
	chainM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, chain, kvcache.DefaultKVCacheBackendConfig())

	var orphanSegs, segs, sampled, matched, walked, keys int
	for i, tr := range r.Turns {
		q := mooncake.Convert[kvblock.BlockHash](tr.Query)
		entries := []kvblock.PodEntry{{PodIdentifier: fmt.Sprintf("10.0.0.%d:8000", tr.Worker), DeviceTier: "gpu"}}
		if i%64 == 0 {
			want, err := refM.MatchBlockKeys(ctx, q, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := chainM.MatchBlockKeys(ctx, q, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("turn %d: match differs\nwant %+v\ngot  %+v", i, want, got)
			}
			sampled++
			best := 0
			for _, m := range want {
				best = max(best, m.MatchedBlocks)
			}
			matched += len(want)
			walked += best
			keys += len(q)
		}
		for _, seg := range segmentsOf(q, mooncake.Convert[kvblock.BlockHash](tr.Stored)) {
			segs++
			if seg.parent != kvblock.EmptyBlockHash && seg.keys[0] != q[0] {
				orphanSegs++
			}
			if err := ref.Add(ctx, seg.keys, seg.keys, entries); err != nil {
				t.Fatal(err)
			}
			if err := chain.AddChain(ctx, seg.parent, seg.keys, seg.keys, entries); err != nil {
				t.Fatal(err)
			}
		}
		if removed := mooncake.Convert[kvblock.BlockHash](tr.Removed); len(removed) > 0 {
			if err := ref.Evict(ctx, kvblock.EngineKey, removed, entries); err != nil {
				t.Fatal(err)
			}
			if err := chain.Evict(ctx, kvblock.EngineKey, removed, entries); err != nil {
				t.Fatal(err)
			}
		}
	}
	stats := chain.Stats()
	t.Logf("turns %d, sampled %d (all equal), pods matched/sample %.1f, best run/L %.3f",
		len(r.Turns), sampled, float64(matched)/float64(sampled), float64(walked)/float64(keys))
	t.Logf("store segments %d, non-root segments %d, closure rebuilds %d (%.4f per turn), dormant %d, pairs %d",
		segs, orphanSegs, stats.Rebuilds, float64(stats.Rebuilds)/float64(len(r.Turns)), stats.Dormant, stats.Pairs)
}
