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

// PROTOTYPE (fork only): hit rates of ChainIndex and InMemoryIndex when
// both are bounded below the working set, replaying the trace in order.

package kvcache_test

import (
	"fmt"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/test/utils/mooncake"
)

func TestMooncakeChainCapacityHitRate(t *testing.T) {
	r := mooncake.Load(t)
	for _, total := range []int{1 << 20, 1 << 18, 1 << 16} {
		for _, variant := range []string{"even-split", "engine-derived"} {
			capacityHitRate(t, r, total, variant)
		}
	}
}

// capacityHitRate replays the trace with main bounded to total keys and the
// chain index bounded per pod: total/Workers each (even-split), or at each
// worker's simulated engine capacity (engine-derived). The chain index
// stamps only the routed pod, after matching.
func capacityHitRate(t *testing.T, r *mooncake.Replay, total int, variant string) {
	ctx := benchContext()
	{
		ref, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: total, PodCacheSize: mooncake.Workers})
		if err != nil {
			t.Fatal(err)
		}
		podCap := total / mooncake.Workers
		if variant == "engine-derived" {
			podCap = mooncake.WorkerBlocks
		}
		chain := kvblock.NewChainIndex(kvblock.ChainIndexConfig{PodCapacity: podCap})
		refM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, ref, kvcache.DefaultKVCacheBackendConfig())
		chainM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, chain, kvcache.DefaultKVCacheBackendConfig())

		var refBest, chainBest, refAll, chainAll, keys float64
		var refRouted, chainRouted float64
		for i, tr := range r.Turns {
			q := mooncake.Convert[kvblock.BlockHash](tr.Query)
			worker := fmt.Sprintf("10.0.0.%d:8000", tr.Worker)
			entries := []kvblock.PodEntry{{PodIdentifier: worker, DeviceTier: "gpu"}}
			want, err := refM.MatchBlockKeys(ctx, q, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := chainM.MatchBlockKeys(ctx, q, nil)
			if err != nil {
				t.Fatal(err)
			}
			if i >= len(r.Turns)/4 { // after warm-up
				refBest += float64(best(want))
				chainBest += float64(best(got))
				refAll += float64(sum(want))
				chainAll += float64(sum(got))
				refRouted += float64(want[worker].MatchedBlocks)
				chainRouted += float64(got[worker].MatchedBlocks)
				keys += float64(len(q))
			}
			chain.TouchPod(worker, q, got[worker].MatchedBlocks)
			for _, seg := range segmentsOf(q, mooncake.Convert[kvblock.BlockHash](tr.Stored)) {
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
		t.Logf("budget %7d keys, %-14s: best-pod hit %.4f (main) vs %.4f (chain); routed-pod hit %.4f vs %.4f; all-pod blocks/key %.3f vs %.3f",
			total, variant, refBest/keys, chainBest/keys, refRouted/keys, chainRouted/keys, refAll/keys, chainAll/keys)
	}
}

func best(m map[string]kvcache.PodMatch) int {
	b := 0
	for _, pm := range m {
		b = max(b, pm.MatchedBlocks)
	}
	return b
}

func sum(m map[string]kvcache.PodMatch) int {
	s := 0
	for _, pm := range m {
		s += pm.MatchedBlocks
	}
	return s
}
