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

// PROTOTYPE (fork only): differential test of ChainIndex against
// InMemoryIndex through the prefix matcher.

package kvcache_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// chainTree draws root paths over a small token alphabet so paths share
// prefixes often. Keys are chained: a key commits to its whole prefix.
type chainTree struct {
	rng    *rand.Rand
	maxLen int
}

func (t chainTree) path() []kvblock.BlockHash {
	n := 1 + t.rng.Intn(t.maxLen)
	out := make([]kvblock.BlockHash, n)
	h := uint64(0x51ed)
	for i := range out {
		h = splitmix(h ^ uint64(1+t.rng.Intn(3))*0x9e3779b97f4a7c15)
		if h == 0 {
			h = 1
		}
		out[i] = kvblock.BlockHash(h)
	}
	return out
}

func splitmix(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func engineOf(rks []kvblock.BlockHash) []kvblock.BlockHash {
	out := make([]kvblock.BlockHash, len(rks))
	for i, k := range rks {
		out[i] = k ^ 0x5bd1e995
	}
	return out
}

func TestChainIndexMatchesInMemoryIndex(t *testing.T) {
	ctx := context.Background()
	pods := []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000", "10.0.0.4:8000"}
	entryFor := func(rng *rand.Rand, pod string) kvblock.PodEntry {
		switch rng.Intn(6) {
		case 0:
			return kvblock.PodEntry{PodIdentifier: pod, DeviceTier: "cpu"}
		case 1:
			return kvblock.PodEntry{PodIdentifier: pod, DeviceTier: kvcache.GPUTier, HasGroup: true, GroupIdx: 1}
		case 2:
			return kvblock.PodEntry{PodIdentifier: pod, DeviceTier: "gpu-unknown-weight"}
		default:
			return kvblock.PodEntry{PodIdentifier: pod, DeviceTier: kvcache.GPUTier}
		}
	}

	for seed := int64(0); seed < 60; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			// Long paths on odd seeds exercise the galloping and bisection steps.
			tree := chainTree{rng: rng, maxLen: 12 + 36*int(seed%2)}
			// One tier only for some seeds keeps every pod uniform, which
			// exercises the closed-form path.
			uniform := seed%3 == 0

			ref, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: 1 << 20, PodCacheSize: 256})
			if err != nil {
				t.Fatal(err)
			}
			chain := kvblock.NewChainIndex(kvblock.ChainIndexConfig{})
			refM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, ref, kvcache.DefaultKVCacheBackendConfig())
			chainM := kvcache.NewIndexerForTestWithoutMetrics(&mockTokenProcessor{}, chain, kvcache.DefaultKVCacheBackendConfig())

			var held [][]kvblock.BlockHash
			for op := 0; op < 400; op++ {
				pod := pods[rng.Intn(len(pods))]
				var entries []kvblock.PodEntry
				if uniform {
					entries = []kvblock.PodEntry{{PodIdentifier: pod, DeviceTier: kvcache.GPUTier}}
				} else {
					entries = []kvblock.PodEntry{entryFor(rng, pod)}
					if rng.Intn(4) == 0 {
						entries = append(entries, entryFor(rng, pod))
					}
				}
				p := tree.path()
				a := rng.Intn(len(p))
				if rng.Intn(3) == 0 {
					a = 0
				}
				b := a + 1 + rng.Intn(len(p)-a)
				seg := p[a:b]
				parent := kvblock.EmptyBlockHash
				if a > 0 {
					parent = p[a-1]
				}

				var desc string
				switch r := rng.Intn(20); {
				case r < 9: // event store
					desc = "store"
					if err := ref.Add(ctx, engineOf(seg), seg, entries); err != nil {
						t.Fatal(err)
					}
					if err := chain.AddChain(ctx, parent, engineOf(seg), seg, entries); err != nil {
						t.Fatal(err)
					}
					held = append(held, p[:b])
				case r < 11 && !uniform: // speculative store from the root
					desc = "speculative"
					spec := []kvblock.PodEntry{{PodIdentifier: pod, Speculative: true}}
					if err := ref.Add(ctx, nil, p[:b], spec); err != nil {
						t.Fatal(err)
					}
					if err := chain.Add(ctx, nil, p[:b], spec); err != nil {
						t.Fatal(err)
					}
				case r < 15: // engine-key eviction, often of a held path's head
					desc = "evict-engine"
					keys := seg
					if len(held) > 0 && rng.Intn(2) == 0 {
						h := held[rng.Intn(len(held))]
						i := rng.Intn(min(3, len(h)))
						keys = h[i : i+1]
					}
					if err := ref.Evict(ctx, kvblock.EngineKey, engineOf(keys), entries); err != nil {
						t.Fatal(err)
					}
					if err := chain.Evict(ctx, kvblock.EngineKey, engineOf(keys), entries); err != nil {
						t.Fatal(err)
					}
				case r < 18: // request-key eviction
					desc = "evict-request"
					if !uniform && rng.Intn(2) == 0 {
						entries = []kvblock.PodEntry{{PodIdentifier: pod, Speculative: true}}
					}
					if err := ref.Evict(ctx, kvblock.RequestKey, seg, entries); err != nil {
						t.Fatal(err)
					}
					if err := chain.Evict(ctx, kvblock.RequestKey, seg, entries); err != nil {
						t.Fatal(err)
					}
				case r < 19: // refill a held path's head
					desc = "refill"
					if len(held) == 0 {
						continue
					}
					h := held[rng.Intn(len(held))]
					i := rng.Intn(min(3, len(h)))
					par := kvblock.EmptyBlockHash
					if i > 0 {
						par = h[i-1]
					}
					if err := ref.Add(ctx, engineOf(h[i:i+1]), h[i:i+1], entries); err != nil {
						t.Fatal(err)
					}
					if err := chain.AddChain(ctx, par, engineOf(h[i:i+1]), h[i:i+1], entries); err != nil {
						t.Fatal(err)
					}
				default:
					desc = "clear"
					if rng.Intn(4) != 0 {
						continue
					}
					if err := ref.Clear(ctx, pod); err != nil {
						t.Fatal(err)
					}
					if err := chain.Clear(ctx, pod); err != nil {
						t.Fatal(err)
					}
				}

				for q := 0; q < 6; q++ {
					keys := tree.path()
					if len(held) > 0 && q%2 == 0 {
						keys = held[rng.Intn(len(held))]
					}
					var filter sets.Set[string]
					if q == 5 {
						filter = sets.New(pods[0], pods[2])
					}
					want, err := refM.MatchBlockKeys(ctx, keys, filter)
					if err != nil {
						t.Fatal(err)
					}
					got, err := chainM.MatchBlockKeys(ctx, keys, filter)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(want, got) {
						t.Fatalf("op %d (%s): match differs\nwant %+v\ngot  %+v", op, desc, want, got)
					}
				}
			}
		})
	}
}
