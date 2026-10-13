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

package approximateprefix

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"testing"
)

// TestIndexerMatchesLRUIndexer replays random adds and pod removals against
// the indexer and the simplelru indexer it replaced, and requires identical
// matches and block counts after every operation.
func TestIndexerMatchesLRUIndexer(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			defaultSize := []int{4, 16, 64}[seed%3]
			got := newIndexer(t.Context(), defaultSize, "test", "test").(*indexer)
			want := newLRUIndexer(t.Context(), defaultSize, "test", "test")

			servers := make([]server, 5)
			for i := range servers {
				servers[i] = server{ServerID: ServerID{Namespace: "ns", Name: fmt.Sprint("pod-", i)}}
				if rng.Intn(2) == 0 {
					servers[i].NumOfGPUBlocks = 1 + rng.Intn(40)
				}
			}
			// Two block-size domains: chains never share keys across them.
			prompt := func() []blockHash {
				domain := uint64(rng.Intn(2)) << 40
				n := 1 + rng.Intn(1+3*defaultSize)
				out := make([]blockHash, n)
				h := domain
				for i := range out {
					h = h*31 + uint64(1+rng.Intn(3))
					out[i] = blockHash(h)
				}
				return out
			}
			var prompts [][]blockHash
			candidates := make([]ServerID, 0, len(servers)+1)
			for _, s := range servers {
				candidates = append(candidates, s.ServerID)
			}
			candidates = append(candidates, ServerID{Namespace: "ns", Name: "unknown"})

			for op := 0; op < 400; op++ {
				if rng.Intn(25) == 0 {
					id := servers[rng.Intn(len(servers))].ServerID
					got.RemovePod(id)
					want.RemovePod(id)
				} else {
					p := prompt()
					prompts = append(prompts, p)
					s := servers[rng.Intn(len(servers))]
					got.Add(p, s)
					want.Add(p, s)
				}
				if g, w := got.PodBlockCounts(), want.PodBlockCounts(); !reflect.DeepEqual(g, w) {
					t.Fatalf("op %d: block counts %v, want %v", op, g, w)
				}
				gp, wp := got.Pods(), want.Pods()
				slices.SortFunc(gp, cmpID)
				slices.SortFunc(wp, cmpID)
				if !reflect.DeepEqual(gp, wp) {
					t.Fatalf("op %d: pods %v, want %v", op, gp, wp)
				}
				for q := 0; q < 4; q++ {
					var keys []blockHash
					if len(prompts) > 0 {
						keys = prompts[rng.Intn(len(prompts))]
					}
					if q == 0 || keys == nil {
						keys = prompt()
					}
					if g, w := got.MatchLongestPrefix(keys, candidates), want.MatchLongestPrefix(keys, candidates); !slices.Equal(g, w) {
						t.Fatalf("op %d: matches %v, want %v", op, g, w)
					}
				}
			}
		})
	}
}

func cmpID(a, b ServerID) int {
	switch {
	case a.String() < b.String():
		return -1
	case a.String() > b.String():
		return 1
	}
	return 0
}
