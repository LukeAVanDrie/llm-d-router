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

package prefixindex

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
)

// A resident key reached through a path whose previous key is not its parent
// is a miss: the run stops before it rather than counting a prefix the
// endpoint does not hold.
func TestRunLengthRejectsWrongParent(t *testing.T) {
	s := NewChainSet[uint64](0, nil)
	s.AddRootChain([]uint64{1, 2})
	s.AddChainUnder(1, []uint64{9}) // 9 is a child of 1, not of 2
	if got := s.RunLength([]uint64{1, 2, 9}, nil); got != 2 {
		t.Fatalf("RunLength([1 2 9]) = %d, want 2", got)
	}
	if got := s.RunLength([]uint64{1, 9}, nil); got != 2 {
		t.Fatalf("RunLength([1 9]) = %d, want 2", got)
	}

	idx := NewIndex[uint64](Config{Endpoints: 2})
	idx.AddRootChain(0, []uint64{1, 2})
	idx.AddChainUnder(0, 1, []uint64{9})
	if got := idx.Match([]uint64{1, 2, 9}, []int{0}); got[0] != 2 {
		t.Fatalf("Match([1 2 9]) = %v, want [2]", got)
	}
}

// A parent left childless inherits the stamp of the child it lost, so a key
// stored long ago but on a recently written path outranks a stale chain.
// AddChainUnder stamps the parent it names, so the inheritance shows two
// levels above the write.
func TestRemoveInheritsChildStamp(t *testing.T) {
	s := NewChainSet[uint64](6, nil)
	s.AddRootChain([]uint64{1, 2, 3, 4})     // stamp 1
	s.AddRootChain([]uint64{11, 12})         // stamp 2
	s.AddChainUnder(2, []uint64{3, 4})       // stamp 3 on 2, 3, 4; 1 keeps stamp 1
	s.Remove(4)                              // 3 inherits stamp 3
	s.Remove(3)                              // 2 inherits stamp 3
	s.Remove(2)                              // 1 inherits stamp 3
	s.AddRootChain([]uint64{21, 22, 23, 24}) // stamp 4; one block over capacity
	checkInvariants(t, s)
	if !s.Contains(1) {
		t.Fatal("the root of the recently written path was evicted")
	}
	if s.Contains(12) {
		t.Fatal("the stalest leaf survived")
	}
}

// EvictOldestLeaf always takes the leaf that orders first by (vis, stamp,
// key), as recomputed by brute force over the resident blocks.
func TestEvictOldestLeafMatchesBruteForce(t *testing.T) {
	for seed := range uint64(100) {
		rng := rand.New(rand.NewPCG(seed, 0x51))
		s := NewChainSet[uint64](0, nil)
		var chains [][]uint64
		for op := range 400 {
			c := makeChain(rng.Uint64N(12), 1+rng.IntN(24))
			switch r := rng.IntN(10); {
			case r < 4:
				s.AddRootChain(c)
				chains = append(chains, c)
			case r < 6 && len(chains) > 0:
				p := chains[rng.IntN(len(chains))]
				i := rng.IntN(len(p))
				if i == 0 {
					s.AddRootChain(p[:1+rng.IntN(len(p))])
				} else {
					s.AddChainUnder(p[i-1], p[i:])
				}
			case r < 8 && len(chains) > 0:
				p := chains[rng.IntN(len(chains))]
				s.Remove(p[rng.IntN(len(p))])
			default:
				want, ok := bruteOldestLeaf(s)
				got, gotOK := s.EvictOldestLeaf()
				if ok != gotOK || got != want {
					t.Fatalf("seed %d op %d: evicted (%x, %v), want (%x, %v)", seed, op, got, gotOK, want, ok)
				}
			}
			checkInvariants(t, s)
		}
	}
}

func bruteOldestLeaf(s *ChainSet[uint64]) (uint64, bool) {
	best, found := block[uint64]{}, false
	for _, b := range s.blocks {
		if b.children != 0 {
			continue
		}
		if !found || b.vis < best.vis ||
			(b.vis == best.vis && (b.stamp < best.stamp || (b.stamp == best.stamp && b.key < best.key))) {
			best, found = b, true
		}
	}
	return best.key, found
}

// lruModel is a key-level LRU filled as the approximate indexer fills it:
// each prompt's keys tail first, so block 0 ends most recent.
type lruModel struct {
	order    []uint64 // newest first
	capacity int
}

func (m *lruModel) addRootChain(keys []uint64) {
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		m.order = slices.DeleteFunc(m.order, func(x uint64) bool { return x == k })
		m.order = append([]uint64{k}, m.order...)
	}
	if len(m.order) > m.capacity {
		m.order = m.order[:m.capacity]
	}
}

// Under root-chain stores alone, capacity eviction keeps exactly the keys a
// tail-first key LRU of the same capacity keeps.
func TestRootChainCapacityMatchesKeyLRU(t *testing.T) {
	for seed := range uint64(100) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x52))
			capacity := []int{3, 8, 20, 64}[seed%4]
			s := NewChainSet[uint64](capacity, nil)
			m := &lruModel{capacity: capacity}
			for op := range 300 {
				// Prompts share prefixes: a few roots, short alphabets.
				keys := make([]uint64, 1+rng.IntN(3*capacity))
				h := uint64(1 + rng.IntN(3))
				for i := range keys {
					if i > 0 {
						h = h*1000003 + uint64(1+rng.IntN(3))
					}
					keys[i] = h
				}
				s.AddRootChain(keys)
				m.addRootChain(keys)
				checkInvariants(t, s)
				if s.Len() != len(m.order) {
					t.Fatalf("op %d: Len %d, want %d", op, s.Len(), len(m.order))
				}
				for _, k := range m.order {
					if !s.Contains(k) {
						t.Fatalf("op %d: key %x missing; model holds it", op, k)
					}
				}
			}
		})
	}
}

// Classes follow their keys through stores, removals, swap-delete moves,
// rebuilds, and capacity eviction; counts and UniformClass match a model.
func TestClassesMatchModel(t *testing.T) {
	for seed := range uint64(100) {
		rng := rand.New(rand.NewPCG(seed, 0x53))
		s := NewChainSet[uint64]([]int{0, 16, 64}[seed%3], nil)
		model := map[uint64]uint32{}
		var chains [][]uint64
		for op := range 400 {
			c := makeChain(rng.Uint64N(10), 1+rng.IntN(20))
			switch r := rng.IntN(10); {
			case r < 3:
				s.AddRootChain(c)
				chains = append(chains, c)
			case r < 5 && len(chains) > 0:
				p := chains[rng.IntN(len(chains))]
				if i := rng.IntN(len(p)); i > 0 {
					s.AddChainUnder(p[i-1], p[i:])
				}
			case r < 7 && len(chains) > 0:
				p := chains[rng.IntN(len(chains))]
				s.Remove(p[rng.IntN(len(p))])
			default:
				if len(chains) == 0 {
					continue
				}
				p := chains[rng.IntN(len(chains))]
				k, cls := p[rng.IntN(len(p))], uint32(rng.IntN(5))
				if s.SetClass(k, cls) != s.Contains(k) {
					t.Fatalf("seed %d op %d: SetClass result disagrees with residency", seed, op)
				}
				if s.Contains(k) {
					model[k] = cls
				}
			}
			checkInvariants(t, s)
			for k := range model {
				if !s.Contains(k) {
					delete(model, k)
				}
			}
			distinct := map[uint32]bool{}
			for _, b := range s.blocks {
				want, labeled := model[b.key]
				if !labeled {
					want = 0 // stored since the last check: new keys carry class 0
					model[b.key] = 0
				}
				if got, ok := s.Class(b.key); !ok || got != want {
					t.Fatalf("seed %d op %d: Class(%x) = (%d, %v), want %d", seed, op, b.key, got, ok, want)
				}
				distinct[want] = true
			}
			u, ok := s.UniformClass()
			if ok != (len(distinct) == 1) || (ok && !distinct[u]) {
				t.Fatalf("seed %d op %d: UniformClass = (%d, %v), classes %v", seed, op, u, ok, distinct)
			}
		}
	}
}

// Shrinking the capacity evicts oldest leaves until the bound holds; zero
// removes the bound.
func TestSetCapacity(t *testing.T) {
	s := NewChainSet[uint64](0, nil)
	s.AddRootChain([]uint64{1, 2, 3})    // stamp 1
	s.AddRootChain([]uint64{11, 12, 13}) // stamp 2
	s.SetCapacity(4)
	checkInvariants(t, s)
	if s.Len() != 4 || s.Contains(2) || s.Contains(3) || !s.Contains(13) {
		t.Fatalf("after shrinking to 4: Len %d, contains 2=%v 3=%v 13=%v",
			s.Len(), s.Contains(2), s.Contains(3), s.Contains(13))
	}
	s.SetCapacity(0)
	s.AddRootChain([]uint64{21, 22, 23, 24})
	if s.Len() != 8 {
		t.Fatalf("unbounded set holds %d, want 8", s.Len())
	}

	idx := NewIndex[uint64](Config{Endpoints: 2})
	idx.AddRootChain(1, []uint64{1, 2, 3, 4})
	idx.SetEndpointCapacity(1, 2)
	if got := idx.Match([]uint64{1, 2, 3, 4}, []int{1}); got[0] != 2 {
		t.Fatalf("Match after SetEndpointCapacity = %v, want [2]", got)
	}
}

// BenchmarkIndexSharedRootParallel queries one prompt whose root 19 of 128
// endpoints hold, from every core: the shape where all queries contend on one
// root shard and the same endpoints' locks.
func BenchmarkIndexSharedRootParallel(b *testing.B) {
	const endpoints, holders = 128, 19
	idx := NewIndex[uint64](Config{Endpoints: endpoints})
	chain := makeChain(7, 320)
	for ep := range holders {
		idx.AddRootChain(ep*6, chain[:256+ep])
	}
	candidates := make([]int, endpoints)
	for i := range candidates {
		candidates[i] = i
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		out := make([]int, endpoints)
		for pb.Next() {
			idx.MatchInto(chain, candidates, out)
		}
	})
}

// Writes to ordinals beyond the initial table grow it, concurrently with
// queries; ordinals outside the growth bound are ignored.
func TestIndexGrowsEndpoints(t *testing.T) {
	idx := NewIndex[uint64](Config{Endpoints: 2})
	chain := makeChain(3, 16)
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for ep := g; ep < 600; ep += 4 {
				idx.AddRootChain(ep, chain[:1+ep%16])
				idx.Match(chain, []int{ep, ep + 1, ep / 2})
			}
		}(g)
	}
	wg.Wait()
	if n := len(idx.slots()); n < 600 {
		t.Fatalf("table has %d slots after writes to ordinal 599", n)
	}
	got := idx.Match(chain, []int{0, 17, 300, 599, 100000, -1})
	want := []int{1, 2, 13, 8, 0, 0}
	if !slices.Equal(got, want) {
		t.Fatalf("Match = %v, want %v", got, want)
	}
	idx.AddRootChain(maxEndpointOrdinals, chain)
	idx.AddRootChain(-1, chain)
	if n := len(idx.slots()); n >= maxEndpointOrdinals {
		t.Fatalf("an out-of-bound write grew the table to %d", n)
	}
	if idx.Len(599) != 8 || idx.Len(100000) != 0 {
		t.Fatalf("Len(599) = %d, Len(100000) = %d; want 8 and 0", idx.Len(599), idx.Len(100000))
	}
	if st := idx.Stats(); st.HeldBlocks == 0 {
		t.Fatal("Stats misses the grown endpoints")
	}
}
