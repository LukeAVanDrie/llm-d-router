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
	"testing"
)

// A resident key reached through a path whose previous key is not its parent
// is a miss: the run stops before it rather than counting a prefix the
// endpoint does not hold.
func TestRunLengthRejectsWrongParent(t *testing.T) {
	s := NewChainSet(0, nil)
	s.AddRootChain([]uint64{1, 2})
	s.AddChainUnder(1, []uint64{9}) // 9 is a child of 1, not of 2
	if got := s.RunLength([]uint64{1, 2, 9}, nil); got != 2 {
		t.Fatalf("RunLength([1 2 9]) = %d, want 2", got)
	}
	if got := s.RunLength([]uint64{1, 9}, nil); got != 2 {
		t.Fatalf("RunLength([1 9]) = %d, want 2", got)
	}

	idx := NewIndex(Config{Endpoints: 2})
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
	s := NewChainSet(6, nil)
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
		s := NewChainSet(0, nil)
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

func bruteOldestLeaf(s *ChainSet) (uint64, bool) {
	best, found := block{}, false
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
			s := NewChainSet(capacity, nil)
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
