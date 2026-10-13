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

package atomictable

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
)

type pair struct{ val, meta uint64 }

// check compares t with the model and verifies the probe structure: every
// entry is reachable from its home without crossing an empty slot.
func check(t *testing.T, tab *Table, model map[uint64]pair) {
	t.Helper()
	if tab.Len() != len(model) {
		t.Fatalf("Len %d, want %d", tab.Len(), len(model))
	}
	for k, want := range model {
		val, meta, ok := tab.Load(k)
		if !ok || val != want.val || meta != want.meta {
			t.Fatalf("Load(%x) = (%x, %x, %v), want (%x, %x)", k, val, meta, ok, want.val, want.meta)
		}
	}
	seen := 0
	tab.Range(func(k, val, meta uint64) bool {
		seen++
		if w, ok := model[k]; !ok || w.val != val || w.meta != meta {
			t.Fatalf("Range yielded (%x, %x, %x), not in the model", k, val, meta)
		}
		return true
	})
	if seen != len(model) {
		t.Fatalf("Range saw %d entries, want %d", seen, len(model))
	}
}

func runModel(t *testing.T, rng *rand.Rand, ops int) {
	var tab Table
	model := map[uint64]pair{}
	keySpace := uint64(1 + rng.IntN(200))
	for range ops {
		k := rng.Uint64N(keySpace) // includes key 0
		switch rng.IntN(5) {
		case 0, 1, 2:
			p := pair{rng.Uint64(), rng.Uint64() &^ occupied}
			tab.Store(k, p.val, p.meta)
			model[k] = p
		case 3:
			_, had := model[k]
			if tab.Delete(k) != had {
				t.Fatalf("Delete(%x) disagreed with the model", k)
			}
			delete(model, k)
		default:
			if rng.IntN(20) == 0 {
				tab.Clear()
				clear(model)
			}
		}
		check(t, &tab, model)
	}
}

func TestTableMatchesMap(t *testing.T) {
	for seed := range uint64(200) {
		runModel(t, rand.New(rand.NewPCG(seed, 0x7a)), 500)
	}
}

func FuzzTable(f *testing.F) {
	f.Add(uint64(1), uint64(2))
	f.Fuzz(func(t *testing.T, a, b uint64) {
		runModel(t, rand.New(rand.NewPCG(a, b)), 200)
	})
}

// Readers racing an active writer terminate and never panic; once the writer
// stops, every Load is exact.
func TestTableConcurrentReaders(t *testing.T) {
	var tab Table
	model := map[uint64]pair{}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := range 4 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 1))
			for !stop.Load() {
				tab.Load(rng.Uint64N(512))
			}
		}(r)
	}
	rng := rand.New(rand.NewPCG(9, 9))
	for range 50000 {
		k := rng.Uint64N(512)
		if rng.IntN(3) == 0 {
			tab.Delete(k)
			delete(model, k)
		} else {
			p := pair{rng.Uint64(), rng.Uint64() &^ occupied}
			tab.Store(k, p.val, p.meta)
			model[k] = p
		}
	}
	stop.Store(true)
	wg.Wait()
	check(t, &tab, model)
}
