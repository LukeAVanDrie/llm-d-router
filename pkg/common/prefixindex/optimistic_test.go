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
	"sync"
	"sync/atomic"
	"testing"
)

// Every result tryRun accepts equals the result the read lock gives at the
// version it reports. The writer records, inside each write, the exact answer
// for every query at the version the write ends at; readers check each
// accepted result against that record.
func TestOptimisticReadsAreExact(t *testing.T) {
	idx := NewIndex[uint64](Config{Endpoints: 1, PerEndpointCapacity: 96})
	queries := make([][]uint64, 6)
	for i := range queries {
		queries[i] = makeChain(uint64(i%3), 40+10*i) // shared roots, varied lengths
	}
	es := idx.slot(0)

	var expected sync.Map // [2]uint64{version, query} -> run
	record := func(s *ChainSet[uint64], version uint64) {
		for qi, q := range queries {
			expected.Store([2]uint64{version, uint64(qi)}, s.RunLength(q, nil))
		}
	}
	idx.View(0, func(s *ChainSet[uint64]) { record(s, es.version.Load()) })

	var stop atomic.Bool
	var accepted, mismatched atomic.Int64
	var wg sync.WaitGroup
	for r := range 6 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 3))
			for !stop.Load() {
				qi := rng.IntN(len(queries))
				run, v, ok := es.tryRun(queries[qi], nil)
				if !ok {
					continue
				}
				accepted.Add(1)
				want, found := expected.Load([2]uint64{v, uint64(qi)})
				if !found || want.(int) != run {
					mismatched.Add(1)
				}
			}
		}(r)
	}

	rng := rand.New(rand.NewPCG(7, 7))
	for range 20000 {
		q := queries[rng.IntN(len(queries))]
		i := rng.IntN(len(q))
		idx.Update(0, func(s *ChainSet[uint64]) {
			switch rng.IntN(4) {
			case 0, 1:
				s.AddRootChain(q[:1+rng.IntN(len(q))])
			case 2:
				s.Remove(q[i]) // interior removals exercise rebuild
			default:
				if i > 0 {
					s.AddChainUnder(q[i-1], q[i:min(i+8, len(q))])
				}
			}
			// The write ends at the version after this odd one.
			record(s, es.version.Load()+1)
		})
	}
	stop.Store(true)
	wg.Wait()
	if accepted.Load() == 0 {
		t.Fatal("no optimistic read succeeded; the test exercised nothing")
	}
	if n := mismatched.Load(); n > 0 {
		t.Fatalf("%d of %d accepted optimistic reads differed from the locked answer", n, accepted.Load())
	}
	t.Logf("%d optimistic reads accepted, all exact", accepted.Load())
}
