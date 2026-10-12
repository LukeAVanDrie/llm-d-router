package prefixindex

import (
	"math/rand/v2"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

// refBlock holds the authoritative lineage of a resident block in the reference oracle.
type refBlock struct {
	parentKey uint64
	isRoot    bool
}

// refSet is a minimal single-map reference oracle for differential testing.
type refSet map[uint64]refBlock

func (r refSet) addRootChain(keys []uint64) {
	r.addSegment(0, true, keys)
}

func (r refSet) addChainUnder(parentKey uint64, keys []uint64) {
	r.addSegment(parentKey, false, keys)
}

func (r refSet) addSegment(parentKey uint64, isRoot bool, keys []uint64) {
	for i, k := range keys {
		if i > 0 {
			parentKey, isRoot = keys[i-1], false
		}
		if _, exists := r[k]; exists {
			continue
		}
		if !isRoot && r.createsCycle(k, parentKey) {
			break
		}
		if isRoot {
			parentKey = 0
		}
		r[k] = refBlock{parentKey: parentKey, isRoot: isRoot}
	}
}

func (r refSet) createsCycle(k, parentKey uint64) bool {
	if parentKey == k {
		return true
	}
	for cur := parentKey; ; {
		b, ok := r[cur]
		if !ok || b.isRoot {
			return false
		}
		if b.parentKey == k {
			return true
		}
		cur = b.parentKey
	}
}

func (r refSet) remove(key uint64) {
	delete(r, key)
}

func (r refSet) runLength(keys []uint64) int {
	for i, k := range keys {
		b, ok := r[k]
		if !ok {
			return i
		}
		if i == 0 {
			if !b.isRoot {
				return 0
			}
		} else if b.isRoot || b.parentKey != keys[i-1] {
			return i
		}
	}
	return len(keys)
}

// checkInvariants verifies that all derived projections (byKey, children, vis,
// dormantCount, waitingOn, and leaves heap) match a fresh recomputation from
// the authoritative facts (key, parentKey, isRoot, stamp) of s.blocks, and
// enforces the Forest Leaf Invariant (zero cycles; len(blocks) > 0 => len(leaves) > 0).
func checkInvariants(t *testing.T, s *ChainSet) {
	t.Helper()
	if len(s.blocks) != len(s.byKey) {
		t.Fatalf("len(blocks)=%d != len(byKey)=%d", len(s.blocks), len(s.byKey))
	}
	if s.Len() != len(s.blocks) {
		t.Fatalf("Len()=%d, want %d", s.Len(), len(s.blocks))
	}

	wantChildren := make([]uint32, len(s.blocks))
	wantWaiting := make(map[uint64]int32)
	for i, b := range s.blocks {
		if idx, ok := s.byKey[b.key]; !ok || int(idx) != i || !s.Contains(b.key) {
			t.Fatalf("block[%d] key=%x maps to (%d, %v) in byKey", i, b.key, idx, ok)
		}
		if !b.isRoot {
			if b.parentKey == b.key {
				t.Fatalf("block[%d] key=%x has self-loop parentKey", i, b.key)
			}
			if pIdx, ok := s.byKey[b.parentKey]; ok {
				wantChildren[pIdx]++
			} else {
				wantWaiting[b.parentKey]++
			}
		}
	}
	if len(s.waitingOn) != len(wantWaiting) {
		t.Fatalf("len(waitingOn)=%d, want %d", len(s.waitingOn), len(wantWaiting))
	}
	for k, wantCnt := range wantWaiting {
		if gotCnt := s.waitingOn[k]; gotCnt != wantCnt {
			t.Fatalf("waitingOn[%x]=%d, want %d", k, gotCnt, wantCnt)
		}
	}

	var computeVis func(idx int, depth int) uint8
	computeVis = func(idx int, depth int) uint8 {
		if depth > len(s.blocks) {
			t.Fatalf("cycle detected in resident parent graph starting from block[%d] key=%x", idx, s.blocks[idx].key)
		}
		b := s.blocks[idx]
		if b.isRoot {
			return visVisible
		}
		pIdx, ok := s.byKey[b.parentKey]
		if !ok {
			return visDormant
		}
		return computeVis(int(pIdx), depth+1)
	}

	wantDormant := 0
	wantLeaves := 0
	for i, b := range s.blocks {
		if b.children != wantChildren[i] {
			t.Fatalf("block[%d] children=%d, want %d", i, b.children, wantChildren[i])
		}
		wantVis := computeVis(i, 0)
		if b.vis != wantVis {
			t.Fatalf("block[%d] vis=%d, want %d", i, b.vis, wantVis)
		}
		if b.vis == visDormant {
			wantDormant++
		}
		if b.children == 0 {
			wantLeaves++
			if b.heapIdx < 0 || int(b.heapIdx) >= len(s.leaves) || s.leaves[b.heapIdx] != uint32(i) {
				t.Fatalf("leaf block[%d] has invalid heapIdx=%d", i, b.heapIdx)
			}
		} else if b.heapIdx != -1 {
			t.Fatalf("non-leaf block[%d] (children=%d) has heapIdx=%d, want -1", i, b.children, b.heapIdx)
		}
	}

	// Forest Leaf Invariant: every non-empty finite forest has at least one leaf.
	if len(s.blocks) > 0 && len(s.leaves) == 0 {
		t.Fatalf("forest leaf invariant violated: len(blocks)=%d > 0 but len(leaves)=0", len(s.blocks))
	}
	if s.dormantCount != wantDormant {
		t.Fatalf("dormantCount=%d, want %d", s.dormantCount, wantDormant)
	}
	if len(s.leaves) != wantLeaves {
		t.Fatalf("len(leaves)=%d, want %d", len(s.leaves), wantLeaves)
	}
	for i := 1; i < len(s.leaves); i++ {
		p := (i - 1) / 2
		if s.lessLeaf(s.leaves[i], s.leaves[p]) {
			t.Fatalf("heap order violated at %d (parent %d)", i, p)
		}
	}
}

func makeChain(seed uint64, length int) []uint64 {
	out := make([]uint64, length)
	h := (seed + 1) * 0x9e3779b97f4a7c15
	for i := range length {
		h ^= uint64(i+1) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		out[i] = h
	}
	return out
}

func TestZeroPointersInBlockAndEndpointSlotAlignment(t *testing.T) {
	typ := reflect.TypeFor[block]()
	if typ.Size() != 40 {
		t.Fatalf("sizeof(block) = %d, want 40", typ.Size())
	}
	if slotSize := reflect.TypeFor[endpointSlot]().Size(); slotSize != 64 {
		t.Fatalf("sizeof(endpointSlot) = %d, want 64", slotSize)
	}
	for i := range typ.NumField() {
		f := typ.Field(i)
		switch f.Type.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.String, reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
			t.Fatalf("block field %s contains a GC-scannable pointer type %v", f.Name, f.Type)
		}
	}
}

func TestZeroValueChainSetAndConfigBounds(t *testing.T) {
	var s ChainSet
	if s.Len() != 0 || s.Contains(42) || s.RunLength([]uint64{42}, nil) != 0 {
		t.Fatalf("unexpected non-empty state on zero-value ChainSet")
	}
	if s.Remove(42) {
		t.Fatalf("unexpected mutation on empty zero-value ChainSet")
	}
	s.AddRootChain([]uint64{10, 20, 30})
	checkInvariants(t, &s)
	if got := s.RunLength([]uint64{10, 20, 30}, nil); got != 3 {
		t.Fatalf("zero-value ChainSet RunLength = %d, want 3", got)
	}

	// Verify Config.Endpoints default and >MaxEndpoints overflow probing path.
	idxDefault := NewIndex(Config{Endpoints: 0})
	if len(idxDefault.endpoints) != 128 {
		t.Fatalf("Endpoints=0 produced %d slots, want 128", len(idxDefault.endpoints))
	}
	idxOverflow := NewIndex(Config{Endpoints: MaxEndpoints + 44})
	if len(idxOverflow.endpoints) != MaxEndpoints+44 {
		t.Fatalf("Endpoints=%d produced %d slots, want %d", MaxEndpoints+44, len(idxOverflow.endpoints), MaxEndpoints+44)
	}
	ch := []uint64{100, 101, 102, 103}
	idxOverflow.AddRootChain(5, ch[:2])
	idxOverflow.AddRootChain(280, ch[:4])
	got := idxOverflow.Match(ch, []int{5, 10, 280, 290})
	if !reflect.DeepEqual(got, []int{2, 0, 4, 0}) {
		t.Fatalf("overflow Match = %v, want [2 0 4 0]", got)
	}
	// Verify when only an overflow endpoint (>= 256) holds k_0 (holders bitmask is empty).
	idxOverflow.ClearEndpoint(5)
	gotAfterClear := idxOverflow.Match(ch, []int{5, 280})
	if !reflect.DeepEqual(gotAfterClear, []int{0, 4}) {
		t.Fatalf("overflow Match after clearing <256 holder = %v, want [0 4]", gotAfterClear)
	}
}

func TestAdversarialCyclesSelfLoopsAndParentLeafStampOrder(t *testing.T) {
	// 1. Self-loop on empty set must be rejected and never create an un-evictable node.
	s := NewChainSet(2, nil)
	s.AddChainUnder(50, []uint64{50})
	checkInvariants(t, s)
	if s.Len() != 0 {
		t.Fatalf("self-loop was inserted: Len()=%d, want 0", s.Len())
	}

	// 2. 2-cycle (10 <-> 20) via out-of-order AddChainUnder must be rejected.
	s.AddChainUnder(20, []uint64{10})
	s.AddChainUnder(10, []uint64{20})
	checkInvariants(t, s)
	if s.Len() != 1 || !s.Contains(10) || s.Contains(20) {
		t.Fatalf("2-cycle was not rejected: Len()=%d", s.Len())
	}

	// 3. Multi-hop cycle (30 -> 20 -> 10 -> 30) and capacity eviction progress.
	s.Clear()
	s.AddChainUnder(20, []uint64{10})
	s.AddChainUnder(30, []uint64{20})
	s.AddChainUnder(10, []uint64{30}) // would close 3-cycle 10 -> 20 -> 30 -> 10
	checkInvariants(t, s)
	if s.Len() > 2 {
		t.Fatalf("capacity bound exceeded due to wedged cycle: Len()=%d, cap=2", s.Len())
	}

	// 4. Duplicate key in a single chain [R, A, B, A] preserves A's original parent R.
	s2 := NewChainSet(0, nil)
	s2.AddRootChain([]uint64{1, 2, 3, 2})
	checkInvariants(t, s2)
	if got := s2.RunLength([]uint64{1, 2, 3}, nil); got != 3 {
		t.Fatalf("duplicate key corrupted chain: RunLength=%d, want 3", got)
	}

	// 5. Updating a leaf parentKey's stamp when keys[0] is already a resident root
	// must maintain min-heap ordering in s.leaves.
	s3 := NewChainSet(0, nil)
	s3.AddRootChain([]uint64{40})
	s3.AddRootChain([]uint64{50})
	s3.AddRootChain([]uint64{60})
	s3.AddRootChain([]uint64{70})
	s3.AddChainUnder(50, []uint64{40}) // 50 is a leaf; 40 is already a root
	checkInvariants(t, s3)
}

func TestDifferentialLeafFirstAndHeadFirstCascades(t *testing.T) {
	// Stage 1 ("Always-Rebuild" baseline) vs. Stage 2 (Incremental fast-path) vs. Reference Oracle.
	sFast := NewChainSet(0, nil)
	sRebuild := NewChainSet(0, nil)
	ref := make(refSet)
	rng := rand.New(rand.NewPCG(42, 99))

	chains := make([][]uint64, 16)
	for c := range chains {
		base := makeChain(uint64(c/4), 16)
		tail := makeChain(uint64(100+c), 32)
		chains[c] = append(base, tail...)
	}

	for step := range 2000 {
		ch := chains[rng.IntN(len(chains))]
		op := rng.IntN(4)
		switch op {
		case 0, 1:
			start := rng.IntN(len(ch) / 2)
			end := start + 1 + rng.IntN(len(ch)-start)
			if start == 0 {
				sFast.AddRootChain(ch[:end])
				sRebuild.AddRootChain(ch[:end])
				ref.addRootChain(ch[:end])
			} else {
				sFast.AddChainUnder(ch[start-1], ch[start:end])
				sRebuild.AddChainUnder(ch[start-1], ch[start:end])
				ref.addChainUnder(ch[start-1], ch[start:end])
			}
		case 2:
			pos := len(ch)/2 + rng.IntN(len(ch)/2)
			sFast.Remove(ch[pos])
			sRebuild.Remove(ch[pos])
			ref.remove(ch[pos])
		case 3:
			pos := rng.IntN(len(ch) / 2)
			sFast.Remove(ch[pos])
			sRebuild.Remove(ch[pos])
			ref.remove(ch[pos])
		}
		sRebuild.rebuild() // Stage 1 baseline: unconditionally recompute all derived state

		checkInvariants(t, sFast)
		if sFast.Len() != sRebuild.Len() || sFast.DormantCount() != sRebuild.DormantCount() {
			t.Fatalf("step=%d: fast-path (held=%d dormant=%d) != always-rebuild (held=%d dormant=%d)",
				step, sFast.Len(), sFast.DormantCount(), sRebuild.Len(), sRebuild.DormantCount())
		}
		for cIdx, q := range chains {
			got := sFast.RunLength(q, nil)
			gotRebuild := sRebuild.RunLength(q, nil)
			want := ref.runLength(q)
			if got != want || got != gotRebuild {
				t.Fatalf("step=%d chain=%d: got=%d rebuild=%d want=%d",
					step, cIdx, got, gotRebuild, want)
			}
		}
	}
}

func TestHeadFirstEvictionAndImmutableResidentLineage(t *testing.T) {
	s := NewChainSet(0, nil)
	ch := makeChain(7, 20)
	s.AddRootChain(ch)
	checkInvariants(t, s)

	// 1. Immutable resident lineage: touching interior blocks via AddRootChain
	// refreshes recency without promoting ch[1] to a fake root.
	s.AddRootChain(ch[1:5])
	checkInvariants(t, s)
	if got := s.RunLength(ch[1:], nil); got != 0 {
		t.Fatalf("resident interior block ch[1] was promoted to root: got=%d, want 0", got)
	}

	// 2. Evicting root ch[0] transitions ch[1..19] to Dormant via 1 rebuild;
	// subsequent head-first removals of Dormant blocks ch[1..18] execute in O(1)
	// with 0 additional rebuilds.
	s.Remove(ch[0])
	checkInvariants(t, s)
	if s.Rebuilds() != 1 || s.RunLength(ch, nil) != 0 {
		t.Fatalf("after root removal: Rebuilds=%d RunLength=%d, want 1 and 0", s.Rebuilds(), s.RunLength(ch, nil))
	}
	for i := 1; i < len(ch); i++ {
		s.Remove(ch[i])
		checkInvariants(t, s)
		if s.Rebuilds() != 1 {
			t.Fatalf("dormant removal at i=%d triggered rebuild: Rebuilds=%d, want 1", i, s.Rebuilds())
		}
	}

	// 3. Out-of-order reverse insertion of Dormant segments triggers 0 rebuilds
	// until the root ch[0] arrives, which triggers 1 rebuild to wake the chain.
	s.Clear()
	baseRebuilds := s.Rebuilds()
	for i := len(ch) - 1; i >= 1; i-- {
		s.AddChainUnder(ch[i-1], ch[i:i+1])
		checkInvariants(t, s)
		if s.Rebuilds() != baseRebuilds {
			t.Fatalf("dormant insertion at i=%d triggered rebuild: got %d, want %d", i, s.Rebuilds(), baseRebuilds)
		}
	}
	s.AddRootChain(ch[:1])
	checkInvariants(t, s)
	if s.Rebuilds() != baseRebuilds+1 || s.RunLength(ch, nil) != len(ch) {
		t.Fatalf("after root arrival: Rebuilds=%d RunLength=%d, want %d and %d",
			s.Rebuilds(), s.RunLength(ch, nil), baseRebuilds+1, len(ch))
	}
}

func TestDormantEvictionZeroKeyAndHintBounds(t *testing.T) {
	// 1. Key == 0 works natively without nonZero sentinel remapping, and
	// restoring a missing root reconnects waiting dormant children.
	idx := NewIndex(Config{Endpoints: 4, PerEndpointCapacity: 4})
	ch := makeChain(99, 4)
	ch[0] = 0
	idx.AddChainUnder(0, ch[0], ch[1:2])
	if got := idx.Match(ch[:2], []int{0})[0]; got != 0 {
		t.Fatalf("dormant segment matched before root arrival: got=%d, want 0", got)
	}
	idx.AddRootChain(0, ch[:1])
	if got := idx.Match(ch[:2], []int{0})[0]; got != 2 {
		t.Fatalf("after storing missing root ch[0]=0, got=%d, want 2", got)
	}
	if st := idx.Stats(); st.HeldBlocks != 2 || st.DormantBlocks != 0 || st.RootKeys != 1 || st.Rebuilds != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}

	// 2. Dormant leaves (visDormant) are evicted before Visible leaves even if newer.
	s := NewChainSet(2, nil)
	s.AddRootChain(ch[:2])
	s.AddChainUnder(0xDEAD, ch[2:3])
	checkInvariants(t, s)
	if got := s.RunLength(ch[:2], nil); got != 2 {
		t.Fatalf("dormant leaf evicted visible chain: got RunLength=%d, want 2", got)
	}
	if s.Len() != 2 || s.DormantCount() != 0 {
		t.Fatalf("unexpected counts after dormant eviction: held=%d dormant=%d", s.Len(), s.DormantCount())
	}

	// 3. A second root key appearing at pos > 0 after a missing block never overcounts,
	// and multiple descending/ascending hints narrow [lo, hi] accurately.
	otherRoot := makeChain(123, 1)
	s2 := NewChainSet(0, nil)
	s2.AddRootChain(ch[:3])
	s2.AddRootChain(otherRoot)
	checkInvariants(t, s2)
	spliced := []uint64{ch[0], ch[1], 0x999999, otherRoot[0]}
	if got := s2.RunLength(spliced, []int{4, 3, 2, 1}); got != 2 {
		t.Fatalf("spliced root at pos=3 overcounted: got=%d, want 2", got)
	}
}

func TestFrontierLeafStampingMatchesWholePathLRU(t *testing.T) {
	const capBlocks = 64
	s := NewChainSet(capBlocks, nil)

	var order []uint64
	inCache := make(map[uint64]bool)
	touchRef := func(k uint64) {
		if inCache[k] {
			for i, v := range order {
				if v == k {
					order = append(order[:i], order[i+1:]...)
					break
				}
			}
		} else {
			inCache[k] = true
		}
		order = append(order, k)
	}

	chains := make([][]uint64, 24)
	for i := range chains {
		shared := makeChain(uint64(i/3), 4)
		branch := makeChain(uint64(1000+i), 8)
		chains[i] = append(shared, branch...)
	}

	rng := rand.New(rand.NewPCG(7, 13))
	for step := range 1000 {
		ch := chains[rng.IntN(len(chains))]
		subLen := 1 + rng.IntN(len(ch))
		prefix := ch[:subLen]

		s.AddRootChain(prefix)
		checkInvariants(t, s)

		for i := len(prefix) - 1; i >= 0; i-- {
			touchRef(prefix[i])
		}
		for len(order) > capBlocks {
			victim := order[0]
			order = order[1:]
			delete(inCache, victim)
		}

		for cIdx, q := range chains {
			got := s.RunLength(q, nil)
			want := 0
			for _, k := range q {
				if !inCache[k] {
					break
				}
				want++
			}
			if got != want {
				t.Fatalf("step=%d chain=%d: frontier leaf-LRU got=%d, whole-path LRU want=%d",
					step, cIdx, got, want)
			}
		}
	}
}

func TestZeroAllocsOnSteadyStateQueries(t *testing.T) {
	const endpoints = 16
	idx := NewIndex(Config{Endpoints: endpoints, PerEndpointCapacity: 256})
	ch := makeChain(1, 64)
	idx.AddRootChain(0, ch[:32])
	idx.AddRootChain(3, ch[:64])

	candidates := make([]int, endpoints)
	for i := range candidates {
		candidates[i] = i
	}
	out := make([]int, endpoints)

	allocs := testing.AllocsPerRun(100, func() {
		res := idx.MatchInto(ch, candidates, out)
		if res[0] != 32 || res[3] != 64 {
			t.Fatalf("unexpected match results: %v", res)
		}
	})
	if allocs != 0 {
		t.Fatalf("MatchInto allocated %v times per run, want 0", allocs)
	}
}

func TestIndexConcurrentRaceAndThresholdSharing(t *testing.T) {
	const endpoints = 32
	idx := NewIndex(Config{Endpoints: endpoints, PerEndpointCapacity: 512})
	candidates := make([]int, endpoints)
	for i := range candidates {
		candidates[i] = i
	}

	chains := make([][]uint64, 16)
	for i := range chains {
		chains[i] = makeChain(uint64(i), 64)
	}

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(worker+1), 77))
			for range 500 {
				ep := rng.IntN(endpoints)
				ch := chains[rng.IntN(len(chains))]
				switch rng.IntN(4) {
				case 0, 1:
					idx.AddRootChain(ep, ch[:1+rng.IntN(len(ch))])
				case 2:
					idx.Remove(ep, ch[rng.IntN(len(ch))])
				case 3:
					if rng.IntN(16) == 0 {
						idx.ClearEndpoint(ep)
					}
				}
			}
		}(w)
	}

	for r := range 8 {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			out := make([]int, endpoints)
			for i := range 500 {
				ch := chains[(reader+i)%len(chains)]
				res := idx.MatchInto(ch, candidates, out)
				for _, d := range res {
					if d < 0 || d > len(ch) {
						t.Errorf("invalid match depth %d", d)
					}
				}
			}
		}(r)
	}
	wg.Wait()
}

func FuzzChainSet(f *testing.F) {
	f.Add([]byte{0, 0, 4, 1, 1, 3, 2, 2, 3, 0, 0, 2, 5, 10, 20, 5, 20, 10})
	f.Fuzz(func(t *testing.T, ops []byte) {
		const capBlocks = 16
		s := NewChainSet(capBlocks, nil)
		chains := [4][]uint64{
			{10, 11, 12, 13, 14, 15},
			{10, 11, 22, 23, 24, 25},
			{30, 31, 32, 33, 34, 35},
			{30, 41, 42, 43, 44, 45},
		}
		for i := 0; i+2 < len(ops); i += 3 {
			ch := chains[ops[i]%4]
			arg := int(ops[i+2]) % len(ch)
			switch ops[i+1] % 5 {
			case 0:
				s.AddRootChain(ch[:arg+1])
			case 1:
				if arg == 0 {
					s.AddRootChain(ch[:1])
				} else {
					s.AddChainUnder(ch[arg-1], ch[arg:])
				}
			case 2:
				s.Remove(ch[arg])
			case 3:
				if arg == 0 {
					s.Clear()
				}
			case 4:
				// Adversarial arbitrary (parentKey, key) pair from raw fuzz bytes,
				// exercising self-loops, 2-cycles, and multi-hop cycle rejection.
				pKey := uint64(ops[i] % 16)
				cKey := uint64(ops[i+2] % 16)
				s.AddChainUnder(pKey, []uint64{cKey})
			}
			checkInvariants(t, s)
			if s.Len() > capBlocks {
				t.Fatalf("capacity exceeded: Len()=%d > cap=%d", s.Len(), capBlocks)
			}
		}
	})
}

func BenchmarkChainSetRunLengthByPrefixLen(b *testing.B) {
	s := NewChainSet(16384, nil)
	full := makeChain(1, 512)
	s.AddRootChain(full)

	for _, tc := range []struct {
		name   string
		qLen   int
		hitLen int
	}{
		{"L4_Hit3", 4, 3},
		{"L16_Hit12", 16, 12},
		{"L64_Hit48", 64, 48},
		{"L320_Hit256", 320, 256},
	} {
		q := append([]uint64(nil), full[:tc.hitLen]...)
		q = append(q, makeChain(999, tc.qLen-tc.hitLen)...)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := s.RunLength(q, nil); got != tc.hitLen {
					b.Fatalf("got %d, want %d", got, tc.hitLen)
				}
			}
		})
	}
}

func BenchmarkChainSetWriteAndEvict(b *testing.B) {
	chains := make([][]uint64, 128)
	for i := range chains {
		chains[i] = makeChain(uint64(i), 32)
	}
	b.Run("LeafFirst_AddRootChain_Cap2048", func(b *testing.B) {
		s := NewChainSet(2048, nil)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			s.AddRootChain(chains[i&127])
			i++
		}
	})
}

func BenchmarkChainSetRebuildByEndpointSize(b *testing.B) {
	for _, tc := range []struct {
		name   string
		blocks int
	}{
		{"N4096", 4096},
		{"N16384", 16384},
		{"N65536", 65536},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const chainLen = 64
			s := NewChainSet(tc.blocks, nil)
			numChains := tc.blocks / chainLen
			for c := range numChains {
				s.AddRootChain(makeChain(uint64(c+1), chainLen))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s.rebuild()
			}
		})
	}
}

func BenchmarkIndexQuery128Endpoints(b *testing.B) {
	const (
		endpoints = 128
		chainLen  = 320
		holders   = 20
	)
	shared := makeChain(1, 64)
	query := append(append([]uint64(nil), shared...), makeChain(2, chainLen-64)...)
	candidates := make([]int, endpoints)
	for i := range candidates {
		candidates[i] = i
	}

	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"Full_RootFilter_ThresholdSharing", Config{Endpoints: endpoints, PerEndpointCapacity: 16384}},
		{"NoThresholdSharing", Config{Endpoints: endpoints, PerEndpointCapacity: 16384, DisableThresholdSharing: true}},
		{"Naive_NoRootFilter_NoSharing", Config{Endpoints: endpoints, PerEndpointCapacity: 16384, DisableRootFilter: true, DisableThresholdSharing: true}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			idx := NewIndex(tc.cfg)
			for ep := range holders {
				depth := 256
				if ep%5 == 0 {
					depth = 128
				}
				idx.AddRootChain(ep, query[:depth])
			}
			out := make([]int, endpoints)
			b.ReportAllocs()
			for b.Loop() {
				idx.MatchInto(query, candidates, out)
			}
		})
	}
}

func BenchmarkIndexConcurrentReadWrite(b *testing.B) {
	const endpoints = 128
	idx := NewIndex(Config{Endpoints: endpoints, PerEndpointCapacity: 4096})
	chains := make([][]uint64, 32)
	for i := range chains {
		chains[i] = makeChain(uint64(i), 128)
	}
	for ep := range 32 {
		idx.AddRootChain(ep, chains[ep])
	}
	candidates := make([]int, endpoints)
	for i := range candidates {
		candidates[i] = i
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		out := make([]int, endpoints)
		step := 0
		for pb.Next() {
			step++
			if step&7 == 0 {
				idx.AddRootChain(step&(endpoints-1), chains[step&31])
			} else {
				idx.MatchInto(chains[step&31], candidates, out)
			}
		}
	})
}

func BenchmarkRetainedGCHeap(b *testing.B) {
	const (
		endpoints         = 128
		blocksPerEndpoint = 4096
		blocksPerChain    = 64
		totalBlocks       = endpoints * blocksPerEndpoint
	)
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	idx := NewIndex(Config{Endpoints: endpoints, PerEndpointCapacity: blocksPerEndpoint})
	for ep := range endpoints {
		for c := range blocksPerEndpoint / blocksPerChain {
			ch := makeChain(uint64(ep*1000+c), blocksPerChain)
			idx.AddRootChain(ep, ch)
		}
	}

	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(idx)

	heapBytes := float64(after.HeapInuse - before.HeapInuse)
	b.ReportMetric(heapBytes/(1024*1024), "MB_heap_inuse")
	b.ReportMetric(heapBytes/float64(totalBlocks), "B_per_block")
	b.ReportMetric(float64(after.HeapObjects-before.HeapObjects), "heap_objects")
}
