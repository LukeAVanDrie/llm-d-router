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

// Package prefixindex implements a pointer-free prefix residency set ([ChainSet])
// and a concurrent multi-endpoint router index ([Index]) for LLM request routing.
//
// # Overview
//
// LLM inference engines cache KV blocks addressed by chained 64-bit hashes:
// block key k_i commits to the token content of block i together with its
// predecessor key k_{i-1}. When serving a prompt with block keys
// k_0, k_1, ..., k_{L-1}, an engine reuses cached blocks only up to the first
// missing block; resident blocks sitting behind an evicted ancestor cannot be
// served until that gap is filled.
//
// This package serves both routing modes used in LLM routers:
//
//   - Approximate routing: the router optimistically records the routed
//     prompt's full chain via [Index.AddRootChain] and relies on per-endpoint
//     capacity eviction to model engine cache replacement.
//   - Precise routing: the router ingests engine KV-cache events via
//     [Index.AddRootChain], [Index.AddChainUnder], [Index.Remove], and
//     [Index.ClearEndpoint].
//
// # State Machine
//
// On any single endpoint, every block key in the universe is in one of three
// states:
//
//   - Absent: not resident on the endpoint (not stored in memory).
//   - Visible: resident on the endpoint and connected to a resident depth-0
//     root block by a contiguous chain of resident ancestors.
//   - Dormant: resident on the endpoint, but waiting on at least one Absent
//     ancestor along its chain to root.
//
// Only resident blocks (Visible or Dormant) are stored in a [ChainSet]. Each
// 40-byte block record contains zero pointers (allocating in Go's noscan heap)
// and separates authoritative facts (key, parentKey, stamp, isRoot) from
// derived projections (children, vis, heapIdx). On leaf insertions, leaf
// removals, and Dormant-only mutations, derived projections update incrementally
// in O(1) / O(log N) time. When a Visible interior block is removed or a
// gap-filling Visible block wakes waiting descendants, [ChainSet] recomputes
// derived projections from the authoritative facts in a single O(N) linear pass.
//
// # Contracts
//
//   - Queries are read-only: [ChainSet.RunLength], [Index.Match] and
//     [Index.MatchInto] never change recency. On the approximate path the
//     caller records the routed prompt with [Index.AddRootChain] after
//     choosing an endpoint.
//   - Remove has set semantics: it removes the block whatever its copy count,
//     and any Visible descendants become Dormant. Callers that receive one
//     event per physical copy deduplicate before calling Remove.
//
// # Concurrency and Zero Value
//
// The zero value of [ChainSet] is an empty set ready for use. A [ChainSet]
// represents a single endpoint and is not safe for concurrent use without
// external synchronization. Use [Index] for concurrent multi-endpoint routing.
package prefixindex

import "cmp"

const (
	visUnvisited uint8 = 0
	visDormant   uint8 = 1
	visVisible   uint8 = 2
)

// block holds the authoritative lineage and recency of a resident KV block
// alongside its derived closure and leaf-heap state (40 bytes, zero pointers).
type block[K ~uint64] struct {
	// Authoritative facts (established on insertion; deleted on Remove).
	key       K
	parentKey K // 0 when isRoot is true; stored by hash so swap-delete never invalidates parent links
	stamp     uint64

	// class is the caller's label for the block; see [ChainSet.SetClass].
	class uint32

	// Derived projections (deterministic function of the resident block set).
	heapIdx  int32  // index in leaves min-heap, or -1 when children > 0
	children uint32 // resident child count
	isRoot   bool   // true iff depth-0 root block
	vis      uint8  // visDormant (1) or visVisible (2)
}

// RootCallback is invoked when a depth-0 root key's residency changes in a [ChainSet].
type RootCallback[K ~uint64] func(rootKey K, held bool)

// ChainSet tracks the prefix blocks resident on a single endpoint.
//
// The zero value of ChainSet is an empty set with unlimited capacity.
// A ChainSet is not safe for concurrent use by multiple goroutines without
// external synchronization; use [Index] for concurrent multi-endpoint routing.
type ChainSet[K ~uint64] struct {
	byKey     map[K]uint32 // key -> index in blocks (0 .. len(blocks)-1)
	blocks    []block[K]   // dense slice of resident blocks
	leaves    []uint32     // min-heap of indices into blocks with children == 0
	waitingOn map[K]int32  // absent parentKey -> count of resident children waiting on it

	// classCount counts resident blocks per class; inUse lists the classes
	// with a nonzero count. Classes are small integers, so counts index a slice.
	classCount []int32
	inUse      []uint32

	seq          uint64
	dormantCount int
	evictions    uint64
	rebuilds     uint64
	capacity     int
	onRoot       RootCallback[K]
}

// NewChainSet returns a new [ChainSet] with an optional block capacity
// (0 means unlimited) and an optional [RootCallback] notified when depth-0
// root keys are added or removed.
func NewChainSet[K ~uint64](capacity int, onRoot RootCallback[K]) *ChainSet[K] {
	s := &ChainSet[K]{
		capacity: capacity,
		onRoot:   onRoot,
	}
	s.ensureInit()
	return s
}

func (s *ChainSet[K]) ensureInit() {
	if s.byKey != nil {
		return
	}
	initCap := 256
	if s.capacity > 0 {
		initCap = min(s.capacity, initCap)
	}
	s.byKey = make(map[K]uint32, initCap)
	s.blocks = make([]block[K], 0, initCap)
	s.waitingOn = make(map[K]int32)
}

// Clear removes all resident blocks from s while retaining allocated backing
// storage. If a [RootCallback] is configured, it is invoked with held=false for
// every resident root block.
func (s *ChainSet[K]) Clear() {
	if s.onRoot != nil {
		for i := range s.blocks {
			if s.blocks[i].isRoot {
				s.onRoot(s.blocks[i].key, false)
			}
		}
	}
	if s.byKey == nil {
		s.ensureInit()
	} else {
		clear(s.byKey)
		clear(s.blocks)
		s.blocks = s.blocks[:0]
		clear(s.waitingOn)
	}
	s.leaves = s.leaves[:0]
	s.dormantCount = 0
	clear(s.classCount)
	s.inUse = s.inUse[:0]
}

// Len returns the number of resident blocks in s (Visible plus Dormant).
func (s *ChainSet[K]) Len() int { return len(s.blocks) }

// DormantCount returns the number of resident blocks currently waiting on an
// absent ancestor. Under leaf-first eviction and in-order stores, DormantCount is 0.
func (s *ChainSet[K]) DormantCount() int { return s.dormantCount }

// Evictions returns the cumulative number of capacity evictions performed on s.
func (s *ChainSet[K]) Evictions() uint64 { return s.evictions }

// Rebuilds returns the cumulative number of out-of-order closure rebuilds on s.
func (s *ChainSet[K]) Rebuilds() uint64 { return s.rebuilds }

// Class returns the class of a resident key. New keys have class 0.
func (s *ChainSet[K]) Class(key K) (uint32, bool) {
	idx, ok := s.byKey[key]
	if !ok {
		return 0, false
	}
	return s.blocks[idx].class, true
}

// SetClass labels a resident key with class and reports whether the key is
// resident. Classes are opaque to s: callers use them to attach metadata,
// such as an interned set of tiers, to blocks. Classes should be small
// integers, since per-class counts index a slice.
func (s *ChainSet[K]) SetClass(key K, class uint32) bool {
	idx, ok := s.byKey[key]
	if !ok {
		return false
	}
	b := &s.blocks[idx]
	if b.class != class {
		s.countClass(b.class, -1)
		s.countClass(class, 1)
		b.class = class
	}
	return true
}

// UniformClass returns the class every resident block carries, when there is
// exactly one.
func (s *ChainSet[K]) UniformClass() (uint32, bool) {
	if len(s.inUse) != 1 {
		return 0, false
	}
	return s.inUse[0], true
}

// countClass adds delta to class c's count, keeping inUse current.
func (s *ChainSet[K]) countClass(c uint32, delta int32) {
	if int(c) >= len(s.classCount) {
		s.classCount = append(s.classCount, make([]int32, int(c)+1-len(s.classCount))...)
	}
	s.classCount[c] += delta
	switch s.classCount[c] {
	case delta:
		if delta > 0 {
			s.inUse = append(s.inUse, c)
		}
	case 0:
		for j, u := range s.inUse {
			if u == c {
				s.inUse[j] = s.inUse[len(s.inUse)-1]
				s.inUse = s.inUse[:len(s.inUse)-1]
				break
			}
		}
	}
}

// Contains reports whether key is currently resident in s (Visible or Dormant).
func (s *ChainSet[K]) Contains(key K) bool {
	_, ok := s.byKey[key]
	return ok
}

// AddRootChain records a prefix chain keys[0..L-1] starting at depth-0 root
// keys[0], refreshing recency and evicting oldest leaves if capacity is exceeded.
func (s *ChainSet[K]) AddRootChain(keys []K) {
	s.addSegment(0, true, keys)
}

// AddChainUnder records a continuation segment keys[0..L-1] whose first block
// keys[0] is the child of parentKey.
//
// If parentKey is not currently resident in s (or is itself Dormant), newly
// inserted blocks in keys are marked Dormant until the missing ancestor chain
// to a depth-0 root is restored.
//
// parentKey must name the actual parent block. A segment that starts at a
// root goes through [ChainSet.AddRootChain]: a parentKey that never becomes
// resident, such as 0 for an event without a parent, leaves the segment
// Dormant until capacity evicts it.
func (s *ChainSet[K]) AddChainUnder(parentKey K, keys []K) {
	s.addSegment(parentKey, false, keys)
}

func (s *ChainSet[K]) addSegment(parentKey K, isRoot bool, keys []K) {
	if len(keys) == 0 {
		return
	}
	s.ensureInit()
	s.seq++
	stamp := s.seq

	if !isRoot {
		if pIdx, ok := s.byKey[parentKey]; ok {
			s.blocks[pIdx].stamp = stamp
			s.syncLeafHeap(pIdx)
		}
	}

	needRebuild := false
	for i, k := range keys {
		if i > 0 {
			parentKey, isRoot = keys[i-1], false
		}
		if idx, exists := s.byKey[k]; exists {
			s.blocks[idx].stamp = stamp
			s.syncLeafHeap(idx)
			continue
		}
		if !isRoot && s.createsCycle(k, parentKey) {
			break
		}
		if s.insertBlock(k, parentKey, isRoot, stamp) {
			needRebuild = true
		}
	}

	if needRebuild {
		s.rebuild()
	}
	s.enforceCapacity()
}

// createsCycle reports whether inserting non-root key k under parentKey would
// form a cycle in the resident parent graph. Because the resident graph is
// inductively acyclic and k is not yet resident, a cycle can only form if
// parentKey == k or if resident blocks are already waiting on k (waitingOn[k] > 0)
// and parentKey walks up to k.
func (s *ChainSet[K]) createsCycle(k, parentKey K) bool {
	if parentKey == k {
		return true
	}
	if len(s.waitingOn) == 0 || s.waitingOn[k] == 0 {
		return false
	}
	for cur := parentKey; ; {
		idx, ok := s.byKey[cur]
		if !ok || s.blocks[idx].isRoot {
			return false
		}
		cur = s.blocks[idx].parentKey
		if cur == k {
			return true
		}
	}
}

func (s *ChainSet[K]) insertBlock(k, parentKey K, isRoot bool, stamp uint64) (needRebuild bool) {
	vis := visVisible
	if isRoot {
		parentKey = 0
	} else if pIdx, ok := s.byKey[parentKey]; ok {
		s.blocks[pIdx].children++
		s.syncLeafHeap(pIdx)
		vis = s.blocks[pIdx].vis
	} else {
		s.waitingOn[parentKey]++
		vis = visDormant
	}

	waitCnt := s.waitingOn[k]
	if waitCnt > 0 {
		delete(s.waitingOn, k)
	}

	idx := uint32(len(s.blocks)) //#nosec G115 -- one endpoint holds far fewer than 2^32 blocks
	s.blocks = append(s.blocks, block[K]{
		key:       k,
		parentKey: parentKey,
		stamp:     stamp,
		heapIdx:   -1,
		children:  uint32(waitCnt), //#nosec G115 -- a positive count of resident blocks
		isRoot:    isRoot,
		vis:       vis,
	})
	s.byKey[k] = idx
	s.countClass(0, 1)
	if vis == visDormant {
		s.dormantCount++
	}
	if isRoot && s.onRoot != nil {
		s.onRoot(k, true)
	}
	if waitCnt > 0 {
		return vis == visVisible
	}
	s.syncLeafHeap(idx)
	return false
}

// SetCapacity changes the bound on resident blocks (0 means unlimited),
// evicting the oldest leaves, Dormant first, when the new bound is below the
// current count.
func (s *ChainSet[K]) SetCapacity(capacity int) {
	s.capacity = capacity
	s.enforceCapacity()
}

func (s *ChainSet[K]) enforceCapacity() {
	for s.capacity > 0 && len(s.blocks) > s.capacity {
		if _, ok := s.EvictOldestLeaf(); !ok {
			break
		}
	}
}

// Remove deletes key from s, propagating its recency stamp to its parent if
// resident. If key was Visible and had resident children, its descendants
// transition to Dormant via an O(N) rebuild; if key was already Dormant, its
// children are recorded as waiting on key in O(1). Remove reports whether key
// was resident in s.
func (s *ChainSet[K]) Remove(key K) bool {
	idx, ok := s.byKey[key]
	if !ok {
		return false
	}
	removed := s.blocks[idx]
	if removed.isRoot && s.onRoot != nil {
		s.onRoot(removed.key, false)
	}
	if !removed.isRoot {
		if pIdx, ok := s.byKey[removed.parentKey]; ok {
			p := &s.blocks[pIdx]
			p.children--
			p.stamp = max(p.stamp, removed.stamp)
			s.syncLeafHeap(pIdx)
		} else if cnt := s.waitingOn[removed.parentKey]; cnt <= 1 {
			delete(s.waitingOn, removed.parentKey)
		} else {
			s.waitingOn[removed.parentKey] = cnt - 1
		}
	}
	if removed.vis == visDormant {
		s.dormantCount--
	}
	s.countClass(removed.class, -1)

	s.deleteBlockAt(idx)
	if removed.children > 0 {
		if removed.vis == visVisible {
			s.rebuild()
		} else {
			s.waitingOn[removed.key] = int32(removed.children) //#nosec G115 -- bounded by the resident block count, far below 2^31
		}
	}
	return true
}

func (s *ChainSet[K]) deleteBlockAt(idx uint32) {
	s.removeFromLeafHeap(idx)
	delete(s.byKey, s.blocks[idx].key)

	last := uint32(len(s.blocks) - 1) //#nosec G115 -- one endpoint holds far fewer than 2^32 blocks
	if idx != last {
		moved := s.blocks[last]
		s.blocks[idx] = moved
		s.byKey[moved.key] = idx
		if moved.heapIdx >= 0 {
			s.leaves[moved.heapIdx] = idx
		}
	}
	s.blocks[last] = block[K]{}
	s.blocks = s.blocks[:last]
}

// EvictOldestLeaf evicts the leaf block (children == 0) with the oldest recency
// stamp, prioritizing Dormant leaves ahead of Visible leaves.
func (s *ChainSet[K]) EvictOldestLeaf() (K, bool) {
	if len(s.leaves) == 0 {
		return 0, false
	}
	key := s.blocks[s.leaves[0]].key
	if s.Remove(key) {
		s.evictions++
		return key, true
	}
	return 0, false
}

// RunLength returns the length of the longest contiguous Visible prefix of keys
// starting at depth-0 root keys[0], using O(log L) binary search with optional
// match-depth hints from earlier endpoints.
//
// Precondition: keys must represent a valid prefix chain where keys[0] is a
// depth-0 root key and each subsequent keys[i] commits to parent keys[i-1].
func (s *ChainSet[K]) RunLength(keys []K, hints []int) int {
	L := len(keys)
	if L == 0 || len(s.blocks) == 0 {
		return 0
	}
	vis0, hasChildren0 := s.probeAt(keys, 0)
	if !vis0 {
		return 0
	}
	if !hasChildren0 || L == 1 {
		return 1
	}

	// Loop invariant: keys[0:lo] are Visible and connected; if hi < L, keys[hi] is not.
	lo, hi := 1, L
	step := func(pos int) {
		if vis, hasChildren := s.probeAt(keys, pos); vis {
			lo = pos + 1
			if !hasChildren {
				hi = lo
			}
		} else {
			hi = pos
		}
	}

	for _, d := range hints {
		if d > lo && d <= hi {
			step(d - 1)
			if lo == d && d < hi {
				step(d)
			}
		}
	}
	for lo < hi {
		step(int(uint(lo+hi) >> 1))
	}
	return lo
}

func (s *ChainSet[K]) probeAt(keys []K, pos int) (visible bool, hasChildren bool) {
	idx, ok := s.byKey[keys[pos]]
	if !ok {
		return false, false
	}
	b := &s.blocks[idx]
	if b.vis != visVisible {
		return false, false
	}
	if pos == 0 {
		return b.isRoot, b.children > 0
	}
	if b.isRoot || b.parentKey != keys[pos-1] {
		return false, false
	}
	return true, b.children > 0
}

// rebuild recomputes all derived projections (children, vis, dormantCount,
// waitingOn, and leaves) from the authoritative (key, parentKey, isRoot, stamp)
// facts of s.blocks.
func (s *ChainSet[K]) rebuild() {
	s.rebuilds++
	clear(s.waitingOn)
	for i := range s.blocks {
		b := &s.blocks[i]
		b.children = 0
		b.heapIdx = -1
		if b.isRoot {
			b.vis = visVisible
		} else {
			b.vis = visUnvisited
		}
	}
	for i := range s.blocks {
		b := &s.blocks[i]
		if !b.isRoot {
			if pIdx, ok := s.byKey[b.parentKey]; ok {
				s.blocks[pIdx].children++
			} else {
				s.waitingOn[b.parentKey]++
			}
		}
	}
	s.dormantCount = 0
	s.leaves = s.leaves[:0]
	for i := range s.blocks {
		if s.resolveVis(uint32(i)) == visDormant {
			s.dormantCount++
		}
		if s.blocks[i].children == 0 {
			s.blocks[i].heapIdx = int32(len(s.leaves)) //#nosec G115 -- heap positions are bounded by the resident block count
			s.leaves = append(s.leaves, uint32(i))
		}
	}
	for i := len(s.leaves)/2 - 1; i >= 0; i-- {
		s.siftDown(i)
	}
}

func (s *ChainSet[K]) resolveVis(idx uint32) uint8 {
	b := &s.blocks[idx]
	if b.vis != visUnvisited {
		return b.vis
	}
	b.vis = visDormant
	if pIdx, ok := s.byKey[b.parentKey]; ok && s.resolveVis(pIdx) == visVisible {
		s.blocks[idx].vis = visVisible
	}
	return s.blocks[idx].vis
}

func (s *ChainSet[K]) syncLeafHeap(idx uint32) {
	b := &s.blocks[idx]
	if b.children > 0 {
		s.removeFromLeafHeap(idx)
		return
	}
	if b.heapIdx < 0 {
		b.heapIdx = int32(len(s.leaves)) //#nosec G115 -- heap positions are bounded by the resident block count
		s.leaves = append(s.leaves, idx)
	}
	s.siftUp(int(b.heapIdx))
	s.siftDown(int(b.heapIdx))
}

func (s *ChainSet[K]) removeFromLeafHeap(idx uint32) {
	i := int(s.blocks[idx].heapIdx)
	if i < 0 {
		return
	}
	s.blocks[idx].heapIdx = -1
	last := len(s.leaves) - 1
	s.leaves[i] = s.leaves[last]
	s.leaves = s.leaves[:last]
	if i < last {
		moved := s.leaves[i]
		s.blocks[moved].heapIdx = int32(i) //#nosec G115 -- heap positions are bounded by the resident block count
		s.siftUp(i)
		s.siftDown(int(s.blocks[moved].heapIdx))
	}
}

// lessLeaf orders leaves by (vis, stamp, key): Dormant (1) before Visible (2),
// then oldest stamp first, breaking ties on the immutable block key so
// swap-delete in s.blocks never alters heap ordering.
func (s *ChainSet[K]) lessLeaf(a, b uint32) bool {
	x, y := &s.blocks[a], &s.blocks[b]
	return cmp.Or(
		cmp.Compare(x.vis, y.vis),
		cmp.Compare(x.stamp, y.stamp),
		cmp.Compare(x.key, y.key),
	) < 0
}

func (s *ChainSet[K]) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !s.lessLeaf(s.leaves[i], s.leaves[p]) {
			break
		}
		s.swapHeap(i, p)
		i = p
	}
}

func (s *ChainSet[K]) siftDown(i int) {
	for n := len(s.leaves); 2*i+1 < n; {
		c := 2*i + 1
		if r := c + 1; r < n && s.lessLeaf(s.leaves[r], s.leaves[c]) {
			c = r
		}
		if !s.lessLeaf(s.leaves[c], s.leaves[i]) {
			break
		}
		s.swapHeap(i, c)
		i = c
	}
}

func (s *ChainSet[K]) swapHeap(i, j int) {
	a, b := s.leaves[i], s.leaves[j]
	s.leaves[i], s.leaves[j] = b, a
	s.blocks[a].heapIdx, s.blocks[b].heapIdx = int32(j), int32(i) //#nosec G115 -- heap positions are bounded by the resident block count
}
