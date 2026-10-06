/*
Copyright 2025 The Kubernetes Authors.

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
	"context"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

const (
	// maxBitsetPods is the number of active pods tracked lock-free in the
	// 256-bit presence bitset on each block entry (4 x uint64). Pods beyond
	// 256 simultaneously active instances fall back to per-entry overflow maps.
	maxBitsetPods = 256

	// numApproxShards is the number of shards partitioning the block-hash table.
	numApproxShards = 256

	// minApproxFastSize and maxApproxFastSize bound the power-of-two direct-mapped
	// lock-free block-hash lookup table.
	minApproxFastSize = 1 << 10
	maxApproxFastSize = 1 << 22

	// podFastSize is the number of slots in the direct-mapped lock-free pod
	// state lookup table used by Add.
	podFastSize = 1024

	// shardCompactMin is the minimum shard map size before lazy dead-entry
	// compaction runs during new-key insertion.
	shardCompactMin = 65536

	// matchJumpStride is the number of blocks skipped per jump probe in
	// MatchLongestPrefix when all active candidate servers hold the stride
	// endpoint.
	matchJumpStride = 16

	// maxApproxFastProbes is the linear-probing window in the lock-free fast table.
	maxApproxFastProbes = 8
)

type podMask [4]uint64

type approxBlockEntry struct {
	hash       blockHash
	mask       [4]atomic.Uint64
	evicted    atomic.Bool
	overflowMu sync.RWMutex
	overflow   map[ServerID]struct{}
}

func (e *approxBlockEntry) loadMask(words int) podMask {
	var m podMask
	m[0] = e.mask[0].Load()
	if words > 1 {
		m[1] = e.mask[1].Load()
		if words > 2 {
			m[2] = e.mask[2].Load()
			if words > 3 {
				m[3] = e.mask[3].Load()
			}
		}
	}
	return m
}

func (e *approxBlockEntry) isEmpty() bool {
	if (e.mask[0].Load() | e.mask[1].Load() | e.mask[2].Load() | e.mask[3].Load()) != 0 {
		return false
	}
	e.overflowMu.RLock()
	n := len(e.overflow)
	e.overflowMu.RUnlock()
	return n == 0
}

type approxShard struct {
	mu        sync.RWMutex
	m         map[blockHash]*approxBlockEntry
	compactAt int
	_         [88]byte
}

type lruNode struct {
	key     blockHash
	entry   *approxBlockEntry
	prev    uint32
	next    uint32
	slotIdx uint32
}

type lruSlot struct {
	key       blockHash
	nodePlus1 uint32
}

// podState holds the per-pod mutex, interned bitset slot, and a flat
// zero-allocation ring-buffer LRU with linear-probing index.
type podState struct {
	mu            sync.Mutex
	id            ServerID
	slot          int
	word          int
	bit           uint64
	cap           int
	head          uint32
	nodes         []lruNode
	table         []lruSlot
	tableMask     uint32
	count         atomic.Int32
	removed       bool
	removedAtomic atomic.Bool
	_             [32]byte
}

func newPodState(id ServerID, slot, lruCap int) *podState {
	initNodes := min(lruCap, 64)
	initTable := uint32(128)
	for int(initTable) < initNodes*2 {
		initTable <<= 1
	}
	ps := &podState{
		id:        id,
		slot:      slot,
		cap:       lruCap,
		nodes:     make([]lruNode, 0, initNodes),
		table:     make([]lruSlot, initTable),
		tableMask: initTable - 1,
	}
	if slot >= 0 {
		ps.word = slot >> 6
		ps.bit = uint64(1) << (slot & 63)
	}
	return ps
}

// Len returns the number of blocks currently cached in this pod's LRU.
func (ps *podState) Len() int {
	return int(ps.count.Load())
}

func (ps *podState) promote(idx uint32) {
	if idx == ps.head {
		return
	}
	tail := ps.nodes[ps.head].prev
	if idx == tail {
		ps.head = idx
		return
	}
	p := ps.nodes[idx].prev
	n := ps.nodes[idx].next
	ps.nodes[p].next = n
	ps.nodes[n].prev = p

	ps.nodes[idx].prev = tail
	ps.nodes[idx].next = ps.head
	ps.nodes[tail].next = idx
	ps.nodes[ps.head].prev = idx
	ps.head = idx
}

func (ps *podState) growTable() {
	newSize := uint32(len(ps.table)) << 1
	newMask := newSize - 1
	newTable := make([]lruSlot, newSize)
	for i := range ps.nodes {
		k := ps.nodes[i].key
		s := uint32(mixApproxHash(k)) & newMask
		for newTable[s].nodePlus1 != 0 {
			s = (s + 1) & newMask
		}
		newTable[s] = lruSlot{
			key:       k,
			nodePlus1: uint32(i) + 1,
		}
		ps.nodes[i].slotIdx = s
	}
	ps.table = newTable
	ps.tableMask = newMask
}

// indexer implements the indexerInterface interface.
type indexer struct {
	podMu          sync.RWMutex
	podToLRU       map[ServerID]*podState
	slotToState    [maxBitsetPods]atomic.Pointer[podState]
	podFast        [podFastSize]atomic.Pointer[podState]
	freeSlots      []uint8
	activeWords    atomic.Int32
	overflowPods   atomic.Int32
	shards         [numApproxShards]approxShard
	fast           []atomic.Pointer[approxBlockEntry]
	fastMask       uint64
	fastOverflow   atomic.Bool
	defaultLRUSize int
	pluginName     string
	pluginType     string
}

// newIndexer initializes an indexer with size limits and starts cache size reporting.
func newIndexer(ctx context.Context, defaultLRUSize int, pluginName, pluginType string) indexerInterface {
	free := make([]uint8, maxBitsetPods)
	for idx := range maxBitsetPods {
		free[idx] = uint8(maxBitsetPods - 1 - idx)
	}
	fastSize := minApproxFastSize
	if defaultLRUSize >= 1024 {
		fastSize = maxApproxFastSize
	}
	i := &indexer{
		podToLRU:       make(map[ServerID]*podState),
		freeSlots:      free,
		fast:           make([]atomic.Pointer[approxBlockEntry], fastSize),
		fastMask:       uint64(fastSize - 1),
		defaultLRUSize: defaultLRUSize,
		pluginName:     pluginName,
		pluginType:     pluginType,
	}
	i.activeWords.Store(1)
	compactInit := max(shardCompactMin, defaultLRUSize*2)
	for s := range numApproxShards {
		i.shards[s].m = make(map[blockHash]*approxBlockEntry)
		i.shards[s].compactAt = compactInit
	}

	go i.reportLRUSize(ctx, time.Second)
	return i
}

func mixApproxHash(h blockHash) uint64 {
	x := uint64(h)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

func (i *indexer) fastIndex(h blockHash) uint64 {
	x := uint64(h)
	return (x ^ (x >> 20) ^ (x >> 40)) & i.fastMask
}

func shardIndex(h blockHash) uint64 {
	return (uint64(h) * 0x9e3779b97f4a7c15) >> (64 - 8)
}

func hashServerID(id ServerID) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for idx := 0; idx < len(id.Namespace); idx++ {
		h = (h ^ uint64(id.Namespace[idx])) * 0x100000001b3
	}
	for idx := 0; idx < len(id.Name); idx++ {
		h = (h ^ uint64(id.Name[idx])) * 0x100000001b3
	}
	return h ^ (h >> 32)
}

func (i *indexer) podAtSlot(slot int) ServerID {
	if ps := i.slotToState[slot].Load(); ps != nil {
		return ps.id
	}
	return ServerID{}
}

func (i *indexer) insertFast(h blockHash, e *approxBlockEntry) {
	base := i.fastIndex(h)
	for p := uint64(0); p < maxApproxFastProbes; p++ {
		idx := (base + p) & i.fastMask
		cur := i.fast[idx].Load()
		if cur == e {
			return
		}
		if cur == nil {
			if i.fast[idx].CompareAndSwap(nil, e) {
				return
			}
			if i.fast[idx].Load() == e {
				return
			}
		}
	}
	i.fastOverflow.Store(true)
	i.fast[base].Store(e)
}

func (i *indexer) lookupEntry(h blockHash) *approxBlockEntry {
	base := i.fastIndex(h)
	overflow := i.fastOverflow.Load()
	for p := uint64(0); p < maxApproxFastProbes; p++ {
		e := i.fast[(base+p)&i.fastMask].Load()
		if e == nil {
			if !overflow {
				return nil
			}
			break
		}
		if e.hash == h {
			return e
		}
	}
	if !overflow {
		return nil
	}
	sh := &i.shards[shardIndex(h)]
	sh.mu.RLock()
	e := sh.m[h]
	sh.mu.RUnlock()
	return e
}

func (i *indexer) compactShardLocked(sh *approxShard) {
	i.fastOverflow.Store(true)
	for h, e := range sh.m {
		if e.isEmpty() {
			e.evicted.Store(true)
			if e.isEmpty() {
				delete(sh.m, h)
				i.fast[i.fastIndex(h)].CompareAndSwap(e, nil)
			} else {
				e.evicted.Store(false)
			}
		}
	}
	sh.compactAt = max(shardCompactMin, len(sh.m)*2)
}

func (i *indexer) setPodOnEntry(h blockHash, ps *podState) *approxBlockEntry {
	if ps.slot >= 0 {
		if e := i.lookupEntry(h); e != nil {
			e.mask[ps.word].Or(ps.bit)
			if !e.evicted.Load() {
				return e
			}
		}
	}

	sh := &i.shards[shardIndex(h)]
	sh.mu.Lock()
	e := sh.m[h]
	if e == nil {
		if len(sh.m) >= sh.compactAt {
			i.compactShardLocked(sh)
		}
		e = &approxBlockEntry{hash: h}
		sh.m[h] = e
	}
	if ps.slot >= 0 {
		e.mask[ps.word].Or(ps.bit)
	} else {
		e.overflowMu.Lock()
		if e.overflow == nil {
			e.overflow = make(map[ServerID]struct{})
		}
		e.overflow[ps.id] = struct{}{}
		e.overflowMu.Unlock()
	}
	i.insertFast(h, e)
	sh.mu.Unlock()
	return e
}

func (i *indexer) clearPodOnEntry(e *approxBlockEntry, ps *podState) {
	if ps.slot >= 0 {
		e.mask[ps.word].And(^ps.bit)
		return
	}
	e.overflowMu.Lock()
	delete(e.overflow, ps.id)
	e.overflowMu.Unlock()
}

func (i *indexer) getOrCreatePodState(pod server) *podState {
	fIdx := hashServerID(pod.ServerID) & (podFastSize - 1)
	if ps := i.podFast[fIdx].Load(); ps != nil && ps.id == pod.ServerID && !ps.removedAtomic.Load() {
		return ps
	}

	i.podMu.RLock()
	ps := i.podToLRU[pod.ServerID]
	i.podMu.RUnlock()
	if ps != nil && !ps.removedAtomic.Load() {
		i.podFast[fIdx].Store(ps)
		return ps
	}

	i.podMu.Lock()
	ps = i.podToLRU[pod.ServerID]
	if ps != nil && !ps.removed {
		i.podFast[fIdx].Store(ps)
		i.podMu.Unlock()
		return ps
	}

	lruSize := pod.NumOfGPUBlocks
	if lruSize <= 0 {
		lruSize = i.defaultLRUSize
	}
	if lruSize <= 0 {
		lruSize = 1
	}

	slot := -1
	if n := len(i.freeSlots); n > 0 {
		slot = int(i.freeSlots[n-1])
		i.freeSlots = i.freeSlots[:n-1]
	} else {
		i.overflowPods.Add(1)
	}

	ps = newPodState(pod.ServerID, slot, lruSize)
	if slot >= 0 {
		if w := int32(slot>>6) + 1; w > i.activeWords.Load() {
			i.activeWords.Store(w)
		}
		i.slotToState[slot].Store(ps)
	}
	i.podToLRU[pod.ServerID] = ps
	i.podFast[fIdx].Store(ps)
	i.podMu.Unlock()
	return ps
}

// Add adds a list of prefix hashes to the cache, tied to the server.
func (i *indexer) Add(hashes []blockHash, pod server) {
	if len(hashes) == 0 {
		return
	}

	ps := i.getOrCreatePodState(pod)
	ps.mu.Lock()
	if ps.removed {
		ps.mu.Unlock()
		return
	}

	// Insert hashes tail-first: matching is anchored at the first block, so
	// the head is the most valuable entry and must stay cached longest; an
	// oversized batch naturally keeps its head.
	for idx := len(hashes) - 1; idx >= 0; idx-- {
		hash := hashes[idx]
		mask := ps.tableMask
		hSlot := uint32(mixApproxHash(hash)) & mask
		hit := false
		for {
			s := ps.table[hSlot]
			if s.nodePlus1 == 0 {
				break
			}
			if s.key == hash {
				ps.promote(s.nodePlus1 - 1)
				hit = true
				break
			}
			hSlot = (hSlot + 1) & mask
		}
		if hit {
			continue
		}

		entry := i.setPodOnEntry(hash, ps)
		n := uint32(len(ps.nodes))
		if int(n) < ps.cap {
			if int(n+1)*2 > len(ps.table) {
				ps.growTable()
				mask = ps.tableMask
				hSlot = uint32(mixApproxHash(hash)) & mask
				for ps.table[hSlot].nodePlus1 != 0 {
					hSlot = (hSlot + 1) & mask
				}
			}
			if n == 0 {
				ps.nodes = append(ps.nodes, lruNode{
					key:     hash,
					entry:   entry,
					prev:    0,
					next:    0,
					slotIdx: hSlot,
				})
				ps.head = 0
			} else {
				tail := ps.nodes[ps.head].prev
				ps.nodes = append(ps.nodes, lruNode{
					key:     hash,
					entry:   entry,
					prev:    tail,
					next:    ps.head,
					slotIdx: hSlot,
				})
				ps.nodes[tail].next = n
				ps.nodes[ps.head].prev = n
				ps.head = n
			}
			ps.table[hSlot] = lruSlot{
				key:       hash,
				nodePlus1: n + 1,
			}
			ps.count.Store(int32(n + 1))
			continue
		}

		// At capacity: evict the tail node in-place and rotate head = tail.
		tail := ps.nodes[ps.head].prev
		evictedNode := &ps.nodes[tail]
		evictedEntry := evictedNode.entry

		delSlot := evictedNode.slotIdx
		j := delSlot
		for {
			j = (j + 1) & mask
			sj := ps.table[j]
			if sj.nodePlus1 == 0 {
				break
			}
			k := uint32(mixApproxHash(sj.key)) & mask
			if ((j - k) & mask) >= ((j - delSlot) & mask) {
				ps.table[delSlot] = sj
				ps.nodes[sj.nodePlus1-1].slotIdx = delSlot
				delSlot = j
			}
		}
		ps.table[delSlot] = lruSlot{}

		insSlot := uint32(mixApproxHash(hash)) & mask
		for ps.table[insSlot].nodePlus1 != 0 {
			insSlot = (insSlot + 1) & mask
		}
		ps.table[insSlot] = lruSlot{
			key:       hash,
			nodePlus1: tail + 1,
		}
		evictedNode.key = hash
		evictedNode.entry = entry
		evictedNode.slotIdx = insSlot
		ps.head = tail

		i.clearPodOnEntry(evictedEntry, ps)
	}
	ps.mu.Unlock()
}

func (i *indexer) entryPods(e *approxBlockEntry) podSet {
	if e == nil {
		return nil
	}
	words := int(i.activeWords.Load())
	m := e.loadMask(words)
	n := bits.OnesCount64(m[0]) + bits.OnesCount64(m[1]) + bits.OnesCount64(m[2]) + bits.OnesCount64(m[3])
	hasOverflow := i.overflowPods.Load() > 0
	if n == 0 && !hasOverflow {
		return nil
	}

	var res podSet
	if n > 0 {
		res = make(podSet, n)
		for w := range words {
			word := m[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				res[i.podAtSlot(w*64+bit)] = struct{}{}
			}
		}
	}
	if hasOverflow {
		e.overflowMu.RLock()
		if len(e.overflow) > 0 {
			if res == nil {
				res = make(podSet, len(e.overflow))
			}
			for pod := range e.overflow {
				res[pod] = struct{}{}
			}
		}
		e.overflowMu.RUnlock()
	}
	if len(res) == 0 {
		return nil
	}
	return res
}

// Get returns a set of servers that have the given prefix hash cached.
func (i *indexer) Get(hash blockHash) podSet {
	return i.entryPods(i.lookupEntry(hash))
}

func (i *indexer) recordMask(active *podMask, words, length int, res map[ServerID]int) {
	for w := range words {
		word := active[w]
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			res[i.podAtSlot(w*64+bit)] = length
		}
	}
}

// MatchLongestPrefix returns the longest contiguous prefix match length in
// blocks for each server that caches the leading block of hashes. Candidate
// servers start as the set holding hashes[0] and shrink monotonically as
// servers miss subsequent blocks; once a server misses at position pos, its
// prefix length is recorded as pos and its bit is cleared from the active mask.
func (i *indexer) MatchLongestPrefix(hashes []blockHash) map[ServerID]int {
	n := len(hashes)
	if n == 0 {
		return nil
	}
	if i.overflowPods.Load() > 0 {
		return i.matchLongestPrefixOverflow(hashes)
	}

	e0 := i.lookupEntry(hashes[0])
	if e0 == nil {
		return nil
	}
	words := int(i.activeWords.Load())
	active := e0.loadMask(words)
	if (active[0] | active[1] | active[2] | active[3]) == 0 {
		return nil
	}

	numActive := bits.OnesCount64(active[0]) +
		bits.OnesCount64(active[1]) +
		bits.OnesCount64(active[2]) +
		bits.OnesCount64(active[3])
	res := make(map[ServerID]int, numActive)
	if n == 1 {
		i.recordMask(&active, words, 1, res)
		return res
	}

	initMask := active
	firstDropLen := 0
	var laterDropMask podMask
	var laterDropPos [maxBitsetPods]int

	recordDrop := func(dropped *podMask, pos int) {
		if firstDropLen == 0 {
			firstDropLen = pos
			return
		}
		for w := range words {
			dw := dropped[w]
			if dw != 0 {
				laterDropMask[w] |= dw
				for dw != 0 {
					bit := bits.TrailingZeros64(dw)
					dw &= dw - 1
					laterDropPos[w*64+bit] = pos
				}
			}
		}
	}

	// Prefix residency per server is contiguous from block 0, so if all active
	// servers hold the block at jumpIdx, every intermediate block in the stride
	// is also present for those servers.
	for blockIdx := 1; blockIdx < n; {
		stepEnd := n - 1
		var mj podMask
		hasJump := false
		if n >= matchJumpStride && blockIdx < n-1 {
			jumpIdx := min(blockIdx|(matchJumpStride-1), n-1)
			if ej := i.lookupEntry(hashes[jumpIdx]); ej != nil {
				allJump := true
				for w := range words {
					aw := active[w]
					if aw != 0 {
						mjw := ej.mask[w].Load()
						mj[w] = mjw
						if (mjw & aw) != aw {
							allJump = false
						}
					}
				}
				if allJump {
					blockIdx = jumpIdx + 1
					continue
				}
				hasJump = (mj[0]&active[0] | mj[1]&active[1] | mj[2]&active[2] | mj[3]&active[3]) != 0
			}
			stepEnd = jumpIdx
		}

		for pos := blockIdx; pos <= stepEnd; pos++ {
			ep := i.lookupEntry(hashes[pos])
			if ep == nil {
				recordDrop(&active, pos)
				active = podMask{}
				stepEnd = n
				break
			}
			var dropped podMask
			anyDropped := false
			for w := range words {
				aw := active[w]
				if aw == 0 {
					continue
				}
				mw := ep.mask[w].Load()
				if (mw & aw) != aw {
					dropped[w] = aw &^ mw
					active[w] = aw & mw
					anyDropped = true
				}
			}
			if anyDropped {
				recordDrop(&dropped, pos)
				if (active[0] | active[1] | active[2] | active[3]) == 0 {
					stepEnd = n
					break
				}
				if hasJump &&
					(mj[0]&active[0]) == active[0] &&
					(mj[1]&active[1]) == active[1] &&
					(mj[2]&active[2]) == active[2] &&
					(mj[3]&active[3]) == active[3] {
					break
				}
			}
		}
		blockIdx = stepEnd + 1
	}

	if firstDropLen == 0 {
		i.recordMask(&initMask, words, n, res)
		return res
	}

	var firstDropped podMask
	for w := range words {
		firstDropped[w] = initMask[w] &^ (laterDropMask[w] | active[w])
	}
	i.recordMask(&firstDropped, words, firstDropLen, res)
	for w := range words {
		lw := laterDropMask[w]
		for lw != 0 {
			bit := bits.TrailingZeros64(lw)
			lw &= lw - 1
			slot := w*64 + bit
			res[i.podAtSlot(slot)] = laterDropPos[slot]
		}
		aw := active[w]
		for aw != 0 {
			bit := bits.TrailingZeros64(aw)
			aw &= aw - 1
			res[i.podAtSlot(w*64+bit)] = n
		}
	}
	return res
}

func (i *indexer) matchLongestPrefixOverflow(hashes []blockHash) map[ServerID]int {
	firstSet := i.Get(hashes[0])
	if len(firstSet) == 0 {
		return nil
	}

	res := make(map[ServerID]int, len(firstSet))
	if len(hashes) == 1 {
		for pod := range firstSet {
			res[pod] = 1
		}
		return res
	}

	active := make([]ServerID, 0, len(firstSet))
	for pod := range firstSet {
		active = append(active, pod)
	}

	for blockIdx := 1; blockIdx < len(hashes); {
		stepEnd := len(hashes) - 1
		if len(hashes) >= matchJumpStride && blockIdx < len(hashes)-1 {
			jumpIdx := min(blockIdx|(matchJumpStride-1), len(hashes)-1)
			jumpPods := i.Get(hashes[jumpIdx])
			allActive := len(jumpPods) >= len(active)
			if allActive {
				for _, pod := range active {
					if !jumpPods.Has(pod) {
						allActive = false
						break
					}
				}
			}
			if allActive {
				blockIdx = jumpIdx + 1
				continue
			}
			stepEnd = jumpIdx
		}

		for pos := blockIdx; pos <= stepEnd; pos++ {
			pods := i.Get(hashes[pos])
			if len(pods) == 0 {
				for _, pod := range active {
					res[pod] = pos
				}
				return res
			}
			keep := active[:0]
			for _, pod := range active {
				if pods.Has(pod) {
					keep = append(keep, pod)
				} else {
					res[pod] = pos
				}
			}
			active = keep
			if len(active) == 0 {
				return res
			}
		}
		blockIdx = stepEnd + 1
	}

	for _, pod := range active {
		res[pod] = len(hashes)
	}
	return res
}

// hashToPods returns a snapshot of all non-empty blockHash -> podSet mappings
// across all shards for white-box unit testing.
func (i *indexer) hashToPods() map[blockHash]podSet {
	res := make(map[blockHash]podSet)
	for s := range numApproxShards {
		sh := &i.shards[s]
		sh.mu.RLock()
		for h, e := range sh.m {
			if pods := i.entryPods(e); len(pods) > 0 {
				res[h] = pods
			}
		}
		sh.mu.RUnlock()
	}
	return res
}

// reportLRUSize starts a goroutine that periodically reports the LRU cache size metric.
func (i *indexer) reportLRUSize(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.reportOnce(ctx)
		}
	}
}

func (i *indexer) reportOnce(ctx context.Context) {
	i.podMu.RLock()
	totalEntries := 0
	maxPodEntries := 0
	var maxPodName ServerID

	for pod, ps := range i.podToLRU {
		size := ps.Len()
		totalEntries += size
		if size > maxPodEntries {
			maxPodEntries = size
			maxPodName = pod
		}
	}
	numPods := len(i.podToLRU)
	i.podMu.RUnlock()

	avg := 0.0
	if numPods > 0 {
		avg = float64(totalEntries) / float64(numPods)
	}

	recordPrefixCacheSize(i.pluginName, i.pluginType, int64(totalEntries))

	log.FromContext(ctx).V(logutil.TRACE).Info("Prefix cache state",
		"total entries", totalEntries,
		"# pods", numPods,
		"avg entries per pod", avg,
		"pod with max cache", maxPodName,
		"max pod size", maxPodEntries,
		"global max LRU cache capacity per pod", i.defaultLRUSize,
	)
}

// RemovePod removes a pod and its associated entries from the indexer.
func (i *indexer) RemovePod(pod ServerID) {
	i.podMu.Lock()
	ps, exists := i.podToLRU[pod]
	if !exists {
		i.podMu.Unlock()
		return
	}
	delete(i.podToLRU, pod)
	i.podFast[hashServerID(pod)&(podFastSize-1)].CompareAndSwap(ps, nil)

	ps.mu.Lock()
	ps.removed = true
	ps.removedAtomic.Store(true)
	for idx := range ps.nodes {
		i.clearPodOnEntry(ps.nodes[idx].entry, ps)
	}
	ps.nodes = nil
	clear(ps.table)
	ps.count.Store(0)
	if ps.slot >= 0 {
		i.freeSlots = append(i.freeSlots, uint8(ps.slot))
	} else {
		i.overflowPods.Add(-1)
	}
	ps.mu.Unlock()
	i.podMu.Unlock()
}

// Pods returns the list of all pods currently tracked in the indexer.
func (i *indexer) Pods() []ServerID {
	i.podMu.RLock()
	defer i.podMu.RUnlock()

	pods := make([]ServerID, 0, len(i.podToLRU))
	for pod := range i.podToLRU {
		pods = append(pods, pod)
	}
	return pods
}

// PodBlockCounts returns the number of cached blocks currently tracked per pod.
func (i *indexer) PodBlockCounts() map[ServerID]int {
	i.podMu.RLock()
	defer i.podMu.RUnlock()

	counts := make(map[ServerID]int, len(i.podToLRU))
	for pod, ps := range i.podToLRU {
		counts[pod] = ps.Len()
	}
	return counts
}
