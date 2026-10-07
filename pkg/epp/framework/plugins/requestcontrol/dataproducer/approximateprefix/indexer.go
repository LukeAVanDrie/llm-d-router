/*
Copyright 2025 The Kubernetes Authors.
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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

const (
	// inlineDropSlots is the stack-allocated drop-position buffer size in
	// MatchLongestPrefix, covering up to 256 active pods without heap allocation.
	inlineDropSlots = 256

	// minApproxFastSize and maxApproxFastSize bound the power-of-two 8-way
	// linear-probing lock-free block-hash lookup table.
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
)

// podState holds the per-pod mutex, interned bitset ordinal, and bounded LRU.
type podState struct {
	mu      sync.Mutex
	id      ServerID
	ord     int
	lruCap  int
	lru     *simplelru.LRU[blockHash, *collections.BitsetEntry[blockHash]]
	count   atomic.Int32
	removed bool
	gone    atomic.Bool
	_       [32]byte
}

func newPodState(id ServerID, ord, lruCap int) *podState {
	lru, _ := simplelru.NewLRU(lruCap, func(_ blockHash, e *collections.BitsetEntry[blockHash]) {
		e.Clear(ord)
	})
	return &podState{
		id:     id,
		ord:    ord,
		lruCap: lruCap,
		lru:    lru,
	}
}

// Len returns the number of blocks currently cached in this pod's LRU.
func (ps *podState) Len() int {
	return int(ps.count.Load())
}

// indexer implements the indexerInterface interface.
type indexer struct {
	podMu          sync.RWMutex
	podToLRU       map[ServerID]*podState
	pods           *collections.Interner[ServerID]
	podFast        [podFastSize]atomic.Pointer[podState]
	blocks         *collections.ShardedBitsetIndex[blockHash]
	defaultLRUSize int
	pluginName     string
	pluginType     string
}

// newIndexer initializes an indexer with size limits and starts cache size reporting.
func newIndexer(ctx context.Context, defaultLRUSize int, pluginName, pluginType string) indexerInterface {
	fastSize := minApproxFastSize
	if defaultLRUSize >= 1024 {
		fastSize = maxApproxFastSize
	}
	compactInit := max(shardCompactMin, defaultLRUSize*2)
	i := &indexer{
		podToLRU:       make(map[ServerID]*podState),
		pods:           collections.NewInterner[ServerID](),
		blocks:         collections.NewShardedBitsetIndex(fastSize, compactInit, fastHash),
		defaultLRUSize: defaultLRUSize,
		pluginName:     pluginName,
		pluginType:     pluginType,
	}

	go i.reportLRUSize(ctx, time.Second)
	return i
}

func fastHash(h blockHash) uint64 {
	x := uint64(h)
	return x ^ (x >> 20) ^ (x >> 40)
}

func hashServerID(id ServerID) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for idx := 0; idx < len(id.Namespace); idx++ {
		h = (h ^ uint64(id.Namespace[idx])) * 0x100000001b3
	}
	h = (h ^ uint64('/')) * 0x100000001b3
	for idx := 0; idx < len(id.Name); idx++ {
		h = (h ^ uint64(id.Name[idx])) * 0x100000001b3
	}
	return h ^ (h >> 32)
}

func (i *indexer) getOrCreatePodState(pod server) *podState {
	fIdx := hashServerID(pod.ServerID) & (podFastSize - 1)
	if ps := i.podFast[fIdx].Load(); ps != nil && ps.id == pod.ServerID && !ps.gone.Load() {
		return ps
	}

	i.podMu.RLock()
	ps := i.podToLRU[pod.ServerID]
	if ps != nil && !ps.gone.Load() {
		i.podFast[fIdx].CompareAndSwap(nil, ps)
		i.podMu.RUnlock()
		return ps
	}
	i.podMu.RUnlock()

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

	ord, _ := i.pods.Intern(pod.ServerID)
	ps = newPodState(pod.ServerID, ord, lruSize)
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
	if len(hashes) > ps.lruCap {
		hashes = hashes[:ps.lruCap]
	}

	// Insert hashes tail-first: matching is anchored at the first block, so
	// the leading block is the most valuable entry and must stay cached
	// longest; an oversized batch naturally keeps its leading blocks.
	for idx := len(hashes) - 1; idx >= 0; idx-- {
		hash := hashes[idx]
		if _, ok := ps.lru.Get(hash); ok {
			continue
		}
		entry := i.blocks.SetBit(hash, ps.ord)
		ps.lru.Add(hash, entry)
	}
	ps.count.Store(int32(ps.lru.Len()))
	ps.mu.Unlock()
}

func (i *indexer) podsFromBitset(bs collections.Bitset) podSet {
	n := bs.Len()
	if n == 0 {
		return nil
	}
	res := make(podSet, n)
	for ord := range bs.All() {
		res[i.pods.At(ord)] = struct{}{}
	}
	return res
}

// Get returns a set of servers that have the given prefix hash cached.
func (i *indexer) Get(hash blockHash) podSet {
	e := i.blocks.Lookup(hash)
	if e == nil {
		return nil
	}
	return i.podsFromBitset(e.Snapshot())
}

func (i *indexer) recordMask(bs *collections.Bitset, length int, res map[ServerID]int) {
	for ord := range bs.All() {
		res[i.pods.At(ord)] = length
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

	e0 := i.blocks.Lookup(hashes[0])
	if e0 == nil {
		return nil
	}
	active := e0.Snapshot()
	if active.IsEmpty() {
		return nil
	}

	res := make(map[ServerID]int, active.Len())
	if n == 1 {
		i.recordMask(&active, 1, res)
		return res
	}

	initMask := active.Clone()
	firstDropLen := 0
	var laterDropMask collections.Bitset
	var inlineDropPos [inlineDropSlots]int
	laterDropPos := inlineDropPos[:]
	if maxOrds := i.pods.ActiveWords() * 64; maxOrds > len(laterDropPos) {
		laterDropPos = make([]int, maxOrds)
	}

	recordDrop := func(dropped *collections.Bitset, pos int) {
		if firstDropLen == 0 {
			firstDropLen = pos
			return
		}
		laterDropMask.Or(dropped)
		for ord := range dropped.All() {
			if ord >= len(laterDropPos) {
				grown := make([]int, ord+1)
				copy(grown, laterDropPos)
				laterDropPos = grown
			}
			laterDropPos[ord] = pos
		}
	}

	// Prefix residency per server is contiguous from block 0, so if all active
	// servers hold the block at jumpIdx, every intermediate block in the stride
	// is also present for those servers.
	for blockIdx := 1; blockIdx < n; {
		stepEnd := n - 1
		var mj collections.Bitset
		hasJump := false
		if n >= matchJumpStride && blockIdx < n-1 {
			jumpIdx := min(blockIdx|(matchJumpStride-1), n-1)
			if ej := i.blocks.Lookup(hashes[jumpIdx]); ej != nil {
				mj = ej.Snapshot()
				if mj.ContainsAll(&active) {
					blockIdx = jumpIdx + 1
					continue
				}
				hasJump = mj.Intersects(&active)
			}
			stepEnd = jumpIdx
		}

		for pos := blockIdx; pos <= stepEnd; pos++ {
			ep := i.blocks.Lookup(hashes[pos])
			if ep == nil {
				recordDrop(&active, pos)
				active = collections.Bitset{}
				stepEnd = n
				break
			}
			mp := ep.Snapshot()
			if !mp.ContainsAll(&active) {
				dropped := active.AndNot(&mp)
				active.And(&mp)
				recordDrop(&dropped, pos)
				if active.IsEmpty() {
					stepEnd = n
					break
				}
				if hasJump && mj.ContainsAll(&active) {
					break
				}
			}
		}
		blockIdx = stepEnd + 1
	}

	if firstDropLen == 0 {
		i.recordMask(&initMask, n, res)
		return res
	}

	initMask.ClearFrom(&laterDropMask)
	initMask.ClearFrom(&active)
	i.recordMask(&initMask, firstDropLen, res)
	for ord := range laterDropMask.All() {
		res[i.pods.At(ord)] = laterDropPos[ord]
	}
	i.recordMask(&active, n, res)
	return res
}

// hashToPods returns a snapshot of all non-empty blockHash -> podSet mappings
// across all shards for white-box unit testing.
func (i *indexer) hashToPods() map[blockHash]podSet {
	res := make(map[blockHash]podSet)
	i.blocks.ForEach(func(h blockHash, bs collections.Bitset) {
		if pods := i.podsFromBitset(bs); len(pods) > 0 {
			res[h] = pods
		}
	})
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
	ps.gone.Store(true)
	ps.lru.Purge()
	ps.count.Store(0)
	i.pods.Release(pod)
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
