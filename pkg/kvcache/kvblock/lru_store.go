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

package kvblock

import (
	"cmp"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

const (
	// lruShardThreshold is the minimum index capacity at which lruStore and
	// engineKeyStore partition keys across maxLRUShards shards and enable the
	// direct-mapped fast table. Below this threshold, a single shard preserves
	// exact global LRU eviction order for small capacities.
	lruShardThreshold = 4096
	// maxLRUShards is the power-of-two shard count used when capacity reaches
	// lruShardThreshold.
	maxLRUShards = 256
	// minLRUFastSize and maxLRUFastSize bound the power-of-two direct-mapped
	// atomic PodCache lookup table used by sharded stores.
	minLRUFastSize   = 1 << 16
	maxLRUFastSize   = 1 << 22
	maxLRUFastProbes = 8
)

type lruShard struct {
	mu   sync.RWMutex
	lru  *simplelru.LRU[BlockHash, *PodCache]
	size int
	_    [88]byte
}

// lruStore is the request-key LRU. Reads follow cached prefix chains or
// direct-mapped resident pointers lock-free, falling back to shard shared locks
// on collisions or misses, and stamp atomic read sequence numbers on visited
// caches; writes reconcile read sequence order before evicting at capacity.
type lruStore struct {
	shards       []lruShard
	shardMask    uint64
	fast         []atomic.Pointer[PodCache]
	fastMask     uint64
	fastOverflow atomic.Bool
	clock        atomic.Uint64
	writeSeq     atomic.Uint64
}

func mixBlockHash(key BlockHash) uint64 {
	h := (uint64(key) ^ (uint64(key) >> 30)) * 0xbf58476d1ce4e5b9
	return h ^ (h >> 27)
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len64(uint64(n-1))
}

func newLRUStore(size int) (*lruStore, error) {
	if size <= 0 {
		if _, err := simplelru.NewLRU[BlockHash, *PodCache](size, nil); err != nil {
			return nil, err
		}
	}
	numShards := 1
	var fast []atomic.Pointer[PodCache]
	var fastMask uint64
	if size >= lruShardThreshold {
		numShards = maxLRUShards
		fastSize := min(max(minLRUFastSize, nextPow2(size)), maxLRUFastSize)
		fast = make([]atomic.Pointer[PodCache], fastSize)
		fastMask = uint64(fastSize - 1)
	}
	shardSize := max(1, (size+numShards-1)/numShards)
	shards := make([]lruShard, numShards)
	onEvict := func(_ BlockHash, pc *PodCache) {
		if pc != nil {
			pc.resident.Store(false)
		}
	}
	for i := range shards {
		lru, err := simplelru.NewLRU[BlockHash, *PodCache](shardSize, onEvict)
		if err != nil {
			return nil, err
		}
		shards[i] = lruShard{lru: lru, size: shardSize}
	}
	return &lruStore{
		shards:    shards,
		shardMask: uint64(numShards - 1),
		fast:      fast,
		fastMask:  fastMask,
	}, nil
}

func (s *lruStore) shardFor(key BlockHash) *lruShard {
	if s.shardMask == 0 {
		return &s.shards[0]
	}
	return &s.shards[mixBlockHash(key)&s.shardMask]
}

func (s *lruStore) insertFast(h uint64, pc *PodCache) {
	if len(s.fast) == 0 || pc == nil {
		return
	}
	base := h & s.fastMask
	for p := uint64(0); p < maxLRUFastProbes; p++ {
		idx := (base + p) & s.fastMask
		cur := s.fast[idx].Load()
		if cur == pc {
			return
		}
		if cur == nil {
			if s.fast[idx].CompareAndSwap(nil, pc) {
				return
			}
			if s.fast[idx].Load() == pc {
				return
			}
		}
	}
	s.fastOverflow.Store(true)
	s.fast[base].Store(pc)
}

// lookupFast returns the resident PodCache for key in the fast table, or nil
// on a miss, hash collision, or evicted entry.
func (s *lruStore) lookupFast(h uint64, key BlockHash) *PodCache {
	if len(s.fast) == 0 {
		return nil
	}
	base := h & s.fastMask
	for p := uint64(0); p < maxLRUFastProbes; p++ {
		pc := s.fast[(base+p)&s.fastMask].Load()
		if pc == nil {
			return nil
		}
		if pc.key == key {
			if pc.resident.Load() {
				return pc
			}
			return nil
		}
	}
	return nil
}

// probeFast returns the resident PodCache for key and whether the result is
// authoritative without consulting the shard LRU.
func (s *lruStore) probeFast(h uint64, key BlockHash) (*PodCache, bool) {
	if len(s.fast) == 0 {
		return nil, false
	}
	base := h & s.fastMask
	overflow := s.fastOverflow.Load()
	for p := uint64(0); p < maxLRUFastProbes; p++ {
		pc := s.fast[(base+p)&s.fastMask].Load()
		if pc == nil {
			return nil, !overflow
		}
		if pc.key == key {
			if pc.resident.Load() {
				return pc, true
			}
			return nil, !overflow
		}
	}
	return nil, false
}

func stampReadSeq(pc *PodCache, seq uint64) {
	for {
		cur := pc.readSeq.Load()
		if cur >= seq || pc.readSeq.CompareAndSwap(cur, seq) {
			break
		}
	}
}

func (s *lruStore) touchLocked(pc *PodCache) {
	if pc == nil {
		return
	}
	if s.shardMask != 0 {
		seq := pc.addedSeq.Load() + 1
		pc.addedSeq.Store(seq)
		pc.readSeq.Store(seq)
		pc.resident.Store(true)
		s.insertFast(mixBlockHash(pc.key), pc)
		return
	}
	seq := s.clock.Add(1)
	pc.addedSeq.Store(seq)
	pc.resident.Store(true)
	stampReadSeq(pc, seq)
	s.writeSeq.Store(seq)
	s.insertFast(mixBlockHash(pc.key), pc)
}

type lruReconcileItem struct {
	pc       *PodCache
	readSeq  uint64
	addedSeq uint64
}

// reconcileShardLocked replays unrecorded read recency into sh.lru before a
// capacity eviction. If the current LRU tail has readSeq <= addedSeq, it was
// not read after its last LRU placement and remains the true eviction victim
// without scanning the shard. In sharded mode, referenced tail entries are
// promoted in place without allocating; in single-shard mode, every entry with
// readSeq >= the minimum dirty readSeq is promoted in ascending (readSeq,
// addedSeq) order. Caller must hold sh.mu.
func (s *lruStore) reconcileShardLocked(sh *lruShard) {
	if s.shardMask != 0 {
		for range sh.lru.Len() {
			_, oldestPC, ok := sh.lru.GetOldest()
			if !ok || oldestPC == nil {
				return
			}
			r := oldestPC.readSeq.Load()
			if r <= oldestPC.addedSeq.Load() {
				return
			}
			sh.lru.Get(oldestPC.key)
			oldestPC.addedSeq.Store(r)
		}
		return
	}

	_, oldestPC, ok := sh.lru.GetOldest()
	if !ok || oldestPC == nil || oldestPC.readSeq.Load() <= oldestPC.addedSeq.Load() {
		return
	}

	values := sh.lru.Values()
	var minDirtySeq uint64
	for _, pc := range values {
		if pc == nil {
			continue
		}
		if r := pc.readSeq.Load(); r > pc.addedSeq.Load() {
			if minDirtySeq == 0 || r < minDirtySeq {
				minDirtySeq = r
			}
		}
	}
	if minDirtySeq == 0 {
		return
	}

	items := make([]lruReconcileItem, 0, len(values))
	for _, pc := range values {
		if pc == nil {
			continue
		}
		if r := pc.readSeq.Load(); r >= minDirtySeq {
			items = append(items, lruReconcileItem{
				pc:       pc,
				readSeq:  r,
				addedSeq: pc.addedSeq.Load(),
			})
		}
	}
	slices.SortFunc(items, func(a, b lruReconcileItem) int {
		if a.readSeq != b.readSeq {
			return cmp.Compare(a.readSeq, b.readSeq)
		}
		return cmp.Compare(a.addedSeq, b.addedSeq)
	})
	for _, item := range items {
		sh.lru.Get(item.pc.key)
		item.pc.addedSeq.Store(item.readSeq)
	}
	s.writeSeq.Store(s.clock.Load())
}

// Get returns the key's cache and marks it most recently used.
func (s *lruStore) Get(key BlockHash) (*PodCache, bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	pc, ok := sh.lru.Get(key)
	if ok && pc != nil {
		s.touchLocked(pc)
	}
	return pc, ok
}

// Peek returns the key's cache without touching recency.
func (s *lruStore) Peek(key BlockHash) (*PodCache, bool) {
	h := mixBlockHash(key)
	if pc, auth := s.probeFast(h, key); auth {
		return pc, pc != nil
	}
	sh := &s.shards[0]
	if s.shardMask != 0 {
		sh = &s.shards[h&s.shardMask]
	}
	sh.mu.RLock()
	pc, ok := sh.lru.Peek(key)
	if ok && pc != nil {
		s.insertFast(h, pc)
	}
	sh.mu.RUnlock()
	return pc, ok
}

// hasNonEmpty reports whether key is resident with at least one entry.
func (s *lruStore) hasNonEmpty(key BlockHash) bool {
	h := mixBlockHash(key)
	if pc, auth := s.probeFast(h, key); auth {
		return pc != nil && pc.size() > 0
	}
	sh := &s.shards[0]
	if s.shardMask != 0 {
		sh = &s.shards[h&s.shardMask]
	}
	sh.mu.RLock()
	pc, ok := sh.lru.Peek(key)
	sh.mu.RUnlock()
	return ok && pc != nil && pc.size() > 0
}

// peekOne returns the resident PodCache for k without touching recency.
func (s *lruStore) peekOne(k BlockHash) *PodCache {
	h := mixBlockHash(k)
	if pc, auth := s.probeFast(h, k); auth {
		return pc
	}
	sh := &s.shards[0]
	if s.shardMask != 0 {
		sh = &s.shards[h&s.shardMask]
	}
	sh.mu.RLock()
	foundPC, found := sh.lru.Peek(k)
	if found && foundPC != nil {
		s.insertFast(h, foundPC)
	}
	sh.mu.RUnlock()
	if found {
		return foundPC
	}
	return nil
}

// peekBatch looks up a prefix of keys (up to len(dst)) without touching
// recency, writes each found cache (or nil when absent) into dst, and returns
// the number of keys peeked and the next chained cache pointer. It stops after
// the first absent key so prefix walks that terminate on a miss do not look up
// unreachable keys.
func (s *lruStore) peekBatch(keys []BlockHash, dst []*PodCache, nextPC *PodCache) (int, *PodCache) {
	n := min(len(keys), len(dst))
	if n == 0 {
		return 0, nextPC
	}
	for i := range n {
		k := keys[i]
		var pc *PodCache
		if nextPC != nil && nextPC.key == k && nextPC.resident.Load() {
			pc = nextPC
		} else {
			pc = s.peekOne(k)
		}
		dst[i] = pc
		if pc == nil {
			return i + 1, nil
		}
		nextPC = pc.next.Load()
	}
	return n, nextPC
}

// stampBatch marks every non-nil cache in caches as read in slice order
// without acquiring shard locks. In sharded mode, stamping is skipped for any
// cache that already satisfies readSeq > addedSeq; in single-shard mode,
// stamping is skipped when readSeq values already form a strictly increasing
// sequence above *prevSeq.
func (s *lruStore) stampBatch(caches []*PodCache, prevSeq *uint64) {
	if s.shardMask != 0 {
		for _, pc := range caches {
			if pc != nil {
				if a := pc.addedSeq.Load(); pc.readSeq.Load() <= a {
					pc.readSeq.Store(a + 1)
				}
			}
		}
		return
	}

	count := 0
	p := *prevSeq
	needStamp := false
	for _, pc := range caches {
		if pc == nil {
			continue
		}
		count++
		if !needStamp {
			if r := pc.readSeq.Load(); r > p {
				p = r
			} else {
				needStamp = true
			}
		}
	}
	if count == 0 || !needStamp {
		if count > 0 {
			*prevSeq = p
		}
		return
	}
	end := s.clock.Add(uint64(count))
	seq := end - uint64(count)
	for _, pc := range caches {
		if pc != nil {
			seq++
			stampReadSeq(pc, seq)
		}
	}
	*prevSeq = end
}

// Promote marks keys most recently used in order, skipping absent keys.
func (s *lruStore) Promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	p := s.writeSeq.Load()
	needStamp := false
	var lockedShard *lruShard
	for _, key := range keys {
		if pc, auth := s.probeFast(mixBlockHash(key), key); auth {
			if pc != nil && pc.readSeq.Load() <= pc.addedSeq.Load() {
				needStamp = true
				break
			}
			continue
		}
		sh := s.shardFor(key)
		if sh != lockedShard {
			if lockedShard != nil {
				lockedShard.mu.RUnlock()
			}
			sh.mu.RLock()
			lockedShard = sh
		}
		if pc, ok := sh.lru.Peek(key); ok && pc != nil {
			if s.shardMask != 0 {
				if pc.readSeq.Load() <= pc.addedSeq.Load() {
					needStamp = true
					break
				}
			} else if r := pc.readSeq.Load(); r > p {
				p = r
			} else {
				needStamp = true
				break
			}
		}
	}
	if !needStamp {
		if lockedShard != nil {
			lockedShard.mu.RUnlock()
		}
		return
	}
	end := s.clock.Add(uint64(len(keys)))
	seq := end - uint64(len(keys))
	for _, key := range keys {
		seq++
		if pc, auth := s.probeFast(mixBlockHash(key), key); auth {
			if pc != nil {
				stampReadSeq(pc, seq)
			}
			continue
		}
		sh := s.shardFor(key)
		if sh != lockedShard {
			if lockedShard != nil {
				lockedShard.mu.RUnlock()
			}
			sh.mu.RLock()
			lockedShard = sh
		}
		if pc, ok := sh.lru.Peek(key); ok && pc != nil {
			stampReadSeq(pc, seq)
		}
	}
	if lockedShard != nil {
		lockedShard.mu.RUnlock()
	}
}

// getOrAddBatch populates dst with the PodCache for each key in keys, creating
// and inserting a new one with capacity podCacheSize when absent.
func (s *lruStore) getOrAddBatch(keys []BlockHash, dst []*PodCache, podCacheSize int) []*PodCache {
	if cap(dst) < len(keys) {
		dst = make([]*PodCache, len(keys))
	} else {
		dst = dst[:len(keys)]
	}
	if len(keys) == 0 {
		return dst
	}
	if s.shardMask != 0 && len(keys) < (1<<24) {
		var orderBuf [512]uint32
		var order []uint32
		if len(keys) <= len(orderBuf) {
			order = orderBuf[:0]
		} else {
			order = make([]uint32, 0, len(keys))
		}
		for i, key := range keys {
			h := mixBlockHash(key)
			if pc := s.lookupFast(h, key); pc != nil {
				dst[i] = pc
				if a := pc.addedSeq.Load(); pc.readSeq.Load() <= a {
					pc.readSeq.Store(a + 1)
				}
				continue
			}
			sIdx := uint32(h & s.shardMask)
			order = append(order, (sIdx<<24)|uint32(i))
		}
		if len(order) > 1 {
			slices.Sort(order)
		}
		var lockedShard *lruShard
		for _, packed := range order {
			sIdx := packed >> 24
			i := int(packed & 0xffffff)
			key := keys[i]
			sh := &s.shards[sIdx]
			if sh != lockedShard {
				if lockedShard != nil {
					lockedShard.mu.Unlock()
				}
				sh.mu.Lock()
				lockedShard = sh
			}
			pc, found := sh.lru.Get(key)
			if !found || pc == nil {
				if sh.lru.Len() >= sh.size {
					s.reconcileShardLocked(sh)
				}
				pc = &PodCache{capacity: podCacheSize, key: key}
				sh.lru.Add(key, pc)
			}
			seq := pc.addedSeq.Load() + 1
			pc.addedSeq.Store(seq)
			pc.readSeq.Store(seq)
			pc.resident.Store(true)
			s.insertFast(mixBlockHash(key), pc)
			dst[i] = pc
		}
		if lockedShard != nil {
			lockedShard.mu.Unlock()
		}
	} else {
		seqBase := s.clock.Add(uint64(len(keys))) - uint64(len(keys))
		var lockedShard *lruShard
		for i, key := range keys {
			h := mixBlockHash(key)
			sh := &s.shards[0]
			if s.shardMask != 0 {
				sh = &s.shards[h&s.shardMask]
			}
			if sh != lockedShard {
				if lockedShard != nil {
					lockedShard.mu.Unlock()
				}
				sh.mu.Lock()
				lockedShard = sh
			}
			pc, found := sh.lru.Get(key)
			if !found || pc == nil {
				if sh.lru.Len() >= sh.size {
					s.reconcileShardLocked(sh)
				}
				pc = &PodCache{capacity: podCacheSize, key: key}
				sh.lru.Add(key, pc)
			}
			seq := seqBase + uint64(i) + 1
			pc.addedSeq.Store(seq)
			pc.resident.Store(true)
			stampReadSeq(pc, seq)
			s.insertFast(h, pc)
			dst[i] = pc
		}
		if lockedShard != nil {
			lockedShard.mu.Unlock()
		}
		if s.shardMask == 0 {
			s.writeSeq.Store(seqBase + uint64(len(keys)))
		}
	}
	for i := 1; i < len(dst); i++ {
		if dst[i-1] != dst[i] && dst[i-1].next.Load() != dst[i] {
			dst[i-1].next.Store(dst[i])
		}
	}
	return dst
}

func (s *lruStore) Add(key BlockHash, value *PodCache) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if value != nil {
		value.key = key
	}
	if old, ok := sh.lru.Peek(key); ok && old != nil && old != value {
		old.resident.Store(false)
	} else if !ok && sh.lru.Len() >= sh.size {
		s.reconcileShardLocked(sh)
	}
	sh.lru.Add(key, value)
	s.touchLocked(value)
}

// ContainsOrAdd reports whether key is present and, when it is not, adds
// value.
func (s *lruStore) ContainsOrAdd(key BlockHash, value *PodCache) (contains, evicted bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.lru.Contains(key) {
		return true, false
	}
	if value != nil {
		value.key = key
	}
	if sh.lru.Len() >= sh.size {
		s.reconcileShardLocked(sh)
	}
	evicted = sh.lru.Add(key, value)
	s.touchLocked(value)
	return false, evicted
}

func (s *lruStore) Remove(key BlockHash) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if pc, ok := sh.lru.Peek(key); ok && pc != nil {
		pc.resident.Store(false)
	}
	return sh.lru.Remove(key)
}

func (s *lruStore) removeIfSame(key BlockHash, target *PodCache) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if cur, ok := sh.lru.Peek(key); ok && cur == target {
		cur.resident.Store(false)
		return sh.lru.Remove(key)
	}
	return false
}

func (s *lruStore) Keys() []BlockHash {
	var all []BlockHash
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		all = append(all, sh.lru.Keys()...)
		sh.mu.RUnlock()
	}
	return all
}

func (s *lruStore) Len() int {
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		total += sh.lru.Len()
		sh.mu.RUnlock()
	}
	return total
}
