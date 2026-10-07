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
	"slices"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
)

const (
	// lruShardThreshold is the minimum index capacity at which lruStore and
	// engineKeyStore partition keys across maxLRUShards shards and enable the
	// 8-way linear-probing fast table. Below this threshold, a single shard
	// preserves exact global LRU eviction order for small capacities.
	lruShardThreshold = 4096
	// maxLRUShards is the power-of-two shard count used when capacity reaches
	// lruShardThreshold.
	maxLRUShards = 256
	// minLRUFastSize and maxLRUFastSize bound the power-of-two 8-way
	// linear-probing PodCache lookup table used by sharded stores.
	minLRUFastSize = 1 << 16
	maxLRUFastSize = 1 << 22
)

type lruShard struct {
	mu   sync.RWMutex
	lru  *simplelru.LRU[BlockHash, *PodCache]
	size int
	_    [88]byte
}

// lruStore is the request-key LRU. Reads probe the lock-free FastTable first,
// falling back to shard shared locks on collisions or misses, and mark visited
// caches as referenced; writes promote referenced tail entries before evicting
// at capacity.
type lruStore struct {
	shards    []lruShard
	shardMask uint64
	fast      collections.FastTable[PodCache]
}

func mixBlockHash(key BlockHash) uint64 {
	h := (uint64(key) ^ (uint64(key) >> 30)) * 0xbf58476d1ce4e5b9
	return h ^ (h >> 27)
}

func newLRUStore(size int) (*lruStore, error) {
	if size <= 0 {
		if _, err := simplelru.NewLRU[BlockHash, *PodCache](size, nil); err != nil {
			return nil, err
		}
	}
	numShards := 1
	var fast collections.FastTable[PodCache]
	if size >= lruShardThreshold {
		numShards = maxLRUShards
		fastSize := min(max(minLRUFastSize, size), maxLRUFastSize)
		fast = collections.NewFastTable[PodCache](fastSize)
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
	}, nil
}

func (s *lruStore) shardFor(key BlockHash) *lruShard {
	if s.shardMask == 0 {
		return &s.shards[0]
	}
	return &s.shards[mixBlockHash(key)&s.shardMask]
}

func (s *lruStore) insertFast(h uint64, pc *PodCache) {
	if pc != nil {
		s.fast.Insert(h, pc)
	}
}

// lookupFast returns the resident PodCache for key in the fast table, or nil
// on a miss, hash collision, or evicted entry.
func (s *lruStore) lookupFast(h uint64, key BlockHash) *PodCache {
	pc, _ := s.fast.Probe(h, func(c *PodCache) bool { return c.key == key && c.resident.Load() })
	return pc
}

// probeFast returns the resident PodCache for key and whether the result is
// authoritative without consulting the shard LRU.
func (s *lruStore) probeFast(h uint64, key BlockHash) (*PodCache, bool) {
	return s.fast.Probe(h, func(c *PodCache) bool { return c.key == key && c.resident.Load() })
}

func (s *lruStore) touchLocked(pc *PodCache) {
	if pc == nil {
		return
	}
	pc.referenced.Store(false)
	pc.resident.Store(true)
	s.insertFast(mixBlockHash(pc.key), pc)
}

// reconcileShardLocked promotes referenced tail entries in sh.lru before a
// capacity eviction until the oldest entry has referenced == false. Caller
// must hold sh.mu.
func (s *lruStore) reconcileShardLocked(sh *lruShard) {
	for range sh.lru.Len() {
		_, oldestPC, ok := sh.lru.GetOldest()
		if !ok || oldestPC == nil || !oldestPC.referenced.CompareAndSwap(true, false) {
			return
		}
		sh.lru.Get(oldestPC.key)
	}
}

// Peek returns the key's cache without touching recency.
func (s *lruStore) Peek(key BlockHash) (*PodCache, bool) {
	pc := s.peekOne(key)
	return pc, pc != nil
}

// hasNonEmpty reports whether key is resident with at least one entry.
func (s *lruStore) hasNonEmpty(key BlockHash) bool {
	pc := s.peekOne(key)
	return pc != nil && pc.size() > 0
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
// the number of keys peeked. It stops after the first absent key so prefix
// walks that terminate on a miss do not look up unreachable keys.
func (s *lruStore) peekBatch(keys []BlockHash, dst []*PodCache) int {
	n := min(len(keys), len(dst))
	for i := range n {
		pc := s.peekOne(keys[i])
		dst[i] = pc
		if pc == nil {
			return i + 1
		}
	}
	return n
}

// stampBatch records read recency for every non-nil cache in caches in slice
// order. In sharded mode, it sets the lock-free referenced bit; in single-shard
// mode, it promotes resident keys directly in the shard LRU.
func (s *lruStore) stampBatch(caches []*PodCache) {
	if len(caches) == 0 {
		return
	}
	if s.shardMask != 0 {
		for _, pc := range caches {
			if pc != nil && !pc.referenced.Load() {
				pc.referenced.Store(true)
			}
		}
		return
	}
	sh := &s.shards[0]
	sh.mu.Lock()
	for _, pc := range caches {
		if pc != nil && pc.resident.Load() {
			sh.lru.Get(pc.key)
		}
	}
	sh.mu.Unlock()
}

// Promote marks keys most recently used in order, skipping absent keys.
func (s *lruStore) Promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	if s.shardMask != 0 {
		for _, key := range keys {
			if pc := s.peekOne(key); pc != nil && !pc.referenced.Load() {
				pc.referenced.Store(true)
			}
		}
		return
	}
	sh := &s.shards[0]
	sh.mu.Lock()
	for _, key := range keys {
		if pc, ok := sh.lru.Peek(key); ok && pc != nil && pc.resident.Load() {
			sh.lru.Get(key)
		}
	}
	sh.mu.Unlock()
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
				if !pc.referenced.Load() {
					pc.referenced.Store(true)
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
			s.touchLocked(pc)
			dst[i] = pc
		}
		if lockedShard != nil {
			lockedShard.mu.Unlock()
		}
	} else {
		var lockedShard *lruShard
		for i, key := range keys {
			sh := s.shardFor(key)
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
			s.touchLocked(pc)
			dst[i] = pc
		}
		if lockedShard != nil {
			lockedShard.mu.Unlock()
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
