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

package collections

import (
	"sync"
	"sync/atomic"
)

const (
	numBitsetShardBits     = 8
	numBitsetShards        = 1 << numBitsetShardBits
	defaultShardCompactMin = 16384
)

// BitsetEntry associates a comparable key K with a lock-free AtomicBitset of
// ordinals and an atomic eviction flag used during lazy shard compaction.
type BitsetEntry[K comparable] struct {
	key     K
	pods    AtomicBitset
	evicted atomic.Bool
}

// Key returns the immutable key for this entry.
func (e *BitsetEntry[K]) Key() K {
	return e.key
}

// Pods returns the underlying AtomicBitset for this entry.
func (e *BitsetEntry[K]) Pods() *AtomicBitset {
	return &e.pods
}

// Snapshot returns a non-atomic Bitset copy of the ordinals set on this entry.
func (e *BitsetEntry[K]) Snapshot() Bitset {
	return e.pods.Snapshot()
}

// Set atomically adds ord to this entry's bitset.
func (e *BitsetEntry[K]) Set(ord int) {
	e.pods.Set(ord)
}

// Clear atomically removes ord from this entry's bitset.
func (e *BitsetEntry[K]) Clear(ord int) {
	e.pods.Clear(ord)
}

// Evicted reports whether this entry has been detached from its shard map by
// lazy dead-entry compaction.
func (e *BitsetEntry[K]) Evicted() bool {
	return e.evicted.Load()
}

type bitsetShard[K comparable] struct {
	mu        sync.RWMutex
	m         map[K]*BitsetEntry[K]
	compactAt int
	_         [88]byte
}

// ShardedBitsetIndex maps comparable keys to BitsetEntry values across 256
// RWMutex-protected shards fronted by a lock-free FastTable. Empty entries left
// behind when all ordinals are cleared via per-pod LRU eviction callbacks are
// reclaimed lazily when a shard's map reaches its compaction threshold.
type ShardedBitsetIndex[K comparable] struct {
	shards     [numBitsetShards]bitsetShard[K]
	fast       FastTable[BitsetEntry[K]]
	hashFn     func(K) uint64
	minCompact int
}

// NewShardedBitsetIndex returns a ShardedBitsetIndex with a FastTable of at
// least fastSize slots and a minimum per-shard compaction threshold of
// minCompact entries.
func NewShardedBitsetIndex[K comparable](fastSize, minCompact int, hashFn func(K) uint64) *ShardedBitsetIndex[K] {
	if minCompact <= 0 {
		minCompact = defaultShardCompactMin
	}
	idx := &ShardedBitsetIndex[K]{
		fast:       NewFastTable[BitsetEntry[K]](fastSize),
		hashFn:     hashFn,
		minCompact: minCompact,
	}
	for s := range numBitsetShards {
		idx.shards[s].m = make(map[K]*BitsetEntry[K])
		idx.shards[s].compactAt = minCompact
	}
	return idx
}

func (idx *ShardedBitsetIndex[K]) shard(h64 uint64) *bitsetShard[K] {
	return &idx.shards[(h64*0x9e3779b97f4a7c15)>>(64-numBitsetShardBits)]
}

// Lookup returns the BitsetEntry for key, or nil if absent.
func (idx *ShardedBitsetIndex[K]) Lookup(key K) *BitsetEntry[K] {
	return idx.LookupWithHash(key, idx.hashFn(key))
}

// LookupWithHash returns the BitsetEntry for key using the caller-supplied
// 64-bit hash h64, probing the lock-free FastTable before falling back to a
// shard read lock.
func (idx *ShardedBitsetIndex[K]) LookupWithHash(key K, h64 uint64) *BitsetEntry[K] {
	if e, ok := idx.fast.Probe(h64, func(e *BitsetEntry[K]) bool {
		return e.key == key && !e.evicted.Load()
	}); ok {
		return e
	}
	sh := idx.shard(h64)
	sh.mu.RLock()
	e := sh.m[key]
	sh.mu.RUnlock()
	return e
}

func (idx *ShardedBitsetIndex[K]) compactShardLocked(sh *bitsetShard[K]) {
	idx.fast.MarkOverflow()
	for k, e := range sh.m {
		if e.pods.IsEmpty() {
			e.evicted.Store(true)
			if e.pods.IsEmpty() {
				delete(sh.m, k)
				idx.fast.Delete(idx.hashFn(k), e)
			} else {
				e.evicted.Store(false)
			}
		}
	}
	sh.compactAt = max(idx.minCompact, len(sh.m)*2)
}

// SetBit sets ord on the BitsetEntry for key, creating the entry if absent, and
// returns the active entry.
func (idx *ShardedBitsetIndex[K]) SetBit(key K, ord int) *BitsetEntry[K] {
	return idx.SetBitWithHash(key, idx.hashFn(key), ord)
}

// SetBitWithHash sets ord on the BitsetEntry for key using the caller-supplied
// 64-bit hash h64, creating the entry under the shard write lock on a miss or
// concurrent compaction race.
func (idx *ShardedBitsetIndex[K]) SetBitWithHash(key K, h64 uint64, ord int) *BitsetEntry[K] {
	if e := idx.LookupWithHash(key, h64); e != nil {
		e.pods.Set(ord)
		if !e.evicted.Load() {
			return e
		}
		e.pods.Clear(ord)
	}

	sh := idx.shard(h64)
	sh.mu.Lock()
	e := sh.m[key]
	if e == nil {
		if len(sh.m) >= sh.compactAt {
			idx.compactShardLocked(sh)
		}
		e = &BitsetEntry[K]{key: key}
		sh.m[key] = e
	}
	e.pods.Set(ord)
	idx.fast.Insert(h64, e)
	sh.mu.Unlock()
	return e
}

// ForEach calls fn for every entry across all shards whose bitset snapshot is
// non-empty.
func (idx *ShardedBitsetIndex[K]) ForEach(fn func(K, Bitset)) {
	for s := range numBitsetShards {
		sh := &idx.shards[s]
		sh.mu.RLock()
		for k, e := range sh.m {
			if bs := e.pods.Snapshot(); !bs.IsEmpty() {
				fn(k, bs)
			}
		}
		sh.mu.RUnlock()
	}
}
