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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// lruStore is the request-key LRU. Reads peek keys under a shared lock and
// stamp atomic read sequence numbers on visited caches; writes reconcile
// read sequence order before evicting at capacity.
type lruStore struct {
	mu    sync.RWMutex
	lru   *simplelru.LRU[BlockHash, *PodCache]
	size  int
	clock atomic.Uint64
}

func newLRUStore(size int) (*lruStore, error) {
	lru, err := simplelru.NewLRU[BlockHash, *PodCache](size, nil)
	if err != nil {
		return nil, err
	}
	return &lruStore{lru: lru, size: size}, nil
}

func stampReadSeq(pc *PodCache, seq uint64) {
	for {
		cur := pc.readSeq.Load()
		if cur >= seq || pc.readSeq.CompareAndSwap(cur, seq) {
			break
		}
	}
}

func (s *lruStore) touchLocked(key BlockHash, pc *PodCache) {
	if pc == nil {
		return
	}
	seq := s.clock.Add(1)
	pc.key = key
	pc.addedSeq = seq
	stampReadSeq(pc, seq)
}

type lruReconcileItem struct {
	pc       *PodCache
	readSeq  uint64
	addedSeq uint64
}

func (s *lruStore) reconcileReadsLocked() {
	_, oldestPC, ok := s.lru.GetOldest()
	if !ok || oldestPC == nil || oldestPC.readSeq.Load() <= oldestPC.addedSeq {
		return
	}

	values := s.lru.Values()
	var minDirtySeq uint64
	for _, pc := range values {
		if pc == nil {
			continue
		}
		if r := pc.readSeq.Load(); r > pc.addedSeq {
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
				addedSeq: pc.addedSeq,
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
		s.lru.Get(item.pc.key)
		item.pc.addedSeq = item.readSeq
	}
}

// Get returns the key's cache and marks it most recently used.
func (s *lruStore) Get(key BlockHash) (*PodCache, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pc, ok := s.lru.Get(key)
	if ok && pc != nil {
		s.touchLocked(key, pc)
	}
	return pc, ok
}

// Peek returns the key's cache without touching recency.
func (s *lruStore) Peek(key BlockHash) (*PodCache, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Peek(key)
}

// peekBatch looks up a prefix of keys (up to len(dst), stopping before any
// duplicate key in the batch) without touching recency, writes each found
// cache (or nil when absent) into dst, and returns the number of keys peeked.
func (s *lruStore) peekBatch(keys []BlockHash, dst []*PodCache) int {
	n := min(len(keys), len(dst))
	if n == 0 {
		return 0
	}
	for i := 1; i < n; i++ {
		k := keys[i]
		for j := range i {
			if keys[j] == k {
				n = i
				break
			}
		}
		if n == i {
			break
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range n {
		pc, found := s.lru.Peek(keys[i])
		if found {
			dst[i] = pc
		} else {
			dst[i] = nil
		}
	}
	return n
}

// stampBatch marks every non-nil cache in caches as read in slice order
// without acquiring s.mu.
func (s *lruStore) stampBatch(caches []*PodCache) {
	count := 0
	for _, pc := range caches {
		if pc != nil {
			count++
		}
	}
	if count == 0 {
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
}

// Promote marks keys most recently used in order under a shared lock, skipping
// absent keys.
func (s *lruStore) Promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	end := s.clock.Add(uint64(len(keys)))
	seq := end - uint64(len(keys))
	for _, key := range keys {
		seq++
		if pc, ok := s.lru.Peek(key); ok && pc != nil {
			stampReadSeq(pc, seq)
		}
	}
}

// getOrAddBatch populates dst with the PodCache for each key in keys, creating
// and inserting a new one with capacity podCacheSize when absent, under a
// single lock acquisition.
func (s *lruStore) getOrAddBatch(keys []BlockHash, dst []*PodCache, podCacheSize int) []*PodCache {
	if cap(dst) < len(keys) {
		dst = make([]*PodCache, len(keys))
	} else {
		dst = dst[:len(keys)]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, key := range keys {
		pc, found := s.lru.Get(key)
		if !found || pc == nil {
			if s.lru.Len() >= s.size {
				s.reconcileReadsLocked()
			}
			pc = &PodCache{capacity: podCacheSize}
			s.lru.Add(key, pc)
		}
		s.touchLocked(key, pc)
		dst[i] = pc
	}
	return dst
}

func (s *lruStore) Add(key BlockHash, value *PodCache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lru.Contains(key) && s.lru.Len() >= s.size {
		s.reconcileReadsLocked()
	}
	s.lru.Add(key, value)
	s.touchLocked(key, value)
}

// ContainsOrAdd reports whether key is present and, when it is not, adds
// value.
func (s *lruStore) ContainsOrAdd(key BlockHash, value *PodCache) (contains, evicted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lru.Contains(key) {
		return true, false
	}
	if s.lru.Len() >= s.size {
		s.reconcileReadsLocked()
	}
	evicted = s.lru.Add(key, value)
	s.touchLocked(key, value)
	return false, evicted
}

func (s *lruStore) Remove(key BlockHash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Remove(key)
}

func (s *lruStore) Keys() []BlockHash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Keys()
}

func (s *lruStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lru.Len()
}
