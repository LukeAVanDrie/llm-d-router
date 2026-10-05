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
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// lruStore is the request-key LRU behind one lock the index owns, so a read
// path can refresh the recency of every key it visited under a single
// acquisition instead of one per key.
type lruStore struct {
	mu  sync.RWMutex
	lru *simplelru.LRU[BlockHash, *PodCache]
}

func newLRUStore(size int) (*lruStore, error) {
	lru, err := simplelru.NewLRU[BlockHash, *PodCache](size, nil)
	if err != nil {
		return nil, err
	}
	return &lruStore{lru: lru}, nil
}

// Get returns the key's cache and marks it most recently used.
func (s *lruStore) Get(key BlockHash) (*PodCache, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Get(key)
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

// Promote marks keys most recently used in order, so the last key ends up
// the most recent, under one acquisition. Absent keys are skipped.
func (s *lruStore) Promote(keys []BlockHash) {
	if len(keys) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		s.lru.Get(key)
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
			pc = &PodCache{capacity: podCacheSize}
			s.lru.Add(key, pc)
		}
		dst[i] = pc
	}
	return dst
}

func (s *lruStore) Add(key BlockHash, value *PodCache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Add(key, value)
}

// ContainsOrAdd reports whether key is present and, when it is not, adds
// value.
func (s *lruStore) ContainsOrAdd(key BlockHash, value *PodCache) (contains, evicted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lru.Contains(key) {
		return true, false
	}
	return false, s.lru.Add(key, value)
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
