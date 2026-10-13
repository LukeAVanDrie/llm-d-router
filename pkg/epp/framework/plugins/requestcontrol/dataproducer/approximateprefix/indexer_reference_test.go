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

package approximateprefix

// lruIndexer is the simplelru indexer that the prefixindex-based indexer
// replaced, kept verbatim (less its size reporting) as the reference for the
// differential test.

import (
	"context"
	"maps"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
)

// lruIndexer implements the indexerInterface interface. It keeps one LRU of block
// hashes per pod and answers prefix queries per candidate pod.
type lruIndexer struct {
	mu             sync.RWMutex // guards pods; each lruPodCache guards its own LRU
	pods           map[ServerID]*lruPodCache
	defaultLRUSize int
	pluginName     string
	pluginType     string
}

type lruPodCache struct {
	mu  sync.RWMutex
	lru *simplelru.LRU[blockHash, struct{}]
}

// newIndexer initializes an lruIndexer with size limits and starts cache size reporting.
func newLRUIndexer(ctx context.Context, defaultLRUSize int, pluginName, pluginType string) *lruIndexer {
	i := &lruIndexer{
		pods:           make(map[ServerID]*lruPodCache),
		defaultLRUSize: defaultLRUSize,
		pluginName:     pluginName,
		pluginType:     pluginType,
	}

	_ = ctx
	return i
}

func (i *lruIndexer) lruPodCacheFor(pod server) *lruPodCache {
	i.mu.RLock()
	pc := i.pods[pod.ServerID]
	i.mu.RUnlock()
	if pc != nil {
		return pc
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if pc = i.pods[pod.ServerID]; pc == nil {
		lruSize := pod.NumOfGPUBlocks
		if lruSize <= 0 {
			lruSize = i.defaultLRUSize
		}
		// We ignore the error since the only possible error is if size <= 0.
		l, _ := simplelru.NewLRU[blockHash, struct{}](lruSize, nil)
		pc = &lruPodCache{lru: l}
		i.pods[pod.ServerID] = pc
	}
	return pc
}

// Add inserts hashes tail-first: matching is anchored at the first block, so
// the leading blocks are the most valuable entries and must stay cached
// longest; an oversized batch naturally keeps exactly its leading blocks.
// Because block hashes are chained, every Add that inserts block j of a prompt
// also inserts blocks 0..j-1 after it, so within a pod's LRU the blocks of any
// prompt that remain cached form a contiguous run from block 0, which is what
// the galloping search in MatchLongestPrefix requires.
//
// If RemovePod runs between looking up the pod's cache and inserting, the
// insert lands in the removed cache and is discarded with it, as if Add had
// completed before RemovePod.
func (i *lruIndexer) Add(hashes []blockHash, pod server) {
	pc := i.lruPodCacheFor(pod)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for idx := len(hashes) - 1; idx >= 0; idx-- {
		pc.lru.Add(hashes[idx], struct{}{})
	}
}

// MatchLongestPrefix finds each candidate's run by galloping search over its
// LRU.
func (i *lruIndexer) MatchLongestPrefix(hashes []blockHash, candidates []ServerID) []int {
	res := make([]int, len(candidates))
	if len(hashes) == 0 {
		return res
	}
	// Release i.mu before waiting on pod locks, so a pending RemovePod or new
	// pod does not stall other queries behind an in-progress Add.
	caches := make([]*lruPodCache, len(candidates))
	i.mu.RLock()
	for j, c := range candidates {
		caches[j] = i.pods[c]
	}
	i.mu.RUnlock()

	for j, pc := range caches {
		if pc == nil {
			continue
		}
		pc.mu.RLock()
		res[j] = lruRunLength(pc.lru, hashes)
		pc.mu.RUnlock()
	}
	return res
}

// runLength returns the largest n such that the LRU holds hashes[n-1], relying
// on the contiguous run described on Add. It uses only Contains, which does not
// update recency, so callers need only the pod's read lock.
func lruRunLength(l *simplelru.LRU[blockHash, struct{}], hashes []blockHash) int {
	if !l.Contains(hashes[0]) {
		return 0
	}
	// held is a length known to be held; missing is one known not to be.
	held, missing := 1, len(hashes)+1
	for step := 1; held+step < missing; step *= 2 {
		if !l.Contains(hashes[held+step-1]) {
			missing = held + step
			break
		}
		held += step
	}
	for missing-held > 1 {
		mid := held + (missing-held)/2
		if l.Contains(hashes[mid-1]) {
			held = mid
		} else {
			missing = mid
		}
	}
	return held
}

// RemovePod removes a pod and its associated entries from the lruIndexer.
func (i *lruIndexer) RemovePod(pod ServerID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.pods, pod)
}

// Pods returns the list of all pods currently tracked in the lruIndexer.
func (i *lruIndexer) Pods() []ServerID {
	i.mu.RLock()
	defer i.mu.RUnlock()

	pods := make([]ServerID, 0, len(i.pods))
	for pod := range i.pods {
		pods = append(pods, pod)
	}
	return pods
}

// PodBlockCounts returns the number of cached blocks currently tracked per pod.
func (i *lruIndexer) PodBlockCounts() map[ServerID]int {
	i.mu.RLock()
	caches := maps.Clone(i.pods)
	i.mu.RUnlock()

	counts := make(map[ServerID]int, len(caches))
	for pod, pc := range caches {
		pc.mu.RLock()
		counts[pod] = pc.lru.Len()
		pc.mu.RUnlock()
	}
	return counts
}
