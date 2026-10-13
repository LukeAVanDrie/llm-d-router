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

package prefixindex

import (
	"slices"
	"sync"
)

// MaxEndpoints is the bit-width of the shared root-holder bitmask filter
// (256 endpoints represented as an inline [4]uint64 bitmask). Endpoints with
// ordinals in [0, MaxEndpoints) use the root-holder filter; endpoints with
// ordinals >= MaxEndpoints fall back to direct per-endpoint probing.
const MaxEndpoints = 256

// endpointBitset is a 256-endpoint bitmask stored in 4 uint64 words (zero pointers).
type endpointBitset [4]uint64

func (b *endpointBitset) set(ep int)   { b[ep>>6] |= 1 << (ep & 63) }
func (b *endpointBitset) clear(ep int) { b[ep>>6] &^= 1 << (ep & 63) }
func (b endpointBitset) test(ep int) bool {
	return uint(ep) < MaxEndpoints && (b[ep>>6]&(1<<(ep&63))) != 0
}

// Config configures a concurrent multi-endpoint [Index].
type Config struct {
	// Endpoints is the number of endpoint slots (values <= 0 default to 128).
	// Endpoints in [0, MaxEndpoints) are indexed in the shared root-holder
	// bitmask; ordinals >= MaxEndpoints are probed directly.
	Endpoints int

	// PerEndpointCapacity is the maximum number of resident blocks per endpoint
	// before leaf-LRU eviction runs (0 means unlimited).
	PerEndpointCapacity int

	// DisableRootFilter disables the shared k_0 root-holder bitmask filter.
	DisableRootFilter bool

	// DisableThresholdSharing disables cross-endpoint match-depth hints during
	// [Index.Match] and [Index.MatchInto].
	DisableThresholdSharing bool
}

// Stats reports aggregate observability metrics across all endpoints in an [Index].
//
// Under leaf-first engine eviction and in-order event delivery, DormantBlocks
// and Rebuilds remain 0; non-zero values surface out-of-order evictions or
// dropped store events.
type Stats struct {
	HeldBlocks    int
	DormantBlocks int
	RootKeys      int
	Evictions     uint64
	Rebuilds      uint64
}

// endpointSlot is padded to 64 bytes so neighboring endpoints' locks do not
// share a cache line.
type endpointSlot struct {
	mu  sync.RWMutex
	set *ChainSet
	_   [32]byte
}

type rootShard struct {
	mu      sync.RWMutex
	holders map[uint64]endpointBitset
}

// Index coordinates per-endpoint [ChainSet] instances with a 64-way sharded
// root-holder bitmask filter on k_0 and cross-endpoint threshold sharing on reads.
//
// Index is safe for concurrent use by multiple goroutines. Endpoints are
// addressed by dense integer ordinals in [0, Endpoints); out-of-range ordinals
// are ignored.
type Index struct {
	cfg        Config
	endpoints  []endpointSlot
	rootShards [64]rootShard
}

// NewIndex returns a new concurrent multi-endpoint [Index] configured by cfg.
func NewIndex(cfg Config) *Index {
	if cfg.Endpoints <= 0 {
		cfg.Endpoints = 128
	}

	idx := &Index{
		cfg:       cfg,
		endpoints: make([]endpointSlot, cfg.Endpoints),
	}
	for i := range idx.rootShards {
		idx.rootShards[i].holders = make(map[uint64]endpointBitset, 32)
	}
	for ep := range idx.endpoints {
		endpointID := ep
		var cb RootCallback
		if !cfg.DisableRootFilter && ep < MaxEndpoints {
			cb = func(rootKey uint64, held bool) {
				idx.setRootHolder(endpointID, rootKey, held)
			}
		}
		idx.endpoints[ep].set = NewChainSet(cfg.PerEndpointCapacity, cb)
	}
	return idx
}

func rootShardIdx(rootKey uint64) uint64 {
	return (rootKey * 0x9e3779b97f4a7c15) >> 58
}

func (idx *Index) setRootHolder(endpoint int, rootKey uint64, held bool) {
	sh := &idx.rootShards[rootShardIdx(rootKey)]
	sh.mu.Lock()
	mask := sh.holders[rootKey]
	if held {
		mask.set(endpoint)
		sh.holders[rootKey] = mask
	} else {
		mask.clear(endpoint)
		if mask == (endpointBitset{}) {
			delete(sh.holders, rootKey)
		} else {
			sh.holders[rootKey] = mask
		}
	}
	sh.mu.Unlock()
}

func (idx *Index) rootHolders(rootKey uint64) endpointBitset {
	sh := &idx.rootShards[rootShardIdx(rootKey)]
	sh.mu.RLock()
	mask := sh.holders[rootKey]
	sh.mu.RUnlock()
	return mask
}

// AddRootChain records a full prefix chain keys[0..L-1] starting at depth-0
// root keys[0] on endpoint (used on both the approximate routing path and
// root KV-event stores).
func (idx *Index) AddRootChain(endpoint int, keys []uint64) {
	if uint(endpoint) >= uint(len(idx.endpoints)) || len(keys) == 0 {
		return
	}
	es := &idx.endpoints[endpoint]
	es.mu.Lock()
	es.set.AddRootChain(keys)
	es.mu.Unlock()
}

// AddChainUnder records a continuation segment keys[0..L-1] under parentKey on
// endpoint (used on the precise KV-event path). If parentKey is not currently
// resident on endpoint, new blocks in keys are stored as Dormant until the
// missing ancestor chain is restored. Segments that start at a root go through
// [Index.AddRootChain]; see [ChainSet.AddChainUnder].
func (idx *Index) AddChainUnder(endpoint int, parentKey uint64, keys []uint64) {
	if uint(endpoint) >= uint(len(idx.endpoints)) || len(keys) == 0 {
		return
	}
	es := &idx.endpoints[endpoint]
	es.mu.Lock()
	es.set.AddChainUnder(parentKey, keys)
	es.mu.Unlock()
}

// Remove deletes a single block key from endpoint (precise KV-event removal).
// It reports whether key was resident on endpoint.
func (idx *Index) Remove(endpoint int, key uint64) bool {
	if uint(endpoint) >= uint(len(idx.endpoints)) {
		return false
	}
	es := &idx.endpoints[endpoint]
	es.mu.Lock()
	ok := es.set.Remove(key)
	es.mu.Unlock()
	return ok
}

// SetEndpointCapacity bounds the resident blocks on endpoint (0 means
// unlimited), typically at the engine's reported block capacity.
func (idx *Index) SetEndpointCapacity(endpoint, capacity int) {
	if uint(endpoint) >= uint(len(idx.endpoints)) {
		return
	}
	es := &idx.endpoints[endpoint]
	es.mu.Lock()
	es.set.SetCapacity(capacity)
	es.mu.Unlock()
}

// ClearEndpoint removes all resident blocks on endpoint and clears its root
// bitmask bits across all shards.
func (idx *Index) ClearEndpoint(endpoint int) {
	if uint(endpoint) >= uint(len(idx.endpoints)) {
		return
	}
	es := &idx.endpoints[endpoint]
	es.mu.Lock()
	es.set.Clear()
	es.mu.Unlock()
}

// Stats returns aggregate observability metrics across all endpoints and root shards.
func (idx *Index) Stats() Stats {
	var st Stats
	for ep := range idx.endpoints {
		es := &idx.endpoints[ep]
		es.mu.RLock()
		st.HeldBlocks += es.set.Len()
		st.DormantBlocks += es.set.DormantCount()
		st.Evictions += es.set.Evictions()
		st.Rebuilds += es.set.Rebuilds()
		es.mu.RUnlock()
	}
	for i := range idx.rootShards {
		sh := &idx.rootShards[i]
		sh.mu.RLock()
		st.RootKeys += len(sh.holders)
		sh.mu.RUnlock()
	}
	return st
}

// Match returns a newly allocated slice containing the contiguous Visible
// prefix match length (in blocks, 0..len(keys)) for each candidate endpoint in
// candidates. Match is read-only and does not update recency stamps.
func (idx *Index) Match(keys []uint64, candidates []int) []int {
	return idx.MatchInto(keys, candidates, nil)
}

// MatchInto writes the contiguous Visible prefix match length for each
// candidate endpoint into out (allocating only if cap(out) < len(candidates)),
// where out[i] corresponds to candidates[i]. MatchInto is read-only and does
// not update recency stamps.
func (idx *Index) MatchInto(keys []uint64, candidates []int, out []int) []int {
	n := len(candidates)
	if cap(out) < n {
		out = make([]int, n)
	} else {
		out = out[:n]
		clear(out)
	}
	if len(keys) == 0 || n == 0 {
		return out
	}

	var holders endpointBitset
	useFilter := !idx.cfg.DisableRootFilter
	if useFilter {
		holders = idx.rootHolders(keys[0])
		if holders == (endpointBitset{}) && len(idx.endpoints) <= MaxEndpoints {
			return out
		}
	}

	var hintsBuf [8]int
	numHints := 0

	for i, ep := range candidates {
		if uint(ep) >= uint(len(idx.endpoints)) {
			continue
		}
		if useFilter && ep < MaxEndpoints && !holders.test(ep) {
			continue
		}
		es := &idx.endpoints[ep]
		var hints []int
		if !idx.cfg.DisableThresholdSharing {
			hints = hintsBuf[:numHints]
		}
		es.mu.RLock()
		d := es.set.RunLength(keys, hints)
		es.mu.RUnlock()
		out[i] = d

		if d > 0 && !idx.cfg.DisableThresholdSharing && numHints < len(hintsBuf) {
			if !slices.Contains(hintsBuf[:numHints], d) {
				hintsBuf[numHints] = d
				numHints++
			}
		}
	}
	return out
}
