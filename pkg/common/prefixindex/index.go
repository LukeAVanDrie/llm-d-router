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
	"sync/atomic"
)

// maxEndpointOrdinals bounds how far writes grow the endpoint table, so a
// corrupt ordinal cannot allocate without limit.
const maxEndpointOrdinals = 1 << 16

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
	// Endpoints is the initial number of endpoint slots (values <= 0 default
	// to 128). Writes to a larger ordinal grow the table. Endpoints in
	// [0, MaxEndpoints) are indexed in the shared root-holder bitmask;
	// ordinals >= MaxEndpoints are probed directly.
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
//
// Writers hold mu and advance version before and after each write, so the
// version is odd while a write is in progress. Readers may run a query without
// mu and keep the result only when the version was even and unchanged across
// it; see tryRun.
type endpointSlot[K ~uint64] struct {
	mu      sync.RWMutex
	version atomic.Uint64
	set     *ChainSet[K]
	_       [24]byte
}

func (es *endpointSlot[K]) lockWrite() {
	es.mu.Lock()
	es.version.Add(1)
}

func (es *endpointSlot[K]) unlockWrite() {
	es.version.Add(1)
	es.mu.Unlock()
}

// optimisticAttempts bounds the lock-free tries before a query falls back to
// the read lock.
const optimisticAttempts = 2

// tryRun matches keys without the endpoint lock. It reports ok, and the
// version the match reflects, only when no write overlapped it; the result is
// then exactly what the read lock would have given at that version. RunLength
// reads only the set's index, whose words are atomic.
func (es *endpointSlot[K]) tryRun(keys []K, hints []int) (run int, version uint64, ok bool) {
	for range optimisticAttempts {
		v := es.version.Load()
		if v&1 != 0 {
			return 0, 0, false
		}
		d := es.set.RunLength(keys, hints)
		if es.version.Load() == v {
			return d, v, true
		}
	}
	return 0, 0, false
}

type rootShard[K ~uint64] struct {
	mu      sync.RWMutex
	holders map[K]endpointBitset
}

// Index coordinates per-endpoint [ChainSet] instances with a 64-way sharded
// root-holder bitmask filter on k_0 and cross-endpoint threshold sharing on reads.
//
// Index is safe for concurrent use by multiple goroutines. Endpoints are
// addressed by dense integer ordinals. A write to an ordinal beyond the table
// grows it, up to 65536 endpoints; writes to negative or larger ordinals are
// ignored, and queries treat ordinals without a slot as holding nothing.
// Callers reusing an ordinal for a different endpoint call
// [Index.ClearEndpoint] first.
type Index[K ~uint64] struct {
	cfg        Config
	endpoints  atomic.Pointer[[]*endpointSlot[K]] // replaced whole under growMu
	growMu     sync.Mutex
	rootShards [64]rootShard[K]
}

// NewIndex returns a new concurrent multi-endpoint [Index] configured by cfg.
func NewIndex[K ~uint64](cfg Config) *Index[K] {
	if cfg.Endpoints <= 0 {
		cfg.Endpoints = 128
	}

	idx := &Index[K]{cfg: cfg}
	for i := range idx.rootShards {
		idx.rootShards[i].holders = make(map[K]endpointBitset, 32)
	}
	slots := make([]*endpointSlot[K], cfg.Endpoints)
	for ep := range slots {
		slots[ep] = idx.newSlot(ep)
	}
	idx.endpoints.Store(&slots)
	return idx
}

func (idx *Index[K]) newSlot(ep int) *endpointSlot[K] {
	var cb RootCallback[K]
	if !idx.cfg.DisableRootFilter && ep < MaxEndpoints {
		cb = func(rootKey K, held bool) {
			idx.setRootHolder(ep, rootKey, held)
		}
	}
	return &endpointSlot[K]{set: NewChainSet(idx.cfg.PerEndpointCapacity, cb)}
}

// slots returns the current endpoint table.
func (idx *Index[K]) slots() []*endpointSlot[K] {
	return *idx.endpoints.Load()
}

// slot returns endpoint's slot, or nil when the table has none.
func (idx *Index[K]) slot(endpoint int) *endpointSlot[K] {
	if s := idx.slots(); uint(endpoint) < uint(len(s)) {
		return s[endpoint]
	}
	return nil
}

// slotForWrite returns endpoint's slot, growing the table to hold it, or nil
// for an ordinal outside [0, maxEndpointOrdinals).
func (idx *Index[K]) slotForWrite(endpoint int) *endpointSlot[K] {
	if es := idx.slot(endpoint); es != nil {
		return es
	}
	if uint(endpoint) >= maxEndpointOrdinals {
		return nil
	}
	idx.growMu.Lock()
	defer idx.growMu.Unlock()
	cur := idx.slots()
	if endpoint < len(cur) {
		return cur[endpoint]
	}
	next := make([]*endpointSlot[K], min(max(endpoint+1, 2*len(cur)), maxEndpointOrdinals))
	copy(next, cur)
	for ep := len(cur); ep < len(next); ep++ {
		next[ep] = idx.newSlot(ep)
	}
	idx.endpoints.Store(&next)
	return next[endpoint]
}

func rootShardIdx[K ~uint64](rootKey K) uint64 {
	return (uint64(rootKey) * 0x9e3779b97f4a7c15) >> 58
}

func (idx *Index[K]) setRootHolder(endpoint int, rootKey K, held bool) {
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

func (idx *Index[K]) rootHolders(rootKey K) endpointBitset {
	sh := &idx.rootShards[rootShardIdx(rootKey)]
	sh.mu.RLock()
	mask := sh.holders[rootKey]
	sh.mu.RUnlock()
	return mask
}

// AddRootChain records a full prefix chain keys[0..L-1] starting at depth-0
// root keys[0] on endpoint (used on both the approximate routing path and
// root KV-event stores).
func (idx *Index[K]) AddRootChain(endpoint int, keys []K) {
	if len(keys) == 0 {
		return
	}
	es := idx.slotForWrite(endpoint)
	if es == nil {
		return
	}
	es.lockWrite()
	es.set.AddRootChain(keys)
	es.unlockWrite()
}

// AddChainUnder records a continuation segment keys[0..L-1] under parentKey on
// endpoint (used on the precise KV-event path). If parentKey is not currently
// resident on endpoint, new blocks in keys are stored as Dormant until the
// missing ancestor chain is restored. Segments that start at a root go through
// [Index.AddRootChain]; see [ChainSet.AddChainUnder].
func (idx *Index[K]) AddChainUnder(endpoint int, parentKey K, keys []K) {
	if len(keys) == 0 {
		return
	}
	es := idx.slotForWrite(endpoint)
	if es == nil {
		return
	}
	es.lockWrite()
	es.set.AddChainUnder(parentKey, keys)
	es.unlockWrite()
}

// Remove deletes a single block key from endpoint (precise KV-event removal).
// It reports whether key was resident on endpoint.
func (idx *Index[K]) Remove(endpoint int, key K) bool {
	es := idx.slot(endpoint)
	if es == nil {
		return false
	}
	es.lockWrite()
	ok := es.set.Remove(key)
	es.unlockWrite()
	return ok
}

// SetEndpointCapacity bounds the resident blocks on endpoint (0 means
// unlimited), typically at the engine's reported block capacity.
func (idx *Index[K]) SetEndpointCapacity(endpoint, capacity int) {
	es := idx.slotForWrite(endpoint)
	if es == nil {
		return
	}
	es.lockWrite()
	es.set.SetCapacity(capacity)
	es.unlockWrite()
}

// ClearEndpoint removes all resident blocks on endpoint and clears its root
// bitmask bits across all shards.
func (idx *Index[K]) ClearEndpoint(endpoint int) {
	es := idx.slot(endpoint)
	if es == nil {
		return
	}
	es.lockWrite()
	es.set.Clear()
	es.unlockWrite()
}

// Update runs fn on endpoint's ChainSet under the endpoint's write lock,
// growing the table to hold endpoint. Callers use it to apply several
// mutations, and to change their own per-endpoint state, atomically with
// respect to queries. fn must not call back into idx.
func (idx *Index[K]) Update(endpoint int, fn func(s *ChainSet[K])) {
	es := idx.slotForWrite(endpoint)
	if es == nil {
		return
	}
	es.lockWrite()
	defer es.unlockWrite()
	fn(es.set)
}

// View runs fn on endpoint's ChainSet under the endpoint's read lock. fn
// must not mutate the set or call back into idx. View reports whether the
// endpoint has a slot.
func (idx *Index[K]) View(endpoint int, fn func(s *ChainSet[K])) bool {
	es := idx.slot(endpoint)
	if es == nil {
		return false
	}
	es.mu.RLock()
	defer es.mu.RUnlock()
	fn(es.set)
	return true
}

// runAndVisit matches keys on es and calls visit for a nonzero match, under
// es's read lock, which a panicking visit still releases.
func (es *endpointSlot[K]) runAndVisit(keys []K, hints []int, i int, visit func(i, run int, s *ChainSet[K])) int {
	es.mu.RLock()
	defer es.mu.RUnlock()
	d := es.set.RunLength(keys, hints)
	if d > 0 {
		visit(i, d, es.set)
	}
	return d
}

// Contains reports whether key is resident on endpoint (Visible or Dormant).
func (idx *Index[K]) Contains(endpoint int, key K) bool {
	es := idx.slot(endpoint)
	if es == nil {
		return false
	}
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.set.Contains(key)
}

// Len returns the number of resident blocks on endpoint.
func (idx *Index[K]) Len(endpoint int) int {
	es := idx.slot(endpoint)
	if es == nil {
		return 0
	}
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.set.Len()
}

// Stats returns aggregate observability metrics across all endpoints and root shards.
func (idx *Index[K]) Stats() Stats {
	var st Stats
	for _, es := range idx.slots() {
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
func (idx *Index[K]) Match(keys []K, candidates []int) []int {
	return idx.MatchInto(keys, candidates, nil)
}

// MatchInto writes the contiguous Visible prefix match length for each
// candidate endpoint into out (allocating only if cap(out) < len(candidates)),
// where out[i] corresponds to candidates[i]. MatchInto is read-only and does
// not update recency stamps.
func (idx *Index[K]) MatchInto(keys []K, candidates []int, out []int) []int {
	return idx.MatchVisit(keys, candidates, out, nil)
}

// MatchVisit is [Index.MatchInto] that also calls visit, when non-nil, for
// each candidate with a nonzero match, under that endpoint's read lock. visit
// receives the candidate's position in candidates, its match length, and its
// ChainSet, from which it may read classes; it must not mutate the set or
// call back into idx.
func (idx *Index[K]) MatchVisit(keys []K, candidates []int, out []int, visit func(i, run int, s *ChainSet[K])) []int {
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

	slots := idx.slots()
	var holders endpointBitset
	useFilter := !idx.cfg.DisableRootFilter
	if useFilter {
		holders = idx.rootHolders(keys[0])
		if holders == (endpointBitset{}) && len(slots) <= MaxEndpoints {
			return out
		}
	}

	var hintsBuf [8]int
	numHints := 0

	for i, ep := range candidates {
		if uint(ep) >= uint(len(slots)) {
			continue
		}
		if useFilter && ep < MaxEndpoints && !holders.test(ep) {
			continue
		}
		es := slots[ep]
		var hints []int
		if !idx.cfg.DisableThresholdSharing {
			hints = hintsBuf[:numHints]
		}
		var d int
		if visit == nil {
			var ok bool
			if d, _, ok = es.tryRun(keys, hints); !ok {
				es.mu.RLock()
				d = es.set.RunLength(keys, hints)
				es.mu.RUnlock()
			}
		} else {
			d = es.runAndVisit(keys, hints, i, visit)
		}
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
