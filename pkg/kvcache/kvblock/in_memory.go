/*
Copyright 2025 The llm-d Authors.

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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

const (
	defaultInMemoryIndexSize = 1e8 // TODO: change to memory-size based configuration
	defaultPodsPerKey        = 10  // number of pods per key
)

// InMemoryIndexConfig holds the configuration for the InMemoryIndex.
type InMemoryIndexConfig struct {
	// Size is the maximum number of keys that can be stored in the index.
	Size int `json:"size"`
	// PodCacheSize is the maximum number of pod entries per key.
	// A non-positive value selects defaultPodsPerKey.
	PodCacheSize int `json:"podCacheSize"`
}

// DefaultInMemoryIndexConfig returns a default configuration for the InMemoryIndex.
func DefaultInMemoryIndexConfig() *InMemoryIndexConfig {
	return &InMemoryIndexConfig{
		Size:         defaultInMemoryIndexSize,
		PodCacheSize: defaultPodsPerKey,
	}
}

// NewInMemoryIndex creates a new InMemoryIndex instance.
func NewInMemoryIndex(cfg *InMemoryIndexConfig) (*InMemoryIndex, error) {
	if cfg == nil {
		cfg = DefaultInMemoryIndexConfig()
	}
	podCacheSize := cfg.PodCacheSize
	if podCacheSize <= 0 {
		podCacheSize = defaultPodsPerKey
	}

	cache, err := newLRUStore(cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize in-memory index: %w", err)
	}

	numShards := 1
	if cfg.Size >= lruShardThreshold {
		numShards = maxLRUShards
	}
	shardSize := max(1, (cfg.Size+numShards-1)/numShards)
	onEngineKeyEvict := func(_ BlockHash, rks []BlockHash) {
		for _, rk := range rks {
			if pc, ok := cache.Peek(rk); ok && pc != nil {
				pc.singleEngineKey.Store(0)
			}
		}
	}
	engineShards := make([]engineKeyShard, numShards)
	for i := range engineShards {
		c, err := simplelru.NewLRU[BlockHash, []BlockHash](shardSize, onEngineKeyEvict)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize in-memory engine key map: %w", err)
		}
		engineShards[i].lru = c
	}

	return &InMemoryIndex{
		data: cache,
		engineToRequestKeys: engineKeyStore{
			shards: engineShards,
			mask:   uint64(numShards - 1),
		},
		podCacheSize: podCacheSize,
		pods:         collections.NewInterner[string](maxInternedPods),
		tiers:        collections.NewInterner[string](maxInternedTiers),
	}, nil
}

// cancellationCheckMask paces context-cancellation checks in loops over
// request keys: positions where idx&mask == 0 poll ctx.Err().
const cancellationCheckMask = 255

// maxInternedPods and maxInternedTiers cap the distinct pod identifiers and
// device tiers an index assigns ordinals to over its lifetime.
const (
	maxInternedPods  = 1 << 20
	maxInternedTiers = 1 << 12
)

// errIndexCardinality reports an Add whose entries would exceed a cap.
var errIndexCardinality = errors.New("index cardinality limit reached")

// engineKeyShard holds one partition of the engine-key to request-key LRU,
// padded to 128 bytes so adjacent shards do not share an L2 cache-line pair
// under spatial prefetching.
type engineKeyShard struct {
	mu  sync.RWMutex
	lru *simplelru.LRU[BlockHash, []BlockHash]
	_   [96]byte
}

type engineKeyStore struct {
	shards []engineKeyShard
	mask   uint64
}

func (e *engineKeyStore) shardFor(key BlockHash) *engineKeyShard {
	if e.mask == 0 {
		return &e.shards[0]
	}
	return &e.shards[mixBlockHash(key)&e.mask]
}

func (e *engineKeyStore) Contains(key BlockHash) bool {
	sh := e.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.lru.Contains(key)
}

// evictCoalesce caches a single-entry removal transition (prevSnap -> nextSnap).
// Add processes key batches and shares one *PodSnapshot across consecutive keys,
// whereas Evict is invoked one key at a time; reusing nextSnap across single-key
// Evict calls preserves *PodSnapshot pointer equality so WalkSnapshots run-coalescing
// survives partial evictions.
type evictCoalesce struct {
	entry    PodEntry
	prevSnap *PodSnapshot
	nextSnap *PodSnapshot
}

func evictCacheSlot(pod string) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < len(pod); i++ {
		h = (h ^ uint64(pod[i])) * 0x100000001b3
	}
	return (h ^ (h >> 32)) & 255
}

// InMemoryIndex is an in-memory implementation of the Index interface.
type InMemoryIndex struct {
	// data holds the mapping of requestKeys to sets of pod identifiers.
	data *lruStore
	// engineToRequestKeys holds sharded mappings of engineKeys to requestKeys.
	engineToRequestKeys engineKeyStore
	// podCacheSize is the maximum number of pod entries per key.
	podCacheSize int
	// pods and tiers assign the ordinals EntryRef carries. Neither is
	// reclaimed when entries leave the index: pod identifiers are endpoint
	// address:port values, bounded by the pod network's address space, and
	// tiers are the engine-reported names. Each is capped, and an Add past
	// a cap fails rather than admitting an entry that cannot be matched.
	pods       *collections.Interner[string]
	tiers      *collections.Interner[string]
	evictCache [256]atomic.Pointer[evictCoalesce]
}

func (m *InMemoryIndex) engineShardFor(key BlockHash) *engineKeyShard {
	return m.engineToRequestKeys.shardFor(key)
}

var _ Index = &InMemoryIndex{}

// PodCache holds one request key's pod entries in LRU order (least recent
// first), bounded by capacity.
type PodCache struct {
	// mu serializes copy-on-write updates to snapshot and emptiness checks on eviction.
	mu sync.Mutex
	// snapshot points to an immutable PodSnapshot replaced as a whole by writers and
	// loaded without locking by readers.
	snapshot atomic.Pointer[PodSnapshot]
	// capacity is the maximum number of PodEntry records retained in snapshot; immutable after creation.
	capacity int
	// key is the request BlockHash this cache serves; immutable while resident in a shard LRU.
	key BlockHash
	// singleEngineKey holds uint64(engineKey) when key has a 1:1 mapping to a single non-empty engineKey.
	singleEngineKey atomic.Uint64
	// referenced marks whether key has been read since its last shard LRU insertion or promotion.
	referenced atomic.Bool
	// resident reports whether this PodCache is still present in its shard LRU; cleared by the LRU eviction callback.
	resident atomic.Bool
}

func (pc *PodCache) loadEntries() []EntryRef {
	if s := pc.snapshot.Load(); s != nil {
		return s.Entries
	}
	return nil
}

// addAll inserts records with LRU semantics on a copy-on-write snapshot: an
// existing entry refreshes its recency, a new entry appends, and overflow
// evicts the least recent entry. When the current snapshot pointer matches
// prevSnap and nextSnap is non-nil, nextSnap is reused directly so consecutive
// keys transitioning from the same state share one PodSnapshot.
func (pc *PodCache) addAll(recs []EntryRef, prevSnap, nextSnap *PodSnapshot) (*PodSnapshot, *PodSnapshot) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	curPtr := pc.snapshot.Load()
	if nextSnap != nil && curPtr == prevSnap {
		pc.snapshot.Store(nextSnap)
		return prevSnap, nextSnap
	}
	var cur []EntryRef
	if curPtr != nil {
		cur = curPtr.Entries
		if len(recs) == 1 && len(cur) > 0 && cur[len(cur)-1].PodEntry == recs[0].PodEntry {
			return curPtr, curPtr
		}
	}
	maxLen := len(cur) + len(recs)
	if pc.capacity > 0 && maxLen > pc.capacity {
		maxLen = pc.capacity
	}
	next := make([]EntryRef, len(cur), maxLen)
	copy(next, cur)
	for _, rec := range recs {
		found := false
		for i := range next {
			if next[i].PodEntry == rec.PodEntry {
				copy(next[i:], next[i+1:])
				next[len(next)-1] = rec
				found = true
				break
			}
		}
		if found {
			continue
		}
		if pc.capacity > 0 && len(next) >= pc.capacity {
			copy(next, next[1:])
			next[len(next)-1] = rec
			continue
		}
		next = append(next, rec)
	}
	snap := BuildPodSnapshot(next)
	pc.snapshot.Store(snap)
	return curPtr, snap
}

// removeAll deletes the given entries on a copy-on-write snapshot, detaches pc
// from data when no entries remain in single-shard mode, and reports whether
// the cache is empty and whether requestKey was removed from data.
func (pc *PodCache) removeAll(entries []PodEntry, data *lruStore, requestKey BlockHash, evictCache *[256]atomic.Pointer[evictCoalesce]) (empty, removedKey bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	curPtr := pc.snapshot.Load()
	if curPtr == nil || len(curPtr.Entries) == 0 {
		if data.shardMask == 0 {
			removedKey = data.removeIfSame(requestKey, pc)
		}
		return true, removedKey
	}
	cur := curPtr.Entries
	if len(cur) == 1 {
		for _, entry := range entries {
			if cur[0].PodEntry == entry {
				pc.snapshot.Store(nil)
				if data.shardMask == 0 {
					removedKey = data.removeIfSame(requestKey, pc)
				} else {
					pc.referenced.Store(false)
				}
				return true, removedKey
			}
		}
		return false, false
	}
	var cSlot uint64
	if len(entries) == 1 && evictCache != nil {
		cSlot = evictCacheSlot(entries[0].PodIdentifier)
		if ec := evictCache[cSlot].Load(); ec != nil && ec.prevSnap == curPtr && ec.entry == entries[0] {
			pc.snapshot.Store(ec.nextSnap)
			if ec.nextSnap == nil {
				if data.shardMask == 0 {
					removedKey = data.removeIfSame(requestKey, pc)
				} else {
					pc.referenced.Store(false)
				}
				return true, removedKey
			}
			return false, false
		}
	}
	var next []EntryRef
	removed := false
	for i := range cur {
		drop := false
		for _, entry := range entries {
			if cur[i].PodEntry == entry {
				drop = true
				break
			}
		}
		if drop {
			if !removed {
				if rem := len(cur) - 1; rem > 0 {
					next = make([]EntryRef, 0, rem)
					next = append(next, cur[:i]...)
				}
				removed = true
			}
			continue
		}
		if removed {
			next = append(next, cur[i])
		}
	}
	if !removed {
		return false, false
	}
	if len(next) == 0 {
		pc.snapshot.Store(nil)
		if len(entries) == 1 && evictCache != nil {
			evictCache[cSlot].Store(&evictCoalesce{entry: entries[0], prevSnap: curPtr, nextSnap: nil})
		}
		if data.shardMask == 0 {
			removedKey = data.removeIfSame(requestKey, pc)
		} else {
			pc.referenced.Store(false)
		}
		return true, removedKey
	}
	snap := BuildPodSnapshot(next)
	pc.snapshot.Store(snap)
	if len(entries) == 1 && evictCache != nil {
		evictCache[cSlot].Store(&evictCoalesce{entry: entries[0], prevSnap: curPtr, nextSnap: snap})
	}
	return false, false
}

// size returns the number of entries.
func (pc *PodCache) size() int {
	return len(pc.loadEntries())
}

// filteredEntries returns the entries whose PodIdentifier is in allowed (all
// entries when allowed is empty) plus the unfiltered entry count.
func (pc *PodCache) filteredEntries(allowed sets.Set[string]) (filtered []PodEntry, total int) {
	entries := pc.loadEntries()
	total = len(entries)
	if total == 0 {
		return nil, 0
	}
	if allowed.Len() == 0 {
		filtered = make([]PodEntry, 0, total)
		for i := range entries {
			filtered = append(filtered, entries[i].PodEntry)
		}
		return filtered, total
	}
	for i := range entries {
		if allowed.Has(entries[i].PodIdentifier) {
			filtered = append(filtered, entries[i].PodEntry)
		}
	}
	return filtered, total
}

// matching returns the entries belonging to podIdentifier.
func (pc *PodCache) matching(podIdentifier string) []PodEntry {
	entries := pc.loadEntries()
	var matched []PodEntry
	for i := range entries {
		if entries[i].PodIdentifier == podIdentifier {
			matched = append(matched, entries[i].PodEntry)
		}
	}
	return matched
}

// internRecords pairs each entry with its pod and tier ordinals. A batch is
// assigned all or nothing: when its new pods or tiers would exceed a cap, no
// ordinal is consumed and the error names the cap.
func (m *InMemoryIndex) internRecords(entries []PodEntry) ([]EntryRef, error) {
	m.pods.RLock()
	m.tiers.RLock()
	var records []EntryRef
	allFound := true
	for i, entry := range entries {
		podOrd, okPod := m.pods.LookupLocked(entry.PodIdentifier)
		if !okPod {
			allFound = false
			break
		}
		tierOrd, okTier := m.tiers.LookupLocked(entry.DeviceTier)
		if !okTier {
			allFound = false
			break
		}
		if records == nil {
			records = make([]EntryRef, len(entries))
		}
		records[i] = EntryRef{
			PodEntry:    entry,
			PodOrdinal:  uint32(podOrd),
			TierOrdinal: uint32(tierOrd),
		}
	}
	m.tiers.RUnlock()
	m.pods.RUnlock()
	if allFound {
		return records, nil
	}

	m.pods.Lock()
	defer m.pods.Unlock()
	m.tiers.Lock()
	defer m.tiers.Unlock()

	if !m.pods.FitsLocked(func(yield func(string) bool) {
		for i := range entries {
			if !yield(entries[i].PodIdentifier) {
				return
			}
		}
	}) {
		return nil, fmt.Errorf("%w: %d pod identifiers", errIndexCardinality, maxInternedPods)
	}
	if !m.tiers.FitsLocked(func(yield func(string) bool) {
		for i := range entries {
			if !yield(entries[i].DeviceTier) {
				return
			}
		}
	}) {
		return nil, fmt.Errorf("%w: %d device tiers", errIndexCardinality, maxInternedTiers)
	}

	if records == nil {
		records = make([]EntryRef, len(entries))
	}
	for i, entry := range entries {
		records[i] = EntryRef{
			PodEntry:    entry,
			PodOrdinal:  uint32(m.pods.InternLocked(entry.PodIdentifier)),
			TierOrdinal: uint32(m.tiers.InternLocked(entry.DeviceTier)),
		}
	}
	return records, nil
}

// Lookup receives a list of requestKeys and a set of pod identifiers,
// and retrieves the filtered pods associated with those keys.
// The filtering is done based on the pod identifiers provided.
// If the podIdentifierSet is empty, all pods are returned.
//
// It returns:
// 1. A map where the keys are those in (1) and the values are pod-identifiers.
// 2. An error if any occurred during the operation.
//
// For non-empty requestKeys, Lookup uses WalkKeys' cancellation checkpoints
// and recency-promotion rules. It retains Lookup's empty-input error.
func (m *InMemoryIndex) Lookup(ctx context.Context, requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, fmt.Errorf("no requestKeys provided for lookup")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE)
	traceEnabled := traceLogger.Enabled()
	if traceEnabled {
		traceLogger = traceLogger.WithName("kvblock.InMemoryIndex.Lookup")
	}

	podsPerKey := make(map[BlockHash][]PodEntry)
	highestHitIdx := 0
	visited := 0
	// Every exit, cancellation included, refreshes what was read.
	defer func() { m.data.Promote(requestKeys[:visited]) }()

	for idx, requestKey := range requestKeys {
		if idx&cancellationCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		pods, found := m.data.Peek(requestKey)
		if !found {
			if traceEnabled {
				traceLogger.Info("key not found in index", "key", requestKey)
			}
			continue
		}
		var filtered []PodEntry
		total := 0
		if pods != nil {
			filtered, total = pods.filteredEntries(podIdentifierSet)
		}
		visited = idx + 1
		if total == 0 {
			if traceEnabled {
				traceLogger.Info("no pods found for key, cutting search", "key", requestKey)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return podsPerKey, nil // early stop since prefix-chain breaks here
		}

		highestHitIdx = idx

		if len(filtered) > 0 {
			podsPerKey[requestKey] = filtered
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if traceEnabled {
		traceLogger.Info("lookup completed", "highest-hit-index", highestHitIdx,
			"pods-per-key", podsPerKeyPrintHelper(podsPerKey))
	}

	return podsPerKey, nil
}

// Add adds a set of engineKeys/requestKeys and their associated pod entries to the index backend.
// If engineKeys is nil, only requestKey -> PodEntry mappings are created (no engineKey -> requestKey mapping).
// This is used for speculative entries where engine keys are not yet known.
// When engineKeys is non-nil, the mapping type is inferred from the ratio of array lengths.
func (m *InMemoryIndex) Add(ctx context.Context, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	if len(requestKeys) == 0 || len(entries) == 0 {
		return fmt.Errorf("no keys or entries provided for adding to index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE)
	traceEnabled := traceLogger.Enabled()
	if traceEnabled {
		traceLogger = traceLogger.WithName("kvblock.InMemoryIndex.Add")
	}

	// Intern once per call, before anything is written: a rejected batch
	// leaves no mapping and no ordinal behind. The same records apply to
	// every request key.
	records, err := m.internRecords(entries)
	if err != nil {
		return err
	}

	var cacheBuf [512]*PodCache
	podCaches := m.data.getOrAddBatch(requestKeys, cacheBuf[:0], m.podCacheSize)
	isOneToOne := len(engineKeys) == len(requestKeys)
	mask := m.engineToRequestKeys.mask
	allSingleMapped := isOneToOne && mask != 0 && len(engineKeys) > 0
	var prevSnap, nextSnap *PodSnapshot
	for i, podCache := range podCaches {
		prevSnap, nextSnap = podCache.addAll(records, prevSnap, nextSnap)
		if !podCache.resident.Load() {
			m.data.Add(requestKeys[i], podCache)
		}
		if allSingleMapped {
			ek := engineKeys[i]
			if ek == EmptyBlockHash || podCache.singleEngineKey.Load() != uint64(ek) {
				allSingleMapped = false
			}
		}

		if traceEnabled {
			traceLogger.Info("added pods to key", "requestKey", requestKeys[i], "pods", entries)
		}
	}
	if allSingleMapped || len(engineKeys) == 0 {
		return nil
	}

	m.addEngineKeyMappings(engineKeys, requestKeys, podCaches, records)
	return nil
}

// addEngineKeyMappings records engineKey -> requestKey mappings across shards.
// The ratio of array lengths determines the mapping type:
//   - equal  (4 eng, 4 req) -> 1:1   E0->R0, E1->R1, ...
//   - many:1 (4 eng, 1 req) -> E0->R0, E1->R0, E2->R0, E3->R0
//   - 1:many (1 eng, 4 req) -> E0->[R0, R1, R2, R3]
func (m *InMemoryIndex) addEngineKeyMappings(engineKeys, requestKeys []BlockHash, podCaches []*PodCache, records []EntryRef) {
	isOneToOne := len(engineKeys) == len(requestKeys)
	mask := m.engineToRequestKeys.mask
	n := max(len(engineKeys), len(requestKeys))
	if n >= (1 << 24) {
		for ek, rks := range engineToRequestMapping(engineKeys, requestKeys) {
			es := m.engineShardFor(ek)
			es.mu.Lock()
			if existing, hadExisting := es.lru.Peek(ek); hadExisting {
				for _, oldRK := range existing {
					if oldPC, ok := m.data.Peek(oldRK); ok && oldPC != nil {
						oldPC.singleEngineKey.Store(0)
					}
				}
			}
			es.lru.Add(ek, rks)
			es.mu.Unlock()
		}
		return
	}

	var runBuf [512]uint64
	runs := runBuf[:0]
	for start := 0; start < n; {
		engIdx := start * len(engineKeys) / n
		ek := engineKeys[engIdx]
		end := start + 1
		for end < n && engineKeys[end*len(engineKeys)/n] == ek {
			end++
		}
		var sIdx uint64
		if mask != 0 {
			sIdx = mixBlockHash(ek) & mask
		}
		runs = append(runs, (sIdx<<48)|(uint64(start)<<24)|uint64(end))
		start = end
	}
	if mask != 0 && len(runs) > 1 {
		slices.Sort(runs)
	}
	var backing []BlockHash
	ensureBacking := func() {
		if backing != nil {
			return
		}
		backing = make([]BlockHash, n)
		if isOneToOne {
			copy(backing, requestKeys)
		} else {
			for i := range n {
				backing[i] = requestKeys[i*len(requestKeys)/n]
			}
		}
	}
	for r := 0; r < len(runs); {
		sIdx := runs[r] >> 48
		rEnd := r + 1
		for rEnd < len(runs) && (runs[rEnd]>>48) == sIdx {
			rEnd++
		}
		es := &m.engineToRequestKeys.shards[sIdx]
		es.mu.Lock()
		for k := r; k < rEnd; k++ {
			packed := runs[k]
			start := int((packed >> 24) & 0xffffff)
			end := int(packed & 0xffffff)
			ek := engineKeys[start*len(engineKeys)/n]
			for idx := start; idx < end; idx++ {
				reqIdx := idx * len(requestKeys) / n
				pc := podCaches[reqIdx]
				if len(pc.loadEntries()) == 0 {
					pc.addAll(records, nil, nil)
					m.data.Add(requestKeys[reqIdx], pc)
				} else if !pc.resident.Load() {
					m.data.Add(requestKeys[reqIdx], pc)
				}
			}
			seenBefore := false
			dupInShard := false
			for p := r; p < rEnd; p++ {
				if p == k {
					continue
				}
				otherStart := int((runs[p] >> 24) & 0xffffff)
				if engineKeys[otherStart*len(engineKeys)/n] == ek {
					dupInShard = true
					if p < k {
						seenBefore = true
					}
				}
			}
			if isOneToOne && !dupInShard && end == start+1 && ek != EmptyBlockHash && podCaches[start].singleEngineKey.Load() == uint64(ek) {
				continue
			}
			existing, hadExisting := es.lru.Peek(ek)
			if isOneToOne && !dupInShard && end == start+1 && ek != EmptyBlockHash && hadExisting && len(existing) == 1 && existing[0] == requestKeys[start] {
				if mask == 0 {
					es.lru.Get(ek)
				} else {
					podCaches[start].singleEngineKey.Store(uint64(ek))
				}
				continue
			}
			if hadExisting {
				for _, oldRK := range existing {
					if oldPC, ok := m.data.Peek(oldRK); ok && oldPC != nil {
						oldPC.singleEngineKey.Store(0)
					}
				}
			}
			ensureBacking()
			if seenBefore {
				es.lru.Add(ek, append(existing, backing[start:end]...))
			} else {
				es.lru.Add(ek, backing[start:end:end])
				if isOneToOne && !dupInShard && end == start+1 && ek != EmptyBlockHash {
					podCaches[start].singleEngineKey.Store(uint64(ek))
				}
			}
		}
		es.mu.Unlock()
		r = rEnd
	}
}

// Evict removes a key and its associated pod entries from the index backend.
// keyType indicates whether the key is an EngineKey (requires engine→request lookup)
// or a RequestKey (used directly for speculative entries without engineKey mapping).
func (m *InMemoryIndex) Evict(ctx context.Context, key BlockHash, keyType KeyType, entries []PodEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries provided for eviction from index")
	}

	traceLogger := log.FromContext(ctx).V(logging.TRACE)
	if traceLogger.Enabled() {
		traceLogger = traceLogger.WithName("kvblock.InMemoryIndex.Evict")
	}

	switch keyType {
	case EngineKey:
		if m.data.shardMask != 0 && key != EmptyBlockHash {
			if pc := m.data.lookupFast(mixBlockHash(key), key); pc != nil && pc.singleEngineKey.Load() == uint64(key) {
				_, removedKey := pc.removeAll(entries, m.data, key, &m.evictCache)
				if traceLogger.Enabled() {
					traceLogger.Info("evicted pods from key", "requestKey", key, "engineKey", key, "pods", entries)
					if removedKey {
						traceLogger.Info("removed requestKey from index as no pods remain", "requestKey", key)
					}
				}
				return nil
			}
		}
		es := m.engineShardFor(key)
		es.mu.RLock()
		rks, found := es.lru.Peek(key)
		es.mu.RUnlock()
		if !found {
			if traceLogger.Enabled() {
				traceLogger.Info("engineKey not found in mapping, nothing to evict", "engineKey", key)
			}
			return nil
		}

		allPossiblyEmpty := true
		for _, rk := range rks {
			if !m.evictPodsFromRequestKey(rk, key, entries, traceLogger) {
				allPossiblyEmpty = false
			}
		}
		if !allPossiblyEmpty {
			return nil
		}

		es.mu.Lock()
		allEmpty := true
		for _, rk := range rks {
			if m.data.hasNonEmpty(rk) {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			for _, rk := range rks {
				if pc, ok := m.data.Peek(rk); ok && pc != nil {
					pc.singleEngineKey.Store(0)
				}
			}
			es.lru.Remove(key)
		}
		es.mu.Unlock()
		return nil
	case RequestKey:
		m.evictPodsFromRequestKey(key, EmptyBlockHash, entries, traceLogger)
		return nil
	default:
		return fmt.Errorf("unknown key type: %d", keyType)
	}
}

// evictPodsFromRequestKey removes the given pod entries from a single request key's cache.
// If the cache becomes empty, the request key is removed from the index.
func (m *InMemoryIndex) evictPodsFromRequestKey(requestKey, engineKey BlockHash, entries []PodEntry, traceLogger logr.Logger) bool {
	podCache, found := m.data.Peek(requestKey)
	if !found || podCache == nil {
		if traceLogger.Enabled() {
			traceLogger.Info("requestKey not found in index, nothing to evict", "requestKey", requestKey, "engineKey", engineKey)
		}
		return true
	}

	empty, removedKey := podCache.removeAll(entries, m.data, requestKey, &m.evictCache)

	if traceLogger.Enabled() {
		traceLogger.Info("evicted pods from key", "requestKey", requestKey, "engineKey", engineKey, "pods", entries)
		if removedKey {
			traceLogger.Info("removed requestKey from index as no pods remain", "requestKey", requestKey)
		}
	}

	return empty
}

// Clear removes every entry for the pod from the index, across all device tiers.
// O(N) over the index, but Clear is rare and off the Lookup/Add hot path. Reuses
// evictPodsFromRequestKey for race-safe removal, and holds no global lock — only
// each PodCache's mu, briefly — so it does not stall Lookup.
//
// Context cancellation does not interrupt Clear.
//
// The engineKey->requestKey mapping (engineShards) is intentionally left
// untouched: it is LRU-bounded, self-heals when the pod re-Adds the same prefixes,
// and any stale mapping resolves to an emptied request key that correctly breaks
// the prefix chain in Lookup.
func (m *InMemoryIndex) Clear(ctx context.Context, podIdentifier string) error {
	traceLogger := log.FromContext(ctx).V(logging.TRACE)
	if traceLogger.Enabled() {
		traceLogger = traceLogger.WithName("kvblock.InMemoryIndex.Clear")
	}

	for _, requestKey := range m.data.Keys() {
		// Peek so a clear does not promote LRU recency on keys it scans.
		podCache, found := m.data.Peek(requestKey)
		if !found || podCache == nil {
			continue
		}

		matched := podCache.matching(podIdentifier)

		if len(matched) > 0 {
			m.evictPodsFromRequestKey(requestKey, EmptyBlockHash, matched, traceLogger)
		}
	}

	if traceLogger.Enabled() {
		traceLogger.Info("cleared pod from index", "pod", podIdentifier)
	}
	return nil
}

// GetRequestKey returns the last request key (highest index in the chain) associated with the given engineKey.
// This is what Pool uses for parent hash resolution.
// Returns an error if the engineKey mapping is missing (e.g., already evicted).
func (m *InMemoryIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	es := m.engineShardFor(engineKey)
	es.mu.RLock()
	rks, found := es.lru.Peek(engineKey)
	es.mu.RUnlock()
	if !found || len(rks) == 0 {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rks[len(rks)-1], nil
}

// podsPerKeyPrintHelper formats a map of keys to pod names for printing.
func podsPerKeyPrintHelper(ks map[BlockHash][]PodEntry) string {
	var b strings.Builder
	for k, v := range ks {
		fmt.Fprintf(&b, "%s: %v\n", k.String(), collections.SliceMap(v, func(pod PodEntry) string {
			return pod.String()
		}))
	}
	return b.String()
}
