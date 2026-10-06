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

package kvcache

import (
	"context"
	"math"
	"math/bits"
	"sync"
	"unsafe"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvcache/metrics"
)

// SpeculativeTier is the tier name under which speculative entries count in
// PodMatch.BlocksByTier. Speculative entries carry no engine-reported device
// tier; an entry whose device tier is reported under this name counts in the
// same chain.
const SpeculativeTier = "speculative"

// speculativeTierWeight scores speculative entries when the speculative tier
// has no configured weight.
const speculativeTierWeight = 1.0

// unknownTierWeight scores blocks held in a tier without a configured weight.
const unknownTierWeight = 0.0

// matchCancellationMask paces context-cancellation checks over key
// positions: positions where pos&mask == 0 poll ctx.Err().
const matchCancellationMask = 255

const maxStaticTierBlocks = 4096

var (
	gpuTierMaps         [maxStaticTierBlocks + 1]map[string]int
	speculativeTierMaps [maxStaticTierBlocks + 1]map[string]int
)

func init() {
	for i := 1; i <= maxStaticTierBlocks; i++ {
		gpuTierMaps[i] = map[string]int{"gpu": i}
		speculativeTierMaps[i] = map[string]int{SpeculativeTier: i}
	}
}

func singleTierMap(tier string, count int) map[string]int {
	if count >= 1 && count <= maxStaticTierBlocks {
		switch tier {
		case "gpu":
			return gpuTierMaps[count]
		case SpeculativeTier:
			return speculativeTierMaps[count]
		}
	}
	return nil
}

// PodMatch is one pod's prefix match for a key sequence. All values cover
// the contiguous chain of keys the pod holds, counted from the first key.
type PodMatch struct {
	// WeightedScore sums, per block of the chain, the highest device-tier
	// weight among the pod's entries for that block. Tiers without a
	// configured weight count unknownTierWeight; speculative entries count
	// speculativeTierWeight unless the speculative tier is configured.
	WeightedScore float64
	// MatchedBlocks is the chain length in blocks, regardless of tier.
	MatchedBlocks int
	// BlocksByTier is the per-tier chain length: a tier counts a block only
	// while the pod holds every previous block in that same tier.
	// Speculative entries count under SpeculativeTier. Never nil.
	BlocksByTier map[string]int
}

var internalSpanStartOpts = []trace.SpanStartOption{
	trace.WithSpanKind(trace.SpanKindInternal),
}

// MatchBlockKeys runs the prefix matcher over keys for the pods in podFilter
// (every pod when empty) and returns one PodMatch per pod that holds the
// first key. Empty keys match nothing. The matcher walks the index when the
// backend is a kvblock.KeyWalker and otherwise materializes Lookup; both
// feed the same accumulator.
func (k *Indexer) MatchBlockKeys(ctx context.Context, keys []kvblock.BlockHash,
	podFilter sets.Set[string],
) (map[string]PodMatch, error) {
	if len(keys) == 0 {
		return map[string]PodMatch{}, nil
	}

	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "match_block_keys", internalSpanStartOpts...)
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCachePrefixMatchKeyCount(len(keys)),
			semconv.LLMDKVCachePrefixMatchPodFilterCount(podFilter.Len()),
			semconv.LLMDKVCachePrefixMatchWalked(k.keyWalker != nil),
		)
	}

	var matches map[string]PodMatch
	var blocksFound int
	var err error
	if k.keyWalker != nil {
		matches, blocksFound, err = matchWalk(ctx, k.keyWalker, keys, k.tierWeights, podFilter)
	} else {
		matches, blocksFound, err = matchLookup(ctx, k.kvBlockIndex, keys, k.tierWeights, podFilter)
	}
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if k.recordHits {
		metrics.MaxPodHitCount.Add(float64(blocksFound))
		metrics.LookupHits.Add(float64(blocksFound))
	}
	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCachePrefixMatchPodsMatched(len(matches)),
			semconv.LLMDKVCachePrefixMatchLongestChain(blocksFound),
		)
	}
	return matches, nil
}

// matchWalk feeds the accumulator from an ordered index walk. The walk ends
// at the first key without entries or once no chain is alive.
func matchWalk(ctx context.Context, walker kvblock.KeyWalker, keys []kvblock.BlockHash,
	weights map[string]float64, filter sets.Set[string],
) (map[string]PodMatch, int, error) {
	acc := acquireAccumulator(weights, filter)
	defer releaseAccumulator(acc)

	if sw, ok := walker.(kvblock.SnapshotWalker); ok {
		acc.snapWalker = sw
		if err := sw.WalkSnapshots(ctx, keys, acc.walkSnapFn); err != nil {
			return nil, 0, err
		}
	} else {
		if err := walker.WalkKeys(ctx, keys, acc.walkFn); err != nil {
			return nil, 0, err
		}
	}
	matches, longest := acc.result()
	return matches, longest, nil
}

// scoreWalk feeds the accumulator from an ordered index walk and returns
// weighted pod scores and the longest matched chain length.
func scoreWalk(ctx context.Context, walker kvblock.KeyWalker, keys []kvblock.BlockHash,
	weights map[string]float64, filter sets.Set[string],
) (map[string]float64, int, error) {
	acc := acquireScoreAccumulator(weights, filter)
	defer releaseAccumulator(acc)

	if sw, ok := walker.(kvblock.SnapshotWalker); ok {
		acc.snapWalker = sw
		if err := sw.WalkSnapshots(ctx, keys, acc.walkSnapFn); err != nil {
			return nil, 0, err
		}
	} else {
		if err := walker.WalkKeys(ctx, keys, acc.walkFn); err != nil {
			return nil, 0, err
		}
	}
	scores, longest := acc.scores()
	return scores, longest, nil
}

// matchLookup feeds the accumulator from a materialized Lookup result, for
// backends without the walk capability.
func matchLookup(ctx context.Context, index kvblock.Index, keys []kvblock.BlockHash,
	weights map[string]float64, filter sets.Set[string],
) (map[string]PodMatch, int, error) {
	keyToPods, err := index.Lookup(ctx, keys, filter)
	if err != nil {
		return nil, 0, err
	}
	return matchMaterialized(ctx, keys, keyToPods, weights, filter)
}

// scoreLookup feeds the accumulator from a materialized Lookup result and
// returns weighted pod scores and the longest matched chain length.
func scoreLookup(ctx context.Context, index kvblock.Index, keys []kvblock.BlockHash,
	weights map[string]float64, filter sets.Set[string],
) (map[string]float64, int, error) {
	keyToPods, err := index.Lookup(ctx, keys, filter)
	if err != nil {
		return nil, 0, err
	}
	acc := acquireScoreAccumulator(weights, filter)
	defer releaseAccumulator(acc)

	if err := feedMaterialized(ctx, acc, keys, keyToPods); err != nil {
		return nil, 0, err
	}
	scores, longest := acc.scores()
	return scores, longest, nil
}

// matchMaterialized feeds the accumulator from a Lookup result, walking keys
// in order and stopping at the first key without entries. Pod and tier
// ordinals are assigned per call, since materialized entries carry none.
func matchMaterialized(ctx context.Context, keys []kvblock.BlockHash,
	keyToPods map[kvblock.BlockHash][]kvblock.PodEntry,
	weights map[string]float64, filter sets.Set[string],
) (map[string]PodMatch, int, error) {
	acc := acquireAccumulator(weights, filter)
	defer releaseAccumulator(acc)

	if err := feedMaterialized(ctx, acc, keys, keyToPods); err != nil {
		return nil, 0, err
	}
	matches, longest := acc.result()
	return matches, longest, nil
}

func feedMaterialized(ctx context.Context, acc *prefixAccumulator, keys []kvblock.BlockHash,
	keyToPods map[kvblock.BlockHash][]kvblock.PodEntry,
) error {
	pods, tiers := ordinalTable{}, ordinalTable{}
	var refs []kvblock.EntryRef
	for pos, key := range keys {
		if pos&matchCancellationMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		entries := keyToPods[key]
		if len(entries) == 0 {
			break
		}
		refs = refs[:0]
		for _, e := range entries {
			refs = append(refs, kvblock.EntryRef{
				PodEntry:    e,
				PodOrdinal:  pods.of(e.PodIdentifier),
				TierOrdinal: tiers.of(e.DeviceTier),
			})
		}
		acc.lastEntriesPtr = nil
		if !acc.key(refs) {
			break
		}
	}
	// Cancellation is sampled at checkpoints along the keys and once more at
	// completion, so a cancelled request never reports a match.
	return ctx.Err()
}

// ordinalTable assigns dense ordinals to names in first-seen order.
type ordinalTable map[string]uint32

func (t ordinalTable) of(name string) uint32 {
	if id, ok := t[name]; ok {
		return id
	}
	id := uint32(len(t))
	t[name] = id
	return id
}

// speculativeTierOrdinal keys the speculative per-tier chain. Feeders assign
// tier ordinals from zero, so the top of the range never collides.
const speculativeTierOrdinal = math.MaxUint32

// slotRef maps one pod ordinal to a request-local slot.
type slotRef struct {
	ordinal uint32
	slot    uint32 // slot index plus one; zero marks an empty bucket
}

// slotTable is an open-addressed map from pod ordinal to request-local slot.
// It is sized by the first key's entry count, so request state scales with
// the live candidates rather than with every ordinal an index ever assigned.
type slotTable struct {
	buckets []slotRef
}

func (t *slotTable) reset(numEntries int) {
	size := 2
	for size < numEntries*2 {
		size <<= 1
	}
	if cap(t.buckets) < size {
		t.buckets = make([]slotRef, size)
		return
	}
	t.buckets = t.buckets[:size]
	clear(t.buckets)
}

func (t *slotTable) lookup(ordinal uint32) (int32, bool) {
	mask := uint32(len(t.buckets) - 1)
	i := ordinal * 2654435761 & mask
	for {
		b := t.buckets[i]
		if b.slot == 0 {
			return 0, false
		}
		if b.ordinal == ordinal {
			return int32(b.slot - 1), true
		}
		i = (i + 1) & mask
	}
}

func (t *slotTable) insert(ordinal uint32, slot int32) {
	mask := uint32(len(t.buckets) - 1)
	i := ordinal * 2654435761 & mask
	for t.buckets[i].slot != 0 {
		i = (i + 1) & mask
	}
	t.buckets[i] = slotRef{ordinal: ordinal, slot: uint32(slot) + 1}
}

// tierChain tracks one tier's contiguous prefix for a candidate pod.
type tierChain struct {
	ordinal uint32
	name    string
	count   int
	// seen is the key stamp of the last key where the pod held this tier.
	seen  uint32
	alive bool
}

// tierWeight is one tier's resolved weight, keyed by tier ordinal.
type tierWeight struct {
	ordinal uint32
	weight  float64
}

// matchSlot is one candidate pod's accumulated state.
type matchSlot struct {
	pod     string
	matched int
	score   float64
	// seen is the key stamp of the last key holding this pod; weight is the
	// highest tier weight among its entries at that key.
	seen   uint32
	weight float64
	tiers  []tierChain
}

// prefixAccumulator folds an ordered walk over request keys into per-pod
// prefix matches. It is the single implementation of the matching rules:
// candidates are the pods holding the first key, each chain ends at the
// first key its pod does not hold, duplicate entries for a pod at one key
// take the highest weight, and every tier tracks its own contiguous prefix.
//
// Feeders present each key's entries through key, in key order, and stop at
// the first key without entries or once key reports no live chain. Ordinals
// only need to be stable within one accumulation; they key request-local
// tables and never size state, so sparse or large values cost nothing.
type prefixAccumulator struct {
	weights map[string]float64
	filter  sets.Set[string]

	table      slotTable
	slots      []matchSlot
	active     []int32
	keyStamp   uint32
	first      bool
	trackTiers bool

	// lastEntriesPtr, lastEntriesLen, and pendingRun coalesce consecutive
	// keys whose EntryRef slices share the same backing array pointer and
	// length into a single batch increment in flushPendingRun. Pointer
	// identity is sound when visiting immutable copy-on-write PodCache
	// snapshots; feedMaterialized clears lastEntriesPtr before each key
	// because it reuses a single scratch slice across keys.
	lastEntriesPtr *kvblock.EntryRef
	lastEntriesLen int
	pendingRun     int
	// matchedKeys is the number of flushed keys accumulated while at least
	// one candidate chain remained alive; entries for pods whose slot.matched
	// lags matchedKeys belong to chains that already terminated.
	matchedKeys int

	// weightCache holds the weight of every tier seen in this accumulation,
	// scanned linearly: requests see a handful of tiers.
	weightCache []tierWeight

	snapWalker   kvblock.SnapshotWalker
	fastMode     bool
	fastTierOrd  uint32
	fastTierName string
	fastWeight   float64
	fastEndPos   int
	firstDropPos int
	initMask     [4]uint64
	activeMask   [4]uint64
	droppedMask  [4]uint64
	dropPos      [256]uint32

	// walkFn and walkSnapFn are bound once when the accumulator is constructed
	// so WalkKeys and WalkSnapshots calls do not allocate a closure per request.
	walkFn     func(int, bool, []kvblock.EntryRef) bool
	walkSnapFn func(int, *kvblock.PodSnapshot) bool
}

var accumulatorPool = sync.Pool{New: func() any {
	a := &prefixAccumulator{}
	a.walkFn = a.walkKey
	a.walkSnapFn = a.walkSnapshot
	return a
}}

func (a *prefixAccumulator) walkKey(_ int, found bool, entries []kvblock.EntryRef) bool {
	return found && len(entries) > 0 && a.key(entries)
}

func (a *prefixAccumulator) walkSnapshot(pos int, snap *kvblock.PodSnapshot) bool {
	if snap == nil || len(snap.Entries) == 0 {
		return false
	}
	if a.first {
		if snap.MaskValid && a.snapWalker != nil {
			mask := snap.PodMask
			if a.filter.Len() > 0 {
				for w := range 4 {
					word := mask[w]
					for word != 0 {
						bit := bits.TrailingZeros64(word)
						word &= word - 1
						ord := uint32(w*64 + bit)
						if !a.filter.Has(a.snapWalker.PodName(ord)) {
							mask[w] &^= uint64(1) << bit
						}
					}
				}
			}
			a.first = false
			if (mask[0] | mask[1] | mask[2] | mask[3]) == 0 {
				return false
			}
			tOrd := snap.TierOrd
			if snap.TierName == SpeculativeTier {
				tOrd = speculativeTierOrdinal
			}
			a.fastMode = true
			a.initMask = mask
			a.activeMask = mask
			a.droppedMask = [4]uint64{}
			a.firstDropPos = 0
			a.fastTierOrd = snap.TierOrd
			a.fastTierName = snap.TierName
			a.fastWeight = a.weightOf(snap.TierName, tOrd)
			a.fastEndPos = 1
			return true
		}
		return a.key(snap.Entries)
	}
	if a.fastMode {
		if snap.MaskValid && snap.TierOrd == a.fastTierOrd {
			if (snap.PodMask[0]&a.activeMask[0]) == a.activeMask[0] &&
				(snap.PodMask[1]&a.activeMask[1]) == a.activeMask[1] &&
				(snap.PodMask[2]&a.activeMask[2]) == a.activeMask[2] &&
				(snap.PodMask[3]&a.activeMask[3]) == a.activeMask[3] {
				a.fastEndPos = pos + 1
				return true
			}
			if a.firstDropPos == 0 {
				a.firstDropPos = pos
				a.activeMask[0] &= snap.PodMask[0]
				a.activeMask[1] &= snap.PodMask[1]
				a.activeMask[2] &= snap.PodMask[2]
				a.activeMask[3] &= snap.PodMask[3]
			} else {
				for w := range 4 {
					dropped := a.activeMask[w] &^ snap.PodMask[w]
					for dropped != 0 {
						bit := bits.TrailingZeros64(dropped)
						dropped &= dropped - 1
						ord := w*64 + bit
						a.dropPos[ord] = uint32(pos)
						a.droppedMask[w] |= uint64(1) << bit
					}
					a.activeMask[w] &= snap.PodMask[w]
				}
			}
			if (a.activeMask[0] | a.activeMask[1] | a.activeMask[2] | a.activeMask[3]) == 0 {
				return false
			}
			a.fastEndPos = pos + 1
			return true
		}
		a.materializeFastMode()
	}
	return a.key(snap.Entries)
}

func (a *prefixAccumulator) materializeFastMode() {
	total := bits.OnesCount64(a.initMask[0]) + bits.OnesCount64(a.initMask[1]) +
		bits.OnesCount64(a.initMask[2]) + bits.OnesCount64(a.initMask[3])
	a.table.reset(total)
	a.keyStamp = uint32(a.fastEndPos)
	a.matchedKeys = a.fastEndPos
	tOrd := a.fastTierOrd
	if a.fastTierName == SpeculativeTier {
		tOrd = speculativeTierOrdinal
	}
	if a.firstDropPos != 0 {
		m := a.firstDropPos
		for w := range 4 {
			word := a.initMask[w] &^ (a.droppedMask[w] | a.activeMask[w])
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				s := a.newSlot(a.snapWalker.PodName(ord))
				a.table.insert(ord, s)
				slot := &a.slots[s]
				slot.matched = m
				slot.score = a.fastWeight * float64(m)
				slot.seen = uint32(m)
				slot.weight = a.fastWeight
				if a.trackTiers {
					slot.tiers = append(slot.tiers, tierChain{
						ordinal: tOrd,
						name:    a.fastTierName,
						count:   m,
						seen:    uint32(m),
						alive:   false,
					})
				}
			}
		}
	}
	for w := range 4 {
		word := a.droppedMask[w]
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			ord := uint32(w*64 + bit)
			m := int(a.dropPos[ord])
			s := a.newSlot(a.snapWalker.PodName(ord))
			a.table.insert(ord, s)
			slot := &a.slots[s]
			slot.matched = m
			slot.score = a.fastWeight * float64(m)
			slot.seen = uint32(m)
			slot.weight = a.fastWeight
			if a.trackTiers {
				slot.tiers = append(slot.tiers, tierChain{
					ordinal: tOrd,
					name:    a.fastTierName,
					count:   m,
					seen:    uint32(m),
					alive:   false,
				})
			}
		}
	}
	m := a.fastEndPos
	for w := range 4 {
		word := a.activeMask[w]
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			ord := uint32(w*64 + bit)
			s := a.newSlot(a.snapWalker.PodName(ord))
			a.table.insert(ord, s)
			slot := &a.slots[s]
			slot.matched = m
			slot.score = a.fastWeight * float64(m)
			slot.seen = uint32(m)
			slot.weight = a.fastWeight
			if a.trackTiers {
				slot.tiers = append(slot.tiers, tierChain{
					ordinal: tOrd,
					name:    a.fastTierName,
					count:   m,
					seen:    uint32(m),
					alive:   true,
				})
			}
			a.active = append(a.active, s)
		}
	}
	a.fastMode = false
	a.lastEntriesPtr = nil
	a.lastEntriesLen = 0
	a.pendingRun = 0
}

func acquireAccumulator(weights map[string]float64, filter sets.Set[string]) *prefixAccumulator {
	a, _ := accumulatorPool.Get().(*prefixAccumulator)
	if a.walkFn == nil {
		a.walkFn = a.walkKey
	}
	if a.walkSnapFn == nil {
		a.walkSnapFn = a.walkSnapshot
	}
	a.weights, a.filter = weights, filter
	a.slots = a.slots[:0]
	a.active = a.active[:0]
	a.keyStamp = 0
	a.first = true
	a.trackTiers = true
	a.lastEntriesPtr = nil
	a.lastEntriesLen = 0
	a.pendingRun = 0
	a.matchedKeys = 0
	a.weightCache = a.weightCache[:0]
	a.snapWalker = nil
	a.fastMode = false
	return a
}

func acquireScoreAccumulator(weights map[string]float64, filter sets.Set[string]) *prefixAccumulator {
	a := acquireAccumulator(weights, filter)
	a.trackTiers = false
	return a
}

func releaseAccumulator(a *prefixAccumulator) {
	a.weights, a.filter, a.lastEntriesPtr, a.snapWalker = nil, nil, nil, nil
	accumulatorPool.Put(a)
}

// key folds one key's entries into the chains and reports whether any chain
// is still alive. entries is borrowed for the duration of the call.
func (a *prefixAccumulator) key(entries []kvblock.EntryRef) bool {
	if !a.first && len(entries) > 0 && len(entries) == a.lastEntriesLen &&
		unsafe.SliceData(entries) == a.lastEntriesPtr {
		a.pendingRun++
		return true
	}
	if a.pendingRun > 0 {
		a.flushPendingRun()
	}

	a.keyStamp++
	if a.first {
		a.table.reset(len(entries))
	}

	var prev *kvblock.EntryRef
	for i := range entries {
		ref := &entries[i]
		// Another rank of an endpoint just folded at this key adds nothing
		// to its chains.
		if prev != nil && ref.PodOrdinal == prev.PodOrdinal && ref.TierOrdinal == prev.TierOrdinal &&
			ref.Speculative == prev.Speculative {
			continue
		}
		prev = ref

		s, ok := a.table.lookup(ref.PodOrdinal)
		if !ok {
			if !a.first || (a.filter.Len() > 0 && !a.filter.Has(ref.PodIdentifier)) {
				continue // the first key fixes the candidate set
			}
			s = a.newSlot(ref.PodIdentifier)
			a.table.insert(ref.PodOrdinal, s)
		}
		slot := &a.slots[s]
		if !a.first && slot.matched != a.matchedKeys {
			continue
		}

		tier, tierOrdinal := ref.DeviceTier, ref.TierOrdinal
		if ref.Speculative || ref.DeviceTier == SpeculativeTier {
			tier, tierOrdinal = SpeculativeTier, speculativeTierOrdinal
		}

		w := a.weightOf(tier, tierOrdinal)
		switch {
		case slot.seen != a.keyStamp:
			slot.seen = a.keyStamp
			slot.weight = w
		case w > slot.weight:
			slot.weight = w
		}

		if a.trackTiers && !a.stampTier(slot, tierOrdinal) && a.first {
			slot.tiers = append(slot.tiers, tierChain{ordinal: tierOrdinal, name: tier, seen: a.keyStamp, alive: true})
		}
	}
	alive := a.endKey()
	if alive {
		a.lastEntriesPtr = unsafe.SliceData(entries)
		a.lastEntriesLen = len(entries)
	} else {
		a.lastEntriesPtr = nil
		a.lastEntriesLen = 0
	}
	return alive
}

func (a *prefixAccumulator) flushPendingRun() {
	run := a.pendingRun
	a.pendingRun = 0
	a.matchedKeys += run
	for _, i := range a.active {
		s := &a.slots[i]
		s.matched += run
		s.score += s.weight * float64(run)
		for t := range s.tiers {
			if s.tiers[t].alive {
				s.tiers[t].count += run
			}
		}
	}
}

// stampTier marks tier as held at the current key and reports whether the
// slot tracks that tier.
func (a *prefixAccumulator) stampTier(slot *matchSlot, tierOrdinal uint32) bool {
	for i := range slot.tiers {
		if slot.tiers[i].ordinal == tierOrdinal {
			slot.tiers[i].seen = a.keyStamp
			return true
		}
	}
	return false
}

// endKey closes the current key and reports whether any chain is still
// alive.
func (a *prefixAccumulator) endKey() bool {
	if a.first {
		a.first = false
		for i := range a.slots {
			s := &a.slots[i]
			s.matched, s.score = 1, s.weight
			for t := range s.tiers {
				s.tiers[t].count = 1
			}
			a.active = append(a.active, int32(i))
		}
		if len(a.active) > 0 {
			a.matchedKeys++
		}
		return len(a.active) > 0
	}

	keep := a.active[:0]
	for _, i := range a.active {
		s := &a.slots[i]
		if s.seen != a.keyStamp {
			continue // the chain ends at the first key the pod does not hold
		}
		s.matched++
		s.score += s.weight
		for t := range s.tiers {
			tc := &s.tiers[t]
			switch {
			case !tc.alive:
			case tc.seen == a.keyStamp:
				tc.count++
			default:
				tc.alive = false
			}
		}
		keep = append(keep, i)
	}
	a.active = keep
	if len(a.active) > 0 {
		a.matchedKeys++
	}
	return len(a.active) > 0
}

// result materializes the accumulated matches and the longest matched chain.
func (a *prefixAccumulator) result() (map[string]PodMatch, int) {
	if a.fastMode {
		total := bits.OnesCount64(a.initMask[0]) + bits.OnesCount64(a.initMask[1]) +
			bits.OnesCount64(a.initMask[2]) + bits.OnesCount64(a.initMask[3])
		out := make(map[string]PodMatch, total)
		if total == 0 {
			return out, 0
		}
		wScore := a.fastWeight
		endPos := a.fastEndPos
		endByTier := singleTierMap(a.fastTierName, endPos)
		endMatch := PodMatch{
			WeightedScore: wScore * float64(endPos),
			MatchedBlocks: endPos,
			BlocksByTier:  endByTier,
		}
		if a.firstDropPos == 0 {
			for w := range 4 {
				word := a.initMask[w]
				for word != 0 {
					bit := bits.TrailingZeros64(word)
					word &= word - 1
					ord := uint32(w*64 + bit)
					if endByTier == nil {
						endMatch.BlocksByTier = map[string]int{a.fastTierName: endPos}
					}
					out[a.snapWalker.PodName(ord)] = endMatch
				}
			}
			return out, endPos
		}

		firstByTier := singleTierMap(a.fastTierName, a.firstDropPos)
		firstMatch := PodMatch{
			WeightedScore: wScore * float64(a.firstDropPos),
			MatchedBlocks: a.firstDropPos,
			BlocksByTier:  firstByTier,
		}
		for w := range 4 {
			word := a.initMask[w] &^ (a.droppedMask[w] | a.activeMask[w])
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				if firstByTier == nil {
					firstMatch.BlocksByTier = map[string]int{a.fastTierName: a.firstDropPos}
				}
				out[a.snapWalker.PodName(ord)] = firstMatch
			}
		}
		for w := range 4 {
			word := a.activeMask[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				if endByTier == nil {
					endMatch.BlocksByTier = map[string]int{a.fastTierName: endPos}
				}
				out[a.snapWalker.PodName(ord)] = endMatch
			}
		}
		for w := range 4 {
			word := a.droppedMask[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				m := int(a.dropPos[ord])
				byTier := singleTierMap(a.fastTierName, m)
				if byTier == nil {
					byTier = map[string]int{a.fastTierName: m}
				}
				out[a.snapWalker.PodName(ord)] = PodMatch{
					WeightedScore: wScore * float64(m),
					MatchedBlocks: m,
					BlocksByTier:  byTier,
				}
			}
		}
		return out, endPos
	}
	if a.pendingRun > 0 {
		a.flushPendingRun()
	}
	out := make(map[string]PodMatch, len(a.slots))
	longest := 0
	for i := range a.slots {
		s := &a.slots[i]
		if s.matched > longest {
			longest = s.matched
		}
		var byTier map[string]int
		if len(s.tiers) == 1 {
			byTier = singleTierMap(s.tiers[0].name, s.tiers[0].count)
		}
		if byTier == nil {
			byTier = make(map[string]int, len(s.tiers))
			for _, tc := range s.tiers {
				byTier[tc.name] = tc.count
			}
		}
		out[s.pod] = PodMatch{WeightedScore: s.score, MatchedBlocks: s.matched, BlocksByTier: byTier}
	}
	return out, longest
}

// scores materializes the accumulated weighted scores and the longest
// matched chain length without allocating per-pod tier maps.
func (a *prefixAccumulator) scores() (map[string]float64, int) {
	if a.fastMode {
		total := bits.OnesCount64(a.initMask[0]) + bits.OnesCount64(a.initMask[1]) +
			bits.OnesCount64(a.initMask[2]) + bits.OnesCount64(a.initMask[3])
		out := make(map[string]float64, total)
		if total == 0 {
			return out, 0
		}
		wScore := a.fastWeight
		endPos := a.fastEndPos
		endScore := wScore * float64(endPos)
		if a.firstDropPos == 0 {
			for w := range 4 {
				word := a.initMask[w]
				for word != 0 {
					bit := bits.TrailingZeros64(word)
					word &= word - 1
					ord := uint32(w*64 + bit)
					out[a.snapWalker.PodName(ord)] = endScore
				}
			}
			return out, endPos
		}

		firstScore := wScore * float64(a.firstDropPos)
		for w := range 4 {
			word := a.initMask[w] &^ (a.droppedMask[w] | a.activeMask[w])
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				out[a.snapWalker.PodName(ord)] = firstScore
			}
		}
		for w := range 4 {
			word := a.activeMask[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				out[a.snapWalker.PodName(ord)] = endScore
			}
		}
		for w := range 4 {
			word := a.droppedMask[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				ord := uint32(w*64 + bit)
				m := int(a.dropPos[ord])
				out[a.snapWalker.PodName(ord)] = wScore * float64(m)
			}
		}
		return out, endPos
	}
	if a.pendingRun > 0 {
		a.flushPendingRun()
	}
	out := make(map[string]float64, len(a.slots))
	longest := 0
	for i := range a.slots {
		s := &a.slots[i]
		out[s.pod] = s.score
		if s.matched > longest {
			longest = s.matched
		}
	}
	return out, longest
}

// newSlot appends a candidate, reusing a pooled slot's tier storage when one
// is available.
func (a *prefixAccumulator) newSlot(pod string) int32 {
	n := len(a.slots)
	if n < cap(a.slots) {
		a.slots = a.slots[:n+1]
		s := &a.slots[n]
		*s = matchSlot{pod: pod, tiers: s.tiers[:0]}
	} else {
		a.slots = append(a.slots, matchSlot{pod: pod})
	}
	return int32(n)
}

// weightOf resolves a tier's weight, caching by ordinal so the configured
// map is consulted once per tier per accumulation.
func (a *prefixAccumulator) weightOf(tier string, ordinal uint32) float64 {
	for i := range a.weightCache {
		if a.weightCache[i].ordinal == ordinal {
			return a.weightCache[i].weight
		}
	}
	w := unknownTierWeight
	if tier == SpeculativeTier {
		w = speculativeTierWeight
	}
	if configured, ok := a.weights[tier]; ok {
		w = configured
	}
	a.weightCache = append(a.weightCache, tierWeight{ordinal: ordinal, weight: w})
	return w
}
