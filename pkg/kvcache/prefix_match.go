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
	"sync"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
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

const (
	maxStaticTierBlocks = 4096
	inlinePodSlots      = 256
)

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
	// ConfirmedBlocks is the chain length in blocks counting only keys the
	// pod holds in an engine-reported device tier; the tier may change from
	// block to block. Speculative entries end the chain.
	ConfirmedBlocks int
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
	acc := acquireAccumulator(weights, filter, true)
	defer releaseAccumulator(acc)

	if err := runWalk(ctx, walker, keys, acc); err != nil {
		return nil, 0, err
	}
	matches, longest := acc.result()
	return matches, longest, nil
}

// scoreWalk feeds the accumulator from an ordered index walk and returns
// weighted pod scores and the longest matched chain length.
func scoreWalk(ctx context.Context, walker kvblock.KeyWalker, keys []kvblock.BlockHash,
	weights map[string]float64, filter sets.Set[string],
) (map[string]float64, int, error) {
	acc := acquireAccumulator(weights, filter, false)
	defer releaseAccumulator(acc)

	if err := runWalk(ctx, walker, keys, acc); err != nil {
		return nil, 0, err
	}
	scores, longest := acc.scores()
	return scores, longest, nil
}

func runWalk(ctx context.Context, walker kvblock.KeyWalker, keys []kvblock.BlockHash, acc *prefixAccumulator) error {
	if sw, ok := walker.(kvblock.SnapshotWalker); ok {
		acc.snapWalker = sw
		return sw.WalkSnapshots(ctx, keys, acc.walkSnapFn)
	}
	pods := ordinalTable{}
	var refs []kvblock.EntryRef
	return walker.WalkKeys(ctx, keys, func(pos int, found bool, entries []kvblock.EntryRef) bool {
		if !found || len(entries) == 0 {
			return false
		}
		refs = refs[:0]
		for _, e := range entries {
			var podOrd uint32
			if pos == 0 {
				podOrd = pods.of(e.PodIdentifier)
				acc.recordLocalPod(podOrd, e.PodIdentifier)
			} else {
				var ok bool
				podOrd, ok = pods[e.PodIdentifier]
				if !ok {
					continue
				}
			}
			refs = append(refs, kvblock.EntryRef{
				PodEntry:    e.PodEntry,
				PodOrdinal:  podOrd,
				TierOrdinal: e.TierOrdinal,
			})
		}
		return len(refs) > 0 && acc.walkSnapshot(pos, kvblock.BuildPodSnapshot(refs))
	})
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
	acc := acquireAccumulator(weights, filter, false)
	defer releaseAccumulator(acc)

	if err := feedMaterialized(ctx, acc, keys, keyToPods); err != nil {
		return nil, 0, err
	}
	scores, longest := acc.scores()
	return scores, longest, nil
}

// matchMaterialized feeds the accumulator from a Lookup result, walking keys
// in order and stopping at the first key without entries.
func matchMaterialized(ctx context.Context, keys []kvblock.BlockHash,
	keyToPods map[kvblock.BlockHash][]kvblock.PodEntry,
	weights map[string]float64, filter sets.Set[string],
) (map[string]PodMatch, int, error) {
	acc := acquireAccumulator(weights, filter, true)
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
			var podOrd uint32
			if pos == 0 {
				podOrd = pods.of(e.PodIdentifier)
				acc.recordLocalPod(podOrd, e.PodIdentifier)
			} else {
				var ok bool
				podOrd, ok = pods[e.PodIdentifier]
				if !ok {
					continue
				}
			}
			refs = append(refs, kvblock.EntryRef{
				PodEntry:    e,
				PodOrdinal:  podOrd,
				TierOrdinal: tiers.of(e.DeviceTier),
			})
		}
		if len(refs) == 0 || !acc.walkSnapshot(pos, kvblock.BuildPodSnapshot(refs)) {
			break
		}
	}
	return ctx.Err()
}

// ordinalTable assigns dense ordinals to names in first-seen order.
type ordinalTable map[string]uint32

func (t ordinalTable) of(name string) uint32 {
	if id, ok := t[name]; ok {
		return id
	}
	id := uint32(len(t)) //#nosec G115 -- bounded by per-request entry count
	t[name] = id
	return id
}

// tierWeight caches one tier's resolved weight, keyed by tier ordinal.
type tierWeight struct {
	ordinal uint32
	weight  float64
}

// podAccum holds per-pod accumulation state for ordinals in the walk.
type podAccum struct {
	dropPos   uint32
	matched   uint32
	confirmed uint32
	score     float64
	keyWeight float64
}

// tierChainBitset tracks one tier's contiguous prefix across candidate pods.
type tierChainBitset struct {
	name       string
	ord        uint32
	active     collections.Bitset
	count      [inlinePodSlots]uint32
	extraCount []uint32
}

func (tc *tierChainBitset) countAt(a *prefixAccumulator, ord int) *uint32 {
	if uint(ord) < inlinePodSlots {
		return &tc.count[ord]
	}
	idx := a.overflowSlot(ord)
	for len(tc.extraCount) <= idx {
		tc.extraCount = append(tc.extraCount, 0)
	}
	return &tc.extraCount[idx]
}

// prefixAccumulator folds an ordered walk over request keys into per-pod
// prefix matches using collections.Bitset. Candidates are the pods holding
// the first key, each chain ends at the first key its pod does not hold,
// multiple tiers for a pod at one key take the highest tier weight, and every
// tier tracks its own contiguous prefix from the first key.
type prefixAccumulator struct {
	weights    map[string]float64
	filter     sets.Set[string]
	trackTiers bool
	first      bool
	singleTier bool

	snapWalker    kvblock.SnapshotWalker
	localPodNames []string

	initMask      collections.Bitset
	activeMask    collections.Bitset
	confirmedMask collections.Bitset
	droppedMask   collections.Bitset
	firstDropPos  int
	endPos        int

	fastTierOrd  uint32
	fastTierName string
	fastWeight   float64

	pods          [inlinePodSlots]podAccum
	extraPods     []podAccum
	overflowSlots map[int]int
	tiers         []tierChainBitset

	weightCache []tierWeight
	walkSnapFn  func(int, *kvblock.PodSnapshot) bool
}

var accumulatorPool = sync.Pool{New: func() any {
	a := &prefixAccumulator{}
	a.walkSnapFn = a.walkSnapshot
	return a
}}

func acquireAccumulator(weights map[string]float64, filter sets.Set[string], trackTiers bool) *prefixAccumulator {
	a, _ := accumulatorPool.Get().(*prefixAccumulator)
	if a.walkSnapFn == nil {
		a.walkSnapFn = a.walkSnapshot
	}
	a.weights, a.filter = weights, filter
	a.trackTiers = trackTiers
	a.first = true
	a.singleTier = false
	a.snapWalker = nil
	a.localPodNames = a.localPodNames[:0]
	a.endPos = 0
	a.tiers = a.tiers[:0]
	a.weightCache = a.weightCache[:0]
	if len(a.overflowSlots) > 0 {
		clear(a.overflowSlots)
		a.extraPods = a.extraPods[:0]
	}
	return a
}

func releaseAccumulator(a *prefixAccumulator) {
	a.weights, a.filter, a.snapWalker = nil, nil, nil
	accumulatorPool.Put(a)
}

func (a *prefixAccumulator) recordLocalPod(ord uint32, name string) {
	for len(a.localPodNames) <= int(ord) {
		a.localPodNames = append(a.localPodNames, "")
	}
	a.localPodNames[ord] = name
}

func (a *prefixAccumulator) podNameOf(ord int) string {
	if a.snapWalker != nil {
		return a.snapWalker.PodName(uint32(ord)) //#nosec G115 -- bitset ordinal is non-negative
	}
	if ord < len(a.localPodNames) {
		return a.localPodNames[ord]
	}
	return ""
}

func (a *prefixAccumulator) overflowSlot(ord int) int {
	if a.overflowSlots == nil {
		a.overflowSlots = make(map[int]int)
	}
	if idx, ok := a.overflowSlots[ord]; ok {
		return idx
	}
	idx := len(a.overflowSlots)
	a.overflowSlots[ord] = idx
	a.extraPods = append(a.extraPods, podAccum{})
	return idx
}

func (a *prefixAccumulator) pod(ord int) *podAccum {
	if uint(ord) < inlinePodSlots {
		return &a.pods[ord]
	}
	return &a.extraPods[a.overflowSlot(ord)]
}

func (a *prefixAccumulator) appendTierChain(ord uint32, name string, active collections.Bitset, initCount uint32) {
	n := len(a.tiers)
	if n < cap(a.tiers) {
		a.tiers = a.tiers[:n+1]
	} else {
		a.tiers = append(a.tiers, tierChainBitset{})
	}
	tc := &a.tiers[n]
	tc.ord = ord
	tc.name = name
	tc.active = active
	tc.extraCount = tc.extraCount[:0]
	for podOrd := range a.initMask.All() {
		if active.Has(podOrd) {
			*tc.countAt(a, podOrd) = initCount
		} else {
			*tc.countAt(a, podOrd) = 0
		}
	}
}

func (a *prefixAccumulator) walkSnapshot(pos int, snap *kvblock.PodSnapshot) bool {
	if snap == nil || len(snap.Tiers) == 0 {
		return false
	}
	if a.first {
		return a.initFirstKey(snap)
	}
	if a.singleTier {
		if len(snap.Tiers) == 1 && snap.Tiers[0].TierOrd == a.fastTierOrd {
			return a.stepSingleTier(pos, &snap.Tiers[0].Pods)
		}
		a.transitionToMultiTier()
	}
	return a.stepMultiTier(pos, snap)
}

func (a *prefixAccumulator) initFirstKey(snap *kvblock.PodSnapshot) bool {
	initMask := snap.AllPods.Clone()
	if a.filter.Len() > 0 {
		for ord := range initMask.All() {
			if !a.filter.Has(a.podNameOf(ord)) {
				initMask.Clear(ord)
			}
		}
	}
	if initMask.IsEmpty() {
		return false
	}
	a.first = false
	a.initMask = initMask
	a.activeMask = initMask.Clone()
	a.endPos = 1

	if len(snap.Tiers) == 1 {
		ts := &snap.Tiers[0]
		a.singleTier = true
		a.droppedMask = collections.Bitset{}
		a.firstDropPos = 0
		a.fastTierOrd = ts.TierOrd
		a.fastTierName = ts.TierName
		a.fastWeight = a.weightOf(ts.TierName, ts.TierOrd)
		return true
	}

	a.singleTier = false
	a.confirmedMask = snap.ConfirmedPods.Clone()
	a.confirmedMask.And(&initMask)
	var scored collections.Bitset
	for i := range snap.Tiers {
		ts := &snap.Tiers[i]
		tierPods := ts.Pods.Clone()
		tierPods.And(&initMask)
		if tierPods.IsEmpty() {
			continue
		}
		w := a.weightOf(ts.TierName, ts.TierOrd)
		for ord := range tierPods.All() {
			p := a.pod(ord)
			if !scored.Has(ord) {
				scored.Set(ord)
				p.score = w
			} else if w > p.score {
				p.score = w
			}
		}
		if a.trackTiers {
			a.appendTierChain(ts.TierOrd, ts.TierName, tierPods, 1)
		}
	}
	for ord := range initMask.All() {
		p := a.pod(ord)
		p.matched = 1
		if a.confirmedMask.Has(ord) {
			p.confirmed = 1
		} else {
			p.confirmed = 0
		}
	}
	return true
}

func (a *prefixAccumulator) stepSingleTier(pos int, podMask *collections.Bitset) bool {
	if podMask.ContainsAll(&a.activeMask) {
		a.endPos = pos + 1
		return true
	}
	if a.firstDropPos == 0 {
		a.firstDropPos = a.endPos
		a.activeMask.And(podMask)
	} else {
		dropped := a.activeMask.AndNot(podMask)
		dropCount := uint32(a.endPos) //#nosec G115 -- endPos is bounded by len(requestKeys)
		for ord := range dropped.All() {
			a.pod(ord).dropPos = dropCount
		}
		a.droppedMask.Or(&dropped)
		a.activeMask.And(podMask)
	}
	if a.activeMask.IsEmpty() {
		return false
	}
	a.endPos = pos + 1
	return true
}

func (a *prefixAccumulator) transitionToMultiTier() {
	a.singleTier = false
	confirmed := a.fastTierName != SpeculativeTier
	if confirmed {
		a.confirmedMask = a.activeMask.Clone()
	} else {
		a.confirmedMask = collections.Bitset{}
	}
	if a.trackTiers {
		a.appendTierChain(a.fastTierOrd, a.fastTierName, a.activeMask.Clone(), 0)
	}
	for ord := range a.initMask.All() {
		p := a.pod(ord)
		var m uint32
		switch {
		case a.activeMask.Has(ord):
			m = uint32(a.endPos) //#nosec G115 -- endPos is bounded by len(requestKeys)
		case a.droppedMask.Has(ord):
			m = p.dropPos
		default:
			m = uint32(a.firstDropPos) //#nosec G115 -- firstDropPos is bounded by len(requestKeys)
		}
		p.matched = m
		if confirmed {
			p.confirmed = m
		} else {
			p.confirmed = 0
		}
		p.score = a.fastWeight * float64(m)
		if a.trackTiers {
			*a.tiers[0].countAt(a, ord) = m
		}
	}
}

func (a *prefixAccumulator) stepMultiTier(pos int, snap *kvblock.PodSnapshot) bool {
	delta := uint32(pos + 1 - a.endPos) //#nosec G115 -- pos+1 >= endPos
	a.activeMask.And(&snap.AllPods)
	if a.activeMask.IsEmpty() {
		a.confirmedMask = collections.Bitset{}
		if a.trackTiers {
			for i := range a.tiers {
				a.tiers[i].active = collections.Bitset{}
			}
		}
		return false
	}

	if !a.confirmedMask.IsEmpty() {
		a.confirmedMask.And(&snap.ConfirmedPods)
		for ord := range a.confirmedMask.All() {
			a.pod(ord).confirmed += delta
		}
	}

	if len(snap.Tiers) == 1 {
		ts := &snap.Tiers[0]
		w := a.weightOf(ts.TierName, ts.TierOrd) * float64(delta)
		for ord := range a.activeMask.All() {
			p := a.pod(ord)
			p.matched += delta
			p.score += w
		}
	} else {
		var scored collections.Bitset
		for i := range snap.Tiers {
			ts := &snap.Tiers[i]
			live := ts.Pods.Clone()
			live.And(&a.activeMask)
			if live.IsEmpty() {
				continue
			}
			w := a.weightOf(ts.TierName, ts.TierOrd) * float64(delta)
			for ord := range live.All() {
				p := a.pod(ord)
				if !scored.Has(ord) {
					scored.Set(ord)
					p.keyWeight = w
				} else if w > p.keyWeight {
					p.keyWeight = w
				}
			}
		}
		for ord := range a.activeMask.All() {
			p := a.pod(ord)
			p.matched += delta
			p.score += p.keyWeight
		}
	}

	if a.trackTiers {
		for i := range a.tiers {
			tc := &a.tiers[i]
			if tc.active.IsEmpty() {
				continue
			}
			ts := findTierSnapshot(snap.Tiers, tc.ord)
			if ts == nil {
				tc.active = collections.Bitset{}
				continue
			}
			tc.active.And(&ts.Pods)
			for ord := range tc.active.All() {
				*tc.countAt(a, ord) += delta
			}
		}
	}

	a.endPos = pos + 1
	return true
}

func findTierSnapshot(tiers []kvblock.TierSnapshot, ord uint32) *kvblock.TierSnapshot {
	for i := range tiers {
		if tiers[i].TierOrd == ord {
			return &tiers[i]
		}
	}
	return nil
}

// result materializes the accumulated matches and the longest matched chain.
func (a *prefixAccumulator) result() (map[string]PodMatch, int) {
	if a.first {
		return map[string]PodMatch{}, 0
	}
	total := a.initMask.Len()
	out := make(map[string]PodMatch, total)
	if a.singleTier {
		wScore := a.fastWeight
		endPos := a.endPos
		confirmedFactor := 1
		if a.fastTierName == SpeculativeTier {
			confirmedFactor = 0
		}
		endByTier := singleTierMap(a.fastTierName, endPos)
		endMatch := PodMatch{
			WeightedScore:   wScore * float64(endPos),
			MatchedBlocks:   endPos,
			ConfirmedBlocks: endPos * confirmedFactor,
			BlocksByTier:    endByTier,
		}
		if a.firstDropPos == 0 {
			for ord := range a.initMask.All() {
				if endByTier == nil {
					endMatch.BlocksByTier = map[string]int{a.fastTierName: endPos}
				}
				out[a.podNameOf(ord)] = endMatch
			}
			return out, endPos
		}

		firstDropped := a.initMask.AndNot(&a.droppedMask)
		firstDropped.ClearFrom(&a.activeMask)
		firstByTier := singleTierMap(a.fastTierName, a.firstDropPos)
		firstMatch := PodMatch{
			WeightedScore:   wScore * float64(a.firstDropPos),
			MatchedBlocks:   a.firstDropPos,
			ConfirmedBlocks: a.firstDropPos * confirmedFactor,
			BlocksByTier:    firstByTier,
		}
		for ord := range firstDropped.All() {
			if firstByTier == nil {
				firstMatch.BlocksByTier = map[string]int{a.fastTierName: a.firstDropPos}
			}
			out[a.podNameOf(ord)] = firstMatch
		}
		for ord := range a.activeMask.All() {
			if endByTier == nil {
				endMatch.BlocksByTier = map[string]int{a.fastTierName: endPos}
			}
			out[a.podNameOf(ord)] = endMatch
		}
		for ord := range a.droppedMask.All() {
			m := int(a.pod(ord).dropPos)
			byTier := singleTierMap(a.fastTierName, m)
			if byTier == nil {
				byTier = map[string]int{a.fastTierName: m}
			}
			out[a.podNameOf(ord)] = PodMatch{
				WeightedScore:   wScore * float64(m),
				MatchedBlocks:   m,
				ConfirmedBlocks: m * confirmedFactor,
				BlocksByTier:    byTier,
			}
		}
		return out, endPos
	}

	for ord := range a.initMask.All() {
		p := a.pod(ord)
		m := int(p.matched)
		var byTier map[string]int
		if len(a.tiers) == 1 {
			c := int(*a.tiers[0].countAt(a, ord))
			byTier = singleTierMap(a.tiers[0].name, c)
			if byTier == nil {
				byTier = map[string]int{a.tiers[0].name: c}
			}
		} else {
			byTier = make(map[string]int, len(a.tiers))
			for i := range a.tiers {
				if c := int(*a.tiers[i].countAt(a, ord)); c > 0 {
					byTier[a.tiers[i].name] = c
				}
			}
		}
		out[a.podNameOf(ord)] = PodMatch{
			WeightedScore:   p.score,
			MatchedBlocks:   m,
			ConfirmedBlocks: int(p.confirmed),
			BlocksByTier:    byTier,
		}
	}
	return out, a.endPos
}

// scores materializes the accumulated weighted scores and the longest
// matched chain length without allocating per-pod tier maps.
func (a *prefixAccumulator) scores() (map[string]float64, int) {
	if a.first {
		return map[string]float64{}, 0
	}
	total := a.initMask.Len()
	out := make(map[string]float64, total)
	if a.singleTier {
		wScore := a.fastWeight
		endPos := a.endPos
		endScore := wScore * float64(endPos)
		if a.firstDropPos == 0 {
			for ord := range a.initMask.All() {
				out[a.podNameOf(ord)] = endScore
			}
			return out, endPos
		}

		firstDropped := a.initMask.AndNot(&a.droppedMask)
		firstDropped.ClearFrom(&a.activeMask)
		firstScore := wScore * float64(a.firstDropPos)
		for ord := range firstDropped.All() {
			out[a.podNameOf(ord)] = firstScore
		}
		for ord := range a.activeMask.All() {
			out[a.podNameOf(ord)] = endScore
		}
		for ord := range a.droppedMask.All() {
			out[a.podNameOf(ord)] = wScore * float64(a.pod(ord).dropPos)
		}
		return out, endPos
	}

	for ord := range a.initMask.All() {
		out[a.podNameOf(ord)] = a.pod(ord).score
	}
	return out, a.endPos
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
