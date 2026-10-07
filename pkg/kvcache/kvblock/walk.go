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
	"context"
	"slices"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
)

const (
	speculativeSnapshotTierOrdinal = ^uint32(0)
	speculativeSnapshotTierName    = "speculative"
	walkBatchSize                  = 256
)

// EntryRef is one indexed entry with the ordinals the index assigned to its
// pod identifier and device tier. Two entries share a PodOrdinal exactly
// when they share a PodIdentifier, and a TierOrdinal exactly when they share
// a DeviceTier. An ordinal is assigned on the first Add of its name and
// belongs to that name for the index lifetime: Evict and Clear never reclaim
// or reassign one, and an Add whose new names would exceed the index's caps
// fails before writing anything.
type EntryRef struct {
	PodEntry
	PodOrdinal  uint32
	TierOrdinal uint32
}

// TierSnapshot holds the set of pod ordinals resident in one device tier (or
// the speculative tier) within a PodSnapshot.
type TierSnapshot struct {
	TierName string
	TierOrd  uint32
	Pods     collections.Bitset
}

// PodSnapshot is an immutable generation of a PodCache's entries.
type PodSnapshot struct {
	Entries []EntryRef
	// Tiers holds one TierSnapshot per distinct effective tier in Entries,
	// sorted by TierOrd. When len(Tiers) == 1, Tiers is backed by tierBuf.
	Tiers []TierSnapshot
	// AllPods has bit PodOrdinal set for every entry in Entries across tiers.
	AllPods collections.Bitset
	// ConfirmedPods has bit PodOrdinal set for every non-speculative entry in Entries.
	ConfirmedPods collections.Bitset
	tierBuf       [1]TierSnapshot
}

func effectiveTier(e *EntryRef) (name string, ord uint32) {
	if e.Speculative || e.DeviceTier == speculativeSnapshotTierName {
		return speculativeSnapshotTierName, speculativeSnapshotTierOrdinal
	}
	return e.DeviceTier, e.TierOrdinal
}

// BuildPodSnapshot constructs an immutable PodSnapshot with per-tier bitsets
// from entries.
func BuildPodSnapshot(entries []EntryRef) *PodSnapshot {
	if len(entries) == 0 {
		return nil
	}
	name0, ord0 := effectiveTier(&entries[0])
	snap := &PodSnapshot{
		Entries: entries,
	}
	snap.tierBuf[0] = TierSnapshot{
		TierName: name0,
		TierOrd:  ord0,
	}
	singleTier := true
	for i := range entries {
		e := &entries[i]
		_, tOrd := effectiveTier(e)
		if tOrd != ord0 {
			singleTier = false
			break
		}
		snap.tierBuf[0].Pods.Set(int(e.PodOrdinal))
	}
	if singleTier {
		snap.Tiers = snap.tierBuf[:1]
		snap.AllPods = snap.tierBuf[0].Pods
		if ord0 != speculativeSnapshotTierOrdinal {
			snap.ConfirmedPods = snap.AllPods
		}
		return snap
	}

	snap.tierBuf[0].Pods = collections.Bitset{}
	var tiers []TierSnapshot
	for i := range entries {
		e := &entries[i]
		tName, tOrd := effectiveTier(e)
		podOrd := int(e.PodOrdinal)
		snap.AllPods.Set(podOrd)
		if tOrd != speculativeSnapshotTierOrdinal {
			snap.ConfirmedPods.Set(podOrd)
		}
		found := false
		for t := range tiers {
			if tiers[t].TierOrd == tOrd {
				tiers[t].Pods.Set(podOrd)
				found = true
				break
			}
		}
		if !found {
			var ts TierSnapshot
			ts.TierName = tName
			ts.TierOrd = tOrd
			ts.Pods.Set(podOrd)
			tiers = append(tiers, ts)
		}
	}
	slices.SortFunc(tiers, func(a, b TierSnapshot) int {
		return cmp.Compare(a.TierOrd, b.TierOrd)
	})
	snap.Tiers = tiers
	return snap
}

// KeyWalker is an optional Index capability: visit requestKeys in input
// order without materializing per-key entry slices.
//
// The walk contract:
//
//   - visit runs once per position of requestKeys, in order, until it
//     returns false, the keys run out, or ctx is cancelled. A key listed
//     twice is visited at both positions, each visit reading the index anew.
//   - found reports whether the index held a generation of the key when the
//     position was looked up. A miss is visited with found=false and no
//     entries; the walk continues.
//   - entries is every unfiltered record in that generation, in an order the
//     consumer must not rely on. Consumers must treat entries as read-only.
//   - entries is backed by an immutable copy-on-write snapshot slice that
//     remains valid for the duration of visit without holding a per-key lock.
//     Writers publish a new slice on each mutation.
//   - A visit sees an internally consistent generation, exclusive of writers
//     and other visits using that generation. Capacity eviction may detach it
//     and a later Add may install a new generation of the same key while
//     visit runs. There is no snapshot across positions: a concurrent Add,
//     Evict, or Clear may be visible at some positions and not at others.
//   - Cancellation is polled at the first position, every 256th position
//     after it, and once more when the walk ends, so a cancelled ctx always
//     yields ctx.Err(); visits may run between the cancellation and the
//     next poll.
//   - Visited keys through the last key found have their read recency recorded
//     in position order, skipping keys absent by then. Positions after the last
//     key found are not promoted. Lookup promotes the same way, so a prefix
//     that is only read stays resident under capacity pressure.
type KeyWalker interface {
	WalkKeys(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, found bool, entries []EntryRef) bool) error
}

// SnapshotWalker is an optional Index capability that visits each request key's
// immutable PodSnapshot and resolves pod ordinals to pod identifiers.
// Consecutive positions that share an identical *PodSnapshot pointer may be
// coalesced so visit is called at the start and end positions of the run; a
// miss visits with snap == nil.
type SnapshotWalker interface {
	WalkSnapshots(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, snap *PodSnapshot) bool) error
	PodName(ord uint32) string
}

// WalkKeys implements KeyWalker. Keys are peeked in batches, each key's
// immutable entry snapshot is visited without locking, and visited caches are
// marked as referenced on every exit path.
func (m *InMemoryIndex) WalkKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	var batch [walkBatchSize]*PodCache
	batchVisited := 0
	// Every exit, cancellation included, refreshes what was read.
	defer func() { m.data.stampBatch(batch[:batchVisited]) }()

	for base := 0; base < len(requestKeys); {
		if base&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		limit := min(base+walkBatchSize, len(requestKeys))
		if nextCheck := (base | cancellationCheckMask) + 1; limit > nextCheck {
			limit = nextCheck
		}
		n := m.data.peekBatch(requestKeys[base:limit], batch[:])
		for i := range n {
			pos := base + i
			pc := batch[i]
			if pc == nil {
				if !visit(pos, false, nil) {
					return ctx.Err()
				}
				continue
			}
			entries := pc.loadEntries()
			if len(entries) == 0 {
				if !visit(pos, false, nil) {
					return ctx.Err()
				}
				continue
			}
			batchVisited = i + 1
			if !visit(pos, true, entries) {
				return ctx.Err()
			}
		}
		m.data.stampBatch(batch[:batchVisited])
		batchVisited = 0
		base += n
	}
	return ctx.Err()
}

// WalkSnapshots implements SnapshotWalker.
func (m *InMemoryIndex) WalkSnapshots(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, snap *PodSnapshot) bool,
) error {
	var batch [walkBatchSize]*PodCache
	batchVisited := 0
	sharded := m.data.shardMask != 0
	defer func() { m.data.stampBatch(batch[:batchVisited]) }()

	recordPC := func(pc *PodCache) {
		if sharded {
			if !pc.referenced.Load() {
				pc.referenced.Store(true)
			}
			return
		}
		if batchVisited == walkBatchSize {
			m.data.stampBatch(batch[:batchVisited])
			batchVisited = 0
		}
		batch[batchVisited] = pc
		batchVisited++
	}

	var carriedPC *PodCache
	carriedPos := -1

	for base := 0; base < len(requestKeys); {
		if base&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		limit := min(base+walkBatchSize, len(requestKeys))
		if nextCheck := (base | cancellationCheckMask) + 1; limit > nextCheck {
			limit = nextCheck
		}
		for pos := base; pos < limit; pos++ {
			var pc *PodCache
			if carriedPos == pos {
				pc = carriedPC
				carriedPC = nil
				carriedPos = -1
			} else {
				pc = m.data.peekOne(requestKeys[pos])
			}
			if pc == nil {
				if !visit(pos, nil) {
					return ctx.Err()
				}
				continue
			}
			snap := pc.snapshot.Load()
			if snap == nil || len(snap.Entries) == 0 {
				if !visit(pos, nil) {
					return ctx.Err()
				}
				continue
			}
			recordPC(pc)
			if !visit(pos, snap) {
				return ctx.Err()
			}
			startPos := pos
			for pos+1 < limit {
				npc := m.data.peekOne(requestKeys[pos+1])
				if npc == nil {
					break
				}
				if npc.snapshot.Load() != snap {
					carriedPC = npc
					carriedPos = pos + 1
					break
				}
				recordPC(npc)
				pos++
			}
			if pos > startPos {
				if !visit(pos, snap) {
					return ctx.Err()
				}
			}
		}
		base = limit
	}
	return ctx.Err()
}

// PodName returns the pod identifier assigned to ord, or "" if unassigned.
func (m *InMemoryIndex) PodName(ord uint32) string {
	return m.pods.At(int(ord))
}
