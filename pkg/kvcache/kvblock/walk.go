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
	"context"
	"sync"
	"sync/atomic"
)

// EntryRef is one indexed entry with the ordinals the index assigned to its
// pod identifier and device tier. Two entries share a PodOrdinal exactly
// when they share a PodIdentifier, and a TierOrdinal exactly when they share
// a DeviceTier. An ordinal is assigned on the first Add of its name and
// belongs to that name for the index lifetime: Evict and Clear never reclaim
// or reassign one, and an Add whose new names would exceed the index's caps
// fails before writing anything. Live entries hold a sparse subset of the
// assigned range, so consumers key request-local tables by ordinals and
// never size state by them.
type EntryRef struct {
	PodEntry
	PodOrdinal  uint32
	TierOrdinal uint32
}

// PodSnapshot is an immutable generation of a PodCache's entries.
type PodSnapshot struct {
	Entries []EntryRef
	// PodMask has bit (PodOrdinal) set for every entry in Entries when
	// MaskValid is true (all entries share one effective tier and have
	// PodOrdinal < 256).
	PodMask   [4]uint64
	TierOrd   uint32
	TierName  string
	MaskValid bool
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
//   - When the walk ends, for any reason, the prefix of requestKeys through
//     the last key found is marked most recently used in position order
//     under one acquisition, skipping keys absent by then. Positions after
//     the last key found are not promoted. Lookup promotes the same way, so
//     a prefix that is only read stays resident under capacity pressure.
type KeyWalker interface {
	WalkKeys(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, found bool, entries []EntryRef) bool) error
}

// SnapshotWalker is an optional Index capability that visits each request key's
// immutable PodSnapshot and resolves pod ordinals to pod identifiers.
type SnapshotWalker interface {
	WalkSnapshots(ctx context.Context, requestKeys []BlockHash,
		visit func(pos int, snap *PodSnapshot) bool) error
	PodName(ord uint32) string
}

const walkBatchSize = 256

// WalkKeys implements KeyWalker. Keys follow cached PodCache chains lock-free
// or are peeked in batches under shard shared locks, each key's immutable
// entry snapshot is visited without locking, and visited caches are stamped
// with read sequence numbers on every exit path.
func (m *InMemoryIndex) WalkKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	var batch [walkBatchSize]*PodCache
	batchVisited := 0
	prevSeq := m.data.writeSeq.Load()
	var nextPC *PodCache
	// Every exit, cancellation included, refreshes what was read.
	defer func() { m.data.stampBatch(batch[:batchVisited], &prevSeq) }()

	for base := 0; base < len(requestKeys); {
		if base&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		limit := min(base+walkBatchSize, len(requestKeys))
		if nextCheck := (base | cancellationCheckMask) + 1; limit > nextCheck {
			limit = nextCheck
		}
		var n int
		n, nextPC = m.data.peekBatch(requestKeys[base:limit], batch[:], nextPC)
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
		m.data.stampBatch(batch[:batchVisited], &prevSeq)
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
	prevSeq := m.data.writeSeq.Load()
	sharded := m.data.shardMask != 0
	var nextPC *PodCache
	defer func() { m.data.stampBatch(batch[:batchVisited], &prevSeq) }()

	recordPC := func(pc *PodCache) {
		if sharded && pc.readSeq.Load() > pc.addedSeq.Load() {
			return
		}
		if batchVisited == walkBatchSize {
			m.data.stampBatch(batch[:batchVisited], &prevSeq)
			batchVisited = 0
		}
		batch[batchVisited] = pc
		batchVisited++
	}

	for base := 0; base < len(requestKeys); {
		if base&cancellationCheckMask == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		limit := min(base+walkBatchSize, len(requestKeys))
		if nextCheck := (base | cancellationCheckMask) + 1; limit > nextCheck {
			limit = nextCheck
		}
		for pos := base; pos < limit; pos++ {
			k := requestKeys[pos]
			var pc *PodCache
			if nextPC != nil && nextPC.key == k && nextPC.resident.Load() {
				pc = nextPC
			} else {
				pc = m.data.peekOne(k)
			}
			if pc == nil {
				nextPC = nil
				if !visit(pos, nil) {
					return ctx.Err()
				}
				continue
			}
			snap := pc.snapshot.Load()
			if snap == nil || len(snap.Entries) == 0 {
				nextPC = pc.next.Load()
				if !visit(pos, nil) {
					return ctx.Err()
				}
				continue
			}
			recordPC(pc)
			nextPC = pc.next.Load()
			if !visit(pos, snap) {
				return ctx.Err()
			}
			if snap.MaskValid {
				startPos := pos
				for pos+1 < limit {
					nk := requestKeys[pos+1]
					var npc *PodCache
					if nextPC != nil && nextPC.key == nk && nextPC.resident.Load() {
						npc = nextPC
					} else {
						npc = m.data.peekOne(nk)
					}
					if npc == nil {
						nextPC = nil
						break
					}
					if npc.snapshot.Load() != snap {
						nextPC = npc
						break
					}
					recordPC(npc)
					pos++
					nextPC = npc.next.Load()
				}
				if pos > startPos {
					if !visit(pos, snap) {
						return ctx.Err()
					}
				}
			}
		}
		base = limit
	}
	return ctx.Err()
}

// PodName returns the pod identifier assigned to ord, or "" if unassigned.
func (m *InMemoryIndex) PodName(ord uint32) string {
	return m.pods.nameForOrdinal(ord)
}

// interner assigns dense uint32 ordinals to strings, stable for its lifetime
// and never reused, up to a fixed number of distinct strings. Callers hold mu
// across a whole batch so a batch is assigned all or nothing.
type interner struct {
	mu        sync.RWMutex
	ids       map[string]uint32
	names     []string
	fastNames [256]atomic.Pointer[string]
	limit     int
}

func newInterner(limit int) *interner {
	return &interner{ids: make(map[string]uint32), limit: limit}
}

// fitsLocked reports whether the names not yet interned fit under the
// limit. Called with mu held.
func (in *interner) fitsLocked(names func(yield func(string))) bool {
	pending := 0
	var seenBuf [8]string
	var seenMap map[string]struct{}
	names(func(s string) {
		if _, ok := in.ids[s]; ok {
			return
		}
		if seenMap != nil {
			if _, dup := seenMap[s]; dup {
				return
			}
			seenMap[s] = struct{}{}
			pending++
			return
		}
		for i := range pending {
			if seenBuf[i] == s {
				return
			}
		}
		if pending < len(seenBuf) {
			seenBuf[pending] = s
			pending++
			return
		}
		seenMap = make(map[string]struct{}, len(seenBuf)+1)
		for _, prev := range seenBuf {
			seenMap[prev] = struct{}{}
		}
		seenMap[s] = struct{}{}
		pending++
	})
	return len(in.ids)+pending <= in.limit
}

// internLocked returns the ordinal for s, assigning the next free one on
// first use. Called with mu held, after fitsLocked.
func (in *interner) internLocked(s string) uint32 {
	if id, ok := in.ids[s]; ok {
		return id
	}
	id := uint32(len(in.ids))
	in.ids[s] = id
	in.names = append(in.names, s)
	if id < uint32(len(in.fastNames)) {
		sCopy := s
		in.fastNames[id].Store(&sCopy)
	}
	return id
}

func (in *interner) nameForOrdinal(ord uint32) string {
	if ord < uint32(len(in.fastNames)) {
		if p := in.fastNames[ord].Load(); p != nil {
			return *p
		}
	}
	in.mu.RLock()
	defer in.mu.RUnlock()
	if int(ord) < len(in.names) {
		return in.names[ord]
	}
	return ""
}
