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

package collections

import (
	"iter"
	"math/bits"
	"sync/atomic"
)

const (
	// InlineBitsetWords is the number of 64-bit words stored inline in a Bitset.
	InlineBitsetWords = 4
	// InlineBitsetBits is the number of ordinals (256) stored inline without heap allocation.
	InlineBitsetBits = InlineBitsetWords * 64

	minExtraBitsetWords = 60
)

// Bitset is a compact set of non-negative integer ordinals. Ordinals 0..255 are
// stored inline in four uint64 words with zero heap allocation; ordinals >= 256
// spill into an overflow slice allocated on demand.
type Bitset struct {
	words [InlineBitsetWords]uint64
	extra []uint64
}

// Set adds ord to b. Negative ordinals are ignored.
func (b *Bitset) Set(ord int) {
	if ord < 0 {
		return
	}
	w := ord >> 6
	bit := uint64(1) << (uint(ord) & 63)
	if w < InlineBitsetWords {
		b.words[w] |= bit
		return
	}
	b.setExtra(w-InlineBitsetWords, bit)
}

func (b *Bitset) setExtra(ew int, mask uint64) {
	if ew >= len(b.extra) {
		grown := make([]uint64, ew+1)
		copy(grown, b.extra)
		b.extra = grown
	}
	b.extra[ew] |= mask
}

// Clear removes ord from b.
func (b *Bitset) Clear(ord int) {
	if ord < 0 {
		return
	}
	w := ord >> 6
	bit := uint64(1) << (uint(ord) & 63)
	if w < InlineBitsetWords {
		b.words[w] &^= bit
		return
	}
	if ew := w - InlineBitsetWords; ew < len(b.extra) {
		b.extra[ew] &^= bit
	}
}

// Has reports whether ord is in b.
func (b *Bitset) Has(ord int) bool {
	if ord < 0 {
		return false
	}
	w := ord >> 6
	bit := uint64(1) << (uint(ord) & 63)
	if w < InlineBitsetWords {
		return (b.words[w] & bit) != 0
	}
	if ew := w - InlineBitsetWords; ew < len(b.extra) {
		return (b.extra[ew] & bit) != 0
	}
	return false
}

// IsInline reports whether b has no overflow words allocated (all ordinals < 256).
func (b *Bitset) IsInline() bool {
	return len(b.extra) == 0
}

// IsEmpty reports whether no ordinals are set in b.
func (b *Bitset) IsEmpty() bool {
	if (b.words[0] | b.words[1] | b.words[2] | b.words[3]) != 0 {
		return false
	}
	if len(b.extra) > 0 {
		return b.isEmptyExtra()
	}
	return true
}

func (b *Bitset) isEmptyExtra() bool {
	for _, w := range b.extra {
		if w != 0 {
			return false
		}
	}
	return true
}

// Len returns the number of ordinals set in b.
func (b *Bitset) Len() int {
	n := bits.OnesCount64(b.words[0]) +
		bits.OnesCount64(b.words[1]) +
		bits.OnesCount64(b.words[2]) +
		bits.OnesCount64(b.words[3])
	if len(b.extra) > 0 {
		n += b.lenExtra()
	}
	return n
}

func (b *Bitset) lenExtra() int {
	n := 0
	for _, w := range b.extra {
		n += bits.OnesCount64(w)
	}
	return n
}

// Clone returns an independent copy of b. When b has no overflow words, Clone
// copies only the inline words without heap allocation.
func (b *Bitset) Clone() Bitset {
	if len(b.extra) == 0 {
		return *b
	}
	extra := make([]uint64, len(b.extra))
	copy(extra, b.extra)
	return Bitset{words: b.words, extra: extra}
}

// And intersects b in place with other (b &= other).
func (b *Bitset) And(other *Bitset) {
	b.words[0] &= other.words[0]
	b.words[1] &= other.words[1]
	b.words[2] &= other.words[2]
	b.words[3] &= other.words[3]
	if len(b.extra) > 0 {
		b.andExtra(other)
	}
}

func (b *Bitset) andExtra(other *Bitset) {
	n := min(len(b.extra), len(other.extra))
	for i := range n {
		b.extra[i] &= other.extra[i]
	}
	clear(b.extra[n:])
}

// Or unions b in place with other (b |= other).
func (b *Bitset) Or(other *Bitset) {
	b.words[0] |= other.words[0]
	b.words[1] |= other.words[1]
	b.words[2] |= other.words[2]
	b.words[3] |= other.words[3]
	if len(other.extra) > 0 {
		b.orExtra(other)
	}
}

func (b *Bitset) orExtra(other *Bitset) {
	if len(b.extra) < len(other.extra) {
		grown := make([]uint64, len(other.extra))
		copy(grown, b.extra)
		b.extra = grown
	}
	for i, w := range other.extra {
		b.extra[i] |= w
	}
}

// ClearFrom clears from b every ordinal set in other (b &^= other).
func (b *Bitset) ClearFrom(other *Bitset) {
	b.words[0] &^= other.words[0]
	b.words[1] &^= other.words[1]
	b.words[2] &^= other.words[2]
	b.words[3] &^= other.words[3]
	if len(b.extra) > 0 {
		b.clearFromExtra(other)
	}
}

func (b *Bitset) clearFromExtra(other *Bitset) {
	n := min(len(b.extra), len(other.extra))
	for i := range n {
		b.extra[i] &^= other.extra[i]
	}
}

// AndNot returns a new Bitset containing ordinals in b that are not in other (b &^ other).
func (b *Bitset) AndNot(other *Bitset) Bitset {
	out := Bitset{
		words: [InlineBitsetWords]uint64{
			b.words[0] &^ other.words[0],
			b.words[1] &^ other.words[1],
			b.words[2] &^ other.words[2],
			b.words[3] &^ other.words[3],
		},
	}
	out.andNotExtra(b, other)
	return out
}

func (b *Bitset) andNotExtra(src, other *Bitset) {
	for i, w := range src.extra {
		if i < len(other.extra) {
			w &^= other.extra[i]
		}
		b.extra = append(b.extra, w)
	}
}

// ContainsAll reports whether every ordinal set in sub is also set in b.
func (b *Bitset) ContainsAll(sub *Bitset) bool {
	return ((sub.words[0]&^b.words[0])|
		(sub.words[1]&^b.words[1])|
		(sub.words[2]&^b.words[2])|
		(sub.words[3]&^b.words[3])) == 0 &&
		b.containsAllExtra(sub.extra)
}

func (b *Bitset) containsAllExtra(sub []uint64) bool {
	for i, sw := range sub {
		if i < len(b.extra) {
			sw &^= b.extra[i]
		}
		if sw != 0 {
			return false
		}
	}
	return true
}

// Intersects reports whether b and other share at least one set ordinal.
func (b *Bitset) Intersects(other *Bitset) bool {
	return ((b.words[0]&other.words[0])|
		(b.words[1]&other.words[1])|
		(b.words[2]&other.words[2])|
		(b.words[3]&other.words[3])) != 0 ||
		(len(b.extra) > 0 && b.intersectsExtra(other))
}

func (b *Bitset) intersectsExtra(other *Bitset) bool {
	for i := range min(len(b.extra), len(other.extra)) {
		if (b.extra[i] & other.extra[i]) != 0 {
			return true
		}
	}
	return false
}

// Equals reports whether b and other contain the exact same set of ordinals.
func (b *Bitset) Equals(other *Bitset) bool {
	return b.words == other.words &&
		(len(b.extra)+len(other.extra) == 0 || b.equalsExtra(other))
}

//go:noinline
func (b *Bitset) equalsExtra(other *Bitset) bool {
	return b.containsAllExtra(other.extra) && other.containsAllExtra(b.extra)
}

// All returns an iterator over all set ordinals in ascending order.
func (b *Bitset) All() iter.Seq[int] {
	return func(yield func(int) bool) {
		for w := range InlineBitsetWords {
			word := b.words[w]
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				word &= word - 1
				if !yield((w << 6) | bit) {
					return
				}
			}
		}
		if len(b.extra) > 0 {
			b.allExtra(yield)
		}
	}
}

func (b *Bitset) allExtra(yield func(int) bool) {
	for i, word := range b.extra {
		base := (InlineBitsetWords + i) << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			if !yield(base | bit) {
				return
			}
		}
	}
}

// AtomicBitset is a lock-free concurrent bitset supporting atomic Set, Clear,
// emptiness checks, and snapshots into a non-atomic Bitset.
type AtomicBitset struct {
	words [InlineBitsetWords]uint64
	extra atomic.Pointer[[]uint64]
}

// Set atomically sets ord in ab. Negative ordinals are ignored.
func (ab *AtomicBitset) Set(ord int) {
	if uint(ord) < InlineBitsetBits {
		atomic.OrUint64(&ab.words[ord>>6], 1<<(ord&63))
		return
	}
	ab.setExtra(ord)
}

func (ab *AtomicBitset) setExtra(ord int) {
	if ord < 0 {
		return
	}
	ew := (ord >> 6) - InlineBitsetWords
	bit := uint64(1) << (uint(ord) & 63)
	for {
		ep := ab.extra.Load()
		if ep != nil && ew < len(*ep) {
			atomic.OrUint64(&(*ep)[ew], bit)
			if ab.extra.Load() == ep {
				return
			}
			continue
		}
		newLen := max(minExtraBitsetWords, ew+1)
		if ep != nil {
			newLen = max(newLen, len(*ep)*2)
		}
		grown := make([]uint64, newLen)
		if ep != nil {
			old := *ep
			for i := range old {
				grown[i] = atomic.LoadUint64(&old[i])
			}
		}
		if ab.extra.CompareAndSwap(ep, &grown) {
			atomic.OrUint64(&grown[ew], bit)
			return
		}
	}
}

// Clear atomically clears ord in ab.
func (ab *AtomicBitset) Clear(ord int) {
	if uint(ord) < InlineBitsetBits {
		atomic.AndUint64(&ab.words[ord>>6], ^(1 << (ord & 63)))
	} else {
		ab.clearExtra(ord)
	}
}

//go:noinline
func (ab *AtomicBitset) clearExtra(ord int) {
	if ord < 0 {
		return
	}
	ew := (ord >> 6) - InlineBitsetWords
	mask := ^(uint64(1) << (uint(ord) & 63))
	for {
		ep := ab.extra.Load()
		if ep == nil || ew >= len(*ep) {
			return
		}
		atomic.AndUint64(&(*ep)[ew], mask)
		if ab.extra.Load() == ep {
			return
		}
	}
}

// IsEmpty reports whether no ordinals are currently set in ab.
func (ab *AtomicBitset) IsEmpty() bool {
	return (atomic.LoadUint64(&ab.words[0])|
		atomic.LoadUint64(&ab.words[1])|
		atomic.LoadUint64(&ab.words[2])|
		atomic.LoadUint64(&ab.words[3])) == 0 &&
		ab.isEmptyExtra()
}

func (ab *AtomicBitset) isEmptyExtra() bool {
	if ep := ab.extra.Load(); ep != nil {
		s := *ep
		for i := range s {
			if atomic.LoadUint64(&s[i]) != 0 {
				return false
			}
		}
	}
	return true
}

// Snapshot returns a non-atomic Bitset copy of all inline and allocated
// overflow words in ab.
func (ab *AtomicBitset) Snapshot() Bitset {
	out := Bitset{
		words: [InlineBitsetWords]uint64{
			atomic.LoadUint64(&ab.words[0]),
			atomic.LoadUint64(&ab.words[1]),
			atomic.LoadUint64(&ab.words[2]),
			atomic.LoadUint64(&ab.words[3]),
		},
	}
	if ep := ab.extra.Load(); ep != nil {
		out.loadExtra(*ep)
	}
	return out
}

func (b *Bitset) loadExtra(src []uint64) {
	for i := range src {
		b.extra = append(b.extra, atomic.LoadUint64(&src[i]))
	}
}
