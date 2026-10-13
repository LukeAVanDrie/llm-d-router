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

// Package atomictable is a pointer-free hash table from uint64 keys to a
// pair of uint64 words, with one writer and lock-free readers.
//
// Every slot word is accessed atomically, so Load may run concurrently with
// Store and Delete without a data race, and always terminates. A Load that
// overlaps a write may observe a partial write or miss an entry that a
// deletion is moving; its result is meaningful only when no write overlapped
// it. Callers detect overlap themselves, typically with a version counter the
// writer advances around each write.
package atomictable

import (
	"math/bits"
	"math/rand/v2"
	"sync/atomic"
)

// occupied marks a slot in use. Callers' second value word must leave this
// bit clear.
const occupied = 1 << 63

// slot is one entry. meta holds the caller's second word with occupied set;
// a zero meta marks an empty slot, so the key itself may take any value.
type slot struct {
	key  atomic.Uint64
	val  atomic.Uint64
	meta atomic.Uint64
}

type array struct {
	slots []slot
	mask  uint64 // len(slots) - 1
	shift uint   // 64 - log2(len(slots))
	seed  uint64 // hash seed, so crafted keys cannot collide on every table
}

// Table maps uint64 keys to (val, meta) pairs. The zero value is an empty
// table. Store and Delete must not run concurrently with each other.
type Table struct {
	arr   atomic.Pointer[array]
	count int // entries, written by the writer only
}

const minSlots = 16

func home(a *array, key uint64) uint64 {
	return ((key ^ a.seed) * 0x9e3779b97f4a7c15) >> a.shift
}

// Load returns the pair stored for key. It is safe to call concurrently with
// the writer; see the package doc for what a concurrent result means.
func (t *Table) Load(key uint64) (val, meta uint64, ok bool) {
	a := t.arr.Load()
	if a == nil {
		return 0, 0, false
	}
	mask := a.mask
	i := home(a, key)
	for range len(a.slots) {
		s := &a.slots[i]
		m := s.meta.Load()
		if m == 0 {
			return 0, 0, false
		}
		if s.key.Load() == key {
			return s.val.Load(), m &^ occupied, true
		}
		i = (i + 1) & mask
	}
	return 0, 0, false
}

// Len returns the number of entries. Writer only.
func (t *Table) Len() int { return t.count }

// Store sets the pair for key. meta must leave bit 63 clear. Writer only.
func (t *Table) Store(key, val, meta uint64) {
	a := t.arr.Load()
	if a == nil || (t.count+1)*4 > len(a.slots)*3 {
		a = t.grow(a)
	}
	mask := a.mask
	i := home(a, key)
	for {
		s := &a.slots[i]
		m := s.meta.Load()
		if m == 0 {
			t.count++
			s.key.Store(key)
			s.val.Store(val)
			s.meta.Store(meta | occupied)
			return
		}
		if s.key.Load() == key {
			s.val.Store(val)
			s.meta.Store(meta | occupied)
			return
		}
		i = (i + 1) & mask
	}
}

// Delete removes key and reports whether it was present. Writer only.
// Entries after it in its probe run shift back, so the table holds no
// tombstones.
func (t *Table) Delete(key uint64) bool {
	a := t.arr.Load()
	if a == nil {
		return false
	}
	mask := a.mask
	i := home(a, key)
	for {
		s := &a.slots[i]
		if s.meta.Load() == 0 {
			return false
		}
		if s.key.Load() == key {
			break
		}
		i = (i + 1) & mask
	}
	t.count--
	// Backward-shift deletion: walk the run after the hole, moving back each
	// entry whose home does not lie strictly between the hole and itself.
	hole := i
	for j := (i + 1) & mask; ; j = (j + 1) & mask {
		s := &a.slots[j]
		m := s.meta.Load()
		if m == 0 {
			break
		}
		k := s.key.Load()
		h := home(a, k)
		if (j-h)&mask >= (j-hole)&mask {
			d := &a.slots[hole]
			d.key.Store(k)
			d.val.Store(s.val.Load())
			d.meta.Store(m)
			hole = j
		}
	}
	a.slots[hole].meta.Store(0)
	return true
}

// Range calls fn for every entry until fn returns false. Writer only.
func (t *Table) Range(fn func(key, val, meta uint64) bool) {
	a := t.arr.Load()
	if a == nil {
		return
	}
	for i := range a.slots {
		s := &a.slots[i]
		if m := s.meta.Load(); m != 0 && !fn(s.key.Load(), s.val.Load(), m&^occupied) {
			return
		}
	}
}

// Clear removes every entry, keeping the current capacity. Writer only.
func (t *Table) Clear() {
	if a := t.arr.Load(); a != nil {
		t.arr.Store(newArray(len(a.slots), a.seed))
	}
	t.count = 0
}

func newArray(n int, seed uint64) *array {
	return &array{
		seed:  seed,
		slots: make([]slot, n),
		mask:  uint64(n - 1),                          //#nosec G115 -- n is a positive power of two
		shift: uint(64 - bits.TrailingZeros(uint(n))), //#nosec G115 -- n is a power of two
	}
}

// grow publishes a table twice the size, or the initial one. Readers holding
// the old array keep reading it safely.
func (t *Table) grow(old *array) *array {
	n, seed := minSlots, rand.Uint64()|1 //#nosec G404 -- hash seed, not a secret
	if old != nil {
		n, seed = 2*len(old.slots), old.seed
	}
	a := newArray(n, seed)
	mask := a.mask
	if old != nil {
		for i := range old.slots {
			s := &old.slots[i]
			m := s.meta.Load()
			if m == 0 {
				continue
			}
			k := s.key.Load()
			j := home(a, k)
			for a.slots[j].meta.Load() != 0 {
				j = (j + 1) & mask
			}
			a.slots[j].key.Store(k)
			a.slots[j].val.Store(s.val.Load())
			a.slots[j].meta.Store(m)
		}
	}
	t.arr.Store(a)
	return a
}
