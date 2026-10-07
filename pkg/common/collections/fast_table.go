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
	"math/bits"
	"sync/atomic"
)

// FastTableProbes is the linear-probing window length of a FastTable.
const FastTableProbes = 8

// FastTable is a bounded, lock-free power-of-two linear-probing cache of
// pointers to V. Before any probe window overflows or has an entry deleted, a
// nil slot encountered during Probe is an authoritative negative lookup.
type FastTable[V any] struct {
	slots    []atomic.Pointer[V]
	mask     uint64
	overflow uint32
}

// NewFastTable returns a FastTable sized to the next power of two >= size.
// When size <= 0, the returned table is disabled and Probe always returns
// (nil, false).
func NewFastTable[V any](size int) FastTable[V] {
	if size <= 0 {
		return FastTable[V]{}
	}
	capPow2 := 1
	if size > 1 {
		capPow2 = 1 << bits.Len64(uint64(size-1))
	}
	return FastTable[V]{
		slots: make([]atomic.Pointer[V], capPow2),
		mask:  uint64(capPow2 - 1),
	}
}

// Insert places val into the 8-slot probe window starting at hash. If all 8
// slots are occupied by other entries, it marks the table as overflowed and
// overwrites the base slot.
func (t *FastTable[V]) Insert(hash uint64, val *V) {
	if len(t.slots) == 0 || val == nil {
		return
	}
	base := hash & t.mask
	for p := range uint64(FastTableProbes) {
		if t.slots[(base+p)&t.mask].Load() == val {
			return
		}
	}
	for p := range uint64(FastTableProbes) {
		idx := (base + p) & t.mask
		cur := t.slots[idx].Load()
		if cur == val {
			return
		}
		if cur == nil {
			if t.slots[idx].CompareAndSwap(nil, val) || t.slots[idx].Load() == val {
				return
			}
		}
	}
	atomic.StoreUint32(&t.overflow, 1)
	t.slots[base].Store(val)
}

// Probe searches the 8-slot probe window starting at hash for an entry
// satisfying matchKey. It returns the matching entry (or nil on a miss) and
// whether the result is authoritative without consulting the backing store.
func (t *FastTable[V]) Probe(hash uint64, matchKey func(*V) bool) (val *V, authoritative bool) {
	if len(t.slots) == 0 {
		return nil, false
	}
	base := hash & t.mask
	for p := range uint64(FastTableProbes) {
		v := t.slots[(base+p)&t.mask].Load()
		if v == nil {
			if atomic.LoadUint32(&t.overflow) == 0 {
				return nil, true
			}
			continue
		}
		if matchKey(v) {
			return v, true
		}
	}
	return nil, false
}

// Delete clears val from the probe window starting at hash if present and
// marks the table as overflowed.
func (t *FastTable[V]) Delete(hash uint64, val *V) {
	if len(t.slots) == 0 || val == nil {
		return
	}
	atomic.StoreUint32(&t.overflow, 1)
	base := hash & t.mask
	for p := range uint64(FastTableProbes) {
		idx := (base + p) & t.mask
		t.slots[idx].CompareAndSwap(val, nil)
	}
}

// MarkOverflow disables authoritative negative lookups on nil slots.
func (t *FastTable[V]) MarkOverflow() {
	atomic.StoreUint32(&t.overflow, 1)
}

// HasOverflow reports whether any probe window has overflowed or had an entry
// deleted.
func (t *FastTable[V]) HasOverflow() bool {
	return atomic.LoadUint32(&t.overflow) != 0
}
