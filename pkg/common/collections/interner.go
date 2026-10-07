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
	"sync"
	"sync/atomic"
)

// Interner assigns dense non-negative integer ordinals (0, 1, 2, ...) to
// comparable values, optionally bounded by a maximum cardinality, and recycles
// ordinals freed by Release.
//
// Reverse lookups via At are lock-free using an atomic copy-on-write slice
// snapshot.
type Interner[T comparable] struct {
	mu          sync.RWMutex
	ids         map[T]int
	free        []int
	values      []T
	snapshot    atomic.Pointer[[]T]
	activeWords atomic.Int32
	limit       int
}

// NewInterner creates an Interner with an optional maximum cardinality limit.
// When no limit is provided or limit[0] <= 0, cardinality is unbounded.
func NewInterner[T comparable](limit ...int) *Interner[T] {
	maxCard := 0
	if len(limit) > 0 && limit[0] > 0 {
		maxCard = limit[0]
	}
	in := &Interner[T]{
		ids:   make(map[T]int),
		limit: maxCard,
	}
	in.activeWords.Store(1)
	return in
}

// Lookup returns the ordinal currently assigned to val without modifying the
// interner.
func (in *Interner[T]) Lookup(val T) (int, bool) {
	in.mu.RLock()
	ord, ok := in.ids[val]
	in.mu.RUnlock()
	return ord, ok
}

// Intern returns the ordinal for val, assigning the next available ordinal (or
// a recycled one from Release) on first use. It returns (0, false) if assigning
// a new ordinal would exceed the configured limit.
func (in *Interner[T]) Intern(val T) (int, bool) {
	in.mu.RLock()
	if ord, ok := in.ids[val]; ok {
		in.mu.RUnlock()
		return ord, true
	}
	in.mu.RUnlock()

	in.mu.Lock()
	defer in.mu.Unlock()
	if ord, ok := in.ids[val]; ok {
		return ord, true
	}
	if in.limit > 0 && len(in.ids) >= in.limit {
		return 0, false
	}
	return in.assignLocked(val), true
}

// Release removes val from the interner and recycles its ordinal for future
// Intern calls. It returns the released ordinal and true if val was present.
func (in *Interner[T]) Release(val T) (int, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	ord, ok := in.ids[val]
	if !ok {
		return 0, false
	}
	delete(in.ids, val)
	in.free = append(in.free, ord)
	return ord, true
}

// At returns the value assigned to ord, or the zero value of T if ord is out of
// range. At is lock-free.
func (in *Interner[T]) At(ord int) T {
	if snap := in.snapshot.Load(); snap != nil && uint(ord) < uint(len(*snap)) {
		return (*snap)[ord]
	}
	var zero T
	return zero
}

// Len returns the number of currently active interned values.
func (in *Interner[T]) Len() int {
	in.mu.RLock()
	n := len(in.ids)
	in.mu.RUnlock()
	return n
}

// ActiveWords returns the number of 64-bit bitset words needed to cover all
// ordinals assigned by the interner (at least 1).
func (in *Interner[T]) ActiveWords() int {
	if w := int(in.activeWords.Load()); w > 0 {
		return w
	}
	return 1
}

// Fits reports whether interning all values in vals would stay within the
// interner's limit without mutating state.
func (in *Interner[T]) Fits(vals iter.Seq[T]) bool {
	in.mu.RLock()
	defer in.mu.RUnlock()
	return in.FitsLocked(vals)
}

// RLock acquires the interner's shared read lock for batched LookupLocked calls.
func (in *Interner[T]) RLock() { in.mu.RLock() }

// RUnlock releases the interner's shared read lock.
func (in *Interner[T]) RUnlock() { in.mu.RUnlock() }

// Lock acquires the interner's exclusive write lock for all-or-nothing batch
// validation (FitsLocked) and assignment (InternLocked).
func (in *Interner[T]) Lock() { in.mu.Lock() }

// Unlock releases the interner's exclusive write lock.
func (in *Interner[T]) Unlock() { in.mu.Unlock() }

// LookupLocked returns the ordinal assigned to val. The caller must hold RLock
// or Lock.
func (in *Interner[T]) LookupLocked(val T) (int, bool) {
	ord, ok := in.ids[val]
	return ord, ok
}

// FitsLocked reports whether interning all values in vals would stay within the
// interner's limit. The caller must hold RLock or Lock.
func (in *Interner[T]) FitsLocked(vals iter.Seq[T]) bool {
	if in.limit <= 0 {
		return true
	}
	pending := 0
	var seenBuf [8]T
	var seenMap map[T]struct{}
	fits := true
	vals(func(v T) bool {
		if _, ok := in.ids[v]; ok {
			return true
		}
		if seenMap != nil {
			if _, dup := seenMap[v]; dup {
				return true
			}
			seenMap[v] = struct{}{}
		} else {
			for i := range pending {
				if seenBuf[i] == v {
					return true
				}
			}
			if pending < len(seenBuf) {
				seenBuf[pending] = v
			} else {
				seenMap = make(map[T]struct{}, len(seenBuf)+1)
				for _, prev := range seenBuf {
					seenMap[prev] = struct{}{}
				}
				seenMap[v] = struct{}{}
			}
		}
		pending++
		if len(in.ids)+pending > in.limit {
			fits = false
			return false
		}
		return true
	})
	return fits
}

// InternLocked returns the ordinal for val, assigning a new or recycled ordinal
// on first use. The caller must hold Lock.
func (in *Interner[T]) InternLocked(val T) int {
	if ord, ok := in.ids[val]; ok {
		return ord
	}
	return in.assignLocked(val)
}

func (in *Interner[T]) assignLocked(val T) int {
	if in.ids == nil {
		in.ids = make(map[T]int)
	}
	var ord int
	if n := len(in.free); n > 0 {
		ord = in.free[n-1]
		in.free = in.free[:n-1]
		in.values[ord] = val
	} else {
		ord = len(in.values)
		in.values = append(in.values, val)
	}
	in.ids[val] = ord
	if w := int32(ord>>6) + 1; w > in.activeWords.Load() {
		in.activeWords.Store(w)
	}
	snap := make([]T, len(in.values))
	copy(snap, in.values)
	in.snapshot.Store(&snap)
	return ord
}
