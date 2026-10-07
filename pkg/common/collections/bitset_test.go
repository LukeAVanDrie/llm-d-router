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

package collections_test

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
)

func TestBitsetInlineAndOverflow(t *testing.T) {
	var b collections.Bitset
	assert.True(t, b.IsEmpty())
	assert.True(t, b.IsInline())
	assert.Equal(t, 0, b.Len())

	b.Set(-1)
	assert.False(t, b.Has(-1))
	assert.True(t, b.IsEmpty())

	ords := []int{0, 1, 63, 64, 127, 128, 255, 256, 511, 512, 1023}
	for _, ord := range ords {
		b.Set(ord)
		assert.True(t, b.Has(ord), "expected ordinal %d to be set", ord)
	}
	assert.False(t, b.IsEmpty())
	assert.False(t, b.IsInline())
	assert.Equal(t, len(ords), b.Len())

	got := make([]int, 0, len(ords))
	for ord := range b.All() {
		got = append(got, ord)
	}
	assert.Equal(t, ords, got)

	// Early break in iterator.
	var firstThree []int
	for ord := range b.All() {
		firstThree = append(firstThree, ord)
		if len(firstThree) == 3 {
			break
		}
	}
	assert.Equal(t, ords[:3], firstThree)

	// Clone independence across inline and overflow words.
	cloned := b.Clone()
	assert.True(t, b.Equals(&cloned))
	cloned.Clear(0)
	cloned.Clear(512)
	cloned.Set(700)
	assert.True(t, b.Has(0))
	assert.True(t, b.Has(512))
	assert.False(t, b.Has(700))
	assert.False(t, b.Equals(&cloned))
}

func TestBitsetSetAlgebra(t *testing.T) {
	var a, b collections.Bitset
	for _, ord := range []int{2, 65, 200, 300, 600} {
		a.Set(ord)
	}
	for _, ord := range []int{65, 200, 400, 600} {
		b.Set(ord)
	}

	assert.True(t, a.Intersects(&b))
	assert.False(t, a.ContainsAll(&b))

	diff := a.AndNot(&b)
	assert.Equal(t, []int{2, 300}, slices.Collect(diff.All()))

	union := a.Clone()
	union.Or(&b)
	assert.True(t, union.ContainsAll(&a))
	assert.True(t, union.ContainsAll(&b))
	assert.Equal(t, []int{2, 65, 200, 300, 400, 600}, slices.Collect(union.All()))

	inter := a.Clone()
	inter.And(&b)
	assert.Equal(t, []int{65, 200, 600}, slices.Collect(inter.All()))

	cleared := union.Clone()
	cleared.ClearFrom(&b)
	assert.True(t, cleared.Equals(&diff))
}

func TestAtomicBitsetConcurrent(t *testing.T) {
	const numBits = 1024
	var ab collections.AtomicBitset
	assert.True(t, ab.IsEmpty())

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for ord := id; ord < numBits; ord += 16 {
				ab.Set(ord)
			}
		}(g)
	}
	wg.Wait()

	assert.False(t, ab.IsEmpty())
	snap := ab.Snapshot()
	require.Equal(t, numBits, snap.Len())

	for g := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for ord := id * 2; ord < numBits; ord += 32 {
				ab.Clear(ord)
			}
		}(g)
	}
	wg.Wait()

	snapAfter := ab.Snapshot()
	assert.Equal(t, numBits/2, snapAfter.Len())
	for ord := range numBits {
		assert.Equal(t, ord%2 == 1, snapAfter.Has(ord), "ord %d", ord)
	}
}
