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
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
)

func TestInternerAssignAndRecycle(t *testing.T) {
	in := collections.NewInterner[string]()
	assert.Equal(t, 0, in.Len())
	assert.Equal(t, 1, in.ActiveWords())
	assert.Equal(t, "", in.At(0))
	assert.Equal(t, "", in.At(-1))

	ordA, ok := in.Intern("pod-a")
	require.True(t, ok)
	assert.Equal(t, 0, ordA)

	ordB, ok := in.Intern("pod-b")
	require.True(t, ok)
	assert.Equal(t, 1, ordB)

	ordA2, ok := in.Lookup("pod-a")
	require.True(t, ok)
	assert.Equal(t, ordA, ordA2)
	assert.Equal(t, "pod-a", in.At(ordA))
	assert.Equal(t, "pod-b", in.At(ordB))
	assert.Equal(t, 2, in.Len())

	relOrd, released := in.Release("pod-a")
	require.True(t, released)
	assert.Equal(t, ordA, relOrd)
	assert.Equal(t, 1, in.Len())
	_, found := in.Lookup("pod-a")
	assert.False(t, found)

	ordC, ok := in.Intern("pod-c")
	require.True(t, ok)
	assert.Equal(t, ordA, ordC, "recycled ordinal should be reused")
	assert.Equal(t, "pod-c", in.At(ordC))
}

func TestInternerCardinalityLimitAndFits(t *testing.T) {
	in := collections.NewInterner[string](3)
	for i := range 3 {
		_, ok := in.Intern(fmt.Sprintf("k-%d", i))
		require.True(t, ok)
	}
	assert.Equal(t, 3, in.Len())

	_, ok := in.Intern("k-overflow")
	assert.False(t, ok)
	assert.Equal(t, 3, in.Len())

	assert.True(t, in.Fits(slices.Values([]string{"k-0", "k-1"})))
	assert.False(t, in.Fits(slices.Values([]string{"k-0", "k-new"})))

	_, released := in.Release("k-1")
	require.True(t, released)
	assert.True(t, in.Fits(slices.Values([]string{"k-0", "k-new", "k-new"})))
	assert.False(t, in.Fits(slices.Values([]string{"k-new-1", "k-new-2"})))
}

func TestInternerConcurrent(t *testing.T) {
	in := collections.NewInterner[int]()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 200 {
				key := id*1000 + (i % 50)
				ord, ok := in.Intern(key)
				if assert.True(t, ok) {
					_ = in.At(ord)
				}
				if i%7 == 0 {
					in.Release(key)
				}
			}
		}(g)
	}
	wg.Wait()
	assert.GreaterOrEqual(t, in.ActiveWords(), 1)
}
