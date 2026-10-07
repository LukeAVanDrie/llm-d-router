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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
)

func TestFastTableProbeInsertDelete(t *testing.T) {
	type item struct {
		key uint64
		val int
	}

	disabled := collections.NewFastTable[item](0)
	disabled.Insert(1, &item{key: 1, val: 10})
	v, auth := disabled.Probe(1, func(it *item) bool { return it.key == 1 })
	assert.Nil(t, v)
	assert.False(t, auth)

	ft := collections.NewFastTable[item](16)
	it1 := &item{key: 42, val: 100}
	ft.Insert(42, it1)

	got, auth := ft.Probe(42, func(it *item) bool { return it.key == 42 })
	assert.True(t, auth)
	assert.Same(t, it1, got)

	miss, auth := ft.Probe(99, func(it *item) bool { return it.key == 99 })
	assert.True(t, auth, "miss before overflow should be authoritative")
	assert.Nil(t, miss)

	// Verify re-inserting an entry at a non-zero probe slot after slot 0 is
	// deleted does not duplicate or leave a dangling entry on subsequent Delete.
	it0 := &item{key: 10, val: 1}
	itCol := &item{key: 26, val: 2} // 10 & 15 == 26 & 15 == 10
	ftDup := collections.NewFastTable[item](16)
	ftDup.Insert(10, it0)
	ftDup.Insert(10, itCol) // placed at slot 11
	ftDup.Delete(10, it0)   // clears slot 10 to nil
	gotAfterHole, authHole := ftDup.Probe(10, func(it *item) bool { return it.key == 26 })
	assert.True(t, authHole)
	assert.Same(t, itCol, gotAfterHole)
	ftDup.Insert(10, itCol) // must recognize itCol at slot 11 and not duplicate at slot 10
	ftDup.Delete(10, itCol)
	gotDup, _ := ftDup.Probe(10, func(it *item) bool { return it.key == 26 })
	assert.Nil(t, gotDup)

	// Overflow a single bucket window (> 8 colliding entries at hash 7).
	for i := range collections.FastTableProbes + 2 {
		ft.Insert(7, &item{key: uint64(1000 + i), val: i})
	}
	assert.True(t, ft.HasOverflow())

	missAfter, auth := ft.Probe(99, func(it *item) bool { return it.key == 99 })
	assert.False(t, auth, "miss after overflow must not be authoritative")
	assert.Nil(t, missAfter)

	ft2 := collections.NewFastTable[item](16)
	ft2.Insert(42, it1)
	ft2.Delete(42, it1)
	assert.True(t, ft2.HasOverflow())
	gotAfterDel, auth := ft2.Probe(42, func(it *item) bool { return it.key == 42 })
	assert.False(t, auth)
	assert.Nil(t, gotAfterDel)
}
