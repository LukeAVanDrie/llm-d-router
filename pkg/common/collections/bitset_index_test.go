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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHashUint64(k uint64) uint64 {
	return k ^ (k >> 20) ^ (k >> 40)
}

func TestShardedBitsetIndexBasicAndCompaction(t *testing.T) {
	idx := NewShardedBitsetIndex[uint64](64, 2, testHashUint64)

	assert.Nil(t, idx.Lookup(42))

	e1 := idx.SetBit(42, 3)
	require.NotNil(t, e1)
	assert.Equal(t, uint64(42), e1.Key())
	assert.False(t, e1.Evicted())
	snap1 := e1.Pods().Snapshot()
	assert.True(t, snap1.Has(3))

	e2 := idx.SetBit(42, 300)
	assert.Same(t, e1, e2)
	snap2 := idx.Lookup(42).Snapshot()
	assert.True(t, snap2.Has(3))
	assert.True(t, snap2.Has(300))

	e1.Clear(3)
	e1.Clear(300)
	snapEmpty := idx.Lookup(42).Snapshot()
	assert.True(t, snapEmpty.IsEmpty())

	// Insert enough keys into the same shard as 42 to trigger lazy compaction
	// (minCompact = 2).
	targetShard := idx.shard(testHashUint64(42))
	added := 0
	for k := uint64(100); added < 4; k++ {
		if idx.shard(testHashUint64(k)) == targetShard {
			idx.SetBit(k, 1)
			added++
		}
	}
	assert.True(t, e1.Evicted(), "empty entry should be marked evicted after shard compaction")

	// Re-inserting key 42 after compaction allocates a fresh BitsetEntry.
	e3 := idx.SetBit(42, 7)
	require.NotNil(t, e3)
	assert.NotSame(t, e1, e3)
	assert.False(t, e3.Evicted())
	snap3 := e3.Snapshot()
	assert.True(t, snap3.Has(7))

	visited := make(map[uint64]int)
	idx.ForEach(func(k uint64, bs Bitset) {
		visited[k] = bs.Len()
	})
	assert.Equal(t, 1, visited[42])
	assert.Len(t, visited, 5)
}

func TestShardedBitsetIndexConcurrentSetAndClear(t *testing.T) {
	idx := NewShardedBitsetIndex[uint64](128, 4, testHashUint64)
	const (
		numWorkers = 8
		numKeys    = 64
	)

	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func(ord int) {
			defer wg.Done()
			for k := range uint64(numKeys) {
				e := idx.SetBit(k, ord)
				bs := e.Snapshot()
				assert.True(t, bs.Has(ord))
				if k%2 == 0 {
					e.Clear(ord)
				}
			}
		}(w)
	}
	wg.Wait()

	nonEmpty := 0
	idx.ForEach(func(k uint64, bs Bitset) {
		if k%2 == 1 {
			nonEmpty++
			assert.Equal(t, numWorkers, bs.Len())
		}
	})
	assert.Equal(t, numKeys/2, nonEmpty)
}
