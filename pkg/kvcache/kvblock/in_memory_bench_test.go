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

package kvblock_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func benchContext() context.Context {
	return log.IntoContext(context.Background(), logr.Discard())
}

func podIdentifier(podIdx int) string {
	return fmt.Sprintf("10.0.%d.%d:8000", podIdx/256, podIdx%256)
}

func reportBlockThroughput(b *testing.B, blocksPerOp int) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	opsPerSec := float64(b.N) / elapsed
	b.ReportMetric(float64(blocksPerOp)*opsPerSec, "block_ops/s")
	b.ReportMetric(opsPerSec, "queries/s")
}

// populateIndex fills an in-memory index with numKeys request keys, each held
// by numPods pods on the gpu tier. Returns the index and the ordered keys.
func populateIndex(b *testing.B, numKeys, numPods int) (*kvblock.InMemoryIndex, []kvblock.BlockHash) {
	b.Helper()
	idx, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         1 << 20,
		PodCacheSize: 128,
	})
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]kvblock.BlockHash, numKeys)
	for i := range keys {
		keys[i] = kvblock.BlockHash(uint64(i) + 1)
	}
	entries := make([]kvblock.PodEntry, numPods)
	for p := range entries {
		entries[p] = kvblock.PodEntry{
			PodIdentifier: podIdentifier(p),
			DeviceTier:    "gpu",
		}
	}
	if err := idx.Add(context.Background(), nil, keys, entries); err != nil {
		b.Fatal(err)
	}
	return idx, keys
}

// BenchmarkInMemoryIndexLookup measures a full-hit lookup of the block keys a
// 240K-token prompt produces at block size 64 (3750 keys), across fleet sizes.
func BenchmarkInMemoryIndexLookup(b *testing.B) {
	const numKeys = 3750
	for _, numPods := range []int{8, 96} {
		for _, filtered := range []bool{false, true} {
			name := fmt.Sprintf("keys=3750/pods=%d/filtered=%v", numPods, filtered)
			b.Run(name, func(b *testing.B) {
				idx, keys := populateIndex(b, numKeys, numPods)
				podSet := sets.New[string]()
				if filtered {
					for p := 0; p < numPods/2; p++ {
						podSet.Insert(podIdentifier(p))
					}
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res, err := idx.Lookup(ctx, keys, podSet)
					if err != nil {
						b.Fatal(err)
					}
					if len(res) != numKeys {
						b.Fatalf("unexpected result size %d", len(res))
					}
				}
			})
		}
	}
}

// BenchmarkInMemoryIndexAdd measures ingesting one BlockStored event's worth
// of keys (64 blocks) for a single pod entry, over a warm index.
func BenchmarkInMemoryIndexAdd(b *testing.B) {
	idx, _ := populateIndex(b, 1<<16, 4)
	entries := []kvblock.PodEntry{{PodIdentifier: "10.0.0.1:8000", DeviceTier: "gpu"}}
	engineKeys := make([]kvblock.BlockHash, 64)
	requestKeys := make([]kvblock.BlockHash, 64)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i)*64 + 1
		for j := range engineKeys {
			engineKeys[j] = kvblock.BlockHash(base + uint64(j))
			requestKeys[j] = kvblock.BlockHash(base + uint64(j))
		}
		if err := idx.Add(ctx, engineKeys, requestKeys, entries); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInMemoryIndexMixed measures lookup latency while concurrent
// writers ingest events, approximating the 96-rank firehose against
// per-request scoring reads.
func BenchmarkInMemoryIndexMixed(b *testing.B) {
	const numKeys = 3750
	idx, keys := populateIndex(b, numKeys, 16)
	ctx := context.Background()

	stop := make(chan struct{})
	defer close(stop)
	for w := 0; w < 4; w++ {
		go func(w int) {
			entries := []kvblock.PodEntry{{PodIdentifier: fmt.Sprintf("10.1.0.%d:8000", w), DeviceTier: "gpu"}}
			engineKeys := make([]kvblock.BlockHash, 64)
			requestKeys := make([]kvblock.BlockHash, 64)
			for i := uint64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				base := (uint64(w)<<32 | i*64) + 1 //#nosec G115 -- test data, w is a small worker index
				for j := range engineKeys {
					engineKeys[j] = kvblock.BlockHash(base + uint64(j))
					requestKeys[j] = kvblock.BlockHash(base + uint64(j))
				}
				_ = idx.Add(ctx, engineKeys, requestKeys, entries)
			}
		}(w)
	}

	podSet := sets.New[string]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Lookup(ctx, keys, podSet); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInMemoryIndexWalkKeys measures the ordered walk over the same
// workload as BenchmarkInMemoryIndexLookup, touching every entry.
func BenchmarkInMemoryIndexWalkKeys(b *testing.B) {
	const numKeys = 3750
	for _, numPods := range []int{8, 96} {
		b.Run(fmt.Sprintf("keys=3750/pods=%d", numPods), func(b *testing.B) {
			idx, keys := populateIndex(b, numKeys, numPods)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seen := 0
				err := idx.WalkKeys(ctx, keys, func(_ int, _ bool, entries []kvblock.EntryRef) bool {
					seen += len(entries)
					return true
				})
				if err != nil {
					b.Fatal(err)
				}
				if seen != numKeys*numPods {
					b.Fatalf("visited %d entries, want %d", seen, numKeys*numPods)
				}
			}
		})
	}
}

type writeBatch struct {
	engineKeys  []kvblock.BlockHash
	requestKeys []kvblock.BlockHash
	entries     []kvblock.PodEntry
}

func buildWriteBatches(numBatches, batchBlocks, numPods int, baseKey uint64) []writeBatch {
	batches := make([]writeBatch, numBatches)
	for bIdx := range batches {
		eng := make([]kvblock.BlockHash, batchBlocks)
		req := make([]kvblock.BlockHash, batchBlocks)
		offset := baseKey + uint64(bIdx*batchBlocks) //#nosec G115 -- bounded benchmark fixture
		for j := range batchBlocks {
			h := kvblock.BlockHash(offset + uint64(j) + 1) //#nosec G115 -- bounded benchmark fixture
			eng[j] = h
			req[j] = h | kvblock.BlockHash(uint64(1)<<62)
		}
		podID := podIdentifier(bIdx % numPods)
		batches[bIdx] = writeBatch{
			engineKeys:  eng,
			requestKeys: req,
			entries: []kvblock.PodEntry{{
				PodIdentifier: podID,
				DeviceTier:    "gpu",
			}},
		}
	}
	return batches
}

func newWriteBenchIndex(b *testing.B, backend string, numPods int) kvblock.Index {
	b.Helper()
	var (
		inner kvblock.Index
		err   error
	)
	switch backend {
	case "InMemory":
		inner, err = kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
			Size:         1 << 20,
			PodCacheSize: max(128, numPods*2),
		})
	case "CostAware":
		inner, err = kvblock.NewCostAwareMemoryIndex(&kvblock.CostAwareMemoryIndexConfig{
			Size:        "2GiB",
			NumCounters: 1 << 20,
		})
	default:
		b.Fatalf("unknown backend %q", backend)
	}
	if err != nil {
		b.Fatal(err)
	}
	return kvblock.NewTracedIndex(kvblock.NewInstrumentedIndex(inner))
}

// BenchmarkWriteScaling evaluates steady-state kvblock.Index.Add and combined
// Add+Evict lifecycle throughput across Mode, Backend, BatchBlocks, and Pods.
func BenchmarkWriteScaling(b *testing.B) {
	for _, op := range []string{"Add", "AddAndEvict"} {
		for _, mode := range []string{"Sequential", "Parallel"} {
			for _, backend := range []string{"InMemory", "CostAware"} {
				for _, batchBlocks := range []int{16, 64} {
					for _, numPods := range []int{16, 96} {
						subName := fmt.Sprintf("Op=%s/Mode=%s/Backend=%s/BatchBlocks=%d/Pods=%d",
							op, mode, backend, batchBlocks, numPods)
						b.Run(subName, func(b *testing.B) {
							runWriteCoordinate(b, op, mode, backend, batchBlocks, numPods)
						})
					}
				}
			}
		}
	}
}

func runWriteCoordinate(b *testing.B, op, mode, backend string, batchBlocks, numPods int) {
	ctx := benchContext()
	idx := newWriteBenchIndex(b, backend, numPods)

	const batchesPerWorker = 16
	numWorkerSlots := max(runtime.GOMAXPROCS(0)*4, 16)
	totalBatches := numWorkerSlots * batchesPerWorker
	batches := buildWriteBatches(totalBatches, batchBlocks, numPods, 1<<32)

	if op == "Add" {
		for i := range batches {
			wb := &batches[i]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	if mode == "Sequential" {
		for i := range b.N {
			wb := &batches[i%totalBatches]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
			if op == "AddAndEvict" {
				for _, k := range wb.engineKeys {
					if err := idx.Evict(ctx, k, kvblock.EngineKey, wb.entries); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
	} else {
		var workerSeq atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			slot := int(workerSeq.Add(1)-1) % numWorkerSlots
			workerBatches := batches[slot*batchesPerWorker : (slot+1)*batchesPerWorker]
			step := 0
			for pb.Next() {
				wb := &workerBatches[step&(batchesPerWorker-1)]
				step++
				if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
					b.Error(err)
					return
				}
				if op == "AddAndEvict" {
					for _, k := range wb.engineKeys {
						if err := idx.Evict(ctx, k, kvblock.EngineKey, wb.entries); err != nil {
							b.Error(err)
							return
						}
					}
				}
			}
		})
	}

	b.StopTimer()
	blocksPerOp := batchBlocks
	if op == "AddAndEvict" {
		blocksPerOp = batchBlocks * 2
	}
	reportBlockThroughput(b, blocksPerOp)
}

// BenchmarkEvictionDynamics evaluates steady-state LRU eviction churn when unique block
// keys (4x index capacity) cycle through InMemoryIndex (global Size overflow and per-key
// PodCacheSize overflow) and CostAwareMemoryIndex.
func BenchmarkEvictionDynamics(b *testing.B) {
	for _, mode := range []string{"Sequential", "Parallel"} {
		b.Run(fmt.Sprintf("Backend=InMemory_GlobalKeyOverflow/Mode=%s", mode), func(b *testing.B) {
			runGlobalKeyEvictionChurn(b, mode)
		})
		b.Run(fmt.Sprintf("Backend=InMemory_PodCacheOverflow/Mode=%s", mode), func(b *testing.B) {
			runPodCacheEvictionChurn(b, mode)
		})
		b.Run(fmt.Sprintf("Backend=CostAware_ByteCostOverflow/Mode=%s", mode), func(b *testing.B) {
			runCostAwareEvictionChurn(b, mode)
		})
	}
}

func runGlobalKeyEvictionChurn(b *testing.B, mode string) {
	ctx := benchContext()
	const (
		indexCapacity = 4096
		keySpace      = indexCapacity * 4
		batchBlocks   = 64
		numBatches    = keySpace / batchBlocks
		numPods       = 16
	)

	inner, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         indexCapacity,
		PodCacheSize: 32,
	})
	if err != nil {
		b.Fatal(err)
	}
	idx := kvblock.NewTracedIndex(kvblock.NewInstrumentedIndex(inner))

	batches := buildWriteBatches(numBatches, batchBlocks, numPods, 1<<32)
	warmBatches := indexCapacity / batchBlocks
	for i := range warmBatches {
		wb := &batches[i]
		if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	runBatchChurnLoop(b, ctx, idx, batches, warmBatches, mode)
	b.StopTimer()
	reportBlockThroughput(b, batchBlocks)
}

func runPodCacheEvictionChurn(b *testing.B, mode string) {
	ctx := benchContext()
	const (
		podCacheCapacity = 16
		totalPods        = podCacheCapacity * 4
		batchBlocks      = 64
	)

	inner, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         1 << 16,
		PodCacheSize: podCacheCapacity,
	})
	if err != nil {
		b.Fatal(err)
	}
	idx := kvblock.NewTracedIndex(kvblock.NewInstrumentedIndex(inner))

	sharedEngineKeys := make([]kvblock.BlockHash, batchBlocks)
	sharedRequestKeys := make([]kvblock.BlockHash, batchBlocks)
	for i := range batchBlocks {
		h := kvblock.BlockHash(uint64(i) + 1)
		sharedEngineKeys[i] = h
		sharedRequestKeys[i] = h
	}

	batches := make([]writeBatch, totalPods)
	for p := range totalPods {
		batches[p] = writeBatch{
			engineKeys:  sharedEngineKeys,
			requestKeys: sharedRequestKeys,
			entries: []kvblock.PodEntry{{
				PodIdentifier: podIdentifier(p),
				DeviceTier:    "gpu",
			}},
		}
	}

	for p := range podCacheCapacity {
		wb := &batches[p]
		if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	runBatchChurnLoop(b, ctx, idx, batches, podCacheCapacity, mode)
	b.StopTimer()
	reportBlockThroughput(b, batchBlocks)
}

func runCostAwareEvictionChurn(b *testing.B, mode string) {
	ctx := benchContext()
	const (
		approxKeyCapacity = 2048
		keySpace          = approxKeyCapacity * 4
		batchBlocks       = 64
		numBatches        = keySpace / batchBlocks
		numPods           = 16
	)

	inner, err := kvblock.NewCostAwareMemoryIndex(&kvblock.CostAwareMemoryIndexConfig{
		Size:        "256KiB",
		NumCounters: keySpace * 10,
	})
	if err != nil {
		b.Fatal(err)
	}
	idx := kvblock.NewTracedIndex(kvblock.NewInstrumentedIndex(inner))

	batches := buildWriteBatches(numBatches, batchBlocks, numPods, 1<<32)
	warmBatches := approxKeyCapacity / batchBlocks
	for i := range warmBatches {
		wb := &batches[i]
		if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	runBatchChurnLoop(b, ctx, idx, batches, warmBatches, mode)
	b.StopTimer()
	reportBlockThroughput(b, batchBlocks)
}

func runBatchChurnLoop(b *testing.B, ctx context.Context, idx kvblock.Index, batches []writeBatch, startIdx int, mode string) {
	numBatches := len(batches)
	if mode == "Sequential" {
		for i := range b.N {
			wb := &batches[(startIdx+i)%numBatches]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
		}
		return
	}
	var workerSeq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		step := int(workerSeq.Add(1)-1) * 9973
		for pb.Next() {
			wb := &batches[(startIdx+step)%numBatches]
			step++
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
