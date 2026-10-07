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

package kvevents //nolint:testpackage // benchmarks use unexported processEventBatch and eventDedupFilter

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

const (
	benchBlockSizeTokens = 64
	benchModelName       = "bench-model"
	benchBatchRingSize   = 64
	benchBatchRingMask   = benchBatchRingSize - 1
)

func benchContext() context.Context {
	return log.IntoContext(context.Background(), logr.Discard())
}

func newBenchPool(b *testing.B, indexSize, numPods int) (*Pool, *kvblock.InMemoryIndex) {
	b.Helper()

	idx, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
		Size:         max(1<<20, indexSize),
		PodCacheSize: max(128, numPods*2),
	})
	if err != nil {
		b.Fatal(err)
	}

	tp, err := kvblock.NewChunkedTokenDatabase(&kvblock.TokenProcessorConfig{
		BlockSizeTokens: benchBlockSizeTokens,
		HashSeed:        "bench-seed",
		HashAlgorithm:   kvblock.HashAlgorithmXXH64,
	})
	if err != nil {
		b.Fatal(err)
	}

	cfg := DefaultConfig()
	pool := NewPool(cfg, idx, tp, nil)
	return pool, idx
}

func makePodList(numPods int) []string {
	pods := make([]string, numPods)
	for i := range pods {
		pods[i] = fmt.Sprintf("10.0.%d.%d:8000", i/256, i%256)
	}
	return pods
}

func reportBatchMetrics(b *testing.B, blockOpsPerIter int) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	batchesPerSec := float64(b.N) / elapsed
	blockOpsPerSec := float64(blockOpsPerIter) * batchesPerSec
	b.ReportMetric(blockOpsPerSec, "block_ops/s")
	b.ReportMetric(batchesPerSec, "batches/s")
}

// buildStoredBatchRing constructs a ring of benchBatchRingSize batches per pod.
// Engine block hashes are scoped per pod while token sequences are shared per ring slot,
// matching how engines emit pod-local block IDs for shared or distinct prompts.
func buildStoredBatchRing(numPods, blocksPerBatch int, hmaEnabled bool) [][]*EventBatch {
	podRings := make([][]*EventBatch, numPods)
	group0 := 0
	group1 := 1

	for p := range numPods {
		podOffset := uint64(p+1) << 40 // #nosec G115 -- bounded benchmark fixture
		ring := make([]*EventBatch, benchBatchRingSize)

		for bIdx := range ring {
			baseKey := podOffset | (uint64(bIdx+1) << 16)                           // #nosec G115 -- bounded benchmark fixture
			baseToken := uint32((bIdx + 1) * blocksPerBatch * benchBlockSizeTokens) // #nosec G115 -- bounded benchmark fixture

			if !hmaEnabled {
				hashes := make([]uint64, blocksPerBatch)
				for i := range hashes {
					hashes[i] = baseKey + uint64(i) // #nosec G115 -- bounded benchmark fixture
				}
				tokens := make([]uint32, blocksPerBatch*benchBlockSizeTokens)
				for i := range tokens {
					tokens[i] = baseToken + uint32(i) // #nosec G115 -- bounded benchmark fixture
				}
				ring[bIdx] = &EventBatch{
					Events: []GenericEvent{
						&BlockStoredEvent{
							BlockHashes: hashes,
							Tokens:      tokens,
							BlockSize:   benchBlockSizeTokens,
						},
					},
				}
				continue
			}

			// Split blocks across two attention groups to exercise GroupCatalog updates
			// and group-scoped PodEntry indexing while keeping total blocks per batch equal.
			halfBlocks := blocksPerBatch / 2
			hashes0 := make([]uint64, halfBlocks)
			hashes1 := make([]uint64, halfBlocks)
			for i := range halfBlocks {
				hashes0[i] = baseKey + uint64(i)            // #nosec G115 -- bounded benchmark fixture
				hashes1[i] = baseKey + uint64(halfBlocks+i) // #nosec G115 -- bounded benchmark fixture
			}
			tokens0 := make([]uint32, halfBlocks*benchBlockSizeTokens)
			tokens1 := make([]uint32, halfBlocks*benchBlockSizeTokens)
			for i := range tokens0 {
				tokens0[i] = baseToken + uint32(i)                                           // #nosec G115 -- bounded benchmark fixture
				tokens1[i] = baseToken + uint32(halfBlocks*benchBlockSizeTokens) + uint32(i) // #nosec G115 -- bounded benchmark fixture
			}

			ring[bIdx] = &EventBatch{
				Events: []GenericEvent{
					&BlockStoredEvent{
						BlockHashes:     hashes0,
						Tokens:          tokens0,
						BlockSize:       benchBlockSizeTokens,
						GroupIdx:        &group0,
						KVCacheSpecKind: KVCacheSpecKindFullAttention,
					},
					&BlockStoredEvent{
						BlockHashes:     hashes1,
						Tokens:          tokens1,
						BlockSize:       benchBlockSizeTokens,
						GroupIdx:        &group1,
						KVCacheSpecKind: KVCacheSpecKindMlaAttention,
					},
				},
			}
		}
		podRings[p] = ring
	}
	return podRings
}

// BenchmarkProcessEventBatchStored measures steady-state Pool.processEventBatch
// throughput for BlockStored events across sequential and parallel execution,
// batch sizes, pod counts, and HMA group modes.
func BenchmarkProcessEventBatchStored(b *testing.B) {
	modes := []string{"Sequential", "Parallel"}
	batchSizes := []int{16, 64}
	podCounts := []int{1, 16, 96}
	hmaModes := []struct {
		name    string
		enabled bool
	}{
		{name: "Disabled", enabled: false},
		{name: "Enabled", enabled: true},
	}

	for _, mode := range modes {
		for _, blocksPerBatch := range batchSizes {
			for _, numPods := range podCounts {
				for _, hma := range hmaModes {
					name := fmt.Sprintf("Mode=%s/BlocksPerBatch=%d/Pods=%d/HMA=%s",
						mode, blocksPerBatch, numPods, hma.name)
					b.Run(name, func(b *testing.B) {
						pool, _ := newBenchPool(b, 1<<20, numPods)
						pods := makePodList(numPods)
						podRings := buildStoredBatchRing(numPods, blocksPerBatch, hma.enabled)
						ctx := benchContext()

						// Warm the index, dedup filter, and GroupCatalog across all pods so every
						// coordinate measures steady-state event ingestion regardless of b.N.
						const warmSlots = 8
						for p := range numPods {
							for s := range warmSlots {
								pool.processEventBatch(ctx, podRings[p][s], pods[p], benchModelName)
							}
						}

						b.ReportAllocs()
						b.ResetTimer()

						if mode == "Sequential" {
							for i := 0; i < b.N; i++ {
								p := i % numPods
								slot := (warmSlots + i/numPods) & benchBatchRingMask
								pool.processEventBatch(ctx, podRings[p][slot], pods[p], benchModelName)
							}
						} else {
							var workerSeq atomic.Uint64
							b.RunParallel(func(pb *testing.PB) {
								step := int(workerSeq.Add(1)-1) * 9973
								for pb.Next() {
									p := step % numPods
									slot := (warmSlots + step/numPods) & benchBatchRingMask
									step++
									pool.processEventBatch(ctx, podRings[p][slot], pods[p], benchModelName)
								}
							})
						}

						b.StopTimer()
						reportBatchMetrics(b, blocksPerBatch)
					})
				}
			}
		}
	}
}

// BenchmarkProcessEventBatchRemoved measures Pool.processEventBatch throughput for
// BlockRemoved events. Each pod owns a disjoint set of pre-populated engine and
// request keys so every iteration executes the full dedup filter, engine-to-request
// key lookup, and index eviction path identically across all pod counts.
func BenchmarkProcessEventBatchRemoved(b *testing.B) {
	modes := []string{"Sequential", "Parallel"}
	batchSizes := []int{16, 64}
	podCounts := []int{1, 16, 96}

	for _, mode := range modes {
		for _, blocksPerBatch := range batchSizes {
			for _, numPods := range podCounts {
				name := fmt.Sprintf("Mode=%s/BlocksPerBatch=%d/Pods=%d",
					mode, blocksPerBatch, numPods)
				b.Run(name, func(b *testing.B) {
					totalBatches := b.N + 64
					batchesPerPod := (totalBatches / numPods) + 2
					keysPerPod := batchesPerPod * blocksPerBatch

					pool, idx := newBenchPool(b, numPods*keysPerPod*2, numPods)
					pods := makePodList(numPods)
					ctx := benchContext()

					removeBatches := make([]*EventBatch, totalBatches)
					for p, pod := range pods {
						podOffset := uint64(p+1) << 40 // #nosec G115 -- bounded benchmark fixture
						engKeys := make([]kvblock.BlockHash, keysPerPod)
						reqKeys := make([]kvblock.BlockHash, keysPerPod)
						rawHashes := make([]uint64, keysPerPod)
						for k := range keysPerPod {
							h := podOffset | uint64(k+1) // #nosec G115 -- bounded benchmark fixture
							rawHashes[k] = h
							engKeys[k] = kvblock.BlockHash(h)
							reqKeys[k] = kvblock.BlockHash(h | (uint64(1) << 32))
						}

						entry := []kvblock.PodEntry{{
							PodIdentifier: pod,
							DeviceTier:    defaultEventSourceDeviceTier,
						}}
						if err := idx.Add(ctx, engKeys, reqKeys, entry); err != nil {
							b.Fatal(err)
						}
						pool.dedup.trackStore(gpuScope(pod), rawHashes)

						for bIdx := range batchesPerPod {
							globalIdx := bIdx*numPods + p
							if globalIdx >= totalBatches {
								break
							}
							start := bIdx * blocksPerBatch
							removeBatches[globalIdx] = &EventBatch{
								Events: []GenericEvent{
									&BlockRemovedEvent{
										BlockHashes: rawHashes[start : start+blocksPerBatch],
									},
								},
							}
						}
					}

					b.ReportAllocs()
					b.ResetTimer()

					if mode == "Sequential" {
						for i := 0; i < b.N; i++ {
							pod := pods[i%numPods]
							pool.processEventBatch(ctx, removeBatches[i], pod, benchModelName)
						}
					} else {
						var seq atomic.Uint64
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								step := int(seq.Add(1) - 1)
								pod := pods[step%numPods]
								pool.processEventBatch(ctx, removeBatches[step], pod, benchModelName)
							}
						})
					}

					b.StopTimer()
					reportBatchMetrics(b, blocksPerBatch)
				})
			}
		}
	}
}

// BenchmarkEventDedupFilter isolates mutex and reference-count map contention in
// eventDedupFilter across sequential and parallel pod workers. Each iteration runs
// a matched trackStore + filterRemove pair (2 * blocksPerBatch block operations)
// on a worker-private hash ring.
func BenchmarkEventDedupFilter(b *testing.B) {
	modes := []string{"Sequential", "Parallel"}
	batchSizes := []int{16, 64}
	podCounts := []int{16, 96}

	for _, mode := range modes {
		for _, blocksPerBatch := range batchSizes {
			for _, numPods := range podCounts {
				name := fmt.Sprintf("Mode=%s/BlocksPerBatch=%d/Pods=%d",
					mode, blocksPerBatch, numPods)
				b.Run(name, func(b *testing.B) {
					filter := newEventDedupFilter()
					pods := makePodList(numPods)
					scopes := make([]blockScope, numPods)
					for i, pod := range pods {
						scopes[i] = gpuScope(pod)
						// Keep one resident block per pod so steady-state store+remove cycles
						// do not delete and reallocate the per-pod map on every iteration.
						filter.trackStore(scopes[i], []uint64{math.MaxUint64})
					}

					hashRing := make([][]uint64, benchBatchRingSize)
					for r := range hashRing {
						hashes := make([]uint64, blocksPerBatch)
						base := uint64(r+1) << 20
						for j := range hashes {
							hashes[j] = base + uint64(j) // #nosec G115 -- bounded benchmark fixture
						}
						hashRing[r] = hashes
					}

					b.ReportAllocs()
					b.ResetTimer()

					if mode == "Sequential" {
						for i := 0; i < b.N; i++ {
							scope := scopes[i%numPods]
							hashes := hashRing[(i/numPods)&benchBatchRingMask]
							filter.trackStore(scope, hashes)
							_ = filter.filterRemove(scope, hashes)
						}
					} else {
						var workerSeq atomic.Uint64
						b.RunParallel(func(pb *testing.PB) {
							wID := int(workerSeq.Add(1) - 1)
							step := wID * 9973
							for pb.Next() {
								scope := scopes[step%numPods]
								hashes := hashRing[(step/numPods)&benchBatchRingMask]
								step++
								filter.trackStore(scope, hashes)
								_ = filter.filterRemove(scope, hashes)
							}
						})
					}

					b.StopTimer()
					reportBatchMetrics(b, blocksPerBatch*2)
				})
			}
		}
	}
}
