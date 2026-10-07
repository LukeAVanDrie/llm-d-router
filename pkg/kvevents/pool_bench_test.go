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

	pool, err := NewPool(DefaultConfig(), idx, tp, nil)
	if err != nil {
		b.Fatal(err)
	}
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
	b.ReportMetric(float64(blockOpsPerIter)*batchesPerSec, "block_ops/s")
	b.ReportMetric(batchesPerSec, "batches/s")
}

func makeBlockSlice(base uint64, count int) []uint64 {
	hashes := make([]uint64, count)
	for i := range hashes {
		hashes[i] = base + uint64(i) //#nosec G115 -- bounded benchmark fixture
	}
	return hashes
}

func makeTokenSlice(base uint32, count int) []uint32 {
	tokens := make([]uint32, count)
	for i := range tokens {
		tokens[i] = base + uint32(i) //#nosec G115 -- bounded benchmark fixture
	}
	return tokens
}

// buildStoredBatchRing constructs a ring of benchBatchRingSize batches per pod.
func buildStoredBatchRing(numPods, blocksPerBatch int, hmaEnabled bool) [][]*EventBatch {
	podRings := make([][]*EventBatch, numPods)
	group0, group1 := 0, 1

	for p := range numPods {
		podOffset := uint64(p+1) << 40 //#nosec G115 -- bounded benchmark fixture
		ring := make([]*EventBatch, benchBatchRingSize)

		for bIdx := range ring {
			baseKey := podOffset | (uint64(bIdx+1) << 16)                           //#nosec G115 -- bounded benchmark fixture
			baseToken := uint32((bIdx + 1) * blocksPerBatch * benchBlockSizeTokens) //#nosec G115 -- bounded benchmark fixture

			if !hmaEnabled {
				ring[bIdx] = &EventBatch{
					Events: []GenericEvent{
						&BlockStoredEvent{
							BlockHashes: makeBlockSlice(baseKey, blocksPerBatch),
							Tokens:      makeTokenSlice(baseToken, blocksPerBatch*benchBlockSizeTokens),
							BlockSize:   benchBlockSizeTokens,
						},
					},
				}
				continue
			}

			half := blocksPerBatch / 2
			ring[bIdx] = &EventBatch{
				Events: []GenericEvent{
					&BlockStoredEvent{
						BlockHashes:     makeBlockSlice(baseKey, half),
						Tokens:          makeTokenSlice(baseToken, half*benchBlockSizeTokens),
						BlockSize:       benchBlockSizeTokens,
						GroupIdx:        &group0,
						KVCacheSpecKind: KVCacheSpecKindFullAttention,
					},
					&BlockStoredEvent{
						BlockHashes:     makeBlockSlice(baseKey+uint64(half), half),                                             //#nosec G115 -- bounded benchmark fixture
						Tokens:          makeTokenSlice(baseToken+uint32(half*benchBlockSizeTokens), half*benchBlockSizeTokens), //#nosec G115 -- bounded benchmark fixture
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

func runModeLoop(b *testing.B, mode string, stepFn func(step int)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	if mode == "Sequential" {
		for i := range b.N {
			stepFn(i)
		}
	} else {
		var workerSeq atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			step := int(workerSeq.Add(1)-1) * 9973
			for pb.Next() {
				stepFn(step)
				step++
			}
		})
	}
	b.StopTimer()
}

// BenchmarkProcessEventBatchStored measures steady-state Pool.processEventBatch
// throughput for BlockStored events across sequential and parallel execution,
// batch sizes, pod counts, and HMA group modes.
func BenchmarkProcessEventBatchStored(b *testing.B) {
	hmaModes := []struct {
		name    string
		enabled bool
	}{
		{name: "Disabled", enabled: false},
		{name: "Enabled", enabled: true},
	}

	for _, mode := range []string{"Sequential", "Parallel"} {
		for _, blocksPerBatch := range []int{16, 64} {
			for _, numPods := range []int{1, 16, 96} {
				for _, hma := range hmaModes {
					name := fmt.Sprintf("Mode=%s/BlocksPerBatch=%d/Pods=%d/HMA=%s",
						mode, blocksPerBatch, numPods, hma.name)
					b.Run(name, func(b *testing.B) {
						pool, _ := newBenchPool(b, 1<<20, numPods)
						pods := makePodList(numPods)
						podRings := buildStoredBatchRing(numPods, blocksPerBatch, hma.enabled)
						ctx := benchContext()

						const warmSlots = 8
						for p := range numPods {
							for s := range warmSlots {
								pool.processEventBatch(ctx, podRings[p][s], pods[p], benchModelName)
							}
						}

						runModeLoop(b, mode, func(step int) {
							p := step % numPods
							slot := (warmSlots + step/numPods) & benchBatchRingMask
							pool.processEventBatch(ctx, podRings[p][slot], pods[p], benchModelName)
						})
						reportBatchMetrics(b, blocksPerBatch)
					})
				}
			}
		}
	}
}

// BenchmarkProcessEventBatchRemoved measures Pool.processEventBatch throughput for
// BlockRemoved events.
func BenchmarkProcessEventBatchRemoved(b *testing.B) {
	for _, mode := range []string{"Sequential", "Parallel"} {
		for _, blocksPerBatch := range []int{16, 64} {
			for _, numPods := range []int{1, 16, 96} {
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
						podOffset := uint64(p+1) << 40 //#nosec G115 -- bounded benchmark fixture
						engKeys := make([]kvblock.BlockHash, keysPerPod)
						reqKeys := make([]kvblock.BlockHash, keysPerPod)
						rawHashes := make([]uint64, keysPerPod)
						for k := range keysPerPod {
							h := podOffset | uint64(k+1) //#nosec G115 -- bounded benchmark fixture
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
						for i := range b.N {
							pool.processEventBatch(ctx, removeBatches[i], pods[i%numPods], benchModelName)
						}
					} else {
						var seq atomic.Uint64
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								step := int(seq.Add(1) - 1)
								pool.processEventBatch(ctx, removeBatches[step], pods[step%numPods], benchModelName)
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
// eventDedupFilter across sequential and parallel pod workers.
func BenchmarkEventDedupFilter(b *testing.B) {
	for _, mode := range []string{"Sequential", "Parallel"} {
		for _, blocksPerBatch := range []int{16, 64} {
			for _, numPods := range []int{16, 96} {
				name := fmt.Sprintf("Mode=%s/BlocksPerBatch=%d/Pods=%d",
					mode, blocksPerBatch, numPods)
				b.Run(name, func(b *testing.B) {
					filter := newEventDedupFilter()
					pods := makePodList(numPods)
					scopes := make([]blockScope, numPods)
					for i, pod := range pods {
						scopes[i] = gpuScope(pod)
						filter.trackStore(scopes[i], []uint64{math.MaxUint64})
					}

					hashRing := make([][]uint64, benchBatchRingSize)
					for r := range hashRing {
						hashRing[r] = makeBlockSlice(uint64(r+1)<<20, blocksPerBatch)
					}

					runModeLoop(b, mode, func(step int) {
						scope := scopes[step%numPods]
						hashes := hashRing[(step/numPods)&benchBatchRingMask]
						filter.trackStore(scope, hashes)
						_ = filter.filterRemove(scope, hashes)
					})
					reportBatchMetrics(b, blocksPerBatch*2)
				})
			}
		}
	}
}
