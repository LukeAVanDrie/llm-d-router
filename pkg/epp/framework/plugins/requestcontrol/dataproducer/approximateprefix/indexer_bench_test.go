/*
Copyright 2026 The Kubernetes Authors.

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

package approximateprefix

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/prefixhash"
)

const (
	modeSequential = "Sequential"
	modeParallel   = "Parallel"
)

// newBenchIndexerContext returns a cancellable context with a discarding logger
// and registers cleanup so the indexer's background reporting goroutine exits
// when the benchmark completes.
func newBenchIndexerContext(b *testing.B) context.Context {
	b.Helper()
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), logr.Discard()))
	b.Cleanup(cancel)
	return ctx
}

// reportThroughput records block_ops/s and reqs/s for a benchmark where each
// operation processes blocksPerOp prefix blocks.
func reportThroughput(b *testing.B, blocksPerOp int) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	reqsPerSec := float64(b.N) / elapsed
	blockOpsPerSec := float64(blocksPerOp) * reqsPerSec
	b.ReportMetric(blockOpsPerSec, "block_ops/s")
	b.ReportMetric(reqsPerSec, "reqs/s")
}

func makeBenchServers(numPods, lruCapacity int) []server {
	pods := make([]server, numPods)
	for p := range numPods {
		pods[p] = server{
			ServerID: ServerID{
				Namespace: "default",
				Name:      "pod-" + strconv.Itoa(p),
			},
			NumOfGPUBlocks: lruCapacity,
		}
	}
	return pods
}

func makeBenchHashBatches(numBatches, numBlocks int, baseOffset uint64) [][]blockHash {
	batches := make([][]blockHash, numBatches)
	for batchIdx := range numBatches {
		batch := make([]blockHash, numBlocks)
		start := baseOffset + uint64(batchIdx*numBlocks) + 1
		for j := range numBlocks {
			batch[j] = blockHash(start + uint64(j))
		}
		batches[batchIdx] = batch
	}
	return batches
}

type approxMatchTopology struct {
	name            string
	expectedMatches int
	queries         [][]blockHash
}

func setupApproxMatchTopology(idx *indexer, topoName string, numBlocks, numPods int) approxMatchTopology {
	pods := makeBenchServers(numPods, max(defaultLRUCapacityPerServer, numBlocks*2))
	switch topoName {
	case "Hotspot":
		hashes := makeBenchHashBatches(1, numBlocks, 0)[0]
		for _, srv := range pods {
			idx.Add(hashes, srv)
		}
		return approxMatchTopology{
			name:            topoName,
			expectedMatches: numPods,
			queries:         [][]blockHash{hashes},
		}
	case "SystemPromptBranch":
		sysLen := max(1, numBlocks/4)
		branchLen := numBlocks - sysLen
		numBranches := min(8, numPods)
		sysHashes := makeBenchHashBatches(1, sysLen, 0)[0]
		branchBatches := makeBenchHashBatches(numBranches, branchLen, uint64(sysLen+1000))
		queries := make([][]blockHash, numBranches)
		for bIdx := range numBranches {
			full := make([]blockHash, numBlocks)
			copy(full[:sysLen], sysHashes)
			copy(full[sysLen:], branchBatches[bIdx])
			queries[bIdx] = full
		}
		for pIdx, srv := range pods {
			idx.Add(queries[pIdx%numBranches], srv)
		}
		return approxMatchTopology{
			name:            topoName,
			expectedMatches: numPods,
			queries:         queries,
		}
	case "Disjoint":
		queries := makeBenchHashBatches(numPods, numBlocks, 0)
		for pIdx, srv := range pods {
			idx.Add(queries[pIdx], srv)
		}
		return approxMatchTopology{
			name:            topoName,
			expectedMatches: 1,
			queries:         queries,
		}
	default:
		panic("unknown topology: " + topoName)
	}
}

// BenchmarkApproxIndexerMatch measures dataProducer.matchLongestPrefix across
// prefix topologies (Hotspot, SystemPromptBranch, Disjoint), sequential and
// parallel execution modes, block lengths, and fleet sizes.
func BenchmarkApproxIndexerMatch(b *testing.B) {
	topologies := []string{"Hotspot", "SystemPromptBranch", "Disjoint"}
	modes := []string{modeSequential, modeParallel}
	blockCounts := []int{64, 512, 3750}
	podCounts := []int{16, 96}

	for _, topoName := range topologies {
		for _, mode := range modes {
			for _, numBlocks := range blockCounts {
				for _, numPods := range podCounts {
					name := fmt.Sprintf("Topology=%s/Mode=%s/Blocks=%d/Pods=%d", topoName, mode, numBlocks, numPods)
					b.Run(name, func(b *testing.B) {
						ctx := newBenchIndexerContext(b)
						lruCap := max(defaultLRUCapacityPerServer, numBlocks*2)
						idx := newIndexer(ctx, lruCap, "bench", ApproxPrefixCachePluginType).(*indexer)
						dp := &dataProducer{indexerInst: idx}
						topo := setupApproxMatchTopology(idx, topoName, numBlocks, numPods)
						numQueries := len(topo.queries)

						b.ReportAllocs()
						b.ResetTimer()
						if mode == modeSequential {
							for i := 0; i < b.N; i++ {
								matches := dp.matchLongestPrefix(ctx, topo.queries[i%numQueries])
								if len(matches) != topo.expectedMatches {
									b.Fatalf("matched %d pods, want %d", len(matches), topo.expectedMatches)
								}
							}
						} else {
							var workerSeq atomic.Uint64
							b.RunParallel(func(pb *testing.PB) {
								step := int(workerSeq.Add(1) - 1)
								for pb.Next() {
									matches := dp.matchLongestPrefix(ctx, topo.queries[step%numQueries])
									if len(matches) != topo.expectedMatches {
										b.Errorf("matched %d pods, want %d", len(matches), topo.expectedMatches)
										return
									}
									step++
								}
							})
						}
						b.StopTimer()
						reportThroughput(b, numBlocks)
					})
				}
			}
		}
	}
}

// BenchmarkApproxIndexerAdd measures indexer.Add across sequential and parallel
// modes, block batch sizes, and pod counts, both within per-server LRU capacity
// and under LRU eviction churn where key cardinality exceeds LRUCapacityPerServer.
func BenchmarkApproxIndexerAdd(b *testing.B) {
	const (
		numBatches = 16 // power of two for bitmask indexing; > 2 (LRU cap) + GOMAXPROCS so concurrent workers never collide on an active batch
		batchMask  = numBatches - 1
	)

	type capacityCase struct {
		name           string
		batchesPerPod  int
		warmBatchesCap int
	}
	cases := []capacityCase{
		{
			name:           "WithinCapacity",
			batchesPerPod:  numBatches * 2,
			warmBatchesCap: numBatches,
		},
		{
			name:           "EvictionChurn",
			batchesPerPod:  2,
			warmBatchesCap: 2,
		},
	}

	modes := []string{modeSequential, modeParallel}
	blockCounts := []int{64, 512}
	podCounts := []int{16, 96}

	for _, tc := range cases {
		for _, mode := range modes {
			for _, numBlocks := range blockCounts {
				for _, numPods := range podCounts {
					name := fmt.Sprintf("Capacity=%s/Mode=%s/Blocks=%d/Pods=%d", tc.name, mode, numBlocks, numPods)
					b.Run(name, func(b *testing.B) {
						ctx := newBenchIndexerContext(b)
						lruCapacity := numBlocks * tc.batchesPerPod
						idx := newIndexer(ctx, lruCapacity, "bench", ApproxPrefixCachePluginType).(*indexer)
						pods := makeBenchServers(numPods, lruCapacity)

						// Scope hash batches per pod so each pod's LRU promotions and evictions
						// operate on its own key space without cross-pod lockstep artifacts.
						podBatches := make([][][]blockHash, numPods)
						podSteps := make([]atomic.Uint64, numPods)
						for pIdx := range numPods {
							baseOffset := uint64(pIdx * numBatches * numBlocks)
							podBatches[pIdx] = makeBenchHashBatches(numBatches, numBlocks, baseOffset)
							for batchIdx := range tc.warmBatchesCap {
								idx.Add(podBatches[pIdx][batchIdx], pods[pIdx])
							}
							podSteps[pIdx].Store(uint64(tc.warmBatchesCap - 1))
						}

						b.ReportAllocs()
						b.ResetTimer()
						if mode == modeSequential {
							for i := 0; i < b.N; i++ {
								podIdx := i % numPods
								batchIdx := (tc.warmBatchesCap + i/numPods) & batchMask
								idx.Add(podBatches[podIdx][batchIdx], pods[podIdx])
							}
						} else {
							var workerSeq atomic.Uint64
							b.RunParallel(func(pb *testing.PB) {
								step := int(workerSeq.Add(1) - 1)
								for pb.Next() {
									podIdx := step % numPods
									batchIdx := int(podSteps[podIdx].Add(1)) & batchMask
									idx.Add(podBatches[podIdx][batchIdx], pods[podIdx])
									step++
								}
							})
						}
						b.StopTimer()
						reportThroughput(b, numBlocks)
					})
				}
			}
		}
	}
}

// BenchmarkApproxIndexerMixedReadWrite measures concurrent read/write contention
// on the approximate prefix indexer using a 90% read (matchLongestPrefix / Get)
// and 10% write (Add) schedule under RunParallel.
func BenchmarkApproxIndexerMixedReadWrite(b *testing.B) {
	const (
		numWriteBatches = 8
		writeBatchMask  = numWriteBatches - 1
		writeKeyOffset  = 1_000_000
	)

	blockCounts := []int{64, 512}
	podCounts := []int{16, 96}

	for _, numBlocks := range blockCounts {
		for _, numPods := range podCounts {
			name := fmt.Sprintf("Blocks=%d/Pods=%d", numBlocks, numPods)
			b.Run(name, func(b *testing.B) {
				ctx := newBenchIndexerContext(b)
				idx := newIndexer(ctx, defaultLRUCapacityPerServer, "bench", ApproxPrefixCachePluginType).(*indexer)
				dp := &dataProducer{indexerInst: idx}

				readHashes := makeBenchHashBatches(1, numBlocks, 0)[0]
				pods := makeBenchServers(numPods, defaultLRUCapacityPerServer)
				podWriteBatches := make([][][]blockHash, numPods)
				for pIdx, srv := range pods {
					idx.Add(readHashes, srv)
					baseOffset := uint64(writeKeyOffset + pIdx*numWriteBatches*numBlocks)
					podWriteBatches[pIdx] = makeBenchHashBatches(numWriteBatches, numBlocks, baseOffset)
				}

				var workerSeq atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					w := int(workerSeq.Add(1) - 1)
					step := w * 10
					writeStep := w
					for pb.Next() {
						step++
						if step%10 == 0 {
							podIdx := writeStep % numPods
							batchIdx := (writeStep / numPods) & writeBatchMask
							idx.Add(podWriteBatches[podIdx][batchIdx], pods[podIdx])
							writeStep++
							continue
						}
						matches := dp.matchLongestPrefix(ctx, readHashes)
						if len(matches) != numPods {
							b.Errorf("matched %d pods, want %d", len(matches), numPods)
							return
						}
					}
				})
				b.StopTimer()
				reportThroughput(b, numBlocks)
			})
		}
	}
}

// BenchmarkPrefixHashGetBlockHashes measures prefixhash.GetBlockHashes across
// prompt block lengths (at 64 tokens per block) in sequential and parallel modes.
func BenchmarkPrefixHashGetBlockHashes(b *testing.B) {
	const blockSizeTokens = 64

	modes := []string{modeSequential, modeParallel}
	blockCounts := []int{64, 512, 3750}

	for _, mode := range modes {
		for _, numBlocks := range blockCounts {
			name := fmt.Sprintf("Mode=%s/Blocks=%d", mode, numBlocks)
			b.Run(name, func(b *testing.B) {
				ctx := log.IntoContext(context.Background(), logr.Discard())
				tokenIDs := make([]uint32, numBlocks*blockSizeTokens)
				for i := range tokenIDs {
					tokenIDs[i] = uint32(i + 1)
				}
				req := &fwksched.InferenceRequest{
					RequestID:   "bench-req",
					TargetModel: "bench-model",
					Body:        tokenizedBody(tokenIDs),
				}

				b.ReportAllocs()
				b.ResetTimer()
				if mode == modeSequential {
					for i := 0; i < b.N; i++ {
						hashes := prefixhash.GetBlockHashes(ctx, req, blockSizeTokens, numBlocks)
						if len(hashes) != 1 || len(hashes[0]) != numBlocks {
							b.Fatalf("got %v prompt hashes, want 1x%d", len(hashes), numBlocks)
						}
					}
				} else {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							hashes := prefixhash.GetBlockHashes(ctx, req, blockSizeTokens, numBlocks)
							if len(hashes) != 1 || len(hashes[0]) != numBlocks {
								b.Errorf("got %v prompt hashes, want 1x%d", len(hashes), numBlocks)
								return
							}
						}
					})
				}
				b.StopTimer()
				reportThroughput(b, numBlocks)
			})
		}
	}
}

type approxMooncakeRecord struct {
	HashIDs []uint64 `json:"hash_ids"`
}

const (
	approxMooncakeExpandFactor    = 16
	approxMooncakeDupFactor       = 20
	approxMooncakeWorkers         = 128
	approxMooncakeWorkerGPUBlocks = 16384
	approxMooncakeMaxBaseReqs     = 1024
)

type approxMooncakeTurn struct {
	workerIdx  int
	queryKeys  []blockHash
	storedKeys []blockHash
	warmKeys   []blockHash
}

type approxMooncakeArtifact struct {
	turns            []approxMooncakeTurn
	avgRequestBlocks int
	avgMixedBlocks   int
}

func mixApproxMooncakeHash(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

var cachedApproxMooncakeArtifact atomic.Pointer[approxMooncakeArtifact]

func loadApproxMooncakeArtifact(tb testing.TB) *approxMooncakeArtifact {
	tb.Helper()
	if art := cachedApproxMooncakeArtifact.Load(); art != nil {
		return art
	}
	art := buildApproxMooncakeArtifact(tb)
	cachedApproxMooncakeArtifact.Store(art)
	return art
}

func buildApproxMooncakeArtifact(tb testing.TB) *approxMooncakeArtifact {
	tb.Helper()
	maxBaseReqs := approxMooncakeMaxBaseReqs
	if envMax := os.Getenv("MOONCAKE_MAX_BASE_REQS"); envMax != "" {
		if n, err := strconv.Atoi(envMax); err == nil && n > 0 {
			maxBaseReqs = n
		}
	}
	tracePath := os.Getenv("MOONCAKE_TRACE_PATH")
	if tracePath == "" {
		tracePath = "/tmp/mooncake_trace.jsonl"
	}
	var baseRecords [][]uint64
	if f, err := os.Open(tracePath); err == nil {
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() && len(baseRecords) < maxBaseReqs {
			var rec approxMooncakeRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil || len(rec.HashIDs) == 0 {
				continue
			}
			baseRecords = append(baseRecords, rec.HashIDs)
		}
	}

	if len(baseRecords) == 0 {
		baseRecords = make([][]uint64, maxBaseReqs)
		for i := range baseRecords {
			numHashIDs := 14 + (i % 10)
			sharedPrefix := 4 + (i % 6)
			groupID := uint64(i / 8)
			rec := make([]uint64, numHashIDs)
			for h := range numHashIDs {
				switch {
				case h == 0:
					rec[h] = groupID
				case h < sharedPrefix:
					rec[h] = 100 + groupID*32 + uint64(h)
				default:
					rec[h] = 10000 + uint64(i)*64 + uint64(h)
				}
			}
			baseRecords[i] = rec
		}
	}

	var evictedCount int
	workerLRUs := make([]*simplelru.LRU[blockHash, struct{}], approxMooncakeWorkers)
	for w := range approxMooncakeWorkers {
		lru, err := simplelru.NewLRU(approxMooncakeWorkerGPUBlocks, func(_ blockHash, _ struct{}) {
			evictedCount++
		})
		if err != nil {
			tb.Fatal(err)
		}
		workerLRUs[w] = lru
	}

	turns := make([]approxMooncakeTurn, 0, len(baseRecords)*approxMooncakeDupFactor)
	totalRequestBlocks := 0
	totalStoredBlocks := 0
	totalRemovedBlocks := 0

	for dup := range uint64(approxMooncakeDupFactor) {
		for recIdx, hids := range baseRecords {
			keys := make([]blockHash, 0, len(hids)*approxMooncakeExpandFactor)
			h := mixApproxMooncakeHash((dup + 1) * 0x9e3779b97f4a7c15)
			for _, hid := range hids {
				for sub := range uint64(approxMooncakeExpandFactor) {
					local := mixApproxMooncakeHash(((hid + 1) * 1000003) ^ (sub + 1))
					h = mixApproxMooncakeHash(h ^ local)
					if h == 0 {
						h = 1
					}
					keys = append(keys, blockHash(h))
				}
			}
			prefixLen := max(1, len(hids)*3/4)
			sessionHash := uint64(42)
			nonZero := false
			for _, hid := range hids[:prefixLen] {
				if hid != 0 {
					nonZero = true
				}
				sessionHash = mixApproxMooncakeHash(sessionHash ^ (hid + 1))
			}
			if !nonZero {
				sessionHash = mixApproxMooncakeHash(sessionHash ^ uint64(recIdx+1))
			}
			w := int(mixApproxMooncakeHash((dup<<32)|sessionHash) % approxMooncakeWorkers)
			wLRU := workerLRUs[w]

			var stored []blockHash
			evictedCount = 0
			for _, k := range keys {
				if _, ok := wLRU.Get(k); !ok {
					stored = append(stored, k)
					wLRU.Add(k, struct{}{})
				}
			}

			totalRequestBlocks += len(keys)
			totalStoredBlocks += len(stored)
			totalRemovedBlocks += evictedCount
			turns = append(turns, approxMooncakeTurn{
				workerIdx:  w,
				queryKeys:  keys,
				storedKeys: stored,
			})
		}
	}

	for i := range turns {
		t := &turns[i]
		if len(t.storedKeys) == 0 {
			continue
		}
		wLRU := workerLRUs[t.workerIdx]
		warm := make([]blockHash, 0, len(t.storedKeys))
		for _, k := range t.storedKeys {
			if wLRU.Contains(k) {
				warm = append(warm, k)
			}
		}
		t.warmKeys = warm
	}

	numTurns := max(1, len(turns))
	avgReq := max(1, totalRequestBlocks/numTurns)
	avgMixed := max(1, (totalRequestBlocks+totalStoredBlocks+totalRemovedBlocks)/numTurns)
	return &approxMooncakeArtifact{
		turns:            turns,
		avgRequestBlocks: avgReq,
		avgMixedBlocks:   avgMixed,
	}
}

// BenchmarkApproxMooncakeReplay replays the Mooncake arxiv trace workload across
// 128 inference workers (16,384 GPU blocks/worker, cumulative sequence hashes,
// dup=20, length-factor=4) against ApproximatePrefixCache under QueryOnly and
// MixedOverload using the INDEXER_BENCH.md block denominator.
func BenchmarkApproxMooncakeReplay(b *testing.B) {
	art := loadApproxMooncakeArtifact(b)
	ctx := newBenchIndexerContext(b)
	idx := newIndexer(ctx, approxMooncakeWorkerGPUBlocks, "bench", ApproxPrefixCachePluginType).(*indexer)
	dp := &dataProducer{indexerInst: idx}
	pods := makeBenchServers(approxMooncakeWorkers, approxMooncakeWorkerGPUBlocks)

	for i := range art.turns {
		turn := &art.turns[i]
		if len(turn.warmKeys) > 0 {
			idx.Add(turn.warmKeys, pods[turn.workerIdx])
		}
	}

	for _, replayMode := range []string{"QueryOnly", "MixedOverload"} {
		withEvents := replayMode == "MixedOverload"
		b.Run("Mode="+replayMode, func(b *testing.B) {
			numTurns := len(art.turns)
			var workerSeq atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				iter := int(workerSeq.Add(1)-1) * 9973
				for pb.Next() {
					iter++
					turn := &art.turns[iter%numTurns]
					_ = dp.matchLongestPrefix(ctx, turn.queryKeys)
					if withEvents && len(turn.storedKeys) > 0 {
						idx.Add(turn.storedKeys, pods[turn.workerIdx])
					}
				}
			})
			b.StopTimer()
			blocksPerOp := art.avgRequestBlocks
			if withEvents {
				blocksPerOp = art.avgMixedBlocks
			}
			reportThroughput(b, blocksPerOp)
		})
	}
}
