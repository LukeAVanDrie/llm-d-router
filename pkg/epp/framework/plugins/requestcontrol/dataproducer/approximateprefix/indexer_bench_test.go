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
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	modeSequential = "Sequential"
	modeParallel   = "Parallel"

	topologyAllPodsHit    = "AllPodsHit"
	topologyHalfPodsHit   = "HalfPodsHit"
	topologySinglePodHit  = "SinglePodHit"
	topologyBranching2575 = "Branching25_75"
)

var (
	benchModes      = []string{modeSequential, modeParallel}
	benchTopologies = []string{
		topologyAllPodsHit,
		topologyHalfPodsHit,
		topologySinglePodHit,
		topologyBranching2575,
	}
	benchBlockCounts = []int{64, 512, 3750}
	benchPodCounts   = []int{16, 96}
)

func newBenchIndexerContext(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), logr.Discard()))
	tb.Cleanup(cancel)
	return ctx
}

func makeBenchServers(numPods, lruCap int) []server {
	servers := make([]server, numPods)
	for i := range servers {
		servers[i] = server{
			ServerID: ServerID(k8stypes.NamespacedName{
				Namespace: "default",
				Name:      fmt.Sprintf("pod-%03d", i),
			}),
			NumOfGPUBlocks: lruCap,
		}
	}
	return servers
}

func makeBenchHashBatches(numBatches, numBlocks int, baseOffset uint64) [][]blockHash {
	batches := make([][]blockHash, numBatches)
	for bIdx := range batches {
		batch := make([]blockHash, numBlocks)
		batchBase := baseOffset + uint64(bIdx*numBlocks) //#nosec G115 -- bounded benchmark fixture
		for i := range batch {
			batch[i] = blockHash(batchBase + uint64(i) + 1) //#nosec G115 -- bounded benchmark fixture
		}
		batches[bIdx] = batch
	}
	return batches
}

func reportThroughput(b *testing.B, blocksPerOp int) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	reqsPerSec := float64(b.N) / elapsed
	b.ReportMetric(float64(blocksPerOp)*reqsPerSec, "block_ops/s")
	b.ReportMetric(reqsPerSec, "reqs/s")
}

func runApproxBenchMode(b *testing.B, mode string, numBlocks int, stepFn func(workerID, step int)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	if mode == modeSequential {
		for i := range b.N {
			stepFn(0, i)
		}
	} else {
		var workerSeq atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			wID := int(workerSeq.Add(1) - 1)
			step := wID * 13
			for pb.Next() {
				stepFn(wID, step)
				step++
			}
		})
	}
	b.StopTimer()
	reportThroughput(b, numBlocks)
}

func populateMatchTopology(
	idx *indexer,
	topology string,
	numBlocks int,
	pods []server,
) (queryBatches [][]blockHash, expectedMatchedPods int) {
	numPods := len(pods)
	switch topology {
	case topologyAllPodsHit:
		batches := makeBenchHashBatches(1, numBlocks, 0)
		for _, srv := range pods {
			idx.Add(batches[0], srv)
		}
		return batches, numPods

	case topologyHalfPodsHit:
		batches := makeBenchHashBatches(1, numBlocks, 0)
		half := numPods / 2
		for i := range half {
			idx.Add(batches[0], pods[i])
		}
		return batches, half

	case topologySinglePodHit:
		batches := makeBenchHashBatches(numPods, numBlocks, 0)
		for i, srv := range pods {
			idx.Add(batches[i], srv)
		}
		return batches, 1

	case topologyBranching2575:
		sharedLen := numBlocks / 4
		sharedPrefix := makeBenchHashBatches(1, sharedLen, 0)[0]
		batches := make([][]blockHash, numPods)
		for i, srv := range pods {
			branch := make([]blockHash, numBlocks)
			copy(branch[:sharedLen], sharedPrefix)
			suffixBase := uint64(sharedLen + 1 + i*(numBlocks-sharedLen)) //#nosec G115 -- bounded benchmark fixture
			for j := sharedLen; j < numBlocks; j++ {
				branch[j] = blockHash(suffixBase + uint64(j-sharedLen)) //#nosec G115 -- bounded benchmark fixture
			}
			batches[i] = branch
			idx.Add(branch, srv)
		}
		return batches, numPods

	default:
		panic(fmt.Sprintf("unknown topology %q", topology))
	}
}

// BenchmarkApproxIndexerMatch measures approximate prefix matching throughput
// and allocations across sequential/parallel execution, prefix topologies,
// block lengths, and fleet sizes.
func BenchmarkApproxIndexerMatch(b *testing.B) {
	for _, mode := range benchModes {
		for _, topology := range benchTopologies {
			for _, numBlocks := range benchBlockCounts {
				for _, numPods := range benchPodCounts {
					name := fmt.Sprintf("Mode=%s/Topology=%s/Blocks=%d/Pods=%d",
						mode, topology, numBlocks, numPods)
					b.Run(name, func(b *testing.B) {
						ctx := newBenchIndexerContext(b)
						idx := newIndexer(ctx, defaultLRUCapacityPerServer, "bench", ApproxPrefixCachePluginType).(*indexer)
						dp := &dataProducer{indexerInst: idx}

						pods := makeBenchServers(numPods, defaultLRUCapacityPerServer)
						queryBatches, expectedPods := populateMatchTopology(idx, topology, numBlocks, pods)
						numQueries := len(queryBatches)

						runApproxBenchMode(b, mode, numBlocks, func(_, step int) {
							matches := dp.matchLongestPrefix(ctx, queryBatches[step%numQueries])
							if len(matches) != expectedPods {
								b.Errorf("matched %d pods, want %d", len(matches), expectedPods)
							}
						})
					})
				}
			}
		}
	}
}

// BenchmarkApproxIndexerAdd measures steady-state Add throughput both within
// per-pod LRU capacity and under continuous LRU eviction churn.
func BenchmarkApproxIndexerAdd(b *testing.B) {
	const (
		batchesPerPod = 16
		batchMask     = batchesPerPod - 1
	)

	regimes := []struct {
		name           string
		warmBatchesCap int
	}{
		{name: "WithinCapacity", warmBatchesCap: batchesPerPod},
		{name: "EvictionChurn", warmBatchesCap: 4},
	}

	for _, tc := range regimes {
		for _, mode := range benchModes {
			for _, numBlocks := range []int{16, 64} {
				for _, numPods := range benchPodCounts {
					name := fmt.Sprintf("Regime=%s/Mode=%s/Blocks=%d/Pods=%d",
						tc.name, mode, numBlocks, numPods)
					b.Run(name, func(b *testing.B) {
						ctx := newBenchIndexerContext(b)
						lruCap := tc.warmBatchesCap * numBlocks
						if tc.name == "WithinCapacity" {
							lruCap *= 2
						}
						idx := newIndexer(ctx, lruCap, "bench", ApproxPrefixCachePluginType).(*indexer)
						pods := makeBenchServers(numPods, lruCap)

						podBatches := make([][][]blockHash, numPods)
						podSteps := make([]atomic.Uint64, numPods)
						for pIdx := range numPods {
							baseOffset := uint64(pIdx * batchesPerPod * numBlocks) //#nosec G115 -- bounded benchmark fixture
							podBatches[pIdx] = makeBenchHashBatches(batchesPerPod, numBlocks, baseOffset)
							for batchIdx := range tc.warmBatchesCap {
								idx.Add(podBatches[pIdx][batchIdx], pods[pIdx])
							}
							podSteps[pIdx].Store(uint64(tc.warmBatchesCap - 1)) //#nosec G115 -- bounded benchmark fixture
						}

						runApproxBenchMode(b, mode, numBlocks, func(_, step int) {
							podIdx := step % numPods
							var batchIdx int
							if mode == modeSequential {
								batchIdx = (tc.warmBatchesCap + step/numPods) & batchMask
							} else {
								batchIdx = int(podSteps[podIdx].Add(1)) & batchMask //#nosec G115 -- masked to [0, 15]
							}
							idx.Add(podBatches[podIdx][batchIdx], pods[podIdx])
						})
					})
				}
			}
		}
	}
}

// BenchmarkApproxIndexerMixedReadWrite measures concurrent read/write contention
// on the approximate prefix indexer using a 90% read (matchLongestPrefix) and
// 10% write (Add) schedule under RunParallel.
func BenchmarkApproxIndexerMixedReadWrite(b *testing.B) {
	const (
		numWriteBatches = 8
		writeBatchMask  = numWriteBatches - 1
		writeKeyOffset  = 1_000_000
	)

	for _, numBlocks := range []int{64, 512} {
		for _, numPods := range benchPodCounts {
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
					baseOffset := uint64(writeKeyOffset + pIdx*numWriteBatches*numBlocks) //#nosec G115 -- bounded benchmark fixture
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
	if f, err := os.Open(tracePath); err == nil { //#nosec G304 -- benchmark trace path configured via env
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() && len(baseRecords) < maxBaseReqs {
			var rec struct {
				HashIDs []uint64 `json:"hash_ids"`
			}
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
			groupID := uint64(i / 8) //#nosec G115 -- bounded benchmark fixture
			rec := make([]uint64, numHashIDs)
			for h := range numHashIDs {
				switch {
				case h == 0:
					rec[h] = groupID
				case h < sharedPrefix:
					rec[h] = 100 + groupID*32 + uint64(h) //#nosec G115 -- bounded benchmark fixture
				default:
					rec[h] = 10000 + uint64(i)*64 + uint64(h) //#nosec G115 -- bounded benchmark fixture
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
	totalRequestBlocks, totalStoredBlocks, totalRemovedBlocks := 0, 0, 0

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
				sessionHash = mixApproxMooncakeHash(sessionHash ^ uint64(recIdx+1)) //#nosec G115 -- bounded benchmark fixture
			}
			w := int(mixApproxMooncakeHash((dup<<32)|sessionHash) % approxMooncakeWorkers) //#nosec G115 -- bounded by approxMooncakeWorkers
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
	return &approxMooncakeArtifact{
		turns:            turns,
		avgRequestBlocks: max(1, totalRequestBlocks/numTurns),
		avgMixedBlocks:   max(1, (totalRequestBlocks+totalStoredBlocks+totalRemovedBlocks)/numTurns),
	}
}

// BenchmarkApproxMooncakeReplay replays the Mooncake trace workload across 128
// inference workers against ApproximatePrefixCache under QueryOnly and
// MixedOverload modes.
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
