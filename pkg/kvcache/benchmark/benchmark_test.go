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

package benchmark

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

type readOpConfig struct {
	name          string
	hashAlgorithm string
	scoreTokens   bool
}

var readOps = []readOpConfig{
	{
		name:          "MatchBlockKeys",
		hashAlgorithm: kvblock.HashAlgorithmXXH64,
		scoreTokens:   false,
	},
	{
		name:          "ScoreTokens_XXH64",
		hashAlgorithm: kvblock.HashAlgorithmXXH64,
		scoreTokens:   true,
	},
	{
		name:          "ScoreTokens_CBORFNV",
		hashAlgorithm: kvblock.HashAlgorithmCBORFNV,
		scoreTokens:   true,
	},
}

// BenchmarkReadMatrix evaluates steady-state prefix-matching read throughput
// across Operation, Mode, Topology, Blocks, and Pods.
func BenchmarkReadMatrix(b *testing.B) {
	for _, op := range readOps {
		for _, mode := range benchModes {
			for _, topology := range benchTopologies {
				for _, numBlocks := range benchBlockLengths {
					for _, fleet := range benchFleetShapes {
						subName := fmt.Sprintf("Op=%s/Mode=%s/Topology=%s/Blocks=%d/Pods=%s",
							op.name, mode, topology, numBlocks, fleet.name())
						b.Run(subName, func(b *testing.B) {
							fixture := setupReadFixture(b, op.hashAlgorithm, topology, numBlocks, fleet)
							runReadCoordinate(b, fixture, op.scoreTokens, mode, numBlocks)
						})
					}
				}
			}
		}
	}
}

func execReadStep(ctx context.Context, fixture *readFixture, scoreTokens bool, step int) (int, error) {
	if scoreTokens {
		qIdx := step % len(fixture.tokenQueries)
		scores, err := fixture.indexer.ScoreTokens(ctx, fixture.tokenQueries[qIdx], benchModelName, nil, nil)
		return len(scores), err
	}
	qIdx := step % len(fixture.queries)
	matches, err := fixture.indexer.MatchBlockKeys(ctx, fixture.queries[qIdx], nil)
	return len(matches), err
}

func runReadCoordinate(b *testing.B, fixture *readFixture, scoreTokens bool, mode execMode, numBlocks int) {
	ctx := benchContext()

	b.ReportAllocs()
	b.ResetTimer()

	if mode == modeSequential {
		for i := range b.N {
			got, err := execReadStep(ctx, fixture, scoreTokens, i)
			if err != nil {
				b.Fatal(err)
			}
			if got != fixture.expectedMatchedPods {
				b.Fatalf("matched %d pods, want %d", got, fixture.expectedMatchedPods)
			}
		}
	} else {
		var workerID atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			idx := int(workerID.Add(1)) * 9973
			for pb.Next() {
				idx++
				got, err := execReadStep(ctx, fixture, scoreTokens, idx)
				if err != nil {
					b.Error(err)
					return
				}
				if got != fixture.expectedMatchedPods {
					b.Errorf("matched %d pods, want %d", got, fixture.expectedMatchedPods)
					return
				}
			}
		})
	}

	b.StopTimer()
	reportThroughput(b, numBlocks)
}

// BenchmarkMixedReadWrite evaluates concurrent read/write contention using a
// deterministic RunParallel mixed-actor schedule (90% read, 10% write+evict).
func BenchmarkMixedReadWrite(b *testing.B) {
	topologies := []topologyType{TopologyHotspot, TopologySystemPromptBranch}
	for _, topology := range topologies {
		for _, numBlocks := range benchBlockLengths {
			for _, fleet := range singleRankFleetShapes {
				subName := fmt.Sprintf("Topology=%s/Blocks=%d/Pods=%d",
					topology, numBlocks, fleet.pods)
				b.Run(subName, func(b *testing.B) {
					runMixedReadWriteCoordinate(b, topology, numBlocks, fleet)
				})
			}
		}
	}
}

func runMixedReadWriteCoordinate(b *testing.B, topology topologyType, numBlocks int, fleet fleetShape) {
	ctx := benchContext()
	fixture := setupReadFixture(b, kvblock.HashAlgorithmXXH64, topology, numBlocks, fleet)

	const (
		batchesPerWorker = 16
		halfWorkerRing   = batchesPerWorker / 2
		writeBatchSize   = 64
		highKeyBase      = uint64(1) << 48
	)
	numWorkerSlots := max(runtime.GOMAXPROCS(0)*4, 16)
	writeBatches := buildWriteBatches(numWorkerSlots*batchesPerWorker, writeBatchSize, fleet.pods, highKeyBase)

	for w := range numWorkerSlots {
		base := w * batchesPerWorker
		for j := halfWorkerRing; j < batchesPerWorker; j++ {
			wb := &writeBatches[base+j]
			if err := fixture.index.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
		}
	}

	numQueries := len(fixture.queries)
	var workerSeq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		slot := int(workerSeq.Add(1)-1) % numWorkerSlots
		workerRing := writeBatches[slot*batchesPerWorker : (slot+1)*batchesPerWorker]
		step := 0
		writeStep := 0
		for pb.Next() {
			step++
			if step%10 == 0 {
				addBatch := &workerRing[writeStep&(batchesPerWorker-1)]
				evictBatch := &workerRing[(writeStep+halfWorkerRing)&(batchesPerWorker-1)]
				writeStep++

				if err := fixture.index.Add(ctx, addBatch.engineKeys, addBatch.requestKeys, addBatch.entries); err != nil {
					b.Error(err)
					return
				}
				for _, k := range evictBatch.engineKeys {
					if err := fixture.index.Evict(ctx, k, kvblock.EngineKey, evictBatch.entries); err != nil {
						b.Error(err)
						return
					}
				}
				continue
			}

			qIdx := (slot + step) % numQueries
			matches, err := fixture.indexer.MatchBlockKeys(ctx, fixture.queries[qIdx], nil)
			if err != nil {
				b.Error(err)
				return
			}
			if len(matches) != fixture.expectedMatchedPods {
				b.Errorf("matched %d pods, want %d", len(matches), fixture.expectedMatchedPods)
				return
			}
		}
	})

	b.StopTimer()
	// 90% of iterations process numBlocks (read) and 10% process 2*writeBatchSize (Add + Evict).
	avgBlocksPerOp := (9*numBlocks + 2*writeBatchSize) / 10
	reportThroughput(b, avgBlocksPerOp)
}

type producerLifecycleOp string

const (
	producerOpProduceOnly          producerLifecycleOp = "ProduceOnly"
	producerOpProduceAndPreRequest producerLifecycleOp = "ProduceAndPreRequest"
)

type producerImpl string

const (
	producerImplPrecise     producerImpl = "PrecisePrefixCache"
	producerImplApproximate producerImpl = "ApproximatePrefixCache"
)

// BenchmarkDataProducerComparison compares preciseprefixcache.Producer and
// approximateprefix DataProducer across ProduceOnly and ProduceAndPreRequest
// lifecycles, Sequential and Parallel execution, fleet sizes, and block lengths.
func BenchmarkDataProducerComparison(b *testing.B) {
	for _, impl := range []producerImpl{producerImplPrecise, producerImplApproximate} {
		for _, op := range []producerLifecycleOp{producerOpProduceOnly, producerOpProduceAndPreRequest} {
			for _, mode := range benchModes {
				for _, fleet := range singleRankFleetShapes {
					for _, numBlocks := range producerBlockLengths {
						subName := fmt.Sprintf("Plugin=%s/Op=%s/Mode=%s/Pods=%d/Blocks=%d",
							impl, op, mode, fleet.pods, numBlocks)
						b.Run(subName, func(b *testing.B) {
							runProducerComparisonCoordinate(b, impl, op, mode, fleet.pods, numBlocks)
						})
					}
				}
			}
		}
	}
}

func runProducerComparisonCoordinate(
	b *testing.B,
	impl producerImpl,
	op producerLifecycleOp,
	mode execMode,
	numPods int,
	numBlocks int,
) {
	var h *producerHarness
	switch impl {
	case producerImplPrecise:
		h = setupPreciseProducerHarness(b, numPods, numBlocks)
	case producerImplApproximate:
		h = setupApproxProducerHarness(b, numPods, numBlocks)
	default:
		b.Fatalf("unknown producer implementation %q", impl)
	}

	ctx := benchContext()
	numWorkers := len(h.workers)

	b.ReportAllocs()
	b.ResetTimer()

	if mode == modeSequential {
		wf := &h.workers[0]
		for range b.N {
			if err := h.producer.Produce(ctx, wf.req, wf.endpoints); err != nil {
				b.Fatal(err)
			}
			if op == producerOpProduceAndPreRequest {
				if err := h.preRequest.PreRequest(ctx, wf.req, wf.schedResult); err != nil {
					b.Fatal(err)
				}
			}
		}
	} else {
		var workerSeq atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			wIdx := int(workerSeq.Add(1)-1) % numWorkers
			wf := &h.workers[wIdx]
			for pb.Next() {
				if err := h.producer.Produce(ctx, wf.req, wf.endpoints); err != nil {
					b.Error(err)
					return
				}
				if op == producerOpProduceAndPreRequest {
					if err := h.preRequest.PreRequest(ctx, wf.req, wf.schedResult); err != nil {
						b.Error(err)
						return
					}
				}
			}
		})
	}

	b.StopTimer()
	reportThroughput(b, numBlocks)
}

type mooncakeTraceRecord struct {
	HashIDs []uint64 `json:"hash_ids"`
}

const (
	mooncakeExpandFactor    = 16
	mooncakeDupFactor       = 20
	mooncakeWorkers         = 128
	mooncakeWorkerGPUBlocks = 16384
	mooncakeMaxBaseReqs     = 1024
)

type mooncakeReplayTurn struct {
	workerIdx   int
	queryKeys   []kvblock.BlockHash
	storedKeys  []kvblock.BlockHash
	removedKeys []kvblock.BlockHash
	warmKeys    []kvblock.BlockHash
}

type mooncakeArtifact struct {
	turns            []mooncakeReplayTurn
	avgRequestBlocks int
	avgMixedBlocks   int
}

func mixMooncakeHash(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

var cachedMooncakeArtifact atomic.Pointer[mooncakeArtifact]

func loadMooncakeArtifact(tb testing.TB) *mooncakeArtifact {
	tb.Helper()
	if art := cachedMooncakeArtifact.Load(); art != nil {
		return art
	}
	art := buildMooncakeArtifact(tb)
	cachedMooncakeArtifact.Store(art)
	return art
}

func buildMooncakeArtifact(tb testing.TB) *mooncakeArtifact {
	tb.Helper()
	maxBaseReqs := mooncakeMaxBaseReqs
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
			var rec mooncakeTraceRecord
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

	var evictedBuf []kvblock.BlockHash
	workerLRUs := make([]*simplelru.LRU[kvblock.BlockHash, struct{}], mooncakeWorkers)
	for w := range mooncakeWorkers {
		lru, err := simplelru.NewLRU(mooncakeWorkerGPUBlocks, func(k kvblock.BlockHash, _ struct{}) {
			evictedBuf = append(evictedBuf, k)
		})
		if err != nil {
			tb.Fatal(err)
		}
		workerLRUs[w] = lru
	}

	turns := make([]mooncakeReplayTurn, 0, len(baseRecords)*mooncakeDupFactor)
	totalRequestBlocks := 0
	totalStoredBlocks := 0
	totalRemovedBlocks := 0

	for dup := range uint64(mooncakeDupFactor) {
		for recIdx, hids := range baseRecords {
			keys := make([]kvblock.BlockHash, 0, len(hids)*mooncakeExpandFactor)
			h := mixMooncakeHash((dup + 1) * 0x9e3779b97f4a7c15)
			for _, hid := range hids {
				for sub := range uint64(mooncakeExpandFactor) {
					local := mixMooncakeHash(((hid + 1) * 1000003) ^ (sub + 1))
					h = mixMooncakeHash(h ^ local)
					if h == 0 {
						h = 1
					}
					keys = append(keys, kvblock.BlockHash(h))
				}
			}
			prefixLen := max(1, len(hids)*3/4)
			sessionHash := uint64(42)
			nonZero := false
			for _, hid := range hids[:prefixLen] {
				if hid != 0 {
					nonZero = true
				}
				sessionHash = mixMooncakeHash(sessionHash ^ (hid + 1))
			}
			if !nonZero {
				sessionHash = mixMooncakeHash(sessionHash ^ uint64(recIdx+1)) //#nosec G115 -- bounded benchmark fixture
			}
			w := int(mixMooncakeHash((dup<<32)|sessionHash) % mooncakeWorkers) //#nosec G115 -- bounded by mooncakeWorkers
			wLRU := workerLRUs[w]

			var stored []kvblock.BlockHash
			evictedBuf = evictedBuf[:0]
			for _, k := range keys {
				if _, ok := wLRU.Get(k); !ok {
					stored = append(stored, k)
					wLRU.Add(k, struct{}{})
				}
			}
			var removed []kvblock.BlockHash
			if len(evictedBuf) > 0 {
				removed = make([]kvblock.BlockHash, len(evictedBuf))
				copy(removed, evictedBuf)
			}

			totalRequestBlocks += len(keys)
			totalStoredBlocks += len(stored)
			totalRemovedBlocks += len(removed)
			turns = append(turns, mooncakeReplayTurn{
				workerIdx:   w,
				queryKeys:   keys,
				storedKeys:  stored,
				removedKeys: removed,
			})
		}
	}

	for i := range turns {
		t := &turns[i]
		if len(t.storedKeys) == 0 {
			continue
		}
		wLRU := workerLRUs[t.workerIdx]
		warm := make([]kvblock.BlockHash, 0, len(t.storedKeys))
		for _, k := range t.storedKeys {
			if wLRU.Contains(k) {
				warm = append(warm, k)
			}
		}
		t.warmKeys = warm
	}

	numTurns := max(1, len(turns))
	return &mooncakeArtifact{
		turns:            turns,
		avgRequestBlocks: max(1, totalRequestBlocks/numTurns),
		avgMixedBlocks:   max(1, (totalRequestBlocks+totalStoredBlocks+totalRemovedBlocks)/numTurns),
	}
}

// BenchmarkMooncakeReplay replays the Mooncake trace workload across 128
// inference workers under QueryOnly and MixedOverload modes.
func BenchmarkMooncakeReplay(b *testing.B) {
	art := loadMooncakeArtifact(b)
	ctx, indexer, idx, workerEntries := setupMooncakeReplay(b, art)

	for _, replayMode := range []string{"QueryOnly", "MixedOverload"} {
		withEvents := replayMode == "MixedOverload"
		b.Run("Mode="+replayMode, func(b *testing.B) {
			runMooncakeReplay(b, ctx, indexer, idx, workerEntries, art, withEvents)
		})
	}
}

func setupMooncakeReplay(
	b *testing.B,
	art *mooncakeArtifact,
) (context.Context, *kvcache.Indexer, kvblock.Index, [][]kvblock.PodEntry) {
	b.Helper()
	ctx := benchContext()
	tp, err := kvblock.NewChunkedTokenDatabase(&kvblock.TokenProcessorConfig{
		BlockSizeTokens: defaultBlockSizeTokens,
		HashAlgorithm:   kvblock.HashAlgorithmXXH64,
	})
	if err != nil {
		b.Fatal(err)
	}
	cfg, err := kvcache.NewDefaultConfig()
	if err != nil {
		b.Fatal(err)
	}
	cfg.KVBlockIndexConfig.InMemoryConfig = &kvblock.InMemoryIndexConfig{
		Size:         1 << 23,
		PodCacheSize: mooncakeWorkers,
	}
	cfg.KVBlockIndexConfig.EnableMetrics = false

	indexer, err := kvcache.NewKVCacheIndexer(ctx, cfg, tp)
	if err != nil {
		b.Fatal(err)
	}
	idx := indexer.KVBlockIndex()

	workerEntries := make([][]kvblock.PodEntry, mooncakeWorkers)
	for w := range mooncakeWorkers {
		workerEntries[w] = []kvblock.PodEntry{{
			PodIdentifier: podIdentifier(w),
			DeviceTier:    "gpu",
		}}
	}

	for i := range art.turns {
		turn := &art.turns[i]
		if len(turn.warmKeys) > 0 {
			if err := idx.Add(ctx, turn.warmKeys, turn.warmKeys, workerEntries[turn.workerIdx]); err != nil {
				b.Fatal(err)
			}
		}
	}
	return ctx, indexer, idx, workerEntries
}

func runMooncakeReplay(
	b *testing.B,
	ctx context.Context,
	indexer *kvcache.Indexer,
	idx kvblock.Index,
	workerEntries [][]kvblock.PodEntry,
	art *mooncakeArtifact,
	withEvents bool,
) {
	numTurns := len(art.turns)
	var workerID atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		iter := int(workerID.Add(1)-1) * 9973
		for pb.Next() {
			iter++
			turn := &art.turns[iter%numTurns]
			if _, err := indexer.MatchBlockKeys(ctx, turn.queryKeys, nil); err != nil {
				b.Error(err)
				return
			}
			if withEvents {
				if len(turn.storedKeys) > 0 {
					if err := idx.Add(ctx, turn.storedKeys, turn.storedKeys, workerEntries[turn.workerIdx]); err != nil {
						b.Error(err)
						return
					}
				}
				for _, k := range turn.removedKeys {
					if err := idx.Evict(ctx, k, kvblock.EngineKey, workerEntries[turn.workerIdx]); err != nil {
						b.Error(err)
						return
					}
				}
			}
		}
	})

	b.StopTimer()
	blocksPerOp := art.avgRequestBlocks
	if withEvents {
		blocksPerOp = art.avgMixedBlocks
	}
	reportThroughput(b, blocksPerOp)
}
