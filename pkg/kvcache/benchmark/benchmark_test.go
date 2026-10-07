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

type readOp struct {
	name          string
	hashAlgorithm string
	scoreTokens   bool
}

var benchReadOps = []readOp{
	{name: "MatchBlockKeys", hashAlgorithm: kvblock.HashAlgorithmXXH64, scoreTokens: false},
	{name: "ScoreTokens_XXH64", hashAlgorithm: kvblock.HashAlgorithmXXH64, scoreTokens: true},
	{name: "ScoreTokens_CBORFNV", hashAlgorithm: kvblock.HashAlgorithmCBORFNV, scoreTokens: true},
}

// BenchmarkReadMatrix evaluates Indexer.MatchBlockKeys and Indexer.ScoreTokens
// (xxh64 and cbor-fnv) across Mode, Topology, Blocks, and Pods.
func BenchmarkReadMatrix(b *testing.B) {
	for _, op := range benchReadOps {
		for _, topology := range benchTopologies {
			for _, numBlocks := range benchBlockLengths {
				for _, fleet := range benchFleetShapes {
					for _, mode := range benchModes {
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

func runReadCoordinate(b *testing.B, fixture *readFixture, scoreTokens bool, mode execMode, numBlocks int) {
	ctx := benchContext()
	numBlockQueries := len(fixture.queries)
	numTokenQueries := len(fixture.tokenQueries)

	b.ReportAllocs()
	b.ResetTimer()

	switch mode {
	case modeSequential:
		for i := range b.N {
			if scoreTokens {
				qIdx := i % numTokenQueries
				scores, err := fixture.indexer.ScoreTokens(ctx, fixture.tokenQueries[qIdx], benchModelName, nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(scores) != fixture.expectedMatchedPods {
					b.Fatalf("scored %d pods, want %d", len(scores), fixture.expectedMatchedPods)
				}
			} else {
				qIdx := i % numBlockQueries
				matches, err := fixture.indexer.MatchBlockKeys(ctx, fixture.queries[qIdx], nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(matches) != fixture.expectedMatchedPods {
					b.Fatalf("matched %d pods, want %d", len(matches), fixture.expectedMatchedPods)
				}
			}
		}

	case modeParallel:
		var workerID atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			idx := int(workerID.Add(1)) * 9973
			for pb.Next() {
				idx++
				if scoreTokens {
					qIdx := idx % numTokenQueries
					scores, err := fixture.indexer.ScoreTokens(ctx, fixture.tokenQueries[qIdx], benchModelName, nil, nil)
					if err != nil {
						b.Error(err)
						return
					}
					if len(scores) != fixture.expectedMatchedPods {
						b.Errorf("scored %d pods, want %d", len(scores), fixture.expectedMatchedPods)
						return
					}
				} else {
					qIdx := idx % numBlockQueries
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
			}
		})
	}

	b.StopTimer()
	reportThroughput(b, numBlocks)
}

type writeBackend string

const (
	backendInMemory  writeBackend = "InMemory"
	backendCostAware writeBackend = "CostAware"
)

type writeOp string

const (
	writeOpAdd         writeOp = "Add"
	writeOpAddAndEvict writeOp = "AddAndEvict"
)

func newWriteBenchIndex(b *testing.B, backend writeBackend, numPods int) kvblock.Index {
	b.Helper()
	var inner kvblock.Index
	var err error
	switch backend {
	case backendInMemory:
		inner, err = kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{
			Size:         1 << 20,
			PodCacheSize: max(128, numPods*2),
		})
	case backendCostAware:
		inner, err = kvblock.NewCostAwareMemoryIndex(&kvblock.CostAwareMemoryIndexConfig{
			Size:        "256MiB",
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
	backends := []writeBackend{backendInMemory, backendCostAware}
	ops := []writeOp{writeOpAdd, writeOpAddAndEvict}

	for _, op := range ops {
		for _, mode := range benchModes {
			for _, backend := range backends {
				for _, batchBlocks := range writeBatchSizes {
					for _, fleet := range singleRankFleetShapes {
						subName := fmt.Sprintf("Op=%s/Mode=%s/Backend=%s/BatchBlocks=%d/Pods=%d",
							op, mode, backend, batchBlocks, fleet.pods)
						b.Run(subName, func(b *testing.B) {
							runWriteCoordinate(b, op, mode, backend, batchBlocks, fleet.pods)
						})
					}
				}
			}
		}
	}
}

func runWriteCoordinate(b *testing.B, op writeOp, mode execMode, backend writeBackend, batchBlocks, numPods int) {
	ctx := benchContext()
	idx := newWriteBenchIndex(b, backend, numPods)

	// Partition batches across worker slots so concurrent goroutines in Parallel mode
	// never race on the same key batch during AddAndEvict.
	const batchesPerWorker = 16
	numWorkerSlots := max(runtime.GOMAXPROCS(0)*4, 16)
	totalBatches := numWorkerSlots * batchesPerWorker
	batches := buildWriteBatches(totalBatches, batchBlocks, numPods, 1<<32)

	if op == writeOpAdd {
		for i := range batches {
			wb := &batches[i]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	switch mode {
	case modeSequential:
		for i := range b.N {
			wb := &batches[i%totalBatches]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
			if op == writeOpAddAndEvict {
				for _, k := range wb.engineKeys {
					if err := idx.Evict(ctx, k, kvblock.EngineKey, wb.entries); err != nil {
						b.Fatal(err)
					}
				}
			}
		}

	case modeParallel:
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
				if op == writeOpAddAndEvict {
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
	if op == writeOpAddAndEvict {
		blocksPerOp = batchBlocks * 2
	}
	reportThroughput(b, blocksPerOp)
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

	// Pre-populate the second half of each worker's ring so the first half of writes
	// have resident keys to evict, keeping index occupancy in steady-state equilibrium.
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
	var workerID atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		wSlot := int(workerID.Add(1)-1) % numWorkerSlots
		workerRing := writeBatches[wSlot*batchesPerWorker : (wSlot+1)*batchesPerWorker]
		iter := wSlot * 9973
		writeStep := 0
		for pb.Next() {
			iter++
			if iter%10 != 0 {
				qIdx := iter % numQueries
				matches, err := fixture.indexer.MatchBlockKeys(ctx, fixture.queries[qIdx], nil)
				if err != nil {
					b.Error(err)
					return
				}
				if len(matches) != fixture.expectedMatchedPods {
					b.Errorf("matched %d pods, want %d", len(matches), fixture.expectedMatchedPods)
					return
				}
				continue
			}

			addWb := &workerRing[writeStep&(batchesPerWorker-1)]
			evictWb := &workerRing[(writeStep+halfWorkerRing)&(batchesPerWorker-1)]
			writeStep++

			if err := fixture.index.Add(ctx, addWb.engineKeys, addWb.requestKeys, addWb.entries); err != nil {
				b.Error(err)
				return
			}
			for _, k := range evictWb.engineKeys {
				if err := fixture.index.Evict(ctx, k, kvblock.EngineKey, evictWb.entries); err != nil {
					b.Error(err)
					return
				}
			}
		}
	})

	b.StopTimer()
	// 90% of ops process numBlocks read keys; 10% process 2*writeBatchSize (Add + Evict) keys.
	avgBlocksPerOp := (9*numBlocks + 2*writeBatchSize) / 10
	reportThroughput(b, avgBlocksPerOp)
}

// BenchmarkEvictionDynamics evaluates steady-state LRU eviction churn when unique block
// keys (4x index capacity) cycle through InMemoryIndex (global Size overflow and per-key
// PodCacheSize overflow) and CostAwareMemoryIndex.
func BenchmarkEvictionDynamics(b *testing.B) {
	for _, mode := range benchModes {
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

func runGlobalKeyEvictionChurn(b *testing.B, mode execMode) {
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
	// Warm the index to capacity before timing.
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
	reportThroughput(b, batchBlocks)
}

func runPodCacheEvictionChurn(b *testing.B, mode execMode) {
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
	reportThroughput(b, batchBlocks)
}

func runCostAwareEvictionChurn(b *testing.B, mode execMode) {
	ctx := benchContext()
	const (
		approxKeyCapacity = 2048
		keySpace          = approxKeyCapacity * 4
		batchBlocks       = 64
		numBatches        = keySpace / batchBlocks
		numPods           = 16
	)

	// Each single-pod key costs ~140 bytes in CostPodCache.CalculateByteSize, so 256KiB
	// holds ~1,800-2,048 keys before Ristretto evicts entries on subsequent writes.
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
	reportThroughput(b, batchBlocks)
}

func runBatchChurnLoop(b *testing.B, ctx context.Context, idx kvblock.Index, batches []writeBatch, startIdx int, mode execMode) {
	numBatches := len(batches)
	switch mode {
	case modeSequential:
		for i := range b.N {
			wb := &batches[(startIdx+i)%numBatches]
			if err := idx.Add(ctx, wb.engineKeys, wb.requestKeys, wb.entries); err != nil {
				b.Fatal(err)
			}
		}
	case modeParallel:
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
	impls := []producerImpl{producerImplPrecise, producerImplApproximate}
	ops := []producerLifecycleOp{producerOpProduceOnly, producerOpProduceAndPreRequest}

	for _, impl := range impls {
		for _, op := range ops {
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

	switch mode {
	case modeSequential:
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

	case modeParallel:
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

// mooncakeExpandFactor, mooncakeDupFactor, mooncakeWorkers, and
// mooncakeWorkerGPUBlocks match dynamo mooncake_bench's defaults:
// (trace_block_size=512 / block_size=128) * trace_length_factor=4 = 16 blocks
// per hash_id, trace_duplication_factor=20, num_unique_inference_workers=128,
// and num_gpu_blocks=16384 per worker.
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
	if f, err := os.Open(tracePath); err == nil {
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
		// Hermetic fallback when mooncake_trace.jsonl is not present on disk.
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
			// Cumulative sequence hashing (SequenceHashMode::Cumulative in dynamo router.rs).
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
				sessionHash = mixMooncakeHash(sessionHash ^ uint64(recIdx+1))
			}
			w := int(mixMooncakeHash((dup<<32)|sessionHash) % mooncakeWorkers)
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
	avgReq := max(1, totalRequestBlocks/numTurns)
	// INDEXER_BENCH.md denominator: total_block_ops = request_blocks + stored_blocks + removed_blocks.
	avgMixed := max(1, (totalRequestBlocks+totalStoredBlocks+totalRemovedBlocks)/numTurns)
	return &mooncakeArtifact{
		turns:            turns,
		avgRequestBlocks: avgReq,
		avgMixedBlocks:   avgMixed,
	}
}

// BenchmarkMooncakeReplay replays the Mooncake arxiv trace workload across 128
// inference workers (16,384 GPU blocks/worker, cumulative sequence hashes,
// dup=20, length-factor=4) under QueryOnly and MixedOverload using the
// INDEXER_BENCH.md block denominator.
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

	// Pre-warm the index with the resident Mooncake working set across the 128 workers.
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
