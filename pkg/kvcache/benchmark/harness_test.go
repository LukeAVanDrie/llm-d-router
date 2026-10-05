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
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/approximateprefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/preciseprefixcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/pkg/kvevents"
	"github.com/llm-d/llm-d-router/test/utils"
)

func init() {
	log.SetLogger(logr.Discard())
}

const (
	defaultBlockSizeTokens = 64
	benchModelName         = "bench-model"
)

// topologyType defines the prefix-sharing pattern across pods in the fleet.
type topologyType string

const (
	// TopologyHotspot places 100% of prompt blocks on every pod in the fleet.
	TopologyHotspot topologyType = "Hotspot"
	// TopologySystemPromptBranch shares the first 25% of blocks across all pods
	// and assigns each pod a distinct 75% suffix.
	TopologySystemPromptBranch topologyType = "SystemPromptBranch"
	// TopologyDisjoint assigns each pod a distinct prefix sequence.
	TopologyDisjoint topologyType = "Disjoint"
)

// execMode selects single-goroutine or RunParallel execution.
type execMode string

const (
	modeSequential execMode = "Sequential"
	modeParallel   execMode = "Parallel"
)

var (
	benchTopologies = []topologyType{
		TopologyHotspot,
		TopologySystemPromptBranch,
		TopologyDisjoint,
	}
	benchModes = []execMode{
		modeSequential,
		modeParallel,
	}
	benchBlockLengths    = []int{64, 512, 3750}
	producerBlockLengths = []int{64, 512}
	writeBatchSizes      = []int{16, 64}
)

// fleetShape describes the number of endpoints and tensor/data-parallel ranks per endpoint.
type fleetShape struct {
	pods  int
	ranks int
}

var (
	benchFleetShapes = []fleetShape{
		{pods: 16, ranks: 1},
		{pods: 96, ranks: 1},
		{pods: 40, ranks: 8},
	}
	singleRankFleetShapes = []fleetShape{
		{pods: 16, ranks: 1},
		{pods: 96, ranks: 1},
	}
)

func (f fleetShape) name() string {
	if f.ranks > 1 {
		return fmt.Sprintf("%dx%d", f.pods, f.ranks)
	}
	return fmt.Sprintf("%d", f.pods)
}

func (f fleetShape) totalEntries() int {
	if f.ranks > 1 {
		return f.pods * f.ranks
	}
	return f.pods
}

func podAddress(podIdx int) string {
	return fmt.Sprintf("10.0.%d.%d", podIdx/256, podIdx%256)
}

func podIdentifier(podIdx int) string {
	return fmt.Sprintf("%s:8000", podAddress(podIdx))
}

func (f fleetShape) podEntriesForPod(podIdx int) []kvblock.PodEntry {
	id := podIdentifier(podIdx)
	if f.ranks <= 1 {
		return []kvblock.PodEntry{{
			PodIdentifier: id,
			DeviceTier:    "gpu",
		}}
	}
	entries := make([]kvblock.PodEntry, f.ranks)
	for r := range f.ranks {
		entries[r] = kvblock.PodEntry{
			PodIdentifier: id,
			DeviceTier:    "gpu",
			HasGroup:      true,
			GroupIdx:      kvblock.GroupID(r),
		}
	}
	return entries
}

func (f fleetShape) allPodEntries() []kvblock.PodEntry {
	entries := make([]kvblock.PodEntry, 0, f.totalEntries())
	for p := range f.pods {
		entries = append(entries, f.podEntriesForPod(p)...)
	}
	return entries
}

func benchContext() context.Context {
	return log.IntoContext(context.Background(), logr.Discard())
}

// reportThroughput computes and reports block_ops/s and reqs/s for a benchmark run.
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

// readFixture holds pre-populated index and query slices for read and mixed benchmarks.
type readFixture struct {
	indexer             *kvcache.Indexer
	index               kvblock.Index
	queries             [][]kvblock.BlockHash
	tokenQueries        [][]uint32
	expectedMatchedPods int
}

// setupReadFixture populates an InMemoryIndex behind the production decorator chain
// according to topology, numBlocks, and fleet.
func setupReadFixture(
	tb testing.TB,
	hashAlgorithm string,
	topology topologyType,
	numBlocks int,
	fleet fleetShape,
) *readFixture {
	tb.Helper()
	ctx := benchContext()

	tp, err := kvblock.NewChunkedTokenDatabase(&kvblock.TokenProcessorConfig{
		BlockSizeTokens: defaultBlockSizeTokens,
		HashAlgorithm:   hashAlgorithm,
	})
	if err != nil {
		tb.Fatal(err)
	}

	cfg, err := kvcache.NewDefaultConfig()
	if err != nil {
		tb.Fatal(err)
	}
	cfg.KVBlockIndexConfig.InMemoryConfig = &kvblock.InMemoryIndexConfig{
		Size:         max(1<<20, numBlocks*fleet.pods*2),
		PodCacheSize: max(128, fleet.totalEntries()*2),
	}
	cfg.KVBlockIndexConfig.EnableMetrics = true

	indexer, err := kvcache.NewKVCacheIndexer(ctx, cfg, tp)
	if err != nil {
		tb.Fatal(err)
	}
	idx := indexer.KVBlockIndex()
	if _, ok := idx.(kvblock.KeyWalker); !ok {
		tb.Fatal("decorator chain does not preserve KeyWalker")
	}

	numTokens := numBlocks * defaultBlockSizeTokens
	numTokenBranches := min(fleet.pods, 8)
	allEntries := fleet.allPodEntries()

	var queries [][]kvblock.BlockHash
	var tokenQueries [][]uint32
	var expectedMatchedPods int

	switch topology {
	case TopologyHotspot:
		tokens := makeTokenSequence(numTokens, 1)
		keys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, benchModelName, nil)
		if err != nil {
			tb.Fatal(err)
		}
		if err := idx.Add(ctx, nil, keys, allEntries); err != nil {
			tb.Fatal(err)
		}
		queries = [][]kvblock.BlockHash{keys}
		tokenQueries = [][]uint32{tokens}
		expectedMatchedPods = fleet.pods

	case TopologySystemPromptBranch:
		sharedBlocks := max(1, numBlocks/4)
		sharedTokensCount := sharedBlocks * defaultBlockSizeTokens
		sharedTokens := makeTokenSequence(sharedTokensCount, 1)

		queries = make([][]kvblock.BlockHash, fleet.pods)
		tokenQueries = make([][]uint32, numTokenBranches)
		for br := range numTokenBranches {
			tokens := make([]uint32, numTokens)
			copy(tokens[:sharedTokensCount], sharedTokens)
			fillTokenSequence(tokens[sharedTokensCount:], uint32((br+1)*100_000))
			keys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, benchModelName, nil)
			if err != nil {
				tb.Fatal(err)
			}
			if br == 0 {
				if err := idx.Add(ctx, nil, keys[:sharedBlocks], allEntries); err != nil {
					tb.Fatal(err)
				}
			}
			if sharedBlocks < len(keys) {
				if err := idx.Add(ctx, nil, keys[sharedBlocks:], fleet.podEntriesForPod(br)); err != nil {
					tb.Fatal(err)
				}
			}
			queries[br] = keys
			tokenQueries[br] = tokens
		}
		for p := numTokenBranches; p < fleet.pods; p++ {
			keys := make([]kvblock.BlockHash, numBlocks)
			copy(keys[:sharedBlocks], queries[0][:sharedBlocks])
			base := uint64(p+1) << 40
			for i := sharedBlocks; i < numBlocks; i++ {
				keys[i] = kvblock.BlockHash(base | uint64(i+1))
			}
			if sharedBlocks < numBlocks {
				if err := idx.Add(ctx, nil, keys[sharedBlocks:], fleet.podEntriesForPod(p)); err != nil {
					tb.Fatal(err)
				}
			}
			queries[p] = keys
		}
		expectedMatchedPods = fleet.pods

	case TopologyDisjoint:
		queries = make([][]kvblock.BlockHash, fleet.pods)
		tokenQueries = make([][]uint32, numTokenBranches)
		for br := range numTokenBranches {
			tokens := makeTokenSequence(numTokens, uint32((br+1)*100_000))
			keys, err := tp.TokensToKVBlockKeys(kvblock.EmptyBlockHash, tokens, benchModelName, nil)
			if err != nil {
				tb.Fatal(err)
			}
			if err := idx.Add(ctx, nil, keys, fleet.podEntriesForPod(br)); err != nil {
				tb.Fatal(err)
			}
			queries[br] = keys
			tokenQueries[br] = tokens
		}
		for p := numTokenBranches; p < fleet.pods; p++ {
			keys := make([]kvblock.BlockHash, numBlocks)
			base := uint64(p+1) << 40
			for i := range keys {
				keys[i] = kvblock.BlockHash(base | uint64(i+1))
			}
			if err := idx.Add(ctx, nil, keys, fleet.podEntriesForPod(p)); err != nil {
				tb.Fatal(err)
			}
			queries[p] = keys
		}
		expectedMatchedPods = 1

	default:
		tb.Fatalf("unknown topology %q", topology)
	}

	for _, checkIdx := range []int{0, len(queries) - 1} {
		matches, err := indexer.MatchBlockKeys(ctx, queries[checkIdx], nil)
		if err != nil {
			tb.Fatal(err)
		}
		if len(matches) != expectedMatchedPods {
			tb.Fatalf("topology %s branch %d matched %d pods, want %d", topology, checkIdx, len(matches), expectedMatchedPods)
		}
	}

	return &readFixture{
		indexer:             indexer,
		index:               idx,
		queries:             queries,
		tokenQueries:        tokenQueries,
		expectedMatchedPods: expectedMatchedPods,
	}
}

func makeTokenSequence(numTokens int, seedOffset uint32) []uint32 {
	tokens := make([]uint32, numTokens)
	fillTokenSequence(tokens, seedOffset)
	return tokens
}

func fillTokenSequence(tokens []uint32, seedOffset uint32) {
	for i := range tokens {
		tokens[i] = seedOffset + uint32(i%50000+1)
	}
}

// writeBatch holds pre-allocated engine keys, request keys, and pod entries for one write/evict op.
type writeBatch struct {
	engineKeys  []kvblock.BlockHash
	requestKeys []kvblock.BlockHash
	entries     []kvblock.PodEntry
}

// buildWriteBatches pre-allocates count writeBatch fixtures of batchBlocks keys across numPods pods.
func buildWriteBatches(count, batchBlocks, numPods int, keyBaseOffset uint64) []writeBatch {
	batches := make([]writeBatch, count)
	for i := range count {
		eng := make([]kvblock.BlockHash, batchBlocks)
		req := make([]kvblock.BlockHash, batchBlocks)
		base := keyBaseOffset + uint64(i)*uint64(batchBlocks) + 1
		for j := range batchBlocks {
			h := kvblock.BlockHash(base + uint64(j))
			eng[j] = h
			req[j] = h
		}
		podIdx := i % numPods
		batches[i] = writeBatch{
			engineKeys:  eng,
			requestKeys: req,
			entries: []kvblock.PodEntry{{
				PodIdentifier: podIdentifier(podIdx),
				DeviceTier:    "gpu",
			}},
		}
	}
	return batches
}

// producerWorkerFixture holds pre-allocated per-worker request and endpoint wrappers
// so concurrent Produce / PreRequest calls do not collide on RequestID in PluginState
// or contend on a single request's Endpoint AttributeMap.
type producerWorkerFixture struct {
	req         *scheduling.InferenceRequest
	endpoints   []scheduling.Endpoint
	schedResult *scheduling.SchedulingResult
}

// producerHarness wraps either a preciseprefixcache.Producer or an approximateprefix DataProducer.
type producerHarness struct {
	producer   requestcontrol.DataProducer
	preRequest requestcontrol.PreRequest
	workers    []producerWorkerFixture
}

func buildProducerWorkers(numWorkers, numPods int, tokens []uint32) ([]*datalayer.EndpointMetadata, []*datalayer.Metrics, []producerWorkerFixture) {
	metas := make([]*datalayer.EndpointMetadata, numPods)
	metricsList := make([]*datalayer.Metrics, numPods)
	for p := range numPods {
		metas[p] = &datalayer.EndpointMetadata{
			ID:      types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("pod-%d", p)},
			Address: podAddress(p),
			Port:    "8000",
		}
		m := datalayer.NewMetrics()
		m.CacheBlockSize = defaultBlockSizeTokens
		m.CacheNumBlocks = 31250
		metricsList[p] = m
	}

	body := &requesthandling.InferenceRequestBody{
		TokenizedRequest: &requesthandling.TokenizedRequest{
			Prompts: []requesthandling.PromptTokens{{TokenIDs: tokens}},
		},
	}

	workers := make([]producerWorkerFixture, numWorkers)
	for w := range numWorkers {
		eps := make([]scheduling.Endpoint, numPods)
		for p := range numPods {
			eps[p] = scheduling.NewEndpoint(metas[p], metricsList[p], datalayer.NewAttributes())
		}
		targetEp := eps[w%numPods]
		workers[w] = producerWorkerFixture{
			req: &scheduling.InferenceRequest{
				RequestID:   fmt.Sprintf("bench-req-%d", w),
				TargetModel: benchModelName,
				Body:        body,
			},
			endpoints: eps,
			schedResult: &scheduling.SchedulingResult{
				PrimaryProfileName: "default",
				ProfileResults: map[string]*scheduling.ProfileRunResult{
					"default": {TargetEndpoints: []scheduling.Endpoint{targetEp}},
				},
			},
		}
	}
	return metas, metricsList, workers
}

func warmProducerHarness(
	tb testing.TB,
	ctx context.Context,
	h *producerHarness,
	producerName string,
	metas []*datalayer.EndpointMetadata,
	metricsList []*datalayer.Metrics,
	tokens []uint32,
	numBlocks int,
) {
	tb.Helper()
	numPods := len(metas)
	eps := make([]scheduling.Endpoint, numPods)
	for p := range numPods {
		eps[p] = scheduling.NewEndpoint(metas[p], metricsList[p], datalayer.NewAttributes())
	}
	body := &requesthandling.InferenceRequestBody{
		TokenizedRequest: &requesthandling.TokenizedRequest{
			Prompts: []requesthandling.PromptTokens{{TokenIDs: tokens}},
		},
	}
	for p := range numPods {
		req := &scheduling.InferenceRequest{
			RequestID:   fmt.Sprintf("warmup-req-%d", p),
			TargetModel: benchModelName,
			Body:        body,
		}
		if err := h.producer.Produce(ctx, req, eps); err != nil {
			tb.Fatal(err)
		}
		res := &scheduling.SchedulingResult{
			PrimaryProfileName: "default",
			ProfileResults: map[string]*scheduling.ProfileRunResult{
				"default": {TargetEndpoints: []scheduling.Endpoint{eps[p]}},
			},
		}
		if err := h.preRequest.PreRequest(ctx, req, res); err != nil {
			tb.Fatal(err)
		}
	}

	// Verify all endpoints report full prefix matches, waiting for any asynchronous
	// PreRequest updates to land before timing begins.
	dk := attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(producerName)
	verifyReq := &scheduling.InferenceRequest{
		RequestID:   "warmup-verify",
		TargetModel: benchModelName,
		Body:        body,
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := h.producer.Produce(ctx, verifyReq, eps); err != nil {
			tb.Fatal(err)
		}
		allWarm := true
		for p := range numPods {
			raw, ok := eps[p].Get(dk)
			if !ok {
				allWarm = false
				break
			}
			info, ok := raw.(*attrprefix.PrefixCacheMatchInfo)
			if !ok || info.MatchBlocks() != numBlocks {
				allWarm = false
				break
			}
		}
		if allWarm {
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("producer %s failed to warm all %d endpoints to %d blocks within timeout", producerName, numPods, numBlocks)
		}
		runtime.Gosched()
	}
}

func setupPreciseProducerHarness(b *testing.B, numPods, numBlocks int) *producerHarness {
	b.Helper()
	ctx, cancel := context.WithCancel(benchContext())
	b.Cleanup(cancel)

	idxCfg, err := kvcache.NewDefaultConfig()
	if err != nil {
		b.Fatal(err)
	}
	idxCfg.KVBlockIndexConfig.InMemoryConfig = &kvblock.InMemoryIndexConfig{
		Size:         1 << 20,
		PodCacheSize: max(128, numPods*2),
	}

	kvEventsCfg := kvevents.DefaultConfig()
	kvEventsCfg.DiscoverPods = false
	kvEventsCfg.ZMQEndpoint = ""

	const producerName = "bench-precise"
	producer, err := preciseprefixcache.New(ctx, producerName, preciseprefixcache.PluginConfig{
		TokenProcessorConfig: &kvblock.TokenProcessorConfig{
			BlockSizeTokens: defaultBlockSizeTokens,
			HashAlgorithm:   kvblock.HashAlgorithmXXH64,
		},
		IndexerConfig:       idxCfg,
		KVEventsConfig:      kvEventsCfg,
		SpeculativeIndexing: true,
		SpeculativeTTL:      "1h",
	})
	if err != nil {
		b.Fatal(err)
	}

	tokens := makeTokenSequence(numBlocks*defaultBlockSizeTokens, 1)
	numWorkers := max(runtime.GOMAXPROCS(0)*4, 16)
	metas, metricsList, workers := buildProducerWorkers(numWorkers, numPods, tokens)

	h := &producerHarness{
		producer:   producer,
		preRequest: producer,
		workers:    workers,
	}
	warmProducerHarness(b, ctx, h, producerName, metas, metricsList, tokens, numBlocks)
	return h
}

func setupApproxProducerHarness(b *testing.B, numPods, numBlocks int) *producerHarness {
	b.Helper()
	ctx, cancel := context.WithCancel(benchContext())
	b.Cleanup(cancel)

	handle := utils.NewTestHandle(ctx)
	const producerName = "bench-approx"
	rawCfg := fmt.Sprintf(`{"autoTune":false,"blockSizeTokens":%d,"maxPrefixTokensToMatch":0,"maxPrefixBlocksToMatch":0,"lruCapacityPerServer":31250}`, defaultBlockSizeTokens)
	pl, err := approximateprefix.ApproxPrefixCacheFactory(producerName, plugin.StrictDecoder([]byte(rawCfg)), handle)
	if err != nil {
		b.Fatal(err)
	}

	dp, ok := pl.(requestcontrol.DataProducer)
	if !ok {
		b.Fatalf("plugin %T does not implement DataProducer", pl)
	}
	pr, ok := pl.(requestcontrol.PreRequest)
	if !ok {
		b.Fatalf("plugin %T does not implement PreRequest", pl)
	}

	tokens := makeTokenSequence(numBlocks*defaultBlockSizeTokens, 1)
	numWorkers := max(runtime.GOMAXPROCS(0)*4, 16)
	metas, metricsList, workers := buildProducerWorkers(numWorkers, numPods, tokens)

	h := &producerHarness{
		producer:   dp,
		preRequest: pr,
		workers:    workers,
	}
	warmProducerHarness(b, ctx, h, producerName, metas, metricsList, tokens, numBlocks)
	return h
}
