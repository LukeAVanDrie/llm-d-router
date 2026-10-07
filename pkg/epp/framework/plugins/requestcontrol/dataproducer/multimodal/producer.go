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

package multimodal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/collections"
	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmm "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/multimodal"
	tokenproducer "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
)

const (
	// ProducerType is the type name used to register the multimodal data producer.
	ProducerType = "mm-embeddings-cache-producer"

	podCleanupInterval = 2 * time.Minute

	// defaultCacheSizeInMB is 4 GiB (4096 MiB).
	defaultCacheSizeInMB = 4096

	// bytesPerImage is the assumed memory per tracked image.
	bytesPerImage = 2 * 1024 * 1024

	minMMFastSize    = 1 << 10
	maxMMFastSize    = 1 << 16
	podFastSize      = 1024
	shardCompactMin  = 16384
	inlineItemSlots  = 8
	subsetCacheSlots = 16
	maxDebugDumpPods = 100
)

var (
	// ProducedKey is the data key emitted by this producer.
	ProducedKey = attrmm.EncoderCacheMatchInfoKey

	_ requestcontrol.DataProducer = &Producer{}
	_ requestcontrol.PreRequest   = &Producer{}
	_ fwkdl.EndpointExtractor     = &Producer{}
	_ plugin.StateDumper          = &Producer{}
)

// Parameters configures the multimodal encoder-cache data producer.
type Parameters struct {
	// CacheSizeInMBPerServer is the per-endpoint LRU memory budget in mebibytes (MiB).
	CacheSizeInMBPerServer int `json:"cacheSizeInMBPerServer"`
}

// lruCapacityFromCacheSizeMB converts a MiB budget to a maximum LRU entry count.
func lruCapacityFromCacheSizeMB(mb int) int {
	if mb <= 0 {
		mb = defaultCacheSizeInMB
	}
	n := (int64(mb) * 1024 * 1024) / bytesPerImage
	if n < 1 {
		return 1
	}
	if n > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(n)
}

// Factory creates a multimodal encoder-cache data producer.
func Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	parameters := Parameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&parameters); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' plugin - %w", ProducerType, err)
		}
	}

	return New(handle.Context(), name, &parameters, handle.PodList)
}

// podState holds the per-pod mutex, interned bitset ordinal, and bounded LRU.
type podState struct {
	mu         sync.Mutex
	id         k8stypes.NamespacedName
	podStr     string
	ord        int
	lru        *simplelru.LRU[string, *collections.BitsetEntry[string]]
	count      atomic.Int32
	removed    bool
	gone       atomic.Bool
	pluginType string
	pluginName string
	imageHits  prometheus.Counter
}

func (ps *podState) Len() int {
	return int(ps.count.Load())
}

func (ps *podState) incHits(modality string) {
	if modality == string(fwkrh.ModalityImage) {
		ps.imageHits.Inc()
		return
	}
	encoderCacheHitsTotal.WithLabelValues(ps.pluginType, ps.pluginName, ps.podStr, modality).Inc()
}

// Producer tracks multimodal content hashes and the pods that likely hold their
// encoder-cache entries. Each pod has its own LRU cache of hashes, while a
// sharded inverted index maps each content hash to the bitset of pod ordinals
// currently caching it.
type Producer struct {
	typedName        plugin.TypedName
	dk               plugin.DataKey
	cacheSize        int
	pluginState      *plugin.PluginState
	podList          func() []k8stypes.NamespacedName
	podMu            sync.RWMutex
	podToLRU         map[k8stypes.NamespacedName]*podState
	pods             *collections.Interner[*podState]
	podFast          [podFastSize]atomic.Pointer[podState]
	index            *collections.ShardedBitsetIndex[string]
	imageQueries     prometheus.Counter
	hitRatioObserver prometheus.Observer
	wg               sync.WaitGroup
}

type requestState struct {
	items []attrmm.MatchItem
}

func (s *requestState) Clone() plugin.StateData {
	if s == nil {
		return nil
	}
	return &requestState{items: attrmm.CloneMatchItems(s.items)}
}

// New creates a Producer.
func New(ctx context.Context, name string, params *Parameters, podList func() []k8stypes.NamespacedName) (*Producer, error) {
	cacheSizeMB := 0
	if params != nil {
		cacheSizeMB = params.CacheSizeInMBPerServer
	}
	cacheSize := lruCapacityFromCacheSizeMB(cacheSizeMB)

	registerEncoderCacheMetrics()

	fastSize := min(max(minMMFastSize, cacheSize*16), maxMMFastSize)
	compactInit := max(shardCompactMin, cacheSize*2)
	p := &Producer{
		typedName:        plugin.TypedName{Type: ProducerType, Name: name},
		dk:               attrmm.EncoderCacheMatchInfoKey.WithNonEmptyProducerName(name),
		cacheSize:        cacheSize,
		pluginState:      plugin.NewPluginState(ctx),
		podList:          podList,
		podToLRU:         make(map[k8stypes.NamespacedName]*podState),
		pods:             collections.NewInterner[*podState](),
		index:            collections.NewShardedBitsetIndex(fastSize, compactInit, hashString),
		imageQueries:     encoderCacheQueriesTotal.WithLabelValues(ProducerType, name, string(fwkrh.ModalityImage)),
		hitRatioObserver: encoderCacheHitRatio.WithLabelValues(ProducerType, name),
	}
	if podList != nil {
		go p.cleanupLoop(ctx)
	}
	return p, nil
}

func hashString(s string) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 0x100000001b3
	}
	return h ^ (h >> 32)
}

func hashNamespacedName(id k8stypes.NamespacedName) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < len(id.Namespace); i++ {
		h = (h ^ uint64(id.Namespace[i])) * 0x100000001b3
	}
	h = (h ^ uint64('/')) * 0x100000001b3
	for i := 0; i < len(id.Name); i++ {
		h = (h ^ uint64(id.Name[i])) * 0x100000001b3
	}
	return h ^ (h >> 32)
}

func (p *Producer) lookupPodState(id k8stypes.NamespacedName) *podState {
	fIdx := hashNamespacedName(id) & (podFastSize - 1)
	if ps := p.podFast[fIdx].Load(); ps != nil && ps.id == id && !ps.gone.Load() {
		return ps
	}
	p.podMu.RLock()
	ps := p.podToLRU[id]
	if ps != nil && !ps.gone.Load() {
		p.podFast[fIdx].CompareAndSwap(nil, ps)
		p.podMu.RUnlock()
		return ps
	}
	p.podMu.RUnlock()
	return nil
}

func (p *Producer) getOrCreatePodState(id k8stypes.NamespacedName) *podState {
	if ps := p.lookupPodState(id); ps != nil {
		return ps
	}

	fIdx := hashNamespacedName(id) & (podFastSize - 1)
	p.podMu.Lock()
	ps := p.podToLRU[id]
	if ps != nil && !ps.removed {
		p.podFast[fIdx].Store(ps)
		p.podMu.Unlock()
		return ps
	}

	podStr := id.String()
	ps = &podState{
		id:         id,
		podStr:     podStr,
		pluginType: p.typedName.Type,
		pluginName: p.typedName.Name,
		imageHits:  encoderCacheHitsTotal.WithLabelValues(p.typedName.Type, p.typedName.Name, podStr, string(fwkrh.ModalityImage)),
	}
	ord, _ := p.pods.Intern(ps)
	ps.ord = ord
	ps.lru, _ = simplelru.NewLRU(p.cacheSize, func(_ string, e *collections.BitsetEntry[string]) {
		e.Clear(ord)
	})
	p.podToLRU[id] = ps
	p.podFast[fIdx].Store(ps)
	p.podMu.Unlock()
	return ps
}

func (p *Producer) addItemsToPod(pod k8stypes.NamespacedName, items []attrmm.MatchItem) {
	if len(items) == 0 {
		return
	}
	ps := p.getOrCreatePodState(pod)
	ps.mu.Lock()
	if ps.removed {
		ps.mu.Unlock()
		return
	}
	for _, item := range items {
		if _, ok := ps.lru.Get(item.Hash); ok {
			continue
		}
		entry := p.index.SetBit(item.Hash, ps.ord)
		ps.lru.Add(item.Hash, entry)
	}
	ps.count.Store(int32(ps.lru.Len()))
	ps.mu.Unlock()
}

func (p *Producer) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(podCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.removeStalePods()
		}
	}
}

// TypedName returns the plugin type/name.
func (p *Producer) TypedName() plugin.TypedName {
	return p.typedName
}

// encoderCacheState is the snapshot returned by DumpState: the known pods from
// the datalayer and per-pod cached-item counts. The multimodal content hashes
// (the cache keys) are not exposed. Both lists are capped at MaxPods (PodList by
// name, Pods by item count) and a list is partial when its matching total
// exceeds MaxPods. PodList is sampled just before the per-pod view rather than
// atomically with it, so the snapshot is best-effort: the two need not be
// perfectly consistent, and a freshly tracked pod can appear in Pods but not yet
// in PodList.
type encoderCacheState struct {
	PodList        []string       `json:"podList"`
	TotalKnownPods int            `json:"totalKnownPods"`
	Pods           []podItemCount `json:"pods"`
	TotalPods      int            `json:"totalPods"`
	MaxPods        int            `json:"maxPods"`
}

type podItemCount struct {
	Pod   string `json:"pod"`
	Items int    `json:"items"`
}

// DumpState reports the known pods from the datalayer and how many encoder-cache
// items are tracked per pod, ordered by count (most first) and capped to
// maxDebugDumpPods so the payload stays bounded. Pod identities and counts are
// exposed; the content hashes (cache keys) are not.
func (p *Producer) DumpState() (json.RawMessage, error) {
	// podList() is the datalayer accessor; call it outside the cache lock, as
	// removeStalePods does.
	podList := []string{}
	var totalKnownPods int
	if p.podList != nil {
		known := p.podList()
		totalKnownPods = len(known)
		podList = make([]string, 0, len(known))
		for _, nn := range known {
			podList = append(podList, nn.String())
		}
		sort.Strings(podList)
		if len(podList) > maxDebugDumpPods {
			podList = podList[:maxDebugDumpPods]
		}
	}

	p.podMu.RLock()
	state := encoderCacheState{
		PodList:        podList,
		TotalKnownPods: totalKnownPods,
		MaxPods:        maxDebugDumpPods,
		TotalPods:      len(p.podToLRU),
	}
	pods := make([]podItemCount, 0, len(p.podToLRU))
	for _, ps := range p.podToLRU {
		pods = append(pods, podItemCount{Pod: ps.podStr, Items: ps.Len()})
	}
	p.podMu.RUnlock()

	sort.SliceStable(pods, func(a, b int) bool {
		if pods[a].Items != pods[b].Items {
			return pods[a].Items > pods[b].Items
		}
		return pods[a].Pod < pods[b].Pod
	})
	if len(pods) > maxDebugDumpPods {
		pods = pods[:maxDebugDumpPods]
	}
	state.Pods = pods
	return json.Marshal(state)
}

// Produces returns the data keys this plugin produces.
func (p *Producer) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{p.dk: attrmm.EncoderCacheMatchInfo{}}
}

// Consumes declares the TokenizedRequest dependency so the data-layer DAG orders
// the token-producer before this producer runs and auto-creates one when none
// is configured; multimodal features come from the tokenizer output.
func (p *Producer) Consumes() plugin.DataDependencies {
	return plugin.DataDependencies{
		Required: map[plugin.DataKey]any{tokenproducer.TokenizedPromptDataKey: scheduling.TokenizedRequest{}},
	}
}

// PluginState returns request-scoped state shared between producer extension points.
func (p *Producer) PluginState() *plugin.PluginState {
	return p.pluginState
}

// Produce attaches multimodal encoder-cache match data to endpoints.
func (p *Producer) Produce(ctx context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) error {
	logger := log.FromContext(ctx).V(logging.DEBUG)
	requestItems := ExtractMMItems(request)
	if len(requestItems) == 0 {
		logger.Info("No multimodal content found, skipping encoder-cache match data")
		return nil
	}

	var stackItemPods [inlineItemSlots]collections.Bitset
	itemPods, anyHit := p.resolveAndRecordItemLookups(requestItems, stackItemPods[:0])

	if request != nil && request.RequestID != "" {
		p.pluginState.Write(request.RequestID, plugin.StateKey(ProducerType), &requestState{items: requestItems})
	}
	if len(endpoints) == 0 {
		return nil
	}

	totalItems := len(requestItems)
	if !anyHit {
		missInfo := attrmm.NewEncoderCacheMatchInfo(nil, requestItems)
		for _, endpoint := range endpoints {
			if endpoint.GetMetadata() == nil {
				continue
			}
			p.recordHitRatio(0, totalItems)
			endpoint.Put(p.dk, missInfo)
		}
		return nil
	}

	var (
		stackMatched [inlineItemSlots]attrmm.MatchItem
		matchedBuf   []attrmm.MatchItem
		subsetKeys   [subsetCacheSlots]uint64
		subsetInfos  [subsetCacheSlots]*attrmm.EncoderCacheMatchInfo
		missInfo     *attrmm.EncoderCacheMatchInfo
	)
	if totalItems <= inlineItemSlots {
		matchedBuf = stackMatched[:0]
	} else {
		matchedBuf = make([]attrmm.MatchItem, 0, totalItems)
	}

	for _, endpoint := range endpoints {
		metadata := endpoint.GetMetadata()
		if metadata == nil {
			continue
		}
		ps := p.lookupPodState(metadata.ID)
		if ps == nil {
			p.recordHitRatio(0, totalItems)
			if missInfo == nil {
				missInfo = attrmm.NewEncoderCacheMatchInfo(nil, requestItems)
			}
			endpoint.Put(p.dk, missInfo)
			continue
		}

		matchedBuf = matchedBuf[:0]
		var subsetMask uint64
		ord := ps.ord
		for idx := range requestItems {
			if itemPods[idx].Has(ord) {
				matchedBuf = append(matchedBuf, requestItems[idx])
				if idx < 64 {
					subsetMask |= uint64(1) << uint(idx)
				}
			}
		}

		nMatched := len(matchedBuf)
		p.recordHitRatio(nMatched, totalItems)
		if nMatched == 0 {
			if missInfo == nil {
				missInfo = attrmm.NewEncoderCacheMatchInfo(nil, requestItems)
			}
			endpoint.Put(p.dk, missInfo)
			continue
		}

		var info *attrmm.EncoderCacheMatchInfo
		if totalItems <= 64 {
			slot := (subsetMask * 0x9e3779b97f4a7c15) >> (64 - 4)
			if subsetKeys[slot] == subsetMask && subsetInfos[slot] != nil {
				info = subsetInfos[slot]
			} else {
				if nMatched == totalItems {
					info = attrmm.NewEncoderCacheMatchInfo(requestItems, requestItems)
				} else {
					info = attrmm.NewEncoderCacheMatchInfo(matchedBuf, requestItems)
				}
				subsetKeys[slot] = subsetMask
				subsetInfos[slot] = info
			}
		} else {
			info = attrmm.NewEncoderCacheMatchInfo(matchedBuf, requestItems)
		}
		endpoint.Put(p.dk, info)
	}

	return nil
}

// ExtractMMItems returns deterministic, unique multimodal encoder-cache items
// derived from the tokenized prompt's multimodal features.
func ExtractMMItems(request *scheduling.InferenceRequest) []attrmm.MatchItem {
	if request == nil || request.Body == nil || request.Body.TokenizedRequest == nil {
		return nil
	}

	prompts := request.Body.TokenizedRequest.Prompts
	totalFeatures := 0
	for i := range prompts {
		totalFeatures += len(prompts[i].MultiModalFeatures)
	}
	if totalFeatures == 0 {
		return nil
	}

	var items []attrmm.MatchItem
	var indexByHash map[string]int
	for i := range prompts {
		for _, feature := range prompts[i].MultiModalFeatures {
			if feature.Hash == "" {
				continue
			}
			item := attrmm.MatchItem{
				Hash:     feature.Hash,
				Size:     1,
				Modality: string(feature.Modality),
			}
			if items == nil {
				items = make([]attrmm.MatchItem, 0, totalFeatures)
				items = append(items, item)
				continue
			}
			if indexByHash != nil {
				if idx, exists := indexByHash[feature.Hash]; exists {
					items[idx] = item
					continue
				}
				indexByHash[feature.Hash] = len(items)
				items = append(items, item)
				continue
			}
			dupIdx := -1
			for idx := range items {
				if items[idx].Hash == feature.Hash {
					dupIdx = idx
					break
				}
			}
			if dupIdx >= 0 {
				items[dupIdx] = item
				continue
			}
			if len(items) == 16 {
				indexByHash = make(map[string]int, totalFeatures)
				for idx := range items {
					indexByHash[items[idx].Hash] = idx
				}
				indexByHash[feature.Hash] = len(items)
			}
			items = append(items, item)
		}
	}
	return items
}

func (p *Producer) incQueries(modality string) {
	if modality == string(fwkrh.ModalityImage) {
		p.imageQueries.Inc()
		return
	}
	encoderCacheQueriesTotal.WithLabelValues(p.typedName.Type, p.typedName.Name, modality).Inc()
}

func (p *Producer) resolveAndRecordItemLookups(items []attrmm.MatchItem, dst []collections.Bitset) ([]collections.Bitset, bool) {
	var itemPods []collections.Bitset
	if len(items) <= cap(dst) {
		itemPods = dst[:len(items)]
	} else {
		itemPods = make([]collections.Bitset, len(items))
	}

	anyHit := false
	for idx, item := range items {
		p.incQueries(item.Modality)
		e := p.index.Lookup(item.Hash)
		if e == nil {
			continue
		}
		bs := e.Snapshot()
		if bs.IsEmpty() {
			continue
		}
		itemPods[idx] = bs
		anyHit = true
		for ord := range bs.All() {
			if ps := p.pods.At(ord); ps != nil && !ps.gone.Load() {
				ps.incHits(item.Modality)
			}
		}
	}
	return itemPods, anyHit
}

// recordItemLookups increments the queries counter for each item and, for every
// endpoint whose LRU contains the hash, increments that endpoint's hits counter.
func (p *Producer) recordItemLookups(items []attrmm.MatchItem) {
	var stackItemPods [inlineItemSlots]collections.Bitset
	_, _ = p.resolveAndRecordItemLookups(items, stackItemPods[:0])
}

// recordHitRatio observes the fraction of a request's multimodal items that
// matched a single endpoint's LRU. A zero total is not a meaningful ratio and
// is not observed.
func (p *Producer) recordHitRatio(matchedItems, totalItems int) {
	if totalItems == 0 {
		return
	}
	ratio := float64(matchedItems) / float64(totalItems)
	p.hitRatioObserver.Observe(ratio)
}

func (p *Producer) removeStalePods() {
	if p.podList == nil {
		return
	}
	podList := p.podList()
	if len(podList) == 0 {
		return
	}
	validPods := make(map[k8stypes.NamespacedName]struct{}, len(podList))
	for _, pod := range podList {
		validPods[pod] = struct{}{}
	}

	p.podMu.RLock()
	var stale []k8stypes.NamespacedName
	for pod := range p.podToLRU {
		if _, ok := validPods[pod]; !ok {
			stale = append(stale, pod)
		}
	}
	p.podMu.RUnlock()

	for _, pod := range stale {
		p.removePod(pod)
	}
}

// Extract removes deleted endpoints from the best-effort multimodal
// cache-affinity state when endpoint lifecycle events are wired through the data layer.
func (p *Producer) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if event.Type != fwkdl.EventDelete || event.Endpoint == nil {
		return nil
	}
	metadata := event.Endpoint.GetMetadata()
	if metadata == nil || metadata.ID.Name == "" {
		return nil
	}
	p.removePod(metadata.ID)
	log.FromContext(ctx).V(logging.DEBUG).Info("Removed stale pod from multimodal encoder-cache state",
		"pod", metadata.ID.String())
	return nil
}

func (p *Producer) removePod(pod k8stypes.NamespacedName) {
	p.podMu.Lock()
	ps, exists := p.podToLRU[pod]
	if !exists {
		p.podMu.Unlock()
		return
	}
	delete(p.podToLRU, pod)
	p.podFast[hashNamespacedName(pod)&(podFastSize-1)].CompareAndSwap(ps, nil)

	ps.mu.Lock()
	ps.removed = true
	ps.gone.Store(true)
	ps.lru.Purge()
	ps.count.Store(0)
	p.pods.Release(ps)
	ps.mu.Unlock()
	p.podMu.Unlock()
}
