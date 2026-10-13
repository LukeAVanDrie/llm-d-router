/*
Copyright 2025 The Kubernetes Authors.
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
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/common/prefixindex"
)

// ordinalQuarantine is how long a removed pod's index ordinal stays unused,
// so an Add that looked the ordinal up before RemovePod cannot land after the
// ordinal belongs to another pod.
const ordinalQuarantine = time.Minute

// indexer implements the indexerInterface interface over a prefixindex.Index:
// each pod's blocks form one endpoint's ChainSet, and queries probe only the
// candidates holding the prompt's first block.
type indexer struct {
	idx *prefixindex.Index[blockHash]

	// ordinals maps pods to index endpoints. It is immutable and replaced whole
	// under mu, so queries read it without locking.
	ordinals atomic.Pointer[map[ServerID]int]
	mu       sync.Mutex
	next     int
	freed    []freedOrdinal

	defaultLRUSize int
	pluginName     string
	pluginType     string
}

type freedOrdinal struct {
	ordinal int
	at      time.Time
}

// newIndexer initializes an indexer with size limits and starts cache size reporting.
func newIndexer(ctx context.Context, defaultLRUSize int, pluginName, pluginType string) indexerInterface {
	i := &indexer{
		idx:            prefixindex.NewIndex[blockHash](prefixindex.Config{}),
		defaultLRUSize: defaultLRUSize,
		pluginName:     pluginName,
		pluginType:     pluginType,
	}

	go i.reportLRUSize(ctx, time.Second)
	return i
}

func (i *indexer) snapshot() map[ServerID]int {
	if m := i.ordinals.Load(); m != nil {
		return *m
	}
	return nil
}

// ordinalFor returns the pod's endpoint, assigning one sized by the pod's
// GPU block count (or the default) on first use.
func (i *indexer) ordinalFor(pod server) int {
	if ord, ok := i.snapshot()[pod.ServerID]; ok {
		return ord
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	cur := i.snapshot()
	if ord, ok := cur[pod.ServerID]; ok {
		return ord
	}
	ord := i.next
	if len(i.freed) > 0 && time.Since(i.freed[0].at) > ordinalQuarantine {
		ord = i.freed[0].ordinal
		i.freed = i.freed[1:]
		i.idx.ClearEndpoint(ord)
	} else {
		i.next++
	}
	size := pod.NumOfGPUBlocks
	if size <= 0 {
		size = i.defaultLRUSize
	}
	i.idx.SetEndpointCapacity(ord, size)
	next := make(map[ServerID]int, len(cur)+1)
	maps.Copy(next, cur)
	next[pod.ServerID] = ord
	i.ordinals.Store(&next)
	return ord
}

// Add records hashes, a prompt's chain from block 0, as the pod's most
// recent entry. At capacity the pod's least recently added leaves go first,
// so the blocks that remain of any prompt form a contiguous run from block 0.
//
// If RemovePod runs between looking up the pod's endpoint and inserting, the
// insert lands in the cleared endpoint and is discarded when the endpoint is
// reused, as if Add had completed before RemovePod.
func (i *indexer) Add(hashes []blockHash, pod server) {
	i.idx.AddRootChain(i.ordinalFor(pod), hashes)
}

// MatchLongestPrefix returns each candidate's run of leading blocks.
func (i *indexer) MatchLongestPrefix(hashes []blockHash, candidates []ServerID) []int {
	res := make([]int, len(candidates))
	if len(hashes) == 0 {
		return res
	}
	pods := i.snapshot()
	var buf [256]int
	ords := buf[:0]
	if len(candidates) > len(buf) {
		ords = make([]int, 0, len(candidates))
	}
	for _, c := range candidates {
		ord, ok := pods[c]
		if !ok {
			ord = -1
		}
		ords = append(ords, ord)
	}
	return i.idx.MatchInto(hashes, ords, res)
}

// reportLRUSize starts a goroutine that periodically reports the LRU cache size metric.
func (i *indexer) reportLRUSize(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.reportOnce(ctx)
		}
	}
}

func (i *indexer) reportOnce(ctx context.Context) {
	counts := i.PodBlockCounts()

	totalEntries := 0
	maxPodEntries := 0
	var maxPodName ServerID
	for pod, size := range counts {
		totalEntries += size
		if size > maxPodEntries {
			maxPodEntries = size
			maxPodName = pod
		}
	}

	numPods := len(counts)
	avg := 0.0
	if numPods > 0 {
		avg = float64(totalEntries) / float64(numPods)
	}

	recordPrefixCacheSize(i.pluginName, i.pluginType, int64(totalEntries))

	log.FromContext(ctx).V(logutil.TRACE).Info("Prefix cache state",
		"total entries", totalEntries,
		"# pods", numPods,
		"avg entries per pod", avg,
		"pod with max cache", maxPodName,
		"max pod size", maxPodEntries,
		"global max LRU cache capacity per pod", i.defaultLRUSize,
	)
}

// RemovePod removes a pod and its associated entries from the indexer. Its
// endpoint is cleared and reused only after ordinalQuarantine.
func (i *indexer) RemovePod(pod ServerID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	cur := i.snapshot()
	ord, ok := cur[pod]
	if !ok {
		return
	}
	next := maps.Clone(cur)
	delete(next, pod)
	i.ordinals.Store(&next)
	i.idx.ClearEndpoint(ord)
	i.freed = append(i.freed, freedOrdinal{ordinal: ord, at: time.Now()})
}

// Pods returns the list of all pods currently tracked in the indexer.
func (i *indexer) Pods() []ServerID {
	pods := i.snapshot()
	ids := make([]ServerID, 0, len(pods))
	for pod := range pods {
		ids = append(ids, pod)
	}
	return ids
}

// PodBlockCounts returns the number of cached blocks currently tracked per pod.
func (i *indexer) PodBlockCounts() map[ServerID]int {
	pods := i.snapshot()
	counts := make(map[ServerID]int, len(pods))
	for pod, ord := range pods {
		counts[pod] = i.idx.Len(ord)
	}
	return counts
}
