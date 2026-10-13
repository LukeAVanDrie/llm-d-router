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

// PROTOTYPE (fork only): Mooncake replay over ChainIndex and an unwrapped
// InMemoryIndex, with stores split into parent-anchored segments.

package kvcache_test

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/llm-d/llm-d-router/test/utils/mooncake"
)

// chainSegment is a contiguous run of a query's keys with the key before it.
type chainSegment struct {
	parent kvblock.BlockHash
	keys   []kvblock.BlockHash
}

// segmentsOf splits subset, a subset of query, into maximal runs contiguous
// in query, each anchored at its predecessor in query.
func segmentsOf(query, subset []kvblock.BlockHash) []chainSegment {
	if len(subset) == 0 {
		return nil
	}
	in := make(map[kvblock.BlockHash]bool, len(subset))
	for _, k := range subset {
		in[k] = true
	}
	var out []chainSegment
	for i := 0; i < len(query); {
		if !in[query[i]] {
			i++
			continue
		}
		j := i
		for j < len(query) && in[query[j]] {
			j++
		}
		parent := kvblock.EmptyBlockHash
		if i > 0 {
			parent = query[i-1]
		}
		out = append(out, chainSegment{parent, query[i:j]})
		i = j
	}
	return out
}

type chainTurn struct {
	query, removed []kvblock.BlockHash
	stored         []chainSegment
	entries        []kvblock.PodEntry
}

func BenchmarkMooncakeReplayChain(b *testing.B) {
	r := mooncake.Load(b)
	ctx := benchContext()
	workers := make([][]kvblock.PodEntry, mooncake.Workers)
	for w := range workers {
		workers[w] = []kvblock.PodEntry{{PodIdentifier: fmt.Sprintf("10.0.0.%d:8000", w), DeviceTier: "gpu"}}
	}
	turns := make([]chainTurn, len(r.Turns))
	warm := make([][]chainSegment, len(r.Turns))
	for i, t := range r.Turns {
		q := mooncake.Convert[kvblock.BlockHash](t.Query)
		turns[i] = chainTurn{
			query:   q,
			removed: mooncake.Convert[kvblock.BlockHash](t.Removed),
			stored:  segmentsOf(q, mooncake.Convert[kvblock.BlockHash](t.Stored)),
			entries: workers[t.Worker],
		}
		warm[i] = segmentsOf(q, mooncake.Convert[kvblock.BlockHash](t.Warm))
	}

	for _, impl := range []string{"inmemory", "chain"} {
		var idx kvblock.Index
		var chain *kvblock.ChainIndex
		var before runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		scanBefore := scanHeapBytes()
		if impl == "chain" {
			chain = kvblock.NewChainIndex(kvblock.ChainIndexConfig{})
			idx = chain
		} else {
			inner, err := kvblock.NewInMemoryIndex(&kvblock.InMemoryIndexConfig{Size: 1 << 23, PodCacheSize: mooncake.Workers})
			if err != nil {
				b.Fatal(err)
			}
			idx = inner
		}
		add := func(seg chainSegment, entries []kvblock.PodEntry) error {
			if chain != nil {
				return chain.AddChain(ctx, seg.parent, seg.keys, seg.keys, entries)
			}
			return idx.Add(ctx, seg.keys, seg.keys, entries)
		}
		for i := range warm {
			for _, seg := range warm[i] {
				if err := add(seg, turns[i].entries); err != nil {
					b.Fatal(err)
				}
			}
		}
		var after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&after)
		heap := float64(after.HeapAlloc) - float64(before.HeapAlloc)
		scan := float64(scanHeapBytes()) - float64(scanBefore)
		matcher := benchMatcher(b, idx)

		for _, mode := range []string{"QueryOnly", "MixedOverload"} {
			b.Run(impl+"/"+mode, func(b *testing.B) {
				blocks := r.QueryBlocks
				if mode == "MixedOverload" {
					blocks = r.EventBlocks
				}
				var rebuildsBefore uint64
				if chain != nil {
					rebuildsBefore = chain.Stats().Rebuilds
				}
				var goroutines atomic.Int64
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := int(goroutines.Add(1)) * 9973
					for pb.Next() {
						i++
						t := &turns[i%len(turns)]
						if _, err := matcher.MatchBlockKeys(ctx, t.query, nil); err != nil {
							b.Error(err)
							return
						}
						if mode == "QueryOnly" {
							continue
						}
						for _, seg := range t.stored {
							if err := add(seg, t.entries); err != nil {
								b.Error(err)
								return
							}
						}
						if len(t.removed) > 0 {
							if err := idx.Evict(ctx, kvblock.EngineKey, t.removed, t.entries); err != nil {
								b.Error(err)
								return
							}
						}
					}
				})
				b.ReportMetric(blocks*float64(b.N)/b.Elapsed().Seconds(), "block_ops/s")
				if chain != nil {
					if mode == "QueryOnly" {
						b.ReportMetric(heap/float64(chain.Stats().Pairs), "heapB/pair")
						b.ReportMetric(scan, "scanB")
					} else {
						b.ReportMetric(float64(chain.Stats().Rebuilds-rebuildsBefore)/float64(b.N), "rebuilds/req")
					}
				} else if mode == "QueryOnly" {
					b.ReportMetric(heap, "heapB")
					b.ReportMetric(scan, "scanB")
				}
			})
		}
	}
}

// scanHeapBytes returns the heap bytes the GC scans, as of the last cycle.
func scanHeapBytes() uint64 {
	s := []metrics.Sample{{Name: "/gc/scan/heap:bytes"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s[0].Value.Uint64()
}

var _ = kvcache.GPUTier
