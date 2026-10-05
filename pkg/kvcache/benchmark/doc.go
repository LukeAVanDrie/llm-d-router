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

// Package benchmark measures throughput, latency, memory allocations, and lock contention
// across the KV-cache indexing stack and prefix-cache data producers.
//
// # Benchmark Categories
//
//   - BenchmarkReadMatrix: evaluates Indexer.MatchBlockKeys and Indexer.ScoreTokens
//     (comparing xxh64 and cbor-fnv token hashing) across execution modes, prefix
//     sharing topologies, block lengths, and single-rank or multi-rank fleet shapes.
//   - BenchmarkWriteScaling: evaluates steady-state kvblock.Index.Add and combined
//     Add+Evict lifecycle throughput across InMemoryIndex and CostAwareMemoryIndex
//     backends, batch sizes, and fleet sizes.
//   - BenchmarkMixedReadWrite: evaluates concurrent read and write/evict contention using
//     a deterministic mixed-actor RunParallel schedule (90% read, 10% write+evict) over
//     pre-allocated fixtures.
//   - BenchmarkEvictionDynamics: evaluates steady-state LRU eviction churn when unique
//     block keys (4x index capacity) cycle through InMemoryIndex (global key capacity
//     overflow and per-key PodCacheSize overflow) and CostAwareMemoryIndex.
//   - BenchmarkDataProducerComparison: compares preciseprefixcache.Producer and
//     approximateprefix data producer across ProduceOnly and ProduceAndPreRequest
//     lifecycles.
//
// # Workload Topologies
//
//   - TopologyHotspot: all pods hold 100% of the prompt blocks, exercising worst-case
//     per-key entry scanning and PodCache lock contention.
//   - TopologySystemPromptBranch: all pods share the first 25% of blocks (system prompt)
//     and each pod holds a distinct 75% suffix (conversation branch).
//   - TopologyDisjoint: each pod holds a distinct prefix sequence and queries target a
//     single pod's cached prefix.
//
// # Custom Metrics
//
//   - block_ops/s: KV-cache block operations processed per second (blocksPerOp * reqs/s).
//   - reqs/s:      completed benchmark operations (requests or batches) per second.
//
// # Running and Profiling
//
// Smoke check (1 iteration per sub-benchmark):
//
//	go test -run='^$' -bench=. -benchtime=1x ./pkg/kvcache/benchmark/...
//
// Full benchmark run with memory allocation statistics:
//
//	go test -run='^$' -bench=. -benchmem -benchtime=200ms ./pkg/kvcache/benchmark/...
//
// Profiling CPU, heap allocations, mutex contention, and blocking:
//
//	go test -run='^$' -bench=BenchmarkReadMatrix -benchmem \
//	  -cpuprofile=cpu.out -memprofile=mem.out \
//	  -mutexprofile=mutex.out -blockprofile=block.out \
//	  ./pkg/kvcache/benchmark/...
package benchmark
