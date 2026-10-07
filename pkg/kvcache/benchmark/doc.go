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

// Package benchmark provides workload-driven benchmarks for the KV-cache
// indexing and prefix-matching pipeline.
//
// # Benchmark Suites
//
//   - BenchmarkReadMatrix: evaluates steady-state prefix-matching read throughput
//     across MatchBlockKeys, ScoreTokens (xxh64 and sha256), execution modes
//     (Sequential, Parallel), prefix-sharing topologies (Hotspot,
//     SystemPromptBranch, Disjoint), block lengths (64, 512, 3750), and fleet
//     shapes (16, 96, 40x8).
//   - BenchmarkMixedReadWrite: evaluates concurrent read and write/evict contention
//     using a deterministic mixed-actor RunParallel schedule (90% read, 10%
//     write+evict) over pre-allocated fixtures.
//   - BenchmarkDataProducerComparison: compares preciseprefixcache.Producer and
//     approximateprefix DataProducer across ProduceOnly and ProduceAndPreRequest
//     lifecycles.
//   - BenchmarkMooncakeReplay: replays the Mooncake trace workload (or its hermetic
//     multi-turn fallback) across 128 inference workers for both PrecisePrefixCache
//     and ApproximatePrefixCache under QueryOnly and MixedOverload modes.
//
// Pure kvblock.Index write and eviction microbenchmarks live in
// pkg/kvcache/kvblock/in_memory_bench_test.go.
package benchmark
