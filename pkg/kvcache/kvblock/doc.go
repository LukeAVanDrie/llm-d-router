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

// Package kvblock computes rolling prefix-chained block keys and indexes their
// pod and device-tier residency across vLLM engines.
//
// TokenProcessor chunks token sequences into fixed-size blocks and hashes each
// block together with its parent BlockHash, model name, and per-block
// multimodal extra features. Because every BlockHash commits to its entire
// preceding token prefix, a flat lookup by BlockHash identifies an exact
// prefix position without traversing a tree.
//
// Index backends (InMemoryIndex, CostAwareMemoryIndex, and RedisIndex) map
// request keys to PodEntry sets and maintain an engine-key to request-key
// reverse mapping for KV-event eviction and parent-hash resolution.
// NewInstrumentedIndex and NewTracedIndex wrap an Index for Prometheus metrics
// and OpenTelemetry spans while preserving the optional KeyWalker and
// SnapshotWalker interfaces when the underlying backend implements them.
//
// InMemoryIndex partitions request-key and engine-key state across 256 shards
// once configured capacity reaches lruShardThreshold. Each request key maps to
// a PodCache whose PodSnapshot (carrying both the EntryRef slice and per-tier
// collections.Bitset pod-ordinal sets) is replaced copy-on-write under
// PodCache.mu and read through an atomic pointer. Readers probe the lock-free
// 8-way linear-probing collections.FastTable in lruStore.fast, validated by
// key equality and PodCache.resident, before falling back to a shard read lock,
// and WalkSnapshots coalesces consecutive keys that share an identical
// *PodSnapshot pointer. Read hits in sharded mode set PodCache.referenced
// without acquiring shard locks, and capacity evictions lazily promote
// referenced tail entries in the shard LRU under the shard write lock.
package kvblock
