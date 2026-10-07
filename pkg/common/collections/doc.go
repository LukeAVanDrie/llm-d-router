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

// Package collections provides generic, allocation-conscious data structures
// shared across routing and cache-indexing packages:
//   - Bitset and AtomicBitset store up to 256 ordinals inline in four uint64
//     words and spill ordinals >= 256 into an on-demand overflow slice.
//   - Interner assigns dense non-negative integer ordinals to comparable
//     values with a copy-on-write reverse lookup table and a free-list for
//     released ordinals.
//   - FastTable is a power-of-two, 8-way linear-probing lock-free pointer
//     cache placed in front of sharded maps and LRUs.
//   - ShardedBitsetIndex and BitsetEntry provide a 256-shard inverted index
//     mapping comparable keys to lock-free AtomicBitset ordinal sets with
//     FastTable front-end caching and lazy dead-entry compaction.
package collections
