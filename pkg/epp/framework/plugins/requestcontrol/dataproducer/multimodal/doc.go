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

// Package multimodal provides a data producer for multimodal encoder-cache
// affinity.
//
// Producer extracts unique multimodal media hashes from tokenized requests,
// matches them against per-pod encoder-cache residency, and attaches
// EncoderCacheMatchInfo to candidate endpoints for downstream scoring.
//
// Residency is tracked via a collections.ShardedBitsetIndex[string] mapping
// each media hash to a collections.AtomicBitset of pod ordinals assigned by
// collections.Interner. Each pod owns an independent simplelru.LRU bounded by
// CacheSizeInMBPerServer whose eviction callback clears the pod's ordinal bit
// in the evicted entry's AtomicBitset. On Produce, each request item's bitset
// is snapshotted once and tested per candidate endpoint via Bitset.Has(ord),
// while PreRequest records the routed request's media hashes under the selected
// pod's per-pod mutex.
package multimodal
