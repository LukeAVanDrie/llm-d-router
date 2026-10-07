/*
Copyright 2026 The Kubernetes Authors.

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

// Package approximateprefix implements a scheduler-driven data producer that
// estimates per-endpoint KV-cache prefix residency from routing decisions
// without engine KV-event telemetry.
//
// For each routed request, Add records the prompt's rolling prefix block
// hashes in the selected endpoint's bounded LRU cache and updates a
// collections.ShardedBitsetIndex[blockHash] on admission and LRU eviction.
// Hashes are inserted tail-first so earlier blocks in the prompt sit closer to
// the most-recently-used front of the queue; when capacity eviction drops
// entries from the LRU tail, each endpoint's remaining cached prefix stays
// contiguous from block 0.
//
// MatchLongestPrefix exploits this contiguity invariant to evaluate candidate
// endpoints with a 16-block jump stride: when every active candidate holds the
// block at the end of a stride, all intermediate blocks in that stride are
// skipped, and single-block probing runs only within the stride where at least
// one candidate's prefix ends.
package approximateprefix
