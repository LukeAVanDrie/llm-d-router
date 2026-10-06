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

// Package kvcache matches prompt token prefixes against a kvblock.Index and
// scores candidate model-serving pods by contiguous KV-block residency.
//
// Indexer pairs a kvblock.TokenProcessor with a kvblock.Index and configured
// per-tier weights. ComputeBlockKeysFromTokens converts tokenized prompts into
// prefix-chained block keys. MatchBlockKeys evaluates a block-key sequence and
// returns a PodMatch per candidate pod containing the weighted score, total
// matched block count, and per-tier contiguous block counts. ScoreTokens
// computes block keys from tokens and returns only the per-pod weighted scores,
// skipping per-tier map allocation on the scoring path.
//
// When the underlying kvblock.Index implements kvblock.KeyWalker, both
// MatchBlockKeys and ScoreTokens stream per-key EntryRef slices into a pooled
// prefixAccumulator without materializing intermediate lookup maps. Backends
// without KeyWalker fall back to Index.Lookup and feed the same accumulator.
package kvcache
