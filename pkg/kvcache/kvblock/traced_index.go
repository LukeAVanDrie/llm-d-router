// Copyright 2025 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvblock

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/common/observability/semconv"
	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
)

var internalSpanStartOpts = []trace.SpanStartOption{
	trace.WithSpanKind(trace.SpanKindInternal),
}

type tracedIndex struct {
	next Index
}

// tracedWalker carries the KeyWalker capability of the wrapped index.
type tracedWalker struct {
	*tracedIndex
	walker     KeyWalker
	snapWalker SnapshotWalker
}

// NewTracedIndex wraps an Index and emits OpenTelemetry traces for index
// operations. The wrapper is a KeyWalker exactly when next is one.
func NewTracedIndex(next Index) Index {
	t := &tracedIndex{next: next}
	if walker, ok := next.(KeyWalker); ok {
		sw, _ := next.(SnapshotWalker)
		return &tracedWalker{tracedIndex: t, walker: walker, snapWalker: sw}
	}
	return t
}

// WalkKeys forwards the walk under a span reporting the keys requested and
// the keys present.
func (t *tracedWalker) WalkKeys(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, found bool, entries []EntryRef) bool,
) error {
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "index_walk", internalSpanStartOpts...)
	defer span.End()

	if !span.IsRecording() {
		if err := t.walker.WalkKeys(ctx, requestKeys, visit); err != nil {
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	}

	span.SetAttributes(semconv.LLMDKVCacheIndexWalkKeyCount(len(requestKeys)))

	present := 0
	err := t.walker.WalkKeys(ctx, requestKeys, func(pos int, found bool, entries []EntryRef) bool {
		if found {
			present++
		}
		return visit(pos, found, entries)
	})
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(semconv.LLMDKVCacheIndexWalkKeysPresent(present))
	return nil
}

// WalkSnapshots forwards the snapshot walk under a span reporting the keys
// requested and the keys present.
func (t *tracedWalker) WalkSnapshots(ctx context.Context, requestKeys []BlockHash,
	visit func(pos int, snap *PodSnapshot) bool,
) error {
	if t.snapWalker == nil {
		return t.WalkKeys(ctx, requestKeys, func(pos int, found bool, entries []EntryRef) bool {
			if !found {
				return visit(pos, nil)
			}
			return visit(pos, buildPodSnapshot(entries))
		})
	}
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "index_walk", internalSpanStartOpts...)
	defer span.End()

	if !span.IsRecording() {
		if err := t.snapWalker.WalkSnapshots(ctx, requestKeys, visit); err != nil {
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	}

	span.SetAttributes(semconv.LLMDKVCacheIndexWalkKeyCount(len(requestKeys)))

	present := 0
	err := t.snapWalker.WalkSnapshots(ctx, requestKeys, func(pos int, snap *PodSnapshot) bool {
		if snap != nil {
			present++
		}
		return visit(pos, snap)
	})
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(semconv.LLMDKVCacheIndexWalkKeysPresent(present))
	return nil
}

// PodName returns the pod identifier assigned to ord.
func (t *tracedWalker) PodName(ord uint32) string {
	if t.snapWalker != nil {
		return t.snapWalker.PodName(ord)
	}
	return ""
}

func (t *tracedIndex) Add(ctx context.Context, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "index_add", internalSpanStartOpts...)
	defer span.End()

	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCacheIndexAddEngineKeyCount(len(engineKeys)),
			semconv.LLMDKVCacheIndexAddRequestKeyCount(len(requestKeys)),
			semconv.LLMDKVCacheIndexAddPodEntryCount(len(entries)),
			semconv.LLMDKVCacheIndexAddDeviceTierCount(deviceTierCount(entries)),
		)
	}

	err := t.next.Add(ctx, engineKeys, requestKeys, entries)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (t *tracedIndex) Evict(ctx context.Context, key BlockHash, keyType KeyType, entries []PodEntry) error {
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "index_evict", internalSpanStartOpts...)
	defer span.End()

	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCacheIndexEvictKeyType(keyTypeLabel(keyType)),
			semconv.LLMDKVCacheIndexEvictPodEntryCount(len(entries)),
			semconv.LLMDKVCacheIndexEvictDeviceTierCount(deviceTierCount(entries)),
		)
	}

	err := t.next.Evict(ctx, key, keyType, entries)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (t *tracedIndex) Lookup(
	ctx context.Context,
	requestKeys []BlockHash,
	podIdentifierSet sets.Set[string],
) (map[BlockHash][]PodEntry, error) {
	tracer := tracing.Tracer(TracerScope)
	ctx, span := tracer.Start(ctx, "index_lookup", internalSpanStartOpts...)
	defer span.End()

	if span.IsRecording() {
		span.SetAttributes(
			semconv.LLMDKVCacheIndexLookupBlockCount(len(requestKeys)),
			semconv.LLMDKVCacheLookupPodFilterCount(podIdentifierSet.Len()),
		)
	}

	result, err := t.next.Lookup(ctx, requestKeys, podIdentifierSet)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if span.IsRecording() {
		blocksFound := 0
		for _, pods := range result {
			if len(pods) > 0 {
				blocksFound++
			}
		}
		cacheHit := blocksFound > 0

		span.SetAttributes(
			semconv.LLMDKVCacheLookupCacheHit(cacheHit),
			semconv.LLMDKVCacheLookupBlocksFound(blocksFound),
		)
	}

	return result, nil
}

func (t *tracedIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	return t.next.GetRequestKey(ctx, engineKey)
}

func (t *tracedIndex) Clear(ctx context.Context, podIdentifier string) error {
	return t.next.Clear(ctx, podIdentifier)
}

func keyTypeLabel(keyType KeyType) string {
	switch keyType {
	case EngineKey:
		return "engine"
	case RequestKey:
		return "request"
	default:
		return "unknown"
	}
}

func deviceTierCount(entries []PodEntry) int {
	if len(entries) == 0 {
		return 0
	}
	if len(entries) == 1 {
		if entries[0].DeviceTier == "" {
			return 0
		}
		return 1
	}
	deviceTiers := make(map[string]struct{})
	for _, entry := range entries {
		if entry.DeviceTier == "" {
			continue
		}
		deviceTiers[entry.DeviceTier] = struct{}{}
	}
	return len(deviceTiers)
}
