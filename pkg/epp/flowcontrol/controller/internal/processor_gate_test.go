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

package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/types"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
)

func TestProcessorEndpointGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	newGatedHarness := func(t *testing.T) (*testHarness, *fwkfcmocks.MockEndpointGate) {
		h := newTestHarness(t, testCleanupTick)
		gate := &fwkfcmocks.MockEndpointGate{}
		h.processor.gate = gate
		return h, gate
	}

	t.Run("a dispatched item carries its reservation", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		q := h.addQueue(testFlow)
		item := h.newTestItem("gated", testFlow, testTTL)
		require.NoError(t, q.Add(item))

		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, types.QueueOutcomeDispatched, item.FinalState().Outcome)
		assert.Same(t, gate.Last(), item.FinalState().Reservation)
		dispatches, refunds := gate.Last().Counts()
		assert.Zero(t, dispatches, "the controller, not the processor, binds the reservation")
		assert.Zero(t, refunds)
	})

	t.Run("the gate sees the request's own candidates", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		subset := []fwkdl.Endpoint{fwkdl.NewEndpoint(nil, nil)}
		h.endpointCandidates.LocateFunc = func(_ context.Context, md map[string]any) []fwkdl.Endpoint {
			if md != nil {
				return subset
			}
			return []fwkdl.Endpoint{fwkdl.NewEndpoint(nil, nil), fwkdl.NewEndpoint(nil, nil)}
		}
		q := h.addQueue(testFlow)
		require.NoError(t, q.Add(h.newTestItem("subset", testFlow, testTTL)))

		assert.True(t, h.processor.dispatchCycle(ctx))
		seen := gate.CandidatesSeen()
		require.Len(t, seen, 1)
		assert.Equal(t, subset, seen[0])
	})

	t.Run("a request with no candidates dispatches without the gate", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		h.endpointCandidates.LocateFunc = func(_ context.Context, md map[string]any) []fwkdl.Endpoint {
			if md != nil {
				return nil
			}
			return []fwkdl.Endpoint{fwkdl.NewEndpoint(nil, nil)}
		}
		q := h.addQueue(testFlow)
		item := h.newTestItem("empty-subset", testFlow, testTTL)
		require.NoError(t, q.Add(item))

		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, types.QueueOutcomeDispatched, item.FinalState().Outcome)
		assert.Empty(t, gate.AskedIDs())
		assert.Nil(t, item.FinalState().Reservation)
	})

	t.Run("a saturated band never consults the gate", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		h.saturationDetector.SaturationFunc = func(context.Context, []fwkdl.Endpoint) float64 { return 1.0 }
		q := h.addQueue(testFlow)
		require.NoError(t, q.Add(h.newTestItem("saturated", testFlow, testTTL)))

		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Empty(t, gate.AskedIDs())
	})

	t.Run("an item the caller already finalized skips the gate", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		q := h.addQueue(testFlow)
		item := h.newTestItem("cancelled", testFlow, testTTL)
		require.NoError(t, q.Add(item))
		item.Finalize(context.Canceled)

		h.processor.dispatchCycle(ctx)
		assert.Empty(t, gate.AskedIDs())
		assert.NotEqual(t, types.QueueOutcomeDispatched, item.FinalState().Outcome)
	})

	t.Run("the reservation is refunded when the sweep removed the item first", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		q := h.addQueue(testFlow)
		require.NoError(t, q.Add(h.newTestItem("swept", testFlow, testTTL)))
		q.RemoveFunc = func(flowcontrol.QueueItemHandle) (flowcontrol.QueueItemAccessor, error) {
			return nil, errors.New("already removed")
		}

		h.processor.dispatchCycle(ctx)
		_, refunds := gate.Last().Counts()
		assert.Equal(t, 1, refunds)
	})

	t.Run("the reservation is refunded when the item's queue is gone", func(t *testing.T) {
		t.Parallel()
		h, _ := newGatedHarness(t)
		item := h.newTestItem("no-queue", testFlow, testTTL) // no queue registered for its flow
		res := &fwkfcmocks.MockReservation{}

		require.Error(t, h.processor.dispatchItem(item, res))
		_, refunds := res.Counts()
		assert.Equal(t, 1, refunds)
	})

	t.Run("the reservation is refunded when the queue returns an unexpected item type", func(t *testing.T) {
		t.Parallel()
		h, _ := newGatedHarness(t)
		q := h.addQueue(testFlow)
		item := h.newTestItem("typed", testFlow, testTTL)
		require.NoError(t, q.Add(item))
		q.RemoveFunc = func(flowcontrol.QueueItemHandle) (flowcontrol.QueueItemAccessor, error) {
			return &fwkfcmocks.MockQueueItemAccessor{}, nil
		}
		res := &fwkfcmocks.MockReservation{}

		require.Error(t, h.processor.dispatchItem(item, res))
		_, refunds := res.Counts()
		assert.Equal(t, 1, refunds)
	})

	t.Run("the reservation is refunded when the controller finalized the item first", func(t *testing.T) {
		t.Parallel()
		h, _ := newGatedHarness(t)
		q := h.addQueue(testFlow)
		item := h.newTestItem("raced", testFlow, testTTL)
		require.NoError(t, q.Add(item))
		item.Finalize(context.Canceled) // finalized between the gate call and dispatch
		res := &fwkfcmocks.MockReservation{}

		require.NoError(t, h.processor.dispatchItem(item, res))
		_, refunds := res.Counts()
		assert.Equal(t, 1, refunds)
		assert.Nil(t, item.FinalState().Reservation)
	})
}

func TestFinalizeDispatched(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t, testCleanupTick)
	res := &fwkfcmocks.MockReservation{}

	won := h.newTestItem("won", testFlow, testTTL)
	require.True(t, won.FinalizeDispatched(res))
	assert.Equal(t, types.QueueOutcomeDispatched, won.FinalState().Outcome)
	assert.Same(t, res, won.FinalState().Reservation)

	lost := h.newTestItem("lost", testFlow, testTTL)
	lost.Finalize(context.Canceled)
	require.False(t, lost.FinalizeDispatched(res))
	assert.Nil(t, lost.FinalState().Reservation)
}
