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
		assert.Zero(t, dispatches, "the controller binds the reservation")
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

	t.Run("a gate hold leaves the item at the head, refunds any reservation, and blocks lower bands", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		res := &fwkfcmocks.MockReservation{}
		gate.GateFunc = func(flowcontrol.FlowControlRequest, []fwkdl.Endpoint) flowcontrol.GateDecision {
			return flowcontrol.GateDecision{Admit: false, Reservation: res}
		}
		qHigh := h.addQueue(testFlow)
		highItem := h.newTestItem("high", testFlow, testTTL)
		require.NoError(t, qHigh.Add(highItem))
		lowFlow := flowcontrol.FlowKey{ID: "low-flow", Priority: testFlow.Priority + 1}
		qLow := h.addQueue(lowFlow)
		lowItem := h.newTestItem("low", lowFlow, testTTL)
		require.NoError(t, qLow.Add(lowItem))

		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Nil(t, highItem.FinalState())
		assert.Nil(t, lowItem.FinalState(), "a gate hold enforces HoL blocking across lower bands")
		assert.Same(t, highItem, qHigh.Peek())
		_, refunds := res.Counts()
		assert.Equal(t, 1, refunds)
	})

	t.Run("a held item is pinned across cycles without advancing the fairness cursor", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		flowA := flowcontrol.FlowKey{ID: "flow-a", Priority: testFlow.Priority}
		flowB := flowcontrol.FlowKey{ID: "flow-b", Priority: testFlow.Priority}
		qA := h.addQueue(flowA)
		qB := h.addQueue(flowB)
		itemA := h.newTestItem("req-a", flowA, testTTL)
		itemB := h.newTestItem("req-b", flowB, testTTL)
		require.NoError(t, qA.Add(itemA))
		require.NoError(t, qB.Add(itemB))

		// Configure a stateful round-robin fairness policy that alternates qA, qB on every Pick call.
		picks := 0
		h.fairnessPolicyPick = func(context.Context, flowcontrol.PriorityBandAccessor) (flowcontrol.FlowQueueAccessor, error) {
			picks++
			if picks%2 == 1 {
				return qA, nil
			}
			return qB, nil
		}

		admit := false
		gate.GateFunc = func(flowcontrol.FlowControlRequest, []fwkdl.Endpoint) flowcontrol.GateDecision {
			return flowcontrol.GateDecision{Admit: admit}
		}

		// Cycle 1 picks itemA (Pick call #1) and holds it.
		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 1, picks)
		assert.Equal(t, []string{"req-a"}, gate.AskedIDs())

		// Cycle 2 re-examines pinned itemA without calling FairnessPolicy.Pick.
		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 1, picks, "FairnessPolicy.Pick must not be called while the held item stays at the head")
		assert.Equal(t, []string{"req-a", "req-a"}, gate.AskedIDs())

		// Cycle 3 admits itemA; Cycle 4 then calls FairnessPolicy.Pick (#2 -> qB) and dispatches itemB.
		admit = true
		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, types.QueueOutcomeDispatched, itemA.FinalState().Outcome)
		assert.Equal(t, 1, picks)

		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 2, picks)
		assert.Equal(t, types.QueueOutcomeDispatched, itemB.FinalState().Outcome)
	})

	t.Run("a held item that is cancelled or swept unpins cleanly without wasting a fairness turn", func(t *testing.T) {
		t.Parallel()
		h, gate := newGatedHarness(t)
		flowA := flowcontrol.FlowKey{ID: "flow-a", Priority: testFlow.Priority}
		flowB := flowcontrol.FlowKey{ID: "flow-b", Priority: testFlow.Priority}
		qA := h.addQueue(flowA)
		qB := h.addQueue(flowB)
		itemA1 := h.newTestItem("req-a1", flowA, testTTL)
		itemA2 := h.newTestItem("req-a2", flowA, testTTL)
		itemB1 := h.newTestItem("req-b1", flowB, testTTL)
		require.NoError(t, qA.Add(itemA1))
		require.NoError(t, qB.Add(itemB1))

		picks := 0
		h.fairnessPolicyPick = func(context.Context, flowcontrol.PriorityBandAccessor) (flowcontrol.FlowQueueAccessor, error) {
			picks++
			if picks%2 == 1 {
				return qA, nil
			}
			return qB, nil
		}

		admit := false
		gate.GateFunc = func(flowcontrol.FlowControlRequest, []fwkdl.Endpoint) flowcontrol.GateDecision {
			return flowcontrol.GateDecision{Admit: admit}
		}

		// Cycle 1 picks itemA1 (Pick #1) and holds it.
		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 1, picks)

		// Caller cancels itemA1 before sweepFinalizedItems runs; Cycle 2 removes the zombie itemA1
		// from qA without consulting the gate or calling Pick (#1 stays 1).
		itemA1.Finalize(context.Canceled)
		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 1, picks)
		assert.Nil(t, qA.Peek())
		assert.Equal(t, []string{"req-a1"}, gate.AskedIDs())
		require.NoError(t, qA.Add(itemA2))

		// Cycle 3 calls Pick (#2 -> qB) and holds itemB1; then sweepFinalizedItems removes itemB1.
		assert.False(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 2, picks)
		itemB1.Finalize(context.Canceled)
		h.processor.sweepFinalizedItems()

		// Cycle 4 sees itemB1 is gone from qB, unpins it, calls Pick (#3 -> qA), and dispatches itemA2.
		admit = true
		assert.True(t, h.processor.dispatchCycle(ctx))
		assert.Equal(t, 3, picks)
		assert.Equal(t, types.QueueOutcomeDispatched, itemA2.FinalState().Outcome)
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
