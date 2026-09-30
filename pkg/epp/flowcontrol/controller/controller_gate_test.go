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

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts/mocks"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/controller/internal"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/types"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
)

func TestEnqueueAndWaitBindsReservation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		dispatch    bool
		wantOutcome types.QueueOutcome
	}{
		{name: "a dispatched reservation is bound to the caller's context", dispatch: true,
			wantOutcome: types.QueueOutcomeDispatched},
		{name: "a rejected item has no reservation to bind", wantOutcome: types.QueueOutcomeRejectedOther},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := &fwkfcmocks.MockReservation{}
			processor := &mockProcessor{SubmitFunc: func(item *internal.FlowItem) error {
				if tc.dispatch {
					go item.FinalizeDispatched(res)
				} else {
					go item.FinalizeWithError(fmt.Errorf("%w: test", types.ErrRejected))
				}
				return nil
			}}
			registry := &mockRegistryClient{FlowRegistryDataPlane: &mocks.MockRegistryDataPlane{}}
			h := newUnitHarness(t.Context(), t, &Config{DefaultRequestTTL: 5 * time.Second}, registry, processor)
			registry.WithConnectionFunc = func(key flowcontrol.FlowKey, fn func(contracts.ActiveFlowConnection) error) error {
				return fn(&mockActiveFlowConnection{RegistryV: h.mockRegistry, FlowKeyV: key})
			}

			callerCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			outcome, _ := h.fc.EnqueueAndWait(callerCtx, newTestRequest(defaultFlowKey))
			require.Equal(t, tc.wantOutcome, outcome)
			dispatches, _ := res.Counts()
			if !tc.dispatch {
				require.Zero(t, dispatches)
				return
			}
			require.Equal(t, 1, dispatches)
			require.Equal(t, callerCtx, res.DispatchedCtx())
			require.NoError(t, res.DispatchedCtx().Err(), "the reservation outlives EnqueueAndWait's own request context")
		})
	}
}

// A caller that cancels after the processor won the dispatch still gets its reservation bound, to a
// context that is already done, which releases it.
func TestEnqueueAndWaitBindsReservationAfterCallerCancels(t *testing.T) {
	t.Parallel()
	res := &fwkfcmocks.MockReservation{}
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processor := &mockProcessor{SubmitFunc: func(item *internal.FlowItem) error {
		item.FinalizeDispatched(res)
		cancel()
		return nil
	}}
	registry := &mockRegistryClient{FlowRegistryDataPlane: &mocks.MockRegistryDataPlane{}}
	h := newUnitHarness(t.Context(), t, &Config{DefaultRequestTTL: 5 * time.Second}, registry, processor)
	registry.WithConnectionFunc = func(key flowcontrol.FlowKey, fn func(contracts.ActiveFlowConnection) error) error {
		return fn(&mockActiveFlowConnection{RegistryV: h.mockRegistry, FlowKeyV: key})
	}

	outcome, _ := h.fc.EnqueueAndWait(callerCtx, newTestRequest(defaultFlowKey))
	require.Equal(t, types.QueueOutcomeDispatched, outcome)
	dispatches, refunds := res.Counts()
	require.Equal(t, 1, dispatches)
	require.Zero(t, refunds)
	require.Error(t, res.DispatchedCtx().Err())
}
