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

package sessionstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	eppdatalayer "github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrsession "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/session"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

func newTestProducer(t *testing.T) *Producer {
	t.Helper()
	plg, err := Factory(SessionStateProducerType, nil, nil)
	require.NoError(t, err)
	producer, ok := plg.(*Producer)
	require.True(t, ok)
	return producer
}

func requestWithAgentIdentity(agentIdentity string) *fwksched.InferenceRequest {
	request := &fwksched.InferenceRequest{}
	request.PutAttribute(agentidentity.AgentIdentityKey, agentIdentity)
	return request
}

func resultWithProfiles(profileCount int) *fwksched.SchedulingResult {
	profiles := make(map[string]*fwksched.ProfileRunResult, profileCount)
	for i := range profileCount {
		profiles[string(rune('a'+i))] = &fwksched.ProfileRunResult{
			TargetEndpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, fwkdl.NewMetrics(), fwkdl.NewAttributes()),
			},
		}
	}
	return &fwksched.SchedulingResult{ProfileResults: profiles}
}

func resultWithTargetEndpoint(id k8stypes.NamespacedName) *fwksched.SchedulingResult {
	return &fwksched.SchedulingResult{
		PrimaryProfileName: "decode",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"prefill": {
				TargetEndpoints: []fwksched.Endpoint{
					fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: id.Namespace, Name: "prefill-pod"}}, fwkdl.NewMetrics(), fwkdl.NewAttributes()),
				},
			},
			"decode": {
				TargetEndpoints: []fwksched.Endpoint{
					fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: id}, fwkdl.NewMetrics(), fwkdl.NewAttributes()),
				},
			},
		},
	}
}

func finalResponse(cause fwkrc.TerminationCause, inputTokens, outputTokens int) *fwkrc.Response {
	return &fwkrc.Response{
		EndOfStream:      true,
		TerminationCause: cause,
		Usage: fwkrh.Usage{
			PromptTokens:     inputTokens,
			CompletionTokens: outputTokens,
		},
	}
}

func TestFactoryAndProduces(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)

	assert.Equal(t, fwkplugin.TypedName{Type: SessionStateProducerType, Name: SessionStateProducerType}, producer.TypedName())
	expectedKey := SessionStateDataKey.WithNonEmptyProducerName(SessionStateProducerType)
	produced, ok := producer.Produces()[expectedKey]
	require.True(t, ok)
	assert.IsType(t, SessionState{}, produced)
	assert.Equal(t, defaultEvictionTTL, producer.evictionTTL)
	assert.Equal(t, defaultEvictionSweepInterval, producer.evictionSweepInterval)

	dependencies := producer.Consumes()
	consumed, ok := dependencies.Required[agentidentity.AgentIdentityKey]
	require.True(t, ok)
	assert.IsType(t, "", consumed)
	assert.Empty(t, dependencies.Optional)
}

func TestAgentIdentityIsARequiredDependency(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin(producer.TypedName().Name, producer)

	err := eppdatalayer.CreateMissingDataProducers(
		context.Background(),
		map[string]string{},
		map[string]fwkplugin.FactoryFunc{},
		handle,
	)
	require.ErrorIs(t, err, eppdatalayer.ErrNoDefaultProducer)
	assert.ErrorContains(t, err, agentidentity.AgentIdentityKey.String())
	assert.ErrorContains(t, err, producer.TypedName().Name)
}

func TestAgentIdentitySatisfiesRequiredDependency(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	handle := fwkplugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin(producer.TypedName().Name, producer)

	identityProvider, err := agentidentity.PluginFactory("custom-agent-identity", nil, handle)
	require.NoError(t, err)
	handle.AddPlugin(identityProvider.TypedName().Name, identityProvider)

	require.NoError(t, eppdatalayer.CreateMissingDataProducers(
		context.Background(),
		map[string]string{},
		map[string]fwkplugin.FactoryFunc{},
		handle,
	))

	ordered, err := eppdatalayer.ValidateAndOrderDataDependencies(handle.GetAllPlugins())
	require.NoError(t, err)
	assert.Equal(t, []string{identityProvider.TypedName().String(), producer.TypedName().String()}, ordered)
}

func TestFactoryParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		parameters      json.RawMessage
		wantTTL         time.Duration
		wantSweep       time.Duration
		wantErrContains string
	}{
		{
			name:       "custom durations",
			parameters: json.RawMessage(`{"evictionTtlSeconds":12.5,"evictionSweepSeconds":1.5}`),
			wantTTL:    12500 * time.Millisecond,
			wantSweep:  1500 * time.Millisecond,
		},
		{
			name:       "zero ttl disables eviction",
			parameters: json.RawMessage(`{"evictionTtlSeconds":0,"evictionSweepSeconds":10}`),
			wantTTL:    0,
			wantSweep:  10 * time.Second,
		},
		{
			name:            "negative ttl",
			parameters:      json.RawMessage(`{"evictionTtlSeconds":-1,"evictionSweepSeconds":10}`),
			wantErrContains: "evictionTtlSeconds must be >= 0",
		},
		{
			name:            "zero sweep",
			parameters:      json.RawMessage(`{"evictionSweepSeconds":0}`),
			wantErrContains: "evictionSweepSeconds must be > 0",
		},
		{
			name:            "negative sweep",
			parameters:      json.RawMessage(`{"evictionSweepSeconds":-1}`),
			wantErrContains: "evictionSweepSeconds must be > 0",
		},
		{
			name:            "unknown field",
			parameters:      json.RawMessage(`{"unknown":1}`),
			wantErrContains: "unknown field",
		},
		{
			name:            "invalid json",
			parameters:      json.RawMessage(`not-json`),
			wantErrContains: "invalid config",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plg, err := Factory("custom", fwkplugin.StrictDecoder(test.parameters), nil)
			if test.wantErrContains != "" {
				require.ErrorContains(t, err, test.wantErrContains)
				return
			}
			require.NoError(t, err)
			producer, ok := plg.(*Producer)
			require.True(t, ok)
			assert.Equal(t, test.wantTTL, producer.evictionTTL)
			assert.Equal(t, test.wantSweep, producer.evictionSweepInterval)
		})
	}
}

func TestProduceWithoutAgentIdentityIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := &fwksched.InferenceRequest{}

	require.NoError(t, producer.Produce(context.Background(), nil, nil))
	require.NoError(t, producer.Produce(context.Background(), request, nil))
	require.NoError(t, producer.Produce(context.Background(), requestWithAgentIdentity(""), nil))

	assert.Empty(t, request.AttributeKeys())
}

func TestSessionIDAttributeAloneIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := &fwksched.InferenceRequest{}
	request.PutAttribute(attrsession.SessionIDDataKey, attrsession.SessionID("session-a"))

	require.NoError(t, producer.Produce(context.Background(), request, nil))
	require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(1)))

	_, published := ReadSessionState(request)
	assert.False(t, published)
}

func TestProduceAndPreRequestTrackSessionHistory(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	first := requestWithAgentIdentity("session-a")

	beforeFirst := time.Now()
	require.NoError(t, producer.Produce(context.Background(), first, nil))
	afterFirst := time.Now()
	state, ok := ReadSessionState(first)
	require.True(t, ok)
	assert.Equal(t, int64(0), state.TurnsTaken)
	assert.Zero(t, state.Duration)
	assert.False(t, state.LastSeenAt.Before(beforeFirst))
	assert.False(t, state.LastSeenAt.After(afterFirst))
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	firstState := state

	require.NoError(t, producer.PreRequest(context.Background(), first, resultWithProfiles(1)))

	second := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), second, nil))
	state, ok = ReadSessionState(second)
	require.True(t, ok)
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.GreaterOrEqual(t, state.Duration, time.Duration(0))
	assert.Equal(t, firstState.LastSeenAt, state.LastSeenAt)
}

func TestResponseBodyTracksCompletedRequestsAndTokens(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), request, nil))
	require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(1)))

	producer.ResponseBody(context.Background(), request, &fwkrc.Response{EndOfStream: false}, nil)
	inFlight := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), inFlight, nil))
	state, ok := ReadSessionState(inFlight)
	require.True(t, ok)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)

	producer.ResponseBody(context.Background(), request, finalResponse(fwkrc.TerminationCauseNatural, 12, 34), nil)
	completed := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), completed, nil))
	state, ok = ReadSessionState(completed)
	require.True(t, ok)
	assert.Zero(t, state.InFlightRequests)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(12), state.TotalInputTokens)
	assert.Equal(t, int64(34), state.TotalOutputTokens)
	assert.Equal(t, int64(46), state.ContextTokens)
}

func TestAbnormalTerminationDoesNotCompleteRequest(t *testing.T) {
	t.Parallel()

	causes := []fwkrc.TerminationCause{
		fwkrc.TerminationCauseClientDisconnect,
		fwkrc.TerminationCauseEvicted,
		fwkrc.TerminationCauseError,
	}
	for _, cause := range causes {
		t.Run(string(cause), func(t *testing.T) {
			t.Parallel()
			producer := newTestProducer(t)
			request := requestWithAgentIdentity("session-a")
			require.NoError(t, producer.Produce(context.Background(), request, nil))
			require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(1)))

			producer.ResponseBody(context.Background(), request, finalResponse(cause, 12, 34), nil)
			next := requestWithAgentIdentity("session-a")
			require.NoError(t, producer.Produce(context.Background(), next, nil))
			state, ok := ReadSessionState(next)
			require.True(t, ok)
			assert.Zero(t, state.InFlightRequests)
			assert.Zero(t, state.CompletedRequests)
			assert.Zero(t, state.TotalInputTokens)
			assert.Zero(t, state.TotalOutputTokens)
			assert.Zero(t, state.ContextTokens)
		})
	}
}

func TestNaturalCompletionWithoutUsage(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), request, nil))
	require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(1)))
	producer.ResponseBody(context.Background(), request, finalResponse(fwkrc.TerminationCauseNatural, 0, 0), nil)

	next := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), next, nil))
	state, ok := ReadSessionState(next)
	require.True(t, ok)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Zero(t, state.ContextTokens)
}

func TestResponseBodyWithoutTrackedDispatchIsNoOp(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), request, nil))

	producer.ResponseBody(context.Background(), nil, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)
	producer.ResponseBody(context.Background(), &fwksched.InferenceRequest{}, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)
	producer.ResponseBody(context.Background(), request, nil, nil)
	producer.ResponseBody(context.Background(), request, finalResponse(fwkrc.TerminationCauseNatural, 1, 2), nil)

	next := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), next, nil))
	state, ok := ReadSessionState(next)
	require.True(t, ok)
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
}

func TestPreRequestCountsMultipleProfilesOnce(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	request := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), request, nil))
	require.NoError(t, producer.PreRequest(context.Background(), request, resultWithProfiles(2)))

	next := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.Produce(context.Background(), next, nil))
	state, ok := ReadSessionState(next)
	require.True(t, ok)
	assert.Equal(t, int64(1), state.TurnsTaken)
}

func TestRequestHeaderDynamicAttributeDuringFlowControlWait(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	ep1 := k8stypes.NamespacedName{Namespace: "default", Name: "pod-1"}
	ep2 := k8stypes.NamespacedName{Namespace: "default", Name: "pod-2"}

	// A request that runs RequestHeader and is dropped by Flow Control before Produce
	// must not allocate a session record in the registry.
	rejected := requestWithAgentIdentity("rejected-session")
	require.NoError(t, producer.RequestHeader(context.Background(), rejected))
	rejectedState, ok := ReadSessionState(rejected)
	require.True(t, ok)
	assert.Equal(t, SessionState{}, rejectedState)
	assert.Zero(t, registrySessionCount(&producer.registry))

	// Turn 1: arrives, runs RequestHeader (read-only), then Produce (allocates record) and PreRequest (in flight on ep1).
	turn1 := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), turn1))
	stateBeforeProduce, ok := ReadSessionState(turn1)
	require.True(t, ok)
	assert.Equal(t, SessionState{}, stateBeforeProduce)
	assert.Zero(t, registrySessionCount(&producer.registry))

	require.NoError(t, producer.Produce(context.Background(), turn1, nil))
	state, ok := ReadSessionState(turn1)
	require.True(t, ok)
	assert.Zero(t, state.TurnsTaken)
	assert.Zero(t, state.Duration)
	assert.False(t, state.LastSeenAt.IsZero())
	turn1SeenAt := state.LastSeenAt
	require.NoError(t, producer.PreRequest(context.Background(), turn1, resultWithTargetEndpoint(ep1)))

	// Turn 2 arrives while Turn 1 is still in flight and enters Flow Control after RequestHeader.
	turn2 := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), turn2))

	// During Flow Control wait (before Produce), ReadSessionState dynamically resolves PeekState
	// without mutating Turn 1's LastSeenAt in the registry.
	stateWhileTurn1InFlight, ok := ReadSessionState(turn2)
	require.True(t, ok)
	assert.Equal(t, int64(1), stateWhileTurn1InFlight.TurnsTaken)
	assert.Equal(t, int64(1), stateWhileTurn1InFlight.InFlightRequests)
	assert.Zero(t, stateWhileTurn1InFlight.CompletedRequests)
	assert.Equal(t, ep1, stateWhileTurn1InFlight.LastEndpoint)
	assert.Equal(t, turn1SeenAt, stateWhileTurn1InFlight.LastSeenAt)

	// Turn 1 completes while Turn 2 is still queued in Flow Control.
	producer.ResponseBody(context.Background(), turn1, finalResponse(fwkrc.TerminationCauseNatural, 100, 50), nil)

	// Without calling Produce on turn2, Flow Control immediately observes Turn 1's completion and ContextTokens.
	stateAfterTurn1Completes, ok := ReadSessionState(turn2)
	require.True(t, ok)
	assert.Equal(t, int64(1), stateAfterTurn1Completes.TurnsTaken)
	assert.Zero(t, stateAfterTurn1Completes.InFlightRequests)
	assert.Equal(t, int64(1), stateAfterTurn1Completes.CompletedRequests)
	assert.Equal(t, int64(100), stateAfterTurn1Completes.TotalInputTokens)
	assert.Equal(t, int64(50), stateAfterTurn1Completes.TotalOutputTokens)
	assert.Equal(t, int64(150), stateAfterTurn1Completes.ContextTokens)
	assert.Equal(t, ep1, stateAfterTurn1Completes.LastEndpoint)
	assert.Equal(t, turn1SeenAt, stateAfterTurn1Completes.LastSeenAt)

	// Turn 2 is admitted, runs Produce, PreRequest on ep2, and completes with new tokens.
	require.NoError(t, producer.Produce(context.Background(), turn2, nil))
	stateTurn2AfterProduce, ok := ReadSessionState(turn2)
	require.True(t, ok)
	assert.Equal(t, turn1SeenAt, stateTurn2AfterProduce.LastSeenAt)
	require.NoError(t, producer.PreRequest(context.Background(), turn2, resultWithTargetEndpoint(ep2)))
	producer.ResponseBody(context.Background(), turn2, finalResponse(fwkrc.TerminationCauseNatural, 180, 40), nil)

	// Turn 3 observes cumulative TotalInputTokens/TotalOutputTokens and latest-turn ContextTokens + LastEndpoint.
	turn3 := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(context.Background(), turn3))
	stateTurn3, ok := ReadSessionState(turn3)
	require.True(t, ok)
	assert.Equal(t, int64(2), stateTurn3.TurnsTaken)
	assert.Equal(t, int64(2), stateTurn3.CompletedRequests)
	assert.Equal(t, int64(280), stateTurn3.TotalInputTokens)
	assert.Equal(t, int64(90), stateTurn3.TotalOutputTokens)
	assert.Equal(t, int64(220), stateTurn3.ContextTokens)
	assert.Equal(t, ep2, stateTurn3.LastEndpoint)
	assert.False(t, stateTurn3.LastSeenAt.Before(turn1SeenAt))
}

func TestRequestHeaderFairnessIDFallback(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)

	_, ok := ReadSessionState(nil)
	assert.False(t, ok)

	// DefaultFairnessID must not be treated as a session identity.
	defaultReq := &fwksched.InferenceRequest{FairnessID: metadata.DefaultFairnessID}
	require.NoError(t, producer.RequestHeader(context.Background(), defaultReq))
	require.NoError(t, producer.Produce(context.Background(), defaultReq, nil))
	_, ok = ReadSessionState(defaultReq)
	assert.False(t, ok)

	// Non-default FairnessID is used when AgentIdentityKey is absent.
	fairnessReq := &fwksched.InferenceRequest{FairnessID: "fairness-session-1"}
	require.NoError(t, producer.RequestHeader(context.Background(), fairnessReq))
	require.NoError(t, producer.Produce(context.Background(), fairnessReq, nil))
	require.NoError(t, producer.PreRequest(context.Background(), fairnessReq, resultWithProfiles(1)))
	producer.ResponseBody(context.Background(), fairnessReq, finalResponse(fwkrc.TerminationCauseNatural, 20, 10), nil)

	nextReq := &fwksched.InferenceRequest{FairnessID: "fairness-session-1"}
	require.NoError(t, producer.RequestHeader(context.Background(), nextReq))
	state, ok := ReadSessionState(nextReq)
	require.True(t, ok)
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(30), state.ContextTokens)
}

func TestPreRequestContextCancellationCleansUpInFlightIdempotently(t *testing.T) {
	t.Parallel()

	producer := newTestProducer(t)
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1 := requestWithAgentIdentity("session-a")

	require.NoError(t, producer.RequestHeader(ctx1, req1))
	require.NoError(t, producer.PreRequest(ctx1, req1, resultWithProfiles(1)))
	assert.Equal(t, int64(1), producer.registry.PeekState("session-a").InFlightRequests)

	// Cancel ctx1 before ResponseBody(EndOfStream): AfterFunc backstop must decrement InFlightRequests to 0.
	cancel1()
	require.Eventually(t, func() bool {
		return producer.registry.PeekState("session-a").InFlightRequests == 0
	}, time.Second, time.Millisecond)

	// Start a second turn so InFlightRequests is 1.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	req2 := requestWithAgentIdentity("session-a")
	require.NoError(t, producer.RequestHeader(ctx2, req2))
	require.NoError(t, producer.PreRequest(ctx2, req2, resultWithProfiles(1)))
	assert.Equal(t, int64(1), producer.registry.PeekState("session-a").InFlightRequests)

	// Late ResponseBody(EndOfStream) for req1 must be a no-op and must not decrement req2's in-flight count.
	producer.ResponseBody(ctx1, req1, finalResponse(fwkrc.TerminationCauseClientDisconnect, 10, 10), nil)
	assert.Equal(t, int64(1), producer.registry.PeekState("session-a").InFlightRequests)

	// Complete req2 normally, then cancel ctx2 afterwards; InFlightRequests must remain 0.
	producer.ResponseBody(ctx2, req2, finalResponse(fwkrc.TerminationCauseNatural, 30, 15), nil)
	cancel2()
	state := producer.registry.PeekState("session-a")
	assert.Zero(t, state.InFlightRequests)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(45), state.ContextTokens)
}
