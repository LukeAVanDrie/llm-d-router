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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

func registrySessionCount(registry *SessionStateRegistry) int {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return len(registry.sessions)
}

func setRegistrySessionTimes(t *testing.T, registry *SessionStateRegistry, identity string, firstSeenAt, lastSeenAt time.Time) {
	t.Helper()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.sessions[identity]
	require.True(t, ok)
	record.firstSeenAt = firstSeenAt
	record.state.LastSeenAt = lastSeenAt
}

func TestRegistryGetState(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	beforeFirst := time.Now()
	state := registry.GetState("session-a")
	afterFirst := time.Now()
	assert.Zero(t, state.TurnsTaken)
	assert.Zero(t, state.Duration)
	assert.False(t, state.LastSeenAt.Before(beforeFirst))
	assert.False(t, state.LastSeenAt.After(afterFirst))
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Zero(t, state.ContextTokens)
	assert.Empty(t, state.LastEndpoint)

	start := time.Now().Add(-2 * time.Minute)
	setRegistrySessionTimes(t, registry, "session-a", start, start)
	beforeSecond := time.Now()
	state = registry.GetState("session-a")
	afterSecond := time.Now()
	assert.GreaterOrEqual(t, state.Duration, beforeSecond.Sub(start))
	assert.LessOrEqual(t, state.Duration, afterSecond.Sub(start))
	assert.Equal(t, start, state.LastSeenAt)
}

func TestRegistryPeekStateIsReadOnly(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	unseen := registry.PeekState("unseen")
	assert.Equal(t, SessionState{}, unseen)
	assert.Zero(t, registrySessionCount(registry))

	registry.GetState("session-a")
	start := time.Now().Add(-3 * time.Minute)
	lastSeen := time.Now().Add(-1 * time.Minute)
	setRegistrySessionTimes(t, registry, "session-a", start, lastSeen)

	beforePeek := time.Now()
	peeked := registry.PeekState("session-a")
	afterPeek := time.Now()
	assert.GreaterOrEqual(t, peeked.Duration, beforePeek.Sub(start))
	assert.LessOrEqual(t, peeked.Duration, afterPeek.Sub(start))
	assert.Equal(t, lastSeen, peeked.LastSeenAt)

	// Verify LastSeenAt in the registry was not mutated by PeekState.
	peekedAgain := registry.PeekState("session-a")
	assert.Equal(t, lastSeen, peekedAgain.LastSeenAt)
}

func TestRegistryRecordsDispatchAndResponse(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	ep1 := k8stypes.NamespacedName{Namespace: "default", Name: "pod-1"}
	ep2 := k8stypes.NamespacedName{Namespace: "default", Name: "pod-2"}
	registry.RecordDispatch("session-a", ep1)
	state := registry.GetState("session-a")
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Equal(t, ep1, state.LastEndpoint)

	registry.RecordResponse("session-a", true, 12, 34)
	state = registry.GetState("session-a")
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Zero(t, state.InFlightRequests)
	assert.Equal(t, int64(1), state.CompletedRequests)
	assert.Equal(t, int64(12), state.TotalInputTokens)
	assert.Equal(t, int64(34), state.TotalOutputTokens)
	assert.Equal(t, int64(46), state.ContextTokens)
	assert.Equal(t, ep1, state.LastEndpoint)

	// Second turn: TotalInputTokens and TotalOutputTokens accumulate, while
	// ContextTokens reflects only the latest completed turn's Prompt + Completion.
	registry.RecordDispatch("session-a", ep2)
	registry.RecordResponse("session-a", true, 50, 20)
	state = registry.GetState("session-a")
	assert.Equal(t, int64(2), state.TurnsTaken)
	assert.Equal(t, int64(2), state.CompletedRequests)
	assert.Equal(t, int64(62), state.TotalInputTokens)
	assert.Equal(t, int64(54), state.TotalOutputTokens)
	assert.Equal(t, int64(70), state.ContextTokens)
	assert.Equal(t, ep2, state.LastEndpoint)

	// Third turn with omitted token usage (0, 0) preserves the prior non-zero ContextTokens.
	registry.RecordDispatch("session-a", k8stypes.NamespacedName{})
	registry.RecordResponse("session-a", true, 0, 0)
	state = registry.GetState("session-a")
	assert.Equal(t, int64(3), state.TurnsTaken)
	assert.Equal(t, int64(3), state.CompletedRequests)
	assert.Equal(t, int64(62), state.TotalInputTokens)
	assert.Equal(t, int64(54), state.TotalOutputTokens)
	assert.Equal(t, int64(70), state.ContextTokens)
	assert.Equal(t, ep2, state.LastEndpoint)

	cloned, ok := state.Clone().(SessionState)
	require.True(t, ok)
	assert.Equal(t, state, cloned)
}

func TestRegistryRecordsAbnormalResponse(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", k8stypes.NamespacedName{})
	registry.RecordResponse("session-a", false, 12, 34)
	state := registry.GetState("session-a")
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Zero(t, state.ContextTokens)

	registry.RecordResponse("session-a", true, 1, 2)
	state = registry.GetState("session-a")
	assert.Zero(t, state.InFlightRequests)
	assert.Zero(t, state.CompletedRequests)
	assert.Zero(t, state.TotalInputTokens)
	assert.Zero(t, state.TotalOutputTokens)
	assert.Zero(t, state.ContextTokens)
}

func TestRegistrySessionsAreIsolated(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", k8stypes.NamespacedName{})
	other := registry.GetState("session-b")
	assert.Zero(t, other.TurnsTaken)
	assert.Zero(t, other.InFlightRequests)

	state := registry.GetState("session-a")
	assert.Equal(t, int64(1), state.TurnsTaken)
	assert.Equal(t, int64(1), state.InFlightRequests)
	assert.Equal(t, 2, registrySessionCount(registry))
}

func TestRegistryEvictsIdleSessions(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.RecordDispatch("session-a", k8stypes.NamespacedName{})
	ttl := time.Hour
	start := time.Now().Add(-2 * ttl)
	setRegistrySessionTimes(t, registry, "session-a", start, start)

	registry.EvictIdle(start.Add(ttl+time.Nanosecond), ttl)
	assert.Equal(t, 1, registrySessionCount(registry), "a session with an in-flight request must remain")

	registry.RecordResponse("session-a", false, 0, 0)
	registry.EvictIdle(start.Add(ttl), ttl)
	assert.Equal(t, 1, registrySessionCount(registry), "a session at the TTL boundary must remain")

	registry.EvictIdle(start.Add(ttl+time.Nanosecond), ttl)
	assert.Zero(t, registrySessionCount(registry))
	state := registry.GetState("session-a")
	assert.Zero(t, state.TurnsTaken)
	assert.Zero(t, state.Duration)
}

func TestRegistryZeroTTLDisablesEviction(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	registry.GetState("session-a")
	registry.EvictIdle(time.Now().Add(24*time.Hour), 0)
	assert.Equal(t, 1, registrySessionCount(registry))
}

func TestRegistryConcurrentUpdates(t *testing.T) {
	t.Parallel()

	registry := &SessionStateRegistry{}
	const requests = 100

	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registry.RecordDispatch("session-a", k8stypes.NamespacedName{})
			registry.RecordResponse("session-a", true, 2, 3)
		}()
	}
	wg.Wait()

	state := registry.GetState("session-a")
	assert.Equal(t, int64(requests), state.TurnsTaken)
	assert.Zero(t, state.InFlightRequests)
	assert.Equal(t, int64(requests), state.CompletedRequests)
	assert.Equal(t, int64(2*requests), state.TotalInputTokens)
	assert.Equal(t, int64(3*requests), state.TotalOutputTokens)
	assert.Equal(t, int64(5), state.ContextTokens)
}
