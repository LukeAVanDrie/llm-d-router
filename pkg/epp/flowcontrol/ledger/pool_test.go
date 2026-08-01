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

package ledger

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testclock "k8s.io/utils/clock/testing"
)

const testBlockSize = 16

// bothAxes gates KV as well as slots. Stage 2 ships with KV in shadow, but the
// KV accounting must be exercised as if it gated, because stage 3 flips it on.
var bothAxes = GatedAxes{KVTokens: true, Slots: true}

// literalTranslator books the footprint a test states verbatim, mapping the
// prediction's fields positionally onto the axes. Pool tests exercise the
// reservation protocol; the prompt/output decomposition is covered by
// TestTokenTranslator.
type literalTranslator struct{}

func (literalTranslator) ToFootprint(p Prediction) Footprint {
	return Footprint{KVTokens: p.PromptTokens, PrefillTokens: p.OutputTokens, Slots: p.Branching}
}

func (lt literalTranslator) ToEngineFootprint(p Prediction) EngineFootprint {
	fp := lt.ToFootprint(p)
	return EngineFootprint{KVBlocks: fp.KVTokens, Slots: fp.Slots}
}

type poolFixture struct {
	ledger *PoolLedger
	clock  *testclock.FakeClock
}

// newFixture builds a ledger over `endpoints` replicas, each with kvPerEndpoint
// tokens of block pool and the configured per-endpoint slot and prefill limits.
func newFixture(t *testing.T, cfg Config, endpoints int, kvPerEndpoint int64) *poolFixture {
	t.Helper()
	clk := testclock.NewFakeClock(time.Now())
	l := NewPoolLedger(clk, literalTranslator{}, cfg)

	for i := range endpoints {
		l.UpsertEndpoint(EndpointCapacity{
			ID:              fmt.Sprintf("ep-%d", i),
			KVTokens:        kvPerEndpoint,
			BlockSizeTokens: testBlockSize,
		})
	}
	return &poolFixture{ledger: l, clock: clk}
}

// commit books a lease whose footprint is exactly fp.
func commit(t *testing.T, l *PoolLedger, reqID, endpoint string, fp Footprint) CommitOutcome {
	t.Helper()
	return l.Commit(reqID, endpoint, LeaseSpec{
		Prediction: Prediction{
			PromptTokens: fp.KVTokens,
			OutputTokens: fp.PrefillTokens,
			Branching:    fp.Slots,
		},
	})
}

func TestPoolLedger_TryAcquireHold(t *testing.T) {
	t.Parallel()

	t.Run("EmptyPoolRefuses", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 0, 0)
		err := f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 1, Slots: 1}, 1.0)
		require.ErrorIs(t, err, ErrNoEndpoints, "holding against no capacity would admit unboundedly")
	})

	t.Run("AggregateExhaustion", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 2}, 2, 1000)

		// 4 slots pool-wide.
		for i := range 4 {
			require.NoError(t, f.ledger.TryAcquireHold(fmt.Sprintf("req-%d", i), Footprint{Slots: 1}, 1.0))
		}
		err := f.ledger.TryAcquireHold("req-4", Footprint{Slots: 1}, 1.0)
		require.ErrorIs(t, err, ErrPoolSaturated)
	})

	t.Run("HoldsCountAgainstAvailability", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 1}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0))
		require.ErrorIs(t, f.ledger.TryAcquireHold("req-2", Footprint{Slots: 1}, 1.0), ErrPoolSaturated,
			"an uncommitted hold must reserve capacity, or concurrent dispatch over-admits")

		f.ledger.ReleaseHold("req-1")
		require.NoError(t, f.ledger.TryAcquireHold("req-2", Footprint{Slots: 1}, 1.0),
			"a released hold refunds capacity")
	})

	t.Run("FragmentationRefusesDespiteAggregateRoom", func(t *testing.T) {
		t.Parallel()
		// Two endpoints, 1000 KV tokens each: 2000 aggregate, 1000 placeable.
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 100}, 2, 1000)

		err := f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 1500, Slots: 1}, 1.0)
		require.ErrorIs(t, err, ErrNoEndpointFits,
			"aggregate room with no feasible placement is not admissible capacity")
	})

	t.Run("CommittedCapacityBlocksPlacement", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 100}, 2, 1000)

		// Fill ep-0 to the brim; 1000 tokens remain on ep-1, so 600 has one home.
		commit(t, f.ledger, "lease-0", "ep-0", Footprint{KVTokens: 1000, Slots: 1})
		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 600, Slots: 1}, 1.0))

		// With that hold outstanding the aggregate still covers a second 600, but
		// only ep-1 could host it and the pool-wide check sees 1000-600 left.
		err := f.ledger.TryAcquireHold("req-2", Footprint{KVTokens: 600, Slots: 1}, 1.0)
		require.ErrorIs(t, err, ErrPoolSaturated)
	})

	t.Run("CeilingScalesPoolNotEndpoints", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 5}, 2, 1000)

		// 10 slots pool-wide; a 0.5 ceiling holds back half of them.
		for i := range 5 {
			require.NoError(t, f.ledger.TryAcquireHold(fmt.Sprintf("lo-%d", i), Footprint{Slots: 1}, 0.5))
		}
		require.ErrorIs(t, f.ledger.TryAcquireHold("lo-5", Footprint{Slots: 1}, 0.5), ErrPoolSaturated)

		// A higher band still reaches the reserve: the ceiling is a band-scoped
		// holdback on the pool, not a hard limit on the ledger.
		require.NoError(t, f.ledger.TryAcquireHold("hi-0", Footprint{Slots: 1}, 1.0))
	})

	t.Run("ZeroCeilingAdmitsNothing", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 5}, 1, 1000)
		require.ErrorIs(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 0), ErrPoolSaturated)
	})

	t.Run("UngatedAxisNeverRefuses", func(t *testing.T) {
		t.Parallel()
		// Stage-2 configuration: slots gate, KV is shadow accounting.
		f := newFixture(t, Config{Gated: GatedAxes{Slots: true}, SlotsPerEndpoint: 4}, 1, 100)

		fp := Footprint{KVTokens: 1_000_000, Slots: 1}
		require.NoError(t, f.ledger.TryAcquireHold("req-1", fp, 1.0),
			"a KV claim far past the block pool must still admit while KV is in shadow")

		assert.Equal(t, int64(1_000_000), f.ledger.Snapshot().Held.KVTokens,
			"the shadow axis is booked and observable even though it does not gate")
	})

	t.Run("DuplicateHold", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0))
		require.ErrorIs(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0), ErrDuplicateHold)
		assert.Equal(t, int64(1), f.ledger.Snapshot().Held.Slots, "the refused duplicate must not book")
	})

	t.Run("ExpiredHoldIsSwept", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 1, HoldTTL: time.Second}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("stalled", Footprint{Slots: 1}, 1.0))
		require.ErrorIs(t, f.ledger.TryAcquireHold("next", Footprint{Slots: 1}, 1.0), ErrPoolSaturated)

		f.clock.Step(2 * time.Second)
		require.NoError(t, f.ledger.TryAcquireHold("next", Footprint{Slots: 1}, 1.0),
			"the TTL reclaims capacity from a scheduling stall")
		assert.Equal(t, 1, f.ledger.Snapshot().Holds)
	})
}

func TestPoolLedger_Commit(t *testing.T) {
	t.Parallel()

	t.Run("ConsumesHoldAndBooksLease", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 10_000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 5000, Slots: 1}, 1.0))
		out := commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 800, Slots: 1})

		assert.False(t, out.HoldMissing)
		assert.False(t, out.Escalated)
		assert.Equal(t, Footprint{KVTokens: 800, Slots: 1}, out.Booked)

		snap := f.ledger.Snapshot()
		assert.Equal(t, Footprint{}, snap.Held, "the hold is consumed, not left double-counted alongside the lease")
		assert.Equal(t, Footprint{KVTokens: 800, Slots: 1}, snap.Committed)
		assert.Equal(t, 1, snap.Leases)
	})

	t.Run("BooksWithoutAHold", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 1}, 1, 100)

		// A footprint no hold could ever have won. The request is already bound to
		// an endpoint, so the ledger records it rather than going blind to it.
		out := commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 99_999, Slots: 9})

		assert.True(t, out.HoldMissing)
		assert.Equal(t, Footprint{KVTokens: 99_999, Slots: 9}, f.ledger.Snapshot().Committed)
	})

	t.Run("EscalationIsObservedNotRefused", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 10_000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 100, Slots: 1}, 1.0))
		out := commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 500, Slots: 1})

		assert.True(t, out.Escalated)
		assert.Equal(t, Footprint{KVTokens: 500, Slots: 1}, f.ledger.Snapshot().Committed,
			"the escalated claim is booked in full; under-booking it would hide real occupancy")
	})

	t.Run("ExpiredHoldOverAdmitsByExactlyOneRequest", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 1, HoldTTL: time.Second}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("slow", Footprint{Slots: 1}, 1.0))
		f.clock.Step(2 * time.Second)
		require.NoError(t, f.ledger.TryAcquireHold("fast", Footprint{Slots: 1}, 1.0))

		out := commit(t, f.ledger, "slow", "ep-0", Footprint{Slots: 1})
		require.True(t, out.HoldMissing)

		// One slot of over-admission, bounded and self-correcting: the lease is
		// counted, so the next check already sees it.
		assert.Equal(t, int64(1), f.ledger.Snapshot().Committed.Slots)
		require.ErrorIs(t, f.ledger.TryAcquireHold("third", Footprint{Slots: 1}, 1.0), ErrPoolSaturated)
	})

	t.Run("UnknownEndpointIsRecordedButWinsNoFit", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 0, 0)

		commit(t, f.ledger, "req-1", "ghost", Footprint{KVTokens: 100, Slots: 1})

		snap := f.ledger.Snapshot()
		require.Len(t, snap.Endpoints, 1)
		assert.True(t, snap.Endpoints[0].Draining)
		assert.Equal(t, Footprint{}, snap.Endpoints[0].Limits)
		assert.Equal(t, Footprint{KVTokens: 100, Slots: 1}, snap.Committed)

		require.Error(t, f.ledger.TryAcquireHold("req-2", Footprint{Slots: 1}, 1.0),
			"an endpoint the pool has not vouched for must not attract new work")
	})
}

func TestPoolLedger_ReleaseIsZeroSum(t *testing.T) {
	t.Parallel()
	f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 10_000)

	require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 5000, Slots: 1}, 1.0))
	commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 800, Slots: 1})

	require.NoError(t, f.ledger.Release("req-1"))
	snap := f.ledger.Snapshot()
	assert.Equal(t, Footprint{}, snap.Committed, "a lease releases exactly what it committed")
	assert.Equal(t, 0, snap.Leases)

	require.NoError(t, f.ledger.Release("req-1"), "a second release is an idempotent no-op")
	require.NoError(t, f.ledger.Release("never-existed"))
	assert.Equal(t, Footprint{}, f.ledger.Snapshot().Committed)
}

func TestPoolLedger_RevokeAndRetire(t *testing.T) {
	t.Parallel()
	f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 1}, 1, 1000)

	commit(t, f.ledger, "victim", "ep-0", Footprint{KVTokens: 500, Slots: 1})
	require.NoError(t, f.ledger.Revoke("victim"))

	snap := f.ledger.Snapshot()
	assert.Equal(t, Footprint{}, snap.Committed)
	assert.Equal(t, Footprint{KVTokens: 500, Slots: 1}, snap.Reclaiming)
	require.ErrorIs(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0), ErrPoolSaturated,
		"reclaiming capacity stays unavailable until the engine acknowledges the free")

	require.ErrorIs(t, f.ledger.Revoke("victim"), ErrLeaseNotFound, "a reclaiming lease cannot be revoked twice")
	require.ErrorIs(t, f.ledger.Retire("absent"), ErrLeaseNotFound)

	require.NoError(t, f.ledger.Retire("victim"))
	assert.Equal(t, Footprint{}, f.ledger.Snapshot().Reclaiming)
	assert.Equal(t, 0, f.ledger.Snapshot().Leases)
	require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0))
}

// TestPoolLedger_PrefillAxis covers the axis whose claim ends at first token
// rather than at end of stream. The two residency axes cannot express it: TTFT is
// a time quantity, and two pools with identical KV occupancy differ in TTFT by
// how much of that occupancy is still un-prefilled.
func TestPoolLedger_PrefillAxis(t *testing.T) {
	t.Parallel()

	prefillGated := Config{
		Gated:                    GatedAxes{PrefillTokens: true, Slots: true},
		SlotsPerEndpoint:         100,
		PrefillTokensPerEndpoint: 1000,
	}

	t.Run("ReleasedAtFirstTokenLeavingResidencyHeld", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, prefillGated, 1, 10_000)

		commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 800, PrefillTokens: 800, Slots: 1})
		require.NoError(t, f.ledger.ReleasePrefill("req-1"))

		assert.Equal(t, Footprint{KVTokens: 800, Slots: 1}, f.ledger.Snapshot().Committed,
			"first token ends the backlog claim; the residency claim runs to end of stream")

		require.NoError(t, f.ledger.Release("req-1"))
		assert.Equal(t, Footprint{}, f.ledger.Snapshot().Committed,
			"the early release is deducted from the lease, so the protocol stays zero-sum")
	})

	t.Run("RepeatReleaseIsANoOp", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, prefillGated, 1, 10_000)

		commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 800, PrefillTokens: 800, Slots: 1})
		require.NoError(t, f.ledger.ReleasePrefill("req-1"))
		require.NoError(t, f.ledger.ReleasePrefill("req-1"),
			"a duplicated first-token signal must not double-refund")
		require.NoError(t, f.ledger.ReleasePrefill("never-existed"))

		assert.Equal(t, Footprint{KVTokens: 800, Slots: 1}, f.ledger.Snapshot().Committed)
	})

	t.Run("BacklogRefusesWithResidencyRoomToSpare", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, prefillGated, 1, 10_000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 600, PrefillTokens: 600, Slots: 1}, 1.0))
		err := f.ledger.TryAcquireHold("req-2", Footprint{KVTokens: 600, PrefillTokens: 600, Slots: 1}, 1.0)
		require.ErrorIs(t, err, ErrPoolSaturated,
			"KV and slots both have room; the prefill backlog is what refuses")
	})

	t.Run("FirstTokenReopensAdmission", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, prefillGated, 1, 10_000)

		commit(t, f.ledger, "resident", "ep-0", Footprint{KVTokens: 800, PrefillTokens: 800, Slots: 1})
		next := Footprint{KVTokens: 500, PrefillTokens: 500, Slots: 1}
		require.ErrorIs(t, f.ledger.TryAcquireHold("req-2", next, 1.0), ErrPoolSaturated)

		require.NoError(t, f.ledger.ReleasePrefill("resident"))
		require.NoError(t, f.ledger.TryAcquireHold("req-2", next, 1.0),
			"draining the backlog admits again while the resident request keeps its KV")
	})

	t.Run("ShadowPrefillNeverRefuses", func(t *testing.T) {
		t.Parallel()
		// Stage-2 configuration: prefill is booked and observable, but only slots gate.
		f := newFixture(t, Config{
			Gated:                    GatedAxes{Slots: true},
			SlotsPerEndpoint:         4,
			PrefillTokensPerEndpoint: 10,
		}, 1, 10_000)

		fp := Footprint{KVTokens: 5000, PrefillTokens: 5000, Slots: 1}
		require.NoError(t, f.ledger.TryAcquireHold("req-1", fp, 1.0))
		assert.Equal(t, int64(5000), f.ledger.Snapshot().Held.PrefillTokens,
			"the shadow axis is booked and observable even though it does not gate")
	})
}

func TestPoolLedger_EndpointLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("UpsertRefreshesLimits", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 1000)

		f.ledger.UpsertEndpoint(EndpointCapacity{ID: "ep-0", KVTokens: 8000, BlockSizeTokens: testBlockSize})
		assert.Equal(t, Footprint{KVTokens: 8000, Slots: 4}, f.ledger.Snapshot().Limits,
			"a re-scrape refreshes capacity in place rather than adding a second entry")
	})

	t.Run("IdleEndpointIsDroppedOutright", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 2, 1000)

		f.ledger.DeleteEndpoint("ep-1")
		snap := f.ledger.Snapshot()
		assert.Len(t, snap.Endpoints, 1)
		assert.Equal(t, Footprint{KVTokens: 1000, Slots: 4}, snap.Limits)
	})

	t.Run("DeletingAnUnknownEndpointIsANoOp", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 1000)

		f.ledger.DeleteEndpoint("never-existed")
		assert.Equal(t, Footprint{KVTokens: 1000, Slots: 4}, f.ledger.Snapshot().Limits)
	})

	t.Run("EndpointWithLeasesDrains", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 2, 1000)
		commit(t, f.ledger, "req-1", "ep-1", Footprint{KVTokens: 300, Slots: 1})

		f.ledger.DeleteEndpoint("ep-1")

		snap := f.ledger.Snapshot()
		require.Len(t, snap.Endpoints, 2, "an endpoint carrying leases must not vanish from the roll-up")
		assert.Equal(t, Footprint{KVTokens: 1000, Slots: 4}, snap.Limits,
			"a draining endpoint contributes no capacity")
		assert.Equal(t, Footprint{KVTokens: 300, Slots: 1}, snap.Committed,
			"its work still occupies real hardware, so it still counts as used")
		assert.Equal(t, Footprint{}, f.ledger.EndpointAvailable("ep-1"),
			"a draining endpoint offers no room")

		require.NoError(t, f.ledger.Release("req-1"))
		assert.Len(t, f.ledger.Snapshot().Endpoints, 1, "the entry is dropped once the last lease ends")
	})

	t.Run("ReturningEndpointStopsDraining", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 2, 1000)
		commit(t, f.ledger, "req-1", "ep-1", Footprint{KVTokens: 300, Slots: 1})

		f.ledger.DeleteEndpoint("ep-1")
		f.ledger.UpsertEndpoint(EndpointCapacity{ID: "ep-1", KVTokens: 1000, BlockSizeTokens: testBlockSize})

		snap := f.ledger.Snapshot()
		assert.Equal(t, Footprint{KVTokens: 2000, Slots: 8}, snap.Limits)
		for _, ep := range snap.Endpoints {
			assert.False(t, ep.Draining, "endpoint %s", ep.ID)
		}

		// The flapped endpoint's lease survived the round trip intact.
		require.NoError(t, f.ledger.Release("req-1"))
		assert.Equal(t, Footprint{}, f.ledger.Snapshot().Committed)
		assert.Len(t, f.ledger.Snapshot().Endpoints, 2)
	})

	t.Run("EndpointAvailableNetsOutHoldsNowhereButLeases", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{KVTokens: 100, Slots: 1}, 1.0))
		assert.Equal(t, Footprint{KVTokens: 1000, Slots: 4}, f.ledger.EndpointAvailable("ep-0"),
			"a hold reserves pool capacity but is not yet placed on any endpoint")

		commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 100, Slots: 1})
		assert.Equal(t, Footprint{KVTokens: 900, Slots: 3}, f.ledger.EndpointAvailable("ep-0"),
			"the commit places the claim, and the endpoint view reflects it")

		assert.Equal(t, Footprint{}, f.ledger.EndpointAvailable("ghost"))
	})
}

func TestPoolLedger_Saturation(t *testing.T) {
	t.Parallel()

	t.Run("EmptyPoolIsSaturated", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 0, 0)
		assert.Equal(t, 1.0, f.ledger.Saturation(), "no capacity must read as saturated, not as idle")
	})

	t.Run("MaxOverGatedAxes", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: 4}, 1, 1000)

		commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 100, Slots: 2})
		assert.InDelta(t, 0.5, f.ledger.Saturation(), 1e-9, "slots at 2/4 dominate KV at 100/1000")
	})

	t.Run("ShadowAxisDoesNotDriveSaturation", func(t *testing.T) {
		t.Parallel()
		// Saturation feeds UsageLimitPolicy, whose ceiling feeds the gate. An ungated
		// axis reaching into it would gate through the back door.
		f := newFixture(t, Config{Gated: GatedAxes{Slots: true}, SlotsPerEndpoint: 4}, 1, 1000)

		commit(t, f.ledger, "req-1", "ep-0", Footprint{KVTokens: 100_000, Slots: 1})
		assert.InDelta(t, 0.25, f.ledger.Saturation(), 1e-9)
	})

	t.Run("HoldsCount", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, Config{Gated: GatedAxes{Slots: true}, SlotsPerEndpoint: 4}, 1, 1000)

		require.NoError(t, f.ledger.TryAcquireHold("req-1", Footprint{Slots: 1}, 1.0))
		assert.InDelta(t, 0.25, f.ledger.Saturation(), 1e-9,
			"a request between the gate and its endpoint is in flight, not idle")
	})
}

// TestPoolLedger_ConcurrentAdmission covers the property the admission mutex
// exists for: concurrent holds must neither over-admit past the limit nor
// mutually inflate the bound and all reject.
func TestPoolLedger_ConcurrentAdmission(t *testing.T) {
	t.Parallel()
	const (
		slots   = 8
		workers = 64
	)
	f := newFixture(t, Config{Gated: bothAxes, SlotsPerEndpoint: slots}, 1, 1_000_000)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		admitted []string
	)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("req-%d", i)
			if err := f.ledger.TryAcquireHold(id, Footprint{KVTokens: 10, Slots: 1}, 1.0); err != nil {
				return
			}
			mu.Lock()
			admitted = append(admitted, id)
			mu.Unlock()
		}()
	}
	wg.Wait()

	require.Len(t, admitted, slots, "the pool must fill exactly, with no over-admission and no spurious rejection")
	assert.Equal(t, int64(slots), f.ledger.Snapshot().Held.Slots)

	for _, id := range admitted {
		commit(t, f.ledger, id, "ep-0", Footprint{KVTokens: 10, Slots: 1})
	}
	assert.Equal(t, Footprint{}, f.ledger.Snapshot().Held)

	wg = sync.WaitGroup{}
	for _, id := range admitted {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, f.ledger.Release(id))
		}()
	}
	wg.Wait()

	snap := f.ledger.Snapshot()
	assert.Equal(t, Footprint{}, snap.Committed, "the ledger must return to zero, not to a drifted residue")
	assert.Equal(t, 0, snap.Leases)
}
