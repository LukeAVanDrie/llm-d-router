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

// Package ledger implements the capacity ledger: pool-scope accounting of the two
// engine-enforced residency stocks (KV tokens and sequence slots) as a two-phase
// hold-then-lease reservation protocol over per-endpoint ledgers that roll up to a
// pool ledger.
//
// The hold is the only operation that can refuse. Commit, Release, Revoke, and
// Retire record facts about requests that are already in flight; a ledger that
// declined to record real occupancy would be blind to it, which is the failure
// this package exists to eliminate.
package ledger

import (
	"errors"
	"fmt"
)

var (
	// ErrUnderflow reports a Footprint subtraction that would go negative. It is
	// surfaced as ledger corruption: correctness comes from zero-sum discipline (a
	// lease releases exactly what it committed), so underflow is never clamped.
	ErrUnderflow = errors.New("footprint underflow")

	// ErrPoolSaturated reports that the pool-wide aggregate check failed.
	ErrPoolSaturated = errors.New("pool capacity exhausted")

	// ErrNoEndpointFits reports that aggregate room exists but no single endpoint
	// can hold the footprint. Aggregate room with no feasible placement is not
	// admissible capacity.
	ErrNoEndpointFits = errors.New("no endpoint fits the footprint")

	// ErrNoEndpoints reports an empty pool. Holding against no capacity would
	// admit unboundedly, so an empty pool refuses.
	ErrNoEndpoints = errors.New("pool has no endpoints")

	// ErrDuplicateHold reports a hold acquisition under a request ID that already
	// holds.
	ErrDuplicateHold = errors.New("hold already exists")

	// ErrLeaseNotFound reports an operation against a lease the ledger does not
	// hold. Release treats it as an idempotent no-op; Revoke and Retire surface it.
	ErrLeaseNotFound = errors.New("lease not found")
)

// Footprint is the hardware-agnostic residency claim of one request. Pool-level
// admission reasons exclusively in these units.
type Footprint struct {
	// KVTokens is the block-pool residency held for the whole request lifetime.
	KVTokens int64
	// PrefillTokens is the prompt backlog: tokens admitted but not yet prefilled.
	// It is released at first token rather than at end of stream, so it is a
	// transient stock that the two lifetime-scoped axes cannot express.
	PrefillTokens int64
	// Slots is the concurrent-sequence claim.
	Slots int64
}

// Add returns the coordinate-wise sum.
func (f Footprint) Add(o Footprint) Footprint {
	return Footprint{
		KVTokens:      f.KVTokens + o.KVTokens,
		PrefillTokens: f.PrefillTokens + o.PrefillTokens,
		Slots:         f.Slots + o.Slots,
	}
}

// Sub returns the coordinate-wise difference. Underflow in any dimension returns
// ErrUnderflow; the result is not usable on error.
func (f Footprint) Sub(o Footprint) (Footprint, error) {
	r := Footprint{
		KVTokens:      f.KVTokens - o.KVTokens,
		PrefillTokens: f.PrefillTokens - o.PrefillTokens,
		Slots:         f.Slots - o.Slots,
	}
	if r.KVTokens < 0 || r.PrefillTokens < 0 || r.Slots < 0 {
		return Footprint{}, fmt.Errorf("%w: %v - %v", ErrUnderflow, f, o)
	}
	return r, nil
}

// Fits reports whether f fits within avail in every dimension.
func (f Footprint) Fits(avail Footprint) bool {
	return f.KVTokens <= avail.KVTokens &&
		f.PrefillTokens <= avail.PrefillTokens &&
		f.Slots <= avail.Slots
}

func (f Footprint) String() string {
	return fmt.Sprintf("{kv:%d prefill:%d slots:%d}", f.KVTokens, f.PrefillTokens, f.Slots)
}

// GatedAxes selects which residency axes carry admission authority. An axis that
// is not gated is booked, rolled up, and exported, but never refuses a hold.
//
// The split exists because the axes are known to different standards. Slots is
// booked from the request's branching factor, an exact quantity against an
// engine-enforced limit. KVTokens is booked from an output-length prediction that
// the deterministic ledger must round up to the client's ceiling, so gating on it
// refuses admission far below true occupancy until the stochastic layer supplies a
// calibrated bound. PrefillTokens is gated against a Little's-law bound derived
// from a TTFT budget, which is an operator intent rather than an engine-reported
// limit.
type GatedAxes struct {
	KVTokens      bool
	PrefillTokens bool
	Slots         bool
}

// gate zeroes the coordinates without admission authority. A zero coordinate fits
// any non-negative availability, so ungated axes cannot refuse.
func (g GatedAxes) gate(f Footprint) Footprint {
	if !g.KVTokens {
		f.KVTokens = 0
	}
	if !g.PrefillTokens {
		f.PrefillTokens = 0
	}
	if !g.Slots {
		f.Slots = 0
	}
	return f
}

// EngineFootprint is the engine-specific physical claim on one replica, recorded
// on the lease as provenance. The ledger never does arithmetic in these units: the
// translator is the sole owner of the tokens-to-blocks relationship, so a change
// of engine geometry cannot desynchronize a release from its commit.
//
// The stage-2 translation is token-denominated (one block per token);
// block-granular translation is a later stage.
type EngineFootprint struct {
	KVBlocks int64
	Slots    int64
}

// Prediction carries the per-request quantities the translation is computed from.
type Prediction struct {
	// PromptTokens is the ISL. At the gate this is the pessimistic bound (request
	// bytes upper-bound tokens, since every token spans at least one byte); at
	// commit it is the tokenized count.
	PromptTokens int64
	// OutputTokens is the output-side booking: the client's MaxOutputTokens
	// ceiling, or the operator-capped estimator when the client sets no ceiling.
	OutputTokens int64
	// CachedTokens is the prefix-cache hit, known at scheduling and zero at the gate.
	CachedTokens int64
	// Branching is the decode width. No request parser extracts best_of or n, so
	// this is one for all traffic; the field exists for the axis, not for a value
	// the EPP can currently read.
	Branching int64
	// BlockSize is the target endpoint's KV block size, injected by the ledger at
	// commit. It is zero at the gate, where no endpoint is chosen yet, and unused
	// by the token-denominated stage-2 translation.
	BlockSize int64
}

// Translator converts predictions into claims. It is a calibrated estimator, not
// block-exact math; estimation error is acknowledged by the reconciliation layer.
type Translator interface {
	ToFootprint(p Prediction) Footprint
	ToEngineFootprint(p Prediction) EngineFootprint
}

// TokenTranslator denominates engine blocks in tokens, so Footprint and
// EngineFootprint coincide numerically.
type TokenTranslator struct{}

var _ Translator = TokenTranslator{}

func (TokenTranslator) ToFootprint(p Prediction) Footprint {
	branching := max(p.Branching, 1)
	// A cached prefix is neither prefilled nor newly resident, so it discounts both
	// the residency claim and the prefill backlog.
	uncached := max(p.PromptTokens-p.CachedTokens, 0)
	return Footprint{
		KVTokens:      uncached + p.OutputTokens*branching,
		PrefillTokens: uncached,
		Slots:         branching,
	}
}

func (t TokenTranslator) ToEngineFootprint(p Prediction) EngineFootprint {
	fp := t.ToFootprint(p)
	return EngineFootprint{KVBlocks: fp.KVTokens, Slots: fp.Slots}
}
