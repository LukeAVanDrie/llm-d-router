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
	"time"

	"k8s.io/utils/clock"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/capacity"
)

// defaultHoldTTL bounds the scheduling window a hold covers. Guess: the window
// runs from the dispatch decision to PreRequest, whose cost is dominated by
// tokenization and scoring. The rate of commits reporting HoldMissing is the
// measurement that sizes this.
const defaultHoldTTL = 5 * time.Second

// LeaseState is the lifecycle position of a committed claim.
type LeaseState int

const (
	// LeaseCommitted holds capacity on an endpoint.
	LeaseCommitted LeaseState = iota
	// LeaseReclaiming is released by the EPP but not yet acknowledged freed by the
	// engine. Reclaiming capacity stays unavailable: engine-side block frees are
	// deferred while an in-flight step may still write the blocks.
	LeaseReclaiming
)

// LeaseSpec is the caller-provided identity and provenance of a lease at commit
// time. Prediction carries the scheduling-time truth-up (actual cached tokens).
type LeaseSpec struct {
	FlowID     string
	Priority   int
	Model      string
	Prediction Prediction
}

// LeaseRecord is the ledger's record of one committed claim. Termination
// provenance (cause, age) joins the record when the plugin layer can observe it.
type LeaseRecord struct {
	LeaseSpec
	RequestID string
	Endpoint  string
	// Booked is what the commit actually added to the endpoint, and what every
	// termination path subtracts. Recording it rather than re-deriving it is what
	// makes the protocol zero-sum: engine geometry can change under a live lease.
	Booked    Footprint
	Committed EngineFootprint
	State     LeaseState
}

// CommitOutcome reports what a commit recorded. A commit always books: by the time
// it runs the request is bound to an endpoint, so refusing to record it would
// leave real occupancy invisible. The flags are observations, not verdicts.
type CommitOutcome struct {
	// Booked is the logical-unit claim added to the endpoint.
	Booked Footprint
	// HoldMissing reports that no live hold backed this commit: it expired, was
	// swept, or was never taken. The lease is booked regardless, so the pool
	// over-admits by this request until it releases. The rate of this flag sizes
	// the hold TTL.
	HoldMissing bool
	// Escalated reports a booking larger than its hold in some dimension. The gate
	// holds a pessimistic bound that the commit truths down, so this is an
	// assertion on the translation, not a condition the protocol expects.
	Escalated bool
}

// EndpointSnapshot is a point-in-time logical-unit view of one endpoint ledger.
type EndpointSnapshot struct {
	ID         string
	Limits     Footprint
	Committed  Footprint
	Reclaiming Footprint
	Leases     int
	Draining   bool
}

// PoolSnapshot is a point-in-time view of the roll-up. It carries every axis
// whether or not it gates, so shadow axes are observable.
type PoolSnapshot struct {
	Limits     Footprint
	Committed  Footprint
	Reclaiming Footprint
	Held       Footprint
	Holds      int
	Leases     int
	Endpoints  []EndpointSnapshot
}

// Config parameterizes the pool ledger.
type Config struct {
	// Gated selects which axes refuse admission. Zero value gates nothing, which
	// runs the ledger as pure accounting.
	Gated GatedAxes
	// SlotsPerEndpoint is the configured concurrent-sequence limit per replica.
	// It is configuration rather than telemetry because vLLM v0.26.0 exports no
	// max_num_seqs metric: its only config info metrics are vllm:cache_config_info
	// and vllm:lora_requests_info.
	SlotsPerEndpoint int64
	// PrefillTokensPerEndpoint bounds the prompt backlog one replica may carry.
	// The engine enforces a per-iteration token budget, which is a rate; the stock
	// this bounds is the queue behind that rate, so the limit is Little's law:
	// prefill throughput multiplied by the TTFT budget. Zero disables the axis.
	PrefillTokensPerEndpoint int64
	// HoldTTL bounds how long a hold reserves capacity before it is swept.
	HoldTTL time.Duration
}

// endpointLimits is the per-endpoint limit vector: the scraped KV axis joined
// with the two configured axes.
func (c Config) endpointLimits(kvTokens int64) Footprint {
	return Footprint{
		KVTokens:      kvTokens,
		PrefillTokens: c.PrefillTokensPerEndpoint,
		Slots:         c.SlotsPerEndpoint,
	}
}

// endpointLedger is the accounting for one replica.
type endpointLedger struct {
	id              string
	limits          Footprint
	blockSizeTokens int64
	committed       Footprint // logical units, incrementally maintained
	reclaiming      Footprint
	leases          int
	// draining marks an endpoint absent from the pool that still carries leases.
	// Its limits are zero so it wins no fit check, but its committed capacity
	// stays in the roll-up until those leases end.
	draining bool
}

func (e *endpointLedger) available() Footprint {
	avail, err := e.limits.Sub(e.committed.Add(e.reclaiming))
	if err != nil {
		// Usage exceeds limits (a shrunk limit, or a draining endpoint): no room.
		return Footprint{}
	}
	return avail
}

type holdEntry struct {
	held      Footprint
	expiresAt time.Time
}

// PoolLedger is the pool-scope admission authority: the endpoint table, the holds
// table, the lease table, and the roll-up
// Available = sum(limits) - sum(committed) - sum(holds) - sum(reclaiming).
//
// It is a core service owned by flow control, not a plugin. Plugins reach its
// read face through the Reader interface published on the plugin handle.
//
// Holds and leases are keyed by request ID. The request already carries that ID
// end to end, so no receipt has to be threaded through the queue, the admission
// controller, and the director to connect the three phases.
//
// One mutex serializes every state transition. The admission path's check-and-add
// runs under it, so concurrent requests cannot mutually inflate the bound and
// reject themselves.
type PoolLedger struct {
	clock      clock.PassiveClock
	translator Translator
	cfg        Config

	mu        sync.Mutex
	endpoints map[string]*endpointLedger
	holds     map[string]*holdEntry
	leases    map[string]*LeaseRecord
	held      Footprint // incremental sum over holds
	limits    Footprint // sum over non-draining endpoint limits
}

var _ capacity.Ledger = (*PoolLedger)(nil)

// NewPoolLedger creates an empty pool ledger.
func NewPoolLedger(clk clock.PassiveClock, translator Translator, cfg Config) *PoolLedger {
	if translator == nil {
		translator = TokenTranslator{}
	}
	if cfg.HoldTTL <= 0 {
		cfg.HoldTTL = defaultHoldTTL
	}
	return &PoolLedger{
		clock:      clk,
		translator: translator,
		cfg:        cfg,
		endpoints:  make(map[string]*endpointLedger),
		holds:      make(map[string]*holdEntry),
		leases:     make(map[string]*LeaseRecord),
	}
}

// Translator exposes the configured translation so callers can build footprints
// in the same units the ledger books.
func (l *PoolLedger) Translator() Translator { return l.translator }

// UpsertEndpoint admits an endpoint to the pool or refreshes its capacity. It is
// driven by endpoint lifecycle events, so the ledger never holds its own copy of
// the endpoint set to diff against.
func (l *PoolLedger) UpsertEndpoint(ec EndpointCapacity) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ep, ok := l.endpoints[ec.ID]
	if !ok {
		ep = &endpointLedger{id: ec.ID}
		l.endpoints[ec.ID] = ep
	}
	ep.limits = l.cfg.endpointLimits(ec.KVTokens)
	ep.blockSizeTokens = max(ec.BlockSizeTokens, 1)
	ep.draining = false
	l.refreshLimitsLocked()
}

// DeleteEndpoint removes an endpoint from the pool. An endpoint still carrying
// leases drains rather than vanishing: that work still occupies real hardware, so
// its committed capacity stays in the roll-up while its zeroed limits keep it out
// of every fit check. The entry is dropped once its last lease ends.
func (l *PoolLedger) DeleteEndpoint(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ep, ok := l.endpoints[id]
	if !ok {
		return
	}
	if ep.leases == 0 {
		delete(l.endpoints, id)
	} else {
		ep.draining = true
		ep.limits = Footprint{}
	}
	l.refreshLimitsLocked()
}

// EndpointAvailable reports unclaimed capacity on one endpoint.
func (l *PoolLedger) EndpointAvailable(id string) Footprint {
	l.mu.Lock()
	defer l.mu.Unlock()

	ep, ok := l.endpoints[id]
	if !ok {
		return Footprint{}
	}
	return ep.available()
}

// refreshLimitsLocked recomputes the roll-up from the endpoint table. Recomputing
// rather than adjusting incrementally keeps the sum exact across limit changes on
// endpoints that are simultaneously draining.
func (l *PoolLedger) refreshLimitsLocked() {
	l.limits = Footprint{}
	for _, ep := range l.endpoints {
		l.limits = l.limits.Add(ep.limits)
	}
}

// TryAcquireHold is the admission decision, and the only ledger operation that can
// refuse. It reserves fp under reqID until the hold TTL elapses or the hold is
// committed or released.
//
// Two checks must pass on the gated axes: the pool-wide aggregate must cover the
// footprint at this band's ceiling, AND at least one endpoint must individually
// fit it. Aggregate room with no feasible placement is not admissible capacity.
// The ceiling scales the pool limits only, so a usage-limit policy reserves a
// fraction of pool capacity without constraining any single replica.
func (l *PoolLedger) TryAcquireHold(reqID string, fp Footprint, ceiling float64) error {
	now := l.clock.Now()
	gated := l.cfg.Gated.gate(fp)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepExpiredLocked(now)

	if _, ok := l.holds[reqID]; ok {
		return fmt.Errorf("%w: %q", ErrDuplicateHold, reqID)
	}
	if len(l.endpoints) == 0 {
		return ErrNoEndpoints
	}

	if ceiling < 0 {
		ceiling = 0
	}
	scaled := Footprint{
		KVTokens:      int64(float64(l.limits.KVTokens) * ceiling),
		PrefillTokens: int64(float64(l.limits.PrefillTokens) * ceiling),
		Slots:         int64(float64(l.limits.Slots) * ceiling),
	}
	avail, err := scaled.Sub(l.usedLocked())
	if err != nil {
		avail = Footprint{}
	}
	if !gated.Fits(avail) {
		return fmt.Errorf("%w: footprint %v, available %v at ceiling %.3f", ErrPoolSaturated, fp, avail, ceiling)
	}

	fits := false
	for _, ep := range l.endpoints {
		if gated.Fits(ep.available()) {
			fits = true
			break
		}
	}
	if !fits {
		return fmt.Errorf("%w: footprint %v", ErrNoEndpointFits, fp)
	}

	l.holds[reqID] = &holdEntry{held: fp, expiresAt: now.Add(l.cfg.HoldTTL)}
	l.held = l.held.Add(fp)
	return nil
}

// ReleaseHold refunds an unconsumed hold. Releasing a hold that was already
// committed, released, or swept is a no-op.
func (l *PoolLedger) ReleaseHold(reqID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropHoldLocked(reqID)
}

// Commit consumes the hold and books the lease on the endpoint. It always books:
// the request is bound to an endpoint by the time this runs, so the only choice is
// whether the ledger sees that occupancy or is blind to it. Anomalies (a missing
// hold, a booking that exceeds its hold) are reported in the outcome.
//
// An endpoint not yet reconciled is created with zero limits, which records the
// lease while keeping the endpoint out of fit checks until a reconcile supplies
// its capacity.
func (l *PoolLedger) Commit(reqID, endpointID string, spec LeaseSpec) CommitOutcome {
	now := l.clock.Now()

	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepExpiredLocked(now)

	hold, hadHold := l.holds[reqID]
	ep, ok := l.endpoints[endpointID]
	if !ok {
		ep = &endpointLedger{id: endpointID, blockSizeTokens: 1, draining: true}
		l.endpoints[endpointID] = ep
	}

	spec.Prediction.BlockSize = ep.blockSizeTokens
	booked := l.translator.ToFootprint(spec.Prediction)

	out := CommitOutcome{Booked: booked, HoldMissing: !hadHold}
	if hadHold {
		out.Escalated = !booked.Fits(hold.held)
	}

	l.dropHoldLocked(reqID)
	l.leases[reqID] = &LeaseRecord{
		LeaseSpec: spec,
		RequestID: reqID,
		Endpoint:  endpointID,
		Booked:    booked,
		Committed: l.translator.ToEngineFootprint(spec.Prediction),
		State:     LeaseCommitted,
	}
	ep.committed = ep.committed.Add(booked)
	ep.leases++
	return out
}

// ReleasePrefill frees the prompt-backlog claim at first token. The prefill axis
// measures work queued ahead of TTFT, so it ends when the first token proves the
// prompt is prefilled, not at end of stream with the residency axes.
//
// The released amount is deducted from the lease's Booked vector, so the eventual
// Release still subtracts exactly what remains outstanding and the protocol stays
// zero-sum. Repeat calls are no-ops, which keeps a retried or duplicated
// first-token signal harmless.
func (l *PoolLedger) ReleasePrefill(reqID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lease, ok := l.leases[reqID]
	if !ok || lease.Booked.PrefillTokens == 0 {
		return nil
	}
	ep, ok := l.endpoints[lease.Endpoint]
	if !ok {
		return nil
	}

	freed := Footprint{PrefillTokens: lease.Booked.PrefillTokens}
	var err error
	switch lease.State {
	case LeaseCommitted:
		ep.committed, err = ep.committed.Sub(freed)
	case LeaseReclaiming:
		ep.reclaiming, err = ep.reclaiming.Sub(freed)
	}
	if err != nil {
		return fmt.Errorf("releasing prefill for lease %q on %q: %w", reqID, lease.Endpoint, err)
	}
	lease.Booked.PrefillTokens = 0
	return nil
}

// Release frees a lease at end of stream: zero-sum, it releases exactly what the
// ledger recorded at commit. Releasing an unknown lease is an idempotent no-op, so
// the explicit end-of-stream release and any janitor fallback can both fire. A
// non-nil error is ledger corruption, not a caller error.
func (l *PoolLedger) Release(reqID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lease, ok := l.leases[reqID]
	if !ok {
		return nil
	}
	ep, ok := l.endpoints[lease.Endpoint]
	if !ok {
		delete(l.leases, reqID)
		return nil
	}

	var err error
	switch lease.State {
	case LeaseCommitted:
		ep.committed, err = ep.committed.Sub(lease.Booked)
	case LeaseReclaiming:
		ep.reclaiming, err = ep.reclaiming.Sub(lease.Booked)
	}
	if err != nil {
		return fmt.Errorf("releasing lease %q on %q: %w", reqID, lease.Endpoint, err)
	}

	delete(l.leases, reqID)
	l.dropLeaseLocked(ep)
	return nil
}

// Revoke is a forced release with different accounting: the lease moves from
// committed to reclaiming and its capacity stays unavailable until Retire
// acknowledges that the engine actually freed it.
func (l *PoolLedger) Revoke(reqID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lease, ok := l.leases[reqID]
	if !ok || lease.State != LeaseCommitted {
		return fmt.Errorf("%w: %q", ErrLeaseNotFound, reqID)
	}
	ep, ok := l.endpoints[lease.Endpoint]
	if !ok {
		return fmt.Errorf("%w: %q", ErrLeaseNotFound, reqID)
	}

	next, err := ep.committed.Sub(lease.Booked)
	if err != nil {
		return fmt.Errorf("revoking lease %q on %q: %w", reqID, lease.Endpoint, err)
	}
	ep.committed = next
	ep.reclaiming = ep.reclaiming.Add(lease.Booked)
	lease.State = LeaseReclaiming
	return nil
}

// Retire acknowledges that the engine freed a reclaiming lease's capacity. The
// acknowledgment channel (engine completion and abort counters per scrape) is a
// later stage; stage-2 callers invoke this directly.
func (l *PoolLedger) Retire(reqID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lease, ok := l.leases[reqID]
	if !ok || lease.State != LeaseReclaiming {
		return fmt.Errorf("%w: %q", ErrLeaseNotFound, reqID)
	}
	ep, ok := l.endpoints[lease.Endpoint]
	if !ok {
		delete(l.leases, reqID)
		return nil
	}

	next, err := ep.reclaiming.Sub(lease.Booked)
	if err != nil {
		return fmt.Errorf("retiring lease %q on %q: %w", reqID, lease.Endpoint, err)
	}
	ep.reclaiming = next
	delete(l.leases, reqID)
	l.dropLeaseLocked(ep)
	return nil
}

// Saturation is the derived, backwards-compatible view the usage-limit policy
// reads: the max over gated dimensions of used/limit. Ungated axes are excluded
// because a shadow axis driving the ceiling down would gate through the back door.
// An empty or zero-limit pool reads as saturated, matching the fail-closed
// convention of the saturation detectors this supersedes.
func (l *PoolLedger) Saturation() float64 {
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepExpiredLocked(now)

	limits := l.cfg.Gated.gate(l.limits)
	used := l.usedLocked()

	sat := 0.0
	gatedAny := false
	for _, axis := range []struct{ limit, used int64 }{
		{limits.KVTokens, used.KVTokens},
		{limits.PrefillTokens, used.PrefillTokens},
		{limits.Slots, used.Slots},
	} {
		if axis.limit <= 0 {
			continue
		}
		gatedAny = true
		sat = max(sat, float64(axis.used)/float64(axis.limit))
	}
	if !gatedAny {
		return 1.0
	}
	return sat
}

// Snapshot returns a point-in-time copy of the roll-up for debugging and tests.
// Operational observability of per-endpoint capacity comes from the Reader view
// published on the plugin handle, not from polling this.
func (l *PoolLedger) Snapshot() PoolSnapshot {
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweepExpiredLocked(now)

	s := PoolSnapshot{
		Limits: l.limits,
		Held:   l.held,
		Holds:  len(l.holds),
		Leases: len(l.leases),
	}
	for _, ep := range l.endpoints {
		s.Committed = s.Committed.Add(ep.committed)
		s.Reclaiming = s.Reclaiming.Add(ep.reclaiming)
		s.Endpoints = append(s.Endpoints, EndpointSnapshot{
			ID:         ep.id,
			Limits:     ep.limits,
			Committed:  ep.committed,
			Reclaiming: ep.reclaiming,
			Leases:     ep.leases,
			Draining:   ep.draining,
		})
	}
	return s
}

func (l *PoolLedger) usedLocked() Footprint {
	used := l.held
	for _, ep := range l.endpoints {
		used = used.Add(ep.committed).Add(ep.reclaiming)
	}
	return used
}

// dropLeaseLocked accounts one lease ending, and drops a drained endpoint once its
// last lease ends.
func (l *PoolLedger) dropLeaseLocked(ep *endpointLedger) {
	ep.leases--
	if ep.draining && ep.leases <= 0 {
		delete(l.endpoints, ep.id)
	}
}

// sweepExpiredLocked drops holds whose scheduling window has elapsed. The TTL
// reclaims capacity from stalled scheduling; the request behind an expired hold
// still commits, and that commit reports HoldMissing.
func (l *PoolLedger) sweepExpiredLocked(now time.Time) {
	for id, h := range l.holds {
		if now.After(h.expiresAt) {
			l.dropHoldLocked(id)
		}
	}
}

func (l *PoolLedger) dropHoldLocked(id string) {
	h, ok := l.holds[id]
	if !ok {
		return
	}
	delete(l.holds, id)
	next, err := l.held.Sub(h.held)
	if err != nil {
		// The incremental sum is maintained under this mutex, so a negative
		// result means the sum and the table diverged; rebuild from the table.
		next = Footprint{}
		for _, e := range l.holds {
			next = next.Add(e.held)
		}
	}
	l.held = next
}

// defaultSlotsPerEndpoint is vLLM's own default for max_num_seqs. The engine
// exports no metric for it, so the ledger cannot scrape the real value and an
// operator running a non-default engine must supply it.
const defaultSlotsPerEndpoint = 256

// DefaultConfig is the accounting-first configuration: slots carry admission
// authority, and the two token axes are booked and exported without refusing.
//
// KV stays in shadow because the deterministic translator books the client's
// output ceiling rather than a calibrated bound, so gating on it would refuse
// admission far below true occupancy. Prefill stays in shadow because its limit
// is a TTFT budget nobody has stated yet, which zero leaves disabled.
func DefaultConfig() Config {
	return Config{
		Gated:            GatedAxes{Slots: true},
		SlotsPerEndpoint: defaultSlotsPerEndpoint,
		HoldTTL:          defaultHoldTTL,
	}
}
