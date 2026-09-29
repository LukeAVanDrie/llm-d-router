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

package capacityledger

import (
	"context"
	"math"

	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// holdCause is why a gated request fit no candidate: reserved when some candidate would fit but for
// other requests' reservations, else capacity when some candidate is over capacity, else stale.
type holdCause string

const (
	holdReserved holdCause = "reserved"
	holdCapacity holdCause = "capacity"
	holdStale    holdCause = "stale"
)

// Gate checks each candidate endpoint for the request at the head of a band and reserves the
// request's demand on the candidate it fits with the most headroom. It never withholds the request:
// when the request fits no candidate, it dispatches without a reservation and counts in
// would_hold_total, or in unaccounted_total when the ledger can account for no candidate. Flow
// control serializes Gate calls, so each sees the reservations of the ones before it.
func (l *Ledger) Gate(_ context.Context, req fwkfc.FlowControlRequest, candidates []fwkdl.Endpoint) fwkfc.GateDecision {
	l.gated.Store(true)
	ir := req.InferenceRequest()
	if ir != nil {
		if prev, ok := l.reservations.LoadAndDelete(ir); ok {
			prev.(*reservation).Refund()
		}
	}
	r := l.gateState(req)

	var witness reservedDemand
	best := math.Inf(-1)
	fitting := 0
	var reserved, overCapacity, stale bool
	for _, c := range candidates {
		e := l.lookup(c.GetMetadata())
		if e == nil {
			continue
		}
		s := l.snapshot(e)
		v, w := check(s, demand(r, s), vec{})
		l.metrics.gateCheck(v.label()).Inc()
		switch v.reason {
		case reasonFits:
			fitting++
			if room := headroom(s.used.add(s.acct.reserved), w, s.capacity); room > best {
				best, witness = room, reservedDemand{e: e, w: w}
			}
		case reasonOverReserved:
			reserved = true
		case reasonOverCapacity:
			overCapacity = true
		case reasonStale:
			stale = true
		}
	}
	l.metrics.fittingEndpoints.Observe(float64(fitting))
	switch {
	case fitting == 0 && reserved:
		l.metrics.wouldHold[holdReserved].Inc()
		return fwkfc.GateDecision{}
	case fitting == 0 && overCapacity:
		l.metrics.wouldHold[holdCapacity].Inc()
		return fwkfc.GateDecision{}
	case fitting == 0 && stale:
		l.metrics.wouldHold[holdStale].Inc()
		return fwkfc.GateDecision{}
	case fitting == 0:
		l.metrics.unaccounted.Inc()
		return fwkfc.GateDecision{}
	case ir == nil:
		return fwkfc.GateDecision{}
	}

	res := &reservation{witness: witness, createdAt: l.clock.Now()}
	res.onSettle = func() {
		l.reservations.CompareAndDelete(ir, res)
		l.metrics.reservations.Dec()
	}
	res.apply()
	l.reservations.Store(ir, res)
	l.metrics.reservations.Inc()
	return fwkfc.GateDecision{Reservation: res}
}

// gateState is the state of a request at dispatch. The prefix match is not known before
// scheduling, so the whole prompt counts as uncached. Only pre-tokenized requests carry a token
// count at dispatch; otherwise the request's size in bytes bounds the prompt.
func (l *Ledger) gateState(req fwkfc.FlowControlRequest) residentState {
	ir := req.InferenceRequest()
	prompts := promptLengths(ir, int64(req.ByteSize()))
	return residentState{prompts: prompts, uncachedPrompt: sum(prompts), n: l.clampedN(ir, prompts)}
}

// Filter rechecks the scheduler's candidates at placement with the request's exact prompt and
// prefix match, and records which the request fits; the outcome is counted when the request is
// placed. It never removes an endpoint.
func (l *Ledger) Filter(_ context.Context, request *fwksched.InferenceRequest,
	endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	v, ok := l.reservations.Load(request)
	if !ok {
		return endpoints
	}
	res := v.(*reservation)
	// Several profiles may run the filter for one request; the first run records the outcome.
	if !res.filtered.CompareAndSwap(false, true) {
		return endpoints
	}
	fitting := []types.NamespacedName{}
	for _, ep := range endpoints {
		e := l.lookup(ep.GetMetadata())
		if e == nil {
			continue
		}
		s := l.snapshot(e)
		// The request's own reservation does not count against its witness. A reservation settled
		// between the snapshot and this read may still be charged in the snapshot; the recheck then
		// counts it, which errs toward reporting a race.
		var own vec
		if res.witness.e == e && res.state.Load() == reservationActive {
			own = res.witness.w
		}
		if v, _ := check(s, demand(l.residentFor(request, ep), s), own); v.reason == reasonFits {
			fitting = append(fitting, e.id)
		}
	}
	res.fitting.Store(&fitting)
	return endpoints
}

// convertReservation settles the request's reservation once its lease is booked on e, and counts
// the placement's outcome. The lease is booked first, so the request is briefly counted twice
// rather than not at all.
func (l *Ledger) convertReservation(request *fwksched.InferenceRequest, e *endpoint, ls *lease) {
	v, ok := l.reservations.Load(request)
	if !ok {
		if l.gated.Load() {
			l.metrics.unreservedPlacements.Inc()
		}
		return
	}
	res := v.(*reservation)
	if !res.convert() {
		return
	}
	l.metrics.placementLatency.Observe(l.clock.Since(res.createdAt).Seconds())
	if race := res.race(e.id); race != "" {
		l.metrics.placementRaces.WithLabelValues(string(race)).Inc()
	}
	ls.mu.Lock()
	leased := ls.contrib
	ls.mu.Unlock()
	for _, a := range []axis{axisMemory, axisStep} {
		if res.witness.w[a] > 0 {
			l.metrics.reservationToLease[a].Observe(float64(leased[a]) / float64(res.witness.w[a]))
		}
	}
}

// reapReservations releases reservations that were never dispatched and have outlived the
// reservation TTL. Flow control refunds or binds every reservation it receives, so a reaped
// reservation means a caller that called neither Dispatched nor Refund. A dispatched reservation is
// released by its request's context instead.
func (l *Ledger) reapReservations() {
	now := l.clock.Now()
	l.reservations.Range(func(_, v any) bool {
		res := v.(*reservation)
		if now.Sub(res.createdAt) > l.cfg.reservationTTL && !res.isDispatched() && res.settle(reservationRefunded) {
			l.metrics.reservationsReaped.Inc()
		}
		return true
	})
}

// lookup returns the ledger's state for an endpoint, or nil.
func (l *Ledger) lookup(meta *fwkdl.EndpointMetadata) *endpoint {
	if meta == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.endpoints[meta.ID.String()]
}
