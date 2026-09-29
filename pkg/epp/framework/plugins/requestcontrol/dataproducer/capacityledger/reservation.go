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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
)

const (
	reservationActive int32 = iota
	reservationConverted
	reservationRefunded
)

// raceCause is why a check at placement would have refused a gated request's placement.
type raceCause string

const (
	// raceRecheck means none of the scheduler's candidates fit the request at placement.
	raceRecheck raceCause = "recheck"
	// raceUnfitPlacement means the request was placed on an endpoint it did not fit at placement,
	// while another candidate did.
	raceUnfitPlacement raceCause = "unfit-placement"
	// raceUnfiltered means the ledger's filter did not run for the request, so its placement was
	// not rechecked.
	raceUnfiltered raceCause = "unfiltered"
)

var _ fwkfc.Reservation = (*reservation)(nil)

// reservedDemand is a reservation's demand on its endpoint.
type reservedDemand struct {
	e *endpoint
	w vec
}

// reservation is capacity reserved for a gated request between dispatch and placement, on one
// endpoint the request fits. It is a witness that the dispatched requests fit their endpoints at
// the same time; placement converts it to a lease on whichever endpoint the scheduler chooses.
type reservation struct {
	witness   reservedDemand
	createdAt time.Time
	// onSettle runs once, after the demand is removed.
	onSettle func()

	state atomic.Int32
	// filtered is set by the first filter run that examines the reservation; fitting is the
	// candidates that run found the request fits.
	filtered atomic.Bool
	fitting  atomic.Pointer[[]types.NamespacedName]

	mu         sync.Mutex
	dispatched bool
	// stop cancels the release armed by Dispatched.
	stop func() bool
}

// apply adds the reservation's demand to its endpoint's account.
func (r *reservation) apply() {
	r.witness.e.acct.apply(vec{}, r.witness.w)
}

// settle moves an active reservation to its final state and removes its demand. It reports whether
// this call settled it.
func (r *reservation) settle(to int32) bool {
	if !r.state.CompareAndSwap(reservationActive, to) {
		return false
	}
	r.witness.e.acct.apply(vec{}, vec{}.sub(r.witness.w))
	if r.onSettle != nil {
		r.onSettle()
	}
	return true
}

// Dispatched arms a release for when requestCtx ends before placement converts the reservation.
func (r *reservation) Dispatched(requestCtx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dispatched || r.state.Load() != reservationActive {
		return
	}
	r.dispatched = true
	r.stop = context.AfterFunc(requestCtx, r.Refund)
}

// Refund releases the reservation. It is a no-op once the reservation is converted or refunded, so
// the release armed by Dispatched and a refund by flow control can both call it.
func (r *reservation) Refund() { r.settle(reservationRefunded) }

// convert settles the reservation at placement. It reports false if it was already refunded.
func (r *reservation) convert() bool {
	if !r.settle(reservationConverted) {
		return false
	}
	r.mu.Lock()
	stop := r.stop
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
	return true
}

// isDispatched reports whether Dispatched has bound the reservation to its request.
func (r *reservation) isDispatched() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dispatched
}

// race is the reservation's placement outcome on the endpoint with the given ID, or "" when the
// placement would have been allowed.
func (r *reservation) race(id types.NamespacedName) raceCause {
	fitting := r.fitting.Load()
	switch {
	case fitting == nil:
		return raceUnfiltered
	case len(*fitting) == 0:
		return raceRecheck
	case !slices.Contains(*fitting, id):
		return raceUnfitPlacement
	}
	return ""
}
