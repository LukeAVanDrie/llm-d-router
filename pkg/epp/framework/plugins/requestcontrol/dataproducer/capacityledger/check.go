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
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
)

// reason classifies a check's outcome.
type reason uint8

const (
	// reasonFits means the request fits the endpoint on every axis, with the reservations that name
	// it.
	reasonFits reason = iota
	// reasonIneligible means the accounting does not hold for the endpoint.
	reasonIneligible
	// reasonStale means the endpoint's metrics are stale, which reads as full.
	reasonStale
	// reasonOverCapacity means the request does not fit the endpoint on some axis.
	reasonOverCapacity
	// reasonOverReserved means the request fits but for other requests' reservations.
	reasonOverReserved
)

// verdict is the outcome of checking one endpoint for one request.
type verdict struct {
	reason        reason
	axis          axis
	ineligibility attrcapacity.Eligibility
}

// label names the verdict for metrics: fits, stale, the ineligibility reason, the axis that does
// not fit, or reserved-<axis>.
func (v verdict) label() string {
	switch v.reason {
	case reasonFits:
		return "fits"
	case reasonStale:
		return string(attrcapacity.Stale)
	case reasonIneligible:
		return string(v.ineligibility)
	case reasonOverReserved:
		return "reserved-" + v.axis.String()
	}
	return v.axis.String()
}

// check evaluates X + w <= C on every axis for a request with demand w on an endpoint in state s,
// where X is the endpoint's used state, first alone and then with the reservations that name it.
// own is the request's own reservation on the endpoint, which does not count against it. It
// returns the verdict and, when the request fits, the demand to reserve.
func check(s snapshot, w, own vec) (verdict, vec) {
	switch s.elig {
	case attrcapacity.Eligible:
	case attrcapacity.Stale:
		return verdict{reason: reasonStale}, vec{}
	default:
		return verdict{reason: reasonIneligible, ineligibility: s.elig}, vec{}
	}
	if _, a, over := fit(s.used, w, s.capacity); over {
		return verdict{reason: reasonOverCapacity, axis: a}, vec{}
	}
	w, a, over := fit(s.used.add(s.acct.reserved.sub(own)), w, s.capacity)
	if over {
		return verdict{reason: reasonOverReserved, axis: a}, vec{}
	}
	return verdict{reason: reasonFits}, w
}

// fit reports the first axis on which x + w exceeds c. The step demand is first capped at the
// budget x leaves, and at least one token: vLLM gives a waiting request whatever step budget its
// running requests leave and prefills the rest of the prompt in later steps, so a prompt fits while
// any budget remains. It returns the capped demand.
func fit(x, w, c vec) (vec, axis, bool) {
	w[axisStep] = min(w[axisStep], max(c[axisStep]-x[axisStep], 1))
	a, over := x.add(w).exceeds(c)
	return w, a, over
}

// headroom is the room left on an endpoint in state x with capacity c after demand w, as the
// smallest fraction of capacity left on any axis.
func headroom(x, w, c vec) float64 {
	room := 1.0
	for a := range numAxes {
		if c[a] > 0 {
			room = min(room, float64(c[a]-x[a]-w[a])/float64(c[a]))
		}
	}
	return room
}

// demand is the footprint of a request in state r, not yet started, on an endpoint in state s,
// capped at the endpoint's capacity on every axis so an empty endpoint can take any single request.
func demand(r residentState, s snapshot) vec {
	w := footprint(r, s.geo)
	for a := range numAxes {
		w[a] = min(w[a], s.capacity[a])
	}
	return w
}
