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

// Package capacity is the vocabulary of pool capacity accounting: the residency
// footprint and the read-only view of the ledger that owns it.
//
// It carries no dependencies so that both the plugin handle, which publishes the
// view, and flow control, which implements it, can import it. The accounting
// itself lives in pkg/epp/flowcontrol/ledger.
package capacity

import (
	"errors"
	"fmt"
)

// ErrUnderflow reports a Footprint subtraction that would go negative. It is
// surfaced as ledger corruption: correctness comes from zero-sum discipline (a
// lease releases exactly what it committed), so underflow is never clamped.
var ErrUnderflow = errors.New("footprint underflow")

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

// EndpointCapacity is one endpoint's scraped capacity. It carries only the KV
// axis: the slots and prefill limits are uniform configuration, so the ledger
// reads them from its own config rather than restamping them per endpoint per
// event.
type EndpointCapacity struct {
	ID string
	// KVTokens is the block-pool capacity in tokens: CacheNumBlocks * CacheBlockSize.
	KVTokens        int64
	BlockSizeTokens int64
}

// Reader is the read-only view of the capacity ledger.
type Reader interface {
	// EndpointAvailable reports unclaimed capacity on one endpoint. An unknown or
	// draining endpoint reports zero.
	EndpointAvailable(id string) Footprint
	// Saturation is the pool-wide gated utilization in [0,1]. A pool with no
	// gated capacity reads as saturated rather than idle.
	Saturation() float64
}

// EndpointSink receives endpoint lifecycle so the ledger learns what hardware
// backs the pool. Reporting hardware is not an admission decision, which is why
// it sits on the plugin-visible surface.
type EndpointSink interface {
	// UpsertEndpoint admits an endpoint or refreshes its capacity.
	UpsertEndpoint(ec EndpointCapacity)
	// DeleteEndpoint removes an endpoint. Occupancy already booked against it
	// survives until its leases end.
	DeleteEndpoint(id string)
}

// Ledger is the capacity ledger as published to plugins through the plugin
// handle. Plugins observe capacity and report the endpoints backing it; the
// reservation protocol is deliberately absent, so only flow control, which holds
// the concrete ledger, can hold or book against it.
type Ledger interface {
	Reader
	EndpointSink
}
