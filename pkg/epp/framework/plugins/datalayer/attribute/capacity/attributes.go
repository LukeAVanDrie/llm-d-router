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

// Package capacity exposes the per-endpoint capacity ledger reading to scheduling
// plugins as an endpoint attribute.
package capacity

import (
	fwkcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/capacity"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	capacityconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/capacityledger/constants"
)

// AvailableCapacityDataKey carries an endpoint's unclaimed capacity as accounted
// by the ledger. It is injected as a dynamic attribute, so every read resolves
// against ledger state at the moment of the read rather than a per-cycle snapshot.
var AvailableCapacityDataKey = plugin.NewDataKey("AvailableCapacityDataKey", capacityconstants.CapacityLedgerAdapterType)

// AvailableCapacity is the headroom the ledger will still admit on an endpoint.
//
// It differs from scraped utilization in what it counts: reservations the ledger
// has made but the engine has not yet reported, which is the entire reason the
// ledger exists. An endpoint reading as idle in metrics can have zero available
// capacity here.
type AvailableCapacity struct {
	fwkcapacity.Footprint
}

// Clone returns an independent copy. The embedded Footprint is all value fields,
// so the value-copy idiom covers it.
func (a *AvailableCapacity) Clone() fwkdl.Cloneable {
	if a == nil {
		return nil
	}
	cp := *a
	return &cp
}
