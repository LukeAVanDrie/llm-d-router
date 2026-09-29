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
	"encoding/json"
	"sort"

	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
)

const maxDumpEndpoints = 100

type ledgerState struct {
	Endpoints      []endpointDump `json:"endpoints"`
	TotalEndpoints int            `json:"totalEndpoints"`
	MaxEndpoints   int            `json:"maxEndpoints"`
	Truncated      bool           `json:"truncated"`
}

type endpointDump struct {
	Endpoint string `json:"endpoint"`
	attrcapacity.EndpointCapacity
}

// DumpState exposes each endpoint's capacity view for the /debug/plugins/state endpoint, endpoints
// with the most used KV cache blocks first.
func (l *Ledger) DumpState() (json.RawMessage, error) {
	l.mu.Lock()
	endpoints := make([]*endpoint, 0, len(l.endpoints))
	for _, e := range l.endpoints {
		endpoints = append(endpoints, e)
	}
	l.mu.Unlock()

	state := ledgerState{
		TotalEndpoints: len(endpoints),
		MaxEndpoints:   maxDumpEndpoints,
		Endpoints:      make([]endpointDump, 0, len(endpoints)),
	}
	for _, e := range endpoints {
		state.Endpoints = append(state.Endpoints, endpointDump{Endpoint: e.id.String(), EndpointCapacity: *l.view(e)})
	}
	sort.Slice(state.Endpoints, func(i, j int) bool {
		a, b := state.Endpoints[i], state.Endpoints[j]
		if a.Memory.Used != b.Memory.Used {
			return a.Memory.Used > b.Memory.Used
		}
		return a.Endpoint < b.Endpoint
	})
	if len(state.Endpoints) > maxDumpEndpoints {
		state.Endpoints = state.Endpoints[:maxDumpEndpoints]
		state.Truncated = true
	}
	return json.Marshal(state)
}
