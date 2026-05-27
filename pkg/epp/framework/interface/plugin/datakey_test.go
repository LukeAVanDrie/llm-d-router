/*
Copyright 2025 The Kubernetes Authors.

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

package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataKey_String(t *testing.T) {
	tests := []struct {
		name     string
		key      DataKey
		expected string
	}{
		{
			name:     "Unscoped uses DefaultProducerType",
			key:      NewDataKey("KeyA", "ProdTypeA"),
			expected: "KeyA/ProdTypeA",
		},
		{
			name:     "Scoped uses ProducerName",
			key:      NewDataKey("KeyA", "ProdTypeA").WithNonEmptyProducerName("ProdNameA"),
			expected: "KeyA/ProdNameA",
		},
		{
			name:     "Scoped with empty name does not override",
			key:      NewDataKey("KeyA", "ProdTypeA").WithNonEmptyProducerName(""),
			expected: "KeyA/ProdTypeA",
		},
		{
			name:     "Request scoped data key String",
			key:      NewRequestDataKey("KeyReq", "ProdReq"),
			expected: "KeyReq/ProdReq[Request]",
		},
		{
			name:     "Endpoint scoped data key String",
			key:      NewEndpointDataKey("KeyEp", "ProdEp"),
			expected: "KeyEp/ProdEp[Endpoint]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.key.String())
		})
	}
}

func TestDataKey_Scopes(t *testing.T) {
	tests := []struct {
		name          string
		key           DataKey
		expectedScope DataScope
	}{
		{
			name:          "NewDataKey default scope is UnspecifiedScope",
			key:           NewDataKey("KeyA", "ProdTypeA"),
			expectedScope: UnspecifiedScope,
		},
		{
			name:          "NewRequestDataKey scope is RequestScope",
			key:           NewRequestDataKey("KeyA", "ProdTypeA"),
			expectedScope: RequestScope,
		},
		{
			name:          "NewEndpointDataKey scope is EndpointScope",
			key:           NewEndpointDataKey("KeyA", "ProdTypeA"),
			expectedScope: EndpointScope,
		},
		{
			name:          "Zero value DataKey scope defaults to UnspecifiedScope",
			key:           DataKey{},
			expectedScope: UnspecifiedScope,
		},
		{
			name:          "Scope propagates through WithNonEmptyProducerName for RequestScope",
			key:           NewRequestDataKey("KeyA", "ProdTypeA").WithNonEmptyProducerName("ProdNameA"),
			expectedScope: RequestScope,
		},
		{
			name:          "Scope propagates through WithNonEmptyProducerName for EndpointScope",
			key:           NewEndpointDataKey("KeyA", "ProdTypeA").WithNonEmptyProducerName("ProdNameA"),
			expectedScope: EndpointScope,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedScope, tt.key.Scope())
		})
	}
}

func TestDataKey_EqualsIgnoringScope(t *testing.T) {
	tests := []struct {
		name     string
		key1     DataKey
		key2     DataKey
		expected bool
	}{
		{
			name:     "Identical unscoped keys",
			key1:     NewDataKey("KeyA", "ProdA"),
			key2:     NewDataKey("KeyA", "ProdA"),
			expected: true,
		},
		{
			name:     "Different scopes, same base",
			key1:     NewRequestDataKey("KeyA", "ProdA"),
			key2:     NewEndpointDataKey("KeyA", "ProdA"),
			expected: true,
		},
		{
			name:     "Unspecified scope vs request scope, same base",
			key1:     NewDataKey("KeyA", "ProdA"),
			key2:     NewRequestDataKey("KeyA", "ProdA"),
			expected: true,
		},
		{
			name:     "Different dataType",
			key1:     NewRequestDataKey("KeyA", "ProdA"),
			key2:     NewRequestDataKey("KeyB", "ProdA"),
			expected: false,
		},
		{
			name:     "Different producerName",
			key1:     NewRequestDataKey("KeyA", "ProdA"),
			key2:     NewRequestDataKey("KeyA", "ProdB"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.key1.EqualsIgnoringScope(tt.key2))
		})
	}
}

func TestDataKey_BaseString(t *testing.T) {
	tests := []struct {
		name     string
		key      DataKey
		expected string
	}{
		{
			name:     "Unscoped key",
			key:      NewDataKey("KeyA", "ProdA"),
			expected: "KeyA/ProdA",
		},
		{
			name:     "Request scoped key",
			key:      NewRequestDataKey("KeyA", "ProdA"),
			expected: "KeyA/ProdA",
		},
		{
			name:     "Endpoint scoped key",
			key:      NewEndpointDataKey("KeyA", "ProdA"),
			expected: "KeyA/ProdA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.key.BaseString())
		})
	}
}
