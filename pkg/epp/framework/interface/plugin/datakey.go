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

import "fmt"

// DataScope defines the scope of the data (Request, Endpoint, or Unspecified).
type DataScope string

const (
	// UnspecifiedScope is the default scope for legacy backward compatibility.
	UnspecifiedScope DataScope = "Unspecified"
	// RequestScope is for request-scoped data.
	RequestScope DataScope = "Request"
	// EndpointScope is for endpoint-scoped data.
	EndpointScope DataScope = "Endpoint"
)

// DataKey uniquely identifies the data for data producer/consumer.
type DataKey struct {
	dataType     string
	producerName string
	scope        DataScope
}

// NewDataKey creates a new DataKey with UnspecifiedScope.
// The defaultProducerName is passed as the initial producerName.
func NewDataKey(dataType, defaultProducerName string) DataKey {
	return DataKey{
		dataType:     dataType,
		producerName: defaultProducerName,
		scope:        UnspecifiedScope,
	}
}

// NewRequestDataKey creates a new DataKey with RequestScope.
func NewRequestDataKey(dataType, defaultProducerName string) DataKey {
	return DataKey{
		dataType:     dataType,
		producerName: defaultProducerName,
		scope:        RequestScope,
	}
}

// NewEndpointDataKey creates a new DataKey with EndpointScope.
func NewEndpointDataKey(dataType, defaultProducerName string) DataKey {
	return DataKey{
		dataType:     dataType,
		producerName: defaultProducerName,
		scope:        EndpointScope,
	}
}

// Scope returns the scope of the DataKey.
// If the scope is empty, it defaults to UnspecifiedScope.
func (dk DataKey) Scope() DataScope {
	if dk.scope == "" {
		return UnspecifiedScope
	}
	return dk.scope
}

// WithNonEmptyProducerName returns a copy of the key with the specified producer name
// if the name is not empty, otherwise returns the key unchanged.
func (dk DataKey) WithNonEmptyProducerName(name string) DataKey {
	if name != "" {
		dk.producerName = name
	}
	return dk
}

// String serializes the key.
// For UnspecifiedScope, it formats as "DataType/ProducerName" to preserve 100% backward compatibility.
// For other scopes, it appends the scope as "DataType/ProducerName[Scope]".
func (dk DataKey) String() string {
	if dk.Scope() == UnspecifiedScope {
		return fmt.Sprintf("%s/%s", dk.dataType, dk.producerName)
	}
	return fmt.Sprintf("%s/%s[%s]", dk.dataType, dk.producerName, dk.Scope())
}

// WithScope returns a copy of the key with the specified scope.
func (dk DataKey) WithScope(scope DataScope) DataKey {
	dk.scope = scope
	return dk
}

// BaseString returns the serialized key without scope, in the format "DataType/ProducerName".
func (dk DataKey) BaseString() string {
	return dk.dataType + "/" + dk.producerName
}

// EqualsIgnoringScope returns true if dataType and producerName match, regardless of scope.
func (dk DataKey) EqualsIgnoringScope(other DataKey) bool {
	return dk.dataType == other.dataType && dk.producerName == other.producerName
}
