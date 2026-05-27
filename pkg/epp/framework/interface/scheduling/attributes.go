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

package scheduling

import (
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// PutAttribute stores value at key in the request's attribute store.
func (r *InferenceRequest) PutAttribute(key string, value any) {
	if r.Attributes == nil {
		r.Attributes = fwkdl.NewAttributes()
	}
	r.Attributes.Put(key, value)
}

// GetAttribute returns the value stored at key, or nil and false if absent.
// Prefer ReadRequestAttribute for type-safe access.
func (r *InferenceRequest) GetAttribute(key string) (any, bool) {
	if r.Attributes == nil {
		return nil, false
	}
	return r.Attributes.Get(key)
}

// AttributeKeys returns the keys currently present in the request's attribute store.
// The order is unspecified.
func (r *InferenceRequest) AttributeKeys() []string {
	if r.Attributes == nil {
		return nil
	}
	return r.Attributes.Keys()
}

// ReadRequestAttribute returns the value stored at key, type-asserted to T.
// It returns the zero value of T and false if the key is missing or the value
// is not of type T.
func ReadRequestAttribute[T any](r *InferenceRequest, key string) (T, bool) {
	var zero T
	if r.Attributes == nil {
		return zero, false
	}
	return fwkdl.ReadAttribute[T](r.Attributes, key)
}
