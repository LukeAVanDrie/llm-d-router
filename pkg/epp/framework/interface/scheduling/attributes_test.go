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
	"testing"

	"github.com/stretchr/testify/assert"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func TestInferenceRequest_Attributes(t *testing.T) {
	t.Run("LazyInitialization", func(t *testing.T) {
		var r InferenceRequest // Zero-value struct, r.Attributes is nil
		assert.Nil(t, r.Attributes)

		r.PutAttribute("session", "abc")
		assert.NotNil(t, r.Attributes)

		v, ok := r.GetAttribute("session")
		assert.True(t, ok)
		assert.Equal(t, "abc", v)
	})

	t.Run("PreAllocated", func(t *testing.T) {
		r := &InferenceRequest{
			Attributes: fwkdl.NewAttributes(),
		}
		assert.NotNil(t, r.Attributes)

		r.PutAttribute("session", "abc")
		v, ok := r.GetAttribute("session")
		assert.True(t, ok)
		assert.Equal(t, "abc", v)
	})

	t.Run("GetMissingKey", func(t *testing.T) {
		var r InferenceRequest
		_, ok := r.GetAttribute("missing")
		assert.False(t, ok)
	})

	t.Run("OverwriteAndKeys", func(t *testing.T) {
		var r InferenceRequest
		r.PutAttribute("a", 1)
		r.PutAttribute("b", "two")
		r.PutAttribute("a", 11) // Overwrites key "a"

		assert.ElementsMatch(t, []string{"a", "b"}, r.AttributeKeys())

		v, ok := r.GetAttribute("a")
		assert.True(t, ok)
		assert.Equal(t, 11, v)
	})
}

func TestReadRequestAttribute(t *testing.T) {
	t.Run("DelegationSuccess", func(t *testing.T) {
		r := &InferenceRequest{}
		r.PutAttribute("count", 42)

		val, ok := ReadRequestAttribute[int](r, "count")
		assert.True(t, ok)
		assert.Equal(t, 42, val)
	})

	t.Run("NilSafety", func(t *testing.T) {
		var r InferenceRequest
		assert.Nil(t, r.Attributes)

		val, ok := ReadRequestAttribute[int](&r, "count")
		assert.False(t, ok)
		assert.Zero(t, val)
	})
}
