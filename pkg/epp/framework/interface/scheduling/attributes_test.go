/*
Copyright 2025 The llm-d Authors.

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
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

type testCloneableInt int

func (t testCloneableInt) Clone() fwkdl.Cloneable { return t }

func testKey(name string) fwkplugin.DataKey { return fwkplugin.NewDataKey(name, "test-producer") }

func TestRequestAttributes_PutThenGet(t *testing.T) {
	r := &InferenceRequest{}

	r.PutAttribute(testKey("session"), "abc")
	v, ok := r.GetAttribute(testKey("session"))
	assert.True(t, ok)
	assert.Equal(t, "abc", v)

	_, ok = r.GetAttribute(testKey("missing"))
	assert.False(t, ok)
}

func TestRequestAttributes_DynamicAttribute(t *testing.T) {
	r := &InferenceRequest{}
	current := testCloneableInt(10)
	r.PutAttribute(testKey("dyn"), &fwkdl.DynamicAttribute{
		Get: func() fwkdl.Cloneable {
			return current
		},
	})

	v, ok := r.GetAttribute(testKey("dyn"))
	assert.True(t, ok)
	assert.Equal(t, testCloneableInt(10), v)

	current = 25
	typed, ok := ReadRequestAttribute[testCloneableInt](r, testKey("dyn"))
	assert.True(t, ok)
	assert.Equal(t, testCloneableInt(25), typed)

	r.PutAttribute(testKey("nil-dyn"), &fwkdl.DynamicAttribute{
		Get: func() fwkdl.Cloneable {
			return nil
		},
	})
	v, ok = r.GetAttribute(testKey("nil-dyn"))
	assert.False(t, ok)
	assert.Nil(t, v)

	typed, ok = ReadRequestAttribute[testCloneableInt](r, testKey("nil-dyn"))
	assert.False(t, ok)
	assert.Equal(t, testCloneableInt(0), typed)

	r.PutAttribute(testKey("nil-func-dyn"), &fwkdl.DynamicAttribute{})
	v, ok = r.GetAttribute(testKey("nil-func-dyn"))
	assert.False(t, ok)
	assert.Nil(t, v)

	r.PutAttribute(testKey("nil-ptr-dyn"), (*fwkdl.DynamicAttribute)(nil))
	v, ok = r.GetAttribute(testKey("nil-ptr-dyn"))
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestRequestAttributes_KeysAfterPuts(t *testing.T) {
	r := &InferenceRequest{}

	r.PutAttribute(testKey("a"), 1)
	r.PutAttribute(testKey("b"), "two")
	r.PutAttribute(testKey("a"), 11) // overwrite

	assert.ElementsMatch(t, []fwkplugin.DataKey{testKey("a"), testKey("b")}, r.AttributeKeys())
}

func TestReadRequestAttribute(t *testing.T) {
	r := &InferenceRequest{}
	r.PutAttribute(testKey("count"), 42)
	r.PutAttribute(testKey("name"), "alpha")

	count, ok := ReadRequestAttribute[int](r, testKey("count"))
	assert.True(t, ok)
	assert.Equal(t, 42, count)

	name, ok := ReadRequestAttribute[string](r, testKey("name"))
	assert.True(t, ok)
	assert.Equal(t, "alpha", name)

	missing, ok := ReadRequestAttribute[int](r, testKey("absent"))
	assert.False(t, ok)
	assert.Equal(t, 0, missing)

	mismatch, ok := ReadRequestAttribute[string](r, testKey("count"))
	assert.False(t, ok)
	assert.Equal(t, "", mismatch)
}

func TestRequestAttributes_ZeroValueRequestIsUsable(t *testing.T) {
	var nilReq *InferenceRequest
	v, ok := nilReq.GetAttribute(testKey("k"))
	assert.False(t, ok)
	assert.Nil(t, v)
	typed, ok := ReadRequestAttribute[string](nilReq, testKey("k"))
	assert.False(t, ok)
	assert.Empty(t, typed)
	assert.Empty(t, nilReq.AttributeKeys())

	var r InferenceRequest

	r.PutAttribute(testKey("k"), "v")
	v, ok = r.GetAttribute(testKey("k"))
	assert.True(t, ok)
	assert.Equal(t, "v", v)
}

func TestRequestAttributes_ConcurrentAfterInit(t *testing.T) {
	r := &InferenceRequest{}
	r.InitializeAttributeStore()

	const writers = 8
	const writes = 200
	var wg sync.WaitGroup

	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				key := testKey(strconv.Itoa(id) + ":" + strconv.Itoa(i))
				r.PutAttribute(key, i)
				if v, ok := ReadRequestAttribute[int](r, key); !ok || v != i {
					t.Errorf("round-trip failed for %s: ok=%v v=%v", key, ok, v)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	assert.Len(t, r.AttributeKeys(), writers*writes)
}
