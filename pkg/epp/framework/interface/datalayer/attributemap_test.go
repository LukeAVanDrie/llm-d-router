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

package datalayer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type dummy struct {
	Text string
}

func (d *dummy) Clone() Cloneable {
	return &dummy{Text: d.Text}
}

type anotherDummy struct {
	Number int
}

func (d *anotherDummy) Clone() Cloneable {
	return &anotherDummy{Number: d.Number}
}

type uncloneable struct {
	Value string
}

func TestExpectPutThenGetToMatch(t *testing.T) {
	attrs := NewAttributes()
	original := &dummy{"foo"}
	attrs.Put("a", original)

	got, ok := attrs.Get("a")
	assert.True(t, ok, "expected key to exist")
	assert.NotSame(t, original, got, "expected Get to return a clone, not original")

	dv, ok := got.(*dummy)
	assert.True(t, ok, "expected value to be of type *dummy")
	assert.Equal(t, "foo", dv.Text)

	_, ok = attrs.Get("b")
	assert.False(t, ok, "expected key not to exist")
}

func TestExpectKeysToMatchAdded(t *testing.T) {
	attrs := NewAttributes()
	attrs.Put("x", &dummy{"1"})
	attrs.Put("y", &dummy{"2"})

	keys := attrs.Keys()
	assert.Len(t, keys, 2)
	assert.ElementsMatch(t, keys, []string{"x", "y"})
}

func TestCloneReturnsCopy(t *testing.T) {
	original := NewAttributes()
	original.Put("k", &dummy{"value"})
	original.Put("primitive", 42)

	cloned := original.Clone()

	kOrig, _ := original.Get("k")
	kClone, _ := cloned.Get("k")

	assert.NotSame(t, kOrig, kClone, "expected cloned value to be a different instance")
	if diff := cmp.Diff(kOrig, kClone); diff != "" {
		t.Errorf("Unexpected output (-want +got): %v", diff)
	}

	pOrig, _ := original.Get("primitive")
	pClone, _ := cloned.Get("primitive")
	assert.Equal(t, pOrig, pClone)
}

func TestReadAttribute(t *testing.T) {
	// successful retrieval
	attrs := NewAttributes()
	original := &dummy{"foo"}
	attrs.Put("a", original)

	got, ok := ReadAttribute[*dummy](attrs, "a")
	assert.True(t, ok, "expected key to exist and value to be of type *dummy")
	assert.NotSame(t, original, got, "expected Get to return a clone, not original")
	assert.Equal(t, "foo", got.Text)

	// missing key
	_, ok = ReadAttribute[*dummy](attrs, "b")
	assert.False(t, ok, "expected key not to exist")

	// type mismatch
	other, ok := ReadAttribute[*anotherDummy](attrs, "a")
	assert.False(t, ok, "expected type mismatch")
	assert.Nil(t, other) // zero value of pointer is nil
}

func TestPutAttribute_GenericsAndIsolation(t *testing.T) {
	attrs := NewAttributes()

	t.Run("Primitives", func(t *testing.T) {
		PutAttribute(attrs, "intKey", 123)
		valInt, ok := ReadAttribute[int](attrs, "intKey")
		assert.True(t, ok)
		assert.Equal(t, 123, valInt)

		PutAttribute(attrs, "strKey", "hello")
		valStr, ok := ReadAttribute[string](attrs, "strKey")
		assert.True(t, ok)
		assert.Equal(t, "hello", valStr)

		PutAttribute(attrs, "boolKey", true)
		valBool, ok := ReadAttribute[bool](attrs, "boolKey")
		assert.True(t, ok)
		assert.True(t, valBool)
	})

	t.Run("CloneableIsolation", func(t *testing.T) {
		original := &dummy{"originalText"}
		PutCloneable(attrs, "cloneableKey", original)

		original.Text = "mutatedText"

		stored, ok := ReadAttribute[*dummy](attrs, "cloneableKey")
		assert.True(t, ok)
		assert.Equal(t, "originalText", stored.Text)

		stored.Text = "mutatedRetrieve"

		storedSecond, ok := ReadAttribute[*dummy](attrs, "cloneableKey")
		assert.True(t, ok)
		assert.Equal(t, "originalText", storedSecond.Text)
	})

	// Note: Trying to compile either of the following lines will fail:
	// PutAttribute(attrs, "uncloneable", uncloneable{Value: "test"}) // fails because uncloneable is not a Primitive
	// PutCloneable(attrs, "uncloneable", &uncloneable{Value: "test"}) // fails because uncloneable is not Cloneable
}

func TestLocalAttributes_NoCloning(t *testing.T) {
	attrs := NewLocalAttributes()
	original := &dummy{"local"}
	attrs.Put("k", original)

	got, ok := attrs.Get("k")
	assert.True(t, ok)
	// For LocalAttributes, it must be the EXACT SAME pointer (bypass cloning)
	assert.Same(t, original, got)

	keys := attrs.Keys()
	assert.Equal(t, []string{"k"}, keys)

	cloned := attrs.Clone()
	gotCloned, ok := cloned.Get("k")
	assert.True(t, ok)
	// Shallow clone, so reference remains same
	assert.Same(t, original, gotCloned)

	// LocalAttributes panics on storing uncloneable custom structs
	unc := &uncloneable{Value: "test"}
	assert.Panics(t, func() {
		attrs.Put("unc", unc)
	})
}

func TestReadOnlyAttributes(t *testing.T) {
	src := NewAttributes()
	original := &dummy{"read-only"}
	src.Put("k", original)
	src.Put("primitive", 100)

	ro := NewReadOnlyAttributes(src)

	// 1. Verify Get clones Cloneable
	got, ok := ro.Get("k")
	assert.True(t, ok)
	assert.NotSame(t, original, got)
	assert.Equal(t, "read-only", got.(*dummy).Text)

	gotPrim, ok := ro.Get("primitive")
	assert.True(t, ok)
	assert.Equal(t, 100, gotPrim)

	// 2. Verify Put panics
	assert.Panics(t, func() {
		ro.Put("newKey", "newValue")
	})

	// 3. Verify concurrent reads are safe
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				v, ok := ro.Get("k")
				assert.True(t, ok)
				assert.Equal(t, "read-only", v.(*dummy).Text)
			}
		}()
	}
	wg.Wait()
}

func TestAttributes_Concurrency(t *testing.T) {
	attrs := NewAttributes()

	const workers = 8
	const ops = 100
	var wg sync.WaitGroup

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d-%d", workerID, i)
				attrs.Put(key, i)
				val, ok := attrs.Get(key)
				if !ok || val != i {
					t.Errorf("concurrency check failed for %s: ok=%v val=%v", key, ok, val)
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestAttributes_PanicOnUncloneable(t *testing.T) {
	attrs := NewAttributes()

	// Primitives are allowed
	assert.NotPanics(t, func() {
		attrs.Put("int", 42)
		attrs.Put("string", "hello")
		attrs.Put("bool", true)
	})

	// Cloneable custom structs are allowed
	assert.NotPanics(t, func() {
		attrs.Put("cloneable", &dummy{Text: "safe"})
	})

	// Uncloneable custom structs must panic
	assert.Panics(t, func() {
		attrs.Put("uncloneable", &uncloneable{Value: "leak"})
	})
}
