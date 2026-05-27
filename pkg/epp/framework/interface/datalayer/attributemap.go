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
	"reflect"
	"sync"
)

// Cloneable types support cloning of the value.
type Cloneable interface {
	Clone() Cloneable
}

// AttributeMap is used to store flexible metadata or traits
// across different aspects of an inference server.
type AttributeMap interface {
	Put(string, any)
	Get(string) (any, bool)
	Keys() []string
	Clone() AttributeMap
}

// Attributes provides a goroutine-safe implementation of AttributeMap.
type Attributes struct {
	data sync.Map // key: attribute name (string), value: attribute value (opaque, any)
}

// NewAttributes returns a new instance of Attributes.
func NewAttributes() AttributeMap {
	return &Attributes{}
}

func assertSafeType(value any, key string) {
	if value == nil {
		return
	}
	if _, ok := value.(Cloneable); ok {
		return
	}

	kind := reflect.TypeOf(value).Kind()
	switch kind {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return // Safe primitive copied by value
	default:
		panic(fmt.Sprintf("datalayer: stored value of type %T at key %q must implement Cloneable to prevent shared pointer leaks", value, key))
	}
}

// Put adds or updates an attribute in the map.
// If the value implements Cloneable, a cloned copy is stored to ensure isolation.
func (a *Attributes) Put(key string, value any) {
	if value == nil {
		return
	}
	assertSafeType(value, key)
	if cloneable, ok := value.(Cloneable); ok {
		a.data.Store(key, cloneable.Clone())
		return
	}
	a.data.Store(key, value)
}

// Get retrieves an attribute by key.
// If the stored value implements Cloneable, a cloned copy is returned.
func (a *Attributes) Get(key string) (any, bool) {
	val, ok := a.data.Load(key)
	if !ok {
		return nil, false
	}
	if cloneable, ok := val.(Cloneable); ok {
		return cloneable.Clone(), true
	}
	return val, true
}

// Keys returns all keys in the attribute map.
func (a *Attributes) Keys() []string {
	var keys []string
	a.data.Range(func(key, _ any) bool {
		if sk, ok := key.(string); ok {
			keys = append(keys, sk)
		}
		return true
	})
	return keys
}

// Clone creates a deep copy of the entire attribute map.
func (a *Attributes) Clone() AttributeMap {
	clone := NewAttributes()
	a.data.Range(func(key, value any) bool {
		if sk, ok := key.(string); ok {
			clone.Put(sk, value)
		}
		return true
	})
	return clone
}

// LocalAttributes is a lockless, clone-free implementation of AttributeMap.
// Confined strictly to the main goroutine.
type LocalAttributes struct {
	data map[string]any
}

// NewLocalAttributes returns a new instance of LocalAttributes.
func NewLocalAttributes() AttributeMap {
	return &LocalAttributes{data: make(map[string]any)}
}

// Put stores a reference directly without cloning.
func (la *LocalAttributes) Put(key string, value any) {
	if value == nil {
		return
	}
	assertSafeType(value, key)
	if la.data == nil {
		la.data = make(map[string]any)
	}
	la.data[key] = value
}

// Get retrieves the reference directly without cloning.
func (la *LocalAttributes) Get(key string) (any, bool) {
	if la.data == nil {
		return nil, false
	}
	val, ok := la.data[key]
	return val, ok
}

// Keys returns all keys.
func (la *LocalAttributes) Keys() []string {
	if la.data == nil {
		return nil
	}
	keys := make([]string, 0, len(la.data))
	for k := range la.data {
		keys = append(keys, k)
	}
	return keys
}

// Clone returns a clone backed by the same references (shallow clone).
func (la *LocalAttributes) Clone() AttributeMap {
	clone := NewLocalAttributes()
	if la.data != nil {
		for k, v := range la.data {
			clone.Put(k, v)
		}
	}
	return clone
}

// ReadOnlyAttributes is a read-only, thread-safe (for reading) implementation of AttributeMap.
type ReadOnlyAttributes struct {
	data map[string]any
}

// NewReadOnlyAttributes wraps an existing AttributeMap into a ReadOnlyAttributes.
// Deep-clones entries to ensure isolation, bypassing redundant double-clones if src is *Attributes.
func NewReadOnlyAttributes(src AttributeMap) AttributeMap {
	backing := make(map[string]any)
	if src != nil {
		switch s := src.(type) {
		case *Attributes:
			for _, k := range s.Keys() {
				val, ok := s.Get(k) // Returns a freshly isolated clone
				if ok {
					backing[k] = val // Safe to store directly
				}
			}
		default:
			for _, k := range src.Keys() {
				val, ok := src.Get(k)
				if ok {
					if cloneable, ok := val.(Cloneable); ok {
						backing[k] = cloneable.Clone()
					} else {
						backing[k] = val
					}
				}
			}
		}
	}
	return &ReadOnlyAttributes{data: backing}
}

// Put panics since ReadOnlyAttributes is read-only.
func (r *ReadOnlyAttributes) Put(key string, value any) {
	panic("cannot write to ReadOnlyAttributes")
}

// Get retrieves the value, cloning if it is Cloneable for safety.
func (r *ReadOnlyAttributes) Get(key string) (any, bool) {
	if r.data == nil {
		return nil, false
	}
	val, ok := r.data[key]
	if !ok {
		return nil, false
	}
	if cloneable, ok := val.(Cloneable); ok {
		return cloneable.Clone(), true
	}
	return val, true
}

// Keys returns all keys.
func (r *ReadOnlyAttributes) Keys() []string {
	if r.data == nil {
		return nil
	}
	keys := make([]string, 0, len(r.data))
	for k := range r.data {
		keys = append(keys, k)
	}
	return keys
}

// Clone returns a copy of the ReadOnlyAttributes (shallow map clone).
func (r *ReadOnlyAttributes) Clone() AttributeMap {
	if r.data == nil {
		return &ReadOnlyAttributes{}
	}
	backing := make(map[string]any, len(r.data))
	for k, v := range r.data {
		backing[k] = v
	}
	return &ReadOnlyAttributes{data: backing}
}

// Primitive interface defines standard primitive types in Go.
type Primitive interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string | ~bool
}

// PutAttribute stores a primitive value at key, enforcing type constraints at compile time.
func PutAttribute[T Primitive](m AttributeMap, key string, val T) {
	m.Put(key, val)
}

// PutCloneable stores a Cloneable value at key, enforcing type constraints at compile time.
func PutCloneable[T Cloneable](m AttributeMap, key string, val T) {
	m.Put(key, val)
}

// ReadAttribute retrieves value at key from AttributeMap and asserts it to type T.
// Second return value is 'false' if the key is not found or the type assertion fails.
func ReadAttribute[T any](m AttributeMap, key string) (T, bool) {
	var zero T
	raw, ok := m.Get(key)
	if !ok {
		return zero, false
	}
	val, ok := raw.(T)
	return val, ok
}
