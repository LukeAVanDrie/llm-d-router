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

package kvblock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/common/prefixindex"
)

// ChainAdder is the event-path write: requestKeys is a contiguous chain whose
// first key is a child of parent, or a root when parent is EmptyBlockHash.
type ChainAdder interface {
	AddChain(ctx context.Context, parent BlockHash, engineKeys, requestKeys []BlockHash, entries []PodEntry) error
}

// ChainWalker reports, per pod, how many leading keys of requestKeys the pod
// holds. visit runs under the pod's read lock and must not call back into
// the index.
type ChainWalker interface {
	WalkChains(ctx context.Context, requestKeys []BlockHash, podFilter sets.Set[string],
		visit func(c *PodChain)) error
}

// PodChain is one pod's held prefix of a walked key sequence. It is valid only
// during the visit it is passed to.
type PodChain struct {
	Pod string
	// Run is the number of leading keys the pod holds.
	Run int
	// Uniform, when non-nil, is the entry set every key the pod holds
	// carries; UniformClass identifies it within the index.
	Uniform      []EntryRef
	UniformClass uint32

	pod  *chainPod
	set  *prefixindex.ChainSet[BlockHash]
	keys []BlockHash
}

// Entries returns the pod's entries at keys[pos], pos < Run. The slice is
// borrowed for the duration of the visit.
func (c *PodChain) Entries(pos int) []EntryRef {
	class, _ := c.set.Class(c.keys[pos])
	return c.pod.refs[class]
}

// errChainNeedsParent rejects an event-path Add, which must name the parent
// of its first key through AddChain.
var errChainNeedsParent = errors.New("chain index: engine-keyed adds need a parent; use AddChain")

// chainWriteChunk bounds the keys a write applies per endpoint-lock hold.
// Every intermediate state is a valid index state, since keys are stored
// parent first.
const chainWriteChunk = 16

// maxEntryClasses caps the distinct entry sets an index labels.
const maxEntryClasses = 1 << 16

// ChainIndexConfig configures a ChainIndex.
type ChainIndexConfig struct {
	// PodCapacity bounds the keys held by a pod until SetPodCapacity gives
	// that pod its own bound; zero leaves pods unbounded. KV events are the
	// index's source of truth, so a bound at the engine's capacity acts only
	// when removals are lost.
	PodCapacity int `json:"podCapacity"`
}

// ChainIndex is a pod-major Index over a prefixindex.Index: each pod is one
// endpoint, and a key's class labels its entry set (device tier, group, and
// whether it is speculative). Each pod's visible keys are ancestor-closed, so
// a prefix match is a galloping search per pod.
type ChainIndex struct {
	cfg   ChainIndexConfig
	idx   *prefixindex.Index[BlockHash]
	tiers *interner

	// podsMu serializes pod creation; pods is replaced whole under it, so
	// readers load it without locking.
	podsMu sync.Mutex
	pods   atomic.Pointer[chainPods]

	classMu  sync.Mutex
	classIDs map[string]uint32
	classes  [][]entryKey // by class; class 0 is the empty set
}

// chainPods is an immutable view of the pods.
type chainPods struct {
	byName map[string]*chainPod
	list   []*chainPod
}

// chainPod is one pod's endpoint and the adapter state kept with it. refs and
// engine are guarded by the endpoint's lock in idx (Update and View).
type chainPod struct {
	name     string
	endpoint int
	// refs holds each class's entries as this pod's EntryRefs, built with
	// the class's first use on the pod.
	refs   map[uint32][]EntryRef
	engine engineMap
}

var (
	_ Index       = &ChainIndex{}
	_ ChainAdder  = &ChainIndex{}
	_ ChainWalker = &ChainIndex{}
)

// NewChainIndex returns an empty ChainIndex.
func NewChainIndex(cfg ChainIndexConfig) *ChainIndex {
	return &ChainIndex{
		cfg:      cfg,
		idx:      prefixindex.NewIndex[BlockHash](prefixindex.Config{PerEndpointCapacity: cfg.PodCapacity}),
		tiers:    newInterner(maxInternedTiers),
		classIDs: map[string]uint32{"": 0},
		classes:  [][]entryKey{nil},
	}
}

func (c *ChainIndex) snapshot() *chainPods {
	if s := c.pods.Load(); s != nil {
		return s
	}
	return &chainPods{}
}

func (c *ChainIndex) pod(name string) (*chainPod, error) {
	if p := c.snapshot().byName[name]; p != nil {
		return p, nil
	}
	c.podsMu.Lock()
	defer c.podsMu.Unlock()
	cur := c.snapshot()
	if p := cur.byName[name]; p != nil {
		return p, nil
	}
	if len(cur.list) >= maxInternedPods {
		return nil, fmt.Errorf("%w: %d pod identifiers", errIndexCardinality, maxInternedPods)
	}
	p := &chainPod{name: name, endpoint: len(cur.list), refs: map[uint32][]EntryRef{}, engine: newEngineMap()}
	next := &chainPods{byName: make(map[string]*chainPod, len(cur.byName)+1), list: append(cur.list[:len(cur.list):len(cur.list)], p)}
	maps.Copy(next.byName, cur.byName)
	next.byName[name] = p
	c.pods.Store(next)
	return p, nil
}

// SetPodCapacity bounds the keys held by the pod, typically at the engine's
// reported block capacity times its tiers. Zero removes the bound.
func (c *ChainIndex) SetPodCapacity(podIdentifier string, capacity int) error {
	p, err := c.pod(podIdentifier)
	if err != nil {
		return err
	}
	c.idx.SetEndpointCapacity(p.endpoint, capacity)
	return nil
}

// TouchPod records that a request routed to the pod read keys[:run], the
// pod's match for those keys. Only the routed pod's engine reads the blocks,
// so only its recency moves.
func (c *ChainIndex) TouchPod(podIdentifier string, keys []BlockHash, run int) {
	if run <= 0 || run > len(keys) {
		return
	}
	p := c.snapshot().byName[podIdentifier]
	if p == nil {
		return
	}
	c.idx.Update(p.endpoint, func(s *prefixindex.ChainSet[BlockHash]) {
		s.Touch(keys[run-1])
	})
}

// Add stores a chain rooted at requestKeys[0], as speculative entries are.
// Engine-keyed adds come from events, which must use AddChain.
func (c *ChainIndex) Add(ctx context.Context, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	if engineKeys != nil {
		return errChainNeedsParent
	}
	return c.AddChain(ctx, EmptyBlockHash, nil, requestKeys, entries)
}

// AddChain implements ChainAdder.
func (c *ChainIndex) AddChain(_ context.Context, parent BlockHash, engineKeys, requestKeys []BlockHash, entries []PodEntry) error {
	if len(requestKeys) == 0 || len(entries) == 0 {
		return errors.New("no keys or entries provided for adding to index")
	}
	groups, err := c.group(entries, true)
	if err != nil {
		return err
	}
	mappings := mappingsOf(engineKeys, requestKeys)
	for _, g := range groups {
		memo := map[uint32]uint32{}
		var merr error
		merge := func(old uint32) uint32 {
			if v, ok := memo[old]; ok {
				return v
			}
			v, err := c.classOf(c.classEntries(old), g.keys, true)
			if err != nil {
				merr, v = err, old
			}
			g.pod.ensureRefs(c, v)
			memo[old] = v
			return v
		}
		for i := 0; i < len(requestKeys); i += chainWriteChunk {
			chunk := requestKeys[i:min(i+chainWriteChunk, len(requestKeys))]
			last := i+chainWriteChunk >= len(requestKeys)
			c.idx.Update(g.pod.endpoint, func(s *prefixindex.ChainSet[BlockHash]) {
				switch {
				case i > 0:
					s.AddChainUnder(requestKeys[i-1], chunk)
				case parent != EmptyBlockHash:
					s.AddChainUnder(parent, chunk)
				default:
					s.AddRootChain(chunk)
				}
				for _, k := range chunk {
					if old, ok := s.Class(k); ok {
						s.SetClass(k, merge(old))
					}
				}
				if last {
					for _, m := range mappings {
						g.pod.engine.set(m.ek, m.rks)
					}
				}
			})
		}
		if merr != nil {
			return merr
		}
	}
	return nil
}

// Evict implements Index.
func (c *ChainIndex) Evict(_ context.Context, keyType KeyType, keys []BlockHash, entries []PodEntry) error {
	if len(keys) == 0 || len(entries) == 0 {
		return errors.New("no keys or entries provided for eviction from index")
	}
	if keyType != EngineKey && keyType != RequestKey {
		return fmt.Errorf("unknown key type: %d", keyType)
	}
	groups, err := c.group(entries, false)
	if err != nil {
		return err
	}
	for _, g := range groups {
		memo := map[uint32]uint32{}
		removeFrom := func(s *prefixindex.ChainSet[BlockHash], k BlockHash) {
			old, ok := s.Class(k)
			if !ok {
				return
			}
			v, seen := memo[old]
			if !seen {
				v, _ = c.classOf(c.classEntries(old), g.keys, false)
				memo[old] = v
			}
			if v == 0 {
				s.Remove(k)
				return
			}
			// A removal can leave a set this pod never stored.
			g.pod.ensureRefs(c, v)
			s.SetClass(k, v)
		}
		for i := 0; i < len(keys); i += chainWriteChunk {
			chunk := keys[i:min(i+chainWriteChunk, len(keys))]
			c.idx.Update(g.pod.endpoint, func(s *prefixindex.ChainSet[BlockHash]) {
				var one [1]BlockHash
				for _, k := range chunk {
					if keyType == RequestKey {
						removeFrom(s, k)
						continue
					}
					rks, ok := g.pod.engine.get(k, &one)
					if !ok {
						continue
					}
					held := false
					for _, rk := range rks {
						removeFrom(s, rk)
						held = held || s.Contains(rk)
					}
					if !held {
						g.pod.engine.del(k)
					}
				}
			})
		}
	}
	return nil
}

// Clear drops every entry of the pod.
func (c *ChainIndex) Clear(_ context.Context, podIdentifier string) error {
	p := c.snapshot().byName[podIdentifier]
	if p == nil {
		return nil
	}
	c.idx.Update(p.endpoint, func(s *prefixindex.ChainSet[BlockHash]) {
		s.Clear()
		p.engine = newEngineMap()
	})
	return nil
}

// GetRequestKey implements Index by asking each pod in turn. Callers that
// know the pod should use GetRequestKeyForPod.
func (c *ChainIndex) GetRequestKey(ctx context.Context, engineKey BlockHash) (BlockHash, error) {
	for _, p := range c.snapshot().list {
		if rk, err := c.GetRequestKeyForPod(ctx, p.name, engineKey); err == nil {
			return rk, nil
		}
	}
	return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
}

// GetRequestKeyForPod returns the last request key the pod mapped engineKey
// to.
func (c *ChainIndex) GetRequestKeyForPod(_ context.Context, podIdentifier string, engineKey BlockHash) (BlockHash, error) {
	var rk BlockHash
	found := false
	if p := c.snapshot().byName[podIdentifier]; p != nil {
		c.idx.View(p.endpoint, func(*prefixindex.ChainSet[BlockHash]) {
			var one [1]BlockHash
			if rks, ok := p.engine.get(engineKey, &one); ok && len(rks) > 0 {
				rk, found = rks[len(rks)-1], true
			}
		})
	}
	if !found {
		return EmptyBlockHash, fmt.Errorf("engine key not found: %s", engineKey.String())
	}
	return rk, nil
}

// Lookup implements Index by probing every pod at every key, so it costs
// pods times keys. The prefix matcher uses WalkChains instead.
func (c *ChainIndex) Lookup(ctx context.Context, requestKeys []BlockHash, podIdentifierSet sets.Set[string]) (map[BlockHash][]PodEntry, error) {
	if len(requestKeys) == 0 {
		return nil, errors.New("no requestKeys provided for lookup")
	}
	out := map[BlockHash][]PodEntry{}
	pods := c.snapshot().list
	for _, k := range requestKeys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, p := range pods {
			if podIdentifierSet.Len() > 0 && !podIdentifierSet.Has(p.name) {
				continue
			}
			c.idx.View(p.endpoint, func(s *prefixindex.ChainSet[BlockHash]) {
				if class, ok := s.Class(k); ok {
					for _, r := range p.refs[class] {
						out[k] = append(out[k], r.PodEntry)
					}
				}
			})
		}
	}
	return out, nil
}

// WalkChains implements ChainWalker.
func (c *ChainIndex) WalkChains(ctx context.Context, requestKeys []BlockHash, podFilter sets.Set[string],
	visit func(c *PodChain),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(requestKeys) == 0 {
		return nil
	}
	pods := c.snapshot().list
	var podBuf [256]*chainPod
	var endBuf, outBuf [256]int
	cands, ends := podBuf[:0], endBuf[:0]
	if len(pods) > len(podBuf) {
		cands, ends = make([]*chainPod, 0, len(pods)), make([]int, 0, len(pods))
	}
	for _, p := range pods {
		if podFilter.Len() > 0 && !podFilter.Has(p.name) {
			continue
		}
		cands = append(cands, p)
		ends = append(ends, p.endpoint)
	}
	out := outBuf[:0]
	if len(ends) > len(outBuf) {
		out = make([]int, 0, len(ends))
	}
	pc := PodChain{keys: requestKeys}
	c.idx.MatchVisit(requestKeys, ends, out, func(i, run int, s *prefixindex.ChainSet[BlockHash]) {
		p := cands[i]
		pc.Pod, pc.Run, pc.pod, pc.set, pc.Uniform, pc.UniformClass = p.name, run, p, s, nil, 0
		if class, ok := s.UniformClass(); ok {
			pc.Uniform, pc.UniformClass = p.refs[class], class
		}
		visit(&pc)
	})
	return ctx.Err()
}

// ChainStats reports index size and closure maintenance.
type ChainStats struct {
	// Pairs is the number of (pod, key) pairs held.
	Pairs int
	// Dormant is the number of held keys waiting on an absent ancestor.
	Dormant int
	// Rebuilds is the number of closure rebuilds after out-of-order events.
	Rebuilds uint64
}

// Stats sums ChainStats over pods.
func (c *ChainIndex) Stats() ChainStats {
	st := c.idx.Stats()
	return ChainStats{Pairs: st.HeldBlocks, Dormant: st.DormantBlocks, Rebuilds: st.Rebuilds}
}

// podEntries is one pod's share of an Add or Evict.
type podEntries struct {
	pod  *chainPod
	keys []entryKey
}

// group splits entries by pod. With create, missing pods and tiers are
// created; without, entries naming them are dropped, as nothing can be
// stored under them.
func (c *ChainIndex) group(entries []PodEntry, create bool) ([]podEntries, error) {
	var out []podEntries
	for _, e := range entries {
		var tier uint32
		var ok bool
		if create {
			if tier, ok = c.tiers.intern(e.DeviceTier); !ok {
				return nil, fmt.Errorf("%w: %d device tiers", errIndexCardinality, maxInternedTiers)
			}
		} else if tier, ok = c.tiers.lookup(e.DeviceTier); !ok {
			continue
		}
		var p *chainPod
		if create {
			var err error
			if p, err = c.pod(e.PodIdentifier); err != nil {
				return nil, err
			}
		} else if p = c.snapshot().byName[e.PodIdentifier]; p == nil {
			continue
		}
		k := entryKey{tier: tier, group: int32(e.GroupIdx)} //#nosec G115 -- group indexes are small
		if e.Speculative {
			k.flags |= entrySpeculative
		}
		if e.HasGroup {
			k.flags |= entryHasGroup
		}
		i := 0
		for i < len(out) && out[i].pod != p {
			i++
		}
		if i == len(out) {
			out = append(out, podEntries{pod: p})
		}
		out[i].keys = append(out[i].keys, k)
	}
	return out, nil
}

// entryKey is a PodEntry without its pod.
type entryKey struct {
	tier  uint32
	group int32
	flags uint8
}

const (
	entrySpeculative uint8 = 1 << iota
	entryHasGroup
)

func (c *ChainIndex) classEntries(class uint32) []entryKey {
	c.classMu.Lock()
	defer c.classMu.Unlock()
	return c.classes[class]
}

// classOf returns the class of base with keys added (or removed).
func (c *ChainIndex) classOf(base, keys []entryKey, add bool) (uint32, error) {
	set := map[entryKey]bool{}
	for _, k := range base {
		set[k] = true
	}
	for _, k := range keys {
		if add {
			set[k] = true
		} else {
			delete(set, k)
		}
	}
	sorted := make([]entryKey, 0, len(set))
	for k := range set {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.tier != b.tier {
			return a.tier < b.tier
		}
		if a.group != b.group {
			return a.group < b.group
		}
		return a.flags < b.flags
	})
	enc := make([]byte, 0, 9*len(sorted))
	for _, k := range sorted {
		enc = binary.LittleEndian.AppendUint32(enc, k.tier)
		enc = binary.LittleEndian.AppendUint32(enc, uint32(k.group)) //#nosec G115 -- bit pattern only
		enc = append(enc, k.flags)
	}
	c.classMu.Lock()
	defer c.classMu.Unlock()
	if id, ok := c.classIDs[string(enc)]; ok {
		return id, nil
	}
	if len(c.classes) >= maxEntryClasses {
		return 0, fmt.Errorf("%w: %d entry sets", errIndexCardinality, maxEntryClasses)
	}
	id := uint32(len(c.classes)) //#nosec G115 -- capped at maxEntryClasses
	c.classIDs[string(enc)] = id
	c.classes = append(c.classes, sorted)
	return id, nil
}

// ensureRefs builds the pod's EntryRefs for class. Called under the pod's
// endpoint write lock.
func (p *chainPod) ensureRefs(c *ChainIndex, class uint32) {
	if _, ok := p.refs[class]; ok || class == 0 {
		return
	}
	es := c.classEntries(class)
	refs := make([]EntryRef, len(es))
	for i, e := range es {
		refs[i] = EntryRef{
			PodEntry: PodEntry{
				PodIdentifier: p.name,
				DeviceTier:    c.tiers.name(e.tier),
				Speculative:   e.flags&entrySpeculative != 0,
				HasGroup:      e.flags&entryHasGroup != 0,
				GroupIdx:      GroupID(e.group),
			},
			PodOrdinal:  uint32(p.endpoint), //#nosec G115 -- capped at maxInternedPods
			TierOrdinal: e.tier,
		}
	}
	p.refs[class] = refs
}

// engineMap maps engine keys to request keys, storing 1:1 mappings inline.
type engineMap struct {
	one  map[BlockHash]BlockHash
	many map[BlockHash][]BlockHash
}

func newEngineMap() engineMap {
	return engineMap{one: map[BlockHash]BlockHash{}, many: map[BlockHash][]BlockHash{}}
}

func (m *engineMap) get(ek BlockHash, buf *[1]BlockHash) ([]BlockHash, bool) {
	if rk, ok := m.one[ek]; ok {
		buf[0] = rk
		return buf[:], true
	}
	rks, ok := m.many[ek]
	return rks, ok
}

func (m *engineMap) set(ek BlockHash, rks []BlockHash) {
	if len(rks) == 1 {
		delete(m.many, ek)
		m.one[ek] = rks[0]
		return
	}
	delete(m.one, ek)
	m.many[ek] = rks
}

func (m *engineMap) del(ek BlockHash) {
	delete(m.one, ek)
	delete(m.many, ek)
}

type engineMapping struct {
	ek  BlockHash
	rks []BlockHash
}

// mappingsOf pairs engine keys with request keys; 1:1 pairs alias
// requestKeys.
func mappingsOf(engineKeys, requestKeys []BlockHash) []engineMapping {
	if engineKeys == nil {
		return nil
	}
	if len(engineKeys) == len(requestKeys) {
		out := make([]engineMapping, len(engineKeys))
		for i, ek := range engineKeys {
			out[i] = engineMapping{ek, requestKeys[i : i+1 : i+1]}
		}
		return out
	}
	m := engineToRequestMapping(engineKeys, requestKeys)
	out := make([]engineMapping, 0, len(m))
	for ek, rks := range m {
		out = append(out, engineMapping{ek, rks})
	}
	return out
}
