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

package capacityledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/ledger"
	fwkcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/capacity"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	testutils "github.com/llm-d/llm-d-router/test/utils"
)

const (
	blockSize = 16
	numBlocks = 100
)

func newTestAdapter(t *testing.T, l fwkcapacity.Ledger) *Adapter {
	t.Helper()
	p, err := AdapterFactory("test", nil, testutils.NewTestHandleWithCapacity(context.Background(), l))
	require.NoError(t, err)
	return p.(*Adapter)
}

func newTestLedger() *ledger.PoolLedger {
	return ledger.NewPoolLedger(clock.RealClock{}, ledger.TokenTranslator{}, ledger.DefaultConfig())
}

// newTestEndpoint builds an endpoint reporting the given block geometry. Zero
// blocks models an endpoint whose first scrape has not landed.
func newTestEndpoint(name string, blocks int) fwkdl.Endpoint {
	meta := &fwkdl.EndpointMetadata{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
	m := fwkdl.NewMetrics()
	m.CacheBlockSize = blockSize
	m.CacheNumBlocks = blocks
	return fwkdl.NewEndpoint(meta, m)
}

func endpointID(name string) string {
	return types.NamespacedName{Name: name, Namespace: "default"}.String()
}

func extract(t *testing.T, a *Adapter, typ fwkdl.EventType, ep fwkdl.Endpoint) {
	t.Helper()
	require.NoError(t, a.Extract(context.Background(), fwkdl.EndpointEvent{Type: typ, Endpoint: ep}))
}

func TestAdapterFactory(t *testing.T) {
	t.Run("RefusesWithoutALedger", func(t *testing.T) {
		_, err := AdapterFactory("test", nil, testutils.NewTestHandle(context.Background()))
		require.ErrorIs(t, err, ErrNoLedger)
	})

	t.Run("RefusesWithoutAHandle", func(t *testing.T) {
		_, err := AdapterFactory("test", nil, nil)
		require.Error(t, err)
	})
}

func TestAdapterEndpointLifecycle(t *testing.T) {
	t.Run("AddRegistersScrapedKVCapacity", func(t *testing.T) {
		l := newTestLedger()
		a := newTestAdapter(t, l)

		extract(t, a, fwkdl.EventAddOrUpdate, newTestEndpoint("ep", numBlocks))

		avail := l.EndpointAvailable(endpointID("ep"))
		assert.Equal(t, int64(blockSize*numBlocks), avail.KVTokens)
	})

	t.Run("UpdateRefreshesCapacity", func(t *testing.T) {
		l := newTestLedger()
		a := newTestAdapter(t, l)

		extract(t, a, fwkdl.EventAddOrUpdate, newTestEndpoint("ep", 0))
		assert.Zero(t, l.EndpointAvailable(endpointID("ep")).KVTokens,
			"an endpoint whose first scrape has not landed reports no KV capacity")

		extract(t, a, fwkdl.EventAddOrUpdate, newTestEndpoint("ep", numBlocks))
		assert.Equal(t, int64(blockSize*numBlocks), l.EndpointAvailable(endpointID("ep")).KVTokens)
	})

	t.Run("DeleteRemovesTheEndpoint", func(t *testing.T) {
		l := newTestLedger()
		a := newTestAdapter(t, l)
		ep := newTestEndpoint("ep", numBlocks)

		extract(t, a, fwkdl.EventAddOrUpdate, ep)
		extract(t, a, fwkdl.EventDelete, ep)

		assert.Zero(t, l.EndpointAvailable(endpointID("ep")), "a deleted endpoint offers no capacity")
	})

	t.Run("StaleDeleteForAReplacedEndpointIsIgnored", func(t *testing.T) {
		l := newTestLedger()
		a := newTestAdapter(t, l)
		original := newTestEndpoint("ep", numBlocks)
		replacement := newTestEndpoint("ep", numBlocks)

		extract(t, a, fwkdl.EventAddOrUpdate, original)
		extract(t, a, fwkdl.EventAddOrUpdate, replacement)
		extract(t, a, fwkdl.EventDelete, original)

		assert.Equal(t, int64(blockSize*numBlocks), l.EndpointAvailable(endpointID("ep")).KVTokens,
			"the delete belongs to the object the replacement superseded")
	})

	t.Run("EventsWithoutMetadataAreIgnored", func(t *testing.T) {
		a := newTestAdapter(t, newTestLedger())

		require.NoError(t, a.Extract(context.Background(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate}))
		require.NoError(t, a.Extract(context.Background(),
			fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: fwkdl.NewEndpoint(nil, nil)}))
	})
}

func TestAdapterPublishesALiveAttribute(t *testing.T) {
	l := newTestLedger()
	a := newTestAdapter(t, l)
	ep := newTestEndpoint("ep", numBlocks)
	extract(t, a, fwkdl.EventAddOrUpdate, ep)

	read := func() attrcapacity.AvailableCapacity {
		raw, ok := ep.GetAttributes().Get(a.dk.String())
		require.True(t, ok, "adapter publishes its data key on the endpoint")
		return *raw.(*attrcapacity.AvailableCapacity)
	}

	before := read()
	require.Equal(t, int64(blockSize*numBlocks), before.KVTokens)

	// A committed lease books capacity the engine has not yet reported. The
	// attribute must reflect it, since observing reservations rather than scraped
	// occupancy is the reason the ledger exists. A hold cannot be observed here:
	// it is pool-scope, held before any endpoint is chosen.
	require.NoError(t, l.TryAcquireHold("req-1", fwkcapacity.Footprint{Slots: 1}, 1.0))
	outcome := l.Commit("req-1", endpointID("ep"), ledger.LeaseSpec{
		Prediction: ledger.Prediction{PromptTokens: 64},
	})
	require.False(t, outcome.HoldMissing)

	assert.Less(t, read().KVTokens, before.KVTokens, "a committed lease shrinks the published headroom")
}

func TestAdapterDeclaresItsProducedKey(t *testing.T) {
	a := newTestAdapter(t, newTestLedger())
	produced := a.Produces()

	require.Len(t, produced, 1)
	_, ok := produced[a.dk]
	assert.True(t, ok)
	assert.Equal(t, CapacityLedgerAdapterType, a.TypedName().Type)

	var _ fwkplugin.ProducerPlugin = a
}
