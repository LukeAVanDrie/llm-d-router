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
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
	tokenproducer "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
	testutils "github.com/llm-d/llm-d-router/test/utils"
)

func TestNewConfig(t *testing.T) {
	valid := testAPIConfig
	with := func(f func(*apiConfig)) apiConfig { a := valid(); f(&a); return a }
	tests := []struct {
		name          string
		api           apiConfig
		staleness     time.Duration
		wantErr       string
		wantStaleness time.Duration
	}{
		{name: "slots required", api: with(func(a *apiConfig) { a.SlotsPerEndpoint = nil }), wantErr: "slotsPerEndpoint is required"},
		{name: "slots positive", api: with(func(a *apiConfig) { a.SlotsPerEndpoint = ptr(int64(0)) }), wantErr: "slotsPerEndpoint must be positive"},
		{name: "step budget required", api: with(func(a *apiConfig) { a.StepTokenBudget = nil }), wantErr: "stepTokenBudget is required"},
		{name: "speculative non-negative", api: with(func(a *apiConfig) { a.SpeculativeTokens = ptr(int64(-1)) }), wantErr: "speculativeTokens"},
		{name: "maxModelLen required", api: with(func(a *apiConfig) { a.MaxModelLen = nil }), wantErr: "maxModelLen is required"},
		{name: "maxModelLen positive", api: with(func(a *apiConfig) { a.MaxModelLen = ptr(int64(0)) }), wantErr: "maxModelLen must be positive"},
		{name: "staleness defaults to the flag's default", api: valid(), wantStaleness: defaultStalenessThreshold},
		{name: "staleness from the handle", api: valid(), staleness: 3 * time.Second, wantStaleness: 3 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := newConfig(tc.api, tc.staleness)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantStaleness, cfg.stalenessThreshold)
		})
	}
}

func TestFactory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := []byte(`{"slotsPerEndpoint": 256, "stepTokenBudget": 8192, "speculativeTokens": 2, "maxModelLen": 32768}`)
	p, err := Factory("ledger", json.NewDecoder(bytes.NewReader(raw)), testutils.NewTestHandle(ctx))
	require.NoError(t, err)
	l := p.(*Ledger)
	require.Equal(t, CapacityLedgerType, l.TypedName().Type)
	require.Contains(t, l.Produces(), attrcapacity.EndpointCapacityDataKey.WithNonEmptyProducerName("ledger"))
	require.Contains(t, l.Consumes().Required, tokenproducer.TokenizedPromptDataKey)

	_, err = Factory("ledger", json.NewDecoder(bytes.NewReader([]byte(`{}`))), testutils.NewTestHandle(ctx))
	require.ErrorContains(t, err, "slotsPerEndpoint is required")
}

type fakeRegistrar struct{ regs []fwkdl.PendingRegistration }

func (f *fakeRegistrar) Register(r fwkdl.PendingRegistration) error {
	f.regs = append(f.regs, r)
	return nil
}

func TestRegisterDependencies(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	r := &fakeRegistrar{}
	require.NoError(t, l.RegisterDependencies(r))
	require.Len(t, r.regs, 2)
	require.Equal(t, sourcenotifications.EndpointNotificationSourceType, r.regs[0].SourceType)
	require.Same(t, l, r.regs[0].Extractor)
	require.NotNil(t, r.regs[0].DefaultSource)
	require.Equal(t, sourcemetrics.MetricsDataSourceType, r.regs[1].SourceType)
	require.Same(t, l.engines, r.regs[1].Extractor)
	require.Equal(t, fwkdl.Warn, r.regs[1].IfMissing)
}

func TestScraped(t *testing.T) {
	use, memory, ok := scraped(testMetrics(0.5, 3, 1, testStart), 3)
	require.True(t, ok)
	require.Equal(t, int64(100), memory, "one block is vLLM's null block")
	require.Equal(t, vec{50, 9, 3}, use, "step is one decode step of 3 tokens per running request")

	use, _, _ = scraped(testMetrics(0.501, 0, 0, testStart), 1)
	require.Equal(t, int64(51), use[axisMemory], "scraped memory rounds up")

	_, _, ok = scraped(&fwkdl.Metrics{CacheNumBlocks: 0, CacheBlockSize: 16}, 1)
	require.False(t, ok)
	_, _, ok = scraped(&fwkdl.Metrics{CacheNumBlocks: 101}, 1)
	require.False(t, ok)
}

func TestEligibility(t *testing.T) {
	fresh := &fwkdl.Metrics{CacheNumBlocks: 101, CacheBlockSize: 16, UpdateTime: testStart}
	meta := func(labels map[string]string) *fwkdl.EndpointMetadata { return &fwkdl.EndpointMetadata{Labels: labels} }
	tests := []struct {
		name string
		meta *fwkdl.EndpointMetadata
		m    *fwkdl.Metrics
		obs  engineObservation
		// speculativeTokens is the ledger's configured value.
		speculativeTokens int64
		now               time.Time
		want              attrcapacity.Eligibility
	}{
		{name: "eligible", meta: meta(nil), m: fresh, obs: engineObservation{engines: 1}, now: testStart, want: attrcapacity.Eligible},
		{name: "decode role", meta: meta(map[string]string{"llm-d.ai/role": "decode"}), m: fresh, now: testStart, want: attrcapacity.Disaggregated},
		{name: "aggregated role", meta: meta(map[string]string{"llm-d.ai/role": "prefill-decode"}), m: fresh, now: testStart, want: attrcapacity.Eligible},
		{name: "other engine", meta: meta(map[string]string{"llm-d.ai/engine-type": "sglang"}), m: fresh, now: testStart, want: attrcapacity.EngineType},
		{name: "explicit vllm", meta: meta(map[string]string{"llm-d.ai/engine-type": "vllm"}), m: fresh, now: testStart, want: attrcapacity.Eligible},
		{name: "never scraped", meta: meta(nil), m: &fwkdl.Metrics{CacheNumBlocks: 101, CacheBlockSize: 16}, now: testStart, want: attrcapacity.Stale},
		{name: "stale", meta: meta(nil), m: fresh, now: testStart.Add(2 * time.Second), want: attrcapacity.Stale},
		{name: "no block count", meta: meta(nil), m: &fwkdl.Metrics{UpdateTime: testStart}, now: testStart, want: attrcapacity.NoBlockCount},
		{name: "multi engine", meta: meta(nil), m: fresh, obs: engineObservation{engines: 2}, now: testStart, want: attrcapacity.MultiEngine},
		{name: "hybrid", meta: meta(nil), m: fresh, obs: engineObservation{engines: 1, hybrid: true}, now: testStart, want: attrcapacity.HybridLayout},
		{name: "speculative unconfigured", meta: meta(nil), m: fresh, obs: engineObservation{engines: 1, speculative: true},
			now: testStart, want: attrcapacity.SpeculativeUnconfigured},
		{name: "speculative configured", meta: meta(nil), m: fresh, obs: engineObservation{engines: 1, speculative: true},
			speculativeTokens: 2, now: testStart, want: attrcapacity.Eligible},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config{stalenessThreshold: time.Second, speculativeTokens: tc.speculativeTokens}
			require.Equal(t, tc.want, eligibility(tc.meta, tc.m, tc.obs, cfg, tc.now))
		})
	}
}

func TestView(t *testing.T) {
	l, _ := newTestLedger(t, testAPIConfig())
	ep := newTestEndpoint("a", 0.1, testStart)
	ep.UpdateMetrics(testMetrics(0.1, 3, 0, testStart))
	e := addEndpoint(t, l, ep)
	e.acct.apply(vec{7, 100, 1})

	raw, ok := ep.GetAttributes().Get(l.dk)
	require.True(t, ok, "the view is installed on the endpoint")
	v := raw.(*attrcapacity.EndpointCapacity)
	require.Equal(t, attrcapacity.Axis{Capacity: 100, Booked: 7, Scraped: 10, Used: 10}, v.Memory)
	require.Equal(t, attrcapacity.Axis{Capacity: testStepBudget, Booked: 100, Scraped: 3, Used: 100}, v.Step)
	require.Equal(t, attrcapacity.Axis{Capacity: testSlots, Booked: 1, Scraped: 3, Used: 3}, v.Slots)
	require.Equal(t, attrcapacity.Eligible, v.Eligibility)
	require.Equal(t, int64(testBlockSize), v.BlockSize)
}

// capacityConsumer is a scheduling-layer consumer of the ledger's view.
type capacityConsumer struct{}

func (capacityConsumer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "capacity-consumer", Name: "consumer"}
}

func (capacityConsumer) Filter(_ context.Context, _ *fwksched.InferenceRequest, eps []fwksched.Endpoint) []fwksched.Endpoint {
	return eps
}

func (capacityConsumer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{Optional: map[fwkplugin.DataKey]any{
		attrcapacity.EndpointCapacityDataKey: attrcapacity.EndpointCapacity{},
	}}
}

type fakeTokenProducer struct{}

func (fakeTokenProducer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "fake-token-producer", Name: "tokens"}
}

func (fakeTokenProducer) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{tokenproducer.TokenizedPromptDataKey: fwksched.TokenizedRequest{}}
}

func TestViewConsumableAtSchedulingLayer(t *testing.T) {
	cfg, err := newConfig(testAPIConfig(), 0)
	require.NoError(t, err)
	l := newLedger(CapacityLedgerType, cfg, nil, 0, nil)
	order, err := datalayer.ValidateAndOrderDataDependencies(
		[]fwkplugin.Plugin{fakeTokenProducer{}, l, capacityConsumer{}})
	require.NoError(t, err)
	indexOf := func(name string) int {
		for i, n := range order {
			if n == name {
				return i
			}
		}
		return -1
	}
	require.Less(t, indexOf(l.TypedName().String()), indexOf(capacityConsumer{}.TypedName().String()))
}
