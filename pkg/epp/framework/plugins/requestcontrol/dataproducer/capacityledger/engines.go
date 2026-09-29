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
	"context"
	"strings"
	"sync"

	dto "github.com/prometheus/client_model/go"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

const (
	engineCounterType = "capacity-ledger-engines"

	vllmCacheConfigInfo = "vllm:cache_config_info"
	vllmRunning         = "vllm:num_requests_running"
	engineLabel         = "engine"
	// vllmSpecDecodePrefix prefixes the metric families vLLM exports when speculative decoding is
	// enabled.
	vllmSpecDecodePrefix = "vllm:spec_decode_"
)

// hybridLayoutLabels are cache_config_info labels whose presence marks a KV cache layout in which a
// request draws blocks from more than one group. vLLM renders unset values as "None".
var hybridLayoutLabels = []string{"sliding_window", "mamba_block_size"}

var _ fwkdl.PollingExtractor[sourcemetrics.PrometheusMetricMap] = (*engineCounter)(nil)

// engineObservation is what the engine counter last read for one endpoint.
type engineObservation struct {
	// engines is the number of distinct engine label values; 0 when the endpoint exports none.
	engines     int
	hybrid      bool
	speculative bool
}

// engineCounter reads the raw metrics scrape to count the engines behind each endpoint and detect
// its KV cache layout and speculative decoding. The metrics extractor keeps one series per metric
// family, so several data-parallel engines behind one port are otherwise read as one. It also calls
// onScrape for every scrape, which records the ledger's bookings at that scrape. It is a separate
// type from the ledger because an endpoint extractor and a polling extractor have different Extract
// signatures.
type engineCounter struct {
	typedName fwkplugin.TypedName
	onScrape  func(fwkdl.Endpoint)
	mu        sync.RWMutex
	obs       map[string]engineObservation
}

func newEngineCounter(ledgerName string, onScrape func(fwkdl.Endpoint)) *engineCounter {
	return &engineCounter{
		typedName: fwkplugin.TypedName{Type: engineCounterType, Name: ledgerName},
		onScrape:  onScrape,
		obs:       map[string]engineObservation{},
	}
}

func (c *engineCounter) TypedName() fwkplugin.TypedName { return c.typedName }

// Extract records the engine count, layout and speculative decoding for the polled endpoint.
func (c *engineCounter) Extract(_ context.Context, in fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]) error {
	if in.Endpoint == nil || in.Endpoint.GetMetadata() == nil {
		return nil
	}
	var obs engineObservation
	family := in.Payload[vllmCacheConfigInfo]
	if family != nil {
		obs.engines = countLabelValues(family, engineLabel)
		obs.hybrid = hasHybridLayout(family)
	} else if running := in.Payload[vllmRunning]; running != nil {
		obs.engines = countLabelValues(running, engineLabel)
	}
	for name := range in.Payload {
		if strings.HasPrefix(name, vllmSpecDecodePrefix) {
			obs.speculative = true
			break
		}
	}
	id := in.Endpoint.GetMetadata().ID.String()
	c.mu.Lock()
	c.obs[id] = obs
	c.mu.Unlock()
	if c.onScrape != nil {
		c.onScrape(in.Endpoint)
	}
	return nil
}

func (c *engineCounter) get(id string) engineObservation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.obs[id]
}

func (c *engineCounter) forget(id string) {
	c.mu.Lock()
	delete(c.obs, id)
	c.mu.Unlock()
}

func countLabelValues(family *dto.MetricFamily, name string) int {
	values := map[string]struct{}{}
	for _, m := range family.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == name {
				values[lp.GetValue()] = struct{}{}
			}
		}
	}
	return len(values)
}

func hasHybridLayout(family *dto.MetricFamily) bool {
	for _, m := range family.GetMetric() {
		for _, lp := range m.GetLabel() {
			for _, name := range hybridLayoutLabels {
				if lp.GetName() == name && lp.GetValue() != "" && lp.GetValue() != "None" {
					return true
				}
			}
		}
	}
	return false
}
