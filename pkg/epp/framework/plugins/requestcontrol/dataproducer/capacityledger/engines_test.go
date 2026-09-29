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
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

func parseMetrics(t *testing.T, text string) sourcemetrics.PrometheusMetricMap {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	require.NoError(t, err)
	return families
}

func TestEngineCounter(t *testing.T) {
	tests := []struct {
		name            string
		text            string
		wantEngines     int
		wantHybrid      bool
		wantSpeculative bool
	}{
		{
			name: "one engine",
			text: `vllm:cache_config_info{block_size="16",engine="0",mamba_block_size="None",sliding_window="None"} 1
`,
			wantEngines: 1,
		},
		{
			name: "two engines behind one port",
			text: `vllm:cache_config_info{block_size="16",engine="0",sliding_window="None"} 1
vllm:cache_config_info{block_size="16",engine="1",sliding_window="None"} 1
`,
			wantEngines: 2,
		},
		{
			name: "sliding window",
			text: `vllm:cache_config_info{block_size="16",engine="0",sliding_window="4096"} 1
`,
			wantEngines: 1,
			wantHybrid:  true,
		},
		{
			name: "fallback to per-engine gauge",
			text: `vllm:num_requests_running{engine="0",model_name="m"} 3
vllm:num_requests_running{engine="1",model_name="m"} 2
`,
			wantEngines: 2,
		},
		{
			name: "speculative decoding",
			text: `vllm:cache_config_info{block_size="16",engine="0"} 1
vllm:spec_decode_num_drafts_total{engine="0",model_name="m"} 10
`,
			wantEngines:     1,
			wantSpeculative: true,
		},
		{
			name:        "no vllm families",
			text:        "other_metric 1\n",
			wantEngines: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var scraped fwkdl.Endpoint
			c := newEngineCounter("l", func(ep fwkdl.Endpoint) { scraped = ep })
			ep := newTestEndpoint("a", 0, testStart)
			require.NoError(t, c.Extract(context.Background(),
				fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{Payload: parseMetrics(t, tc.text), Endpoint: ep}))
			require.Same(t, fwkdl.Endpoint(ep), scraped, "every scrape is reported")
			obs := c.get("ns/a")
			require.Equal(t, tc.wantEngines, obs.engines)
			require.Equal(t, tc.wantHybrid, obs.hybrid)
			require.Equal(t, tc.wantSpeculative, obs.speculative)
			c.forget("ns/a")
			require.Equal(t, engineObservation{}, c.get("ns/a"))
		})
	}
}
