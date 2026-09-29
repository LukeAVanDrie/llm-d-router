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
	"errors"
	"fmt"
	"time"
)

const (
	// defaultStalenessThreshold is the default of --metrics-staleness-threshold, used when the
	// handle reports none.
	defaultStalenessThreshold = 2 * time.Second
	// defaultSampleInterval is used when the handle reports no metrics refresh interval.
	defaultSampleInterval = 50 * time.Millisecond
)

// apiConfig is the ledger's parameters as decoded from the plugin's configuration.
type apiConfig struct {
	// SlotsPerEndpoint is the engine's limit on concurrent sequences per endpoint (vLLM
	// max_num_seqs). The engine does not export it. Required.
	SlotsPerEndpoint *int64 `json:"slotsPerEndpoint,omitempty"`

	// StepTokenBudget is the engine's per-step token budget (vLLM max_num_scheduled_tokens when
	// set, else max_num_batched_tokens). The engine does not export it. Required.
	StepTokenBudget *int64 `json:"stepTokenBudget,omitempty"`

	// SpeculativeTokens is the number of speculative tokens the engine proposes per step (vLLM
	// num_speculative_tokens). The engine reserves this many extra KV slots for every running
	// request and schedules this many extra tokens per decode step. Defaults to 0.
	SpeculativeTokens *int64 `json:"speculativeTokens,omitempty"`

	// MaxModelLen is the model's context length in tokens (vLLM max_model_len). It bounds every
	// request's output, including requests that set no maximum output. Required.
	MaxModelLen *int64 `json:"maxModelLen,omitempty"`

	// PrefixMatchInfoProducerName selects which prefix-cache producer's match to read when
	// computing a lease's uncached prompt. Empty selects the approximate-prefix producer.
	PrefixMatchInfoProducerName string `json:"prefixMatchInfoProducerName,omitempty"`
}

// config is the validated configuration.
type config struct {
	slotsPerEndpoint   int64
	stepTokenBudget    int64
	speculativeTokens  int64
	stalenessThreshold time.Duration
	maxModelLen        int64
	prefixProducerName string
}

// decodeStep is the tokens one decode step schedules for one sequence, and the KV slots one
// sequence holds for that step: the next token's slot plus the speculative tokens.
func (c config) decodeStep() int64 { return 1 + c.speculativeTokens }

// newConfig validates the decoded parameters. stalenessThreshold is the EPP's
// --metrics-staleness-threshold, zero when unset.
func newConfig(api apiConfig, stalenessThreshold time.Duration) (config, error) {
	cfg := config{
		stalenessThreshold: stalenessThreshold,
		prefixProducerName: api.PrefixMatchInfoProducerName,
	}
	if cfg.stalenessThreshold <= 0 {
		cfg.stalenessThreshold = defaultStalenessThreshold
	}
	var err error
	if cfg.slotsPerEndpoint, err = requiredPositive("slotsPerEndpoint", api.SlotsPerEndpoint); err != nil {
		return config{}, err
	}
	if cfg.stepTokenBudget, err = requiredPositive("stepTokenBudget", api.StepTokenBudget); err != nil {
		return config{}, err
	}
	if api.SpeculativeTokens != nil {
		if *api.SpeculativeTokens < 0 {
			return config{}, fmt.Errorf("speculativeTokens must be non-negative, got %d", *api.SpeculativeTokens)
		}
		cfg.speculativeTokens = *api.SpeculativeTokens
	}
	if cfg.maxModelLen, err = requiredPositive("maxModelLen", api.MaxModelLen); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func requiredPositive(name string, v *int64) (int64, error) {
	if v == nil {
		return 0, errors.New(name + " is required")
	}
	if *v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, *v)
	}
	return *v, nil
}
