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

package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFootprint_Sub(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		a, b     Footprint
		expected Footprint
		wantErr  bool
	}{
		{
			name:     "ExactDrain",
			a:        Footprint{KVTokens: 100, Slots: 2},
			b:        Footprint{KVTokens: 100, Slots: 2},
			expected: Footprint{},
		},
		{
			name:     "Partial",
			a:        Footprint{KVTokens: 100, Slots: 2},
			b:        Footprint{KVTokens: 40, Slots: 1},
			expected: Footprint{KVTokens: 60, Slots: 1},
		},
		{
			name:    "UnderflowOnKV",
			a:       Footprint{KVTokens: 10, Slots: 5},
			b:       Footprint{KVTokens: 11, Slots: 1},
			wantErr: true,
		},
		{
			name:    "UnderflowOnSlotsAloneStillFails",
			a:       Footprint{KVTokens: 100, Slots: 0},
			b:       Footprint{KVTokens: 1, Slots: 1},
			wantErr: true,
		},
		{
			name:    "UnderflowOnPrefillAloneStillFails",
			a:       Footprint{KVTokens: 100, PrefillTokens: 5, Slots: 2},
			b:       Footprint{KVTokens: 1, PrefillTokens: 6, Slots: 1},
			wantErr: true,
		},
		{
			name:     "PrefillDrainsIndependently",
			a:        Footprint{KVTokens: 100, PrefillTokens: 100, Slots: 1},
			b:        Footprint{PrefillTokens: 100},
			expected: Footprint{KVTokens: 100, Slots: 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.a.Sub(tc.b)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrUnderflow, "underflow must be surfaced, never clamped")
				assert.Equal(t, Footprint{}, got, "the result must not be usable on error")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestFootprint_Fits(t *testing.T) {
	t.Parallel()
	avail := Footprint{KVTokens: 100, Slots: 4}

	assert.True(t, Footprint{KVTokens: 100, Slots: 4}.Fits(avail), "an exact fit is a fit")
	assert.True(t, Footprint{}.Fits(avail), "a zero footprint always fits")
	assert.False(t, Footprint{KVTokens: 101, Slots: 1}.Fits(avail), "KV alone can refuse")
	assert.False(t, Footprint{KVTokens: 1, Slots: 5}.Fits(avail), "slots alone can refuse")
	assert.True(t, Footprint{}.Fits(Footprint{}), "nothing fits in nothing")
}

func TestGatedAxes_Gate(t *testing.T) {
	t.Parallel()
	fp := Footprint{KVTokens: 4096, Slots: 1}

	// Stage 2: slots carry admission authority, KV is booked but never refuses.
	gated := GatedAxes{Slots: true}.gate(fp)
	assert.Equal(t, Footprint{Slots: 1}, gated)
	assert.True(t, gated.Fits(Footprint{KVTokens: 0, Slots: 1}),
		"an ungated axis must fit even against zero availability")

	assert.Equal(t, Footprint{}, GatedAxes{}.gate(fp), "gating nothing runs the ledger as pure accounting")
	assert.Equal(t, fp, GatedAxes{KVTokens: true, PrefillTokens: true, Slots: true}.gate(fp))

	backlog := Footprint{KVTokens: 4096, PrefillTokens: 4096, Slots: 1}
	assert.Equal(t, Footprint{PrefillTokens: 4096},
		GatedAxes{PrefillTokens: true}.gate(backlog),
		"the backlog axis can gate alone, without residency authority")
}

func TestTokenTranslator(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		pred     Prediction
		expected Footprint
	}{
		{
			name:     "PromptPlusOutput",
			pred:     Prediction{PromptTokens: 1000, OutputTokens: 256, Branching: 1},
			expected: Footprint{KVTokens: 1256, PrefillTokens: 1000, Slots: 1},
		},
		{
			name:     "CachedPrefixDiscountsPromptOnly",
			pred:     Prediction{PromptTokens: 1000, OutputTokens: 256, CachedTokens: 900, Branching: 1},
			expected: Footprint{KVTokens: 356, PrefillTokens: 100, Slots: 1},
		},
		{
			name:     "CacheHitExceedingPromptDoesNotGoNegative",
			pred:     Prediction{PromptTokens: 100, OutputTokens: 10, CachedTokens: 500, Branching: 1},
			expected: Footprint{KVTokens: 10, Slots: 1},
		},
		{
			name:     "UnsetBranchingIsOne",
			pred:     Prediction{PromptTokens: 100, OutputTokens: 10},
			expected: Footprint{KVTokens: 110, PrefillTokens: 100, Slots: 1},
		},
		{
			name: "BranchingScalesOutputAndSlots",
			pred: Prediction{PromptTokens: 100, OutputTokens: 10, Branching: 4},
			// The prompt is shared across branches; only decode replicates.
			expected: Footprint{KVTokens: 140, PrefillTokens: 100, Slots: 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, TokenTranslator{}.ToFootprint(tc.pred))

			engine := TokenTranslator{}.ToEngineFootprint(tc.pred)
			assert.Equal(t, EngineFootprint{KVBlocks: tc.expected.KVTokens, Slots: tc.expected.Slots}, engine,
				"the stage-2 translation is token-denominated, so the two unit systems coincide")
		})
	}
}
