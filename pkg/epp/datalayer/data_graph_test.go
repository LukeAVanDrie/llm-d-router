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
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const mockProducedDataKey = "mockProducedData"

func TestValidatePluginExecutionOrder(t *testing.T) {
	dkReq := fwkplugin.NewRequestDataKey("keyReq", "mock")
	dkEp := fwkplugin.NewEndpointDataKey("keyEp", "mock")

	pluginA := &mockDataProducerP{
		name: "A",
		produces: map[fwkplugin.DataKey]any{
			dkReq: nil,
			dkEp:  nil,
		},
	}
	consumerFairnessPolicyPlugin := MockConsumerFairnessPolicy{consumes: map[fwkplugin.DataKey]any{dkReq: nil}}
	consumerSchedulingPlugin := MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{dkReq: nil}}

	t.Run("PluginsWithNoDependencies", func(t *testing.T) {
		_, err := ValidateAndOrderDataDependencies([]fwkplugin.Plugin{pluginA})
		assert.NoError(t, err)
	})

	t.Run("InvalidLayerExecutionOrder", func(t *testing.T) {
		_, err := ValidateAndOrderDataDependencies([]fwkplugin.Plugin{pluginA, &consumerFairnessPolicyPlugin})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid plugin layer execution order")
	})

	t.Run("ValidSchedulingPluginDependency", func(t *testing.T) {
		_, err := ValidateAndOrderDataDependencies([]fwkplugin.Plugin{pluginA, &consumerSchedulingPlugin})
		assert.NoError(t, err)
	})
}

func TestDAGAndTopologicalOrder(t *testing.T) {
	dkA := fwkplugin.NewDataKey("keyA", "mock")
	dkB := fwkplugin.NewDataKey("keyB", "mock")
	dkX := fwkplugin.NewDataKey("keyX", "mock")
	dkY := fwkplugin.NewDataKey("keyY", "mock")
	dkZ := fwkplugin.NewDataKey("keyZ", "mock")
	dkP := fwkplugin.NewDataKey("keyP", "mock")

	pluginA := &mockDataProducerP{name: "A", produces: map[fwkplugin.DataKey]any{dkA: nil}}
	pluginB := &mockDataProducerP{name: "B", consumes: map[fwkplugin.DataKey]any{dkA: nil}, produces: map[fwkplugin.DataKey]any{dkB: nil}}
	pluginC := &mockDataProducerP{name: "C", consumes: map[fwkplugin.DataKey]any{dkB: nil}}
	pluginD := &mockDataProducerP{name: "D", consumes: map[fwkplugin.DataKey]any{dkA: nil}}
	pluginE := &mockDataProducerP{name: "E"}

	pluginX := &mockDataProducerP{name: "X", produces: map[fwkplugin.DataKey]any{dkX: nil}, consumes: map[fwkplugin.DataKey]any{dkY: nil}}
	pluginY := &mockDataProducerP{name: "Y", produces: map[fwkplugin.DataKey]any{dkY: nil}, consumes: map[fwkplugin.DataKey]any{dkX: nil}}

	pluginZ1 := &mockDataProducerP{name: "Z1", produces: map[fwkplugin.DataKey]any{dkZ: int(0)}}
	pluginZ2 := &mockDataProducerP{name: "Z2", consumes: map[fwkplugin.DataKey]any{dkZ: string("")}}

	pluginP1 := &mockDataProducerP{name: "P1", produces: map[fwkplugin.DataKey]any{dkP: &mockProducedDataType{}}}
	pluginP2 := &mockDataProducerP{name: "P2", consumes: map[fwkplugin.DataKey]any{dkP: &mockProducedDataType{}}}

	testCases := []struct {
		name        string
		plugins     []fwkrc.DataProducer
		expectedDAG map[string][]string
		expectedErr string
	}{
		{
			name:        "NoPlugins",
			plugins:     []fwkrc.DataProducer{},
			expectedDAG: map[string][]string{},
		},
		{
			name:    "PluginsWithNoDependencies",
			plugins: []fwkrc.DataProducer{pluginA, pluginE},
			expectedDAG: map[string][]string{
				"A/mock": {},
				"E/mock": {},
			},
		},
		{
			name:    "LinearDependency",
			plugins: []fwkrc.DataProducer{pluginA, pluginB, pluginC},
			expectedDAG: map[string][]string{
				"A/mock": {},
				"B/mock": {"A/mock"},
				"C/mock": {"B/mock"},
			},
		},
		{
			name:    "MultipleDependencies",
			plugins: []fwkrc.DataProducer{pluginA, pluginB, pluginD, pluginE},
			expectedDAG: map[string][]string{
				"A/mock": {},
				"B/mock": {"A/mock"},
				"D/mock": {"A/mock"},
				"E/mock": {},
			},
		},
		{
			name:        "GraphWithCycle",
			plugins:     []fwkrc.DataProducer{pluginX, pluginY},
			expectedErr: "cycle detected",
		},
		{
			name:        "DataTypeMismatch",
			plugins:     []fwkrc.DataProducer{pluginZ1, pluginZ2},
			expectedErr: "data type mismatch between produced and consumed data",
		},
		{
			name:    "SameTypeDifferentPointers",
			plugins: []fwkrc.DataProducer{pluginP1, pluginP2},
			expectedDAG: map[string][]string{
				"P1/mock": {},
				"P2/mock": {"P1/mock"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			producers := make(map[string]fwkplugin.ProducerPlugin)
			consumers := make(map[string]fwkplugin.ConsumerPlugin)
			for _, p := range tc.plugins {
				if pp, ok := p.(fwkplugin.ProducerPlugin); ok {
					producers[p.TypedName().String()] = pp
				}
				if cp, ok := p.(fwkplugin.ConsumerPlugin); ok {
					consumers[p.TypedName().String()] = cp
				}
			}
			dag, err := buildDAG(producers, consumers, nil)
			if err != nil {
				if tc.expectedErr != "" {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.expectedErr)
					return
				}
				assert.NoError(t, err)
			}
			orderedPlugins, err := topologicalSort(dag)

			if tc.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
				return
			}
			assert.NoError(t, err)

			normalizedDAG := make(map[string][]string)
			maps.Copy(normalizedDAG, dag)
			normalizedExpectedDAG := make(map[string][]string)
			maps.Copy(normalizedExpectedDAG, tc.expectedDAG)

			if diff := cmp.Diff(normalizedExpectedDAG, normalizedDAG); diff != "" {
				t.Errorf("dataProducerGraph() mismatch (-want +got):\n%s", diff)
			}

			assertTopologicalOrder(t, dag, orderedPlugins)
		})
	}
}

func TestCompilePipeline_Validations(t *testing.T) {
	t.Run("DuplicateProducers_UnspecifiedScope", func(t *testing.T) {
		dkA := fwkplugin.NewDataKey("keyA", "mock")
		p1 := &mockDataProducerP{name: "P1", produces: map[fwkplugin.DataKey]any{dkA: nil}}
		p2 := &mockDataProducerP{name: "P2", produces: map[fwkplugin.DataKey]any{dkA: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{p1, p2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate producers detected")
		assert.Contains(t, err.Error(), "keyA")
	})

	t.Run("DuplicateProducers_RequestScope", func(t *testing.T) {
		dkReq1 := fwkplugin.NewRequestDataKey("sharedKey", "mock")
		dkReq2 := fwkplugin.NewRequestDataKey("sharedKey", "mock")
		pReq1 := &mockDataProducerP{name: "PReq1", produces: map[fwkplugin.DataKey]any{dkReq1: nil}}
		pReq2 := &mockDataProducerP{name: "PReq2", produces: map[fwkplugin.DataKey]any{dkReq2: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pReq1, pReq2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate producers detected")
	})

	t.Run("DuplicateProducers_EndpointScope", func(t *testing.T) {
		dkEp1 := fwkplugin.NewEndpointDataKey("sharedKey", "mock")
		dkEp2 := fwkplugin.NewEndpointDataKey("sharedKey", "mock")
		pEp1 := &mockDataProducerP{name: "PEp1", produces: map[fwkplugin.DataKey]any{dkEp1: nil}}
		pEp2 := &mockDataProducerP{name: "PEp2", produces: map[fwkplugin.DataKey]any{dkEp2: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pEp1, pEp2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate producers detected")
	})

	t.Run("MultipleKeysTypeMismatch", func(t *testing.T) {
		dkA := fwkplugin.NewDataKey("keyA", "mock")
		dkB := fwkplugin.NewDataKey("keyB", "mock")

		pluginA := &mockDataProducerP{
			name: "A",
			produces: map[fwkplugin.DataKey]any{
				dkA: int(0),
				dkB: string(""),
			},
		}
		pluginB := &mockDataProducerP{
			name: "B",
			consumes: map[fwkplugin.DataKey]any{
				dkA: int(0),
				dkB: int(0), // Mismatch!
			},
		}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pluginA, pluginB})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "data type mismatch between produced and consumed data")
	})

	t.Run("KeyCollisionScopeIsolation_Disallowed", func(t *testing.T) {
		dkReq1 := fwkplugin.NewRequestDataKey("sharedKey", "mock")
		dkEp1 := fwkplugin.NewEndpointDataKey("sharedKey", "mock")

		pReq := &mockDataProducerP{name: "PReq", produces: map[fwkplugin.DataKey]any{dkReq1: nil}}
		pEp := &mockDataProducerP{name: "PEp", produces: map[fwkplugin.DataKey]any{dkEp1: nil}}
		admitterReq := &MockPreAdmitter{name: "PreAdmitter", consumes: map[fwkplugin.DataKey]any{dkReq1: nil}}
		admitterEp := &MockAdmitter{name: "Admitter", consumes: map[fwkplugin.DataKey]any{dkEp1: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pReq, pEp, admitterReq, admitterEp})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate producers detected")
		assert.Contains(t, err.Error(), "sharedKey")
	})

	t.Run("ScopeMismatchBetweenProducerAndConsumer_HaltsBoot", func(t *testing.T) {
		dkReq := fwkplugin.NewRequestDataKey("keyA", "mock")
		dkEp := fwkplugin.NewEndpointDataKey("keyA", "mock")

		pReq := &mockDataProducerP{name: "PReq", produces: map[fwkplugin.DataKey]any{dkReq: nil}}
		admitterEp := &MockAdmitter{name: "AdmitterEp", consumes: map[fwkplugin.DataKey]any{dkEp: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pReq, admitterEp})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scope mismatch detected for key")
		assert.Contains(t, err.Error(), "PReq")
		assert.Contains(t, err.Error(), "AdmitterEp")
	})

	t.Run("OptionalScopeMismatchBetweenProducerAndConsumer_HaltsBoot", func(t *testing.T) {
		dkReq := fwkplugin.NewRequestDataKey("keyA", "mock")
		dkEp := fwkplugin.NewEndpointDataKey("keyA", "mock")

		pReq := &mockDataProducerP{name: "PReq", produces: map[fwkplugin.DataKey]any{dkReq: nil}}
		admitterEp := &MockOptionalAdmitter{
			MockAdmitter:     MockAdmitter{name: "AdmitterEp"},
			optionalConsumes: []fwkplugin.DataKey{dkEp},
		}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pReq, admitterEp})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scope mismatch detected for optional key")
		assert.Contains(t, err.Error(), "PReq")
		assert.Contains(t, err.Error(), "AdmitterEp")
	})
}

func TestCompilePipeline_CycleDetection(t *testing.T) {
	t.Run("PreAdmissionCycle", func(t *testing.T) {
		dkReqA := fwkplugin.NewRequestDataKey("keyReqA", "mock")
		dkReqB := fwkplugin.NewRequestDataKey("keyReqB", "mock")

		pA := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "PA",
				produces: map[fwkplugin.DataKey]any{dkReqA: nil},
				consumes: map[fwkplugin.DataKey]any{dkReqB: nil},
			},
			eager: true,
		}
		pB := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "PB",
				produces: map[fwkplugin.DataKey]any{dkReqB: nil},
				consumes: map[fwkplugin.DataKey]any{dkReqA: nil},
			},
			eager: true,
		}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pA, pB})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to sort Pre-Admission slice")
		assert.Contains(t, err.Error(), "cycle detected")
	})

	t.Run("PostAdmissionCycle", func(t *testing.T) {
		dkA := fwkplugin.NewDataKey("keyA", "mock")
		dkB := fwkplugin.NewDataKey("keyB", "mock")

		pA := &mockDataProducerP{
			name:     "PA",
			produces: map[fwkplugin.DataKey]any{dkA: nil},
			consumes: map[fwkplugin.DataKey]any{dkB: nil},
		}
		pB := &mockDataProducerP{
			name:     "PB",
			produces: map[fwkplugin.DataKey]any{dkB: nil},
			consumes: map[fwkplugin.DataKey]any{dkA: nil},
		}
		admitter := &MockAdmitter{
			name:     "Admitter",
			consumes: map[fwkplugin.DataKey]any{dkA: nil},
		}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pA, pB, admitter})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to sort Post-Admission slice")
		assert.Contains(t, err.Error(), "cycle detected")
	})

	t.Run("KahnDFSCyclePathReconstruction", func(t *testing.T) {
		dkX := fwkplugin.NewDataKey("keyX", "mock")
		dkY := fwkplugin.NewDataKey("keyY", "mock")
		dkZ := fwkplugin.NewDataKey("keyZ", "mock")

		pX := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "X",
				produces: map[fwkplugin.DataKey]any{dkX: nil},
				consumes: map[fwkplugin.DataKey]any{dkY: nil},
			},
			eager: true,
		}
		pY := &mockDataProducerP{name: "Y", produces: map[fwkplugin.DataKey]any{dkY: nil}, consumes: map[fwkplugin.DataKey]any{dkZ: nil}}
		pZ := &mockDataProducerP{name: "Z", produces: map[fwkplugin.DataKey]any{dkZ: nil}, consumes: map[fwkplugin.DataKey]any{dkX: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pX, pY, pZ})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cycle detected")
		errStr := err.Error()
		assert.True(t,
			strings.Contains(errStr, "X/mock -> Y/mock -> Z/mock -> X/mock") ||
				strings.Contains(errStr, "Y/mock -> Z/mock -> X/mock -> Y/mock") ||
				strings.Contains(errStr, "Z/mock -> X/mock -> Y/mock -> Z/mock"),
			"expected cycle path in error, got: %s", errStr,
		)
	})

	t.Run("MultipleIndependentCycles", func(t *testing.T) {
		dkX := fwkplugin.NewDataKey("keyX", "mock")
		dkY := fwkplugin.NewDataKey("keyY", "mock")
		dkU := fwkplugin.NewDataKey("keyU", "mock")
		dkV := fwkplugin.NewDataKey("keyV", "mock")

		pX := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "X",
				produces: map[fwkplugin.DataKey]any{dkX: nil},
				consumes: map[fwkplugin.DataKey]any{dkY: nil},
			},
			eager: true,
		}
		pY := &mockDataProducerP{name: "Y", produces: map[fwkplugin.DataKey]any{dkY: nil}, consumes: map[fwkplugin.DataKey]any{dkX: nil}}

		pU := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "U",
				produces: map[fwkplugin.DataKey]any{dkU: nil},
				consumes: map[fwkplugin.DataKey]any{dkV: nil},
			},
			eager: true,
		}
		pV := &mockDataProducerP{name: "V", produces: map[fwkplugin.DataKey]any{dkV: nil}, consumes: map[fwkplugin.DataKey]any{dkU: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pX, pY, pU, pV})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cycle detected")

		errStr := err.Error()
		hasCycle1 := strings.Contains(errStr, "X/mock -> Y/mock -> X/mock") || strings.Contains(errStr, "Y/mock -> X/mock -> Y/mock")
		hasCycle2 := strings.Contains(errStr, "U/mock -> V/mock -> U/mock") || strings.Contains(errStr, "V/mock -> U/mock -> V/mock")
		assert.True(t, hasCycle1 || hasCycle2, "expected at least one cycle path formatted in error, got: %s", errStr)
	})
}

func TestCompilePipeline_Pruning(t *testing.T) {
	t.Run("PruneUnusedProducers", func(t *testing.T) {
		dkA := fwkplugin.NewDataKey("keyA", "mock")
		dkB := fwkplugin.NewDataKey("keyB", "mock")
		dkC := fwkplugin.NewDataKey("keyC", "mock")

		pUsed := &mockDataProducerP{name: "P_used", produces: map[fwkplugin.DataKey]any{dkA: nil}}
		pUnused := &mockDataProducerP{name: "P_unused", produces: map[fwkplugin.DataKey]any{dkB: nil}}
		pEager := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{name: "P_eager", produces: map[fwkplugin.DataKey]any{dkC: nil}},
			eager:             true,
		}

		admitter := &MockAdmitter{name: "Admitter", consumes: map[fwkplugin.DataKey]any{dkA: nil}}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pUsed, pUnused, pEager, admitter})
		assert.NoError(t, err)

		allKept := append(preAdmission, postAdmission...)

		assert.Contains(t, allKept, "P_used/mock")
		assert.Contains(t, allKept, "P_eager/mock")
		assert.Contains(t, allKept, "Admitter/mock")
		assert.NotContains(t, allKept, "P_unused/mock")
	})

	t.Run("KeepEagerProducersDependencies", func(t *testing.T) {
		dkDep := fwkplugin.NewRequestDataKey("depKey", "mock")
		dkEager := fwkplugin.NewRequestDataKey("eagerKey", "mock")

		pDep := &mockDataProducerP{name: "P_dep", produces: map[fwkplugin.DataKey]any{dkDep: nil}}
		pEager := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{
				name:     "P_eager",
				produces: map[fwkplugin.DataKey]any{dkEager: nil},
				consumes: map[fwkplugin.DataKey]any{dkDep: nil},
			},
			eager: true,
		}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pDep, pEager})
		assert.NoError(t, err)

		allKept := append(preAdmission, postAdmission...)
		assert.Contains(t, allKept, "P_eager/mock")
		assert.Contains(t, allKept, "P_dep/mock")
	})

	t.Run("DeepDependencyPruningChain", func(t *testing.T) {
		dkA := fwkplugin.NewRequestDataKey("keyA", "mock")
		dkB := fwkplugin.NewRequestDataKey("keyB", "mock")
		dkC := fwkplugin.NewRequestDataKey("keyC", "mock")
		dkD := fwkplugin.NewRequestDataKey("keyD", "mock")
		dkE := fwkplugin.NewRequestDataKey("keyE", "mock")
		dkUnused := fwkplugin.NewRequestDataKey("keyUnused", "mock")

		pA := &mockDataProducerP{name: "PA", produces: map[fwkplugin.DataKey]any{dkA: nil}}
		pB := &mockDataProducerP{name: "PB", consumes: map[fwkplugin.DataKey]any{dkA: nil}, produces: map[fwkplugin.DataKey]any{dkB: nil}}
		pC := &mockDataProducerP{name: "PC", consumes: map[fwkplugin.DataKey]any{dkB: nil}, produces: map[fwkplugin.DataKey]any{dkC: nil}}
		pD := &mockDataProducerP{name: "PD", consumes: map[fwkplugin.DataKey]any{dkC: nil}, produces: map[fwkplugin.DataKey]any{dkD: nil}}
		pE := &mockDataProducerP{name: "PE", consumes: map[fwkplugin.DataKey]any{dkD: nil}, produces: map[fwkplugin.DataKey]any{dkE: nil}}

		pUnused := &mockDataProducerP{name: "PUnused", produces: map[fwkplugin.DataKey]any{dkUnused: nil}}

		admitter := &MockAdmitter{name: "Admitter", consumes: map[fwkplugin.DataKey]any{dkE: nil}}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pA, pB, pC, pD, pE, pUnused, admitter})
		assert.NoError(t, err)

		allKept := append(preAdmission, postAdmission...)

		assert.Contains(t, allKept, "PA/mock")
		assert.Contains(t, allKept, "PB/mock")
		assert.Contains(t, allKept, "PC/mock")
		assert.Contains(t, allKept, "PD/mock")
		assert.Contains(t, allKept, "PE/mock")
		assert.Contains(t, allKept, "Admitter/mock")
		assert.NotContains(t, allKept, "PUnused/mock")
	})
}

func TestCompilePipeline_PipelineSlicing(t *testing.T) {
	t.Run("PrePostAdmissionSplit", func(t *testing.T) {
		dkReqA := fwkplugin.NewRequestDataKey("reqKeyA", "mock")
		dkEpB := fwkplugin.NewEndpointDataKey("epKeyB", "mock")

		pPre := &mockDataProducerP{name: "P_pre", produces: map[fwkplugin.DataKey]any{dkReqA: nil}}
		preAdmitter := &MockPreAdmitter{name: "PreAdmitter", consumes: map[fwkplugin.DataKey]any{dkReqA: nil}}

		pPost := &mockDataProducerP{name: "P_post", produces: map[fwkplugin.DataKey]any{dkEpB: nil}}
		admitter := &MockAdmitter{name: "Admitter", consumes: map[fwkplugin.DataKey]any{dkEpB: nil}}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pPre, preAdmitter, pPost, admitter})
		assert.NoError(t, err)

		assert.Contains(t, preAdmission, "PreAdmitter/mock")
		assert.Contains(t, preAdmission, "P_pre/mock")
		assert.NotContains(t, preAdmission, "Admitter/mock")
		assert.NotContains(t, preAdmission, "P_post/mock")

		assert.Contains(t, postAdmission, "Admitter/mock")
		assert.Contains(t, postAdmission, "P_post/mock")
		assert.NotContains(t, postAdmission, "PreAdmitter/mock")
		assert.NotContains(t, postAdmission, "P_pre/mock")
	})

	t.Run("PreAdmissionRootsAndClosure", func(t *testing.T) {
		dkReqA := fwkplugin.NewRequestDataKey("reqKeyA", "mock")
		dkReqB := fwkplugin.NewRequestDataKey("reqKeyB", "mock")
		dkEpC := fwkplugin.NewEndpointDataKey("epKeyC", "mock")

		pPreA := &mockDataProducerP{name: "P_pre_A", produces: map[fwkplugin.DataKey]any{dkReqA: nil}}
		pPreB := &mockDataProducerP{name: "P_pre_B", produces: map[fwkplugin.DataKey]any{dkReqB: nil}}

		fairness := &MockConsumerFairnessPolicy{
			MockFairnessPolicy: fwkfcmocks.MockFairnessPolicy{
				TypedNameV: fwkplugin.TypedName{Name: "MyFairnessPolicy", Type: "mock"},
			},
			consumes: map[fwkplugin.DataKey]any{dkReqA: nil},
		}
		ordering := &MockConsumerOrderingPolicy{
			MockOrderingPolicy: fwkfcmocks.MockOrderingPolicy{
				TypedNameV: fwkplugin.TypedName{Name: "MyOrderingPolicy", Type: "mock"},
			},
			consumes: map[fwkplugin.DataKey]any{dkReqB: nil},
		}

		pPost := &mockDataProducerP{name: "P_post", produces: map[fwkplugin.DataKey]any{dkEpC: nil}}
		admitter := &MockAdmitter{name: "Admitter", consumes: map[fwkplugin.DataKey]any{dkEpC: nil}}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pPreA, pPreB, fairness, ordering, pPost, admitter})
		assert.NoError(t, err)

		assert.Contains(t, preAdmission, "MyFairnessPolicy/mock")
		assert.Contains(t, preAdmission, "P_pre_A/mock")
		assert.Contains(t, preAdmission, "MyOrderingPolicy/mock")
		assert.Contains(t, preAdmission, "P_pre_B/mock")

		assert.Contains(t, postAdmission, "P_post/mock")
		assert.Contains(t, postAdmission, "Admitter/mock")
	})

	t.Run("EagerRequestScopedProducersArePreAdmissionRoots", func(t *testing.T) {
		dkReq := fwkplugin.NewRequestDataKey("reqKey", "mock")
		dkEp := fwkplugin.NewEndpointDataKey("epKey", "mock")

		pEagerReq := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{name: "PEagerReq", produces: map[fwkplugin.DataKey]any{dkReq: nil}},
			eager:             true,
		}
		pEagerEp := &mockEagerProducer{
			mockDataProducerP: mockDataProducerP{name: "PEagerEp", produces: map[fwkplugin.DataKey]any{dkEp: nil}},
			eager:             true,
		}

		preAdmission, postAdmission, err := CompilePipeline([]fwkplugin.Plugin{pEagerReq, pEagerEp})
		assert.NoError(t, err)

		assert.Contains(t, preAdmission, "PEagerReq/mock")
		assert.NotContains(t, preAdmission, "PEagerEp/mock")

		assert.Contains(t, postAdmission, "PEagerEp/mock")
		assert.NotContains(t, postAdmission, "PEagerReq/mock")
	})

	t.Run("TransitiveClosureRequestScopedDependencies", func(t *testing.T) {
		dkReqA := fwkplugin.NewRequestDataKey("reqKeyA", "mock")
		dkReqB := fwkplugin.NewRequestDataKey("reqKeyB", "mock")

		pA := &mockDataProducerP{name: "PA", produces: map[fwkplugin.DataKey]any{dkReqA: nil}}
		pB := &mockDataProducerP{name: "PB", produces: map[fwkplugin.DataKey]any{dkReqB: nil}, consumes: map[fwkplugin.DataKey]any{dkReqA: nil}}
		preAdmitter := &MockPreAdmitter{name: "PreAdmitter", consumes: map[fwkplugin.DataKey]any{dkReqB: nil}}

		preAdmission, _, err := CompilePipeline([]fwkplugin.Plugin{pA, pB, preAdmitter})
		assert.NoError(t, err)

		assert.Contains(t, preAdmission, "PA/mock")
		assert.Contains(t, preAdmission, "PB/mock")
		assert.Contains(t, preAdmission, "PreAdmitter/mock")
	})

	t.Run("PreAdmissionInvalidScopesHaltBoot", func(t *testing.T) {
		dkEp := fwkplugin.NewEndpointDataKey("epKey", "mock")

		pEp := &mockDataProducerP{name: "PEp", produces: map[fwkplugin.DataKey]any{dkEp: nil}}
		preAdmitter := &MockPreAdmitter{name: "PreAdmitter", consumes: map[fwkplugin.DataKey]any{dkEp: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pEp, preAdmitter})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scope mismatch")
		assert.Contains(t, err.Error(), "Pre-Admission consumes key")
	})

	t.Run("PreAdmissionCoercedUnspecifiedScopeIsEndpointScope", func(t *testing.T) {
		dkUnspecified := fwkplugin.NewDataKey("unspecKey", "mock")

		pUnspec := &mockDataProducerP{name: "PUnspec", produces: map[fwkplugin.DataKey]any{dkUnspecified: nil}}
		preAdmitter := &MockPreAdmitter{name: "PreAdmitter", consumes: map[fwkplugin.DataKey]any{dkUnspecified: nil}}

		_, _, err := CompilePipeline([]fwkplugin.Plugin{pUnspec, preAdmitter})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scope mismatch")
		assert.Contains(t, err.Error(), "Pre-Admission consumes key")
	})

	t.Run("KeepOptionalDependenciesOfActiveConsumers", func(t *testing.T) {
		dkOpt := fwkplugin.NewRequestDataKey("optKey", "mock")

		pOpt := &mockDataProducerP{name: "POpt", produces: map[fwkplugin.DataKey]any{dkOpt: nil}}

		admitterReq := &MockOptionalPreAdmitter{
			MockPreAdmitter:  MockPreAdmitter{name: "PreAdmitter", consumes: nil},
			optionalConsumes: []fwkplugin.DataKey{dkOpt},
		}

		preAdmission, _, err := CompilePipeline([]fwkplugin.Plugin{pOpt, admitterReq})
		assert.NoError(t, err)

		assert.Contains(t, preAdmission, "POpt/mock")
		assert.Contains(t, preAdmission, "PreAdmitter/mock")

		indexOfProducer := -1
		indexOfConsumer := -1
		for i, name := range preAdmission {
			switch name {
			case "POpt/mock":
				indexOfProducer = i
			case "PreAdmitter/mock":
				indexOfConsumer = i
			}
		}
		assert.True(t, indexOfProducer != -1 && indexOfConsumer != -1)
		assert.Less(t, indexOfProducer, indexOfConsumer, "Producer POpt should execute before optional consumer PreAdmitter")
	})
}

func TestCreateMissingDataProducers(t *testing.T) {
	producerTypeA := "producer-a"
	producerTypeB := "producer-b"
	nonProducerType := "non-producer"
	failingType := "failing"

	keyA := fwkplugin.NewDataKey("keyA", producerTypeA)
	keyB := fwkplugin.NewDataKey("keyB", producerTypeB)
	keyAFailing := fwkplugin.NewDataKey("keyA", failingType)
	keyANonProducer := fwkplugin.NewDataKey("keyA", nonProducerType)

	producerAFactory := fwkplugin.FactoryFunc(func(name string, _ *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
		return &mockDataProducerP{name: name, produces: map[fwkplugin.DataKey]any{keyA: nil}}, nil
	})

	producerBFactory := fwkplugin.FactoryFunc(func(name string, _ *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
		return &mockDataProducerP{name: name, produces: map[fwkplugin.DataKey]any{keyB: nil}}, nil
	})

	nonProducerFactory := fwkplugin.FactoryFunc(func(name string, _ *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
		return &MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyA: nil}}, nil
	})

	failingFactory := fwkplugin.FactoryFunc(func(name string, _ *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
		return nil, errors.New("requires params")
	})

	testCases := []struct {
		name                    string
		existingPlugins         []fwkplugin.Plugin
		defaultProducerRegistry map[string]string
		factoryRegistry         map[string]fwkplugin.FactoryFunc
		wantTypes               []string
		wantErr                 bool
	}{
		{
			name: "creates producer for missing consumed key",
			existingPlugins: []fwkplugin.Plugin{
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyA: nil}},
			},
			defaultProducerRegistry: map[string]string{keyA.String(): producerTypeA},
			factoryRegistry:         map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:               []string{producerTypeA},
		},
		{
			name: "no missing keys - nothing created",
			existingPlugins: []fwkplugin.Plugin{
				&mockDataProducerP{name: "existing-a", produces: map[fwkplugin.DataKey]any{keyA: nil}},
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyA: nil}},
			},
			factoryRegistry: map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:       nil,
		},
		{
			name: "producer already present by type - not duplicated",
			existingPlugins: []fwkplugin.Plugin{
				&typedMockPlugin{typeName: producerTypeA, produces: map[fwkplugin.DataKey]any{keyA: nil}},
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyA: nil}},
			},
			factoryRegistry: map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:       nil,
		},
		{
			name: "failing factory returns error",
			existingPlugins: []fwkplugin.Plugin{
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyAFailing: nil}},
			},
			defaultProducerRegistry: map[string]string{keyAFailing.String(): failingType},
			factoryRegistry:         map[string]fwkplugin.FactoryFunc{failingType: failingFactory},
			wantErr:                 true,
		},
		{
			name: "non-ProducerPlugin registry entry is invalid",
			existingPlugins: []fwkplugin.Plugin{
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyANonProducer: nil}},
			},
			defaultProducerRegistry: map[string]string{keyANonProducer.String(): nonProducerType},
			factoryRegistry:         map[string]fwkplugin.FactoryFunc{nonProducerType: nonProducerFactory},
			wantErr:                 true,
		},
		{
			name: "only relevant producer is created among multiple registry entries",
			existingPlugins: []fwkplugin.Plugin{
				&MockSchedulingPlugin{consumes: map[fwkplugin.DataKey]any{keyA: nil}},
			},
			defaultProducerRegistry: map[string]string{keyA.String(): producerTypeA},
			factoryRegistry: map[string]fwkplugin.FactoryFunc{
				producerTypeA: producerAFactory,
				producerTypeB: producerBFactory,
			},
			wantTypes: []string{producerTypeA},
		},
		{
			name:            "no consumers - nothing created",
			existingPlugins: []fwkplugin.Plugin{},
			factoryRegistry: map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:       nil,
		},
		{
			name: "missing optional key with no default producer is skipped",
			existingPlugins: []fwkplugin.Plugin{
				&MockOptionalPreAdmitter{
					MockPreAdmitter:  MockPreAdmitter{name: "PreAdmitter"},
					optionalConsumes: []fwkplugin.DataKey{keyA},
				},
			},
			defaultProducerRegistry: map[string]string{},
			factoryRegistry:         map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:               nil,
			wantErr:                 false,
		},
		{
			name: "missing optional key WITH default producer is created",
			existingPlugins: []fwkplugin.Plugin{
				&MockOptionalPreAdmitter{
					MockPreAdmitter:  MockPreAdmitter{name: "PreAdmitter"},
					optionalConsumes: []fwkplugin.DataKey{keyA},
				},
			},
			defaultProducerRegistry: map[string]string{keyA.String(): producerTypeA},
			factoryRegistry:         map[string]fwkplugin.FactoryFunc{producerTypeA: producerAFactory},
			wantTypes:               []string{producerTypeA},
			wantErr:                 false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handle := fwkplugin.NewEppHandle(context.Background(), func() []k8stypes.NamespacedName { return nil })
			for _, p := range tc.existingPlugins {
				handle.AddPlugin(p.TypedName().Name, p)
			}

			err := CreateMissingDataProducers(context.Background(), tc.defaultProducerRegistry, tc.factoryRegistry, handle)

			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			var gotNames []string
			for _, p := range handle.GetAllPlugins() {
				isExisting := false
				for _, ep := range tc.existingPlugins {
					if ep.TypedName() == p.TypedName() {
						isExisting = true
						break
					}
				}
				if !isExisting {
					gotNames = append(gotNames, p.TypedName().Name)
				}
			}

			assert.ElementsMatch(t, tc.wantTypes, gotNames)
		})
	}
}

// --- Mocks and Helper Assertions ---

type mockDataProducerP struct {
	name     string
	produces map[fwkplugin.DataKey]any
	consumes map[fwkplugin.DataKey]any
}

type mockProducedDataType struct {
	value int
}

func (m *mockProducedDataType) Clone() fwkdl.Cloneable {
	return &mockProducedDataType{value: m.value}
}

func (m *mockDataProducerP) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Name: m.name, Type: "mock"}
}

func (m *mockDataProducerP) Produces() map[fwkplugin.DataKey]any {
	return m.produces
}

func (m *mockDataProducerP) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

func (m *mockDataProducerP) Produce(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	endpoints[0].Put(mockProducedDataKey, &mockProducedDataType{value: 42})
	return nil
}

type typedMockPlugin struct {
	typeName string
	produces map[fwkplugin.DataKey]any
}

func (m *typedMockPlugin) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Name: m.typeName, Type: m.typeName}
}

func (m *typedMockPlugin) Produces() map[fwkplugin.DataKey]any { return m.produces }
func (m *typedMockPlugin) Produce(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	return nil
}

type MockConsumerFairnessPolicy struct {
	fwkfcmocks.MockFairnessPolicy
	consumes map[fwkplugin.DataKey]any
}

func (m *MockConsumerFairnessPolicy) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

type MockConsumerOrderingPolicy struct {
	fwkfcmocks.MockOrderingPolicy
	consumes map[fwkplugin.DataKey]any
}

func (m *MockConsumerOrderingPolicy) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

type MockSchedulingPlugin struct {
	fwksched.Scorer
	consumes map[fwkplugin.DataKey]any
}

func (m *MockSchedulingPlugin) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Name: "MockSchedulingPlugin", Type: "mock"}
}

func (m *MockSchedulingPlugin) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

type mockEagerProducer struct {
	mockDataProducerP
	eager bool
}

func (m *mockEagerProducer) Eager() bool {
	return m.eager
}

type MockAdmitter struct {
	name     string
	consumes map[fwkplugin.DataKey]any
}

func (m *MockAdmitter) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Name: m.name, Type: "mock"}
}

func (m *MockAdmitter) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

func (m *MockAdmitter) Admit(ctx context.Context, request *fwksched.InferenceRequest, pods []fwksched.Endpoint) error {
	return nil
}

type MockPreAdmitter struct {
	name     string
	consumes map[fwkplugin.DataKey]any
}

func (m *MockPreAdmitter) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Name: m.name, Type: "mock"}
}

func (m *MockPreAdmitter) Consumes() map[fwkplugin.DataKey]any {
	return m.consumes
}

func (m *MockPreAdmitter) PreAdmit(ctx context.Context, request *fwksched.InferenceRequest) error {
	return nil
}

type MockOptionalPreAdmitter struct {
	MockPreAdmitter
	optionalConsumes []fwkplugin.DataKey
}

func (m *MockOptionalPreAdmitter) OptionalConsumes() []fwkplugin.DataKey {
	return m.optionalConsumes
}

type MockOptionalAdmitter struct {
	MockAdmitter
	optionalConsumes []fwkplugin.DataKey
}

func (m *MockOptionalAdmitter) OptionalConsumes() []fwkplugin.DataKey {
	return m.optionalConsumes
}

func assertTopologicalOrder(t *testing.T, dag map[string][]string, ordered []string) {
	t.Helper()
	positions := make(map[string]int)
	for i, p := range ordered {
		positions[p] = i
	}

	for node, dependencies := range dag {
		for _, dep := range dependencies {
			assert.Less(t, positions[dep], positions[node], "Dependency %s should come before %s", dep, node)
		}
	}
}
