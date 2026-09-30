/*
Copyright 2025 The llm-d Authors.

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

package loader

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	configapiv1 "github.com/llm-d/llm-d-router/apix/config/v1"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkfcmocks "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol/mocks"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	extractormetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	testutils "github.com/llm-d/llm-d-router/test/utils"
)

// mockFilterDetector implements both SaturationDetector and Filter, like the real utilization-detector.
type mockFilterDetector struct{ mockPlugin }

var (
	_ fwksched.Filter = &mockFilterDetector{}
)

func (m *mockFilterDetector) Saturation(_ context.Context, _ []fwkdl.Endpoint) float64 { return 0 }
func (m *mockFilterDetector) Filter(_ context.Context, _ *fwksched.InferenceRequest, eps []fwksched.Endpoint) []fwksched.Endpoint {
	return eps
}

// metricsPlugins returns an allPlugins map with mock stubs for both default metrics plugins.
// Providing them prevents ensureDataLayer from calling registerDefaultPlugin (which needs the
// global factory registry). The function still injects the DataLayer.Sources entries.
func metricsPlugins(handle fwkplugin.Handle) map[string]fwkplugin.Plugin {
	handle.AddPlugin(sourcemetrics.MetricsDataSourceType, &mockPlugin{t: fwkplugin.TypedName{Type: sourcemetrics.MetricsDataSourceType, Name: sourcemetrics.MetricsDataSourceType}})
	handle.AddPlugin(extractormetrics.MetricsExtractorType, &mockPlugin{t: fwkplugin.TypedName{Type: extractormetrics.MetricsExtractorType, Name: extractormetrics.MetricsExtractorType}})
	return handle.GetAllPluginsWithNames()
}

func TestEnsureDataLayer(t *testing.T) {
	// Not parallel: shares helpers with configloader_test.go that depend on global state.

	t.Run("nil DataLayer injects metrics defaults", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{}
		handle := testutils.NewTestHandle(context.Background())

		err := ensureDataLayer(cfg, handle, metricsPlugins(handle))

		require.NoError(t, err)
		require.NotNil(t, cfg.DataLayer)
		require.Len(t, cfg.DataLayer.Sources, 1)
		require.Equal(t, sourcemetrics.MetricsDataSourceType, cfg.DataLayer.Sources[0].PluginRef)
		require.Len(t, cfg.DataLayer.Sources[0].Extractors, 1)
		require.Equal(t, extractormetrics.MetricsExtractorType, cfg.DataLayer.Sources[0].Extractors[0].PluginRef)
	})

	t.Run("empty DataLayer {} injects metrics defaults (regression: was no-op)", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{},
		}
		handle := testutils.NewTestHandle(context.Background())

		err := ensureDataLayer(cfg, handle, metricsPlugins(handle))

		require.NoError(t, err)
		require.Len(t, cfg.DataLayer.Sources, 1)
		require.Equal(t, sourcemetrics.MetricsDataSourceType, cfg.DataLayer.Sources[0].PluginRef)
	})

	t.Run("non-metrics source gets metrics injected too (additive)", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{
				Sources: []configapiv1.DataLayerSource{
					{PluginRef: "k8s-notification-source"},
				},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		err := ensureDataLayer(cfg, handle, metricsPlugins(handle))

		require.NoError(t, err)
		require.Len(t, cfg.DataLayer.Sources, 2)
		refs := []string{cfg.DataLayer.Sources[0].PluginRef, cfg.DataLayer.Sources[1].PluginRef}
		require.Contains(t, refs, "k8s-notification-source")
		require.Contains(t, refs, sourcemetrics.MetricsDataSourceType)
	})

	t.Run("source of another type does not suppress injection", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{
				Sources: []configapiv1.DataLayerSource{
					{PluginRef: "dcgmSource"},
				},
			},
		}
		handle := testutils.NewTestHandle(context.Background())
		allPlugins := metricsPlugins(handle)
		handle.AddPlugin("dcgmSource", &mockPlugin{t: fwkplugin.TypedName{Type: "dcgm-data-source", Name: "dcgmSource"}})

		err := ensureDataLayer(cfg, handle, allPlugins)

		require.NoError(t, err)
		require.Len(t, cfg.DataLayer.Sources, 2)
	})

	t.Run("existing metrics-data-source is not double-injected", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{
				Sources: []configapiv1.DataLayerSource{
					{PluginRef: sourcemetrics.MetricsDataSourceType},
				},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		err := ensureDataLayer(cfg, handle, metricsPlugins(handle))

		require.NoError(t, err)
		require.Len(t, cfg.DataLayer.Sources, 1, "no duplicate metrics source")
	})

	t.Run("metrics source under a custom instance name is not double-injected", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{
				Sources: []configapiv1.DataLayerSource{
					{
						PluginRef:  "metricsSource",
						Extractors: []configapiv1.DataLayerExtractor{{PluginRef: "customMetricsExtractor"}},
					},
				},
			},
		}
		handle := testutils.NewTestHandle(context.Background())
		handle.AddPlugin("metricsSource", &mockPlugin{t: fwkplugin.TypedName{Type: sourcemetrics.MetricsDataSourceType, Name: "metricsSource"}})
		handle.AddPlugin("customMetricsExtractor", &mockPlugin{t: fwkplugin.TypedName{Type: extractormetrics.MetricsExtractorType, Name: "customMetricsExtractor"}})

		err := ensureDataLayer(cfg, handle, handle.GetAllPluginsWithNames())

		require.NoError(t, err)
		require.Len(t, cfg.DataLayer.Sources, 1, "no duplicate metrics source")
		require.Equal(t, "metricsSource", cfg.DataLayer.Sources[0].PluginRef)
		require.Len(t, cfg.DataLayer.Sources[0].Extractors, 1, "no duplicate metrics extractor")
		require.Equal(t, "customMetricsExtractor", cfg.DataLayer.Sources[0].Extractors[0].PluginRef)
	})

	t.Run("injectDefaults: false suppresses injection", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			DataLayer: &configapiv1.DataLayerConfig{
				InjectDefaults: ptr.To(false),
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		err := ensureDataLayer(cfg, handle, metricsPlugins(handle))

		require.NoError(t, err)
		require.Empty(t, cfg.DataLayer.Sources)
	})

}

func TestEnsureSaturationDetector_InjectsFilter(t *testing.T) {
	const detectorName = "my-detector"
	t.Run("detector implementing Filter is injected into profiles", func(t *testing.T) {
		w := 2.0
		cfg := &configapiv1.EndpointPickerConfig{
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer", Weight: &w}}},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		detector := &mockFilterDetector{mockPlugin{t: fwkplugin.TypedName{Type: detectorName, Name: detectorName}}}
		handle.AddPlugin(detectorName, detector)

		allPlugins := handle.GetAllPluginsWithNames()
		cfg.FlowControl = &configapiv1.FlowControlConfig{
			SaturationDetector: &configapiv1.SaturationDetectorConfig{PluginRef: detectorName},
		}

		err := ensureSaturationDetector(cfg, handle, allPlugins)

		require.NoError(t, err)
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 2)
		require.Equal(t, detectorName, cfg.SchedulingProfiles[0].Plugins[1].PluginRef)
	})

	t.Run("detector not implementing Filter is not injected", func(t *testing.T) {
		w := 2.0
		cfg := &configapiv1.EndpointPickerConfig{
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer", Weight: &w}}},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		plainName := "plain-detector"
		detector := &mockSaturationDetector{mockPlugin{t: fwkplugin.TypedName{Type: plainName, Name: plainName}}}
		handle.AddPlugin(plainName, detector)

		allPlugins := handle.GetAllPluginsWithNames()
		cfg.FlowControl = &configapiv1.FlowControlConfig{
			SaturationDetector: &configapiv1.SaturationDetectorConfig{PluginRef: plainName},
		}

		err := ensureSaturationDetector(cfg, handle, allPlugins)

		require.NoError(t, err)
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 1, "non-filter detector should not be injected")
	})

	t.Run("detector already in profile is not duplicated", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{
					{PluginRef: detectorName},
					{PluginRef: "picker"},
				}},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		detector := &mockFilterDetector{mockPlugin{t: fwkplugin.TypedName{Type: detectorName, Name: detectorName}}}
		handle.AddPlugin(detectorName, detector)

		allPlugins := handle.GetAllPluginsWithNames()
		cfg.FlowControl = &configapiv1.FlowControlConfig{
			SaturationDetector: &configapiv1.SaturationDetectorConfig{PluginRef: detectorName},
		}

		err := ensureSaturationDetector(cfg, handle, allPlugins)

		require.NoError(t, err)
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 2, "already present, no duplicate")
	})

	t.Run("detector filter is not injected and explicit detector is preserved when an endpoint gate is configured", func(t *testing.T) {
		w := 2.0
		cfg := &configapiv1.EndpointPickerConfig{
			FeatureGates: configapiv1.FeatureGates{flowcontrol.FeatureGate},
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer", Weight: &w}}},
			},
		}
		handle := testutils.NewTestHandle(context.Background())

		detector := &mockFilterDetector{mockPlugin{t: fwkplugin.TypedName{Type: detectorName, Name: detectorName}}}
		handle.AddPlugin(detectorName, detector)
		handle.AddPlugin("gate", &mockGateDetector{mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{
			TypedNameV: fwkplugin.TypedName{Type: "capacity-ledger", Name: "gate"},
		}}}})

		allPlugins := handle.GetAllPluginsWithNames()
		cfg.FlowControl = &configapiv1.FlowControlConfig{
			SaturationDetector:    &configapiv1.SaturationDetectorConfig{PluginRef: detectorName},
			EndpointGatePluginRef: "gate",
		}

		err := ensureSaturationDetector(cfg, handle, allPlugins)

		require.NoError(t, err)
		require.Equal(t, detectorName, cfg.FlowControl.SaturationDetector.PluginRef)
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 1, "endpoint gate suppresses detector filter injection")
	})

	t.Run("endpoint gate implementing SaturationDetector is reused when saturation detector is omitted", func(t *testing.T) {
		w := 2.0
		cfg := &configapiv1.EndpointPickerConfig{
			FeatureGates: configapiv1.FeatureGates{flowcontrol.FeatureGate},
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer", Weight: &w}}},
			},
			FlowControl: &configapiv1.FlowControlConfig{
				EndpointGatePluginRef: "ledger",
			},
		}
		handle := testutils.NewTestHandle(context.Background())
		gate := &mockGateDetector{mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{
			TypedNameV: fwkplugin.TypedName{Type: "capacity-ledger", Name: "ledger"},
		}}}}
		handle.AddPlugin("ledger", gate)

		err := ensureSaturationDetector(cfg, handle, handle.GetAllPluginsWithNames())

		require.NoError(t, err)
		require.Equal(t, "ledger", cfg.FlowControl.SaturationDetector.PluginRef)
		require.Nil(t, handle.Plugin("utilization-detector"), "utilization-detector should not be registered")
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 1)
	})

	t.Run("endpoint gate is ignored when flowControl feature gate is disabled", func(t *testing.T) {
		w := 2.0
		cfg := &configapiv1.EndpointPickerConfig{
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer", Weight: &w}}},
			},
			FlowControl: &configapiv1.FlowControlConfig{
				EndpointGatePluginRef: "ledger",
			},
		}
		handle := testutils.NewTestHandle(context.Background())
		gate := &mockGateDetector{mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{
			TypedNameV: fwkplugin.TypedName{Type: "capacity-ledger", Name: "ledger"},
		}}}}
		handle.AddPlugin("ledger", gate)
		handle.AddPlugin("utilization-detector", &mockFilterDetector{mockPlugin{
			t: fwkplugin.TypedName{Type: "utilization-detector", Name: "utilization-detector"},
		}})

		err := ensureSaturationDetector(cfg, handle, handle.GetAllPluginsWithNames())

		require.NoError(t, err)
		require.Equal(t, "utilization-detector", cfg.FlowControl.SaturationDetector.PluginRef)
		require.Len(t, cfg.SchedulingProfiles[0].Plugins, 2, "utilization-detector filter should be injected when flowControl is disabled")
		require.Equal(t, "utilization-detector", cfg.SchedulingProfiles[0].Plugins[1].PluginRef)
	})

	t.Run("endpoint gate not implementing SaturationDetector falls back to utilization detector", func(t *testing.T) {
		cfg := &configapiv1.EndpointPickerConfig{
			FeatureGates: configapiv1.FeatureGates{flowcontrol.FeatureGate},
			FlowControl: &configapiv1.FlowControlConfig{
				EndpointGatePluginRef: "gate",
			},
		}
		handle := testutils.NewTestHandle(context.Background())
		gate := &mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{
			TypedNameV: fwkplugin.TypedName{Type: "g", Name: "gate"},
		}}}
		handle.AddPlugin("gate", gate)
		handle.AddPlugin("utilization-detector", &mockFilterDetector{mockPlugin{
			t: fwkplugin.TypedName{Type: "utilization-detector", Name: "utilization-detector"},
		}})

		err := ensureSaturationDetector(cfg, handle, handle.GetAllPluginsWithNames())

		require.NoError(t, err)
		require.Equal(t, "utilization-detector", cfg.FlowControl.SaturationDetector.PluginRef)
	})
}

// mockGate implements EndpointGate; mockGateFilter adds Filter; mockGateDetector also implements
// SaturationDetector.
type mockGate struct{ fwkfcmocks.MockEndpointGate }

type mockGateFilter struct{ mockGate }

type mockGateDetector struct{ mockGateFilter }

func (m *mockGateDetector) Saturation(_ context.Context, _ []fwkdl.Endpoint) float64 { return 0 }

func (m *mockGateFilter) Filter(_ context.Context, _ *fwksched.InferenceRequest, eps []fwksched.Endpoint) []fwksched.Endpoint {
	return eps
}

func TestEnsureSchedulingLayerSkipsEndpointGate(t *testing.T) {
	ctx := context.Background()
	gate := &mockGateDetector{mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{TypedNameV: fwkplugin.TypedName{Type: "g", Name: "gate"}}}}}
	scorer := &mockScorer{mockPlugin{t: fwkplugin.TypedName{Type: "s", Name: "scorer"}}}
	picker := &mockPicker{mockPlugin{t: fwkplugin.TypedName{Type: "p", Name: "picker"}}}
	handler := &mockHandler{mockPlugin{t: fwkplugin.TypedName{Type: "h", Name: "handler"}}}

	t.Run("endpoint gate is excluded from synthesized default profile in ensureSchedulingLayer", func(t *testing.T) {
		handle := testutils.NewTestHandle(ctx)
		handle.AddPlugin("gate", gate)
		handle.AddPlugin("picker", picker)
		handle.AddPlugin("handler", handler)
		cfg := &configapiv1.EndpointPickerConfig{
			FlowControl: &configapiv1.FlowControlConfig{EndpointGatePluginRef: "gate"},
		}
		require.NoError(t, ensureSchedulingLayer(cfg, handle, handle.GetAllPluginsWithNames()))
		require.Len(t, cfg.SchedulingProfiles, 1)
		for _, sp := range cfg.SchedulingProfiles[0].Plugins {
			require.NotEqual(t, "gate", sp.PluginRef)
		}
	})

	t.Run("referenced endpoint gate is prepended to synthesized default profile when flowControl is enabled", func(t *testing.T) {
		registerTestPlugins(t)
		handle := testutils.NewTestHandle(ctx)
		handle.AddPlugin("gate", gate)
		handle.AddPlugin("scorer", scorer)
		handle.AddPlugin("picker", picker)
		handle.AddPlugin("handler", handler)
		cfg := &configapiv1.EndpointPickerConfig{
			FeatureGates: configapiv1.FeatureGates{flowcontrol.FeatureGate},
			FlowControl:  &configapiv1.FlowControlConfig{EndpointGatePluginRef: "gate"},
		}
		require.NoError(t, applySystemDefaults(cfg, handle))
		require.Equal(t, "gate", cfg.FlowControl.SaturationDetector.PluginRef)
		require.Len(t, cfg.SchedulingProfiles, 1)
		require.Equal(t, "gate", cfg.SchedulingProfiles[0].Plugins[0].PluginRef)
	})

	t.Run("referenced endpoint gate is excluded and utilization detector filter is injected when flowControl is disabled", func(t *testing.T) {
		registerTestPlugins(t)
		handle := testutils.NewTestHandle(ctx)
		handle.AddPlugin("gate", gate)
		handle.AddPlugin("scorer", scorer)
		handle.AddPlugin("picker", picker)
		handle.AddPlugin("handler", handler)
		handle.AddPlugin("utilization-detector", &mockFilterDetector{mockPlugin{
			t: fwkplugin.TypedName{Type: "utilization-detector", Name: "utilization-detector"},
		}})
		cfg := &configapiv1.EndpointPickerConfig{
			FlowControl: &configapiv1.FlowControlConfig{EndpointGatePluginRef: "gate"},
		}
		require.NoError(t, applySystemDefaults(cfg, handle))
		require.Equal(t, "utilization-detector", cfg.FlowControl.SaturationDetector.PluginRef)
		require.Len(t, cfg.SchedulingProfiles, 1)
		require.NotContains(t, cfg.SchedulingProfiles[0].Plugins, configapiv1.SchedulingPlugin{PluginRef: "gate"})
		require.Contains(t, cfg.SchedulingProfiles[0].Plugins, configapiv1.SchedulingPlugin{PluginRef: "utilization-detector"})

		cfgExplicit := &configapiv1.EndpointPickerConfig{
			SchedulingProfiles: []configapiv1.SchedulingProfile{
				{Name: "default", Plugins: []configapiv1.SchedulingPlugin{{PluginRef: "scorer"}}},
			},
			FlowControl: &configapiv1.FlowControlConfig{EndpointGatePluginRef: "gate"},
		}
		require.NoError(t, applySystemDefaults(cfgExplicit, handle))
		require.Equal(t, "utilization-detector", cfgExplicit.FlowControl.SaturationDetector.PluginRef)
		require.Equal(t, []configapiv1.SchedulingPlugin{
			{PluginRef: "scorer"},
			{PluginRef: "utilization-detector"},
		}, cfgExplicit.SchedulingProfiles[0].Plugins)
	})
}

func TestValidateEndpointGate(t *testing.T) {
	gate := &mockGateFilter{mockGate{fwkfcmocks.MockEndpointGate{TypedNameV: fwkplugin.TypedName{Type: "g", Name: "gate"}}}}
	otherFilter := &mockFilterDetector{mockPlugin{t: fwkplugin.TypedName{Type: "f", Name: "other-filter"}}}
	plugins := map[string]fwkplugin.Plugin{
		"gate":         gate,
		"other-filter": otherFilter,
		"scorer":       &mockScorer{mockPlugin{t: fwkplugin.TypedName{Type: "sc", Name: "scorer"}}},
		"bare":         &mockGate{fwkfcmocks.MockEndpointGate{TypedNameV: fwkplugin.TypedName{Type: "b", Name: "bare"}}},
		"not-gate":     &mockPlugin{t: fwkplugin.TypedName{Type: "s", Name: "not-gate"}},
	}
	profile := func(name string, refs ...string) configapiv1.SchedulingProfile {
		p := configapiv1.SchedulingProfile{Name: name}
		for _, r := range refs {
			p.Plugins = append(p.Plugins, configapiv1.SchedulingPlugin{PluginRef: r})
		}
		return p
	}
	tests := []struct {
		name      string
		ref       string
		profiles  []configapiv1.SchedulingProfile
		gateOff   bool
		extra     map[string]fwkplugin.Plugin
		wantErr   string
		wantFirst string
	}{
		{name: "no reference", profiles: []configapiv1.SchedulingProfile{profile("a", "scorer")}},
		{name: "flowControl feature gate off: not validated", ref: "missing", gateOff: true},
		{name: "listed first and prepended to profiles that omit it", ref: "gate", profiles: []configapiv1.SchedulingProfile{
			profile("a", "gate", "scorer"), profile("prefill", "scorer")}, wantFirst: "gate"},
		{name: "prepended when omitted from profile", ref: "gate",
			profiles:  []configapiv1.SchedulingProfile{profile("a", "other-filter", "scorer")},
			wantFirst: "gate"},
		{name: "scorer before gate is allowed when gate is first filter", ref: "gate",
			profiles: []configapiv1.SchedulingProfile{profile("a", "scorer", "gate", "other-filter")}},
		{name: "another filter before gate is rejected", ref: "gate",
			profiles: []configapiv1.SchedulingProfile{profile("a", "other-filter", "gate")},
			wantErr:  `must be the first filter in scheduling profile "a"`},
		{name: "non-filter gate is allowed without modifying profiles", ref: "bare",
			profiles:  []configapiv1.SchedulingProfile{profile("a", "scorer")},
			wantFirst: "scorer"},
		{name: "second instance of the gate's type is allowed", ref: "gate",
			profiles:  []configapiv1.SchedulingProfile{profile("a", "scorer")},
			extra:     map[string]fwkplugin.Plugin{"gate2": &mockPlugin{t: fwkplugin.TypedName{Type: "g", Name: "gate2"}}},
			wantFirst: "gate"},
		{name: "undefined", ref: "missing", wantErr: "is not defined"},
		{name: "not a gate", ref: "not-gate", wantErr: "is not an endpoint gate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &configapiv1.EndpointPickerConfig{
				FeatureGates:       configapiv1.FeatureGates{"flowControl"},
				SchedulingProfiles: tc.profiles,
				FlowControl:        &configapiv1.FlowControlConfig{EndpointGatePluginRef: tc.ref},
			}
			if tc.gateOff {
				cfg.FeatureGates = nil
			}
			all := maps.Clone(plugins)
			maps.Copy(all, tc.extra)
			err := validateEndpointGate(cfg, all)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.wantFirst != "" {
				for _, prof := range cfg.SchedulingProfiles {
					require.Equal(t, tc.wantFirst, prof.Plugins[0].PluginRef)
				}
			}
		})
	}
	require.True(t, flowControlSettingsConfigured(&configapiv1.FlowControlConfig{EndpointGatePluginRef: "gate"}))
}
