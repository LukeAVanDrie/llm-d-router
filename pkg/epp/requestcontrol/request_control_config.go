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

package requestcontrol

import (
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
)

// NewConfig creates a new Config object and returns its pointer.
func NewConfig() *Config {
	return &Config{
		preAdmissionPlugins:        []fwkrc.PreAdmitter{},
		admissionPlugins:           []fwkrc.Admitter{},
		dataProducerPlugins:        []fwkrc.DataProducer{},
		preAdmissionDataProducers:  []fwkrc.DataProducer{},
		postAdmissionDataProducers: []fwkrc.DataProducer{},
		preRequestPlugins:          []fwkrc.PreRequest{},
		responseReceivedPlugins:    []fwkrc.ResponseHeaderProcessor{},
		responseStreamingPlugins:   []fwkrc.ResponseBodyProcessor{},
	}
}

// Config provides a configuration for the requestcontrol plugins.
type Config struct {
	preAdmissionPlugins        []fwkrc.PreAdmitter
	admissionPlugins           []fwkrc.Admitter
	dataProducerPlugins        []fwkrc.DataProducer // Flat repository to keep backward-compatibility
	preAdmissionDataProducers  []fwkrc.DataProducer
	postAdmissionDataProducers []fwkrc.DataProducer
	preRequestPlugins          []fwkrc.PreRequest
	responseReceivedPlugins    []fwkrc.ResponseHeaderProcessor
	responseStreamingPlugins   []fwkrc.ResponseBodyProcessor
}

// WithPreAdmissionPlugins sets the given plugins as the PreAdmitter plugins.
func (c *Config) WithPreAdmissionPlugins(plugins ...fwkrc.PreAdmitter) *Config {
	c.preAdmissionPlugins = plugins
	return c
}

// WithPreRequestPlugins sets the given plugins as the PreRequest plugins.
// If the Config has PreRequest plugins already, this call replaces the existing plugins with the given ones.
func (c *Config) WithPreRequestPlugins(plugins ...fwkrc.PreRequest) *Config {
	c.preRequestPlugins = plugins
	return c
}

// WithResponseReceivedPlugins sets the given plugins as the ResponseReceived plugins.
// If the Config has ResponseReceived plugins already, this call replaces the existing plugins with the given ones.
func (c *Config) WithResponseReceivedPlugins(plugins ...fwkrc.ResponseHeaderProcessor) *Config {
	c.responseReceivedPlugins = plugins
	return c
}

// WithResponseStreamingPlugins sets the given plugins as the ResponseStreaming plugins.
// If the Config has ResponseStreaming plugins already, this call replaces the existing plugins with the given ones.
func (c *Config) WithResponseStreamingPlugins(plugins ...fwkrc.ResponseBodyProcessor) *Config {
	c.responseStreamingPlugins = plugins
	return c
}

// WithDataProducerPlugins sets the given plugins as the DataProducer plugins.
func (c *Config) WithDataProducerPlugins(plugins ...fwkrc.DataProducer) *Config {
	c.dataProducerPlugins = plugins
	c.postAdmissionDataProducers = plugins
	return c
}

// WithPreAdmissionDataProducers sets the Pre-Admission data producer plugins.
func (c *Config) WithPreAdmissionDataProducers(plugins ...fwkrc.DataProducer) *Config {
	c.preAdmissionDataProducers = plugins
	return c
}

// WithPostAdmissionDataProducers sets the Post-Admission data producer plugins.
func (c *Config) WithPostAdmissionDataProducers(plugins ...fwkrc.DataProducer) *Config {
	c.postAdmissionDataProducers = plugins
	return c
}

// WithAdmissionPlugins sets the given plugins as the Admit plugins.
func (c *Config) WithAdmissionPlugins(plugins ...fwkrc.Admitter) *Config {
	c.admissionPlugins = plugins
	return c
}

// AddPlugins adds the given plugins to the Config.
// The type of each plugin is checked and added to the corresponding list of plugins in the Config.
// If a plugin implements multiple plugin interfaces, it will be added to each corresponding list.
func (c *Config) AddPlugins(pluginObjects ...plugin.Plugin) {
	for _, plugin := range pluginObjects {
		if preAdmissionProcessor, ok := plugin.(fwkrc.PreAdmitter); ok {
			c.preAdmissionPlugins = append(c.preAdmissionPlugins, preAdmissionProcessor)
		}
		if preRequestPlugin, ok := plugin.(fwkrc.PreRequest); ok {
			c.preRequestPlugins = append(c.preRequestPlugins, preRequestPlugin)
		}
		if responseReceivedPlugin, ok := plugin.(fwkrc.ResponseHeaderProcessor); ok {
			c.responseReceivedPlugins = append(c.responseReceivedPlugins, responseReceivedPlugin)
		}
		if responseStreamingPlugin, ok := plugin.(fwkrc.ResponseBodyProcessor); ok {
			c.responseStreamingPlugins = append(c.responseStreamingPlugins, responseStreamingPlugin)
		}
		if dataProducerPlugin, ok := plugin.(fwkrc.DataProducer); ok {
			c.dataProducerPlugins = append(c.dataProducerPlugins, dataProducerPlugin)
		}
		if admissionPlugin, ok := plugin.(fwkrc.Admitter); ok {
			c.admissionPlugins = append(c.admissionPlugins, admissionPlugin)
		}
	}
}

// OrderPreAdmissionDataProducers filters and orders Pre-Admission DataProducer plugins based on compiled names.
func (c *Config) OrderPreAdmissionDataProducers(sortedPluginNames []string) {
	c.preAdmissionDataProducers = make([]fwkrc.DataProducer, 0, len(sortedPluginNames))
	nameToPlugin := make(map[string]fwkrc.DataProducer)
	for _, plugin := range c.dataProducerPlugins {
		nameToPlugin[plugin.TypedName().String()] = plugin
	}
	for _, name := range sortedPluginNames {
		if plugin, ok := nameToPlugin[name]; ok {
			c.preAdmissionDataProducers = append(c.preAdmissionDataProducers, plugin)
		}
	}
}

// OrderPostAdmissionDataProducers filters and orders Post-Admission DataProducer plugins based on compiled names.
func (c *Config) OrderPostAdmissionDataProducers(sortedPluginNames []string) {
	c.postAdmissionDataProducers = make([]fwkrc.DataProducer, 0, len(sortedPluginNames))
	nameToPlugin := make(map[string]fwkrc.DataProducer)
	for _, plugin := range c.dataProducerPlugins {
		nameToPlugin[plugin.TypedName().String()] = plugin
	}
	for _, name := range sortedPluginNames {
		if plugin, ok := nameToPlugin[name]; ok {
			c.postAdmissionDataProducers = append(c.postAdmissionDataProducers, plugin)
		}
	}
}

// OrderDataProducerPlugins reorders the DataProducer plugins in the Config based on the given sorted plugin names.
// This preserves maximum backward compatibility for legacy callers/tests.
func (c *Config) OrderDataProducerPlugins(sortedPluginNames []string) {
	c.preAdmissionDataProducers = []fwkrc.DataProducer{}
	c.postAdmissionDataProducers = make([]fwkrc.DataProducer, 0, len(sortedPluginNames))
	nameToPlugin := make(map[string]fwkrc.DataProducer)
	for _, plugin := range c.dataProducerPlugins {
		nameToPlugin[plugin.TypedName().String()] = plugin
	}
	for _, name := range sortedPluginNames {
		if plugin, ok := nameToPlugin[name]; ok {
			c.postAdmissionDataProducers = append(c.postAdmissionDataProducers, plugin)
		}
	}
}
