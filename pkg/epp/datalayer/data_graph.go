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
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// ValidateAndOrderDataDependencies validates the data dependencies among the given plugins
// and returns a single flattened topologically sorted order of all active, kept plugin names.
func ValidateAndOrderDataDependencies(plugins []plugin.Plugin) ([]string, error) {
	preAdmission, postAdmission, err := CompilePipeline(plugins)
	if err != nil {
		return nil, err
	}
	return append(preAdmission, postAdmission...), nil
}

// CompilePipeline compiles the active plugins into two topologically sorted slices:
// Pre-Admission and Post-Admission, applying pruning and scope validations.
func CompilePipeline(plugins []plugin.Plugin) ([]string, []string, error) {
	coercedScopes := auditAndCoerceScopes(plugins)

	if err := validateStaticScopes(plugins, coercedScopes); err != nil {
		return nil, nil, err
	}

	pluginMap := make(map[string]plugin.Plugin)
	for _, p := range plugins {
		pluginMap[p.TypedName().String()] = p
	}

	producers, consumers, err := validateNoDuplicateProducers(pluginMap)
	if err != nil {
		return nil, nil, err
	}

	dag, err := buildDAG(producers, consumers, coercedScopes)
	if err != nil {
		return nil, nil, err
	}

	V_kept, filteredDag := pruneUnusedProducers(pluginMap, dag)

	preAdmission, postAdmission, err := slicePipelineByAdmissionGating(pluginMap, V_kept, filteredDag, coercedScopes)
	if err != nil {
		return nil, nil, err
	}

	dagPreAdmission := make(map[string][]string)
	for node := range preAdmission {
		var deps []string
		for _, dep := range filteredDag[node] {
			if preAdmission[dep] {
				deps = append(deps, dep)
			}
		}
		dagPreAdmission[node] = deps
	}
	sortedPreAdmission, err := topologicalSort(dagPreAdmission)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sort Pre-Admission slice: %w", err)
	}

	dagPostAdmission := make(map[string][]string)
	for node := range postAdmission {
		var deps []string
		for _, dep := range filteredDag[node] {
			if postAdmission[dep] {
				deps = append(deps, dep)
			}
		}
		dagPostAdmission[node] = deps
	}
	sortedPostAdmission, err := topologicalSort(dagPostAdmission)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sort Post-Admission slice: %w", err)
	}

	return sortedPreAdmission, sortedPostAdmission, nil
}

// validateNoDuplicateProducers checks that no single data key has multiple active producing plugins configured.
func validateNoDuplicateProducers(
	pluginMap map[string]plugin.Plugin,
) (
	producers map[string]plugin.ProducerPlugin,
	consumers map[string]plugin.ConsumerPlugin,
	err error,
) {
	keyToProducers := make(map[string][]string)
	producers = make(map[string]plugin.ProducerPlugin)
	consumers = make(map[string]plugin.ConsumerPlugin)
	for name, p := range pluginMap {
		if producer, ok := p.(plugin.ProducerPlugin); ok {
			producers[name] = producer
			if producer.Produces() != nil {
				for key := range producer.Produces() {
					keyStr := key.BaseString()
					keyToProducers[keyStr] = append(keyToProducers[keyStr], name)
				}
			}
		}
		if consumer, ok := p.(plugin.ConsumerPlugin); ok {
			consumers[name] = consumer
		}
	}

	var dupErrs []error
	for key, prods := range keyToProducers {
		if len(prods) > 1 {
			dupErrs = append(dupErrs, fmt.Errorf("key %q has duplicate producers: %v", key, prods))
		}
	}
	if len(dupErrs) > 0 {
		return nil, nil, fmt.Errorf("duplicate producers detected: %w", errors.Join(dupErrs...))
	}
	return producers, consumers, nil
}

// pruneUnusedProducers removes any producers that are neither eager nor consumed by any active components.
func pruneUnusedProducers(
	pluginMap map[string]plugin.Plugin,
	dag map[string][]string,
) (map[string]bool, map[string][]string) {
	V_kept := make(map[string]bool)
	// All active non-producers are kept by default.
	for name, p := range pluginMap {
		if _, ok := p.(plugin.ProducerPlugin); !ok {
			V_kept[name] = true
		}
	}

	// Identify roots for reachability.
	var roots []string
	for name, p := range pluginMap {
		if _, ok := p.(plugin.ProducerPlugin); !ok {
			if _, inDag := dag[name]; inDag {
				roots = append(roots, name)
			}
		} else if eagerP, ok := p.(plugin.EagerProducerPlugin); ok && eagerP.Eager() {
			roots = append(roots, name)
			V_kept[name] = true
		}
	}

	// Traverse DAG to find reachable producers.
	visited := make(map[string]bool)
	var traverse func(node string)
	traverse = func(node string) {
		if visited[node] {
			return
		}
		visited[node] = true
		V_kept[node] = true
		for _, dep := range dag[node] {
			traverse(dep)
		}
	}
	for _, root := range roots {
		traverse(root)
	}

	// Filter DAG to only keep V_kept nodes.
	filteredDag := make(map[string][]string)
	for node, deps := range dag {
		if V_kept[node] {
			var filteredDeps []string
			for _, dep := range deps {
				if V_kept[dep] {
					filteredDeps = append(filteredDeps, dep)
				}
			}
			filteredDag[node] = filteredDeps
		}
	}
	return V_kept, filteredDag
}

// slicePipelineByAdmissionGating partitions kept plugins into Pre-Admission and Post-Admission sets.
func slicePipelineByAdmissionGating(
	pluginMap map[string]plugin.Plugin,
	V_kept map[string]bool,
	filteredDag map[string][]string,
	coercedScopes map[string]plugin.DataScope,
) (map[string]bool, map[string]bool, error) {
	preAdmissionRoots := make(map[string]bool)
	for name := range V_kept {
		p := pluginMap[name]
		isRoot := false
		if _, ok := p.(fwkrc.PreAdmitter); ok {
			isRoot = true
		} else if _, ok := p.(fwkfc.FairnessPolicy); ok {
			isRoot = true
		} else if _, ok := p.(fwkfc.OrderingPolicy); ok {
			isRoot = true
		} else if eagerP, ok := p.(plugin.EagerProducerPlugin); ok && eagerP.Eager() {
			if eagerP.Produces() != nil {
				for key := range eagerP.Produces() {
					if resolveScope(key, coercedScopes) == plugin.RequestScope {
						isRoot = true
						break
					}
				}
			}
		}
		if isRoot {
			preAdmissionRoots[name] = true
		}
	}

	// Transitive closure for preAdmission.
	preAdmission := make(map[string]bool)
	var traversePreAdmission func(node string)
	traversePreAdmission = func(node string) {
		if preAdmission[node] {
			return
		}
		preAdmission[node] = true
		for _, dep := range filteredDag[node] {
			traversePreAdmission(dep)
		}
	}
	for root := range preAdmissionRoots {
		traversePreAdmission(root)
	}

	// postAdmission = V_kept \ preAdmission
	postAdmission := make(map[string]bool)
	for name := range V_kept {
		if !preAdmission[name] {
			postAdmission[name] = true
		}
	}

	return preAdmission, postAdmission, nil
}

// validateStaticScopes validates that Pre-Admission plugins do not consume or produce EndpointScope or
// UnspecifiedScope keys at startup statically.
func validateStaticScopes(plugins []plugin.Plugin, coercedScopes map[string]plugin.DataScope) error {
	for _, p := range plugins {
		layer := pluginToLayerExecutionOrder(p, coercedScopes)
		if layer == PreAdmissionLayer || layer == FlowControlLayer {
			if consumer, ok := p.(plugin.ConsumerPlugin); ok {
				for key := range consumer.Consumes() {
					if resolveScope(key, coercedScopes) == plugin.EndpointScope {
						return fmt.Errorf("scope mismatch: plugin %q in Pre-Admission consumes key %q with invalid scope %q",
							p.TypedName().String(), key.String(), resolveScope(key, coercedScopes))
					}
				}
			}
			if optConsumer, ok := p.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
				for _, key := range optConsumer.OptionalConsumes() {
					if resolveScope(key, coercedScopes) == plugin.EndpointScope {
						return fmt.Errorf("scope mismatch: plugin %q in Pre-Admission consumes key %q with invalid scope %q",
							p.TypedName().String(), key.String(), resolveScope(key, coercedScopes))
					}
				}
			}
			if producer, ok := p.(plugin.ProducerPlugin); ok {
				for key := range producer.Produces() {
					if resolveScope(key, coercedScopes) == plugin.EndpointScope {
						return fmt.Errorf("scope mismatch: plugin %q in Pre-Admission produces key %q with invalid scope %q",
							p.TypedName().String(), key.String(), resolveScope(key, coercedScopes))
					}
				}
			}
		}
	}
	return nil
}

// CreateMissingDataProducers inspects the set of already-configured plugins,
// finds data keys that are consumed but not yet produced, and auto-instantiates
// the default DataProducer plugin for each such key using nil parameters.
func CreateMissingDataProducers(ctx context.Context, defaultProducerRegistry map[string]string, factoryRegistry map[string]plugin.FactoryFunc, handle plugin.Handle) error {
	logger := log.FromContext(ctx)

	// Audit and coerce scopes for legacy unspecified keys
	coercedScopes := auditAndCoerceScopes(handle.GetAllPlugins())

	// Collect all keys already produced by existing plugins.
	producedKeys := make(map[string]bool)
	for _, p := range handle.GetAllPlugins() {
		if producer, ok := p.(plugin.ProducerPlugin); ok {
			if producer.Produces() != nil {
				for key := range producer.Produces() {
					normKey := normalizeKey(key, coercedScopes)
					producedKeys[normKey.String()] = true
				}
			}
		}
	}

	// Helper to check if a key is optional for a given plugin
	isKeyOptional := func(p plugin.Plugin, key plugin.DataKey) bool {
		if optConsumer, ok := p.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
			for _, optKey := range optConsumer.OptionalConsumes() {
				normOptKey := normalizeKey(optKey, coercedScopes)
				normKey := normalizeKey(key, coercedScopes)
				if normOptKey.String() == normKey.String() {
					return true
				}
			}
		}
		return false
	}

	for _, p := range handle.GetAllPlugins() {
		consumer, ok := p.(plugin.ConsumerPlugin)
		if !ok {
			continue
		}

		// Collect all consumed keys (both required and optional)
		var consumedKeys []plugin.DataKey
		if consumer.Consumes() != nil {
			for key := range consumer.Consumes() {
				consumedKeys = append(consumedKeys, key)
			}
		}
		if optConsumer, ok := p.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
			for _, key := range optConsumer.OptionalConsumes() {
				// Avoid duplicates if a key is in both (though they shouldn't be)
				exists := false
				for _, k := range consumedKeys {
					normK := normalizeKey(k, coercedScopes)
					normKey := normalizeKey(key, coercedScopes)
					if normK.String() == normKey.String() {
						exists = true
						break
					}
				}
				if !exists {
					consumedKeys = append(consumedKeys, key)
				}
			}
		}

		for _, key := range consumedKeys {
			normKey := normalizeKey(key, coercedScopes)
			keyStr := normKey.String()
			if producedKeys[keyStr] {
				continue
			}

			defaultProducerNameOrType, ok := defaultProducerRegistry[keyStr]
			if !ok {
				if isKeyOptional(p, key) {
					logger.Info("WARNING: no default producer found for optional missing data key, skipping",
						"dataKey", keyStr,
						"consumer", consumer.TypedName().String())
					continue
				}
				return fmt.Errorf("no default producer found for missing data key: %v, which is consumed by: %v", keyStr, consumer.TypedName().String())
			}

			if handle.Plugin(defaultProducerNameOrType) != nil {
				if prod, ok := handle.Plugin(defaultProducerNameOrType).(plugin.ProducerPlugin); ok && prod.Produces() != nil {
					for k := range prod.Produces() {
						normK := normalizeKey(k, coercedScopes)
						producedKeys[normK.String()] = true
					}
				}
				continue
			}

			factory, ok := factoryRegistry[defaultProducerNameOrType]
			if !ok {
				return fmt.Errorf("factory not found for default producer: %v, this is required by datakey: %v, which is consumed by: %v", defaultProducerNameOrType, keyStr, consumer.TypedName().String())
			}

			plg, err := factory(defaultProducerNameOrType, nil, handle)
			if err != nil {
				return fmt.Errorf("failed to instantiate data producer %q: %w, this is required by datakey: %v, which is consumed by: %v", defaultProducerNameOrType, err, keyStr, consumer.TypedName().String())
			}

			producer, ok := plg.(plugin.ProducerPlugin)
			if !ok {
				return fmt.Errorf("auto-created default entry %q is not a ProducerPlugin, this is required by datakey: %v, which is consumed by: %v", defaultProducerNameOrType, keyStr, consumer.TypedName().String())
			}

			handle.AddPlugin(plg.TypedName().Name, plg)
			logger.Info("auto-created default producer",
				"producer", plg.TypedName().String(),
				"dataKey", keyStr,
				"consumer", consumer.TypedName().String())

			if producer.Produces() != nil {
				for k := range producer.Produces() {
					normK := normalizeKey(k, coercedScopes)
					producedKeys[normK.String()] = true
				}
			}
		}
	}

	return nil
}

// Define constants for layer execution order. Lower value means earlier execution.
const (
	PreAdmissionLayer   = 0
	FlowControlLayer    = 1
	RequestControlLayer = 2
	SchedulingLayer     = 3
	DefaultLayer        = -1
)

func pluginToLayerExecutionOrder(p plugin.Plugin, coercedScopes map[string]plugin.DataScope) int {
	// PreAdmitter
	if _, ok := p.(fwkrc.PreAdmitter); ok {
		return PreAdmissionLayer
	}

	// Flow control plugins
	if _, ok := p.(fwkfc.FairnessPolicy); ok {
		return FlowControlLayer
	}
	if _, ok := p.(fwkfc.OrderingPolicy); ok {
		return FlowControlLayer
	}

	// Data Producers
	if producer, ok := p.(plugin.ProducerPlugin); ok {
		isRequestScoped := true
		hasProduces := false
		if producer.Produces() != nil && len(producer.Produces()) > 0 {
			hasProduces = true
			for key := range producer.Produces() {
				if resolveScope(key, coercedScopes) != plugin.RequestScope {
					isRequestScoped = false
					break
				}
			}
		}
		if hasProduces && isRequestScoped {
			return PreAdmissionLayer
		}
		return RequestControlLayer
	}

	// Other Request control plugins
	if _, ok := p.(fwkrc.Admitter); ok {
		return RequestControlLayer
	}
	if _, ok := p.(fwkrc.PreRequest); ok {
		return RequestControlLayer
	}
	if _, ok := p.(fwkrc.ResponseHeaderProcessor); ok {
		return RequestControlLayer
	}

	// Scheduling plugins
	if _, ok := p.(fwksched.ProfileHandler); ok {
		return SchedulingLayer
	}
	if _, ok := p.(fwksched.Filter); ok {
		return SchedulingLayer
	}
	if _, ok := p.(fwksched.Scorer); ok {
		return SchedulingLayer
	}
	if _, ok := p.(fwksched.Picker); ok {
		return SchedulingLayer
	}

	return DefaultLayer
}

// buildDAG builds a dependency graph among data preparation plugins based on their
// produced and consumed data keys.
func buildDAG(producers map[string]plugin.ProducerPlugin, consumers map[string]plugin.ConsumerPlugin, coercedScopes map[string]plugin.DataScope) (map[string][]string, error) {
	// 1. Linear O(N) Scope Validation
	type producerInfo struct {
		name  string
		scope plugin.DataScope
	}
	producedBaseKeys := make(map[string]*producerInfo)

	for pName, producer := range producers {
		if producer.Produces() != nil {
			for key := range producer.Produces() {
				normKey := normalizeKey(key, coercedScopes)
				producedBaseKeys[normKey.BaseString()] = &producerInfo{
					name:  pName,
					scope: normKey.Scope(),
				}
			}
		}
	}

	for cName, consumer := range consumers {
		if consumer.Consumes() != nil {
			for key := range consumer.Consumes() {
				normKey := normalizeKey(key, coercedScopes)
				if pInfo, ok := producedBaseKeys[normKey.BaseString()]; ok {
					if pInfo.scope != normKey.Scope() {
						return nil, fmt.Errorf("scope mismatch detected for key %q: producer %s produces scope %q, but consumer %s consumes scope %q",
							normKey.BaseString(), pInfo.name, pInfo.scope, cName, normKey.Scope())
					}
				}
			}
		}
		if optConsumer, ok := consumer.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
			for _, key := range optConsumer.OptionalConsumes() {
				normKey := normalizeKey(key, coercedScopes)
				if pInfo, ok := producedBaseKeys[normKey.BaseString()]; ok {
					if pInfo.scope != normKey.Scope() {
						return nil, fmt.Errorf("scope mismatch detected for optional key %q: producer %s produces scope %q, but consumer %s consumes scope %q",
							normKey.BaseString(), pInfo.name, pInfo.scope, cName, normKey.Scope())
					}
				}
			}
		}
	}

	// Pre-normalize consumes and produces for O(1) lookup
	normalizedConsumersConsumes := make(map[string]map[plugin.DataKey]any)
	for cName, consumer := range consumers {
		if consumer.Consumes() != nil {
			normConsumes := make(map[plugin.DataKey]any)
			for k, v := range consumer.Consumes() {
				normConsumes[normalizeKey(k, coercedScopes)] = v
			}
			normalizedConsumersConsumes[cName] = normConsumes
		}
	}

	normalizedProducersProduces := make(map[string]map[plugin.DataKey]any)
	for pName, producer := range producers {
		if producer.Produces() != nil {
			normProduces := make(map[plugin.DataKey]any)
			for k, v := range producer.Produces() {
				normProduces[normalizeKey(k, coercedScopes)] = v
			}
			normalizedProducersProduces[pName] = normProduces
		}
	}

	dag := make(map[string][]string)
	for _, producer := range producers {
		dag[producer.TypedName().String()] = []string{}
	}
	for _, consumer := range consumers {
		dag[consumer.TypedName().String()] = []string{}
	}

	for pName, producer := range producers {
		for cName, consumer := range consumers {
			if pName == cName {
				continue
			}
			hasDependency := false
			if producer.Produces() != nil {
				// Required consumes check
				normConsumes := normalizedConsumersConsumes[cName]
				if normConsumes != nil {
					for producedKey, producedData := range producer.Produces() {
						normProducedKey := normalizeKey(producedKey, coercedScopes)
						if consumedData, ok := normConsumes[normProducedKey]; ok {
							// Check types are same.
							if reflect.TypeOf(producedData) != reflect.TypeOf(consumedData) {
								return nil, fmt.Errorf("data type mismatch between produced and consumed data for key: %s", producedKey.String())
							}
							if pluginToLayerExecutionOrder(producer, coercedScopes) > pluginToLayerExecutionOrder(consumer, coercedScopes) {
								return nil, fmt.Errorf("invalid plugin layer execution order: producer %s needs to be executed before consumer %s", pName, cName)
							}
							hasDependency = true
						}
					}
				}
				// Optional consumes check
				if optConsumer, ok := consumer.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
					normProduces := normalizedProducersProduces[pName]
					if normProduces != nil {
						for _, producedKey := range optConsumer.OptionalConsumes() {
							normOptKey := normalizeKey(producedKey, coercedScopes)
							if _, ok := normProduces[normOptKey]; ok {
								if pluginToLayerExecutionOrder(producer, coercedScopes) > pluginToLayerExecutionOrder(consumer, coercedScopes) {
									return nil, fmt.Errorf("invalid plugin layer execution order: producer %s needs to be executed before consumer %s (via optional dependency)", pName, cName)
								}
								hasDependency = true
							}
						}
					}
				}
			}
			if hasDependency {
				dag[cName] = append(dag[cName], pName)
			}
		}
	}
	return dag, nil
}

// TopologicalSort performs Kahn's Algorithm on a DAG.
// It returns the sorted order or an error if a cycle is detected.
func topologicalSort(graph map[string][]string) ([]string, error) {
	inDegree := make(map[string]int)

	for u, neighbors := range graph {
		if _, ok := inDegree[u]; !ok {
			inDegree[u] = 0
		}
		for _, v := range neighbors {
			inDegree[v]++
		}
	}

	var queue []string
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	var result []string

	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]

		result = append(result, u)

		if neighbors, ok := graph[u]; ok {
			for _, v := range neighbors {
				inDegree[v]--
				if inDegree[v] == 0 {
					queue = append(queue, v)
				}
			}
		}
	}

	if len(result) != len(inDegree) {
		cycleStr := findAndFormatCycle(graph, inDegree)
		return nil, fmt.Errorf("cycle detected: %s", cycleStr)
	}

	slices.Reverse(result)
	return result, nil
}

func findAndFormatCycle(graph map[string][]string, inDegree map[string]int) string {
	cycleNodes := make(map[string]bool)
	for node, deg := range inDegree {
		if deg > 0 {
			cycleNodes[node] = true
		}
	}

	visited := make(map[string]int)
	var path []string
	var cyclePath []string

	var dfs func(node string) bool
	dfs = func(node string) bool {
		visited[node] = 1
		path = append(path, node)

		for _, neighbor := range graph[node] {
			if !cycleNodes[neighbor] {
				continue
			}
			if visited[neighbor] == 1 {
				startIdx := -1
				for i, p := range path {
					if p == neighbor {
						startIdx = i
						break
					}
				}
				if startIdx != -1 {
					cyclePath = append([]string{}, path[startIdx:]...)
					cyclePath = append(cyclePath, neighbor)
					return true
				}
			} else if visited[neighbor] == 0 {
				if dfs(neighbor) {
					return true
				}
			}
		}

		visited[node] = 2
		path = path[:len(path)-1]
		return false
	}

	for node := range cycleNodes {
		if visited[node] == 0 {
			if dfs(node) {
				break
			}
		}
	}

	if len(cyclePath) > 0 {
		return strings.Join(cyclePath, " -> ")
	}
	return "unknown cycle"
}

// normalizeKey resolves legacy scopes to coerced scopes if unspecified.
func normalizeKey(key plugin.DataKey, coercedScopes map[string]plugin.DataScope) plugin.DataKey {
	if key.Scope() == plugin.UnspecifiedScope {
		if coerced, ok := coercedScopes[key.String()]; ok {
			return key.WithScope(coerced)
		}
	}
	return key
}

// resolveScope resolves a key's scope, falling back to its coerced scope if it is UnspecifiedScope.
func resolveScope(key plugin.DataKey, coercedScopes map[string]plugin.DataScope) plugin.DataScope {
	return normalizeKey(key, coercedScopes).Scope()
}

// auditAndCoerceScopes audits all active plugins and determines dynamic scopes for UnspecifiedScope keys.
func auditAndCoerceScopes(plugins []plugin.Plugin) map[string]plugin.DataScope {
	logger := log.Log.WithName("epp-graph-compiler")
	coercedScopes := make(map[string]plugin.DataScope)
	unspecifiedKeysLogged := make(map[string]bool)

	for _, p := range plugins {
		var unspecifiedKeys []plugin.DataKey

		if consumer, ok := p.(plugin.ConsumerPlugin); ok && consumer.Consumes() != nil {
			for key := range consumer.Consumes() {
				if key.Scope() == plugin.UnspecifiedScope {
					unspecifiedKeys = append(unspecifiedKeys, key)
				}
			}
		}
		if optConsumer, ok := p.(plugin.OptionalConsumerPlugin); ok && optConsumer.OptionalConsumes() != nil {
			for _, key := range optConsumer.OptionalConsumes() {
				if key.Scope() == plugin.UnspecifiedScope {
					unspecifiedKeys = append(unspecifiedKeys, key)
				}
			}
		}
		if producer, ok := p.(plugin.ProducerPlugin); ok && producer.Produces() != nil {
			for key := range producer.Produces() {
				if key.Scope() == plugin.UnspecifiedScope {
					unspecifiedKeys = append(unspecifiedKeys, key)
				}
			}
		}

		for _, key := range unspecifiedKeys {
			keyStr := key.String()
			if unspecifiedKeysLogged[keyStr] {
				continue
			}
			unspecifiedKeysLogged[keyStr] = true

			coercedScopes[keyStr] = plugin.EndpointScope

			logger.Info(
				"WARNING: Legacy/un-upgraded plugin registered using UnspecifiedScope data key. Coercing to EndpointScope. Please upgrade the plugin to declare an explicit scope.",
				"plugin", p.TypedName().String(),
				"dataKey", keyStr,
				"coercedScope", plugin.EndpointScope,
			)
		}
	}

	return coercedScopes
}
