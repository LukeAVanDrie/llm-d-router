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

package multimodal

import (
	k8stypes "k8s.io/apimachinery/pkg/types"

	attrmm "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/multimodal"
)

// cacheSnapshot returns a hash→pod-set view of the per-endpoint caches for assertions.
func (p *Producer) cacheSnapshot() map[string]map[string]struct{} {
	p.podMu.RLock()
	defer p.podMu.RUnlock()
	snapshot := map[string]map[string]struct{}{}
	for _, ps := range p.podToLRU {
		ps.mu.Lock()
		for _, hash := range ps.lru.Keys() {
			if snapshot[hash] == nil {
				snapshot[hash] = map[string]struct{}{}
			}
			snapshot[hash][ps.podStr] = struct{}{}
		}
		ps.mu.Unlock()
	}
	return snapshot
}

func (p *Producer) putCacheEntry(hash string, pods ...k8stypes.NamespacedName) {
	item := [1]attrmm.MatchItem{{Hash: hash, Size: 1}}
	for _, pod := range pods {
		p.addItemsToPod(pod, item[:])
	}
}
