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

// PROTOTYPE (fork only).

package kvblock_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func TestChainIndexConcurrent(t *testing.T) {
	ctx := context.Background()
	idx := kvblock.NewChainIndex(kvblock.ChainIndexConfig{})
	chain := make([]kvblock.BlockHash, 64)
	for i := range chain {
		chain[i] = kvblock.BlockHash(i + 1)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 3000; i++ {
				pod := fmt.Sprintf("10.0.0.%d:8000", rng.Intn(4))
				entries := []kvblock.PodEntry{{PodIdentifier: pod, DeviceTier: []string{"gpu", "cpu"}[rng.Intn(2)]}}
				a := rng.Intn(len(chain))
				b := a + 1 + rng.Intn(len(chain)-a)
				parent := kvblock.EmptyBlockHash
				if a > 0 {
					parent = chain[a-1]
				}
				var err error
				switch rng.Intn(5) {
				case 0, 1:
					err = idx.AddChain(ctx, parent, chain[a:b], chain[a:b], entries)
				case 2:
					err = idx.Evict(ctx, kvblock.EngineKey, chain[a:b], entries)
				case 3:
					err = idx.WalkChains(ctx, chain, nil, func(c *kvblock.PodChain) {
						if c.Run < 1 || c.Run > len(chain) {
							t.Errorf("run %d", c.Run)
						}
						if c.Uniform == nil && len(c.Entries(c.Run-1)) == 0 {
							t.Errorf("held key without entries")
						}
					})
				default:
					if rng.Intn(50) == 0 {
						err = idx.Clear(ctx, pod)
					} else {
						err = idx.Evict(ctx, kvblock.RequestKey, chain[a:b], entries)
					}
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
