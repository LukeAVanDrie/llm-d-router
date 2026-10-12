package prefixindex_test

import (
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/common/prefixindex"
)

func ExampleChainSet() {
	set := prefixindex.NewChainSet(4, nil)

	// Store a 3-block root chain [100, 101, 102], then extend it with [103]
	// under parent 102.
	set.AddRootChain([]uint64{100, 101, 102})
	set.AddChainUnder(102, []uint64{103})

	query := []uint64{100, 101, 102, 103, 104}
	fmt.Println("match before eviction:", set.RunLength(query, nil))

	// If an interior block (102) is removed out of order, block 103 becomes
	// Dormant and cannot be served across the gap.
	set.Remove(102)
	fmt.Println("match with interior gap:", set.RunLength(query, nil))
	fmt.Println("dormant blocks:", set.DormantCount())

	// Restoring block 102 under parent 101 reconnects block 103 automatically.
	set.AddChainUnder(101, []uint64{102})
	fmt.Println("match after gap filled:", set.RunLength(query, nil))
	// Output:
	// match before eviction: 4
	// match with interior gap: 2
	// dormant blocks: 1
	// match after gap filled: 4
}

func ExampleChainSet_twoTier() {
	// Staged two-tier composition (primary tier + secondary offload tier):
	//   - primary and union (primary | secondary) are ChainSets queried via RunLength.
	//   - secondary is a plain membership map guarding union.Remove when a block
	//     is evicted from primary while still resident in secondary.
	var primary, union prefixindex.ChainSet
	secondary := make(map[uint64]struct{})

	prompt := []uint64{10, 11, 12, 13}
	primary.AddRootChain(prompt)
	union.AddRootChain(prompt)
	for _, k := range prompt {
		secondary[k] = struct{}{}
	}

	// Evict tail blocks [12, 13] from primary while they remain in secondary:
	// only remove from union when absent from both tiers.
	for _, k := range []uint64{13, 12} {
		primary.Remove(k)
		if _, ok := secondary[k]; !ok {
			union.Remove(k)
		}
	}

	// Pass dPrimary as a threshold hint to union.RunLength so the inclusive
	// continuation search starts directly at the primary frontier in O(1).
	dPrimary := primary.RunLength(prompt, nil)
	dAny := union.RunLength(prompt, []int{dPrimary})
	fmt.Printf("primary match: %d, secondary continuation: %d (total: %d)\n", dPrimary, dAny-dPrimary, dAny)
	// Output:
	// primary match: 2, secondary continuation: 2 (total: 4)
}

func ExampleIndex() {
	idx := prefixindex.NewIndex(prefixindex.Config{
		Endpoints:           4,
		PerEndpointCapacity: 1024,
	})

	prompt := []uint64{0xA0, 0xA1, 0xA2, 0xA3}
	idx.AddRootChain(0, prompt[:2]) // Endpoint 0 holds 2 blocks
	idx.AddRootChain(2, prompt[:4]) // Endpoint 2 holds 4 blocks

	matches := idx.Match(prompt, []int{0, 1, 2, 3})
	fmt.Println("matches per endpoint:", matches)
	// Output:
	// matches per endpoint: [2 0 4 0]
}
