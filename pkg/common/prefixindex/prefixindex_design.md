# `prefixindex` package design and design-space extensions

This document describes the design of the `prefixindex` Go package ([`set.go`](set.go) and [`index.go`](index.go)) for code reviewers and maintainers, followed by an appendix analyzing three extensions of the design space. For the problem statement, domain invariants, and lower bounds, see [prefix_residency_first_principles.md](prefix_residency_first_principles.md).

---

## 1. Scope and API surface

`prefixindex` is a two-file Go package with no external dependencies that backs both the approximate (router-predicted whole-chain) and precise (engine KV-event) routing paths:

- [`ChainSet`](set.go): single-endpoint prefix residency set. Its zero value is an empty set with unlimited capacity ready for use.
  - Mutation methods: `AddRootChain(keys)`, `AddChainUnder(parentKey, keys)`, `Remove(key)`, `Clear()`, `EvictOldestLeaf()`.
  - Read methods: `RunLength(keys, hints)`, `Contains(key)`, `Len()`, `DormantCount()`, `Evictions()`, `Rebuilds()`.
- [`Index`](index.go): concurrent multi-endpoint router index coordinating per-endpoint `ChainSet` instances with a 64-shard root-holder bitmask filter on $k_0$ and cross-endpoint threshold sharing on reads.
  - Mutation methods: `AddRootChain(endpoint, keys)`, `AddChainUnder(endpoint, parentKey, keys)`, `Remove(endpoint, key)`, `ClearEndpoint(endpoint)`.
  - Read methods: `Match(keys, candidates)`, `MatchInto(keys, candidates, out)`, `Stats()`.

---

## 2. Memory layout and resident-only state model

Only resident blocks (`Visible` or `Dormant`) are stored in a `ChainSet` (`len(s.blocks) == s.Len()`). Absent blocks consume no memory.

```go
type block struct {
	// Authoritative facts (established on insertion; deleted on Remove).
	key       uint64
	parentKey uint64 // 0 when isRoot is true; stored by hash so swap-delete never invalidates parent links
	stamp     uint64

	// Derived projections (deterministic function of the resident block set).
	heapIdx  int32  // index in leaves min-heap, or -1 when children > 0
	children uint32 // resident child count
	isRoot   bool   // true iff depth-0 root block
	vis      uint8  // visDormant (1) or visVisible (2)
}
```

Key properties of `block` (`40 bytes`, verified in `TestZeroPointersInBlockAndEndpointSlotAlignment`):
1. **Zero pointers (`noscan`).** `s.blocks` (`[]block`), `s.byKey` (`map[uint64]uint32`), and `s.leaves` (`[]uint32`) contain no pointers in their elements or buckets, so Go's runtime allocates their backing storage in `noscan` spans. Across $524{,}288$ resident blocks on 128 endpoints, `BenchmarkRetainedGCHeap` measures `40.84 MB` total `HeapInuse` (`81.7 B/block`) across `3,204` heap objects, leaving under `1 MB` of slice/map headers for the GC mark phase to scan.
2. **Inline parent-key verification.** Storing `parentKey uint64` by value on `block` lets `probeAt` verify `b.parentKey == keys[pos-1]` on the same cache line during binary search. A wrong-parent link or hash collision becomes a miss rather than an overcount.
3. **32-bit child counter (`children uint32`).** Using `uint32` rather than `uint16` avoids child-count overflow or saturation when more than $65{,}535$ sessions branch from a shared system-prompt root on a large endpoint. Two bytes of struct padding remain alongside `isRoot` and `vis`, which can host a `uint16` copy counter at zero memory cost if physical copy counting is moved into `ChainSet`.
4. **Immutable resident lineage and inductive forest guard (`createsCycle`).** Once a key is resident in `s.blocks`, its `(parentKey, isRoot)` lineage cannot change until it is evicted; subsequent `AddRootChain` or `AddChainUnder` touches on an already-resident key only refresh `stamp`. Before inserting a non-root key `k` under `parentKey`, `createsCycle` checks `parentKey == k` in $O(1)$ and, if resident blocks are already waiting on `k` (`s.waitingOn[k] > 0`), walks `parentKey` upward in $O(\text{depth})$ to reject cycles. Because the resident graph is inductively a finite forest, every non-empty `ChainSet` has at least one leaf (`len(s.blocks) > 0 => len(s.leaves) > 0`).

---

## 3. Closure maintenance: `rebuild()` vs. $O(\text{subtree})$ cascade

### 3.1 How `set.go` updates visibility

- **Leaf insertions and leaf removals ($O(1)$ / $O(\log N_p)$):** A newly inserted leaf inherits `vis = s.blocks[pIdx].vis` from its resident parent in $O(1)$ (or starts as `visDormant` if its parent is absent) and sifts into `s.leaves` in $O(\log N_p)$. Removing a leaf (`removed.children == 0`) updates its parent's `children` count and propagates `p.stamp = max(p.stamp, removed.stamp)` in $O(\log N_p)$.
- **Dormant interior insertions and removals ($O(1)$):** Because all descendants of a `Dormant` block are already `Dormant`, removing a `Dormant` interior block (`removed.children > 0 && removed.vis == visDormant`) does not change the visibility of any remaining block; `Remove` records `s.waitingOn[removed.key] = int32(removed.children)` in $O(1)$ without rebuilding. Likewise, inserting a `Dormant` block `k` that had `waitCnt := s.waitingOn[k]` children waiting on it sets `children: uint32(waitCnt)` in $O(1)$ without rebuilding.
- **Visible interior removals and gap-filling insertions ($O(N_p)$ via `rebuild()`):** When a `Visible` interior block is removed (`removed.children > 0 && removed.vis == visVisible`), or when a newly inserted `Visible` block wakes waiting children (`waitCnt > 0 && vis == visVisible`), `rebuild()` recomputes `children`, `vis`, `dormantCount`, `waitingOn`, and `leaves` in a linear pass over `s.blocks`.

### 3.2 Measured cost and worst-case trade-off

`rebuild()` trades worst-case time complexity on visible interior events for structural simplicity (storing only `parentKey` without maintaining parent-to-child links across swap-delete). Because `rebuild()` performs two passes of map lookups into `s.byKey` across all $N_p$ resident blocks on the endpoint under that endpoint's write lock, its per-call latency scales linearly with $N_p$:

| Resident blocks on endpoint ($N_p$) | `BenchmarkChainSetRebuildByEndpointSize` latency | Allocations |
| :--- | :--- | :--- |
| $4{,}096$ blocks | `214 µs/op` | `0 B/op`, `0 allocs/op` |
| $16{,}384$ blocks | `1.16 ms/op` | `0 B/op`, `0 allocs/op` |
| $65{,}536$ blocks | `8.24 ms/op` | `0 B/op`, `0 allocs/op` |

**Implications for callers:**
- On leaf-first engines with in-order event delivery (and upstream multi-copy deduplication), visible interior removals do not occur (`0` rebuilds on the Mooncake leaf-first replay).
- When an entire length-$L$ chain is evicted head-first, removing root $k_0$ triggers 1 rebuild ($O(N_p)$), and the remaining $L-1$ removals of now-dormant blocks $k_1 \dots k_{L-1}$ cost $O(1)$ each (`TestHeadFirstEvictionAndImmutableResidentLineage`).
- **Worst-case regime:** If an endpoint holds $16\text{K}\text{–}100\text{K}+$ blocks and experiences frequent visible interior removals or out-of-order gap fills (for example from un-deduplicated vLLM recompute removals, `unpin_blocks` ordering, lost store events, or interior host reclaims propagated to a union `ChainSet`), each such event stalls readers on that endpoint for $1\text{–}8+\text{ ms}$. If integration replay on production traces shows non-negligible `Rebuilds() * N_p` cost, `ChainSet` should replace `rebuild()` with an $O(\text{affected subtree})$ incremental cascade using explicit parent-to-child links.

---

## 4. Trust-boundary guards and `pool.go` integration

| Mechanism | Failure prevented | Layer | Status |
| :--- | :--- | :--- | :--- |
| `Visible` / `Dormant` state separation | Out-of-order interior eviction overcounts or breaks binary search | `ChainSet` ([`set.go`](set.go)) | Implemented |
| `children uint32` on `block` | $>65{,}535$ branches under a shared root overflow `uint16` and break leaf detection | `ChainSet` ([`set.go`](set.go)) | Implemented |
| Separate `AddRootChain` and `AddChainUnder` | Parentless tier updates (`pool.go:533`) promote interior blocks to fake roots and overcount | `ChainSet` & `Index` | Implemented |
| Inductive forest guard (`createsCycle`) & immutable resident lineage | Cyclic parent links (`A <-> B`) create un-evictable non-leaf blocks | `ChainSet` ([`set.go`](set.go)) | Implemented |
| Dormant-first leaf-LRU eviction (`visDormant` before `visVisible`) | Capacity eviction drops servable visible leaves ahead of unreachable dormant leaves | `ChainSet` ([`lessLeaf`](set.go)) | Implemented |
| Set semantics in `ChainSet` + single-source copy dedup in `pool.go` | Double-refcounting with `pool.go` `eventDedupFilter` or `"full"` mode inflates copy counts | `ChainSet` + `pool.go` | Set semantics implemented in `ChainSet` |
| Engine-to-request key translation (`1:1`, `many:1`, `1:many`) | Mid-block hash misalignment | Event ingestion (`pool.go`) | Caller layer |
| Capacity bound + `ClearEndpoint` + event-health expiry | Lost `Remove` leaves stale keys indefinitely | `ChainSet` + `Index` + caller | Capacity & `ClearEndpoint` in `prefixindex`; health timer in caller |

**GC trade-off with `pool.go`'s `eventDedupFilter`:**
Keeping set semantics in `ChainSet` avoids double-refcounting against `pool.go`'s `eventDedupFilter`. However, `pool.go`'s current `eventDedupFilter` stores pointer-rich keys (strings/slices), which retain an estimated `100–130 MB` of GC-scannable heap on the Mooncake replay. To realize the full GC reduction end-to-end in `llm-d`, either `eventDedupFilter` should be keyed by flat `uint64` hashes (`noscan`) or physical copy counting (`uint16`) should move into `block`'s 2-byte struct padding.

---

## 5. Multi-endpoint `Index` and `>256` endpoint overflow path

[`Index`](index.go) partitions state across per-endpoint slots (`endpointSlot`, padded to 64 bytes to prevent false sharing between adjacent `sync.RWMutex` headers) and a 64-shard root-holder table (`rootShards[64]rootShard`).

- **Ordinals in `[0, MaxEndpoints)` (`0 .. 255`):** Each `rootShard` maps a depth-0 root key $k_0$ to a 256-bit `endpointBitset` (`[4]uint64`, zero pointers). When $k_0$ is added or removed on endpoint `ep < 256`, `ChainSet`'s `onRoot` callback updates bit `ep` in `rootShards`. On `Match` / `MatchInto`, candidates in `0 .. 255` whose bit is 0 in `holders` are skipped without taking their per-endpoint `RLock`.
- **Overflow path for `Endpoints > 256`:** `NewIndex` does not clamp `Config.Endpoints` at `256`. Endpoints with ordinal `ep >= 256` omit the `onRoot` bitmask callback and bypass the bitmask check in `MatchInto`, falling back to direct per-endpoint probing (`es.set.RunLength(keys, hints)`), which rejects non-holders on the first map lookup (`probeAt(keys, 0)`).
- **Ordinal lifecycle under endpoint churn:** Before reassigning a departed endpoint's integer ordinal to a new endpoint, the caller must call `idx.ClearEndpoint(ep)`, which clears the endpoint's `ChainSet` and synchronously clears bit `ep` from every root key it held.

---

## 6. Staged two-tier composition

For engines with a primary tier (GPU HBM) and a secondary offload tier (CPU DRAM), staged continuation scoring ($d_p^{\mathrm{primary}} \in [0, L]$ followed by $d_p^{\mathrm{union}} \in [d_p^{\mathrm{primary}}, L]$) is composed using two `ChainSet`s and a plain membership map (see [`ExampleChainSet_twoTier`](example_test.go)):

- `primary`: a `ChainSet` tracking blocks resident in the primary tier.
- `union`: a `ChainSet` tracking blocks resident in the inclusive union ($\text{primary} \cup \text{secondary}$).
- `secondary`: a plain `map[uint64]struct{}` tracking block keys resident in the secondary tier.

**Why `secondary` is a plain membership map rather than a `ChainSet`:**
1. Queries only run `RunLength` from block 0 on `primary` and `union` (passing `dPrimary := primary.RunLength(keys, nil)` as a hint to `union.RunLength(keys, []int{dPrimary})`). `secondary` is only queried for point membership (`_, ok := secondary[k]`) when deciding whether a primary-tier eviction also removes `k` from `union`.
2. Using a plain map for `secondary` avoids running `ChainSet` closure maintenance on a tier that evicts heads-first.
3. Parentless tier-offload events (`pool.go:533`, where a block already in `union` is copied to CPU without parent keys) simply insert `secondary[k] = struct{}{}` without needing parent links.

**Limitation when blocks enter the secondary tier first:**
If a block appears on the secondary tier without having passed through the primary tier with a parent link (for example via SGLang host inserts, external-storage prefetch, or offload-connector loads that omit parent keys), it cannot be linked into `union`. In that case `union` omits the block and undercounts (the safe failure direction).

---

## 7. Benchmarks and verification suites

Measured on Go 1.26.6 (`linux/amd64`, AMD EPYC 7B12) in `set_test.go`:

| Benchmark / Test | Result | Notes |
| :--- | :--- | :--- |
| `BenchmarkChainSetRunLengthByPrefixLen/L4_Hit3` | `56.9 ns/op`, `0 allocs/op` | Single-endpoint binary search ($L=4$, hit depth 3) |
| `BenchmarkChainSetRunLengthByPrefixLen/L16_Hit12` | `78.4 ns/op`, `0 allocs/op` | Single-endpoint binary search ($L=16$, hit depth 12) |
| `BenchmarkChainSetRunLengthByPrefixLen/L64_Hit48` | `117.8 ns/op`, `0 allocs/op` | Single-endpoint binary search ($L=64$, hit depth 48) |
| `BenchmarkChainSetRunLengthByPrefixLen/L320_Hit256` | `150.9 ns/op`, `0 allocs/op` | Single-endpoint binary search ($L=320$, hit depth 256) |
| `BenchmarkIndexQuery128Endpoints/Full_RootFilter_ThresholdSharing` | `2.46 µs/op`, `0 allocs/op` | 128 endpoints, 20 holders of $k_0$, $L=320$ |
| `BenchmarkIndexQuery128Endpoints/NoThresholdSharing` | `3.37 µs/op`, `0 allocs/op` | Root filter enabled, threshold hints disabled |
| `BenchmarkIndexQuery128Endpoints/Naive_NoRootFilter_NoSharing` | `4.30 µs/op`, `0 allocs/op` | Both root filter and threshold hints disabled |
| `BenchmarkIndexConcurrentReadWrite` | `654.0 ns/op`, `0 allocs/op` | 8-worker `b.RunParallel` (87.5% `MatchInto` / 12.5% `AddRootChain`) |
| `BenchmarkRetainedGCHeap` | `40.84 MB` (`81.7 B/block`) | $524{,}288$ resident blocks across 128 endpoints (`3,204` heap objects) |

---

## Appendix A: Design-space extensions

### A.1 Flat key-major masks with bitset bisection (read view over endpoint-major sets)

Combining chained-key random access with ancestor closure shows that key-major lookup does not require a pointer-based radix tree or a linear walk:
1. Because $k_i$ commits to blocks $0\dots i$, a flat map `k_i -> HolderMask` suffices (analogous to NVIDIA Dynamo's `PositionalIndexer`).
2. Under ancestor closure, holder bitmasks along any query path are nested: $\mathrm{Mask}(k_0) \supseteq \mathrm{Mask}(k_1) \supseteq \dots \supseteq \mathrm{Mask}(k_{L-1})$. Recursively bisecting any interval $[l, r]$ where $\mathrm{Mask}(k_l) \neq \mathrm{Mask}(k_r)$ and skipping intervals where $\mathrm{Mask}(k_l) == \mathrm{Mask}(k_r)$ resolves all endpoints' match boundaries in $\mathcal{O}(D \log_2(L/D))$ mask probes across all endpoints at once.
3. **Why it is best used as a read view rather than primary storage:** Every store and removal still mutates shared per-key state, and fixed-width bitmasks reintroduce endpoint-width limits and ordinal-reuse bookkeeping. It fits best as an optional read view over popular prefix keys, falling back to endpoint-major probes for endpoints whose generation is not current.

### A.2 Superblock cursor transposition (and why branching breaks scalar cursors)

Packing 128 endpoints' single-writer cursors (`cursor[p]`, the number of blocks $0\dots 64$ endpoint $p$ holds within a 64-block stride starting at $k_{64j}$) into contiguous cache lines keyed by $k_{64j}$ fails on branching subtrees unless augmented with branch fingerprints:
- **Branching hazard:** Closure restricts an endpoint to 65 prefix lengths along a single path, not across a branching subtree under $k_{64j}$. If two requests share $k_{64j}$ and diverge at block $64j + 10$, and endpoint $p$ holds 41 blocks along branch A (`cursor[p] = 41`), a query arriving on branch B reads `cursor[p] = 41` and overcounts by 31 blocks.
- **Required fallback and Go write costs:** Each slot must also store a fingerprint of the endpoint's deepest held key in that window (or set a `branched` sentinel when holding multiple branches under $k_{64j}$) and fall back to probing the endpoint's set on a mismatch. In addition, adjacent single-writer slots in one 64-byte cache line incur MESI false sharing across writer cores, and Go's `sync/atomic` lacks 8-bit atomics.

### A.3 Global leaf-LRU order via composite `(millisecond, sequence)` stamps

For single-endpoint and per-endpoint capacity eviction, each `ChainSet` uses a local counter `s.seq++`. If a caller implements a cross-endpoint global evictor ([prefix_residency_first_principles.md §7.4](prefix_residency_first_principles.md)):
- Instead of a single router-wide `atomic.Uint64` counter (which puts all writer workers onto one contended cache line), each endpoint can stamp touched leaves with `(uint64(monotonicMillis) << 24) | (localSeq & 0xFFFFFF)`.
- Each endpoint maintains its local leaf min-heap and publishes its minimum leaf stamp atomically so a background reclaimer can evict from the endpoint with the oldest published leaf, yielding global leaf-LRU order within millisecond granularity.
