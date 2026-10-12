# `prefixindex`

Package `prefixindex` implements a compact, pointer-free (`noscan`) prefix residency set ([`ChainSet`](set.go)) and concurrent multi-endpoint router index ([`Index`](index.go)) for LLM request routing in Go 1.26+.

It backs both **Approximate** (router-predicted whole-chain) and **Precise** (engine KV-event segment/removal) routing paths with a single data structure.

- **First-principles problem statement & findings:** [`prefix_residency_first_principles.md`](prefix_residency_first_principles.md)
- **Package design & design-space extensions:** [`prefixindex_design.md`](prefixindex_design.md)

---

## Core Design: 3-State Resident-Only Model

An inference engine can reuse a cached token-block prefix `keys[0:d]` only up to the first missing block; resident blocks sitting behind an evicted ancestor cannot be served until that gap is filled.

`ChainSet` models every block key in the universe as being in one of three states on a given endpoint:

1. **`Absent`**: Not resident on the endpoint (not stored in memory).
2. **`Visible`**: Resident on the endpoint and connected to a resident depth-0 root block by a contiguous chain of resident ancestors.
3. **`Dormant`**: Resident on the endpoint, but waiting on at least one `Absent` ancestor along its chain to root.

Only resident blocks (`Visible` or `Dormant`) exist in memory (`set.Len()`). Each 40-byte `block` record contains zero pointers (allocating in Go's `noscan` heap) and strictly separates:

- **Authoritative facts:** `(key, parentKey, stamp, isRoot)` — set on insertion, immutable while resident, and deleted via swap-delete on removal.
- **Derived projections:** `(children, vis, heapIdx, waitingOn)` — updated incrementally in $O(1)$ / $O(\log N)$ on leaf writes, leaf removals, and `Dormant`-only mutations, and recomputed in a single linear pass (`rebuild()`) whenever a `Visible` interior block is removed or a gap-filling `Visible` block wakes waiting descendants.

---

## Usage & Operational Contracts

```go
idx := prefixindex.NewIndex(prefixindex.Config{
    Endpoints:           128,
    PerEndpointCapacity: 16384,
})

// Approximate path (or root KV-event store): record a full root chain on endpoint 0.
idx.AddRootChain(0, promptKeys)

// Precise KV-event path: record a continuation segment under parentKey or remove an evicted block.
idx.AddChainUnder(0, parentKey, suffixKeys)
idx.Remove(0, evictedKey)

// Query contiguous Visible prefix match lengths across candidate endpoints.
matches := idx.Match(promptKeys, candidateEndpoints)
```

### Key Contracts
- **Zero-value usability:** The zero value `var s prefixindex.ChainSet` is an empty set with unlimited capacity ready for immediate use.
- **Endpoint ordinals:** `Index` addresses endpoints by dense integer ordinal `0 .. Endpoints-1`. Ordinals in `[0, MaxEndpoints)` (`0 .. 255`) use the shared 256-bit root-holder bitmask filter; ordinals `>= 256` fall back to direct per-endpoint probing. Out-of-range ordinals are no-ops.
- **Read-only queries:** `Match` and `MatchInto` are read-only (`RLock`) and never mutate recency stamps. Callers on the approximate routing path should call `AddRootChain(selectedEndpoint, promptKeys)` after selecting an endpoint.
- **Set semantics on `Remove`:** `Remove(endpoint, key)` immediately removes the block and marks any resident `Visible` descendants `Dormant`. Callers receiving per-physical-block KV events should deduplicate multi-copy reference counts upstream before calling `Remove`.

---

## Testing & Benchmarks

```bash
go test -race -v ./...
go test -fuzz=FuzzChainSet -fuzztime=10s ./...
go test -bench=. -benchmem ./...
```

Measured on Go 1.26.6 (`linux/amd64`, AMD EPYC 7B12):

| Benchmark | Result |
| :--- | :--- |
| `ChainSet.RunLength` ($L = 4 \to 320$ blocks) | **`56.9 ns/op`** ($L=4$) to **`150.9 ns/op`** ($L=320$), `0 B/op`, `0 allocs/op` |
| `Index.MatchInto` (128 endpoints, $L = 320$, Root Filter + Threshold Sharing) | **`2.46 µs/op`**, `0 B/op`, `0 allocs/op` |
| `Index` Concurrent 87.5/12.5 Read/Write (`b.RunParallel`, 128 endpoints) | **`654.0 ns/op`**, `0 B/op`, `0 allocs/op` |
| Retained GC Heap ($524{,}288$ resident blocks across 128 endpoints) | **`40.84 MB`** (`81.7 B/block` incl. map + heap), `noscan` (`0` pointers per block) |
