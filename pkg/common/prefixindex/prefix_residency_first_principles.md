# Prefix residency indexing for LLM request routing

This document states the prefix residency indexing problem from first principles, summarizes what the domain and empirical measurements establish, and lists the open questions for maintainers deciding on index architecture. Package-level design details for the Go implementation live in [prefixindex_design.md](prefixindex_design.md).

---

## 1. The problem

An LLM serving fleet runs many model-server endpoints (vLLM, SGLang). Each endpoint keeps a prefix cache holding the KV-cache blocks of recently processed token sequences. A request whose prompt shares a long prefix with blocks an endpoint already holds can skip recomputing that prefix, which saves GPU time and cuts time-to-first-token.

The router (in `llm-d`, the endpoint picker or EPP) chooses an endpoint for each request. To prefer endpoints that hold the request's prefix, it keeps an index of which blocks each endpoint holds, and asks: **for this request, how many leading blocks does each candidate endpoint hold?**

There are two ways to feed that index:

- **Precise.** The engines publish KV events (block stored, block removed, all blocks cleared) and the router applies them. The index then tracks what each engine reports.
- **Approximate.** The router records its own routing decisions ("I sent this prompt to endpoint E, so E probably holds it now") and applies its own eviction model. No engine signal is involved.

Both feed the same consumers: scorers and filters that turn per-endpoint match lengths into routing preferences.

---

## 2. The objects

### 2.1 Blocks and chained keys

A prompt is tokenized and cut into fixed-size blocks (for example 16 or 64 tokens). Each block gets a key, and the key is chained: key $i$ hashes block $i$'s tokens together with key $i-1$. Key $i$ therefore commits to the entire prefix, blocks $0$ through $i$.

Three consequences follow:

1. **All keys form a forest.** Each key has at most one parent, the key before it, unless a 64-bit hash collides, which is treated as negligible. Call this forest $U$, the union prefix trie of everything ever seen.
2. **A request is a root path.** A prompt of $L$ blocks is a path $k_0, \dots, k_{L-1}$ from a root of $U$. The path may leave $U$ partway, where its later keys are new.
3. **A key gives random access into the path.** Probing a single key $k_i$ asks "does this endpoint hold blocks $0..i$?" without visiting earlier keys. This is what makes sub-linear search possible.

Engines and the router may use different hash functions and block sizes. The router computes its own request keys, and for the precise path it maps engine keys to request keys. That mapping can be 1:1, many:1 (engine blocks smaller than the router's), or 1:many.

### 2.2 Per-endpoint residency

For each endpoint $p$, its held set $S_p$ is the set of keys whose blocks $p$ holds. Blocks can sit in several tiers (GPU, CPU, disk, shared storage) and, for hybrid models, several KV-cache groups. Each held key may carry metadata: tier, group, whether it is speculative (router-predicted) or confirmed (event-reported), and how many physical copies exist.

### 2.3 The query's answer

For request path $q$ and endpoint $p$, the match length is $d_p(q)$, the number of leading keys of $q$ that $p$ holds, or the index of the first miss.

This is what engines actually serve. vLLM's prefix lookup scans from block 0 and stops at the first miss, so blocks after a gap are unusable even if present. An endpoint's useful match is therefore the length of its contiguous run from block 0, not the count of held keys.

The consumers want, for each candidate endpoint: the matched blocks, the confirmed (non-speculative) matched blocks, a per-tier breakdown, and a weighted score. In the current scorer, the weighted score sums, per matched block, the highest weight among the tiers holding it.

---

## 3. Invariants and facts the domain provides

### 3.1 Ancestor closure: the central invariant

**Claim:** for a full-attention KV-cache group in vLLM, each endpoint's GPU-held set is ancestor-closed. If a key is held, so is its parent.

**Why (derived from vLLM source, not formally verified):**
- A request's blocks are freed in reverse order, tail first (`single_type_kv_cache_manager.py`: "Free blocks in reverse order so that the tail blocks are freed first").
- Eviction takes the oldest entry from the free queue.
- Any request referencing a block also references all its ancestors, so an ancestor's reference count reaches zero no later than its descendants'. Within one free call, descendants are queued before ancestors.
- A cache hit touches a whole prefix from block 0, which re-queues ancestors with or after their descendants.
- Descendants therefore sit ahead of their ancestors in the free queue and are evicted first. Eviction is leaf-first.

SGLang's radix cache evicts leaves only, so closure holds there structurally.

**Where closure fails (known exceptions):**
- Duplicate physical copies of one hash. Evicting one copy reports "removed" while another copy, and that copy's descendants, remain. With per-hash copy counting this is not a hole; with set semantics it creates a false hole.
- Sliding-window and Mamba groups, which are sparse by design. Old blocks are freed while later ones are kept.
- CPU offload tiers. vLLM's offload LRU evicts in store order, heads first, until a request finishes. The CPU tier alone is not closed, though the union across tiers often is.
- `unpin_blocks` order and KV load failures: edge paths in vLLM.
- Lost events. A dropped remove leaves a stale entry; a dropped store leaves an under-count.
- Router restart. Blocks stored before the restart are unknown, so later stores whose parent is unknown cannot be placed.

**Why closure matters.** On a closed set, "endpoint holds key $i$" is monotone along any root path: true up to some point, false after. Finding $d_p$ becomes a threshold search, $O(\log L)$ probes instead of $L$. The answer also equals the first-miss semantics engines serve, so it is exact.

**Maintaining closure in storage.** A key is *visible* iff it is resident and its parent is visible (or it is a depth-0 root). Visibility is closed by construction, and the match equals the first non-visible key on the path.
- When a resident visible interior key is removed, its visible descendants become *dormant*: they remain resident, but do not count toward match length.
- When the missing ancestor returns as visible, those descendants are restored to visible.
- **Measured on the `prefixchain/ChainIndex` prototype:** under vLLM's leaf-first eviction order, replaying a production-derived trace (Mooncake) produced zero visibility cascades. Under the opposite order, heads first, the incremental-cascade prototype produced 114 cascades per request and answers stayed exact.
- **Cost of maintaining visibility on interior events:** With explicit parent-to-child links, updating visibility when a visible interior block is removed or restored costs $O(\text{affected subtree})$. With child-to-parent links only, removing a dormant interior block is still $O(1)$ because its descendants are already dormant, but removing or restoring a visible interior block requires an $O(N_p)$ pass over the endpoint's $N_p$ resident blocks under that endpoint's write lock. If interior events recur frequently on large endpoints (from recompute duplicates, `unpin_blocks` order, lost stores, or head-first evictions), $O(N_p)$ rebuilds can block reads on that endpoint for hundreds of microseconds to milliseconds per event.

Dormant entries match engine semantics: the engine cannot serve blocks past a gap either.

#### 3.1.1 Trust boundaries and failure directions

Because threshold search reads visibility rather than raw residency, engine eviction order is a performance assumption, not a correctness assumption. Answers are exact for any eviction order relative to received events that carry valid parent links. The trust boundaries are:

1. **Parent-link accuracy (mistakes overcount, the dangerous direction).** Visibility is closed with respect to the recorded parent links. If an interior key is recorded as a root or linked to the wrong parent when its true predecessor is missing, a key whose real ancestors are absent can appear visible, and binary search overcounts. In particular, parentless tier-update events (`pool.go:533`) must not pass through a write path that defaults a missing parent to a depth-0 root.
2. **Event completeness (relative to the event stream, not the engine).**
   - A lost `Remove` leaves a stale held key and overcounts. Closure cannot detect a lost leaf removal because a stale leaf is still a valid closed state. Mitigations are an engine-derived capacity bound, endpoint reset on sequence gaps, and event-health staleness expiry.
   - A lost `Store` or router restart leaves a missing ancestor and undercounts. Undercounting is the safe failure direction because it does not route a request to an endpoint that lacks the prefix.
   - Periodic `"full"` re-announcements or upstream event deduplication filters (`pool.go`'s `eventDedupFilter`) require copy counting to live in a single layer so duplicate stores do not inflate copy counts.

### 3.2 Writes are partitioned by endpoint

KV events come from one endpoint at a time. In `llm-d`, the event ingestion pool shards by endpoint, so each endpoint's events arrive in order on one worker. Speculative writes from the router also target one endpoint. No natural operation needs to update many endpoints' residency atomically.

### 3.3 The engine is the source of truth for precise residency

In the precise path, removals are announced. The index does not need its own eviction policy for correctness. A capacity bound is only a backstop against lost removals or a deliberate memory cap. The index should never need to hold more than the engines hold.

### 3.4 Workload facts (measured on one trace, Mooncake, 128 simulated endpoints)

- About 1.1 `(endpoint, key)` pairs per distinct key. Little prefix content is shared across endpoints in this trace. Production fleets with many replicas sharing system prompts may differ.
- Holders of block 0 per query: median 19, p90 118. Shared roots are common on this trace, though `hash_id 0` appears in 46% of Mooncake records and may inflate root sharing relative to production workloads.
- Distinct values among a query's per-endpoint match lengths ($D$): median 1, p90 2. Endpoints that hold a prefix tend to hold it to the same depth.
- Walked prefix about 190 to 460 keys per query.
- Reads far outnumber writes per key.

Caveat: the benchmark harness originally simulated head-first eviction, the opposite of vLLM. Statistics gathered under that model, such as runs per query, were distorted.

---

## 4. The operations

| Operation | Precise | Approximate |
|---|---|---|
| Store | segment of keys under a known parent or root, per endpoint | whole root chain for the routed endpoint |
| Remove | by engine key (mapped to request keys) or request key, per endpoint | none; eviction by the router's per-endpoint capacity model |
| Clear | whole endpoint on engine prefix-cache reset | endpoint removal |
| Query | per candidate endpoint: match length | per candidate endpoint: match length |
| Recency signal | which endpoint stored or touched blocks | which endpoint was routed to |

---

## 5. Cost model: what actually matters

At production request rates, single-threaded index read latency is small: $35\,\mu\text{s} \times 1{,}000\text{ req/s}$ is about $3.5\%$ of one core, and tokenization (an HTTP call to a renderer) takes milliseconds and dominates end-to-end routing latency. The index matters for three other reasons:

1. **Concurrency collapse under multi-threaded load.** Today's index serializes on shared locks and LRU state, saturating at about $8\text{M block\_ops/s}$ across 14 threads and inflating tail latency when queries run concurrently.
2. **Read and write interference.** High-rate KV event ingestion mutates the same shared structures that routing queries read, stalling queries behind event workers.
3. **Memory footprint and garbage collection mark work.** Index size scales with endpoints times blocks times tiers. In Go, pointer-rich structures cost GC mark work on every cycle. **Measured:** the legacy precise index leaves $503\text{ MB}$ of pointer-bearing heap that the GC must scan on the Mooncake replay, whereas a pointer-free per-endpoint table leaves under $1\text{ MB}$ of pointer-bearing headers in the index (subject to also flattening or relocating `pool.go`'s pointer-rich `eventDedupFilter`, which otherwise retains an estimated $100\text{–}130\text{ MB}$ of GC-scannable heap).
4. **Hit-rate fidelity under memory pressure.** Whether the index keeps the residency information that maximizes routing quality when it cannot hold everything.
5. **Correctness and testability.** Exact answers allow bit-exact differential tests against reference implementations. Approximate answers need statistical testing.

---

## 6. The core data-structure problem

Stated abstractly:

> A dynamic forest $U$. Each color (endpoint) holds a downward-closed subset of $U$, updated only by that color's single writer: add a path segment below a held node, remove leaves, clear. Query: a root path $q$ and a set of candidate colors. Output, for each candidate holding $q$'s root, the depth at which $q$ leaves that color's subset.

This is a colored-ancestor reporting problem over downward-closed color classes, with partitioned writers.

### 6.1 Lower bounds

- **Output bound.** Every holder must be reported, so a query costs at least proportional to the number of holders $H$.
- **Single-writer memory bound ($\Omega(|C|)$ without shared root state, $\Omega(H)$ with a shared root filter).** If memory is partitioned into single-writer regions $M = \bigsqcup_{p \in C} M_p$, an exact query must read at least one cell from every candidate endpoint $p \in C$ ($\Omega(|C|)$ cell reads), because a query that reads zero cells of $M_p$ cannot distinguish $S_p = \emptyset$ from $S_p = \{k_0\}$. Crossing the single-writer boundary only at depth $0$ via a shared root-holder map (written about 0.24 times per request on the trace) reduces the required per-endpoint cell reads from $\Omega(|C|)$ to $\Omega(H)$.
- **Probe bound for one endpoint.** With only membership probes on a monotone predicate, finding one endpoint's threshold among $L$ positions needs $\lceil \log_2(L+1) \rceil$ probes.
- **Getting below about one probe per holder requires shared state** that summarizes many endpoints per key. Holding per-endpoint work under a constant $T$ requires shared state that replicates child-membership for every key held by more than $T$ endpoints. Shared writes are paid there.
- **Relation to colored-ancestor lower bounds.** The Alstrup–Husfeldt–Rauhe lower bounds for general colored- and marked-ancestor problems rely on hard instances with non-monotone marks along a path, which ancestor closure excludes.

### 6.2 The two families

**Key-major:** key mapped to its holders (Dynamo's concurrent radix tree, SGLang's router, Mooncake's conductor, `llm-d`'s legacy precise index).
- A query walks the path once and reads holder sets. Ideally one probe per key, or per compressed edge, independent of endpoint count. Holders of a key sit together in memory, for example one 256-bit mask.
- Every store writes shared per-key state, so concurrency control is needed (locks, atomics, version validation), and reads can race with writes.
- Memory deduplicates content shared across endpoints, which matters when sharing is high.
- Production evidence: Dynamo's bitset redesign documents rare undercounts under races, and an earlier rare overcount.

**Endpoint-major (Pod-major):** per endpoint, a set of its keys (`llm-d`'s approximate index since #3241, and `prefixindex`).
- A query probes each candidate endpoint: binary search over prefix length (analogous to Waldvogel et al.'s IP-lookup scheme) costs $O(\log L)$ per holder, plus one bitset test per non-holding candidate.
- Writes touch only the writing endpoint's state, so they partition along the same axis as event ingest. No cross-endpoint coordination is needed beyond depth-0 root arrivals and evictions.
- Exact per endpoint relative to received events with valid parent links, and simple to test differentially.
- Costs about one cache miss per holder at minimum, and duplicates shared content per endpoint (about 10% on the measured trace).
- Maintains closure via the visible/dormant state machine so threshold search remains valid relative to recorded parent links even when interior blocks are evicted out of order.

### 6.3 Ways to narrow the gap within endpoint-major

- **Threshold sharing.** Search later endpoints first at match lengths already found for earlier endpoints. If the $D$ distinct match depths are shared across endpoints, each later holder needs 2 membership probes to verify its depth equals a known threshold ($k_{\tau-1} \in S_p$ and $k_\tau \notin S_p$, or 1 probe when $k_{\tau-1}$ is a childless leaf in $S_p$), plus $\le \lceil \log_2(D+1) \rceil$ probes to select among $D$ thresholds, and $\sum_{r=1}^D \log_2(\Delta_r) \le D \log_2(L/D)$ probes across the $D$ newly discovered thresholds, for $\mathcal{O}(H(1 + \log D) + D \log(L/D))$ probes total. This is about 2 probes per holder when $D$ is small ($D \le 2$) on medium-to-long prefixes ($L \ge 16$); for very short prefixes ($L \le 4$), binary search and linear scan perform the same $1\text{–}3$ probes.
- **Root holder index.** Leaf-first eviction means an endpoint loses a shallow key only after its whole subtree is gone, so a shared map from root key to a fixed-width holder bitmask (e.g. 256 bits) is written rarely (estimated 0.24 shared writes per request on the trace). It removes the per-candidate probe and lock for non-holders within the bitmask width, with endpoints beyond ordinal 255 falling back to direct per-endpoint probing, and requires clearing an endpoint's bits before its integer ordinal is reused.
- **Popular-boundary layer.** Shared holder masks only where more than about $T$ endpoints share a key. That bounds per-endpoint work by $T$, with shared writes confined to popular keys.
- **Key-major read view with exact fallback.** A shared holder-mask view, plus per-endpoint versions. Use the view for endpoints it is current for, and probe the others directly. Exact, but adds bookkeeping and memory.
- **Constant factors.** A hash table indexed directly by the key's bits (keys are already hashes), interleaved probing across endpoints, fewer locks per query.
- **Beyond isolated queries.** Start each search from the previous turn's result in a session, batch concurrent queries, filter non-holders before taking locks.

---

## 7. Recency and capacity

### 7.1 Does the index need recency at all?

- **Precise path.** Correctness comes from events. Recency matters only when a capacity bound binds, either from lost removals or from a deliberate memory cap.
- **Approximate path.** Recency is the eviction model. It predicts what the engine has likely evicted.

### 7.2 Recency follows from closure

Reaching a node passes through every ancestor, so under whole-path LRU a node's true recency is the latest touch anywhere in its subtree. Because maximum over timestamps is associative, commutative, and idempotent:
- Touching only the end of a newly stored segment, and propagating a child's timestamp to its parent via `max` when the child is evicted, gives every node the exact maximum timestamp of its historical subtree by the time its last child is removed and the node itself becomes an evictable leaf.
- Evicting the leaf with the oldest timestamp therefore reproduces the eviction order of refreshing every key along each whole-chain touch. Under whole-chain touches (as in the approximate path), this is verified bit-exact against a whole-path LRU oracle (`TestFrontierLeafStampingMatchesWholePathLRU`), whereas interior-only touches without touching descendants update only the touched prefix.
- Eviction stays leaf-first, prioritizing dormant leaves ahead of visible leaves, so closure is preserved.

### 7.3 Who should be touched by a read?

Only the routed endpoint's engine reads its blocks. A router query should therefore refresh only the endpoint the request is sent to, not every endpoint that matched. `main`'s key-level LRU refreshes keys regardless of endpoint, which mixes signals from different endpoints.

### 7.4 Budget allocation

**Measured:** splitting one global budget evenly into per-endpoint budgets loses hit rate to a single global LRU under uneven routing. At the smallest budget, routed-endpoint hit rate dropped from 0.28 to 0.16. Options:
- **Per-endpoint budget from engine capacity.** The index never needs more than the engine holds, so the bound then fires only on lost events. **Measured** at or above the global LRU at every budget, though that comparison is not equal-memory because sizing each endpoint to engine capacity grants more total memory than the constrained global budget.
- **Global eviction over per-endpoint storage.** A heap or tournament tree over each endpoint's oldest leaf gives global-LRU behavior, with eviction done by a background reclaimer or try-locks. It needs comparable stamps across endpoints: a global clock or coarse epochs.
- **Per-endpoint quotas rebalanced in the background** by write share or routed share.

---

## 8. Production semantics that complicate the model

1. **Copy counts.** Duplicate physical copies and vLLM recompute (which re-emits stores beside old copies) mean a remove may mean "one copy gone", not "block gone". Set semantics create false holes (which undercount); per-hash counting fixes them and must survive dormancy, but must not be combined with a separate caller-side `eventDedupFilter` (which suppresses removes) or `"full"` report-mode re-announcements without idempotent handling.
2. **Many:1 engine keys.** A request key is servable only if all its constituent engine blocks are present (an AND). Counting at request-key granularity gives an OR, which overstates.
3. **Hybrid models (HMA groups).** The engine's hit is the minimum across groups. Only full-attention groups are closed, so a full-attention run is an upper bound.
4. **Tiers.**
   - Per-tier sets are not closed (vLLM's CPU tier evicts heads first); the union across tiers often is.
   - The legacy per-block max-weight score is not monotone along the path for mixed-tier endpoints, so exact parity needs a linear walk for those endpoints.
   - A staged model (primary-tier run from block 0, then inclusive primary-plus-secondary continuation from that point) matches how engines serve requests and supports binary search on both sets. However, if blocks appear on the secondary tier first without passing through the primary tier with a parent link (such as SGLang host inserts, prefetch from external storage, or offload-connector loads without parent keys), they cannot be placed in the union chain and the union undercounts.
5. **Data-parallel ranks.** Separate pools per rank; routing to an endpoint does not choose a rank.
6. **Lost events and staleness.** Confirmed state needs expiry driven by event health. A lost remove otherwise lives forever.
7. **Speculative entries.** Router-predicted, short TTL, then confirmed or expired. They need clear rules for promotion and retirement.
8. **Router restart.** Although a per-endpoint set can hold segments with an unknown parent as dormant when the caller supplies chained request keys, router restart recovery in `llm-d`'s precise path cannot rely on dormant promotion: `pool.go` (`pool.go:612-622`) drops `BlockStored` events whose parent engine key cannot be resolved because request keys cannot be computed without the parent's request key, and vLLM in delta mode does not re-announce cached parent blocks that remain resident across a restart. Restart recovery therefore requires an engine snapshot or full resync.

---

## 9. Concurrency model choices

- **Per-endpoint `sync.RWMutex` locks.** Simple and exact. Without root filtering, taking 128 shared locks per query contends under multi-core load. With a root-holder filter narrowing candidates to the $H$ holders (median $H = 19$), 19 uncontended single-threaded `RLock`/`RUnlock` pairs take about $230\text{ ns}$ total. Under multi-goroutine reader load hitting the same hot holders, `RWMutex.readerCount` still experiences atomic `LOCK XADD` cache-line contention across cores ($654\text{ ns/op}$ in 8-worker `BenchmarkIndexConcurrentReadWrite`), though throughput remains well above realistic per-EPP request rates.
- **Per-endpoint optimistic seqlock with `RLock` fallback.** Avoids reader atomic increments when no writer is active, but in Go every field read inside the optimistic window must use `sync/atomic` loads, or concurrent reads and writes are data races under the Go memory model and fail `-race`.
- **Shared structures with atomics** (Dynamo). Fast reads and parallel writes; complex, and approximate under races.
- **Immutable snapshots published by writers** (RCU-style). Lock-free reads; the cost moves to writes and memory.
- **Relaxing exactness** (bounded staleness, undercount-only) opens more designs. Undercounting biases routing away from the right endpoint; overcounting routes toward an endpoint that lacks the prefix.

---

## 10. What is established, argued, or open

**Established on the Mooncake trace harness (`prefixchain/ChainIndex` prototype in `llm-d`, one trace, one machine):**
- With closure maintained by visibility, matches are bit-exact against `InMemoryIndex` on the precise trace replay.
- Under vLLM's leaf-first eviction order, maintaining closure produced zero cascades on the trace replay; under head-first eviction on the incremental-cascade prototype, it produced 114 cascades per request while remaining bit-exact.
- Per-endpoint writes raise mixed read/write throughput about 8x over the legacy key-major precise index on the trace harness.
- Splitting one global budget evenly across endpoints loses hit rate under uneven routing; per-endpoint budgets sized from engine capacity do not lose hit rate, though that comparison is not equal-memory (§7.4).

**Established by unit, fuzz, and microbenchmark suites on the standalone `prefixindex` package (`set_test.go`):**
- Bit-exact differential parity against the reference oracle under random leaf and interior removals (`TestDifferentialLeafFirstAndHeadFirstCascades`) and fuzzing (`FuzzChainSet`).
- Head-first eviction of a 20-block chain triggers 1 rebuild when the visible root is removed and 0 rebuilds on the remaining 19 dormant removals (`TestHeadFirstEvictionAndImmutableResidentLineage`).
- Bit-exact equivalence between leaf-frontier stamping and whole-path LRU under whole-chain touches (`TestFrontierLeafStampingMatchesWholePathLRU`).
- Pointer-free per-endpoint block storage (`40.84 MB` total `HeapInuse`, `81.7 B/block`, 0 pointers per block across $524{,}288$ resident blocks on 128 endpoints in `BenchmarkRetainedGCHeap`).

**To verify when integrating `prefixindex` into `llm-d`:**
1. Approximate differential parity against `simplelru`.
2. Precise differential parity against `InMemoryIndex` on the Mooncake trace harness.
3. In-order Mooncake trace replay parity under both leaf-first and head-first eviction orders.
4. Capacity budget and hit-rate benchmark on the Mooncake trace.
5. Approximate A/B throughput and latency benchmark in `llm-d`.

---

## 11. Questions for reviewers

1. Is ancestor closure the right central invariant, and are the listed exceptions complete for vLLM and SGLang?
2. Is the single-writer-memory lower bound sound? Does any known structure beat about one probe per holder without shared writes?
3. Should a routing index be exact, or is bounded inaccuracy (undercount-only, stale) acceptable in exchange for key-major speed?
4. For precise residency, should the index have any eviction policy beyond engine events and an engine-derived bound?
5. Which tier semantics should the scorer use: per-block max, or staged continuation?
6. What do production fleets look like for prefix sharing across endpoints, endpoint counts, and request rates? These decide between the families more than any algorithmic argument.
7. Which workloads beyond the Mooncake replay should be the reference: multi-tier offload, hybrid models, agentic multi-turn traffic?
