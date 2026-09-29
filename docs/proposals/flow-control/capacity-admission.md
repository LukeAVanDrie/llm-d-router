# Capacity admission for flow control

Status: proposed. Tracking issues: to be filed before review. Line references are to `main` at
`6fd3a7fac`.

## Summary

Flow control releases the request at the head of its queue when a pooled saturation signal is
below the band's ceiling. This proposal adds a per-endpoint check behind that gate. A capacity
ledger books every placed request against the endpoint that serves it, updates the booking from
the response stream, and releases it on completion. Before the head dispatches, flow control asks
an endpoint gate which of the request's candidate endpoints could take it now without the request
waiting inside an engine. Flow control picks no endpoint; the scheduler places the request.

For one endpoint, the rule is a vector inequality over its resources:

  admit on e  iff  X + w <= C on every axis, and the memory look-ahead passes.

X is the endpoint's state, w the request's demand, and C the endpoint's capacity, each a vector
over three axes: KV cache blocks, tokens committed to the engine's next step, and concurrent
sequences. Only memory grows between decisions, so only memory needs a look-ahead; it arrives in a
later stage.

The work lands in three pull requests:
- PR A adds the ledger as a data producer: it books and publishes, with no effect on routing.
- PR B adds the endpoint gate to flow control and makes the ledger that gate in shadow mode: it
  reserves capacity and records what an enforcing gate would do, and never withholds a request.
- PR C, after shadow data exists, makes the gate enforce.

vLLM is the only engine this proposal validates.

## Motivation

Take a pool of four endpoints, each with 1,000 KV cache blocks, at 60%, 70%, 80% and 90% KV
utilization. The pool mean is 75%. The utilization detector's default KV threshold is 0.8
(`pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization/config.go:33`), so flow
control releases. The head request has a 450-block prompt. No endpoint has 450 free blocks. The
request lands on one of them and either waits in that engine's queue or forces a preemption of a
running request. Had it stayed at the router, it could have gone to whichever endpoint freed first.

The pooled signal has two gaps that this example shows.

- **Pooling.** `SaturationDetector.Saturation` returns one number for the candidate pool
  (`pkg/epp/framework/interface/flowcontrol/plugins.go:115-128`). The dispatch cycle compares that
  number with each band's ceiling before it selects an item
  (`pkg/epp/flowcontrol/controller/internal/processor.go:501-519`). The head request's size never
  enters the decision.
- **Lag.** The dispatch cycle runs every millisecond (`processor.go:228`) and on every arrival
  (`processor.go:268-269`). The utilization detector reads scraped metrics refreshed every 50 ms by
  default (`pkg/epp/server/options.go:154`). Requests released between scrapes are invisible to it.

The target behavior is that a request waits at the router until some endpoint can start it. Once
dispatched, a request's waiting is bound to one engine, and a preemption discards work the engine
already did.

## Goals

- Accounting that does not wait for a scrape: every placement is booked when it happens and
  corrected upward by the scrape.
- A per-endpoint view of that accounting that flow control, filters and scorers can consume,
  validated by the framework's data-dependency graph.
- A per-endpoint check for the head request, against the request's own candidates.
- A shadow mode whose metrics show, before any gating, what the check would do.

## Non-goals

- Prefill/decode disaggregation; the ledger marks prefill and decode endpoints ineligible.
- More than one router replica writing to the same endpoints.
- Choosing which endpoint serves a request, queue ordering, or shedding.
- Engines other than vLLM, until each is validated.
- Changes to vLLM or any other engine.

## Terms

- **Endpoint**: one model server as the datalayer sees it. Each data-parallel rank on its own port
  is its own endpoint (`pkg/epp/framework/interface/datalayer/endpoint_metadata.go:44-46`).
- **Head**: the item the dispatch cycle selects from a band (`processor.go:520-534`).
- **Axis**: one resource of an endpoint: memory (KV cache blocks), step (tokens committed to the
  engine's next step: prompts not yet prefilled plus one decode step per running sequence), and
  slots (concurrent sequences).
- **Decode step**: 1 plus the engine's speculative tokens: the tokens one decode step schedules for
  one sequence, and the KV slots it holds for that step.
- **Footprint**: the charge of a request in a given state on an endpoint with a given block size
  and step budget, per axis.
- **Lease**: a placed request's footprint on its endpoint, from placement to completion.
- **Reservation**: a dispatched request's footprint between dispatch and placement, on one endpoint
  that could take it (the witness).
- **Booked**: the sum of an endpoint's leases. **Reserved**: the sum of the reservations that name
  it. **Scraped**: the engine's latest report. **Used**: scraped plus the bookings made since that
  scrape, and never less than booked, per axis.

## Design

### 1. The ledger is a data producer plugin

The ledger's state is read in three places: by flow control before dispatch, by a filter at
placement, and by any scorer that wants per-endpoint headroom. The framework's pattern for
per-endpoint state written by the request lifecycle and read on both sides is the in-flight load
producer: it publishes `InFlightLoad`
(`pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload/producer.go:371-380`,
`:740-745`), which the concurrency detector, two filters, three scorers and two other producers
consume. The framework validates those dependencies at startup
(`pkg/epp/framework/interface/datalayer/registry.go:124-128`). A ledger outside the plugin model
could not declare what it produces, so no consumer could depend on it.

The ledger is one plugin, `capacity-ledger`, in
`pkg/epp/framework/plugins/requestcontrol/dataproducer/capacityledger/`:

| role | interface | use | PR |
|---|---|---|---|
| producer | `ProducerPlugin` | publishes the per-endpoint `EndpointCapacity` view | A |
| consumer | `ConsumerPlugin` | tokenized prompt (required), prefix-cache match (optional) | A |
| endpoint lifecycle | `EndpointExtractor` | endpoint add and delete; installs the view | A |
| placement | `PreRequest` | books the lease; converts the reservation | A, B |
| response | `ResponseBodyProcessor` | ages the lease; releases it at end of stream | A |
| gate | `EndpointGate` (new) | the per-endpoint check flow control calls | B |
| filter | `Filter` | rechecks at placement | B |

The package keeps the accounting in small pure pieces and uses the plugin type as a facade over
them:
- `account.go`: the axes, a vector type `vec`, a per-endpoint `account` with booked and reserved
  vectors and the booked vector at the latest scrape, and the state rule. `apply` and `markScrape`
  are its only writers and `snapshot` its only reader, so every read sees a state a sequence of
  writes produced.
- `resident.go`: a request's state on an endpoint, the endpoint's geometry, and `footprint`, the
  only definition of a request's charge. A lease recomputes its footprint on every update and
  applies the change; the gate's demand is the footprint of a request that has not started.
- `check.go`: `check(snapshot, demand, own)` evaluates the inequality and returns a verdict with
  the axis that failed.
- The remaining files adapt the framework's extension points to these.

A sibling type counts the engines behind each endpoint from the raw metrics scrape; one object
cannot implement both the endpoint and the polling extractor interfaces
(`pkg/epp/framework/interface/datalayer/plugin.go:45-59`).

The published view, in `pkg/epp/framework/plugins/datalayer/attribute/capacity/`, is per axis:

```go
type Axis struct{ Capacity, Booked, Scraped, Used int64 }

type EndpointCapacity struct {
    Memory, Step, Slots    Axis
    BlockSize              int64
    Eligibility            Eligibility
    Generation             uint64
    ScrapeTime             time.Time
}
```

Reservations are not published; they overstate any one endpoint's load by design.

### 2. Booking

**Keys.** Endpoints are keyed by ID, since scheduling endpoints are per-request copies
(`pkg/epp/framework/interface/scheduling/types.go:114-125`). Leases and reservations are keyed by
the request's `*InferenceRequest` pointer: flow control, the filter and the lifecycle hooks receive
the same pointer, and the request ID comes from a header a client can set.

**Lifecycle.**

| event | hook | effect |
|---|---|---|
| endpoint added | `Extract` | state created; view installed |
| endpoint recreated under the same ID | `Extract` | state kept, so running requests stay booked; view installed on the new endpoint |
| endpoint deleted | `Extract` | state and series dropped, with the stale-delete guard (`inflightload/producer.go:352-357`) |
| placement | `PreRequest` | lease booked on the primary profile's first target |
| first chunk | `ResponseBody` | prefill done; the lease charges one decode step per sequence |
| later chunk | `ResponseBody` | each sequence's age is its share of the cumulative event count, one decode step per event, or of cumulative completion tokens when the stream reports usage |
| sampling pass | ticker | every lease recomputed with its endpoint's current block size; a lease with no chunk advances by imputed progress |
| completion | `ResponseBody` | lease released |
| request context ends | `context.AfterFunc` | lease released if no end of stream released it |

`Response.StreamedEvents` and `Response.Usage` are running totals
(`pkg/epp/framework/interface/requestcontrol/types.go:52-78`).

**A request's footprint.** A request generates n sequences per prompt (the request's `n`), and a
completions request may carry several prompts.
- Memory: for each sequence, its prompt, the tokens it has generated, and its next decode step,
  rounded up to blocks.
- Step: the uncached prompt of every sequence, capped at the step budget, until prefill ends; then
  one decode step per sequence.
- Slots: one per sequence.

The prefix match reduces the step axis only, counting cached blocks without device-tier weighting, since
a block cached in any tier is not prefilled again. vLLM counts cached blocks that no running request
references as free, and a prefix hit on one moves it out of the free pool, so the endpoint's use
rises by the matched blocks. The router's approximate index can also report blocks the engine has
evicted. The cost is an overstatement when k leases or sequences share a prefix: the ledger books
it k times while the engine holds it once, an excess of (k-1) times the prefix (open decision 1).

**Imputed progress.** A request that has streamed no chunk (a non-streaming request, or a streaming
request answered with one buffered body) advances at each sampling pass by the endpoint's prefill
rate (uncached prompt tokens per second to first chunk, measured from prompts of at least 128
uncached tokens) and decode rate (tokens per second between chunks, and, for a request that
streamed none, its reported completion tokens over its time at end of stream), capped at the
maximum output. An endpoint without a decode rate uses the rate across all endpoints.
Imputed progress never decreases, and the first chunk's observed age replaces it. A rate expires
30 seconds after its last observation (convention). Without a prefill rate, the request counts as
prefilled at the next sampling pass: the step budget is held only for the prefill's few steps, and
holding it for the request's lifetime would cap an endpoint at the budget over the prompt (four
concurrent 2048-token requests at a budget of 8192). Without any decode rate, the lease books no
output: booking the maximum output would pin the endpoint full, since used is never below booked,
while the engine's report covers the growth from the next scrape. A gauge counts such leases.

**The state rule.** Per axis, used = max(booked, scraped + added since the scrape), where added is
the running total of increases to booked. A request placed after the scrape adds to the engine's
report instead of hiding under it. Releases do not offset the additions: the engine frees a
finished request before the router sees its end of stream, so a release after the scrape may be of
a request the scrape already omits. A booking made and released between two scrapes therefore
counts until the next one. The ledger records the total at each scrape whose time advances; when
it has no record for the current scrape, used is the larger of booked and scraped. Scraped memory
is the usage fraction times one less than the block count, rounded up (vLLM reserves one null
block and computes usage over the rest); scraped step is one decode step per running request, a floor since the engine
reports no pending prefill; scraped slots are the running count.

**Eligibility.** An endpoint is ineligible when its metrics are older than the EPP's
`--metrics-staleness-threshold`, it reports no block count, more than one vLLM `engine` label value appears behind it, its
cache configuration reports a sliding-window or hybrid layout, it exports speculative-decoding
metrics while `speculativeTokens` is 0, its engine type is not vLLM, or it has a prefill or decode
role.

### 3. The endpoint gate

```go
type EndpointGate interface {
    plugin.Plugin
    Gate(ctx context.Context, req FlowControlRequest, candidates []datalayer.Endpoint) GateDecision
}

type GateDecision struct {
    Reservation Reservation
}

type Reservation interface {
    Dispatched(requestCtx context.Context)
    Refund()
}
```

`GateDecision` is a struct so that enforcement can add a verdict without changing the method.

**Flow control.** After a band's saturation gate passes and the head is selected, the processor
calls `Locate(ctx, req.GetMetadata())`, so the gate sees the request's own subset
(`pkg/epp/requestcontrol/candidates.go:256-280`). It skips the gate when the subset is empty (the
director's 503 answers it, `pkg/epp/requestcontrol/director.go:256-262`) and when the caller has
already finalized the item. The existing band gate is unchanged, so usage limits and priority
holdback keep working. With no gate configured, the cycle is unchanged.

**Reservation handover.** Every path ends in exactly one of `Dispatched` and `Refund`:
- The processor finalizes the dispatch with `FinalizeDispatched(reservation)`, which carries the
  reservation in the item's final state. If the caller's cancellation finalized the item
  first (`pkg/epp/flowcontrol/controller/controller.go:425-437`), the call reports that it lost and
  the processor refunds.
- If the cleanup sweep removed the item first (`processor.go:627-633`), the processor refunds.
- After `EnqueueAndWait` observes the dispatch, it calls `Dispatched` with the caller's context.
  That is the external-processing stream's context (`pkg/epp/handlers/server.go:304`), which ends
  with the request, so a reservation placement never converted is released then.

**The ledger as gate.** For each candidate, the ledger builds the request's demand w and checks
`used + w <= C` per axis, then again with the reservations that name the endpoint. The step axis
asks only that some budget remain: vLLM serves running requests first and gives the leftover budget
to waiting requests as chunked prefill, so a long prompt starts while any budget remains, and its
step demand is capped at what remains. An endpoint with stale metrics reads as full, since vLLM's
metrics endpoint slows under the overload the gate exists for.

The ledger reserves w on one endpoint where both checks pass: the one whose tightest axis has the
most room left after the request. Every dispatched request is then backed by one endpoint that
holds it alongside the other reservations, which is a packing of all in-flight requests on all
three axes. Reserving on every fitting endpoint would multiply one request's demand by the number
of candidates, and a pool-wide sum would admit requests that fit no single endpoint. Placement
converts the reservation to a lease on whichever endpoint the scheduler chooses. The demand is the
footprint of the request before it starts. The prefix match is not
known before scheduling, so the step demand is the whole prompt, and before tokenization the prompt
is bounded by the request's size in bytes. The demand is capped at the endpoint's capacity on every
axis, so an empty endpoint can take any single request. When no candidate passes, the request
dispatches without a reservation and is counted as a would-hold, labeled reserved, capacity or
stale; when no candidate is eligible at all, it is counted as unaccounted.

**Shadow mode.** In PR B the gate never withholds a request. The filter, listed before any other
filter in each profile that places the gated requests, rechecks the scheduler's candidates with
the exact prompt and prefix match and records which the request fits; it never removes an
endpoint. At placement the outcome is counted once per request: a race when no candidate fit, an
unfit placement when the scheduler chose an endpoint that did not fit while another did, and
unfiltered when the filter did not run. This measures the would-hold rate, the placement race
rate, and how much a reservation sized from bytes overstates the lease on memory and step, which
are the inputs to open decisions 1-3.

### 4. Configuration

Plugin parameters, decoded strictly:

| parameter | required | default | notes |
|---|---|---|---|
| `slotsPerEndpoint` | yes | | vLLM `max_num_seqs` |
| `stepTokenBudget` | yes | | vLLM `max_num_scheduled_tokens` when set, else `max_num_batched_tokens` |
| `maxModelLen` | yes | | vLLM `max_model_len`; bounds every request's output |
| `speculativeTokens` | no | 0 | vLLM `num_speculative_tokens` |
| `prefixMatchInfoProducerName` | no | approximate-prefix producer | |
| `reservationTTL` | no | 1s | releases reservations that flow control neither bound to a dispatched request nor refunded |

The engine parameters have no default because vLLM does not export them and their values vary by
version and hardware. The plugin is not created automatically.

Flow control gains `flowControl.endpointGatePluginRef` (Alpha). The loader requires the referenced
plugin to be an endpoint gate, a filter and a `PreRequest` plugin, and every profile that lists it
to list it first. It reorders nothing the operator wrote; in a default profile the loader
synthesizes, it places the gate first.

### 5. Metrics

Metrics follow the plugin pattern (`inflightload/metrics.go`), with a `producer_name` label and,
per endpoint, `endpoint_name` and `namespace`. `docs/metrics.md` lists them.

- PR A: booked and capacity per endpoint and axis; eligibility per endpoint; per-scrape drift
  (booked minus scraped over capacity) and bypass excess (engine running plus waiting minus
  booked slots).
- PR B: reservations; gate checks by result (fits, ineligibility, the failing axis, or
  reserved-axis); fitting endpoints per request; would-holds; unaccounted requests; placement
  races by cause (filtered-out, recheck, outside-set); placement latency; reservation-to-lease
  ratio; reservations reaped; unreserved placements.

Per-scrape metrics are sampled by a ledger ticker at the refresh interval. The engine's preemption
counter is not scraped by default; shadow deployments add it through the engine configuration's
`customMetrics` (`pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go:65-74`).

## Guarantees

PRs A and B change no routing decision. With a gate configured, flow control makes one extra
`Locate` call and one gate call per dispatch.

## Delivery

Each PR has its own issue and a green `make presubmit`.

- **PR A.** The ledger: account, leases, state rule, eligibility, engine counter, view, metrics,
  README, runner registration.
- **PR B.** The `EndpointGate` contract and processor, controller, loader and runner changes; the
  ledger's shadow gate and filter; a shared mock gate for tests.
- **PR C (enforcement).** The gate may return a wait verdict; a waiting head is pinned so later
  cycles recheck the same item instead of a fresh fairness pick
  (`pkg/epp/framework/plugins/flowcontrol/fairness/roundrobin/roundrobin.go:105-122`), with the
  stop scoped to requests whose candidates overlap; the filter restricts to the reserved set and
  refuses a race at placement with 429 `rejected-saturated`, through a single-call filter form
  that returns an error; enforcement requires a declared single writer. Before it merges, an
  overload run on the inference simulator (`test/e2e/README.md`) shows lower engine waiting and
  preemption than the band gate alone, booked returning to zero after drain, and races mapped to
  429. The memory fit check alone does not remove preemption: in simulation it spent 8.9% of
  compute on recompute in overload on a bimodal workload, against 3.3% with the look-ahead.

## Later stages

**The look-ahead.** Memory grows between completions while slots and the step budget do not. For a
resident i, let x_i be its tokens now (prompt plus output so far) and M_i its maximum output less
its output so far, its remaining ceiling. The no-completion demand curve D(tau) = sum over
residents with M_i above tau of (x_i + tau) has a first crossing of capacity, t_ruin: t_ruin = 0 is the memory fit check, and
t_ruin infinite is the ceiling rule (every resident fits at its maximum output). Between them, a
statistic on survival to t_ruin decides. It sits behind one internal interface that reads an
endpoint's residents and the head and returns admit or hold with a risk value; it needs a survival
estimator per model and requires `maxModelLen`. It models only what happens before an overflow,
never which request the engine preempts, which keeps it independent of one engine's policy.

**Convergence with in-flight load.** In-flight load's `Requests` and default-mode `Tokens` are the
ledger's slots axis and pre-decode step axis. Once the ledger covers prefill/decode pools and
cross-replica aggregation, it can publish `InFlightLoadDataKey` as a projection of its account and
the standalone tracker can be deprecated.

## Open decisions

To settle with PR B's shadow data, before enforcement:
1. **Shared-prefix overstatement.** Refcount prefix blocks per endpoint (from block hashes the
   prefix producer would publish per request), discount by measured drift, or accept it.
2. **Reservation sizing.** The byte bound overstates by the tokenizer's bytes per token. Candidates:
   a per-model bytes-per-token ratio learned from leases at a conservative quantile; tokenizing
   before flow control.
3. **Race at placement.** Refuse with 429, or return the request to its queue at its original
   position.
4. **Strictness of the step check.** A prompt charged at the full step budget waits for decode
   tokens that vLLM's chunked prefill would schedule alongside it.
5. **Queued work the router did not send.** vLLM's waiting requests hold no KV blocks, so the memory
   scrape never shows them. Candidates: `num_requests_waiting_by_reason` with the preemption
   counter, or a detector of stalled streams.
6. **Shadow-mode bar.** Which metrics, at which values, justify enforcement. Shadow data does not
   yet link a would-hold or race to its request's outcome (latency, engine queue time,
   preemption); candidates are request latency split by gate verdict and a histogram of the time
   until some candidate would fit, with the preemption counter scraped through `customMetrics`.
7. **Enforce-mode contract.** `GateDecision` has no hold verdict or eligible set; round-robin
   fairness advances its cursor before the gate runs, so a hold would cost the flow its turn; a
   per-endpoint shortage never triggers reclamation; the gate ignores the band, so a low-priority
   request can reserve headroom priority holdback protects; candidates are not partitioned by
   role under disaggregation.
8. **Filter coverage.** Screeners run between the gate's `Locate` and scheduling, and the director
   locates again, so the filter may not see the gate's candidates; the unfiltered cause counts
   placements the filter never rechecked.
9. **Replica-aware state rule.** The additions since the scrape are keyed to the scrape's
   processing time, not per lease; with more than one sender the rule needs per-sender deltas.

Later: per-endpoint engine configuration for mixed hardware; cold start of the
survival estimator.

## Appendix: review notes (local; remove before publishing)

Mapping to the exploration's spec (`explorations/capacity-ledger/admission-paper/DESIGN.md` on the
capacity-poc worktree), the flow-control design records, and the plan
(`~/.claude/plans/glowing-discovering-fiddle.md`, round 3).

**Owner decisions (2026-09-29).** Data producer plugin in `dataproducer/capacityledger`; vector
model; names `EndpointGate` and `Reservation`; the gate's filter listed by the operator;
non-streaming prefill imputed from the measured prefill rate; the local branch rewritten into
PR A/B (backup branch `admission-v1-prerefactor`). Round 3: floor-plus-own-delta state rule;
first-sample prefill release kept when no prefill rate exists (step budget is transient);
`maxModelLen` required; multi-sequence requests booked per sequence. Backup of the round-2 state:
branch `admission-v1-r3`. Round 4 (outside review, 2026-09-29): reservation on one witness
endpoint (most headroom); decode rate learned at end of stream with a pool fallback, no growth
without any rate; stale reads as full with the global staleness threshold; fixes folded into the
existing commits (backup `admission-v1-r4`).

**Commits (local, unpushed).** 8694a92f8 (PR A), 9daa8bc8f + a9afb5a54 (PR B). A shared
`PrefixCacheMatchInfo.UncachedTokens` (a former PR 0) was dropped: in-flight load counts the
tier-weighted `MatchBlocks`, while the ledger needs the unweighted `CachedBlockCount`.
Presubmit passes all gates except verify-boilerplate on upstream `scripts/collect-sboms.sh`.

**Spec coverage.** DESIGN 3.1 vector rule: Summary, sections 2-3. 3.2 re-evaluation on every ledger
update: with no blocking in PR B it does not arise; PR C rechecks a pinned head every cycle. 3.3
estimator corrections: later stages. 4 contract: section 3. 5 guarantees G1a-G1c: PR C. 9 fork bugs:
decode tokens on the step axis; no fail-open (PR C); every completion credited (look-ahead);
posterior averaging (look-ahead); "engine waiting > unstarted" not built (open decision 5); PRIORITY
read not needed; one estimator per model (look-ahead); P/D ineligible; prefix netting on the step
axis only; replicas (PR C requires a single-writer declaration).

**Records needing amendment.** 0039: staleness is the EPP's global threshold and reads as full;
the state rule is floor plus additions since the scrape, not the larger of booked and scraped. 0042: its leader-election
selector is off in single-replica installs; enforcement needs an explicit single-writer
declaration. 0043: a reservation is charged to one witness endpoint (most headroom), not every eligible
endpoint, and sized from bytes; no fail-open
filter; the KV-axis discount from the prefix match dropped. The handoff's requeue conflicts with
admission-final (open decision 3).
