# Assessment: the capacity-ledger design against the codebase

Subject: `docs/flow-control-capacity-ledger.md` (PR #2061, informational companion document).
Evidence: the code at `9a8c999f` on `main`; file paths below are repo-relative. Related input
read but treated as low-confidence: `capacity-ledger-review.md` and `ledger-prototype-brief.md`
(repo root of the main checkout); positions taken from them are re-derived here or marked.

Scope: this memo evaluates feasibility and difficulty per section of the ledger document,
identifies the design problems a prototype must resolve, and records the validation program.
It lives on the `capacity-ledger` fork branch and feeds later revisions of the document; it is
not itself a proposal.

## Verdict summary

| Ledger document section | Verdict |
|---|---|
| Motivation (scalar gauge defects) | Agree; defects confirmed in code (finding 1) |
| Resource model: residency axes, lifecycle split | Agree; half-implemented today (finding 1) |
| Resource model: prefill axis (labeled Directional) | Change requested: rate gate beside the ledger, not a vector coordinate (finding 2) |
| Types and Translator seam | Agree; slots axis lacks telemetry (finding 6) |
| Hold-then-lease protocol | Change requested: hold placement conflicts with admission-time information ordering (finding 3) |
| Endpoint and pool ledgers, dual admission check | Agree |
| Reconciliation: event truth-up over filtering | Agree; two telemetry gaps block the estimation track (finding 4) |
| Stochastic layer | Open: value unproven; pre-registered falsification underway (finding 5) |
| Tiered admission | Open: generalize to confidence-level knob (finding 7) |
| Migration path (stage 2 grows from in-flight load accounting) | Agree; confirmed viable (finding 1) |
| Multi-EPP posture | Open: absent from the document; needs a stated position (finding 9) |

## Finding 1: the deterministic ledger is closer to done than the document implies

Severity: none (positive finding; reduces scope estimates).

`InFlightLoadProducer` (`pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload/`
`producer.go`) already implements the accounting skeleton the ledger needs:

- Per-endpoint counters, commit at PreRequest: `requestTracker` and `tokenTracker` are maps of
  endpoint to `*atomic.Int64`, incremented once per scheduled request (producer.go:337-391).
- Prefill release at first token: `releaseTokensEarly` on `StartOfStream` (producer.go:407-468),
  which is the transient/persistent lifecycle split in degenerate form.
- Exactly-once release with leak protection: `addedTokensEntry` swaps its value to zero before
  decrementing, so early release, end-of-stream release, and the TTL janitor can race safely,
  and each entry captures pointers to the exact counter instances it incremented so release
  survives endpoint flap (producer.go:136-180).
- Prefix-cache-discounted footprints: `uncachedInputTokens` subtracts matched blocks using
  `PrefixCacheMatchInfo` (producer.go:528-557).

The gaps between this and the document's `EndpointLedger` are: no lease record (the counters
aggregate; individual claims are only reconstructable from `PluginState` entries), no
reclaiming state, no hold phase, and a single token dimension instead of a footprint vector.
Each is an extension of an existing mechanism rather than new machinery. The document's claim
that stage 2 "grows out of" this component is confirmed.

The motivation section's defect list is also confirmed in code: the dispatch gate is a single
scalar comparison `saturation >= ceilings[i]` recomputed per 1ms cycle with strict head-of-line
blocking (`pkg/epp/flowcontrol/controller/internal/processor.go:370-424`), and both saturation
detectors reduce heterogeneous per-endpoint state to one pool ratio
(`pkg/epp/framework/plugins/flowcontrol/saturationdetector/{concurrency,utilization}/`).

## Finding 2: prefill belongs beside the ledger as a backlog gate, not inside the vector

Severity: medium (blocks the `Footprint` type definition). Concurs with the prior review's
finding 1; re-derived independently.

The engine enforces a per-iteration token budget shared between prefill chunks and decode
steps. That budget is a service rate; no engine-enforced inventory of outstanding prefill
tokens exists. A vector coordinate for prefill therefore has no unit for its limit, which the
document itself flags ("limit's semantics and units undefined").

The resolution is already running in production configuration: token-mode accounting
(incremented at dispatch, released at first token) is a per-endpoint backlog counter of
admitted-but-not-yet-prefilled work. Gate it directly: a backlog bound of
`Q_p_max = mu_p * TTFT_budget`, with `mu_p` (uncached prompt tokens per second per endpoint)
measured from telemetry, turns the existing `maxTokenConcurrency` constant into a quantity an
operator can state in seconds. `Footprint` reduces to `{KVTokens, Slots}`.

Recommendation: adopt the rate-gate formulation; relabel the prefill treatment from
Directional to Proposed with the backlog gate as its mechanism.

## Finding 3: hold placement conflicts with admission-time information ordering

Severity: high (the one unresolved architectural problem in the deterministic design).

The document takes holds "before scheduling," in `Footprint` units. The code's ordering makes
that impossible at the flow-control gate: tokenization and prefix matching run in data
producers after `Admit` returns (`pkg/epp/requestcontrol/director.go:309`), and endpoint choice
happens later still. At the dispatch gate, the only size information is request bytes
(`hasCapacity`, processor.go:336-357). A footprint-denominated hold cannot exist there.

Candidate resolutions, in preference order:

- (a) Director-side hold. The gate consumes a ledger view (Available per band) as its
  saturation signal; the hold itself is taken after tokenization and before `Schedule`, where
  prompt tokens and `MaxOutputTokens` are known. The hold still closes the admit-to-commit
  race, which is its purpose. Cost: a request can pass the gate and then fail the hold; that
  needs an outcome path. Requeue does not exist (dispatch finalizes the item,
  `FinalizeWithOutcome(QueueOutcomeDispatched)`), so the initial answer is rejection with a
  retryable status, counted and alarmed.
- (b) Two-stage hold. A byte-denominated provisional hold at the gate (bytes-to-token estimate,
  the tokenizer's `estimateBackend` already implements one), upgraded to footprint units at
  tokenization. Closes the gate-to-tokenize window that (a) leaves open, at the cost of a
  second reservation state.
- (c) Tokenize before enqueue. Makes footprint holds possible at the gate but moves tokenizer
  cost onto the pre-queue path of every request, including ones that will be rejected, and
  changes the `EnqueueAndWait` contract.

Recommendation: prototype (a) in the replay harness; measure the width of the gate-to-hold
window under burst before deciding whether (b) is needed. Do not start with (c).

## Finding 4: two telemetry gaps block the estimation track and cannot be backfilled

Severity: high for the stochastic-layer program, low implementation cost. These are the items
worth upstreaming ahead of everything else, because the estimator's training data accrues only
after they land.

- Termination cause is not observable at the plugin layer. The stream-abort cleanup path forces
  `HandleResponseBody` with `EndOfStream=true` and no cause (`pkg/epp/handlers/server.go`,
  defer at ~359-366), so a plugin cannot distinguish natural completion from abort. A cause
  taxonomy exists for pre-dispatch outcomes (`types.QueueOutcome`) and for forced eviction
  (`errcommon.RequestDroppedReason`), but not for the post-dispatch EOS/abort/disconnect split
  that survival estimation needs as its censoring label.
- Censored age is not recorded. Aborted streams carry no usage block, and no counter tracks
  tokens generated so far, so an aborted request's age at censoring is unknown. A per-request
  streamed-token counter (chunk-based approximation is acceptable; state the approximation)
  closes this.

Recommendation: define the lease record now (provenance at admission: prompt tokens, cached
tokens at scheduling, `MaxOutputTokens`, flow, priority, model; termination: cause enum, age at
termination) and upstream the two plumbing changes as a small, self-contained PR once the
prototype validates the record shape.

## Finding 5: the stochastic layer's value is unproven, and the test is cheap

Severity: high if built without validation; contained otherwise.

The layer's consumers need aggregate forecasts (occupancy of the pool at a horizon of seconds),
not per-request output-length predictions. The serving literature's negative results
(arrival-time length predictors of the S3 / TetriInfer / SSJF line) concern the per-request
problem and do not transfer: the aggregate object is a sum over many leases whose individual
errors partially cancel, estimated from completed observations rather than request content.
The precedents with the same structure are effective-bandwidth admission and actuarial
reserving.

Transfer is not automatic either. Whether age-conditioned hazard curves beat trivial
forecasters at this aggregate level, at these horizons, is an empirical question, and the
strongest trivial baseline is not "occupancy stays flat" but "every lease grows at its decode
rate and none completes", which requires no training data. The pre-registered experiment at
`explorations/capacity-ledger/h1-aggregate-forecast/` (kill criteria committed before results,
`eac07223`) decides:

- H1 (running): hazard curves vs deterministic growth, constant hazard, and parametric fits,
  on aggregate occupancy quantiles at 1-10 s horizons. Kill criteria K1/K2 in RESULTS.md.
- H2 (next, if H1 survives): shadow-mode calibration inside the EPP against replayed and live
  traffic; the forecast gates nothing until quantile coverage holds.
- H3 (last): closed-loop behavior when eviction censors the estimator's own training data
  (informative censoring). This is the genuine research risk in the design; it activates only
  when the forecast has admission authority and revocation volume is material.

Consequence for sequencing: the deterministic ledger does not depend on any of this. If H1
kills the layer, the ledger still corrects the token-mode under-count, gives exact eviction
sizing, and adds the dual admission check.

## Finding 6: the slots axis has no telemetry; the KV axis is already covered

Severity: low.

Absolute KV capacity is scraped today: `vllm:cache_config_info` populates
`Metrics.CacheNumBlocks` and `Metrics.CacheBlockSize` per endpoint, with equivalent mappings
for SGLang and TRT-LLM (`pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go`).
`max_num_seqs` is scraped nowhere in the repo; the only existing hook is a `customMetrics`
scalar attribute. `Metrics.KvCacheMaxTokenCapacity` exists but no code writes it.

Recommendation: build the prototype on the KV axis; carry slots as a configured limit or defer
the axis until a deployment shows slots binding before KV. Do not build scraping for it first.

## Finding 7: generalize tiers to a confidence-level knob

Severity: medium (shapes the admission API).

The document already frames tiers as "admission against different confidence levels of the
same ledger" but then fixes guaranteed = pessimistic bound. Keep the general form: the operator
tunable is a one-sided upper confidence bound on future occupancy, and the ceiling bound is the
100% end of that dial. Two consequences:

- If an operator dials guaranteed traffic below 100%, quantile calibration becomes a
  correctness input rather than an efficiency input. H1/H2 coverage measurements are the
  evidence for how far the dial can safely move; the compound event (quantile miss while the
  sheddable pool is exhausted) must be bounded under dependence, since both are load-driven.
- The default posture stays ceiling-bound: prediction failure then costs multiplexing
  efficiency and sheddable churn, never the guaranteed contract. The engine's
  preemption-with-recompute remains the residual backstop.

Recommendation: the document should name this dial, its default (100%), and the calibration
evidence required before any lower setting is documented as supported.

## Finding 8: positions on the prior review (`capacity-ledger-review.md`)

Recorded because that document is an input with acknowledged context limitations.

Confirmed here: prefill-as-rate (finding 2, re-derived from the code); the corrected forecast
object (survivor growth included; adopted in the H1 spec); the three-unit discipline (token age
for estimation, wall-clock for consumers, KV tokens for arithmetic); the containment argument
(adopted as the default posture in finding 7).

Not adopted as stated:

- The canary construction (never-revoked sheddable subset as an approximately unbiased sample)
  assumes which requests land sheddable is independent of content within a stratum. Sheddable
  admission concentrates in bursts, and burst traffic differing in length is the expected case
  for agentic workloads, not an edge case. Use the canary as a divergence alarm only.
- The estimator specification (censoring-aware discrete hazard, strata shrinkage, drift
  resets) is premature before H1 establishes that hazard curves beat simpler forecasters at
  all. The specification is banked in the H1 code as estimator B3.

## Finding 9: opens carried forward

Recorded as opens; none block the prototype.

- Multi-EPP: independent replica ledgers double-book pool capacity. Endpoint partitioning (one
  writer per endpoint ledger) is the least machinery with the strongest invariant; the
  document currently says nothing.
- Prefix sharing breaks footprint additivity: two leases sharing a cached prefix book it
  twice. Direction of error is conservative (under-admission). The per-scrape reconciliation
  already compares booked footprints against scraped occupancy, so the sharing gap is
  measurable before it is corrected; instrument it and defer the fix.
- Holds are bounded only by the scheduling window, which stretches under the bursts that
  create contention; an aggregate held-footprint cap with reject-beyond keeps holds from
  becoming an admission bypass.

## Validation layers

- L0 (running): statistical falsification outside the EPP, Python,
  `explorations/capacity-ledger/h1-aggregate-forecast/`. Ground truth known, decode rates
  controlled, full grid in minutes.
- L1: Go port of any surviving estimator, differential-tested against the L0 reference via
  fixed-seed golden files; replay harness and shadow-mode calibration inside the EPP.
- L2: end-to-end through a real EPP with existing tooling: inference-perf for load generation
  and trace replay, llm-d-inference-sim or the IBM BLIS simulator as backends (links to be
  pinned when supplied). BLIS models batching-coupled decode rates more faithfully than the L0
  sensitivity, so it is the intermediate check between L1 and live traffic.

## Related work positioning (unverified)

Arrival-time output-length prediction for scheduling is published and weak-to-fragile (S3,
TetriInfer, SSJF; the learning-to-rank result suggests pointwise prediction was the wrong
frame). Hazard-conditioned aggregate occupancy forecasting for pool-scope admission with
revocation as enforcement has no publication this memo's author can name, but that is a memory
claim, not a search result. A literature sweep is owed before any external claim is made; until
then the positioning is a working assumption.
