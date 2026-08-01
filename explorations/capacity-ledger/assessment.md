# Assessment: the capacity-ledger design against the codebase

Subject: `docs/flow-control-capacity-ledger.md` from PR #2061
(https://github.com/llm-d/llm-d-router/pull/2061, informational companion document there;
reviewed as fetched from the PR on 2026-07-31 — the file does not exist on this branch).
Evidence: the code at `9a8c999f` on `main`; file paths below are repo-relative. Related input
read but treated as low-confidence: `capacity-ledger-review.md` and `ledger-prototype-brief.md`
(copies in the kept-texts directory; sources.md `prior-review`); positions taken from them are
re-derived here or marked.

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
| Stochastic layer | Open: H1 rounds 1-2 complete — nonparametric machinery killed twice (K1, K1'); layer passes at 5-10 s under both void rules (K2'); drift verdict KD failed, so drift is a detected regime with a deterministic fallback; H2/H3 pending (finding 5) |
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
steps (recalled engine behavior; the standing table's engine-source verification item
covers it). That budget is a service rate; no engine-enforced inventory of outstanding
prefill tokens exists. A vector coordinate for prefill therefore has no unit for its limit, which the
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
not per-request output-length predictions. The arrival-time length-prediction line (S3,
TetriInfer, SSJF, and Fu et al.'s learning-to-rank — read, locators in sources.md
`s3-line`) does not reach this problem, on three verified grounds: its pointwise record
is fragile largely by its own reporting (S3's accuracy collapses off-distribution with
headline results measured on the easy set; TetriInfer's prediction-guided placement at
real accuracy exactly ties its prediction-free baseline; SSJF's accuracy holds only on a
length-filtered dataset); its one strong result, Fu et al., succeeds precisely by
abandoning the pointwise target for relative rank, conceding the target this design also
avoids; and all four evaluate ordering, placement, and batching only — the line is
silent on admission, so nothing in it tests, let alone solves, the aggregate problem.
The aggregate object is a sum over many leases whose individual errors partially cancel,
estimated from completed observations rather than request content. The precedents with
the same structure are effective-bandwidth admission and actuarial reserving.

Transfer is not automatic either. Whether age-conditioned hazard curves beat trivial
forecasters at this aggregate level, at these horizons, is an empirical question, and the
strongest trivial baseline is not "occupancy stays flat" but "every lease grows at its decode
rate and none completes", which requires no training data. The pre-registered experiment at
`explorations/capacity-ledger/h1-aggregate-forecast/` (kill criteria committed before results,
`eac07223`) decides:

- H1 (two rounds, complete 2026-07-31): hazard curves vs deterministic growth, constant
  hazard, parametric fits, and a two-component censored-EM mixture, on aggregate
  occupancy quantiles at 1-10 s horizons. Round 1 (RESULTS.md) killed the bucketed
  nonparametric machinery (K1) and passed the layer conditionally, but its harness had
  three defects the round-2 diagnostics exposed: warm-up training sets were
  length-truncated during pool fill, calibration residuals were drawn from the fill
  transient, and capacity pilots were sized inside it. Round 2 (RESULTS-2.md) fixed the
  harness and re-adjudicated everything under the original criteria. Outcome: K1' kills
  B3 again on unbiased data (+0.0% CI-gated median over the censored lognormal, and no
  width reduction near the 10% bar in its favorable regimes: R3 +2.8%, R4 -1.7%; 10x
  training data still leaves its best cell ~2 skill points over the best parametric
  alternative), the B4 mixture also fails to earn a slot (the R3/N=1000 prize it chased
  was a round-1 artifact that shrank from +22%/+49% to +3%/+4%), and K2' passes at its
  verdict horizons t in {5, 10} s under both void-cell rules (+15.4%/+19.8%),
  unconditionally superseding the round-1 conditional verdict at 5 s; the t = 2 s
  context cells also clear their 5% threshold (+6.8%). The round-1 "bias binds at
  scale" emergent finding is superseded: with settled training and calibration, fitted
  estimators are valid at N = 1000 and the parametric lognormal with conformal
  calibration (censoring-aware fitting is inert in the uncapped R2) sits essentially at
  the oracle ceiling in the hard heavy-tail regime (R2/N=1000: +6.4% vs oracle +6.1% at
  5 s). The new negative
  result is drift (KD): under an abrupt or ramped mix shift at N = 1000 the fitted layer
  either loses coverage entirely or, when a short rolling conformal window restores it,
  underperforms rolling-conformal persistence — calibrated forecasting does not survive
  drift, so drift must be detected (coverage canary) and met with the deterministic
  bound, not forecast through. Consequence for the ledger document: the stochastic
  layer's Directional label survives on the stationary evidence, its mechanism sentence
  reads "per-stratum censored parametric survival with conformal residual calibration on
  settled-pool residuals", and its operating envelope now carries three read-off rules:
  no value at t = 1 s, calibration and sizing only from settled pools, and a drift
  fallback path as a first-class design element.
- H2 (next, if H1 survives): shadow-mode calibration inside the EPP against replayed and live
  traffic; the forecast gates nothing until quantile coverage holds.
- H3 (last): closed-loop behavior when eviction censors the estimator's own training data
  (informative censoring). This is the genuine research risk in the design; it activates only
  when the forecast has admission authority and revocation volume is material.

Consequence for sequencing: the deterministic ledger does not depend on any of this. If H1
kills the layer, the ledger still corrects the token-mode under-count, gives exact eviction
sizing, and adds the dual admission check.

Per-request bookkeeping is not per-request prediction. A lease's footprint decomposes into
a measured part (prompt tokens, the cached-prefix discount, tokens generated so far), a
bounded part (the max_tokens ceiling, a request field rather than a forecast), and a
predicted part that exists only in aggregate: the expected view discounts the sum of
bookings by pooled survival probabilities, and no admission consumer reads one lease's
probability alone. A per-lease probability is calibrated on average and wrong for any
individual, which is acceptable wherever it is consumed summed. The two places a
probability would be consumed individually, victim selection and wait-vs-evict, are where
the per-lease Brier diagnostic and the censoring-alignment invariant sit.

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
evidence required before any lower setting is documented as supported. For setting the dial,
arXiv:2607.16892 (read; Proposition IV.2, p. 5) places the optimal admission-time
reservation at the critical fractile rho/(rho+1) of the (worst-case) length distribution,
where rho = cp/cw is the ratio of per-token preemption cost to per-token waste cost; the
proposition is the classical distributionally-robust newsvendor ratio applied to this
setting, so citations outside this branch should credit the newsvendor lineage, not the
paper's originality. The same structure applies here with revocation cost in place of
preemption cost, and gives the dial an operational meaning beyond "pick a percentile".

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

Existing tooling owns everything except the narrow phase-1 attribution instrument.
inference-perf and llm-d-inference-sim are llm-d projects, and donation of BLIS to the
project is under discussion (https://github.com/llm-d/llm-d/pull/2015), so capability gaps
the validation program hits are filed and fixed upstream in those repositories rather than
worked around in harness code; the validation investment then serves all three projects.

Repositories:

- inference-perf: https://github.com/kubernetes-sigs/inference-perf
- llm-d-inference-sim: https://github.com/llm-d/llm-d-inference-sim
- BLIS: https://github.com/inference-sim/inference-sim

- L0 (running): statistical falsification outside the EPP, Python,
  `explorations/capacity-ledger/h1-aggregate-forecast/`. The minimal fixed-step simulator is
  retained only because phase-1 attribution needs fixed decode rates and an analytically known
  generating distribution (the oracle gate). Workload parameter conventions stay mappable to
  inference-perf's synthetic workload configs; no custom trace-format or download code.
- L0.5: the same estimator ladder re-scored on BLIS (a discrete-event cluster simulator
  per its repository description; not yet read, `blis` in sources.md). BLIS owns the
  load-coupled decode-rate and arrivals-realism questions the L0 simulator deliberately
  excludes; its actual capability surface is confirmed at the read that precedes the
  re-score.
- L1: Go port of any surviving estimator, differential-tested against the L0 reference via
  fixed-seed golden files; replay harness and shadow-mode calibration inside the EPP.
- L2: end-to-end through a real EPP: kubernetes-sigs/inference-perf for load generation and
  trace replay (its dataset loaders are also the day-2 path for calibrating L0 regimes against
  Azure/ShareGPT-class traces), llm-d-inference-sim as the backend. Both tools'
  capability descriptions are machine-read standing (sources.md).

## Field inventory

The design draws on established results in several fields; naming them makes the remaining
imports visible.

In use: survival analysis (hazard, conditional survival, censoring taxonomy), queueing theory
(Little's law, backlog gates, M/G/infinity occupancy), operations research (newsvendor
critical fractile, overbooking, two-phase reservations), probabilistic forecast verification
(pinball loss, coverage, skill versus persistence/trend baselines), conformal prediction,
causal inference (informative censoring as selection bias; the victim-selection logging
invariant as ignorability), control theory (the overcommit governor), concentration of
measure, and distributed-systems accounting discipline.

Identified but not yet pulled:

- Credibility theory (Buhlmann): the optimal blend weight between a stratum's own experience
  and the pooled curve, replacing ad hoc pseudo-count shrinkage if B3 survives H1.
- Effective bandwidth (large deviations): prices each flow as a scalar such that additive
  admission meets a target overflow probability; a candidate endgame for tiered admission
  that would keep the hot-path arithmetic trivial.
- Extreme value theory: peaks-over-threshold as the rigorous form of hazard-curve tail
  extrapolation; ruin theory as the frame for guaranteed-tier violation probability.
- Bernstein-type concentration bounds: distribution-free upper bounds on aggregate occupancy,
  wider than calibrated quantiles but guaranteed, suited to the conservative end of the
  confidence dial.

## Standing

The table repeats the exploration's layers so the weakest can be found by scanning; rows
collapse or strengthen as work-table items resolve.

| Layer | Source | Standing |
|---|---|---|
| Scalar-gauge defects motivate the ledger | finding 1; dispatchCycle, both detectors | verified in code |
| Deterministic ledger grows from in-flight load accounting | finding 1; producer.go paths | verified in code |
| Residency stocks (block pool, max_num_seqs) are engine-enforced, abort frees on disconnect, prefill and decode share a per-iteration token budget | resource model's axis selection; finding 2's premise | recalled engine behavior; unverified against engine source this branch |
| Prefill is a rate; the backlog gate is the existing token-mode accounting | finding 2 | derived; accounting verified in code |
| Hold placement | finding 3 | open; candidates named, none validated |
| Absolute KV telemetry exists; slots telemetry does not | finding 6; extractor factories | verified in code |
| Termination cause and censored age observable to plugins | finding 4 | false today; small plumbing, upstream-early |
| Footprint = measurement + max_tokens bound; prediction only as an aggregate discount | finding 5 | derived from the definitions |
| Lease independence in the aggregate variance | H1 protocol | assumed; burst-correlated lengths untested |
| Decode rate known per lease | H1 simulator choice | assumed; noisy-rate sensitivity untested |
| Nonparametric hazard machinery earns its complexity | H1 K1, K1' | false at ~500 unbiased observations under stationarity; 10x data does not change it; the B4 mixture is also unearned |
| A stochastic forecaster beats trivial baselines at 5-10 s | H1 K2' | measured, both void-cell rules, corrected harness; t = 2 s also passes, t = 1 s does not; stationary synthetic regimes only |
| Calibration survives pool scale | RESULTS-2.md question A | yes, given transient discipline: training and calibration residuals from settled pools; the round-1 N=1000 collapse was harness artifact |
| Calibration survives drift | RESULTS-2.md KD | false, both scenarios: coverage lost outright, or validity restored by short rolling windows with negative skill vs rolling-conformal persistence; drift is a detected regime with a deterministic fallback |
| Informative-censoring loop is governable | H3 design (alignment invariant, canary, governor) | open; late-stage by construction |
| Prefix sharing breaks additivity in the conservative direction | finding 9 | direction derived; magnitude unmeasured until scrape reconciliation exists |
| Multi-EPP posture | finding 9 | open |
| Tier = one-sided confidence dial; fractile setting rule | finding 7; `dro2026` | proposed; setting rule read (Prop. IV.2, classical DR-newsvendor) |
| No published occupant of the combination (age-conditioned, content-blind, pool-scope, revocation-enforced) | related work below | verified for the read subset (dro2026, tie2026, s3-line); PLP/remlen/UniBoost machine-read |

## Related work positioning (2026-07-31)

Source states are tracked in [sources.md](sources.md); `dro2026`, `tie2026`, and the
`s3-line` papers are read with locators pinned there; PLP, remlen, and UniBoost remain
machine-read, so their specifics are provisional pending reads.

The nearest published work, and where this design differs:

- arXiv:2607.16892 (`dro2026`, read) is the closest system: joint per-class KV
  reservation, routing, and configuration at admission under length uncertainty, with a
  Wasserstein-DRO control plane re-solved on 5-10 minute ticks, evaluated in simulation
  on BurstGPT/Azure/ShareGPT traces. The reservation policy is the classical
  distributionally-robust newsvendor fractile rho/(rho+1) (Proposition IV.2; rho the
  preemption-to-waste cost ratio) — the paper's contribution is the coupling, not the
  fractile. Every decision conditions only on admission-time information; the online
  phase is O(1) table lookup. Two contrasts need precise wording after the read: it does
  bound expected occupancy (a Little's-law constraint with a fixed headroom factor), and
  its per-request reservation is a hard bound the data plane enforces by preemption — so
  the differentiators are conditioning on decode progress, forecasting realized occupancy
  online over second-scale horizons, and choosing revocation victims from lease state,
  not the existence of occupancy control or of an enforced bound. Its headline negative
  result ("no single fixed-quantile heuristic is optimal across cost regimes",
  Sec. V-B) supports the confidence-dial framing in finding 7, and its authors name
  online adaptation as future work.
- arXiv:2604.00499 (TIE, `tie2026`, read) argues output length should be modeled as a
  distribution (log-t; their Theorem 3.2 derives power-law tails structurally) rather
  than a point, engine-side for queue ordering, and names distributional updates during
  decoding as future work (App. E: "dynamically update the distribution as tokens are
  generated"). The precise gap matters: point-estimate re-prediction during decode
  already exists (TRAIL, ELIS), so what is open is exactly the distributional,
  age-conditioned form this design proposes — and TIE's predictor needs ~20 resampled
  generations per training prompt, where a survival estimator trains on observational
  production completions.
- arXiv:2602.11812 (PLP) and ELIS re-predict remaining length during decode, per-request,
  from model activations or partial output, for engine-side scheduling. These condition on
  progress but are content/activation-based per-request predictors, the frame whose fragility
  motivated this design's population-level approach.
- arXiv:2607.05316 finds LLMs linearly encode their own remaining output length in the
  residual stream. If engines ever export that signal, it slots into the ledger document's
  stage-5 engine co-design as a per-lease survival input; it does not change the pool-scope
  architecture.
- UniBoost (ICML 2026) reports the same prompt varying more than 2x in output length across
  sampling runs and abandons per-request prediction for statistical priority signals,
  consistent with the population-level framing here.

Unclaimed in this set: content-blind, age-conditioned survival estimation driving pool-scope
(gateway) admission with revocation as the enforcement mechanism, and the informative-censoring
feedback loop that revocation introduces into the estimator. For the read subset this is
checked against the texts rather than recalled: none of the six read papers is content-blind or
age-conditioned (every predictor reads the prompt; TetriInfer's remaining-token
arithmetic decrements a frozen arrival estimate rather than re-conditioning); the only
pool-scope system among them admits everything, and the only revoking one revokes as
per-sequence misprediction recovery, not as enforcement of an admission bound. PLP,
remlen, and UniBoost remain machine-read. This remains a targeted sweep, not a
systematic review.
