# Flow Control: The Capacity Ledger

Draft status: local rewrite of the capacity-ledger document from PR #2061 (snapshot at
[inputs/flow-control-capacity-ledger-pr2061.md](inputs/flow-control-capacity-ledger-pr2061.md)),
revised against the code assessment ([assessment.md](assessment.md)), the H1
experiment verdicts (rounds 1-3,
[h1-aggregate-forecast/](h1-aggregate-forecast/README.md)), and the closed-form
results in [theory.md](theory.md). It lives on the
`capacity-ledger` exploration branch; citations into `explorations/` resolve there and
need public homes before this text goes into an upstream PR. Code references are to
`main` at `9a8c999f`.

This document records direction, not commitment. Each major section carries a
confidence label:

- **Proposed**: settled shape; implementation may begin against it.
- **Directional**: the argument is made and the seam is fixed, but the mechanism behind
  the seam is expected to evolve.
- **Open**: known problem, no chosen answer.

Related: `docs/flow-control-eviction.md`. The v1 eviction design is the scalar
projection of this design; its controller survives this redesign with type upgrades
only.

## Summary

Flow control today reasons about pool capacity through a single delayed scalar (the
saturation gauge). This document proposes replacing that with a **capacity ledger**:
every request is modeled as a resource **footprint** over the two engine-enforced
residency stocks (KV tokens and sequence slots), held as a **lease** against
per-endpoint ledgers that roll up to a pool ledger. Admission, holdback, and eviction
become bookkeeping operations against the ledger rather than threshold comparisons
against a gauge. Prefill compute, which is a rate rather than a stock, is governed by a
backlog gate beside the ledger, not by a vector coordinate.

The engine's own scheduler already runs this accounting model inside each replica
(recalled engine behavior; the resource model section carries the verification
obligation). This design raises it to pool scope, the one place the engine cannot.

The design also carries the QoS story: tiers are admission against different confidence
levels of the same ledger. The operator tunable is a one-sided confidence level on
future occupancy; guaranteed traffic reserves at the 100% end (the deterministic
bound), sheddable traffic is statistically multiplexed against calibrated quantiles,
and revocation (eviction) is the enforcement mechanism that makes the overcommit safe.

## Motivation: what a scalar gauge cannot do

*(Proposed.)*

Recurring capacity-management defects in the flow control layer trace to the same root:
heterogeneous, multi-dimensional, lifecycle-varying resource claims are collapsed into
one delayed dimensionless number. Each defect below is observable in the code today.

- **Eviction sizing** (#1119): "how many requests must be evicted" is unanswerable in
  gauge units. Requests have unrelated footprints, and the gauge reflects an eviction
  only after abort, GC, and a scrape. The v1 eviction design works around this with a
  mean-footprint estimate and pending-reclaim debits, a deliberately crude scalar
  shadow of this design.
- **Holdback stranding**: ceilings reserve a fraction of a gauge, not capacity for a
  class of footprints; the reserve cannot be sized to expected burst demand.
- **Token-mode under-count**: in-flight token accounting that releases at first token
  conflates the prefill-compute claim (which does end at TTFT) with the KV-residency
  claim (which does not), admitting past capacity during decode-heavy load. The bug is
  a lifecycle distinction the scalar cannot express.
- **Admission/scheduling interference**: gauges built from means mask per-endpoint
  skew. The dispatch gate is a single scalar comparison `saturation >= ceilings[i]`
  recomputed per cycle with strict head-of-line blocking
  (`pkg/epp/flowcontrol/controller/internal/processor.go:370-424`), and both saturation
  detectors reduce heterogeneous per-endpoint state to one pool ratio
  (`pkg/epp/framework/plugins/flowcontrol/saturationdetector/`).

The remedy is to account in the units the hardware enforces.

## The resource model

*(Proposed: the residency axes, the lifecycle split, and the prefill backlog gate.
Open: the shared-resource extension.)*

### The two residency axes

Continuous-batching inference holds two per-request stocks that the engine enforces
with hard limits, each with a distinct saturation mode:

| Axis | Physical bottleneck | Saturates as | Limit |
|---|---|---|---|
| `Residency.KVTokens` | VRAM capacity: KV history storage (PagedAttention blocks) | OOM / swap thrashing | block pool size |
| `Residency.Slots` | HBM bandwidth: decode streams weights per active sequence per token | TPOT degradation; scheduler queue saturation | `max_num_seqs` |

Both are claimed at admission and released at end of stream. Absolute KV capacity is
already scraped per endpoint (`vllm:cache_config_info` populates
`Metrics.CacheNumBlocks` and `Metrics.CacheBlockSize`, with equivalent mappings for
SGLang and TRT-LLM in
`pkg/epp/framework/plugins/datalayer/extractor/metrics/factories.go`). `max_num_seqs`
is scraped nowhere in the repository; the slots axis therefore starts as a configured
limit, and scraping for it is deferred until a deployment shows slots binding before
KV.

The engine-side premises here — that the block pool and `max_num_seqs` are
engine-enforced stocks, that abort frees on disconnect, and that prefill and decode
share a per-iteration token budget — are recalled engine behavior, not yet verified
against engine source; that verification precedes any EPP wiring.

### Prefill is a rate, not a stock

Prefill compute is the third bottleneck, and it is different in kind: the engine
enforces a per-iteration token budget shared between prefill chunks and decode steps.
That budget is a service rate. No engine-enforced inventory of outstanding prefill
tokens exists, so a vector coordinate for prefill has no unit for its limit.

The resolution is already running in production configuration: token-mode accounting —
incremented at dispatch, released at first token — is a per-endpoint backlog counter of
admitted-but-not-yet-prefilled work. Gate it directly: bound the backlog at

```
Q_p_max = mu_p * TTFT_budget
```

where `mu_p` is the endpoint's measured uncached-prompt-token throughput (tokens/s) and
`TTFT_budget` is the operator's time-to-first-token target in seconds. This turns the
existing `maxTokenConcurrency` constant into a quantity an operator can state in
seconds. The lifecycle split survives — the transient claim releases at TTFT, the
persistent claim at end of stream — but the transient side is enforced by the backlog
gate beside the ledger, and `Footprint` reduces to residency.

### Types

Two representations, one translation seam:

```go
// Prediction carries the per-request quantities the translation is computed from.
type Prediction struct {
    PromptTokens int64 // ISL, known at admission
    OutputTokens int64 // predicted OSL (see Stochastic layer)
    CachedTokens int64 // prefix-cache hit, known at scheduling; zero at admission
    Branching    int64 // decode width (best_of, beam, n)
    BlockSize    int64 // engine KV block size
}

// Footprint is the hardware-agnostic residency claim of one request. Pool-level
// admission reasons exclusively in these units.
type Footprint struct {
    KVTokens int64 // (prompt - cached prefix) + predicted output * branching
    Slots    int64 // concurrent sequences (branching factor; maps to engine max_num_seqs)
}

// EngineFootprint is the engine-specific physical claim on one replica:
// block-granular, fragmentation- and copy-on-write-aware. Endpoint-level commits use
// these units.
type EngineFootprint struct {
    KVBlocks int64
    Slots    int64
}

type Translator interface {
    ToFootprint(p Prediction) Footprint
    ToEngineFootprint(p Prediction) EngineFootprint
}
```

Design rules:

- **The Translator is a calibrated estimator.** Block-exact math (copy-on-write
  duplication, fragmentation, prefix-boundary rounding) couples the router to
  engine-version internals with no contract. Estimates carry error acknowledged by the
  reconciliation layer; the long-term fix is an engine API reporting actual per-request
  block usage (see Engine co-design).
- **Underflow is an error.** Footprints support coordinate-wise `Add`/`Sub`; underflow
  is surfaced as ledger corruption rather than silently clamped. Correctness comes from
  zero-sum discipline (a lease releases exactly what it committed).
- **Shared resources stay out of the vector** *(Open)*. LoRA adapter slots are
  set-union-scoped (the first request pays, co-tenants ride free), which breaks
  per-request additivity. They require a reference-counted side ledger, not a third
  coordinate.
- **Prefix sharing breaks footprint additivity** *(Open)*. Two leases sharing a cached
  prefix book it twice. The direction of error is conservative (under-admission), and
  the per-scrape reconciliation measures the gap before any correction is designed;
  instrument first, fix later.

## The ledger architecture

*(Proposed: the protocol, the ledgers, the dual admission check. Open: where the hold
is taken.)*

A booking's output side is the `MaxOutputTokens` ceiling and everything else in it is
measured, so estimation error moves the deterministic ledger in one direction:
under-admission. Eviction sizing is unaffected — revoking a lease frees its current,
measured footprint, not its booking — and so are the race closure and the fit check.
What loose ceilings cost is utilization (typical outputs run well below their
ceilings), and that cost is the yield the stochastic layer exists to recover.

### Hold, then lease

Admission is a two-phase reservation protocol:

```
        TryAcquireHold(footprint)          Commit(endpoint, engineFootprint)
request -------------------------> HOLD ----------------------------------> LEASE
                                    |  TTL expiry / cancel                    |
                                    v                                        |  Release (EOS)     natural
                                 dropped                                     |  Revoke (eviction)  forced
                                                                              v
                                                             reclaiming --> reclaimed
```

- A **hold** is a tentative, TTL-bounded, endpoint-unbound reservation in `Footprint`
  units. Holds exist only for the scheduling window (from the admission decision to
  commit or cancellation), not for queued requests, so the holds table stays small and
  a deep queue cannot zero out available capacity. Holds close the admit-to-commit race
  at pool granularity. The race is not closed at endpoint granularity (two holds can
  each pass the fit check against the same lone endpoint); that residual is caught at
  commit time. TTL expiry cancels the admission and the request is rejected to the
  client: the TTL reclaims capacity from scheduling stalls rather than acting as a
  queueing mechanism. An aggregate held-footprint cap with reject-beyond keeps holds
  from becoming an admission bypass during the bursts that stretch scheduling windows
  *(Open: cap sizing)*.
- A **lease** is the committed claim, bound to an endpoint, in `EngineFootprint` units.
  The *escalation guard* enforces `commit <= hold` per dimension, compared in logical
  units: the committed footprint's blocks are converted at the endpoint's block size,
  rounding the committed side up, so rounding can never excuse an escalation.
  Scheduling may not discover a larger footprint than admission approved.
- **Release** at end of stream frees residency; the prefill backlog counter releases at
  TTFT as today. **Revocation** (eviction) is a forced release: the same ledger
  operation with a different initiator, entering the same reclaiming/reclaimed
  accounting (released by the EPP, not yet acknowledged freed by the engine).
- Pool admission requires both an **aggregate check** (pool-wide available capacity
  covers the footprint) and a **fit check** (at least one healthy endpoint can hold
  it). Aggregate room with no single endpoint able to fit the request is not admissible
  capacity.

### Where the hold is taken

*(Open; this is the one unresolved architectural problem in the deterministic design.)*

A footprint-denominated hold cannot exist at the flow-control gate as the code stands:
tokenization and prefix matching run in data producers after `Admit` returns
(`pkg/epp/requestcontrol/director.go:309`), and endpoint choice happens later still. At
the dispatch gate, the only size information is request bytes
(`hasCapacity`, `processor.go:336-357`). Candidate resolutions, in preference order:

- **(a) Director-side hold.** The gate consumes a ledger view (available capacity per
  band) as its saturation signal; the hold is taken after tokenization and before
  `Schedule`, where prompt tokens and `MaxOutputTokens` are known. The hold still
  closes the admit-to-commit race, which is its purpose. Cost: a request can pass the
  gate and then fail the hold, and requeue does not exist (dispatch finalizes the
  item), so the initial answer is rejection with a retryable status, counted and
  alarmed.
- **(b) Two-stage hold.** A byte-denominated provisional hold at the gate (the
  tokenizer's `estimateBackend` already implements a bytes-to-token estimate), upgraded
  to footprint units at tokenization. Closes the gate-to-tokenize window that (a)
  leaves open, at the cost of a second reservation state.
- **(c) Tokenize before enqueue.** Makes footprint holds possible at the gate but moves
  tokenizer cost onto the pre-queue path of every request, including ones that will be
  rejected, and changes the `EnqueueAndWait` contract.

The plan is to prototype (a) in a replay harness and measure the width of the
gate-to-hold window under burst before deciding whether (b) is needed. (c) is not a
starting point.

### Endpoint and pool ledgers

- `EndpointLedger`: the deterministic map of committed leases on one replica, plus the
  reclaiming-state accounting for released-but-unacknowledged capacity. Hot-path reads
  are lock-free snapshots.
- `PoolLedger`: registration/draining of endpoints, the holds table, and the roll-up:
  `Available = sum(limits) - sum(committed) - sum(holds) - sum(reclaiming)`.
- Every consumer that today reads the saturation gauge becomes a view over the ledger:
  the dispatch gate asks "does the head request's hold fit"; holdback reserves
  footprint-denominated headroom per tier; the eviction controller computes a
  per-dimension deficit from a hold-fit failure. The `SaturationDetector` abstraction
  survives as a derived, backwards-compatible view (saturation approximately equals the
  max over dimensions of used/limit), not as the source of truth.

### What this does to the eviction controller

Nothing structural; that invariance is the design goal (see
`docs/flow-control-eviction.md`). Three type upgrades:

| v1 (scalar) | Ledger world |
|---|---|
| `deficit = saturation - ceiling` | per-dimension deficit: `blocked hold's Footprint - Available` |
| `credit = saturation / leases` (mean estimate) | exact per-lease `EngineFootprint` from the ledger |
| pending-reclaim debits (controller-local) | the ledger's reclaiming-state accounting |

Victim selection graduates from heap-order to subset selection: choose the
minimum-waste set of revocable leases whose footprints cover the deficit vector
(`VictimSelector(candidates, deficit)`).

## Reconciliation: two sources of truth

*(Directional. The seam and the argument are fixed; the estimator behind the seam is
not.)*

The ledger is a predicted view (footprints are estimates over predicted output
lengths); scraped engine telemetry is a delayed view of reality. Something must close
the loop. The design principle: correct the ledger at the events that reveal actual
values, and model only the one quantity no event reveals (time to release).

Filtering approaches (Kalman-style or observer-based bias estimation) are rejected.
They earn their complexity when a system has continuous hidden dynamics observable only
indirectly. Here, capacity changes in discrete jumps at knowable events (commit, TTFT,
EOS, abort), and the dominant reconciliation errors are systematic (output-length
over-prediction, prefix-cache discounts, translation drift) and correctable at those
events. A filter also faces an identification problem: it cannot distinguish
"predictions run 20% high" from "three requests completed and the scrape has not
landed," and the guard machinery that distinction demands grows without bound.

Event truth-up instead:

- **At scheduling**: actual cached-prefix tokens are known; replace the zero-cache-hit
  pessimistic KV estimate.
- **At EOS**: actual output length is known; the lease releases its committed footprint
  exactly (zero-sum), and the prediction error feeds the predictor, not the ledger.
- **At abort/eviction**: the revocation event marks the lease reclaiming; engine
  acknowledgment (completion/abort counters, block counts) retires it. *(Open: engines
  count aborts and natural completions in different metrics; the acknowledgment channel
  must include both or reclaiming entries stall.)*
- **Per scrape**: telemetry validates the roll-up and catches drift (translation error,
  missed events, the prefix-sharing gap); persistent per-endpoint discrepancy is
  surfaced as calibration error on the Translator rather than silently absorbed.
- **At EPP restart**: ledger state is process-local and the deployment model has no
  in-place upgrades, so a new process has no lease records for work admitted by its
  predecessor. Scrape reconciliation carries that work as observed, unattributed
  occupancy until it drains — the error direction is conservative (it suppresses
  admission rather than enabling over-admission), and per-lease operations (eviction
  sizing, victim selection) are simply unavailable for unattributed work. *(Open: the
  rebuild sequence and how long unattributed occupancy typically persists.)*

### Telemetry prerequisites for the estimation track

Two plumbing gaps corrupt the stochastic layer's training stream in every process
lifetime, and the exported observation record built on that stream — the only
cross-restart history the design uses — cannot be backfilled, so they are worth
upstreaming ahead of everything else:

- **Termination cause is not observable at the plugin layer.** The stream-abort cleanup
  path forces `HandleResponseBody` with `EndOfStream=true` and no cause
  (`pkg/epp/handlers/server.go`), so a plugin cannot distinguish natural completion
  from abort. Survival estimation needs that split as its censoring label.
- **Censored age is not recorded.** Aborted streams carry no usage block and no counter
  tracks tokens generated so far, so an aborted request's age at censoring is unknown.
  A per-request streamed-token counter closes this (chunk-based approximation is
  acceptable if stated).

The lease record should be defined now — provenance at admission (prompt tokens, cached
tokens at scheduling, `MaxOutputTokens`, flow, priority, model) and termination (cause
enum, age at termination) — and the two plumbing changes upstreamed as one small PR
once a prototype validates the record shape.

## Stochastic layer: calibrated release forecasting

*(Directional. The mechanism is measured at simulation level; shadow-mode validation in
the EPP is the next gate. Citations in this section are to the pre-registered
experiment records under `explorations/capacity-ledger/h1-aggregate-forecast/`.)*

After truth-up, one quantity remains uncertain: when each active lease will release.
Output length is a random variable; everything the ledger wants to know about the
future is a function of its distribution.

### The model in plain terms

Each active request is a weighted coin. Given that it has already produced n tokens,
its stratum's survival curve gives the probability it is still running at the horizon
(age matters: a request at token 3000 has different odds than one at token 30). If
still running it contributes its current size plus the horizon's decode growth; if
finished, zero. Summing a thousand such coins cancels most individual error and yields
a distribution for total occupancy; admission reads its 95th percentile. The conformal
layer then corrects that bound against reality: it tracks how past bounds compared to
what actually happened and shifts today's bound by the observed error quantiles, so the
95% holds even when the survival curve is wrong — provided the near future resembles
the recent past. That proviso is the entire drift problem: when the workload
stops resembling its recent past, no estimator can be calibrated to data that does not
exist yet, so the layer detects the break and falls back rather than forecasting
through it. Everything measured in the validation program reduces to: which survival
curve (a two-parameter censored lognormal; richer forms do not pay), which residual
window, and how fast detection and re-entry work.

### The framing

Let `L` be a request's output length in tokens. Define, per stratum (flow, model, or
workload class): survival `S(n) = P(L > n)` and, for a lease at decode age `n` with
decode rate `r`, the conditional survival over a horizon `t` of
`P(L > n + r*t | L > n)`. The pool-level forecast object is occupancy at horizon `t`:
each active lease contributes its grown footprint with that probability, and the
admission consumer reads an upper quantile of the sum. This is the quantity a scalar
gauge cannot provide: not how full the pool is, but how fast it will empty.

### What the evidence supports

The layer was built against a falsification program (kill criteria committed before
results; RESULTS.md, RESULTS-2.md, RESULTS-3.md), on synthetic regimes spanning chat,
heavy-tail, bimodal-mixture, and truncation workloads at pools of 100-1000 leases.
Findings the design now rests on:

- **The mechanism is a per-stratum censored parametric survival fit (lognormal body,
  cap-aware likelihood) with conformal residual calibration on settled-pool residuals,
  refit continuously on trailing completions.** Both more elaborate estimators failed
  their pre-registered bars twice: bucketed nonparametric hazard curves add +0.0%
  median skill over the parametric fit at ~500 unbiased observations and stay under the
  bar at 10x data (RESULTS-2.md, K1'), and a two-component censored-EM mixture tracks
  the plain censored fit within noise everywhere its target prize existed
  (RESULTS-2.md, B4). Convergence is front-loaded: the fit itself is two parameters
  from ~500 completions per stratum (the verdicts are pinned there, and 10x data
  changes nothing measurable; RESULTS-2.md training-size context), so the wall clock to
  a usable bound is dominated by accumulating settled-pool calibration residuals — the
  committed window rule targets ~40 effective residual samples (RESULTS-2.md harness
  amendments) — and, in deployment, by the telemetry prerequisites above.
- **The layer earns its keep at horizons of 2-10 seconds and not at 1 second.** Median
  skill of the best stochastic mode over the best trivial baseline (deterministic
  growth or persistence, conformally calibrated): +15.4% at 5 s and +19.8% at 10 s —
  unchanged whether cells with no valid-coverage mode are excluded or counted as zero —
  +6.8% at 2 s, a fail at 1 s where deterministic growth is near-optimal
  (RESULTS-2.md, K2'). Skill is regime-dependent: +25-48% in chat and truncation
  regimes, +3-9% in heavy tails, where the fit with conformal calibration sits at the
  oracle ceiling (R2/N=1000: +6.4% vs oracle +6.1% at 5 s).
- **Calibration requires transient discipline.** Training completions and calibration
  residuals must come from settled pools: residuals drawn inside a pool-fill transient
  produce a total coverage collapse that is indistinguishable, from the scores alone,
  from estimator failure (RESULTS-2.md, question A). The deployment analog: calibrate
  and size only against steady-state windows, and treat scale-up transients as
  uncalibrated periods.
- **Drift is a detected regime, not a forecastable one.** Frozen fits lose coverage
  outright under an abrupt mix shift and lose skill to rolling-conformal persistence
  under a ramp (RESULTS-2.md, KD). Continuous refit narrows this but does not remove
  it (RESULTS-3.md, KR): while a ramp is actively moving, no deployable mode holds
  valid coverage at all; after an abrupt break, the refit loop restores valid,
  reference-level forecasting at 5 s horizons within 300 s, but 10 s horizons stay
  behind rolling-conformal persistence until the completion stream clears the shift's
  length bias (in-flight long requests are exactly the observations a
  trailing-completion fit is missing; the bias persists on the order of the new tail's
  residence time).
- **The canary is the rolling residual-quantile shift.** Scored on detection latency at
  a matched zero-observed-false-alarm budget, the rolling shift of the residual q95
  detects an abrupt break in a median of 16 s, the rolling coverage-deficit twin in
  25 s, and completion-mix distance in 56 s — completion-side signals inherit the same
  length bias that defeats refit at long horizons, so mix distance must never be the
  sole trigger (RESULTS-3.md, DD).

### Cold start is the steady state

The layer has no pretrained state, no history requirement, and no persistence: all
estimator state is process-local, and the EPP deployment model has no in-place
upgrades, so every restart replays cold start. That is a design position, not a
limitation to engineer around. A fresh process boots as the deterministic ledger (the
confidence dial's 100% default), acquires the degraded mode once a settled residual
window exists (minutes of steady traffic, per the window rule above), and earns fitted
curves stratum by stratum as completions accrue — fast enough that restart-frequency
amnesia costs minutes of yield, never correctness. Amnesia is also aligned with the
drift posture: warm-starting from persisted fit parameters would reintroduce the
frozen-fit failure the drift verdicts measured (a stale curve wrapped in fresh
confidence), where a re-learned fit cannot be stale by construction. Because the
estimators are content-blind — completion lengths and decode ages, never prompts,
activations, or model internals — nothing couples them to a model family, tokenizer,
or workload; every process learns its own curves in place.

Two consequences follow. Strata too rare to reach the completion requirement within a
process lifetime never earn their own curves and live on the credibility hierarchy
permanently (next section). And the only history the design uses anywhere is exported
observation — shadow-mode coverage metrics recorded outside the process — which
survives restarts, is the evidence an operator needs before moving the dial below
100%, and is the one record that cannot be backfilled; the telemetry prerequisites
exist to make each process's live stream correct and that exported record trustworthy,
not to assemble a corpus.

### Many workloads, one pool

Fits are per stratum, keyed by what is known at admission (flow identity, model,
priority class), and the pool forecast sums each lease against its own stratum's
curve. Two cases, with different evidence:

- **Labels that correlate with output shape** are the best case: separately-labeled
  workloads changing share is composition, not drift — every lease carries the right
  curve from admission, so the sum tracks the admitted mix automatically. The
  harness's per-lease oracle is this construction with perfect labels, and it held
  valid coverage through every drift scenario, including mid-ramp, while every
  marginal model failed (RESULTS-3.md slice detail).
- **Labels that do not** are priced by the measured verdicts, not assumed away. The
  test regimes are deliberate within-stratum pathologies: a single stratum with hard
  bimodality holds valid coverage under the two-parameter fit at near-oracle skill
  (explicit mixture modeling fails to pay; RESULTS-2.md, B4), and a single stratum
  with a Pareto tail is valid at the oracle ceiling. Variance inside a stratum widens
  the bound — it costs yield, never coverage. What breaks calibration is
  nonstationarity inside a stratum, and that is the case the drift verdicts bound
  (KD, KR, DD).

Stratification is therefore a variance-reduction opportunity, not an assumption the
design rests on. Sparse and new strata fall up a hierarchy rather than off a cliff: a
stratum starts on the pool-wide curve and blends continuously toward its own as its
completions accrue (credibility weighting in the Buhlmann sense, replacing any hard
sample cutoff; the blend-weight design is open). Two backstops hold beneath the
hierarchy: conformal calibration operates on the pool forecast's residuals, so a
stratum running on a borrowed curve stays inside the coverage guarantee — its
miscalibration surfaces in pool residuals and is absorbed into the bound as width —
and the canary watches those same residuals, so label schemes that stop tracking
reality trip the same fallback as any other calibration loss.

### The operating contract

The rules above compose into the layer's runtime posture:

1. Statistically multiplex while calibration demonstrably holds, measured by the
   canary on the production forecast's own residuals. The measured latencies used
   thresholds calibrated against stationary null runs the simulation can manufacture;
   the online threshold-setting rule for a deployment is open (see Open questions).
2. On a canary trip, drop the affected horizons to the degraded mode —
   rolling-conformal residuals on the trivial growth forecast — or to the deterministic
   bound, per tier. The undetected exposure after an abrupt break is tens of seconds at
   the calibrated thresholds.
3. Re-admit the fitted layer per horizon as its coverage returns: short horizons
   (~5 s) re-arm within minutes under the refit loop; longer horizons re-arm only once
   post-shift completions are representative again.
4. A predictor that stays calibrated through arbitrary regime breaks without a fallback
   is unmeetable in principle — the new distribution must be observed before anything
   can be calibrated to it — so the fallback is a requirement of the problem, and the
   deterministic ledger it falls back to has standalone value.

### Bookkeeping is not prediction

Per-request bookkeeping is not per-request prediction. A lease's footprint decomposes
into a measured part (prompt tokens, the cached-prefix discount, tokens generated so
far), a bounded part (the `MaxOutputTokens` ceiling, a request field rather than a
forecast), and a predicted part that exists only in aggregate: the expected view
discounts the sum of bookings by pooled survival probabilities, and no admission
consumer reads one lease's probability alone. A per-lease probability is calibrated on
average and wrong for any individual, which is acceptable wherever it is consumed
summed. The two places a probability would be consumed individually — victim selection
and wait-vs-evict — are where the per-lease reliability diagnostics sit, and they are a
separate hypothesis from aggregate forecasting, not a corollary of it.

This distinction is what separates the design from the published per-request
output-length prediction line (S3, TetriInfer, SSJF), whose pointwise accuracy is
fragile largely by its own reporting and which targets ordering and placement, not
admission.

### Open questions

- Shadow-mode calibration inside the EPP against replayed and live traffic (H2); the
  forecast gates nothing until quantile coverage holds there.
- Closed-loop behavior when eviction censors the estimator's own training data (H3,
  informative censoring). This is the genuine research risk; it activates only when the
  forecast has admission authority and revocation volume is material.
- Censoring-aware refit: fitting on completions plus in-flight ages as right-censored
  observations attacks the completion length bias behind both the long-horizon refit
  failure and the mix-distance detector's lag.
- Online canary threshold setting: the scored latencies calibrated thresholds from
  stationary null runs, which a deployment cannot manufacture; the online analog
  (trailing quantiles of the detector statistic during canary-quiet operation, or an
  operator-set false-alarm budget) is undesigned.
- Stratification depth (flow, prompt length, model) before data sparsity dominates,
  and the credibility blend weight for the stratum-to-pool hierarchy; decode-rate
  variability under load (excluded from the simulation by design; owned by the
  external-simulator re-score).

## Tiered admission: the confidence dial

*(Directional.)*

The operator tunable is a one-sided upper confidence bound on future occupancy, and the
guaranteed tier's deterministic bound is the 100% end of that dial. Consequences:

- **The default posture is ceiling-bound.** At the default, prediction failure costs
  multiplexing efficiency and sheddable churn, never the guaranteed contract. The
  engine's preemption-with-recompute remains the residual backstop.
- **Any setting below 100% makes quantile calibration a correctness input** rather than
  an efficiency input. The coverage evidence from shadow-mode operation (H2) is the
  gate for documenting any lower setting as supported, and the compound event —
  quantile miss while the sheddable pool is exhausted — must be bounded under
  dependence, since both are load-driven.
- **The dial has a decision-theoretic setting rule.** The critical fractile
  `rho / (rho + 1)`, with `rho` the ratio of per-token revocation cost to per-token
  waste cost, is the classical distributionally-robust newsvendor reservation applied
  to this setting; arXiv:2607.16892 (Proposition IV.2) derives exactly this ratio for
  admission-time KV reservation under length uncertainty. Its companion negative result
  — no single fixed quantile is optimal across cost regimes — is the argument for
  exposing the dial rather than hard-coding a percentile.

## Migration path

*(Proposed.)*

Each stage subsumes the previous stage's bookkeeping; no stage requires rework of the
eviction controller or the dispatch gate's structure.

1. **Scalar eviction**: gauge-unit deficit and pending-reclaim debits
   (`docs/flow-control-eviction.md`).
2. **Dual ledger**: `KVTokens` + `Slots` held dispatch-to-EOS, with the prefill backlog
   gate keeping the TTFT release. This fixes the token-mode under-count and the
   dispatch race, and needs token estimation but no block-level translation. The
   accounting skeleton exists today in `InFlightLoadProducer`
   (`pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload/producer.go`):
   per-endpoint atomic counters committed at PreRequest, first-token release,
   exactly-once release surviving endpoint flap, prefix-cache-discounted footprints.
   The gaps — a lease record, reclaiming state, the hold phase, the second axis — are
   extensions of existing mechanisms, not new machinery.
3. **Footprint ledger**: full types, hold-then-lease protocol, event truth-up;
   `SaturationDetector` becomes a derived view.
4. **Stochastic layer**: the calibrated release forecast, expected-release schedules,
   tiered admission against the confidence dial; `VictimSelector` with
   deficit-covering subset selection.
5. **Engine co-design**: the engine reports actual per-request block usage (retiring
   Translator estimation) and accepts priority (local preemption; the EPP handles only
   the cross-endpoint case).

## Multi-EPP posture

*(Open, with a preferred position.)*

Independent replica ledgers double-book pool capacity. Endpoint partitioning — one
writer per endpoint ledger, with ownership following the existing endpoint-to-EPP
affinity — is the least machinery with the strongest invariant, and is the position
this document proposes to adopt when multi-EPP deployment becomes concrete. What is not
acceptable is silence: any multi-EPP deployment of the ledger without a partitioning
story re-introduces the over-admission the ledger exists to prevent.

## Relationship to existing components

- `concurrency-detector` + `inflight-load-producer` are a proto-ledger (stage 2 grows
  out of them); `utilization-detector` becomes a reconciliation input rather than the
  primary gauge.
- `UsageLimitPolicy` survives as the tier-policy seam; its ceilings become
  footprint-denominated reserves in stage 4.
- The eviction plumbing (`RequestEvictor`, `Evictor`, `EvictionRegistry`, ext_proc
  channel) is unchanged throughout; only selection and sizing upgrade.

## Prior art and what is claimed

Every component has direct precedent: multi-dimensional resource vectors
(Borg/Kubernetes, DRF), two-phase TTL'd reservations (slot booking, allocators),
overcommit with revocation (airline overbooking, statistical multiplexing and effective
bandwidth in telecom), survival analysis (reliability theory, actuarial reserving),
conformal calibration. The claimed contribution is the synthesis and its placement:
engine-grade, lifecycle-split, revocation-capable capacity accounting at the fleet
choke point.

The nearest published work, and the boundary with it: arXiv:2607.16892 reserves KV per
class at admission under a distributionally-robust fractile, re-solved on
minutes-scale ticks, with every decision conditioned on admission-time information
only; this design differs by conditioning on decode progress and by forecasting
realized occupancy online over second-scale horizons, and its authors name online
adaptation as future work. arXiv:2604.00499 (TIE) models output length as a
distribution engine-side for queue ordering and names distributional updates during
decoding — exactly the age-conditioned form used here — as future work. Neither line,
nor the per-request prediction line above, occupies content-blind, age-conditioned,
pool-scope admission with revocation as the enforcement mechanism; that combination is
checked against the read texts in the exploration's related-work notes, not recalled.
