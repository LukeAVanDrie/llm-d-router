# Admission control: program document

Read this file first, then DESIGN.md. This file states the goal, the method, the ordered work with
its gates, what is established, and what failed. DESIGN.md states what is being built and the model it
rests on (section 3 there); this file does not restate it.

## 1. Goal

**Per-endpoint admission for the head of the router queue that maximizes goodput under the declared
SLOs, or token throughput when none are declared, with guarantees stated and proved or measured.**

The operator's only lever is the SLOs (DESIGN.md section 2). The router binds late: a request held at
the router can still go to whichever endpoint frees first, while a dispatched request is committed to
one endpoint. The rule errs two ways:
- **False hold:** some endpoint had headroom, and the request waited anyway.
- **False dispatch:** the endpoint could not carry the request without harm. The request waits inside
  the engine, or its growth forces a preemption, and it cannot be moved.

Success is measured in three layers:
1. **The model's errors per resource**, against ground truth: the true future peak for memory, realized
   step time and engine wait for compute.
2. **Outcomes:** goodput under declared SLOs below capacity; token throughput in overload, with the
   served mix reported.
3. **Guarantees** (DESIGN.md section 5): each one proved, model-checked or measured, with its premises
   stated.

The end product is the fork's rule, validated on a real engine, and a paper whose claims are stated per
decomposition rung.

## 2. Method: decompose, then add one phenomenon at a time

The problem has several independent dimensions of complexity, and their combinations multiply. The
program learns the smallest form first and then departs from it one axis at a time.

| axis | what it alone introduces | design component it exercises |
|---|---|---|
| 0. base: one engine, one tenant, fixed length law | growth risk, delay conservation, waste, the window horizon, θ from waste, the TPOT limit | risk estimate, calibrator, compute limit |
| 1. one engine to many | late binding, the existential check, placement interaction, fragmentation, pool size in the tail, multi-replica state | contract and shield |
| 3b. independent lengths to correlated | the product of survival terms breaks, even under a fixed law | risk estimate (group factor) |
| 3. fixed length law to shifting | estimator lag, calibrator tracking with delayed labels, change detection | survival band, calibrator |
| 2. one tenant to many | priorities, composed SLOs, per-class victim order and θ, isolation, head-of-line blocking across bands | ordering, shedding, class terms |

Every rung is scored in two regimes: below capacity (goodput; delay is conserved) and in overload
(throughput; work is conserved). The regime is a scoring condition, not an axis.

Rules for the ladder:
- Each experiment departs from the base by at most one axis and measures what that axis adds.
- Two axes are combined only where a stated mechanism predicts an interaction; the mechanism is written
  in the registration.
- The full product of axes is never the unit of test.
- Adding an axis must not require changing a component built for an earlier one. If it does, that is a
  coupling, and it is recorded as a finding.

Most existing evidence sits at four endpoints, one tenant, a fixed law: on rung 1, not the base.
Section 4 re-indexes it.

## 3. Ordered work

### 3.0 Reading and ideation (no runs)

The model is DESIGN.md section 3: the target invariant (requests wait at the router, never inside an
engine) and the rule it implies. The reading and ideation here support its optimality claims (sections
3.6 and 5, G7 and G8). T's pricing (κ, G, the chain's severity weighting, δ calibration) is closed to
further ideation until rung 0's comparison against P₁ ≤ θ reports.

1. **Prior art.** Done 2026-09-28: related-work.md. Follow-ups: read Ao, Dong, Luo and Simchi-Levi
   (arXiv 2606.15555, the saturation limit cycle) in full before the attribution test; read
   BalanceRoute (arXiv 2605.06113) for the late-binding comparison. The original list:
   - Ao et al. (arXiv 2504.11320). From the abstract: one worker; WAIT for known output lengths, Nested
     WAIT for unknown lengths using decode-stage segments and a safety buffer; asymptotic optimality
     against a fluid benchmark. To extract: the buffer's form and size, the scaling under which the
     gap vanishes, and the proof machinery for G7.
   - Jaillet et al. (arXiv 2502.07115). From the abstract: no deterministic online algorithm has a
     constant competitive ratio under arbitrary arrivals; a polynomial-time algorithm has a constant
     ratio under stated conditions. To extract: whether output lengths are assumed known, and the
     conditions.
   - Online allocation of reusable resources (Rusmevichientong, Sumida and Topaloglu; Feng, Niazadeh
     and Saberi; Baek and Ma; citations to confirm): LP benchmarks and ratio guarantees.
   - Heavy-traffic asymptotic optimality in the Halfin-Whitt regime (Halfin and Whitt; Armony, Atar
     and Reiman-style many-server control): how O(√n) optimality gaps are proved.
   - Achievable region and priority optimality (Coffman and Mitrani; Bertsimas and Niño-Mora): the
     ordering half of the optimality pair.
   - Denning's working-set load control and thrashing: the lineage of the memory constraint.
   - Ruin theory and finite dam models (Cramér-Lundberg; Moran; Prabhu; Harrison): the window risk P₁
     as a first-passage (ruin) probability of a process with deterministic drift and upward jumps.
   - Phase-type length laws (Coxian, hyperexponential): closed-form P₁ for the MDP yardstick and the
     proofs; the mechanism keeps the nonparametric survival estimate.
2. **Ideation.** One written session per question, each ending in a proposed statement or a reason to
   drop it:
   - a formal statement of the target invariant and of what "at most θ per admission" guarantees;
   - the logarithmic cushion: the margin's dependence on θ and on m/K, and where the plateau narrows;
   - the proof routes for G7 and G8, and which premises (hazard shape, independence, scaling) each
     needs.
3. **Offline checks on existing data** (exploratory; seeds 76-120 and 136-150, already seen; each
   written as a prediction before it is computed; results inform registrations and decide nothing):
   - **O1, the calibrated level at δ = -2.** Isotonic calibration of N_win against the closed-gate
     labels on the classifier logs (results-v14-round2/classifier/, labels from v15_classifier.py):
     calibrated P(overflow) at the δ = -2 boundary lies in [0.10, 0.25], the range of the
     interventional Bayes level. The P₁ version needs P₁ logged and moves to rung 0.
   - **O2, calibration on the calibrated runs** (results-v14-round2/cal/ and batch/): the realized
     false-admission rate stays within the calibrator's bound of its target in every window of 500
     decisions after warm-up.
   - **O3, T's pricing in hindsight.** On the classifier logs, among decisions where T and a
     calibrated-N_win constraint at matched admit rate disagree, compare hindsight wait saved against
     overflows caused. Prediction under "the pricing carries no value": the net is within noise of
     zero.

### 3.1 Rung 0: the base case

One engine, one tenant, fixed length law.

**Done:**
- **Front battery** (registered rung0a-2026-09-28; results-v10.md, "Rung 0 front battery"): the
  throughput plateau at four endpoints and one; T and P₁ ≤ θ; victims per overflow. PL1, PL2, PL4,
  SV1, SV2 pass; PL3, SF1, SF2 fail (section 5).
- **Extension** (exploratory, seeds 104-106; "Rung 0 extension"): T's δ sweep on half cache and the
  extended P₁ grids. T at its best δ ties P₁; one fixed θ lands within about 1% everywhere.
- **The small MDP yardstick** (exploratory; reserve-reopen/v17_mdp.py; "The small MDP yardstick"):
  holding pays only through the batch-wide recompute stall; one threshold on P₁ is near-optimal where
  it does; the admit region is monotone almost everywhere.
- Per-decision P₁ logging and the E = 1, half-cache and longout variants (v17_rung0.py).
- **Confirmation battery** (registered confirm-2026-09-28; seeds 256-265; 660 runs; "Confirmation
  battery"): C1, C2 and C4 failed, C3 passed weakly. In overload γ₀ = 1.5 is within 0.64% of the best
  arm everywhere; below capacity ten seeds do not resolve ±1%. The best γ differs by cell, and T at a
  fixed δ = −1 is the more robust fixed setting (DESIGN.md section 10).
- **The self-reuse check** (exploratory, seeds 104-108; reserve-reopen/v18_reuse.py; "The self-reuse
  check"): with vLLM v1's free-block queue modelled, the look-ahead's gain over F roughly halves and
  stays positive; γ₀ = 1.5 stays within 0.1% of the best γ in overload.
- **The upcrossing gate prototype** (reserve-reopen/v19_ucheck.py): the tail-difference bound in
  blocks, Harris, the running minimum over [Y₀, C], interval refinement and the dyadic bound. Two
  review rounds (theory, systems, statistics) found it sound on every state tested, at about 0.03 s
  per decision; 1.6-3.8 times the true overflow probability on bimodal (DESIGN.md section 3.3).

**Decisions agreed with Luke, 2026-09-29** (after the confirmation battery and two review rounds on
an external theory review; the findings are in DESIGN.md sections 3.3 and 8 and in proofs.md C49):
1. **The look-ahead's statistic.** Register a comparison of the upcrossing gate (v19_ucheck.py;
   DESIGN.md section 3.3) against the current window gate, P₁ repaired with the deterministic-exit
   ruin horizon: t_ruin as the first crossing of the no-completion demand curve, with max_tokens as
   deterministic exits, M_i ≥ τ, no cap credit before a request's first token, and a block-rounded
   envelope. P₁ is a lower bound on overflow and the upcrossing gate an upper bound, 1.6-3.8 times the
   truth on bimodal, so tightness does not decide it: score goodput and throughput at matched realized
   overflow. Each statistic gets its own level. In the simulator, pass footprints as tokens + 2 (v10
   holds blocks(tokens + k) through a request's final step) and count shared prefix blocks as load that
   frees only with the last sharer.
2. **The level.** γ₀ = 1.5 on the mean estimate as the interim default (best worst-case regret among
   the fixed options that assume nothing about the engine; errs safe if real recompute is cheaper).
   Register γ₀ = b·ln(K/x̄) − k₀ with b ∈ [1, 2] declared in advance and b = 1 against b = 2 as the
   test (b = 1 if an overflow costs a fixed number of victims, 2 if the stall grows with the batch), on
   at least 30 fresh seeds per cell plus out-of-sample cells with real spread in K/x̄ (bimodal at 2x
   cache, a long-context law, a heavy tail, a cheap-recompute cell). The formula is a heuristic fit,
   not a derivation. Freeze the structural form (DESIGN.md section 3.5: κ, the youngest-first depth,
   ℓ̄ normalization, victim-recompute share under self-reuse) as a second prediction; both are scored
   on the fresh seeds and new cells only, since the structural form was fitted after the fact on the
   confirmation data. Primary endpoint: whether either beats a constant γ₀ = 1.5 on the default's
   maximum regret across cells. γ₀ stays a default plus the shadow-mode audit; the overflow cost is
   never estimated online. γ₀ = 2.0 is refuted (half cache at 0.95: −2.7%).
3. **The estimator.** Refresh on events (completions and censoring) or lazily at decision time, with
   decay on the step clock applied in closed form (both builds refresh every 25 completions, which
   freezes the curve on an upward length shift). Gamma-Poisson hazards on geometric age bins (ratio
   1.25), exposure per streamed event, events on EOS, hazard 0 past the oldest exposed age. Average
   survival over the hazard posterior: the closed form (1 + k·w/β)^(−α) for P₁ and sub-batch terms; for
   the upcrossing computation, generalized Gauss-Laguerre with extra nodes or a conservative
   lower-quantile hazard in sparse bins (DESIGN.md section 3.3). The product of posterior means is
   optimistic by about n(n − 1)·Λ²/(2d) nats, 0.12-0.98 in realistic cases. Registered choices: the
   forgetting horizon (candidate five pool turnovers), a proper prior in exposure units with its mean at
   or below a low-hazard reference, and cold-start behavior (hold or admit; a decision for Luke).
   Success criterion: the p10-p90 signed error of γ near γ₀ on the registered cells within a registered
   band, and the anti-conservative window after an upward length shift within a stated number of steps.
4. **The engine firewall.** Under the γ rule, delete T's victim-chain path (rank, newest-first order,
   the k ≥ 1 loop). Detect in-engine stalls by sibling events (DESIGN.md section 8, "Stall detection"):
   a stream silent while its siblings on the same engine produced more events than `stream_interval`
   plus the longest holdback plus the largest speculative burst, with a small margin. No σ and no γ₀
   enter it. It fails with fewer than two streaming siblings on one engine, data-parallel engines behind
   one endpoint, several replicas and non-streaming traffic; the engine's waiting and preemption counts
   stay as the cross-check.
5. **The real-engine path.** Fix `SelfReuseEndpoint` (it shields a victim's cache from allocations;
   vLLM loses a victim's tail blocks to every new allocation), run the self-reuse check, then the
   single-GPU vLLM kill test, which can make the memory look-ahead moot. The fix and the check are done
   (v18_reuse.py, commit ea5a35bd3); the kill test needs Luke's GPU.

**Further tests** (each registered before it runs):
- **Refill.** Log the share of completion epochs whose post-completion state has γ < γ₀, the term the
  C44 bound carries without the refill premise.
- **Waste accounting.** In overload, does throughput equal capacity less the recompute and under-fill
  shares, within a registered tolerance? At E = 1 here and at E = 4 on rung 1.
- **F over no gate.** At one engine there is no late binding, so any gain of F over no gate is prefill
  pacing. Prediction: most of the 3.3-3.8% measured at four endpoints vanishes.
- **TPOT limit.** Does a compute limit from the TPOT SLO, checked jointly with TTFT for feasibility,
  cap streams at load 0.95 without the collapse measured in "Test 1"?
- **Scaling.** In overload across cache sizes 0.5x to 4x: the free memory at which the rule refuses
  stays of order γ₀·m plus one step of growth rather than growing with K, and F's loss does not shrink.
- **Prompt-length covariates.** Coarse log-prompt buckets (floor(log₂ max(p, 32))) shrunk toward the
  pooled hazard, with posterior averaging, against one pooled curve. Lift cell: conv2023 in arrival
  order (the prompt bucket explains 49% of the variance of log output length). Negative controls:
  code2023 (0.3%) and the synthetic laws, where the loss must stay within a margin set in advance.
  Constants (pseudocount, bucket geometry, priors, age bins) fixed on an earlier, disjoint time slice.
  Endpoints: calibration of S at ages up to 30 (time-dependent Brier and log score), realized overflow
  by gate bin, and goodput at matched realized overflow; plus a same-bucket burst stress. The Azure
  2024, BurstGPT and Mooncake traces come later; Mooncake's block hashes are the only public test of a
  prefix covariate, which is not proposed.
- **Ordering below capacity** (PL3's failure). Thresholds crossed with FIFO, EDF, shortest-prompt-first
  with aging (arrival plus predicted prefill), feasibility-aware least slack (still-feasible requests
  first, hopeless ones last) and deadline shedding. Primary endpoint: base-SLO goodput; secondary:
  hold-episode length. Prediction: least slack recovers part of PL3's gap (up to about 44% in the
  replay) and EDF on arrival minus predicted prefill does not beat FIFO.
- **Gate validation.** Soundness of each gate against exact enumeration on tiny instances and against
  Monte Carlo with a per-case k·SE threshold; conservatism (bound over truth) tracked as a power metric
  on the bimodal, atoms, lognormal and geometric regimes, stratified by distance from the level.

**Exit:** the default (statistic, estimator and γ₀) is kept or revised; the TPOT limit is kept or
dropped.

### 3.2 Rung 1: many engines

**Tests:**
- **Phase 2, "is max() enough?"** (registered 2026-09-28, tag phase2-2026-09-28, frozen code, seeds
  231-245, no scored run; research/battery-registration-draft.md). Runs as registered: state estimate,
  scrape delay, shared prefixes, a second router, and the delay-feedback stability test.
- **Waste accounting at E = 4** on fresh seeds; reads the Phase R result forward as a registered test.
- **Detector attribution:** runs at the front of rung 0's battery (section 3.1). Precondition: check
  the emulation against llm-d's detector code path.
- **F without the release condition** at E = 4, only if rung 0 leaves the F-over-no-gate question
  open.
- **Placement:** a prefix-affinity scorer in the simulator, then the upstream-baseline comparison
  (DESIGN.md section 8).
- **G5:** the multi-replica rule is the scrape plus each replica's own unreflected admissions
  (DESIGN.md section 4, "Replicas"); derive its bound and check it against Phase 2's replica scenario.
  Leasing capacity shares across replicas is out of scope.

**Exit:** state estimate decided; attribution measured; the when/where interaction with prefix affinity
measured.

### 3.3 Rung 3b: correlated lengths

- Implement the group factor (paper.md section 5.1) in the simulator and in the fork's `WindowTest`, or
  withdraw it from the paper's claims.
- A correlated-length cell: shared per-program length multipliers. Measure how far the independent
  product under-predicts overflow, and whether the comonotone factor restores safety or over-holds,
  scored on goodput as well as overflow, since the group factor holds more on the agentic workloads
  where TTFT matters most.
- A frailty model only if the comonotone factor over-holds measurably.
- Settle the group key (DESIGN.md section 11).

### 3.4 Real engine, shadow mode

**First, the kill test** (one vLLM on one GPU, forced overload, no gating): recompute tokens per
preemption with prefix caching on and off, readmission delay, repeat preemptions, and the step-time
line fitted from `vllm:iteration_tokens_total`. If the implied batch-stall cost puts γ₀ at about 0.5 or
less, F plus the ceiling rule is the design and the window test is dropped (DESIGN.md G10). Then, in
order: ledger fidelity (booked minus scraped at scrape instants, under prefix sharing); shadow
calibration (predicted Σ e^(−γ) against the engine's preemption counter, and the refill term); release
accounting with the true step budget; a gated A/B at loads 0.95 and 1.1, one router replica, then
several.

After rungs 0, 1 and 3b for the full shadow mode: in kind with CPU vLLM, then one GPU, with the
shadow-mode bar registered first (DESIGN.md section 11). The step line's fit checks for the roofline ridge. The
runbook is reserve-reopen/validation-runbook.md. Registered prediction to carry: recompute costs less
on a GPU than the simulator's linear charge, so the overload gain shrinks by the difference.

### 3.5 Rung 3: shifting length law

The shift test (law2 and shift_frac in v11.simulate): false dispatches and stalls over time for the
calibrated rule against a static threshold. The calibrator's step size from G4. Change detection on the
survival band (CUSUM) only if the band's lag shows up as a measured transient.

### 3.6 Rung 2: many tenants

multi-tenant-extension.md holds the extension. Tests, each registered on its own: class-weighted victim
order, the stream cap against placement, isolation, and whether any SLO set moves the goodput-optimal θ
(including a maximum-gap SLO under heavy streaming). The pick rule, weights and per-tenant budgets
belong to the sharing pillar; capacity owns only its side. Ordering (FIFO, prompt-size order, least
slack) and overload shedding run here with flow control. Hypothesis for the class terms: cumulative
hazards add across classes, so every class's completions refill the same memory and γ needs no class
term; a class-aware victim order changes only the overflow cost c, and so γ₀ (DESIGN.md section 3.5).
vLLM's default preemption is newest-first, not by class, so the hypothesis needs the priority mode. Backfilling past a held head (EASY backfilling
from HPC scheduling) is tested only if head-of-line blocking on these workloads exceeds what rung 1
measured (3-10% of blocked time at load 1.3; "Head-of-line blocking at load 1.3").

### 3.7 Paper

After the real-engine result. The model chapter states DESIGN.md section 3 per rung; the guarantees
chapter states section 5 with premises; the optimality chapter states G7 and G8 against the C43 fluid
bound, and the ordering half through the achievable region; G3's proof uses event-based dynamic
programming with the MDP as its check; related work covers Ao et al., Jaillet et al., Denning's
working-set load control, the
Simplex architecture, adaptive conformal inference and stochastic network optimization, DistServe,
Sarathi-Serve, Vidur, QLM, Andes and Mooncake.

## 4. Established

Each item names its rung. Rung 1 is four endpoints unless stated.

**Proved or model-checked** (proofs.md, claim-ledger.md section 7, tla/ReserveQ.tla), rung 0 unless
noted:
- booked accounting (Proposition 1), and its general form;
- rows that never rise;
- the resident bound;
- liveness for one flow, model-checked with preemption and pending victims (3.1 million states at
  K = 8, with a negative control and a witness);
- R7;
- Lemma H, H' and H'Δ; Lemma C; Theorem W and its two-term bound; W0;
- W under group dependence (C42), rung 3b;
- the memory-time identity (C43);
- the one-sided timing/commitment decomposition (C17).

**Measured** (simulator; results-v10.md), rung 1. Items from Phase 1, 1b and R are registered at their
stated windows; the remaining items (true κ, the classifier AUC, online calibration, the length-aware
reference, charging unstarted requests, exact crossings, queue-length placement, self-reuse) come from
2,500-request windows, which have flipped a sign before, and are short-window results:
- **Delay is conserved below capacity** (within 1.1%; 1.3% work-weighted) and not in overload
  ("Phase 1").
- **The best threshold does not move with the SLO set** on bimodal, across four sets ("Phase 1").
- **The optimum is flat:** δ ∈ {0, -1, -2} statistically tied on bimodal ("Interventional marginal
  costs").
- **T goes inert at large cache** and its gain follows Φ̄(z) across five variants, one at 16 endpoints
  ("Phase 1b").
- **Overload throughput:** the gate +5.4-5.6% over no gate at loads 1.1 and 1.3, 10,000 requests
  ("Phase R").
- **Against the shipped detectors:** about +10% goodput on bimodal and longctx, level elsewhere ("Ship
  comparison"; "Phase R").
- **T with the engine's κ** beats gate-off by 3.2% on bimodal; the κ floor loses 1.6% ("True κ in T").
- **T's score has AUC 0.94-0.96** against closed-gate labels where overflow occurs ("The gate as a
  classifier").
- **One calibrated target** lands within noise of the best fixed threshold in every cell, and the
  learned thresholds converge near δ = -2 ("T calibrated online").
- **The length-aware reference** leads the calibrated rule by 2-3% on bimodal and is level elsewhere
  ("T's threshold at the true κ"; "T calibrated online").
- **Timing does not move deep risk:** R = -0.002 and -0.003. **Windows stay clean at the edge:**
  attributed preemptions at most 0.0007 per completion (O1).
- **T carries the reserve:** with T the best static reserve is 0 at κ0 = 150.
- **Charging unstarted requests in T** cuts preemptions sevenfold (0.038 to 0.005 per completion).
- **Exact crossings (O1)** add +1.33 points [0.92, 1.74] where growth binds.
- **Queue-length placement** removes a TTFT penalty of 13-15 points at no throughput cost.
- **Self-reuse halves the throughput gains:** with vLLM v1's free-block queue modelled, the gain of
  γ₀ = 1.5 over F at load 1.1 falls from +5.4% to +3.0% on bimodal and from +2.6% to +1.2% on half
  cache (exploratory, 10,000 requests, seeds 104-108; "The self-reuse check"). An earlier engine that
  shielded victims' cache from allocations gave +1.3% on bimodal at 1.3 (2,500-request window).

**Measured, rung 0** (registered rung0a-2026-09-28, 10,000 requests; "Rung 0 front battery"):
- **The throughput plateau** holds at four endpoints and one: δ = 0 and -1 within 0.2% of the best at
  load 1.1, and δ = -3 costs 2.6% (PL1, PL2, PL4).
- **Overflow severity:** about two victims per episode on bimodal, more on longout (SV1, SV2).
- **γ₀ = 1.5 in overload** is within 0.64% of the best arm in every cell (confirm-2026-09-28; its C1
  failed below capacity).

**Exploratory** (seen data; DESIGN.md section 10):
- overload waste accounting; the decision-score variance split near the threshold;
- the rung 0 extension: T at its best δ ties P₁ ≤ θ on half cache; one fixed θ within about 1% of the
  best in every cell;
- the small MDP: holding pays only through the batch-wide recompute stall; one threshold on P₁ is
  within 0-1.8% of optimal at realistic stall costs; the admit region is monotone almost everywhere;
- m/K per cell (0.019 bimodal, 0.037 half cache, 0.023 longout) orders how sensitive each is to γ₀.

## 5. Negative results (do not re-derive)

- **A static price model (newsvendor) for the reserve** failed its exploratory and confirmatory
  tests. A reserve idles only 21-90% of its nominal memory, so its cost is endogenous.
- **The floor or a static reserve in the throughput default:** removing the floor raises throughput,
  by +0.49 and +0.58 points.
- **"Gain over gate-off is at most gate-off's waste" as a theorem:** it needs equal memory-time per
  token outside recompute (C43); on 2,500-request windows it was exceeded in most pairs. The
  10,000-request reading in DESIGN.md section 10 reopens it only as a registered test.
- **The low-κ reserve gain:** a warm-up transient of the 2,500-request window. At 10,000 requests the
  reserve lowers throughput by 2.7-4.7% ("Step-time check").
- **The registered goodput win (round 2):** failed. The rule traded TTFT for stalls, and an SLO on
  TTFT plus mean TPOT counts the cost and not the stalls.
- **Metrics under permanent overload:** goodput is dominated by queueing. Test goodput below capacity
  and saturated throughput on token throughput, with the mix reported.
- **A compute boundary chasing the TPOT percentile alone** cut capacity below arrivals at load 0.95
  and cost 5-23% goodput ("Test 1").
- **The Bayes threshold from naive costs** failed offline; the hold cost counted one request, not the
  detours and queue ("The Bayes threshold, offline").
- **Learning the hold cost online:** a dither identifies the dispatch cost but not the hold cost at
  run length ("Dithered cost estimation"); a structural full-queue hold cost overstates it about
  ninefold ("Structural hold cost").
- **SLO sets moving the threshold:** the TTFT-tight and stall-tight (maximum gap 10r) sets did not flip
  the sign of δ = -2 against δ = 0 (E2a); the per-decision rule is shelved (E2b).
- **√(m₂/(m₁·K)) as a ranking or scaling law:** failed across cells (B1) and within bimodal (D1, D2);
  Φ̄(z) replaced it.
- **κ invariance within a run:** a constant κ is absorbed only once calibration converges, and the
  calibrator is too slow to converge from a poor start in 2,500 requests (P-S3).
- **The predictor-based detector:** the latency predictor is sunset and is not proposed.
- **δ = 0 in the goodput band below capacity** (PL3): 2.3% below the best at load 0.95 on bimodal.
- **The simple rule beating T** (SF2's direction on half cache): the gap was T at δ = -2 being too
  permissive there; at its best δ, T ties P₁ ≤ θ (exploratory extension). SF1 failed on interval width
  because T preempts more than the grid's most permissive arm.
- **C44(c) per admission:** fails on windows that start at a completion (the verifier's
  100-small-residents counterexample); repaired per decision epoch under the refill premise.
- **C47's vanishing overload gap:** withdrawn; one step of growth is a constant term (n'/K, about 0.07%
  on bimodal), and the stall cost grows with the batch.
- **C48 as first stated:** fails for back-to-back admissions and readmitted victims; holds for the last
  admission with no later entry to the running list.
- **Deriving θ per decision as κ·G/(n'·x_head):** dropped. It is T's gain-versus-cost pricing again,
  and at their best settings T and the simple rule tie.
- **A universal "natural" γ₀:** there is no critical point at γ = 1; γ₀ = 1.5 is a measured default.
- **The γ₀ formula as derived:** the frozen ln(K·c/(m·ℓ̄)) missed longout (0.14 against a best of
  2.5-3); a balance of an overflow cost of order n against a hold benefit of order 1/n gives slope 2 in
  ln n, not 1; the consistent-unit form's slope depends on how recompute scales. It is a fit with a free
  slope (decision 2).
- **Lower bounds as gates:** P₁, the best sub-batch and the sup over steps of P(Y_t > C) all lie below
  the overflow probability. Sub-batch terms must use the discrete ruin step (floor(τ_L) + 1 in tokens),
  not ceil(τ_L) or the continuous τ_L.
- **Shortcuts that break the upcrossing bound:** restricting it to greedy candidate horizons, or
  replacing the tails with normal or saddlepoint approximations (both miss in either direction); the
  slab (C − |A|, C] in place of (C, C + N]; a running minimum over all C' from 0 (U is 0 below Y₀, so
  it admits everything); rounding Y's weights up and W's down (valid but about 9x looser); a token-level
  bound against a block engine (unsafe); truncating the horizon before max_tokens (unsafe).
- **A pooled survival curve as the safe direction:** it is optimistic for the long-output class whenever
  footprint and output length are correlated (a two-class example: true overflow 0.20, pooled about 0).
- **Gating the step-time fit on no pending prefill:** leaves the high-concurrency region almost without
  data, and misses the prefill of readmitted victims.
- **EDF on arrival minus predicted prefill** (to repair PL3): serves long prompts first; 12 more misses
  than FIFO in a counterfactual replay.
- **Deriving every constant online** (the review's Part 17 in its first form): the γ₀ formula with the
  step budget in place of κ clamps to 0 in every cell; the overflow cost is unobservable per episode in
  stock vLLM and would ratchet upward if estimated online; refresh every block-size steps, forgetting
  over one pool turnover, a prior of one exposure step, and a Gaussian stall threshold tied to e^(−γ₀)
  all failed their checks (DESIGN.md section 3.3 and section 8 hold the corrected forms).
- **Plain Gauss-Laguerre for the posterior average:** wrong for α ≠ 1; the generalized three-node rule
  underestimates P₁ in sparse bins.
- **Leasing capacity shares across router replicas:** strands requests near saturation and still needs
  the reconciliation of unreflected admissions it was meant to avoid.

## 6. State

- DESIGN.md states the rule as X + w ≤ C and γ(X + w) ≥ γ₀ (section 3); the default is the mean survival
  estimate at γ₀ = 1.5; T's pricing is a candidate refinement that has not beaten the simple rule.
- A five-reviewer panel (queueing theory, safety and control, vLLM internals, statistics, systems
  novelty) read the synced docs on 2026-09-28. Its findings are folded into DESIGN.md (sections 3.3,
  4, 5, 8, 9, 10, 11), proofs.md (C49, C50, and the status of C46 and G8) and related-work.md (section 3).
  The most decision-relevant: vLLM v1 keeps a preempted request's blocks cached, so the recompute cost
  that justifies holding may be much smaller on a real engine (DESIGN.md section 8).
- proofs.md C44-C48 repaired after one adversarial read; a second read is owed before any is cited as
  proven.
- The confirmation battery reported (section 3.1). Decisions 1-5 were agreed with Luke on 2026-09-29.
  An external theory review and two panel rounds on it are folded into DESIGN.md (sections 3.3, 3.5,
  4, 5, 8, 9, 10, 12) and section 5 here. The self-reuse check is done and the upcrossing gate
  prototyped (v19_ucheck.py). Next: register decisions 1-3 together with the covariate, ordering and
  gate-validation tests (section 3.1); then the real-engine kill test (section 3.4), then rung 3b.
- Decisions pending with Luke: DESIGN.md section 11.

## 7. Guardrails

- DESIGN.md changes only through a registered test or a decision recorded in its section 11.
- No framing is presented as solved before its registered test passes. Claims carry standing words:
  proved, conditional, measured, exploratory, model-checked, cited, open.
- Registration comes before runs; the runner refuses scored runs without a committed registration
  naming their tag (research/battery-registration-draft.md). Amendments disclose what was seen and
  which way they lean. Confirmatory claims use fresh seeds; 246 and above are fresh.
- Check the regime before trusting a metric: goodput below capacity; saturated throughput on token
  throughput; a KV-bound check (gate-off preempts at least 0.05 per completion) for memory-side claims.
- Every SLO bound is checked against the physical TTFT floor (dispatch lag, prefill, one step) before
  registration.
- Thresholds come from goodput or throughput, never from declared targets; SLOs are the only operator
  input.
- Flow control decides when, the scheduler where. Model admission as an existential per-endpoint check
  feeding a separate placement.
- Timebox theory to one written session before any compute.
- Registration design (from the statistical review):
  - one primary endpoint per registration, such as the default's maximum regret across cells, with a
    simultaneous bound (multiple comparisons with the best, Hsu) in place of post hoc best-arm tests;
  - a power calculation from earlier variances before choosing seeds; ±1% at load 0.95 needs about 30-40
    seeds at the observed noise, or longer windows;
  - paired log-ratios (or a bootstrap cross-check) in place of averaged percentages;
  - a prediction must beat a stated null (for γ₀: a constant), and slope tests are preferred to levels;
  - the scorer reports missing seeds and dropped denominators;
  - every claim in DESIGN.md names its registered test and whether it passed, or says exploratory, and
    marks results from 2,500-request windows.
- Local signed commits of explicit paths only (`git -C <worktree> commit -s -S -- <paths>`), never
  pushed, no AI trailers. Never stage `.err` files.

## 8. Where things live

Worktree `capacity-poc`, branch `capacity-ledger`.

- **admission-paper/**
  - DESIGN.md: what is being built, the model, guarantees, open slots;
  - related-work.md: reading notes and what each source means for the design;
  - paper.md: chapters 1-10; proofs.md: C1-C17, C42, C43;
  - claim-ledger.md: claims and rulings; section 7 is the verification record;
  - saturation-model.md: the step-time line t(n) and the compute side;
  - frontier-model.md: the TPOT-to-stream-cap mapping and the shedding handoff;
  - multi-tenant-extension.md: rung 2;
  - check_proofs.py (vLLM running-list engine; seven mutants); worked_example.py.
- **reserve-reopen/**
  - results-v10.md: every measurement section cited here;
  - research/battery-registration-draft.md: every registration and amendment;
  - v11.py (simulator), v15_errors.py (instrumented runner), v16_*.py (Phase 1, 1b, R, 2),
    v17_rung0.py (rung 0 arms), v18_reuse.py (vLLM-style self-reuse engine), v19_ucheck.py (the
    upcrossing gate);
  - results-v14-round2/: rows, decision logs and evals by phase;
  - tla/ReserveQ.tla; validation-runbook.md; research/round3-design-draft.md.
- **Running scripts:** use a wrapper that cds into reserve-reopen and sets
  `PYTHONPATH=.:../h1-aggregate-forecast`; the sandbox rejects inline PYTHONPATH.
- **Lanes files** (v11*.py, lanes*, theory-lanes.md, results-v11.md, research/lanes-*) belong to the
  lanes session. Import them; do not edit them.
