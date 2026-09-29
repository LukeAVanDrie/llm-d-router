# Capacity admission: design

Working document. It states what the admission design is, the model it rests on, which parts are
decided, and which are open and tied to an experiment. PROGRAM.md holds the sequence of work and its
state; results-v10.md (in reserve-reopen/) holds the measurements. This document points at both and
restates neither.

Every behavioral claim carries a standing:
- **shipped:** merged and on by default on the fork's capacity-ledger branch, file and line given;
- **shipped, flagged:** merged on the fork behind a flag that is off by default;
- **measured:** a registered simulator result, with its results-v10.md section;
- **exploratory:** a simulator reading on data already seen, not registered; a hypothesis for a
  registered test;
- **proven:** a theorem in proofs.md or paper.md, with its premises;
- **proposed:** this design, not yet on the fork or not yet tested;
- **open:** undecided, with the experiment or decision that settles it.

## 1. Scope

The capacity ledger replaces the saturation detectors (utilization and concurrency) as the signal that
tells flow control when to release a queued request. It books every dispatch against the endpoint that
received it, so it knows each endpoint's committed memory, slots and pending prefill without waiting
for a metrics scrape. A per-endpoint rule decides which endpoints may take the request at the head of
the queue.

Out of scope: prefill/decode disaggregation and everything in section 12.

## 2. Operator interface

The operator declares SLOs, or none.
- **With SLOs** (TTFT, TPOT or NTPOT, E2E, per class, composed jointly), the objective is goodput: the
  rate of requests that meet every declared bound.
- **Without SLOs**, the objective is token throughput.

The operator also supplies engine configuration the engine does not export:
- `slotsPerEndpoint`, the engine's max_num_seqs, when the slots axis is gated (shipped,
  `ledger/config.go:56-60`). vLLM exports no metric for it (`ledger/pool.go:294`).
- `ReleaseStepTokens`, the engine's per-step token budget (shipped, flagged, `forecaster.go:63-65`).
  Its default of 2048 is the value the simulator ran under; section 11 lists it as a decision.

KV block size and KV capacity are discovered from the engine's `cache_config_info` metric (shipped,
`datalayer/extractor/metrics/extractor.go:53-54`). A tokenizer before commit gives exact prompt counts;
without one the request's bytes bound the prompt (`filter.go:162-164`).

Nothing else is operator input: not an overflow rate, not the step-cost ratio κ, not hardware
parameters, not workload profiles. Section 3.5 says where the SLOs act and where the rule's one
internal level, γ₀, comes from.

## 3. The model (proposed)

The rule in section 6 and the guarantees in section 5 derive from the model below. Each part carries
its own standing, and PROGRAM.md tests it one decomposition rung at a time.

**Target invariant: requests wait at the router, never inside an engine.** A request held at the
router can still go to whichever endpoint frees first. Once dispatched, any waiting it does is bound to
one engine, either in the engine's queue or after a preemption, and a preemption wastes work.
- The release condition and F check it at dispatch, for compute and memory (the release condition
  has a known gap: it ignores the decode tokens the engine schedules first, section 9).
- The memory rule targets it probabilistically over the window to the next completion. At the
  boundary P₁ = e^(−γ₀), and P₁ is a lower bound on the window's overflow probability (section 3.3),
  so e^(−γ₀) is a level, not a budget on overflow; only a threshold on an upper bound, such as the
  upcrossing bound of section 3.3, reads as one. {γ ≥ γ₀} is not forward-invariant: γ decays between events and
  holding cannot lower occupancy, so the rule is a chance-constrained admission filter, not an
  invariant it enforces. The only set that stays safe with certainty is the ceiling rule's.
- The existential check (section 4) releases a request as soon as some endpoint can take it without
  in-engine waiting, which keeps late binding.
- Booked accounting keeps the invariant enforceable when scrapes lag.

The invariant accounts for these measurements (section 10): the flat optimum below capacity (delay is
conserved, so moving waiting between router and engine changes only waste and binding); the overload
gain (in-engine waiting through preemption burns compute); T going inert at large cache (preemption is
rare there); F's gain over no gate without preemptions (no gate binds requests early into engine
queues); and the detectors' losses (gating on lagging pooled metrics lets engines queue and preempt).

### 3.1 The rule in vector form

Take one endpoint with 1,000 KV blocks, 900 of them committed to 30 decoding residents, and a head
request with a 40-block prompt.

Write the endpoint's state as a vector X over its resources: committed memory, prefill already placed
but not yet run, and slots in use, with capacities C. The head brings a demand vector w: its prompt
blocks, its prompt tokens, and one slot. The rule is two inequalities:

  admit the head on this endpoint  iff  X + w ≤ C  and  γ(X + w) ≥ γ₀.

- **X + w ≤ C** is a plain vector check at the moment of dispatch. Its coordinates are F (memory), the
  release condition (the step's prefill budget) and the slot cap. In SLO mode a fourth coordinate
  limits decode step time (section 3.5).
- **γ(X + w) ≥ γ₀** is the only look-ahead, and it concerns memory alone. γ is, roughly, the number of
  completions expected before the endpoint's memory runs out (section 3.3). The ceiling rule is the
  case γ = ∞: if every request fits at its max_tokens, nothing can run out.
- Flow control releases the head if some endpoint satisfies both (section 4).

In the example, 900 + 40 fits and the prefill budget allows the prompt, so the vector check passes.
The ceiling rule declines (at their max_tokens the residents and the head need 1,150 blocks), so the
answer turns on γ: whether enough of the 30 residents are expected to finish before the remaining 60
blocks are consumed.

Only memory needs a look-ahead because only memory drifts between events. The prefill budget is renewed
every step, and slots change only at arrivals and completions, so a check at dispatch is complete for
both. Memory grows by one token per resident per step and frees only when a request finishes, so its
future can breach the limit with no decision in between.

The rule's checks in this form:

| check | coordinate | horizon | how it arises |
|---|---|---|---|
| release condition | pending prefill | one step | vector check at dispatch |
| F, the fit check | memory | now | vector check at dispatch |
| slot cap | slots | now | vector check at dispatch (when slots are gated) |
| ceiling rule | memory | whole life | γ = ∞: nothing can run out at max_tokens |
| window test | memory | until memory is replenished (section 3.2) | γ ≥ γ₀ |
| TPOT limit (section 3.5) | decode step time | one step | vector check at dispatch; SLO mode only |

The persistence floor and the pooled forecast reserve have no place in this form: both hold room for
growth that γ already counts. This agrees with the measurements (section 10).

### 3.2 The horizon

Between completions an endpoint's residents only grow, so its free memory only falls, and memory
frees only at a completion. The risk that matters for admitting now is therefore the risk before the
next completion, for any length law, provided a completion restores a safe margin: the finished
request must free at least the margin plus one step of the batch's growth (proofs.md C44, the refill
premise). A completion that frees less leaves the next window at risk with no admission to refuse. For
LLM serving a completing request frees its prompt and output, thousands of tokens against a few dozen
tokens of growth per step, so the premise usually holds; it is a premise, not a guarantee. It holds
on average rather than always: on bimodal the margin γ₀·m (about 2,300 tokens) exceeds a typical
completion's footprint (about 2,000), and safety is restored only because an endpoint rarely sits at
zero headroom when a request finishes. Without the premise the bound is θ per decision epoch plus the
probability that a post-completion state falls outside the safe set, a quantity the simulator can log
(the share of completion epochs with γ < γ₀) and the design treats as measured, not assumed.

The general horizon runs until completions have freed at least the margin plus the growth since the
decision. Its risk is the full ruin probability of the memory process (free memory falls at n' tokens
per step and jumps up by each finishing request's footprint: a Cramér-Lundberg process with random
jump sizes), and the window to the first completion is its leading term. The general form matters
only when completions free less than a step of growth, as with many small residents; it is stated for
the paper and not built.

A second, narrower claim is that a held head can become admissible only at a completion, so that
decisions need to be revisited only then. That holds for one endpoint and one head under constant or
decreasing hazard. Under increasing hazard, residents move closer to finishing as they age, so the
risk can fall between completions and a held head can become admissible without one. The rule
therefore re-evaluates a held head on every ledger update (streamed tokens, completions, scrapes), not
only at completions. The router sees streamed tokens and stream closes, not engine steps, so a ledger
update is the finest clock it has.

The compute horizon of one step follows from the engine's semantics: the scheduler re-forms the batch
and refills the prefill budget every step.

### 3.3 The memory process

An endpoint's memory occupancy is a piecewise-deterministic Markov process: between random jumps it
moves deterministically. The flow is growth (each decoding resident adds one token per step); the
jumps are completions and admissions. The state must carry each resident's size and age, because
lengths are not memoryless.

Within T's window the only randomness is which residents finish before each crossing. Growth fixes
the times at which blocks are needed (Lemma H and H'), so the overflow probability reduces to survival
probabilities at known times.

In the vocabulary of ruin theory (a finite dam, or a mirrored Cramér-Lundberg process), free memory
falls deterministically and jumps up at completions. Let H be the free tokens left after admitting
the head, net of unstarted prefill already placed, and n' the number of growing requests including the
head. If nobody finishes, memory runs out after

  t_ruin = H / n' steps,

and overflow happens exactly when no resident finishes before t_ruin:

  P₁ = P(first completion > t_ruin | state) = Π_i S_i(a_i + t_ruin) / S_i(a_i),

where the product runs over all n' growing requests, the head included (age 0), S_i is request i's
survival curve and a_i its age. For a memoryless length law with mean m,
P₁ = e^(−H/m): the resident count cancels, and risk depends only on headroom measured in mean remaining
lengths. A resident whose ceiling falls before t_ruin finishes by then for certain, so its factor is 0
and P₁ = 0 (`reserve_window.go:99-103`); bounded growth near the ceiling needs no separate term.

**γ, the rule's quantity.** The rule works with γ = −ln P₁, so P₁ ≤ θ is the same as γ ≥ ln(1/θ); the
proofs keep θ = e^(−γ₀).
- For independent requests γ is exactly the sum of their cumulative hazards over the t_ruin steps, and
  P₁ = e^(−γ) exactly. The expected number of completions in the window is at most γ, close to it
  when each request's chance of finishing is small. The rule reads, approximately: admit if at least
  γ₀ completions are expected before this endpoint runs out of memory.
- **The resident count cancels.** For a memoryless law γ = H/m_eff: the headroom measured in mean
  remaining output lengths, whatever the number of residents. In operational time (tokens generated
  rather than steps) free memory falls at unit rate and completions arrive at rate 1/m_eff whatever n
  is, which is why n drops out. In the vector picture the memory face of the safe region is flat, at
  C_mem − γ₀·m_eff; residents' ages move it only through departures from memorylessness.
- **Two meanings of m.** The rule itself never uses m: it computes γ from each resident's survival at
  its age. In the explanatory formulas, the margin, the roofline's ceiling and γ₀ use m_eff, the
  inverse of the residents' average hazard, which in steady state is E[S] by Little's law (992 tokens
  on bimodal). The steady-state memory per resident (the ray below) uses the residual mean
  E[S²]/(2E[S]) (1,520 on bimodal).
- A comonotone group (section 3.3's group factor, below) survives as long as its shortest-lived member,
  so it contributes its largest cumulative hazard, not the sum: it counts as one clock.

**Bounds on overflow.** P₁ counts one way to overflow: nobody finishes before the whole batch runs out.
Memory also runs out if a few small residents finish and the rest keep growing. Write Y_t for the memory
held at step t by the residents still alive, with resident i holding b_i(t) = ceil((x_i + t)/B) blocks
(B tokens per block). Overflow over the closed horizon is exactly the union, over sub-batches L of the
residents, of the event that every member of L survives to L's ruin step t_L, the first step at which
L's members alone hold more than C. In tokens t_L = floor(τ_L) + 1 with τ_L = (C − Σ_L x_i)/|L|; in
blocks it is found by search. Each such term, P₁ included, is a lower bound on the overflow
probability, and so is max_t P(Y_t > C). A threshold on P₁ is therefore optimistic as a gate. Three
upper bounds are cheap enough to run:
- **Upcrossings.** At the first overflow step t*, the survivors' footprints one step earlier,
  W_{t*} = Σ_i b_i(t* − 1)·1{R_i > t*}, satisfy W_{t*} ≤ Y_{t*−1} ≤ C < Y_{t*}, and W_t ≤ Y_t on every
  path. So P(overflow) ≤ U(C) = Σ_t [P(Y_t > C) − P(W_t > C)], a sum of differences of two
  one-dimensional weighted Bernoulli tails. Y and W must use the same ceiling function b_i; a
  token-level bound run against a block engine is unsafe because it ignores partly filled blocks.
- **Harris.** Every event {Y_t ≤ C} decreases in the lifetimes, so they are positively associated and
  P(overflow) ≤ H(C) = 1 − Π_t P(Y_t ≤ C).
- **Dyadic.** For step intervals (a, b], a single-event bound B(a, b) on the first overflow falling in
  the interval counts each path at most once. V(a, b) = min(B(a, b), V(a, m) + V(m, b)) is sound and
  never looser than U, because it merges paths that cross, dip and re-cross. On the 10 bimodal
  and atoms states tested (0.01 ≤ G ≤ 1) it equalled U wherever U was below 1, and
  tightened it only where both exceed 1 (exploratory), so it does not remove the bimodal
  conservatism below.

Overflow probability does not rise with capacity, so the gate may take the minimum of these bounds over
capacities C' from the current occupancy Y₀ up to C, each summed from step 1. Below Y₀ neither bound
applies and U is 0, so an unrestricted minimum admits everything. All the bounds assume independent
lifetimes, as P₁ does. Standing: proven (pathwise arguments checked by a three-reviewer panel). The
prototype, reserve-reopen/v19_ucheck.py, is sound on every state tested (exact enumeration of 10,000
small states; Monte Carlo on medium ones) and takes about 0.03 s per decision in NumPy with a P₁
screen and interval refinement. Its conservatism depends on the length law (exploratory, 14
near-boundary states): the bound is 1.05-1.3 times the true overflow probability on geometric and
lognormal laws, 2.2-2.6 with max_tokens atoms near the crossing, and 1.6-3.8 on bimodal, where it
rejected three of six states that were in fact safe. Which statistic the gate thresholds is PROGRAM.md's
decision 1, scored on goodput and throughput at matched realized overflow.

**Two corrections to how γ is computed** (proposed; not yet implemented or measured):
- **Sufficient completions.** Under P₁, a resident whose max_tokens falls inside the window sets
  P₁ = 0 (`reserve_window.go:99-103`), however little it frees: a 50-token request finishing among 30
  growing residents does not rescue the endpoint. The upcrossing bound needs no separate correction,
  because each sub-batch term credits a completion only with the memory it frees.
- **Posterior averaging.** The mean estimate multiplies each resident's mean conditional survival.
  Residents whose hazards come from the same estimated age bin share its uncertainty, so their
  survivals are positively correlated and the product of means is optimistic. For k residents crossing
  a bin whose hazard has a Gamma(α, β) posterior, each exposed for w steps, the exact average is
  E[e^(−k·w·h)] = (1 + k·w/β)^(−α), against e^(−k·w·α/β) for the plug-in. For n residents with
  cumulative hazard Λ each and d events behind the estimate the gap is about n(n − 1)·Λ²/(2d) nats:
  0.12 at n = 50, Λ = 0.1, d = 100, and 0.9-1.0 at d = 5-10 (exploratory calculation), against γ₀
  between 1.5 and 3. P₁ and every "all of a sub-batch survive" term take the closed form. The
  upcrossing computation needs residents conditionally independent, so it integrates the shared
  hazard, and a per-resident attenuated hazard is not a substitute. Plain Gauss-Laguerre nodes are
  wrong for α ≠ 1 (29-100% relative error); the generalized rule with weight x^(α−1)e^(−x) and three
  nodes is accurate for α ≥ 10 but underestimates P₁ in sparse bins (−0.16 nats at α = 0.5 with two
  expected completions), the anti-conservative direction. Sparse bins therefore need more nodes, the
  exact alternating binomial sum (unstable for large counts), or a conservative lower-quantile hazard.
- **Covariates.** One pooled survival curve is optimistic for the class that causes overflow whenever
  footprint and output length are correlated. In a two-class example (4,000-token prompts with outputs
  averaging 1,200 tokens, among 60-token prompts with outputs averaging 10), the pooled gate put the
  overflow probability near 0 where the true value was 0.20 for ten long residents and 0.041 at the
  arrival mix (exploratory). The error sits in young residents, fivefold at age 0 and gone by age 60,
  which is where the gate decides. The proposed fix is coarse log-prompt buckets shrunk toward the
  pooled hazard, with posterior averaging. A prefix-hash covariate is not proposed: it has high
  cardinality, drifts as prompts change, and a tenant can game it. The synthetic laws cannot test this,
  because they draw output length independently of prompt length. The Azure conv2023 cell carries the
  signal (log2 prompt bucket explains 49% of the variance of log output length), and code2023 is a
  negative control (0.3%).

**The estimator's constants.** Four are fixed by convention:
- the hazard past the oldest exposed age is 0 up to max_tokens. It is the lower bound on the hazard,
  so the conservative choice for overflow, and it removes about 0.8% of γ on bimodal (exploratory).
  It needs max_tokens known: when a client omits it the cap is the model length minus the prompt. With
  little data, old residents look immortal and the gate holds;
- geometric age bins with ratio 1.25 (about 37 bins to 4,096 tokens). The occurrence/exposure estimate
  is unbiased over a whole bin, and the error at partial bins is first order in the hazard's slope:
  at most 0.0074 nats per resident on bimodal, up to about 0.25 across 33-42 residents if aligned;
- the fit refreshes on events (completions and censoring) or lazily at decision time with the decay
  applied in closed form. Block size has no bearing on it;
- posterior averaging as above.

Three are registered (PROGRAM.md decision 3), because each trades bias for variance or sets behavior:
- the forgetting horizon. One pool turnover leaves 0.3-1.2 events per old bin, ±15-23% error in γ, and
  an estimate about twice the truth, the anti-conservative direction, for about 2,500 steps after an
  upward length shift. Five turnovers keep the error within about 0.15 nats at γ₀ = 1.5 and lag about
  5,000 steps (exploratory, one bimodal law);
- the prior. It must be proper, in exposure units (one trajectory through a bin of width w contributes
  w), with its mean at or below a low-hazard reference. The choice flips cold start: a vague prior
  holds everything, a prior with the wrong units admits everything (γ = 128 against a truth of 0.85),
  and Jeffreys with no exposure admits everything. Cold-start behavior, hold or admit, is a decision;
- whether to split by prompt bucket. One shared dispersion across age bins, not one per bin: per-bin
  estimates come out positive 26-41% of the time with no real bucket signal and degrade error by
  19-35%, which does not shrink with data; a shared one degrades it by about 4%.

Related estimator work: max_tokens handled as a competing risk rather than censoring, one band per
model and adapter, and a logged calibration curve.

**Which face binds: the capacity roofline.** By Little's law and the memory-time identity (proofs.md
C43), a resident holds on average X̄/n̄ = p̄ + E[S²]/(2E[S]) = p̄ + m tokens in steady state, with
m = ½·E[S]·(1 + CV²) (CV the coefficient of variation of output length). An endpoint's steady state
sits on that ray in the (streams, memory) plane, and

  Φ = C_slots·(p̄ + m) / (C_mem − γ₀·m)

says which face it meets first: Φ < 1, the slot cap, and the memory rule is dormant; Φ > 1, the memory
face, and the rule governs. The variance term is the difference from a naive p̄ + E[S] footprint: on
bimodal m is about 1,520 tokens against a mean output of 992. Φ is a mean; near Φ = 1 fluctuations of
order √K decide, which is the headroom law below. This plays the role for capacity that the roofline
model (Williams, Waterman and Patterson, 2009) plays for compute and bandwidth.

P₁ is T's first chain term (`reserve_window.go:94-95`) and the probability of any overflow in the
window. The victim chain's sum, N_win = Σ_k P_k, is instead an upper bound on the expected distinct
victims (proven: Theorem W, paper.md section 6.4, on the band event, under A1' and Lemma C's premises).
N_win also rests on two conservative choices:
- the chain times t_k are lower bounds on the true crossing times (`reserve_window.go:92-97`);
- survival is read from the upper edge of the learned band (`reserve_window.go:99-104`).

The chain's later terms measure how severe an overflow is, not whether one happens. Under
newest-first preemption the head is the first victim. Preempting it frees only its prompt, which buys
about p/n' more steps before the next crossing, so an overflow takes successive victims until the first
completion arrives. The expected depth therefore grows with the residual wait to the first completion
relative to p̄/n'. Measured: about 1.90 victims per false admission on bimodal and 3.15 on longout, where
residents wait longer for a completion ("The Bayes threshold, offline"). Severity is not constant
across workloads.

Both drift and completions are counted in steps (tokens), so P₁ does not change when step time dilates
with batch size. Wall-clock time enters only the compute side.

Exclusions, stated where they apply:
- Unstarted residents grow in prefill chunks, not one token per step.
- A preempted victim does not leave; it returns carrying its full size. The chain counts victims and
  does not follow them.
- **Correlated lengths.** The product of survival terms assumes residents finish independently. For
  positively associated lengths (one program, session or fork burst), the joint survival is at least
  the product, so independence understates overflow risk. paper.md section 5.1 specifies a group
  factor: residents sharing a program or session key are treated as comonotone, and the group's
  factor is its smallest member's survival bound. That is safe under any dependence within a group.
  Neither the fork's rule (`WindowTest`, `reserve_window.go:68`) nor the simulator implements it; both
  run the independence assumption. Section 8 lists it as an open slot.

The same process, taken unconditionally in its stationary state, gives the headroom law: the Gaussian
tail Φ̄(z) at the Halfin-Whitt margin z = (K - λ·m₁)/√(λ·m₂). T evaluates the process conditionally on
the current residents; the headroom law evaluates its stationary tail. Where the stationary tail is
small, the conditional one is small in almost every state, which is why T goes inert at large cache
(section 10).

### 3.4 Two regimes

**Below capacity, delay is conserved** (measured: "Phase 1"). Across thresholds, mean end-to-end
latency moves by at most 1.1%, and wasted capacity stays under 2% in every arm ("Capacity waste across
the threshold sweep"). The rule cannot change how much requests wait; it changes where they wait:
- at the router, unbound, where the request keeps the option to go to whichever endpoint frees first;
- inside an engine, bound, either in the engine's queue now or after a preemption later.

Every check in section 3.1 refuses to turn router waiting into engine waiting, either with certainty
(release condition, F) or at probability e^(−γ₀) (the window test). The goodput effect of γ₀ is second
order, which is why the optimum is flat and does not move with the SLO set.

**Above capacity, work is conserved.** Compute is saturated, so every token of recompute displaces a
useful token, and memory held idle while requests wait under-fills batches. Throughput is capacity less
these two wastes (exploratory: section 10, "Overload waste accounting").

### 3.5 Where the SLOs act, and γ₀

The SLOs set the limits of the constraints and govern who waits and who is served:
- a TPOT SLO sets the compute limit on decode step time at the next step's batch, through the
  step-time line (saturation-model.md). Step time grows with stream count and with total context (KV
  reads), so the limit is on step time, not on a stream count. Throughput mode has no such limit; the
  engine's max_num_seqs is the only stream cap, since more streams never cost throughput before memory
  binds ("Step-time check"). The line is fitted from engine-side timing where the engine exports it:
  per-scrape deltas of `vllm:iteration_tokens_total` (steps per window) and
  `vllm:inter_token_latency_seconds` (engine clock, unaffected by client lag or stream coalescing).
  Router-side intervals are the fallback, regressed on stream count, context and prefill tokens per
  step, with intervals dropped where the engine's preemption counter moved, since a readmitted victim's
  prefill is invisible to the router. Fitting only on intervals with no pending prefill leaves the
  high-concurrency region almost without data;
- TTFT and TPOT limits are checked jointly for feasibility. A TPOT limit that would push capacity below
  the arrival rate is infeasible; the gate does not shrink the pool to meet it (the failure measured in
  "Test 1"). What happens instead is a decision in section 11;
- ordering (who waits), shedding in overload (who is served), and per-class terms across tenants
  (whose requests are held last, whose are preempted last) are SLO-driven and belong to flow control
  and the sharing pillar.

**γ₀, the rule's one level.** γ₀ is the number of expected completions of headroom the rule insists on.
It is not operator input. Its meaning comes from a balance: holding γ mean remaining lengths of
headroom costs about (m/K)·γ of throughput as idle memory, and an overflow, which happens with
probability e^(−γ), costs c tokens of throughput (the batch-wide stall of recomputing the preempted
request's tokens, about n'·p̄/κ, plus its requeue; proofs.md C48). The sum is smallest at

  γ₀ = ln( K·c / (m·ℓ̄) ),

the logarithm of how much more an overflow costs than holding one mean remaining length of headroom,
with ℓ̄ the mean output length. Because the cost ratio sits inside a logarithm, a threefold error in
either cost moves γ₀ by about 1, which is why outcomes are flat across a wide band (section 10).

- **Standing.** The formula is a heuristic balance under a memoryless law, not a derivation, and it
  prices throughput. The confirmation battery (registered; "Confirmation battery") put the best γ at
  load 1.1 near 3.0 on bimodal and 1.5 on half cache, in the order the formula predicts, and at 2.5-3
  on longout, where it predicted 0.14: its c priced an overflow as one prompt's recompute and omitted the
  cascade through older residents. In consistent units the balance gives γ₀ = b·ln(K/x̄) − k₀, with x̄
  the mean resident footprint: b = 1 if an overflow costs a fixed number of victims, b = 2 if the stall
  grows with the batch, as c ≈ n'·p̄/κ assumes. The slope depends on how recompute scales on the engine.
  It is registered as b ∈ [1, 2], with b = 1 against b = 2 tested on cells with real spread in K/x̄
  (PROGRAM.md decision 2).
- **A structural form.** Repairing the balance's terms gives γ₀ = ln(K·c/(m·ℓ̄)) with
  c = d·n·(p + a)_young/κ: the stall uses κ (a prefill token costs 1/κ of a step), not the step
  budget; the victim is the youngest resident (about 993 tokens on bimodal, 38 on longout), not an
  average one; the depth d counts youngest victims until their freed memory covers the growth to the
  first completion (1.89 on bimodal by Monte Carlo, against 1.90 measured); under self-reuse c shrinks
  by the share of victim recompute saved (about 0.44 on bimodal, not the 0.19 share of all prefill). It
  gives 3.12 / 1.75 / 2.14 against measured optima of 3.0 / 1.5 / 2.5-3 on the recompute engine, and
  predicts the 0.5-1 nat shift under self-reuse. It has no free constant but was fitted after the fact
  on the data it matches, the optima sit on flat plateaus a constant also passes, and the depth
  depends on the gate (the gated depth gives 1.41 on longout). It is a second frozen prediction in
  decision 2, scored on fresh cells. The overflow cost is never estimated online: overflows are rare
  under a working gate, and the ones that pass a higher γ₀ are the costly ones, so the estimate would
  ratchet γ₀ upward.
- **Overshoot.** Admissions happen just after completions, when the state has jumped up by a finished
  request's footprint, and prompts are lumpy, so admitted states usually sit well above γ₀. Realized
  overflow per window is therefore far below e^(−γ₀), which is why nominal θ of 30-70% looked best on
  the upper band and why preemptions run far below θ.
- **The default.** γ₀ = 1.5 with the mean survival estimate (θ = e^(−1.5), about 22%). In overload the
  confirmation battery found it within 0.64% of the best arm in every cell and within ±1% of T at its
  best δ. Below capacity ten seeds did not resolve ±1%; the one resolved miss is bimodal at 0.95, where
  T at δ = −1 beat it by about 1.1%. γ₀ = 2.0 is refuted (half cache at 0.95: −2.7%). Under a
  self-reuse engine that follows vLLM v1's free-block queue, γ₀ = 1.5 stays within 0.1% of the best γ
  in overload in every cell (exploratory; "The self-reuse check"). The formula explains the default and
  its direction, and does not run online.
- **Direction.** γ₀ falls with less cache, with cheaper recompute (a larger κ, as hardware FLOPs
  outgrow memory bandwidth, or victims readmitted from their own cache), and with longer outputs at
  fixed cache (larger m and ℓ̄). Measured for recompute: with self-reuse the best γ moves down, and on
  half cache γ = 3 falls from +2.0% to +0.1% over F in overload. Below capacity a hold costs TTFT rather
  than idle memory, so goodput prefers a lower γ than throughput does, as measured.
- **Where the band narrows.** As m/K grows (small cache, long outputs) the same change in γ moves a
  larger share of capacity, and heavy tails understate risk in the first-order form.

One open risk remains: a workload where the goodput-optimal γ₀ moves with the SLOs (heavy streaming
under maximum-gap SLOs is the likely one). Section 8 lists it.

### 3.6 Optimality (proposed)

Exact optimality is out of reach beyond small instances, so the design aims at asymptotic optimality:
the gap to an upper bound that holds for every policy vanishes, at a stated rate, as the system scales.

**The benchmark.** The memory-time identity (proven: C43, proofs.md) bounds any policy's token
throughput by KV capacity divided by the memory-time per token. A request with prompt p and output S
holds p·S + S²/2 token-steps, so the mean is p̄·E[S] + E[S²]/2: output-length variance consumes
capacity directly. The bound is fluid: it has no preemption and no idle memory, so it is waste-free.
Throughput is also limited by compute (step time grows with resident tokens), so the benchmark is the
smaller of the memory bound and the compute bound, as in Ao et al.'s fluid model (related-work.md).

**Overload.** With a backlog the endpoint sits near full, so only fluctuations within one completion
window matter. Two wastes separate the rule from the waste-free bound: hold waste (memory idle while
requests wait), at most the margin γ₀·m plus one step of growth (n' tokens) plus one prompt, relative
to K; and overflow waste, about e^(−γ₀)·c per decision epoch. The margin term shrinks relative to K; the
one-step term n'/K is about one over the mean resident footprint and does not vanish as K grows
(about 0.07% on bimodal), and c grows with the batch (proofs.md C47, where an earlier vanishing-gap
claim is withdrawn). γ₀ enters the hold waste linearly and the overflow waste exponentially, which is
the balance of section 3.5. F is expected not to converge in overload (its overflow waste does not
shrink); that comparison is asserted, not proved.

**Below capacity, the stationary scale.** Whether memory reaches full at all is a question about the
stationary occupancy, whose fluctuations are of order √K (Halfin-Whitt). That is the headroom law
Φ̄(z), and it governs where T matters.

**Below capacity.** Conjecture: mean delay under any admission policy equals the mean delay of the
waste-free ideal plus a term bounded by the policy's waste, which would make delay conservation a
theorem up to waste. Goodput then depends on how the conserved delay is distributed across requests,
which ordering controls: the achievable per-class delay vectors form a polytope whose vertices are
priority rules (Coffman and Mitrani), for a work-conserving server that does not anticipate service
times; with preemption that premise holds only up to waste. The optimality claim therefore covers a
pair: admission minimizes waste, and ordering picks the vertex the SLOs call for.

**Prior art** (related-work.md). Ao et al. (arXiv 2504.11320) prove that fluid-guided admission
approaches the fluid benchmark for one GPU, below capacity or at its boundary, as arrival rate and time
grow with capacity fixed. Nested WAIT handles unknown lengths by pausing resident requests at segment
boundaries, which a router cannot do, and bounds the probability of any eviction over a horizon at a
buffer cost of ln(horizon/δ). The rule here bounds overflow per admission instead, so evictions
accumulate at rate at most θ per admission. No verified work covers the multi-endpoint layer (late
binding, the existential check, placement) with a proof. For flow time, every deterministic online
algorithm has competitive ratio Ω(√n) even with known lengths (Jaillet et al., arXiv 2502.07115), so
the design claims asymptotic throughput and conservation results, not competitive ratios. G7's proof
can reuse Ao et al.'s tools: Lindley recursion with Kingman's bound, an exponential martingale with
Doob's inequality for the margin, and union bounds over endpoints.

**Overload shedding.** Choosing whom to serve over a reusable resource has LP-ratio guarantees in
online allocation of reusable resources, for example 1 - min(1/2, √(log c / c)) for minimum capacity c
(Feng, Niazadeh and Saberi). Those models do not grow their footprint
and lose rather than hold requests, so they are the template for shedding, not for the rule.

## 4. Contract

**Flow control decides when; the scheduler decides where.** (shipped)
- A hold for the head request walks the endpoints and, when a per-endpoint rule is attached, asks it
  which endpoints may take the request (`pkg/epp/flowcontrol/ledger/pool.go:1080-1112`). The hold is
  granted only if that eligible set is nonempty, and the hold records the set.
- The scheduler's capacity filter restricts placement to the eligible set, with no fail-open past it
  (`pkg/epp/framework/plugins/scheduling/filter/capacity/filter.go:124-138`). Within the set, the
  scorers choose, so prefix affinity keeps working. The filter's own fail-open (`filter.go:153`)
  applies only inside the set.
- The endpoint is charged when the request is placed (record 0043).

Flow control never picks an endpoint. It asks whether some endpoint can take the request, using each
endpoint's own state. That is an existential check over endpoints, so a request is never released into
a pool whose free memory is spread too thinly for any single endpoint to take it.

The contract resembles a runtime safety filter (the Simplex architecture in control; shielding in safe
reinforcement learning) in one respect: flow control restricts the scheduler's choices to an eligible
set and otherwise leaves them alone. It is not a Simplex shield: there is no recovery controller and no
recoverable set, and the filter's condition is a chance constraint, not an invariant. Two gaps between
hold and placement: the capacity filter fails open inside the eligible set when no candidate still fits
(`filter.go:153-158`), a deliberate breach of F on a race; and γ is not rechecked at placement.

Three decisions stack in order, each owned once:
- **ordering** (flow control): which request is the head, and whom to shed in overload; SLO- and
  tenant-driven;
- **admission** (this rule): whether some endpoint can take the head now; outputs the eligible set;
- **placement** (the scheduler): which eligible endpoint; prefix affinity and load.

Admission reads no tenant priority, with one planned exception: per-class victim order and γ₀ across
tenants (rung 2 in PROGRAM.md), where class enters the risk a request imposes on others.

**The per-endpoint KV rule is flagged.** `KVAdmissionReserve` is off by default and marked unruled
(`pkg/epp/flowcontrol/forecast/forecaster.go:60-62`). Without it, the hold checks only that the
request's footprint fits some endpoint's available capacity (`pool.go:1082-1089`). Section 6 describes
the rule as it runs when the flag is on.

**Replicas.** The admission guarantees are single-replica: the ledger's safety argument assumes it sees
every admission to its endpoints. With several router replicas, each replica counts the last scraped
engine occupancy plus its own admissions the scrape does not yet reflect, tracked per lease (a request
dispatched before the scrape but not yet allocated belongs to the own term, so timestamps cannot key
it). Peer admissions become visible only through the scrape, or through a peer-sum channel if one is
configured. The residual over-commitment per endpoint is then at most the peers' admission rate times
(sync interval + scrape age + dispatch-to-allocation lag); the engine's preemption absorbs it, and the
stale-scrape and drift instruments observe it. The target e^(−γ₀) holds per replica, not across the
fleet. The shipped rule is not this one: `used()` takes the larger of the own booked total and the scrape
(`ledger/pool.go:506-520`), so with a second writer the scrape hides the replica's own post-scrape
admissions, and over-commitment grows with the whole pool's admission rate. The CrossReplicaSyncer seam
on main has no aggregating implementation in the tree (only a mock that returns the replica's own
snapshot), reads a store miss as zero load, overwrites the replica's own live counter with the store's
copy, and never expires a departed replica. Missing peer data must fail closed, and departed replicas
must expire, before any peer channel is trusted. Leasing capacity shares to replicas is out of scope
(section 12).

## 5. Guarantees

B0 is the premise that every request on an endpoint was sent by this router.

| id | guarantee | standing and premises |
|---|---|---|
| G1a | **No over-commitment at admission.** The rule never releases a request onto an endpoint where committed memory plus the request's prompt (net of prefix blocks a resident already holds, plus the first output token's slot) exceeds KV capacity, nor past the slot limit when slots are gated. Growth after admission is T's job. | shipped, flagged (`reserve_admission.go:215-259`); exact state and B0 |
| G1b | **Progress.** A held request is eventually admitted. | proven (proofs.md, Theorem R22): one flow; every prompt plus its output ceiling fits the model length; traffic the router did not book does not arrive infinitely often. Model-checked for one endpoint with preemption (reserve-reopen/tla/ReserveQ.tla). The release condition blocks while the engine queues a request the router did not send, so persistent foreign traffic or a stuck victim stops release |
| G1c | **State is never optimistic.** Committed memory is the larger of the booked total and the latest scrape; a stale scrape reads as full. | shipped, flagged (`reserve_admission.go:245`); shipped (`ledger/admission.go:122-128`); B0 |
| G2 | **Overflow compensator.** Expected overflowing windows are at most the expected sum of e^(−γ) over decision epochs (admissions, completions, and engine readmissions of preempted requests), each evaluated just after the epoch. The right-hand side is computable online, so it is also a monitor against the engine's preemption counter. | proposed theorem (a counting-process compensator with optional stopping; proofs.md C44 holds the earlier form); premises: true survival, independence, exact state, one router. The label is the closed-gate overflow, a proxy for engine preemptions |
| G2z | **Zero-risk release.** No head is held while some released endpoint can take it with every resident and the head fitting at their max_tokens ceilings. | proposed: follows from the ceiling rule once the persistence floor is removed (section 6, step 3) |
| G3 | **Monotone in free memory.** At fixed residents and ages, if admitting is optimal, it stays optimal with more free memory. | open, proof target (coupling). The stronger claim, monotone under removing a resident, has violations in the small MDP (28 of 9,846 pairs in one instance) unless those are solver ties, to check |
| G4 | **Calibrator regret** grows like √((1 + d)·T) with label delay d, which sets its step size. | open; cite online learning with delayed feedback |
| G5 | **Multi-replica bound.** Without B0, with each replica counting the scrape plus its own admissions the scrape does not yet reflect, over-commitment in blocks is at most the other replicas' admission rate × (sync interval + scrape age + dispatch-to-allocation lag) × the largest prompt footprint. | proposed (section 4, "Replicas"); the shipped max rule does not meet it (its bound uses the whole pool's admission rate); extends G1c |
| G6 | **The group factor is safe** under any dependence within a group. | proven (paper.md section 5.1: a joint survival never exceeds its smallest marginal); not implemented (section 3.3) |
| G7 | **Overload loss bounded by the margin and the overflow cost.** Relative loss at most (m·ln(1/θ) + n' + p_max)/K plus the overflow cost at θ per epoch. | proof sketch (C47); the earlier claim that the gap vanishes as K grows is withdrawn: the n'/K term (one step of growth) is constant, about 0.07% on bimodal, and the stall cost grows with the batch. A consistent one-unit bound and F's constant loss are open |
| G8 | **Memory-time conservation.** Below capacity, the memory-time workload is conserved up to idle memory and recompute, on every path (C43). | follows from C43. The earlier claim, mean delay within a waste-bounded term of the waste-free ideal, is withdrawn: delay-conservation laws need work-conserving, non-anticipating service, and admission changes which requests are resident |
| G9 | **Holding what would wait inside an engine loses nothing.** Every dispatch policy has a counterpart that never creates engine-side waiting, with the same engine trajectories, plus the option to use another endpoint. | proof sketch (C45); premises: waiting requests hold no KV or compute, engines serve in order, zero dispatch lag, the router can dispatch past a held request, and the router knows exactly when the engine would start the request. The design breaks the last two: head-of-line holding (measured 3-10% of blocked time at load 1.3) and F's conservative charge, which can refuse a request the engine would start |
| G10 | **Holding pays only through the externality.** Under newest-first preemption, if an overflow costs only the preempted request (its tokens are kept), admitting everything that fits is throughput-optimal; holding helps only through the recompute stall a victim imposes on the whole batch. | proposed theorem (C48's coupling plus an interchange argument); shown numerically in the small MDP. Throughput only: under SLOs the victim's own stall can miss its target |
| G11 | **Bounds on overflow.** Over the closed horizon, P₁ ≤ max_t P(Y_t > C) ≤ P(overflow) ≤ min(1, min over C' in [Y₀, C] of min(U(C'), H(C'))), and the dyadic bound V(C) ≤ U(C) is also an upper bound (section 3.3). | proven (pathwise arguments; exact enumeration on 10,000 small states and Monte Carlo on medium ones found no violation); premises: independent lifetimes, true survival, exact state, Y₀ ≤ C, survival known out to every max_tokens, and block counts when memory is allocated in blocks |

G2 alone is satisfied by holding everything; G2z and the fixed θ inside the plateau are what exclude
that.

## 6. Default mechanism

Per endpoint, in order, for the head request with prompt p (all quantities in KV blocks). Steps 1-5
are shipped, flagged; proposed changes are marked.

1. **Release condition.** The engine queues no request the router did not send (a preempted victim or
   foreign traffic would block everything behind it), and either nothing is pending or the unstarted
   prefill already placed there plus p fits one step's budget. The empty-queue exception lets a prompt
   larger than the budget go when it is alone. (`reserve_admission.go:199-209`)
2. **F, the fit check.** Committed memory (booked residents at their current size, prompt blocks
   shared across residents counted once, raised to the scrape if the scrape is higher) plus p's
   blocks, net of blocks already held and including the first output token's slot, fits the capacity.
   An endpoint with no residents takes any head that fits alone, and the remaining steps do not apply
   (the serial exception). (`reserve_admission.go:215-259`)
3. **Persistence floor.** The fit also leaves a derived floor free (`reserve_admission.go:260-268`).
   Proposed: removed from the default (section 3.1; decision in section 11).
4. **Ceiling rule.** If every resident and the head fit at their max_tokens ceilings, admit. This needs
   no length law. (`forecast/reserve_window.go:42-51`)
5. **T, the window test.** Otherwise, admit if the expected head start the request gains by entering
   now outweighs the expected recompute of the newest-first victim chain up to the next completion.
   - Formally, T admits when κ·G_u ≥ λ_h·n_u·Σ_k P_k·x̄_k, where G_u is the expected tokens the head
     generates before the next completion, P_k the chain's probability of reaching its k-th victim in
     that window, x̄_k that victim's expected recompute size, n_u the number of growing requests, and
     λ_h = e^δ. The derivation is in paper.md (the victim chain and Lemma H) and proofs.md.
   - On the fork, κ is its floor, the resident count plus one (`reserve_admission.go:304`), and δ = 0.
   - Proposed default: T becomes γ ≥ γ₀ (section 3.3) with the mean survival estimate and γ₀ = 1.5
     fixed (section 3.5; equivalently P₁ ≤ e^(−1.5), about 22%). T's pricing (κ, G, the chain's severity
     weighting, δ calibration) is a candidate refinement: it enters the default only by beating the
     simple rule by more than the plateau's width on a registered test (section 8). If it does, κ must
     be constant in n.
6. **TPOT limit** (proposed, SLO mode only). The endpoint's step time at its next batch, including the
   head, stays within the limit its TPOT SLO sets (section 3.5).

**Illustrative example.** An endpoint with 1,000 blocks has 900 committed. The head has a 40-block
prompt, and nothing is pending, so the release condition holds. F passes: 900 + 40 is under 1,000. At
their ceilings the residents and the head need 1,150 blocks, so the ceiling rule declines. The window
test then asks how many of the 31 growing requests are expected to finish before they consume the
remaining 60 blocks (γ), and admits if that is at least γ₀ = 1.5. Suppose output lengths cluster around
400 tokens (an increasing hazard). If most residents have generated 50 tokens, few will finish in time,
γ is small, and the request is held. If most have generated 380 tokens, several will finish soon, γ is
large, and the request is admitted. Under a heavy-tailed law the ordering reverses: a resident that has
already run long is expected to run longer.

## 7. How the simulator maps to the code

The simulator (reserve-reopen/v11.py, run through v15_errors.py) is the evidence base. It matches the
contract in section 4 and differs from the fork's rule in the places below. Two gates are measured: T
with the engine's κ at thresholds δ (arms truek_d*; δ = -2 is truek_dm2), and the proposed window rule
γ ≥ γ₀ on the mean or upper-band survival estimate (arms p1m_t* and p1u_t*, the suffix being θ = e^(−γ₀)
in percent; v17_rung0.py).

| | fork | simulator's measured gate |
|---|---|---|
| when | eligible set nonempty | the same: `place()` walks endpoints and takes the first eligible |
| where | scheduler scorers inside the set | shortest queue (waiting plus running) inside the set (`v14_round2.py:32`, `v11.py:406`) |
| release condition | `reserve_admission.go:203` | the same condition (`v11.py:352`) |
| F | max(booked, scraped) | engine truth (exact by construction) |
| persistence floor | derived floor | none |
| window test | T: κ floor n + 1, δ = 0 | T: engine κ0 = 150, δ swept; the proposed rule: γ ≥ γ₀ swept, γ₀ = 1.5 as the default arm |
| group factor | none | none |
| prefix affinity | scorer | none in the measured workloads |
| endpoints | any | 4 in every measured cell except Phase 1b's 16; the simulator takes E as a parameter (`v11.py:220`) |

Consequences:
- **T's price.** The fork's configuration is the one the simulator found mispriced: with the κ floor, T
  gave up 1.6% goodput to no gate on bimodal; with the engine's κ it gained 3.2% ("True κ in T"). The
  shipped T is not safe to enable on KV-bound pools below capacity until κ changes (section 9). In
  overload the κ-floor rule matches the calibrated gate's throughput ("Phase R").
- **The floor.** The fork's floor removed about 40% of preemptions on bimodal for 0.39 points of
  throughput ("Step 0"), and the throughput-optimal reserve is essentially zero ("Reserve sweep"). The
  measured gate has no floor.
- **State.** The simulator's F reads engine truth. Phase 2 measures what the fork's max(booked,
  scraped) costs against truth.
- **Placement.** Production places by its scorers, prefix affinity above all. The production form of
  the when/where interaction, a cache-hit placement sending a request to the eligible endpoint with the
  least growth headroom, is not simulated.
- **Step cost.** The simulator charges every prefill token linearly (`v11.py:147-149`), recompute
  included. On a GPU, chunked prefill can ride along with memory-bound decode steps at lower cost, which
  would shrink the overload gains measured here.

## 8. Open slots

Each slot is a mechanism with a default and an experiment. Filling one changes no guarantee. The rung
column names the PROGRAM.md decomposition rung that settles it.

| slot | default | open question | decided by | rung |
|---|---|---|---|---|
| T's pricing | P₁ ≤ θ (proposed) | Rung 0 and its extension: at their best settings T and P₁ ≤ θ tie in every cell. The confirmation battery: T at a fixed δ = −1 is the best arm or within 0.2% of it in five of six cell-loads, while a fixed γ needs per-cell values to match; the differences are 0.4-1.1%, all in T's favour, and self-reuse does not separate them. Decision 4 (PROGRAM.md) deletes T's victim chain under the γ rule. Do κ, G, the chain's severity weighting or δ calibration beat P₁ ≤ θ by more than the plateau's width? Near the threshold G and the size terms carry about a fifth to a quarter of T's score variance (section 10), but outcomes are flat across thresholds, so ranking agreement cannot settle it | matched-overflow-rate comparison of P₁ ≤ θ against full T on bimodal, half cache and a heavy-tailed law | 0 |
| γ₀ | 1.5, mean survival estimate (proposed) | The confirmation battery held γ₀ = 1.5 within 0.64% of the best arm in overload and did not resolve ±1% below capacity; the best γ differs by cell (section 3.5). Does γ₀ = b·ln(K/x̄) − k₀ with b ∈ [1, 2] predict the cell optima better than a constant, and is b nearer 1 or 2? | registration on at least 30 fresh seeds per cell plus out-of-sample cells with spread in K/x̄ (bimodal at 2x cache, a long-context law, a heavy tail, a cheap-recompute cell) | 0 |
| the look-ahead's statistic | P₁ ≤ e^(−γ₀) (a lower bound on overflow) | Does a threshold on the upcrossing bound (section 3.3) beat the P₁ gate repaired with the deterministic-exit ruin horizon? The bound is sound but 1.6-3.8 times the true overflow probability on bimodal, so a tighter bound can still lose goodput | goodput and throughput at matched realized overflow; each statistic gets its own level | 0 |
| survival estimator | mean band, refreshed every 25 completions | A decode-step clock, Gamma-Poisson hazards on geometric age bins with posterior averaging (section 3.3) | registered with the statistic comparison; an upward length shift for the clock | 0, 3 |
| prompt-length covariates | one pooled curve per endpoint | Do coarse log-prompt buckets, shrunk toward the pooled hazard with posterior averaging, lift goodput where output length depends on prompt length, and cost little where it does not? | conv2023 cell in arrival order (lift), code2023 and the synthetic laws (negative controls); calibration of S at ages up to 30 and gate calibration by bin, goodput at matched overflow; a same-bucket burst stress | 0 |
| ordering below capacity | FIFO | Registered PL3 failed through storms of requests waiting at the router, not through short requests. EDF on arrival minus predicted prefill serves long prompts first and adds misses. Does another order recover the gap? | thresholds crossed with FIFO, EDF, shortest-prompt-first with aging, feasibility-aware least slack (hopeless requests last; up to about 44% of the gap in replay) and deadline shedding; scored on base-SLO goodput and hold-episode length | 0 |
| overflow severity | population average | How many victims does an overflow take, per workload? Measured 1.90 per false admission on bimodal and 3.15 on longout | victims per overflow episode, logged | 0 |
| cached blocks in scraped usage | max(booked, scraped) | Settled for vLLM v1 (checked in source by the review panel): usage is 1 − free/(blocks − 1) and unreferenced cached blocks sit in the free queue, so max() does not read evictable cache as committed. Caveats: the logged value averages across data-parallel engines. Hybrid and sliding-window models keep one shared block pool, so capacity is counted in blocks; a sliding window W caps total context, so a resident's growth over τ steps is min(τ, max(0, W − p − a)) tokens, rounded to blocks, and interleaved models sum per layer group | done | 1 |
| recompute cost on a real engine | linear, full recompute (simulator) | vLLM v1 keeps a preempted request's full blocks hashed at the tail of the free queue, so a victim readmitted soon mostly hits its own cache; chunked prefill also rides on memory-bound decode steps. The batch-stall cost c may be 5-10x below the simulator's, which by G10 and section 3.5 would push γ₀ toward F alone. The simulator's self-reuse engine, which follows vLLM v1's free-block queue, halves the gain over F and leaves it positive (overload at γ₀ = 1.5: bimodal +5.4% to +3.0%, half cache +2.6% to +1.2%; exploratory, "The self-reuse check") | done: the self-reuse check; next, one vLLM on one GPU under forced overload measuring recompute tokens per preemption with prefix caching on (if the implied γ₀ is at most about 0.5, ship F plus the ceiling rule) | 0, real engine |
| fixed-ratio baseline | none | SGLang reserves min(remaining max_new_tokens, 4096) × new_token_ratio for each running request; Llumnix dispatches to the highest freeness (M − ΣV)/B, which is t_ruin. Does the age-conditioned P₁ beat a tuned fixed ratio at the router? If it only ties, the contribution is the plateau and conservation finding, not the rule | a fixed-ratio arm in the simulator | 0 |
| γ₀ and SLOs | fixed γ₀ | Does any workload move the goodput-optimal γ₀ with the SLO set? | a maximum-gap SLO cell under heavy streaming | 2 |
| TPOT limit | none | Does the joint TTFT/TPOT feasibility check cap streams without the Test 1 collapse? | registered arm at loads 0.95 and 1.1 | 0 |
| group factor | none (independence) | Does independence under-predict overflow on correlated lengths, and does the comonotone factor over-hold? Where does the group key come from on the request path? | correlated-length cell; group key design | 3b |
| κ | floor n + 1 (fork) | Must be constant in n if G stays. Source: the fitted step-time line's a/b_pf, else an engine-class constant (about 150 for bf16 dense models on H100-class hardware, from the roofline; unvalidated). Calibration absorbs a constant once converged ("Ship comparison", P-S3) | real-engine fit; moot if G leaves | 0, real engine |
| state estimate | max(booked, scraped) | Is max() enough, or does an observer recover what max() loses under prefix sharing? Does the gate stay stable as scrape delay grows? | Phase 2 (registered) | 1 |
| pooled gates | band viability on pooled `Saturation()` (`controller/internal/processor.go:419-436`); pooled ceiling and forecast reserve in the hold (`ledger/pool.go:1053-1067`) | The pooled forecast reserve duplicates T and may hold too little on a fragmented pool. Below a ceiling of 1 the band ceilings are the usage-limit holdback, by intent | proposed: forecast reserve off when a per-endpoint rule is attached (section 9) | 1 |
| F over no gate | none | The 3.3-3.8% at 2x and 4x cache: late binding or prefill pacing? At one endpoint there is no late binding, so the gain there is pacing alone | E = 1 cell; then F without the release condition at E = 4 if needed | 0, 1 |
| attribution | none | How much of the gate's win over the detectors comes from scrape staleness, commitment accounting, and the per-endpoint predicate? | three arms (utilization detector with a scrape at every event; its pool-mean predicate fed booked state; the gate). Precondition: check the emulation against llm-d's detector code path | 1 |
| upstream baseline | none | Against the optimized baseline (llm-d repository, guides/optimized-baseline: no flow control, prefix-affinity filter plus a token-load scorer), does holding at the router keep more cache hits? | prefix-affinity scorer in the simulator, then a registered comparison | 1 |
| overload | release when eligible, FIFO | Which requests to shed, in what order to serve | flow control's shedding work | 2 |

**Inputs by observability.** Each input the rule uses, by where it comes from:

| tier | inputs | notes |
|---|---|---|
| scraped from the engine | KV capacity and block size (`cache_config_info`), KV usage, running and waiting counts; with new scrape specs, the preemption counter, `iteration_tokens_total` and inter-token latency | K is ambiguous on hybrid models, where attention and state share the pool |
| measured by the router | prompt lengths, max_tokens, completed output lengths (from the usage block), resident ages, stream event times per endpoint | ages count stream events, which undercount tokens under speculative decoding and `stream_interval` > 1; clients that omit `include_usage` report no length; non-streaming requests have no age until they finish |
| configured or calibrated | the step token budget and max_num_seqs (operator config, section 2); κ (calibrated from the step metrics where scraped, an engine-class constant otherwise; not operator input) | vLLM exports neither the budget nor max_num_seqs; inferring the budget from the iteration histogram is off by up to 2x |
| unobservable in stock vLLM | the share of a victim's recompute saved by its own cache; the cost of one overflow episode | vLLM counts victims' cache hits separately and does not export them; the preemption counter carries no size, timing or episode |

**Stall detection** (PROGRAM.md decision 4). A stream stalls inside an engine when it is silent while its
siblings on the same engine keep producing events. Counting sibling events cancels shared step-time
variation, so the threshold is nearly deterministic: `stream_interval` plus the longest detokenizer
holdback plus the largest speculative burst per step, with a small margin for chunk merging; these are
engine configuration. It needs at least two streaming siblings on one engine, so it fails with a single
resident, data-parallel engines behind one endpoint, several router replicas, and non-streaming
traffic, and it cannot tell a preempted stream from one held back by a slow client. The engine's
waiting-by-reason and preemption counts stay as the cross-check.

## 9. Changes the design asks of the fork

- T replaced by γ ≥ γ₀ = 1.5 on the mean survival estimate, if the confirmation battery reproduces the
  tie; otherwise T's κ from the
  floor n + 1 to a constant in n.
- The group factor in `WindowTest`, with a group key on the request (section 8).
- The pooled forecast reserve off whenever a per-endpoint KV rule is attached; the band ceilings stay
  as the usage-limit holdback.
- The observer correction, if Phase 2 shows max() loses enough to need it.
- Found by the review panel (code-verified where marked):
  - the release condition counts decode tokens against the step budget: pending + prompt + growing
    requests ≤ budget (verified: `reserve_admission.go:207-209` omits them);
  - on a race at placement, requeue instead of failing open, and recheck γ (verified: `filter.go:153`);
  - stop crediting every completion inside the window (verified: `reserve_window.go:99-103`): count
    only sufficient completions under P₁, or adopt the upcrossing bound, which needs no correction,
    if decision 1 registers it;
  - average survival over the hazard posterior rather than multiplying posterior means (section 3.3);
  - replace the "engine waiting > unstarted" test, which masks victims during long prefills, blocks on
    stale scrapes and deadlocks with several router replicas, with the engine's waiting-by-reason
    counts and a rise in its preemption counter;
  - read vLLM's scheduling policy at startup and refuse the newest-first assumption under PRIORITY;
  - one survival band per model and adapter;
  - disable the rule explicitly on P/D decode pods, where pending prefill is meaningless and waiting
    requests hold blocks;
  - net the head's prompt against held prefix hashes in the release condition, as F does for memory;
  - with several replicas, count committed memory as the scrape plus the replica's own unreflected
    admissions instead of the larger of the two (`ledger/pool.go:506-520`), and add a peer-sum term
    only from a syncer that fails closed on a miss, keeps the local live counter, and expires departed
    replicas (section 4, "Replicas"); until then, document that the guarantees assume one replica.
- The decisions in section 11, once made.

## 10. Evidence and its limits

Each item names its results-v10.md section. All of it is simulation of a vLLM-shaped engine with a
linear step-time model, recompute on preemption, and synthetic length laws. Almost all of it is at four
endpoints, one tenant and a fixed length law. No real-engine result exists. Standing is stated per item:
a registered test and whether it passed, or exploratory; results from 2,500-request windows are marked,
because that window has flipped a sign before ("Step-time check"). The headline magnitudes assume no
cache reuse by preempted requests; with a self-reuse engine that follows vLLM v1's free-block queue the
gains over F roughly halve (below).

- **Delay is conserved below capacity** ("Phase 1", registered E1 passed within its ±3% bound). The
  largest shift is a point estimate of 1.1% (F: +1.09% [+0.38, +1.80]); 1.3% work-weighted. In overload
  this fails.
- **The best threshold did not move with the SLO set** ("Phase 1", bimodal only). The registered test
  (E2a) failed to flip a sign; that is failure to reject, not an equivalence test, and the TTFT-tight set
  was infeasible in two cells.
- **The throughput optimum is flat in overload** (registered PL1, PL2, PL4 passed at 10,000 requests).
  Below capacity it is not flat at δ = 0: registered PL3 failed, with δ = 0 at -2.31% on goodput. The
  earlier "δ ∈ {0, -1, -2} tied" came from an exploratory analysis on seen seeds ("Interventional
  marginal costs").
- **T goes inert where the overflow tail is small** ("Phase 1b"). On bimodal, T's gain over F is +10% at
  half cache, +7.6% at the base cache, and zero at 2x and 4x, where T admits exactly what F admits.
- **The headroom law** ("Phase 1b", exploratory in effect). Φ̄(z) orders the variants by T's gain over F,
  but Z1 rests on four points, two tied at zero, so any monotone function of cache size ranks them the
  same; the 16-endpoint point is post hoc, and the law does no better than the raw preemption count
  (Z2).
- **Overload throughput** ("Phase R", 10,000 requests). Registered: R1 (the gate over F on bimodal at
  1.1, +5.61%) and R3 (the κ = n rule over no gate at 1.1, bimodal +5.38% and longctx +5.13%) passed. The
  gate's 5.4-5.6% over no gate at 1.3 is reported, not registered.
- **The throughput plateau in overload** (exploratory; Phase 1 rows at load 1.1, 2,500 requests, seeds
  161-175). Throughput by δ on bimodal: 61.89, 61.87, 61.56, 60.58, 60.18 for δ = 0, -1, -2, -3, -5;
  longout and theta01kv are flat within 0.1 across the sweep, and longctx does not overflow. The top of
  the plateau spans δ = 0 to -1 (the difference is about two requests in the window); throughput falls
  past about -2. At 10,000 requests the κ-floor rule and δ = -2 differ by 0.2% ("Phase R").
- **Rung 0 front battery** (registered, "Rung 0 front battery", 10,000 requests, seeds 246-255). The
  throughput plateau holds at four endpoints and at one (δ = 0 and -1 within 0.2% of the best at load
  1.1; δ = -3 costs 2.6%). Below capacity the goodput plateau is δ = -1 to -2; δ = 0 is 2.3% lower. The
  simple rule P₁ ≤ θ is never behind T: it ties on bimodal at the base cache (θ = 10-30% with the
  band's upper edge, 10% with its mean) and on longout, and on half cache P₁ at θ = 30% beats T at
  δ = -2 by 7% in goodput and 1.2% in throughput with half the preemptions. The matched comparison
  failed formally (SF1 on interval width, SF2's direction on half cache) because T preempts more than
  the grid's most permissive arm; the grid is extended in the next registration. About two victims per
  overflow on bimodal and 2.2-4.3 on longout.
- **Rung 0 extension** (exploratory, three development seeds, "Rung 0 extension"). T at its best
  threshold ties P₁ ≤ θ on half cache (δ = -1 within 0.6% in goodput, δ = 0 within 0.03% in
  throughput), so rung 0's half-cache gap was δ = -2 being too permissive there. The P₁ plateau is wide
  (on half cache, θ = 30-70% upper band or 15-40% mean within about 1.4% of the best) with a right edge
  on bimodal in overload. One fixed setting, the upper band at θ = 30%, is within 0.6% of the best arm
  in all six cell-load pairs; the mean estimate at 20-30% is within about 1%.
- **The small MDP yardstick** (exploratory, "The small MDP yardstick"). One endpoint, solved exactly.
  With the recompute cost charged to the victim alone, F is optimal and holding never pays; holding
  pays only through the batch-wide recompute stall. Where it does (the stall relative to a request's
  output, ρ, about 0.3 or more), one threshold on P₁ is within 0-1.8% of optimal at ρ = 0.3 and 2.3-6.9%
  at ρ = 1, at least as good as a headroom threshold, and the admit region is monotone almost
  everywhere. The toy's m/K is about 0.25, where the plateau is narrow, as the cushion predicts.
- **Confirmation battery** (registered confirm-2026-09-28, seeds 256-265, 10,000 requests;
  "Confirmation battery"). C1 and C2 failed (bimodal and longout at 0.95 on C1; bimodal on its estimate
  and the others on width on C2), C4 failed on bimodal at γ = 1.0, and C3 passed weakly. In overload
  γ₀ = 1.5 is within 0.64% of the best arm in every cell. The best γ differs by cell (bimodal 2-3, half
  cache 1.5, longout 2.5-3); the frozen formula missed longout (predicted 0.14). T at δ = −1 is the
  best arm or within 0.2% of it in five of six cell-loads.
- **The self-reuse check** (exploratory, seeds 104-108, 10,000 requests; "The self-reuse check"). With
  victims readmitted from whatever of their cache survives vLLM v1's free-block queue, the gain of
  γ₀ = 1.5 over F in overload falls from +5.4% to +3.0% on bimodal and from +2.6% to +1.2% on half
  cache; longout does not resolve at five seeds. The best γ moves down, γ₀ = 1.5 stays within 0.1% of
  it in overload, and T at δ = −1 still tracks γ₀ = 1.5.
- **Overflow severity** ("The Bayes threshold, offline"). About 1.90 victims per false admission on
  bimodal and 3.15 on longout.
- **Overload waste accounting** (exploratory; Phase R rows, seeds 221-230). Predicting each overload
  throughput gap from the arms' recompute shares of compute alone gives +6.0-6.5% where +5.4-5.7% was
  measured (gate over no gate, four cell-load pairs) and +4.3-6.1% against +3.7-5.6% (gate over F). On
  bimodal at 1.1, F spends 8.9% of compute on recompute and the gate 3.3%. The κ-floor rule spends 0.8%
  and matches the gate's throughput; it leaves about three times as much memory idle while requests
  wait. PROGRAM.md's negative results record that "gain ≤ gate-off's waste" failed as a theorem on
  2,500-request windows; at 10,000 requests the gain stays below the recompute share removed in all
  eight pairs. Needs a registration on fresh seeds.
- **The decision score** (exploratory; classifier logs, bimodal, seeds 76-90, 36,991 decisions). T's
  log score ranks decisions like -log N_win (Spearman 0.996). Near the threshold (score in [-3, 1],
  9,348 decisions) the standard deviations are 1.25 for log N_win, 0.48 for log G and 0.46 for the size
  term log(cost/N_win). The median N_win at the decision boundary is 0.13 at δ = 0, 0.36 at δ = -1 and
  0.97 at δ = -2.
- **Against the shipped detectors** (not a claim). "About +10%" at 2,500 requests rests on detectors
  tuned in hindsight; at 10,000 requests registered R2 failed on bimodal, the emulations show zero
  goodput on 5 of 10 (bimodal) and 9 of 10 (longctx) seeds, which looks like an emulation or regime
  artefact, and the emulation itself has not been checked against llm-d's code path. Attribution is
  unmeasured.
- **F over no gate** ("Phase 1b"). At 2x and 4x cache, where T is inert, F beats no gate by 3.3-3.8%
  with no preemptions in either arm. The explanation (late binding or prefill pacing) is unseparated.

## 11. Decisions for Luke

- **The persistence floor:** delete it, or keep it off by default as an optional stall dial. The
  evidence (section 7) supports removing it from the default, and G2z needs it gone.
- **`ReleaseStepTokens`:** make it required config with no default, like `slotsPerEndpoint`, since
  vLLM's max_num_batched_tokens varies by version and configuration and is not exported; keep a
  default and document it; or infer it from the upper tail of `vllm:iteration_tokens_total` (the engine
  exports that histogram, not the configured budget). vLLM's OpenAI-server defaults on main are 8192 on
  H100/H200-class GPUs and 16384 on B200-class, against the fork's 2048, which under-releases H100 pools
  about fourfold.
- **The shadow-mode bar:** what shadow mode must show before gating is turned on. Candidates, to be
  registered before it runs: the gate's predicted overflow risk against the engine's preemption counter
  per endpoint; the share of would-be-refused dispatches that the engine then preempted or queued,
  against the same share for admitted ones; the scrape-mismatch distribution; the refusal rate on the
  benchmark guide's workload. The fork's shadow mode (`forecaster.go:69-71`) decides and exports
  without gating.
- **TTFT and TPOT jointly infeasible:** when meeting the TPOT limit would push capacity below the
  arrival rate, either shed (flow control refuses a share of arrivals; under all-or-nothing SLOs this
  keeps goodput for the admitted share) or relax the TPOT limit (every request degrades gradually).
  Throughput mode has no TPOT limit, so the choice arises only when SLOs are declared.
- **The group key:** which request field identifies a program or session for the group factor, and
  what the rule does when it is absent. Grouping by shared prompt prefix is ruled out for prefixes
  common across unrelated users (system prompts): it would build large comonotone groups and over-hold.
  Grouping by client address is ruled out for the same reason (unrelated users behind one address).
  Candidates are a session header, or the part of a shared prefix beyond the common system prompt.

## 12. Out of scope

- A per-decision rule that picks δ from the SLOs per request: shelved on the Phase 1 evidence.
- Router-queue ordering and shedding policy: flow control's, studied separately; section 3.5 states
  only where they meet admission.
- Prefill/decode disaggregation. The decode sidecar issues the prefill leg, which the router never
  sees; prefill pods hold blocks under a lease until decode pulls them; decode pods hold blocks for
  transfers that do not grow.
- Leasing capacity shares across router replicas. A replica's share of the headroom falls below one
  request's footprint near saturation, where the rule matters, and computing the headroom needs the
  same reconciliation of unreflected admissions that section 4's rule already does. Section 4,
  "Replicas", states the multi-replica scope.
- Upgrading non-streaming requests to streaming at the gateway (reassembling tool calls, n > 1,
  logprobs and usage): a gateway feature. Without streaming the router sees no ages, so the rule treats
  such a request by its max_tokens ceiling.
