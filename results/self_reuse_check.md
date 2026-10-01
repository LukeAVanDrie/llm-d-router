# Results: the self-reuse check

Standing: exploratory and not preregistered. These are simulated results only (5 paired seeds,
10,000 requests per run) and have not been validated on a real engine.
`examples/self_reuse_check.sh` reruns them, and `python tests/parity.py full` checks that this
package reproduces the 360 rows behind every column except T exactly. The research run had 420
rows; the 60 for T (`truek_dm1`, an older priced look-ahead) are not reproducible here because
that policy is not part of this package.

The section below is copied unchanged from the research log, so it uses the research names:

| research name | policy in this package |
|---|---|
| no gate (arm `gateoff`) | `nogate` |
| F (arm `late`) | `fit` |
| gamma = 1, 1.5, 2, 3 (arms `p1m_t36.79`, `p1m_t22.31`, `p1m_t13.53`, `p1m_t4.98`) | `lookahead:36.79`, `lookahead:22.31`, `lookahead:13.53`, `lookahead:4.98` |
| T at delta = -1 (arm `truek_dm1`) | not in this package |

The look-ahead threshold is exp(-gamma) in percent. Half cache means cell `bimodal` at kf 0.5. P1 is
the probability that no resident finishes before the endpoint's free memory runs out. The balance
formula and the kill test named at the end belong to the wider research and are not in this package.

Correction to the reading below: "gamma = 1.5 stays within 0.1% of the best gamma in overload in
every cell under both engines" holds for vllmreuse only. With recompute, the tables show gamma = 1.5
0.7 points below gamma = 3 on bimodal (+5.3 against +6.0) and 0.5 below on longout.

Relation to the paper's Section 7.3, suite 4. This package reproduces these claims: gamma0 = 1.5
gains +5.4 over the fit check (G_e = 0) on bimodal with recompute, and +3.0 (bimodal) and +1.2
(half cache) with vLLM free-block self-reuse; under self-reuse gamma0 = 1.5 is within 0.1 of the
best threshold. Against no gate, the bimodal gain here is +5.3, not +5.4 to 5.6. These numbers in
suite 4 come from other experiments and cannot be produced with this package: the recompute share
of step compute (8.9% to 3.3%), eta_reuse of about 0.44 and the -0.58 nat shift, the longctx
workload, the 0.64% distance from a per-workload oracle across gamma0 in [1.0, 2.0] (on bimodal
with recompute, these rows put gamma = 1 1.2 points below gamma = 3), the +3.3 to 3.8% goodput at
2x to 4x cache, and the below-capacity delay-conservation figures.

## The self-reuse check (exploratory, not registered; v18_reuse.py, results-v14-round2/reuse/; development seeds 104-108)

420 runs at 10,000 requests, no errors. Two engines ran on the same arrivals:
- **recompute:** a preempted request recomputes its whole prompt;
- **vllmreuse:** a victim keeps its cached prefix until new allocations reach it in vLLM v1's free-block queue.

The arms were no gate, F alone, the rule on the mean estimate at γ = 1, 1.5, 2 and 3 (P₁ ≤ 36.79 / 22.31 / 13.53 / 4.98%), and T at δ = -1. The cells were bimodal, bimodal at half cache and longout, all at E = 4. Each figure is the mean log ratio against no gate (the `gateoff` arm) in percent over five paired seeds, ±1.96 standard errors. It measures throughput at 1.1 and goodput at 0.95. Against F (the `late` arm) the load-1.1 figures move by at most 0.1 point, because F and no gate are level there; the load-0.95 figures differ, as the next table shows.

| cell, load | engine | γ = 1 | γ = 1.5 | γ = 2 | γ = 3 | T δ = -1 |
|---|---|---|---|---|---|---|
| bimodal, 1.1 | recompute | +4.8 ± 0.0 | +5.3 ± 0.1 | +5.7 ± 0.1 | +6.0 ± 0.2 | +5.9 ± 0.1 |
| bimodal, 1.1 | vllmreuse | +2.5 ± 0.1 | +2.9 ± 0.0 | +3.0 ± 0.1 | +2.9 ± 0.1 | +2.9 ± 0.1 |
| half cache, 1.1 | recompute | +2.4 ± 0.1 | +2.5 ± 0.2 | +2.5 ± 0.1 | +1.9 ± 0.2 | +2.4 ± 0.2 |
| half cache, 1.1 | vllmreuse | +1.1 ± 0.1 | +1.1 ± 0.1 | +0.8 ± 0.1 | +0.0 ± 0.1 | +1.1 ± 0.1 |
| longout, 1.1 | recompute | +1.5 ± 1.2 | +1.2 ± 1.3 | +1.5 ± 1.4 | +1.7 ± 1.4 | +1.7 ± 1.5 |
| longout, 1.1 | vllmreuse | +1.2 ± 1.0 | +1.3 ± 1.2 | +1.3 ± 1.2 | +1.4 ± 1.3 | +1.3 ± 1.3 |
| bimodal, 0.95 | recompute | +7.7 ± 2.4 | +7.7 ± 2.6 | +7.4 ± 2.8 | +6.4 ± 2.2 | +7.1 ± 2.3 |
| bimodal, 0.95 | vllmreuse | +5.0 ± 1.5 | +5.1 ± 0.9 | +5.0 ± 0.8 | +3.6 ± 0.9 | +4.4 ± 1.4 |
| half cache, 0.95 | recompute | +7.9 ± 2.6 | +7.7 ± 2.9 | +5.9 ± 2.8 | -0.5 ± 1.3 | +7.5 ± 2.9 |
| half cache, 0.95 | vllmreuse | +2.4 ± 2.0 | +2.1 ± 1.6 | -0.1 ± 2.0 | -7.7 ± 2.4 | +2.8 ± 2.5 |
| longout, 0.95 | recompute | +0.0 ± 3.2 | +0.2 ± 4.5 | +1.0 ± 4.2 | +1.2 ± 5.2 | +2.2 ± 3.4 |
| longout, 0.95 | vllmreuse | -2.5 ± 2.2 | -1.6 ± 2.6 | -2.6 ± 3.1 | -1.4 ± 2.5 | -1.1 ± 3.4 |

F against no gate, and γ = 1.5 against F (mean log ratio in percent, ±1.96 standard errors):

| cell, load | engine | F over no gate | γ = 1.5 over F |
|---|---|---|---|
| bimodal, 1.1 | recompute | -0.1 ± 0.1 | +5.4 ± 0.2 |
| bimodal, 1.1 | vllmreuse | -0.1 ± 0.0 | +3.0 ± 0.1 |
| half cache, 1.1 | recompute | -0.1 ± 0.0 | +2.6 ± 0.1 |
| half cache, 1.1 | vllmreuse | -0.1 ± 0.1 | +1.2 ± 0.1 |
| longout, 1.1 | recompute | -0.3 ± 0.3 | +1.5 ± 1.1 |
| longout, 1.1 | vllmreuse | -0.1 ± 0.4 | +1.4 ± 1.1 |
| bimodal, 0.95 | recompute | +0.9 ± 2.1 | +6.8 ± 3.6 |
| bimodal, 0.95 | vllmreuse | +3.0 ± 1.3 | +2.1 ± 0.8 |
| half cache, 0.95 | recompute | -5.0 ± 4.3 | +12.7 ± 6.3 |
| half cache, 0.95 | vllmreuse | -1.8 ± 1.4 | +3.9 ± 2.2 |
| longout, 0.95 | recompute | -2.6 ± 2.8 | +2.8 ± 5.2 |
| longout, 0.95 | vllmreuse | -1.6 ± 1.3 | +0.1 ± 2.0 |

F alone gains nothing over no gate in overload in these cells, under either engine, and is mixed below
capacity (ahead on bimodal under self-reuse, behind on half cache and longout, mostly within noise).
The fit check's measured gain over no gate (+3.3-3.8%, "Phase 1b") comes from 2x and 4x cache, where
nothing preempts; in these KV-bound cells the gain over no gate is the look-ahead's.

Under self-reuse, F's own throughput at 1.1 rises by 3.8% (bimodal), 2.7% (half cache) and 0.6% (longout). Readmitted victims recover prefill demand from their own cache:
- under F at 1.1: 18.6%, 19.4% and 8.4%;
- under γ = 1.5: 7.9%, 11.2% and 8.9%.

Preemptions per completion barely change between engines. On bimodal at 1.1 under F they are 0.728 with self-reuse and 0.713 without.

**Reading.**
- Self-reuse roughly halves the memory look-ahead's gain on bimodal and half cache, and the gain stays positive. In overload at γ = 1.5, +5.3% becomes +2.9% on bimodal, and +2.5% becomes +1.1% on half cache. Longout does not resolve at five seeds under either engine.
- The best γ moves down when recompute is cheaper, which is the direction the balance formula predicts. On half cache, γ = 3 falls from +1.9% to +0.0% in overload, and from -0.5% to -7.7% below capacity. γ = 1.5 stays within 0.1% of the best γ in overload in every cell under both engines.
- T at δ = -1 tracks γ = 1.5 under both engines, so self-reuse does not separate them.
- These numbers stand on the simulator's reuse model. The single-GPU vLLM kill test decides whether the look-ahead's gain survives on a real engine.
