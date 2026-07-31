# Experiments

One row per script, one script per question, kept because a number in the prose has to
show its derivation and the script is the derivation. Runs live under
`h1-aggregate-forecast/` (venv, fixed seeds; `results/` is regenerated, not committed).
Pre-registration and verdicts: [h1-aggregate-forecast/RESULTS.md](h1-aggregate-forecast/RESULTS.md).

## H1: aggregate occupancy forecasting

| Script | Question | What it found |
|---|---|---|
| `tests.py` | Are the harness statistics right? | Analytic oracle survival matches 10M-sample empirical CDFs to < 5e-4 in all four regimes (exact at integer arguments by the ceil-sampling argument in `sim/workloads.py`); censored lognormal MLE recovers (mu, sigma) under cap truncation ~25x better than the naive fit; conformal coverage nominal on held-out gaussian residuals; normal-approx aggregation matches Monte Carlo mean/sd/q95 |
| `eval.run_grid` | Do age-conditioned survival curves earn their complexity for aggregate forecasts (K1), and does any stochastic forecaster beat trivial baselines (K2)? | K1 killed the bucketed nonparametric estimator at 500 training observations: +0.0% CI-gated median gain over best-of(B1, B2/B2c), and wider q95 bounds in both pre-declared favorable regimes. K2 passed at t in {5, 10} s (+14.5%, +30.1% over best trivial), failed at t <= 2 s as pre-registered-expected; the gain is carried by the censored parametric fit with conformal calibration. Emergent: at N = 1000 under heavy tails every fitted estimator went coverage-void while the oracle held 0.951 (bias, not stochastic width, binds at scale); the conformal mode was the best valid mode almost everywhere. Five pre-verdict amendments recorded in RESULTS.md |
| `eval.plots` | (figures) | `results/skill_vs_horizon.png`, `results/coverage_width.png` from `results/cells.csv` |

## Threads

Each thread names an open question and what it waits on; a thread re-earns its line at
triage or is deleted with the reason in the commit body ([STYLE.md](STYLE.md)).

- **Training-size sensitivity.** Is B3's kill data-starvation? The verdict is pinned at
  ~500 completions (RESULTS.md amendment 4); production accumulates far more. Waits on a
  run with warm-up extended to ~5000 completions, reported as context, not verdict.
- **Mixture-EM estimator.** B3's single win (R3/N=1000, t >= 5 s: +22%/+49%) is latent
  bimodality at scale. Does a two-component lognormal mixture (EM) capture it with two
  parameters per component? Waits on an estimator implementation; the day-2 candidate to
  try before any nonparametric revival.
- **Conformal-window hypothesis.** Untested: the R2/N=1000 coverage collapse may trace to
  the 300 s calibration window holding too few effective samples at E[T] ~ 30 s
  autocorrelation. Waits on a window-length sweep; if confirmed, the practical fix is
  cheaper than better estimators.
- **T2 / K3 admissions dominance.** Does arrival forecasting dominate length modeling for
  total-occupancy forecasts? Waits on the shared admissions forecaster and the
  oracle-arrivals ablation (protocol section, deferred day-2).
- **Censoring preview.** How much do censoring-aware fits degrade under independent vs
  length-correlated (eviction-shaped) censoring of the training stream? Waits on the
  phase-2 injectors; the flag threshold is pre-registered in the protocol.
- **BLIS re-score (L0.5).** Do the H1 verdicts survive batching-coupled decode rates and
  realistic arrivals? Waits on reading the BLIS API (`blis` in
  [sources.md](sources.md)) and mapping the estimator interface onto its outputs.
- **Small-pool regime (N = 10).** Annex cells show Little's-law deviations of 11-32%
  (time-average variance at small N) and need MC-quantile treatment throughout. Waits on
  anyone caring about pools under ~50 leases.
- **Bernstein bound for the conservative dial.** A concentration bound gives a
  distribution-free upper bound on aggregate occupancy: wider than calibrated quantiles
  but guaranteed, suited to the guaranteed end of the confidence dial. Paper exercise;
  waits on the tiered-admission design round.
- **Credibility-theory shrinkage.** The optimal stratum-vs-pool blend weight in closed
  form, replacing pseudo-count shrinkage. Gated behind a nonparametric revival, which is
  gated behind mixture-EM failing.
- **Design threads (not experiment-shaped).** Hold placement (assessment finding 3) waits
  on a design discussion; the termination-cause + streamed-token telemetry change
  (assessment finding 4) waits on the user's go-ahead to touch upstream; the related-work
  reads that would upgrade `dro2026` and `tie2026` from machine-read wait on reading time.
