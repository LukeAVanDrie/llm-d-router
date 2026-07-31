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

## Work table

Ordered by decisiveness per cost; future sessions take work from the top unless directed
otherwise. A row re-earns its place at triage or is deleted with the reason in the commit
body ([STYLE.md](STYLE.md)).

1. **H1 drift extension (L0).** The top open risk in the stochastic layer: at N = 1000
   every fitted estimator went coverage-void under heavy tails while the oracle held, so
   bias binds exactly where multiplexing gains are largest, and every current result is
   stationary. Add regime-drift scenarios and a conformal-window sweep (which also settles
   the untested hypothesis that the R2/N=1000 collapse traces to the 300 s calibration
   window holding ~10 effective samples at E[T] ~ 30 s). Pre-register criteria before
   running. Waits on nothing.
2. **Mixture-EM estimator.** Does a two-component lognormal mixture capture B3's only win
   (R3/N=1000, t >= 5 s: +22%/+49%) with parametric degrees of freedom? Settles whether a
   nonparametric revival is ever needed. Waits on nothing.
3. **Training-size sensitivity.** Is B3's kill data-starvation? Context run (not verdict)
   with warm-up extended to ~5000 completions; the verdict is pinned at ~500 (RESULTS.md
   amendment 4). Waits on nothing; the cheapest row.
4. **Source reads: `dro2026`, `tie2026`, `s3-line`.** Three claims the assessment leans
   on stand at machine-read or recalled ([sources.md](sources.md)), and the positioning
   narrative rests on the weakest of them. Waits on PDFs landing in the kept-texts directory.
5. **Hold-placement design round.** Assessment finding 3: the decision that unblocks the
   deterministic ledger skeleton. Waits on a design discussion.
6. **Telemetry change (upstream).** Termination-cause enum and a streamed-token counter
   (assessment finding 4); training-data accrual is the long pole and cannot be
   backfilled. Waits on the user's go-ahead to prepare an upstream issue and PR.
7. **T2 / K3 admissions dominance.** Does arrival forecasting dominate length modeling
   for total-occupancy forecasts? Waits on the shared admissions forecaster and the
   oracle-arrivals ablation.
8. **Censoring preview (phase 2).** Degradation of censoring-aware fits under independent
   vs eviction-shaped censoring; flag threshold pre-registered in the protocol. Waits on
   the injectors.
9. **BLIS re-score (L0.5).** Do the verdicts survive batching-coupled decode rates and
   realistic arrivals? Waits on reading the BLIS API (`blis` in sources.md).
10. **Ledger-document revision.** Fold the assessment and H1 verdicts into
    `docs/flow-control-capacity-ledger.md`: the stochastic mechanism sentence, the
    confidence dial, the new opens, and the per-request-probability vs point-prediction
    distinction. Waits on rows 1-2 settling the mechanism.
11. **Backlog.** Bernstein-bound note for the conservative end of the dial (paper
    exercise); credibility-theory shrinkage (gated on a nonparametric revival, itself
    gated on row 2 failing); small-pool (N = 10) regime treatment (MC quantiles
    throughout; Little deviations 11-32% are expected time-average variance).
