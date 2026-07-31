# H1 results (pre-registered)

This file is committed with the kill criteria stated and the result tables empty, before any
evaluation run. Git history is the pre-registration record. Criteria may not be changed after
the first result-bearing commit; if a criterion proves ill-posed, the change and its reason are
recorded here and the affected cells are re-run, not reinterpreted.

## Hypothesis

H1: age-conditioned nonparametric survival modeling (B3) materially improves aggregate
KV-occupancy forecasts over simpler alternatives at realistic lease populations and horizons.

Verdict grid: target T1, pinball@q95, regimes R1-R4, N in {100, 1000}, t in {5, 10} s,
3 seeds. All other cells reported as context only.

## Kill criteria

- **K1 — B3 does not earn its complexity.** B3 is killed if BOTH hold:
  - (a) median relative pinball@q95 improvement of B3 over best-of(B1, B2/B2c) across the
    verdict grid is < 10%, CI-gated (paired 90% bootstrap CI must exclude zero for a cell to
    count as an improvement); AND
  - (b) B3 fails to reduce valid-coverage q95 interval width by >= 10% in either of its
    pre-declared favorable regimes (R3 mixture, R4 truncation) at N >= 100.

  Rationale: ~10% of the conservative-quantile width is the scale of overcommit headroom the
  machinery would reclaim; below that, censoring-aware fitting, strata shrinkage, and drift
  resets do not pay for their operational surface. B3 gets its best-case regimes as an escape
  hatch; failing even there makes the kill decisive.

- **K2 — nothing beats trivial.** The stochastic layer as a whole (best of B1/B2/B2c/B3, best
  valid mode) is killed per horizon if its median improvement over
  best-of(B0, B0g, conformal-B0g) is < 5% at t in {1, 2} s and < 10% at t in {5, 10} s.
  Pre-registered expectation: short horizons fail K2 and that is acceptable; the layer justifies
  itself at 5-10 s or not at all.

- **Coverage gate.** Quantile claims from any estimator with cell coverage < 90% are void in
  that cell; width comparisons require >= 93%.

- **K3 (deferred, day 2).** Admissions dominance on T2: if the pinball spread from swapping
  survival models is < 0.2x the improvement from an oracle-arrivals ablation at N in
  {100, 1000}, record "length modeling is second-order; invest in arrival forecasting first."

## Sanity gates (must pass before any table below is filled)

| Gate | Requirement | Status |
|---|---|---|
| Little's law | realized steady-state N within 10% of target, all cells (hard-fail on verdict cells; annex deviations reported) | PENDING |
| Oracle coverage | oracle q95 coverage on T1 in [93%, 98%], all verdict cells (amended, see notes) | PENDING |

### Pre-verdict amendments (recorded before the full evaluation run)

1. Oracle quantiles are Monte Carlo (2000 draws), not normal-approximation: the gate tests
   harness math, and the normal approximation's skew error is a property of the estimator
   machinery (which all competitors share uniformly), not of the harness. Smoke run showed
   normal-approx oracle q95 coverage 0.972-0.977 in R3/R4.
2. Oracle coverage upper bound amended 97% -> 98%. With lumpy footprints (R3 long mode, R4
   cap atom) realized occupancy is a discrete sum with multi-percent atoms, so a one-sided
   q95 covers at least 95% by construction and can sit near 98% legitimately. The dangerous
   direction (under-coverage, broken math) still fails below 93%.
3. B3 tail extrapolation upgraded from constant hazard to a Hill power-law fit on the top
   decile, and B3 given the known max_tokens cap (B2c already had it). The reviewed
   specification called for heavy-tail extrapolation; the smoke run showed the constant-
   hazard variant coverage-void in every R2 cell. Direction favors the candidate under
   test: a kill after this fix is more decisive, not less.
4. Training-set size is pinned at the warm-up's ~500 completions in all cells. A verdict
   against B3 is therefore a verdict at 500 observations; a training-size sensitivity is a
   pre-declared day-2 item, not part of this verdict.

## Results

### T1 pinball@q95 skill vs best-of(B0, B0g) — verdict cells

Skill = 1 - loss(model)/loss(best trivial); positive = better than trivial. CI-gated.

| Regime | N | t (s) | B1 | B2/B2c | B3 | Oracle |
|---|---|---|---|---|---|---|
| R1 | 100 | 5 | | | | |
| R1 | 100 | 10 | | | | |
| R1 | 1000 | 5 | | | | |
| R1 | 1000 | 10 | | | | |
| R2 | 100 | 5 | | | | |
| R2 | 100 | 10 | | | | |
| R2 | 1000 | 5 | | | | |
| R2 | 1000 | 10 | | | | |
| R3 | 100 | 5 | | | | |
| R3 | 100 | 10 | | | | |
| R3 | 1000 | 5 | | | | |
| R3 | 1000 | 10 | | | | |
| R4 | 100 | 5 | | | | |
| R4 | 100 | 10 | | | | |
| R4 | 1000 | 5 | | | | |
| R4 | 1000 | 10 | | | | |

### K1 inputs

| Quantity | Value |
|---|---|
| Median B3 improvement over best-of(B1, B2/B2c), verdict grid | |
| B3 q95 width reduction, R3, N >= 100 (valid coverage only) | |
| B3 q95 width reduction, R4, N >= 100 (valid coverage only) | |

### K2 inputs (median improvement of best stochastic over best trivial, per horizon)

| t (s) | Median improvement | Threshold | Pass/Fail |
|---|---|---|---|
| 1 | | 5% | |
| 2 | | 5% | |
| 5 | | 10% | |
| 10 | | 10% | |

### Coverage / width — q95, verdict cells

(Coverage %, width as % of mean headroom; entries void if coverage < 90%.)

| Regime | N | t (s) | B0g+conf | B1 | B2/B2c | B3 | Oracle |
|---|---|---|---|---|---|---|---|
| (filled by eval/run_grid.py) | | | | | | | |

### Context cells (non-verdict)

Short horizons (t <= 2 s), N = 10 annex, MAE tables, per-lease Brier diagnostic: see
`results/` CSVs and plots; summarized here after the verdict section is filled.

## Verdicts

- K1: PENDING
- K2: PENDING
- Notes:
