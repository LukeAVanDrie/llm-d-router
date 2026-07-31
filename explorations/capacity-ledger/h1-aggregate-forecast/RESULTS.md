# H1 results (pre-registered)

The kill criteria precede the results: `eac07223` committed this file with the tables
empty, and git history is the pre-registration record. Criteria changes after that commit
are amendments below, each dated relative to the runs by commit and stating whom it
favors; affected cells are re-run, not reinterpreted.

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
| Little's law | realized steady-state N within 10% of target, all cells (hard-fail on verdict cells; annex deviations reported) | PASS on all verdict cells; N=10 annex deviates 11-32% as expected from time-average variance at small N |
| Oracle coverage | seed-pooled oracle q95 coverage on T1 in [93%, 98%], all verdict cells (amended, see notes) | PASS, all 16 verdict cells |

### Amendments

Amendments 1-4 were recorded before the full evaluation run (`447a7caa`). Amendments 5-6
were recorded after full-run results existed; each states its ordering and whom it favors.

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
5. Post-hoc (recorded in the verdict commit, after the first full run — an ordering
   violation of this file's own rule, caught by the session's cold read and relabeled
   here). The oracle-coverage gate is evaluated on seed-pooled coverage per verdict cell
   (regime, N, t), not per seed. Beneficiary: the harness gate itself — five per-seed
   failures became passes. The statistical case stands on its own: the oracle's realized
   value is by construction a draw from the exact distribution its quantile is computed
   from, so conditional coverage is exact and residual variation is sampling noise;
   per-seed coverage over ~600 autocorrelated snapshots has a standard error of 3-5
   points, and a hard band applied per seed false-fails routinely (observed: 5 of 48
   per-seed checks, spread across regimes, all consistent with noise). Competitor scores
   are unaffected.
6. Post-hoc (recorded after the verdict run, from the cold read). VOID-cell handling in
   the K2 medians was never pre-registered: as implemented (eval/run_grid.py), a verdict
   cell where every stochastic mode fails the 0.90 coverage floor is excluded from the
   median rather than counted, a choice that favors the layer. Sensitivity, derived from
   the verdict table: counting such cells (R2/N=1000 at both verdict horizons) as zero
   improvement gives +7.3% at t = 5 s (below the 10% threshold) and +29.9% at t = 10 s
   (still passing). The K2 verdict below carries this conditioning.

## Results

### T1 pinball@q95 skill vs best-of(B0, B0g) — verdict cells

Skill = 1 - loss(model)/loss(best trivial); positive = better than trivial. CI-gated.

(* = paired 90% bootstrap CI excludes zero; VOID = no mode reached coverage 0.90.)

| Regime | N | t (s) | B1 | B2/B2c | B3 | Oracle |
|---|---|---|---|---|---|---|
| R1 | 100 | 5 | +4.5% | +14.5%* | +10.9% | +22.7%* |
| R1 | 100 | 10 | VOID | +33.7%* | +29.9%* | +39.0%* |
| R1 | 1000 | 5 | +12.5%* | +17.5%* | +15.3%* | +35.1%* |
| R1 | 1000 | 10 | +40.4%* | +42.4%* | +43.5%* | +51.9%* |
| R2 | 100 | 5 | -43.8% | +2.5% | -11.1% | +4.6%* |
| R2 | 100 | 10 | -43.7% | +7.2%* | -11.3% | +11.3%* |
| R2 | 1000 | 5 | VOID | VOID | VOID | +28.1%* |
| R2 | 1000 | 10 | VOID | VOID | VOID | +44.6%* |
| R3 | 100 | 5 | VOID | -3.0% | VOID | +7.1%* |
| R3 | 100 | 10 | VOID | -5.6% | VOID | +11.3%* |
| R3 | 1000 | 5 | VOID | VOID | +22.2%* | +46.5%* |
| R3 | 1000 | 10 | VOID | VOID | +49.2%* | +65.0%* |
| R4 | 100 | 5 | +8.5%* | +25.3%* | +23.4%* | +30.7%* |
| R4 | 100 | 10 | +19.0%* | +30.1%* | +28.3%* | +36.9%* |
| R4 | 1000 | 5 | -9.0% | +8.0% | -18.1% | +33.9%* |
| R4 | 1000 | 10 | +22.3%* | +29.6%* | +20.3%* | +46.0%* |

### K1 inputs

| Quantity | Value |
|---|---|
| Median B3 improvement over best-of(B1, B2/B2c), verdict grid (CI-gated, non-significant counted 0) | +0.0% |
| B3 q95 width reduction, R3, N >= 100 (valid coverage only) | no valid-coverage cells |
| B3 q95 width reduction, R4, N >= 100 (valid coverage only) | -5.9% (B3 wider) |

### K2 inputs (median improvement of best stochastic over best trivial, per horizon)

| t (s) | Median improvement | Threshold | Pass/Fail |
|---|---|---|---|
| 1 | +0.0% | 5% | FAIL (pre-registered as expected) |
| 2 | +0.0% | 5% | FAIL (pre-registered as expected) |
| 5 | +14.5% | 10% | PASS |
| 10 | +30.1% | 10% | PASS |

### Coverage / width — q95, t = 5 s, best valid mode per estimator

(Seed-averaged coverage and width as a fraction of mean headroom; VOID = no mode reached
coverage 0.90. Full tables in results/cells.csv — results/ is regenerated rather than
committed; reproduce with the two commands under Context cells.)

| Cell | B0 | B0g | B1 | B2 | B2c | B3 | Oracle |
|---|---|---|---|---|---|---|---|
| R1/100 | .959/0.51 | .954/0.55 | .916/0.39 | .934/0.38 | .934/0.38 | .930/0.38 | .953/0.38 |
| R1/1000 | .962/0.20 | .964/0.23 | .937/0.19 | .958/0.18 | .958/0.18 | .956/0.18 | .948/0.13 |
| R2/100 | .957/0.30 | .954/0.32 | .942/0.49 | .978/0.33 | .978/0.33 | .978/0.38 | .967/0.31 |
| R2/1000 | .997/0.13 | 1.00/0.23 | VOID | VOID | VOID | VOID | .951/0.12 |
| R3/100 | .932/0.20 | .939/0.21 | VOID | .917/0.20 | .917/0.20 | VOID | .947/0.22 |
| R3/1000 | 1.00/0.12 | 1.00/0.22 | VOID | VOID | VOID | .922/0.09 | .949/0.07 |
| R4/100 | .939/0.44 | .930/0.45 | .944/0.41 | .956/0.40 | .932/0.32 | .931/0.33 | .945/0.33 |
| R4/1000 | .982/0.17 | .987/0.19 | .924/0.16 | .956/0.16 | .923/0.13 | .916/0.17 | .956/0.10 |

### Context cells (non-verdict)

Short horizons (t <= 2 s), the N = 10 annex, MAE, and the per-lease Brier diagnostic are in
`results/cells.csv` and the plots (`results/skill_vs_horizon.png`,
`results/coverage_width.png`); regenerate with `.venv/bin/python -m eval.run_grid --synthetic`
followed by `.venv/bin/python -m eval.plots` (fixed seeds, deterministic).

## Verdicts

- **K1: KILLED.** Both arms hold: (a) median CI-gated improvement of B3 over the best
  simpler fitted estimator is +0.0% (< 10%); (b) B3 reduced valid-coverage q95 width in
  neither favorable regime (R3: no valid cells; R4: 5.9% wider). At 500 training
  observations, bucketed nonparametric hazard estimation does not earn its complexity over
  a censored parametric fit. Scope: this is a verdict at 500 observations under
  stationarity (amendment 4); the training-size sensitivity is the pre-declared follow-up.
- **K2: PASSES robustly at t = 10 s** (+30.1% as implemented, +29.9% under VOID-as-zero)
  **and conditionally at t = 5 s** (+14.5% as implemented, +7.3% and failing under
  VOID-as-zero; amendment 6). Fails at t <= 2 s exactly as pre-registered. The stochastic
  layer justifies itself at the 10 s horizon under either rule, carried almost entirely
  by the parametric estimators (B2/B2c); the 5 s case rests on how heavy-tail
  large-pool cells are counted, which is the same calibration-at-scale problem note 2
  records.
- Notes:
  1. The conformal mode was the best valid mode almost everywhere, including for the
     survival models; native distributional quantiles were rarely optimal. The residual
     history is doing much of the calibration work.
  2. Large pools make calibration harder, not easier: at N = 1000 in the heavy-tail regime
     every fitted estimator went coverage-void while the oracle stayed at 0.951. The
     aggregate's stochastic width shrinks like 1/sqrt(N), so estimator bias dominates the
     interval exactly where multiplexing gains are largest. Bias control (better
     estimators, longer conformal windows, or a governor) is the binding constraint at
     scale.
  3. B3's single clear win is R3/N=1000 at t >= 5 s (+22%/+49%): latent bimodality at
     scale over long horizons is where age-conditioning pays. A two-component parametric
     mixture (EM) might capture the same gain with far fewer degrees of freedom; that is
     the day-2 estimator to try before reviving B3.
  4. The oracle-to-best-fitted gap is large in the hard regimes (R2/1000: oracle +28-45%
     vs all fitted VOID), so the forecasting frame has substantial unrealized headroom;
     estimator quality, not the frame, is what limits it.
