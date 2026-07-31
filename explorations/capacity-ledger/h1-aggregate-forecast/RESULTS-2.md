# H1 results, round 2 (pre-registered): corrected warm-up, calibration windows, drift

Round-2 criteria for the H1 harness. As in [RESULTS.md](RESULTS.md), the commit that adds
this file with empty result tables is the pre-registration record; criteria changes after
that commit are amendments, dated relative to the runs by commit and stating whom they
favor. Round-1 criteria, thresholds, and gate bands carry over unless restated here.

## Motivating measurement: warm-up length truncation

The round-1 warm-up ends at a completion count (~500). Completions observed while the
pool is still filling are exactly the jobs short enough to have finished, so the training
set is truncated near r * t_warmup_end tokens, and the effect grows with N because
arrival rate scales with N while fill time does not shrink. Measured
(`eval/diag_training_bias.py`, seed 0, legacy rule): R2/N=1000 training mean 184 vs true
756, max 723 in a regime with true q95 ~ 2989 and tail support to 16384; R1/N=1000
training max 272 vs true q95 ~ 657. Every round-1 fitted estimator trained on these
samples, so the round-1 scope statement "a verdict at 500 observations" was in fact a
verdict at 500 length-truncated observations, most severely at N = 1000 — the same cells
where round 1 recorded coverage collapse. Round 2 fixes the harness, re-adjudicates K1
and K2 on unbiased training sets, and separates this mechanism from the
calibration-window mechanism by a factorial (question A below).

## Harness amendments (uniform, pre-registered before any round-2 run)

- **Steady warm-up.** Training completions are collected only after a burn-in of
  `steady_burn_in_s = 500` s (derived: completions of length L cannot be observed before
  t = L/r, so unbiased collection requires burn-in >= tail support / r = 16384 / 40 =
  410 s; 500 rounds up). In steady state, completions over an observation window are
  unbiased draws from the output distribution. The completion count target (~500) is
  unchanged (RESULTS.md amendment 4 keeps the K1 verdict pinned there). Beneficiary:
  fitted estimators, uniformly; trivial baselines are unaffected. The legacy rule remains
  available for the attribution factorial only.
- **Adaptive conformal calibration window.** W(regime cell) = max(300 s,
  40 * mean(training lengths) / r, rounded up to 60 s). The 40 is a convention: with
  residual decorrelation time ~ E[T] = E[L]/r, W holds ~40 effective residual samples,
  i.e. ~2 expected q95 exceedances. The calibration window of the simulation equals W.
  Round 1's fixed 300 s window held ~10 effective samples in R2 (derived in the round-1
  work-table entry).

Sanity gates carry over: Little's-law deviation <= 10% on verdict cells; seed-pooled
oracle q95 coverage per verdict cell in [0.93, 0.98]. Coverage floors carry over: < 0.90
voids a cell for a mode, width comparisons need >= 0.93. Under drift the oracle is the
per-lease component-true survival, so the gate applies unchanged.

## Questions and decision rules

### A — attribution of the R2/N=1000 collapse (factorial, no kill)

R2/N=1000, stationary, 3 seeds, factorial: warm-up in {legacy, steady} x W in
{300, 600, 1200, 2400} s (windows evaluated as trailing slices of one 2400 s calibration
window per warm-up arm; `window_sweep_s` in params). Readings, on the best fitted mode's
seed-pooled q95 coverage:

- **Truncation mechanism confirmed** if steady/W=300 reaches coverage >= 0.90 while
  legacy/W=300 stays below.
- **Window mechanism confirmed** if legacy/W>=1200 reaches coverage >= 0.90 with no
  warm-up fix.
- Both may hold; the recorded attribution is which factor alone suffices. If neither
  restores coverage at any W, both hypotheses are refuted and the collapse is recorded
  as estimator bias beyond training-data and calibration-window repair.

### K1' — B3 complexity, re-verdict on unbiased training

Full verdict grid (R1-R4, N in {100, 1000}, t in {5, 10} s, 3 seeds), steady warm-up,
adaptive window. Criteria identical to round-1 K1, reference set unchanged
(best-of(B1, B2/B2c); B4 is reported separately and is not in the K1' reference, for
comparability with round 1): B3 stays killed iff (a) median CI-gated relative
pinball@q95 improvement < 10% AND (b) no >= 10% valid-coverage q95 width reduction in
R3 or R4 at N >= 100. This re-verdict replaces the round-1 K1 scope: a kill here is a
kill at ~500 unbiased observations under stationarity.

### K2' — stochastic layer, re-verdict (replaces the RESULTS.md amendment-6 conditional)

Same run as K1'. Median improvement of best stochastic (B1/B2/B2c/B3/B4, best valid
mode) over best trivial (B0/B0g, conformal) per horizon; thresholds unchanged (>= 10%
at t in {5, 10} s). Evaluated under BOTH void-cell rules — void-excluded and
void-as-zero — and K2' passes a horizon only if both rules pass. This closes the
round-1 hole that amendment 6 documented: the t = 5 s verdict here supersedes the
round-1 conditional verdict, in whichever direction it lands. t = 10 s is
re-adjudicated under the same rule.

### B4 — two-component lognormal mixture (EM), the parametric answer to B3's one win

Same run. B4 is a two-component lognormal mixture fit by censored EM (cap-aware like
B2c). Success rule, at R3/N=1000, t in {5, 10} s: B4 succeeds if its best valid mode
reaches >= 75% (convention) of B3's best-valid CI-gated skill vs best trivial in the
same cells, or if B4 is valid where B3 is void. Guard: B4 must not go void in any
verdict cell where B2c is valid. Success settles the work-table mixture row: no
nonparametric revival is needed. If B3 (revived by clean training) beats B4 by more
than the complement, the nonparametric case reopens instead.

### KD — drift: does calibration survive non-stationarity at scale?

Scenario parameters live in `data/params_default.json` under `drift` (all conventions or
guesses per the numbers rule, chosen before any drift run). Components A = R1 output,
B = R2 output; mixing weight on A is w(t):

- **DS (shift):** w = 1.0 until eval start + 300 s, then w = 0.25. Post-drift slice =
  [shift, shift + 600 s].
- **DR (ramp):** w falls linearly 1.0 -> 0.25 over [eval start, eval start + 600 s].
  Post-drift slice = [eval start + 600 s, eval start + 900 s]; the ramp itself is
  reported as context.

Arrival rate lambda(t) = N r / E[L](t) holds the target pool size constant, isolating
length-mix drift from load drift. Capacity is sized by pilot at the post-drift mix.
Training and fits are frozen pre-drift (steady warm-up at w = 1.0); the only adaptation
under test is conformal calibration: the static calibration-window quantiles, plus
rolling windows of {150, 300, 600} s applied identically to every estimator including
trivials. Rolling honesty: a residual is usable at forecast time s only if its
realization time (snapshot time + horizon) is <= s.

Verdict cells: N = 1000 (N = 100 as context), t in {5, 10} s, both scenarios, 3 seeds,
scored on the post-drift slice. Per scenario x horizon, the fitted layer passes iff some
fitted (estimator, mode, window) with seed-pooled post-slice coverage >= 0.90 achieves
CI-gated pinball@q95 skill >= 10% vs the best post-slice-valid trivial (trivials get the
same window options). Drift verdict per horizon: **PASS iff both scenarios pass.** A
FAIL means calibrated forecasting cannot be assumed through drift at scale, and the
ledger design must treat drift as a detected regime (governor or fallback), not a
forecastable one.

### Context: training-size sensitivity (no verdict)

Steady warm-up extended to ~5000 completions, N = 1000, R1-R4, 3 seeds, adaptive window.
Reports B3 and B4 skill deltas vs B2c relative to the ~500-completion run. The K1'
verdict stays pinned at ~500 (RESULTS.md amendment 4); this answers only whether B3's
standing is data-starvation.

## Results

### A — attribution factorial (best fitted mode, seed-pooled cov95 / width95, t = 5 s)

| Warm-up | W = 300 | W = 600 | W = 1200 | W = 2400 |
|---|---|---|---|---|
| legacy | | | | |
| steady | | | | |

Reading:

### K1' inputs

| Quantity | Value |
|---|---|
| Median B3 improvement over best-of(B1, B2/B2c), verdict grid (CI-gated, non-significant counted 0) | |
| B3 q95 width reduction, R3, N >= 100 (valid coverage only) | |
| B3 q95 width reduction, R4, N >= 100 (valid coverage only) | |

### K2' inputs (median improvement of best stochastic over best trivial)

| t (s) | Void-excluded | Void-as-zero | Threshold | Pass/Fail |
|---|---|---|---|---|
| 5 | | | 10% | |
| 10 | | | 10% | |

### B4 at R3/N=1000 (skill vs best trivial, best valid mode)

| t (s) | B3 | B4 | B4/B3 ratio | B4 valid where B2c valid? |
|---|---|---|---|---|
| 5 | | | | |
| 10 | | | | |

### KD — drift (N = 1000, post-drift slice, best fitted (mode, window) vs best trivial)

| Scenario | t (s) | Best fitted cov95 | Skill vs trivial | Pass |
|---|---|---|---|---|
| DS | 5 | | | |
| DS | 10 | | | |
| DR | 5 | | | |
| DR | 10 | | | |

### Context: training size (~5000 vs ~500 completions, N = 1000)

| Regime | B3 skill delta | B4 skill delta | B2c skill delta |
|---|---|---|---|
| R1 | | | |
| R2 | | | |
| R3 | | | |
| R4 | | | |

## Verdicts

- **A:**
- **K1':**
- **K2':**
- **B4:**
- **KD:**
