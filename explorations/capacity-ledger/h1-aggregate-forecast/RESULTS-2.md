# H1 results, round 2 (pre-registered): corrected warm-up, calibration windows, drift

Round-2 criteria for the H1 harness. As in [RESULTS.md](RESULTS.md), the commit that adds
this file with empty result tables (`91b58fa4`) is the pre-registration record; criteria
changes after that commit are amendments, dated relative to the runs by commit and
stating whom they favor. Round-1 criteria, thresholds, and gate bands carry over unless
restated here.

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

## Harness amendments (uniform across all round-2 runs)

The steady-warm-up and adaptive-window amendments precede every round-2 run
(`91b58fa4`). The pilot amendment's ordering is recorded in its own bullet.

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
  Round 1's fixed 300 s window held ~15 effective samples in R2 (derived: W / E[T] with
  E[T] = 756 / 40 ~ 19 s, the true mean per the diagnostic's 200k-draw reference
  sample).

Sanity gates carry over: Little's-law deviation <= 10% on verdict cells; seed-pooled
oracle q95 coverage per verdict cell in [0.93, 0.98]. Coverage floors carry over: < 0.90
voids a cell for a mode, width comparisons need >= 0.93. Under drift the oracle is the
per-lease component-true survival, so the gate applies unchanged.

- **Pilot amendment (recorded after a first grid run failed its own Little gate, before
  any verdict was adopted; all runs below are from the re-run).** Capacity pilots settle
  `pilot_settle_s = 600` s and measure over the following `pilot_measure_s = 600` s
  (previously ~250/250). The settling period must clear the same fill transient as
  calibration (tail support / r = 410 s): the short pilot undersized capacity in tail
  regimes, and with the round-2 eval window sitting post-transient, R2/N=100 dropped
  6-10% of arrivals at the 0.95 C threshold and realized N fell 9-12% under target,
  failing the pre-registered Little gate (first-run record: results are not adopted from
  that run; the gate did its job). Uniform across all cells and estimators; beneficiary:
  the harness gate itself. The A and KD tables were first filled from pre-amendment
  runs; the re-runs under the resized pilots reproduced every reported number exactly,
  because capacity enters the simulation only through the drop branch and no arrival is
  dropped in those cells at either sizing (measured: `eval/diag_drop_invariance.py`,
  zero drops across all A and KD verdict cells at both capacities).

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

(`eval.run_window_factorial`, seeds 0-2, R2/N=1000; all windows are trailing slices of
one 2400 s calibration window, so every slice sits in steady state.)

| Warm-up | W = 300 | W = 600 | W = 1200 | W = 2400 |
|---|---|---|---|---|
| legacy | 0.978 / 65772 (B2) | 0.971 / 65400 (B2) | 0.972 / 62847 (B2) | 0.973 / 64602 (B2) |
| steady | 0.942 / 53156 (B2) | 0.954 / 56470 (B2) | 0.950 / 55295 (B2) | 0.952 / 54233 (B2) |

Reading: neither pre-registered reading's premise held, and the reason is a design flaw
in the factorial itself: trailing slices of one long calibration window confound window
length with window placement, so every cell — including legacy/W=300 — sits in steady
state and is valid. A confirmatory cell, added after the flaw was seen
(post-registration; beneficiary: attribution clarity, it can only refine the mechanism
story, not any kill verdict), decomposes further: steady warm-up with the same 300 s
window placed immediately after warm-up (post-burn-in;
`run_grid --ns 1000 --regimes R2 --calib-rule fixed`, results/summary-r2fixed300.txt) is
also valid (B2 +1.4% skill at t = 5 s, pooled oracle coverage 0.967), while the round-1
record — legacy warm-up, the same window length placed at ~10-310 s, inside the
pool-fill transient — is void. Attribution: the transient-placed window is necessary to
the collapse — repairing placement alone (legacy training, late window) restores
validity. Whether repairing training alone would also suffice is untestable in this
harness: the steady warm-up's burn-in forces every subsequent calibration window
post-transient, so no unbiased-training/transient-window arm exists. Window length is
not the driver: 300 s post-transient windows hold ~15 effective samples in R2 (derived
in the adaptive-window amendment above) and are valid everywhere observed, refuting the
effective-samples hypothesis. Training truncation costs width, not validity: legacy
widths run 14-24% above steady across the sweep (65772 vs 53156 tokens at W = 300).

### T1 pinball@q95 skill vs best trivial — verdict cells

(`eval.run_grid --ns 100,1000`, steady warm-up, adaptive window, seeds 0-2; all gates
passed: Little dev <= 4.6% per verdict-cell seed, pooled oracle coverage in band in all
16 verdict cells. Resolved adaptive windows: R1 300 s, R2 660-900 s, R3 900-1080 s,
R4 480 s. * = paired 90% bootstrap CI excludes zero; VOID = no mode reached coverage
0.90. Context horizons and the training-size run's full table are in the regenerable
summaries.)

| Regime | N | t (s) | B1 | B2/B2c | B3 | B4 | Oracle |
|---|---|---|---|---|---|---|---|
| R1 | 100 | 5 | +23.3%* | +27.3%* | +25.9%* | +25.6%* | +27.6%* |
| R1 | 100 | 10 | +36.6%* | +41.5%* | +41.6%* | +40.6%* | +41.7%* |
| R1 | 1000 | 5 | +33.9%* | +32.5%* | +32.1%* | +32.8%* | +33.0%* |
| R1 | 1000 | 10 | +48.4%* | +47.0%* | +46.8%* | +47.3%* | +46.6%* |
| R2 | 100 | 5 | VOID | +2.9%* | -1.4% | +2.4% | +1.7% |
| R2 | 100 | 10 | VOID | +5.7%* | -7.0% | +5.3%* | +2.7% |
| R2 | 1000 | 5 | -26.7% | +6.4%* | -2.2% | +6.2%* | +6.1%* |
| R2 | 1000 | 10 | VOID | +9.4%* | -8.1% | +9.1%* | +10.6%* |
| R3 | 100 | 5 | -4.5% | -1.6% | -0.4% | -1.7% | +3.0%* |
| R3 | 100 | 10 | -2.6% | -1.0% | +3.5% | +0.8% | +3.3% |
| R3 | 1000 | 5 | +0.1% | -1.2% | +3.2%* | +2.3% | +2.8% |
| R3 | 1000 | 10 | -2.0% | -3.2% | +4.1%* | +2.6% | +3.0% |
| R4 | 100 | 5 | +13.1%* | +24.8%* | +20.5%* | +24.4%* | +26.2%* |
| R4 | 100 | 10 | +31.0%* | +39.3%* | +38.0%* | +39.6%* | +39.6%* |
| R4 | 1000 | 5 | +11.4%* | +24.5%* | +22.1%* | +24.1%* | +26.5%* |
| R4 | 1000 | 10 | +18.9%* | +30.2%* | +30.2%* | +30.2%* | +32.4%* |

### K1' inputs

| Quantity | Value |
|---|---|
| Median B3 improvement over best-of(B1, B2/B2c), verdict grid (CI-gated, non-significant counted 0) | +0.0% |
| B3 q95 width reduction, R3, N >= 100 (valid coverage only) | +2.8% |
| B3 q95 width reduction, R4, N >= 100 (valid coverage only) | -1.7% (B3 wider) |

### K2' inputs (median improvement of best stochastic over best trivial)

No verdict cell was void for the whole stochastic ladder, so the two rules coincide.

| t (s) | Void-excluded | Void-as-zero | Threshold | Pass/Fail |
|---|---|---|---|---|
| 2 (context) | +6.8% | +6.8% | 5% | PASS |
| 5 | +15.4% | +15.4% | 10% | PASS |
| 10 | +19.8% | +19.8% | 10% | PASS |

(t = 1 s remains a fail at +2.6% against its 5% threshold, as pre-registered-expected;
t = 2 s, a round-1 expected-fail, passes under the corrected harness.)

### B4 at R3/N=1000 (skill vs best trivial, best valid mode)

| t (s) | B3 | B4 | B4/B3 ratio | B4 valid where B2c valid? |
|---|---|---|---|---|
| 5 | +3.2% | +2.3% | 0.72 | yes, every verdict cell |
| 10 | +4.1% | +2.6% | 0.64 | yes, every verdict cell |

The prize itself shrank: round 1 recorded B3's R3/N=1000 edge as +22%/+49%; under the
corrected harness it is +3.2%/+4.1%, because round 1's margin was measured against
trivial baselines crippled by transient-window calibration. B4 tracks B2c within noise
in every other cell.

### KD — drift (N = 1000, post-drift slice, best fitted (mode, window) vs best trivial)

(`eval.run_drift`, seeds 0-2; gates passed: Little dev <= 5.3% at N = 1000, per-lease
oracle post-slice coverage in band.)

| Scenario | t (s) | Best fitted cov95 | Skill vs trivial | Pass |
|---|---|---|---|---|
| DS | 5 | VOID (max cov 0.803) | - | FAIL |
| DS | 10 | VOID (max cov 0.775) | - | FAIL |
| DR | 5 | 0.937 (B2, rolling 150 s) | -5.4% vs B0 rolling 300 s | FAIL |
| DR | 10 | 0.939 (B2, rolling 150 s) | -7.8% vs B0 rolling 300 s | FAIL |

### Context: training size (~5000 vs ~500 completions, N = 1000)

Skill-point deltas at t = 5 / t = 10 s, derived by subtracting the two run summaries
(`eval.run_grid --ns 1000 --warmup-completions 5000 --out-suffix=-train5000` minus the
K1'/K2' run). One context-run gate flag: pooled oracle coverage at R4/t=10 was 0.929,
marginally under the 0.93 band floor; no verdict rides on this run.

| Regime | B3 delta | B4 delta | B2c delta |
|---|---|---|---|
| R1 | +1.0 / -0.6 | +0.2 / -1.1 | +0.3 / -0.8 |
| R2 | +8.4 / +16.7 | -0.3 / +0.2 | -0.3 / +0.0 |
| R3 | +0.7 / +3.6 | +1.3 / +3.0 | +0.2 / +1.5 |
| R4 | -1.5 / +1.4 | -1.1 / +2.3 | -0.7 / +2.3 |

Reading: the parametric fits are data-insensitive at 10x training (deltas within ~2
points, indistinguishable from noise); B3 alone moves materially, recovering from
negative to parity in R2 (+8.4/+16.7 from the verdict-table bases above, its
geometric-bucket tail was data-starved) and widening its R3 edge at 10 s (+4.1% to
+7.7%). Even so, at 5000 completions B3's best cell beats the best simpler alternative
by ~2 skill points (R3/t=10: B3 +7.7% vs B4 +5.6%) and still trails B2/B2c in R2 at
10 s (+8.6% vs +9.4%, from the training-size run's summary). B3's kill is partly data
starvation, and repairing it with 10x data still leaves the machinery under the 10%
materiality bar it was registered against.

## Verdicts

- **A: both pre-registered readings were mis-designed; the factorial answered a better
  question.** The round-1 R2/N=1000 collapse hinges on the calibration window sitting in
  the pool-fill transient: repairing placement alone restores validity (legacy training,
  late window), the training-only arm is untestable by construction, and window length
  was never the driver (300 s post-transient windows are valid with ~15 effective
  samples, refuting the effective-samples hypothesis); truncation costs 14-24% interval
  width but not coverage. Operational consequence: calibration residuals must come from
  a settled pool, and training from steady-state completions; neither requires a long
  window.
- **K1': B3 stays KILLED, now on clean footing.** Both arms hold on unbiased training:
  median CI-gated improvement +0.0% (< 10%), and no >= 10% valid-coverage width
  reduction in either favorable regime (R3 +2.8%, R4 -1.7%). The round-1 kill's scope
  defect is repaired: this is a verdict at ~500 unbiased observations under
  stationarity.
- **K2': PASS at t = 5 and 10 s under both void-cell rules** (+15.4% and +19.8%; no
  stochastic-void verdict cells remain, so the rules coincide). This supersedes the
  round-1 conditional verdict at t = 5 s in the layer's favor and removes the
  amendment-6 conditioning. Two supersessions of round-1 emergent notes follow from the
  same tables: the N=1000 heavy-tail coverage collapse (round-1 note 2) was harness
  artifact, not estimator bias — every fitted estimator except B1 is valid at N = 1000
  under the corrected harness; and the oracle-to-best-fitted gap (round-1 note 4) closes
  at stationarity (R2/N=1000: B2 +6.4%/+9.4% vs oracle +6.1%/+10.6%), so the
  stationary headroom is essentially captured by a censored lognormal with conformal
  calibration.
- **B4: fails its threshold; the reopen clause it guarded is moot.** B4 reaches 0.72 /
  0.64 of B3's R3/N=1000 skill (< 0.75), which by the letter of the pre-registered rule
  would reopen the nonparametric case. But that clause presumed the round-1 prize
  (+22%/+49%); the corrected prize is +3.2%/+4.1%, below the 10% materiality bar that
  K1' — the governing, first-registered criterion for estimator complexity — applies,
  and K1' holds. Adjudication, recorded rather than silently resolved: neither B3 nor
  B4 earns a slot beyond B2c; the residual latent-bimodality edge at R3/N=1000 is 1-2
  points of pinball skill and does not pay for either estimator's surface. The
  training-size context (B3 +7.7% at R3/t=10 on 5000 completions) does not change this:
  still ~2 points over B4 and under the bar. Beneficiary of the adjudication: the kill
  and simplicity — recorded so the direction is explicit.
- **KD: FAIL at both horizons, differently per scenario.** Under the abrupt shift no
  fitted mode reaches 0.90 post-shift coverage at N = 1000 at any rolling window (max
  0.803). Under the ramp, a 150 s rolling window restores validity (0.937/0.939) but
  the fitted layer's skill against rolling-conformal persistence is negative
  (-5.4%/-7.8%): the adaptation value lives entirely in the conformal layer, and the
  stale survival fit degrades the point forecast it wraps. Design consequence, as
  pre-registered: calibrated forecasting cannot be assumed through drift at scale; the
  ledger must detect drift (coverage canary) and fall back to the deterministic bound or
  a governor, and the cheapest robust adaptation observed is rolling-conformal residuals
  on a trivial point forecast, not refitting the survival model.
