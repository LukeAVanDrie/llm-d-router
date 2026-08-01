# H1 results, round 3 (pre-registered): continuous refit under drift, and drift-detection scoring

Round-3 criteria for the H1 harness. As in [RESULTS.md](RESULTS.md) and
[RESULTS-2.md](RESULTS-2.md), the commit that adds this file with empty result tables is
the pre-registration record; criteria changes after that commit are amendments, dated
relative to the runs by commit and stating whom they favor. Round-2 harness rules carry
over unchanged: steady warm-up, settled pilots, the drift scenario parameters under
`data/params_default.json` `drift.*`, the Little gate (<= 10% per verdict-run seed), the
per-lease-oracle coverage gate ([0.93, 0.98] seed-pooled on each verdict slice), and the
coverage floors (< 0.90 voids a mode, >= 0.93 for width readings).

Scope: N = 1000 only (the KD verdict tier; the N = 100 context tier is dropped for run
cost), horizons t in {5, 10} s, drift seeds 0-2. Round-3 conventions live under
`drift.refit` and `drift.detection` in `data/params_default.json`, committed with this
file.

## What round 2 left open

KD adjudicated the harshest deployable configuration: survival fits frozen pre-drift,
adaptation through conformal calibration alone. It failed both scenarios, and the KD
verdict states the consequence — drift must be detected and met with a fallback. Two
questions follow, and they are this round:

- **KR.** The realistic loop — estimators refit on trailing completions at some cadence —
  is untested, not failed. If it recovers under gradual drift, the deterministic fallback
  is a rare-event path (abrupt breaks, detection gaps) and the product is a prediction
  engine with a safety net. If gradual drift defeats it too, the fallback is the common
  case and the product is a safety net with an occasional prediction engine.
- **DD.** Whatever KR finds, the fallback path needs a trigger. Which online signal
  detects calibration loss fastest at a matched false-alarm budget: rolling coverage
  deficit, residual-quantile shift, or completion-mix distance?

## Round-3 conventions

**Refit ladder.** Estimators {B1, B2}, identical on the refit and reference sides. B2 is
the mechanism sentence's estimator (B2c reduces to B2 in the uncapped drift mix); B1 is
the cheap age-blind control. B3 and B4 stay out: both are killed for complexity at
stationarity (K1', B4 verdicts), and reviving them here would change the question from
"does the shipped loop survive drift" to "does anything". Refit arms: cadence in
{30, 150, 600} s x trailing window in {500, 2000} completions (12 arms). Cadences are a
guess spanning near-continuous to once-per-drift-epoch; 500 completions is the design's
stated data requirement, 2000 is the smoother-but-staler alternative.
`min_fit_completions = 100` guards degenerate fits (convention; never binds after
warm-up).

**Refit honesty.** Refit epochs are anchored at the first calibration snapshot and step
by the cadence. The fit used at snapshot time u is trained only on the trailing window of
completions whose completion time is <= the latest epoch <= u. A completion is usable the
moment it completes (its length is observed then); residuals remain usable only at
realization time as in round 2. Gated in tests.py.

**Trailing-window wall time (derived).** Little's law gives the completion rate
lambda = N r / E[L], with E[L] from the committed samplers
(`sim/workloads.py` `OutputDist.mean()`): pre-drift (w = 1.0, pure R1) E[L] = 225 tokens,
lambda = 1000 * 40 / 225 = 178 completions/s, so 500 trailing completions span ~2.8 s;
post-drift (w = 0.25) E[L] = 623, lambda = 64/s, 500 completions span ~7.8 s (2000:
~11-31 s). Consequence, recorded before the runs: at this scale the trailing window is
always wall-clock fresh in count terms, so the binding constraints on refit adaptation
are the cadence (fit staleness) and the completion stream's inherent post-shift length
bias — immediately after a shift toward long outputs, the new mix's long requests have
not yet completed, so trailing completions under-represent exactly the mass that moved.
KR is a test of that bias as much as of the cadence.

**Modes.** As KD: every fitted arm (frozen and refit) is scored in native, static, and
rolling {150, 300, 600} s conformal modes; trivials B0/B0g get static and rolling. Static
residuals for refit arms come from the calibration window under the same refit
trajectory (the fit evolves during calibration too).

**Reference runs (the stationary yardstick).** Three seeds of the same cell held
stationary at the post-drift mix: null schedule w_start = w_end = 0.25, rng label
`DRIFTREF`, steady warm-up (training drawn from the settled 0.25 mix), calibration 600 s,
evaluation 900 s, same capacity as the drift runs (the post-mix pilot). Reference skill
per horizon = pinball@q95 skill of the best valid frozen fitted arm-mode over the best
valid trivial on the full eval window, seed-pooled, same mode menu. This is the skill the
layer would have if it had always lived at the post-drift mix; it is what "full recovery"
means.

**Detection: monitored configuration.** The production mode KD showed degrading: B2
frozen on warm-up training, static conformal q95 from the calibration window, horizon
t = 5 s (shortest verdict horizon = shortest realization lag; t = 10 s recorded as
context in the CSV, no verdict). Residual of the snapshot at time u realizes at u + t.

**Detectors.** Statistic computed at every eval snapshot time s over a trailing window
W_d = 150 s (convention: the rolling window that restored validity in KD; {60, 300}
recorded as context), requiring >= 20 window samples (convention), else no alarm:

- **D-cov (rolling coverage deficit):** miss rate of the monitored q95 bound over
  residuals realizing in (s - W_d, s], minus the nominal 0.05.
- **D-qshift (residual-quantile shift):** (rolling q95 of realized residuals minus the
  static calibration q95) / IQR of calibration residuals (robust scale; always defined).
- **D-mix (completion-mix distance):** two-sample KS statistic between completion lengths
  completing in (s - W_d, s] and the frozen warm-up training set. The work table named
  this signal "admission-mix distance"; in DS/DR the drifting quantity is output length,
  which is observable only at completion — admission-time features (prompt lengths) do
  not drift by construction — so the implementable form is completion-mix distance, and
  the row is renamed accordingly.

**Threshold rule (matched false-alarm budget).** Six stationary null runs (schedule
w_start = w_end = 1.0, rng label `DRIFTNULL`, seeds 0-5, same windows and capacity).
Per detector, theta = the maximum of its statistic pooled over all null-run eval
snapshots (zero observed stationary false alarms by construction). The max rule favors
false-alarm avoidance over latency, uniformly across detectors, which matches the
trigger's cost structure (a false fallback forfeits multiplexing). Context, no ranking
weight: alarm counts at the pooled q0.99 threshold.

## Questions and decision rules

### KR — does the continuous-refit loop recover under drift?

Runs: DS and DR at N = 1000, seeds 0-2, KD's scenario parameters and pilot capacity.
Fitted side under verdict = the 12 refit arms; frozen B1/B2 are reported as context tying
to KD. Verdict slices, relative to eval start:

- **DR post:** [600, 900] s (as KD).
- **DS recovery:** [600, 900] s — the last 300 s of the KD post slice, i.e. shift + 300 s
  onward. The full DS post slice [300, 900] is context: it necessarily contains the
  adaptation transient, and the framing question is about the steady posture after a
  break, with the transient priced separately by DD's detection latency.

**Recovery criterion**, per slice x horizon: some refit arm-mode with seed-pooled slice
coverage >= 0.90 achieves pinball@q95 skill vs the slice's best valid trivial of at least
(reference skill at the same horizon) - 5 points (margin: convention). The comparison is
across runs and unpaired; both sides are point estimates with their own 90% CIs reported.
Selection over 12 arms x 5 modes favors the refit layer; accepted because KR asks an
existence question — can any realistic refit loop recover — and a pass still owes H2
shadow-mode validation before the layer gets authority.

**Secondary (KD continuity):** the KD pass rule verbatim — valid coverage plus CI-gated
skill >= 10% vs the best post-slice-valid trivial — with refit arms added to the fitted
side, on the KD post slices. This reading answers "does the layer clear the round-2
materiality bar during drift"; it can fail while recovery passes if the post-drift mix is
a low-skill regime even at stationarity (round 2 measured R2-dominated stationary skill
at +6.4%/+9.4%, below 10%, with the oracle at +6.1%/+10.6% — the bar can be unreachable
in principle there, which is why recovery, not this rule, carries the framing verdict).

**Framing verdict (the row-1 question), pre-registered readings:**

- **"Prediction engine with a safety net"** iff recovery holds on DR-post and DS-recovery
  at both horizons: the loop tracks gradual drift and re-earns its keep within 300 s of
  an abrupt break, so the fallback is a transient/detection-gap path.
- **"Safety net with an occasional prediction engine"** iff recovery fails on DR-post at
  both horizons: gradual drift defeats even the refit loop, and calibrated forecasting is
  only trustworthy in demonstrated stationary stretches.
- Any other pattern is mixed and is adjudicated in writing against these two readings,
  with the direction of the adjudication recorded.

Gates: as round 2 (Little per seed, per-lease-oracle pooled coverage per verdict slice in
[0.93, 0.98]); reference runs gate on their full eval window. Edge rules as KD: a slice
with no valid trivial, or a reference run with no valid fitted arm, is flagged for
adjudication rather than auto-scored.

### DD — which signal detects calibration loss fastest?

Runs: the DS/DR runs (seeds 0-2; identical rng streams to KD's) and the six null runs.
Detection latency per drift run = (first eval snapshot time s in the scoring window at
which the statistic exceeds theta) minus onset. Onsets and scoring windows: DS onset =
shift at 300 s, scored on (300, 900]; DR onset = ramp start at 0 s, scored on (0, 900].
No alarm in the window = MISS (ranks below any finite latency). Alarms during the DS
pre-shift stationary stretch [0, 300) are false alarms; reported, not ranked (the
threshold rule already fixed the false-alarm budget).

**Ranking rule:** detectors are ranked by median DS latency across seeds; ties broken by
median DR latency. DS carries the ranking because the abrupt break is the case the
fallback trigger exists for; DR latency is reported alongside (a slow ramp is
legitimately detected late — the calibration loss itself arrives late).

**Usability floor (convention):** the ledger document may describe the coverage canary as
a fast trigger only if the winning detector's median DS latency is <= 60 s
(`fast_trigger_floor_s`; ~10% of the 600 s drift epoch). If no detector meets the floor,
the drift-posture sentence must state that detection is slow and the fallback
correspondingly conservative.

No kill attaches to DD; it selects a design and bounds the exposure window
(latency x horizon) that the fallback leaves open.

## Results

### KR — recovery table (N = 1000, seed-pooled)

(`eval.run_drift_refit`, seeds 0-2; all gates passed: Little dev <= 5.3% per verdict
seed, per-lease-oracle coverage in band on every verdict slice and the reference eval
window. Reference skill, from the three stationary w = 0.25 runs: +2.4% at t = 5 s
(B2-static vs B0-static, 90% CI on the paired diff [-177, +4]) and +4.7% at t = 10 s
(B2-static vs B0-roll600, CI [-441, +3]) — both CIs include zero, so the stationary
prize at this heavy-tail-dominated mix is itself within noise, consistent with round 2's
R2 rows sitting under the 10% bar. Skill CIs below are the paired diff vs the slice's
best valid trivial; negative bounds favor the refit arm.)

| Slice | t (s) | Best refit arm-mode | Refit cov95 | Refit skill | Reference skill | Recovery |
|---|---|---|---|---|---|---|
| DR post | 5 | B2-c150-n2000 native | 0.902 | +2.1% (CI [-305, +191]) | +2.4% | YES |
| DR post | 10 | B2-c30-n2000 roll150 | 0.986 | -17.5% (CI [+315, +1277]) | +4.7% | NO |
| DS recovery | 5 | B2-c150-n500 native | 0.923 | +3.2% (CI [-498, +396]) | +2.4% | YES |
| DS recovery | 10 | B2-c150-n2000 roll150 | 0.973 | -3.9% (CI [-348, +618]) | +4.7% | NO |
| DS post (context) | 5 | B2-c30-n2000 roll150 | 0.929 | +32.4% (CI [-6327, +579]) | +2.4% | (context) |
| DS post (context) | 10 | B2-c30-n2000 roll150 | 0.921 | +44.6% (CI [-15487, -910]) | +4.7% | (context) |

Slice detail from `results/drift_refit.csv`, recorded because the verdicts lean on it:

- **Active ramp (DR ramp, 0-600 s): no deployable mode is valid at either horizon.**
  Family coverage maxima: refit 0.893 / 0.830 (B2-c30-n2000 roll150, t = 5 / 10 s),
  trivial 0.745 / 0.718 (B0g roll150), frozen 0.665 / 0.520 (B2 roll150). Continuous
  refit is closest to the floor but under it; only the per-lease oracle is valid while
  the mix is moving.
- **Post-break transient (DS early, 300-600 s): fast refit is the only valid family.**
  B2-c30 arms with roll150 hold 0.900-0.906 at both horizons; no trivial and no frozen
  mode reaches 0.90. The DS post context rows' large skill (+32.4%/+44.6%) is earned in
  this transient, against trivials whose rolling windows are polluted by the shift.
- **The t = 10 s failure is completion-stream length bias, not staleness.** On DR post
  and DS recovery, every refit arm's native mode is void at t = 10 s (coverage
  0.824-0.878) even at cadence 30 s with a trailing window spanning ~11-31 s of wall
  time (derivation in the conventions above): after a shift toward the long-output mix,
  completions under-represent the new tail for a duration on the order of its residence
  time (16384 / 40 = 410 s), the fit under-covers, and only short rolling windows
  restore validity at a width that forfeits skill (best valid refit pinball 4965 vs
  best trivial 4227 on DR post t = 10 s). At t = 5 s the same bias is survivable:
  refit native modes are valid (0.902-0.923) and match the reference skill.

### KR — KD-continuity table (10% CI-gated rule, refit arms included)

| Scenario | t (s) | Best fitted cov95 (arm-mode) | Skill vs trivial | Pass |
|---|---|---|---|---|
| DS | 5 | B2-c30-n2000 roll150, 0.929 | +32.4% vs B0g-roll150 | FAIL (CI includes zero) |
| DS | 10 | B2-c30-n2000 roll150, 0.921 | +44.6%* vs B0g-roll150 | PASS |
| DR | 5 | B2-c150-n2000 native, 0.902 | +2.1% vs B0-roll300 | FAIL |
| DR | 10 | B2 (frozen) roll150, 0.939 | -7.8% vs B0-roll300 | FAIL |

(The DS/t=10 PASS is earned in the transient: on the recovery slice alone the best
refit arm is -3.9%. The KD-continuity rule scores the full KD post slice as registered,
so the pass stands, with this note recording where the skill lives. At DR/t=10 the best
valid fitted mode overall is the frozen fit with rolling conformal, reproducing the KD
round-2 cell; refitting makes t = 10 s worse there, per the bias mechanism above.)

### DD — detection scoring (t = 5 s, W_d = 150 s, thresholds from pooled stationary max)

(`eval.run_drift_detect`, drift seeds 0-2, null seeds 0-5; all Little deviations <= 5.3%.
Latencies in seconds from onset; DS onset = shift at 300 s, DR onset = ramp start.)

| Detector | theta | DS latency per seed (s) | DS median (s) | DR latency per seed (s) | DR median (s) | False alarms |
|---|---|---|---|---|---|---|
| D-cov | 0.130 | 23, 25, 27 | 25 | 94, 86, 85 | 86 | 0 |
| D-qshift | 0.683 | 16, 19, 13 | 16 | 86, 86, 76 | 86 | 0 |
| D-mix | 0.051 | 83, 54, 56 | 56 | 249, 165, 181 | 181 | 0 |

Window-sweep context (median DS latency at each window's own stationary-max theta):
W = 60 s gives D-cov 20 / D-qshift 15 / D-mix 23; W = 300 s gives 32 / 20 / 97. The
ranking is the same at every window.

## Verdicts

- **KR: recovery splits by horizon — YES at t = 5 s on both verdict slices, NO at
  t = 10 s on both.** At the 5 s horizon the refit loop returns to reference-level
  skill with valid native quantiles within 300 s of an abrupt break and after a ramp
  settles; at 10 s no refit arm recovers, because the post-shift completion stream
  under-represents the new mix's tail for roughly its residence time and short rolling
  windows buy validity at a width that hands the skill advantage to rolling-conformal
  persistence. Two findings outside the pre-registered cells, both from the committed
  CSV: nothing deployable is valid while a ramp is in progress (refit comes closest,
  0.893/0.830 vs the 0.90 floor), and in the first 300 s after a break the fast-cadence
  refit arms are the only valid family at either horizon.
- **Framing: mixed by the pre-registered readings; adjudicated as follows (direction:
  this narrows the layer's claimed envelope rather than expanding it, and neither
  pre-registered pole is adopted).** The fallback is not a rare-event path: it is the
  standing posture whenever drift is actively in progress (no valid deployable mode on
  the ramp) and, for horizons near 10 s, for a further window on the order of the new
  tail's residence time after the mix settles. The fallback is also not the common
  case: at stationarity the layer is valid and skilled (round 2), at t <= 5 s the refit
  loop re-arms within 300 s of a break — marginally holding validity even inside the
  transient at cadence 30 s — and the detection latency that opens the fallback window
  is 16-25 s (DD). The shipped sentence for the ledger document: statistically
  multiplex while calibration demonstrably holds; on a canary trip, drop to the
  degraded mode (rolling-conformal residuals on trivial growth) or the deterministic
  bound; re-admit the fitted layer per horizon as its coverage returns, which the
  refit loop achieves at 5 s horizons within minutes and does not achieve at 10 s
  horizons until the completion stream clears the shift. One overlay the framing
  question presupposed away: at this heavy-tail-dominated mix the stationary prize is
  +2.4%/+4.7% with CIs through zero, so "engine vs net" at drift is a second-order
  question there; the engine's first-order value lives in the chat/truncation regimes
  (round 2: +25-48%) and in the post-break transient (+32%/+45% vs polluted trivials).
- **DD: D-qshift wins — median DS latency 16 s at zero observed false alarms; the
  fast-trigger floor (60 s) is met.** D-cov is a close second (25 s) and is the
  directly interpretable form (it reads the monitored bound's own miss rate); D-mix is
  last (56 s, and 3.5x slower on the ramp) for the same reason refit fails at t = 10 s:
  completion-side signals inherit the post-shift length bias by construction. The
  ranking is unchanged at every window in the sweep. Design consequence: the coverage
  canary should trigger on the rolling residual-quantile shift (or its miss-rate twin),
  never on completion-mix distance alone; with a 150 s window the fallback's undetected
  exposure after an abrupt break is tens of seconds at N = 1000, t = 5 s.
