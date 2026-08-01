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

| Slice | t (s) | Best refit arm-mode | Refit cov95 | Refit skill | Reference skill | Recovery |
|---|---|---|---|---|---|---|
| DR post | 5 | | | | | |
| DR post | 10 | | | | | |
| DS recovery | 5 | | | | | |
| DS recovery | 10 | | | | | |
| DS post (context) | 5 | | | | | |
| DS post (context) | 10 | | | | | |

### KR — KD-continuity table (10% CI-gated rule, refit arms included)

| Scenario | t (s) | Best fitted cov95 (arm-mode) | Skill vs trivial | Pass |
|---|---|---|---|---|
| DS | 5 | | | |
| DS | 10 | | | |
| DR | 5 | | | |
| DR | 10 | | | |

### DD — detection scoring (t = 5 s, W_d = 150 s, thresholds from pooled stationary max)

| Detector | theta | DS latency per seed (s) | DS median (s) | DR latency per seed (s) | DR median (s) | False alarms |
|---|---|---|---|---|---|---|
| D-cov | | | | | | |
| D-qshift | | | | | | |
| D-mix | | | | | | |

## Verdicts

- **KR:**
- **Framing:**
- **DD:**
