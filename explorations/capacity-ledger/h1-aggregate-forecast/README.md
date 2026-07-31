# H1: does age-conditioned survival modeling earn its complexity?

Falsification experiment for the capacity ledger's stochastic layer
(`docs/flow-control-capacity-ledger.md`). Pre-registered protocol; kill criteria live in
[RESULTS.md](RESULTS.md) and are committed before any evaluation code produces numbers.

## Question

For horizons of 1-10 seconds and realistic lease populations, does the proposed machinery
(age-conditioned nonparametric survival curves) forecast **aggregate** KV-token occupancy
materially better than much simpler alternatives?

The curves are over output length in tokens, conditioned on tokens generated so far; wall-clock
enters only via each lease's observed decode rate. This is population-level forecasting.
Per-request output-length prediction (unsolved in the serving literature per recalled,
unverified sources — `s3-line` in [../sources.md](../sources.md)) is required nowhere: the
estimators below never read request content, only stratum identity and progress.

## Scope and non-goals

The verdict applies to the admission/overcommit use case (aggregate occupancy quantiles) only.
Per-lease decisions (wait-vs-evict victim choice) need per-lease survival and are a separate
hypothesis; a per-lease Brier diagnostic is recorded here as a pointer, not a verdict input.
The simulation is stationary; the verdict is conditional on stationarity (B3's drift machinery
is motivated by non-stationarity and is out of scope).

## Forecast object

Per active lease i with current footprint f_i tokens, decode rate r_i tok/s, age n_i tokens:
occupancy contribution at horizon t is (f_i + r_i t) if the lease is still running, else 0.
Given fixed known r_i this is a scaled Bernoulli with survival probability
p_i = S(n_i + r_i t) / S(n_i); all estimator differences flow through S alone.
Pool forecast: mean = sum p_i (f_i + r_i t), variance = sum (f_i + r_i t)^2 p_i (1 - p_i)
under lease independence. Native quantiles: normal approximation at N >= 100, Monte Carlo
(2000 draws) at N = 10 and as a spot check.

Primary target **T1**: occupancy at t attributable to leases active at forecast time (isolates
survival modeling from arrival forecasting). Total-occupancy target T2 with a shared admissions
model is deferred (day 2, kill criterion K3).

## Estimator ladder

| ID | Estimator | Training data | Quantiles |
|---|---|---|---|
| B0 | Persistence: O(t) = O(0) | none | conformal only |
| B0g | Deterministic growth, no completions: sum min(f_i + r_i t, p_i + max_tokens). The strongest baseline needing no training data. | none | conformal only |
| B1 | Age-blind constant hazard (geometric; exposure-form fit) per stratum | completions | native + conformal |
| B2 | Lognormal MLE per stratum, analytic conditional survival | completions | native + conformal |
| B2c | B2 with cap-hits treated as right-censored + point mass at max_tokens (R4 fairness) | completions | native + conformal |
| B3 | Discrete hazard over geometric age buckets (ratio 1.5 from 16 tok), KM product, shrinkage toward B1 (alpha = 5 pseudo-obs/bucket). The proposed machinery. | completions | native + conformal |
| Oracle | True generating S | n/a | native |

Conformal wrapper: per-horizon empirical quantiles of signed residuals from a fixed
calibration window, added to the point forecast. Every baseline is scored in its best valid
mode, so point forecasters are not disadvantaged on quantile metrics (and if conformal-B0g
matches native-B3, the machinery is dead).

The oracle defines the achievable ceiling; the reported quantity of interest per estimator is
gap-to-oracle.

## Simulator

Fixed-step pool, dt = 100 ms, vectorized numpy.

- Arrivals: Poisson, rate from Little's law to hit target steady-state N. Drop (no queue) when
  occupancy > 0.95 C; queueing dynamics deliberately excluded from phase 1.
- Lease: prompt p_i ~ regime prompt dist (cache-hit ratio 0 in phase 1); true length L_i ~ regime
  output dist; r_i = 40 tok/s fixed (sensitivities deferred: lognormal cv 0.3; load-coupled).
- Completion frees the full footprint when generated >= L_i or generated >= max_tokens (R4).
- Capacity C sized for ~75-80% mean utilization.
- Protocol: warm-up until >= 500 completions (training set), 300 s calibration window (conformal
  residuals), 600 s evaluation window, snapshots every 1 s joined against realized occupancy at
  each horizon.

Sanity gates, checked before any comparison numbers are read:

1. Realized steady-state N within 10% of the Little's-law target.
2. Oracle q95 coverage on T1 in [93%, 97%]. Outside that band, the forecast math or the
   simulator is wrong; stop. (Band and evaluation amended after the smoke and first full
   runs — upper bound 98%, seed-pooled; RESULTS.md amendments 2 and 5 carry the reasons.)

## Regimes

- **R1 chat**: lognormal, median ~150 tok, sigma_log 0.9.
- **R2 agentic heavy tail**: lognormal sigma_log 1.4, Pareto splice above p95, tail to 16k.
- **R3 mixture (pre-declared B3-favorable)**: 0.6 lognormal(median 80) + 0.4 lognormal(median 2000).
- **R4 truncation (pre-declared B3-favorable)**: R2 capped at max_tokens = 1024 (atom at the cap).

Grid: {R1..R4} x N {10, 100, 1000} x t {1, 2, 5, 10} s x 3 seeds. Verdict cells are
N in {100, 1000} and t in {5, 10} s; t <= 2 s cells are reported but pre-registered as
expected-null (deterministic growth is near-optimal there); N = 10 is a small-pool annex
(native normality fails; deployment target is pools of >= ~50 leases).

## Metrics

1. **Primary: pinball loss at q in {0.5, 0.9, 0.95, 0.99}**, headline q95 (the admission bound is
   a one-sided upper quantile of occupancy).
2. Conservative-quantile quality: q95 empirical coverage (validity gate: width counts only if
   coverage >= 93%; < 90% disqualifies the cell) and mean width, in tokens and as % of
   mean headroom (C minus mean occupancy).
3. Secondary: MAE of the mean forecast.
4. Diagnostic, non-verdict: per-lease Brier score + reliability curve for P(alive at t).
5. Skill scores vs best-of(B0, B0g) per cell.

Uncertainty: moving-block bootstrap over snapshot times (block 30 s, 1000 resamples), 90% CIs on
cell metrics and on paired skill differences. An improvement counts only if the paired CI
excludes zero.

## Data

Synthetic-first: `data/params_default.json` carries frozen calibrated parameters so the verdict
requires no downloads. Real traces (Azure LLM inference traces, BurstGPT) are day-2 external
validity checks, not verdict inputs. Workload regime shapes should be cross-checked against
inference-perf's synthetic workload distributions where applicable rather than inventing new
conventions.

## Layout

```
README.md            this protocol
RESULTS.md           pre-registered criteria + result tables (criteria committed first)
data/                params_default.json (frozen parameters; trace calibration goes
                     through inference-perf loaders, no scripts written here)
sim/                 workloads.py (regime samplers), simulator.py (pool dynamics, sanity gates)
estimators/          base.py (interface + aggregation), baselines.py, b3_km.py, conformal.py
eval/                metrics.py, run_grid.py, bootstrap.py, plots.py
phase2/              deferred and unwritten: independent + informative censoring preview
results/             gitignored
.venv/               gitignored (python3 -m venv; numpy, scipy, matplotlib)
```

Run: `.venv/bin/python -m eval.run_grid --synthetic` from this directory.

## Execution order

1. Commit README.md + RESULTS.md (pre-registration).
2. workloads + simulator + oracle; pass both sanity gates.
3. B0/B0g/B1/B2/B2c + aggregation + metrics.
4. B3 + conformal wrapper.
5. Run grid on T1, bootstrap, fill RESULTS.md, evaluate K1/K2; commit results separately.

Deferred (day 2+): T2/K3 admissions dominance, phase-2 censoring preview, real-trace replay,
sensitivities (noisy r_i, load-coupled rates, strata misassignment, N = 10 annex).
