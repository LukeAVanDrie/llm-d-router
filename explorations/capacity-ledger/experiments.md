# Experiments

One row per script, one script per question, kept because a number in the prose has to
show its derivation and the script is the derivation. Runs live under
`h1-aggregate-forecast/` (venv, fixed seeds; `results/` is regenerated, not committed).
Pre-registration and verdicts: [h1-aggregate-forecast/RESULTS.md](h1-aggregate-forecast/RESULTS.md)
(round 1), [h1-aggregate-forecast/RESULTS-2.md](h1-aggregate-forecast/RESULTS-2.md)
(round 2: corrected harness, windows, drift), and
[h1-aggregate-forecast/RESULTS-3.md](h1-aggregate-forecast/RESULTS-3.md) (round 3:
continuous refit under drift, drift-detection scoring).

## H1: aggregate occupancy forecasting

| Script | Question | What it found |
|---|---|---|
| `tests.py` | Are the harness statistics right? | Analytic oracle survival matches 10M-sample empirical CDFs in all four regimes (observed max error 3.8e-4; the committed gate asserts < 5e-3); censored lognormal MLE recovers (mu, sigma) under cap truncation where the naive fit does not (mu error 0.010 vs 0.251; committed gate asserts < 0.05); conformal coverage nominal on held-out gaussian residuals; normal-approx aggregation matches Monte Carlo mean/sd/q95. Round 2 adds: B4 censored-EM recovers the R3-class mixture (w error < 0.02, survival error < 0.01 censored and not), rolling conformal provably uses no unrealized residuals, drift mixture and per-lease oracle exact, steady warm-up unbiased (train max 11192 vs legacy 723 at R2/N=1000) |
| `eval.diag_training_bias` | Is the warm-up training set length-biased? | Legacy rule (collect from t = 0) truncates near r * t_warmup: R2/N=1000 training mean 184 vs true 756, max 723 against true q95 ~ 2989; R1/N=1000 max 272 vs q95 657. The steady rule (500 s burn-in) is unbiased. This defect motivated the round-2 re-adjudication (RESULTS-2.md) |
| `eval.run_grid` | Do age-conditioned survival curves earn their complexity for aggregate forecasts (K1/K1'), and does any stochastic forecaster beat trivial baselines (K2/K2')? | Round 1 (RESULTS.md, legacy warm-up, fixed 300 s window): K1 killed B3 at 500 training observations; K2 passed at 10 s, conditionally at 5 s (amendment 6); N=1000 heavy-tail cells went coverage-void. Round 2 (RESULTS-2.md, steady warm-up, adaptive window, B4 in the ladder): the round-1 voids were harness artifacts (truncated training + transient-placed calibration + undersized pilots); with them fixed, K1' re-kills B3 on unbiased data (+0.0% median, no width win; 10x training still leaves it under the bar), K2' passes at its verdict horizons 5/10 s under both void rules (+15.4%/+19.8%; the t = 2 s context cells also clear their 5% threshold), B4 fails its 0.75 threshold against a prize that shrank to +3-4%, and the parametric lognormal with conformal calibration reaches the oracle ceiling at stationarity (R2/N=1000: +6.4% vs oracle +6.1% at 5 s) |
| `eval.run_window_factorial` | Which mechanism collapsed R2/N=1000 in round 1: training truncation or calibration-window length? | Neither alone: window placement. Every steady-state-placed window is valid at every length (legacy 0.971-0.978 across W = 300-2400 s), and a 300 s post-transient window with unbiased training is valid too (confirmatory cell), so the round-1 collapse required truncated training AND a calibration window inside the pool-fill transient; either repair suffices. Truncation costs 14-24% interval width, not coverage. Refutes the effective-samples hypothesis (stated with its derivation in RESULTS-2.md question A) |
| `eval.run_drift` | Does parametric+conformal calibration survive drift at N = 1000 (KD)? | FAIL at both horizons, both scenarios. Abrupt 75% mix shift: no fitted mode recovers 0.90 post-shift coverage at any rolling window (max 0.803). Ramp: a 150 s rolling window restores validity (0.937/0.939) but skill vs rolling-conformal persistence is negative (-5.4%/-7.8%) — the adaptation value is in the conformal layer, and the stale fit hurts the point forecast it wraps. Consequence: drift is a detected regime (coverage canary, deterministic fallback), not a forecastable one |
| `eval.run_drift_refit` | Does the continuous-refit loop recover under drift (KR)? | Split by horizon (RESULTS-3.md). At t = 5 s: YES on both verdict slices — refit-B2 returns to reference-level skill (+2.1%/+3.2% vs reference +2.4%) with valid native quantiles within 300 s of an abrupt break. At t = 10 s: NO on both — post-shift completions under-represent the new tail for ~its residence time (410 s), native modes go void (0.82-0.88), and rolling-window repairs forfeit skill to rolling-conformal persistence (-17.5%/-3.9%). Nothing deployable is valid while a ramp is in progress; in the first 300 s after a break, fast-cadence refit + roll150 is the only valid family. Framing adjudicated: multiplex while calibration holds, degrade on canary trip, re-admit per horizon as coverage returns |
| `eval.run_drift_detect` | Which online signal detects calibration loss fastest (DD)? | D-qshift (rolling residual-q95 shift) wins: median DS latency 16 s at zero observed false alarms across six stationary null runs, fast-trigger floor (60 s) met; D-cov 25 s; D-mix (completion-mix KS) last at 56 s because completion-side signals inherit the post-shift length bias. Ranking unchanged across window sweep {60, 150, 300} s (RESULTS-3.md) |
| `eval.plots` | (figures) | `results/skill_vs_horizon.png`, `results/coverage_width.png` from `results/cells.csv` |

## Work table

Ordered by decisiveness per cost; future sessions take work from the top unless directed
otherwise. A row re-earns its place at triage or is deleted with the reason in the commit
body ([STYLE.md](STYLE.md)).

1. **Censoring-aware refit under drift (KR follow-up).** KR's t = 10 s failure is
   completion-stream length bias: in-flight long leases are exactly the observations the
   trailing-completion fit is missing. Refitting on completions plus in-flight ages as
   right-censored observations (the B2c/Tobit machinery, already committed and gated in
   tests.py) removes that bias in principle and would also repair D-mix's lag. One
   RESULTS-3 amendment or a short RESULTS-4; the harness needs only an in-flight-ages
   feed into the refit epoch. If it restores t = 10 s native validity post-drift, the
   re-admission rule in the drift posture simplifies to one horizon-blind sentence.
   Waits on nothing.
2. **Hold-placement design round.** Assessment finding 3: the decision that unblocks the
   deterministic ledger skeleton. Waits on a design discussion.
3. **Ledger-revision review and upstreaming.** The local rewrite is drafted at
   [ledger-revision.md](ledger-revision.md) (mechanism sentence, confidence dial with
   the read fractile rule, drift posture per RESULTS-3, transient discipline, horizon
   boundary, per-request-probability vs point-prediction distinction). Waits on the
   user's read, and on public homes for the exploration citations before any upstream
   PR.
4. **Telemetry change (upstream).** Termination-cause enum and a streamed-token counter
   (assessment finding 4); training-data accrual is the long pole and cannot be
   backfilled. Waits on the user's go-ahead to prepare an upstream issue and PR.
5. **T2 / K3 admissions dominance.** Does arrival forecasting dominate length modeling
   for total-occupancy forecasts? Waits on the shared admissions forecaster and the
   oracle-arrivals ablation.
6. **Censoring preview (phase 2).** Degradation of censoring-aware fits under independent
   vs eviction-shaped censoring; flag threshold pre-registered in the protocol. Waits on
   the injectors.
7. **BLIS re-score (L0.5).** Do the verdicts survive batching-coupled decode rates and
   realistic arrivals? Waits on reading the BLIS API (`blis` in sources.md).
8. **Backlog.** L0 sensitivities from the protocol not yet run: noisy decode rates
   (estimated r with lognormal error, applied to all estimators alike) and
   burst-correlated lengths (the lease-independence stress). Engine-source verification
   that the residency stocks, abort-on-disconnect, and the shared per-iteration token
   budget (finding 2's premise) behave as the resource model assumes (currently
   recalled, per the assessment's standing table); precedes any EPP wiring. Bernstein-bound note for the conservative end of the dial (paper exercise).
   Small-pool (N = 10) regime treatment (MC quantiles throughout; Little deviations
   11-32% are expected time-average variance). Reads for the still-machine-read sources
   (PLP, remlen, UniBoost) if any claim comes to lean on them.
