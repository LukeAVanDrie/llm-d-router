"""Question KD (RESULTS-2.md): does calibration survive non-stationarity at scale?

Scenarios DS (shift) and DR (ramp) from params drift.*: arrivals are a two-component mix
(A = R1 chat, B = R2 heavy tail) whose weight moves from w_start to w_end during the eval
window; arrival rate holds the target pool size constant. Training and fits are frozen
pre-drift (steady warm-up at w_start); adaptation under test is conformal calibration
only: static calibration-window quantiles plus honest rolling windows
(drift.rolling_windows_s), identically available to every estimator including trivials.

Verdict (KD, pre-registered): per scenario x horizon at N=1000 on the post-drift slice,
the fitted layer passes iff some fitted (estimator, mode, window) with seed-pooled
coverage >= 0.90 achieves CI-gated pinball@q95 skill >= 10% vs the best post-slice-valid
trivial. Gates: Little's law on the eval window; seed-pooled per-lease-oracle q95
coverage on the post-drift slice in [0.93, 0.98].

Run: .venv/bin/python -m eval.run_drift
Outputs: results/drift.csv, printed KD table for RESULTS-2.md.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from estimators.b3_km import B3DiscreteHazardKM        # noqa: E402
from estimators.baselines import (                     # noqa: E402
    B0Growth, B0Persistence, B1ConstantHazard, B2cCensoredLognormal, B2Lognormal,
    OraclePerLease)
from estimators.conformal import rolling_quantile      # noqa: E402
from estimators.mixture_em import B4LognormalMixtureEM  # noqa: E402
from eval import bootstrap, metrics                    # noqa: E402
from eval.forecast_common import collect               # noqa: E402
from sim import simulator, workloads                   # noqa: E402

SCENARIOS = (("DS", "shift"), ("DR", "ramp"))
NS = (100, 1000)
VERDICT_N = 1000
SEEDS = (0, 1, 2)
HORIZONS = (5.0, 10.0)
COVERAGE_VOID = 0.90
SKILL_THRESHOLD = 0.10
FITTED = ("B1", "B2", "B2c", "B3", "B4")
TRIVIAL = ("B0", "B0g")
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def slices(scenario: str, d: dict) -> dict:
    """Named slices in seconds relative to eval start: the pre-registered post-drift
    verdict slice plus context slices."""
    if scenario == "DS":
        s = d["shift_at_eval_s"]
        return {"post": (s, s + 600.0), "transient": (s, s + 150.0)}
    ramp_end = d["ramp_over_eval_s"]
    return {"post": (ramp_end, ramp_end + 300.0), "ramp": (0.0, ramp_end)}


def main():
    params = workloads.load_params()
    d = params["drift"]
    rolling = [float(w) for w in d["rolling_windows_s"]]
    comp_a, _ = workloads.build_regime(d["components"][0], params)
    comp_b, _ = workloads.build_regime(d["components"][1], params)
    post_mix = workloads.TwoComponentMix(comp_a, comp_b, d["w_end"])

    rows, table, gate_failures = [], {}, []
    for n in NS:
        pilot = simulator.run_cell("DRIFT", n, 999, params, capacity=None,
                                   out_dist_override=post_mix)
        print(f"[pilot] post-mix N={n}: capacity={pilot.capacity:,.0f} tokens")
        for label, kind in SCENARIOS:
            schedule = workloads.build_drift_schedule(kind, params)
            per_seed = []
            for seed in SEEDS:
                res = simulator.run_cell(
                    "DRIFT", n, seed, params, capacity=pilot.capacity,
                    warmup_rule="steady", calib_window_s=d["calibration_window_s"],
                    drift=schedule, eval_window_s=d["eval_window_s"])
                rng = np.random.default_rng(np.random.SeedSequence([987, n, seed]))
                fitted = [
                    B1ConstantHazard().fit(res.training_lengths),
                    B2Lognormal().fit(res.training_lengths),
                    B2cCensoredLognormal(None).fit(res.training_lengths),
                    B3DiscreteHazardKM(cap=None).fit(res.training_lengths),
                    B4LognormalMixtureEM(None).fit(res.training_lengths),
                    OraclePerLease(comp_a, comp_b),
                ]
                trivials = [B0Persistence(), B0Growth(np.inf)]
                ct, calib = collect(res.calib, trivials, fitted,
                                    res.rate_tokens_per_s, HORIZONS, rng,
                                    skip=("oracle",))
                et, evald = collect(res.eval, trivials, fitted,
                                    res.rate_tokens_per_s, HORIZONS, rng)
                little_ok, little_dev = simulator.little_gate(res)
                if not little_ok and n == VERDICT_N:
                    gate_failures.append(f"{label}/N={n}/s{seed}: Little {little_dev:.2%}")
                per_seed.append({"res": res, "ct": ct, "calib": calib,
                                 "et": et, "eval": evald})
                print(f"[sim] {label} N={n} seed={seed}: "
                      f"little_dev={little_dev:.3f} train={len(res.training_lengths)}")

            for t in HORIZONS:
                for sl_name, (lo, hi) in slices(label, d).items():
                    # Per (model, mode): pooled coverage, mean pinball, concat series.
                    pooled = {}
                    for name in FITTED + TRIVIAL + ("oracle",):
                        modes = {}
                        if name == "oracle":
                            modes["native"] = "native"
                        else:
                            modes["static"] = "static"
                            for W in rolling:
                                modes[f"roll{int(W)}"] = W
                            if name in FITTED:
                                modes["native"] = "native"
                        for mode_name, mode in modes.items():
                            covs, pins, series = [], [], []
                            for s in per_seed:
                                eval_start = s["res"].eval_start_s
                                de = s["eval"][(name, t)]
                                rel = s["et"] - eval_start
                                sel = (rel >= lo) & (rel < hi)
                                realized = de["realized"][sel]
                                if mode == "native":
                                    upper = de["q0.95"][sel]
                                elif mode == "static":
                                    dc = s["calib"][(name, t)]
                                    resid = dc["realized"] - dc["point"]
                                    upper = de["point"][sel] + float(
                                        np.quantile(resid, 0.95, method="higher"))
                                else:
                                    dc = s["calib"][(name, t)]
                                    all_times = np.concatenate([s["ct"], s["et"]])
                                    all_resid = np.concatenate(
                                        [dc["realized"] - dc["point"],
                                         de["realized"] - de["point"]])
                                    q = rolling_quantile(all_times, t, all_resid,
                                                         s["et"][sel], mode, 0.95)
                                    upper = de["point"][sel] + q
                                covs.append(metrics.coverage(realized, upper))
                                pins.append(metrics.pinball(realized, upper, 0.95))
                                series.append(metrics.pinball_per_snapshot(
                                    realized, upper, 0.95))
                            pooled[(name, mode_name)] = (
                                float(np.mean(covs)), float(np.mean(pins)),
                                np.concatenate(series))
                            rows.append([label, n, t, sl_name, name, mode_name,
                                         f"{np.mean(covs):.4f}",
                                         f"{np.mean(pins):.2f}"])

                    if sl_name != "post":
                        continue
                    # Gate: pooled per-lease-oracle coverage on the post slice.
                    ocov = pooled[("oracle", "native")][0]
                    if n == VERDICT_N and not (0.93 <= ocov <= 0.98):
                        gate_failures.append(
                            f"{label}/N={n}/t={t}: oracle post-slice cov {ocov:.3f}")

                    def best(names):
                        cands = [(k, v) for k, v in pooled.items()
                                 if k[0] in names and v[0] >= COVERAGE_VOID]
                        return min(cands, key=lambda kv: kv[1][1]) if cands else None

                    bt = best(TRIVIAL)
                    bf = best(FITTED)
                    if n == VERDICT_N:
                        rng2 = np.random.default_rng(2026)
                        if bf is None or bt is None:
                            # A valid fitted mode with no valid trivial reference is
                            # outside the pre-registered rule; flag for adjudication.
                            verdict = ("FAIL (no valid fitted mode)" if bf is None
                                       else "NO-VALID-TRIVIAL (adjudicate)")
                            maxcov = max(v[0] for k, v in pooled.items()
                                         if k[0] in FITTED)
                            table[(label, t)] = (f"max cov {maxcov:.3f}", "-", verdict)
                        else:
                            _, lo_ci, hi_ci = bootstrap.ci_paired_diff(
                                bf[1][2], bt[1][2], rng2)
                            skill = 1.0 - bf[1][1] / bt[1][1]
                            ok = hi_ci < 0 and skill >= SKILL_THRESHOLD
                            table[(label, t)] = (
                                f"{bf[1][0]:.3f} ({bf[0][0]}-{bf[0][1]})",
                                f"{skill:+.1%}{'*' if hi_ci < 0 else ''} "
                                f"vs {bt[0][0]}-{bt[0][1]}",
                                "PASS" if ok else "FAIL")

    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "drift.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "N", "t", "slice", "model", "mode", "cov95",
                    "pinball95"])
        w.writerows(rows)

    if gate_failures:
        print("\n!! GATE FAILURES (KD verdict void until resolved):")
        for x in gate_failures:
            print(f"   {x}")
    print(f"\nKD table (N={VERDICT_N}, post-drift slice):")
    print(f"{'scenario':<10}{'t':>4}  {'best fitted cov95 (mode)':<34}"
          f"{'skill vs trivial':<28}{'verdict'}")
    for (label, kind) in SCENARIOS:
        for t in HORIZONS:
            cov, skill, verdict = table[(label, t)]
            print(f"{label:<10}{int(t):>4}  {cov:<34}{skill:<28}{verdict}")
    if gate_failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
