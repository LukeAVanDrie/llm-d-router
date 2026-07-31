"""Question A (RESULTS-2.md): attribution of the R2/N=1000 collapse.

Factorial: warm-up rule {legacy, steady} x conformal window W (params
conformal.window_sweep_s), R2/N=1000, 3 seeds, stationary. One simulation per
(warm-up, seed) with a calibration window equal to the largest W; each smaller W is
scored from the trailing W seconds of calibration residuals. Native modes are reported
alongside for completeness; the pre-registered readings are on the best fitted mode's
seed-pooled q95 coverage (RESULTS-2.md, question A).

Run: .venv/bin/python -m eval.run_window_factorial
Outputs: results/factorial.csv, printed table for the RESULTS-2.md A table.
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
    OracleSurvival)
from estimators.mixture_em import B4LognormalMixtureEM  # noqa: E402
from eval import metrics                               # noqa: E402
from eval.forecast_common import collect               # noqa: E402
from sim import simulator, workloads                   # noqa: E402

REGIME, N = "R2", 1000
SEEDS = (0, 1, 2)
HORIZONS = (5.0, 10.0)
COVERAGE_VOID = 0.90
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def main():
    params = workloads.load_params()
    sweep = [float(w) for w in params["conformal"]["window_sweep_s"]]
    w_max = max(sweep)
    out_dist, cap = workloads.build_regime(REGIME, params)

    pilot = simulator.run_cell(REGIME, N, 999, params, capacity=None)
    print(f"[pilot] {REGIME} N={N}: capacity={pilot.capacity:,.0f} tokens")

    rows = []
    table = {}  # (warmup, W) -> cell text for the t=5s table
    for warmup in ("legacy", "steady"):
        per_seed = []
        for seed in SEEDS:
            res = simulator.run_cell(REGIME, N, seed, params, capacity=pilot.capacity,
                                     warmup_rule=warmup, calib_window_s=w_max)
            rng = np.random.default_rng(np.random.SeedSequence([987, N, seed]))
            fitted = [
                B1ConstantHazard().fit(res.training_lengths),
                B2Lognormal().fit(res.training_lengths),
                B2cCensoredLognormal(cap).fit(res.training_lengths),
                B3DiscreteHazardKM(cap=cap).fit(res.training_lengths),
                B4LognormalMixtureEM(cap).fit(res.training_lengths),
                OracleSurvival(out_dist),
            ]
            trivials = [B0Persistence(), B0Growth(cap if cap is not None else np.inf)]
            ct, calib = collect(res.calib, trivials, fitted, res.rate_tokens_per_s,
                                HORIZONS, rng, skip=("oracle",))
            et, evald = collect(res.eval, trivials, fitted, res.rate_tokens_per_s,
                                HORIZONS, rng)
            per_seed.append({"res": res, "ct": ct, "calib": calib, "et": et,
                             "eval": evald,
                             "train_mean": float(res.training_lengths.mean())})
            print(f"[sim] {warmup} seed={seed}: train_n={len(res.training_lengths)} "
                  f"train_mean={res.training_lengths.mean():.0f} "
                  f"little_dev={simulator.little_gate(res)[1]:.3f}")

        fitted_names = ("B1", "B2", "B2c", "B3", "B4")
        for t in HORIZONS:
            # Native modes (window-independent).
            for name in fitted_names + ("oracle",):
                covs, pins, widths = [], [], []
                for s in per_seed:
                    d = s["eval"][(name, t)]
                    covs.append(metrics.coverage(d["realized"], d["q0.95"]))
                    pins.append(metrics.pinball(d["realized"], d["q0.95"], 0.95))
                    widths.append(metrics.mean_width(d["q0.95"], d["q0.5"]))
                rows.append([warmup, "native", name, t, "",
                             np.mean(covs), np.mean(widths), np.mean(pins)])
            # Conformal modes per window: trailing W seconds of calibration residuals.
            for W in sweep:
                best = None
                for name in fitted_names + ("B0", "B0g"):
                    covs, pins, widths = [], [], []
                    for s in per_seed:
                        eval_start = s["res"].eval_start_s
                        dc, de = s["calib"][(name, t)], s["eval"][(name, t)]
                        sel = s["ct"] > eval_start - W
                        resid = dc["realized"][sel] - dc["point"][sel]
                        q95 = float(np.quantile(resid, 0.95, method="higher"))
                        q50 = float(np.quantile(resid, 0.5, method="higher"))
                        upper = de["point"] + q95
                        covs.append(metrics.coverage(de["realized"], upper))
                        pins.append(metrics.pinball(de["realized"], upper, 0.95))
                        widths.append(q95 - q50)
                    cov, pin, wid = float(np.mean(covs)), float(np.mean(pins)), \
                        float(np.mean(widths))
                    rows.append([warmup, f"conformal{int(W)}", name, t, int(W),
                                 cov, wid, pin])
                    if name in fitted_names:
                        cand = (name, cov, wid, pin)
                        if cov >= COVERAGE_VOID and (best is None or pin < best[3]):
                            best = cand
                # Best fitted mode for this (warmup, W): conformal candidates above plus
                # native modes (valid natives can win on pinball).
                for name in fitted_names:
                    covs = [metrics.coverage(s["eval"][(name, t)]["realized"],
                                             s["eval"][(name, t)]["q0.95"])
                            for s in per_seed]
                    pins = [metrics.pinball(s["eval"][(name, t)]["realized"],
                                            s["eval"][(name, t)]["q0.95"], 0.95)
                            for s in per_seed]
                    wids = [metrics.mean_width(s["eval"][(name, t)]["q0.95"],
                                               s["eval"][(name, t)]["q0.5"])
                            for s in per_seed]
                    cov, pin = float(np.mean(covs)), float(np.mean(pins))
                    if cov >= COVERAGE_VOID and (best is None or pin < best[3]):
                        best = (f"{name}-native", cov, float(np.mean(wids)), pin)
                if t == 5.0:
                    if best is None:
                        max_cov = max(
                            r[5] for r in rows
                            if r[0] == warmup and r[3] == t and r[2] in fitted_names
                            and (r[1] == "native" or r[4] == int(W)))
                        table[(warmup, W)] = f"VOID (max cov {max_cov:.3f})"
                    else:
                        table[(warmup, W)] = (f"{best[1]:.3f}/{best[2]:.0f} "
                                              f"({best[0]})")

    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "factorial.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["warmup", "mode", "model", "t", "window_s", "cov95", "width95",
                    "pinball95"])
        w.writerows(rows)

    print(f"\nA table (best fitted mode, pooled cov95/width95 tokens, t=5s), "
          f"{REGIME}/N={N}:")
    hdr = "".join(f"{'W=' + str(int(W)):>28}" for W in sweep)
    print(f"{'warmup':<8}{hdr}")
    for warmup in ("legacy", "steady"):
        cells = "".join(f"{table[(warmup, W)]:>28}" for W in sweep)
        print(f"{warmup:<8}{cells}")


if __name__ == "__main__":
    main()
