"""Grid driver: simulate cells, score estimators, evaluate gates and K1/K2 inputs.

Usage (from the experiment root):
  .venv/bin/python -m eval.run_grid --synthetic          # full grid
  .venv/bin/python -m eval.run_grid --synthetic --quick  # smoke: N=100, seed 0

Outputs: results/cells.csv, results/gates.csv, results/summary.txt. RESULTS.md is filled
by hand from summary.txt so the pre-registered tables are never machine-overwritten.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from estimators import base as estbase          # noqa: E402
from estimators.b3_km import B3DiscreteHazardKM  # noqa: E402
from estimators.baselines import (               # noqa: E402
    B0Growth, B0Persistence, B1ConstantHazard, B2cCensoredLognormal, B2Lognormal,
    OracleSurvival)
from estimators.conformal import ConformalWrapper  # noqa: E402
from eval import bootstrap, metrics              # noqa: E402
from sim import simulator, workloads             # noqa: E402

REGIMES = ("R1", "R2", "R3", "R4")
NS = (10, 100, 1000)
SEEDS = (0, 1, 2)
HORIZONS = simulator.HORIZONS_S
VERDICT_NS = {100, 1000}
VERDICT_TS = {5.0, 10.0}
COVERAGE_VOID = 0.90    # below this, quantile claims are void in the cell
COVERAGE_VALID = 0.93   # width comparisons require at least this
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def score_cell(regime: str, n: int, seed: int, params: dict, capacity: float) -> dict:
    """Run one cell's simulation and score every estimator mode on the eval window."""
    res = simulator.run_cell(regime, n, seed, params, capacity=capacity)
    out_dist, cap = workloads.build_regime(regime, params)
    r = res.rate_tokens_per_s
    rng = np.random.default_rng(np.random.SeedSequence([987, n, seed]))

    survival_models = [
        B1ConstantHazard().fit(res.training_lengths),
        B2Lognormal().fit(res.training_lengths),
        B2cCensoredLognormal(cap).fit(res.training_lengths),
        B3DiscreteHazardKM(cap=cap).fit(res.training_lengths),
        OracleSurvival(out_dist),
    ]
    b0, b0g = B0Persistence(), B0Growth(cap if cap is not None else np.inf)

    # Forecast every snapshot (calibration + eval) for every model and horizon.
    def forecast_window(snaps):
        data = defaultdict(lambda: defaultdict(list))  # (model,t) -> field -> per-snap list
        for snap in snaps:
            for t in HORIZONS:
                realized = snap.realized_t1[t]
                for m in (b0, b0g):
                    d = data[(m.name, t)]
                    d["point"].append(m.point(snap, r, t))
                    d["realized"].append(realized)
                grow = r * t
                alive = snap.ages + grow < snap.lengths
                for m in survival_models:
                    agg = estbase.aggregate(m, snap, r, t, rng=rng,
                                            force_mc=(m.name == "oracle"))
                    d = data[(m.name, t)]
                    d["point"].append(agg["mean"])
                    d["realized"].append(realized)
                    for q in metrics.QUANTILES:
                        d[f"q{q}"].append(agg["quantile"](q))
                    d["brier"].append(metrics.brier(agg["p"], alive))
        return {k: {f: np.asarray(v) for f, v in d.items()} for k, d in data.items()}

    calib = forecast_window(res.calib)
    evald = forecast_window(res.eval)

    conformal = ConformalWrapper()
    for key, d in calib.items():
        conformal.calibrate(key, d["point"], d["realized"], metrics.QUANTILES)

    all_names = [b0.name, b0g.name] + [m.name for m in survival_models]
    scores = {}
    for name in all_names:
        for t in HORIZONS:
            d = evald[(name, t)]
            realized = d["realized"]
            modes = []
            if f"q{metrics.HEADLINE_Q}" in d:
                modes.append("native")
            modes.append("conformal")
            for mode in modes:
                if mode == "native":
                    qpred = {q: d[f"q{q}"] for q in metrics.QUANTILES}
                else:
                    qpred = {q: d["point"] + conformal.residual_q[(name, t)][q]
                             for q in metrics.QUANTILES}
                row = {
                    "cov95": metrics.coverage(realized, qpred[0.95]),
                    "width95": metrics.mean_width(qpred[0.95], qpred[0.5]),
                    "mae": metrics.mae(realized, d["point"]),
                    "brier": float(np.mean(d["brier"])) if "brier" in d else float("nan"),
                    "pinball95_series": metrics.pinball_per_snapshot(
                        realized, qpred[0.95], 0.95),
                    "mean_headroom": float(capacity - np.mean(
                        [s.occupancy for s in res.eval])),
                }
                for q in metrics.QUANTILES:
                    row[f"pinball{q}"] = metrics.pinball(realized, qpred[q], q)
                scores[(name, mode, t)] = row

    little_ok, little_dev = simulator.little_gate(res)
    gates = {"little_dev": little_dev, "little_ok": little_ok}
    for t in HORIZONS:
        gates[f"oracle_cov95_t{t}"] = scores[("oracle", "native", t)]["cov95"]
    return {"scores": scores, "gates": gates, "capacity": capacity,
            "mean_active": res.mean_active, "n_training": len(res.training_lengths)}


def best_valid(scores_by_seed: list[dict], candidates: list[tuple[str, str]], t: float):
    """Pick the (name, mode) with lowest mean pinball@q95 among modes whose mean coverage
    across seeds is >= COVERAGE_VOID. Returns (key, mean_loss, series) or None."""
    best = None
    for name, mode in candidates:
        covs, losses, series = [], [], []
        for s in scores_by_seed:
            row = s[(name, mode, t)]
            covs.append(row["cov95"])
            losses.append(row[f"pinball{metrics.HEADLINE_Q}"])
            series.append(row["pinball95_series"])
        if np.mean(covs) < COVERAGE_VOID:
            continue
        loss = float(np.mean(losses))
        if best is None or loss < best[1]:
            best = ((name, mode), loss, np.concatenate(series), float(np.mean(covs)))
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--synthetic", action="store_true", default=True)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--regimes", default=",".join(REGIMES))
    args = ap.parse_args()

    params = workloads.load_params()
    regimes = args.regimes.split(",")
    ns = (100,) if args.quick else NS
    seeds = (0,) if args.quick else SEEDS
    RESULTS_DIR.mkdir(exist_ok=True)

    capacities: dict[tuple[str, int], float] = {}
    cell_scores: dict[tuple[str, int, int], dict] = {}
    gate_rows = []

    for regime in regimes:
        for n in ns:
            if (regime, n) not in capacities:
                pilot = simulator.run_cell(regime, n, 999, params, capacity=None)
                capacities[(regime, n)] = pilot.capacity
                print(f"[pilot] {regime} N={n}: capacity={pilot.capacity:,.0f} tokens")
            for seed in seeds:
                cell = score_cell(regime, n, seed, params, capacities[(regime, n)])
                cell_scores[(regime, n, seed)] = cell
                g = cell["gates"]
                gate_rows.append({"regime": regime, "N": n, "seed": seed, **{
                    k: (f"{v:.4f}" if isinstance(v, float) else v) for k, v in g.items()}})
                print(f"[cell] {regime} N={n} seed={seed}: little_dev={g['little_dev']:.3f} "
                      f"oracle_cov95(t=5)={g['oracle_cov95_t5.0']:.3f} "
                      f"train={cell['n_training']}")

    # ---- Gates ----
    gate_failures = []
    annex_warnings = []
    for (regime, n, seed), cell in cell_scores.items():
        g = cell["gates"]
        if not g["little_ok"]:
            msg = f"{regime}/N={n}/s{seed}: Little dev {g['little_dev']:.2%}"
            (gate_failures if n in VERDICT_NS else annex_warnings).append(msg)
    # Oracle-coverage gate on seed-pooled coverage per verdict cell (RESULTS.md amendment 5).
    # Band [0.93, 0.98]: upper bound covers discrete-atom over-coverage, which is
    # conservative by construction; under-coverage below 0.93 indicates broken math.
    for regime in regimes:
        for n in [x for x in ns if x in VERDICT_NS]:
            for t in VERDICT_TS:
                covs = [cell_scores[(regime, n, s)]["gates"][f"oracle_cov95_t{t}"]
                        for s in seeds if (regime, n, s) in cell_scores]
                cov = float(np.mean(covs))
                if not (0.93 <= cov <= 0.98):
                    gate_failures.append(
                        f"{regime}/N={n}/t={t}: pooled oracle cov {cov:.3f} outside [.93,.98]")

    with open(RESULTS_DIR / "gates.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(gate_rows[0].keys()))
        w.writeheader()
        w.writerows(gate_rows)

    # ---- Per-cell CSV ----
    with open(RESULTS_DIR / "cells.csv", "w", newline="") as f:
        cols = ["regime", "N", "seed", "t", "model", "mode", "pinball0.5", "pinball0.9",
                "pinball0.95", "pinball0.99", "cov95", "width95", "width95_pct_headroom",
                "mae", "brier"]
        w = csv.writer(f)
        w.writerow(cols)
        for (regime, n, seed), cell in cell_scores.items():
            for (name, mode, t), row in cell["scores"].items():
                w.writerow([regime, n, seed, t, name, mode,
                            *[f"{row[f'pinball{q}']:.2f}" for q in metrics.QUANTILES],
                            f"{row['cov95']:.4f}", f"{row['width95']:.1f}",
                            f"{row['width95'] / max(row['mean_headroom'], 1e-9):.4f}",
                            f"{row['mae']:.2f}", f"{row['brier']:.5f}"])

    # ---- Verdict aggregation at (regime, N, t) ----
    rng = np.random.default_rng(2026)
    stochastic = [(m, md) for m in ("B1", "B2", "B2c", "B3") for md in ("native", "conformal")]
    b12 = [(m, md) for m in ("B1", "B2", "B2c") for md in ("native", "conformal")]
    trivial = [("B0", "conformal"), ("B0g", "conformal")]

    lines = ["H1 summary (T1, pinball@q95, skill positive = better)", ""]
    if gate_failures:
        lines += ["!! GATE FAILURES (verdict void until resolved):"] + \
                 [f"   {x}" for x in gate_failures] + [""]
    if annex_warnings:
        lines += ["Annex-cell gate deviations (non-verdict, reported only):"] + \
                 [f"   {x}" for x in annex_warnings] + [""]

    k1_improvements, k2_by_t = [], defaultdict(list)
    width_reduction = {"R3": [], "R4": []}
    lines.append(f"{'cell':<22}{'B1':>8}{'B2/B2c':>8}{'B3':>8}{'oracle':>8}  (skill vs best trivial)")
    for regime in regimes:
        for n in [x for x in ns if x in VERDICT_NS]:
            by_seed = [cell_scores[(regime, n, s)]["scores"] for s in seeds]
            for t in sorted(HORIZONS):
                triv = best_valid(by_seed, trivial, t)
                if triv is None:
                    continue
                row_sk = {}
                for label, cands in (("B1", [("B1", "native"), ("B1", "conformal")]),
                                     ("B2", b12[2:]), ("B3", [("B3", "native"),
                                                              ("B3", "conformal")]),
                                     ("oracle", [("oracle", "native")])):
                    bv = best_valid(by_seed, cands, t)
                    if bv is None:
                        row_sk[label] = None
                        continue
                    _, loss, series, _ = bv
                    _, lo, hi = bootstrap.ci_paired_diff(series, triv[2], rng)
                    sig = hi < 0  # significantly better than trivial
                    row_sk[label] = (1.0 - loss / triv[1], sig)
                if t in VERDICT_TS:
                    b3 = best_valid(by_seed, [("B3", "native"), ("B3", "conformal")], t)
                    ref = best_valid(by_seed, b12, t)
                    if b3 and ref:
                        _, lo, hi = bootstrap.ci_paired_diff(b3[2], ref[2], rng)
                        imp = 1.0 - b3[1] / ref[1]
                        k1_improvements.append(imp if hi < 0 else 0.0)
                        if regime in width_reduction and b3[3] >= COVERAGE_VALID \
                                and ref[3] >= COVERAGE_VALID:
                            w_b3 = np.mean([s[(b3[0][0], b3[0][1], t)]["width95"]
                                            for s in by_seed])
                            w_ref = np.mean([s[(ref[0][0], ref[0][1], t)]["width95"]
                                             for s in by_seed])
                            width_reduction[regime].append(1.0 - w_b3 / w_ref)
                best_st = best_valid(by_seed, stochastic, t)
                if best_st:
                    _, lo, hi = bootstrap.ci_paired_diff(best_st[2], triv[2], rng)
                    imp = 1.0 - best_st[1] / triv[1]
                    k2_by_t[t].append(imp if hi < 0 else 0.0)
                cellname = f"{regime} N={n} t={int(t)}s"
                def fmt(v):
                    if v is None:
                        return "   VOID"
                    s = f"{v[0]:+.1%}"
                    return f"{s}{'*' if v[1] else ' ':>1}"
                lines.append(f"{cellname:<22}{fmt(row_sk['B1']):>8}{fmt(row_sk['B2']):>8}"
                             f"{fmt(row_sk['B3']):>8}{fmt(row_sk['oracle']):>8}")

    lines += ["", "(* = paired 90% CI excludes zero; VOID = no mode met coverage >= 0.90)", ""]
    lines.append("K1 inputs:")
    med_k1 = float(np.median(k1_improvements)) if k1_improvements else float("nan")
    lines.append(f"  median B3 improvement over best(B1,B2/B2c), verdict grid "
                 f"(CI-gated, non-significant -> 0): {med_k1:+.1%}")
    for rg in ("R3", "R4"):
        wr = width_reduction[rg]
        lines.append(f"  B3 q95 width reduction {rg} (valid-coverage cells): "
                     + (f"{float(np.mean(wr)):+.1%}" if wr else "no valid cells"))
    lines.append("")
    lines.append("K2 inputs (median improvement of best stochastic over best trivial):")
    for t in sorted(k2_by_t):
        med = float(np.median(k2_by_t[t]))
        thresh = 0.05 if t <= 2 else 0.10
        lines.append(f"  t={int(t):>2}s: {med:+.1%}  (threshold {thresh:.0%} -> "
                     f"{'PASS' if med >= thresh else 'FAIL'})")

    summary = "\n".join(lines)
    (RESULTS_DIR / "summary.txt").write_text(summary + "\n")
    print("\n" + summary)
    if gate_failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
