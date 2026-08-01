"""Question KR (RESULTS-3.md): does the continuous-refit loop recover under drift?

Same DS/DR scenarios and rng streams as run_drift, with the fitted side under verdict
replaced by 12 refit arms (B1/B2 x cadence {30, 150, 600} s x trailing {500, 2000}
completions); frozen B1/B2 are context tying to KD. Three stationary reference runs at
the post-drift mix (w = 0.25, rng label DRIFTREF) supply the recovery yardstick:
reference skill = best valid frozen fitted vs best valid trivial on the full eval window.

Verdicts (pre-registered): per verdict slice x horizon at N = 1000, the refit loop
recovers iff some refit arm-mode with seed-pooled slice coverage >= 0.90 reaches skill
vs the slice's best valid trivial >= reference skill - 5 points. Secondary KD-continuity
reading: the KD 10% CI-gated rule with refit arms on the fitted side. Gates as KD.

Run: .venv/bin/python -m eval.run_drift_refit
Outputs: results/drift_refit.csv, printed KR tables for RESULTS-3.md.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from estimators.baselines import (                      # noqa: E402
    B0Growth, B0Persistence, B1ConstantHazard, B2Lognormal, OraclePerLease)
from estimators.conformal import rolling_quantile       # noqa: E402
from eval import bootstrap, metrics                     # noqa: E402
from eval.forecast_common import collect                # noqa: E402
from eval.refit import collect_refit, refit_models      # noqa: E402
from sim import simulator, workloads                    # noqa: E402

N = 1000
SEEDS = (0, 1, 2)
HORIZONS = (5.0, 10.0)
COVERAGE_VOID = 0.90
KD_SKILL_THRESHOLD = 0.10
FROZEN = ("B1", "B2")
TRIVIAL = ("B0", "B0g")
FACTORIES = {"B1": B1ConstantHazard, "B2": B2Lognormal}
NO_REFIT = {"estimators": (), "cadences_s": (), "trailing_completions": (),
            "min_fit_completions": 0}
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

# (slice name, lo, hi, role) relative to eval start. Roles: "verdict" feeds the
# recovery table; "kd" feeds the KD-continuity table; "context" is CSV-only except
# where noted in RESULTS-3.md (DS post is the KD slice and a context row).
SLICES = {
    "DS": [("recovery", 600.0, 900.0, "verdict"),
           ("post", 300.0, 900.0, "kd+context"),
           ("early", 300.0, 600.0, "context")],
    "DR": [("post", 600.0, 900.0, "verdict+kd"),
           ("ramp", 0.0, 600.0, "context")],
}


def pooled_eval(per_seed, name, t, lo, hi, mode):
    """Seed-pooled (coverage, mean pinball, per-snapshot series) for one model-mode on
    one slice. mode: 'native', 'static', or a rolling window length in seconds."""
    covs, pins, series = [], [], []
    for s in per_seed:
        de = s["eval"][(name, t)]
        rel = s["et"] - s["res"].eval_start_s
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
            all_resid = np.concatenate([dc["realized"] - dc["point"],
                                        de["realized"] - de["point"]])
            q = rolling_quantile(all_times, t, all_resid, s["et"][sel], mode, 0.95)
            upper = de["point"][sel] + q
        covs.append(metrics.coverage(realized, upper))
        pins.append(metrics.pinball(realized, upper, 0.95))
        series.append(metrics.pinball_per_snapshot(realized, upper, 0.95))
    return float(np.mean(covs)), float(np.mean(pins)), np.concatenate(series)


def all_modes(name, fitted_names, rolling_ws):
    modes = {"static": "static"}
    for W in rolling_ws:
        modes[f"roll{int(W)}"] = W
    if name in fitted_names:
        modes["native"] = "native"
    return modes


def best_valid(pooled, names):
    cands = [(k, v) for k, v in pooled.items()
             if k[0] in names and v[0] >= COVERAGE_VOID]
    return min(cands, key=lambda kv: kv[1][1]) if cands else None


def fmt_ref(ref):
    return f"{ref:+.1%}" if ref is not None else "n/a"


def run_one(label, schedule, seed, params, capacity, d, refit_cfg, comp_a, comp_b):
    res = simulator.run_cell(
        label, N, seed, params, capacity=capacity, warmup_rule="steady",
        calib_window_s=d["calibration_window_s"], drift=schedule,
        eval_window_s=d["eval_window_s"])
    rng = np.random.default_rng(np.random.SeedSequence([987, N, seed]))
    fitted = [B1ConstantHazard().fit(res.training_lengths),
              B2Lognormal().fit(res.training_lengths),
              OraclePerLease(comp_a, comp_b)]
    trivials = [B0Persistence(), B0Growth(np.inf)]
    ct, calib = collect(res.calib, trivials, fitted, res.rate_tokens_per_s,
                        HORIZONS, rng, skip=("oracle",))
    et, evald = collect(res.eval, trivials, fitted, res.rate_tokens_per_s,
                        HORIZONS, rng)

    snaps_all = list(res.calib) + list(res.eval)
    arms = {}
    for est in refit_cfg["estimators"]:
        for cad in refit_cfg["cadences_s"]:
            for trail in refit_cfg["trailing_completions"]:
                arms[f"{est}-c{int(cad)}-n{trail}"] = refit_models(
                    snaps_all, FACTORIES[est], res.completion_times_s,
                    res.completion_lengths, float(cad), int(trail),
                    int(refit_cfg["min_fit_completions"]))
    n_calib = len(res.calib)
    calib.update(collect_refit(res.calib, {k: m[:n_calib] for k, m in arms.items()},
                               res.rate_tokens_per_s, HORIZONS, rng))
    evald.update(collect_refit(res.eval, {k: m[n_calib:] for k, m in arms.items()},
                               res.rate_tokens_per_s, HORIZONS, rng))
    little_ok, little_dev = simulator.little_gate(res)
    print(f"[sim] {label} seed={seed}: little_dev={little_dev:.3f} "
          f"train={len(res.training_lengths)} completions={len(res.completion_lengths)}")
    return ({"res": res, "ct": ct, "calib": calib, "et": et, "eval": evald},
            list(arms.keys()), little_ok, little_dev)


def main():
    params = workloads.load_params()
    d = params["drift"]
    rf = d["refit"]
    rolling = [float(w) for w in d["rolling_windows_s"]]
    comp_a, _ = workloads.build_regime(d["components"][0], params)
    comp_b, _ = workloads.build_regime(d["components"][1], params)
    post_mix = workloads.TwoComponentMix(comp_a, comp_b, d["w_end"])

    pilot = simulator.run_cell("DRIFT", N, 999, params, capacity=None,
                               out_dist_override=post_mix)
    print(f"[pilot] post-mix N={N}: capacity={pilot.capacity:,.0f} tokens")

    rows, gate_failures = [], []

    # Reference runs: stationary at the post-drift mix; frozen ladder only.
    ref_sched = workloads.DriftSchedule(a=comp_a, b=comp_b, w_start=d["w_end"],
                                        w_end=d["w_end"], kind="shift")
    ref_seeds = []
    for seed in SEEDS:
        s, _, ok, dev = run_one("DRIFTREF", ref_sched, seed, params, pilot.capacity,
                                d, NO_REFIT, comp_a, comp_b)
        if not ok:
            gate_failures.append(f"DRIFTREF/s{seed}: Little {dev:.2%}")
        ref_seeds.append(s)

    ref_skill = {}
    for t in HORIZONS:
        pooled = {}
        for name in FROZEN + TRIVIAL + ("oracle",):
            modes = ({"native": "native"} if name == "oracle"
                     else all_modes(name, FROZEN, rolling))
            for mode_name, mode in modes.items():
                pooled[(name, mode_name)] = pooled_eval(
                    ref_seeds, name, t, 0.0, float("inf"), mode)
                c, p, _ = pooled[(name, mode_name)]
                rows.append(["REF", N, t, "eval", name, mode_name,
                             f"{c:.4f}", f"{p:.2f}"])
        ocov = pooled[("oracle", "native")][0]
        if not (0.93 <= ocov <= 0.98):
            gate_failures.append(f"DRIFTREF/t={t}: oracle eval cov {ocov:.3f}")
        bt, bf = best_valid(pooled, TRIVIAL), best_valid(pooled, FROZEN)
        if bf is None or bt is None:
            gate_failures.append(f"DRIFTREF/t={t}: no valid arm (adjudicate)")
            ref_skill[t] = None
        else:
            rng2 = np.random.default_rng(2026)
            _, lo_ci, hi_ci = bootstrap.ci_paired_diff(bf[1][2], bt[1][2], rng2)
            ref_skill[t] = 1.0 - bf[1][1] / bt[1][1]
            print(f"[ref] t={t}: skill {ref_skill[t]:+.1%} "
                  f"({bf[0][0]}-{bf[0][1]} vs {bt[0][0]}-{bt[0][1]}, "
                  f"CI [{lo_ci:+.0f}, {hi_ci:+.0f}])")

    # Drift runs.
    recovery_table, kd_table = {}, {}
    for label, kind in (("DS", "shift"), ("DR", "ramp")):
        schedule = workloads.build_drift_schedule(kind, params)
        per_seed, refit_names = [], None
        for seed in SEEDS:
            s, names, ok, dev = run_one("DRIFT", schedule, seed, params,
                                        pilot.capacity, d, rf, comp_a, comp_b)
            if not ok:
                gate_failures.append(f"{label}/s{seed}: Little {dev:.2%}")
            per_seed.append(s)
            refit_names = names
        fitted_all = tuple(FROZEN) + tuple(refit_names)

        for t in HORIZONS:
            for sl_name, lo, hi, role in SLICES[label]:
                pooled = {}
                for name in fitted_all + TRIVIAL + ("oracle",):
                    modes = ({"native": "native"} if name == "oracle"
                             else all_modes(name, fitted_all, rolling))
                    for mode_name, mode in modes.items():
                        pooled[(name, mode_name)] = pooled_eval(
                            per_seed, name, t, lo, hi, mode)
                        c, p, _ = pooled[(name, mode_name)]
                        rows.append([label, N, t, sl_name, name, mode_name,
                                     f"{c:.4f}", f"{p:.2f}"])
                ocov = pooled[("oracle", "native")][0]
                if "verdict" in role or "kd" in role:
                    if not (0.93 <= ocov <= 0.98):
                        gate_failures.append(
                            f"{label}/{sl_name}/t={t}: oracle cov {ocov:.3f}")

                bt = best_valid(pooled, TRIVIAL)
                br = best_valid(pooled, refit_names)
                rng2 = np.random.default_rng(2026)
                if "verdict" in role or "context" in role:
                    tag = ("(context)" if "verdict" not in role else None)
                    if bt is None:
                        recovery_table[(label, sl_name, t)] = (
                            "-", "-", tag or "NO-VALID-TRIVIAL (adjudicate)")
                    elif br is None:
                        maxcov = max(v[0] for k, v in pooled.items()
                                     if k[0] in refit_names)
                        recovery_table[(label, sl_name, t)] = (
                            f"VOID (max cov {maxcov:.3f})", "-", tag or "NO")
                    else:
                        _, lo_ci, hi_ci = bootstrap.ci_paired_diff(
                            br[1][2], bt[1][2], rng2)
                        skill = 1.0 - br[1][1] / bt[1][1]
                        ref = ref_skill[t]
                        rec = (ref is not None and
                               skill >= ref - rf["recovery_margin_points"] / 100.0)
                        recovery_table[(label, sl_name, t)] = (
                            f"{br[0][0]}-{br[0][1]} cov {br[1][0]:.3f}",
                            f"{skill:+.1%} (ref {fmt_ref(ref)}) "
                            f"CI [{lo_ci:+.0f}, {hi_ci:+.0f}]",
                            tag or ("YES" if rec else "NO"))

                if "kd" in role:
                    bf = best_valid(pooled, fitted_all)
                    if bf is None or bt is None:
                        kd_table[(label, t)] = ("VOID", "-", "FAIL")
                    else:
                        _, lo_ci, hi_ci = bootstrap.ci_paired_diff(
                            bf[1][2], bt[1][2], rng2)
                        skill = 1.0 - bf[1][1] / bt[1][1]
                        ok = hi_ci < 0 and skill >= KD_SKILL_THRESHOLD
                        kd_table[(label, t)] = (
                            f"{bf[0][0]}-{bf[0][1]} cov {bf[1][0]:.3f}",
                            f"{skill:+.1%}{'*' if hi_ci < 0 else ''} "
                            f"vs {bt[0][0]}-{bt[0][1]}",
                            "PASS" if ok else "FAIL")

    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "drift_refit.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "N", "t", "slice", "model", "mode", "cov95",
                    "pinball95"])
        w.writerows(rows)

    if gate_failures:
        print("\n!! GATE FAILURES (KR verdict void until resolved):")
        for x in gate_failures:
            print(f"   {x}")
    print(f"\nKR recovery table (N={N}):")
    print(f"{'slice':<22}{'t':>4}  {'best refit (cov)':<34}{'skill (vs ref)':<44}"
          f"{'recovery'}")
    for (label, sl, t), (c, sk, rec) in sorted(recovery_table.items()):
        print(f"{label + ' ' + sl:<22}{int(t):>4}  {c:<34}{sk:<44}{rec}")
    print("\nKD-continuity table (10% CI-gated, refit arms included):")
    for (label, t), (c, sk, v) in sorted(kd_table.items()):
        print(f"{label:<10}{int(t):>4}  {c:<34}{sk:<34}{v}")
    if gate_failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
