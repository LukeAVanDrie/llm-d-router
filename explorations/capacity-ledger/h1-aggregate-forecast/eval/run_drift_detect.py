"""Question DD (RESULTS-3.md): which online signal detects calibration loss fastest?

Monitored configuration: B2 frozen on warm-up training, static conformal q95 from the
calibration window, t = 5 s (t = 10 s recorded in the CSV as context). Three detectors,
each a statistic over a trailing W_d = 150 s window at every eval snapshot (>= 20 window
samples required, else no alarm):

- D-cov:    miss rate of the monitored q95 bound (residuals realizing in the window)
            minus the nominal 0.05.
- D-qshift: (rolling q95 of realized residuals - static calibration q95) / IQR of
            calibration residuals.
- D-mix:    two-sample KS statistic between completion lengths completing in the window
            and the frozen warm-up training set.

Thresholds: per detector, the max of its statistic pooled over six stationary null runs
(w = 1.0, rng label DRIFTNULL, seeds 0-5) — a matched zero-observed-false-alarm budget.
Scoring: detection latency = first alarming eval snapshot minus onset (DS: shift at
300 s; DR: ramp start at 0 s); no alarm = MISS. Ranking: median DS latency, ties by
median DR latency. Context: alarm counts at the pooled q0.99 threshold, W_d in {60, 300}.

Run: .venv/bin/python -m eval.run_drift_detect
Outputs: results/drift_detect.csv, printed DD table for RESULTS-3.md.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np
from scipy import stats as sstats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from estimators.baselines import B2Lognormal            # noqa: E402
from estimators import base as estbase                  # noqa: E402
from sim import simulator, workloads                    # noqa: E402

N = 1000
DRIFT_SEEDS = (0, 1, 2)
HORIZONS = (5.0, 10.0)
MONITOR_T = 5.0
DETECTORS = ("D-cov", "D-qshift", "D-mix")
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

ONSET_S = {"DS": 300.0, "DR": 0.0}


def monitored_series(res, rng):
    """Per-snapshot (times, point, realized) for frozen B2 over calib and eval, plus the
    static q95 offset and residual IQR from the calibration window, per horizon."""
    model = B2Lognormal().fit(res.training_lengths)
    out = {}
    for t in HORIZONS:
        rows = {"calib": ([], []), "eval": ([], [])}
        for part, snaps in (("calib", res.calib), ("eval", res.eval)):
            pts, real = rows[part]
            for snap in snaps:
                agg = estbase.aggregate(model, snap, res.rate_tokens_per_s, t, rng=rng)
                pts.append(agg["mean"])
                real.append(snap.realized_t1[t])
        c_pts, c_real = map(np.asarray, rows["calib"])
        e_pts, e_real = map(np.asarray, rows["eval"])
        c_resid = c_real - c_pts
        out[t] = {
            "calib_times": np.array([s.time_s for s in res.calib]),
            "eval_times": np.array([s.time_s for s in res.eval]),
            "calib_resid": c_resid,
            "eval_resid": e_real - e_pts,
            "q95_static": float(np.quantile(c_resid, 0.95, method="higher")),
            "iqr": float(np.subtract(*np.percentile(c_resid, [75, 25]))) or 1.0,
        }
    return out


def detector_stats(mon, res, w_d: float, min_samples: int, t: float):
    """Statistic arrays over eval snapshots for the three detectors, honest: residuals
    enter at realization time (snapshot + t), completions at completion time."""
    m = mon[t]
    times = np.concatenate([m["calib_times"], m["eval_times"]])
    resid = np.concatenate([m["calib_resid"], m["eval_resid"]])
    ready = times + t
    order = np.argsort(ready)
    ready, resid_r = ready[order], resid[order]
    misses_r = (resid_r > m["q95_static"]).astype(np.float64)

    ct, cl = res.completion_times_s, res.completion_lengths
    train = np.sort(res.training_lengths.astype(np.float64))

    eval_times = m["eval_times"]
    out = {d: np.full(len(eval_times), np.nan) for d in DETECTORS}
    for i, s in enumerate(eval_times):
        j_hi = np.searchsorted(ready, s, side="right")
        j_lo = np.searchsorted(ready, s - w_d, side="right")
        if j_hi - j_lo >= min_samples:
            out["D-cov"][i] = misses_r[j_lo:j_hi].mean() - 0.05
            q_roll = np.quantile(resid_r[j_lo:j_hi], 0.95, method="higher")
            out["D-qshift"][i] = (q_roll - m["q95_static"]) / m["iqr"]
        k_hi = np.searchsorted(ct, s, side="right")
        k_lo = np.searchsorted(ct, s - w_d, side="right")
        if k_hi - k_lo >= min_samples:
            out["D-mix"][i] = sstats.ks_2samp(cl[k_lo:k_hi], train).statistic
    return out


def first_alarm(stat: np.ndarray, rel_times: np.ndarray, theta: float,
                lo: float, hi: float):
    """First time in (lo, hi] with stat > theta, or None."""
    sel = (rel_times > lo) & (rel_times <= hi) & ~np.isnan(stat)
    idx = np.nonzero(sel & (stat > theta))[0]
    return float(rel_times[idx[0]]) if len(idx) else None


def main():
    params = workloads.load_params()
    d = params["drift"]
    det = d["detection"]
    w_d = float(det["window_s"])
    min_samples = int(det["min_window_samples"])
    comp_a, _ = workloads.build_regime(d["components"][0], params)
    comp_b, _ = workloads.build_regime(d["components"][1], params)
    post_mix = workloads.TwoComponentMix(comp_a, comp_b, d["w_end"])

    pilot = simulator.run_cell("DRIFT", N, 999, params, capacity=None,
                               out_dist_override=post_mix)
    print(f"[pilot] post-mix N={N}: capacity={pilot.capacity:,.0f} tokens")

    def run(label, schedule, seed):
        res = simulator.run_cell(
            label, N, seed, params, capacity=pilot.capacity, warmup_rule="steady",
            calib_window_s=d["calibration_window_s"], drift=schedule,
            eval_window_s=d["eval_window_s"])
        rng = np.random.default_rng(np.random.SeedSequence([555, N, seed]))
        mon = monitored_series(res, rng)
        _, dev = simulator.little_gate(res)
        print(f"[sim] {label} seed={seed}: little_dev={dev:.3f}")
        return res, mon

    rows = []
    windows = [w_d] + [float(w) for w in det["window_sweep_s"]]

    # Stationary null runs: pooled statistics set the thresholds.
    null_sched = workloads.DriftSchedule(a=comp_a, b=comp_b, w_start=d["w_start"],
                                         w_end=d["w_start"], kind="shift")
    null_stats = {W: {det_name: [] for det_name in DETECTORS} for W in windows}
    for seed in det["stationary_seeds"]:
        res, mon = run("DRIFTNULL", null_sched, seed)
        for W in windows:
            stats_w = detector_stats(mon, res, W, min_samples, MONITOR_T)
            for det_name in DETECTORS:
                null_stats[W][det_name].append(stats_w[det_name])
            rel = mon[MONITOR_T]["eval_times"] - res.eval_start_s
            for det_name in DETECTORS:
                for r_t, v in zip(rel, stats_w[det_name]):
                    rows.append(["NULL", seed, W, det_name, f"{r_t:.0f}",
                                 f"{v:.4f}" if not np.isnan(v) else ""])

    theta = {}
    theta_q99 = {}
    for W in windows:
        for det_name in DETECTORS:
            pooled = np.concatenate(null_stats[W][det_name])
            pooled = pooled[~np.isnan(pooled)]
            theta[(W, det_name)] = float(pooled.max())
            theta_q99[(W, det_name)] = float(np.quantile(pooled, 0.99))
    for det_name in DETECTORS:
        print(f"[theta] {det_name}: max={theta[(w_d, det_name)]:.4f} "
              f"q99={theta_q99[(w_d, det_name)]:.4f} (W_d={w_d:.0f})")

    # Drift runs: latency scoring.
    latencies = {(W, det_name, lab): [] for W in windows for det_name in DETECTORS
                 for lab in ("DS", "DR")}
    false_alarms = {det_name: 0 for det_name in DETECTORS}
    for label, kind in (("DS", "shift"), ("DR", "ramp")):
        schedule = workloads.build_drift_schedule(kind, params)
        for seed in DRIFT_SEEDS:
            res, mon = run("DRIFT", schedule, seed)
            rel = mon[MONITOR_T]["eval_times"] - res.eval_start_s
            for W in windows:
                stats_w = detector_stats(mon, res, W, min_samples, MONITOR_T)
                for det_name in DETECTORS:
                    onset = ONSET_S[label]
                    alarm = first_alarm(stats_w[det_name], rel,
                                        theta[(W, det_name)], onset,
                                        d["eval_window_s"])
                    lat = None if alarm is None else alarm - onset
                    latencies[(W, det_name, label)].append(lat)
                    if W == w_d and label == "DS":
                        fa = first_alarm(stats_w[det_name], rel,
                                         theta[(W, det_name)], 0.0, onset)
                        if fa is not None:
                            false_alarms[det_name] += 1
                    rows.append([label, seed, W, det_name, "latency",
                                 f"{lat:.0f}" if lat is not None else "MISS"])

    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_DIR / "drift_detect.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run", "seed", "window_s", "detector", "key", "value"])
        w.writerows(rows)

    def med(vals):
        """Median with MISS ranked below any finite latency (as +inf); a majority of
        misses makes the median itself MISS."""
        m = float(np.median([np.inf if v is None else v for v in vals]))
        return None if not np.isfinite(m) else m

    def fmt_lat(vals):
        return ", ".join(f"{v:.0f}" if v is not None else "MISS" for v in vals)

    print(f"\nDD table (t={MONITOR_T:.0f} s, W_d={w_d:.0f} s, theta = stationary max):")
    print(f"{'detector':<10}{'theta':>9}  {'DS latencies':<22}{'DS med':>8}  "
          f"{'DR latencies':<22}{'DR med':>8}  {'FA(DS pre)':>10}")
    ranking = []
    for det_name in DETECTORS:
        ds = latencies[(w_d, det_name, "DS")]
        dr = latencies[(w_d, det_name, "DR")]
        ds_med, dr_med = med(ds), med(dr)
        ranking.append((float("inf") if ds_med is None else ds_med,
                        float("inf") if dr_med is None else dr_med, det_name))
        print(f"{det_name:<10}{theta[(w_d, det_name)]:>9.4f}  {fmt_lat(ds):<22}"
              f"{ds_med if ds_med is not None else 'MISS':>8}  {fmt_lat(dr):<22}"
              f"{dr_med if dr_med is not None else 'MISS':>8}  "
              f"{false_alarms[det_name]:>10}")
    ranking.sort()
    winner = ranking[0][2]
    floor = float(det["fast_trigger_floor_s"])
    fast = ranking[0][0] <= floor
    print(f"\nRanking: {' > '.join(r[2] for r in ranking)}")
    print(f"Winner: {winner}; median DS latency {ranking[0][0]:.0f} s; "
          f"fast-trigger floor {floor:.0f} s: {'MET' if fast else 'NOT MET'}")
    print("\nContext (window sweep, median DS latency at stationary-max theta):")
    for W in windows:
        parts = []
        for det_name in DETECTORS:
            m = med(latencies[(W, det_name, "DS")])
            parts.append(f"{det_name}={'MISS' if m is None else f'{m:.0f}'}")
        print(f"  W={W:.0f}s: {', '.join(parts)}")


if __name__ == "__main__":
    main()
