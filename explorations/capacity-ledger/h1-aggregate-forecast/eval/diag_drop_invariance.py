"""Diagnostic: the A and KD cells are capacity-invariant across the pilot amendment.

Capacity enters the simulation only through the drop branch; if no arrival is dropped
under either pilot sizing, the trajectories (same rng stream) and therefore every scored
number are identical. This script counts drops for every factorial (A) and drift-verdict
(KD) cell at both sizings: the pre-amendment capacities recorded in the first-run logs
and the post-amendment capacities recomputed from the current pilot. All counts must be
zero for the RESULTS-2.md pilot-amendment reproduction claim to hold.

Run: .venv/bin/python -m eval.diag_drop_invariance
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sim import simulator, workloads  # noqa: E402

# Pre-amendment pilot capacities, from the first-run logs (recorded in the pilot
# amendment's history; not reproducible from the current code, which is the point).
PRE_FACTORIAL_C = 3_028_121.0
PRE_DRIFT_C = 2_983_044.0
SEEDS = (0, 1, 2)


def main():
    params = workloads.load_params()
    total = 0

    post_c = simulator.run_cell("R2", 1000, 999, params, capacity=None).capacity
    for cap in (PRE_FACTORIAL_C, post_c):
        for wu in ("legacy", "steady"):
            for seed in SEEDS:
                r = simulator.run_cell("R2", 1000, seed, params, capacity=cap,
                                       warmup_rule=wu, calib_window_s=2400.0)
                print(f"A cap={cap:.0f} {wu} s{seed}: dropped={r.dropped}")
                total += r.dropped

    d = params["drift"]
    a, _ = workloads.build_regime(d["components"][0], params)
    b, _ = workloads.build_regime(d["components"][1], params)
    mix = workloads.TwoComponentMix(a, b, d["w_end"])
    post_c = simulator.run_cell("DRIFT", 1000, 999, params, capacity=None,
                                out_dist_override=mix).capacity
    for cap in (PRE_DRIFT_C, post_c):
        for kind in ("shift", "ramp"):
            sched = workloads.build_drift_schedule(kind, params)
            for seed in SEEDS:
                r = simulator.run_cell("DRIFT", 1000, seed, params, capacity=cap,
                                       warmup_rule="steady",
                                       calib_window_s=d["calibration_window_s"],
                                       drift=sched, eval_window_s=d["eval_window_s"])
                print(f"KD cap={cap:.0f} {kind} s{seed}: dropped={r.dropped}")
                total += r.dropped

    print(f"total dropped across all cells and sizings: {total}")
    sys.exit(0 if total == 0 else 1)


if __name__ == "__main__":
    main()
