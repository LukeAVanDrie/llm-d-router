"""Per-variant capacity and SLO thresholds, stored in calibration.json.

A variant is a cell at a cache factor kf on E endpoints, keyed like "bimodal-K0.5-E4".

capacity: the pool's saturated completion rate (requests per time unit), the larger of the fit
check and no gate with 1.3 n requests queued at time 0. The run length n (capacity_requests)
starts at 8,000 and doubles until the completion rate and the token rate over the mean output
length agree within 3% (capacity_gap); short runs overstate capacity by completing short requests
first. The fit-check run places by KV blocks allocated and counts only unprefilled tokens in the
release condition.

ttft_slo, tpot_slo: the 90th percentiles of TTFT and TPOT under no gate at load 0.9, over seeds 101
to 103 with 625 requests per endpoint (slo_requests completed after warm-up).
"""

import json
import os

import numpy as np

from .config import variant
from .metrics import tpot
from .policies import Fit, NoGate
from .simulation import simulate

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "calibration.json")


def key(cell, kf, E):
    return f"{cell}-K{kf:g}-E{E}"


def read_table():
    with open(PATH) as f:
        return json.load(f)


def load(cell, kf, E):
    table = read_table()
    k = key(cell, kf, E)
    if k not in table:
        raise SystemExit(f"no calibration for {k}; run: python -m admsim prep {cell} {kf:g} {E}")
    return table[k]


def _rates(run):
    span = max(run.T - run.t0, 1e-9)
    return sum(1 for r in run.completed if r.finished >= run.t0) / span, (run.tokens - run.tok_warm) / span


def measure_capacity(cell, kf, E, n_req=8000, tol=0.03, n_max=32000):
    cfg, workload = variant(cell, kf)
    d = np.random.default_rng(7)
    mean_l = float(np.mean([workload.draw(d)[1] for _ in range(50000)]))
    while True:
        a = _rates(
            simulate(
                workload,
                cfg,
                "recompute",
                Fit(),
                E,
                n_req,
                11,
                saturate=True,
                placement="kv",
                pending="remaining",
            )
        )
        b = _rates(simulate(workload, cfg, "recompute", NoGate(), E, n_req, 11, saturate=True))
        comp, tok = max(a[0], b[0]), max(a[1], b[1])
        gap = abs(comp / (tok / mean_l) - 1.0)
        if gap <= tol or n_req >= n_max:
            return {"capacity": comp, "capacity_requests": n_req, "capacity_gap": gap}
        n_req *= 2


def measure_slos(cell, kf, E, capacity):
    cfg, workload = variant(cell, kf)
    rows = []
    for seed in (101, 102, 103):
        run = simulate(workload, cfg, "recompute", NoGate(), E, 625 * E, seed, rate=0.9 * capacity)
        rs = [
            r
            for r in run.requests
            if r.arrived >= run.t0 and r.finished is not None and r.first_token is not None
        ]
        rows += [(r.first_token - r.arrived, tpot(r), r.output) for r in rs]
    ttft = np.asarray([x[0] for x in rows])
    tp = np.asarray([x[1] for x in rows if x[2] > 1])
    return {
        "ttft_slo": float(np.quantile(ttft, 0.9)),
        "tpot_slo": float(np.quantile(tp, 0.9)),
        "slo_requests": len(rows),
    }


def prep(cell, kf, E, save=True):
    """Compute a variant's calibration; with save, add it to calibration.json."""
    entry = measure_capacity(cell, kf, E)
    entry.update(measure_slos(cell, kf, E, entry["capacity"]))
    if save:
        table = read_table()
        table[key(cell, kf, E)] = entry
        with open(PATH, "w") as f:
            json.dump(table, f, indent=1, sort_keys=True)
            f.write("\n")
    return entry
