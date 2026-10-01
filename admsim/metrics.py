"""Per-run metrics and paired comparisons between policies.

All rates are per sim time unit, over the span from the warm-up completion to the end of the run.
Requests count when they arrived after warm-up and completed. A request is good when its TTFT is at
most ttft_slo and its TPOT at most tpot_slo, the variant's SLO thresholds (calibration.py).
"""

import collections
import math
import statistics

import numpy as np


def tpot(r):
    return (r.finished - r.first_token) / (r.output - 1) if r.output > 1 else 0.0


def summarize(run, slo):
    """The metrics of a Run. slo: a calibration entry, with keys ttft_slo and tpot_slo."""
    t0 = run.t0
    span = max(run.T - t0, 1e-9)
    rs = [r for r in run.requests if r.arrived >= t0 and r.finished is not None and r.first_token is not None]
    good = sum(1 for r in rs if r.first_token - r.arrived <= slo["ttft_slo"] and tpot(r) <= slo["tpot_slo"])
    ttft = np.asarray([r.first_token - r.arrived for r in rs]) if rs else np.zeros(1)
    counts = collections.Counter()
    for e in run.endpoints:
        counts.update(e.counters())
    return {
        "throughput_tok": (run.tokens - run.tok_warm) / span,
        "goodput": good / span,
        "attainment": good / max(len(rs), 1),
        "completions_rate": sum(1 for r in run.completed if r.finished >= t0) / span,
        "ttft_p90": float(np.quantile(ttft, 0.9)),
        "preempts_per_completion": counts["preempts"] / max(len(run.completed), 1),  # warm-up included
        "completed": len(rs),
        **counts,
    }


def paired_log_ratios(rows, baseline):
    """For each (load, cell, kf, E, engine) and policy: the mean and 1.96 standard errors of
    100 ln(metric / baseline's metric) over seeds present in both. The metric is goodput below
    capacity (load < 1) and token throughput at or above it.

    Returns {(load, cell, kf, E, engine): {policy: (mean, ci, n)}}."""
    by = collections.defaultdict(lambda: collections.defaultdict(dict))
    for r in rows:
        by[(r["load"], r["cell"], r["kf"], r["E"], r["engine"])][r["policy"]][r["seed"]] = r
    out = {}
    for key, d in sorted(by.items()):
        if baseline not in d:
            continue
        met = "goodput" if key[0] < 1 else "throughput_tok"
        base = d[baseline]
        res = {}
        for pol, seeds in d.items():
            if pol == baseline:
                continue
            v = [
                100 * math.log(seeds[s][met] / base[s][met])
                for s in base
                if s in seeds and seeds[s][met] > 0 and base[s][met] > 0
            ]
            if v:
                ci = 1.96 * statistics.stdev(v) / math.sqrt(len(v)) if len(v) > 1 else float("nan")
                res[pol] = (statistics.mean(v), ci, len(v))
        out[key] = res
    return out
