"""Diagnostic: length bias of the warm-up training set vs the true output distribution.

The warm-up ends when a completion count is reached. Completions observed while the pool
is still filling are the jobs short enough to have finished, so the training set is
truncated at roughly r * t_warmup_end tokens; the effect grows with N because arrival
rate (hence completion count) scales with N while fill time does not shrink. This script
measures that bias per (regime, N) under a given warm-up rule. Diagnostic only; verdict
criteria live in RESULTS-2.md.

Run: .venv/bin/python -m eval.diag_training_bias [--warmup-rule legacy|steady]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sim import simulator, workloads  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup-rule", default="legacy", choices=("legacy", "steady"))
    ap.add_argument("--regimes", default="R1,R2,R3,R4")
    ap.add_argument("--ns", default="100,1000")
    args = ap.parse_args()

    params = workloads.load_params()
    rng = np.random.default_rng(1)
    print(f"warm-up rule: {args.warmup_rule}")
    print(f"{'cell':<12}{'train_n':>8}{'mean':>8}{'true':>8}{'q95':>8}{'true_q95':>9}{'max':>8}")
    for regime in args.regimes.split(","):
        dist, _ = workloads.build_regime(regime, params)
        ref = dist.sample(rng, 200_000)
        true_mean, true_q95 = float(ref.mean()), float(np.quantile(ref, 0.95))
        for n in (int(x) for x in args.ns.split(",")):
            pilot = simulator.run_cell(regime, n, 999, params, capacity=None)
            res = simulator.run_cell(regime, n, 0, params, capacity=pilot.capacity,
                                     warmup_rule=args.warmup_rule)
            tl = res.training_lengths
            print(f"{regime} N={n:<6}{len(tl):>8}{tl.mean():>8.0f}{true_mean:>8.0f}"
                  f"{np.quantile(tl, 0.95):>8.0f}{true_q95:>9.0f}{tl.max():>8.0f}")


if __name__ == "__main__":
    main()
