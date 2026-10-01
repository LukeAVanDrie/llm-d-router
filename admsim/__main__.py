"""Command line: python -m admsim {run,batch,score,prep} --help"""

import argparse
import json
import os

os.environ.setdefault("OMP_NUM_THREADS", "1")  # runs are single-threaded; batch parallelizes

from . import calibration, experiment  # noqa: E402
from .metrics import paired_log_ratios  # noqa: E402

POLICIES = [
    "fit",
    "nogate",
    "lookahead:36.79",
    "lookahead:22.31",
    "lookahead:13.53",
    "lookahead:4.98",
]
CELLS = ["bimodal:1", "bimodal:0.5", "longout:1"]


def _cell_kf(s):
    cell, _, kf = s.partition(":")
    return cell, float(kf or 1)


def _policy_order(spec):
    """nogate, fit, then lookahead from loosest to strictest threshold, then the rest."""
    name, _, arg = spec.partition(":")
    rank = {"nogate": 0, "fit": 1, "lookahead": 2}.get(name, 3)
    return rank, name, -float(arg) if arg else 0.0


def cmd_run(a):
    print(json.dumps(experiment.run(a.cell, a.engine, a.policy, a.load, a.seed, a.kf, a.E, a.requests)))


def cmd_batch(a):
    experiment.batch(
        a.out,
        engines=a.engines,
        policies=a.policies,
        cells=[_cell_kf(c) for c in a.cells],
        loads=a.loads,
        seeds=a.seeds,
        E=a.E,
        n_req=a.requests,
        jobs=a.jobs,
    )


def cmd_score(a):
    table = paired_log_ratios(experiment.read_rows(a.dir), a.baseline)
    for load in sorted({k[0] for k in table}):
        met = "goodput" if load < 1 else "throughput_tok"
        print(
            f"\n== load {load:g}: {met}, % log ratio against {a.baseline}, +-1.96 standard errors (n seeds)"
        )
        for key, res in table.items():
            if key[0] != load:
                continue
            _, cell, kf, E, engine = key
            cols = [
                f"{p}: {res[p][0]:+.1f} +-{res[p][1]:.1f} ({res[p][2]})"
                for p in sorted(res, key=_policy_order)
            ]
            print(f"{cell} K{kf:g} E{E} {engine:9s}  " + "  ".join(cols))


def cmd_prep(a):
    print(json.dumps(calibration.prep(a.cell, a.kf, a.E)))


def main():
    p = argparse.ArgumentParser(prog="python -m admsim", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="one run; prints one JSON line")
    r.add_argument("--cell", default="bimodal", help="bimodal | longout")
    r.add_argument("--kf", type=float, default=1.0, help="KV cache factor")
    r.add_argument("--E", type=int, default=4, help="endpoints")
    r.add_argument("--engine", default="recompute", help="recompute | vllmreuse")
    r.add_argument("--policy", default="fit", help="nogate | fit | lookahead:<pct>")
    r.add_argument("--load", type=float, default=1.1, help="arrival rate over capacity")
    r.add_argument("--seed", type=int, default=101)
    r.add_argument("--requests", type=int, default=10000)
    r.set_defaults(fn=cmd_run)

    b = sub.add_parser("batch", help="a grid of runs in parallel, one JSON file per run")
    b.add_argument("-o", "--out", required=True, help="output directory")
    b.add_argument("--engines", nargs="+", default=["recompute", "vllmreuse"])
    b.add_argument("--policies", nargs="+", default=POLICIES)
    b.add_argument("--cells", nargs="+", default=CELLS, help="cell:kf pairs")
    b.add_argument("--loads", nargs="+", type=float, default=[0.95, 1.1])
    b.add_argument("--seeds", nargs="+", type=int, default=[104, 105, 106, 107, 108])
    b.add_argument("--E", type=int, default=4)
    b.add_argument("--requests", type=int, default=10000)
    b.add_argument("-j", "--jobs", type=int, default=None, help="worker processes (default: all cores)")
    b.set_defaults(fn=cmd_batch)

    s = sub.add_parser("score", help="paired comparison of a batch directory against a baseline policy")
    s.add_argument("dir")
    s.add_argument("--baseline", default="fit")
    s.set_defaults(fn=cmd_score)

    q = sub.add_parser("prep", help="compute capacity and SLO thresholds for a new variant")
    q.add_argument("cell")
    q.add_argument("kf", type=float)
    q.add_argument("E", type=int)
    q.set_defaults(fn=cmd_prep)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
