"""Parity check: admsim reproduces the research simulator it was extracted from.

parity_reference.jsonl holds rows the research code produced, with floats rounded to 6 decimals:
  smoke  2 runs at 2,500 requests (about 10 s)
  quick  144 runs at 2,500 requests, the quick self-reuse grid (under a minute on 10 cores)
  full   360 runs at 10,000 requests, the rows behind results/self_reuse_check.md (about 5 minutes)
Each row must match exactly: floats after rounding to 6 decimals, counters as integers.

  python tests/parity.py [smoke|quick|full ...] [-j JOBS]
  python tests/parity.py --calibration        recompute calibration.json's entries and compare
"""

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor

os.environ.setdefault("OMP_NUM_THREADS", "1")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from admsim import calibration, experiment  # noqa: E402

KEYS = ["cell", "kf", "E", "engine", "policy", "load", "seed", "n_req"]
FLOATS = [
    "throughput_tok",
    "goodput",
    "attainment",
    "completions_rate",
    "ttft_p90",
    "preempts_per_completion",
]
INTS = ["completed", "prefill_demand", "prefill_saved", "self_hits", "self_hit_tokens", "reuse_blocks_lost"]


def check(ref):
    row = experiment.run(
        ref["cell"], ref["engine"], ref["policy"], ref["load"], ref["seed"], ref["kf"], ref["E"], ref["n_req"]
    )
    diff = [k for k in FLOATS if round(row[k], 6) != ref[k]] + [k for k in INTS if row.get(k, 0) != ref[k]]
    return ref, diff, row


def check_calibration():
    table = calibration.read_table()
    bad = 0
    for k, want in sorted(table.items()):
        cell, kf, E = k.split("-")
        got = calibration.prep(cell, float(kf[1:]), int(E[1:]), save=False)
        diff = [f for f in want if got[f] != want[f]]
        print(f"{k}: {'ok' if not diff else 'MISMATCH ' + str(diff)}")
        bad += bool(diff)
    return bad


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("groups", nargs="*", default=["smoke", "quick"])
    p.add_argument("-j", "--jobs", type=int, default=None)
    p.add_argument("--calibration", action="store_true")
    a = p.parse_args()
    if a.calibration:
        sys.exit(1 if check_calibration() else 0)
    refs = [json.loads(line) for line in open(os.path.join(HERE, "parity_reference.jsonl"))]
    refs = [r for r in refs if r["group"] in a.groups]
    print(f"{len(refs)} reference runs ({', '.join(a.groups)})")
    bad = 0
    with ProcessPoolExecutor(a.jobs) as ex:
        for i, (ref, diff, row) in enumerate(ex.map(check, refs), 1):
            if diff:
                bad += 1
                print("MISMATCH", {k: ref[k] for k in KEYS}, {k: (ref[k], row.get(k)) for k in diff})
            if i % 20 == 0 or i == len(refs):
                print(f"{i}/{len(refs)} checked, {bad} mismatched", flush=True)
    print("PASS" if not bad else "FAIL")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
