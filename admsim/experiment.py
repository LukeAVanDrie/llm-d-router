"""Named-cell runs: one run to one result row, and grids of runs in parallel."""

import itertools
import json
import os
from concurrent.futures import ProcessPoolExecutor

from . import calibration
from .config import variant
from .metrics import summarize
from .policies import make_policy
from .simulation import simulate


def run(cell, engine, policy, load, seed, kf=1.0, E=4, n_req=10000):
    """One run of a named cell at `load` times its capacity. Returns a flat dict."""
    cal = calibration.load(cell, kf, E)
    cfg, workload = variant(cell, kf)
    result = simulate(
        workload, cfg, engine, make_policy(policy, cfg, seed), E, n_req, seed, rate=load * cal["capacity"]
    )
    row = {
        "cell": cell,
        "kf": kf,
        "E": E,
        "engine": engine,
        "policy": policy,
        "load": load,
        "seed": seed,
        "n_req": n_req,
        "capacity": cal["capacity"],
        "ttft_slo": cal["ttft_slo"],
        "tpot_slo": cal["tpot_slo"],
    }
    row.update(summarize(result, cal))
    return row


def _file_name(p):
    return (
        f"{p['engine']}-{p['policy'].replace(':', '_')}-{p['cell']}-K{p['kf']:g}-E{p['E']}"
        f"-{p['load']:g}-{p['seed']}.json"
    )


def _run_to_file(args):
    params, path = args
    row = run(**params)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(json.dumps(row) + "\n")
    os.replace(tmp, path)
    return path


def batch(out_dir, engines, policies, cells, loads, seeds, E=4, n_req=10000, jobs=None, log=print):
    """Run the grid engines x policies x cells ((cell, kf) pairs) x loads x seeds, one JSON file per
    run in out_dir. Runs whose file exists are skipped, so an interrupted batch resumes."""
    os.makedirs(out_dir, exist_ok=True)
    todo = []
    for seed, load, engine, policy, (cell, kf) in itertools.product(seeds, loads, engines, policies, cells):
        p = dict(cell=cell, kf=kf, E=E, engine=engine, policy=policy, load=load, seed=seed, n_req=n_req)
        calibration.load(cell, kf, E)  # fail before starting workers
        path = os.path.join(out_dir, _file_name(p))
        if not os.path.exists(path):
            todo.append((p, path))
    log(f"{len(todo)} runs to do in {out_dir}")
    with ProcessPoolExecutor(jobs) as ex:
        for i, _ in enumerate(ex.map(_run_to_file, todo), 1):
            if i % 10 == 0 or i == len(todo):
                log(f"{i}/{len(todo)} done")


def read_rows(out_dir):
    rows = []
    for name in sorted(os.listdir(out_dir)):
        if name.endswith(".json"):
            with open(os.path.join(out_dir, name)) as f:
                rows.append(json.loads(f.read()))
    return rows
