"""Shared snapshot-forecast collection for the round-2 drivers (factorial, drift)."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from estimators import base as estbase


def collect(snaps, trivials, survival_models, r, horizons, rng,
            qs=(0.5, 0.95), skip: tuple = ()):
    """Forecast every snapshot for every model and horizon. Returns (times, data) where
    data[(name, t)] holds point/realized arrays and native quantile arrays for survival
    models. Models named in `skip` are not forecast (calibration windows do not need the
    oracle: it has no conformal mode)."""
    data = defaultdict(lambda: defaultdict(list))
    times = np.array([s.time_s for s in snaps])
    for snap in snaps:
        for t in horizons:
            realized = snap.realized_t1[t]
            for m in trivials:
                if m.name in skip:
                    continue
                d = data[(m.name, t)]
                d["point"].append(m.point(snap, r, t))
                d["realized"].append(realized)
            for m in survival_models:
                if m.name in skip:
                    continue
                agg = estbase.aggregate(m, snap, r, t, rng=rng,
                                        force_mc=(m.name == "oracle"))
                d = data[(m.name, t)]
                d["point"].append(agg["mean"])
                d["realized"].append(realized)
                for q in qs:
                    d[f"q{q}"].append(agg["quantile"](q))
    return times, {k: {f: np.asarray(v) for f, v in d.items()} for k, d in data.items()}
