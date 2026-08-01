"""Continuous-refit forecast collection (RESULTS-3.md, question KR).

A refit arm re-fits its estimator on the trailing window of observed completions at a
fixed cadence. Epochs are anchored at the first snapshot time and step by cadence_s; the
model used at snapshot time u is the one fit at the latest epoch <= u, trained only on
completions whose completion time is <= that epoch (honesty rule; gated in tests.py). A
completion is usable the moment it completes.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from estimators import base as estbase


def refit_models(snaps, factory, comp_times: np.ndarray, comp_lengths: np.ndarray,
                 cadence_s: float, trailing_n: int, min_fit: int) -> list:
    """One fitted model per snapshot. factory() returns an unfitted estimator; the
    previous fit is kept when an epoch has fewer than min_fit usable completions."""
    order = np.argsort(comp_times, kind="stable")
    ct, cl = comp_times[order], comp_lengths[order]
    models, current, next_epoch = [], None, snaps[0].time_s
    for snap in snaps:
        while snap.time_s >= next_epoch:
            j = int(np.searchsorted(ct, next_epoch, side="right"))
            window = cl[max(0, j - trailing_n):j]
            if len(window) >= min_fit:
                current = factory().fit(window)
                current.trained_through_s = next_epoch  # honesty gate hook
            next_epoch += cadence_s
        if current is None:
            raise RuntimeError("no fit available at first snapshot")
        models.append(current)
    return models


def collect_refit(snaps, arms: dict, r: float, horizons, rng,
                  qs=(0.5, 0.95)) -> dict:
    """Forecast every snapshot for every refit arm and horizon. arms maps arm name to
    the per-snapshot model list from refit_models. Returns the same structure as
    forecast_common.collect's data dict."""
    data = defaultdict(lambda: defaultdict(list))
    for i, snap in enumerate(snaps):
        for t in horizons:
            realized = snap.realized_t1[t]
            for name, models in arms.items():
                agg = estbase.aggregate(models[i], snap, r, t, rng=rng)
                d = data[(name, t)]
                d["point"].append(agg["mean"])
                d["realized"].append(realized)
                for q in qs:
                    d[f"q{q}"].append(agg["quantile"](q))
    return {k: {f: np.asarray(v) for f, v in d.items()} for k, d in data.items()}
