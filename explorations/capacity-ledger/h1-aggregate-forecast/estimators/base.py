"""Estimator interface and pool-level forecast aggregation.

Given fixed known decode rates, a lease's horizon-t occupancy contribution is a scaled
Bernoulli: (f + r*t) with probability p = S(n + r*t)/S(n), else 0. Pool mean and variance
follow from independence; native quantiles use the normal approximation at N >= 100 and
Monte Carlo below that (and as a spot check).
"""

from __future__ import annotations

import numpy as np

MC_DRAWS = 2000          # convention; MC-vs-normal agreement checked in tests.py
NORMAL_APPROX_MIN_N = 100  # guess for where CLT suffices; oracle gate would catch a bad one


class SurvivalModel:
    name = "base"
    has_native_quantiles = True

    def fit(self, lengths: np.ndarray) -> "SurvivalModel":
        return self

    def survival(self, n: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def p_alive(self, ages: np.ndarray, growth: float) -> np.ndarray:
        """P(L > n + growth | L > n), vectorized over leases."""
        s_now = self.survival(ages)
        s_then = self.survival(ages + growth)
        with np.errstate(divide="ignore", invalid="ignore"):
            p = np.where(s_now > 0, s_then / s_now, 0.0)
        return np.clip(p, 0.0, 1.0)


def aggregate(model: SurvivalModel, snap, r: float, t: float,
              rng: np.random.Generator | None = None, force_mc: bool = False) -> dict:
    """Pool forecast for one snapshot and horizon: mean, sd, per-lease p, and a
    quantile(q) callable (normal approx or MC depending on pool size). force_mc is used
    by the oracle so the harness gate tests exact quantiles rather than the normal
    approximation's skew error."""
    grow = r * t
    vals = snap.footprints + grow
    p = model.p_alive(snap.ages, grow)
    mean = float((p * vals).sum())
    var = float((vals * vals * p * (1.0 - p)).sum())
    sd = var ** 0.5

    n = len(vals)
    if not force_mc and (n >= NORMAL_APPROX_MIN_N or rng is None):
        from scipy import stats
        def quantile(q: float) -> float:
            return mean + stats.norm.ppf(q) * sd
    else:
        draws = (rng.random((MC_DRAWS, n)) < p) @ vals
        def quantile(q: float) -> float:
            return float(np.quantile(draws, q, method="higher"))
    return {"mean": mean, "sd": sd, "p": p, "quantile": quantile}
