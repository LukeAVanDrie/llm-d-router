"""Baselines B0, B0g, B1, B2, B2c and the oracle.

B0/B0g are point forecasters (no survival model, no native quantiles); they get quantiles
only through the conformal wrapper. B1/B2/B2c produce survival curves consumed by
base.aggregate. The oracle wraps the regime's analytic survival and bounds achievable skill.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize, stats

from .base import SurvivalModel


class B0Persistence:
    """O(t) = O(0)."""
    name = "B0"
    has_native_quantiles = False

    def point(self, snap, r: float, t: float) -> float:
        return snap.occupancy


class B0Growth:
    """Deterministic growth, no completions: sum of min(f + r*t, prompt + max length).
    The strongest trivial baseline; requires no training data."""
    name = "B0g"
    has_native_quantiles = False

    def __init__(self, max_len: float):
        self.max_len = max_len  # regime cap if any, else inf (no ceiling short of growth)

    def point(self, snap, r: float, t: float) -> float:
        ceil = snap.prompts + self.max_len
        return float(np.minimum(snap.footprints + r * t, ceil).sum())


class B1ConstantHazard(SurvivalModel):
    """Age-blind geometric: MLE per-token hazard h = completions / total token exposure.
    Exposure form so the same fit is censoring-aware for free in phase 2."""
    name = "B1"

    def fit(self, lengths):
        self.h = len(lengths) / float(lengths.sum())
        return self

    def survival(self, n):
        return (1.0 - self.h) ** np.maximum(np.asarray(n, dtype=np.float64), 0.0)


class B2Lognormal(SurvivalModel):
    """Age-conditioned parametric: lognormal MLE, analytic conditional survival."""
    name = "B2"

    def fit(self, lengths):
        logs = np.log(lengths.astype(np.float64))
        self.mu = float(logs.mean())
        self.sigma = max(float(logs.std()), 1e-6)
        return self

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        out = np.ones_like(n)
        pos = n > 0
        out[pos] = stats.norm.sf((np.log(n[pos]) - self.mu) / self.sigma)
        return out


class B2cCensoredLognormal(SurvivalModel):
    """B2 with observations at the cap treated as right-censored (Tobit likelihood), and
    survival forced to 0 at and beyond the cap (the effective length cannot exceed it).
    With no cap or no cap-hits this reduces to B2."""
    name = "B2c"

    def __init__(self, cap: int | None):
        self.cap = cap

    def fit(self, lengths):
        x = lengths.astype(np.float64)
        if self.cap is None or not (x >= self.cap).any():
            b2 = B2Lognormal().fit(lengths)
            self.mu, self.sigma = b2.mu, b2.sigma
            return self
        cens = x >= self.cap
        obs_logs = np.log(x[~cens])
        log_cap = np.log(float(self.cap))
        n_cens = int(cens.sum())

        def nll(params):
            mu, log_sigma = params
            sigma = np.exp(log_sigma)
            ll = stats.norm.logpdf(obs_logs, mu, sigma).sum() - obs_logs.sum()
            ll += n_cens * stats.norm.logsf((log_cap - mu) / sigma)
            return -ll

        start = [float(np.log(x).mean()), float(np.log(np.log(x).std() + 1e-6))]
        r = optimize.minimize(nll, start, method="Nelder-Mead")
        self.mu, self.sigma = float(r.x[0]), float(np.exp(r.x[1]))
        return self

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        out = np.ones_like(n)
        pos = n > 0
        out[pos] = stats.norm.sf((np.log(n[pos]) - self.mu) / self.sigma)
        if self.cap is not None:
            out[n >= self.cap] = 0.0
        return out


class OracleSurvival(SurvivalModel):
    """True generating survival; defines the achievable ceiling and gates the harness."""
    name = "oracle"

    def __init__(self, dist):
        self.dist = dist

    def fit(self, lengths):
        return self

    def survival(self, n):
        return self.dist.survival(np.asarray(n, dtype=np.float64))
