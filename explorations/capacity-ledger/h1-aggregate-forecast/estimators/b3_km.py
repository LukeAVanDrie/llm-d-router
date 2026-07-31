"""B3: age-conditioned nonparametric discrete hazard over geometric age buckets.

The proposed machinery under test. Per-token hazard per bucket is deaths over token
exposure, shrunk toward the age-blind constant hazard (B1) with a prior weighted at
`prior_obs` average observations: h_b = (deaths_b + h1 * w) / (exposure_b + w), where
w = prior_obs * mean(L) tokens of pseudo-exposure. The constructor defaults (bucket start
16, ratio 1.5, prior_obs 5, Hill on the top decile) are all guesses, per STYLE.md's
numbers rule; the verdict in RESULTS.md is a verdict on B3 at these settings. Survival is the product of per-bucket
geometric survival, computed in log space with cumulative sums at bucket edges and linear
interpolation (in log-survival) inside a bucket, so survival(n) is vectorized. Beyond the
last populated bucket the last hazard extends (constant-hazard extrapolation).
"""

from __future__ import annotations

import numpy as np

from .base import SurvivalModel
from .baselines import B1ConstantHazard


def geometric_edges(start: float, ratio: float, upper: float) -> np.ndarray:
    edges = [0.0, start]
    while edges[-1] < upper:
        edges.append(edges[-1] * ratio)
    return np.asarray(edges)


class B3DiscreteHazardKM(SurvivalModel):
    name = "B3"

    def __init__(self, start: float = 16.0, ratio: float = 1.5, prior_obs: float = 5.0,
                 cap: int | None = None):
        self.start, self.ratio, self.prior_obs = start, ratio, prior_obs
        self.cap = cap

    def fit(self, lengths):
        x = lengths.astype(np.float64)
        self.edges = geometric_edges(self.start, self.ratio, float(x.max()))
        lo, hi = self.edges[:-1], self.edges[1:]
        nb = len(lo)

        # Token exposure per bucket: an observation of length L spends
        # min(L, hi) - lo tokens in bucket b if L > lo.
        exposure = np.zeros(nb)
        deaths = np.zeros(nb)
        for b in range(nb):
            over = x > lo[b]
            exposure[b] = np.minimum(x[over], hi[b]).sum() - over.sum() * lo[b]
            deaths[b] = ((x > lo[b]) & (x <= hi[b])).sum()

        h1 = B1ConstantHazard().fit(lengths).h
        w = self.prior_obs * float(x.mean())
        h = (deaths + h1 * w) / (exposure + w)
        self.hazard = np.clip(h, 1e-12, 1.0 - 1e-9)

        # Cumulative log-survival at bucket edges.
        log1m = np.log1p(-self.hazard)
        widths = hi - lo
        self.cum_log_s = np.concatenate([[0.0], np.cumsum(log1m * widths)])
        self.log1m = log1m

        # Heavy-tail extrapolation beyond the last populated bucket: Hill estimator on the
        # top decile gives a power-law survival exponent; constant-hazard extrapolation
        # (geometric tail) collapses too fast under Pareto-class tails. Falls back to the
        # last bucket's hazard when the tail is degenerate (ties, e.g. a max_tokens atom).
        k = max(int(0.1 * len(x)), 20)
        tail = np.sort(x)[-k:]
        logs = np.log(tail / tail[0])
        self.alpha_tail = k / logs.sum() if logs.sum() > 1e-9 else None
        return self

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        lo, hi = self.edges[:-1], self.edges[1:]
        b = np.clip(np.searchsorted(self.edges, n, side="right") - 1, 0, len(lo) - 1)
        within = np.clip(n - lo[b], 0.0, hi[b] - lo[b])
        log_s = self.cum_log_s[b] + self.log1m[b] * within
        # Beyond the last edge: Hill power-law tail when available, else last hazard.
        beyond = n > self.edges[-1]
        if beyond.any():
            if self.alpha_tail is not None:
                log_s[beyond] = self.cum_log_s[-1] - self.alpha_tail * np.log(
                    n[beyond] / self.edges[-1])
            else:
                log_s[beyond] = self.cum_log_s[-1] + self.log1m[-1] * (
                    n[beyond] - self.edges[-1])
        out = np.exp(log_s)
        if self.cap is not None:
            out[n >= self.cap] = 0.0
        return out
