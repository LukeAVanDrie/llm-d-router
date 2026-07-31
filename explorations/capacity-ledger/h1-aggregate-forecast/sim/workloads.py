"""Regime samplers and analytic ground-truth survival functions.

Every regime provides sample(rng, size) -> integer output lengths (tokens) and
true_survival(n) -> P(L > n) evaluated analytically, so the oracle estimator and the
oracle-vs-empirical unit test share one definition with the sampler.

Length caps (R4) are applied as effective length L_eff = min(L, cap): the process under
study completes when generation reaches the cap, so estimators see L_eff and the true
survival of L_eff is S(n) for n < cap and 0 for n >= cap.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from scipy import stats

PARAMS_PATH = Path(__file__).resolve().parent.parent / "data" / "params_default.json"


def load_params(path: Path = PARAMS_PATH) -> dict:
    with open(path) as f:
        return json.load(f)


class OutputDist:
    """Base: continuous latent length; sampled lengths are rounded up to >= 1 token."""

    def sample(self, rng: np.random.Generator, size: int) -> np.ndarray:
        raise NotImplementedError

    def survival(self, n: np.ndarray) -> np.ndarray:
        """P(L > n) for the continuous latent variable; adequate for token-scale n."""
        raise NotImplementedError

    def mean(self) -> float:
        """E[L], used only for Little's-law arrival sizing (estimated numerically)."""
        rng = np.random.default_rng(12345)
        return float(self.sample(rng, 200_000).mean())


@dataclass
class Lognormal(OutputDist):
    median: float
    sigma: float

    @property
    def mu(self) -> float:
        return math.log(self.median)

    def sample(self, rng, size):
        return np.maximum(1, np.ceil(rng.lognormal(self.mu, self.sigma, size))).astype(np.int64)

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        out = np.ones_like(n)
        pos = n > 0
        out[pos] = stats.norm.sf((np.log(n[pos]) - self.mu) / self.sigma)
        return out


@dataclass
class LognormalParetoSplice(OutputDist):
    """Lognormal body below its q-quantile x_q; Pareto(x_q, alpha) truncated at tail_max
    above it, with tail mass 1 - q. Survival is continuous at the splice point."""

    median: float
    sigma: float
    splice_q: float
    alpha: float
    tail_max: float

    def __post_init__(self):
        self.body = Lognormal(self.median, self.sigma)
        self.x_q = float(stats.lognorm.ppf(self.splice_q, self.sigma, scale=self.median))

    def _pareto_sf_trunc(self, n):
        # P(X > n | Pareto(x_q, alpha) truncated at tail_max)
        a, xm, h = self.alpha, self.x_q, self.tail_max
        denom = 1.0 - (xm / h) ** a
        n = np.clip(n, xm, h)
        return ((xm / n) ** a - (xm / h) ** a) / denom

    def sample(self, rng, size):
        body_n = rng.random(size) < self.splice_q
        out = np.empty(size, dtype=np.float64)
        nb = int(body_n.sum())
        # Body: lognormal conditioned on <= x_q via inverse-CDF on U(0, splice_q).
        u = rng.random(nb) * self.splice_q
        out[body_n] = stats.lognorm.ppf(u, self.sigma, scale=self.median)
        # Tail: truncated Pareto via inverse-CDF.
        nt = size - nb
        u = rng.random(nt)
        a, xm, h = self.alpha, self.x_q, self.tail_max
        denom = 1.0 - (xm / h) ** a
        out[~body_n] = xm * (1.0 - u * denom) ** (-1.0 / a)
        return np.maximum(1, np.ceil(out)).astype(np.int64)

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        below = n < self.x_q
        out = np.empty_like(n)
        # For n < x_q: P(L > n) = 1 - P(body) * CDF_LN(n)/CDF_LN(x_q) = 1 - CDF_LN(n)
        # (because P(body) = splice_q = CDF_LN(x_q)).
        out[below] = self.body.survival(n[below])
        out[~below] = (1.0 - self.splice_q) * self._pareto_sf_trunc(n[~below])
        return out


@dataclass
class LognormalMixture(OutputDist):
    weights: tuple
    medians: tuple
    sigmas: tuple

    def __post_init__(self):
        self.components = [Lognormal(m, s) for m, s in zip(self.medians, self.sigmas)]

    def sample(self, rng, size):
        comp = rng.choice(len(self.weights), size=size, p=self.weights)
        out = np.empty(size, dtype=np.int64)
        for i, c in enumerate(self.components):
            mask = comp == i
            out[mask] = c.sample(rng, int(mask.sum()))
        return out

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        return sum(w * c.survival(n) for w, c in zip(self.weights, self.components))


@dataclass
class Capped(OutputDist):
    """Effective length min(L, cap): survival S(n) for n < cap, exactly 0 at n >= cap."""

    inner: OutputDist
    cap: int

    def sample(self, rng, size):
        return np.minimum(self.inner.sample(rng, size), self.cap)

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        out = self.inner.survival(n)
        out[n >= self.cap] = 0.0
        return out


@dataclass
class PromptDist:
    median: float
    sigma: float
    lo: int
    hi: int

    def sample(self, rng, size):
        x = np.ceil(rng.lognormal(math.log(self.median), self.sigma, size))
        return np.clip(x, self.lo, self.hi).astype(np.int64)


def build_regime(name: str, params: dict) -> tuple[OutputDist, int | None]:
    spec = params["regimes"][name]
    ospec = spec["output"]
    if "same_as" in ospec:
        ospec = params["regimes"][ospec["same_as"]]["output"]
    if ospec["dist"] == "lognormal":
        dist: OutputDist = Lognormal(ospec["median"], ospec["sigma_log"])
    elif ospec["dist"] == "lognormal_pareto_splice":
        dist = LognormalParetoSplice(
            ospec["median"], ospec["sigma_log"], ospec["splice_quantile"],
            ospec["pareto_alpha"], ospec["tail_max"])
    elif ospec["dist"] == "lognormal_mixture":
        dist = LognormalMixture(
            tuple(ospec["weights"]), tuple(ospec["medians"]), tuple(ospec["sigmas_log"]))
    else:
        raise ValueError(f"unknown dist {ospec['dist']}")
    cap = spec["cap"]
    if cap is not None:
        dist = Capped(dist, int(cap))
    return dist, cap


def build_prompt_dist(params: dict) -> PromptDist:
    p = params["prompt"]
    return PromptDist(p["median"], p["sigma_log"], p["min"], p["max"])
