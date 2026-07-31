"""B4: two-component lognormal mixture fit by censored EM.

The parametric answer to B3's one round-1 win (latent bimodality at scale, R3/N=1000):
five parameters instead of B3's per-bucket hazards. Cap-hits are right-censored, handled
in both EM steps: the E-step assigns censored observations responsibilities from component
survival at the cap; the M-step replaces their sufficient statistics with truncated-normal
conditional moments (in log space) under the current parameters. With no cap or no
cap-hits this is standard EM on log lengths.

Initialization splits observations at the median log length (deterministic, no restarts);
component separation on the order of the R3 modes makes this reliable, and a collapse to
two near-identical components just reproduces B2. Convergence: relative log-likelihood
change < tol or max_iter (guesses, defined here).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from .base import SurvivalModel

MAX_ITER = 200   # guess
TOL = 1e-8       # guess: relative log-likelihood change
SIGMA_FLOOR = 1e-3
W_FLOOR = 1e-3


class B4LognormalMixtureEM(SurvivalModel):
    name = "B4"

    def __init__(self, cap: int | None = None):
        self.cap = cap

    def fit(self, lengths):
        x = lengths.astype(np.float64)
        cens = (x >= self.cap) if self.cap is not None else np.zeros(len(x), dtype=bool)
        z = np.log(x[~cens])
        log_cap = np.log(float(self.cap)) if self.cap is not None else None
        n_cens = int(cens.sum())

        # Median-split init on observed logs.
        med = np.median(z)
        lo, hi = z[z <= med], z[z > med]
        if len(lo) < 2 or len(hi) < 2:
            lo = hi = z
        self.w = np.array([0.5, 0.5])
        self.mu = np.array([lo.mean(), hi.mean() if n_cens == 0 else max(hi.mean(), med)])
        self.sigma = np.maximum([lo.std(), hi.std()], SIGMA_FLOOR)

        prev_ll = -np.inf
        for _ in range(MAX_ITER):
            # E-step: responsibilities.
            log_dens = np.stack([stats.norm.logpdf(z, self.mu[k], self.sigma[k])
                                 for k in range(2)], axis=1) + np.log(self.w)
            m = log_dens.max(axis=1, keepdims=True)
            dens = np.exp(log_dens - m)
            resp = dens / dens.sum(axis=1, keepdims=True)
            ll = float((m.ravel() + np.log(dens.sum(axis=1))).sum())

            if n_cens:
                log_sf = np.array([stats.norm.logsf((log_cap - self.mu[k]) / self.sigma[k])
                                   for k in range(2)]) + np.log(self.w)
                mc = log_sf.max()
                sfc = np.exp(log_sf - mc)
                resp_c = sfc / sfc.sum()
                ll += n_cens * float(mc + np.log(sfc.sum()))

            # M-step with truncated-normal conditional moments for censored mass.
            s0 = resp.sum(axis=0)
            s1 = (resp * z[:, None]).sum(axis=0)
            s2 = (resp * z[:, None] ** 2).sum(axis=0)
            if n_cens:
                alpha = (log_cap - self.mu) / self.sigma
                lam = np.exp(stats.norm.logpdf(alpha) - stats.norm.logsf(alpha))
                ez = self.mu + self.sigma * lam
                vz = self.sigma ** 2 * np.clip(1.0 + alpha * lam - lam ** 2, 1e-12, None)
                wc = n_cens * resp_c
                s0 = s0 + wc
                s1 = s1 + wc * ez
                s2 = s2 + wc * (vz + ez ** 2)
            self.w = np.clip(s0 / s0.sum(), W_FLOOR, 1.0 - W_FLOOR)
            self.w /= self.w.sum()
            self.mu = s1 / s0
            self.sigma = np.sqrt(np.clip(s2 / s0 - self.mu ** 2, SIGMA_FLOOR ** 2, None))

            if abs(ll - prev_ll) < TOL * (abs(prev_ll) + 1.0):
                break
            prev_ll = ll
        return self

    def survival(self, n):
        n = np.asarray(n, dtype=np.float64)
        out = np.ones_like(n)
        pos = n > 0
        s = np.zeros(int(pos.sum()))
        for k in range(2):
            s += self.w[k] * stats.norm.sf(
                (np.log(n[pos]) - self.mu[k]) / self.sigma[k])
        out[pos] = s
        if self.cap is not None:
            out[n >= self.cap] = 0.0
        return out
