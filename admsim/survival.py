"""Output-length survival learned online from completions.

S(x) is the probability that a request generates more than x tokens. The band holds a Bayesian
bootstrap of Kaplan-Meier curves: each curve reweights the observations with Dirichlet(1, ..., 1)
weights. Observations are the last `window` completed lengths, where a length that reached its
client ceiling is censored (the request would have run longer), plus the current ages of running
requests, also censored, plus one censored pseudo-observation at the context length.
"""

import numpy as np


def kaplan_meier(times, events, weights):
    """Weighted Kaplan-Meier: (event times ascending, survival just after each)."""
    o = np.argsort(times, kind="stable")
    t, ev, w = times[o], events[o], weights[o]
    at_risk = np.cumsum(w[::-1])[::-1]
    hz = np.where(ev, w / np.maximum(at_risk, 1e-300), 0.0)
    s = np.cumprod(1.0 - hz)
    m = ev.astype(bool)
    return t[m], s[m]


class SurvivalBand:
    def __init__(self, rng, context, q=0.95, curves=20, window=1000):
        self.rng, self.context, self.q = rng, context, q
        self.n_curves, self.window = curves, window
        self.completed = []  # (length, ceiling) of recent completions
        self.ready = False
        self.table = None  # (curves, context + 2): S tabulated on the integer grid
        self.qi = None  # index of the q-quantile curve

    def observe(self, length, ceiling):
        self.completed.append((length, ceiling))
        if len(self.completed) > self.window:
            del self.completed[: len(self.completed) - self.window]

    def refresh(self, ages):
        """Rebuild the curves from the completions so far and the running requests' ages."""
        if not self.completed:
            return
        L = np.asarray([n for n, _ in self.completed])
        C = np.asarray([c for _, c in self.completed])
        A = np.asarray(ages, float)
        times = np.concatenate([L, A, [float(self.context)]])
        events = np.concatenate([~(L >= C - 1e-6), np.zeros(len(A), bool), [False]])
        xs = np.arange(self.context + 2, dtype=float)
        rows = []
        for _ in range(self.n_curves):
            w = self.rng.dirichlet(np.ones(len(times)))
            tt, ss = kaplan_meier(times, events, w)
            if len(tt) == 0:
                tt, ss = np.asarray([float(self.context)]), np.asarray([1.0])
            i = np.searchsorted(tt, xs, side="right") - 1
            rows.append(np.where(i >= 0, ss[np.maximum(i, 0)], 1.0))
        self.table = np.stack(rows)
        self.qi = int(round(self.q * (len(rows) - 1)))
        self.ready = True

    def _eval(self, x):
        idx = np.clip(np.asarray(x, float).astype(np.int64), 0, self.context + 1)
        return self.table[:, idx]

    def surv_mean(self, x):
        """S(x), averaged over curves."""
        return self._eval(x).mean(axis=0)

    def _cond(self, a, grid):
        """S(grid) / S(a) per curve, for ages a (n,) and grid (m, n): shape (curves, m, n)."""
        num = self._eval(grid)
        den = self._eval(a)[:, None, :]
        return np.where(den > 0, np.minimum(1.0, num / np.maximum(den, 1e-300)), 1.0)

    def cond_mean(self, a, grid):
        """Conditional survival S(grid) / S(a), averaged over curves: (m, n)."""
        return self._cond(a, grid).mean(axis=0)

    def cond_upper(self, a, grid):
        """Conditional survival S(grid) / S(a), the curves' upper q-quantile: (m, n)."""
        return np.partition(self._cond(a, grid), self.qi, axis=0)[self.qi]
