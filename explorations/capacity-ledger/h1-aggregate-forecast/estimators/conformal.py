"""Conformal wrapper: distribution-free quantiles for any point forecaster.

Per (estimator, horizon): collect signed residuals (realized - point) on the calibration
window, then forecast quantile q = point + empirical residual quantile q ("higher"
interpolation, conservative). This is the control for quantile fairness: if
conformal-wrapped deterministic growth matches native hazard-model quantiles, the
distributional machinery adds nothing the residual history did not already contain.
"""

from __future__ import annotations

import numpy as np


def rolling_quantile(snap_times: np.ndarray, horizon_s: float, residuals: np.ndarray,
                     eval_times: np.ndarray, window_s: float, q: float) -> np.ndarray:
    """Honest rolling conformal quantile: for each forecast time s in eval_times, the
    empirical q-quantile ("higher") of residuals whose realization time
    (snapshot time + horizon) lies in (s - window_s, s]. A residual is usable only once
    realized; no future information enters. snap_times/residuals cover every snapshot
    with a computed residual (calibration and evaluation), in time order."""
    ready = snap_times + horizon_s
    order = np.argsort(ready)
    ready, resid = ready[order], residuals[order]
    out = np.empty(len(eval_times))
    for i, s in enumerate(eval_times):
        j_hi = np.searchsorted(ready, s, side="right")
        j_lo = np.searchsorted(ready, s - window_s, side="right")
        out[i] = (np.quantile(resid[j_lo:j_hi], q, method="higher")
                  if j_hi > j_lo else np.nan)
    return out


class ConformalWrapper:
    def __init__(self):
        self.residual_q: dict[tuple[str, float], np.ndarray] = {}

    def calibrate(self, key: tuple[str, float], points: np.ndarray, realized: np.ndarray,
                  quantiles: tuple[float, ...]):
        resid = realized - points
        self.residual_q[key] = {
            q: float(np.quantile(resid, q, method="higher")) for q in quantiles}

    def quantile(self, key: tuple[str, float], point: float, q: float) -> float:
        return point + self.residual_q[key][q]

    def has(self, key) -> bool:
        return key in self.residual_q
