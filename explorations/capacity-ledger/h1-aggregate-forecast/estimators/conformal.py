"""Conformal wrapper: distribution-free quantiles for any point forecaster.

Per (estimator, horizon): collect signed residuals (realized - point) on the calibration
window, then forecast quantile q = point + empirical residual quantile q ("higher"
interpolation, conservative). This is the control for quantile fairness: if
conformal-wrapped deterministic growth matches native hazard-model quantiles, the
distributional machinery adds nothing the residual history did not already contain.
"""

from __future__ import annotations

import numpy as np


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
