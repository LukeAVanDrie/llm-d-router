"""Scoring: pinball loss, coverage/width, MAE, per-lease Brier."""

from __future__ import annotations

import numpy as np

QUANTILES = (0.5, 0.9, 0.95, 0.99)
HEADLINE_Q = 0.95


def pinball(realized: np.ndarray, predicted: np.ndarray, q: float) -> float:
    d = realized - predicted
    return float(np.mean(np.maximum(q * d, (q - 1.0) * d)))


def pinball_per_snapshot(realized: np.ndarray, predicted: np.ndarray, q: float) -> np.ndarray:
    d = realized - predicted
    return np.maximum(q * d, (q - 1.0) * d)


def coverage(realized: np.ndarray, upper: np.ndarray) -> float:
    """Fraction of snapshots where the one-sided upper bound held."""
    return float(np.mean(realized <= upper))


def mean_width(upper: np.ndarray, median: np.ndarray) -> float:
    """Width of the one-sided q95 bound above the median forecast."""
    return float(np.mean(upper - median))


def mae(realized: np.ndarray, mean_forecast: np.ndarray) -> float:
    return float(np.mean(np.abs(realized - mean_forecast)))


def brier(p_alive: np.ndarray, alive: np.ndarray) -> float:
    """Per-lease Brier score for P(alive at horizon); diagnostic only."""
    return float(np.mean((p_alive - alive.astype(np.float64)) ** 2))


def reliability_bins(p_alive: np.ndarray, alive: np.ndarray, bins: int = 10):
    edges = np.linspace(0.0, 1.0, bins + 1)
    idx = np.clip(np.digitize(p_alive, edges) - 1, 0, bins - 1)
    pred_mean, obs_mean, count = [], [], []
    for b in range(bins):
        m = idx == b
        if m.sum() == 0:
            continue
        pred_mean.append(float(p_alive[m].mean()))
        obs_mean.append(float(alive[m].mean()))
        count.append(int(m.sum()))
    return pred_mean, obs_mean, count
