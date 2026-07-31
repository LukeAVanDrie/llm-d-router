"""Moving-block bootstrap over snapshot times.

Snapshots one second apart share lease populations, so per-snapshot losses are strongly
autocorrelated; iid resampling would understate CI width. Blocks of consecutive snapshots
(default 30 s) are resampled with replacement to the original length.
"""

from __future__ import annotations

import numpy as np

BLOCK_LEN = 30
N_RESAMPLES = 1000


def _block_indices(n: int, rng: np.random.Generator, block: int) -> np.ndarray:
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, max(n - block, 1), size=n_blocks)
    idx = (starts[:, None] + np.arange(block)[None, :]).ravel()[:n]
    return np.clip(idx, 0, n - 1)


def ci_mean(series: np.ndarray, rng: np.random.Generator,
            block: int = BLOCK_LEN, reps: int = N_RESAMPLES,
            alpha: float = 0.10) -> tuple[float, float, float]:
    """(mean, lo, hi) with a 100*(1-alpha)% moving-block bootstrap CI."""
    n = len(series)
    means = np.empty(reps)
    for i in range(reps):
        means[i] = series[_block_indices(n, rng, block)].mean()
    return float(series.mean()), float(np.quantile(means, alpha / 2)), \
        float(np.quantile(means, 1 - alpha / 2))


def ci_paired_diff(series_a: np.ndarray, series_b: np.ndarray, rng: np.random.Generator,
                   block: int = BLOCK_LEN, reps: int = N_RESAMPLES,
                   alpha: float = 0.10) -> tuple[float, float, float]:
    """CI on mean(a - b) using the same block indices for both series (paired)."""
    return ci_mean(series_a - series_b, rng, block, reps, alpha)
