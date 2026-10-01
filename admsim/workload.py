"""Synthetic request workloads.

A workload is a fixed table of (prompt, output) length pairs built once from its own seed. Each
request draws one row uniformly, plus a client ceiling (the request's max_tokens): half the requests
carry the context length or more, the rest the output law's 0.999 quantile. Output lengths are
clipped at that quantile, and every request is clipped to the context window.
"""

import math

import numpy as np

TABLE_ROWS = 400_000
TABLE_SEED = 20260923
OPEN_CEILING = 32768.0  # the ceiling of a request without a tight max_tokens; clipped to the context


def _lognormal(rng, n, median, sigma):
    return np.maximum(rng.lognormal(math.log(median), sigma, n), 1.0)


def _outputs_exponential(rng, n):
    return np.maximum(rng.exponential(225.0, n), 1.0)


def _outputs_lognormal(rng, n):
    return _lognormal(rng, n, 150.0, 0.9)


def _outputs_bimodal(rng, n):
    """60% short answers (median 80 tokens), 40% long ones (median 2,000)."""
    pick = rng.random(n) < 0.6
    return np.where(pick, _lognormal(rng, n, 80, 0.6), _lognormal(rng, n, 2000, 0.6))


def _outputs_heavy(rng, n):
    """Heavy-tailed outputs: lognormal, median 150, sigma 1.2."""
    return np.maximum(rng.lognormal(np.log(150.0), 1.2, n), 1.0)


OUTPUT_LAWS = {
    "exponential": _outputs_exponential,
    "lognormal": _outputs_lognormal,
    "bimodal": _outputs_bimodal,
    "heavy": _outputs_heavy,
}


class Workload:
    """Draws (prompt, output, ceiling) lengths in tokens.

    law: a key of OUTPUT_LAWS. theta: mean prompt length over mean output length; prompts are
    lognormal (sigma 1) and independent of outputs. context: the model's context window."""

    def __init__(self, law, theta, context):
        rng = np.random.default_rng(TABLE_SEED)
        outputs = OUTPUT_LAWS[law](rng, TABLE_ROWS)
        raw = rng.lognormal(0.0, 1.0, TABLE_ROWS)
        self.prompts = raw * (theta * outputs.mean() / raw.mean())
        self.outputs = outputs
        self.ceiling = float(np.quantile(outputs, 0.999))
        self.context = context

    def draw(self, rng):
        i = rng.integers(len(self.outputs))
        prompt = float(self.prompts[i])
        output = min(float(self.outputs[i]), self.ceiling)
        ceiling = OPEN_CEILING if rng.random() < 0.5 else self.ceiling
        prompt = int(min(max(prompt, 1), self.context - 2))
        ceiling = int(max(min(ceiling, self.context - prompt), 1))
        return prompt, int(max(min(output, ceiling), 1)), ceiling
