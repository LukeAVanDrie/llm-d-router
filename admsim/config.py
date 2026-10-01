"""Engine configuration and the named cells (workload plus KV cache size)."""

from dataclasses import dataclass

from .workload import Workload

BLOCK = 16  # tokens per KV block
CONTEXT = 8192  # model context window, tokens


@dataclass(frozen=True)
class Cell:
    law: str  # output-length law, a key of workload.OUTPUT_LAWS
    theta: float  # mean prompt length over mean output length
    cache_contexts: float  # KV cache per endpoint, in full contexts (cache_contexts * CONTEXT tokens)


CELLS = {
    "bimodal": Cell(law="bimodal", theta=1.0, cache_contexts=10.0),
    "longout": Cell(law="heavy", theta=0.1, cache_contexts=3.0),
}


@dataclass(frozen=True)
class EngineConfig:
    """One endpoint. Time is in units of a step's fixed cost: a step takes
    a + b * decoding_streams + c * resident_tokens + b_pf * prefill_tokens, times lognormal noise."""

    K: int  # KV blocks
    c: float  # per resident token (a stand-in for HBM bandwidth)
    a: float = 1.0
    b: float = 0.01  # per decoding stream
    kappa0: float = 150.0  # prefill tokens that cost one step's fixed cost (b_pf = a / kappa0)
    budget: int = 2048  # tokens per step (chunked prefill plus decodes)
    max_seqs: int = 512
    noise: float = 0.05  # sigma of the step-time noise
    lag: float = 1.0  # delay from a routing decision to the request reaching the engine
    context: int = CONTEXT

    @property
    def b_pf(self):
        return self.a / self.kappa0


def engine_config(cell, kf=1.0):
    """The cell's endpoint with its KV cache scaled by kf. The per-token cost c stays at the full
    cache's value, so a smaller cache changes memory, not step time."""
    spec = CELLS[cell]
    k_full = int(spec.cache_contexts * CONTEXT / BLOCK)
    return EngineConfig(K=int(spec.cache_contexts * kf * CONTEXT / BLOCK), c=0.5 * 1.0 / (k_full * BLOCK))


def variant(cell, kf=1.0):
    """(EngineConfig, Workload) for a cell at cache factor kf."""
    cfg = engine_config(cell, kf)
    spec = CELLS[cell]
    return cfg, Workload(spec.law, spec.theta, cfg.context)
