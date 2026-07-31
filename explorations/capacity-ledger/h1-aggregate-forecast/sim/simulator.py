"""Fixed-step pool simulator with exact realized-T1 computation.

Leases decode at a fixed known rate r (phase-1 attribution choice: all estimator differences
flow through the survival curve, none through rate estimation). Because true lengths are drawn
at admission and rates are deterministic, the realized T1 target at any snapshot is computed
exactly from lease state: a lease with age n survives horizon t iff n + r*t < L_eff, and a
surviving lease's footprint at the horizon is f + r*t.

Load-coupled decode rates are deliberately out of scope; that realism belongs to an external
discrete-event simulator (BLIS) in a later phase.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from . import workloads

HORIZONS_S = (1.0, 2.0, 5.0, 10.0)


@dataclass
class Snapshot:
    """Pool state at one forecast instant plus exact realized T1 per horizon."""

    time_s: float
    ages: np.ndarray        # tokens generated so far, per active lease
    footprints: np.ndarray  # prompt + generated tokens, per active lease
    prompts: np.ndarray
    lengths: np.ndarray     # hidden true L_eff (used only for realized targets / oracle test)
    occupancy: float
    realized_t1: dict       # horizon -> realized occupancy at t from currently active leases


@dataclass
class SimResult:
    capacity: float
    rate_tokens_per_s: float
    cap: int | None
    training_lengths: np.ndarray  # completed L_eff during warm-up (uncensored)
    calib: list = field(default_factory=list)  # snapshots in calibration window
    eval: list = field(default_factory=list)   # snapshots in evaluation window
    mean_active: float = 0.0
    target_n: int = 0
    dropped: int = 0
    admitted: int = 0


class Pool:
    """Active-lease state as growable parallel arrays."""

    def __init__(self):
        self.prompts = np.empty(0, dtype=np.float64)
        self.lengths = np.empty(0, dtype=np.float64)
        self.ages = np.empty(0, dtype=np.float64)

    def occupancy(self) -> float:
        return float(self.prompts.sum() + self.ages.sum())

    def step(self, tokens: float) -> np.ndarray:
        """Advance decode by `tokens`; returns completed lengths removed from the pool."""
        self.ages += tokens
        done = self.ages >= self.lengths
        completed = self.lengths[done]
        keep = ~done
        self.prompts, self.lengths, self.ages = (
            self.prompts[keep], self.lengths[keep], self.ages[keep])
        return completed

    def admit(self, prompts: np.ndarray, lengths: np.ndarray):
        self.prompts = np.concatenate([self.prompts, prompts.astype(np.float64)])
        self.lengths = np.concatenate([self.lengths, lengths.astype(np.float64)])
        self.ages = np.concatenate([self.ages, np.zeros(len(prompts))])

    def snapshot(self, time_s: float, r: float) -> Snapshot:
        f = self.prompts + self.ages
        realized = {}
        for t in HORIZONS_S:
            grow = r * t
            alive = self.ages + grow < self.lengths
            realized[t] = float(((f + grow) * alive).sum())
        return Snapshot(
            time_s=time_s,
            ages=self.ages.copy(),
            footprints=f.copy(),
            prompts=self.prompts.copy(),
            lengths=self.lengths.copy(),
            occupancy=float(f.sum()),
            realized_t1=realized,
        )


def _arrival_rate(target_n: int, r: float, mean_len: float) -> float:
    # Little's law: N = lambda * E[T], E[T] = E[L]/r.
    return target_n * r / mean_len


def run_cell(regime: str, target_n: int, seed: int, params: dict,
             capacity: float | None = None) -> SimResult:
    """Simulate one grid cell. With capacity=None runs an uncapped pilot (no drops) used
    to size C = mean occupancy / target_utilization."""
    sim_p = params["sim"]
    dt = sim_p["dt_s"]
    r = params["decode_rate_tokens_per_s"]
    out_dist, cap = workloads.build_regime(regime, params)
    prompt_dist = workloads.build_prompt_dist(params)
    regime_key = int.from_bytes(regime.encode(), "big") % (2**31)  # process-stable, unlike hash()
    rng = np.random.default_rng(np.random.SeedSequence(
        [regime_key, target_n, seed, 7 if capacity is None else 11]))

    lam = _arrival_rate(target_n, r, out_dist.mean())
    pool = Pool()
    res = SimResult(capacity=capacity or float("inf"), rate_tokens_per_s=r, cap=cap,
                    training_lengths=np.empty(0), target_n=target_n)

    training: list[np.ndarray] = []
    training_count = 0
    tokens_per_step = r * dt
    drop_thresh = (capacity * sim_p["drop_above_capacity_fraction"]
                   if capacity is not None else float("inf"))

    # Phase boundaries in steps, resolved once warm-up completes.
    calib_steps = int(sim_p["calibration_window_s"] / dt)
    eval_steps = int(sim_p["eval_window_s"] / dt)
    warmup_max_steps = int(sim_p["warmup_max_s"] / dt)
    pilot_steps = int((200.0 + sim_p["calibration_window_s"]) / dt)
    snap_every = int(round(sim_p["snapshot_every_s"] / dt))

    warm_done_step = None
    active_counts = []
    occ_accum = []
    step = 0
    while True:
        t_now = step * dt
        # Arrivals (occupancy checked once per step; documented simplification).
        n_arr = rng.poisson(lam * dt)
        if n_arr > 0:
            if pool.occupancy() > drop_thresh:
                res.dropped += n_arr
            else:
                pool.admit(prompt_dist.sample(rng, n_arr), out_dist.sample(rng, n_arr))
                res.admitted += n_arr
        completed = pool.step(tokens_per_step)
        if warm_done_step is None and len(completed):
            training.append(completed)
            training_count += len(completed)

        if capacity is None:
            # Pilot: measure occupancy after a settling period.
            if step > pilot_steps // 2:
                occ_accum.append(pool.occupancy())
            if step >= pilot_steps:
                res.capacity = float(np.mean(occ_accum)) / sim_p["target_utilization"]
                return res
        else:
            if warm_done_step is None:
                if training_count >= sim_p["warmup_min_completions"] or step >= warmup_max_steps:
                    warm_done_step = step
                    res.training_lengths = np.concatenate(training) if training else np.empty(0)
            else:
                rel = step - warm_done_step
                if rel > calib_steps + eval_steps:
                    break
                if rel % snap_every == 0 and rel > 0:
                    snap = pool.snapshot(t_now, r)
                    (res.calib if rel <= calib_steps else res.eval).append(snap)
                if rel > calib_steps:
                    active_counts.append(len(pool.ages))
        step += 1

    res.mean_active = float(np.mean(active_counts)) if active_counts else 0.0
    return res


def little_gate(res: SimResult, tolerance: float = 0.10) -> tuple[bool, float]:
    dev = abs(res.mean_active - res.target_n) / res.target_n
    return dev <= tolerance, dev
