"""Fixed-step pool simulator with exact realized-T1 computation.

Leases decode at a fixed known rate r (phase-1 attribution choice: all estimator differences
flow through the survival curve, none through rate estimation). Because true lengths are drawn
at admission and rates are deterministic, the realized T1 target at any snapshot is computed
exactly from lease state: a lease with age n survives horizon t iff n + r*t < L_eff, and a
surviving lease's footprint at the horizon is f + r*t.

Warm-up rules (RESULTS-2.md harness amendments):
- "steady": training completions are collected only after `steady_burn_in_s`, so they are
  steady-state draws, unbiased in length (a completion of length L cannot be observed before
  t = L/r; the burn-in exceeds tail support / r for every regime).
- "legacy": collection starts at t = 0, so the training set is length-truncated while the
  pool fills; kept only for the round-1 attribution factorial.

The calibration window is either a fixed length or "adaptive40": max(min_window_s,
target_effective_samples * mean(training lengths) / r), rounded up to round_window_to_s,
resolved when warm-up completes and recorded on the result.

Drift: a DriftSchedule makes arrivals a time-varying two-component mix. Component identity
is tracked per lease so the per-lease-true oracle stays exact; arrival rate follows
lambda(t) = N * r / E[L](t), holding the target pool size constant so length-mix drift is
isolated from load drift.

Load-coupled decode rates are deliberately out of scope; that realism belongs to an external
discrete-event simulator (BLIS) in a later phase.
"""

from __future__ import annotations

import math
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
    comp: np.ndarray | None = None  # drift component id per lease (0 = a, 1 = b)


@dataclass
class SimResult:
    capacity: float
    rate_tokens_per_s: float
    cap: int | None
    training_lengths: np.ndarray  # completed L_eff during warm-up collection (uncensored)
    calib: list = field(default_factory=list)  # snapshots in calibration window
    eval: list = field(default_factory=list)   # snapshots in evaluation window
    mean_active: float = 0.0
    target_n: int = 0
    dropped: int = 0
    admitted: int = 0
    calib_window_s: float = 0.0   # resolved calibration window length
    eval_start_s: float = 0.0     # absolute sim time of eval-window start


class Pool:
    """Active-lease state as growable parallel arrays."""

    def __init__(self):
        self.prompts = np.empty(0, dtype=np.float64)
        self.lengths = np.empty(0, dtype=np.float64)
        self.ages = np.empty(0, dtype=np.float64)
        self.comp = np.empty(0, dtype=np.int8)

    def occupancy(self) -> float:
        return float(self.prompts.sum() + self.ages.sum())

    def step(self, tokens: float) -> np.ndarray:
        """Advance decode by `tokens`; returns completed lengths removed from the pool."""
        self.ages += tokens
        done = self.ages >= self.lengths
        completed = self.lengths[done]
        keep = ~done
        self.prompts, self.lengths, self.ages, self.comp = (
            self.prompts[keep], self.lengths[keep], self.ages[keep], self.comp[keep])
        return completed

    def admit(self, prompts: np.ndarray, lengths: np.ndarray, comp: np.ndarray | None = None):
        self.prompts = np.concatenate([self.prompts, prompts.astype(np.float64)])
        self.lengths = np.concatenate([self.lengths, lengths.astype(np.float64)])
        self.ages = np.concatenate([self.ages, np.zeros(len(prompts))])
        if comp is None:
            comp = np.zeros(len(prompts), dtype=np.int8)
        self.comp = np.concatenate([self.comp, comp.astype(np.int8)])

    def snapshot(self, time_s: float, r: float, with_comp: bool = False) -> Snapshot:
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
            comp=self.comp.copy() if with_comp else None,
        )


def _arrival_rate(target_n: int, r: float, mean_len: float) -> float:
    # Little's law: N = lambda * E[T], E[T] = E[L]/r.
    return target_n * r / mean_len


def resolve_calib_window(spec, training_lengths: np.ndarray, r: float, params: dict) -> float:
    """spec: a number of seconds, or "adaptive40" (RESULTS-2.md), or None for the params
    default."""
    if spec is None:
        return float(params["sim"]["calibration_window_s"])
    if spec == "adaptive40":
        c = params["conformal"]
        w = c["target_effective_samples"] * float(training_lengths.mean()) / r
        step = c["round_window_to_s"]
        return max(float(c["min_window_s"]), math.ceil(w / step) * step)
    return float(spec)


def run_cell(regime: str, target_n: int, seed: int, params: dict,
             capacity: float | None = None, warmup_rule: str = "steady",
             calib_window_s=None, warmup_min_completions: int | None = None,
             out_dist_override=None, drift: workloads.DriftSchedule | None = None,
             eval_window_s: float | None = None) -> SimResult:
    """Simulate one grid cell. With capacity=None runs an uncapped pilot (no drops) used
    to size C = mean occupancy / target_utilization. `regime` still names the prompt
    parameters and rng stream when out_dist_override or drift is supplied."""
    sim_p = params["sim"]
    dt = sim_p["dt_s"]
    r = params["decode_rate_tokens_per_s"]
    if drift is not None:
        out_dist, cap = None, None
        mean_a, mean_b = drift.a.mean(), drift.b.mean()
    else:
        if out_dist_override is not None:
            out_dist, cap = out_dist_override, getattr(out_dist_override, "cap", None)
        else:
            out_dist, cap = workloads.build_regime(regime, params)
        mean_a = mean_b = out_dist.mean()
    prompt_dist = workloads.build_prompt_dist(params)
    regime_key = int.from_bytes(regime.encode(), "big") % (2**31)  # process-stable, unlike hash()
    rng = np.random.default_rng(np.random.SeedSequence(
        [regime_key, target_n, seed, 7 if capacity is None else 11]))

    pool = Pool()
    res = SimResult(capacity=capacity or float("inf"), rate_tokens_per_s=r, cap=cap,
                    training_lengths=np.empty(0), target_n=target_n)

    training: list[np.ndarray] = []
    training_count = 0
    min_completions = (warmup_min_completions if warmup_min_completions is not None
                       else sim_p["warmup_min_completions"])
    tokens_per_step = r * dt
    drop_thresh = (capacity * sim_p["drop_above_capacity_fraction"]
                   if capacity is not None else float("inf"))

    eval_win = eval_window_s if eval_window_s is not None else sim_p["eval_window_s"]
    eval_steps = int(eval_win / dt)
    warmup_max_steps = int(sim_p["warmup_max_s"] / dt)
    burn_in_steps = int(sim_p["steady_burn_in_s"] / dt) if warmup_rule == "steady" else 0
    pilot_steps = int((200.0 + sim_p["calibration_window_s"]) / dt)
    snap_every = int(round(sim_p["snapshot_every_s"] / dt))

    warm_done_step = None
    calib_steps = None       # resolved when warm-up completes (adaptive window)
    eval_start_step = None
    active_counts = []
    occ_accum = []
    step = 0
    while True:
        t_now = step * dt
        rel_eval = (t_now - eval_start_step * dt) if eval_start_step is not None else -1.0
        # Arrivals (occupancy checked once per step; documented simplification).
        if drift is not None:
            lam = _arrival_rate(target_n, r, drift.mean_len(rel_eval, mean_a, mean_b))
        else:
            lam = _arrival_rate(target_n, r, mean_a)
        n_arr = rng.poisson(lam * dt)
        if n_arr > 0:
            if pool.occupancy() > drop_thresh:
                res.dropped += n_arr
            else:
                # Prompts drawn before lengths: preserves the round-1 rng stream order,
                # so legacy-flag runs reproduce the round-1 record bit-exactly.
                prompts = prompt_dist.sample(rng, n_arr)
                if drift is not None:
                    from_a = rng.random(n_arr) < drift.weight(rel_eval)
                    lengths = np.empty(n_arr, dtype=np.int64)
                    na = int(from_a.sum())
                    if na:
                        lengths[from_a] = drift.a.sample(rng, na)
                    if n_arr - na:
                        lengths[~from_a] = drift.b.sample(rng, n_arr - na)
                    comp = (~from_a).astype(np.int8)
                else:
                    lengths, comp = out_dist.sample(rng, n_arr), None
                pool.admit(prompts, lengths, comp)
                res.admitted += n_arr
        completed = pool.step(tokens_per_step)
        if warm_done_step is None and len(completed) and step >= burn_in_steps:
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
                if training_count >= min_completions or step >= warmup_max_steps:
                    warm_done_step = step
                    res.training_lengths = np.concatenate(training) if training else np.empty(0)
                    res.calib_window_s = resolve_calib_window(
                        calib_window_s, res.training_lengths, r, params)
                    calib_steps = int(res.calib_window_s / dt)
                    eval_start_step = warm_done_step + calib_steps
                    res.eval_start_s = eval_start_step * dt
            else:
                rel = step - warm_done_step
                if rel > calib_steps + eval_steps:
                    break
                if rel % snap_every == 0 and rel > 0:
                    snap = pool.snapshot(t_now, r, with_comp=drift is not None)
                    (res.calib if rel <= calib_steps else res.eval).append(snap)
                if rel > calib_steps:
                    active_counts.append(len(pool.ages))
        step += 1

    res.mean_active = float(np.mean(active_counts)) if active_counts else 0.0
    return res


def little_gate(res: SimResult, tolerance: float = 0.10) -> tuple[bool, float]:
    dev = abs(res.mean_active - res.target_n) / res.target_n
    return dev <= tolerance, dev
