"""Admission policies: what the router checks before sending the head request to an endpoint.

Every gated policy applies the release condition and the fit check F (see router.py). A policy may
add a test on top; the router calls it only when F passes and the endpoint is not idle.

  nogate           no router queue: each arrival goes straight to the shortest engine queue
  fit              F alone
  lookahead:<pct>  F, then admit if the ceiling rule passes or if P1 <= pct%, where P1 is the
                   probability that no resident finishes before the endpoint's free memory runs out

The ceiling rule admits when every resident and the candidate fit at their client ceilings, so no
preemption is possible. The look-ahead policies learn output-length survival from completions and
refuse until the first curves exist.
"""

import math

import numpy as np

from .config import BLOCK
from .engine import blocks
from .survival import SurvivalBand

REFRESH_EVERY = 25  # completions between survival refreshes, after the first 50


class Policy:
    """Base class. gated: requests wait in the router queue (False for nogate). cache_refusals: a
    refusal from admits() stands until the endpoint's next completion or preemption (see router.py);
    set it False for a policy whose answer can change in between, such as one that watches time or
    transfers in flight."""

    gated = True
    cache_refusals = True

    def admits(self, ep, r):
        """The test on top of F: True or False, or None for no test (nothing is cached)."""
        return None

    def observe(self, completed, n_done, endpoints):
        """Called after each engine step that completes requests."""


class Fit(Policy):
    """`fit`: the fit check alone."""


class NoGate(Policy):
    """`nogate`: no router queue."""

    gated = False


def ceiling_fits(ep, r):
    """Every resident, queued request and r fit in KV at their client ceilings."""
    tot = sum(blocks(x.prompt + x.ceiling) for x in ep.running)
    tot += sum(blocks(x.prompt + x.ceiling) for x in ep.waiting)
    return tot + blocks(r.prompt + r.ceiling) <= ep.cfg.K


class Growth:
    """The requests that grow on ep if r is admitted, and how far they can grow.

    growers: the running requests, then the unstarted waiting ones; r comes last in the arrays.
    n_u: their count including r. h: free KV in tokens after r's prompt and one block of slack per
    grower, where free blocks also charge every queued prompt and the unprefilled remainder of
    running requests. Every grower decodes one token per step, so memory runs out once each has
    generated t0 = h / n_u more tokens. a: each grower's output so far (0 for r). ceiling_left: the
    tokens each may still generate before its ceiling."""

    def __init__(self, r, growers, unstarted, h):
        self.growers, self.unstarted, self.h = growers, unstarted, h
        self.n_u = len(growers) + 1
        self.a = np.asarray([x.gen for x in growers] + [0], float)
        self.ceiling_left = np.asarray([max(x.ceiling - x.gen, 1) for x in growers] + [r.ceiling], float)


def growth(ep, r):
    """Growth for admitting r on ep, or None if r does not fit now."""
    unstarted = [w for w in ep.waiting if not w.started]
    unprefilled = sum(
        max(blocks(x.pf_target) - blocks(x.pf_done), 0) for x in ep.running if x.pf_done < x.pf_target
    )
    growers = list(ep.running) + unstarted
    n_u = len(growers) + 1
    free_blocks = ep.cfg.K - ep.used - sum(blocks(w.pf_target) for w in ep.waiting) - unprefilled
    h = BLOCK * (free_blocks - ep.increment(r) - n_u)
    if h < 0:
        return None
    return Growth(r, growers, unstarted, h)


class _SurvivalPolicy(Policy):
    """A look-ahead: admit if the ceiling test passes; otherwise, once survival curves exist and r
    fits now, let the subclass decide from the growth it would join."""

    def __init__(self, rng, context):
        self.band = SurvivalBand(rng, context)

    def observe(self, completed, n_done, endpoints):
        for r in completed:
            self.band.observe(r.output, r.ceiling)
        if completed and (len(self.band.completed) <= 50 or n_done % REFRESH_EVERY == 0):
            self.band.refresh([x.gen for ep in endpoints for x in ep.running])

    def admits(self, ep, r):
        if ceiling_fits(ep, r):
            return True
        if not self.band.ready:
            return False
        g = growth(ep, r)
        if g is None:
            return False
        return self._decide(ep, r, g)

    def _decide(self, ep, r, g):
        raise NotImplementedError


class LookAhead(_SurvivalPolicy):
    """`lookahead:<pct>`: admit if P1 <= pct%. P1 = prod_i S(a_i + t0) / S(a_i) over the growers and r,
    with S the band's mean curve, is the probability that none finishes before memory runs out. A
    grower whose ceiling ends before t0 is certain to finish in time and contributes a factor 0."""

    def __init__(self, rng, context, pct):
        super().__init__(rng, context)
        self.threshold = pct / 100.0

    def _decide(self, ep, r, g):
        a, ceiling_left = g.a, g.ceiling_left
        t0 = g.h / g.n_u
        mean = self.band.cond_mean(a, a[None, :] + t0)
        alive = (t0 < ceiling_left)[None, :]
        p1 = math.exp(float(np.log(np.maximum(mean * alive, 1e-300)).sum()))
        return p1 <= self.threshold


def make_policy(spec, cfg, seed):
    """Build a policy from its spec string (see the module docstring)."""
    name, _, arg = spec.partition(":")
    rng = np.random.default_rng(seed + 7)
    if name == "nogate":
        return NoGate()
    if name == "fit":
        return Fit()
    if name == "lookahead":
        return LookAhead(rng, cfg.context, float(arg))
    raise ValueError(f"unknown policy {spec!r}")
