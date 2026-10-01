"""The discrete-event loop: Poisson arrivals, the router, and the endpoints' steps.

Events are arrivals, step completions, and requests reaching an engine `lag` after the routing
decision. After every event the router releases what it can and every idle endpoint starts a step.
The run ends when n_req requests have completed. The first 20% of completions are warm-up: metrics
count tokens and requests after the warm-up completion.
"""

import heapq

import numpy as np

from .engine import ENGINES, Request
from .router import Router


class Run:
    """The state a finished simulation leaves for metrics."""

    def __init__(self, endpoints, requests, completed, tokens, t_warm, tok_warm, T):
        self.endpoints, self.requests, self.completed = endpoints, requests, completed
        self.tokens, self.tok_warm, self.T = tokens, tok_warm, T
        self.t0 = t_warm if t_warm is not None else 0.0


class Simulation:
    """One run's state and event handlers.

    The heap holds (time, seq, handler, arg); seq is unique, so ties in time resolve in scheduling
    order and handlers are never compared. A new kind of event is a new handler passed to push()."""

    def __init__(self, workload, cfg, engine, policy, E, n_req, seed, rate, placement, pending):
        self.workload, self.cfg, self.policy, self.n_req, self.rate = workload, cfg, policy, n_req, rate
        self.rng = np.random.default_rng(seed)
        engine_cls = ENGINES[engine] if isinstance(engine, str) else engine
        self.eps = [engine_cls(cfg, i, seed) for i in range(E)]
        self.router = Router(self.eps, policy, placement, pending)
        self.heap = []
        self.seq = 0
        self.now = 0.0
        self.requests, self.completed = [], []
        self.tokens, self.t_warm, self.tok_warm = 0, None, 0
        self.warm_n = int(0.2 * n_req)

    def push(self, t, handler, arg=None):
        heapq.heappush(self.heap, (t, self.seq, handler, arg))
        self.seq += 1

    def new_request(self, arrived):
        r = Request(len(self.requests) + 1, *self.workload.draw(self.rng), arrived)
        self.requests.append(r)
        return r

    def dispatch(self, e, r):
        """Route r to endpoint e: it reaches the engine's queue after the lag."""
        if self.cfg.lag > 0:
            r.at_engine = self.now + self.cfg.lag
            self.push(r.at_engine, self.on_reach_engine, e)
        self.eps[e].waiting.append(r)
        self.kick(e)

    def kick(self, e):
        """Start a step on endpoint e if it is idle and has work."""
        ep = self.eps[e]
        if ep.busy_until is None:
            ep.now = self.now
            d = ep.schedule()
            if d is not None:
                ep.busy_until = self.now + d
                self.push(ep.busy_until, self.on_step_end, e)

    def on_arrival(self, _):
        r = self.new_request(self.now)
        if self.policy.gated:
            self.router.queue.append(r)
        else:
            self.dispatch(self.router.shortest_queue(), r)
        self.push(self.now + self.rng.exponential(1.0 / self.rate), self.on_arrival)

    def on_reach_engine(self, e):
        self.kick(e)

    def on_step_end(self, e):
        ep = self.eps[e]
        comp = []
        self.tokens += ep.finish(self.now, comp)
        ep.busy_until = None
        for r in comp:
            self.completed.append(r)
            if len(self.completed) == self.warm_n:
                self.t_warm, self.tok_warm = self.now, self.tokens
        if comp:
            self.policy.observe(comp, len(self.completed), self.eps)
        self.kick(e)

    def preload(self, n):
        """Queue n requests at time 0. Without a gate they start in the engines' queues, with no lag."""
        for _ in range(n):
            r = self.new_request(0.0)
            if self.policy.gated:
                self.router.queue.append(r)
            else:
                self.eps[self.router.shortest_queue()].waiting.append(r)

    def run(self):
        self.router.release(self.dispatch)
        for i in range(len(self.eps)):
            self.kick(i)
        while self.heap and len(self.completed) < self.n_req:
            self.now, _, handler, arg = heapq.heappop(self.heap)
            handler(arg)
            self.router.release(self.dispatch)
            for i in range(len(self.eps)):
                self.kick(i)
        return Run(self.eps, self.requests, self.completed, self.tokens, self.t_warm, self.tok_warm, self.now)


def simulate(
    workload,
    cfg,
    engine,
    policy,
    E,
    n_req,
    seed,
    rate=None,
    saturate=False,
    placement="queue",
    pending="prompt",
):
    """Run one simulation and return its Run.

    engine: an Endpoint subclass or a key of engine.ENGINES. policy: a policies.Policy. rate:
    Poisson arrival rate (requests per time unit). saturate: instead of arrivals, start with
    1.3 n_req requests queued. placement, pending: router options (router.Router)."""
    sim = Simulation(workload, cfg, engine, policy, E, n_req, seed, rate, placement, pending)
    if saturate:
        sim.preload(int(n_req * 1.3))
    else:
        sim.push(sim.rng.exponential(1.0 / rate), sim.on_arrival)
    return sim.run()
