"""The router: one FIFO queue in front of the endpoints, released head first (late binding).

On every event the router tries to place the head request. It visits endpoints from least to most
loaded and sends the head to the first eligible one; if none is eligible, the head and everything
behind it wait. An endpoint is eligible when:

  1. release condition: no request that has already emitted tokens (a preempted victim) waits in
     its queue, and the prefill of its requests that have not emitted a token yet, plus the head's
     prompt, fits in one step's token budget (or nothing is pending);
  2. idle endpoint: the head's prompt fits in KV, and nothing else is checked;
  3. fit check F: blocks committed (allocated plus queued) plus the head's prompt fit in KV;
  4. the policy's own test, if any.

A policy refusal is cached per (request, endpoint) until the endpoint's next completion or
preemption (its `version`), so the policy is not asked again while nothing has left the endpoint.
Policies with cache_refusals = False are asked every time.

With the `nogate` policy there is no router queue: each arrival goes to the endpoint with the
fewest requests (waiting plus running).
"""

import collections

from .engine import blocks


class Router:
    """placement: "queue" orders endpoints by requests held, "kv" by blocks allocated.
    pending: "prompt" counts each unstarted request's whole prompt in the release condition,
    "remaining" only its unprefilled part."""

    def __init__(self, endpoints, policy, placement="queue", pending="prompt"):
        self.endpoints, self.policy = endpoints, policy
        self.placement, self.pending = placement, pending
        self.queue = collections.deque()
        self.refused = {}  # (uid, endpoint) -> endpoint version at refusal

    def shortest_queue(self):
        eps = self.endpoints
        return min(range(len(eps)), key=lambda x: len(eps[x].waiting) + len(eps[x].running))

    def release(self, send):
        """Place head requests while one fits; send(endpoint index, request) dispatches each."""
        while self.queue:
            r = self.queue[0]
            e = self._place(r)
            if e is None:
                return
            self.queue.popleft()
            send(e, r)

    def _load(self, ep):
        if self.placement == "kv":
            return ep.used
        return len(ep.waiting) + len(ep.running)

    def _place(self, r):
        for ep in sorted(self.endpoints, key=self._load):
            ok, cacheable = self._eligible(ep, r)
            if ok:
                return ep.idx
            if cacheable:
                self.refused[(r.uid, ep.idx)] = ep.version
        return None

    def _release_ok(self, ep, r):
        if any(w.started for w in ep.waiting):
            return False
        if self.pending == "remaining":
            pend = sum(w.prompt for w in ep.waiting) + sum(
                x.pf_target - x.pf_done for x in ep.running if not x.started
            )
        else:
            pend = sum(w.pf_target for w in ep.waiting) + sum(
                x.pf_target for x in ep.running if not x.started
            )
        return pend == 0 or pend + r.prompt <= ep.cfg.budget

    def _eligible(self, ep, r):
        """(eligible, whether a refusal may be cached)."""
        if not self._release_ok(ep, r):
            return False, False
        if not ep.running and not ep.waiting:
            return blocks(r.prompt) <= ep.cfg.K, True
        if ep.committed() + ep.increment(r) > ep.cfg.K:
            return False, False
        if self.refused.get((r.uid, ep.idx)) == ep.version:
            return False, True
        ok = self.policy.admits(ep, r)
        return (True, False) if ok is None else (ok, self.policy.cache_refusals)
