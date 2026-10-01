"""Contracts of the building blocks, on hand-built states.

python -m unittest discover -s tests
"""

import collections
import os
import sys
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from admsim.config import EngineConfig  # noqa: E402
from admsim.engine import Endpoint, Request, VLLMReuseEndpoint, blocks  # noqa: E402
from admsim.policies import Policy  # noqa: E402
from admsim.router import Router  # noqa: E402
from admsim.survival import kaplan_meier  # noqa: E402


def cfg(K=100, budget=2048):
    return EngineConfig(K=K, c=0.0, noise=0.0, budget=budget)


def run_step(ep, completed=None):
    """One engine step at time 0; returns the requests it completed."""
    completed = [] if completed is None else completed
    assert ep.schedule() is not None
    ep.finish(0.0, completed)
    return completed


class Blocks(unittest.TestCase):
    def test_holds_a_slot_for_the_next_token(self):
        self.assertEqual(blocks(0), 0)
        self.assertEqual(blocks(1), 1)
        self.assertEqual(blocks(15), 1)
        self.assertEqual(blocks(16), 2)  # 16 tokens plus the next token's slot
        self.assertEqual(blocks(31), 2)
        self.assertEqual(blocks(-5), 0)


class KaplanMeier(unittest.TestCase):
    def test_censored_middle_observation(self):
        # events at 1 and 3, censored at 2: S drops by 1/3 at t = 1, then to 0 at t = 3
        t, s = kaplan_meier(np.array([1.0, 2.0, 3.0]), np.array([True, False, True]), np.ones(3) / 3)
        np.testing.assert_array_equal(t, [1.0, 3.0])
        np.testing.assert_allclose(s, [2 / 3, 0.0])


class ReuseFreeQueue(unittest.TestCase):
    def test_allocation_eats_victim_blocks_from_the_head(self):
        ep = VLLMReuseEndpoint(cfg(K=9), 0, 0)
        ep.free_queue = collections.deque([[None, 1], [7, 3], [None, 5]])
        ep._allocate(2)
        self.assertEqual(list(ep.free_queue), [[7, 2], [None, 5]])
        self.assertEqual(ep.reuse_blocks_lost, 1)

    def test_preempted_victim_readmits_with_its_cached_prefix(self):
        ep = VLLMReuseEndpoint(cfg(K=9), 0, 0)
        a, b = Request(1, 62, 2, 100, 0.0), Request(2, 62, 10, 100, 0.0)
        ep.waiting.extend([a, b])
        run_step(ep)  # both admitted: 4 blocks each for 62 prompt tokens plus the first token
        self.assertEqual(ep.used, 8)
        done = run_step(ep)  # a's decode takes the last block; b, the newest, preempts itself
        self.assertEqual(done, [a])  # a then finishes and frees its 5 blocks to the tail
        self.assertEqual(ep.preempts, 1)
        # b's 3 full blocks sit behind its partial block, and a's freed blocks queue behind b
        self.assertEqual(list(ep.free_queue), [[None, 1], [2, 3], [None, 5]])
        run_step(ep)  # b readmits and skips its 48 cached tokens
        self.assertEqual((ep.self_hits, ep.self_hit_tokens), (1, 48))
        self.assertEqual(list(ep.free_queue), [[None, 4]])
        self.assertEqual(ep.used, 5)


class SpyPolicy(Policy):
    def __init__(self, answer, cache_refusals=True):
        self.answer, self.calls, self.cache_refusals = answer, 0, cache_refusals

    def admits(self, ep, r):
        self.calls += 1
        return self.answer


class RouterRules(unittest.TestCase):
    def router(self, policy, K=100, budget=2048):
        ep = Endpoint(cfg(K, budget), 0, 0)
        return Router([ep], policy), ep

    def released(self, router, r):
        sent = []
        router.queue.append(r)
        router.release(lambda e, x: sent.append((e, x)))
        router.queue.clear()
        return sent

    def busy(self, ep, used=10):
        """Give ep one running request that has started, so it is not idle."""
        x = Request(99, 100, 50, 100, 0.0)
        x.started, x.pf_done = True, 100
        ep.running.append(x)
        ep.used = used

    def test_idle_endpoint_skips_the_policy(self):
        policy = SpyPolicy(False)
        router, _ = self.router(policy)
        self.assertEqual(len(self.released(router, Request(1, 100, 10, 100, 0.0))), 1)
        self.assertEqual(policy.calls, 0)

    def test_waiting_preempted_request_blocks_release(self):
        router, ep = self.router(SpyPolicy(True))
        victim = Request(2, 100, 10, 100, 0.0)
        victim.started = True
        ep.waiting.append(victim)
        self.assertEqual(self.released(router, Request(1, 10, 10, 100, 0.0)), [])

    def test_pending_prefill_must_fit_one_step_budget(self):
        router, ep = self.router(SpyPolicy(True), budget=1000)
        self.busy(ep)
        ep.waiting.append(Request(2, 900, 10, 100, 0.0))
        self.assertEqual(self.released(router, Request(1, 200, 10, 100, 0.0)), [])
        self.assertEqual(len(self.released(router, Request(3, 100, 10, 100, 0.0))), 1)

    def test_fit_check_counts_queued_prompts(self):
        router, ep = self.router(SpyPolicy(True), K=19)
        self.busy(ep, used=10)
        ep.waiting.append(Request(2, 16 * 4, 10, 100, 0.0))  # claims 5 blocks; the head needs 5 more
        self.assertEqual(self.released(router, Request(1, 16 * 4, 10, 100, 0.0)), [])
        self.assertEqual(len(self.released(router, Request(3, 16 * 3, 10, 100, 0.0))), 1)

    def test_refusal_is_cached_until_the_endpoint_changes(self):
        policy = SpyPolicy(False)
        router, ep = self.router(policy)
        self.busy(ep)
        r = Request(1, 10, 10, 100, 0.0)
        self.released(router, r)
        self.released(router, r)
        self.assertEqual(policy.calls, 1)
        ep.version += 1  # a completion or preemption
        self.released(router, r)
        self.assertEqual(policy.calls, 2)

    def test_uncached_policy_is_asked_every_time(self):
        policy = SpyPolicy(False, cache_refusals=False)
        router, ep = self.router(policy)
        self.busy(ep)
        r = Request(1, 10, 10, 100, 0.0)
        self.released(router, r)
        self.released(router, r)
        self.assertEqual(policy.calls, 2)


if __name__ == "__main__":
    unittest.main()
