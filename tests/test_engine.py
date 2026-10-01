"""Block-accounting invariants, checked after every engine step of short runs.

python -m unittest discover -s tests
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from admsim import calibration  # noqa: E402
from admsim.config import variant  # noqa: E402
from admsim.engine import ENGINES, VLLMReuseEndpoint, blocks  # noqa: E402
from admsim.policies import make_policy  # noqa: E402
from admsim.simulation import simulate  # noqa: E402


def checked(base):
    class Checked(base):
        def finish(self, now, completed):
            out = super().finish(now, completed)
            assert 0 <= self.used <= self.cfg.K, self.used
            assert self.used == sum(blocks(r.tokens()) for r in self.running), "leaked blocks"
            if isinstance(self, VLLMReuseEndpoint):
                assert sum(n for _, n in self.free_queue) == self.cfg.K - self.used, "free queue out of sync"
            return out

    return Checked


class BlockAccounting(unittest.TestCase):
    def run_cell(self, engine, policy, kf):
        cfg, workload = variant("bimodal", kf)
        capacity = calibration.load("bimodal", kf, 4)["capacity"]
        engine_cls = checked(ENGINES[engine])
        run = simulate(workload, cfg, engine_cls, make_policy(policy, cfg, 1), 4, 600, 1, rate=1.2 * capacity)
        self.assertGreater(sum(ep.preempts for ep in run.endpoints), 0, "the run should preempt")

    def test_recompute(self):
        for policy in ("nogate", "fit", "lookahead:22.31"):
            self.run_cell("recompute", policy, 0.5)

    def test_vllmreuse(self):
        for policy in ("nogate", "fit", "lookahead:22.31"):
            self.run_cell("vllmreuse", policy, 0.5)


if __name__ == "__main__":
    unittest.main()
