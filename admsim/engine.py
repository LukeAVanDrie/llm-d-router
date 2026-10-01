"""A step-level model of one vLLM v1 engine: continuous batching, chunked prefill, paged KV blocks,
and newest-first preemption.

Each step schedules running requests first, oldest first, one token per decode and up to the token
budget per prefill chunk. When blocks run out, the newest running request is preempted: its blocks
are freed and it returns to the head of the waiting queue to prefill its prompt and generated tokens
again. If no preemption happened in a step, waiting requests are admitted in FIFO order while their
whole prefill fits in free blocks.

Endpoint recomputes a victim's context from scratch. VLLMReuseEndpoint lets a readmitted victim hit
its own cached blocks where they survive in vLLM's free-block queue.
"""

import collections

import numpy as np

from .config import BLOCK


def blocks(tokens):
    """Blocks held by a request with this many tokens: ceil((tokens + 1) / BLOCK), the extra slot
    being the next token's."""
    tokens = int(tokens)
    return 0 if tokens <= 0 else (tokens + BLOCK) // BLOCK


class Request:
    """Lengths in tokens (output: the length the request will generate; ceiling: its max_tokens).
    Times in sim units."""

    __slots__ = (
        "uid",
        "prompt",
        "output",
        "ceiling",
        "arrived",
        "at_engine",
        "first_token",
        "finished",
        "started",
        "gen",
        "pf_done",
        "pf_target",
        "rank",
    )

    def __init__(self, uid, prompt, output, ceiling, arrived):
        self.uid, self.prompt, self.output, self.ceiling = uid, prompt, output, ceiling
        self.arrived = arrived  # arrival time at the router
        self.at_engine = 0.0  # time the request reaches the engine's waiting queue
        self.first_token = self.finished = None  # first-token and completion times
        self.started = False  # has emitted a token
        self.gen = 0  # output tokens generated
        self.pf_done = 0  # tokens prefilled since the last (re)admission
        self.pf_target = prompt  # tokens to prefill: the prompt, or prompt plus output after preemption
        self.rank = 0  # admission order on its endpoint; the newest is preempted first

    def tokens(self):
        """Tokens resident in KV."""
        return self.pf_done if self.pf_done < self.pf_target else self.prompt + self.gen


class Endpoint:
    """One engine that recomputes a preempted request's context from scratch.

    Contract with the rest of the simulator. The event loop sets `now` and `busy_until`, appends
    routed requests to `waiting`, and calls schedule() when the endpoint is idle and finish() when
    its step ends. The router and policies read `running`, `waiting`, `used`, `version`, `admissions`,
    increment() and committed(). metrics sums counters() over endpoints.

    Subclasses change behavior through the hooks below; the reuse engine also wraps
    _preempt_newest() and finish() and infers freed blocks from changes in `used`."""

    def __init__(self, cfg, idx, seed):
        self.cfg, self.idx = cfg, idx
        self.running: list[Request] = []
        self.waiting: collections.deque[Request] = collections.deque()
        self.used = 0  # blocks allocated
        self.now = 0.0
        self.busy_until = None  # end of the step in flight
        self.plan = None  # [(request, tokens, is_prefill)] for the step in flight
        self.planned = {}  # uid -> blocks allocated for the step in flight
        self.admissions = 0  # admissions so far; stamps each request's rank
        self.version = 0  # bumps on every completion and preemption; keys the router's refusal cache
        self.preempts = 0
        self.prefill_demand = 0  # tokens to prefill over all admissions
        self.prefill_saved = 0  # of which served from cache
        self.noise_rng = np.random.default_rng((seed, idx, 11)) if cfg.noise > 0 else None

    def increment(self, r):
        """Blocks admitting r's prompt and first token would add."""
        return blocks(r.prompt + 1)

    def committed(self):
        """Blocks allocated plus those the waiting queue will claim at admission."""
        return self.used + sum(blocks(w.pf_target) for w in self.waiting)

    def counters(self):
        """Event counts reported in each run's result row, summed over endpoints."""
        return {
            "preempts": self.preempts,
            "prefill_demand": self.prefill_demand,
            "prefill_saved": self.prefill_saved,
        }

    # Hooks for engines with cache reuse or other costs.

    def _step_duration(self, n_dec, S, pf_tok):
        """Duration of a step with n_dec decoding requests, S tokens resident and pf_tok prefill tokens."""
        cfg = self.cfg
        d = cfg.a + cfg.b * n_dec + cfg.c * S + cfg.b_pf * pf_tok
        if self.noise_rng is not None:
            s = cfg.noise
            d *= float(np.exp(s * self.noise_rng.standard_normal() - 0.5 * s * s))
        return d

    def _allocate(self, n, reused=0):
        """Allocate n blocks, of which `reused` are the request's own cached blocks."""
        self.used += n

    def _readmit_hit(self, r):
        """(cached tokens, cached blocks) r can skip prefilling at admission."""
        return 0, 0

    def _preempt_newest(self):
        v = self.running.pop()
        self.used -= blocks(v.tokens()) + self.planned.pop(v.uid, 0)
        v.pf_target, v.pf_done = v.prompt + v.gen, 0
        self.preempts += 1
        self.version += 1
        self.waiting.appendleft(v)
        return v

    def schedule(self):
        """Plan the next step and return its duration, or None if there is nothing to run."""
        self.planned = {}
        plan, budget, preempted = self._plan_running(self.cfg.budget)
        if not preempted:
            plan += self._admit_waiting(budget)
        if not plan:
            self.plan = None
            return None
        n_dec = sum(1 for _, _, pf in plan if not pf)
        S = sum(r.tokens() for r in self.running)
        pf_tok = sum(t for _, t, pf in plan if pf)
        self.plan = plan
        return self._step_duration(n_dec, S, pf_tok)

    def _plan_running(self, budget):
        """Give each running request, oldest first, its next decode token or prefill chunk, preempting
        the newest when blocks run out. Returns (plan, budget left, whether anything was preempted)."""
        plan = []
        preempted = False
        i = 0
        while i < len(self.running) and budget > 0:
            r = self.running[i]
            prefill = r.pf_done < r.pf_target
            n_tok = min(r.pf_target - r.pf_done, budget) if prefill else 1
            cur = r.tokens()
            extra = 1 if prefill and r.pf_done + n_tok >= r.pf_target else 0  # first-token slot
            need = blocks(cur + n_tok + extra) - blocks(cur)
            self_victim = False
            while self.used + need > self.cfg.K:
                v = self._preempt_newest()
                preempted = True
                if v is r:
                    self_victim = True
                    break
            if self_victim:
                continue
            self._allocate(need)
            self.planned[r.uid] = need
            plan.append((r, n_tok, prefill))
            budget -= n_tok
            i += 1
        return plan, budget, preempted

    def _admit_waiting(self, budget):
        """Admit waiting requests in FIFO order while the head has reached the engine, its whole
        prefill fits in free blocks, and budget and sequence slots remain. Returns their plan entries."""
        cfg = self.cfg
        plan = []
        while self.waiting and budget > 0 and len(self.running) < cfg.max_seqs:
            r = self.waiting[0]
            if r.at_engine > self.now:
                break
            if blocks(r.pf_target + 1) + self.used > cfg.K:
                break
            self.waiting.popleft()
            hit, reused = self._readmit_hit(r)
            self.prefill_demand += r.pf_target
            self.prefill_saved += hit
            r.pf_done = hit
            n_tok = min(r.pf_target - hit, budget)
            need = blocks(hit + n_tok + (1 if hit + n_tok >= r.pf_target else 0))
            self._allocate(need, reused)
            self.planned[r.uid] = need
            self.admissions += 1
            r.rank = self.admissions
            self.running.append(r)
            plan.append((r, n_tok, True))
            budget -= n_tok
        return plan

    def finish(self, now, completed):
        """Apply the step in flight at time now. Appends finished requests to completed and returns
        the number of tokens emitted."""
        emitted = 0
        for r, n_tok, prefill in self.plan:
            if prefill:
                r.pf_done += n_tok
                if r.pf_done < r.pf_target:
                    continue
            r.gen += 1
            emitted += 1
            if not r.started:
                r.first_token = now
                r.started = True
            if r.gen >= r.output:
                self.running.remove(r)
                self.used -= blocks(r.tokens())
                r.finished = now
                completed.append(r)
                self.version += 1
        self.plan, self.planned = None, {}
        return emitted


class VLLMReuseEndpoint(Endpoint):
    """Preempted victims keep their cached blocks until new allocations reach them, as in vLLM v1.

    vLLM v1 (block_pool.free_blocks, kv_cache_utils.FreeKVCacheBlockQueue) returns a preempted
    request's blocks in reverse order to the tail of one free queue, with its partial block to the
    head, and allocates from the head. A readmitted request hits only the contiguous cached prefix
    from its first block. So a victim's cached prefix shrinks with every block allocated after its
    preemption, while blocks freed later by completions queue behind it.

    The free queue is a deque of segments [uid or None, blocks], head first. A victim's segment loses
    blocks from its tail end as the head reaches it, which keeps its prefix."""

    def __init__(self, cfg, idx, seed):
        super().__init__(cfg, idx, seed)
        self.free_queue = collections.deque([[None, cfg.K]])
        self.self_hits = 0  # readmissions that hit their own cache
        self.self_hit_tokens = 0
        self.reuse_blocks_lost = 0  # victim blocks overwritten before readmission

    def counters(self):
        return super().counters() | {
            "self_hits": self.self_hits,
            "self_hit_tokens": self.self_hit_tokens,
            "reuse_blocks_lost": self.reuse_blocks_lost,
        }

    def _allocate(self, n, reused=0):
        self.used += n
        n = max(n - reused, 0)
        while n > 0 and self.free_queue:
            seg = self.free_queue[0]
            take = min(n, seg[1])
            seg[1] -= take
            n -= take
            if seg[0] is not None:
                self.reuse_blocks_lost += take
            if seg[1] <= 0:
                self.free_queue.popleft()

    def _free_to_tail(self, n):
        if n > 0:
            self.free_queue.append([None, n])

    def _readmit_hit(self, r):
        seg = next((s for s in self.free_queue if s[0] == r.uid), None)
        if seg is None:
            return 0, 0
        hit = min(seg[1] * BLOCK, r.pf_target - 1)
        reused = hit // BLOCK
        self.free_queue.remove(seg)
        self._free_to_tail(seg[1] - reused)
        if hit <= 0:
            return 0, 0
        self.self_hits += 1
        self.self_hit_tokens += hit
        return hit, reused

    def _preempt_newest(self):
        v = self.running[-1]
        tok = v.tokens()
        before = self.used
        out = super()._preempt_newest()
        freed = before - self.used
        full = min(tok // BLOCK, freed)
        if full > 0:
            self.free_queue.append([v.uid, full])
        if freed - full > 0:
            self.free_queue.appendleft([None, freed - full])
        return out

    def finish(self, now, completed):
        before = self.used
        out = super().finish(now, completed)
        self._free_to_tail(before - self.used)
        return out


ENGINES = {"recompute": Endpoint, "vllmreuse": VLLMReuseEndpoint}
