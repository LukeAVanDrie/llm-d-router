# The model

This is the overview; each module's docstring states what it does in detail. Terms: a **cell**
is a workload plus a KV cache size, a **variant** is a cell at cache factor `kf` on `E` endpoints,
and **load** is the arrival rate over the variant's capacity.

## The engine (`engine.py`, `config.py`)

Each endpoint is a step-level model of one vLLM v1 engine:

- **Batching.** Each step schedules running requests first, oldest first, within a token budget
  (`budget`, 2,048) shared by decodes (one token each) and chunked prefill. At most `max_seqs`
  (512) requests run at once.
- **KV cache.** `K` blocks of 16 tokens. A request with x tokens resident holds ceil((x + 1) / 16)
  blocks; the extra slot is for its next token.
- **Preemption.** When a step needs more blocks than are free, the newest running request is
  preempted: its blocks are freed and it goes back to the head of the waiting queue, to prefill its
  prompt and its generated tokens again. Waiting requests are admitted in FIFO order, and only in
  steps without a preemption, while their whole prefill fits in free blocks.
- **Step time.** a + b n_decode + c S + b_pf n_prefill, where n_decode is the number of decoding
  requests, S the tokens resident in KV (a stand-in for the cost of reading KV from HBM), and
  n_prefill the prefill tokens in the step. `EngineConfig` sets a = 1 (so time is measured in
  step fixed costs), b = 0.01, c = 0.5 / (full cache size in tokens), so a full cache adds half a
  fixed cost, and b_pf = 1/150 (`kappa0` = 150 prefill tokens cost one fixed cost). Each step's
  time is multiplied by lognormal noise (sigma 0.05). `Endpoint._step_duration` computes it.
- **Dispatch lag.** A routed request reaches the engine 1 time unit after the routing decision.

With `--engine vllmreuse`, a preempted request keeps its cached blocks until new allocations
overwrite them (`VLLMReuseEndpoint`). vLLM v1 returns a victim's full blocks to the tail of one
free-block queue and allocates from the head, so the victim's cached prefix shrinks with every
block allocated after its preemption. On readmission it skips prefilling whatever contiguous
prefix survives. `--engine recompute` recomputes the victim's whole context.

## The router (`router.py`)

One FIFO queue in front of the pool, released head first. The router binds a request to an
endpoint only when it releases it (late binding), not on arrival. After every event, the
router offers the head request to endpoints from fewest to most requests held (waiting plus
running) and sends it to the first eligible one. If none is eligible, the head and everything
behind it wait. An endpoint is eligible when:

1. no request that has already emitted tokens (a preempted one) waits in its queue, and either
   nothing is pending or the prefill of its requests with no token yet, plus the head's prompt,
   fits in one step's budget;
2. if the endpoint is idle, the head's prompt fits in KV, and nothing more is checked;
3. **the fit check F:** allocated blocks plus queued prompts plus the head's prompt fit in KV;
4. the policy's test passes, if the policy has one.

A policy refusal is cached per (request, endpoint) until that endpoint's next completion or
preemption, and the policy is not asked again before then.

## The policies (`policies.py`)

| policy | rule |
|---|---|
| `nogate` | no router queue: each arrival goes straight to the endpoint with the fewest requests |
| `fit` | release condition and F only |
| `lookahead:<pct>` | F, then admit if every request would fit at its client ceiling (`max_tokens`), or if the probability that no running request finishes before the endpoint's free memory runs out is at most pct% |

The look-ahead uses output-length survival learned online. The survival band (`survival.py`) is a
bootstrap of Kaplan-Meier curves over recent completions, where lengths that hit their ceiling and
the ages of running requests count as censored. Until the first completion there is no curve, and
the look-ahead refuses whatever the ceiling test does not admit. The `LookAhead` docstring in
`policies.py` gives the test as a formula.

The research results name look-ahead thresholds by gamma, with pct = 100 exp(-gamma): gamma = 1,
1.5, 2, 3 give 36.79, 22.31, 13.53, 4.98. A larger gamma is a stricter gate. The cost balance the
research uses to pick gamma is not part of this package.

## Cells, loads and metrics (`config.py`, `workload.py`, `calibration.py`, `metrics.py`)

| cell | outputs | prompts | KV cache per endpoint |
|---|---|---|---|
| `bimodal` | 60% lognormal around 80 tokens, 40% around 2,000 | lognormal, mean = mean output | 81,920 tokens (10 contexts of 8,192) |
| `bimodal`, `--kf 0.5` | same | same | 40,960 tokens |
| `longout` | heavy-tailed lognormal, median 150 | mean = 0.1 x mean output | 24,576 tokens |

Output lengths are clipped at the law's 0.999 quantile. Half the requests declare that quantile
as their client ceiling (`max_tokens`); the other half declare the context limit, so their actual
lengths are the same but the router cannot rule out a long output. Arrivals are Poisson at `load`
times the variant's capacity, so 1.1 means overload and 0.95 means just below capacity. The first
20% of completions are warm-up. The comparison metric is token throughput in overload and goodput
below capacity.

`calibration.json` stores each variant's capacity and SLO thresholds (`calibration.py`). Capacity
is the pool's saturated completion rate: the larger of `fit` and `nogate` with a full queue at time
0, where the `fit` run places by KV blocks in use and counts only unprefilled tokens in the release
condition. The SLO thresholds are the 90th percentiles of TTFT and TPOT under `nogate` at load 0.9.

## What it does not model

- CPU offload of KV blocks and PCIe transfer time. A victim either recomputes or reuses blocks
  still in GPU memory.
- Prefix caching across requests. No two requests in these cells share a prefix.
- vLLM scheduling details beyond the above: long-prefill thresholds, the partial-prefill limit,
  speculative decoding, priority scheduling.
- More than one router replica.
- A real GPU. The step-time model is linear and not calibrated against hardware. The length
  distributions are synthetic.
