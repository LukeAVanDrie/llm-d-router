# Extending admsim

The pieces compose explicitly in `simulation.simulate(workload, cfg, engine, policy, ...)`, where
`engine` is an `Endpoint` subclass or a name in `engine.ENGINES`; nothing is patched at run time.
Keep `python -m unittest discover -s tests` passing. Keep `python tests/parity.py` passing too,
unless you intend to change the behavior of an existing engine or policy.

- **A new cell:** add it to `CELLS` in `config.py` (an output law from `workload.OUTPUT_LAWS`, the
  prompt-to-output ratio, the cache size in contexts), then run
  `python -m admsim prep <cell> <kf> <E>`. That computes capacity and thresholds (a minute or two)
  and adds them to `calibration.json`. Any other kf or endpoint count needs `prep` too.
- **A new policy:** subclass `policies.Policy` and implement `admits(ep, r)`, which the router
  calls after the fit check F ([MODEL.md](MODEL.md)) passes and returns True or False. Implement
  `observe(...)` if it learns from completions, and register a spec name in `make_policy`. A
  look-ahead can subclass `_SurvivalPolicy` and implement `_decide(ep, r, g)`, where
  `g` is a `Growth`: the requests that would grow alongside r and the headroom they share. If the policy's answer can change without a completion or
  preemption (it watches time, or transfers in flight), set `cache_refusals = False`.
- **A new engine, for example CPU offload:** subclass `engine.VLLMReuseEndpoint` or
  `engine.Endpoint` and add it to `ENGINES`. The `Endpoint` docstring states what the rest of the
  simulator reads and writes. The hooks:
  - `_preempt_newest()` frees a victim and returns it;
  - `_readmit_hit(r)` returns (tokens, blocks) a readmitted request skips prefilling;
  - `_allocate(n, reused)` allocates n blocks, `reused` of them the request's own cached blocks;
  - `_step_duration(n_dec, S, pf_tok)` returns a step's duration;
  - `counters()` returns the counts that go into each result row.

  An offload engine would, on preemption, move the victim's blocks to a host tier with finite
  capacity instead of freeing them for good. On readmission `_readmit_hit` would return those
  tokens as a hit, and the engine would record the blocks moved in. `_step_duration` would add
  blocks moved times a per-block PCIe cost; a transfer that overlaps compute needs its own event in
  `simulation.py` instead. Add a counter for blocks moved. The invariants test shows how to check a
  new engine's block accounting.
- **A new kind of event:** `simulation.Simulation` keeps a heap of (time, seq, handler, arg).
  Add a handler method and schedule it with `push(t, handler, arg)`; `on_step_end` and
  `on_reach_engine` are the examples.

`tests/test_units.py` shows how to drive an endpoint step by step and how to test router
rules on hand-built states.

## Layout

```
admsim/
  config.py        EngineConfig (step-time coefficients, cache size) and the CELLS table
  workload.py      output-length laws and request draws
  engine.py        Request, Endpoint (recompute), VLLMReuseEndpoint (self-reuse)
  router.py        the FIFO router: release condition, fit check, placement, refusal cache
  policies.py      nogate, fit, lookahead; the ceiling test; Growth
  survival.py      the bootstrap Kaplan-Meier survival band
  simulation.py    the event loop (Simulation) and simulate()
  metrics.py       per-run metrics and paired comparisons
  calibration.py   capacity and SLO thresholds per variant; calibration.json holds them
  experiment.py    named-cell runs and parallel grids
  __main__.py      the command line: run, batch, score, prep
tests/
  test_units.py    contracts of blocks, Kaplan-Meier, the reuse free queue, router rules
  test_engine.py   block-accounting invariants over short runs
  parity.py        exact reproduction of the research code's rows (parity_reference.jsonl)
docs/
  MODEL.md         what the simulator models and what it does not
  EXTENDING.md     this file
examples/self_reuse_check.sh
results/self_reuse_check.md
```
