# admsim: admission control on a simulated vLLM pool

admsim is a discrete-event simulator of a pool of vLLM-shaped engines (four endpoints by default)
behind a router that decides, per endpoint, whether to admit the request at the head of its queue.
It was built to compare admission rules when KV cache memory is the bottleneck: no gate, a memory
fit check, and a predictive look-ahead that estimates whether admitting now will force preemptions.
It also models cheaper preemption, where a preempted request reuses its own cached blocks the way
vLLM v1 allows.

It is pure Python on NumPy, about 1,250 lines in `admsim/`.

Three terms recur below. A **cell** is a workload plus a KV cache size (`bimodal`, `longout`).
A **variant** is a cell at a cache factor `kf` (0.5 halves the cache) on `E` endpoints. **Load**
is the arrival rate over the variant's measured capacity.

## Quick start

Run every command from the `admission-sim/` directory. Install into a fresh virtual environment
(Python 3.10 or later; the reference numbers were produced on 3.14.5 with NumPy 2.5.1, which
`requirements.txt` pins):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

### One run

Run the look-ahead policy with a 22.31% threshold (see [docs/MODEL.md](docs/MODEL.md)) on the
bimodal cell, 10% over capacity, 2,500 requests, once per engine. Each run takes about 10 seconds:

```bash
python -m admsim run --policy lookahead:22.31 --cell bimodal --load 1.1 --seed 101 --requests 2500
python -m admsim run --policy lookahead:22.31 --cell bimodal --load 1.1 --seed 101 --requests 2500 --engine vllmreuse
```

Each prints one JSON line. Expect `"throughput_tok": 61.16891274580165` from the first and
`"throughput_tok": 62.193276737673855` from the second, with `"self_hits": 169`. A different number
means your NumPy draws differently from 2.5.1. Check with `pip show numpy`.

The fields you will use most:

| field | meaning |
|---|---|
| `throughput_tok` | output tokens per time unit after warm-up (the overload metric) |
| `goodput` | requests per time unit that met both SLOs (the below-capacity metric) |
| `attainment` | share of completed requests that met both SLOs |
| `ttft_p90`, `preempts_per_completion` | 90th percentile time to first token; preemptions per completion (warm-up included) |
| `prefill_saved`, `self_hits` | with `vllmreuse`: prefill tokens served from a victim's own cache, and how many readmissions hit |

Time is measured in units of one engine step's fixed cost ([docs/MODEL.md](docs/MODEL.md)).
Throughput numbers are comparable across policies and engines within a cell, not across cells.

### A small grid, then a paired comparison

```bash
examples/self_reuse_check.sh --quick out/quick
```

This runs 6 policies x 3 cells x 2 engines x 2 loads x 2 seeds (144 runs at 2,500 requests) on all
cores. It takes under a minute on 10 cores (about 4 CPU-minutes) and prints four tables, two per
baseline (`fit` and `nogate`). Each entry is the mean log ratio, in percent, of a policy's metric
against the baseline policy's metric on the same seeds, with 1.96 standard errors and the number
of seeds:

```
== load 1.1: throughput_tok, % log ratio against fit, +-1.96 standard errors (n seeds)
bimodal K1 E4 recompute  nogate: -0.1 +-0.7 (2)  lookahead:36.79: +3.4 +-0.3 (2)  lookahead:22.31: +4.1 +-0.2 (2)  ...
```

Two seeds are enough to check that the pipeline works. They are not enough for conclusions; the
results below use five seeds at 10,000 requests (`examples/self_reuse_check.sh` with no flag, 360
runs, about 5 minutes on 10 cores). `batch` skips runs whose output file exists, so an interrupted
grid resumes.

To score any batch directory: `python -m admsim score out/quick --baseline nogate`.

### Checks

```bash
python -m unittest discover -s tests      # unit tests and block-accounting invariants, seconds
python tests/parity.py                    # 146 runs against reference rows, under a minute
```

`tests/parity.py` reruns rows produced by the research code this package was extracted from and
requires an exact match (floats to 6 decimals, counters exactly). `python tests/parity.py full`
reruns the 360 rows behind the results below and prints `PASS` or each mismatched row.
`python tests/parity.py --calibration` recomputes every entry of `calibration.json` and prints `ok`
or `MISMATCH` per variant; it does not write the file.

## What the results say

`results/self_reuse_check.md` holds the tables. Everything below is exploratory and simulated,
over five paired seeds at 10,000 requests, with no validation on a real engine yet. Figures are
mean log ratios in percent, with 1.96 standard errors.

- `fit` against `nogate`: the fit check alone does not beat no gate in these KV-bound cells. In
  overload it is level (-0.1 to -0.3 in every cell, under both engines). Below capacity it is
  mixed: ahead on bimodal under self-reuse (+3.0 +- 1.3), behind or within noise elsewhere.
- `lookahead:22.31` (gamma = 1.5) against `fit`, in overload: on bimodal the gain is +5.4 +- 0.2
  with recompute and +3.0 +- 0.1 with vLLM-style self-reuse. On half cache it is +2.6 +- 0.1 and
  +1.2 +- 0.1. Longout gives +1.5 +- 1.1 and +1.4 +- 1.1, too noisy at five seeds to rank the
  look-ahead settings.
- Against `nogate`, cheaper preemption roughly halves the look-ahead's gain and moves the best
  gamma down. At gamma = 1.5 in overload, bimodal goes from +5.3 to +2.9. On half cache, gamma = 3
  goes from +1.9 to +0.0 in overload once victims reuse their cache.

## Further reading

- [docs/MODEL.md](docs/MODEL.md): the engine, router, policies, cells and metrics, and what the
  simulator does not model.
- [docs/EXTENDING.md](docs/EXTENDING.md): adding a cell, a policy or an engine (such as CPU
  offload), and the code layout.
