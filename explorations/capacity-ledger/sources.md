# Sources

Every external work or artifact the capacity-ledger exploration leans on, with what is
actually known about it. States, per [STYLE.md](STYLE.md):

- **unverified** — recalled from model memory while drafting; existence, attribution, and
  contents all unchecked.
- **machine-read** — a fetch-and-summarize pass or search snippet produced the claims;
  existence confirmed, contents not read by a person or against the full text.
- **verified** — the artifact exists and bibliographic/link details are confirmed;
  contents not read.
- **read** — read in full or in named part, by a person or by an assistant pass against a
  kept full text, with locators available.

| Key | Work | State | Note |
|---|---|---|---|
| `dro2026` | "Robust KV Cache Management for LLM Serving under Output Token Length Uncertainty." arXiv:2607.16892, 2026. | machine-read | One fetch-summary pass (2026-07-31). Claims leaned on by the assessment's finding 7: per-class admission-time KV reservation via Wasserstein DRO; optimal reservation at the critical fractile rho/(rho+1) of the length distribution, rho the preemption-to-waste cost ratio; conditions only on admission-time information; periodic control-plane, preemption as cost not enforcement; "no single fixed-quantile heuristic is optimal across cost regimes." Locators unknown; a read should pin the fractile theorem before the claim is used outside this branch |
| `tie2026` | "Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions" (TIE). arXiv:2604.00499. | machine-read | Search-snippet standing only. Claims leaned on: output length modeled as a distribution (log-t, heavy-tailed); updating the distribution during decoding named as open future work — the claim that our age-conditioning occupies an acknowledged gap rests on this and needs the read |
| `plp2026` | "Predicting LLM Output Length via Entropy-Guided Representations" (EGTP/PLP). arXiv:2602.11812. | machine-read | Search-snippet standing. Progressive re-prediction of remaining length during decode from model activations; per-request, engine-side |
| `remlen2026` | "How Much is Left? LLMs Linearly Encode Their Remaining Output Length." arXiv:2607.05316. | machine-read | Search-snippet standing. Residual stream linearly encodes remaining response length; relevant only to stage-5 engine co-design |
| `uniboost2026` | "Beyond Prediction: Tail-Aware Scheduling for LLM Inference" (UniBoost). arXiv:2606.18431, ICML 2026. | machine-read | Search-snippet standing. Same prompt varies more than 2x in output length across sampling runs; replaces per-request prediction with statistical priority signals |
| `s3-line` | S3 (NeurIPS 2023), TetriInfer, SSJF (Qiu et al. 2024), learning-to-rank scheduling (Fu et al. 2024). | unverified | Recalled from model memory as the arrival-time length-prediction line and its weak-to-fragile record; titles, venues, and the characterization all unchecked this branch. The assessment's "per-request prediction is the wrong frame" narrative leans on this recollection |
| `azure-traces` | AzurePublicDataset LLM inference traces (2023 conv/code, 2024). | unverified | Recalled as public CSVs with input/output token counts and timestamps; existence and schema unchecked. Named as the day-2 calibration source; goes through `inference-perf` loaders when used |
| `burstgpt` | BurstGPT trace. | unverified | Recalled; also attested as an evaluation trace in `dro2026`'s machine-read summary. Unchecked |
| `inference-perf` | kubernetes-sigs/inference-perf. https://github.com/kubernetes-sigs/inference-perf | machine-read | README fetch-summary (2026-07-31): synthetic workloads with configurable input/output token distributions, ShareGPT/Azure-class trace replay, Poisson and multi-stage load, pip-installable. llm-d project; owns workload formats and trace loaders for this exploration's L2 and day-2 calibration |
| `llm-d-inference-sim` | llm-d/llm-d-inference-sim. https://github.com/llm-d/llm-d-inference-sim | machine-read | Search-snippet standing: OpenAI-compatible real-time vLLM mock, latency modeling (TTFT, inter-token, load-coupled), vLLM-subset Prometheus metrics. llm-d project; the L2 backend |
| `blis` | inference-sim/inference-sim (BLIS). https://github.com/inference-sim/inference-sim | verified | Link supplied by the project owner-adjacent user; repo not yet fetched or read. Discrete-event cluster simulator per its description; donation to llm-d under discussion at https://github.com/llm-d/llm-d/pull/2015. The L0.5 venue; a read of its API precedes the re-score |
| `ledger-doc` | docs/flow-control-capacity-ledger.md and docs/flow-control-eviction.md (PR #2061). | read | Read in full this branch; the assessment reviews them section by section |
| `prior-review` | capacity-ledger-review.md and ledger-prototype-brief.md (main checkout root). | read | Read in full, but authored by another model near context exhaustion, so per STYLE.md the claims inside carry machine-read standing: positions the assessment adopts are re-derived there or marked as adopted-unverified. The canary-unbiasedness claim is the recorded example of one that did not survive scrutiny |

Claims in [assessment.md](assessment.md) that lean on machine-read or unverified entries
say so at the point of use; upgrading an entry's state (an actual read with locators)
is recorded here, not by silently strengthening the prose.

Kept texts: source PDFs live outside every git tree, at
`~/Desktop/Code/llm-d-router-data/capacity-ledger/papers/`, because of licensing and
because worktree removal must not delete them. They are never committed. An assistant
pass against a kept full text upgrades a work to read, with locators pinned in its Note
so any claim the exploration leans on can be checked against the text in minutes.
