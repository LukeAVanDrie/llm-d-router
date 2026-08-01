# Seam map: ledger concepts onto the EPP as it stands

Scope: the stage-2 skeleton (work table row 2). Every ledger concept from
[ledger-revision.md](ledger-revision.md) is mapped to the seam that hosts it before
any new code is written. Code references are to this branch's tree; the design
authority order is ledger-revision.md first, the pre-doc prototype
(`~/Desktop/prototype`, February 2026) for interface ergonomics, then the repo's own
conventions.

## Placement: core layer, not a plugin

The ledger is a core architectural layer, not a plugin, and specifically not a data
producer. The tests that decide this:

- Producers publish per-endpoint observations; consumers read snapshots. The ledger
  is transactional: `TryAcquireHold` is check-and-reserve with a deny verdict, which
  cannot be expressed through an attribute map. Holds are pool-scoped and
  endpoint-unbound, and the datalayer's unit is the endpoint.
- Plugins are config-optional by design. When flow control is on, the ledger is the
  admission source of truth; a configuration that omitted it would silently restore
  the over-admission it exists to prevent. Accounting whose absence is a correctness
  bug cannot be droppable by YAML.
- The precedent is the FlowController itself: a runtime layer switched by the
  `flowControl` feature gate, constructed by the runner, injected into consumers.
  The prototype agrees: its `ShardProcessor` carries a
  `ledger hypervisor.AdmissionLedger` dependency in the position where today's
  processor carries `saturationDetector`
  (`pkg/epp/flowcontrol/controller/internal/processor.go:71`).

The dividing line: mechanism is core, policy is pluggable. `UsageLimitPolicy`,
fairness, ordering, the future `VictimSelector`, and the stage-4 estimator remain
plugins acting against the ledger. Where the ledger touches plugin seams it wears
thin adapters — its PreRequest/ResponseBody hook implementations are appended to the
director's hook lists by the runner in code, and read-only views can later be
exported as datalayer attributes for scorers — without the core becoming a plugin.

Concretely: one `PoolLedger` built in `runner.initAdmissionControl`
(`cmd/epp/runner/runner.go:870-897`) when the feature gate is on, injected into the
processor and the director. Nothing enters the config-file plugin registry.

## The mapping

| Ledger concept | Seam |
|---|---|
| Hold, candidate (a) | The director's post-tokenization window: data producers run at `pkg/epp/requestcontrol/director.go:309`, `Schedule` at `:320`. The hold is a direct director dependency invoked between them; `TryAcquireHold` from tokenized prompt plus the `MaxOutputTokens` ceiling, receipt carried on the request for commit |
| Hold-fit at the gate | The processor's `saturationDetector` dependency is superseded by the ledger. The per-band scalar viability check (`processor.go:390`, before item selection) becomes select-head-then-fit: peek via `selectItem`, ask whether the head's estimated footprint fits, HoL-block on failure. View-only at the gate: prompt tokens do not exist pre-dispatch (the doc's finding-3 ordering), so the reservation itself stays director-side |
| Commit (hold to lease) | `PreRequest` (`director.go:484,591`), where the endpoint is known. Consumes the receipt under the escalation guard; the cached-prefix truth-up applies here (`PrefixCacheMatchInfo`), the same discount the prototype applied at its commit |
| Release at end of stream | `Director.HandleResponseBody` EndOfStream (`director.go:522-566`). Missed-EOS fallback is the PluginState janitor TTL with exactly-once semantics, the `addedTokensEntry` swap-to-zero pattern (`inflightload/producer.go:142-180`); the prototype's `PrefillReleased` CAS is the same idempotency device |
| Prefill backlog gate | The existing token-mode counter released at StartOfStream (`producer.go:430-445`) stays as-is beside the ledger; the ledger does not absorb it (prefill is a rate, not a stock) |
| Slots axis | `requestTracker`'s PreRequest-to-EOS request count (`producer.go:369,463-467`) is the precedent; in the ledger it is the lease's Slots coordinate |
| Reclaiming state | New accounting in the `EndpointLedger`: `Revoke` moves a lease from committed to reclaiming, `Retire` acknowledges. Retirement is stubbed at stage 2 (scrape acknowledgment is stage 3). The state models real engine lag: block frees defer while a step is in flight (sources.md `vllm-src`) |
| Endpoint lifecycle and limits | The endpoint-notification extractor mechanism (`producer.go:282-315`), registered programmatically by the runner. KV limit from scraped `CacheNumBlocks * CacheBlockSize`; the Slots limit is configured, because engines do not export `max_num_seqs` |
| Rejection path | A hold denial is backpressure: `errcommon.ResourceExhausted` (429) with a dropped-reason header, the vocabulary of `translateFlowControlOutcome` (`pkg/epp/requestcontrol/admission.go:246-285`). The director must preserve the typed code on this path (the admission-plugin deny at `director.go:317` flattens to Internal; the scheduler-error path at `:327-331` shows the errors.As preservation pattern) |
| Hold failure topology | The director owns the receipt, so any error between hold and commit (Schedule failure, conditional-decode 412, prepare failure) releases the hold explicitly on the `HandleRequest` error path; TTL expiry is the backstop for paths the director cannot see, per the doc (the TTL reclaims capacity from scheduling stalls, it is not a queueing mechanism) |
| P/D scope | The lease binds to the primary profile's endpoint only. The prefill worker's transient claim is the backlog gate's domain (released at StartOfStream); a prefill-side residency lease is out of stage-2 scope |

## Where the prototype and the doc diverge, the doc wins

- Axes: the prototype's four-dimension `ResourceVector` (PrefillTokens, DecodeTokens,
  KVBlocks, ActiveRequests) is reduced by the doc to `Footprint{KVTokens, Slots}`
  with prefill governed by the backlog gate beside the ledger.
- Reconciliation: the prototype treats the scraped baseline as authoritative for
  spatial dimensions and self-tracks only a temporal transit window (three 50 ms
  epoch buckets); the doc inverts this to event truth-up with per-scrape validation.
  Stage 2 implements the doc's event truth-up; the scrape seam is instrumentation
  only.
- Hold placement: the prototype acquires the hold at the dispatch gate, which was
  possible against an upstream where prompt tokens existed pre-dispatch; this tree
  tokenizes after `Admit` returns, so the hold is director-side (candidate (a)) and
  the gate check is a fit preview.
- Underflow: the prototype floors subtraction at zero (`subAtomicNoNegative`); the
  doc makes underflow a surfaced ledger-corruption error. The ledger core follows the
  doc; clamped floors remain only in the backlog counters that keep their existing
  semantics.

What the skeleton adopts from the prototype unchanged: the receipt-based protocol
surface (`TryAcquireHold` returning a value receipt, `ReleaseHold` refunds on any
pre-commit failure, `Commit(endpoint, actual, receipt)`), the O(1) aggregate check
plus short-circuit per-endpoint fit check serialized by an admission mutex
(thundering-herd protection), and idempotent releases.
