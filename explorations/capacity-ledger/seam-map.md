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
| Hold, at the gate | The processor's `saturationDetector` dependency is superseded by the ledger. The per-band scalar viability check (`processor.go:390`, before item selection) becomes select-head-then-hold: peek via `selectItem`, `TryAcquireHold` for the head's pessimistic footprint (prompt bytes upper-bound tokens; `MaxOutputTokens` and branching are parsed fields), HoL-block on failure with the item left queued, `ReleaseHold` if the dispatch step itself fails. Flow control holds, scheduling commits |
| Receipt handoff | The receipt rides the item: FlowItem final state carries it through `EnqueueAndWait` to `FlowControlAdmissionController`, which stores it as a request attribute for the commit hook (the prototype extends `FinalizeWithOutcome` the same way) |
| Commit (hold to lease) | `PreRequest` (`director.go:484,591`), where the endpoint is known. Consumes the receipt under the escalation guard; the cached-prefix truth-up applies here (`PrefixCacheMatchInfo`), the same discount the prototype applied at its commit |
| Release at end of stream | `Director.HandleResponseBody` EndOfStream (`director.go:522-566`). Missed-EOS fallback is the PluginState janitor TTL with exactly-once semantics, the `addedTokensEntry` swap-to-zero pattern (`inflightload/producer.go:142-180`); the prototype's `PrefillReleased` CAS is the same idempotency device |
| Prefill backlog gate | The existing token-mode counter released at StartOfStream (`producer.go:430-445`) stays as-is beside the ledger; the ledger does not absorb it (prefill is a rate, not a stock) |
| Slots axis | `requestTracker`'s PreRequest-to-EOS request count (`producer.go:369,463-467`) is the precedent; in the ledger it is the lease's Slots coordinate |
| Reclaiming state | New accounting in the `EndpointLedger`: `Revoke` moves a lease from committed to reclaiming, `Retire` acknowledges. Retirement is stubbed at stage 2 (scrape acknowledgment is stage 3). The state models real engine lag: block frees defer while a step is in flight (sources.md `vllm-src`) |
| Endpoint lifecycle and limits | The endpoint-notification extractor mechanism (`producer.go:282-315`), registered programmatically by the runner. KV limit from scraped `CacheNumBlocks * CacheBlockSize`; the Slots limit is configured, because engines do not export `max_num_seqs` |
| Rejection path | None added. A failed hold leaves the item queued (HoL block); queue-wait TTL and capacity rejections keep their existing mapping in `translateFlowControlOutcome` (`pkg/epp/requestcontrol/admission.go:246-285`) |
| Hold failure topology | Dispatch failure releases the hold inside the processor (prototype's `dispatchCycle`). Post-dispatch failures (Schedule error, conditional-decode 412, prepare failure) release it on the `HandleRequest` error path via the receipt on the request; hold TTL expiry is the backstop, and a commit against an expired hold fails, rejecting the stalled request (the TTL reclaims capacity from scheduling stalls, it is not a queueing mechanism) |
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
- Hold placement: none. The prototype's placement is adopted — flow control holds at
  dispatch, scheduling commits. The prototype's upstream had prompt tokens
  pre-dispatch; this tree does not, and the gap is closed by holding a pessimistic
  bound (prompt bytes upper-bound prompt tokens) that the commit truths down, which
  is the protocol's own hold-then-truth-up semantics.
- Underflow: the prototype floors subtraction at zero (`subAtomicNoNegative`); the
  doc makes underflow a surfaced ledger-corruption error. The ledger core follows the
  doc; clamped floors remain only in the backlog counters that keep their existing
  semantics.

What the skeleton adopts from the prototype unchanged: the receipt-based protocol
surface (`TryAcquireHold` returning a value receipt, `ReleaseHold` refunds on any
pre-commit failure, `Commit(endpoint, actual, receipt)`), the O(1) aggregate check
plus short-circuit per-endpoint fit check serialized by an admission mutex
(thundering-herd protection), and idempotent releases.
