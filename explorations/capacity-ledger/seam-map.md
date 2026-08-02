# Seam map: ledger concepts onto the EPP as it stands

Where each ledger concept lives in the tree. Rows are marked landed or open; a landed
row cites the code, which is the authority, and an open row states what the seam will
have to be. Design intent lives in [ledger-revision.md](ledger-revision.md); the
pre-doc prototype (`~/Desktop/prototype`, February 2026) is consulted for interface
ergonomics only.

## Placement: a handle service

The ledger is a runtime service owned by the runner and published to plugins through
the plugin handle, alongside the metrics recorder. It is not a plugin and not a data
producer. The tests that decide this:

- Producers publish per-endpoint observations; consumers read snapshots. The ledger is
  transactional: `TryAcquireHold` is check-and-reserve with a deny verdict, which an
  attribute map cannot express. Holds are pool-scoped and endpoint-unbound, and the
  datalayer's unit is the endpoint.
- Plugins are config-optional by design. When flow control is on, the ledger is the
  admission source of truth, and a configuration that omitted it would silently restore
  the over-admission it exists to prevent. Accounting whose absence is a correctness bug
  cannot be droppable by YAML.
- `Handle.Metrics()` is the precedent for the shape: runner-owned, singular, stateful,
  imperative methods, injected by a `HandleOption`, nil when absent, not itself a
  plugin. `Handle.CapacityLedger()` follows it exactly
  (`pkg/epp/framework/interface/plugin/handle.go`).

Ruled out and not to be re-derived: a `ProducerPlugin` added via `AddPlugin` before
phase two, and a ledger that reads `InFlightLoadProducer` rather than owning its own
state. The second fails because freshness is not atomicity: admission needs a
check-and-reserve, and an observation that is merely current does not provide one.

The dividing line: mechanism is core, policy is pluggable. `UsageLimitPolicy`, fairness,
ordering, the future `VictimSelector`, and the stage-4 estimator remain plugins acting
against the ledger.

Construction: one `PoolLedger` built at the top of `parseConfigurationPhaseTwo`
(`cmd/epp/runner/runner.go:741`) when the feature gate is on, published to the handle at
`runner.go:746` through a converter (`runner.go:728`) that avoids a non-nil interface
wrapping a nil pointer. Nothing enters the config-file plugin registry except the
endpoint adapter.

## The mapping

| Ledger concept | Seam | Standing |
|---|---|---|
| Endpoint lifecycle and limits | An `EndpointExtractor` + `Registrant` at `pkg/epp/framework/plugins/datalayer/extractor/capacityledger`, self-wiring its endpoint-notification source through `PendingRegistration`. KV limit from scraped `CacheNumBlocks * CacheBlockSize`; the Slots limit is configuration, because engines export no `max_num_seqs` metric | Landed |
| Per-endpoint headroom, for scorers | The adapter publishes `AvailableCapacityDataKey` as a `DynamicAttribute` closing over `EndpointAvailable`, so a read resolves against live ledger state rather than a per-cycle snapshot. The reading nets out committed leases but not holds, which are pool-scope and taken before an endpoint is chosen | Landed |
| Plugin-visible surface | `capacity.Reader` (observe) and `capacity.EndpointSink` (report hardware), composed as `capacity.Ledger`. The reservation protocol appears on none of them: only flow control, which holds the concrete `*ledger.PoolLedger`, can hold or book | Landed |
| Hold, at the gate | The processor's `saturationDetector` dependency (`processor.go:71`) is superseded by the ledger. The per-band scalar viability check (`processor.go:378`, before item selection) becomes select-head-then-hold: peek via `selectItem`, `TryAcquireHold` for the head's footprint, HoL-block on failure with the item left queued, `ReleaseHold` if the dispatch step itself fails. Flow control holds, scheduling commits. Moving the ceiling check from before `selectItem` to the head item is a behavior change: an empty-but-over-ceiling band no longer stops the whole cycle | Open |
| Hold-to-commit correlation | The request ID, which the request already carries end to end. No receipt value is threaded through `EnqueueAndWait` or stored as a request attribute | Landed |
| Footprint prediction | `TokenTranslator` over a `Prediction` built from `TokenizedPrompt` when a tokenizer ran, `RequestSizeBytes` as the pessimistic bound otherwise, and `MaxOutputTokens` for the output ceiling. Branching is not available: no request parser extracts `best_of` or `n`. No estimator supplies an output figure when the client sets no ceiling, so that default is unresolved | Open |
| Commit (hold to lease) | `runPreRequestPlugins` (`director.go:484,591`), where the endpoint is known. `Commit` always books: the request is bound to an endpoint by then, so the only choice is whether the ledger sees the occupancy or is blind to it. A missing hold and a booking that exceeds its hold are reported in `CommitOutcome`, not refused | Open |
| Release at end of stream | `Director.HandleResponseBody` at end of stream (`director.go:522`) | Open |
| Prefill backlog | A ledger axis, released at first token by `ReleasePrefill` rather than at end of stream. The engine's own limit is a per-iteration token budget, which is a rate; the gate-worthy quantity is the backlog stock it drains, bounded by Little's law at `mu_p * TTFT_budget`. The release point mirrors the existing token-mode counter freed at StartOfStream (`inflightload/producer.go:430,450`) | Core landed, hook open |
| Slots axis | `requestTracker`'s PreRequest-to-EOS request count is the precedent; in the ledger it is the lease's Slots coordinate | Landed |
| Gating authority | `GatedAxes` gives each axis admission authority independently. Saturation is the max over gated axes only, so a shadow axis is booked and exported without being able to refuse. `DefaultConfig` gates slots alone: KV is shadow because the deterministic translator books the client's output ceiling rather than a calibrated bound, prefill because its limit is a TTFT budget nobody has stated | Landed |
| Reclaiming state | `Revoke` moves a lease from committed to reclaiming, `Retire` acknowledges. Retirement is unacknowledged by any scrape path; the state models real engine lag, since block frees defer while a step is in flight (sources.md `vllm-src`) | Core landed, scrape ack open |
| Rejection path | None added. A failed hold leaves the item queued (HoL block); queue-wait TTL and capacity rejections keep their existing mapping in `translateFlowControlOutcome` (`pkg/epp/requestcontrol/admission.go:246-285`) | Open |
| Hold failure topology | Dispatch failure releases the hold inside the processor. Post-dispatch failures (Schedule error, conditional-decode 412, prepare failure) release it on the `HandleRequest` error path by request ID. Hold TTL expiry is the backstop: it reclaims capacity from scheduling stalls and is not a queueing mechanism. Leases have no equivalent backstop, which is an open design hole rather than a decision | Open |
| P/D scope | The lease binds to the primary profile's endpoint only. A prefill-side residency lease is out of scope | Deferred |

## Where the prototype and the doc diverge, the doc wins

- Axes: the prototype's four-dimension `ResourceVector` (PrefillTokens, DecodeTokens,
  KVBlocks, ActiveRequests) becomes `Footprint{KVTokens, PrefillTokens, Slots}`.
  Decode tokens are not separately tracked; they are the KV axis's growth term.
- Reconciliation: the prototype treats the scraped baseline as authoritative for
  spatial dimensions and self-tracks only a temporal transit window (three 50 ms epoch
  buckets); the doc inverts this to event truth-up with per-scrape validation. The
  ledger implements the event truth-up; the scrape seam is instrumentation only.
- Hold placement: none. The prototype's placement is adopted, flow control holding at
  dispatch and scheduling committing. The prototype's upstream had prompt tokens
  pre-dispatch; this tree does not, and the gap is closed by holding a pessimistic bound
  that the commit truths down, which is the protocol's own semantics.
- Correlation: the prototype hands back a value receipt; the ledger keys by request ID.
- Underflow: the prototype floors subtraction at zero (`subAtomicNoNegative`); the
  ledger surfaces underflow as a corruption error, on the grounds that correctness comes
  from zero-sum discipline and a clamp hides exactly the accounting bug worth catching.

What the ledger adopts from the prototype unchanged: `ReleaseHold` refunding on any
pre-commit failure, the O(1) aggregate check plus short-circuit per-endpoint fit check
serialized by a single admission mutex, and idempotent releases.
