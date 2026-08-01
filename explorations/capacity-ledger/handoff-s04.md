# Handoff: capacity ledger, stage 4

Branch `capacity-ledger`, worktree `capacity-poc`. HEAD `6e0fdc1e`. Tag `s04` NOT applied.

Fork-only. Nothing merges. No pushes, no GitHub writes, no tagging without explicit
per-action authorization from the user.

## Where the last session landed

The core is built and green. `go test -race -count=1 ./pkg/epp/flowcontrol/ledger/`
passes; `go vet` and `gofmt` clean; coverage 91.8%.

The session's main output was not code, it was settling **where the ledger lives in the
architecture**. That question was reopened twice and answered wrong twice before the
evidence settled it. Do not re-derive this from scratch; the answer is below with its
citations.

## The placement question, settled

The ledger has two faces, and they belong in different places.

**Read face.** Per-endpoint available capacity, pool saturation. Plugins (scorers,
filters) should see this. Idiomatic, no friction.

**Write face.** `TryAcquireHold` / `Commit` / `Release` / `Revoke` / `Retire`. Stateful,
cross-request atomic, return value gates admission, one instance shared by the flow
control processor and two director hooks.

Every core-to-plugin edge in this codebase is one of three shapes, and the write face is
none of them:

| shape | example | singular? | return gates? | mutates shared state? |
|---|---|---|---|---|
| fan-out callback | `PreRequest`, `ResponseBody`, `Produce` | no | no | plugin-local |
| pure policy | `SaturationDetector.Saturation`, `UsageLimitPolicy.ComputeLimit` | via defaultable iface | yes | no |
| attribute publish/read | `DynamicAttribute` | no | no | no |

So the ledger is **not a plugin**. The precedent for what it is: `Handle.Metrics()
MetricsRecorder` (`pkg/epp/framework/interface/plugin/handle.go:36-37`) is a
runner-owned, singular, stateful service with imperative methods, injected by a
`HandleOption` (`WithMetricsRecorder`, `handle.go:113-120`), reachable by any plugin,
nil when absent, and not itself a plugin. The ledger takes that shape.

Two dead ends, recorded so they are not retried:

- **Ledger as a `ProducerPlugin` injected via `handle.AddPlugin` before
  `parseConfigurationPhaseTwo`.** The only non-framework `AddPlugin` callers are inside
  the config loader (`configloader.go:242`, `defaults.go:360`). Injecting a
  runner-constructed instance there fights the lifecycle, and it collides with
  `CreateMissingDataProducers` (`data_graph.go:72-97`), which either errors with
  "no default producer found for missing data key" or auto-creates an orphan duplicate.
- **Ledger reading the `InFlightLoadProducer` instead of owning state.** No hold phase,
  no per-lease records, no revocation. Freshness is not atomicity: between `Get(ep1)` and
  `Get(ep2)` state moves and nothing reserves.

The mechanism that *does* exist and was initially missed: `Registrant` /
`PendingRegistration` (`pkg/epp/framework/interface/datalayer/registrar.go:28-46`). This
is the sanctioned path for a plugin that wires itself to a data source without appearing
in YAML, including `DefaultSource` auto-creation and yield-to-user-config resolution
(`runtime.go:187+`). `InFlightLoadProducer.RegisterDependencies`
(`producer.go:272-279`) is the working template. The **adapter** below uses this; the
ledger itself does not.

## What is done (commit `6e0fdc1e`)

`pkg/epp/flowcontrol/ledger/`:

- **Third axis `PrefillTokens`.** Claimed at admission, released at first token via
  `ReleasePrefill`, which deducts from `LeaseRecord.Booked` so the eventual `Release`
  stays zero-sum. Repeat calls are no-ops. Limit is `Config.PrefillTokensPerEndpoint`,
  a Little's-law bound (prefill throughput times TTFT budget), not an engine-reported
  number. Shadow-gated in stage 2.
  - Why the axis is real: the vLLM *engine* limit is a rate (one shared per-iteration
    `token_budget`, verified at `scheduler.py:445,619,669,1029`), but the gate-worthy
    quantity is the backlog *stock*. Two pools with identical KV occupancy differ in
    TTFT by how much is un-prefilled, so the residency axes provably cannot express it.
- **`Reconcile` replaced by `UpsertEndpoint` / `DeleteEndpoint`.** The ledger no longer
  keeps an endpoint-set copy to diff against.
  - Deviation from the previous plan, deliberate: **the `draining` flag was kept.** It
    earns its place independently of `Reconcile`. `DeleteEndpoint` on an endpoint with
    live leases must keep that occupancy in the roll-up (the work still occupies real
    hardware) while zeroing its limits so it wins no fit check. Entry drops when its
    last lease ends.
- **`Reader` interface** (`EndpointAvailable`, `Saturation`) with
  `var _ Reader = (*PoolLedger)(nil)`. This is the plugin-visible surface. Plugins
  observe; only flow control, holding the concrete type, reserves.
- `Snapshot()` demoted in its doc comment to debugging and tests. It is not the metrics
  path.
- Tests use a `literalTranslator` so pool tests state the footprint they mean;
  `TokenTranslator`'s decomposition stays covered by `TestTokenTranslator`.

Carried over unchanged from stage 3 and still load-bearing: underflow is an error and
never clamped; `LeaseRecord.Booked` records what commit added so a mid-lease geometry
change cannot desynchronize the release; saturation is max over **gated** axes only, so
a shadow axis cannot gate through `UsageLimitPolicy`.

## Remaining work, in order

### 1. Handle wiring

- Add `CapacityLedger() ledger.Reader` to the `Handle` interface and a
  `WithCapacityLedger(ledger.Reader)` `HandleOption`, mirroring `Metrics()` /
  `WithMetricsRecorder` exactly (`handle.go:36-37`, `:113-120`).
- Runner constructs `*ledger.PoolLedger` and passes it into `NewEppHandle` at
  `runner.go:718`. That is the first statement of `parseConfigurationPhaseTwo`, ahead of
  `CreateMissingDataProducers` (`:729`) and every factory call, so plugin factories can
  reach it. No ordering hazard remains; the earlier one was an artifact of the wrong
  design.
- Open sub-question, not yet decided: what `CapacityLedger()` returns when the flow
  control gate is off. Nil is consistent with `Metrics()`, and a plugin that requires it
  can fail at construction. Confirm with the user rather than picking.

### 2. Adapter plugin

Small plugin in the plugins tree that owns none of the accounting:

- Implements `Registrant`, registering a `PendingRegistration` against
  `sourcenotifications.EndpointNotificationSourceType` with a `DefaultSource`, copying
  `producer.go:272-279`.
- Implements `EndpointExtractor`. On `EventAddOrUpdate`, calls `ledger.UpsertEndpoint`
  with scraped KV capacity and publishes a `DynamicAttribute` closure over
  `Reader.EndpointAvailable(id)`, following `producer.go:302-311`. On `EventDelete`,
  calls `ledger.DeleteEndpoint`. Keep the stale-delete pointer-identity guard from
  `producer.go:290-300`.
- Pulls the `Reader` off the handle in its factory. Declares `Produces()`; does **not**
  implement `Produce()` (no per-request pre-scheduling materialization, and the gate
  needs the footprint before `Produce` runs anyway).
- Needs a `Cloneable` attribute type for the published footprint; see
  `attrconcurrency.InFlightLoad` for the shape.

Where the KV capacity number comes from on the endpoint event still needs checking
against what the metrics extractor actually populates (`CacheNumBlocks *
CacheBlockSize`). Verify before writing, do not assume.

### 3. Flow control integration

- Processor `dispatchCycle`: typed `*ledger.PoolLedger` in `Deps`. When present it
  supersedes the saturation detector outright, for both the saturation read and the
  hold. The `saturationDetector` field stays only for the gate-off path. **This was
  recommended to the user and neither vetoed nor explicitly confirmed. Confirm before
  building on it.**
- Controller `Deps`, runner wiring at `initAdmissionControl` (`runner.go:451`) — passes
  the concrete ledger, no plugin lookup, no defaulting question.
- Director: commit at `runPreRequestPlugins` (`director.go:484`), EOS release in
  `HandleResponseBody`, `ReleaseHold` on the error paths, `ReleasePrefill` at
  StartOfStream (mirror the early prefill release at
  `inflightload/producer.go:447-457`).
- Behavior change to flag in the eventual writeup: moving the ceiling check from before
  `selectItem` to the head item means an empty-but-over-ceiling band no longer stops the
  whole cycle.
- Targeted package tests green in the builder container before claiming done, then
  `make presubmit` if there is room.

### 4. Close-out

- `experiments.md`: mark work-table row 1 done (sources.md reached "read" standing at
  `568afb3a`); rewrite row 2, which still says "extend `InFlightLoadProducer` toward the
  dual ledger" and references "the director-side hold (candidate (a))". Both are stale.
- `seam-map.md`: the "core layer, not a plugin" section is drawn in the wrong place;
  replace with the Handle-service framing above. Also wrong there: the receipt-handoff
  row, and the claim that branching factors are parsed fields (no request parser
  extracts `best_of` or `n`; also wrong in `ledger-revision.md`). The TTL-rejection
  language is stale too.
- Homeless-threads sweep. Open threads noted but not filed: vLLM scheduler-config info
  metric upstream; hold-inflation-under-burst measurement; P/D per-profile lease
  placement (stage 3, deferred when prefill scope was fixed to single-endpoint lease on
  the primary); `InFlightLoadProducer` subsumption.
- Cold read if more than a couple of docs changed. Then tag `s04` **only with explicit
  authorization**.

## Undecided, needs the user

- Receipt-by-value vs request-ID keying. The current code keys by request ID, argued on
  the grounds that the request already carries the ID end to end. The user has not ruled.
- `CapacityLedger()` nil semantics when the gate is off (item 1).
- Whether the ledger supersedes the detector outright in `Deps` (item 3).

## Process rules for the next session

State the why for each design decision as you make it. When a decision is genuinely
open, stop and ask rather than picking silently. One plan, checked in at the API
boundary. Relitigating inherited design decisions is explicitly in scope and encouraged;
ground the answer in the code, not in the shape the previous session wanted.
