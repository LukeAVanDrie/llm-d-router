# Self-review: the ledger design and the stage-4 implementation

Adversarial pass over the design and the code as of `a10f60a8`, written by the session
that built it. That is its limitation and the reason it is not called a review: an
author auditing their own work finds the flaws they already half-knew about and is
blind in exactly the places the design's premises are blind. It is recorded as a diff
target, not as an assessment.

An independent review should form its own findings first and only then read this file,
to see what the author could not reach. See `prompt-s05-review.md`.

Findings are ordered by how much they threaten the design, not by how hard they are to
fix.

## 1. Leases never expire; holds do

`TryAcquireHold` stamps `expiresAt` and `sweepExpiredLocked` reclaims it. A committed
lease has no equivalent. `Release`, `Revoke`, and `Retire` are all caller-driven, so
every lease depends on the director observing a terminal signal for that request.

Any missed terminal signal leaks the lease permanently. A stream that dies without EOS,
a panic on the response path, a client disconnect the EPP does not translate into a
release, a bug in a future refactor of `HandleResponseBody`: each one removes capacity
from the pool for the lifetime of the process and nothing ever gives it back. The leak
is monotone. Slots is the gated axis, so the observable consequence is a pool that
admits progressively less over hours and eventually refuses everything, with the ledger
reporting itself perfectly healthy and internally consistent the whole time.

This is worse than the problem the ledger was built to solve. The delayed-saturation
scalar it replaces is wrong in both directions and self-corrects on the next scrape; a
leaked lease is wrong in one direction and never self-corrects.

The zero-sum discipline makes it sharper, not softer. "Underflow is an error, never
clamped" is the right invariant for catching accounting bugs, and it is exactly what
prevents any sloppy-but-safe reconciliation from papering over the leak.

The design needs a reconciliation path: a lease TTL, or periodic truing-up against
scraped engine occupancy, or both. The exploration already parked "EPP-restart ledger
rebuild as a reconciliation open" — this is the same hole in the steady state, and it
is the one that decides whether the ledger is deployable at all.

## 2. The one gated axis is gated against a number we invented

`DefaultConfig` gates on slots at `SlotsPerEndpoint = 256`, chosen because that is
vLLM's `max_num_seqs` default. The engine exports no metric for it.

If an operator runs `max_num_seqs=64`, which is ordinary for large models, the ledger
admits four times the engine's real concurrency limit and refuses nothing. The gate
then supplies false assurance: the pool looks governed, the metric reads well below
1.0, and the actual queueing has moved into the engine where the ledger cannot see it.
There is no way to detect the mismatch from inside the EPP.

The honest framings are: make the value required configuration with no default, or ship
the axis in shadow like the other two until the value can be obtained. Shipping it
gated against a guess is the weakest of the three, and it is what the code does.

The upstream reviewer's version of this objection is shorter: "where does 256 come
from, and what happens when it is wrong?"

## 3. An empty pool reads as saturation 1.0, conflating two states the layer already
distinguishes

`Saturation()` returns 1.0 when no gated axis has a positive limit, which includes the
zero-endpoint case. But flow control already separates these two conditions and depends
on the separation: `translateFlowControlOutcome` maps a queue-wait TTL to 429 when the
pool has endpoints and 503 when it does not, and `Processor.poolEmpty` exists precisely
to carry that distinction. A saturation scalar cannot express it, so routing the signal
through `Saturation()` discards information the error taxonomy needs.

This is not hypothetical: the adapter-instantiation blocker recorded in the handoff is
the same defect seen from the other side. A wiring gap that leaves the ledger unfed is
indistinguishable, at this interface, from a genuinely saturated pool.

Fail-closed is the right instinct for an admission gate. Encoding it as a magic value
on a continuous scalar is not.

## 4. Per-dispatch and per-read linear scans under one exclusive mutex

`PoolLedger.mu` is a `sync.Mutex`, not `RWMutex`, and every operation takes it
exclusively, including the two pure reads.

- `TryAcquireHold` scans all endpoints for the at-least-one-fits check, and sweeps all
  holds, on every dispatch.
- `Saturation()` sweeps all holds, on every dispatch cycle.
- `EndpointAvailable` is published as a `DynamicAttribute`, so every scorer read of
  every candidate endpoint takes the pool-wide exclusive lock. That is O(endpoints)
  exclusive acquisitions per scheduling cycle, contending directly with the dispatch
  loop's holds.

This repeats a mistake the project has already paid for once: the `IterateQueues`
hotspot, where a per-dispatch O(F) snapshot accounted for the overwhelming majority of
flow-control benchmark allocations. The shape is the same — a linear pass on the hot
path that looks negligible at three endpoints and is not at three hundred.

There is no allocation here, which is the one thing in its favour. But the lock
coupling is arguably worse than the scan: the design deliberately puts scheduling-time
reads and admission-time writes on the same lock, and the dynamic attribute makes the
read side unbounded in frequency.

## 5. The dual-check invariant is asserted, not tested against adversarial placement

"Aggregate room with no feasible placement is not admissible capacity" is the right
principle. The implementation checks aggregate-at-ceiling, then at-least-one-endpoint
fits at raw limits.

But the hold is pool-scope: it records no endpoint. So the endpoint that satisfied the
fit check is not reserved, and by commit time the request may land elsewhere. Under
concurrent admission, N requests can each pass the fit check against the same single
endpoint with room, then all commit to it. The aggregate check bounds the total, but the
per-endpoint feasibility the check claimed to establish is not carried forward to
anything.

So the second check is weaker than its comment implies. It proves a placement existed at
hold time, not that one exists at commit time, and nothing in the protocol closes that
gap. Whether that matters depends on how far the scheduler's placement can diverge from
the ledger's view, which is unmeasured.

## 6. Smaller things a reviewer would still catch

- `TryAcquireHold` clamps `ceiling < 0` but not `ceiling > 1`, so a misbehaving
  `UsageLimitPolicy` can scale pool limits above real capacity. The asymmetry is
  arbitrary.
- Ceiling scaling truncates: `int64(float64(limit) * ceiling)`. Small limits times a
  small ceiling floor to zero, so the last fraction of capacity is unreachable rather
  than partially available.
- `Prediction.BlockSize` is carried, stamped at commit, and used by nothing, because the
  stage-2 translation is token-denominated. Dead field until block-granular translation
  lands.
- The adapter hard-fails construction when the handle has no ledger, and is registered
  via `RegisterAsDefaultProducer`. No other default producer's factory fails on handle
  state; whether `CreateMissingDataProducers` surfaces that error legibly or as a
  generic "no default producer found" has not been checked.
- `Metrics.KvCacheMaxTokenCapacity` is declared and cloned but populated by no
  extractor. Not ours, not to be fixed here, but it is the field a future reader will
  reach for instead of `CacheNumBlocks * CacheBlockSize`.

## What survives the review

The core claims hold up. Reservations the engine has not yet reported are real and
unobservable in scraped metrics; a hold-then-lease protocol is the right shape for them;
the multi-axis footprint is justified, and the prefill-as-stock argument in particular
is well grounded in engine source. The placement decision (handle service, not a plugin)
is correct and now well evidenced. The gated/shadow split is the right way to ship an
axis whose limit is not yet trustworthy.

What does not hold up is the claim that the current implementation is closer to
deployable than the mechanism it replaces. Finding 1 alone means it is not, and findings
2 and 3 mean the parts that do run are governed by numbers and states the design has not
earned.

## Consequence for the plan

The stage-4 plan's ordering was: resolve adapter instantiation, then integrate flow
control, then close out. That ordering assumes the accounting is sound and only the
wiring is open. It is not. Finding 1 is a design hole, not a wiring gap, and integrating
the director's commit and release paths without a reconciliation backstop builds the
leak into the system at exactly the moment it becomes reachable.

Reconciliation should come before, or with, the director hooks.
