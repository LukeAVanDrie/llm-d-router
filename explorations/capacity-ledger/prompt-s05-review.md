# Session 5 prompt: independent review of the capacity ledger

Paste the block below into a fresh session. It is written to be read cold, by a session
with no stake in the design.

The one thing that makes this session worth running is that it has not made any of the
decisions it is judging. Preserve that: do not feed it the prior sessions' reasoning
before it has formed its own.

---

You are reviewing a prototype you did not write, on branch `capacity-ledger` in the
worktree `.claude/worktrees/capacity-poc`. Work from that worktree path.

Constraints, in force for the whole session: fork-only. Nothing merges. No pushes, no
GitHub writes, no comments, no tags, without explicit per-action authorization from me.
Local drafts only.

## What exists

A "capacity ledger": pool-scope multi-dimensional resource accounting intended to
replace flow control's single delayed saturation scalar. Three axes (KV tokens, prefill
tokens, slots). A hold-then-lease protocol: `TryAcquireHold` is the only operation that
can refuse, `Commit` books unconditionally, `Release`/`Revoke`/`Retire` give capacity
back.

The code:

- `pkg/epp/flowcontrol/ledger/` — the accounting core.
- `pkg/epp/framework/interface/capacity/` — the shared vocabulary and the
  plugin-visible interfaces.
- `pkg/epp/framework/interface/plugin/handle.go` — `Handle.CapacityLedger()`.
- `pkg/epp/framework/plugins/datalayer/extractor/capacityledger/` — the adapter that
  feeds endpoints in and republishes the ledger's reading as an endpoint attribute.
- `cmd/epp/runner/runner.go` — construction and wiring.

Nothing is integrated into the dispatch path yet. Flow control still uses the
saturation detector. The director does not call the ledger.

## What I want

An adversarial review of the design and the implementation. Not a code-quality pass:
the question is whether this thing is sound and whether it should exist in this shape
in the EPP. Assume a hostile upstream reviewer on a multi-vendor project with scarce
review bandwidth, who will ask "why is this in the router at all" before they ask
anything else.

Specifically, I want to know:

1. Where the accounting can be wrong, and whether it can be wrong in a way that does
   not self-correct.
2. Where a number in the code is a guess wearing the costume of a measurement.
3. What the design assumes about the engine (vLLM) that is not true, or is true only
   for some configurations. Read the engine's actual behavior; do not take the code
   comments' word for it.
4. Whether the seams are in the right places: the split between flow control, the
   plugin handle, and the datalayer, and whether a plugin-visible interface onto a
   flow-control-owned object is a defensible shape in this codebase.
5. What this costs on the hot path, measured against how the project has treated hot
   paths before.
6. The case for not building this: what a smaller change, or no change, would get, and
   what specifically it would fail to get.

Ground every finding in code you have read. Cite `file:line`. Where a claim is about
runtime behavior you have not observed, say so and say what would settle it. Weight
findings by how much they threaten the design, not by how easy they are to fix.

Argue the other side where the design survives — a review that finds only problems is
as uninformative as one that finds none.

## Reading order, and what to avoid

Start from the EPP itself, not from the exploration's writeups. Read `docs/`, the flow
control package, the datalayer plugin model, and the existing saturation detector the
ledger intends to replace, before you read anything in `explorations/`. The prototype
is only judgeable against what the EPP already does.

Then read the ledger code.

Do not read `explorations/capacity-ledger/self-review-s04.md` until you have written
your own findings. It is the building session's self-critique and it will anchor you to
the author's frame. Once you have your own list, diff against it and tell me what it
missed, what it overstated, and where you disagree.

`explorations/capacity-ledger/handoff-s04.md` is the current status and open questions;
read it when you want to know what is deliberate versus unfinished, but treat "this was
deliberate" as a claim to be evaluated, not a defense.

`explorations/capacity-ledger/STYLE.md` governs anything you write into that directory.

## Output

Write your findings to `explorations/capacity-ledger/review-s05.md` and give me the
summary in the session. Do not change any code this session. If a finding implies a
fix, describe it; do not implement it.

---

## Notes for whoever queues this (not part of the prompt)

- The build session's own top finding is that leases have no TTL and no reaper, so any
  missed terminal signal leaks capacity permanently and monotonically. If the review
  does not find this independently, that is information about the review.
- The open blocker is adapter instantiation: nothing guarantees the adapter exists when
  the feature gate is on, and an unfed ledger reads as fully saturated. Three candidate
  fixes are in `handoff-s04.md`. This decision is mine to make and should not be made
  by the review session.
- After the review lands, the build session that follows should re-plan from it rather
  than resume `handoff-s04.md`'s ordering. That ordering assumes only wiring is open.
