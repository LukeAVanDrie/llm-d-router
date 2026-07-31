# Working rules for explorations/capacity-ledger

Scope: everything under `explorations/capacity-ledger/` on this branch. The repository's
CLAUDE.md still governs code style, commits, and comments; nothing here overrides it. These
rules exist because this directory holds research artifacts whose value is their epistemic
reliability: a reader (including a future session) must be able to tell what is known, how
it is known, and what is merely believed.

## The numbers rule

Every number is derived, measured, or guessed, and its home says which.

- Derived: show or link the derivation. A committed script is a derivation; a number in
  prose that no committed script produces does not get committed.
- Measured: name the run that produced it. Experiment results cite the script and the
  seeds; the run must be reproducible from what is committed.
- Guessed: mark it "guess" (or "convention") at its definition site. Guessed constants get
  one canonical home (a params file or the defining docstring), not a label at every
  mention.

## Standing of claims

Prose states each claim's standing in the sentence, not in a marker:

- Published work: attributed, with the source's own state tracked in
  [sources.md](sources.md). A claim leaned on carries a locator once the source is read;
  until then the sentence says the standing is machine-read or recalled.
- Derived here: cite the script or the code path (`file:line`).
- Assumed or untested: say so where the claim appears. "This is untested" is a complete
  and acceptable sentence.

## Verdict rules precede numbers

A script or document that will issue a verdict states its decision rule before the first
result-bearing run: kill criteria in the pre-registration document, verdict logic in the
script's docstring. Git history is the proof of ordering. Amendments after first results
are allowed only with the reason and the direction of the change (whom it favors) recorded
in the same document before the verdict run.

## Machine-read standing

Summaries produced by fetching and machine-summarizing a source (web fetches, search
snippets) and reviews produced by other models are machine-read: useful for leads and
frictions, never upgraded to "read" by specificity or confidence. Claimed contents of
unread sources are not folded into design documents as fact; they enter as machine-read
claims pending verification, tracked in [sources.md](sources.md). This applies equally to
this assistant's own paper summaries.

## Hanging threads

Open questions with no home in a committed artifact go to the threads section of
[experiments.md](experiments.md) before a session ends, each naming the question and what
it waits on. Threads are triaged periodically: a thread re-earns its line or is deleted,
with the deletion explained in the commit body.

## History

Documents do not narrate their own drafts (repository policy, restated here because
research notes are where it slips): no "previously", "it turned out", "we expected".
Corrections rewrite the text; the commit body explains the change.
