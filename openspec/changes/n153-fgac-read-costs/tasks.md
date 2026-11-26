# FGAC read costs F1-F5 (release/v1.5.3)

- [x] Reproduce the five public paths on the release line and retain the supplied 100k latency, heap, and plan evidence; add matched F1-F3 benchmarks at the exact PR base.
- [x] F1: seek indexed candidates only when the principal's indexed property and candidate population can be read; hydrate and filter through the scoped graph. Cover admin, readable, hidden, conditional, and denied cases.
- [x] F2: admit a restricted property-vector population before scoring, then reuse graph/policy-bound admission decisions; preserve exact ordering, filtering, and no hidden score or count leakage.
- [x] F3: reuse full-text document views only across a proven stable graph and policy revision, including unrestricted admin sessions; invalidate on mutation or policy change.
- [x] F4: scope result cache entries to the correct principal, database, policy and graph revisions; keep transaction, mutable policy and time-dependent paths uncached.
- [x] F5: stream FGAC Bolt reads with policy validation at every PULL and bounded memory; preserve cancellation and policy-change invalidation.
- [x] Review shared index, cache, and stream owners; integrate all five fixes on one branch. CI owns the staged duplicate checker per maintainer direction.
- [x] Run focused and affected-package checks, security isolation tests, performance comparisons, and document remaining costs and verification limits.
- [x] Open one PR into release/v1.5.3 with exact head/base evidence and the AC map (#2010).
