# Row subquery adapter context equivalence

Contract: compiled row programs cache syntax, not execution context. EXISTS,
COUNT and COLLECT use the statement context carried by the row, just as the
text adapter does. A missing statement context stays unresolved; adapters do
not invent one. Body failures retain their original identity.

- [x] Read standing principles, Rule 73, CONTRIBUTING and retained integrated RED.
- [x] Trace text/compiled adapters to `rowSubqueryValue` and establish the row-owned context contract.
- [x] Retain fail-first evidence and converge the adapters without changing regression assertions.
- [x] Qualify public paths in both parser modes, request values/cancellation, body error identity, and unchanged controls with focused normal/race tests.

Qualification: 61 top-level tests / 585 pass records including subtests, no skips,
in each fresh normal (6.137s) and race (36.138s) run. Public tests cover both
`nornic` and `antlr`, text/program evaluation, direct/nested subqueries, changing
parameters and correlation, and unchanged body errors. A cache assertion proves
that public nested execution actually compiled through the row-program owner.
The original two failing tests were not edited.

Scope excludes scanner, fingerprints, storage, auth and knowledgepolicy.
Broad verification and CI are deferred by the caller; no commit or push.

## Endpoint context follow-up

The corrected real-edge, below-namespace fixtures exposed swallowed endpoint
point-read errors, not replacement of operation context. A canceled endpoint
read became null and was cached before outer cancellation reporting. Both
full-node and scalar-property row paths must preserve the canonical edge/node
read error; missing entities remain null.

- [x] Trace fixture capability hiding versus real read/retry behavior with read counters.
- [x] Retain full/focused RED and fail-first full-node/property consumer tests.
- [x] Propagate canonical edge/node read errors across optimized and ordinary endpoints.
- [x] Qualify both public parser modes, text/program evaluation, request cancellation,
  same-executor retry, metadata ownership, and unchanged endpoint/row/subquery controls.

Endpoint qualification: fresh normal 7.338s / race 42.587s, each 84 top-level
tests and 774 pass records including subtests, no skips. This reruns the earlier
subquery controls against the integrated endpoint correction. The two original
endpoint assertions remain intact; the corrected fixture adds a read counter to
prove that a failed read did not publish a cached null. USE benchmark source was
frozen before this follow-up, so it does not directly qualify this source state;
no new performance claim or benchmark is made.
