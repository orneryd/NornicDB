# N152-54: admit procedure wildcard composition before binding scopes

## Problem and contract

The recorded NOR-168 Neo4j 5.26.30 reference rejects composed `YIELD *` with
``Cannot use `YIELD *` outside standalone call``. On release base
`1a6eb031471688a71966a68147740ead6c43d6cb`, the exact public Execute matrix
reproduced 20 failures: 16 diagnostic wording failures and four cases where
`WITH missing RETURN missing` obscured the wildcard rule with a missing-variable
error. No rejected custom invocation reached its handler.

Both parser modes and direct/composite routes must reject before downstream
binding checks and handler execution. Standalone wildcard calls, named yields,
and genuine missing-variable diagnostics after named yields remain unchanged.
The assertions use the recorded reference headline, not Neo4j source positions.

## Implementation and reuse investigation

`admitStatement` is the existing shared statement admission owner.
For statements whose existing effects record calls, its admitted syntax visitor
reads query and nested subquery bodies through `queryBodyFromSyntax`; procedure clauses call
`validateProcedureYieldBindings` before the retained binding program. Composition
uses the already parsed branch clause list, covering preceding and following
clauses and UNION branches; no extra parser or query-text special case
is introduced. The existing validation owner keeps its SyntaxError classification
and emits the reference headline.

At the base revision, `executeProcedureCall` and `pipelineApplyProcedureCall`
already invoke the same owner after admission, before handlers. These are later
execution boundaries, not competing implementations: retain their guards and
reuse their owner during admission. `semanticScopeWalk` handles yielded static
types; it is not a second composition-rule owner. The existing embedding frame
matrix's diagnostic assertion follows the canonical message. The reference
matrix uses `registerProcedureForTest`, which restores only its own registry
entry rather than clearing unrelated user procedures.

## Evidence and limits

Baseline exact reproduction: `/tmp/n152-54-baseline.log`, FAIL, 1.104s runtime.
The first attempted placement in `validateStatementTypes` was too late: the
binding program runs before deferred type validation. `/tmp/n152-54-focused.log`
preserves those four remaining precedence failures (4.605s). That superseded
change was removed; the complete correction lives in `admitStatement`.

The intermediate top-level admission correction passed the exact matrix and
scoped controls in `/tmp/n152-54-focused2.log` (5.183s). Nested and UNION
composition controls were then added and the admitted syntax visitor extended
to nested bodies; that intermediate pass does not qualify the final source.

Focused post-fix verification and duplicate checker receipts are recorded in
`tasks.md`. The regression file preserves the pinned reference image digest and
all recorded controls; reference Neo4j execution was not repeated in this task.
Full release verification is owned by the parent task and is not established by
these focused tests. This is a semantic correctness fix, not a performance claim.
