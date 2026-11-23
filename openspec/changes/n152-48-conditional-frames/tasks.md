- [x] Preserve fail-first proof that broad recursive CALL relaxation accepts a previously rejected user nested CALL.
- [x] Restore baseline CALL policy and preserve guarded conditional lowering.
- [x] Retain trusted generated-CALL provenance across source rewrites/windows; allow recursion only through those frames.
- [x] Prevent relaxed semantic verdicts leaking through text-only caches.
- [x] Verify original reference matrix on both parsers and normalization settings, user-written/pasted frames, nested lowering, quoted inputs, and aggregate guards.
- [x] Record manual canonical-owner/duplicate analysis and focused verification evidence; parent owns checker, broad qualification, push and PR.

Base: `18b36a85d10e775b9d5d3500ecf3482682754a8d`.

Fail-first command against the superseded broad helper:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestConditionalFixPreservesUserNestedCallRejection$' -count=1 -timeout=3m
```

FAIL, 1.116s: expected rejection but got nil (`/tmp/n152-48-user-nested-red.log`).

Initial focused command after the narrow production correction:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./internal/cyphertext ./pkg/cypher -run '^(TestSpanInAncestor|TestConditional|TestCallSubqueryColumnsCannotRedeclareOuterVariables|TestCallSubqueryReturnsOuterUnchangedBranches|TestQueryStructureMatchesNeo4j)' -count=1 -timeout=4m
```

PASS: source mapping 0.312s, Cypher 1.442s (`/tmp/n152-48-focused-first.log`).
This includes the original 19-case reference matrix on both parsers with normalization
on/off, existing conditional tests, quoted inherited values, nested/chained conditions,
inactive aggregate guards, and both generated/pasted execution orders. Production
source stayed unchanged afterward; added tests strengthen user-written CALL rejection
inside conditional frames and verify exact cached-AST/source-window provenance.

Final targeted race run includes those added tests:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./internal/cyphertext ./pkg/cypher -run '^(TestSpanInAncestor|TestConditional|TestCallSubqueryColumnsCannotRedeclareOuterVariables|TestCallSubqueryReturnsOuterUnchangedBranches|TestQueryStructureMatchesNeo4j)' -count=1 -timeout=5m
```

PASS, exit 0: source mapping 1.297s, Cypher 5.202s (`/tmp/n152-48-focused-race.log`).
The added tests retain user nested-CALL rejection inside conditional arms across
both parser/normalization settings and distinguish the two generated CALL tokens
from an arm's user-written CALL after a rewrite/window, including reuse of the same
cached syntax without trusted context. `git diff --check` passed. The linker emitted
its existing duplicate `-lobjc` warning. No tests were canceled in this agent's runs.

These are focused, fresh runs, not full-suite or release qualification. The fresh
Neo4j reference run, performance profiling, required duplicate graph/checker,
canonical verifier, final-head rebase, commit, push and PR remain parent-owned;
none is represented as passed by this task. Original fixture reference expectations
were retained from the ticket. Manual owner/reuse review is documented in proposal.md.


Parent review: inspected all changed production owners. The final scoped verifier
`make verify BASE=origin/release/v1.5.2 ONLY=localization` passed (7s); UI, llama,
build, full tests, allocations, conformance, differential and docs were skipped.
The staged checker ran and reported incomplete advisory analysis because the
code-graph transaction endpoint timed out. Manual source/caller review does not
replace that missing graph discovery; this remains a draft review candidate.

Pristine release recheck after implementation: a detached worktree at the exact
base with only the original reproduction file copied ran
`TestConditionalOutputReferenceMatrix|TestConditionalArmReturnsInheritedVariable`.
Exit 1, 1.125s (`/tmp/n152-48-release-red.log`): all 19 reference/control entries
passed across four parser/normalization lanes (76 leaves); the reported inherited
column, chosen ELSE, and two-inherited-column variants failed all four lanes
(12 leaves), with `Variable kept already declared in outer scope`. This is a
fresh base recheck, not a claim that it preceded implementation. Final focused
and race runs above pass those same reproduction variants.
