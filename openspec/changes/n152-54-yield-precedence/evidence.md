# Parent verification of N152-54

Final runtime/test source: `cec091ca7faf788986793ed18d0c4b225cc6ce79`.
Release base: `e8912f801ffece3356ca8ca8d94ba0df61c7ee3d`, including the
recent migration and APOC dispatch fixes. Later evidence-only commits do not
change the checked runtime or test source.

## Reproduction and independent acceptance

The parent independently reproduced the original ticket's 20 failures on
`1a6eb031471688a71966a68147740ead6c43d6cb` (`/tmp/n152-next-baseline.log`).
The subagent preserved its failed first placement, moved the guard before
binding admission, and passed the original matrix and additional controls.
The parent's earlier broad run was canceled (exit 130) during source changes;
it is not qualifying evidence (`/tmp/n152-54-parent-review.log`).

After reviewing the actual code, the parent identified single-call UNION
branches as another composition case. The new public regression failed with
the wrong missing-variable diagnostic on all four parser/route combinations
(`/tmp/n152-54-union-red.log`, 1.080s). Composition now includes both multiple
clauses and multiple UNION branches, using the existing admitted query body.
USE metadata remains separate from clauses and does not invalidate standalone
calls. Nested bodies use the existing syntax visitor. Non-CALL statements do
not take the new walk.

## Current-source checks

- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher ./apoc -run 'Procedure|Yield|Nor168|APOCCall' -count=1 -timeout=5m`: PASS, cypher 20.511s and apoc 0.391s; `/tmp/n152-54-final-integrated.log`. Includes the recently merged APOC dispatch regressions.
- `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^(TestNor168|TestProcedureYield|TestStandaloneCallYieldWhereOrderAndPage|TestInQueryProcedureCallRejectsImplicitArguments)' -count=1 -timeout=5m`: PASS, 4.746s; `/tmp/n152-54-race.log`.
- `make verify-list BASE=origin/release/v1.5.2 FETCH=0`: PASS, selects localization, UI, native libraries, build, test groups, allocations, conformance and differential; docs is path-filtered out.
- `make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization`: PASS selected localization checks; `/tmp/n152-54-final-verify.log`. Other checks were unselected, not passed.
- `TestProcedureDifferentialNeo4j` explicitly audited with `-run '^TestProcedureDifferentialNeo4j$' -count=1 -v`: SKIP because Neo4j connection variables are not configured; `/tmp/n152-54-reference-skip.log`. The ticket's pinned reference was used, not freshly rerun.
- `git diff --check`: PASS. Linker reports the existing duplicate `-lobjc` warning.

## Duplicate review and disposition

The parent reran the unchanged staged advisory checker against the final code
diff with the configured code-intelligence interpreter. The graph request
timed out (`/tmp/n152-54-final-duplicate.log`), so indexed candidate analysis is
incomplete despite the advisory wrapper's successful exit. Current-source
comparison and canonical-owner reuse are documented in proposal.md; no indexed
baseline or clean duplicate verdict is claimed.

Draft candidate. Full repository/native/UI build, allocation ratchet,
conformance, live differential, stress and performance qualification remain
unexecuted. Focused correctness evidence does not establish full release
readiness. Keep task worktrees, branches and evidence for those checks;
preserve shared caches and other active work.
