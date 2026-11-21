- [x] Read repository workflow, standing design principles, and duplicate-review skill.
- [x] Reproduce the exact recorded public Execute matrix before production edits.
- [x] Reuse the existing yield validation owner during shared statement admission.
- [x] Preserve canonical SyntaxError and align the diagnostic headline with the reference.
- [x] Use scoped registry cleanup, preserve standalone/named/missing-variable controls, update changelog.
- [x] Run focused regression and affected embedding-frame tests on final source (PASS, 4.949s).
- [ ] Parent: run meaningful focused race checks after source handoff.
- [x] Attempt staged duplicate checker and record its actual result and limitations (analysis remains unavailable).
- [ ] Parent: independent integrated review, canonical release verification, push, and PR.

Duplicate check receipts:
- `python3 scripts/code_intelligence.py check-staged`: exit 1, missing Python dependencies (`/tmp/n152-54-duplicate.log`).
- `python3 /Users/timothysweet/src/NornicDB/.codex/hooks/nornic_checkin.py --advisory`: exit 1, missing `ijson` (`/tmp/n152-54-advisory.log`).
- Configured local-model venv Python running the same advisory hook: exit 1, missing `ijson` (`/tmp/n152-54-advisory-configured.log`).
- No exposed graphify tool. No dependencies installed or graph configuration changed. Source-owner comparison completed in proposal; graph-based candidate analysis remains incomplete. These failed attempts are not a clean duplicate verdict.
- Parent retried the advisory hook using `/Users/timothysweet/.local/share/nornicdb-swarm/code-intel/bin/python`: graph request `/db/nornicdbcode/tx/commit` timed out (`/tmp/n152-54-duplicate-parent.log`). This confirms graph candidate analysis remains incomplete even with the configured dependencies.

Final focused command (exit 0, uncached, `/tmp/n152-54-final-focused.log`):

```sh
GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^(TestNor168YieldWildcardReferenceReproduction|TestProcedureYield|TestProcedureArguments|TestInQueryProcedureCallRejectsImplicitArguments|TestStandaloneCallYieldWhereOrderAndPage|TestFabricEmbedding|TestScopeProcedureRegistration)' -count=1 -timeout=3m
```

This includes all 40 original reference cases, prior-clause wildcard and named
controls, nested and UNION wildcard composition, duplicate named bindings,
procedure argument/yield scope tests, and embedding execution-frame matrices.
`git diff --cached --check` passed. Linker emitted the existing duplicate `-lobjc`
warning. Full suite, race, fresh Neo4j execution, and final release-head rebasing
remain parent-owned; this focused pass is not full release qualification.
