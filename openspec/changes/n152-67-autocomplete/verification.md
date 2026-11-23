# Verification and remaining gates

Base: `f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210`. Tests ran on macOS with
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, Go `-p=1`, and `noui,nolocalllm` unless noted.
No rebase; the target advanced to `e17003df6f5d3621db95302fb14e7c701753790c`.
This is focused evidence for this branch, not combined release qualification.

| Check | Result |
| --- | --- |
| Exact fresh ticket public HTTP witnesses | RED: two failures, 0.525s; original request/ordinary/framing controls pass |
| Actual bundled action route | RED: unknown action HTTP500, 0.606s |
| Both-parser witness validity | PASS 1.020s before fix; focused GREEN 1.164s; race 3.069s |
| Cross-stage source preservation | RED: 15 corruption cases fail; five outside-quote controls pass, 0.480s |
| Public generation parameters | RED 0.477s before removing unsafe text stops |
| Final full Heimdall/plugin normal suites | PASS 10.822s / 0.779s; extractor 100% statement coverage; package coverage 89.7% / 40.2% |
| Final focused Heimdall/plugin race | PASS 1.631s / 1.726s; no race warnings |
| Actual native stop-helper and existing controls | PASS 0.330s, `-tags=noui`; eight unsafe-marker controls plus nil-stop streaming preservation; no model loaded |
| Vet | PASS Heimdall/plugin with `noui,nolocalllm`; PASS native localllm with `noui` |
| Actual browser editor | RED one trailing-backtick case / two controls pass; GREEN seven cases, 15.8s, Chrome with current app build |
| UI unit suite | PASS 48 tests |
| Verification plan | `make verify-list BASE=origin/release/v1.5.2` PASS; runtime owners selected, docs path-skipped |
| Scoped localization | `make verify ONLY=localization BASE=origin/release/v1.5.2` PASS (2s); all other owners explicitly skipped |

Focused Go pattern for final race: `TestNOR281|TestAutocomplete|TestHeimdallCoverage_HandlerAutocompleteAndHelpers|TestWatcherPlugin_Actions`.
Full normal package command: `go test -p=1 -tags=noui,nolocalllm ./pkg/heimdall ./plugins/heimdall -count=1 -timeout=5m -coverprofile=/tmp/n152-67-final-coverage.out`.
Native helper pattern: `TestAutocompleteQuotedTextStopBoundary|TestSplitVisibleTextForStopTokens`.
Browser command: `npm run test:browser -- query-autocomplete.spec.mjs --workers=1`.

## Failures and incomplete evidence

- `./internal/cyphertext` normal tests fail at the unchanged baseline minimum
  `pkg/cypher/executor_mutations_delete_guard.go:deleteExpressionRootIdentifier`.
  The minimum still names it, but the committed source removed it in
  `953379035` (#1921). Failure occurs before discovery output or new-consumer
  comparison. The new extractor and affected handler dispositions are manually
  derived from the existing selector and source; census acceptance is pending.
  No unrelated minimum or checker assertions were modified.
- Staged duplicate check failed at localhost7474 analysis/ingestion before
  candidate reporting. DeepSeek graph retrieval was also incomplete. Manual
  source review converged backend/UI framing and reused the shared lexical
  owner, but does not substitute for an indexed pass. No hook bypass.
- An attempted scoped verifier used environment `VERIFY_ONLY=localization`,
  which Makefile overwrote from its `ONLY` argument. It ran localization/UI,
  skipped Linux native hydration on Darwin, and entered build. Its task-owned
  process tree was cancelled (exit143). Logs and generated-file patch were
  archived; task-generated README/version/lock changes were restored. The
  corrected scoped command above passed. The cancelled full run is incomplete.
- Full release suite, allocation ratchet, conformance, pinned Neo4j differential,
  Linux execution, actual inference and CI are not qualified by this work.
- Historical fc79 receipt remains unavailable: exact fresh ticket witness bytes
  were replayed, not the missing historical probe. Keep that AC open.

## Evidence and cleanup

Durable archive: `/Users/timothysweet/.local/share/nornicdb-release-evidence/n152-67-20261121`.
It retains original witness sources, prior proposal patch, logs, partial/final
`.verify`, final patch/head/source hashes and native artifact hash. Native tests
used a copy-on-write clone of the existing macOS static library; the shared
artifact was not modified. UI dependencies initially cloned the prior proposal's
installation after matching lockfile hashes; no dependency change is included.

Retain the open PR worktree/branch, review dependencies and shared Go cache.
No worker worktree, Docker image or isolated Go cache was created. Required
review evidence is preserved; disposable commit/review scratch files may be
removed after publication. Publication does not establish merge readiness.
