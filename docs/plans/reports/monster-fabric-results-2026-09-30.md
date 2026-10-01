# Fabric Issue Results: 2026-09-30

Worktree: `NornicDB-monster-fabric-0930`.
Branch: `fix/monster-fabric-routing-0930`.
Baseline: `1125684d64ab2c2782791bb87212d070fedc526c`.
Original issue bodies and latest comments for #745, #683, #738, and #668 were
read. Existing dirty changes were preserved and completed. No performance runs.

## Ownership

Return this commit to the coordinator for integration into
https://github.com/orneryd/NornicDB/pull/771. Do not publish a competing PR.
The work uses the shared fabric executor and selected-database admission next
to #771's Cypher routing surface. No server lifecycle/logger fix was added.
The changelog insertion must be combined with #771's own changelog changes.

## Issue Results

| Issue | Observed Result | Change / Residual |
| --- | --- | --- |
| #683 | Fail-before: one/two-write Bolt composite transactions read their writes but COMMIT fails with a canceled participant context. Embedded cancellation regression also fails rollback. Pass-after: both Bolt cases persist the expected nodes; canceled commit still fails and a live rollback discards writes. | Local participant terminal callbacks use `context.WithoutCancel(beginCtx)` rather than retaining statement cancellation. Context values are preserved; normal statement execution and outer terminal-context checks are unchanged. Remote participant lifecycle and original failed-transaction regressions pass in the selected gate. No full-family closure claim. |
| #738 | Fail-before: HTTP accepts `USE system CREATE`, and unknown USE target returns 404. Pass-after: Bolt autocommit/explicit and HTTP implicit/explicit return SemanticError for the system write and store no U717 nodes. Unknown USE target returns DatabaseNotFound, HTTP implicit status 200; missing endpoint database remains 404. | Admission runs on the resolved target before cross-database transaction access checks. HTTP status classification distinguishes endpoint database existence from statement target failure. HTTP transaction creation keeps its existing 201 status. Original family/dynamic-target external replay was not rerun. |
| #668 | Baseline serializer overlay fails all six temporal cases on both HTTP transaction endpoints, including scalar/list/map positions, returning Go field maps instead of text. Current serializer passes; nil duration boundary passes. | Preserve the inherited typed serializer fix: temporal String methods supply ISO text and scalar metadata; recursive list/map conversion remains shared. Selected original HTTP framing/atomicity regressions also pass. No full-family or pinned-reference claim. |
| #745 | Existing embedded composite subquery, HTTP constituent metadata, and real-driver Bolt identity regressions pass before any identity change and in the final race gate. | No production identity edit: the reported constituent provenance failure was not reproduced locally. Obtain the reporter's exact environment/replay before claiming the reopened section fixed or closing the issue. |

## Validation

Every Go check used `-tags noui,nolocalllm` and `-count=1`.

- `/tmp/nornicdb-fabric-0930-current-isolated.log`: initial #683 failures and
  passing existing #745 / inherited #668 checks.
- `/tmp/nornicdb-fabric-0930-context-after.log`: embedded and real-driver Bolt
  #683 regressions pass after the participant callback fix.
- `/tmp/nornicdb-fabric-0930-routing-before.log`: new HTTP #738 regressions
  reproduce both failures. Initial test status/setup assumptions were corrected
  for HTTP transaction creation and endpoint database resolution.
- `/tmp/nornicdb-fabric-0930-routing-after.log`: HTTP #738 tests pass.
- `/tmp/nornicdb-fabric-0930-bolt-routing.log`: real-driver Bolt #738 checks pass
  in autocommit and explicit transactions with no system writes.
- `/tmp/nornicdb-fabric-0930-temporal-before.log`: fail-before proof using Go's
  overlay to supply the unmodified baseline serializer, without reverting the
  dirty source file.
- `/tmp/nornicdb-fabric-0930-original-contracts.log`: all four selected packages
  pass, `FABRIC_CONTRACTS_EXIT=0`. Selector:
  `^Test(Gh(683|745|738|668)_|CompositeExplicitTx_|FailedStatement|CancelledStatement|HTTPTransactionAPIMatchesNeo4j|HTTPExplicitTransaction|HTTPFailedStatement|HTTPTransactionEntityRows|TransactionHTTP|SessionGetExecutorForDatabase_|FabricTransaction_|UseClause|UseCommand)`.
- `/tmp/nornicdb-fabric-0930-race.log`: focused #683/#745/#738/#668 tests pass
  under `-race` in Cypher, Bolt, and server, `FABRIC_RACE_EXIT=0`.
- Changed Go files were formatted; `git diff --check` passed.

## Remaining Limits

No pinned Neo4j container replay, external reporter matrix, full repository
suite, full official TCK, linter run, or new-code coverage percentage was
established in this task. No Docker containers or shared ports were changed.
macOS linking emits the existing duplicate `-lobjc` warning.

Sibling-worktree tests are not discoverable by the VS Code test runner, and
gopls reports the sibling module outside the current workspace. Tagged Go
compilation/test results, rather than those workspace diagnostics, were used.
Shared terminal interference displaced or interrupted several commands; the
incomplete `focused-final` / `http-after` logs are not final pass evidence.
The detached bounded original-contract and race runs above provide final results.

Leave all partially verified issue families open. The coordinator must validate
the combined #771 head after integration.