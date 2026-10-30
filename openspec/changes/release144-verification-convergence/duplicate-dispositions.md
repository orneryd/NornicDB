# Disposition of the last duplicate inventory

The original report analyzed staged tree
`754e831bd5ad57db955dc399cc37655bb96d178f` against release graph
`cd67390c0e4aacf8a12ea34a141d752d637dd08f`: 432 functions, 294 ownership
proofs, nine findings, no cap skips or warnings. That is a completed inventory,
not a clean check. This follow-up starts from merged release
`0c0495b6db64453ed966923e0c8d114956ccdc50`.

The checker is advisory by explicit maintainer instruction. Findings remain
visible even when the correct solution keeps separate entry points. No analyzer
threshold, scope, exclusion or proof rule is changed here.

| # | Finding | Decision and specific reason |
|---|---|---|
| 1 | `transactionOwnedStorage.SubscribeEvents` / `algorithmRelationshipReader.SubscribeEvents` | Retain required interface adapters. Both call the single `storage.SubscribeEvents` owner. Removing either method loses `EventSubscriber` because an embedded `Engine` does not include that optional capability. Replacing explicit forwarding with generic unwrapping changes the existing fail-closed capability contract and can bypass namespace event translation. The transaction and adjacency-filter views have different storage responsibilities; merging their concrete types would mix transaction ownership with relationship filtering. |
| 2 | `callDbInfo` / `callDbmsInfo` | Converged into typed `callInfo`. Both registered procedures call it directly; the old private functions are removed. Database counts, context-selected name, system-database selection, null identity and UTC timestamp projection retain their existing contracts. No callback or extra intermediate row representation is introduced. |
| 3 | Generic reservation `current` / previous edge-only receiver | The edge-only receiver was already retired by merged PR #1638. Current source has one generic `identityMutation[ID].current`, with typed node and edge registries. Combining the registry *instances* would let the same node/relationship ID supersede the wrong kind of mutation. The previous indexed receiver is not a second current implementation. The graph refresh and same-ID domain regression establish the current population without an analyzer-rule change. |
| 4 | Vector-index / ordinary-index construction | Converged into `executeCreateIndex` in `index_creation.go`: one modern parse/admission path and entity/name selection, with explicit kind-specific persistence and post-commit vector registration. VECTOR still requires a name and exactly one property; node/relationship and disabled-vector behavior remain distinct. RANGE no longer rewrites its query text. The old vector/range functions are removed and all callers migrated; supported Neo4j `ON :Label(properties)` syntax remains on its existing property-index path. |
| 5 | `executeTransactionBatch` / `executePeriodicBatch` | Retain their different transaction admission/completion policies; both execute through the existing `executeWithImplicitTransactionCallback` owner. Transactional CALL rejects prior outer writes and requires an empty snapshot before advancing it. Shipped periodic procedures fold into an explicit native transaction; in autocommit they retain pending outer writes and resume after their independent commit. A common admission/snapshot policy would either reject supported periodic calls or commit/retain writes that transactional CALL must reject. |
| 6 | Cypher / DB `PrepareSearchRefresh` | Retain policy adapters around the single generic `search.RefreshIndexes` mutation owner. Cypher must defer uncommitted changes and use one commit-captured service, including a meaningful nil capture. DB-level refresh must never defer: it waits for the initial index build and returns resolver/cancellation errors. Re-resolving the Cypher capture can partially publish a batch; using transaction deferral or bypassing build wait for DB deletion can leave stale entries. The policies depend on different lifecycle state, not two implementations of the index mutation. |
| 7 | `linkPredictionGraph` / `buildGraphProjection` | Retain live prediction and named projection import responsibilities. Named prediction already consumes the projection snapshot. Forcing unnamed prediction through full projection materialization was tested and rejected: on the recorded 1,000-node/5,000-edge workload, median bytes rose 113.04%, allocations 80.80%, and time 5.23%. That experiment verified graph/degree equality on its named topology, not all filters, snapshots or cancellation cases. It demonstrates the cost of this particular collapse, not that every possible redesign must be slower. No extra intermediate representation is shipped. |
| 8 | `executeViaFabricWithTx` / `prepareFabricExecution` | Retain reusable planning versus per-execution lifecycle ownership. Execution already calls the preparation owner when no prepared plan is supplied. The ordinary execution entry also prepares once for result-cache lookup and passes that same plan into dispatch. Combining planning with transaction creation/enlistment would perform effects before cache and authorization decisions or require replanning a cache miss. Prepared catalog/fragment state must not capture one execution's transaction or cleanup. |
| 9 | `withRunningStatement` / `executeShowTransactions` | Retain registry mutation versus observation. Statement registration owns cancellation and completion; SHOW snapshots existing registrations and projects rows. Registering inside the observation owner would double-register the SHOW statement or retain an observer in the process-wide registry. Conversely, projecting SHOW results during ordinary statement registration adds work to every query and cannot replace transaction-filter/version-specific observation. Both use the same `runningTransactions` registry. |

## Contract evidence

Focused integrated checks cover:

- event delivery exactly once through each view and unsupported-capability errors;
- database/DBMS result columns, counts, selected name, missing identity and UTC dates;
- node/relationship reservation domain separation and comparable tokens;
- Cypher commit/rollback deferral and captured nil service, DB build-wait cancellation
  and resolver errors, and the shared refresh operation's error/order semantics;
- explicit periodic folding, independently committed batches and transactional CALL
  rejection after prior explicit writes;
- observing and terminating explicit and running autocommit transactions without
  losing rollback or derived-context cancellation.

The refreshed graph and final verification results will be recorded separately.
The historical graph-materialization experiment remains workload-scoped evidence;
it is not a substitute for the full release verification.

## Advisory review of the follow-up

The first follow-up sweep on staged tree
`29dd5ca2c329b24d646c4b5ee8c0e76eb8f540ae`, against the refreshed release
graph, analyzed 15 functions with no cap skips or warnings. The production
consolidations use existing scanner/admission owners. Two further findings identify
shared test-resource construction across five schema branch/error tests. Their
engine/namespace/executor setup now uses the existing `newTestExecutor` owner
while preserving all original namespaces, operations and assertions. These are additional findings, not replacements
for the nine historical dispositions above.

The local advisory entry point initially failed while serializing its *summary*
because it returned Finding objects instead of their counts. The complete structured
analysis report had already been written and was preserved. Only the local entry-point
summary was corrected; analyzer decisions and the report contents were not changed.
An advisory error is not counted as a clean check.
