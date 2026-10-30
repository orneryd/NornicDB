# Conflict dispositions

Rebase stage2 is release69115e8a2; stage3 is original182893c0d; stage1 is old parent4e382ba2. Each original delta is compared before resolution. No test/failure is waived by a textual conflict resolution.

| File | Original delta and current owner | Decision |
| --- | --- | --- |
| pkg/storage/lifecycle/manager.go | Only replaced a nil-context Background fallback with a no-op guard. Current release already removed the nil fallback under context-first caller contracts. | Keep current release source; do not reintroduce alternate nil handling. |
| pkg/storage/schema_unique_values.go | Only replaced nil-context Background with ErrInvalidData. Current release already removed this fallback while migrating all lock callers to owned contexts. | Keep current source; retain current key-building cancellation APIs. |
| pkg/storage/stored_entity_reader.go | Only replaced nil-context Background with ErrInvalidData. Current release requires caller-owned context and no fallback. | Keep current stream owner unchanged. |

These retired historical guards are not accepted-command or DROP corrections; those require current-release public reproductions and integrated fixes separately.

## Current-owner ports reviewed after integration

| Family/path | Historical delta | Integrated disposition and proof boundary |
| --- | --- | --- |
| `stored_entity_publication.go` | Reject nil context before `PrepareEntityChanges`. | Current context-first API requires a caller context and already retired this input/fallback. Preserve required-context API and caller cancellation; do not restore a separate nil path. Full blocking flow census still pending. |
| `bolt/transaction_lifecycle.go` and its tests | Nil-parent guard in the old begin API. | Keep current API requiring parent context plus explicit server work owner, including timeout-timer enrollment/join. Original guard does not replace that lease block. Existing context/owner tests retained; no assertion that nil remains supported. |
| `cypher/apoc_load_export.go` and helper tests | Thread query context into removed inline resolver/HTTP helpers. | Exact release source retained: bound `apocio` capability owns DNS, request, filesystem policy and cancellation; both JSON names share `jsonLoadProcedure`. Existing public cancellation matrix replaces the obsolete test seams. Dedicated current-candidate execution recorded separately; no old HTTP globals restored. |
| `multidb/storage_transaction.go`, tracking engine | Map nil to old `AcceptedWorkContext` and use that for managed writes. | Preserve required caller admission contexts. Accepted completion remains separately owned; the current direct `WithoutCancel` receipt wait is retained. Selected managed/accepted lifetime tests passed; no nil fallback or forwarding helper restored. |
| `badger_transaction.go` retired 692-line block | Context-free transaction read/validation functions adjacent to changed endpoint logic. | Do not restore moved implementations. Current canonical `badger_transaction_graph_reads.go` and `badger_transaction_validation.go` retain context-first methods; the accepted endpoint decision was ported to the latter. Production build and focused/race endpoint controls pass. |
| `replication/raft.go`, HA and storage adapter | Old two-argument application API and work-context substitutions. | Preserve current typed WAL mode and exact own-record publication position, FIFO queue, original caller deadline and physical state-machine completion. No synchronous overflow fallback restored. Accepted transport and ACK-budget tests pass. |
| `badger_mvcc.go`, `badger_temporal_index.go` | Work-lifetime lease plus optional nil fallback. | Keep the existing engine work lease without nil fallback. Unchanged release pruning after Close panicked; current candidate rejects safely. The operation lease prevents retirement while an admitted maintenance call uses engine state. |
| `badger_transaction_publication.go`, `buffered_write_order.go` | Accepted validation bypass and pre-decision endpoint check. | Ordinary client checks retained. Accepted replay is not a second client admission. Direct order is already held/validated before preparation; only buffered preparation acquires mixed stripes. Independent duplicate replay failed before the body-admission correction and passes afterwards; ordinary endpoint rejection controls and three race repetitions pass. |

The read-only original-caveat audit is preserved externally under `nor288-current/original-caveat-audit.md`. It is an inventory, not approval. Several statuses in its snapshot predate the parent’s subsequent runtime checks; current receipts supersede only the specific exercised cases. No historical failed review or cost result is represented as a current pass.
