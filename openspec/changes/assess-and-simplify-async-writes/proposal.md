# Proposal: Assess and simplify the async write path

## Why

The implicit write path in `pkg/cypher/executor.go` picks one of **three**
routes per write statement, based on a keyword scan and a schema scan:

1. `tryAsyncCreateNodeBatch` (`executor.go:1629`) — plain `CREATE` of nodes
   only, on labels without write rules.
2. `executeWithoutTransaction` — `isEventualAsyncEligible` shapes on labels
   without write rules (no statement atomicity).
3. `executeWithImplicitTransaction` — everything else, including **any** write
   to a constrained label; `beginTransactionSnapshot`
   (`transaction_admission.go:8`) runs `FlushBeforeSnapshot` (synchronous
   flush), executes against a `BadgerTransaction`, commits to Badger, appends
   WAL markers and a receipt, then calls `asyncEngine.Flush()` again
   (`executor.go:2052`).

Consequences observed on main (`7412533a`):

- A schema that constrains every label (Soraban-style) never uses the async
  cache and pays two flushes plus a Badger commit per statement.
- Three routes carry different atomicity guarantees, chosen by query shape —
  exactly the divergent execution the cypher convergence work has been
  deleting elsewhere.
- Durability is already provided by the layers below the cache: Badger runs
  `SyncWrites=false` with grouped commits (`badger.go:756-763`), and the WAL
  defaults to batch sync every `WALSyncInterval` (100 ms). The async cache is
  a third buffering layer on top; its crash-loss window is **added** risk, not
  added durability.
- Northwind 1.4.1 measured NornicDB seeding ~18k nodes/s vs Memgraph ~61k
  nodes/s (3.4×), with NornicDB ≈ Neo4j seed time — the harness itself may be
  a shared bottleneck, so ingest costs must be measured in-process.
- A Soraban-style constrained load ran ~1 s per batched `UNWIND` statement for
  thousands of rows: the cost is per-row execution, not fsync.

None of the three routes has been measured against a single-transactional-path
baseline. This change establishes that baseline before any code is deleted.

## What Changes

1. **Measure first** (step 0, no behavior change): in-process ingest matrix
   across durability configs and constraint coverage, plus CPU/allocation
   profiles of the constrained sync path.
2. **Decision gates** (see design.md): the owner picks one of
   - **G1 — remove async routing**: collapse `executeImplicitAsync` to one
     transactional route; delete `tryAsyncCreateNodeBatch`,
     `isEventualAsyncEligible`, and the `writesAreChecked` gate; remove or
     bypass the `AsyncEngine` on the write path (read path and search/embedding
     consumers audited separately). Then tune Badger and WAL, and only re-add
     an async shim if a measured, atomic bulk path earns it.
   - **G2 — keep async, make it correct**: proceed with the staged-transaction
     design in `docs/plans/always-async-writes-plan.md`.
3. **Write-path tuning** (under G1): Badger options (conflict detection,
   memtable/table sizes, compaction), WAL group commit, per-shape bulk CREATE
   planning through the existing atomic pipeline planner (#908 convergence
   already pools adjacent CREATE clauses), never a divergent interpreter.
4. **Specs**: lock the single-route invariant as observable requirements with
   WHEN/THEN scenarios (specs/write-path-routing/spec.md).

## Impact

- Affected code: `pkg/cypher/executor.go` (routing), `pkg/cypher/transaction*.go`
  (admission), `pkg/storage/async_engine.go` (removal or read-only role),
  `pkg/nornicdb/db.go` (engine stack), `pkg/storage/badger.go` (options),
  `pkg/config/config.go` (documentation of knobs).
- Constraints, atomicity, receipts, explicit transactions and read semantics
  are preserved; only which storage layer a write lands in may change.
- Compatibility: no Cypher or Bolt surface changes. Async-mode durability
  contract (`X-NornicDB-Consistency: eventual`) either disappears under G1 or
  is re-specified under G2.
- Related: PR #1027 (constraint fixes, merged), cypher convergence work
  (#908/#907 routing ownership), #911 throughput umbrella.

## Capabilities

### Modified Capabilities

- `storage-write-path`: one measured transactional route; no
  query-shape/schema-driven route selection.
- `async-write-cache`: removed from the write path (G1) or re-implemented as
  an atomic staged commit (G2); measured before either happens.
