# Tasks: Assess and simplify the async write path

Workload-scoped measurements come first; no deletion until a decision gate is
recorded. Gates reference design.md §3.

## Step 0 — measure (no behavior change)

- [ ] Commit scratch ingest benchmark `pkg/nornicdb/zz_ingest_bench_test.go`
      (`ZZ_INGEST=1` gate, benchmark-only, removed after the decision is
      recorded). Matrix cells: `{async-on, async-off} × {WAL default, WAL
      none/sync, strict} × {no constraints, all labels constrained}` across
      node CREATE, batched UNWIND CREATE, MERGE, rel CREATE, SET.
- [ ] CPU/mem/alloc profile of one Soraban-style constrained `UNWIND`
      statement; identify top costs (Badger commit, WAL append per op,
      constraint locks `SchemaManager.lockConstraintKeysOf`, MVCC/index
      upkeep, re-validation) and publish them in evidence/.
- [ ] Profile async flush cycle under load; measure `Flush` cost and whether
      cache merging reduces per-row work vs the Badger commit it replaces.
- [ ] Fairness check of the Northwind seed harness (client-side): compare
      in-process nodes/s against 1.4.1 numbers (NornicDB 18,081; Memgraph
      60,780; NornicDB ≈ Neo4j) and state the corrected comparison target.
- [ ] Write decision record `evidence/decision.md`: G1 or G2, with the
      measured numbers. Tasks below run only for the chosen gate.

## G1 — remove async routing, tune the main path

- [ ] Collapse `executeImplicitAsync` to one transactional route; delete
      `tryAsyncCreateNodeBatch`, `isEventualAsyncEligible`, `writesAreChecked`
      and the async branch; keep `CALL … IN TRANSACTIONS` → no-transaction and
      reads → no-transaction.
- [ ] Remove `FlushBeforeSnapshot` from `transaction_admission.go` and the
      post-commit `asyncEngine.Flush()` in `executor.go`.
- [ ] Audit AsyncEngine consumers (read path, search/embedding writeback,
      cache merge callers). If none require buffered visibility: remove the
      engine from the `db.go` stack and delete `pkg/storage/async_engine.go`;
      else record the explicit read-only exception (design §4.2b).
- [ ] Migrate async-routing tests to assert the single route and commit-time
      constraint behavior (Neo4j parity); add regression: constrained and
      unconstrained labels take the same code path.
- [ ] Badger tuning experiments, each reversible with before/after ns/op +
      nodes/s and a profile: conflict detection on/off (with a dedicated
      `-race` concurrency test), `MemTableSize`, `NumMemtables`,
      `ValueThreshold`, `NumCompactors`, compression.
- [ ] WAL group-commit sweep (batch size × `WALSyncInterval`); document the
      ack/durability contract explicitly in config docs.
- [ ] Measure the #908 atomic CREATE pooling on the unconstrained node-create
      cell; add a bulk shim only if it keeps statement atomicity + receipts,
      passes constraint locks, and shows a measured win over the tuned sync
      path (design §4.4).
- [ ] Full validation: `go test ./...`, `-race`, cypher + storage suites,
      benchmark matrix re-run (no cell regresses vs sync baseline), coverage
      on touched files.
- [ ] Update `docs/api-reference`/async-mode docs for the removed eventual
      ack; note breaking change in CHANGELOG.

## G2 — keep async, make it correct (only if gate 2 fires)

- [ ] Implement staged transactions per `docs/plans/always-async-writes-plan.md`
      (separate change); this change then only contributes the measurement
      artifacts and the routing-inventory record.
- [ ] Collapse the three-route selection into one admission path with the
      staged participant (no shape-based routing remains either way).

## Both gates

- [ ] Record final workload-scoped before/after table (baseline vs decision
      state) in evidence/ and link it from tasks/proposal.
- [ ] Archive the change after owner review.
