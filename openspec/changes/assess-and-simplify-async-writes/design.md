# Design: Assess and simplify the async write path

## 1. Current routing inventory (main `7412533a`)

`executeImplicitAsync` (`pkg/cypher/executor.go:1801`) decides per statement:

| Route | Entry | Trigger | Atomicity | Durability layer |
|---|---|---|---|---|
| Async CREATE batch | `tryAsyncCreateNodeBatch` (`:1629`) | `CREATE (n:Lbl …)` single query, no write rules, async engine present | statement | async cache (flush ~50 ms), then WAL |
| Eventual (no tx) | `executeWithoutTransaction` | `isEventualAsyncEligible` (`:1756`) && `!writesAreChecked` (`:1781`) | **none** | async cache only |
| Implicit transaction | `executeWithImplicitTransaction` | everything else, incl. any constrained label | statement | Badger grouped commit + WAL markers + receipt; `asyncEngine.Flush()` called after commit (`:2052`) |

`beginTransactionSnapshot` (`pkg/cypher/transaction_admission.go:8`) runs
`AsyncEngine.FlushBeforeSnapshot` synchronously before opening the Badger
transaction — so a constrained write pays a full cache flush **and** a Badger
commit. Explicit transactions (`pkg/cypher/transaction.go:212`) share the
admission path.

Stack (`pkg/nornicdb/db.go:874`): `Badger → WAL → Async → (Replicated) →
Namespaced`. The WAL is **below** the async cache, so cached writes are not
logged until flush; Badger commits are `SyncWrites=false` (grouped) unless
`SyncWrites` config is set (`pkg/storage/badger.go:756-763`); WAL batches
sync on `WALSyncInterval` (default 100 ms, `pkg/config/config.go`).

Three buffering layers (Badger group commit, WAL batch, async cache) overlap;
each individually is fine, but the combination is unmeasured and the middle
routing layer is shape-dependent.

## 2. Measurement matrix (step 0 — before any code change)

Gate: `ZZ_INGEST=1` scratch benchmark in `pkg/nornicdb/zz_ingest_bench_test.go`
(committed with the measurement, removed once decision is recorded), following
the existing `testify`-less benchmark pattern.

Cells: `{async-on (50 ms default), async-off} × {WAL default, WAL none/sync,
strict durability (Badger SyncWrites + WAL sync)} × {no constraints,
ALL labels constrained}` on the **same** dataset (≈10k–50k nodes, mix of
node-only CREATE, batched `UNWIND` CREATE, `MERGE`, relationship CREATE, `SET`).
Collect: ns/op, allocs/op, B/op, flush counts, and end-to-end nodes/s.

Profiles (CPU, mem, alloc) of:
1. One Soraban-style constrained batched `UNWIND` statement (~1 s) — verify
   where the second is spent: Badger commit, WAL append per op, constraint
   locks (`SchemaManager.lockConstraintKeysOf`, `pkg/storage/schema.go:1069`),
   MVCC/index upkeep, or re-validation.
2. The async path's flush cycle under load — verify `Flush` cost and whether
   merging actually reduces per-row work vs the Badger commit it replaces.
3. Northwind seed harness itself (client-side) — confirm or reject the
   "harness bottlenecks NornicDB and Neo4j equally" hypothesis by comparing
   in-process nodes/s to the harness numbers (1.4.1: NornicDB 18,081 nodes/s,
   Memgraph 60,780 nodes/s, NornicDB ≈ Neo4j).

Profile, do not guess: run `-cpuprofile` during benchmarks; allocations from
`-benchmem` + `-memprofile`.

## 3. Decision gates

- **G1 (remove async routing)** if async-off ≈ async-on (within noise) for at
  least 3 of the 4 statement shapes **and** the profile shows the constrained
  path's cost is per-row execution/Badger commit, not flush latency. Expected
  from the Soraban observation (per-statement work dominates).
- **G2 (keep async, make it correct)** only if async-on beats async-off by
  ≥20% on a measured shape **and** the win survives the harness-fairness
  correction. Then implement the staged-transaction design from
  `docs/plans/always-async-writes-plan.md` instead of this removal.
- No third option: a hybrid "route by shape" is the status quo being
  eliminated.

## 4. G1 design: single transactional route

1. **Collapse routing.** `executeImplicitAsync` becomes: `CALL … IN
   TRANSACTIONS` → `executeWithoutTransaction`; writes → one implicit
   transaction; reads → `executeWithoutTransaction`. Delete
   `tryAsyncCreateNodeBatch`, `isEventualAsyncEligible`, and the
   `writesAreChecked` gate; `resolveImplicitTxEngines` drops the async branch.
2. **AsyncEngine exit from the write path.** Two sub-options, measured:
   a. Remove it from the engine stack in `db.go` entirely; audit read-path
      consumers (search/embedding writeback, cache merge methods) first — if a
      consumer depends on buffered visibility, that consumer is itself a
      divergent path and gets fixed to read committed state.
   b. Keep the engine for its read/warmup role but stop routing writes
      through it (only if removal of the stack breaks a non-write consumer —
      an explicit, reviewed exception, not a silent fallback).
   `FlushBeforeSnapshot`/`Flush` calls in `transaction_admission.go` and
   `executor.go:2052` disappear in both sub-options.
3. **Tune the main path** (each a separate, reversible experiment with
   before/after ns/op + nodes/s):
   - Badger `WithDetectConflicts(true)` is always on. Measure off; conflicts
     matter for concurrent tx on the same keys — keep off only if the
     constraint-lock + per-key write path already serializes (prove with a
     concurrency test, not an assertion).
   - `MemTableSize`, `NumMemtables`, `ValueThreshold`, `NumCompactors`,
     `Compression` under a fixed workload.
   - WAL group commit: batch size vs `WALSyncInterval` sweep; ensure ack
     contract is documented, not silently weakened.
   - Reduce per-statement overhead measured in step 0 (receipt append, marker
     writes, double flush removal is already included above).
4. **Async shims only where earned.** For node-only bulk `CREATE`, the #908
   convergence already pools adjacent CREATE clauses into one atomic plan;
   measure that first. A new shim is acceptable only if it (a) keeps statement
   atomicity + receipts, (b) passes the constraint-lock path, (c) shows a
   measured win over the tuned sync path. Otherwise: no shim.
5. **Acceptance numbers.** Report the 1.4.1-style seed (in-process): baseline
   ~18k nodes/s; Memgraph reference 60,780 nodes/s (harness). Targets are
   workload-scoped: no regression vs sync baseline on every cell of the
   matrix, and a stated fraction of the Memgraph number on the unconstrained
   node-create cell — recorded in tasks.md, not promised in advance.

## 5. G2 design (reference only)

Staged transactions per `docs/plans/always-async-writes-plan.md`: async
staging becomes a first-class, atomic, constrained-visible transaction
participant; the routing above still collapses into one admission path. This
change is not the implementation vehicle for G2.

## 6. Risks and mitigations

- **Flush-time constraint visibility** disappears under G1 — constraint
  violations become commit-time again (Neo4j behavior). Mitigation: keep
  `test` suite coverage in `pkg/cypher` for duplicate/constraint errors on
  write.
- **Durability contract change**: eventual-mode acks go away. Mitigation:
  document; grep HTTP/Bolt header use; update `docs/api-reference` async
  mentions.
- **Badger option regressions** are config-only and can be reverted per
  experiment (profile-backed, per the performance protocol).
- **Concurrent-tx conflicts** if conflict detection is disabled: gate on a
  dedicated concurrency test with `-race`.

## 7. Deletion targets (G1)

- `pkg/cypher/executor.go`: `tryAsyncCreateNodeBatch` (:1629),
  `isEventualAsyncEligible` (:1756), `writesAreChecked` (:1781), the async
  branch in `executeImplicitAsync` (:1815-1826).
- `pkg/cypher/transaction_admission.go`: `FlushBeforeSnapshot` call (:8 area).
- `pkg/nornicdb/db.go`: async engine in the stack (sub-option 2a) or write
  routing to it (2b).
- `pkg/storage/async_engine.go`: full removal only under 2a after consumer
  audit; otherwise keep with a `// read-path only` role documented.
- Tests referencing eventual async routing: migrated to assert the single
  route, not deleted silently.
