# Proposal: Rotating commit buffer (write-behind at the Badger commit path)

## Why

The AsyncEngine strip (`dc482982`) collapsed every write onto the single
transactional route: Cypher autocommit writes run inside a `BadgerTransaction`
and commit to Badger inline, then append WAL tx markers for receipts. The
per-statement cost is now the full Badger commit, and the previously buffered
node-only CREATE shapes run ~5× slower (14.3 µs → 71.4 µs per statement on the
WAL+Badger stack).

The removed cache is **not** what we want back: it buffered only a
keyword-scanned subset of writes, dropped statement atomicity on those shapes,
had no receipts, and hid constraint violations until an unrelated flush. This
change adds buffering at the **one place all writes already pass through** —
`BadgerTransaction.Commit` — with statement-level units and the single-route
semantics preserved.

## What Changes

1. **A rotating commit buffer** in `pkg/storage`: committed transactions are
   appended to an active buffer and acknowledged immediately (WAL markers and
   receipts are written exactly as today, so durability is unchanged: batch
   sync at `BatchSyncInterval`).
2. **Rotation**: when the flush interval elapses or the active buffer exceeds
   a size threshold, it is atomically rotated out and a fresh buffer takes its
   place; writers never block on flush. The drained buffer is replayed into
   Badger by one background flusher inside a single real Badger transaction —
   the amortization that recovers ingest throughput.
3. **Read overlay**: all `BadgerEngine` reads consult active + draining
   buffers newest-first, so acknowledged writes are visible immediately and
   ordering is preserved across generations. Label scans, edge-type scans and
   counts merge the overlay, as the removed cache did — but as part of the
   engine contract, not a shape-routed layer.
4. **Constraint semantics**: uniqueness/domain checks stay at commit time.
   Validation runs synchronously against committed + buffered state before a
   transaction is buffered, so a violation still fails the statement and keeps
   nothing, exactly as Neo4j behaves.
5. **Explicit transactions stay synchronous**: BEGIN/COMMIT writes commit to
   Badger inline and the buffer is drained before an explicit snapshot opens.
   Autocommit buffering is a single policy, not per-shape routing.

## Impact

- Affected: `pkg/storage/commit_buffer.go` (new core), `pkg/storage/badger*.go`
  (commit hook, reads, snapshot drain), `pkg/nornicdb/db.go` (enable via the
  existing `AsyncWritesEnabled` + `AsyncFlushInterval` knobs, which already
  drive WAL batch sync), `pkg/config/config.go` (no new knobs).
- Preserved: single write route, statement atomicity, receipts, constraint
  semantics, explicit-transaction durability, WAL crash recovery (replay
  re-applies buffered ops).
- Behavior change: acknowledged autocommit writes become visible to Badger
  scans only after the buffer drains (ms-scale); they are always visible to
  API reads through the overlay.
- Measurement: re-run the ingest matrix from
  `openspec/changes/assess-and-simplify-async-writes/`; target recovering the
  5× on node-only CREATE without regressing other shapes.

## Capabilities

### Modified Capabilities

- `storage-write-path`: one transactional route, with a single write-behind
  buffer at commit for autocommit writes; no shape/schema routing.
