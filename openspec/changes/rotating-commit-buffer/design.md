# Design: Rotating commit buffer

## 1. Where it sits

Post-strip, `resolveImplicitTxEngines` finds `txEngine` on `BadgerEngine`
(only it and `MemoryEngine` implement `BeginTransaction`). Every autocommit
write becomes a `BadgerTransaction` whose `Commit()` is the single choke
point. The buffer therefore lives at the **commit path of `BadgerEngine`**:

```
Cypher autocommit ──► BadgerTransaction ──► Commit() ──► WriteBehindBuffer
                                                              │ (background)
                                                              ▼
                                                  Badger (replay in one tx)
```

Explicit BEGIN/COMMIT and snapshot opens call `Drain()` first and commit
inline — a single policy, not shape routing.

## 2. Buffer core (`pkg/storage/commit_buffer.go`)

`CommitBuffer` (one generation):

- `nodes map[NodeID]*Node`, `edges map[EdgeID]*Edge`,
  `deletes map[string]bool` — the tx's pending state, copied at Commit.
- `order []bufferedMutation` — replay order across node/edge/delete ops.
- `nodesByLabel map[string][]NodeID`, `edgesByType map[string][]EdgeID` —
  scan overlays maintained on append.
- `applied atomic.Bool` — set by the flusher after replay commits; readers
  skip applied generations.

`WriteBehindBuffer`:

- `active *CommitBuffer`, `draining []*CommitBuffer` (oldest first),
  guarded by `mu sync.RWMutex`.
- `AppendNode / AppendEdge / AppendDelete`: RLock-free append into `active`
  (a short `mu.Lock` for map writes, O(1)); rotation check after append.
- `rotate()`: swap in a fresh buffer, move the old one to `draining`,
  signal the flusher. O(1) — writers never wait for flush.
- Flusher goroutine: take `draining[0]`, call `apply` (injected; replay into
  Badger inside one transaction), set `applied`, drop it from `draining`
  under `mu.Lock`.
- `Flush()`: rotate and wait for all generations to be applied (shutdown,
  explicit tx admission).
- Triggers: `FlushInterval` ticker (the `AsyncFlushInterval` knob) and a
  `MaxOps` size threshold.

## 3. Read overlay

`BadgerEngine` read methods consult generations newest-first (active, then
`draining` reversed, skipping `applied`):

- `GetNode`/`BatchGetNodes`: latest buffered value wins; buffered delete →
  `ErrNotFound`.
- `GetEdge`/`BatchGetEdges`: same for edges.
- `GetNodesByLabel`, `GetEdgesByType`, counts, projections, streams: merge
  committed rows with overlay rows (buffered update replaces, buffered delete
  hides, buffered create adds).

This is the same overlay shape the removed cache had, but implemented against
the engine contract and only reachable through the single commit path.

## 4. Correctness contract

- **Statement atomicity**: a `CommitBuffer` holds whole committed transactions;
  replay applies each in commit order, so partial statement application is
  impossible (a replay is one Badger transaction per generation, containing
  only complete committed statements).
- **Receipts/durability**: unchanged — WAL tx markers are appended before ACK,
  batch-synced at `BatchSyncInterval`. A crash loses at most one interval of
  applied-to-Badger state, which WAL replay restores (replay must skip or
  re-apply buffered-but-lost ops; WAL is the ledger, Badger is derived).
- **Ordering**: generations drain FIFO; reads merge newest-first, so a later
  statement always shadows an earlier one.
- **Constraints**: commit-time validation runs synchronously before buffering,
  reading through the overlay, so buffered concurrent statements cannot both
  pass a uniqueness check.
- **Memory bound**: `MaxOps` + interval rotation bound the buffered working
  set; `draining` can hold multiple generations only while flushes lag.

## 5. Decisions

- Explicit transactions and snapshot opens drain first (blocking) — required
  for snapshot isolation; autocommit latency is unaffected.
- Constraint validation stays synchronous at commit (no post-ACK
  violations) — the invariant the strip restored.
- No new config: `AsyncWritesEnabled` gates the buffer, `AsyncFlushInterval`
  drives rotation, cache-size knobs become `MaxOps` guidance.

## 6. Risks

- Overlay coverage gaps: a missed read method shows stale committed data.
  Mitigation: enable only behind `AsyncWritesEnabled`, keep a read-overlay
  parity test matrix (every read method × buffered create/update/delete).
- Replay ordering vs WAL recovery: WAL replay applies ops again; replay must
  be idempotent per op or skip ops already present in Badger.
- Flush error: apply failure must surface on the next read/`Flush()` and
  retry, never silently drop a generation.
