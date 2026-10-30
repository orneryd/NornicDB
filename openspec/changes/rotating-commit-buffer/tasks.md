# Tasks: Rotating commit buffer

- [x] Core `CommitBuffer` + `WriteBehindBuffer` in `pkg/storage/commit_buffer.go`:
      append (node/edge/delete), rotation (interval + size), FIFO background
      apply, newest-first overlay reads, `Flush`, `Close`.
- [x] Unit + race tests: never-block rotation, background apply, read-your-own
      writes, cross-generation shadowing, delete-hides-write, size-threshold
      rotation, drain-on-flush, error retry.
- [x] Wire into `BadgerEngine`: commit hook copies tx pending state into the
      buffer when `WriteBehind` is enabled (`BadgerOptions`); autocommit-only
      path (`implicit`, no schema/knowledge/temporal writes) acks after all
      synchronous validation; replay applies generations inside one Badger
      transaction with per-namespace MVCC versions, prop-key drains, ID-dict
      counters, label/edge-type count rebuilds and cache/notification tails
      (`badger_write_behind.go`). Close drains; explicit `BEGIN` drains before
      snapshot (`pkg/cypher/transaction.go`). GetNode reads through the overlay.
- [x] Constraint validation before buffering (commit-time): unique locks,
      connected-delete resolution, `validateAllConstraints` and SI-conflict
      checks all run synchronously before the buffered ACK. (Overlay-aware
      reads for validation still gap — see read-overlay parity below.)
- [x] Read-overlay parity for committed reads (monster retest part 1):
      adjacency (`GetOutgoingEdges`/`GetIncomingEdges`/`GetAdjacentEdges` and
      the transaction-level readers), between-index (`GetEdgesBetween`,
      `GetEdgeBetween`, `MatchEdgesBetween` — MERGE identity reads), DETACH
      DELETE enumeration over buffered incident edges, and every counter
      (`EdgeCount`, `NodeCount`, prefix-scoped namespace counts, per-type and
      positional label/type counts) now fold the buffered generation in.
      Tests: `pkg/storage/write_behind_adjacency_overlay_test.go` (50 users /
      40 :Friend pre-flush == post-flush matrix, MERGE no-dup, DETACH DELETE
      no-orphans) and `pkg/cypher/write_behind_index_visibility_test.go`
      (`TestWriteBehind_MergeAndDetachDeleteOverBufferedEdges`).
- [ ] WAL replay idempotency for buffered ops (crash before flush currently
      loses the unflushed generation — monster retest part 5).
- [ ] Ingest matrix re-run (`ZZAsyncStrip` bench, Northwind-style seed):
      report before/after; target recovering the 5× on node-only CREATE with
      no regression on other shapes.
- [ ] Full suites + `-race` + benchmark ratchet. (Green without `-race` on
      this branch: `pkg/storage`, `pkg/cypher`, `pkg/bolt`, `pkg/server`,
      `make cypher-conformance`, and the 15k ops/s benchmark gate.)

## Write-path optimizations (PR #1043 measurement series, monster comments 1-7)

Status of the ranked list (https://github.com/orneryd/NornicDB/pull/1043#issuecomment-6108785932):

- [x] 1. Shard the node-ID dictionary — `d7695dae`; measured 87% of lock wait → ~2× batched node creates (69k→148k nodes/s, 16 writers, write-behind).
- [X] 2. Counts as per-commit delta keys — profiled: `labelCountWriteMu`/`edgeTypeCountWriteMu` are 99% of durable-path lock wait; one read-modify-write count key per label under the lock. Removing the lock alone triggers O(nodes) recount storms (86% CPU), so the fix is delta keys + read-side summation + background folding, plus bounding the synchronous repair path. Pending: storage-format change; implement with before/after measurement.
- [x] 3. Bulk-create `UNWIND … CREATE` — `05917ba0`; pure UNWIND+CREATE stretches marked independent → one createPlan + one BulkCreateNodes/BulkCreateEdges (measured +27-39% at 16 writers).
- [x] 4. GC tuning — `GOGC=400` measures +19-38% write-behind (GC mark ~9-12% of CPU). Production form is a GOMEMLIMIT-bounded high GOGC; benchmark harness now has `NORTHWIND_GOGC` (default 400) and the docs note it.
- [x] 5. Keyword-position cache contention — `5a64b9ef`; the per-text caches (keyword index, semantic, merge-semantic, valid-syntax) are 64-shard with allocation-free hashing, and literal-bearing statements cache like any other text. Sharding removes the lock wait the skips avoided (was 88% of lock wait) without sacrificing repeated-statement hits (the skips cost the conformance gate ~50% and were removed).
- [x] 6. Auto-parameterize literals — implemented as canonical literal-normalized compile-cache keys (semantic, merge-semantic, valid-syntax caches). Literal forms whose value can change validity (out-of-range ints/hex, leading-zero decimals, escaped strings) stay verbatim so they never share a verdict with a valid statement. Direct literal CREATE: 29.2→20.5µs/op (−30%), allocs 180→145; parameterized baseline 14.4µs. The remaining gap is text-specific keyword scanning / clause splitting, which positions make unsharable.
- [x] 7. Don't re-read endpoints in `validateNewEdgeLocked` — `59832bd0`; `getCommittedNodeLocked` seeds the tx label cache (own 8K/1MiB budget), so edge-batch validation hits it instead of a second body read. Order-lines wiring shape: 35.0→27.5 µs/row.
- [x] 8. Parse the CREATE pattern once per statement — `5a64b9ef`; `pipelineCreateTemplate` parses a CREATE clause once per clause text and plans per row (profiled ~7% of UNWIND-100 CPU in `planCreateNode` re-parsing per row). Clauses with row-dependent dynamic labels/types keep the general route, planning is atomic (rollback on per-row fallback), and the template/general parity test uses real row bindings.
- [x] 9. Shard the edge-ID maps — `05917ba0`; same 64-shard pattern as nodes. Strip benchmarks improved or neutral (create_rel 38.6→35.3µs).
- [x] 10. Skip `checkNodeCreateConflict` head read for tx-generated IDs — `05917ba0`; `Operation.FreshID` gates both node and edge create-conflict head reads (was ~3-4% CPU: a Badger view per created entity).
- [ ] 11. Fewer keys per relationship — measured only as key counts (11 keys / ~580 bytes: adjacency in plain + MVCC indexes, edge-between set + head; node = 5 keys / ~450 bytes). The gain was NOT measured; a storage-layout change needs a measured win before merging. Pending: measure the win first.
- [x] 12. Bolt / server path — profiled with the new permanent `pkg/bolt` write-path benchmarks (Bolt server vs direct executor, write-behind engine): the Bolt path is syscall-dominated (55% of samples in socket read/write for one op per round-trip), cypher work is 6.5%; server/direct ratio ~3× at 1 op/request, protocol-inherent. Removed the per-request empty-params map allocation in `boltQueryExecutorAdapter`. The database-side lever on this path is #6 (literal statements pay the compile cost per request).
