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
- [ ] Read-overlay parity matrix test: every read method ×
      {buffered create, update, delete} × {active, draining}. Only `GetNode`
      is overlaid today; label scans, edge reads, counts and streams still
      see committed state only (merge/set-through-buffer caveat).
- [ ] WAL replay idempotency for buffered ops (crash before flush currently
      loses the unflushed generation).
- [ ] Ingest matrix re-run (`ZZAsyncStrip` bench, Northwind-style seed):
      report before/after; target recovering the 5× on node-only CREATE with
      no regression on other shapes.
- [ ] Full suites + `-race` + benchmark ratchet.

## Write-path optimizations (PR #1043 measurement series, monster comments 1-7)

Status of the ranked list (https://github.com/orneryd/NornicDB/pull/1043#issuecomment-6108785932):

- [x] 1. Shard the node-ID dictionary — `d7695dae`; measured 87% of lock wait → ~2× batched node creates (69k→148k nodes/s, 16 writers, write-behind).
- [ ] 2. Counts as per-commit delta keys — profiled only: `labelCountWriteMu`/`edgeTypeCountWriteMu` are 99% of durable-path lock wait; one read-modify-write count key per label under the lock. Removing the lock alone triggers O(nodes) recount storms (86% CPU), so the fix is delta keys + read-side summation + background folding, plus bounding the synchronous repair path. Deferred: storage-format change; needs its own measured change.
- [x] 3. Bulk-create `UNWIND … CREATE` — `05917ba0`; pure UNWIND+CREATE stretches marked independent → one createPlan + one BulkCreateNodes/BulkCreateEdges (measured +27-39% at 16 writers).
- [x] 4. GC tuning — `GOGC=400` measures +19-38% write-behind (GC mark ~9-12% of CPU). Production form is a GOMEMLIMIT-bounded high GOGC; benchmark harness now has `NORTHWIND_GOGC` (default 400) and the docs note it.
- [x] 5. Keyword-position cache contention — `05917ba0`; literal-bearing statements skip the shared per-text caches (keyword index, semantic, merge-semantic, valid-syntax) instead of contending (was 88% of lock wait).
- [ ] 6. Auto-parameterize literals — profiled: literal vs parameterized single creates are 9.6k vs 17.4k/s (1 writer); `validateSemanticScopes` alone 25% of CPU. Deferred: query-rewrite layer; #5 removes the contention but the parse cost itself remains.
- [x] 7. Don't re-read endpoints in `validateNewEdgeLocked` — `59832bd0`; `getCommittedNodeLocked` seeds the tx label cache (own 8K/1MiB budget), so edge-batch validation hits it instead of a second body read. Order-lines wiring shape: 35.0→27.5 µs/row.
- [ ] 8. Parse the CREATE pattern once per statement — profiled ~7% of UNWIND-100 CPU (`planCreateNode` re-parses per row). Deferred: a CREATE template alongside `pipelineMergeTemplate`/`pipelineNodeMatchTemplate`.
- [x] 9. Shard the edge-ID maps — `05917ba0`; same 64-shard pattern as nodes. Strip benchmarks improved or neutral (create_rel 38.6→35.3µs).
- [x] 10. Skip `checkNodeCreateConflict` head read for tx-generated IDs — `05917ba0`; `Operation.FreshID` gates both node and edge create-conflict head reads (was ~3-4% CPU: a Badger view per created entity).
- [ ] 11. Fewer keys per relationship — measured only as key counts (11 keys / ~580 bytes: adjacency in plain + MVCC indexes, edge-between set + head; node = 5 keys / ~450 bytes). The gain was NOT measured; a storage-layout change needs a measured win before merging.
- [ ] 12. Bolt / server path — measured 1.5-1.7× slower than in-process on one host, not broken down. Needs a server CPU profile during a Bolt load before changes.
