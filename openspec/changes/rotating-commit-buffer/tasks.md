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
