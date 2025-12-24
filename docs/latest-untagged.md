# Latest (Untagged) Changelog

This document summarizes the changes that are **not yet released as a version tag**, but are being shipped in newly-built images under the `latest` tag.

---

## Highlights

- **WAL durability + operability hardening**: fewer allocations on append paths, more reliable corruption backups, structured diagnostics.
- **Bolt/PackStream performance + correctness**: chunked message handling fixes, lower allocation send path, end-to-end streaming benchmarks, test fixtures consolidated.
- **Cypher parsing correctness + speed**: unified keyword scanning utilities with alloc-guards; reduced internal `Execute()` recursion for lower overhead and fewer routing edge cases.
- **Storage hot-path improvements**: reduced Badger transaction boilerplate and clarified cache invalidation invariants; fixed RangeIndex update semantics and worst-case insert behavior.

---

## Detailed Changes (by area)

### WAL / Durability (`pkg/storage/wal*.go`)

- Deduplicated v2 atomic record construction and reused it across append + batch commit paths.
- Reduced allocations on append paths via buffer pooling for compact JSON marshaling.
- Centralized database prefix handling (parse/strip/ensure) to avoid multi-db edge cases.
- Routed corruption diagnostics through a logger interface while preserving JSON artifacts; added a “WAL degraded” surface for upper layers.
- Hardened corrupted-WAL backup durability by syncing the directory entry after file sync.

### Bolt / PackStream (`pkg/bolt/*`)

- Centralized PackStream/Bolt test fixtures: message builders, chunk framing helpers, and reusable assertions.
- Fixed/strengthened chunked message parsing so tests and clients don’t hang waiting on mis-parsed message boundaries.
- Improved send path performance:
  - reduced transient allocations in record streaming
  - protocol-correct multi-chunk encoding for >64KiB messages
  - earlier surfacing of write errors (instead of deferring to flush)
- Added/expanded microbenchmarks and end-to-end Bolt streaming benchmarks for tuning buffer sizes and confirming throughput.

### Cypher (`pkg/cypher/*`)

- Consolidated keyword detection and clause splitting into a single scanner:
  - quote/backtick/comment aware
  - consistent word-boundary behavior
  - whitespace-tolerant multi-word matching (`ORDER   BY`, etc.)
  - alloc-guard tests for common keyword scans
- Reduced recursive `Execute()` re-entry for internal clause execution by introducing an internal entrypoint that preserves caller context and reduces overhead.
- Improved ANTLR validation throughput via pooling and reduced parse tree construction overhead when `NORNICDB_PARSER=antlr`.
- Traversal micro-optimizations:
  - precomputed relationship-type sets for multi-type filters
  - typed `visited` maps using `storage.NodeID` (avoid repeated conversions)
  - reduced allocations in shortest-path path building

### Storage / Badger (`pkg/storage/badger*.go`, `pkg/storage/schema.go`)

- Reduced repetitive Badger transaction scaffolding and iterator option boilerplate via shared helpers (behavior-preserving).
- Clarified cache invariants and centralized invalidation entrypoints for node/edge caches.
- **RangeIndex correctness + performance**:
  - treat reinserting the same `NodeID` as an update (avoid accumulating duplicates)
  - removed O(n) “reindex all node positions” work from each insert/delete
  - fixed `BenchmarkRangeIndex_Insert` to use a bounded working set so it reflects realistic update workloads (and no longer appears to “hang”).

---

## Notes for release tagging

- This changelog intentionally focuses on shipped hardening/performance/correctness work. It is meant to accompany container images pushed as `latest` prior to a formal version tag.