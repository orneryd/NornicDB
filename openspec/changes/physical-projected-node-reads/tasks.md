# Tasks

- [x] Inspect the current public projected read and Badger table/value-log paths.
- [x] Reproduce the missing physical projection through an encrypted public read.
- [x] Record a baseline benchmark and CPU profile for the current path.
- [x] Parse and persist an immutable, default-false `projected` creation option
      in the isolated-database catalog; public configuration test passes.
- [x] Focused-test the additive fork value-range primitive for plaintext and
      encrypted 2 MiB value-log records; final fork package check remains open.
- [x] Use a versioned property-ID directory with offsets relative to the
      current Badger value pointer. The pointer moves with compaction; no
      persistent absolute SST or value-log address is stored.
- [x] Reproduce the full-value read through the public path and prove bounded
      logical value-log reads after the fix in both physical layouts.
- [x] Publish the four-mode Northwind and mixed-corpus baseline before tuning:
      `testing/benchmarks/projected_reads/results/matrix-2026-11-27/baseline/`.
- [x] Add the opt-in writer, bounded lookup, full-read reconstruction and an
      on-disk marker that rejects incompatible open modes.
- [x] Connect point, scan, cache-widening and transactional projected reads
      to snapshot-correct property IDs and the physical read API.
- [x] Profile baseline regressions and measure each viable improvement against
      the same workload. Revert experiments that do not clear measurement noise
      or add unjustified complexity; preserve accepted and rejected evidence.
- [x] Publish the final four-mode Northwind and mixed-corpus report separately
      from the immutable baseline, including per-query and per-phase comparisons.
- [ ] Verify mutation, TTL, delete, MVCC, missing-property, mixed type,
      compaction, value-log GC, reopen, backup/restore and WAL/snapshot recovery.
- [ ] Complete matched read/write latency and allocation comparisons, CPU
      profile analysis, and disk-size and physical-I/O checks. Existing
      `badger_read_bytes_vlog` evidence counts logical requested bytes only.
- [ ] Rebase on the release head, run focused and package tests plus
      `make verify ALL=1 BASE=origin/release/v1.5.3`, then update and push #1826.
