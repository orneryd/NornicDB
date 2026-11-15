# Qualify pinned-compaction costs

- [x] Reuse the actual automatic-compaction kernel and public WAL-backed writes.
- [x] Generate40,000nodes with20KBproperties and a bounded managed-vector fixture in temporary stores.
- [x] Separate fixture setup, warmup, measured compaction, validation and recovery.
- [x] Capture allocation/RSS counters before validation and join samplers/writers on cleanup.
- [x] Profile idle/overlap modes independently without other local tests.
- [x] Compare fixture recovery and assert populated managed-vector chunks are checked.
- [x] Document sampled peaks, monotonic RSS, limited write rate and unmeasured transport/retention scope.
- [ ] Complete advisory duplicate review and exact-head CI.
