# Pin compaction populations before serializing files

- [x] Reproduce mutation-barrier ownership during actual parked snapshot-file output.
- [x] Pin native stored-inclusive entity populations and reuse canonical scan kernels.
- [x] Release mutation barrier before serialization; serialize compaction file ownership separately.
- [x] Preserve captured checkpoint progress and post-cut WAL records.
- [x] Retain custom stored-reader override behavior under its existing barrier.
- [x] Reproduce live-dictionary lookup failure after namespace recreation; resolve bindings in the pinned physical view.
- [x] Parent integrated snapshot/source/dictionary race controls pass (4.431s).
- [ ] Qualify workload-specific snapshot/write latency and allocation costs without competing local tests.
- [ ] Publish candidate and complete exact-head CI and duplicate review.

No storage keys, payload format, WAL encoding or exported Go signature changes.
