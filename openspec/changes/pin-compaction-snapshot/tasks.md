# Pin compaction populations before serializing files

- [x] Reproduce mutation-barrier ownership during actual parked snapshot-file output.
- [x] Pin native stored-inclusive entity populations and reuse canonical scan kernels.
- [x] Release mutation barrier before serialization; serialize compaction file ownership separately.
- [x] Preserve captured checkpoint progress and post-cut WAL records.
- [x] Retain custom stored-reader override behavior under its existing barrier.
- [x] Reproduce live-dictionary lookup failure after namespace recreation; resolve bindings in the pinned physical view.
- [x] Parent integrated snapshot/source/dictionary race controls pass (4.431s).
- [x] Reproduce and preserve custom node-stream overrides; integrated race controls pass after correction (4.037s).
- [x] Isolated existing 1,000-node stream comparison recorded with all samples and small allocation overhead.
- [ ] Qualify production-size snapshot/write latency and peak retention.
- [ ] Publish candidate and complete exact-head CI and duplicate review.

No storage keys, payload format, WAL encoding or exported Go signature changes.

- [x] Public recovery fixture failure reproduced locally; retain exact in-flight file via hard link and wait for completion, preserving all vector-recovery assertions. Five repeated focused runs pass (12.043s).
- [x] Retire private Close forwarding method and duplicate periodic snapshot helpers; one edge-body writer chooses pinned/current acquisition.
