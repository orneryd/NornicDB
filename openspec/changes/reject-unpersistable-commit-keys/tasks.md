# Reject unpersistable commit keys

- [x] Reproduce buffered implicit acknowledgement on release ce22592d2.
- [x] Validate actual staged/count/dictionary key sizes before journal handoff.
- [x] Preserve canonical key encodings and shared storage error classification.
- [x] Verify native/public CREATE, valid boundary, refusal, cancellation and reopen controls under both parsers.
- [x] Profile existing buffered preparation benchmark; allocation counts unchanged. Short timing samples vary and do not establish throughput equivalence.
- [ ] Complete exact-head CI and address failures.
- [ ] Integrate and verify combined release.

Scope: RC report section 6 / B03. Memory-policy rejection and already-poisoned historical journals remain separate. No storage-format change.
