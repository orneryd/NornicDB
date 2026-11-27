# N152-334: namespace first-use stall

- [x] Reproduce clean-reopen first-use cost on the current release head; retain actual counts, timings, and data controls. Concurrent duplicate recovery established from the cold-load path.
- [x] Identify the canonical MVCC floor owner and engine-open readiness boundary without weakening durable sequence recovery.
- [x] Preload existing namespaces before engine publication and single-flight concurrent cold callers; keep the recovered sequence and timestamp floors unchanged.
- [x] Test recovery errors, concurrent first callers, multiple namespaces, restart durability and public BEGIN/write paths. Cancellation is outside the no-context namespaceMVCC API; the engine-owned startup operation completes before publication.
- [ ] Run focused and affected package checks, record exact head/base and skipped release verification, then open the release PR.
