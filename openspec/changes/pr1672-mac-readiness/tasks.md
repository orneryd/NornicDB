# PR 1672 Mac readiness

Scope: legacy Bolt DateTime instant preservation, integrated with current release/v1.4.4. Local merge gate is make test; review the current implementation and complete the pending DateTime encoding comparison.

- [x] Read PR handoff and all review/conversation channels; no unresolved threads.
- [x] Rebase onto release e5b35a220, retaining the release's native dependency versions and both changelog entries.
- [ ] Independently review the legacy/UTC codec contract and public consumers.
- [ ] Run focused regression/race checks on both parsers.
- [ ] Compare DateTime encoding benchmarks and existing row/record controls on identical base/head fixtures.
- [ ] Complete make test on the integrated source; correct introduced failures.
- [ ] Push and post exact-source evidence, update stale PR text and mark ready.
