# PR 1630 release readiness

- [x] Confirm NOR-16 is assigned to Tim and excludes T3.
- [x] Read PR description, reviews, inline comments and handoff notes.
- [x] Rebase onto release/v1.4.4 containing merged PR 1632; retain canonical readers and transport fixes.
- [x] Resolve and verify integrated import lifetime, validation and transport regressions.
- [ ] Run make test on the final source, preserving failures and skips honestly.
- [ ] Publish validated head and receipt; update NOR-16.

The maintainer-selected local gate is make test. Broader verification remains for CI.
