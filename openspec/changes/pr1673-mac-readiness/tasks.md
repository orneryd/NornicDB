# PR 1673 Mac readiness

Local gate: make test. Broader verification deferred to CI per maintainer instruction.

- [x] Read handoff, review bodies, inline and conversation comments; no outstanding feedback.
- [x] Rebase onto current release/v1.4.4; preserve all changelog entries.
- [ ] Review integrated MERGE admission and public transaction consumers.
- [ ] Run focused regression tests with race detection on both parsers.
- [ ] Run make test; fix introduced failures and distinguish existing release failures.
- [ ] Push verified branch, publish current evidence and mark PR ready.
