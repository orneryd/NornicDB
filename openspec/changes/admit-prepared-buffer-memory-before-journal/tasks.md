# Admit prepared-buffer memory before journal acceptance

- [x] Reproduce refused prepared writes entering semantic WAL and appearing after reopen (3/3).
- [x] Reserve the real buffered payload once before the prepared handoff and transfer that lease to its generation.
- [x] Preserve entity/endpoint authority while releasing and reacquiring the same write-order stripes around the parked handoff.
- [x] Release memory, dictionary claims and MVCC reservation on discard/rejection/cancellation.
- [x] Preserve direct-commit memory-policy behavior and reuse captured accounting images.
- [x] Worker affected storage checks and selected race controls pass; limits recorded separately.
- [x] Independent review reproduces accounting using canceled preparation context after acceptance; explicit accepted lifetime correction passes parent controls (1.797s).
- [x] Collapse direct/buffered handoff into one completion-context owner.
- [x] Current integrated parent race controls pass (4.382s).
- [ ] Publish candidate and complete current-head CI/duplicate review.

No exported Go API, stored key/payload or WAL format changes. No double reservation
or approximate preflight is introduced. Heavy full suites/censuses run in CI.

- [x] Operator approved transfer/re-pin of the two existing accepted-completion context sites; focused scope/held-request rejection controls pass1.744s. No analyzer rules or new cancellation exceptions.
- [x] Exact CI986 source-generated storage reports refreshed with zero violations; current full CI still required.
