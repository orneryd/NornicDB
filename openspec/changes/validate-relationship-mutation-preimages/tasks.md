# Relationship mutation preimage validation

- [x] Rebase the preserved reproduction onto release 83b8c5884.
- [x] Reproduce the stale implicit writer; retain the explicit conflict control.
- [x] Share the existing node/relationship first-mutation fold and coherent read cut.
- [x] Reject supplied stale observations before staging; validate staged predecessors before publication.
- [x] Cover repeated updates and in-frame CREATE followed by UPDATE.
- [x] Exercise buffered/replayed peer publication and successful retry with final counter=2.
- [x] Independently review canonical callers and public query coverage; both parsers pass conflict/retry controls.
- [ ] Run focused race and affected-package tests; report exact source and skips.
- [ ] Push the fixed candidate and run full make test CI.
- [ ] Document advisory duplicate findings and ticket acceptance evidence.
