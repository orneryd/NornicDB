# N152-421: indexed relationship equality in WHERE

- [x] Reproduce the archived 8,000-pair C68 statement on the release head with exact row and public PROFILE assertions.
- [x] Reuse the existing single-edge relationship equality seed for mandatory, stable WHERE conjuncts without changing its storage/index owner.
- [x] Preserve the original WHERE as the residual filter and decline disjunctions, unsupported predicates, and multi-edge ordinary-index results.
- [x] Check both parsers, unique and ordinary indexes, the WAL namespace stack, pending writes, missing/null values, reversed equality, residual predicates, and duplicate values.
- [ ] Run the uncached affected Cypher package and review duplicate owners.
- [ ] Prepare the release PR description and exact verification evidence.
