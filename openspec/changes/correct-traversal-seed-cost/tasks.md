- [x] Read NOR-414's current regression comment and original PR review; distinguish the already-fixed subquery finding.
- [x] Assign the reopened regression to the maintainer and create isolated parent/reproduction worktrees on current main.
- [x] Reproduce membership and unevaluated-inline-key costs with exact public results; retain selective-end controls.
- [x] Correct the canonical end-seed cost decision without new storage state or broad planner changes.
- [x] Independently qualify both parsers, affected routes and error/cancellation behavior (focused tests and race checks; full package verification remains separate).
- [x] Run paired workload benchmarks and CPU/allocation profiles without concurrent tests; document the selective-end metadata overhead.
- [ ] Push a new PR with AC/evidence mapping, update Linear and record actual CI/advisory status.

## Copilot review corrections

- [x] Reproduce all four review findings through public queries under both parsers before changing production code.
- [x] Admit anonymous inline seeks, count broad inline postings before hydration, and remove singleton-only direction choices.
- [x] Consolidate start-pattern slice admission and refresh matched baseline/candidate measurements including single-document controls.
- [x] Complete fresh focused/race checks after all review corrections.
- [ ] Complete fresh full Cypher package coverage; push the corrections, resolve fixed threads and dispatch full-SHA CI.
