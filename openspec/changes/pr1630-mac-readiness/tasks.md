# PR 1630 release readiness

- [x] Confirm NOR-16 is assigned to Tim and excludes T3.
- [x] Read PR description, reviews, inline comments and handoff notes.
- [x] Rebase onto release/v1.4.4 containing merged PR 1632; retain canonical readers and transport fixes.
- [x] Resolve and verify integrated import lifetime, validation and transport regressions.
- [x] Run make test on the final source, preserving failures and skips honestly.
- [x] Prepare the validated head and exact result receipt for PR publication and NOR-16.

The maintainer-selected local gate is make test. Broader verification remains for CI.

Complete source gate at d29dc47fc: make test PASS, 107 packages (85 cached). Cypher 397.304s, search 282.449s, server 345.340s, storage 153.660s ran fresh. The earlier complete failed gate and focused fail/green logs remain under /tmp/nornic-1630-*. Existing reference-dependent skips remain; broader checks are deferred to CI. A final publication-head make test receipt is recorded in the PR and Linear after this bookkeeping commit.
