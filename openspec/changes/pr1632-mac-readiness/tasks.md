# PR 1632 release readiness

- [x] Confirm NOR-15 ownership and exclude T3.
- [x] Audit conversation, reviews and inline comments.
- [x] Integrate current statement-schema dependency and merged release fixes.
- [x] Reproduce and resolve nullable result bounds, RETURN-star trivia and grammar census findings.
- [x] Resolve integrated command, procedure-schema and local-binding regression families found by the first complete make test run.
- [ ] Run make test on final integrated source; diagnose failures against release.
- [ ] Push readiness changes and record exact head/base/results on PR and Linear.

Local gate is make test per maintainer; broader verification is deferred to CI.
