# PR 1642 / NOR-55 readiness

Local final gate: make test; broader gates deferred to CI per maintainer.

- [x] Confirm ownership, read review record and rebase onto release after PR 1650.
- [x] Reproduce and fix every open review finding: outer/batch transaction ownership, bare CREATE effects, ordinary mutation phases, composed list types, pagination timing and effect census.
- [ ] Verify public routes and both parsers on integrated source.
- [ ] Complete make test, publish checked head/base and limitations, push and update Linear.


Fail-first evidence: outer/PROFILE rollback, bare-node effects/dependency, composed reverse/tail types, consecutive CREATE and CREATE self-read phases, missing clause-census membership, and unsupported composite batching failed before correction. Restoring the old global pagination preflight through a Go overlay reproduces the aggregation/SKIP lost-commit failure. An initial pass-through SKIP expectation was wrong; the fresh Neo4j oracle establishes zero earlier commits there, versus retained commits after aggregation/sort or an earlier dependent MATCH. Both sides are now covered.

Thirteen supplemental cases were freshly captured from disposable Neo4j 5.26.30 with version assertion, exact codes and independent post-query reads. They supplement the unchanged original oracle through the same Execute/Bolt/HTTP fixture owner. The syntax census now includes reading/updating clause alternatives and an in-memory removed-CREATE mutation proof.

Correction: the statement keeps its implicit transaction, a shared child-batch boundary commits independently, and only an unwritten implicit outer transaction advances via storage's existing snapshot refresh owner. Batches cannot interleave with ordinary outer mutations row by row. CREATE graph inputs are planned before publication and dependent consecutive CREATE clauses separate. Clause effects carry materialization, so pagination and batch sizes share consumer-to-input binding; runtime errors remain classified. Unsupported composite batches reject before writes. No preexisting cost/semantic assertions were weakened or skipped.

Final integrated source verification pending. Broader make verify, allocation-ratchet and final-source performance qualification are deferred to CI under the maintainer's local make test-only gate. No performance pass is claimed. PR 1638's future composite integration must preserve this independent-batch/outer-rollback contract when rebased.
