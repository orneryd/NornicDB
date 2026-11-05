# NOR-31 / PR #1560 readiness

- [x] Confirm Tim's assignment, release target and non-T3 scope; close merged NOR-28 dependency.
- [x] Audit PR conversation, reviews and inline threads; rebase onto the merged procedure-owner release head.
- [x] Reproduce and resolve the complete reported identity family: endpoint reconstruction, relationship creation, imported mutations, policy/decay functions, HTTP retrieval and bulk writers.
- [ ] Preserve fixture and bounded-stream contracts; resolve all in-scope suite regressions.
- [ ] Commit the complete correction and prepare PR/Linear publication evidence.
- [ ] Run make test on the final committed head and publish its result without skipping failures.

Local gate: make test only, per maintainer direction. Broader verification is deferred to CI. Final receipt and publication status belong to the PR/Linear record, avoiding documentation changes after the verified commit.

Evidence: initial full suite failed at provenance consumers, fixture ownership, event forwarding, numeric ownership inventory and quota accounting. Corrected the causal paths without changing thresholds or adding skips. New public regressions cover projected endpoints, CREATE/MERGE source selection and cross-source rejection, imported DELETE, pending SET/commit/rollback, constituent policy metadata and HTTP constituent retrieval. A production carrier census guards ID-copying native projections. Earlier full runs are superseded by the final committed-head receipt; cached packages and unexecuted broader checks must be reported there.

Duplicate-checker findings: consolidate entity scalar argument binding; unify policy/decay scoring; centralize node/relationship MERGE; remove the CREATE wrapper; converge HTTP/Bolt construction on the canonical executor factory; unify prepared/unprepared Fabric execution; unify relationship property projection; share policy source/schema selection. Checker source and runtime remain unchanged and immutable. Hook rerun and final suite are pending after these corrections.

The second unchanged-checker run reports none of the original production findings. Addressing its additional findings: one table-driven scalar contract matrix replaces duplicated policy tests, shared namespace-transaction fixtures serve both schema layers, and Bolt cache/explicit lifetimes share one executor selection owner. Prior thresholds and assertions remain enforced. Focused five-package identity/protocol validation passed before this additional consolidation; final focused and full-suite receipts remain pending.

Third unchanged-checker run: remaining findings were the Bolt test factory pair and standalone BEGIN wire handler. Consolidate the test factory across lifetimes and fold BEGIN wire decoding/admission into the central dispatcher; existing transactionLifecycle and Cypher retain native BEGIN/terminal ownership. Migrate handler tests to the actual dispatcher, without a compatibility wrapper. The complete prior source suite passed Cypher (446.381s) and all packages except the startup embedder integration test in cmd/nornicdb; isolated reproduction of that test passed and package diagnostics are running. This earlier run is not a final-head pass.

Fourth unchanged-checker run reports only duplicated Bolt test cases. Merge database-scoped rollback cases into one matrix and consolidate transactional/protocol-only BEGIN, COMMIT, ROLLBACK and RESET cases with all original state and invocation assertions. The full Bolt package and cmd/nornicdb package both pass before this final test-only consolidation. Keep the initial startup-embedder failure as historical diagnostic evidence; do not call the superseded full run a pass.
