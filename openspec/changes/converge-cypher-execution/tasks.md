## 1. Establish TCK and issue baseline

- [ ] 1.1 Pin official TCK revision/checksum/license and record the complete feature/scenario/step inventory.
- [ ] 1.2 Implement Go step bindings, typed result/error comparison and observable side-effect checks with negative-control tests.
- [ ] 1.3 Run fresh-fixture autocommit and explicit-transaction scenarios over the production Bolt server; consume results and verify transaction completion.
- [ ] 1.4 Pin a Neo4j patch image/digest and validate the runner and fixed differential corpus.
- [ ] 1.5 Import every reproduction/variant for the 35 scoped issues, including #452's comment; record exact TCK matches or local-only coverage.
- [ ] 1.6 Check in exact known failures; fail on new/changed failures, missing scenarios, unexpected passes and harness errors.
- [ ] 1.7 Add required CI workflow, artifacts and runnable make targets; configure required-check enforcement during rollout.
- [ ] 1.8 Record test/coverage/benchmark baseline and regenerate the divergence candidate ledger with reproducible inputs.

## 2. Share execution context and lexical contracts

- [ ] 2.1 Extract common preparation while retaining public/internal cache, limits and transaction differences.
- [ ] 2.2 Introduce bound scope, inherited parameters, source spans and cancellation propagation without text substitution.
- [ ] 2.3 Converge quote/comment/bracket-aware scanning and prove complete shape/fragment consumption.
- [ ] 2.4 Add typed dispatch outcomes, effect-boundary tests and unresolved-expression instrumentation.

## 3. Repair mutations and storage visibility

- [ ] 3.1 Reproduce then fix #462/#474 through scoped assignment and recursively evaluated map values; enforce rollback on evaluation failure.
- [ ] 3.2 Reproduce then fix #455/#456/#470/#480; verify persisted graph, MERGE branches, null removal and label updates.
- [ ] 3.3 Build storage contract tests across Memory, Badger, production wrappers and transaction/multidb views.
- [ ] 3.4 Reproduce then fix #461 and #448; verify atomic publication, embedding flush visibility, reopen and any existing-data repair needs.
- [ ] 3.5 Normalize typed property access and capability forwarding; cover #475's storage leg and individual/batch mutation equivalence.
- [ ] 3.6 Verify own writes, stable snapshots, rollback, authorization, cache isolation and cancellation.

## 4. Connect ANTLR fallback

- [ ] 4.1 Audit grammar/AST representation and implement rule adapters for first RETURN/MATCH/WHERE/UNWIND/WITH/mutation slices.
- [ ] 4.2 Execute fallback through shared operations without reentering the legacy text dispatcher.
- [ ] 4.3 Add forced route selection for tests and extend the exact baseline with fallback results.
- [ ] 4.4 Prove effect-free misses fall back and runtime failures never replay statements.

## 5. Converge expressions

- [ ] 5.1 Fold computed-row and predicate evaluators into shared typed semantics; retain measured compiled fast leaves.
- [ ] 5.2 Fix list comprehension, numeric, reduce, postfix access, map and temporal issue families in all expression positions.
- [ ] 5.3 Propagate typed errors through all migrated callers; remove expression-text and implicit-null error fallthrough.
- [ ] 5.4 Verify CASE/COALESCE short-circuiting, null/undefined distinction, scope and parameter handling.

## 6. Converge composition

- [ ] 6.1 Fix aggregate discovery/grouping, projection aliases, ordered collections and full sort-key evaluation.
- [ ] 6.2 Fix UNWIND/OPTIONAL MATCH cardinality and correlated CALL; migrate UNION/FOREACH to bound child contexts.
- [ ] 6.3 Complete #447/#449–#452/#457/#459/#463/#464/#468/#469/#478/#479/#481 regressions and applicable TCK cases.
- [ ] 6.4 Preserve write/read barriers and prohibit LIMIT from skipping required writes, sorting or grouping.

## 7. Stream snapshot reads

- [ ] 7.1 Implement projected snapshot-visible iterators including pending mutations and early termination.
- [ ] 7.2 Reproduce #487 and demonstrate per-shape latency/allocation improvements with visit counters and profiles.
- [ ] 7.3 Verify snapshot isolation, cancellation, buffer lifetime and performance of existing correct workloads.

## 8. Retire remaining divergence

- [ ] 8.1 Converge non-policy DDL and relevant node/edge kernels with contract tests and benchmarks.
- [ ] 8.2 Complete all remaining pinned-core TCK gaps and preserve Neo4j/Nornic extension suites.
- [ ] 8.3 Remove superseded handlers/evaluators/text-substitution paths; document retained optimizations and public API adapters.
- [ ] 8.4 Resolve every Cypher-relevant report candidate and document unrelated deferrals.

## 9. Qualify completion

- [ ] 9.1 Pass full tests, race suite, coverage gates, lint, build and scoped performance/retrieval checks.
- [ ] 9.2 Emit redacted informational fallback reports and a reproducible optimization backlog.
- [ ] 9.3 Attach fixing commits/PRs and passing evidence to every scoped issue; verify closure criteria.
- [ ] 9.4 Update compatibility/parser-mode docs, public API examples and CHANGELOG; synchronize verified OpenSpec contracts.
- [ ] 9.5 Review final completion evidence, close verified issues through the normal repository workflow, and archive the completed program.
