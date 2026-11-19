# TODO and evidence

- [x] Preserve original NOR70 fail-first sources and exact base e8cd436e540ed421327bb72478d4d98479f7a2ac.
- [x] Observe baseline null-only group/count-column failures on both parsers; ordinary Execute and native transaction controls already pass.
- [x] Reuse executePipeline/pipelineAggregateState for the exported optimizer; delete the independent reducer. Detector and PatternInfo behavior are unchanged.
- [x] Add typed group/count/null/order/window/failure checks under both parser processes.
- [x] Verify initial proposal on macOS and Linux; preserve benchmark and CPU profiles.
- [x] Investigate mandatory checker findings and converge repeated memory fixture construction into the existing newStatementExecutor(testing.TB, namespace, statement) owner in path_selector_search_bench_test.go. Migrate conditional, classification, duration, NOR70 and new aggregate test consumers; remove obsolete constructors.
- [ ] Reverify the fixture-converged source on macOS/Linux and fail-first old-production overlay.
- [ ] Pass unchanged canonical staged duplicate check over complete final source.
- [ ] Publish release/v1.5.2 PR and preserve complete evidence inline on existing Plane records; run CI.

## Correction and scope

PatternEdgePropertyAgg uses the canonical clause pipeline and only returns an
outcome when handled. Failed/declined evaluation keeps (nil, false). This fixes
an exported optimizer API defect; normal HTTP/Bolt Execute was already correct.
No global query correctness or new optimization claim is made. The deleted
private reducer's lexical population entry was removed: 4095 to 4094.

The two original reproduction files were copied unchanged onto e8 for the
fail-first run and preserved verbatim in the Plane evidence bundle and
/tmp/nor70-original-reproduction-sources. Final test setup is subsequently
converged for duplicate ownership; test assertions and scenarios are retained.
A programmatic comparison verifies original Test bodies unchanged after
normalizing only fixture-call lines. No checker suppression or threshold change.

## Actual checks and limits

Worker earlier whole Cypher package passed651.371s, internal/cyphertext and vet
passed. These predate parent test-only refinements and are not fresh final-source
package passes. An earlier default10minute run timed out: incomplete, not pass.
Worker initially used an interpreter missing ijson; parent used the configured
code-intelligence interpreter. The first canonical parent check analyzed39
functions and blocked on repeated fixture setup; no publication bypass.

Parent pre-convergence Mac boundary checks passed2.072s; migrated two API cases
with both parsers passed1.231s. Linux focused Cypher passed2.629s, lexical
population4.131s, migrated API cases both parsers0.804s. The first converged
compile exposed a missed duration fixture caller; it was migrated and reruns
are pending, not silently treated as passing.

## Bounded benchmark

Both arms use the same correct pipeline, same data and disabled result cache.
50/500 groups, ten relationships per group, ten iterations x3; Linux2CPU/4GiB,
Go1.27.4, GOMAXPROCS2/GOGC50/GOMEMLIMIT1500MiB. Correctness-gated run passed4.844s.
Median ns/op (canonical Execute / corrected ExecuteOptimized):

| Groups | Query | Execute | ExecuteOptimized |
| --- | --- | ---: | ---: |
| 50 | avg/count | 1330203 | 1136775 |
| 50 | name order | 1997129 | 1700378 |
| 50 | average order/limit10 | 1900445 | 1734659 |
| 500 | avg/count | 14195093 | 13010746 |
| 500 | name order | 21112697 | 20217062 |
| 500 | average order/limit10 | 21896238 | 20952221 |

Short sequential samples do not establish a general speedup or latency
neutrality. Execute includes entry/transaction bookkeeping. Old optimized
results fail correctness and are not a valid performance baseline. CPU profile
includes setup/validation outside benchmark timers:4.81s duration,5.57s samples.
The benchmark has unchanged source after fixture consolidation; applicable
runtime source is unchanged. Final fixture checks and CI remain pending.

No full repository/race/stress or local make verify is run under the requested
focused-check policy. make verify-list is a preview, not an executed suite.


The second checker pass found the already-general newStatementExecutor owner
(44functions,199.51s analysis). Parent inspected its full implementation and
chain/grid consumers: it owns exactly the same isolated memory-backed statement
setup and accepts testing.TB. All fixture consumers now call that existing owner
directly; the temporary generalized classification helper is removed. No new
setup abstraction or forwarding wrapper is retained. Both blocked checks remain
recorded; final gate rerun is required after staging complete final source.
