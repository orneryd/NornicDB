# Validation

Original release head: f2e264f1f02f615ccbf9be498ed94993448dcb9d. Main comparison: b9036d7e2a8302982f0bd361ca458e43039d2a60. Candidate is stacked on prerequisite #1941 (af7c9f398c0729798dff450cbeb92833ff21a701), whose inventory changes do not alter the measured execution paths.

## Matched measurements

Mac Apple M2 Max, Go 1.27.4, GOMAXPROCS=2, GOMEMLIMIT=1500MiB, tags noui,nolocalllm, identical checked-in ratchet fixture installed temporarily in both trees. Each workload ran 10,000 operations, five samples, with CPU profiles and fixed result assertions. Allocation attribution used separate memprofilerate=1 runs; their timings are not compared below. Quiet windows were coordinated with the parent.

Median ns/op / B/op / allocs/op:

| Workload | Main baseline | Release before | Candidate |
| --- | --- | --- | --- |
| Internal scalar parameter | 5,471 / 902 / 15 | 12,046 / 2,111 / 33 | 5,513 / 903 / 15 |
| Public scalar parameter | 10,853 / 1,943 / 28 | 19,501 / 3,151 / 46 | 11,526 / 1,943 / 28 |
| Cached WITH parameter rebinding | Not part of this comparison | 29,507 / 8,072 / 85 | 29,588 / 8,072 / 85 |

The exact scalar allocation counts return to baseline. The public scalar latency remains 6.2% above main in these samples; this is not a latency-neutrality claim. Compared with the release, both scalar workloads improve materially and the broader rebinding workload retains identical allocation counts and bytes. General admission cost accepted in #1926 remains outside this correction.

## Checks

- Focused boundary/rebinding tests: PASS 1.079s.
- Broad normal `^Test(ParameterPassthrough|NOR79|Nor79|Static|CheckOperator|CompileTimeTypeErrors)`: PASS 88.803s.
- Package vet: PASS.
- Final lexical population: PASS 2.575s; exactly one new shared caller and two mechanism changes, with no unrelated inventory changes.
- `make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization`: PASS 7s. All other verifier steps explicitly skipped; this is not full verification.
- Race and canonical allocation verifier: pending at this checkpoint.
- Staged duplicate analysis using the configured Python environment failed: configured localhost:7474/nornicdbcode refused the connection. No checker/configuration override was applied. An earlier invocation using system Python lacked dependencies; the configured environment has them and reached the service. Parent independently confirmed the service failure and authorized normal Git commit under CONTRIBUTING's current release policy. Manual owner review confirms reuse of returnProjectionPlan, the pipeline clause reader and ParameterReferences, with no parallel parser/cache. Indexed duplicate analysis remains incomplete.
- First draft build failed on an incorrect returnProjection field name; corrected before runtime tests. Original log retained.
- Lexical scratch on #1941 found one added shared caller plus two existing selectors reclassified by the conservative credential-field discovery. The existing owners still have shared=true; the field is only an immutable syntax-proof Boolean. An initial candidate inventory assertion failed before those exact mechanism changes were recorded; no source or population guard was weakened.
- The temporary exact ratchet fixture is not part of the committed change; the canonical verifier installs that same fixture itself.

No full make test, full make verify, Linux CI or broader performance qualification is claimed by these focused results. Native canonical ratchet results will be recorded separately against their exact committed head.
