# Relationship reader and expression follow-up

Implementation branch: `fix/relationship-readers`, worktree `wt-rel-readers`.
Head: `47000a4027cc91bf7bd5d284fd7f30bf8dbd67d5`.
Base: develop `16923aef778025fab3490f6a57549763b536f3d5`, after #1407 merged. The branch was initially based on `5fcc6f84`, then rebased with only this follow-up's commits.
Parent thread: `075cf8e3-9632-45e4-a74b-ecbe4e778f84`.
PR: [#1473](https://github.com/orneryd/NornicDB/pull/1473), draft while A/B is pending; base develop, bug, milestone v1.4.4. Linked to this implementation thread and parent thread. Do not merge.

## Findings per reader

- `clauses.go`, UNWIND/MERGE chain: the static fast path declines dynamic expressions both before and after. Probes cover maps, slices, multiplication, quotes, braces, stars and ranges; repeated MERGE retains one relationship and runs both actions. It now takes the complete bracket with `findMatchingDelimiter` and reads its declaration through `relationshipDeclarationOf`, retaining the conservative static-plan admission. No runtime wrong answer was established in the old fast path.
- `optional_match_traversal.go`, endpoint reader: direct probes reproduced truncated dynamic and quoted types. This helper currently has no production callers; runtime OPTIONAL MATCH probes pass. It now takes its variable and type text from the shared declaration.
- `pipeline_write_stretch.go`, dependency tokens: direct probes reproduced the expression's `ts` map key being recorded instead of the actual relationship property `w`. Dynamic types already implied any relationship, so no runtime wrong result was established. The reader now records the shared declaration's actual properties and types. Clause visibility probes cover dynamic writes followed by reads.
- `query_info.go` and `query_patterns.go`, plan/info readers: direct probes reproduced truncated types and missing property expressions for quoted type names containing delimiters. Both consume the declaration; the neighboring relationship-variable reader also consumes it. Dynamic expressions do not become alleged static MERGE keys. EXPLAIN/PROFILE probes cover dynamic types with maps/slices/arithmetic and real ranges.
- PROFILE's recursive execution revalidated generated internal predicates as user text. It now retains the source before expression rewrites and executes that source through the regular entry, preserving validation, caching behavior and implicit write transactions. A failed profiled write leaves zero nodes.

## Expression and range corrections

- Nested-map inline properties: `rowSubscriptReceiverStart` now includes the map/call/list primary before a property chain, so `{outer:{xs:[11,17]}}.outer.xs[i]` resolves to 17. The text and compiled row evaluators already consume this single reader. Tests cover both entity kinds, CREATE/MERGE/MATCH/OPTIONAL MATCH, decoys, repeated MERGE, autocommit and explicit transactions.
- CASE dynamic types: dynamic-token admission now uses the existing function-signature owner's union handling. Both possible list result types are admissible; single-hop runtime validation remains. Tests also cover dynamic labels and property keys, and retain rejection of incompatible static types.
- Quoted-string panic: `opensSubquery` formerly counted `)` inside a string and sliced at -1. Both it and the map/subquery brace classifier now consume the shared quote-aware `matchingRowDelimiterStart`. The declaration type splitter also skips string contents. Red/green tests cover single quotes, double quotes and a map-expression type with `D):Z|Q{*..`, in both transaction modes and parsers.
- Dynamic ranges: Neo4j 5.26.30 ignores dynamic type values on legacy `*` ranges, including the first hop and unresolved row-dependent values; static types and single-hop dynamic types still filter, and property maps still filter every hop. This is normalized once by the relationship label-expression rewriter before traversal paths diverge. A retained internal predicate enables the existing static token check while returning true without evaluating a row-dependent type expression. The TCK corpus predates dynamic type syntax; its static map/property behavior was checked before adopting the Neo4j behavior.

## Verification

- Initial red expressions/readers, both parsers: `i20261018104419-72244-2349` (baseline `5fcc6f84` plus tests, commit `21fed52a`). Earlier diagnostic red: `i20261018103915-106128-3b60`.
- Added panic and range red, both parsers: `i20261018104923-86304-63bb`.
- Focused green, both parsers: `i20261018105352-53232-77e1` at `384ba8bc` (before final formatting and shared brace-reader convergence). Final-head full suite below includes all these tests.
- Neo4j references: `j20261018104546-61396-fce8` (nested maps, CASE, dynamic writes, OPTIONAL MATCH, EXPLAIN/PROFILE, write visibility); `j20261018104920-75080-d734` (panic query and range modes; final incompatible-static-type query intentionally errors); `j20261018105027-86104-dff5` (range values of mixed string/integer/null, deliberately not evaluated by Neo4j).
- Develop baseline known failure: `i20261018105449-83188-b545`, both parsers, `TestEvaluateWhereForContext_Branches` line 48. Baseline `e67ed1a0` is develop `16923aef` plus the exact head's test fixtures only; product code is unchanged. The existing #1384 regression is tracked separately and fixed by #1468.
- Final full `pkg/cypher`, both parsers, unsharded: `j20261018105726-63240-2b68`, complete: only `TestEvaluateWhereForContext_Branches` fails on each parser, identically to the develop baseline. All new tests pass. Default 273.8 s, ANTLR 366.3 s test time. Command: `bash sig/tnrun.sh go wt-rel-readers --parser both --shards 1 -- ./pkg/cypher -count=1 -timeout 90m`.
- Vet `i20261018105449-103892-799b` and real gofmt check `i20261018105449-72756-b896` passed before the final one-call convergence; final-head vet `i20261018105905-86280-5292` and gofmt `i20261018105905-75084-a032` also pass.
- Paired A/B: `a20261018105736-99416-70a9`, 10 rounds, dedicated lane, submitted `--no-wait`; queued. Baseline `wt-rel-readers-benchbase@e67ed1a0`, head `47000a40`. Both binaries contain identical test fixtures. Benchmarks: StatementRouting, MatchSemanticValidationCached, MergeUniqueKey, 1 s rounds. No other jobs held or moved.
- Superseded A/B jobs `a20261018104740-106124-9a2e` and `a20261018105429-89712-512b` were cancelled while still queued, with exact ID/base/head/label guards, via remote Go control jobs `j20261018105220-86424-ab59` and `j20261018105726-86924-833e`. Their terminal records state cancelled, exit 130. Earlier full-suite jobs were automatically superseded when this worktree advanced; they are not full-suite evidence.

Logs, reference queries and preparation notes are `sig/handoff/rel-readers-*`.
Confirmed compatibility findings are recorded in [#907](https://github.com/orneryd/NornicDB/issues/907#issuecomment-6248996254). Part of #1055.

All execution uses the TrueNAS clients; no local Go, Python, Node, Docker, builds, tests, formatting or benchmarks were run. Local operations are git, gh, file edits and tnrun clients.
