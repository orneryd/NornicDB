# AGENT_STATUS (fix/per-row-1214-on-1231)
- Root cause: rowValueCompiler.compile copied evaluateRowValueText's trailing-subscript test (strings.LastIndex "[");
  #1231 changed the text path to closingBracketGroupStart, so m[r[k]] compiled to the fallback (unresolved) while
  the text path evaluated it. Failing forms: any/none(k IN keys(e) WHERE m[e[k]] ...) in QPP step predicates.
- Fix a05f949e: one reader each for subscript (rowSubscriptStart), subquery atom (rowSubqueryAtom), pattern
  comprehension atom (rowPatternComprehensionAtom); corpora extended (fail without fix).
- #1231 + #1214 test sets pass both parsers. Full suite: sig/p1214/msuite.out. Sweeps+TCK: sig/p1214/sweep/run.out.
- Bench queued p1214m-a (wt-1214on1231base=0b514891+bench file vs wt-i77on1231). Don't edit until built.
- msuite: all chunks pass both parsers (TestM[b-df-z] antlr OOM-killed, passes split). Floor interleaved vs 0b514891: antlr 16.0K vs 14.8K, default 19.5K vs 19.6K. Sweep 1768 cases + TCK pass both parsers.
- rebased: #1214 commits + fix onto #1231 1023048a -> head 15c84dd5 (no conflicts; round-4 touches no row-evaluator file). backup branch backup/per-row-1214-on-1231-a05f949e. Focused tests pass both parsers. Full suite + TCK ratchet running (sig/p1214/mfinal.out).
- 15c84dd5: full suite both parsers clean (TestM[o-z] default OOM-killed once at 66s under VM pressure, re-run whole/split passes in 7s); TCK ratchet both parsers ok. p1214m-a (old base 0b514891 vs a05f949e): speedups kept; flags Northwind CustomerCategoryDistinctOrders +7.3% p=0.023, Rel450/property B/op +8% p=0.043. Queued p1214m-nw (1023048a vs 15c84dd5) to check Northwind.
- p1214m-nw (1023048a vs 15c84dd5): Northwind ~ p=0.739; no regression. Base worktree removed. DONE; not pushed.
- develop rebase: head f3c31a2d on origin/develop 59d59237 (patch identical to 15c84dd5; backup branch backup/per-row-1214-on-1231-15c84dd5). Tests running (sig/p1214/mdev.out). Bench queued p1214dev-a (wt-devbase-59d vs head), p1214dev-b (wt-1214benchbase=497dc18d+head bench file vs head).
- f3c31a2d GREEN: focused both parsers, full suite 50/50 chunks, TCK ratchet both parsers. Waiting p1214dev-a/b.

## PR #1240 round 1 implementation
- Starting production HEAD: f3c31a2d8f181a1a4d5969034b47a2bb0333ccca.
- Read binding RULE_TRUENAS.md; after user update all local Docker and host Go prohibited.
- Created neo-1240f before update; removed with docker rm -f as final Docker command. Reference query batch was interrupted; no Neo4j answers claimed yet.
- Added regression tests for review findings 1-4, including text/compiled values and explicit transactions. No production changes in this checkpoint; use it to reproduce f3c31a2d behavior on both parsers remotely.
- Focused tests and Neo4j checks pending announced TrueNAS runner. Full verification and benchmarks will be queued on final committed head.
- Implementation checkpoint: shared runCaseExpression for context/text/compiled; shared list construction and string value predicate; context quantifier/comprehension adapters call row list loops; removed integer equality bypasses and dead SET/compare helpers plus direct tests.
- Logical folds now defer explicit unresolved-with-error operands, context operands use isolated error sinks, and planned junctions retain null/error values until the shared fold decides.
- CASE predicate oracle now has literal expected answers, replacing the self-comparison for CASE corpus entries.
- Source diff passes git diff --check. Go formatting/build/focused tests and all Neo4j expectations remain pending TrueNAS; no host Go used.
- Reproduction checkpoint f0d36b9b retains exactly f3c31a2d production files plus failing expectations. Detached worktree wt-1240-r1-repro preserves it while the implementation stays on its assigned branch.
- Added leading-CASE logical operand regressions; both compilation and text evaluation now leave top-level logical expressions to their shared fold.
- Final source pass: a whole CASE must end at its own END; top-level logical chains remain under the logical owner even when both operands are CASE expressions.
- Current round-1 changes are source-confirmed only. Reproduction on f3c31a2d (via test-only f0d36b9b), both-parser focused tests, actual gofmt, Neo4j references, and speed validation remain pending runner readiness. Do not treat the implementation as verified.
- Local Docker cleanup is complete; no local Docker or host Go has run after the prohibition. Await the readiness announcement requested asynchronously before invoking tnrun.

## PR #1240 round 1 TrueNAS verification (15 Oct 2026)
- Runner used after the readiness announcement; no approval prompt occurred and no local Docker, Go, Python or Node test harness ran.
- Both-parser pre-fix reproduction: f0d36b9b contains unchanged f3c31a2d production code plus the tests. Findings 1-4 fail on both default and ANTLR: context CASE returns 1 instead of 0, list comparison raises the map type error, deciding OR/AND still raises division by zero, and deleted relationship endpoints return live node values. Log: review/1240-prior/r1-repro-f3c31a2d.txt.
- Focused 322c988e run: 64 top-level tests pass on default and the same 64 pass on ANTLR, including review regressions, independent CASE oracle, compiled/text corpora, logical operators, quantifiers, comprehensions, subscripts, CASE and deleted-entity reads. Log: review/1240-prior/r1-focused-322c988e.txt.
- Neo4j 5.26.30 neo-ref: all positive reference queries match the expected results, including DELETE / DETACH DELETE inside explicit transactions; all 10 separately submitted error queries raise the expected division-by-zero error. Logs: review/1240-prior/r1-neo-positive.txt and r1-error-1.txt through r1-error-10.txt.
- Remote Go formatting (Go 1.26.6, go/format) accepted all changed Go files and removed two extra blank lines in mixed-purpose tests. No semantic fixes were required after the focused run.
- Follow-up changes are formatting, a verified-reference comment, and this status record only. User will requeue the new committed head; verification and benchmark queues were not modified in this follow-up.
- Full make verify and performance comparison results remain pending the user's TrueNAS relay.

## Allocation measurement follow-up (15 Oct 2026)
- User reported intermittent failures of TestRowValueProgramListAllocationsDoNotGrowWithList during the old f3c31a2d full coverage run.
- On 8d02ae4c, the requested isolated -count=10 runs passed on both parsers, both without and with -cover (40 passes). An additional both-parser -count=10 -cover run with GOGC=1 passed too (20 passes). No current-head per-element growth reproduced.
- Measurement hardening under verification: complete any prior GC and suspend automatic GC for the bounded test (restore the original setting), warm short and long rows together, increase AllocsPerRun from 20 to 100, log both counts, and keep the exact zero-growth assertion.
- Both-parser hardened runs (plain and coverage) and a deliberate no-memo control are queued on TrueNAS. The temporary memo mutation was restored immediately after the client uploaded its snapshot; no production code changes remain locally.
- Hardened measurement verified: ten runs per parser pass without coverage, and ten per parser pass with coverage (40 passes; all 360 expression/list-size measurement pairs have identical short/long counts). Logs: review/1240-prior/r1-alloc-hardened.txt and r1-alloc-cover-hardened.txt.
- Sensitivity control verified on both parsers with coverage: temporarily bypassing memo reuse fails the unchanged zero-growth assertion for four expressions. For none(k IN $keys WHERE k = startNode(r).id), allocations rise from 484 (20 elements) to 4,804 (200), or 24 per added element. Other detected slopes are 56, 28 and 24 allocations per element. Log: review/1240-prior/r1-alloc-memo-disabled-control.txt. Mutation is absent from the final source.
- This follow-up changes only the allocation test and AGENT_STATUS.md. No allocation-growth production bug was reproduced on 8d02ae4c; the change removes cold/GC measurement sensitivity without changing the list sizes, expressions or allowed per-element growth (zero).
- User owns requeueing the new head; no verification or benchmark queue edits were made in this follow-up.

## PR #1240 round 2 checkpoint
- Review wrong-result families 1-4 reproduced on both parsers before edits (review/1240-prior/r2-before.txt); literal expectations independently checked on Neo4j 5.26.30.
- Removed CASE prebinding, eager argument-validation walkers, and dead string predicate implementation. Context/binding string predicates use the shared value owner; compiled null predicates and coalesce call shared owners; integer ordering uses compareCypherPredicateValue.
- Subquery OR operands use an isolated error context. Reached invalid arguments still error; Neo4j confirms the existing EXISTS AND error behavior, which remains covered.
- Initial focused implementation run passes both parsers (r2-third.txt). Final coverage run including reached-error controls and fn owner checks pending.
- Items 5, 8 and 10 remain untouched for the separate stacked refactor/one-operation-owner branch.
- Expanded coverage checks pass both parsers (r2-focused-pass.txt). Binding checks exposed integer boxing in the shared comparator; compareCypherPredicateValue now accepts typed operands, keeps the integer fast path inside that owner, and shares the comparison-result fold. Existing arithmetic zero-allocation assertions pass both parsers (r2-integer-owner.txt).
- Validation now checks graph arguments after evaluation in row/context adapters, retaining rejection of ordinary maps. Neo4j confirms all ten reached-error probes, including map arguments (r2-error-1.txt through r2-error-10.txt).
- Final committed-head focused coverage checks pass default and ANTLR, including all new reproductions, function-owner tests, binding filters and unchanged allocation assertions (r2-committed-focused.txt). Removed the now-unused string operand helper after confirming it has no callers.
- Queued p1240-r2 benchmark comparison with the requested regex. User owns full-check requeue and push. Main branch excludes items 5, 8 and 10, which are on the stacked follow-up.

## PR #1240 round 3 checkpoint
- Starting main head a9d0049f; stacked head c955225c. Read RULE_TRUENAS.md again; all execution uses tnrun on TrueNAS.
- Independent new tests reproduce entity CASE/membership snapshot failures, pattern-comprehension OR error leakage, null substring range failure and skipped-subquery allocation growth on both parsers (r3-impl-before.txt). Counts are 83/803; fixed allocation limit is 3 for each list length.
- Own Neo4j 5.26.30 queries confirm snapshot values [true,true,1], deciding pattern OR result 1, null substring rows, integer equality and graph/size values (r3-impl-neo-positive.txt). Negative substring emits the literal range error (r3-impl-neo-error-substring.txt).
- Per git diff against 59d59237, finding 3's isolation and finding 4's cached substring validation are PR changes; labels/type were changed in round 2. These move to #1240 under the user's scope rule. Context indexing and size value implementations are unchanged from base and remain on the stack.
- Implemented entity identity in cypherEquality, typed equality through that owner, reached-only error isolation, shared live null handlers, removed dead null helper/direct tests, shared fallback resolution, removed duplicate substring validator, and shared labels/type value owners. Focused checks in progress.
- Focused coverage checks pass both parsers, including round 1-3 reproductions, text/program corpora, CASE/logical/comparison tests, labels/type registry adapters, and unchanged arithmetic allocation assertions (r3-impl-focused-cover.txt).
- Both skipped-subquery cases measure exactly 3 allocations at 20 and 200 elements (r3-impl-second.txt). Typed integer equality also passes its zero-allocation assertion. Negative non-null substring matches the exact Neo4j range message.
- This round is one new fix commit on top of a9d0049f; pending full verification will be queued by replacing its existing line. Context indexing and size owner convergence remain for the rebased follow-up.
- Final finding-1 audit also reproduced indexed membership comparing graph pointers by address on both parsers (r3-impl-indexed-entity-before.txt). Graph values now stay outside the scalar comparable-key index and compare through cypherEquality; snapshot, membership/cache and unchanged allocation checks pass both parsers with coverage (r3-impl-indexed-entity-fixed.txt).

## PR #1240 round 4 checkpoint
- Re-read review/1240-prior/r4.md and binding TrueNAS rules. No local workloads run.
- Both parsers reproduce the nested-function stack overflow, predicate allocations 13/15 against budgets 10/12, pattern AND leakage, and comparison-chain errors (r4-impl-before.txt, r4-impl-crash-before.txt).
- Shared fallback terminates subquery-containing function calls at the registry; unary math detects top-level argument commas. Shared comparison owner now carries operand errors and lets a deciding false discard them. Pattern-comprehension evaluation returns isolated errors when reached. Predicate logical adapters allocate isolation only for context writers/fallbacks.
- Added 150 literal Neo4j corpus expectations; own Neo4j 5.26.30 run confirms them (r4-impl-oracle-neo.txt). Prior evaluator comparisons are explicitly supplementary adapter contracts.
- Both-parser focused tests and coverage pass (r4-impl-main-after.txt, r4-impl-main-focused.txt). Deciding OR uses 9 allocations, skipped EXISTS uses 11, below unchanged base budgets 10/12. Required bare EXISTS/AND and comparison errors verified separately against Neo4j.
- Stack-only indexing and eager validation repairs are being implemented separately. Final push and queue updates pending final verification.
- Round-4 reproduction/allocation tests also pass 10 repetitions with coverage on both parsers (r4-impl-main-repeat.txt); budgets remain unchanged. Removed unused per-operand identity arrays now that comparison normalization is owned by the shared chain.
