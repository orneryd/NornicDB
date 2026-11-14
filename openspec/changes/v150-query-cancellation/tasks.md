# B27 / B28: expression work stops on the caller's cancellation

Target: `release/v1.5.0` (checkout `ce22592d28cf5ca2e651491e174367df8781dcb0`,
worktree `v150-query-cancellation`, branch `codex/v150-query-cancellation`).

Source: `NornicDB-v1.5.0-state-report.md` lines 923-1055, sections 19 and 20.

- B27 (section 19): `reduce` over `range()` kept working for 7-14 s after a
  200 ms deadline, and the range-only statement for 408-1,033 ms, because
  `evaluateCypherRange` took no context and appended every value, and the
  reduce element loop folded every element without observing the caller.
- B28 (section 20): a statement joining about 720 string literals with `+`
  ran for 27-34 s and returned its deadline error only after that time. The
  saved stacks put the time in the expression-form recognisers
  (`expressionTextAccepted`, `containsOutsideStrings`, `splitRowArithmeticTier`),
  which scan an expression's text recursively and took no context.

## Approved contract

Public path: `StorageExecutor.Execute(ctx, …)` ends as the caller's own
cancellation (`context.Canceled` / `context.DeadlineExceeded`) within the
release statement budget of 100 ms after that cancellation, on both parsers
(`nornic` and `ANTLR`), and a canceled write stores nothing. Uncancelled
results for both parsers are unchanged. No parser replacement, no per-leaf
patching, no change to stored keys, payloads, WAL format or storage version.

Reserved findings B19, B20, B23, B24 and B32 are not touched.

## Canonical owners changed

- [x] `pkg/cypher/range_semantics.go`: `evaluateCypherRange` takes the
  caller's context and observes it every `readProjectionCancellationInterval`
  values while materializing, the amortization the transaction wrapper's
  projection already uses. Callers: `row_function_expression.go`,
  `functions_eval_functions.go`, `range_semantics_test.go`.
- [x] `pkg/cypher/row_list_loops.go`: `rowReduceLoop` observes the caller's
  context at each element it binds, so a fold stops at the element it is on.
  This is the one reduce loop owner for the text and compiled-program paths.
- [x] `pkg/cypher/runtime_type_errors.go`: `expressionTextAccepted`, the
  shared owner of expression-form admission, takes the caller's context and
  stops recognising a form once the caller is canceled (false, as the keyword
  scanners stop finding a keyword), so one long operand's scan no longer runs
  to its end. Behaviour for an uncancelled context is unchanged.
- [x] `pkg/cypher/static_operator_types.go`: `staticOperatorChecker.check`
  observes the caller's context at every operand and returns the cancellation
  as the statement's error without caching a typing verdict;
  `staticArithmeticOperandText` passes the context to the shared recogniser.
- [x] `pkg/cypher/operators.go`: the shared arithmetic evaluator observes the
  caller's context at each operand and records the cancellation in the
  statement's existing expression failure slot.
- [x] `pkg/cypher/row_subscript_validation.go`: passes the statement context
  to the shared recogniser (both call sites).

## Verification

- [x] Public-path regression tests written in
  `pkg/cypher/expression_cancellation_test.go`: the reported 722-literal
  chain as `RETURN` and as a `CREATE` property value, `range(1, 5000000)`,
  and a 200,000-element reduce, each with a deadline and each asserting
  `context.DeadlineExceeded` within 100 ms of the deadline, plus uncancelled
  controls asserting both parsers' exact results and that a canceled `CREATE`
  stores no node.
- [x] Current-release public chain/range failures recorded; direct original fold control also fails after cancellation.
- [x] Green run of public/component tests, both parser subtests, with the measured
  post-cancellation residue recorded.
- [x] Focused package checks for the touched owners (`pkg/cypher` build/vet
  and the existing range, reduce, static-operator, subscript and cancellation
  tests), plus the generated lexical-population checks that record
  `expressionTextAccepted` and `staticArithmeticOperandText`.
- [ ] Heavy make-test/census verification in exact-head CI; no local full-suite pass claimed.

## Known limits of this change

- The 722-literal statement stays cubic: recognising one operand costs
  O(terms²) and the static check recognises one operand per level, so the
  uncancelled statement still takes tens of seconds. This change makes the
  statement *stop* on cancellation; it does not make it fast. Removing the
  cubic cost means rewriting the operand-form recogniser into one linear pass,
  which is a parser-adjacent change the parent owns.
- Cancellation latency is bounded by one recognition step (one expression
  text scan at the current depth, microseconds for the reported shape), not
  proven by the tests: the tests assert the product result (statement ends
  within 100 ms of the deadline), not which owner observed the cancellation.
- The reported builds for the numbers in sections 19 and 20 are `2dbc67e96`
  and `b6d8e762a`; this checkout is `ce22592d2`. No fresh timing baseline was
  taken here (no Go commands were run by this worker).
- B28's `RETURN`-only route may not run the static operator check at all
  (`executeReturn` validates booleans, range calls and subscripts, not
  operator types). Its cancellation is then observed by the shared arithmetic
  evaluator or by the row evaluator's own bounded recursion.
- The streamed range source (`pkg/cypher/pipeline_row_stream.go`, the
  `UNWIND range(...)` row source) still yields every value without observing
  the caller; it was not in sections 19 or 20 and is left unchanged. Its
  consumers were not re-established here.
- `make verify`, the generated lexical-population checks and gofmt/vet were
  not run by this worker (no Go commands in the delegated scope). The census
  entries for `expressionTextAccepted` and `staticArithmeticOperandText`
  record function names and mechanisms only, but the parent must confirm the
  generated checks still pass after the signature and body changes.

## Remaining group10 acceptance criteria

- [ ] Correlated CALL imports through nested FOREACH / transaction subqueries (report28 / B22).
- [ ] CSV missing-property and float-list round trip (report30 / B25).
- [ ] Cached-result copy amplification without shared mutable outputs (report37 / B31).
- [ ] Scalar node materialization and first explicit-statement allocation costs (38.13/38.20 / B33/B34).
- [ ] Known actual CI failures receive verified corrections or explicit pending evidence.

Parent corrections: the initial constant-sum 200K reduce could finish before its deadline; use dynamic UUID folding over supplied items, plus a deterministic component that cancels during the first fold step. Compare semantic results, not timing alone.
