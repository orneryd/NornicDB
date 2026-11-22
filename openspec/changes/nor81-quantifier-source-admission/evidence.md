# NOR-81 / N152-148 evidence and limits

This file preserves the supplied reference and reproduction evidence, the
source-owner inventory at the inspected checkout, the duplicate/reuse
comparison, the sibling-defect separation and the open limits. No production
code, test, build or benchmark was run for this artifact: CPU was reserved by
the parent and the task is bounded to investigation and OpenSpec text.

## 1. Sources, provenance and freshness

| Item | Record |
|---|---|
| Review ticket | `/tmp/n152-148-ticket.json`, sha256 `1f959d3d26e0077bc621b948c7d92a33b1d26feb61198d64e28b50f4d10c6de3` (Plane N152-148, `NOR-81 · Match reference acceptance of integer-valued quantifier sources`) |
| Original issue | Linear NOR-81, https://linear.app/nornicdb/issue/NOR-81/match-reference-acceptance-of-integer-valued-quantifier-sources (created 2026-10-23, `Backlog`, labels `Internal`, `Bug`) |
| Original triage attachment | `nornicdb-bug-triage-2026-10-23.md`, section `D14: Match reference acceptance of integer-valued quantifier sources`; content-type `text/markdown`, 29,040 bytes, etag `3cb57807c81ab258097c028e0a19ef66`, last-modified 2026-10-23. The Linear download URL is signed and expiring and is intentionally not copied here. |
| Inspected checkout | `/private/tmp/n152-148-quantifier`, HEAD `6f52ed1187fd9061edf5eb59d15d06fe59ebe938`, tree `4faa9d0ce0f6ae542af839db0086d696503b1cad`, clean at inspection time |
| Ticket source | `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5` (NornicDB source recorded by the ticket) |

Freshness checks performed (source inspection only, no execution):

- `git diff d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5..HEAD` over the ten
  cited production files shows only `pkg/cypher/match_semantic_validation.go`
  (+16/−6, CALL-conclusion wording and helper) and `pkg/cypher/statement_admission.go`
  (+50, protocol CALL/YIELD admission) changed. The list-source rule files
  (`expression_validation.go`, `static_comprehension_types.go`,
  `static_operator_types.go`, `static_operand.go`, `quantifier_fold.go`,
  `executor_internal.go`, `static_function_argument_types.go`,
  `static_operand_owner_test.go`) are byte-identical at `d6ec7766…` and HEAD.
  `d6ec7766…` is not an ancestor of HEAD, so this is content equality across
  diverged history, not an ancestry claim.
- HEAD is newer than the last two Linear retests. Between `b098f7e70`
  (2026-11-04) and HEAD, four rule files changed (+109/−31): the NOR-407
  backward header read in `coercingIterationDeclaration`, the NOR-150 context
  migration in `static_comprehension_types.go`/`static_operator_types.go`, and
  `static_operand.go` changes. No hunk in that diff mentions quantifier,
  `List<T>`, `isListTypeName` or `timestamp`; the rejection lines cited below
  are present in the inspected HEAD source. The ticket's own claim that the
  relevant v1.5.1 source is unchanged is true for the rule files but not for
  the two admission-plumbing files; the current HEAD is what was inspected.
- The last two Linear comments record the same outcome, NOT FIXED, on release
  heads `31208b17f` and `b098f7e70` with `expected List<T> but was Integer`.
- Graph RAG supplied with the task was incomplete (search/commit timeouts, code
  baseline unavailable), and live discovery of the `nornicdb`/`nornicdb_docs`
  MCP servers failed at handshake (30 s timeout / connection closed). The
  duplicate checker was not run. The owner inventory below is source-level in
  this checkout and is not an indexed or checker-verified verdict. It does not
  claim that no other duplicate exists outside the searched paths.

## 2. Recovered original fixture

The review ticket embeds the complete reproduction harness inline. Extracted
verbatim from the ticket's `description_html` code block (not retyped):

| Artifact | Bytes | sha256 |
|---|---|---|
| `fixtures/nor79_to_82_static_claims_test.go.txt` | 17,241 | `ced90f8c9729737ab2b8d81416426a418540bb990143fe852d176f8ee53918c5` |

The extractor is `fixtures/extract_fixture.py`; it reads the ticket path and rewrites the two files. The extracted harness is 391 newline-terminated lines and defines `TestNor79To82StaticClaims`,
`TestNor79ParameterAwareConstructorClaims`,
`TestNor79To82NegativeControlsReuseCanonicalVerdicts`, the parser/mode/statement
helpers and the verdict classifier. The NOR-81 claims in it are:

```go
var nor81StaticClaims = []norStaticClaim{
    {
        name:    "any_over_timestamp",
        query:   "UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a",
        accepts: true,
    },
    {
        // Negative control, supplied as rejected; also the canonical owner's entry.
        name:         "control_list_comprehension_over_date",
        query:        "UNWIND [] AS s RETURN [x IN date() | x] AS a",
        messageClass: "Type mismatch",
    },
    {
        // Negative control, supplied as rejected; also the canonical owner's entry.
        name:         "control_reduce_over_toInteger",
        query:        "UNWIND [] AS s RETURN reduce(a = 0, x IN toInteger('1') | a) AS a",
        messageClass: "Type mismatch",
    },
}
```

The Linear description gives the original forms without the wrapper:
`UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a`,
`[x IN date() | x]` and `reduce(a=0,x IN toInteger('1') | a)`. Both forms are
preserved: the wrapper is the matrix/reference empty-input form, not a rewrite
of the control expressions.

SHA-256 of the three supplied probe texts:

- `UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a` —
  `e791d5df7f90475a55b16e5d28376f4f3656f461e41ee066625555152878b559`
- `UNWIND [] AS s RETURN [x IN date() | x] AS a` —
  `63db26ac8a82db4651e5113f6d52983c9410d1e3dbf711446d06d703584f15fb`
- `UNWIND [] AS s RETURN reduce(a = 0, x IN toInteger('1') | a) AS a` —
  `c508c51e75d1f177007b652789b704c5006f16d12147cd08430d697e3b05bb93`

`fixtures/nor81-reference-evidence.txt` preserves the exact reference script
excerpts and result excerpts for the three NOR-81 statements (see next section).

## 3. Reference evidence (Neo4j 5.26.30 community, supplied by the ticket)

From `/tmp/nor79-82-reference.sh` (recovered inline) and
`/tmp/nor79-82-reference-results.log`:

```text
81-accept RUN
exit=0
81-accept EXPLAIN
Plan: "EXPLAIN"
Statement: "READ_ONLY"
...
exit=0
81-accept PROFILE
Plan: "PROFILE"
Statement: "READ_ONLY"
...
DbHits: 0
Rows: 0
Memory (Bytes): 64
exit=0

81-list-reject RUN
Type mismatch: expected List<T> but was Date (line 1, column 29 (offset: 28))
"UNWIND [] AS s RETURN [x IN date() | x] AS a"
                             ^
exit=1
```

The reference script passes `-P '{i:5,f:1.5}'` to every statement, although the
three NOR-81 statements reference no parameter. From
`/tmp/nor80-81-reference.sh` / `/tmp/nor80-81-reference-results.log`, the
reduce control rejects for RUN and EXPLAIN/PROFILE:

```text
3 RUN
Type mismatch: expected List<T> but was Integer (line 1, column 42 (offset: 41))
"UNWIND [] AS s RETURN reduce(a = 0, x IN toInteger('1') | a) AS a"
                                          ^
exit=1
```

The reference container image is recorded by the ticket as
`sha256:174ca8edaafb189d2676027a738815b04afd56d7dc720bd08bbb37afb469483e`
(Neo4j 5.26.30 community, 2 CPU / 4 GiB, isolated ephemeral store, no published
ports). All 36 reference commands completed before shutdown; the normal
`docker stop` exceeded its default timeout and exited 137 with
`OOMKilled=false`, which the ticket explicitly does not present as a clean-exit
claim or a failed query. This evidence is supplied, not freshly reproduced
here.

## 4. NornicDB observed evidence (supplied parent run, not rerun here)

The ticket records a focused parent run (macOS 1.938 s, independent Linux
2.612 s) of:

```sh
env GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test -tags=noui,nolocalllm \
  -p=1 -count=1 -timeout=180s -run '^TestNor79To82' -v ./pkg/cypher
```

Expected FAIL, with the same 60 failing / 84 matching cells on both hosts and
the canonical negative-control-owner check PASS. The NOR-81 cells are 12
false rejections and 24 matching negative-control cells. Exact failure lines:

```text
nor79_to_82_static_claims_test.go:208: RUN: UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a: accepted by Neo4j, rejected here: Neo.ClientError.Statement.SyntaxError: Type mismatch: expected List<T> but was Integer
nor79_to_82_static_claims_test.go:208: EXPLAIN: UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a: accepted by Neo4j, rejected here: Neo.ClientError.Statement.SyntaxError: Type mismatch: expected List<T> but was Integer
nor79_to_82_static_claims_test.go:208: PROFILE: UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a: accepted by Neo4j, rejected here: Neo.ClientError.Statement.SyntaxError: Type mismatch: expected List<T> but was Integer
```

Every negative-control cell reports `--- PASS` in both parsers, both
transaction modes and all three statement kinds (24 cells). No NornicDB plan,
row count or runtime result for the accepted form exists yet because NornicDB
never accepts it.

## 5. Source owner and call path at HEAD

Type fact:

- `staticFunctionResultTypes` (`pkg/cypher/static_operator_types.go:39–41`)
  maps `timestamp` to `Integer`. This is the canonical function-result type
  table; it is correct and is not the defect.

Admission decision, first consumer:

- `validateListOperands` (`expression_validation.go:25–41`) is called at the
  end of `validateBoundParameterExpressions`
  (`executor_internal.go:231–260`). For each list position it calls
  `staticListOperand` (`expression_validation.go:690–762`), which:
  - reads a whole call (`timestamp()`) at 745–752;
  - types it with `staticTypeScope.operandOf` at 757, which delegates to
    `staticOperatorChecker.check` (`static_operator_types.go:415–445`);
  - returns `Integer` because `Integer` excludes `List<…>` at 758–761.
  `validateListOperands` then returns
  `localizedStatusError("Neo.ClientError.Statement.SyntaxError",
  "InvalidArgumentType", localization.CypherCoreListOperandTypeMismatch("Integer"))`
  (`expression_validation.go:39–40`); the message text is
  `Type mismatch: expected List<T> but was Integer`
  (`pkg/localization/messages_cypher_core.go:520–522`).
- `forEachListOperand` visits every `IN` outside quotes/comments/names
  (`expression_validation.go:46–69`). `coercingIterationDeclaration` already
  excludes only `FOREACH`/`FOR` headers (122 and following); quantifier
  headers are not excluded. The `IN` in `any(x IN timestamp() …)` is therefore
  treated as an ordinary list position.

Admission decision, second consumer:

- The static deriver types a whole quantifier call in `checkAtom`:
  `isQuantifierFunction(lowerASCII(function))` →
  `parseQuantifierArguments` → `checker.checkListComprehension(ctx, variable,
  list, predicate, "")` (`static_operator_types.go:1287–1290`).
- `checkListComprehension` (`static_comprehension_types.go:48–53`) types the
  source, then `if source.excludes(isListTypeName) { return
  operandMismatch(source, "List<T>") }`. For `timestamp()` this is the same
  `Type mismatch: expected List<T> but was Integer` text
  (`typeNameMismatchError`, `size_semantics.go:26–31`).
- `forEachClauseOperatorExpression` visits RETURN/WITH items whose text may
  need a static type check (`static_operator_types.go:1425–1495`);
  `mayNeedStaticTypeCheck` returns true for the statement because it contains
  `" IN "` (1396–1402). So the deriver reach is not hypothetical for this
  statement; it is the same clause/expression the operator check visits.

Call order (why the scan is called the observed first rejector):

- `executeInternal` normalizes the text, validates syntax and calls
  `validateBoundParameterExpressions` (`executor_internal.go:202`; function at
  231–260), whose last step is `validateListOperands` (260).
- The semantic admission that calls `validateStatementTypes`
  (`statement_admission.go:70`) happens later, and `validateStatementTypes`
  reaches `validateMatchSemanticScopes` → `validateStaticClauseTypes`
  (`semantic_validation.go:175`, `match_semantic_validation.go:1314–1352`).
- No runtime trace was taken; this is source-order evidence. Both consumers
  produce an identical message class, so the observed message alone cannot
  distinguish them. What is established is that correcting only one leaves the
  other rejecting the same statement.

Graph-variable consumer (out of the correction's scope):

- `staticListOperandTypeError` (`expression_validation.go:90–109`) walks the
  same `forEachListOperand` positions but only consults the scope's bound
  variable type. Its doc comment and
  `list_operand_graph_type_test.go` record `any(x IN r WHERE …)` for a
  Relationship-bound `r` as Neo4j's `List<T> but was Relationship` type
  mismatch. That recorded claim was not independently re-verified in this
  task. A quantifier exemption added to the scan must not bypass this
  separate path.

Function-catalog note:

- `function_catalog.go:114–117` declares `all`/`any`/`none`/`single` with
  `list :: LIST<ANY>`. `functionSyntaxForms` (`function_arity.go:27–30`)
  excludes those syntax forms from `buildStaticFunctionArguments` and
  `buildFunctionArities`, so the quantifier header is not checked through the
  catalog's argument walker. Changing the catalog would not reach this defect.

## 6. Duplicate / reuse analysis (nornicdb-duplicate-review skill)

The configured duplicate checker and code graph were unavailable (section 1).
This is the required source-level comparison of the candidate implementations.

| Candidate | Location | Input contract | Behavior on `any(x IN timestamp() …)` |
|---|---|---|---|
| Pre-execution list scan | `expression_validation.go:25–41`, `staticListOperand:690–762` | Whole literal, bound parameter value, whole call/property/variable after `IN`; no scope except parameters | Rejects: `Integer` excludes `List<…>` |
| Scope-typed list scan | `expression_validation.go:90–109` | Only a whole identifier bound to Node/Relationship/Path in the current scope | No opinion for `timestamp()` (not an identifier); rejects `any(x IN n …)` for graph-bound `n` |
| Comprehension/quantifier deriver | `static_comprehension_types.go:48–53`, reached for quantifiers at `static_operator_types.go:1287–1290` | Any expression the checker can type, with the local scope and params (semantic path passes nil params) | Rejects: `Integer` excludes `List<…>` |
| Reduce deriver | `static_comprehension_types.go:146–151` | Parsed reduce form: acc initializer + list source + step/predicate | Not reached for `any`; keeps rejecting the reduce control |

Comparison and decision:

- Candidates 1 and 3 implement the same admission rule for the same source
  expression and produce the same public message class. They are duplicates of
  one decision, not two responsibilities: 1 is the pre-execution scan used
  before/without the deriver and for bound parameter values; 3 is the typed
  expression deriver needed for the quantifier result and body types.
- Candidate 2 is genuinely different: its input is a scope-bound graph entity,
  and its recorded reference behavior (reject Node/Relationship/Path) is
  distinct from a static function result. It should stay separate.
- Candidate 4 is reduce's own form contract (source list, accumulator type,
  step type); it is genuinely different and carries one of the required
  negative controls.
- Reuse decision: the correction belongs in the one shared decision, with the
  typed deriver as the canonical home because it is in the declared static
  typing owner files (`static_operand_owner_test.go:86–89`) and already owns
  the quantifier's element/result typing. The pre-execution scan must consume
  the same decision for the positions it alone covers; it must not grow a
  second, independently worded rule. No third admission implementation is
  justified. The function catalog is not a reuse target for the reason in
  section 5.
- Consolidation risk: the scan also serves literal, parameter, `IN`-operator,
  comprehension, reduce and `FOREACH` positions. The correction must key on
  the quantifier header position, not on the source type, so those positions
  keep the current contract.

## 7. Sibling defects (distinct; not fixed or claimed here)

Same matrix, same run. The following observations are quoted from the supplied
log; owners are source-level classifications, not fresh causal proofs.

### NOR-79 — parameter value in a comprehension source

- NornicDB: `Type mismatch for parameter 'f': expected List<T> but was Float`
  for `UNWIND [] AS r MATCH (n) WITH n, [x IN $f | x] AS v RETURN v[0] AS a`
  with `f = 1.5`; reference accepts (12 false rejections).
- Owner: the parameter boundary of the same scan,
  `staticListOperand`'s `$` branch (`expression_validation.go:695–708`,
  `isCypherListParameter`, `cypherTypeName`) and
  `CypherCoreListParameterTypeMismatch`
  (`messages_cypher_core.go:524–526`).
- Distinction: the reference fact is a bound parameter value in a
  *list-comprehension* position. It is a different rule branch and a different
  reference acceptance than NOR-81's function-call quantifier source. A
  quantifier-only exemption must not silently change it, and fixing it must
  not be claimed as NOR-81 acceptance. NOR-79's 258-case canonical population
  and 120 generated parameter cases remain its own evidence burden.

### NOR-80 — DELETE CASE with a COUNT subquery

- NornicDB: `Neo.ClientError.Statement.SyntaxError: DELETE expression "CASE
  WHEN COUNT { MATCH (m) } > 0 THEN n END" refers to an undefined variable`
  for `UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN
  n END`; reference accepts (12 false rejections).
- Owner: DELETE target resolution/validation
  (`staticDeleteTargetTypeError`, `executor_mutations_delete_guard.go:351–359`;
  message owner `set_semantic_validation.go:140`), reached from the
  `pipelineClauseDelete` branch of `validateStaticOperatorTypes`
  (`static_operator_types.go:1583–1589`). The procedure-`CALL` variants of
  NOR-80 are separate expression/WHERE cases.
- Distinction: no list position is involved. The other two NOR-80 statements
  and their CALL-bearing variant pass.

### NOR-82 — uncertain addition result into `head`/`toUpper`

- NornicDB: accepts `UNWIND [] AS s MATCH (n) RETURN
  toUpper(head(n.k + date())) AS a` and the `duration('P1D')` variant, while
  the reference rejects both with `Type mismatch: expected String but was Any
  or Date/Duration` (24 false accepts across the two probes).
- Owner: static argument/derivation typing of the addition result and
  `head`/`toUpper` (`validateStaticFunctionVariablesTyped`,
  `static_function_argument_types.go:817–877`; addition result types
  `static_operator_types.go:312–331`; `head` element typing via
  `elementOperand`, `static_operand.go:360–369`). The ticket records the
  reconstructed `[]`/`n.j` variants as accepted by both.
- Distinction: this is a missing rejection, not a false list-source rejection;
  it is in function-argument/addition typing, not list admission. The
  ticket's 64 inherited H2 false accepts were not supplied inline and are not
  covered.

## 8. Limitations (must not be read past)

- No production correction, test, build, benchmark, race/stress or mesh run
  was performed here; this change is investigation and OpenSpec text only.
  The failing parent run and the reference run are supplied ticket evidence,
  not fresh runs.
- No fresh Neo4j query was executed by this task. The reference verdicts for
  the exact three NOR-81 statements are taken from the ticket's recorded
  Neo4j 5.26.30 run and the Linear issue. No other reference verdict is
  invented.
- The exact family population behind D14 ("Out-of-family handoff §5" / static
  PR known differences) was not supplied and is not present in this
  repository. The D14 attachment section and the recovered harness cover only
  the three statements above; no whole-family acceptance is claimed.
- The reference gives no verdict for quantifier sources that are non-list
  literals, bound non-list parameters, other scalar-returning functions,
  temporal/duration values, or graph-bound Node/Relationship/Path sources.
  Whether the correction admits all of them or only function-call sources is
  an operator/parent decision; the proposal records the gap.
- Nonempty-data runtime semantics are out of scope and unverified. The current
  evaluator already coerces a non-list to a one-element list in the direct
  row-expression test (`functions_test.go:2699–2708`), but that is not the
  public path once static admission changes, and no accepted-form runtime
  behavior has been recorded.
- The semantic path calls `validateStaticOperatorTypes` with nil params in
  several clause scopes (`match_semantic_validation.go:1348`), so the deriver
  does not reproduce the scan's bound-parameter-value typing. The two
  consumers are not interchangeable, which is why the proposal keeps one
  decision but two call sites.
- The observed first-rejector attribution rests on source call order, not a
  runtime trace. Both consumers produce the identical message text, so a
  future run should confirm which one fires before and after the correction.
- Duplicate/graph retrieval was incomplete and the checker was not run
  (section 1). The owner inventory covers the searched `pkg/cypher` source in
  this checkout; it is not a clean duplicate-checker pass.
- The two admission-plumbing files differ from v1.5.1; the analysis was done
  on HEAD `6f52ed118…`, not on the older release head. The Linear retest heads
  are older than HEAD.
- `openspec/config.yaml` references convergence plan documents that are absent
  from this checkout, so no plan step can be cited. This artifact does not
  amend the config or invent a step.
