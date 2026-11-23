# NOR-82 / N152-149 evidence

Everything below is extracted from the approved ticket or read from the checkout.
This card ran no build, test, benchmark or Neo4j mesh; CPU was left to the
parent's performance verification. Recorded evidence is quoted as recorded and
is not presented as a fresh run.

## Artifacts

| Artifact | Source | Integrity |
|---|---|---|
| `fixtures/nor79_to_82_static_claims_test.go.txt` | ticket `<pre>` block under the heading `pkg/cypher/nor79_to_82_static_claims_test.go`, HTML-unescaped, byte-for-byte otherwise | 391 lines, 17241 bytes, sha256 `ced90f8c9729737ab2b8d81416426a418540bb990143fe852d176f8ee53918c5` |

The recovered fixture is the full file: the shared cell harness
(`norStaticClaim`, `norStaticExecutions`, `norStaticTransactionModes`,
`norStaticFreshExecutor`, `norStaticRollback`, `norClassifyVerdict`), the four
issue tables, `TestNor79To82StaticClaims`,
`TestNor79ParameterAwareConstructorClaims` (the canonical 258-case constructor
population through its positional oracle) and
`TestNor79To82NegativeControlsReuseCanonicalVerdicts`. Its NOR-82 table is the
recovered part this card preserves:

- `toUpper_head_dotk_plus_date` — rejected by the reference (`Type mismatch`);
- `toUpper_head_dotk_plus_duration` — reconstructed, rejected (`Type mismatch`);
- `toUpper_head_dotk_plus_empty_list` — reconstructed, **accepted** by the fresh
  reference, asserted as an acceptance control;
- `toUpper_head_dotk_plus_dotj` — reconstructed, **accepted** by the fresh
  reference, asserted as an acceptance control.

The fixture's own provenance comments already say the historical inferred
rejection for the two controls was wrong, that the `duration('P1D')` variant is
composed from supplied parts, and that the inherited 64 H2 false accepts are not
covered. Those statements are preserved unchanged.

Symbol check at `18b36a85` (read-only `rg`, no compile): `forEachParser`
(`quantified_group_predicates_test.go:211`), `newTestMemoryEngine`
(`test_helpers_test.go:14`), `nornicerrors.Neo4jStatus`
(`pkg/errors/status.go:129`), `staticTypeNeo4jVerdicts`
(`static_types_neo4j_verdicts_test.go:14`, `query`/`rejection` fields),
`staticConstructorTypesOracle` / `staticConstructorTypesPopulation` /
`staticConstructorTypesParams` (`static_constructor_types_test.go:23,39,27`), and
the embedded oracle's keys `population` (12960), `populationSha256`,
`neo4jRejected` (10531) and `inherited` (258) in
`pkg/cypher/testdata/static_constructor_types_oracle.json`.

## Reproduction (recorded, parent run)

Source `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5`; relevant static validation
production source unchanged from v1.5.1 `cb0a574f`. Command and result as
recorded on the ticket:

```
env GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test -tags=noui,nolocalllm -p=1 \
  -count=1 -timeout=180s -run '^TestNor79To82' -v ./pkg/cypher
```

macOS: package `github.com/orneryd/nornicdb/pkg/cypher` 1.938 s, FAIL;
`TestNor79To82NegativeControlsReuseCanonicalVerdicts` PASS. The matrix totals are
60 failing and 84 matching cells out of 144 (12 probes × 2 parsers × auto/explicit
× RUN/EXPLAIN/PROFILE).

NOR-82 alone, per probe (12 cells each: two parsers × auto/explicit × three
statement kinds):

| Probe | Verdict | Cells | Meaning |
|---|---|---|---|
| `toUpper_head_dotk_plus_date` | accepted here, reference rejects | 12 FAIL | missing rejection |
| `toUpper_head_dotk_plus_duration` | accepted here, reference rejects | 12 FAIL | missing rejection |
| `toUpper_head_dotk_plus_empty_list` | accepted here, reference accepts | 12 PASS | acceptance control |
| `toUpper_head_dotk_plus_dotj` | accepted here, reference accepts | 12 PASS | acceptance control |

The failing cells are identical in both parsers and both transaction modes, and
in RUN, EXPLAIN and PROFILE. Recorded cell text (parser `nornic`, `RUN/auto`):

```
RUN: accepted
RUN: UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + date())) AS a: rejected by Neo4j (Type mismatch), accepted here
```

```
RUN: accepted
RUN: UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + duration('P1D'))) AS a: rejected by Neo4j (Type mismatch), accepted here
```

Both parsers and all three statement kinds produce the same shape, e.g.
`antlr/EXPLAIN/tx/toUpper_head_dotk_plus_date` logs `EXPLAIN: accepted` with the
same detail line. That is evidence that the compile-time admission path is shared
by the two parsers and by all three statement kinds for this probe.

## Sibling failures (separate, not part of this card)

Same matrix, opposite direction (accepted by the reference, rejected here), 36
failing and 60 matching cells:

| Issue | Probe | Recorded NornicDB error |
|---|---|---|
| NOR-79 | `parameter_list_comprehension_subscript` | `Type mismatch for parameter 'f': expected List<T> but was Float` (reference accepts) |
| NOR-80 | `delete_case_count_subquery` | `DELETE expression "CASE WHEN COUNT { MATCH (m) } > 0 THEN n END" refers to an undefined variable` (reference accepts) |
| NOR-81 | `any_over_timestamp` | `Type mismatch: expected List<T> but was Integer` (reference accepts) |

The NOR-79 probe that matches the reference (`parameter_arithmetic_into_toUpper`,
rejected as `expected String but was Float, Integer or Duration`) and the two
NOR-81 negative controls pass in all 12 cells each. No NOR-82 statement shares a
root cause with those three; no such claim is made here.

## Reference (recorded, not re-recorded)

Neo4j 5.26.30 community, cached image
`sha256:174ca8edaafb189d2676027a738815b04afd56d7dc720bd08bbb37afb469483e`,
isolated ephemeral store, 2 CPU / 4 GiB, no published ports. All 36 commands
(12 probes × RUN/EXPLAIN/PROFILE) completed before shutdown; normal `docker stop`
exceeded its default timeout and exited 137 with `OOMKilled=false`, which is not
a clean-exit claim and not a failed query. Relevant transcript (from
`/tmp/nor79-82-reference-results.log`):

| Probe / statement | Reference verdict |
|---|---|
| `82-date() RUN/EXPLAIN/PROFILE` | exit 1, `Type mismatch: expected String but was Any or Date` |
| `82-duration("P1D") RUN/EXPLAIN/PROFILE` | exit 1, `Type mismatch: expected String but was Any or Duration` |
| `82-[] RUN` | exit 0 (no rows printed) |
| `82-[] EXPLAIN/PROFILE` | exit 0, `Plan: "EXPLAIN"`/`"PROFILE"`, `Rows: 0` |
| `82-n.j RUN/EXPLAIN/PROFILE` | exit 0 |

Two properties of the recorded reference matter for any correction and are not
decided by this card: the mismatch is reported at `toUpper` (the reference's
`head(n.k + date())` type is the union `Any or Date`), and the reference attaches
no list alternative to the temporal/duration addition, unlike the two acceptance
controls.

## Source-level trace (static reading, not executed)

Read at `18b36a85`; this is a reading of the existing owners, not a verified
single-helper causal claim, and no test was run to confirm it.

1. `n.k` — `checkPostfix` (`static_operator_types.go:819`) reads a `Node`
   receiver and returns `typedOperand(staticPropertyValueType).proved()`: the
   property value type, whose operator category is empty, so `known()` is false
   (`static_operand.go:308`).
2. `n.k + date()` — `checkOperator` (`134`) → `checkOperatorAlternatives` (`180`)
   → `checkSingleOperator` (`224`) with a non-union left. The `left.temporal()`
   branch (`250`), which does restrict the other operand, only fires when the
   temporal operand is on the left. With an unknown left, the `+` rules fall
   through to `signatureOperand(additionResultTypes(left, right))`
   (`312`), whose temporal branch (`328`) yields `{Date, List<T>}` (duration,
   `326`: `{Duration, Date, Time, LocalTime, LocalDateTime, DateTime, List<T>}`),
   i.e. the union `Date or List<T>`.
3. `head(...)` — `checkAtom` (`1211`) types the call through
   `derivedFunctionOperand` (`static_operand.go:859`) with
   `derivationElement` → `elementOperand` (`360`) → `listPart` (`376`). On the
   union `Date or List<T>`, `listPart` keeps only the list alternative, and
   `elementOfType("List<T>")` returns `""` (`562`), so the element type is
   unknown.
4. `toUpper(...)` — `validateStaticFunctionVariablesTyped`
   (`static_function_argument_types.go:817`) types the argument and
   `staticArgumentType.accepts` (`995`) accepts an empty type name, so no
   rejection is produced anywhere in the chain.

Acceptance controls under the same reading: `n.k + []` reaches
`concatenatedOperand` (right list, unknown element type) and `n.k + n.j` reaches
the both-unknown branch of `additionResultTypes` (`321`); both end in a list or
unknown element type, so they are accepted today and must stay accepted.

Consequence for reuse: the divergence is inside the existing owners
(`static_operator_types.go`, `static_operand.go`,
`static_function_argument_types.go`) that `TestStaticOperandCategoryOwner` and
`TestStaticTypeNameDecisionOwner` already guard. No new type checker, no second
temporal/arithmetic table and no new consumer are justified by this evidence.

## Limits

- No build, `go test`, benchmark, mesh or reference run was performed by this
  card; the only execution was read-only `rg`/`python3` extraction. The recorded
  macOS failure and the recorded Neo4j transcript are the evidence, and their
  continuing applicability to a corrected head must be established by the parent.
- The configured code graph was unavailable: `integration_tools` discovery
  returned zero tools with `MCP startup failed: timed out handshaking with MCP
  server after 30s` for `nornicdb` and `nornicdb_docs`, plus a closed handshake
  for `plane`. The supplied retrieval also recorded `nornicdb/search` timeouts
  and no code baseline. Duplicate/reuse analysis is therefore source-level only
  and incomplete, not a clean result. It was not retried, because the failure is
  a handshake failure rather than a partial page.
- The staged duplicate checker was not run (it needs staged edits; this card
  leaves the worktree uncommitted by instruction).
- The original 64 inherited H2 false accepts are not recovered, so nothing here
  says how other uncertain-string consumers behave; the whole-issue acceptance
  criterion that depends on that population remains open.

Declarations: none of the fixture's identifiers (`norStaticClaim`, `nor79StaticClaims` through `nor82StaticClaims`, `TestNor79To82StaticClaims`, `TestNor79ParameterAwareConstructorClaims`, `TestNor79To82NegativeControlsReuseCanonicalVerdicts`, `norClassifyVerdict`, `norStatic*`) exists in `pkg/cypher` at `18b36a85` (read-only `rg`), so the file can be installed as supplied without renaming or merging declarations.


## Follow-up source review (tests added, not run)

The existing `elementOperand` → `listPart` → `elementOfType` composition already
preserves typed list alternatives. The producer currently supplies `List<T>` for
unknown plus Date/Duration, which erases the element type. A candidate correction
belongs in `additionResultTypes`/`staticAdditionWithUnknown`, not a new consumer
checker. A typed `List<Any>` plus `List<Date>` alternative would naturally derive
`Any or Date`; the Duration analogue similarly derives `Any or Duration`.
These are proposed producer alternatives, not independently measured full result
unions. Keep the scalar alternatives pending reference verification.

The shared table also affects Duration on the left; the existing branch fixture
expects an untyped list there. Date on the left returns earlier in
`checkSingleOperator`. Neither the recorded four forward probes nor the canonical
constructor population proves the reverse result. Do not silently rewrite that
expectation. Local `docker` and `cypher-shell` executables exist; no daemon,
container, mesh or network action was taken in this follow-up. The recorded
reference inspect names `/plane-static-reference` with exited status; this is
historical availability evidence only.

Four probes now live in `staticTypeNeo4jVerdicts`; the focused public test selects
those verdicts for 48 parser/transaction/statement cells and reuses `requireStatus`
for exact errors. Tests and builds remain unexecuted under the parent's CPU
reservation. The original 391-line `.go.txt` fixture was not edited.


## Fresh reference: Date and Duration boundary

Executed all 66 cells on Radeon through its named SSH host and account validation
slot. `dbms.components()` returned Neo4j 5.26.30 community; pulled tag
`neo4j:5.26.30`, image ID
`sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c`.
Container capped at 2 CPU / 1536 MiB, heap 256–512 MiB, page cache 256 MiB, no
published ports. Exact queries and results are in
`fixtures/reference-boundary-probes.json`; full commands/log are preserved in
`/tmp/n152-149-reference-run.py` and `/tmp/n152-149-reference-results.log`.
All 22 probes agree across RUN/EXPLAIN/PROFILE: 36 rejected cells, 30 accepted.
Both operand orientations agree for Date/Duration. Direct `toUpper` reports
`Date, List<Any> or List<Date>` or
`Duration, Date, Time, LocalTime, LocalDateTime, DateTime, List<Any> or List<Duration>`.
`toUpper(head(...))` reports `Any or Date` / `Any or Duration`; both `size(...)`
and `head(...)` accept. Both historical acceptance controls still accept.
`labels(...)` rejects with a broad contextual union in both orientations; this
is not evidence to rewrite the contextual diagnostic owner in this bounded fix.

The task container and volume were removed; the task-pulled image tag/ID was
removed only after confirming no remaining container consumer. All cleanup
commands succeeded before slot release. Existing workloads were untouched.
A second temporal boundary lifecycle is pending. Its first generated runner
failed with NameError after read-only inventory, before creating/pulling any
resource; `/tmp/n152-149-temporal-reference-results.log` preserves that failure.


## Fresh reference: shared temporal producer boundary

The corrected second lifecycle completed 24 RUN-only cells (16 rejected, 8
accepted) on the same verified Neo4j version. Both orientations for Time,
LocalTime, LocalDateTime and DateTime report `T, List<Any> or List<T>` under
`toUpper`, `Any or T` under `toUpper(head(...))`, and accept `size(...)`.
`fixtures/reference-temporal-boundary-probes.json` preserves exact queries and
verdicts. This is 24 RUN cells, not EXPLAIN/PROFILE coverage for those types.
Together the two successful runs establish 90 reference cells. Container,
volume and task-pulled image cleanup all succeeded again, before releasing the
slot. Full second runner/log:
`/tmp/n152-149-temporal-reference-run.py`,
`/tmp/n152-149-temporal-reference-results-retry.log`.

Parent accepted the generic producer correction: preserve typed list alternatives
in `additionResultTypes`; allow temporal-left unknown-right operands through that
owner, retaining known temporal arithmetic. No spelling/consumer conditional or
new checker. Existing `elementOperand` composition is reused unchanged.
The static typing proof, constructor and property oracle populations are generated
independently of `staticTypeNeo4jVerdicts`; their population/hash expectations
remain untouched and must pass their existing tests. Original 64 H2 cases remain
unrecovered; the expanded reference is not a substitute for them.


## Fail-first and implementation state

Before production edits, bounded local command
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestStaticUncertainAdditionAdmission$' -count=1`
failed: all 24 Date/Duration rejection cells returned no error; the 24 acceptance
controls passed. Full output: `/tmp/n152-149-fail-first.log`; test runtime 1.191s
(excludes compilation). Production then changed only `static_operator_types.go`:
unknown right operand no longer prematurely collapses a temporal addition to the
scalar left type; the canonical result producer retains typed list alternatives
for temporal/duration additions. The original scalar alternatives remain.

The 42 unique queries measured through direct/string/list consumers and the two
acceptance controls now share `staticUncertainAdditionVerdicts`, appended into the
existing canonical verdict matrix. The focused public matrix reuses those
verdicts and `requireStatus`, with 504 cells. This expands local mode checks beyond
the 24 RUN-only extra-temporal reference cells; it does not claim those reference
cells were measured in all modes. Green verification remains pending the parent's
CPU window. Branch tests preserve known temporal arithmetic and exact new Date
union alternatives in both orientations. No consumer checker was added.


## First integrated run and canonical ordering correction

The first integrated command
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^Test(Static|CheckOperator|CompileTimeTypeErrors)' -count=1`
failed in 34.280s. Only `TestCheckOperatorBranches` and the focused admission
matrix failed: expected rejection behavior was restored, but the existing
`canonicalTypeOrder` emitted `List<Date> or List<Any>`, which propagated to
`Date or Any` through `head`. Exact messages were not weakened. Full log:
`/tmp/n152-149-focused-green.log`. The selection covered 33 top-level test
functions before the new ordering test; unchanged proof populations did not fail.

Parent approved correcting the existing canonical order owner: emit `List<Any>`
after scalars and before property list types, analogous to its existing scalar
Any-first rule. This order is supported by fresh temporal reference and existing
Float/String/Boolean/list addition verdicts already recorded in the canonical
matrix. No consumer rewrite. A focused test in `static_type_choices_test.go`
checks mixed scalar/list Any order, producer-input immutability, element
composition and retained order of otherwise unknown types. The additional
correction is staged but not yet tested; CPU is reserved for parent benchmarks.


## Acceptance and cleanup inventory (pending final green verification)

- Product contract: uncertain temporal/duration addition preserves scalar and
  typed-list alternatives in both orientations; downstream string consumers
  reject before execution; list consumers and the two recovered controls accept.
- Reuse: existing addition-result producer, canonical union ordering and element
  reader are retained. The public matrix reuses the existing verdict and status
  owners. No new checker, consumer-specific classification or temporal runtime
  implementation was added.
- Required proof outstanding: final green static suite after ordering correction,
  race coverage and parent-controlled canonical verification. Initial failure
  logs remain preserved; neither failed run is represented as passing evidence.
- Duplicate analysis: parent executed the staged checker; it reported
  `NornicDB duplicate analysis incomplete (advisory): /db/nornicdbcode/tx/commit:
  <urlopen error timed out>`. Log `/tmp/n152-149-duplicate.log`. Manual source
  review does not replace this unresolved indexed result.
- Compatibility limit: original 64 inherited H2 cases remain unrecovered. Fresh
  90 reference cells and 504 local public cells are bounded populations and do
  not establish whole-ticket closure or release qualification.
- Owned remote resources: both `n152-149-reference` and
  `n152-149-temporal-reference` containers and corresponding `-data` volumes were
  removed. Each lifecycle removed its own pulled `neo4j:5.26.30` tag/image after
  checking remaining container consumers. Successful commands and final
  `CLEANUP_COMPLETE` are in both logs. No existing container was reused.
- Retained resources: `/tmp/n152-149-addition` and its fix branch contain active
  staged work; keep them for parent integration. Preserve the reference runner,
  raw logs, verdict summary and failed local test logs as review evidence. No
  task-owned Go cache was created; shared caches and other agents' work remain
  untouched while concurrent parent verification is active.


## Final focused normal run

After the canonical ordering correction, the same bounded static/operator/
compile-time selection passed in 37.238s (34 selected top-level tests, including
504 focused public admission cells and the unchanged proof/constructor/property
oracle populations). `/tmp/n152-149-focused-final.log` preserves output. This is
focused package verification, not all-package/release qualification. Race and
vet are separate pending checks. Original fixture provenance was independently
rechecked against the ticket's HTML-unescaped `<pre>` block: one exact match;
its bytes remain unchanged. The earlier documented SHA omitted an `e`; evidence
now records the measured full SHA correctly.


## Final bounded verification and handoff

All commands ran in `/tmp/n152-149-addition` on base
`18b36a85d10e775b9d5d3500ecf3482682754a8d` plus the staged patch, with
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB`:

- Normal: `go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^Test(Static|CheckOperator|CompileTimeTypeErrors)' -count=1`
  passed, 37.238s; `/tmp/n152-149-focused-final.log`.
- Race: `go test -p=1 -race -tags=noui,nolocalllm ./pkg/cypher -run '^Test(StaticUncertainAdditionAdmission|StaticCanonicalTypeOrderAnyAlternatives|CheckOperatorBranches|StaticTypesMatchNeo4jVerdicts)$' -count=1`
  passed, 9.036s; `/tmp/n152-149-focused-race.log`.
- Vet: `go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher` passed with empty output;
  `/tmp/n152-149-vet.log`.

The original sibling fixture verdict-delta rerun was not performed; existing
static oracle success is not the same population. Full verification, newer
release-head integration, original H2 recovery and unresolved indexed duplicate
analysis remain parent-owned limits. No performance benchmark claim is made.
No commit or push was performed. Branch and staged work remain for integration;
no N149 build/test process remains running after vet.
