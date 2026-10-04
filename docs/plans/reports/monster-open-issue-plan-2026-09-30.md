# Monster Open Issue Plan: 2026-09-30

Baseline: `origin/main` at `1125684d`. Scope: all 22 open issues authored by
`mm0nst3r`, excluding product feature requests. There are no explicitly labeled
feature requests in this inventory. Test infrastructure and technical-debt
reports remain in scope, but are not treated as new product features.

Issue bodies and each latest comment were read with `gh issue view` on
2026-09-30. Latest comments supersede previous DONE entries, closures, and older
reproduction matrices. A passing original reproducer does not close a family
whose latest comment still identifies an unresolved case or convergence item.

## Rules

- Reproduce failures before changing production behavior; retain regression tests.
- Analyze and implement in dedicated worktrees rooted at main, not the user's checkout.
- Anything overlapping #770/#772 work belongs in existing PR #771. Do not open
  a second PR for its shared streaming, projection, CALL, MERGE, or lifecycle work.
- Independent clusters may publish their own PR against main. If their minimal
  fix touches PR #771's changed ownership surface, return commits for integration
  there instead of publishing a competing PR.
- Reuse the shared parser, evaluator, row source, aggregate collector, and typed
  storage contracts. Remove divergent implementations rather than adding another.
- Run correctness checks only: no benchmarks, timing comparisons, or performance runs.
- Compare observable semantics with pinned Neo4j 5.26.30 on Bolt autocommit,
  Bolt explicit transactions, and HTTP transactions. Check errors and graph effects.
- Close only fully resolved issues with evidence. Keep family, architecture,
  environment-dependent, and partially fixed issues open with precise status.

## Per-Issue Plan

| Issue | Latest Scope and Local Hypothesis | First Discriminating Check / Repair | Owner / Publication |
| --- | --- | --- | --- |
| #770 | No comments; server races already fixed in PR #771, not merged into main. | Preserve existing fail-before race evidence; verify no new lifecycle or logger race regressions on integrated head. | Existing PR #771; no duplicate PR. |
| #772 | No issue comments; external PR verification found unfiltered aggregate OOM, now addressed in `ca2da7a9`. | Preserve exact 30M count/sum/projected-count AC/TX correctness tests and allocation gates; do not recreate a private range executor. | Existing PR #771. |
| #728 | Latest 2026-09-30 comment: unfiltered comma-product count retains all rows; filtered joins now match. MATCH producer needs the same incremental consumer as UNWIND. | Fail-before bounded-allocation product count plus grouped/non-count aggregate checks; feed generated bindings to shared collector, not a count-only shortcut. | Stream cluster; integrate PR #771. |
| #713 | Latest 2026-09-28 comment: 32/32 behavior cases match, but 37 projection implementations remain. | Inventory named full implementations against main and PR #771, collapse adjacent duplicates into shared projection/order/window contracts; track each remaining function. | Stream cluster; PR #771 for overlap; keep family open until convergence is complete. |
| #648 | Latest 2026-09-30 comment: transactional CALL loses outer rows and rejects explicit transactions even with zero inputs. Admission likely occurs before input evaluation. | Reproduce seeded outer count=1 and empty-input explicit no-error; preserve outer bindings and defer transaction rejection until a batch actually executes. | Stream cluster; integrate PR #771. |
| #640 | Latest 2026-09-30 comment: relationship MERGE rejects unbound bare endpoints. Pattern admission appears narrower than shared MERGE semantics. | Directed, undirected, and one-labeled-endpoint cases with graph effects; admit full pattern to canonical MERGE, no new private implementation. | Stream cluster; integrate PR #771. |
| #744 | Latest 2026-09-30 comment: semicolon statement chaining executes, conflicting EXPLAIN/PROFILE has wrong error, PROFILE publishes both metadata keys. | Three-route exact statements plus Bolt summary assertions; reject multi-statement query and conflicting mode in preparation, publish one execution-mode metadata key. | Statement-boundary cluster; independent PR unless #771 overlap is required. |
| #743 | Latest 2026-09-30 comment: `RETURN 1 AS x UNION FINISH` accepted. UNION branch output contracts lack validation. | Fail-before mixed RETURN/FINISH query including write-effect checks; enforce compatible UNION output contracts during shared preparation. | Statement-boundary cluster; shared PR with #744 if same preparation root. |
| #739 | Latest 2026-09-28 comment: ANTLR still rejects scoped `CALL (x)` after UNWIND. | Force ANTLR for exact scoped-CALL case, compare default parser; fix grammar/adapter or admission, regenerate using repository tooling, no silent fallback claim. | Statement-boundary cluster; independent issue PR if separable. |
| #745 | Latest 2026-09-30 comment: composite subquery node is stamped with composite rather than constituent element ID. | Direct USE versus CALL USE identity checks, including Bolt entity ID and HTTP metadata; preserve origin database through typed entity transport. | Fabric cluster; independent PR unless existing #771-owned code is needed. |
| #683 | Latest 2026-09-30 comment: explicit Bolt composite writes see own writes but COMMIT uses canceled context. | One/two-write and read-own-write composite transactions; trace transaction context lifetime through commit/compensation, preserve live transaction-owned context. | Fabric cluster; independent PR; any #770 lifecycle overlap must integrate PR #771. |
| #738 | Latest 2026-09-30 comment: USE system write has wrong behavior/error, unknown USE target over HTTP gives 404 instead of 200/errors. | Three-route system-write and missing-target cases; share system semantic admission and distinguish statement target errors from invalid endpoint database. | Fabric cluster; same routing PR only where root is shared. |
| #668 | Latest 2026-09-26 comment: HTTP temporal values expose Go struct fields rather than ISO text; original body also covers float/error result framing. | Replay date, duration, time, localtime, datetime, and localdatetime including nested values; use typed temporal serialization, retain FLOAT token types and transaction error framing. | Fabric/transport cluster; independent serialization PR if root differs from routing. |
| #514 | Latest 2026-09-30 comment: malformed reduce/comprehension property values stored as text/null; literal OPTIONAL<tab>MATCH garbage accepted. | Fail-before exact malformed queries on empty/populated stores; enforce shared lexical/expression validation before property writes; assert no partial data. | Expression cluster; return overlapping CREATE/MERGE/pipeline changes to PR #771. |
| #657 | Latest 2026-09-30 comment: undefined `r` inside `startNode(r).id` silently returns null. | startNode/endNode member access in RETURN/SET with missing variables; shared scope validation must walk function-result property access. Replay remaining issue-body error families too. | Expression cluster; independent shared-expression PR if no #771 overlap. |
| #698 | Latest 2026-09-28 comment: 65/98 match; float string formatting, Unicode case expansion, null and boolean conversion remain. | Exact typed FLOAT string cases, sharp-s uppercase, null functions and boolean conversion across routes; one shared function contract, no row-only special cases. | Expression cluster; independent PR for isolated evaluator ownership; keep unresolved family items explicit. |
| #531 | Latest 2026-09-28 comment: composite uniqueness/TEXT/POINT admission, old constraint syntax, community NODE KEY and duplicate errors differ. | Reset user schema before each exact DDL case; implement atomic typed schema admission and reference-compatible error classes; validate native knowledge-profile commands separately. | Schema/SHOW cluster; independent PR. |
| #530 | Latest 2026-09-28 comment: SHOW columns match but values/procedure inventory differ; YIELD WHERE error phase remains. | Reset schema, replay exact SHOW/YIELD cases; distinguish environment-specific values from incorrect semantics; validate static YIELD types before runtime. | Schema/SHOW cluster; coordinate shared semantic validation with expression cluster. |
| #715 | Latest comment suggests existing FlushWAL and immediate Mark/sweep synchronization. | Replace fixed sleeps with existing completion primitive; deterministic recent-peer test plus existing concurrent test; repeat correctness/race tests without latency thresholds. Inspect original storage async flakes too. | Test cluster; independent test-only PR. |
| #754 | No comments; body requests a ratcheted multi-route differential CI gate, allocation gate, TestKit. | Audit workflow against issue-body checklist; wire pinned deterministic correctness corpus and allocation checks, then establish real TestKit execution or document concrete unavailable prerequisites. | Test cluster; separate CI PR from #715 if independent. |
| #547 | Latest comment lists structural debt carried over from closed #521: preparation, typed bindings/outcomes, scanner, storage contract/repair/iterators, ANTLR, typed evaluation, file limits, CI/reporting. | Map every numbered item to a local owning API and explicit removal/contract gate. Credit only measured completed migrations; each large architectural stage requires its own validation. | Cross-cluster ledger; #771 owns its overlapping convergence. Keep open until all items are proven complete. |
| #446 | Latest 2026-09-30 comment: implementation fixes are merged; reporter will supply real 609k-vector recall results on October 3/4. | Verify existing recall fixtures and prerequisites; do not claim real-workload recall without dataset or substitute synthetic measurements. | External-data blocker; keep open, no speculative PR or performance run. |

## Worktrees

All new worktrees are siblings of the repository. Existing worktrees are left intact.

| Cluster | Directory | Branch |
| --- | --- | --- |
| Plan | `NornicDB-monster-plan-0930` | `docs/monster-issue-plan-2026-09-30` |
| Streaming / CALL / MERGE / projection | `NornicDB-monster-stream-0930` | `fix/monster-stream-call-0930` |
| Statement boundaries / ANTLR | `NornicDB-monster-boundaries-0930` | `fix/monster-statement-boundaries-0930` |
| Fabric identity / transaction / routing | `NornicDB-monster-fabric-0930` | `fix/monster-fabric-routing-0930` |
| Expression / static scope | `NornicDB-monster-expressions-0930` | `fix/monster-expression-contracts-0930` |
| Schema / SHOW | `NornicDB-monster-schema-0930` | `fix/monster-schema-show-0930` |
| Deterministic tests / CI | `NornicDB-monster-tests-0930` | `fix/monster-test-determinism-0930` |

## Completion Gates

Each delegate reports observed fail-before/pass-after evidence, changed APIs,
commit IDs, tests actually run, issue-level residuals, and PR URLs or #771
integration requirements. No guessed success or premature issue closure.

The coordinator validates #771 integration, reviews independent PR ownership,
records final issue status here, and reports unavailable external evidence.

## Published Results

All changes overlapping #770/#772 remain in existing PR #771. Independent
worktrees started from main `1125684d`; the stream cluster fast-forwarded to
PR #771's shared-operator dependency before adding its changes. No performance
runs were performed.

| Cluster | Publication | Verified Scope / Residuals |
| --- | --- | --- |
| Streaming / CALL / MERGE | PR #771, integrated commit `e1acb2b2` | Latest #648 scoped transactional CALL outer rows and empty explicit inputs; #640 directed/undirected/labeled bare endpoints; #728 bounded typed node-product aggregation. Shared collector handles grouped/non-count aggregates. Other product paths and exhaustive #713/#547 convergence remain open. |
| Expressions / static scope | PR #771, integrated commit `e0615742` | Latest #514 malformed expressions/lexical admission, #657 missing function-endpoint variables, #698 shared scalar conversion/string contracts. Null propagation, Unicode expansion, FLOAT/temporal text, and nested property-function scope pass reference checks. Full external 98-case family replay and remaining evaluator migrations are not claimed. |
| Fabric / transport | PR #771, integrated commit `b56a84f0` plus follow-up below | #683 composite commit/rollback context lifetime, #738 system admission and HTTP missing-USE-target admission, #668 recursive temporal/entity/float/error framing. The exact #745 constituent-subquery reproducer subsequently failed and was repaired; the earlier direct-USE test did not cover it. Listed local composite identity, nested entity, HTTP metadata, rollback, and real RemoteEngine controls pass the final acceptance gates below. |
| Statement boundaries / ANTLR | [PR #774](https://github.com/orneryd/NornicDB/pull/774), `6ec20a92` | Latest #743 UNION/FINISH output contract, #744 statement chaining/mode metadata, #739 scoped CALL grammar. Full affected-package suites, native TCK, and 266 pinned Bolt/HTTP comparisons pass. Forced-ANTLR CALL retains two pre-existing error-detail mismatches per mode; broader query options remain unverified. |
| Schema / SHOW | [PR #775](https://github.com/orneryd/NornicDB/pull/775), `97163d8d` | #531 composite UNIQUE enforcement/malformed definitions and typed TEXT/POINT admission; #530 selected static YIELD checks. Delegate reports full repository correctness, official TCK, focused races, and vet passes. Coordinator fixed HTTP schema-reset isolation and constraint-creation error namespace; 302 pinned Bolt/HTTP comparisons pass. Community NODE KEY policy and broader SHOW values/inventory remain open. |
| Deterministic tests / CI audit | [PR #773](https://github.com/orneryd/NornicDB/pull/773), `5fcdbdb9` | #715 WAL completion, peer sweep, async flush configuration; focused correctness 20 repeats and race 10 repeats pass. Untouched Bolt throughput-floor test remains. #754 mismatch ratcheting, reset retries, complete error/effect checks, and TestKit remain incomplete; no duplicate CI workflow was added. |

## Integrated Verification

- Full Cypher packages, Bolt, Fabric, and server suites pass on the combined
  PR #771 tree. Server's old missing-USE-target 404 assertion was updated to
  HTTP 200 with DatabaseNotFound; missing endpoint database remains 404.
- Official openCypher TCK passes in both transaction modes (7,794 scenarios).
- Pinned Neo4j 5.26.30 corpus: 94 shared cases per Bolt mode and 99 per HTTP
  mode, 386 passing comparisons, plus real RemoteEngine interoperability.
- Selected expression, product, CALL, MERGE, transaction, identity, and temporal
  race regressions pass in Cypher, Bolt, and HTTP server. No full-repository
  race pass is claimed.
- Exact 30M count/sum/projected-count queries still pass in AC/TX using the
  embedded executor with `GOMEMLIMIT=256MiB` (soft limit, not a fresh 4 GiB server).
- Touched non-generated package vet and editor diagnostics pass. Direct vet of
  the unchanged generated ANTLR parser still reports pre-existing unreachable
  code; existing duplicate `-lobjc` linker warnings remain.
- Expression retry repaired substring diagnostics and time.Time text conversion
  caught by full tests. Pinned replay rejected a proposed mixed numeric equality
  change, so the reference contract was preserved. TCK treats numeric signed
  zeros equally while still distinguishing their string representations.

Final coordinator logs: `/tmp/nornicdb-771-monster-{packages,server-final,tck,
reference,race,large,vet}.log`. Interrupted and partial earlier logs are not
credited as complete gates. Cluster reports provide fail-before evidence and
owning API details:
[Fabric](monster-fabric-results-2026-09-30.md) and
[streaming](monster-stream-call-2026-09-30.md).

Independent reference gates also pass: PR #774 has 266 comparisons and PR #775
has 302. Logs: `/tmp/nornicdb-boundaries-final-reference.log` and
`/tmp/nornicdb-schema-final-reference2.log`. Schema's first HTTP replay exposed
fixture schema leakage; structured reset cleanup restores the two default
lookup indexes. The isolated replay then exposed a real error-namespace mismatch
for pre-existing duplicate tuples, corrected to DatabaseError rather than
ClientError. Neither failure was hidden by changing corpus expectations.

## Reverification Corrections

External verification of published head `685b146e` demonstrated six remaining
defects. Each was reproduced before its production repair; all corrections
remain in existing PR #771, using the shared execution and transport contracts.

- #728: compose the shared typed node source across successive MATCH and
  row-local WITH clauses, feeding the existing incremental aggregate collector.
  Six 256-node AC/TX boundary shapes failed the 8 MiB correctness ceiling before
  repair and pass afterward. General/grouped aggregates, projected WHERE,
  DISTINCT, ORDER BY/LIMIT/SKIP, empty products, cancellation, and source replay
  controls pass. This is not a count-only formula or a private evaluator.
- #514: computed property expressions use the canonical row evaluator, including
  typed parameter/context bindings. `{k: 1}.k` returns integer 1 rather than text.
  Shared pre-write admission rejects incomplete arithmetic in CREATE, a second
  CREATE, MERGE, and SET with SyntaxError and no extra committed nodes.
  The duplicate scalar-property expression parser was removed; the batch-CREATE
  helper for already-evaluated binary values remains shared.
- #698: both lowercase entry points use full Unicode casing, retaining U+0307
  for dotted-I. RETURN, WITH, UNWIND, CREATE properties, and SET routes pass.
- #668: HTTP node/relationship properties recurse through the existing temporal
  serializer in row and graph output. Exact date, datetime-in-entity-list, and
  date-list property cases pass in AC/TX without Go struct fields or extra entity
  metadata entries.
- #738: shared USE admission rejects system MATCH, OPTIONAL MATCH, and nested
  graph reads with SemanticError before cross-database transaction admission.
  Bolt and HTTP AC/TX pass; scalar/admin routing remains unchanged.
- #745/#648: explicit Fabric projection continuations execute through the main
  Cypher row pipeline, replacing rather than joining output columns. Typed
  record-only continuations do not reopen remote constituent storage. Recursive
  Bolt entity encoding resolves constituent node/edge origins, including lists
  and relationship endpoints. Exact composite CALL queries pass with only the
  requested columns and `4:pother:`/`5:pother:` IDs in Bolt AC/TX. The reporter's
  corrected distinction is retained: `elementId(n)` was already correct; node
  wire identity and leaked inner `n` columns were the demonstrated defects.

Follow-up gates pass: full repository correctness; official openCypher TCK with
7,794 supported scenarios in each transaction mode; focused Cypher/Bolt/server/
Fabric races; touched non-generated package vet; and 474 fresh pinned Neo4j
5.26.30 comparisons (116 shared cases per Bolt mode, 121 per HTTP mode), plus
real RemoteEngine interoperability. No benchmarks or performance runs were
performed. Logs: `/tmp/nornicdb-771-reverify-{repository,tck,race,vet,
reference-final}.log`.

## Final PR #771 Acceptance

The PR-owned follow-up is complete. The initial six reproductions were expanded
into explicit expression, CALL, projection, transport, ordering, rollback, and
identity controls rather than leaving an undefined family-replay task.

- Mutation RETURN/WITH helpers delegate to the main pipeline; private projection
  and aggregation implementations on those paths were removed. The row evaluator
  retains explicit value, unsupported, and error outcomes.
- CALL batching uses typed rows and the shared transaction callback, not bound
  values rewritten into transaction script text. Empty inputs, unit/returning
  subqueries, counters, terminal CALL, chained clauses, UNION exports, explicit
  transaction rejection, and rollback controls pass.
- Explicit Fabric projections cannot use either private in-memory shortcut.
  Quoted incoming names, property-based ORDER BY, outer-column replacement,
  constituent identity, correlated continuations, and remote targets pass.
- HTTP entity/list/map/path temporal and nonfinite values, FLOAT representation,
  metadata, failed-statement framing, partial runtime-error rows, skipped later
  statements, and whole-request rollback pass.
- Shared math/parameter contracts retain Unicode expansion, null/zero diagnostics,
  reference-compatible rounding, typed nonfinite writes, and pre-write admission.
  MERGE retry metadata and indexed large-integer ordering controls also pass.
- Full repository correctness passes on the final tree. Fresh pinned Neo4j
  5.26.30 replay passes 814 comparisons: 201 cases per Bolt mode and 206 per HTTP
  mode, checking values, errors, and effects, plus real RemoteEngine controls.
- Official openCypher TCK passes 7,794 supported scenarios in each transaction
  mode. Focused races pass in Cypher, Bolt, server, Fabric, storage, and txsession;
  touched non-generated package vet passes.
- The rebuilt Linux server passes all five 5,000-node product shapes through
  Bolt AC/TX and HTTP commit: every result is exactly 25,000,000. Docker verifies
  a hard 4 GiB cap with swap disabled, server survival, and `OOMKilled=false`.
  The isolated container was removed. No benchmarks or performance runs were
  launched.

Final logs: `/tmp/nornicdb-pr771-publish-{repository4,reference4,tck,race,vet,
hard4g,hard4g-http}.log`. Earlier failing or incomplete logs are not credited as
passing publication gates.

## Ownership Boundaries

PR #771 contains the completed execution/projection/transport work above.
Independent statement-boundary and schema work remains in PRs #774 and #775,
and deterministic-test work in #773; their changes are not silently folded into
this PR. Separate TestKit infrastructure and the reporter's external recall
dataset are not PR #771 execution acceptance tasks. This report does not imply
those independent issues were closed or their evidence fabricated.

## Residual Close-Out: 2026-10-02

Current main baseline: `1da71c87`, after #773 and #774 merged. #775 is rebased
and pushed separately at `c596209e`; its fixes are not credited to main until
merged. The current instruction is to fix residuals on main, then commit, push,
and close only fully resolved issues with observed Neo4j comparison matrices.

Latest issue bodies/comments and the open issue list were reviewed. This table
separates behavioral residuals from larger acceptance criteria; a passing narrow
reproducer is not a claim that an architecture family is complete.

| Issue | Remaining Scope | Current Status |
| --- | --- | --- |
| #698 | Five latest expression diagnostics and parameter/static/runtime subscript distinctions. | Reproduced and repaired; eight-path raw-code/phase/effect comparisons pass. FLOAT parameter indexes retain runtime errors. CASE label-predicate variable references also repaired. |
| #715 | Timing-dependent correctness tests under host load/race instrumentation. | Bolt throughput, Cypher fast-path/profile workloads and COUNT timing now require explicit non-race/non-short opt-in. Deterministic gate controls pass; COUNT correctness stays active. No performance claim. |
| #739 | Forced-ANTLR valid Neo4j syntax and CALL diagnostic details. | Reported cases and newly exposed standard grammar gaps repaired; both parser ratchets pass 7,794/7,794 and full eight-path reference matrix passes. Native-only DSL/legacy permissive forms are not represented as Neo4j parity. |
| #743, #781 | FINISH-only UNION, pre-write column mismatch rejection and phantom/unlabeled node prevention. | Reproduced and repaired; graph-effect and raw-code comparisons pass across all eight paths. |
| #782 | Leading unit CALL CREATE FINISH and scoped SET FINISH. | Reproduced and repaired in both parsers; rows and exact graph effects match the reference. |
| #783 | Explicit/empty CALL imports and variable-only import validation. | Shared scope validation repaired; valid imports and rejected outer/property reads match the reference. |
| #744, #784 | Query options/versions, statement termination, repeated modes and HTTP PROFILE shape. | Preamble diagnostics and HTTP envelopes match live reference. Composite EXPLAIN/PROFILE/CYPHER dispatch passes explicit-transaction native controls in both parsers; no live Enterprise comparison is claimed. |
| #785 | Repeated composite UNIQUE property diagnostic. | Repaired to RepeatedPropertyInCompositeSchema; raw status, compile phase and no-effects controls pass. This does not credit unmerged #775 composite uniqueness. |
| #786 | Empty/populated SHOW non-boolean predicates. | Shared compile-time boolean validation repaired; invalid/valid alias-predicate controls pass in both parsers and both wire protocols. |
| #530 | Procedure/function inventory and descriptions, database/user/transaction values, wider SHOW value parity. | Broad family remains open; #775 covers only selected schema/YIELD cases. |
| #531 | Community NODE KEY compatibility policy and native knowledge-profile option/admission residuals, broader schema values. | Broad family remains open; typed TEXT/POINT/composite uniqueness work stays in #775. Native features must not be silently removed to imitate Community licensing. |
| #812 | Invalid execute body abort, empty bodies/objects, trailing document framing. | Repaired lifecycle/status/raw-code/graph effects; first document executes and suffixes are ignored without executing a second document. Size-limit rejection remains before writes. Both backends and live reference controls pass. |
| #814 | Complete label-less indexed property MATCH. | Repaired with complete fallback unless indexed labels cover the namespace; other-label/unlabeled and transaction read controls pass. |
| #815 | Parameter named as before AS alias. | Shared lexical/UNWIND split and ANTLR parameter admission repaired; eight-path parameter regressions pass. |
| #816 | Comma-MATCH bound paths and shortestPath. | Variable recognition/binding repaired; rows and mutations match reference across all eight paths. |
| #713 | 32 reported statements already match; one projection/column-naming/planning implementation remains an acceptance criterion. | Structural inventory/convergence not complete. |
| #728 | 38 statements and bounded products already match; WHERE placement/evaluator convergence and compiled-WHERE cost evidence remain. | Structural inventory/convergence not complete; no benchmark claim. |
| #547 | Preparation, typed bindings/outcomes, scanner, storage contract/repair/iterators, typed evaluation, file limits and CI reporting. | Multi-module architecture checklist remains open. |
| #754 | Issue-linked mismatch ratchet, reset retries, complete error/effect checks, real TestKit orchestration/baseline. | Infrastructure acceptance remains open. |
| #657 | Already closed; latest 13-diagnostic/four-route matrix is published. | Do not reopen or claim original 71-case replay/message-position coverage from that finite matrix. |

Unrelated GraphQL, UI, ORM and i18n product requests are not closed by Cypher
residual work. Interrupted checks are not counted as passing evidence.

### Static Subscript Evidence

The three #698 subscript queries failed before repair with TypeError rather
than the reference SyntaxError. Permanent ordinary tests cover all five latest
cases. Pinned Neo4j 5.26.30 was queried for the affected older boolean/numeric/
Unicode literal tests before correcting their expectations. FLOAT parameter
indexes remain runtime TypeError; parameter MAP/STRING cases are compile errors.

The unmodified official openCypher List1 scenario [6] expects TypeError for
four statically known scalar receivers, and Map2 scenario [6] expects a runtime
integer-map-key error. Pinned Neo4j 5.26.30 returns compile-time SyntaxError
for these exact queries. Explicit scenario-scoped harness profiles accept only
these five diagnostics, retaining raw Bolt codes, compile phase and graph-effect
assertions. The corpus, ratchet baseline and differential wire-code checks are
unchanged. Negative controls reject other scenarios, unrelated errors, runtime
failures and graph effects.

### Final Reference Matrix

Reference: `neo4j:5.26.30-community`, digest
`sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c`.
Each fixed case runs in auto-commit and explicit transactions. Both parser
settings run independently; HTTP and Bolt reset the shared reference sequentially.
The original 116-case prefix remains intact.

| Gate | Native Parser | ANTLR Parser |
| --- | --- | --- |
| Official openCypher, two transaction modes | 7,794/7,794 | 7,794/7,794 |
| Bolt differential, 320 cases x two modes | 640/640 | 640/640 |
| HTTP differential, 325 cases x two modes | 650/650 | 650/650 |

Total: 2,580 live comparisons, checking columns/rows, raw errors, phase and graph
effects. HTTP-specific plan-envelope cases account for the five additional cases.
No unexpected error-presence differences occurred. New grammar controls cover
both quote styles with literal newlines, post-index parameter/member access
(also in WHERE), standalone CALL pagination and scoped CALL UNION.

### Verification Limits

- The broad Cypher race suite passes after correcting the now-valid CALL/UNION
  redaction fixture and separating COUNT timing from unconditional correctness.
  Broad ANTLR/Bolt/TCK race checks and the focused HTTP lifecycle/plan slice
  (with live reference) pass. The broad server race suite exceeded its cumulative
  ten-minute budget while starting TestRetentionPolicies_POST_AddPolicy; it is
  not counted as passing and emitted no race report/assertion failure.
- The complete repository correctness rerun passes with performance/throughput/
  benchmark-named tests excluded and profiling workloads disabled by default.
  Scoped vet and the tagged server build pass; editor checks report no errors
  in the touched owner modules.
- No throughput, latency benchmark or allocation claim is made. Timing/profile
  workloads require `NORNICDB_RUN_PERFORMANCE_TESTS=1` and are disabled under
  race/short runs. Larger architecture/allocation gates remain open.
- Full vet reports existing generated-ANTLR unreachable code, Vulkan/oslocale
  unsafe-pointer and pool method-signature categories. Scoped vet excludes only
  those categories; golangci-lint is unavailable.
- The ordinary forced-parser suite also contains native policy/cardinality/
  contract/knowledge-profile DSL, single-quoted map keys, legacy bare index ON
  forms, invalid SET-to-UNWIND ordering and old malformed-input message checks.
  These are not claimed as an all-green Neo4j-standard suite, and no grammar
  bypass was added. Native correctness is validated separately.
- #530, #531, #713, #728, #547 and #754 retain the acceptance criteria listed
  above. #775 remains separate/unmerged. Community licensing and Enterprise
  composites are not silently treated as reference-tested native behavior.

## Schema Acceptance: 2026-10-03

This update supersedes the earlier #531 residual status. Work is published
directly on main under the user's current instructions; #547 remains excluded.
The latest reported behaviors, shared procedure/name admission and concurrent
creation are covered by the final family audit below.
Native inventory, truthful backend representations, localization, and free
Enterprise-equivalent constraints are approved policy differences. In particular,
native NODE KEY support is retained, not misreported as Community equivalence.

Published progress includes native option/target/suffix validation and durable
schema transaction staging (`d8116ef0`, `e8f7597c`, `b43a87e0`, `ca4e2d2b`,
`79b74029`, `67cbff64`, `74584cf3`), ordinary/vector/fulltext/typed duplicate
admission (`977fbb4a`, `104f3dc0`), reciprocal constraint/index names and the
incoming ANTLR type-predicate regression (`2967e967`), and locked LOOKUP name
admission (`3b6fc1e8`). Issue comments contain their individual matrices.

The reporter's latest replay on `e5de57d7` identified three remaining standard
DDL defects. Each now has fail-before/pass-after regression coverage:

- A multi-property node index sharing its first property with a single-property
  index now uses the existing durable ordered RANGE representation. Both appear
  in SHOW; subsequent graph writes remain query-visible. No new cache or
  optimized composite-query claim is made.
- Fulltext node label unions use the existing quote-aware ordered target decoder.
  Two-label inventory and retrieval match the reference; quoted literal pipes,
  escaped backticks, and malformed unions have native controls.
- Missing constraint drops use the observed database-error class. A neighboring
  missing-index probe exposed another raw class mismatch hidden by the generic
  type/phase comparison; it is also repaired. Both corpus cases now require their
  exact `Neo.DatabaseError.Schema.*DropFailed` codes.

Native-only public-route tests cover both actual runtime parsers: HTTP
autocommit/explicit commit/rollback and real-driver Bolt autocommit/explicit
transactions with persistent-schema rollback. Profiles and policies persist,
the multiplier remains 1.5, invalid clauses/options reject without changing
stored values, and profile DDL creates no phantom graph nodes. Embedded and
storage tests retain broader option, target, suffix, conflict, mixing, lifecycle,
namespace, and persistence controls. These are native evidence, not fabricated
Neo4j comparisons.

Final gates for this acceptance batch:

- Pinned Neo4j 5.26.30: Bolt 890 comparisons per parser, HTTP 900 per parser,
  total 3,580; both transaction modes, rows/columns, errors and graph effects.
- Ordinary/procedure schema lifetime and transaction mixing: 32 Bolt plus 12
  HTTP comparisons per parser, 88 additional comparisons.
- Full repository correctness excludes performance-named tests. Focused native
  policy/schema/wire races, touched-package vet, diagnostics and whitespace pass.
  No benchmark, timing comparison or allocation claim is made.
- New target-union parsing and multi-property routing statements are exercised;
  the shared union decoder has 100% coverage. Whole legacy helper coverage is
  lower and is not represented as 90%+ package coverage.

The fixed original 116-case prefix is unchanged. No new failures are waived or
silently treated as native policy. Broader raw-code enforcement, CI/TestKit,
projection and WHERE convergence remain separate #754/#713/#728 acceptance work;
#530 still requires its final family audit. Earlier intermittent cached-property
map panic in unchanged AsyncEngine iteration is recorded in issue evidence and
is not claimed repaired by this schema work.

### Final Shared Admission Audit

Procedure-backed creation now shares DDL admission against the actual mutation
schema view (`2d0af20d`). Legacy node-vector duplicate definitions and
index/constraint name collisions have exact ProcedureCallFailed reference
controls. All four native vector/fulltext creators have unchanged-inventory
duplicate controls. The complete live corpus is 896 Bolt and 906 HTTP
comparisons per parser, 3,604 total; native procedure extensions are validated
separately rather than compared with absent Neo4j procedures.

A final concurrent same-name test reproduced four successful creators for one
name. A schema-manager compound-operation lock now serializes ordinary DDL and
procedure admission through mutation, counters and transaction staging.
Correctness controls cover plain creators (one winner), guarded creators (all
successful with one definition), and competing ordinary/vector-procedure/UNIQUE
creators (one winner). The expanded control passes race detection. Vector runtime
registration stays inside successful mutation; explicit transactions still defer
it until commit.

The #531 acceptance decision covers the original and consolidated reported
behaviors, including native profile/policy routing and validation. NODE KEY is
the approved free Enterprise-equivalent policy difference. It does not claim
#547 structural cleanup, optimized composite caches, full-repository race
coverage, or repair of the unrelated cached-property iterator panic. The other
original families retain their own completion gates.

### SHOW Round-Trip Parser Audit

The latest #530 reporter's composite and multi-label fulltext defects are fixed
by the published #531 work. The existing SHOW recreation fixture now uses the
reported shapes rather than avoiding a composite index's already-indexed first
property. It also checks native POINT metadata and runs under both actual parsers.

The expanded fixture reproduced strict ANTLR rejection of RELATIONSHIP KEY.
Explicit grammar productions now admit relationship keys and the documented
temporal, domain, cardinality and relationship-policy constraints. Their SHOW
createStatement values recreate equivalent metadata under both parsers. New
keyword tokens remain usable as schema names, and RELATIONSHIP type predicates
retain admission. There is no permissive parser fallback.

Current pinned-reference inventory checks pass all 141 function signatures,
28 shared procedure definitions and three token-procedure comparisons. Normal
`go generate ./pkg/localization` succeeds without catalog drift. The live corpus
confirms statistics clearing rejects active collection and succeeds after stop.
Native POINT scan metadata has no spatial acceleration bounds; database store
metadata does not fabricate Neo4j's record-aligned format. These are native
backend representation differences, not literal reference-value matches.

This batch passes full repository correctness excluding performance-named
tests, focused parser races, owning-package vet and whitespace checks. The live
matrix passes 896 Bolt and 906 HTTP comparisons per parser, 3,604 total.
Validation used a detached copy of committed main plus this batch: concurrent
shared-checkout edits had unresolved storage symbols and removed unrelated
regression tests. Those edits were not reverted, included or credited as tested.
No test function was removed by this batch; the fixture workaround was replaced
with stronger assertions. #530 remains open pending its final family decision;
#713/#728/#754 retain separate gates and #547 remains excluded.

### Final SHOW Family Acceptance

The #530 acceptance decision now covers its original CALL/YIELD aggregation,
empty-input aggregation, projection/filter/window and row-carrying cases, and
the reporter's latest SHOW inventory addendum. Composite-after-single-property
and multi-label fulltext metadata are fixed by #531 and retained in the stronger
SHOW recreation fixture. Missing reference function listings and shared metadata
are covered by the passing 141-signature inventory comparison; procedure metadata
has 28 shared-definition and three token comparisons. The historical localization
generation and active-collector clear regressions no longer reproduce.

The approved native policy remains explicit: truthful product/database/user
identity, localized metadata and additional native inventory/constraint columns;
native POINT scan metadata does not advertise nonexistent spatial acceleration
bounds, and store metadata does not invent Neo4j's record-aligned format. These
are documented backend representations, not claims of literal Neo4j value parity.
The decision does not close projection/WHERE architecture convergence or #547.

Published implementation `d7f67a1d233fa4897f0f59e74a5f8c9e6bf9ac54` has green
GitHub [CI](https://github.com/orneryd/NornicDB/actions/runs/37177106250),
[Cypher Conformance](https://github.com/orneryd/NornicDB/actions/runs/37177106209)
and [Docs Pages](https://github.com/orneryd/NornicDB/actions/runs/37177106256).
Its tested source also passes repository correctness, focused races, vet,
catalog generation and 3,604 pinned Bolt/HTTP comparisons. Only graphify scripts
and documentation are currently uncommitted; production code compiles. No local
installation or running service was changed for this acceptance decision.

### OPTIONAL Count Projection Convergence

The latest #713 replay reproduced two wrong-result cases in both actual parsers:
products sharing a projected name returned separate rows, and incoming ORDERS
edges from Customer sources were counted despite an Order label in the pattern.
Both entry points to `tryFastCompoundOptionalMatchCount` are removed, along with
its private item parsing, projection, ordering and integer-only pagination.
The existing shared OPTIONAL matcher and aggregation path now apply label
filtering and grouping; no new counter or fallback was added.

The old helper tests were migrated rather than discarded. Shared-route controls
retain empty input, outgoing and alternate relationships, WITH tails, and
ORDER BY/SKIP/LIMIT assertions. A replacement error test injects faults at the
actual projected-label scan and checks wrapped error propagation. #821's
indexed-seed tests still require zero node scans. The obsolete lazy-loader
callback check is not retained as an API contract after deleting that helper.
Read-only Graphify inspection identified its separate scanning/pagination
dependencies; a source reference check confirms no remaining calls.

Two appended reference cases preserve the original corpus prefix. Both
transaction modes and parsers pass 900 Bolt and 910 HTTP comparisons per
parser, 3,620 total, including the exact new label and grouping controls.
Both official ratchets pass all 7,794 outcomes per parser. Repository
correctness excluding performance-named tests, focused races, vet and editor
diagnostics pass. No benchmark or performance-equivalence claim is made.

This removes one incorrect full projection shortcut; it does not establish
that every RETURN/WITH producer uses one projection planner. #713 remains open,
as do the separate #728/#754 gates. #547 is still excluded. Graphify ingestion,
its UI edits and the user's local installation remain untouched.

### Traversal OPTIONAL RETURN Planner Convergence

`projectTraversalOptionalRows` is now a thin binding-row adapter to the existing
shared RETURN planner. Its private projection, aggregate dispatch, ordering and
pagination implementation and `applyTraversalReturnModifiers` are removed.
Read-only Graphify inspection recorded the latter's sole caller as the private
projector. Aggregate helpers still used by other producers are retained for
their own convergence checks, not silently deleted.

Public execution already handled expression pagination correctly. The direct
traversal fallback failed all eight initial parameter/arithmetic controls in
both parsers. Delegating fixes those results. Missing node and relationship
pointers are normalized to Cypher null at the binding boundary: otherwise the
shared count collector counted typed nil pointers as values. Seven controls per
parser compare public and fallback rows/columns, including missing-entity nulls,
zero counts and a matched node/relationship count. Existing indexed-seed
zero-scan and neighboring OPTIONAL MATCH tests remain intact.

The complete repository gate caught an implementation-specific `count()` error
message assertion. It now checks the pinned structured SyntaxError class, and
both plain and mixed empty-COUNT rejection controls remain. Six appended wire
cases preserve the protected corpus prefix; the empty-COUNT cases pin the exact
raw code. No test function was removed in this batch.

Final observed checks: 912 Bolt and 922 HTTP comparisons per parser, 3,668 total;
7,794 official ratchet outcomes per parser with zero gaps, setup blockers or
harness errors; full repository correctness excluding performance-named tests;
focused races, vet, editor diagnostics and whitespace. Both changed production
functions have 100% focused statement coverage. The official Make target uses
the repository's pinned temporal archive; an initial run using Go's archive
failed two historical-timezone cases and is not credited as a passing gate.

Validation used an isolated copy of the five-file patch, then advanced to local
main's incoming graphify commits and reran repository correctness and all four
reference matrices. Their static UI build passes with the chunk-size warning.
No running installation was stopped, started or reconfigured. No benchmark or
performance claim is made. #713 remains open for the remaining projection and
column-planning inventory; #728/#754 remain separate and #547 is excluded.

### Localization CI Repair

The published `2007d796` CI build failed catalog generation because the typed
`graph.direction_invalid` message lacked a source entry. The exact CI command
reproduced locally. A new catalog regression fails for all three supported
locales without fallback, then passes after adding the English, Spanish and
pseudo-locale entries while preserving the out/in/both machine values.

The complete CI localization test, generation, manifest-drift and catalog-check
sequence now passes locally. Focused localization races and graph neighborhood
direction endpoint tests pass. No generated manifest change or test removal is
needed; existing runtime direction validation is unchanged. This repairs the
observed build gate, not the remaining #754 TestKit/CI acceptance items.

### Shared Traversal Aggregate Planner: 2026-10-04

Traversal aggregate projection now adapts complete bindings into the shared
parsed RETURN plan instead of running a private aggregate classifier/reducer.
Multi-MATCH aggregation also enters the shared MATCH/WHERE producer with seeded
parameters, retaining named paths and variable-length relationship lists.
Path-producing traversal projection retains the existing complete path values.

Regression-first checks exposed private percentile calls returning null and a
shared SUM float round-trip losing integer precision above 2^53. Continuous and
discrete percentiles now share the established collector; SUM retains exact
integer values across all ten accepted integer types, with float transitions,
DISTINCT and null controls. Two existing CALL tests used invalid `type(r)` on a
relationship list; they now use `type(head(r))` and assert all five neighbors,
relationship types, labels and the one/two-hop distance distribution.

Remove the production-dead classifier, grouping accumulator and mixed-expression
finalizer. The accumulator test now asserts shared null/DISTINCT collection,
including values and order. No test functions are removed. Scanner/placeholder
helpers still serve production; private parser/finalizer contract helpers remain
until their remaining controls are migrated. This is not full #713 completion.

Six appended corpus cases preserve the protected prefix. Final pinned reference
matrices pass 924 Bolt and 934 HTTP comparisons per actual parser: 3,716 total.
Both official Make-target ratchets pass 7,794 outcomes, zero gaps, setup blockers
or harness errors, using the pinned temporal archive. Repository correctness,
full Cypher correctness, focused races, scoped vet and editor diagnostics pass.
Adapter, numeric decoder, item-plan builder and projection classifier each have
100% measured coverage; extracted shared execution has 93.5%. Whole Cypher
coverage is 86.7%, not a claim of meeting the whole-package coverage target.

The first earlier repository run exited on a bare Badger `Assert failed` without
an owning test. Subsequent full Cypher and repository reruns, including the final
cleanup tree, pass. Its cause remains unexplained and is not credited as fixed.
Interrupted checks are not credited; ANTLR HTTP and ratchet reruns completed.
Main was pulled before publication; unrelated Graphify changes are excluded.
No local installation was managed and no performance measurement is made.
#713/#728/#754 remain open; #530/#531 are closed and #547 remains excluded.

### Graphify Dynamic-Key WHERE Regression: 2026-10-04

The requested special test freezes the reported 812-byte CosineSimilarity body
and scalar metadata. It reproduces the exact String-minus-Long error in three
parameter-map WHERE variants per parser; seven controls pass before repair.
The failure boundary is predicate value materialization, not a demonstrated
property-dictionary arithmetic defect. RETURN and static-key controls preserve
typed values and do not fail.

Initial-node MATCH now retains original WHERE text for complete-row evaluation.
Index admission recognizes values already bound in the incoming scope, including
scalar and nested-map operands, while rejecting unbound candidate references.
Typed scope reaches index lookup; predicates depending on outer rows defer
node-only early LIMIT filtering and cannot reuse another row's candidate cache.
Earlier matcher-only experiments were withdrawn after existing tests exposed
the joined LIMIT and indexed-read requirements; the final repair passes both.

Ten special controls include timestamp-only no-ops, genuine updates, body
variants, static keys, direct comparison, RETURN and unguarded SET, with exact
rows, columns and persisted properties. Admission and two-row indexed controls
retain exact integer values, reversed equality and separate lookup results.
No tests were removed and the importer/UI hash workaround remains untouched.

One appended reference case retains the same 812-byte payload and checks its
persisted update and graph effects against pinned Neo4j 5.26.30. The protected
corpus prefix is unchanged. Final matrices pass 926 Bolt and 936 HTTP comparisons
per actual parser: 3,724 total. Both official ratchets pass 7,794 outcomes with
zero gaps, setup blockers or harness errors and the pinned temporal archive.
Repository correctness, focused races, scoped vet and diagnostics pass. Changed
resolver coverage is 100%; the existing initial-node matcher has 90.2% focused
coverage. No whole-package coverage or performance-equivalence claim is made.

Incoming Graphify commits are preserved and the isolated snapshot is advanced
to their main revision before publication. No running installation was managed.
This closes the special reproduction, not the remaining #713/#728/#754 family
acceptance; #547 remains excluded.

### Shared Plain Multi-MATCH Projection: 2026-10-04

Plain `executeMultiMatch` now passes complete binding rows to the existing
shared RETURN projector. Remove the unreachable private aggregate reducer and
manual projection, ordering and integer-only pagination tail. Aggregate
production and its existing numeric compatibility contract are unchanged.

The initial direct regression passed literal SKIP/LIMIT but returned all four
rows for parameterized and arithmetic windows instead of rows 1 and 2. All
eight direct/public controls now pass: literal, parameter and arithmetic
windows, DISTINCT, hidden sort keys, parameter projection, empty matches and
LIMIT 0. A ninth control checks ArithmeticError propagation. Direct tests must
supply the normal statement failure context and inspect structured status;
public Execute may return empty column metadata alongside an error, whereas
the direct projector returns nil. No runtime change was needed for that test
contract correction. The new adapter retains query statistics.

Eight appended corpus cases preserve the protected original prefix. Pinned
Neo4j 5.26.30 comparisons pass on both actual parsers and transaction modes:
942 Bolt and 952 HTTP per parser, 3,788 total. Both official ratchets pass
7,794 outcomes with zero gaps, setup blockers or harness errors using the
pinned timezone archive. Isolated repository correctness, scoped vet and
diagnostics pass; final focused tests pass under race instrumentation in both
parsers. Every statement in the new plain adapter is covered; this is not a
whole-function or whole-package coverage claim.

The snapshot was advanced to incoming UI commit `668494a5`. Its first full
run encountered the previously observed bare Badger `Assert failed` while an
in-memory concurrency test was active. That test passed 20 isolated runs;
the active test name does not establish the assertion's source. A fresh full
integrated repository run passed. The assertion remains unexplained, not fixed
or proven unrelated by this increment; no test was removed.

No performance measurement or running-installation changes were made.
Unrelated matcher whitespace and concurrent UI edits are excluded. Publish
this increment with `Refs #713`; broader #713/#728/#754 acceptance remains
open and #547 remains excluded.

### Traversal Aggregate Helper Retirement: 2026-10-04

The private traversal aggregate parser/spec, finalizer, SUM and deviation
implementations had only test callers after production RETURN convergence.
Remove those unused implementations and migrate their existing assertions to
the shared aggregate parser and collector. Keep the aggregate-span scanner,
function-name list and placeholder helpers that still have production callers.

Retained controls cover valid and invalid call forms, DISTINCT, count/star,
null and empty input, mixed numeric SUM, collections, min/max and sample versus
population deviation. Unknown functions are explicitly unhandled rather than
silently producing null. The typed localization descriptor identity/text test
remains, while existing execution regressions still reject empty COUNT calls.
No test functions were deleted and no public execution behavior changed.

Fresh isolated repository correctness and both-parser focused races pass,
alongside scoped vet and diagnostics. The unchanged differential corpus passes
942 Bolt and 952 HTTP comparisons per actual parser: 3,788 total against pinned
Neo4j 5.26.30 in autocommit and explicit transactions. Both official ratchets
pass 7,794 outcomes with zero expected gaps, setup blockers or harness errors,
using the pinned timezone archive. An earlier interrupted repository log was
not counted as a pass; the fresh completed run supplies this evidence.

The isolated snapshot was advanced to incoming UI commit `5a33c3b0` and the
integrated repository correctness gate passed again before publication.

No performance measurement or running-installation changes were made.
Concurrent UI changes and matcher whitespace remain excluded. This completes
the obsolete traversal helper retirement, not the remaining #713/#728/#754
family acceptance. Publish with `Refs #713`; #547 remains excluded.

### Shared Async CREATE RETURN Planning: 2026-10-04

The async node-only CREATE batch path evaluated each RETURN item independently
and substituted parameter values into the entire query before deriving column
names. Direct batching and production async autocommit therefore ignored LIMIT
0 and parameterized SKIP, and renamed an unaliased `$p` column to its value.
Explicit transactions were the passing controls for all three reproductions.

Delegate the complete original RETURN clause to `projectCreateReturn`, retaining
typed parameters and canonical column naming. Parameter substitution now affects
only the CREATE pattern. Evaluate projection before publishing the batch, so an
ArithmeticError cannot leave created nodes. Remove the now-unused single-item
adapter and migrate its existing assertions onto whole-clause projection.

Eight column/row cases pass on direct batching, production async autocommit and
explicit transactions: LIMIT 0, parameter SKIP, DISTINCT, unaliased integer,
float and map parameters, quoted aliases and grouped collection/count control.
Persisted readback verifies required writes even when RETURN produces zero rows.
An additional control pins ArithmeticError and zero persisted nodes in all three
routes. Existing CREATE expression, parameter-injection and admission tests are
retained; no tests were removed.

Nine appended reference cases compare columns, rows, errors and graph effects
against pinned Neo4j 5.26.30. Both actual parsers pass 960 Bolt and 970 HTTP
comparisons in both transaction modes: 3,860 total. Both official ratchets pass
7,794 outcomes with zero gaps, setup blockers or harness errors using the pinned
timezone archive. Isolated repository correctness, CREATE-scoped both-parser
races, scoped vet, diagnostics and whitespace checks pass. Shared CREATE
projector coverage is 100%; the existing batch function has 88.7% focused
coverage. No whole-package or performance-equivalence claim is made.

The broader ANTLR async-schema control fails on the existing unquoted
`vector.dimensions` map key. Running its complete parent test on the prior
published source reproduces the identical failure. It remains unchanged and is
not counted as passing; track its valid-schema admission contract separately
under #754. The initial baseline attempt selected only one subtest and also
broke parent fixture assertions; only the complete-parent run supplies evidence.

Graphify's snapshot predates prior CREATE convergence; current source confirms
the async batch now joins the canonical CREATE/RETURN planner. No running
installation was touched. Publish with `Refs #713`; remaining #713/#728/#754
acceptance stays open and #547 remains excluded.

### Async Schema Fixture Syntax: 2026-10-04

The expanded ANTLR safety selection exposed an existing async-schema test whose
vector index options used the unquoted dotted key `vector.dimensions`. The full
parent test failed identically on prior published source, independent of the
async CREATE repair. Pinned Neo4j 5.26.30 also rejects that exact syntax with
SyntaxError and accepts the backtick-quoted key. The temporary reference index
was dropped after verification.

Correct only the fixture query. No production parser behavior, assertions,
subtests or admission coverage changed. The complete async-schema parent test
passes under both actual parsers with race instrumentation; the expanded ANTLR
CREATE/schema safety selection now passes without excluding the fixture. Fresh
isolated repository correctness and whitespace checks pass. The previous CREATE
reference matrix and official ratchets describe unchanged runtime code, not a
new run attributed to this test-only correction.

This resolves the independently recorded fixture failure, not the complete
#754 CI/TestKit family. Publish with `Refs #754`; #713/#728 remain open and
#547 remains excluded. No performance measurement or running-installation change.

### Compiled Relationship-Batch Columns: 2026-10-04

The relationship-batch RETURN compiler independently split AS and admitted only
simple explicit aliases. Compile its row-field expressions and column names from
the cached shared RETURN plan instead. Keep the row-field extractor and builder
as compiled leaves; reject star, DISTINCT, aggregation and modifier plans rather
than silently bypassing their semantics.

The ordinary explicit alias is the passing control. Quoted, escaped-backtick and
inferred expression columns fail the old compiler/shared-projector parity check
and pass after convergence. Decline controls retain malformed aliases, wrong
bindings, computed expressions and unsupported plan forms. Four end-to-end cases
assert actual batch use, exact columns/rows, repeated-MERGE idempotence and stored
relationship/vector values. Existing indexed lookup and identity tests remain.

Four appended reference cases preserve the protected corpus prefix. Both actual
parsers pass 968 Bolt and 978 HTTP comparisons in autocommit and explicit
transactions: 3,892 total against pinned Neo4j 5.26.30. Official ratchets pass
7,794 outcomes/parser with zero gaps, setup blockers or harness errors using the
pinned timezone archive. Isolated repository correctness, changed-slice races in
both parsers, scoped vet and diagnostics pass. Compiler and row builder both have
100% focused coverage; no whole-package or performance-equivalence claim.

The broader ANTLR node-batch annotation test fails at EOF on its existing
SET-to-MATCH template. Its exact full test fails identically on prior published
source without this compiler change. It remains unchanged and is not counted as
passing; track its clause-boundary contract separately under #754. No test was
deleted. Graphify's older dependency snapshot identifies the private alias
boundary; current source confirms the new shared-plan delegation.

No running installation was touched. Publish with `Refs #713`; remaining
#713/#728/#754 acceptance stays open and #547 remains excluded.

### Canonical Node-Template Boundary: 2026-10-04

The MERGE-first annotation test template omitted a WITH boundary between SET
and a subsequent MATCH. Its full ANTLR test fails identically on prior source,
independent of the relationship-column compiler. Pinned Neo4j 5.26.30 rejects
the same phase boundary with "WITH is required between SET and MATCH" and
accepts the scope-preserving `WITH n, row` form. Temporary reference nodes and
relationships were removed after verification.

Add only that boundary to the test template. Preserve its actual batch-use,
unique-schema-lookup and zero-scan assertions. The exact fixture passes under
both parsers with races enabled; the previously failing expanded ANTLR
relationship/node-batch selection now passes without exclusions. Fresh isolated
repository correctness and whitespace checks pass. No runtime implementation,
assertion or test was removed. Earlier 3,892 reference comparisons and official
ratchets describe unchanged runtime source, not a new test-only matrix.

Publish with `Refs #754`; complete #713/#728/#754 family acceptance remains open
and #547 remains excluded. No performance or running-installation change.