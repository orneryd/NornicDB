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