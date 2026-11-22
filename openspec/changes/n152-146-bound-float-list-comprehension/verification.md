# N152-146 verification ledger

Base: f77ec7706de099588ac232b32b6908860511d3da. All working-tree checks include
uncommitted task changes; no final-head verification or publication claim.

## Completed evidence

- Original24 RED before edits:1.159s on3aa; repeated1.222s onf77. Both:12 false
  rejections and12 matching arithmetic controls. Logs `/tmp/n152-146-{original,integrated}-red.log`.
- Fresh reference:46+10 RUN probes on Neo4j5.26.30. All56 shipped JSON records
  exactly equal concatenated raw records including query, parameter, output and
  exit. Both remote lifecycle logs end CLEANUP_COMPLETE. No original120 claim.
- First green attempt before session interruption:empty log, unexecuted.
- Recovered narrow-fix normal:FAIL52.427s. Original24, fresh112 and named numeric/
  nonnumeric/function/property/owner guards passed; recovered258×12 inherited
  family had257 missing rejections per lane. Log `focused-green-recovered.log`.
- Detached f77 baseline exact inherited fixture:nornic/RUN/auto FAIL3.320s,
  257 missing rejections plus1 Float false rejection. Log `baseline-inherited.log`.
- `make verify-list BASE=origin/release/v1.5.2`:exit0.
- `env GOMAXPROCS=2 GOMEMLIMIT=1500MiB make verify BASE=origin/release/v1.5.2 ONLY=localization`:
  pass8s before propagation changes; package tests4.234s/catalog0.280s/inventory0.435s,
  not cached. UI,llama-libs,build,tests,allocations,conformance,differential,docs
  skipped by ONLY. This is not a full verifier pass or final-source evidence.
- Initial staged duplicate hook:exit1, analysis/ingestion failure at configured
  localhost7474/nornicdbcode. No empty-result/clean claim. Canonical CONTRIBUTING
  treats release duplicate analysis as advisory; candidate publication remains
  parent-owned and does not establish readiness.
- Go source whitespace clean before propagation;24 preserved historical transcript
  heading warnings (EXPLAIN/PROFILE trailing spaces). Raw source is not rewritten.
- Initial propagation normal check:exit1 with only FAIL during shared host disk
  exhaustion; no runtime evidence. No Go process from this task remained afterward.

## Commands and limits

All Go checks use GOMAXPROCS=2,GOMEMLIMIT=1500MiB,-p=1,-tags=noui,nolocalllm.
The normal source selection is `^Test(NOR79|Nor79ParameterAwareConstructorClaims|Static|CheckOperator|CompileTimeTypeErrors|PropertyAccess)`.
Full verifier/default-tag/platform qualification, indexed discovery and the
unrecovered120 remain pending. Final scoped checks and matched costs follow below.

## Propagation feedback and causal reassessment

- Broad propagation normal run: FAIL90.565s (`propagation-green-recovered.log`).
  Original24, fresh112, rebinding108 and shadowing18 passed. All257 inherited
  rejection cases now reject, but32 MATCH cases per lane have a diagnostic-class
  mismatch. The full constructor oracle exposed seven accepted Map MATCH cases
  falsely rejected. The existing nested-scope allocation guard also failed.
  These are unresolved findings, not a qualification pass.
- Exact allocation guard on preserved f77 binary: PASS0.33s, 3681 allocations at
  depth5 and9972 at depth10. Current: FAIL1.79s,15850 and496084. Both captured CPU
  profiles (`nested-baseline.pprof`, `nested-current.pprof`). Current profile
  attributes81% cumulative CPU to nested checker/checkScoped,79% to signature
  traversal. `checkScoped` retained two memo tables after both passes became
  parameter-aware, repeating the same nested derivation. Parent approved sharing
  one request-local table; nil-parameter compilation retains its existing facts.
- Rule73 reread: no per-case exemption, verdict/threshold change or duplicate
  classifier is permitted. Fresh reference MATCH probes are in progress before
  changing the Map owner or existing conflict diagnostic class.

- Fresh49 MATCH RUN probes completed on Neo4j5.26.30, exact input metadata
  independently compared with generated probes and canonical population hash:
  all32 inherited cases reject with Type mismatch; all9 historical Map cases
  accept. Eight controls distinguish literal Map/Integer/String, Node binding,
  Node-as-relationship and EXISTS scopes. All rejected controls use Type mismatch;
  literal Map rejects while parameter Map accepts, including EXISTS. Raw lifecycle
  `/tmp/n152-146-reference-match.log` ends CLEANUP_COMPLETE, owned container,
  volume and newly pulled image removed. Shipped records are
  `pkg/cypher/testdata/nor79_reference_match.json`. No verdict oracle changed.
- Parent-approved corrections reuse `typedOperand` for Map parameter alternatives,
  share `checkScoped`'s request-local memo and prefix the existing binding-conflict
  message with Type mismatch. Status/detail and previous wording remain; this is
  diagnostic-class parity, not exact Neo4j message-text parity. The internal Map
  branch assertion now expects the proven union representation. Literal Map stays
  concrete. Final integrated checks remain pending.

- Integrated owner corrections normal: FAIL83.890s. Original24, inherited258×12,
  fresh56×2 and49×12, rebinding/shadowing, conflict controls and allocation guard
  pass. Full constructor oracle retains nine List-parameter NOT false rejections
  in auto and tx. These were also present in the preceding90.565s log; its earlier
  Map-only summary was incomplete. No verdict was changed.
- Fresh34 Neo4j5.26.30 RUN probes independently match prepared query/parameter
  metadata:12 historical accepted list-NOT forms,8 parameter/literal list pairs,
  and6 AND/CASE/WHERE controls. Every parameter case accepts; every literal case
  rejects list coercion. Raw lifecycle `reference-boolean.log` ends CLEANUP_COMPLETE;
  owned container/volume/new image removed. Exact records shipped in
  `testdata/nor79_reference_boolean.json`; original120 still unavailable.
- Exact-version `FrontEndCompilationPhases.scala` places SemanticTypeCheck before
  AstRewriting(parameterTypeMapping); `SemanticTypeCheck.scala` owns
  ListCoercedToBooleanCheck. Normal SemanticAnalysis then checks parameter types.
  The canonical Boolean owner now enforces list coercion only in the nil/empty
  parameter phase, retaining ordinary Boolean signature checks in both phases.
  Request admission is entered only with nonempty params. Before its uncached
  walk it invokes the existing cached nil-parameter semantic owner: public Execute
  already populated the cache; opaque executeInternal bodies gain the missing
  value-independent phase. No second walker or parameter-provenance classifier.
- New36 public cases exercise opaque APOC statement parameters, visible CALL and
  FOREACH, each with parameter-only, literal-only and mixed predicates in both
  parsers and transaction modes. Their integrated normal run is pending.

- Phase correction normal: FAIL97.659s solely in eight new opaque-test prefix
  assumptions. Original24, inherited258×12, entire constructor oracle, fresh34×12
  and49×12, fresh56×2, scope/rebinding and allocation controls passed. Opaque
  literals and mixed predicates correctly reject as SyntaxError; APOC's existing
  message starts `apoc.cypher.run inner query failed: `. The new test now pins that
  existing wrapper prefix; no production or historical expectation changed.

## Current integrated normal result

PASS98.531s (`/tmp/n152-146-final-normal.log`), exact selection:
`^Test(NOR79|Nor79ParameterAwareConstructorClaims|Static|CheckOperator|CompileTimeTypeErrors|PropertyAccess|SubqueryExpressionScopesConverge)`.
Includes original24, inherited258×12, complete historical constructor oracle,
fresh56×2, MATCH49×12, Boolean34×12,36 public phase controls,108 rebinding and18
shadowing calls, broad static/function/operator/property consumers. Nested guard
4467 allocations at depth5 and11757 at depth10; its threshold is unchanged.
Binary saved as `/tmp/n152-146-final.test`. At this checkpoint race, vet and matched performance were pending; final results
are recorded below. Staged duplicate analysis still fails at localhost7474;
no indexed-clean or complete-qualification claim.

## Original120 recovery attempt

Refreshed original Linear NOR-79 and downloaded its linked Triage findings,D12
attachment. Exact D12 excerpt is retained in fixtures; full raw source remains
`/tmp/n152-146-original-triage-attachment`. It names
`bugqueue-1632-r6-out-of-family-20261023.md` §1, populationG and PR1632 Known
differences, without supplying the120 queries. The named file is absent from both
known `sig/handoff` trees (main checkout and task checkout) and Git all-ref path
history. PR1632 body repeats the count without population; its comments supply
no matching archive/fixture. No exact120 recovered and no equivalent population
invented. The258 historical constructors and fresh probes are separate evidence.
PR supports this issue; whole-issue closure remains pending original120 and full
release/performance qualification. Parent owns Plane/PR updates.

Current integrated race selection PASS515.868s, fresh `-race -count=1`, same
selection as normal, timeout20m. Log `/tmp/n152-146-final-race.log`; compiler's
existing duplicate `-lobjc` linker warning is nonfatal. GOMAXPROCS2 and
GOMEMLIMIT1500MiB set; observed process RSS included additional race/native
memory (approximately3.8GiB), so this heap limit is not a hard RSS cap. At this checkpoint vet and scoped verifier were pending; their final results
follow below. No full-release qualification claim.

## Final handoff checks

- Existing internal consumers:24 top-level tests PASS in preserved final binary
  (`final-internal-consumers.log`), including opaque evaluated arguments, CALL
  cache/import isolation, loop values, effects and cancellation. This supplemental
  run is normal, not an additional race run.
- `env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher`:
  exit0 (`final-vet.log`).
- Current `make verify-list BASE=origin/release/v1.5.2`:exit0.
- Current `env GOMAXPROCS=2 GOMEMLIMIT=1500MiB make verify BASE=origin/release/v1.5.2 ONLY=localization`:
  PASS7s, fresh localization3.969s/catalog0.512s/inventory0.220s. Run
  `.verify/runs/20261121-203259`; UI,llama-libs,build,tests,allocations,conformance,
  differential,docs skipped by ONLY. This is scoped verification, not full verify.
- Final staged duplicate checker:exit1 (`final-current-duplicate.log`), configured
  localhost7474 analysis/ingestion unavailable. Manual owner review establishes
  canonical reuse; indexed discovery is incomplete. No bypass or commit made.
- All Go whitespace checks pass. Historical transcripts remain byte-preserved.
- Base/head before commit remains f77ec7706de099588ac232b32b6908860511d3da;
  these are working-tree results. No commit/push/PR was performed by this child.
- Baseline worktree/binaries and raw profiles remain for parent review. Remote
  owned resources from all four reference runs were removed with cleanup receipts.
- Matched timing shows measured correctness cost, retained by parent: MERGE+0.78%
  (overlapping ranges), filtered traversal+8.77%, tiny cached rebinding+89.42%
  (+14.084µs). No performance-qualified/no-regression or whole-ticket claim.
