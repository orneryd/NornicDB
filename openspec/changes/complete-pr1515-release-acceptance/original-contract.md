# Original PR1515 acceptance contract and follow-up scope

Captured before the checkpoint integration. These are source requirements, not claims of completion. The checkpoint full-suite result must be recorded separately. Unverified items remain open.

## Original linked issue1513

Remote constituents created with explicit `USER`/`PASSWORD` fail authentication through `USE composite.alias` on both develop and release/v1.4.4. The same constituent works when addressed directly as the database. URI credentials also fail across complete remote workflows.

Reproduced against two disposable, authenticated NornicDB instances with synthetic credentials on develop `16923aef778025fab3490f6a57549763b536f3d5` and release `24d08776cdb9cb624da1073007da437306518426`. HTTP, GraphQL and Bolt clients of the coordinator are affected, with both HTTP and Bolt remotes.

Minimal reproduction:

1. Start an authenticated disposable remote with user `admin`, password `synthetic-userpass-investigation-48263917`, and HTTP/Bolt enabled. Create a remote-only marker using `CREATE (:AuthInvestigation {marker: 'real-remote-marker-67193824'})`. Verify the correct password reads it and a wrong password is rejected. Configure the coordinator's remote credential encryption with a synthetic key.
2. On the coordinator's system database:

```cypher
CREATE COMPOSITE DATABASE explicit_h
ALIAS r FOR DATABASE nornic AT 'http://127.0.0.1:REMOTE_HTTP_PORT'
USER admin PASSWORD 'synthetic-userpass-investigation-48263917'
TYPE remote ACCESS read
```

3. Execute `USE explicit_h.r MATCH (n:AuthInvestigation) RETURN n.marker AS marker` through the coordinator. Creation succeeds, but the remote rejects authentication and the remote-only marker is absent. Addressing `explicit_h.r` directly as the database with the same `MATCH` query succeeds. Substituting a Bolt remote produces the same Fabric authentication failure.

Cause: `cypher.buildFabricCatalog` copies the encrypted stored password into `LocationRemote`. The remote executor uses that stored ciphertext as the outbound password, bypassing `DatabaseManager`'s existing runtime decryption. The alternative catalog builder has the same boundary error.

The related URI cases are part of the same credential family: HTTP URI userinfo authenticates transaction-open, but query and rollback requests lose authentication when following the returned transaction URL; Bolt URI userinfo selects `NoAuth`.

The same family also has a persistence omission: CREATE and ALTER ADD encrypt only the separate explicit Password field. URI-only and URI-plus-explicit forms retain plaintext URI userinfo in the serialized `system:databases:metadata` node, even with a credential key configured; storage backups can therefore copy it. The independent six-case persistence regression reproduced four failures per branch (URI-only and combined forms through both writers), jobs `i20261018232611-15212-d229` / `i20261018232611-93828-dbdd`. This is an existing writer omission, not newly introduced by the runtime correction.

Required behavior: one owner resolves runtime credentials, and the resolved identity authenticates every open, query, commit and rollback request over both transports, for implicit and explicit transactions. Runtime plaintext must remain covered by the existing display/error redaction owner.

All new accepted credential forms must normalize to a userinfo-free endpoint and encrypted credentials at the common persistence boundary, preserving explicit-over-token-over-URI precedence. A writable open with a key migrates legacy records. For upgrade compatibility, existing unkeyed/read-only catalogs must continue opening and working with one secret-free operator warning, then migrate on a later writable keyed open. New URI credentials without a key follow the same existing rejection rule as new explicit USER/PASSWORD. Existing backup files are not rewritten; restored legacy metadata follows the same open-time migration policy.

Scope note: Neo4j-style `CREATE ALIAS ... AT ... USER ... PASSWORD ...` remote alias syntax is a missing feature, deferred to a **1.5.0 follow-up**. It is outside this 1.4.4 bug fix. The supported inline composite syntax above is the reproduction.


## Original PR1515 body

Work in progress, not ready for review: published by request so the maintainer has the latest work.

Authorship: credentials lane (Codex) wrote the implementation and this report. Nornic2 relayed the publication and scope decisions. TJ owns the retained release catalog/storage implementation and the ongoing #1515 integration. The technical statements below are the credentials lane's observations, with evidence limits stated explicitly.

## What is wrong

Remote constituent selection and diagnostics must preserve the selected alias's saved credentials. Independent readers of query text and decoded catalog names can disagree about that selection or expose a credential in output. The whole affected population is still being reviewed; this publication is a handover of unfinished work.

**Reproduce:** admit distinct constituent aliases `" r "` and `"r"`, then select the former through `GetStorageWithAuth("catalog. r ", ...)`.

- Expected: the exact admitted alias and its own credential are selected.
- Observed in the saved baseline: the plain alias can be selected instead. This case is still open.

## What this PR fixes

This publishes the existing remote credential ownership, source readers and diagnostic work, rebased onto the merged release. The implementation contains manager-owned credential normalization/runtime resolution and shared lexical/source products used by its consumers. The integrated population correction is unfinished; catalog rejection, exact selector identity and diagnostic disclosure findings remain open.

Related: #1513. The remote DDL extension remains stacked in #1525. This commit has only the merged release as its parent and imports no integration-branch history.

## How it was fixed

- Keep saved remote credentials and runtime resolution with the database manager; route remote locations through that manager.
- Share quote, comment, name, number, statement and original-source products rather than adding another parser.
- Carry accepted-source spans into the existing administration, USE and diagnostic adapters.
- Retain the merged release's database view, pending-DROP recovery and persistence snapshot owners while adding the credential migration/runtime behavior.
- Preserve the accumulated regression tests and census data. The frozen census client still needs replacement by the approved shared engine.

## How it was tested

The new release-based commit receives a compile-only check of the 18 changed Go packages. No behavioral suite or readiness gate is being claimed for this rebased head. Earlier focused behavioral receipts are historical evidence at their named heads; they have not been rerun on this composition.

The saved population before this publication restack has **25,743 declarations: 4,653 classified and 21,090 OPEN**. The decoded-name input has **2,104 sites: 15 classified and 2,089 OPEN**. These are the last saved census counts, not a completed census of the new head; it must be refreshed.

Known open regressions include rejected catalog writes changing live state, whitespace-bearing alias/database identities, native quoted-name decoding, query-source framing/readers, remote access-mode selection, wire credential preservation and credential-bearing diagnostic output. Passing compilation does not close those regressions.

## Performance

No performance qualification was run for this publication.

## Neo4j compatibility

No fresh external compatibility run was performed for this rebased head. Existing syntax policy and the release's invalid `USE $db` assertion are retained.

## Review history

The credentials foundation has repeated REQUEST CHANGES and Rule 73 remains active. Discovery now unions graph queries, all-tracked-source searches, the shared census engine and the actual delta. Classification and the integrated correction remain incomplete. This PR is not offered for another review.

## Branch and milestone

- [x] Based directly on the merged release for v1.4.4.
- [x] Release milestone retained; PR remains draft.
- [ ] Full verification and independent review passed.

## Type of Change

- [x] Bug fix.
- [x] Shared owner convergence.

## Code Quality Checklist

- [x] Existing regression tests and source-owner data retained.
- [ ] Whole population classified and every defect verified.
- [ ] All behavioral tests, coverage and formatting requalified on this head.

## Documentation

- [x] Work-in-progress status and verification limits documented.
- [ ] Final release documentation and readiness evidence complete.

## Proof of Value

- [ ] Integrated public regressions pass on this composition.
- [ ] Required final gates and performance qualification complete.

<details><summary>Evidence and remaining handover work</summary>

Author: credentials lane (Codex). Scope/publication direction: orchestrator Nornic2. These statements do not claim maintainer approval.

- Head: dad30a7a1789cc716bef53283016bc2ef30feb90; base: d663ce8f1320e409e2eaa29560b987ffe6526207; one credentials commit, 313 changed files.
- Compile receipt: Tagged compilation: initial 36 package/parser units at20642a84 (34 PASS; two Cypher test-build failures). A test-only import/independent-layer assertion correction was folded; finaldad30a7a Cypher units i20261026120049-45240-66ed / i20261026120100-45240-2b6b PASS. The other17package test sources and allproduction/dependency sources are byte-identical between the two commits. This is compilation/init only, run^$, race/count1; no behavioral pass claimed..
- Historical focused metadata/persistence and fixture qualification: `i20261026031156-39212-d540`, subsequently folded into lower `12608bd14`; 46 pass events including the package and no leaked stores. This is not behavioral qualification of the new release rebase.
- **F25 OPEN:** the one catalog admission/persistence transition is not implemented. It must preserve the release owner and distinguish rejection, cancellation before acceptance and accepted work with a later error. The contextual consumer will supply its original cancellation signal, with no second rollback.
- **F26 OPEN:** catalog selector, cache dependency, planner, session and enumeration identities still need the integrated correction. R14/R16/R17 and the corrected R19b public baselines retain failure and control receipts. The new shared-body capability-view composition remains pending.
- **F27 OPEN:** asynchronous periodic query output still emits supplied credential-bearing text. R18 public API baselines fail on the credentials and ready-release compositions; ordinary display controls pass. No statement execution is inferred from these APIs.
- **F23 OPEN:** contextual Bolt parameter diagnostics still disclose a known credential repeated under an ordinary parameter key. R13 is historical evidence on the exact old credentials head, not a fresh current-release run.
- Other unresolved own-family findings include F1–F8, F11–F13, F15, F18, F19 and F22. NOR-167 is the existing Unicode-name issue; it is not claimed fixed. Other-owner findings and tracked external issues remain in the private evidence register.
- The URL-sink inventory conflict applies the exact credentials data delta mechanically to the release inventory. Its behavioral census has not been rerun or regenerated in this compile-only publication.
- Untagged build attempts i20261026120120-33584-a9aa / i20261026120128-33584-1743 built the UI but failed during local-LLM preparation because the pinned library image was not baked into the lab image. Go compilation never started in those attempts; untagged/local-LLM compilation is unqualified.
- Compile-only source correction: add the missing ANTLR test import and remove a direct assertion on the private helper supplied only by unmerged#1679. Every public Execute diagnostic case/assertion remains; no#1679 product helper was imported.
- No full test suite, final gate, changed-line coverage, performance run or fresh Neo4j comparison was performed for this handover. No approval is claimed.

</details>


## Detailed carry-forward inventory

FALLBACK PLACEHOLDER — the authorized external evidence path
/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1503/pr1515/ was rejected by
workspace_write/workspace_command as outside a registered worktree, so this draft was written
in-repo pending parent relocation.

# PR1515 → follow-up PR evidence handoff (DRAFT)

Status: draft inventory only. No repository edits, tests, benchmarks, Git mutations or Linear
writes were performed. The parent agent owns integration, acceptance and any Linear/GitHub action.

Base / release baseline: d663ce8f1320e409e2eaa29560b987ffe6526207 (remote graph orneryd/NornicDB,
62,333 nodes / 2,175,827 edges).

## Scope (checkpoint merge vs follow-up)
The current checkpoint merge is separately authorized: its gate is that all observed integrated
regressions are fixed and an uncached make test passes on frozen source. It does NOT claim all-AC
completion, a duplicate-checker pass, or broader merge-readiness; the checker is temporarily
disabled in local config by explicit maintainer decision. The follow-up PR carries every AC
without sufficient current evidence, all remaining convergence-audit findings, and the deferred
performance qualification. Merged checkpoint and follow-up PR must be linked reciprocally and
incomplete Linear/GitHub acceptance items stay open (checkpoint-integration-plan.json → tracking).
Benchmarks are deferred until the major refactor is pushed.

## Evidence classes
CONFIRMED-CURRENT = current-source evidence passing for the named scope only.
EARLIER-EVIDENCE = produced on an earlier snapshot/staged tree; needs continuity on frozen source.
REMAINING-IMPLEMENTATION = work/design/adoption still due in the follow-up PR.
UNKNOWN = not established by the supplied artifacts.
Retirement, unchecked boxes and earlier snapshot results are NOT completions.

## A. Acceptance criteria / task items without sufficient current evidence
44 unchecked boxes exist; 6 are superseded-but-open (§D) and 38 are open inventory items.

A1 Release gate (9): T01 credential patch vs metadata DROP/recovery/quota owners — REMAINING-IMPLEMENTATION/EARLIER-EVIDENCE. T02 reconcile all ACs and review feedback, retain red evidence — REMAINING-IMPLEMENTATION. T03 persistence/runtime/auth workflows — EARLIER-EVIDENCE (credential workflows 60.5s/68.6s; auth race 23.988s scope-only). T04 profile/compare lookup workloads vs immutable baseline — REMAINING-IMPLEMENTATION (deferred benchmark). T05 enabled duplicate checker + resolve all findings — REMAINING-IMPLEMENTATION. T06 uncached make test incl. baseline regressions — REMAINING-IMPLEMENTATION (make-test-handover.log FAIL exit 2 832.5s; make-test-round2.log FAIL exit 2 874.64s). T07 publish merge-ready PR + Linear evidence/limitations — REMAINING-IMPLEMENTATION. T08 shared access-mode admission owner incl. remote procedure/dynamic statement — EARLIER-EVIDENCE (read-alias-source.log 2.502s; read-alias-integrated-race.log Server 16.608s / Cypher 3.264s / Bolt 2.910s). T09 DeepSeek follow-up review of corrected critical boundary — UNKNOWN.

A2 Local cleanup and continuing readiness (5): L01 unknown-DROP red state — CONFIRMED-CURRENT named-scope reconciliation (unknown-drop-public-contract.log, public BoltErrorCode SyntaxError). L02 reviewed lexical/URL sink inventories + duplicate convergence + checker rerun + uncached make test — REMAINING-IMPLEMENTATION. L03 CLI R2 ALTER ADD/catalog (cli-r2-catalog.log 39.046s) — EARLIER-EVIDENCE, partly superseded by cli-r2-preparation-after.log and cli-r2-preparation-antlr-after.log. L04 URL sink inventory stale/unreviewed entries (url-sink-current-review.log) — REMAINING-IMPLEMENTATION item-by-item, no blanket hash refresh. L05 full CLI regression failing at EXPLAIN — EARLIER-EVIDENCE (composite-preparation-public3.log); frozen-source CLI rerun UNKNOWN.

A3 Current integrated correction round (4): I01 final integrated make test — REMAINING-IMPLEMENTATION. I02 F25 detached proposals/terminal-receipt publication incl. pending DROP recovery + offline reopen — CONFIRMED-CURRENT for named scope (f25-* evidence); combined release qualification REMAINING-IMPLEMENTATION. I03 F26 decoded identity correction — CONFIRMED-CURRENT for named scope (f26-identity-correction-evidence.md; worker source rejected, parent correction passes). I04 final local review evidence, no remote mutation — REMAINING-IMPLEMENTATION.

A4 Composite command admission and EXPLAIN (1): C01 final integrated make test — REMAINING-IMPLEMENTATION.

A5 F25 catalog acceptance and publication (2): F25-1 final integrated make test (f25-multidb-final-source.log, f25-catalog-final-source-race.log are scope-only) — REMAINING-IMPLEMENTATION. F25-2 source-restoration duplicate convergence preserving source coordinates, typed diagnostic opacity, stage order, exception identity — EARLIER-EVIDENCE with finding still open.

A6 F18 required remote access mode (1): F18-1 parent disposition of cold imported allocation tradeoff (+8.3% bytes / +4.5% allocations) + frozen inventories/checker/make test — REMAINING-IMPLEMENTATION, parent decision. External Neo4j null-parameter reference test UNKNOWN/unexecuted without reference endpoint; no added skip or passing claim.

A7 Complete admitted-registration snapshots (1): G01 reconcile frozen inventories/checker for the later UNSTAGED snapshot correction — REMAINING-IMPLEMENTATION; batch3 staged result is not proof.

A8 Storage compatibility constraint (3): S01 reproduced legacy credential startup rewrite under unchanged V3 — REMAINING-IMPLEMENTATION. S02 exact UTF-16 surrogate design without silent JSON encoding switch or historical \u reinterpretation — REMAINING-IMPLEMENTATION (design only; no exploitability assessment). S03 rerun unchanged checker after deadline failure — REMAINING-IMPLEMENTATION (extended-deadline run covers prior staged snapshot).

A9 Advisory identities and standalone containers (2): A01 requalify after typed Unicode source integration, F13/F15 still open — EARLIER-EVIDENCE (advisory-container-acceptance-evidence.md for F2/F3/F4; Source.StatementSpans for F5/F6). A02 119 fresh checker rows from checker-batch3-findings.json — REMAINING-IMPLEMENTATION/EARLIER-EVIDENCE; that file was NOT supplied, so row identity is UNKNOWN.

A10 F12 native alias identity (1): F12-1 requalify after Unicode source/AST integration — EARLIER-EVIDENCE (f12-parent-alias-identity-evidence.md, alias race 3.990s).

A11 Unicode advisory and Fabric source boundaries (1): U01 imported/scoped real remote source proof after CALL source-coordinate correction + measured cold/warm policy/transport comparison — EARLIER-EVIDENCE partly superseded (fabric-raw-emission-call-fixed, call-source-coordinate-repeat, server 9.509s); measured comparison REMAINING-IMPLEMENTATION (deferred benchmark).

A12 Private security advisory preparation (3): SEC1 released-tag evidence/affected config per v1.4.4 candidate — REMAINING-IMPLEMENTATION (partial: v1.4.2/v1.4.3 files verified identical; private draft GHSA-g7f5-ggj6-ghqj submitted with maintainer-assumed <1.4.4 range). SEC2 review documentation-only credential-persistence proposal — CONFIRMED-CURRENT (273.107s review). SEC3 submit verified findings as private draft advisories, record links/unresolved fields, no publication or CVE claim — REMAINING-IMPLEMENTATION. Sensitive details are not reproduced here; no new exploitability analysis.

A13 Combined later-edits checkpoint (4): K01 finish/review bounded DeepSeek mechanical changes incl. incomplete knowledge-policy test-family output — REMAINING-IMPLEMENTATION; worker completion is not verification. K02 fold reviewed edits into one staged candidate + enabled checker on refreshed graph (189-row batch4 covers older 6807b9b) — REMAINING-IMPLEMENTATION. K03 one frozen uncached GOFLAGS=-count=1 make test — REMAINING-IMPLEMENTATION; narrower/canceled/source-drifting runs do not satisfy. K04 commit/push verified candidate — CONFIRMED-CURRENT as gate definition only.

A14 Rendering and current integrated convergence (4): R01 canonical identifier fold + actual public HTTP/auth qualification — CONFIRMED-CURRENT named scope (44 reviewed callers; identity-http-integrated-after2 race 15.204s); performance/frozen gate REMAINING-IMPLEMENTATION. R02 source-stream/ANTLR + remaining pattern/property-owner corrections, inventories after freeze — EARLIER-EVIDENCE (stream/source race 13.434s; Cypher policy/quoted 5.658s). R03 user-record mapping vs exact previous codec incl. malformed stored values and allocation/timing — REMAINING-IMPLEMENTATION, adoption decision parent-owned. R04 fold later edits + fresh enabled checker + uncached make test + push — REMAINING-IMPLEMENTATION.

A15 Current public-boundary and parser convergence proof (3): P01 LongASCII Lex 5.63–8.78% regression; lookahead prototype installed but UNVERIFIED — REMAINING-IMPLEMENTATION. P02 staged numeric/relationship canonical-owner wave + private user-record consolidation prototype — REMAINING-IMPLEMENTATION. P03 refresh inventories after freeze, one candidate, unchanged enabled checker, uncached make test, push; declaration retirement counts are not a checker pass — REMAINING-IMPLEMENTATION.

## B. Deferred performance qualification (benchmarks deferred until push)
B1 T04 lookup workloads vs immutable baseline — REMAINING-IMPLEMENTATION.
B2 LongASCII Lex regression + unverified lookahead prototype (P01) — REMAINING-IMPLEMENTATION.
B3 imported/scoped cold/warm policy/transport measured comparison (U01) — REMAINING-IMPLEMENTATION.
B4 F18 cold imported allocation tradeoff disposition — REMAINING-IMPLEMENTATION (parent decision).
B5 user-record mapping allocation/timing vs previous codec (R03) — REMAINING-IMPLEMENTATION.
B6 SHOW/MCP baseline-vs-current product and allocation comparison — EARLIER-EVIDENCE (controls agree; final measured comparison pending; render-actual-consumer-race).
B7 render/inventory allocation populations retained within noise — CONFIRMED-CURRENT (render-public-abba-summary.json).

## C. Remaining convergence audit items
C1 duplicate-findings.md tracker: 780 functions, four moves, 67 findings, exit 1, every checkbox
still open, no finding waived, "checkboxes remain pending until a fresh enabled check confirms
convergence". Focused passes noted but not converged: DF-02 populationReceiver/admissionTypeName,
DF-04 requireNoSyntheticProviderCredentials/assertNoSyntheticCredential, DF-17 kpScanInt/kpScanNumber.
All 67 identifiers must be carried into the follow-up audit list verbatim; earlier inventories
(641 functions/64 findings; 847 functions/63 candidates; 780 functions/67 findings 459.1s) are
historical only.

C2 checker-batch5-findings.json: 210 rows = 164 reuse candidates + 46 refactor targets.
Disposition artifact batch5-declaration-disposition-readonly.json: baseline tree = source index
2ddf47c34ee19b9a796f49de3130ff21f3f27db5, baseline_errors empty, scope note "Declaration-body
delta only; changed/retired is NOT a resolved finding or behavioral proof".
retired-at-finding-file 106 (60 reuse, 46 refactor) — not resolved, not a pass.
unchanged-live 94 — open convergence review.
changed-live-needs-semantic-review 9 — open, highest priority: indices 103 .LA()
(pkg/cypher/antlr/source_charstream.go), 119 TestResultHeaderUseMatrix(), 150 .GenerateAPIToken()
(pkg/auth/auth.go), 157 .GetText(), 169 Open() (pkg/nornicdb/db.go), 170 .Render()
(pkg/localization/manager.go), 177 .OriginalText(), 179 sourceInputStream(), 196 addStretchMapKeys().
non-Go-requires-language-owner 1 — index 210 splitCypherStatements() (ui/src/utils/cypherStatements.ts).
Package grouping (all 210 rows retained): pkg/cypher 123; pkg/storage 27; pkg/server 24;
pkg/fabric 7; pkg/bolt 5; pkg/qdrantgrpc 4; pkg/auth 3; pkg/nornicdb 3; testing/cypher 2; pkg/mcp 2;
cmd/nornicdb 2; pkg/graphql 2; pkg/adminimport 2; apoc/cypher 1; pkg/observability 1;
pkg/localization 1; ui/src 1.
Dominant families: knowledge-policy bootstrap/DDL test families, credential/display owners, ANTLR
source-chain readers, server handler families (retention/auth/admin), transaction/WAL readers.

## D. Superseded-but-open boxes (continuity, not closure)
Unknown-DROP internal assertion (L01), CLI R2 ALTER/catalog (L03) and the HTTP500 CALL observation
(U01) are superseded by later evidence but still require a frozen-source rerun before closure.

## E. Conflict and drift flags
1. Checker counts differ by generation (64 / 63 / 67 / 210). Treat duplicate-findings.md (67) and
checker-batch5-findings.json (210) as two independent open inventories unless the parent proves
supersession; this draft does not assume it.
2. checker-batch3-findings.json (119 rows) and batch4 (189 rows) are referenced but unsupplied;
row-level content UNKNOWN.
3. Batch5 disposition tree (2ddf47c…) is neither the release baseline d663ce8f1 nor the frozen
follow-up source; all dispositions require re-derivation after the follow-up push.
4. Checker is temporarily locally disabled; any checker-pass statement is unsupported.
5. Security items are tracking-only; no new exploitability analysis and no sensitive detail here.

## F. Not claimed
No all-AC completion, no merge-readiness, no checker pass, no benchmark qualification. No remote
mutation, Linear write, commit, push, re-ingest, test or benchmark was executed. Source treated
as frozen during the running make test; no behavioural re-verification beyond the four artifacts.
