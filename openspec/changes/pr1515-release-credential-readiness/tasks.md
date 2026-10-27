# PR #1515 / NOR-6 — release/v1.4.4

Base: d663ce8f1320e409e2eaa29560b987ffe6526207 (merged #1503).
Original author head: 94246dd12d17b13a62bbed5c753209faa1bdd835.
Maintainer explicitly approved this PR despite the earlier size cutoff.

- [x] Fetch merged release and start immutable-checkout remote graph refresh.
- [x] Select oldest Urgent release PR and assign NOR-6 / public #1513 to Tim.
- [x] Preserve original author ref in an external Git bundle.
- [x] Complete graph refresh and independently verify indexed revision.
- [ ] Integrate credential patch with current metadata DROP/recovery and quota owners.
- [ ] Reconcile all acceptance criteria and review feedback; retain red evidence.
- [ ] Verify required persistence/runtime/authentication workflows and fix failures.
- [ ] Profile and compare affected lookup workloads against immutable release baseline.
- [ ] Run enabled duplicate checker against refreshed graph; resolve all findings.
- [ ] Run uncached make test; fix every failure including baseline regressions.
- [ ] Publish merge-ready PR and update Linear with evidence and limitations.

Evidence: /Users/timothysweet/.local/share/nornicdb-pr-readiness/post1503/pr1515.
Graph refresh runs separately in post1503/graphify-release. Original patch applied
without committing; metadata integration preserves pending_drops and the current
limits-change persistence contract. Inventory merge is provisional pending its
structural test and source audit. No new verification pass is claimed yet.

Current evidence: release-base credential persistence red reproduces four URI/both
CREATE/ADD failures. Integrated default and ANTLR public real-server credential
workflows pass (60.5s/68.6s). Lexer/Fabric/txsession complete races and Cypher USE
owner focused properties pass. Initial integration exposed two obsolete fixture
initializers (corrected); the next multidb race run passed assertions but failed
the process store boundary with 22 open stores. Complete fixture-family ownership
migration is now delegated to DeepSeek, using existing internal/testfixture owners.
The store boundary is not waived or modified. The remote Qwen narrow edit was
canceled after 8 minutes with no edits and stalled stream; it supplies no proof.

Graph refresh completed (tool exit 0; 811.6s importer). Independent CodeRepository
name/last_commit query confirms d663ce8f1, 62,333 nodes and 2,175,827 edges.
Raw importer updated 672 existing nodes and removed 65 stale nodes/312 stale edges.
The known SIMD header parse warning remains; detached branch label is empty.
The initial enabled rebase checker is running against a staged snapshot; fixture
ownership edits and its results must be integrated and checked again afterward.

Initial refreshed-graph checker result: FAIL, 641 functions analyzed, 64 reuse
findings. Complete report retained in evidence/checker-initial-findings.md
(relative to the external evidence directory). All findings require resolution;
none are waived. Grouped work covers lexical/scanner owners, canonical Cypher
execution readers, credential/display/metadata owners, lifecycle and fixtures.
Current snapshot is an in-progress port, not merge-ready.

Security readiness follow-up:
- [x] Run background DeepSeek source security review (396.1s completed).
- [x] Independently verify READ remote alias write bypass and preserve actual
  authenticated HTTP red regression (USE implicit/explicit, direct implicit).
- [ ] Correct the shared access-mode admission owner, including remote procedure
  and dynamic-statement handling; verify public consumers and allowed controls.
- [ ] Complete DeepSeek follow-up review of the corrected critical boundary.

The new vulnerability is privately triaged in the ignored security file; no
public issue/comment contains its details. Direct explicit HTTP transaction
startup is unsupported and is not counted as a reproduced write bypass.
No complete security or merge-readiness pass is claimed.

Active implementation delegation: DeepSeek job
646e9f5af80d4bbba5e714452aeb6a9e owns the read-only alias admission correction
and focused regressions; parent review and integrated verification are required.
DeepSeek job dcf8c6395b4d40eead950c67502a2502 owns the IS label operand
reader consolidation from the initial checker findings. No readiness claim.

Security implementation attempt 646e9f5af80d4bbba5e714452aeb6a9e was canceled:
it truncated authorization.go from 761 to 248 lines. Parent restored the exact
pre-worker integrated file from the preserved three-way snapshot; rejected
content is retained privately. This attempt supplies no fix or verification.
Remote author head has advanced to dad30a7a1789cc716bef53283016bc2ef30feb90.
Both author versions are bundled; 120-file integration preview has 14 conflicts.
Final gates require the newer handover plus retained local corrections.

Handover integration progress: 104 clean three-way changes applied; remaining
conflicts integrated with retained context slots, typed encryption, exact USE
sigil diagnostics and fixture ownership. Six affected package groups compiled
(no tests selected). Later focused reader run fails the existing lowercase-label
cache extraction assertion; failure retained in handover-reader-final.log.
IS operand and quoted-result consolidations completed locally after worker tool
JSON failures (353.2s/485.9s); worker partial quoted source was preserved and
replaced. These changes still require fresh passing integrated proof/checker.
Security replacement job f9c09530883f4508883570821fbe1f9e is patch-only and
running; no security correction has yet been accepted/applied.

Current milestone: READ alias security correction applied after the original
full-suite run finished. DeepSeek narrow selection patch 077191... completed
21.2s; independent review 4c700... completed152.5s. Public actual-source regression
matrix passes2.502s. Known pure-read procedure overlay control passes2.508s.
The first overlay race run exposed an incomplete legacy test catalog fixture
(missing required AccessMode); fixtures now declare read_write and a new
read-only/grant-independent case asserts Forbidden. Actual-source race rerun
is running; this is not yet a race pass.

Refreshed checker: FAIL,780functions,4moves,67findings,459.1s. Fullmake test:
FAIL exit2,832.5s; failures in CLI,cyphertext,Cypher,nornicdb,observability,server,
storage andstoragecensus. Preserve make-test-handover.log andreceipt. No failures
waived. Current security source differs from this full-run snapshot; finalgate
mustrerunafterallcorrections. Failedfreshdiagnostic tests retained separately.

Security correction and follow-up verification: applied source passes
read-alias-source.log (2.502s) and read-alias-integrated-race.log (Server16.608s,
Cypher3.264s,Bolt2.910s). The public test includes actual authenticated remote
storage, no-auth configuration, static/dynamic/direct selection, opaque and
nested-statement remote procedures, denied writes, read and writable controls.
No blanket principal write grant or checker change was added. Documentation
records remote opaque-procedure behavior and unsupported direct remote explicit
transaction startup. Private security triage updated; no public exploit details.

Further corrected failures: stale quote/Unicode-whitespace expectations now
assert canonical accepted/invalid input contracts explicitly; focused three-test
suite passes0.980s without production parser changes. Re-reviewed changed
queryRewrite.restoreValue body (new localized message/error restoration, not an
AST-offset-only change), reconciled numeric opaque-leaf classification hash;
source-restoration/numeric guard passes1.135s. Rejected DeepSeek's suggested
ASCII-only leaf fallback and unneeded string-reader shortcut after direct proof.
Lazy embedder registry now allocates only when an embedder exists; configuration
copy semantics remain asserted. Focused default-config/resolver tests pass0.796s.

Census refresh: remote worker4213d... completed242.9s (focusedcommand22.5s),
preserved secondary-failure-details.log; worker5c4ab... stopped51.4s when its
verifier rejected env-prefixed argv. Parent ran existing documented generator:
stored-census-regenerate.log passes8.554s; only one source-position change in
JSON/Markdown, no classification removed. Independent current-census check
runs without update mode in stored-census-current.log. Lexical source minimum
entries migrated to retained canonical owners with an explicit retirement map;
full extracted population delta (25added,42removed,23mechanism changes) is
preserved for review, not silently reclassified or claimed complete.

Remaining: complete lexical/output inventories, all other full-suite failures,
all67refreshed checker findings, fullAC reconciliation and fresh finalmake test
and enabledchecker. No commit/push or readiness claim.

Local cleanup requested by maintainer: archived and removed17stale worktrees
including canceledT3; primary checkout remains. Pruned missingregistrations and
90proven-merged localbranches (89topic+1historicalrelease), preserving primary,
main/develop,currentrelease and unproven/unmerged branches. AllGitrefs bundled
and allremovedworktrees privatelyarchived beforedeletion. 33tasktmpartifacts
archived/removed;437orphancompiler/checker scratchdirs removed;go clean -cache
-testcache completed (60GBbuildcache ->12KB); modulecache retained. About102GB
reclaimed. Backup/receipts in external readiness cleanup/. The only authorized
remote mutation was closingT3PR1648, completed; no pushes/remotebranchdeletes,
no furtherLinearedits orPR1515metadatachanges permitted during this cleanup.

Continuing all67checkerfindings: DeepSeek20ea80... returned receiver-reader
proposal54.0s. Parentrejected inaccessible_test.goexport andpointeridentity
regression; implemented one internaltestfixture ReceiverName with explicit
pointer-vsbasename policy, removedboth oldreaders, migratedconsumers andadded
boundarytests. Coldcache focusedverification runs receiver-owner-convergence.
DeepSeek syntheticcredentialassertion refactor proposal running (textonly).

Post-cleanup consolidation batch: receiver-owner-convergence PASStestfixture0.439s,
Cypher1.041s; credential-assertion-convergence PASStestfixture0.453s,Server2.049s,
Heimdall0.456s. DeepSeek credential assertion job853999... ended50.99s with
onlyanunfinishedsentence; parentcompletedsharedmarkerassertionandwithheld-input
failuretest. LocalQwen3ccf5... returnedno-ciphertable371.2s; reviewed3cases and
localizedIDs retained, draft .new/.bak artifacts archivedoutsidecheckout.
remote-credential-test-convergence PASSmultidbrace2.646s. kp-decimal-owner
PASSCypher1.205s (int64 extrema/2^53+1 preserved, integer-only rejection kept).
case-predecessor-owner PASS6.952s; case-role-owner PASS6.766s afterconvergingthe
CASE/NOT/DISTINCTcontinuationreaders. unwind-keyword-owner PASS2.962s usingthe
existingpipelineclauseowner. identifier-reference-owner initialcompilefailed
onaquotedtestinputcallrewrite; correctedandrerancompleteaffectedselection:
cyphertext0.240s,Cypher3.009s,Fabric1.275s,multidb0.583sPASS. identifier-validation
initialcompilecaughtmissingimportinsingle-importfile; auditedaffectedimports,
correctedcheckPASSCypher36.444s,Fabric0.650s. No assertionsweakenedorskips.
RemoteQwen596ab... splitterdraftleftemptyretiredfunctionsandmissedtestconsumers;
rejectedandtaskcanceledassupersededbyparentcorrection. Botholdsplittersremoved,
allcallersdirecttocanonicalsplitTopLevelComma,focusedPASSCypher1.089s. No worker
proposalacceptedwithoutreview. Enabledcheckerbatchrerunpendingonstagedsource.

### Local cleanup and continuing readiness (2026-10-26)
- [x] User approved closing T3 PR #1648 as the sole remote-state exception; PR closed. No other remote changes authorized for this pass.
- [x] Archived and removed 17 stale registered worktrees; primary checkout is the only remaining worktree. Preserved complete ref bundle and dirty source archives before removal.
- [x] Removed 90 local branches with merged-history proof (89 topics, one historical release). Remote branches unchanged.
- [x] Archived/removed 48 identified stale task temp entries; removed 437 orphan build scratch directories; cleaned Go build/test caches. Module cache retained. Verification subsequently repopulates build cache normally.
- [x] Fresh enabled checker batch1: 847 changed functions, 3 moves, 63 reported candidates, exit 1, 436.16 seconds. Full report/receipt preserved; no candidate waived. Earlier 67-finding tracker remains historical, not a fresh pass.
- [x] Canonical physical participant identity fix: same-owner alias/root admission and retained reads share ParticipantKeyFromLocation. Public fail-first tests reproduced both wrong second-writer rejections; corrected source passes new tests, R8 and existing composite transaction checks.
- [x] Consolidated negative integer/decimal tests and identifier cases; retired isSimpleIdentifier wrapper and migrated all callers. Focused tests and full Fabric package passed uncached.
- [x] DeepSeek security investigation 9245f4df436d4991ba21dbe190235340 completed in 151.07s. Proposed structural trust of original fallback is unsafe and rejected; no proposed source adopted.
- [x] Parent fail-first tests reproduced lost static diagnostic text and unmasked nested diagnostic data/PluralCount. Canonical projection now masks structured data and reconstructs parameter-free diagnostics through the embedded trusted catalog, without trusting original fallback. Focused diagnostic/catalog/Cypher command checks pass (diagnostic-catalog-integrated.log).
- [ ] Unknown DROP regression still fails internal SemanticError assertion despite public SyntaxError classification; inspect original/public contract before any correction. No waiver or skipped test.
- [ ] Complete reviewed lexical/URL sink inventories, remaining duplicate convergence, enabled checker rerun, and final integrated uncached make test. Current source is not merge-ready.

- [x] Unknown DROP contract reconciled: assert stable public BoltErrorCode SyntaxError rather than an internal SemanticError implementation; retain exact rejection and strengthen unchanged-data proof (full result rows, retained property and total graph count). Unknown DROP and missing schema-object classification focused checks pass (unknown-drop-public-contract.log).
- [x] DeepSeek independent read-only diagnostic review 07bdf79db0164c36a02c5ae387488fb1 completed in 9.884s: no confirmed disclosure in supplied current-source ranges; original Unwrap remains deliberately reachable for identity, not emitted by reviewed callers. Bounded code-reading evidence only.
- [ ] CLI R2 uncached current-source rerun still fails ALTER ADD and catalog output (cli-r2-catalog.log, 39.046s); isolated public-wire investigation delegated. No skips or waiver.
- [ ] URL sink inventory current-source rerun fails stale and unreviewed ownership entries (url-sink-current-review.log). Review and resolve individually; no blanket hash refresh.

- [x] Reproduced release-callback test contention failure with 1,000 repetitions. Corrected test scheduling: fresh serial owners prove unlocked reentry; separate fresh concurrent owners prove exactly-once release. No production assertions removed; 1,000 corrected repetitions pass (release-owner-contention-repro.log / release-owner-deterministic.log).
- [x] Frozen lexical population reconciled with unchanged extractor/assertions: 3,529 candidates, nine retained review-minimum declarations, 50 retired identities removed only after source review, 28 additions classified. Fresh census checks and 50 bounded behavior tests across six packages pass; repeat after later source edits.
- [ ] Public native ALTER classifier correction advances CLI R2 past ADD/CREATE/SHOW; actual full CLI regression still fails at EXPLAIN, so readiness remains blocked and investigation continues. No passing full-suite claim.

### Composite command admission and EXPLAIN correction

- [x] Preserve fail-first public native ALTER remote ADD rejection and EXPLAIN remote CREATE rejection (`alter-admission-before.log`, `alter-explain-before.log`).
- [x] Restore supported ALTER COMPOSITE DATABASE to the existing non-graph command classifier.
- [x] Move CREATE, ALTER ADD and ALTER DROP parsing into typed preparation shared by execution and the metadata compiler; preserve source credential recording, declared result schemas and existing IF NOT EXISTS no-op behavior. Only execution mutates the manager.
- [x] Verify both public parser configurations, exact command/EXPLAIN columns, EXPLAIN zero effects, malformed-command rejection and credential nondisclosure in focused tests (`composite-preparation-public3.log`).
- [x] Complete refreshed public CLI/HTTP R2 checks in native and ANTLR environments (`cli-r2-preparation-after.log`, `cli-r2-preparation-antlr-after.log`); every HTTP/Bolt URI-form case and strict-parser restart checks pass.
- [ ] Complete final integrated `make test`; focused evidence does not establish the final gate.

Evidence directory: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1503/pr1515`.
The first moved-preparation run still failed EXPLAIN because its graph-composition reader ran before typed metadata; the correction keeps native catalog syntax in its command preparation owner. `composite-preparation-public2.log` is an incomplete run caused by concurrent sanitizer source integration; the following integrated run passed and no blocker was waived.

### Current integrated correction round

- [x] Preserve failed full-suite round2: `make-test-round2.log` (exit 2, 874.64s). Three failures: typed import rendering expectation, procedure-privilege fixture, generated census. This is not a passing final gate.
- [x] Restore strict original bare parameter rendering expectation after independently confirming both forms on the reference server; add generated-query public execution proof. Correct procedure-privilege manager fixture without changing production authorization. Focused corrected checks pass (`typed-import-public-contract.log`).
- [x] Consolidate native/ANTLR syntax admission through the typed validation owner; focused/race and alternating diagnostic benchmark evidence saved.
- [x] Retire CREATE/ALTER composite execution wrappers; canonical execution family owner passes focused/race and native/ANTLR CLI wire workflows (`composite-execution-convergence-evidence.md`).
- [x] F27: reproduce credential disclosure through all five real public periodic APIs, then redact output through the existing canonical text owner. Repeated subprocess/virtual-time and race checks pass (`periodic-redaction-evidence.md`). No query-execution claim is made for these stdout-only APIs.
- [x] F23: reproduce actual Bolt RUN contextual parameter and nested credential disclosure (`bolt-parameter-context-before.log`); execution input controls pass. Integrated correction and focused revalidation in progress.
- [x] User approves F25 context-first Go catalog mutators and receipt-capable storage contract migration. Preserve 55-case prototype red evidence (43 failures, 12 controls).
- [ ] Implement F25 detached proposals and terminal-receipt-owned publication, migrate original contexts through all public consumers, and prove pending DROP recovery plus offline reopen.
- [ ] Finish F26 decoded identity correction with DeepSeek. First worker stopped on a harness JSON decoding error, not a completed correction; resumed job preserves existing work.
- [x] Reverify iteration declaration and correlation convergence after catalog API composition builds: focused Cypher/Fabric regressions pass (`scanner-correlation-iteration-integrated.log`). Initial attempt was an incomplete concurrent-migration compilation; no failure waived.
- [ ] Reconcile all remaining duplicate findings and refresh reviewed inventories on frozen integrated source, then rerun enabled checker and uncached `make test`.
- [ ] Prepare final local review evidence. Current restriction permits no remote mutations beyond the already completed T3 PR closure; no new push, merge, Linear update or notification is claimed.

### F25 catalog acceptance and publication

- [x] Preserve 55-case local native fail-first evidence across all 11 catalog mutation owners, physical rejection/cancellation, applied acknowledgement failure, recovery-pending terminal outcomes, runtime routing, cache scopes and actual reopen (`f25-catalog-publication-final-red.log`).
- [x] Migrate existing APIs/callers to original context-first operations and explicit process-lifetime startup; writable catalog storage requires authoritative entity preparation/receipts. No compatibility wrappers or direct-write fallback.
- [x] Detach candidate validation/normalization/identity/serialization from live catalog state. Serialize through canonical receipt settlement; rejected/discarded proposals remain private, applied acknowledgement failures publish accepted effects, and waiter cancellation retains accepted authority.
- [x] Preserve canonical pending-DROP intent/purge/completion, limits/rate identity, server identities, cache-index notification and managed runtime owners. Fix offline reopen registration without admitting offline storage.
- [x] Pass original 55 cases plus accepted-wait cancellation and close/cache controls; pass the full multidb package (`f25-multidb-full-final.log`).
- [x] Pass final-source full multidb package and focused receipt/concurrency/migration/credential race checks (`f25-multidb-final-source.log`, `f25-catalog-final-source-race.log`).
- [ ] Finish final integrated `make test`; package and focused race evidence do not substitute for it.

Existing migration assertions now compare every exported Node/Edge field (future fields included), timestamps by instant, and explicitly check native hydrated embedding representation. Private transient native engine handles are not persisted entity data. No pre-existing payload checks were dropped. The first experimental close harness blocked and two runs were captured/canceled as incomplete; they are not passing evidence. Rejected F26 worker source and its admission-regression run were preserved before restoring the approved admission contracts.

- [x] F26: reject/cancel the worker's exact-first alias policy and admission removal; archive rejected source and restore original admission. Parent real-server fail-first reproduces wrong registration dispatch, then narrow decoded-byte preservation passes HTTP/USE/explicit composite and actual driver folded/spaced ownership controls. Focused race checks pass; see `f26-identity-correction-evidence.md`.
- [x] F23 structured parameter owner and opaque-password preparation projection pass public RUN and focused race controls. A newly introduced cyclic-map regression was independently reproduced and corrected through the same traversal; shared DAGs and execution values are preserved.
- [x] F27 expanded explicit PASSWORD case reproduced after URL-only correction. Canonical quote/comment-aware source reader plus one output owner now preserves ordinary and quoted-word controls; twice-repeated race check passes (`periodic-opaque-password-after2.log`). The earlier positional-boundary attempt failed endpoint context and remains recorded, not waived.
- [x] Catalog API caller migration: all 182 packages compile; original-context public tests and existing HTTP/Bolt/GraphQL/Qdrant/Cypher behavior checks pass. Full multidb package and original 55 fault cases pass at the named source.
- [x] F25 protocol review found concurrent DROP/recreate and final Sync gaps not qualified by the original 55 cases. Preserve deterministic red boundary evidence; serialize physical DROP invocations and keep Applied publication/Sync/fence settlement under the authoritative receipt. Both create APIs reject absent-record pending names; retry/reopen clears that fence without purge. Current full package and focused race pass (`f25-drop-final-package.log`, `f25-drop-final-race.log`).
- [x] Prove accepted durable DROP completion racing actual native Close: callback Sync completes without a native-close cycle, acknowledgement errors retain Applied state, failed Sync retains the pending fence, and closed cache/checker resources stay closed. Preserve CDC post-close fail-first evidence (`f25-drop-close-native-red.log`); accepted publication now skips CDC activation after the manager lifetime closes.
- [x] Retire redundant map-tail and delimiter-start/forwarding scanners into canonical gap and typed delimiter-anchor owners. Public/parser/race proofs and named workload profiles retained; allocation counts unchanged, measured timing within observed noise.
- [ ] Source-restoration duplicate convergence in progress; preserve source coordinates, typed diagnostic opacity, stage order and exception identity. Frozen inventories, checker and final uncached `make test` remain pending.

### F18 required remote access mode

- [x] Preserve actual public Bolt driver/session wrong-mode red (tabs/comments SET/REMOVE, quoted CREATE/SET), reject the automatic full-grammar transport variant's cold regression, retain profiles.
- [x] Implement user-approved required RemoteAccessMode Go argument; migrate all interfaces/callers/fakes/docs. Retire automatic storage inference. Generated storage operations supply declared intent; explicit/batch lifetime and result-derived write participation remain authoritative.
- [x] Reuse cached canonical structural effects for exact post-import Fabric source and live local registry policy. Public compiler→Fabric→real transport and driver routing/invalid/canceled controls pass; all repository packages compile and focused integrated race passes (`f18-explicit-mode-evidence.md`).
- [x] Record standalone cold/warm balanced binary pairs and complete compiler/transport pipeline profiles. Required mode removes standalone inference cost; complete cold imported source adds about8.3%bytes/4.5%allocations, explicitly reported to parent for review.
- [ ] Parent disposition of cold imported allocation tradeoff; final frozen-source inventories/checker and uncached make test. Existing external Neo4j null-parameter reference test remains unexecuted without its required reference endpoint; no newly added skip or passing claim.

### Complete admitted-registration snapshots

- [x] Preserve real native CREATE/reopen/remove-winner fail-first evidence: current liveRaw2/storedRaw1 causes different removal results after reopen. No immutable-release behavior invented; baseline source copied raw registrations.
- [x] Give canonical cloneDatabaseInfo explicit full-admitted versus visible-winner purpose; persist/candidate full state, retain public canonical views, remove candidate's manual compensation. No format change or reconstruction of lost records.
- [x] Promote native payload/public selection/unrelated-mutation regression. Focused PASS0.683s, affected race PASS4.860s, full multidb PASS13.582s (`catalog-registration-snapshot-consistency.md`).
- [ ] Reconcile frozen inventories/checker for later UNSTAGED snapshot correction and finish final integrated gate. Batch3 checked an earlier staged snapshot and is not proof for this correction.

### Storage compatibility constraint

- [x] Record operator clarification: additive new keys are allowed; preserve existing keys and payload interpretation. Avoid migration; if unavoidable, use one storage version bump and one shared release migration arm with old-database and interrupted-upgrade proof.
- [ ] Resolve reproduced legacy credential startup rewrite under unchanged V3 before readiness.
- [ ] Complete exact UTF-16 surrogate design without silently switching JSON encoding or reinterpreting historical literal backslash-u names.
- [ ] Rerun unchanged duplicate checker after deadline failure; extended supported deadline run is in progress on the prior staged snapshot, not final-source proof.

### Static storage census diagnostics

- [x] Preserve actual static-call mutant nil-Method panic and subsequent real transitionCatalog finding. Reuse one canonical visibleReadTarget owner for classification and diagnostic naming; no static-call omission.
- [x] Move only the existing retired persistMetadata system-metadata annotation to actual transitionCatalog after source review. Non-system static and existing invoke mutants both retain their violations (PASS26.118s).
- [x] Regenerate canonical storage census, inspect four source-position-only changes, pass no-update current/ordered checks8.922s; native/public catalog checks pass. Full integrated gate remains pending.

### Advisory identities and standalone containers

- [x] Preserve public ASTBuilder red and fix F2/F3/F4 decoded labels, executable UNWIND AS, expression identities and string escapes using existing canonical owners; public-path proof retained in advisory-container-acceptance-evidence.md.
- [x] Preserve F5/F6 standalone framing red (4 instead of2), migrate both adapters to Source.StatementSpans, and pass full affected packages under race. No execution/mutation claim for standalone placeholders.
- [ ] Requalify these paths after typed Unicode source integration; F13/F15 remain open.
- [ ] Resolve119 fresh checker reuse/refactor rows, inventory structured list checker-batch3-findings.json; no bypass or readiness claim.

### Single catalog V3→V4 native integration

- [x] Add exact injected CatalogUpgradeV4 plan and atomic canonical-codec catalog body/version transaction under existing maintenance lease; no scans/new runner/normal-write reentry. Current target is V4 and bootstrap supplies the exact configured catalog plan before native open.
- [x] Real native old-V3/reopen/unknown-fields/unrelated-KV/atomic visibility/cancellation/rejection/missing/noop/idempotence/growth controls pass; focused old-runner/schema controls plus new arm race PASS3.453s (`catalog-upgrade-v4-native-evidence.md`).
- [x] Integrate startup-only catalog ownership, one global V4 bump, fresh/no-plan contract and canonical cryptographic transformer. Genuine child-process interruption before commit preserves the complete logical V3 fixture; abrupt exit after public open preserves V4 plus decryptable descriptor (`catalog-v4-process-interruption.log`). This is a constructed ASCII V3 fixture and process-crash proof, not old-binary or power-loss qualification. Final integrated gates remain pending.

- [x] Keep one private typed migration phase: constructor opening vs public manual. Future-V4-target fail-first proves manual invocation used to mutate V2→V3; corrected guard rejects before any old arm and leaves all KV unchanged. Future-overlay PASS0.839s; currentV3/native/oldarm/phase race PASS3.442s. Current target is V4; startup integration is enabled. Manual older-store migration remains rejected before any arm to protect cached catalog ownership.

### F12 native alias identity

- [x] Replace rejected worker proposal (including unauthorized Skip) with parent native public CREATE/DROP+reopen fail-first test; preserve exact admitted names/decoy and whitespace rejection. Alias race controls PASS3.990s; f12-parent-alias-identity-evidence.md.
- [ ] Requalify after Unicode source/AST integration; freshchecker stillpending.
- [x] Stable-key origin/precedence tests pass under race1.402s; same two-process public restart proof passes on fingerprinted source with no ephemeral credential rewrite. V4 missing/wrong-key denial and public descriptor integration have focused proof; final combined release qualification remains pending.

### Unicode advisory and Fabric source boundaries

- [x] Advisory ASTBuilder projects raw ingress once and carries immutable Source windows through actual helpers; original RawQuery/clause byte spans/nested expression/pattern/action spelling and accepted semantic identities have fail-first/public/race proof. Optional canonical MERGE span output restores original runtime string product; saved-binary parser timings within noise and original allocation populations retained. `N/ast-source-view-evidence.md`; focused current race10.170s, full combined final gate pending.
- [x] Fabric selector/planner APIs carry original contexts and explicit Raw/Accepted stage (public aliases); canonical Source-view USE owner preserves original Args/End/rest plus distinct accepted grammar cursor. Existing generated property/oracle/negative controls unchanged; focused fabric0.824s/Cypher8.455s and full Fabric race7.503s. `N/fabric-source-stage-evidence.md`.
- [x] Preserve actual HTTP coordinator→real NornicDB remote fail-first: generated literal backslash-u was interpreted twice; direct boundary correction emits through canonical EmitRawSource before exact raw-source policy/transport. Direct remote executor raw-input contract retained.
- [ ] Complete imported/scoped real remote source proof after parent CALL source-coordinate correction; current public tests preserve HTTP500 rather than masking it. Requalify after ongoing mixed-stage API migration and complete measured cold/warm policy/transport comparison. No full make-test/checker readiness claim.

- [x] Correct CALL body slices to use the original source matching their byte spans. Raw-source fixtures now use Go raw literals so the compiler cannot consume the Unicode escape under test. Focused CALL reader race and actual HTTP direct/imported/scoped remote source tests pass (`call-source-coordinate-repeat`, `fabric-raw-emission-call-fixed`, server9.509s). This supersedes the HTTP500 observation above; frozen full-suite/checker qualification remains open.

### Private security advisory preparation

- [x] Verify the live GitHub repository advisory inventory:14 published records, no existing drafts; compare historical disclosures before creating duplicates.
- [ ] Establish released-tag evidence and affected configuration for each v1.4.4 candidate; distinguish defects introduced in the unmerged PR from released vulnerabilities.
- [ ] Review DeepSeek's documentation-only credential-persistence proposal against independent source and reproduction evidence.
- [ ] Submit verified findings as private GitHub draft advisories for maintainer review; record their private links and unresolved fields. Do not publish or claim a CVE has been assigned.

- [x] Review completed DeepSeek credential-persistence proposal (273.107s); independently verify relevant tagged v1.4.2/v1.4.3 files are identical. Submitted and re-read private draft GHSA-g7f5-ggj6-ghqj with maintainer-assumed `< 1.4.4` range, released-source evidence limits, no severity/CVE assignment and no released-patch claim. Broader candidate inventory and final release proof remain pending.

### Combined later-edits checkpoint

- [x] Review DeepSeek's private pipeline projection API consolidation: retire the materialized-row wrapper, retain one five-argument owner, migrate all four production and eighteen test call sites without changing query/assertion populations. Runtime requalification remains pending.
- [x] Preserve F15 oracle failures in the actual differential package and correct opaque-source classification plus exact JSON code-unit comparison. Full affected package race passes1.333s (`f15-oracle-source-after-second`); this is not a fresh external Neo4j differential run.
- [x] Preserve the HTTP terminal-commit failure: Fabric could successfully commit and then frame its response using the expired request context. Retain the admitted commit context for post-commit framing; unchanged eight-case public HTTP race proof passes35.997s (`combined-current-boundaries`). The combined Cypher/server command still fails the separate stored promotion-expression spelling assertion; no full-gate pass is claimed.
- [ ] Finish and review bounded DeepSeek mechanical changes against the original populations, including incomplete knowledge-policy test-family output; do not drop assertions or accept worker completion as verification.
- [ ] Fold every reviewed later working-tree edit into one staged candidate and run the enabled checker against the refreshed release code graph. The189-row batch4 result covers the older6807b9b snapshot, not this combined candidate; every remaining finding stays open.
- [ ] Finish runtime and structural failures, then run one frozen uncached `GOFLAGS=-count=1 make test`. Canceled, failed, source-drifting and narrower runs do not satisfy this gate.
- [ ] Commit and push the verified candidate to PR1515 for maintainer review, with exact source/command results and remaining practical limits. No merge or advisory publication is included.

### Rendering and current integrated convergence

- [x] Obtain maintainer approval for typed Go `RenderRequest`; retire `LocalizedError.Render` and migrate `Manager.Render`, `ProcedureMetadataRenderer`, Go consumers and documentation.
- [x] Preserve actual public renderer behavior with current localization/errors/Cypher/ANTLR/MCP race checks: all five packages pass (`render-actual-consumer-race`). Actual SHOW and MCP baseline/current product and allocation controls agree; final measured comparison remains pending.
- [x] Requalify the native shared JWT issuer with the full uncached auth race suite (`auth-issuer-current-full-race`,23.988s). Reject worker source resets and restore both the exact source and checkpoint index; no rejected worker patch is included.
- [x] Requalify stored knowledge-policy raw spelling and quoted grammar boundaries on the actual native reopen/evaluation path, preserving both parser populations and original assertions.
- [x] Establish genuine public UTF-16 case-fold reds: native aliases, manager connection-limit selection, Fabric catalog resolution, function registration/evaluation, and role/procedure grants. Record current-WIP security triage privately; do not attribute this integrated defect to a released version without evidence.
- [ ] Complete canonical identifier fold and actual public HTTP/auth qualification, retaining distinct lower-key and simple-fold semantics without storage-layout changes.
- [ ] Qualify the source-stream/ANTLR and remaining pattern/property-owner corrections, and refresh reviewed lexical/census inventories only after source freeze.
- [ ] Compare the proposed user-record mapping against the exact previous codec, including malformed stored values and workload-specific allocation/timing evidence, before adoption.
- [ ] Fold all later reviewed edits into the final candidate; obtain a fresh enabled duplicate-checker result and uncached full `make test`, then commit and push for review.

- [x] Integrate the exact-name canonical correction through all44 reviewed lower-key/simple-fold callers and named glob traversal; current core/manager/auth/Fabric/function race cases pass. Both parser modes also pass unchanged actual authenticated coordinator→remote isolation tests under race15.204s (`identity-http-integrated-after2`). Preserve earlier build failures separately. Performance and frozen full-gate qualification remain open.

### Current public-boundary and parser convergence proof

- [x] Consolidate GPU, backup/restore, retention and embedding-failure HTTP handlers through required typed operations, retiring overlapping private entry points. Actual route/admission/lifecycle and existing Rule73 tests pass under race131.543s; backup now requires a real isolated artifact. Evidence: server-handler-family-convergence-evidence.md.
- [x] Preserve genuine F15 identifier-role red: `n.limit`/map `limit` could waive wrong values or omit write-state checks. One independent lexical role owner now distinguishes names, clauses and child query scopes across comparison, shape and rerun consumers. Original coarse collect-list contract/assertions stay unchanged. Full affected package race passes1.491s (`f15-clause-roles-current-race2`); this is not external Neo4j qualification.
- [x] Consolidate property-key projection into the canonical map grammar with required comment-admission policy; prove the actual CST comment-free boundary and public Execute path. Reject cached-plan experiment after29–38% cold regression. Final aligned warm/cold/public comparison is neutral or faster and removes one allocation per key-projection workload; final-map-comparison.json/final-public-comparison.json.
- [x] Migrate stream/source APIs and consumers coherently, including validity-aware OriginalText and policy ErrInvalidData preservation. Core/ANTLR race13.434s and actual Cypher policy/quoted consumer race5.658s pass. Third-party ANTLR remains unchanged.
- [ ] Resolve measured LongASCII Lex5.63–8.78% regression before parser performance readiness; bounded same-owner cold-Unicode/directASCII lookahead prototype is installed but unverified.
- [ ] Qualify the staged numeric/relationship canonical-owner wave and current private user-record consolidation prototype before adoption/readiness.
- [x] Recheck live remote graph metadata: CodeRepository.name=orneryd/NornicDB, last_commit=d663ce8f1320e409e2eaa29560b987ffe6526207,62333nodes/2175827edges. This is the exact release baseline; current unmerged edits are intentionally outside that remote graph.
- [ ] Refresh inventories after the final source freeze, fold all reviewed edits into one candidate, run the unchanged enabled duplicate checker and uncached full make test, then commit and push PR1515. Declaration retirement counts are not a checker pass.


### Maintainer-approved checkpoint and follow-up sequence (2026-10-26)

The maintainer authorizes merging this integrated checkpoint after fixing all
observed test regressions and obtaining a fresh uncached `make test` pass.
Benchmarking is deferred until the major refactor is pushed. The duplicate checker
is temporarily disabled only through local Git configuration; its code, rules and
immutable script are unchanged. This is not a duplicate-check pass or a claim that
all original acceptance criteria are complete.

- [x] Preserve the frozen checkpoint full-suite failure and unchanged source fingerprint: `checkpoint-make-test` failed after 1176.1 seconds; seven packages failed. Preserve focused assertion logs before corrections.
- [x] Correct UNWIND original-expression comment extents without weakening the original assertion; add adjacent-comment, Unicode and line-comment boundary controls. Focused AST/numeric/public UNWIND checks pass.
- [x] Review the numeric helper-name-only hash change and six URL-sink identity/handler manifest migrations; numeric ownership and URL sink checks pass with all dispositions retained.
- [ ] Finish lexical/fixture inventories, storage mutator classification and normal census validation; inspect the server failure with unfiltered diagnostics.
- [ ] Freeze the integrated candidate and obtain an uncached full `make test` pass, fixing every regression.
- [ ] Commit, push and merge PR #1515 into `release/v1.4.4` with exact results and explicit outstanding acceptance criteria.
- [ ] Pull the merged release head and re-ingest that immutable revision into the remote code graph.
- [ ] Open a follow-up release PR carrying **every acceptance criterion from this PR that lacks sufficient current verification**, all remaining convergence findings and deferred performance qualification. Preserve evidence and reciprocally link the PRs; keep incomplete linked issues/Linear acceptance criteria open.

The external `follow-up-handoff-draft.{md,json}` inventories unchecked criteria and
audit rows for reconciliation. It is a draft, not resolution evidence. Earlier
checkboxes or declaration-retirement counts do not establish current qualification.


### Checkpoint regression correction status

- [x] Normal lexical population check passes3,928 entries; retain explicit structural-only classification limits and all remaining convergence findings in the follow-up inventory.
- [x] Regenerate the fixture allocation inventory after reviewing the four added helper/path lines; add the two required TestMain boundaries. Normal fixture inventory and both affected packages pass.
- [x] Normal storage census and manual migration-boundary controls pass. Classify the manual migration entry against its enforced startup-only V4 boundary; storage mutator population check passes.
- [x] Preserve isolated authenticated graph-response failure and private encoder diagnostics. The source-drifting full server diagnostic was stopped with SIGQUIT after excessive resource use; it is incomplete, not a passing package run.
- [ ] Resolve JSON binding compatibility before the final suite. Upstream v0.11.2 corrects the public graph regression and focused server/plugin checks pass29.977s, but three unchanged exact-UTF16 codec test families fail because the decoder now normalizes transient WTF8. No third-party code patch has been applied. A concrete opt-in decoder proposal awaits maintainer direction; do not merge with these failures or claim the dependency upgrade fully qualified.

Updated Linear NOR-6 with the checkpoint/follow-up scope and honest incomplete
gate status (comment f55ececd-06aa-494b-9b01-1b7b0f787089). Preserve the tracked
binary diff, checkpoint index diff and all34 untracked source files in external
checkpoint-regression-recovery evidence. No commit, push, merge or graph refresh
has happened for this checkpoint yet.


### Approved JSON dependency decision

The maintainer responded to the concrete third-party patch proposal with
“whatever is the cleanest architectural decision, highest performance”. The
selected design is the narrow request-local opt-in flag in a maintained upstream
v0.11.2 copy, preserving upstream defaults and the corrected encoder. The earlier
permission hold is resolved. No second first-party binder, storage format change
or additional migration arm is approved or introduced.

- [x] Preserve upstream source/license/tests and clean-file SHA256 inventory; pin the module replacement to the maintained copy under third_party/vendor/go-json.
- [ ] Review DeepSeek's decoder option plumbing and prove upstream defaults, opted-in exact bytes, mixed concurrent calls, stream/long-string and error contracts.
- [ ] Requalify the existing first-party codec and public graph/identity cases, refresh affected inventories and complete the frozen uncached make test gate.

The live PR and issue1513 requirements are preserved in external
live-acceptance-criteria.json. The follow-up inventory must also reconcile the
original whole-declaration/decoded-name populations and all still-unverified
finding families named in the live PR body, not merely unchecked task rows or
the latest210 duplicate-check rows. Historical population counts are not a
current qualification result. Keep original issue1513 requirements open where
current evidence or approved contract reconciliation is incomplete.


### Completed JSON correction qualification

- [x] Parent restored a truncated worker option.go from the exact upstream copy, reviewed the complete14-file decoder/path delta and added independent opt-in tests. No upstream encoder file changed; all265 original files/license/tests are retained. Exact maintenance delta is third_party/go-json-lossless-decoding.patch.
- [x] Complete maintained upstream-module tests pass20.297s (`checkpoint-go-json-full`). Focused default/opt-in, map keys, long strings, single-byte streaming, token/path and concurrent option-isolation race checks pass (`checkpoint-go-json-preserve-race3`). Original upstream key-test assertions are unchanged; only the required explicit default parameter was migrated.
- [x] Unchanged first-party JSON contract cases and actual public graph/remote exact-identity regressions pass under race (`checkpoint-codec-public-race`,47.501s command, server18.304s / codec1.292s). No grammar/escape-validation bypass, storage-layout change, reflection binder or global decoder mode was introduced. Earlier failed/stopped tests remain preserved and are not credited as passes.
- [ ] Complete final-source structural checks and a fresh frozen uncached make test before commit/push/merge. Benchmarks remain deferred; all remaining acceptance criteria and convergence work belong to the linked follow-up PR.


### Direct maintained-fork integration and final candidate

- [x] Correct the embedding contract: local module replacements are not inherited by consumers. A separate public-storage embedding module reproduced the undefined lossless-option build failure, then builds successfully with GOWORK=off and no consumer-side fork replacement after the direct dependency pin.
- [x] Publish the maintained fork and pin github.com/orneryd/go-json at v0.11.3-0.20261027022034-ec39cafc98b3 (ec39cafc98b3fd8e1282468e5d2da6d64362d373). Remove only the verified owned local module copy. Upstream license and original tests remain in the fork; third_party documents the exact maintenance delta and upgrade obligations.
- [x] Preserve the genuine cyclic-map WAL timeout and full-run sample. Fix the fork's cold cycle-detection path to retain the underlying map identity across copied interface slots. Direct/indirect/mixed cyclic maps reject with the existing error; shared acyclic maps remain valid. Full fork suite and focused race checks pass on Go1.26.9. Earlier copy-only 'encoder unchanged' statements are superseded by this necessary encoder correction.
- [x] Retain every original344 user-record fixture assertion. The record reader applies metadata's successful string entries in source order, stopping before a rejected entry through the existing shared TokenReader/typed decoder. Add five focused entry-admission controls; no general compatibility binder or extra dependency mode is added.
- [x] Directly pinned codec/auth/WAL/public graph/identity race checks pass all four packages (checkpoint-pinned-fork-contracts). Structural lexical/fixture/URL/census checks pass normally (checkpoint-direct-fork-structural). The original goccy module remains indirect only through the Ladybug/Arrow benchmark adapter.
- [x] Preserve the full run checkpoint-final-make-test as FAILED/INCOMPLETE: auth fixture failed and storage was stopped with SIGQUIT after confirming its infinite cycle traversal; other packages, including server and all census checks, passed. Pre/post execution-source fingerprints match; this is not a complete passing gate.
- [ ] Finish a fresh frozen uncached make test on the directly pinned candidate, then commit/push/merge the checkpoint and pull/re-ingest the merged release head. Re-enable the locally paused checker after checkpoint push. No benchmark run or fresh checker pass is claimed.

The reviewed lexical manifest keeps all3,928 entries and their semantic data;
compact generated formatting keeps it below2,500 lines. The follow-up PR must
carry every unverified original acceptance criterion, the whole-population and
decoded-name audits, all remaining convergence findings and deferred qualification.

## Release checkpoint verification (2026-10-26 Phoenix)

- [x] Fresh `GOFLAGS=-count=1 make test`: exit 0, 121 package passes, 970.966 seconds; lint-slog and lint-cardinality pass. Source fingerprint before/after: `e991f6f1edc9a91e92cb1b59751c812104acae8e0fbbb86a34f57f15503b0d1d`.
- [x] Four subsequent gofmt spacing corrections have identical Go scanner token streams, including comments and literals; no behavioral source changed after the full run.
- [x] Direct pinned JSON fork passed full dependency tests, focused race/public-boundary tests, and an independent Go embedding consumer build without dependency replace rules. Original 344-case stored-user fixture remains unchanged.
- [ ] Publish and integrate this checkpoint; continue all unverified original acceptance criteria and convergence work in a follow-up release PR.
- [ ] Re-ingest the merged release head and rerun the enabled duplicate checker. The maintainer temporarily authorized bypass for this commit/push; no disabling or hook edits are committed.
- [ ] Complete deferred performance qualification and remaining audit proof. `make verify`, external reference/conformance and benchmarking were not run for this checkpoint; make test is the authorized full local gate.
