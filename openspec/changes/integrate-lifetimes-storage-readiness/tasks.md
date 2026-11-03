# Combined release/v1.4.4 readiness: #1693

The approved dependency order is current #1684 terminal-outcome prerequisite,
#1636 request/service/storage lifetimes, #1693 storage foundations, then #1667
label-index readiness and remaining preparation. Preserve each author's work;
port into current owners rather than restoring obsolete implementations.

## Local continuation after published checkpoint `6b546791`

These are working-tree results, not qualification of a frozen committed head.
The branch remains `fix/node-envelope-foundation-v144`, based on release
`de70c0e8375e9c74800d5ab6d7da5a6505650cb8`. No readiness or publication is claimed.

- [ ] Execute the operator-approved remaining proof plan without further routine approval requests. The parent is completing the canceled capture lane's work directly. Extend the existing initializer capability owner only for a prior, uniquely stored, dominating interface value with source-proven foreign capability provenance; all owned receivers, overwrites and escapes remain rejected. The complete capability controls pass (25.841s), and actual native-WAL admission plus all ten checked callers pass (9.538s). The private inline-field proof checks all actual field reads and copies, sibling/context guard dominance and escape frontiers, with actual-source guard/replay/alias mutants. Its integrated census delta, full model/race qualification and residual 85/114 obligations remain pending; no waiver or merge-readiness claim.

- [x] Apply the operator's clarified PR-scoped contract decision: converge competing legacy nil/context-free paths and required-context paths for the same in-scope lifecycle/storage/request intent onto the canonical required-context owner. This is **not a repository-wide context sweep**; inspect only this PR's affected items, readiness findings and directly coupled consumers. Remove unused nil compatibility there, retaining existing nonnil behavior and explicitly owned independent shutdown/accepted-completion contexts. Establish complete affected direct/interface/callback caller evidence and update tests/docs. Tests alone or hypothetical external legacy callers do not justify retaining competing old paths. Unresolved dispatch is not proof of no production nil use. Requalify integrated corrections; do not install an optional-ingress analyzer workaround to preserve retired behavior.
- [x] Remove and audit obsolete compatibility tests/code within the PR-scoped remote/rollback unit. `defaultCtx`, `remoteReadContext`, `nativeRemoteCount` and the direct Cypher nil-default branches are absent. The combined HTTP/closed-branch test retains its valid query/result/transaction assertions using `t.Context()`; required-context rejection/no-dispatch controls and nonnil lifecycle/cancellation/deadline/value/rollback controls remain. No unrelated tests or census adversarial fixtures changed. Current storage remote controls pass normal/race (1.343s / 3.201s); Cypher rollback/lifecycle controls pass normal/race (1.133s / 3.221s). The attempted subsequent `make test` tool call was interrupted before a running shell was established; it is not gate evidence.
- [x] Converge remote operations onto required-context admission: remove `defaultCtx`, reject nil before dispatch in all 29 `remoteCallContext` callers, the `AllNodes` alias and both read/write `QueryCypher` / `BeginCypherTx` entrances. Per-entry tests assert the required-context error and no remote dispatch. Production context traces cover Cypher statement ownership, Fabric and remote interface/callback forwarding. Preserve nonnil deadlines, cancellation, values and transaction ownership. `go test ./pkg/storage -run '^TestRemote' -count=1 -timeout=10m` passes (1.301s); the same selection with `-race` passes (3.889s). Strict MkDocs build passes (13.51s, existing navigation/link notices), and scoped whitespace checks pass. The original nil-entry test failed before implementation; its transcript result is retained, with no separate RED log. Coupled nonremote failure rollback and fresh integrated gates remain pending.
- [ ] Correct all known failures from the subsequent operator-canceled `make test` run before rerunning with live progress. The run reached Cypher (1050.857s) and storage (835.536s) passes, but seven packages reported failures: internal/cyphertext, internal/testfixture, multidb, nornicdb, observability, search and server. Other results included cached packages; this was not a fresh full-suite pass. The operator confirmed sending SIGTERM, so no deadlock or unexplained termination is claimed.
- [x] Reproduce quota refill/cancellation fixture failing during BEGIN instead of its intended commit admission. Stage the transaction under a healthy context and pass the canceled context only at the tested commit boundary; preserve token/refill/count assertions. Remove the same affected admission owner's test-only nil-to-Background fallback and obsolete nil-compat assertion, retaining the read-only nonblocking contract. Complete transaction-quota controls pass (0.988s); managed/quota/admission race family passes (6.665s). Production commit and prepared-admission callers retain their actual contexts.
- [x] Correct the three static ownership guards without weakening their assertions: review all lexical population deltas (12 additions / four removals), register the three missing APOC fixture constructors through the existing lifetime owner, and review ten new / eight stale URL-sink fingerprints with credential-bearing cancellation-wrapper negative controls. The focused ten-package race packet passes 929 test/subtest events, zero failures or skips. Subsequent full uncached corrected packages pass: cyphertext 2.316s, testfixture 57.868s, observability 21.593s. Full multidb also passes (14.805s). Remaining search/database/server corrections and the integrated rerun are pending.
- [x] Correct the temporal-search capability mismatch at the canonical contextual temporal decision owner, retaining build/live/queued/continuation contexts. Historical nodes are filtered before dimension adoption and text/vector installation. Original two search failures and strengthened historical-text/current-search/dimension assertions pass; full uncached search package passes (237.442s) after updating a queue fixture to provide its owning context. The obsolete non-Badger unwrap fixture now exposes only its declared wrapper interface; the production unwrap owner is unchanged and the original control passes. Exact surface/error/cancellation evidence is being finalized; full database and server qualification plus integrated rerun remain pending.
- [x] Qualify final temporal provider error/cancellation paths without publishing historical text/vectors/dimensions, failed queued admission or reusable canceled builds; final full search rerun passes (236.731s) and focused temporal/queue race passes (2.937s). An intermediate full database run failed (328.990s) in two plugin package-build identity mismatches; the cause remains unestablished and no plugin workaround was introduced. Focused plugin loading then passed, and the complete database package on frozen final source passes uncached with live output (323.829s), 561 top-level passes and six existing plugin-integration skips. This is not a no-skips or final full-suite claim.
- [ ] Complete server qualification: the three original HTTP commit/graph-read failure groups are corrected (admitted terminal result lifetime and propagation of real graph read failures; absence/snapshot visibility semantics retained). The affected 222.134s normal family passes, and full txphase/txsession normal/race packages pass. The integrated server race family fails after 1298.209s in newly exposed `TestTransactionReplacementRoutesAtQuota`, with `Neo.TransientError.Transaction.Outdated` during decayed-entity cleanup on HTTP-explicit/Bolt-explicit routes. Investigate and correct this existing quota/live-policy causal unit before rerunning; do not waive it as preexisting. Five pinned-Neo4j checks skip without the reference URI. Superseded stopped and timed-out runs remain incomplete/failed evidence.
- [x] Correct the newly exposed policy-rewrite conflict before acceptance: native preparation now validates physical versions of exclusively owned entity bodies against the writer snapshot, preserving untouched endpoints, buffered implicit acceptance and accepted receipt authority. Deterministic native/public RED controls retain the original late physical conflict; the correction rejects it before handoff and permits subsequent cleanup without added retries. Integrated server normal/race passes (43.949s / 264.865s), including all 576 original quota cells; native preparation normal/race passes (5.085s / 9.861s), managed quota/accounting passes (4.517s / 8.094s), and full txphase/txsession normal/race passes. The exact GOMAXPROCS=2 race reproduction passes (42.710s). No focused skips; full server package with live output is now running. This later storage correction requires fresh integrated gates.
- [x] Complete the full uncached server package with live output: passes (589.738s), with five existing pinned-Neo4j skips. Then run the make-test prerequisites and exact default Go suite unfiltered/uncached for progress: `make lint-slog lint-cardinality && go test -count=1 -v -timeout 30m ./...`, preserving pipefail and full tee output. Both lint gates pass; 141 packages pass and three fail (replication, capturecensus, storagecensus), with 124 top-level skips. This completed run is a failure, not readiness.
- [ ] Resolve the new full-run failures: replication wire standby own-record ACK timeout; capturecensus real-source correlation, strict context census and reconciliation; storagecensus current population/order/mutant/suppression/release/kernel-context controls. Current strict context counts are 551 nil / 85 other / 114 separately tracked unfulfilled API sites. The whole raw log is retained; source review, actual public and planted negative controls remain required before any fingerprint or proof requalification. Re-run the integrated gate only after corrections, with visible progress.
- [x] Correct both reproduced replication causes without increasing the original timeout: accepted entity completion retains the configured absolute publication/own-record ACK budget, and uncorrelated wire RPCs use one cancellable exchange owner per connection, retiring abandoned in-flight connections. Fail-first direct/bulk/receipt and public transport controls are retained. Complete uncached replication passes (107.920s), affected race controls pass (20.961s), focused normal five repetitions pass (23.967s), and the original wire regression passes 100 repetitions normal/race (5.780s / 8.429s). Source applicability and limits are recorded in [the scoped receipt](replication-ack-correction.md). This is not a full integrated gate or full-package race pass.
- [x] Qualify all six storagecensus failure groups against current source without changing production behavior or expected guard outcomes. Review regenerated artifacts, migrated mutation fixtures/source anchors and classifications for two transient commitWriter fields. All actual-source mutants pass (589.671s); the companion run exercises every other package test, including current population/order, suppression bypass, release consumers and kernel fail-closed checks (165.298s). Five isolated analyzer race controls pass (1.365s); no skips. Current artifact has zero violations, unordered writes or pending items. No full-package race or final integrated pass is claimed. Release the whole-program analysis window to capturecensus.
- [x] Qualify the current capture-census causal population. The approved `defaultCtx` retirement is confirmed; no separate nil-default decision remains. Fresh current-source `TestStorageNilContextCensus` fails after 307.64s with 551 nil/absent-context flows (535 unchanged WAL refusal-sentinel witnesses plus 16 `deferredIndexWrite.ctx` field-projection witnesses), 85 other context violations, 114 unfulfilled contract findings, and 636 compiled findings. Bounded source tracing plus the search lane's actual queued caller-cancellation regression establish no observed runtime nil path in those 16 flows, but this does not accept or waive strict SSA findings. Storagecensus complete mutation suite passes (589.671s), remaining storage tests pass (165.298s), isolated race passes (1.365s, zero skips). Preserve all other findings; no readiness claim.
- [x] Request approval before any analyzer change for the bounded `deferredIndexWrite.ctx` candidate. The complete session-evidence proposal (shared SSA owner, correlated fields/dominance mechanism, full 16-site inventory, fail-closed frontier, mutants, blockers, cost plan, and exact scope) is `~/.copilot/session-state/fa89b40f-556a-44ec-889c-869e9ccc6a8f/files/pr1693-deferred-index-context-proof-proposal.md`. No analyzer implementation is authorized. The separate 535-root initializer proposal belongs to its prior proof owner; do not duplicate or fold it into this scope.
- [x] Reconcile the current result-correlation source marker: approved owner consolidation retired the prior `commitCtx`/`drainContext` marker population. The test-only assertion counts the current deferred-write field findings in `NilContextsElsewhere` without weakening the assertion to nonempty overall flow. Synthetic negative control and fresh real-source `TestNilResultCorrelationRealSources` pass (22.646s).
- [ ] Reconcile and revalidate the inference source pin. Current `runInferenceForEmbeddedNode` forwards the admitted `RunBackgroundTask` context to both `OnStoreBestOfChunks` and `CreateEdge`; the actual-path test passes normal/race. `TestStorageReconciliationRecords` currently fails earlier at `cleanupLeakedSystemNodes` (expected `8883c5…`, actual `a4bbe6…`), so the updated inference pin `39a72d…` has not been revalidated by a passing record gate. Do not blindly repin the earlier mismatch or claim the inference record passed.
- [x] Close the queued-search actual-request-cancellation test gap through public BuildIndexes/RefreshNode: cancellation during temporal admission retains request values, returns the original cancellation, publishes no queued work/text/vector/dimensions, and permits fresh healthy retry after build completion. Tests-only change; no runtime defect reproduced. Focused uncached normal/race passes (0.634s / 1.727s), and the combined temporal family passes (0.648s). Strict conditional-field census proof remains separate and unresolved.
- [x] Reproduce and correct authoritative WAL dispatch, replay identity and retention; retain native blind staging and bulk-delete semantics. Focused family passes uncached (42.717s); complete 20-case authoritative-decorator matrix and rejection/retry pass under race (3.386s).
- [x] Reproduce pending-index cancellation after mutation staging; make already-staged bookkeeping complete atomically with its node owner. Combined affected controls pass under race (21.105s).
- [x] Correct remote deadline/context ownership and closed-engine/embedding/projection fixtures; focused normal/race controls pass.
- [x] Run fresh full storage package after these corrections: `go test ./pkg/storage -count=1 -timeout=30m` passes (463.968s). This replaces the earlier 19-root-group failure result for this tested source, not for future edits.
- [x] Requalify the existing two-edge rollback compensation record after native WAL integration. Actual native large-commit/repair/reopen controls pass under race (120.487s); exact current-source proof plus five source/caller rejection mutants pass (121.404s). No new blanket phase exception added.
- [x] Reproduce generic compiled-call identity collisions; retain separate source/instantiated nil flows with stable instantiation-qualified keys. Synthetic/generic identity tests pass under race (2.147s); inline-field identity control passes under race (1.447s).
- [x] Complete the approved USE scanner migration: 13 exported context/error APIs and stateless context-first `ClauseKeywordNameReader`/`CallImportReader`. Focused normal controls pass (Cypher 41.701s / Fabric 0.873s), as do race controls (320.729s / 5.012s). Both-parser 32MiB public cancellation now measures 32.375us / 25.833us under race against the unchanged 100ms bound. Tagged repository compilation passes but executes zero tests. Public benchmark medians changed +4.89%, +2.93%, -0.76% for 1/1024/32768 whitespace bytes, with unchanged allocations; these are workload-scoped measurements, not a noise or final performance-pass claim.
- [x] Obtain paired, profiled USE migration cost evidence and explicit operator disposition. Twelve alternating pairs per parser/workload on frozen type-compatible baseline/current inputs measure small-query CPU effects of +3.19%/+3.99% (nornic, 1/1024 spaces) and +3.91%/+2.57% (antlr); large-workload effects of -0.02%/+0.26% remain statistically unresolved. Allocations are unchanged. The operator selected **accept and document the measured correctness cost**; do not claim noise, equivalence or nondegradation. No speculative optimization retained. Other readiness gates remain blocking; full distributions, source manifests, profiles and limits are retained in session evidence.
- [x] Converge accepted native WAL admission transfer into its existing lease owner. Complete affected WAL/managed/pending-index race family passes after convergence (93.250s); the earlier full storage result is not a frozen final-head pass.
- [x] Correct the auth transaction-start fixture to expose only its declared Engine capability rather than accidentally unwrapping to MemoryEngine's physical backend. Original injected-start-failure assertion is preserved. Focused race passes (1.700s), and full uncached auth package passes (8.064s).
- [x] Restore actual unsupported native-capability fixtures in knowledge-policy tests without changing assertions or product behavior. Preserve namespace identity by wrapping below the namespace owner. Full affected policy/localization/schema-helper family passes under race (3.666s).
- [x] Review the two pre-existing graph-carrier numeric fingerprints and migrated static-list operand declaration against their exact source changes. Context arguments are the only changes at those boundaries; graph arguments and static/runtime type decisions remain unchanged. Public APOC/seed/list-operand/owner controls pass under race (12.844s). The migrated row compiler declaration similarly forwards context without changing its native nil/bool/int64/float64/string literal switch or generic literal fallback; numeric ownership, literal/width/precedence, map projection and function/operand policy controls pass (10.151s). This boundary proof is separate from pending row-subquery semantic convergence.
- [ ] Qualify the approved complete typed package-capability non-exposure initialization proof on the five actual admission sites, wider 14 occurrences and complete adversarial population. The first incomplete candidate was restored; its historical generic passes do not qualify the current implementation. No runtime sentinel/API workaround, copy extension or opaque-function allowlist is approved.
- [x] Correct the integrated compiled/text row-subquery convergence failures through one row-context adapter into the existing canonical body evaluator. Preserve actual statement context, missing-context resolution, changed parameters, cancellation and body-error identity; public nested controls prove compiled-path reachability on both parsers. Both original gates pass unchanged. Fresh normal/race family passes (6.137s / 36.138s), 61 top-level tests and 585 pass records each, zero skips or cached results. This is not a full Cypher-package or frozen final-head pass.
- [ ] Resolve the four root groups exposed by the subsequent fresh full default Cypher package run: `go test ./pkg/cypher -count=1 -timeout=30m` fails after 645.278s. Three storage-API context integration groups and remote WAL-decorated issued views fail; original complete log is retained. The BEGIN fixture's undeclared unwrap capability is corrected and its original assertions now pass focused normal controls. Endpoint fixtures now persist the edge required by canonical endpoint visibility and reach the cancellation seam, but healthy retry and operation-versus-metadata context still need complete correction/qualification. Remote issued-view writes fail with `invalid data` and remain unresolved. No full-package pass is claimed.
- [x] Correct endpoint read errors at the canonical edge/node endpoint owners. Swallowed cancellation had become a successful null and poisoned result caching; operation-versus-metadata context selection was already correct. Preserve missing/deleted endpoint null semantics and propagate real failures through full-node and optimized property adapters. Original storage-API assertions and healthy-retry fresh-read controls pass; both parser/program modes, real error identity and the prior row-subquery family pass normal/race (7.338s / 42.587s), 84 top-level tests and 774 pass records each, zero skips or cached results. This resolves three groups only at focused scope; remote issued views and fresh full-package qualification remain open. Earlier frozen USE performance evidence does not directly qualify these later endpoint edits.
- [x] Correct remote WAL-decorated issued views in shared mutation admission: recognize delegated statement write authority without granting native journal authority to unowned remote engines. Preserve parent lease-transfer integration. The unchanged complete storage-membership matrix passes normal/race (51.167s / 554.363s); all ten mutation entrances, both remote factories, repeated decorators, owner errors, empty decorator journals, unowned-remote rejection and affected WAL/replay consumers pass normal/race on final scoped source (3.004s / 5.349s). This resolves the fourth group at focused scope, not the failed full-package run. Subsequent integrated storage source proofs and full-package tests must include this admission change.
- [x] Re-run both complete affected packages after all four corrections: `go test ./pkg/cypher ./pkg/storage -count=1 -timeout=30m` passes uncached (Cypher 657.872s, storage 543.702s). This is integrated package evidence, not the final frozen `make test` gate.
- [x] Resolve the remaining exact context ownership findings. Fresh strict census after storage corrections fails (272.716s): 957 nil-flow reports and 85 other findings, with 114 separately tracked unfulfilled contract sites. These are analyzer findings, not 1,042 established runtime defects. Both full diagnostic logs are retained losslessly as `context-census-*.log.gz` in the session evidence.
- [x] Reconcile the complete retained non-nil/API report population against current typed source: 199 reports at 195 distinct historical coordinates, including generic multiplicity; 198 still match current fresh-context coordinates and one USE source was removed by the approved migration. Catalog 657 current fresh-context syntax candidates with ownership/frontier references and proof gaps; these are not 657 violations. Selected-package type loading and final source drift checks report zero errors/drift. The coordinated canonical diagnostic was canceled when initializer qualification reopened, so exact current violation/API counts remain unestablished. No exception, runtime correction, API approval or current census pass follows from this inventory; source/closure/caller and adversarial qualification remain blocking.
- [x] Finish the approved complete bounded initializer proof: generic normal/race (76.789s / 212.341s), actual-source normal/race (31.577s / 152.147s), 97 controls, eight current-source mutations, all five target sites / 14 admission occurrences and 543 matching parser pins with zero drift. Cold complete Cypher proof measures 16.47ms versus the old 56.9us rejection (different work); cached lookup measures 4.77ns with zero allocations. This proof is not a whole-census pass.
- [x] Resolve the fresh blocking census result after that proof: `go test ./testing/capturecensus -run '^TestStorageNilContextCensus$' -count=1 -v -timeout=30m` fails (test 316.60s / package 317.045s), with 536 nil-flow reports, 85 other findings and 114 separately tracked unfulfilled API sites. Of the nil witnesses, 535 root at `WALEngine.beginNativeMutation`; one is a direct nil into `defaultCtx` / `remoteCallContext`. These are current static findings, not 621 established runtime defects. Preserve the full log and qualify the complete mutation-admission return/consumer unit and remaining ownership/API population without blanket exceptions.
- [x] Establish the WAL nil-report causal unit without changing runtime returns: five admission returns, ten callers and twenty context uses; every caller checks admission failure before consuming context. No runtime nil-admission defect reproduced. The 535 refusal witnesses retain `(nil, nil, ErrInvalidData)` because the immutable sentinel's 573-instruction initializer prefix encounters an earlier foreign-origin interface read in `fmt.Errorf("%w: ...", ErrConflict)`. Existing proof rejects that shape. All-ten-mutation canceled-admission and unowned-remote rejection controls pass normal/race (2.006s / 3.877s); temporary typed diagnostics pass normal/race (10.636s / 41.608s). Production owners remain unchanged, preserving applicability of the prior full-package production evidence. A complete structural proof-extension proposal and separate disposition of shipped remote nil-default API compatibility are required before implementation; no exception or API change is approved.
- [x] Remove every nil-context fallback and context-free duplicate path in production code, delete nil-only tests, and fix census findings at their source: Bolt claim results return explicit `nil, err`; search build-queue writes take ctx as a parameter instead of a zero-able field; session-close rollback, accepted replication apply, namespace index cleanup and server stop use `context.WithoutCancel` of their owning context; deleted-entity publication preparation receives the delete's ctx; the fixture closed-state probe keeps the test context. Strict capture census (`TestStorageNilContextCensus`, `TestNilResultCorrelationRealSources`, `TestInstalledRollbackPhaseRecord`) passes with 0 nil flows and 0 violations (177.365s); storage census regenerated; txsession, server, replication, bolt, search, observability, cmd/nornicdb, northwind_power and testfixture packages pass.
- [ ] Freeze the complete correction, run fresh full `make test` and blocking strict census, and retain exact candidate/base evidence before publishing. Broader verification remains deferred to CI under the recorded maintainer gate.

An agent attempted broader `make verify` despite that deferral: auth coverage
failed and a later stage was canceled. Neither that attempt nor any canceled or
compile-only run is a completed gate. Retain the logs; do not report a full pass.

Current gate (maintainer override, 2026-11-01): **`make test` only** on the final integrated committed head; broader `make verify` remains deferred to CI. Frozen checkpoint `41118c05a11942508fed54898482e851c0334d08`, tree `5ffb2acc1cdd9333ac1ddee29bc43626a2c0ddc3`, completed a fresh full run: **143 packages passed, one failed, zero cached**. Sole failure: `TestStorageNilContextCensus`. That census remains blocking by explicit maintainer decision; duplicate comparison stays unchanged/advisory and timed out incomplete. No final pass, qualified push or readiness is claimed.

## Remaining context-census correction batch

- [x] Preserve the full failed41118 receipt and unchanged frozen source tree.
- [x] Retire nine exact stale nil-flow allowances with tombstones; add no acceptance rule.
- [x] Correct public snapshot scan cancellation/admission and complete private callback consumers; parent public racecount3 passes3.602s. View state72→96B; completed-cache admission remains zero allocations.
- [x] Stop retaining unused plugin setter contexts; preserve actual admitted initialization contexts. Full integrated heimdall race passes13.857s.
- [x] Carry the existing preparation lifetime through six private constraint-validation owners; integrated constraint/snapshot racecount3 passes3.249s. No new public cancellation bug claimed for context-free CRUD.
- [x] Use the existing manager lifetime for six session enrollment roots; full parent txsession race passes7.994s. Preserve original request context at BEGIN and independent rollback completion.
- [x] Qualify typed HNSW mode ownership on the integrated source (parent race2.822s, actual receiver proof1.029s); shared-load timings are inconclusive, not a performance gain. Historical claims that NoCtx probes were machine-code eliminated are not supported: before/after assembly retains indirect dispatch; the canonical typed modes and allocation controls are the supported contract.
- [x] Integrate approved SubsystemHost/RevealScopes Go metadata APIs and canonical callers/docs. Host full package20.898s/full race24.677s and composition race Heimdall3.321s/database2.109s/server31.320s pass; reveal race storage2.558s/Cypher3.867s passes. Final census/full-suite gate remains pending.
- [x] Qualify integrated approved context-first parser/retry APIs and original terminal-error preservation. Integrated normal Cypher1.228s/Bolt1.882s/internal1.820s; race3.785s/4.399s/2.563s pass. Label/namespace public race storage4.714s/Cypher2.662s passes. A newly identified private Bolt write-classification context gap remains in the next correction batch; the full gate is still pending.
- [x] Complete the private batch-context pipeline and actual metadata receiver proof (parent phase race10.250s; actual receiver nil witnesses zero); retire redundant adapter, preserve intermediate/final publication decisions and independent rollback repair. No analyzer prototype or new phase-boundary record is accepted merely because generic fixtures pass. The exact two-edge cold rollback phase record remains unactivated pending human review.
- [x] Retire the nullable transaction commitCtx marker; private helpers receive the selected phase context directly, including beforeLarge. Integrated public nil/admitted-late-cancel racecount3 passes3.481s; five identical explicit completion selections pass existing backup/flush/close controls under race3.839s.
- [x] Retire the maintenance lease context field and migrate its private consumers. Lease layout48→32B (representation evidence); integrated backup/drop/recovery/dictionary/temporal race passes5.787s. Release body and native I/O policy are unchanged.
- [x] Reproduce and correct actual TLS replication dial cancellation; retain configured certificate/auth checks. Integrated public TLS racecount3 passes2.260s.
- [x] Correct private Bolt RUN write classification to retain actual operation context and fail closed without replacing accepted result or original error; integrated normal0.984s/race3.165s passes.
- [ ] Resolve remaining scoped context findings with actual source/phase proof and public regression controls; preserve required defensive redaction and independent BEGIN snapshot ownership. Latest unchanged diagnostic after commitCtx retirement:17nil+134other=151; it predates the later lease/direct-completion corrections. All104 WorkLifetime.Context sites are source-classified, not automatically approved or final-source re-extracted.
- [ ] Qualify the delegated private function/row-cache and path/label-rewriter context corrections; retain canonical scanners and no canceled cache verdicts.
- [ ] Qualify the per-plugin event-queue representation experiment while retaining the ingress parent ticket and public nested accepted-admission behavior.
- [ ] Establish the actual StorageAdapter WAL close/backlog contract and reproduce any supported durability failure before correcting it; currently unproved, not claimed data loss.
- [ ] Refresh affected inventories only after integrated source freezes, then obtain unchanged strict census and fresh full make-test pass before commit/publication.


## Current make-test regression corrections

- [x] Preserve exact failed893 make-test receipt/source pins and all7 package failures.
- [x] Integrate12 fixture ownership registrations and retire2 obsolete admin-route inventory entries; parent constructor guard passes.
- [x] Integrate per-use same-invocation successful tuple proof with adversarial controls; no error-only zero object is blindly accepted.
- [x] Integrate canonical immutable-cell writer-parameter proof and current storage census artifacts; independent full packagePASS960.043s/all37mutants/zero skips; parent focused generic checks pass. Final source artifact must be refreshed after remaining edits.
- [ ] Independently review and integrate lexical/security manifest deltas for actual current source, never blanket-refresh findings.
- [x] Qualify actual managed forwarding boundary and public both-parser follower implicit/explicit commit rejection; preserve veto/action identity/receipts/leader outcome. Parent integrated replication racePASS3.861s and credentialed empty-header Cypher racePASS3.746s; structural security racePASS18.534s.
- [x] Reproduce inherited-memory default pinning through uncached public queries and actual second Open; integrate canonical per-setting override selection, removal/equal-value transitions and coherent config snapshots. Independent combined race remains running; full make-test gate stays open.
- [x] Complete private update-request context propagation at40 production sites; independently correct accepted positional-count completion and preserve supported nil MVCC public boundaries. Parent focused race checks pass; final full gate remains pending.
- [x] Replace rejected unsound local-inline proof with distinct whole-cell and strict nonescape facts. Parent exact closure negative and merged generic race controls pass; full integrated internal-model racePASS100.259s.
- [x] Retire proven dead private DELETE subgraph and migrate meaningful semantic/fault/transaction cases to public execution on both parsers; parent focused race passes7.981s.
- [ ] Resolve current context census findings and refresh all affected inventories on the integrated source.
- [ ] Commit corrected candidate; run exact `make test`, fix any remaining failures, then publish protected PR head and current PR/Linear evidence.


- [x] Preserve original #1636, #1693 and #1667 heads and the separate #1667 prototype.
- [x] Rebase #1636 S1–S4 onto current #1684 (61d13cded), preserving release APOC I/O policy.
- [x] Rebase #1693 foundation commits onto the lifetime stack without merge commits.
- [x] Correct the remaining long-lived read-view panic release to use WorkLifetime.
- [ ] Verify lifetime shutdown contracts and correct obsolete gate schedules without weakening completion, rollback or admission assertions.
- [ ] Validate foundation read admission, concurrent-limit rejection and shared envelope consumers on the combined source.
- [ ] Reconcile compiled source/control populations with the integrated implementation; fix census portability and stale artifacts.
- [ ] Review #1667's own acceptance criteria against the combined foundations and preserve all still-required author work.
- [x] Integrate proven label readiness/LowMemory fixes, retaining existing marker bytes and storage layout. Completed focused native60k/rebuild/snapshot controls are recorded below; final current-source performance/full-suite qualification stays open.
- [ ] Resolve all failures exposed by integration, including existing release regressions.
- [ ] Measure affected ready/building reads and preparation workloads; reject regressions and unsupported prototype behavior.
- [ ] Run full make test on the frozen combined source and fix every failure.
- [ ] Update PR #1693 and linked Linear descriptions with implementation, exact evidence and outstanding qualification.
- [ ] Publish the qualified head with remote-head protection; fold superseded PRs only after inclusion is proved.
- [ ] Clean task-owned obsolete worktrees/artifacts/cache with the cleanup skill, preserving active work and required evidence.

The database is healthy again. The unchanged installed advisory checker completed
on staged tree `94c62f99764c492947e01194a42275e5bc9d22a0`: 140 functions,
seven reuse candidates and zero refactor targets. These candidates remain under
current-source review; this is not a clean result or final-candidate qualification.
The indexed baseline is older (`35e3bddbf`), and current source is authoritative.
The approved terminal-context API and caller migrations, optional bound drain
operation, and inline context-field census coverage are integrated locally. Focused
race checks are running; no final full verification, push or merge is claimed.

## Latest integration checks (2026-11-01; uncommitted candidate)

- [x] Integrate approved Fabric context-first terminal APIs and migrate callbacks, tests and docs; retire the mutable native completion bridge.
- [x] Verify selected integrated terminal/lifecycle/HTTP cleanup tests under race: Fabric2.491s, Cypher174.372s, Server127.235s. These nonverbose runs do not enumerate optional reference-server skips.
- [x] Verify exact terminal dispatch and optional-drain contracts under race: Fabric1.752s and Cypher3.224s.
- [x] Preserve explicit BEGIN's distinct request-drain and independent snapshot lifetimes using a bound optional operation; reject the aggregate closure prototype that added48B/one allocation.
- [x] Integrate selected publication contexts and eliminate noncancellable flush-wait timer allocations. Parent selected cancellation/preparation/large-commit race controls pass102.897s.
- [x] Qualify inline struct-field context inputs and shared source-argument binding: parent focused internal/capture race2.069s/4.024s; exact new fixture also checked independently.
- [x] Reproduce and correct a named-source census collision that hid a second distinct source at the same call. Parent focused integrated capture race passes1.898s; no duplicate-checker rule changed.
- [x] Complete seven reuse-candidate source review and safe fixture consolidations; unchanged corrected-tree checker completes264functions/11reuse/0refactor targets.
- [x] Qualify follow-up common stream instrumentation, SHOW-command matrix and shared atomicity outcome oracle under integrated race8.037s; remote session-mode matrix race1.836s and planner UNION/import/allocation controls race2.388s pass. Retain the source-backed phase/lifetime/visibility distinctions in the PR.
- [ ] Run the unchanged normal commit hook on the final staged source before full verification.
- [ ] Commit the reviewed frozen candidate through the existing advisory hook, then run full make verify including allocation checks of that commit.

## Current focused results (not final readiness)

- [x] Reproduce and fix native60k LowMemory transaction-size failure through bounded retries; combined completion/readiness/restart controls pass.
- [x] Reproduce incomplete paused-rebuild public reads on both parsers; shared candidate owner returns complete namespace-isolated rows.
- [x] Reproduce and fix loss of an older ready transaction's label index during forced rebuild using existing versioned metadata writes.
- [x] Confirm LowMemory/public/snapshot controls with focused race checks (96.864s before later cursor work).
- [x] Reproduce ready-ID streaming returning success after real cancellation; carry context into private cursor and verify first/final-delivery cancellation with focused race controls (3.821s).
- [x] Correct lifetime fixtures to join actual accepted work, use constructor/enrollment paths, and retain repeated error outcomes; canonical plugin/gRPC controls pass13.761s.
- [x] Correct the HTTP MERGE fault fixture to take native commit ownership before rejection; no publication and exactly-one-node healthy retry pass2.087s; direct classifier controls pass1.452s.
- [x] Migrate exported index/pending-merge context/error APIs (explicitly approved). Preserve original50k failure (all candidates read,229ms after cancellation); focused corrected run stops in14.708µs with0 candidate reads. Final frozen suite remains pending.
- [x] Integrate and independently run layered public continuation/retry plus scoring/reader-release component controls; final frozen full suite remains pending. No old unreachable probe is claimed as a successful injection.
- [ ] Complete namespace-retirement/body/property binding correction and census/cost qualification.

Current proposals, original failures, actual commands, source patches/new-file archive
and per-file hashes are retained under the task evidence directory
`~/.local/share/nornicdb-pr-readiness/pr144-storage-integration`. No combined head has
been pushed. The normal full make test gate uses30m per package; the earlier10m
server diagnostic was incomplete, not a full make test failure or pass. A separate
agent's release verification remains active, so timing qualification and shared
Go-cache cleanup must wait for an appropriate idle window. DeepSeek's DROP review
failed with malformed tool JSON and has no accepted findings; parent review remains.

Both contract decisions are now approved: canonical context-first Go index APIs with error propagation, and layered public continuation plus component scoring cancellation proof. DeepSeek implementation tasks run in separate registered worktrees; parent owns storage retirement architecture, integration, verification and publication.

Current index and unique API migration builds; focused50K cancellation, pending/copy/order/lock-admission controls pass. Additional metadata-discovery and composite-key/digest exported context contracts are approved; two bounded DeepSeek migration jobs are underway. Current #1684 is61d13cded; restack is complete and all final evidence must include that prerequisite.

- [x] Reproduce composite-key delimiter ambiguity through exact lookup and native bulk node-key creation; frame separate values through the existing digest owner. Focused integrated controls pass1.671s; persisted temporal keys remain unchanged.
- [x] Separate generic cancellable lock admission from publication-hold observation; original zero-allocation budget and observer/cancellation controls pass1.231s.
- [ ] Regenerate and qualify current source-pinned census artifacts. FilteredJoin error propagation is corrected and focused controls pass; no complete census pass is claimed.

## Additional approved API completion and proof corrections

- [x] Maintainer approved context-first schema discovery, composite-key and keyed-digest APIs; no pending contract question remains for these operations.
- [x] Rebase the11 preserved integration commits onto newer prerequisite61d13cded; range comparison retains all changes (one formatting-only context difference).
- [x] Independently verify new prerequisite error tests and focused transaction metadata/logger race controls on the combined source.
- [x] Share the complete label-read benchmark kernel; preserve ready/building completeness and namespace checks. Smoke8 named cases passes1.941s; not timing qualification.
- [x] Fix source-less nil-call identities without merging distinct compiler wrappers or suppressing their nil witnesses; independent control passes0.578s.
- [x] Integrate approved discovery/key/hash callers and constraint-admission propagation. Real cancellation controls pass; detached nil publication and retained-authority contracts remain supported without empty-key fallback.
- [x] Reuse one digest scratch and length buffer, retire three auth forwarding functions, preserve framed digest bytes and context-free cache contract. Final digest cancellation/race controls pass2.740s.
- [x] Validate provider package file pins against anonymous aggregate initializer poisoning and preserve failure sensitivity; exact integrated controls pass1.513s.
- [x] Correct captured-cell repeated-store and success-edge dominance proof. Independent generic race controls pass51.895s; full current-source census remains pending.
- [x] Correct remote nil-shape fixture to exercise the scoped native root and retain exact forwarding witness. Current integrated control passes15.337s.
- [ ] Re-extract and qualify frozen current-source populations once all pending source edits converge; previous capture707 findings remain provisional, not a clean audit.

## Current integrated follow-up

- [x] Independently verify the behavior-preserving transaction file split:147 function declarations/bodies and12 globals preserved exactly.
- [x] Correct sorted-index rebuild write-to-read handoff atomically; unchanged public concurrent-reader and race controls pass.
- [x] Independently qualify safe handoff test teardown and deterministic controls under race:2.390s.
- [x] Reproduce public snapshot cancellation blocked by buffer admission; canonical contextual admission plus safe joined regression tests pass integrated race7.029s, preserving nil-detached and zero-allocation cached paths.
- [ ] Complete metadata-scan canonical owner review and measured cost comparison.
- [ ] Review the key/digest preparation certificate as one family only; complete population qualification remains open.

- [x] All-package compile-only retry passes after migrating the missed admin-import test caller; many package results cached, no test-suite pass implied.
- [ ] Review key/digest52-member certificate as a proposal only; investigate supported list/map normalization cancellation before accepting affected rows.

- [x] Private filled composite-index discovery retains caller context through actual schema-lock admission and metadata/property enumeration; integrated admission/validation/discovery race controls pass3.356s.
- [x] Pure metadata relocation preserves91 function declarations/bodies/docs,19 global declarations/order and complete comments/import union; schema.go2424lines.
- [x] Migrate the two remaining request-bearing exported discovery contracts: SeekableCompositeIndexesForLabel and GetConstraintsForLabels take the caller context and return an error, all typed callers/tests are migrated and the old signatures are retired. The public PrepareMergeNode held-schema-writer control now passes: discovery admission receives the statement context, the canceled request returns context.Canceled, admits no key, and healthy retries (including another namespace) still work.

## Updated verification gate (maintainer direction)

- [x] Fetch actual release base de70c0e83 and record clean local verification checkpoint3a611d2c2.
- [x] Attempt existing installed advisory duplicate checker on exact staged integration; database transaction endpoint timed out. Preserve incomplete result; continue current-source owner review, no checker/hook edits.
- [x] Complete the diagnostic `make verify BASE=origin/release/v1.4.4` on3a611d2c2: FAIL109m26s, recorded below with seven failed coverage groups and incomplete/failed checks. This closes execution of that historical diagnostic only; the final corrected-source full gate remains open.


## Full-verification fallout (pinned3a611d2c2; fixes not yet integrated)

- [x] Preserve full verification failures; do not alter the pinned primary runtime mid-run.
- [x] Independently review Bolt disconnect/native listener retirement fix. Real TCP and close-owner controls pass race count3 in isolated source (2.603s); keep net.ErrClosed filtering at the existing close-outcome owner.
- [x] Verify exact injected closed-WAL rollback shutdown outcomes in the existing terminal matrix (isolated race2.994s); all other matrix cells still require clean retirement.
- [x] Review retirement of the zero-production-caller DELETE-with-LIMIT subgraph. Preserve already-correct public both-parser mixed-type behavior; no live performance or data-loss claim.
- [x] Integrate reviewed Fabric, Search, Bolt and retired-DELETE corrections after the pinned diagnostic run; nine reviewed fallout patches and focused integrated checks are recorded below. Final full-source revalidation remains open.
- [ ] Complete immutable transaction-image effect guard repair without exempting metadata effect accounting.
- [x] Correct quota replacement shutdown access metadata through approved conditional WriteAccessMeta, existing entity authority/buffer ordering and same-transaction presence. Parent native/kp race passes3.703s/1.459s; genuine storage faults remain errors.
- [x] Correct HTTP stop fixtures through the actual production stop owner, retaining native listener/forced-close outcomes and joined work. Integrated exact failure race controls pass7.011s (recorded below).
- [x] Complete normalization review and repair actual flat-map serialization cancellation; integrated normalized-value controls pass4.427s and four private/public cancellation controls pass2.369s. Full current-source qualification remains separate.
- [x] Complete public DB.CreateIndex investigation: populated-store missing rows were reproduced, then canonical transactional DDL population/admission corrected. The current-prerequisite integration records the accepted implementation; no unresolved proposal is claimed here.

- [x] Independently verify integrated candidate Bolt/Fabric/Cypher fallout controls under race (4.197s/1.955s/6.643s); convert package built with no matching test, not claimed a test pass.
- [x] Independently verify integrated Search lifecycle/nil-entry controls under race (12.864s).
- [x] Consolidate HTTP fixture registration/cleanup into one owner and independently verify the two exact failures under race (7.011s); preserve actual-completion/error assertions.
- [x] Independently verify normalized value-key/map/relationship controls in candidate under race (4.427s), then the four actual private/public normalization cancellation cases under race (2.369s).
- [ ] Reject broad effect-image proof proposal after read-only review exposed alias/literal-publication holes; preserve counterexamples and implement a narrow sound adaptation rather than marking the holder non-effect.
- [x] Fix confirmed populated-store DB.CreateIndex missing rows through existing transactional DDL; canonical native pre-snapshot DDL admission is integrated, without a second population owner or ready-empty publication. Final suite remains open.


## Completed initial full gate and recovery

- [x] Complete full `make verify` at `3a611d2c2` against `de70c0e83`: **FAIL**, 109m26s. Localization, UI, build and allocations passed. Linux llama qualification is n/a on Darwin. Seven coverage groups failed; the complete conformance package exceeded its global 600s budget; Docker preflight failed; docs reported two broken source links. Result: PR comment 6556032917.
- [x] Integrate nine reviewed fallout patches after that run; retain the dirty-source manifest. Primary default-build effect-image controls pass (16.162s).
- [x] Repair the two docs source links and verify both release targets exist. Strict docs recovery passes (14s).
- [x] Start local Docker and recover differential verification against pinned Neo4j (234s; Bolt 113.245s, HTTP 101.885s). This selected only differential/docs; seven other steps were skipped.
- [x] Align the complete conformance package budget with the existing 30-minute full-suite standard. Three complete coverage traversals of the pinned head took 613.036/645.943/609.897s and failed only the known numeric-owner guard. Preserve the 12,960-case constructor population and every per-case assertion/deadline. A fresh full conformance pass remains required.
- [x] Complete the previously blocked whole Search recovery: PASS244.040s after reader/IVF/retirement fixture repairs, recorded below. This historical whole-package result does not qualify subsequent source edits or replace the final full gate.
- [x] Reject the clear-buffer-only access-retirement proposal. Real-store node/edge tests fail after a batch has drained: orphan metadata reappears and shutdown retains a not-found error (1.150s). Stop superseded job `2a5bcfe32e464f4fa2af2974f62da834`; preserve its proposal and the RED evidence.
- [x] Complete canonical access lookup/publication correction: typed WriteAccessMeta prevents orphan metadata resurrection, skips only true absence, retains genuine faults and preserves storage formats. Integrated parent native/kp race passes3.703s/1.459s.
- [ ] Complete schema DDL population/quoted-identifier correction, job `ed67549586224a0aa21131fa438282c1`.
- [x] Retain bounded 52/94-member correspondence reviews, including their cost, dependency and provenance gaps. Neither review establishes complete population qualification.
- [ ] Run final full `make verify` on the complete, frozen, rebased source. The original failed run and selected recovery passes do not establish readiness.


## Current recovery checkpoint

- [x] Integrate the exact-vector reader fixture correction after parent review. The original read-holds-metadata/queued-admin-writer schedule is staged using production admission and vector leaf locks, with two chunk reads. A real recursive-lock overlay reproduces the deadlock; independent primary race count=3 passes (1.844s). Whole Search rerun is in progress; no full pass claimed yet.
- [ ] Complete DDL proposal regression repairs: concurrent index-name admission, transactional persistence-failure fixture, and scoped traversal fixture. DeepSeek job `365956528c1447ed95d507b5d8dc20ff`.
- [x] Review/integrate exact schema JSON persistence via canonical lossless JSON owner: escaped-surrogate durable reopen, ordinary payload bytes and old JSON read/rewrite pass parent default race2.822s. No key/layout/version migration.
- [ ] Review whether deterministic mid-fill publication tests require a new global test seam; prefer existing owners and require suite-safe cleanup. DeepSeek job `1891fe79e9b24ba79c3b9544981a895c`.
- [x] Obtain typed access writer, remaining discovery APIs, and custom-engine atomicity contract decisions; all approved in the latest operator reply.
- [ ] Follow the latest user gate: full `make verify`; duplicate checker best effort, with any timeout/incomplete inventory reported explicitly. Do not change checker rules or claim a clean result from missing output.

- [x] Complete fresh whole Search after reader/IVF/retirement fixture repairs: PASS244.040s. Original900s timeout and later237s failure remain preserved. Test-only forwarding fixture then retired; focused race repeated for canonical callers.
- [x] Verify current numeric ownership guard independently: PASS1.106s after retired DELETE route removal, not attributed to unrelated normalizer controls.

- [x] Review and integrate canonical JSON owner reuse in native transaction schema read/write/roundtrip. Parent default race passes2.822s for escaped surrogate durable reopen, exact ordinary payload bytes, old JSON read/rewrite. No key/layout/version migration. Worker fullstorage and final integrated gate remain pending.
- [x] Correct reproduced bulk UNIQUE NaN/list identity false positives via canonical equality. Parent default race passes2.946s; matched4096-node preparation median4.030→1.791ms and73775→24524allocations. Broader throughput qualification is not inferred.

- [x] Integrate canonical native bulk constraint equality correction: false NaN/list duplicates reproduced first; original error slice ownership preserved. Parent default race passes2.946s. Matched preflight4096-node median4.030→1.791ms and73775→24524allocations; this is not full bulk/transport throughput proof.
- [x] Integrate private pending tracking ctx/error correction and atomic publication, preserving accepted maintenance lifetimes. Parent default race passes2.839s, including actual canceled public publication while buffer writer remains held and full32768-ID control. Allocationparity retained; latency qualification under contention remains open.
- [x] Integrate canonical live pre-snapshot native DDL admission and after-publication release, preserving explicit snapshot conflicts and managed commit ownership. Current-prerequisite integration records focused proof; final full gate remains open.
- [x] Integrate schema document exact UTF16 import/export through the canonical codec, preserving ordinary document formatting/layout; whole adminimport race passes2.933s. Destination persistence is separately corrected/verified below.

- [x] Integrate schema document lossless import/export owner reuse; preserve ordinary indentation/newline/0600 and old input. Independent whole adminimport race passes2.933s. Destination restart durability finding remains separately open, not hidden by source-reopen controls.
- [x] Repeat unchanged installed advisory checker on frozen alternate-index tree628503fc6f043db44580808c4929835148a18a53: INCOMPLETE database tx/commit timeout. Advisoryexit0 is not a pass; realindexSHA unchanged; no checker/config modifications. Continue priorinventory/sourceownerreview per maintainer instruction.
- [ ] Complete all original DDL publication tests with minimum safe one-shot test seam and actualjoins; preserve original9tests and addforeignfillcontrol. DeepSeekddf303.
- [x] Reproduce and fix destination schema import durability through canonical ReplaceDefinitionDurable; strict binding precompile and direct-loader failure atomicity included. Integrated storage5.916s/adminimport4.014s race passes; no storage-layout migration.


## 2026-11-01 current-prerequisite integration

- [x] Rebase the combined source onto prerequisite `11416628493f2a219a01ed1f2e56154dd55b532c`; retain release base `de70c0e8375e9c74800d5ab6d7da5a6505650cb8`. Local checkpoint `8dd04737d` is not the published PR head.
- [x] Run full `make verify BASE=origin/release/v1.4.4` diagnostic: localization/UI/build and GraphQL passed; MCP coverage and first complete Cypher traversal failed. Preserve results; supersede repeat traversal before integration. Remaining steps were unexecuted, not passed.
- [x] Integrate canonical buffered/grouped/non-null index reads, native pre-snapshot DDL admission, actual initial-search-build join correction and request-valued Pre/PostExecute callbacks. Focused integrated race checks and whole MCP coverage pass; all-package compile-only passes, not full qualification.
- [x] Migrate retired status-helper/transaction-field test callers without restoring removed APIs.
- [x] Verify the disputed unlabeled MERGE followed by constrained-label SET against pinned Neo4j 5.26.30. Retain permanent UNIQUE status, genuine stale-MVCC transient controls, winner-only state and manual retry convergence. No broad retry heuristic restored.
- [x] Carry commit preparation context into canonical node/relationship constraint enumerations and the scalar relationship-uniqueness cache owner. Include cancellable cache admission and discard incomplete cold cache builds. Parent integrated focused race passes (6.169s).
- [x] Classify only `propertyIndexSnapshot.groupEnds` as owned read-result group offsets; a planted sibling slice field remains unclassified (focused positive/negative census controls pass, 28.408s). Full extra race-instrumented census package runs timed out at 30 minutes and remain incomplete.
- [x] Complete approved context-first IndexNode and request-owned BackfillCompositeIndex migration. Canonical prepare/apply preserves old postings on canceled replacement; all callers/docs migrated; both-parser public DDL cancellation/retry and unchanged list equality/membership controls pass in combined parent race.
- [x] Integrate durable schema replacement, strict pre-persist binding compilation and direct-loader failure atomicity. Independent integrated focused race passes storage5.916s/adminimport4.014s; full combined qualification pending. No persisted layout/version change.
- [x] Obtain explicit approval for typed conditional WriteAccessMeta, context/error GetConstraintsForLabels and SeekableCompositeIndexesForLabel, and fail-closed atomicity for custom local write engines. All three contract choices are approved; implementation and final combined qualification remain pending.
- [ ] Reconcile current source pins and regenerate stored-entity census artifacts from the final frozen source. Historical counts and retired guards are not current qualification.
- [ ] Commit the final local candidate so the allocation ratchet measures the corrected source, then run a fresh full `make verify BASE=origin/release/v1.4.4`. Fix every actual failure before readiness.
- [x] Attempt unchanged advisory duplicate checking; graph commit timed out. Continue prior inventory/current-source ownership review per maintainer best-effort policy; no clean result or checker/hook modification claimed.
- [ ] Publish the qualified PR head with a protected lease, exact verification evidence and updated Linear acceptance state; keep review/merge ownership with the maintainer.
- [ ] Perform task-owned workspace cleanup using the cleanup skill after readiness, preserving active work and required evidence. Shared Go cache cleanup remains deferred while verification/workers use it.

- [x] Integrate fail-closed local-write atomic capability check and preserve delegated/native owner behavior. Parent focused default checks pass; independent public/callback/native/deferred-outcome race checks pass5.942s.
- [x] Complete approved WriteAccessMeta atomic-presence owner and all Go callers/docs. After two failed DeepSeek jobs (connection reset, malformed response), native takeover reused existing typed authority, buffer order and same-transaction stored reads. Parent native/kp race passes3.703s/1.459s. No storage-format change.
- [x] Integrate approved discovery migration from DeepSeek `a5ea54f497b64cbcb6676d2b30283c75`, including all typed callers and embedding migration documentation. Worker public held-schema-writer fail-first and focused controls pass; parent integrated race passes storage3.671s/adminimport3.353s; Cypher compiled with no matched tests, not a test pass.
- [x] Complete context-first IndexNode preparation-before-apply and DDL BackfillCompositeIndex propagation; canceled replacement preserves old postings, all typed callers/docs migrate and both-parser DDL cancellation/retry controls pass (recorded above).

## Final contract integration checkpoint

- [x] Parent fresh combined focused race passes storage10.346s, Cypher8.877s, adminimport6.970s, knowledgepolicy7.073s. It includes index/discovery/DDL/constraint preparation, durable schema replacement and conditional access-state publication. The earlier missed Backfill test caller compile failure is preserved and corrected; no test skipped.
- [x] Fresh whole-module compile-only passes with `go test ./... -run '^$' -count=1 -timeout=8m`; this is not a suite pass.
- [x] Parent full adminimport race passes3.850s before the final migrations; final combined affected controls also pass afterward.
- [ ] Finish normal current-source census controls; preserve prior incomplete race runs.
- [ ] Create the frozen local candidate and run full `make verify BASE=origin/release/v1.4.4`; no qualified push or readiness claim before actual results.

## Current diagnostic correction checkpoint

- [x] Preserve failed 5bf14fceb full verification and supersession receipt. First Cypher traversal failed; retry was canceled deliberately. Later checks remain unexecuted.
- [x] Reconcile exact URL-sink manifests against current bodies and verify whole observability independently (16.742s); no runtime security change or waived sink.
- [x] Qualify frozen stored-entity census on 065635afd: all37 mutants pass, full package947.681s. Artifact applicability must be re-established after subsequent source edits.
- [x] Integrate required traversal-context legacy retirement after exhaustive initializer review; preserve separate undirected context defect.
- [x] Migrate five transparent read decorators through the existing EngineUnwrapper capability. Parent integrated race test population passes42.766s; opaque write/fault decorators remain separately unresolved.
- [x] Review and integrate generic captured-cell reaching-store proof, retaining negative/unknown cases. Parent internal/workcensus passes14.825s; parent real-source correlation checks pending. This is not the duplicate checker and does not modify its rules.
- [x] Re-pin only two moved trackedRWMutex nil-call sites after byte-identical method-body comparison to HEAD and release; no new detached acceptance.
- [x] Integrate complete canonical preparation context/error propagation and approved metadata/discovery callers; parent preparation controls pass5.040s and combined storage/Cypher/adminimport/knowledgepolicy checks are recorded above. Final full qualification remains open.
- [x] Correct public undirected adjacency cancellation and dynamic native autocommit APOC USE routing through originating context plus existing statement coordinator. Integrated parent undirected race11.839s and native coordinator/metadata focused checks are preserved; full suite remains open.
- [ ] Resolve remaining opaque test-fixture failures with meaningful public and component assertions; never blindly unwrap mutation/fault probes.
- [x] Reproduce and correct durable schema creation/privilege preparation cancellation: parent five creation controls race3.134s and wholeauth race27.531s; context propagation, no partial grants and rename/delete0writes retained. No blanket unqualified-population closure.
- [ ] Freeze corrected source, refresh source-pinned evidence, commit and run fresh full make verify before publishing readiness.

- [x] Integrate complete canonical commit-preparation ctx/error propagation and three discovery methods; parent tagged race passes storage2.788s (Cypher compiled with no matched tests, not qualified by that selection). No per-transaction context field or stored-format change.
- [x] Integrate actual public undirected MATCH cancellation at both route owners, retaining originating ctx through canonical adjacency/combine. Parent public cancellation/relationship/counter race result is recorded in parent-current-integration-checks.json.
- [x] Add public undirected typed-count positive control to prove the materialization probe is reached; overlay qualification passes1.826s, integrated race also run.
- [x] Parent real-source captured-context proof controls pass; exact log in census-result-correlation-review/parent-result-correlation-tests.log. No duplicate checker modification.
- [ ] Finish newly reproduced receipt/acknowledged-overlay admission, durable creation validation and DELETE guard context propagation.
- [x] Integrate native autocommit coordinator reuse with exact nativeRoot provenance, original auth/lifetime and independent periodic batch preservation; public APOC/standard USE REDs retained, child focused race22.776s and parent integrated checks preserved. Final full gate remains open.

- [x] Parent independently runs all12 preparation public/component tests after receipt/overlay and canonical bulk-poll completion: PASS5.040s; inner-poll negative control remains preserved.
- [x] Integrate e3 schema creation validators/cache ctx migration. Parent all5public controls race PASS3.134s; introduced duplicate-selection deadlock and stale-selection success were reproduced and corrected before integration.
- [x] Integrate defensive privilege/role original-context propagation, owning loops and canonical cancellable sorting from DeepSeek; parent wholeauth race PASS27.531s. Preserve actual post-read cancellation/no partialgrants and rename/delete0writes controls.
- [ ] Qualify cache-staging allocation overhead and unexplained buffered-preparation timing increase with matched/profiled measurements; neither is waived or declared noise.
- [ ] Complete all affected fixture migrations after integrated nativeRoot statement coordination, callback auth/lifetime and periodic ownership. Commit rejection, buffered completion/HTTP202 and optimistic-ID focused controls pass, but remaining opaque observation/Graphify/Graphiti fixtures and final current Cypher/race qualification remain open.

- [ ] Correct newly reproduced runtime schema identity defects: colon-bearing label/property pairs collide, and a relationship-only UNIQUE cache rejects unrelated node writes. Both-parser native public RED evidence preserved; no storage-key/payload/version change permitted.
- [x] Integrate structured committed terminal failure retention at canonical Fabric owner and authoritative response-commit truth in IsCommitRolledBack; contradictory retained-marker, generic confirmed-commit and native WAL outcome controls preserved. Full errors/Fabric race and focused current integration evidence exist; final full gate remains open.

### Resume: integrated native metadata and fixture corrections

- [x] Independently verify actual buffered ACK metadata / HTTP status / optimistic IDs: Cypher 4.676s, storage 3.736s, server 2.239s.
- [x] Review and integrate seven layered atomic error fixture files; retain component error checks and public native rollback controls. Focused parent run passed 2.486s; strengthen accepted SET to change persisted value.
- [ ] Complete whole Cypher run (parent-current-cypher-suite.log). Integrated claim/metadata/DELETE race passed storage3.026s/Cypher5.665s; layered error fixtures race6.113s and remaining MERGE/index fixtures race4.311s. These selections do not qualify the whole package.
- [ ] Accept or revert typed shared-claim experiment after matched performance qualification.
- [ ] Integrate independently verified DeepSeek bulk-read waiter correction only for reproduced defects (job 17c51c975a874a58b1de6b5a0bccd07d).
- [ ] Refresh source-bound census evidence and freeze candidate for full make verify; no full verification or readiness claimed.

- [x] Independently review exactly two stale reconciliation function proofs; holdsEntities now receives originating phase context, and trackPendingValues has only the canonical typed key rename. Apply JSON-only source pins; strict revalidation in progress, no new exceptions.


## 2026-11-01 resumed integration checkpoint

- [x] Preserve normal local checkpoint `9e6b63e8ad03bf6165eabe9d5d57a8313c052d7a`; advisory duplicate check timed out, not a clean result. No qualified push or readiness claimed.
- [x] Integrate the reviewed stream visitor lock-release/retake correction from DeepSeek jobs bd1b5457ca53493eaf3837231fa53648 and daa45366962746eaa6b10886c898f807. All three streams preserve callback/cancellation errors, do not release a foreign lock after panic, and recheck transaction ownership before waiting and after acquisition. Parent focused storage race PASS 3.024s; log `parent-visitor-cancellation-final-race.log` in task evidence.
- [ ] Complete fresh whole Cypher/storage package diagnostics on the integrated source; preserve exact logs and fix every failure.
- [ ] Finish lexical population reconciliation against current source, with reviewed member dispositions and no extractor/rule weakening.
- [ ] Complete GDS public cancellation qualification (DeepSeek 694c0a95eafe45cda3b67de692e8b6f0) and explicit/Fabric index-drop qualification (DeepSeek 40d528875f1443a08d74845c683c7606). No runtime changes justified by a source candidate alone.
- [ ] Complete remaining current-source context census proof obligations, including deferred error composition and native completion field ordering; prior census failures remain preserved and unqualified.
- [ ] Qualify shared-claim and buffered-preparation performance in an isolated matched window, including the visitor correction's measured +24 B / one allocation per label/edge stream invocation (whole-graph unchanged). No timing claim from concurrent runs.
- [ ] Freeze corrected source, run full `make verify BASE=origin/release/v1.4.4`, resolve failures, publish with protected lease, update PR/Linear evidence, then perform task-owned cleanup. Never merge automatically.

- [x] Fresh whole Cypher diagnostic PASS649.683s (`current-integrated-cypher-suite-round2.log`). It predates the GDS correction; final frozen full verification remains required.
- [x] Fresh whole storage diagnostic FAIL530.927s with exactly one failed test, `TestQueryNodeScanNestedVisitorsKeepTheirOptions`. Preserve the original intentional early-stop contract; do not change its assertion. Parent actual contract RED and three blocked-retake controls preserved, then corrected canonical visitor helper. Focused combined race PASS3.473s (`visitor-intentional-stop-contract-final-race.log`). Full storage rerun remains part of final verification.
- [x] Parent lexical manifest guards PASS3.450s,3985 entries (`parent-current-lexical-manifest-guards.log`).
- [ ] Complete reviewed GDS cancellation integration and test qualification. Initial worker finished before cancellation request took effect (no run was canceled). Its older-tree full run is not current qualification. Parent accepted originating-context forwarding and post-build publication gate; test timestamp/join/public-vs-component corrections delegated as8a6535b46e7b4fa2b10e32d1454f8837.
- [ ] Reproduce/correct newly reported composite explicit transaction schema visibility through public paths (DeepSeek ed061a62f18544e28fc1f18f8ab015d4). No API/storage-format change authorized by that report alone.

### Current qualification continuation

- [x] GDS originating-context and pre-publication correction independently qualified: public both-parser node/edge-heavy controls plus deterministic stream-prefix, final-stream cancellation and healthy retry race PASS69.885s. Context-loss and missing-gate mutants independently fail. No timing-based skip retained.
- [x] Shared-claim two-file change qualifies against matched common prerequisites: 128-node/four-UNIQUE median161337→139874ns (-13.3%),185610→185193B,1998→1997alloc; empty target latency within noise,4802→4401B and75→72alloc. Invalid model-overlap attempt retained separately. This does not qualify whole ingestion/preparation.
- [x] Buffered preparation pair compiled with common identity prerequisites, six production file deltas and identical public fixture. Both 1/64-node fixtures pass one-iteration physical/visible/ACK assertions. Timing/profile qualification remains open.
- [x] Deferred-error composition proof independently passes full internal/workcensus race58.169s and actual native statement owner source proof. Full current-source capture census remains open.
- [ ] Integrate canonical shared-join edge-fact and stable native-origin field proofs after independent review; preserve soundness negatives and do not weaken census policy.
- [ ] Finish explicit composite schema visibility correction and comment-prefix routing coverage. Worker transport failure is incomplete; recovery job0b5e1470e47d44f1879d12a25f5ebdfb continues in the same isolated checkout.

- [x] Parent reviewed/integrated canonical stable-field/TLS shared-join corrections; combined full internal/workcensus race PASS94.023s. Guard-return, loop-Phi, alias/mutation/escape and deferred recovery negatives remain. No census policy/duplicate checker change.
- [x] Lexical discovery identifies exactly8 additional SSA metadata functions and no removed/changed member; review records data-grammar dispositions, retains3985 prior entries and names actual controls. Guard pass1.984s before final evidence-name metadata amendment.
- [x] Parent took over remote native lifetime migration from DeepSeek0bb after implementation; no public nil contract removed or timeout duplicated. Rejected new AST mirror tests, retained actual fake/HTTP/Bolt lifetime/result/error controls; integrated focused race PASS2.682s. Worker extra diagnostic/canceled response is not a parent full-suite qualification.

## Latest correction checkpoint (2026-11-01 Phoenix)

- [x] Correct the analysis model's single-field whole-value projection through the canonical per-use constructor proof. Frozen baseline fails; integrated race controls pass3.986s. Unknown/error/escaped/cyclic/overwritten values remain reported. This is not a runtime nil normalization or a census exemption.
- [x] Fix compiled-source attestation to hash exact parser inputs including overlays, rather than changed disk bytes. Parent full internal/workcensus race passes107.588s; same-width overlay counterexample is retained.
- [x] Prove Backup includes acknowledged but unapplied buffered nodes, edges and updates; omitted-drain mutant fails and parent default-build race count3 passes2.997s. Backup cancellation remains a separate public-contract limitation.
- [x] Complete source review of lexical3991-member manifest candidate; latest discovery has no further additions/removals/mechanism changes. Apply and qualify after the frozen context diagnostic.
- [ ] Resolve remaining context audit findings without invented cancellation owners or blanket nil exceptions; fresh source-pinned round8 diagnostic is running.
- [ ] Correct actual public CALL cancellation misreporting as empty-name ProcedureNotFound after native BEGIN; DeepSeek bounded public-path regression task is active. No publication occurs in the reproduced failure.
- [ ] Finish corrected-source inventories, commit, run make test, and publish exact-head results. Broader make verify stays canceled for CI.

- [x] Integrate explicit accepted replay/token completion owner: parent focused race3.073s; worker15-root normal32.079s/race287.049s and token-ctx omission mutantRED preserved. No storage-format or lifetime-policy change.
- [x] Retire private parameter-binding nil fallback and ineffective map-comparison path; parent integrated race3.354s. Both-parser MERGE coverage establishes actual helper reach; same-map mutation/rebind refresh remains intact.
- [x] Reproduce actual CALL cancellation error regression against old owner (parent real-CancelFunc baselineRED1.076s), propagate canonical parser errors, and qualify integrated race6.644s. Parent rejected manufactured deadline context and used a real expired deadline for reader/public-entry controls; no mid-BEGIN deadline claim.
- [ ] Finish remaining optional journal representation and old whole-field projection consolidation, teardown owner proof and final inventories. Fresh round8 audit fails84nil/177other on unchanged source; those findings are not all runtime bugs and are not waived.

## Approved compensation and current integration (2026-11-01)

- [x] Record the maintainer's explicit approval of exactly two rollback/recovery compensation boundaries. Whole-module installed-record proof passes83.058s, including all five changed-source/new-caller rejection controls. Runtime behavior and census rules unchanged. Evidence: rollback-phase-current-review/installed-whole-proof.log.
- [x] Integrate the reviewed private point-read context packet, including actual request-owned commit endpoint checks and latest-visible batch reads. Context-free public point APIs retain their existing contract; no new optional capability or storage change.
- [x] Integrate sole FIFO replication WAL publication, sealed ingress/drain, ordered publication windows and retained shutdown errors. Gate excludes native storage/listener callbacks; accepted recovery intents remain logged.
- [x] Parent integrated point-read race count2 PASS3.340s and replication adapter race count2 PASS13.360s. Logs preserved in private-point-read-context/parent-integrated-race.log and adapter-close-backlog-review/parent-integrated-race.log. These are focused checks, not full make test.
- [ ] Complete fresh source-pinned context census after these integrations; fix remaining blocking findings.
- [ ] Resolve reproduced HA quorum stale-position acknowledgement through the pending typed Go API decision; no implementation assumed approved.
- [ ] Freeze source and pass full make test before commit/push/readiness. Broader make verify remains for CI. Duplicate inventory remains advisory/incomplete under maintainer instruction.

- [x] Retire ordinary private lock nil sentinels; context-first acquisition requires its actual nonnil lifetime. Nine affected cancellation/admission controls race count2 PASS5.000s; reviewed source-record test PASS10.874s. Quiet alternating lock-kernel samples preserve0B/0alloc; medians ordinary18.41→18.33ns, request20.29→20.27ns, tracked20.87→20.89ns. This is a bounded kernel check, not ingestion or system throughput qualification.
- [x] Retire exactly three obsolete detached nil-call records (removed commitCtx field and two ordinary tracked lock calls), retaining tombstones; no active exception or census rule added.
- [ ] Complete private count-fold context correction (DeepSeek a03f5b5ebc3d4b3892dcca2a49321a02) and ingress event-field proof review (DeepSeek df6af1b042ad43718742d94849cd2f6e). Latest completed diagnostic before these changes:11nil+130other, source unchanged, FAIL.
- [x] Parent independently reproduces private fold context contract failure on integrated preimage (canceled/deadline contexts wrongly return nil); preserve managed-fold-context/parent-preimage-red-race.log. Apply reviewed three-file private correction and focused component regression; parent integrated race count2 in progress. Actual Restore/Close callers still choose independent lifetimes: no public request-cancellation defect claimed.
- [ ] Latest explicit-lock diagnostic has3nil+133other, FAIL. Its production source stayed stable, but newly copied count_delta_fold_context_test.go appears in before/after source drift; do not call it an unchanged whole-source run. Remaining nils are event channel Extract(Select) provenance, not established runtime nil values. Ordinary lock roots now appear as substitution findings; approved rollback source-closure pin correctly became stale after lock code changed and requires exact member review.
- [ ] Await explicit Go compatibility decisions for HA per-command publication and the four remaining NOR337 public API families. Concrete proposals are ha-quorum-position-review/proposal.md and remaining-public-context-contracts-proposal.md in task evidence. No dependent API changes implemented.

- [x] Parent integrated fold/count/restore/recovery race count2 PASS23.638s. This is private phase-context convergence, not public request cancellation qualification.
- [x] Review stale compensation closure member diff:64identities unchanged, onlycontextRWMutex.acquire body changed. Same2approvedcompensation edges and nonnil unbounded completion retained. Refresh single closure pin after review; installedrecord+5mutationrejection controls PASS80.718s. No census rules or duplicate checker edits.
- [ ] Evaluate isolated event-channel projection experiment DeepSeek739659e55b0c45dbbf302055d7b0fe0a. No product guard/pointerchannel/allocation workaround; no accepted model patch. Must reject unselectedcase, closedchannel, unknown/escaped/reassignedproducer and zero-value sends.

## Approved remaining Go contract migration

- [x] Maintainer approves typed HA own-record publication API and all four remaining Go context-first families. No pending compatibility question; do not ask again for these contracts or their canonical callers.
- [ ] HA exact publication/one total ACK budget: DeepSeek43db160b82e549b0946bc33fda3ee17a.
- [ ] Storage point CRUD/native staging/admission/capabilities: DeepSeek5da0e3c7b2884841a4450fec3e4f2ce3. Family includes query/projected/MVCC point readers; ensure no capability still drops actual context.
- [ ] Metadata/auth/schema families: DeepSeekc43dea3f4d7e42ac9a684824e0fac1df. Preserve security publication/cache/audit order and accepted completion.
- [ ] Canonical external callers in isolated native lanes while all DeepSeek slots are occupied: Cypher; service/cmd/test/APOC storage adapter and consumers. No context forwarding wrappers or stored request fields.
- [ ] Accept accurate census receive projection only after repeated-send and hidden-carrier escape regressions are fixed. Parent reproduced a missed second send and fixed enumeration; independent native review reproduced actual reflection/unsafe close hidden by private-field exposure. Prototype remains isolated; no R model/policy/checker modification.
- [x] Parent integrates accurate Select projection (4 files) after both repeated-send and actual hidden-reflection-close false-proof corrections; complete internal/workcensus race PASS175.670s. No predicate weakening, policy exception or duplicate-checker script edit.
- [ ] Fresh source-pinned select-projection diagnostic remains FAIL1nil+132other (unchanged source); sole nil is accepted managed commit wait after its source line shifted. Replace two actual publication-wait nil sentinels with explicit independent Background, retire private oracle optional-nil branch, migrate2tests; affected commit/oracle/embedding/recovery race in progress. No nil-zero claim without fresh extraction.

### Approved remaining Go API integration — 2026-11-02

- [x] Integrate frozen storage CoreV2 (184 files), Cypher (461 files), metadata/auth/schema (243 files), typed HA publication/Engine callers, and service callers (446 files), preserving overlapping edits with exact source preimages. Integration is not qualification.
- [x] Correct the documented optional-nil `PrepareEntityCommit` boundary once using the existing preparation lifetime owner. The original cleanup-hang regression and new caller-identity boundary checks pass in the internal storage population; focused race checks pass (2.768s).
- [x] Pass focused token cancellation/publication race controls (4.485s): stopped calls publish nothing, canceled admission releases its wait, and accepted persistence finishes publication.
- [x] Migrate entity-metadata lookup to the accepted flush cycle context; `go test -count=1 ./pkg/knowledgepolicy` passes. Storage lifecycle package passes (1.801s).
- [ ] Complete APOC backend-reaching caller/error-result migration; preserve ordinary missing results and propagate cancellation. Failed DeepSeek transport output retained; native takeover active.
- [ ] Finish remaining optional-capability service callers and fixture/tool consumers, then compile the complete test population.
- [ ] Fix the reproduced public WAL client cancellation defect: a rejected native create currently leaves a replayable create record. Preserve accepted state-machine intent semantics; do not filter records after native failure.
- [ ] Complete independent defensive security review of the new auth/metadata ownership.
- [ ] Pass the blocking context census on combined source, then a fresh full `make test`. Prior 143-package checkpoint does not qualify the current API migration.
- [ ] Rebase onto current release, verify final source, commit/push, update PR/Linear and perform task-owned cleanup. No automatic merge; duplicate inventory remains advisory this round as instructed.

### Current approved corrections — November 2

- [x] Integrate `SubgraphAll` nodes/rels map plus error contract; parent focused race check passes (1.460s).
- [x] Integrate exactly two approved census soundness fixes: exclusive success-edge error guards and source-proven global initialization. Parent focused controls pass (2.658s); full model verification passes (33.673s). No phi-precision or scope-exclusion change.
- [x] Integrate sidecar audit-only replay, external storage fixture migration, clustering progress correction, explicit admission-refusal transaction cleanup, and selective CLI bootstrap failure propagation. Focused evidence retained in the readiness evidence directory.
- [ ] Finish native WAL publication/recovery/compaction correction and independently accept its workload-specific performance. Rejected generic prepared candidate remains excluded.
- [x] Recover interrupted private read/revision context migration; original failure and partial edits retained, 19-file packet integrated.
- [ ] Complete exact lifecycle phase proof and obtain any required source-pinned approval; no blanket census waiver.
- [ ] Run fresh blocking context census and full `make test` on integrated final source before commit/push.

- [x] Compile the entire integrated test population: `go test ./... -run '^$' -count=1` exits 0. This is compilation evidence only; full runtime gate remains outstanding.

### Joined worker packets

- [x] Integrate private accounting/embedding read-context packet (19 files) and native WAL correction packet, preserving prepared-inner admission ownership, buffered replay idempotency and accepted-failure snapshot retention.
- [x] Joined storage/nornicdb/multidb focused race controls pass (14.101s / 3.835s / 1.687s). No full-suite claim.
- [ ] Frozen-source census `final-packets-soundness` fails (289.518s; unchanged source): 98 context records and 1,013 nil traces from three admission roots. Approved source-order proof and exact phase review remain in progress.
- [ ] Direct single-node WAL benchmark is a confirmed regression: 27,956 to 31,061 ns/op (+11.11%), +8.93% bytes and +19 allocations. Profile-guided correction is active; do not claim a performance pass.
- [x] Exact public quota-managed outer-WAL control: original submission binding removal reproduces missing recovery (0.533s); current race proof passes (1.592s). Quota setup integrated after the diagnostic finished.

- [x] Integrate bounded existing-encoder semantic scratch (three fewer allocations); direct performance qualification remains open.
- [ ] Full storage package failed (601.141s): unsupported-wrapper write fixture plus naturally timed-out stopped-write-behind snapshot fixture. Preserve both failures and fix without weakening assertions.
- [ ] Finish actual schema preparation/admission cancellation, buffered named-count admission, and temporal preparation corrections in isolated DeepSeek lanes; validate each original failure and joined consumers.
- [ ] Implement newly approved same-package source-order census proof with early-call/mutation/escape/unknown-ordering rejection controls; no runtime guard or duplicate-checker change.

### Current whole-package fallout

- [x] Schema preparation/live-admission, temporal preparation and buffered count cancellation packets integrated; public fail-first controls retained. Joined storage race passes (4.082s).
- [x] Approved source-order proof integrated; parent focus passes (9.707s), child full model normal/race pass (40.209s/151.120s). Five actual same-package refusal callsites remain unproved.
- [ ] Storage round3 completes with failures (465.188s; 19 root groups), while the full adminimport package passes (3.093s). Fix WAL authority routing and remaining remote/helper contract regressions; do not waive them.
- [ ] Wait for required public Fabric callback context-first contract decision before scanner implementation; actual both-parser post-cancel RED and healthy controls are retained.
- [ ] Record the operator's direct-WAL latency tradeoff decision; map/scratch allocation reductions are integrated, ID optimization remains held. No claimed <=5% primitive latency result.
- [ ] Finish strict context-census phase proofs/records and remaining real findings, then fresh full make test before commit/push. No duplicate-checker blocker or automatic merge.
