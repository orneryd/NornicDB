# Combined release/v1.4.4 readiness: #1693

The approved dependency order is current #1684 terminal-outcome prerequisite,
#1636 request/service/storage lifetimes, #1693 storage foundations, then #1667
label-index readiness and remaining preparation. Preserve each author's work;
port into current owners rather than restoring obsolete implementations.

Current gate (2026-11-01): full `make verify BASE=origin/release/v1.4.4` on a frozen committed candidate, with unchanged duplicate checker best effort. Earlier deferred/test-only directions and running-job notes below are historical; the current-prerequisite integration section and external current-resume.json record the active state. All four remaining API decisions are explicitly approved. No final verification pass or published correction head is claimed.

- [x] Preserve original #1636, #1693 and #1667 heads and the separate #1667 prototype.
- [x] Rebase #1636 S1–S4 onto current #1684 (61d13cded), preserving release APOC I/O policy.
- [x] Rebase #1693 foundation commits onto the lifetime stack without merge commits.
- [x] Correct the remaining long-lived read-view panic release to use WorkLifetime.
- [ ] Verify lifetime shutdown contracts and correct obsolete gate schedules without weakening completion, rollback or admission assertions.
- [ ] Validate foundation read admission, concurrent-limit rejection and shared envelope consumers on the combined source.
- [ ] Reconcile compiled source/control populations with the integrated implementation; fix census portability and stale artifacts.
- [ ] Review #1667's own acceptance criteria against the combined foundations and preserve all still-required author work.
- [ ] Integrate proven label readiness/LowMemory fixes, retaining existing marker bytes and storage layout.
- [ ] Resolve all failures exposed by integration, including existing release regressions.
- [ ] Measure affected ready/building reads and preparation workloads; reject regressions and unsupported prototype behavior.
- [ ] Run full make test on the frozen combined source and fix every failure.
- [ ] Update PR #1693 and linked Linear descriptions with implementation, exact evidence and outstanding qualification.
- [ ] Publish the qualified head with remote-head protection; fold superseded PRs only after inclusion is proved.
- [ ] Clean task-owned obsolete worktrees/artifacts/cache with the cleanup skill, preserving active work and required evidence.

The user deferred the full make verify sweep and requested no new duplicate-checker
run this round. This does not assert a clean duplicate inventory. Remote code graph
refresh at de70c0e83 failed during relationship import and is incomplete; current
source is authoritative. No release tag, remote merge or storage migration is claimed.

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
- [ ] Complete `make verify BASE=origin/release/v1.4.4` on3a611d2c2; started with all mapped checks selected. This supersedes the earlier simple make-test-only/deferred-verify direction for this run. No final result or qualification is asserted.


## Full-verification fallout (pinned3a611d2c2; fixes not yet integrated)

- [x] Preserve full verification failures; do not alter the pinned primary runtime mid-run.
- [x] Independently review Bolt disconnect/native listener retirement fix. Real TCP and close-owner controls pass race count3 in isolated source (2.603s); keep net.ErrClosed filtering at the existing close-outcome owner.
- [x] Verify exact injected closed-WAL rollback shutdown outcomes in the existing terminal matrix (isolated race2.994s); all other matrix cells still require clean retirement.
- [x] Review retirement of the zero-production-caller DELETE-with-LIMIT subgraph. Preserve already-correct public both-parser mixed-type behavior; no live performance or data-loss claim.
- [ ] Integrate reviewed Fabric, Search, Bolt and retired-DELETE corrections after the pinned full run completes, then reverify the integrated source.
- [ ] Complete immutable transaction-image effect guard repair without exempting metadata effect accounting.
- [ ] Fix quota replacement shutdown access-metadata failures (DeepSeek2a5bcfe32e464f4fa2af2974f62da834).
- [ ] Correct HTTP stop test fixtures to enroll the actual production stop owner and observe native listener errors; retain forced-close/outcome assertions and join test work.
- [ ] Complete normalization proposal review and repair the flat-map serialization cancellation gap found during independent review.
- [ ] Complete public DB.CreateIndex readiness investigation before making any product-defect claim.

- [x] Independently verify integrated candidate Bolt/Fabric/Cypher fallout controls under race (4.197s/1.955s/6.643s); convert package built with no matching test, not claimed a test pass.
- [x] Independently verify integrated Search lifecycle/nil-entry controls under race (12.864s).
- [x] Consolidate HTTP fixture registration/cleanup into one owner and independently verify the two exact failures under race (7.011s); preserve actual-completion/error assertions.
- [x] Independently verify normalized value-key/map/relationship controls in candidate under race (4.427s), then the four actual private/public normalization cancellation cases under race (2.369s).
- [ ] Reject broad effect-image proof proposal after read-only review exposed alias/literal-publication holes; preserve counterexamples and implement a narrow sound adaptation rather than marking the holder non-effect.
- [ ] Fix confirmed public DB.CreateIndex populated-store missing-row defect through existing transactional DDL owner (DeepSeek3cfb53d729444270a88d3b373bd873df); no second population implementation or ready-empty publication.


## Completed initial full gate and recovery

- [x] Complete full `make verify` at `3a611d2c2` against `de70c0e83`: **FAIL**, 109m26s. Localization, UI, build and allocations passed. Linux llama qualification is n/a on Darwin. Seven coverage groups failed; the complete conformance package exceeded its global 600s budget; Docker preflight failed; docs reported two broken source links. Result: PR comment 6556032917.
- [x] Integrate nine reviewed fallout patches after that run; retain the dirty-source manifest. Primary default-build effect-image controls pass (16.162s).
- [x] Repair the two docs source links and verify both release targets exist. Strict docs recovery passes (14s).
- [x] Start local Docker and recover differential verification against pinned Neo4j (234s; Bolt 113.245s, HTTP 101.885s). This selected only differential/docs; seven other steps were skipped.
- [x] Align the complete conformance package budget with the existing 30-minute full-suite standard. Three complete coverage traversals of the pinned head took 613.036/645.943/609.897s and failed only the known numeric-owner guard. Preserve the 12,960-case constructor population and every per-case assertion/deadline. A fresh full conformance pass remains required.
- [ ] Complete the full Search run after its original panic masked later cases. Evidence: `primary-search-full-after-fallout.log`. Investigate any remaining failure or hang.
- [x] Reject the clear-buffer-only access-retirement proposal. Real-store node/edge tests fail after a batch has drained: orphan metadata reappears and shutdown retains a not-found error (1.150s). Stop superseded job `2a5bcfe32e464f4fa2af2974f62da834`; preserve its proposal and the RED evidence.
- [ ] Complete canonical access lookup/publication correction, job `e75e51b467b84650bd69db6f8528ae6c`: prevent metadata resurrection, retain genuine failures, and preserve storage formats.
- [ ] Complete schema DDL population/quoted-identifier correction, job `ed67549586224a0aa21131fa438282c1`.
- [x] Retain bounded 52/94-member correspondence reviews, including their cost, dependency and provenance gaps. Neither review establishes complete population qualification.
- [ ] Run final full `make verify` on the complete, frozen, rebased source. The original failed run and selected recovery passes do not establish readiness.


## Current recovery checkpoint

- [x] Integrate the exact-vector reader fixture correction after parent review. The original read-holds-metadata/queued-admin-writer schedule is staged using production admission and vector leaf locks, with two chunk reads. A real recursive-lock overlay reproduces the deadlock; independent primary race count=3 passes (1.844s). Whole Search rerun is in progress; no full pass claimed yet.
- [ ] Complete DDL proposal regression repairs: concurrent index-name admission, transactional persistence-failure fixture, and scoped traversal fixture. DeepSeek job `365956528c1447ed95d507b5d8dc20ff`.
- [ ] Review exact schema string persistence through the existing lossless JSON owner, including old ordinary JSON compatibility and durable reopen. DeepSeek job `c3511b2986d146379f5e48b67a61b078`; no storage-layout migration is approved.
- [ ] Review whether deterministic mid-fill publication tests require a new global test seam; prefer existing owners and require suite-safe cleanup. DeepSeek job `1891fe79e9b24ba79c3b9544981a895c`.
- [x] Obtain typed access writer, remaining discovery APIs, and custom-engine atomicity contract decisions; all approved in the latest operator reply.
- [ ] Follow the latest user gate: full `make verify`; duplicate checker best effort, with any timeout/incomplete inventory reported explicitly. Do not change checker rules or claim a clean result from missing output.

- [x] Complete fresh whole Search after reader/IVF/retirement fixture repairs: PASS244.040s. Original900s timeout and later237s failure remain preserved. Test-only forwarding fixture then retired; focused race repeated for canonical callers.
- [x] Verify current numeric ownership guard independently: PASS1.106s after retired DELETE route removal, not attributed to unrelated normalizer controls.

- [x] Review and integrate canonical JSON owner reuse in native transaction schema read/write/roundtrip. Parent default race passes2.822s for escaped surrogate durable reopen, exact ordinary payload bytes, old JSON read/rewrite. No key/layout/version migration. Worker fullstorage and final integrated gate remain pending.
- [ ] Finish newly reproduced public bulk UNIQUE false positives (NaN/list identity collision) through canonical constraint equality; qualify ordinary and nonreflexive scale costs.

- [x] Integrate canonical native bulk constraint equality correction: false NaN/list duplicates reproduced first; original error slice ownership preserved. Parent default race passes2.946s. Matched preflight4096-node median4.030→1.791ms and73775→24524allocations; this is not full bulk/transport throughput proof.
- [x] Integrate private pending tracking ctx/error correction and atomic publication, preserving accepted maintenance lifetimes. Parent default race passes2.839s, including actual canceled public publication while buffer writer remains held and full32768-ID control. Allocationparity retained; latency qualification under contention remains open.
- [ ] Resolve concurrent ordinary DDL admission through canonical live gate before snapshot and native after-publication release before reentrant listeners. Context-aware gate/managed commit Go contracts are pending operator choice; no wrapper/fallback proposal accepted.
- [ ] Correct schema document export/import exact UTF16 identity through existing canonical codec while preserving ordinary document layout; DeepSeek1e858 scoped to adminimport.

- [x] Integrate schema document lossless import/export owner reuse; preserve ordinary indentation/newline/0600 and old input. Independent whole adminimport race passes2.933s. Destination restart durability finding remains separately open, not hidden by source-reopen controls.
- [x] Repeat unchanged installed advisory checker on frozen alternate-index tree628503fc6f043db44580808c4929835148a18a53: INCOMPLETE database tx/commit timeout. Advisoryexit0 is not a pass; realindexSHA unchanged; no checker/config modifications. Continue priorinventory/sourceownerreview per maintainer instruction.
- [ ] Complete all original DDL publication tests with minimum safe one-shot test seam and actualjoins; preserve original9tests and addforeignfillcontrol. DeepSeekddf303.
- [ ] Reproduce/fix public schema import destination persistence before readiness; no dismissal as baseline/unrelated. DeepSeekfab8 publicRED andwritecontractproposal first.


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
- [ ] Correct IndexNode follow-up review findings: preserve old posting until all new keys are prepared; propagate request context through DDL backfill. Existing production request cache build is unpublished, so no published-cache corruption is claimed without proof.

## Final contract integration checkpoint

- [x] Parent fresh combined focused race passes storage10.346s, Cypher8.877s, adminimport6.970s, knowledgepolicy7.073s. It includes index/discovery/DDL/constraint preparation, durable schema replacement and conditional access-state publication. The earlier missed Backfill test caller compile failure is preserved and corrected; no test skipped.
- [x] Fresh whole-module compile-only passes with `go test ./... -run '^$' -count=1 -timeout=8m`; this is not a suite pass.
- [x] Parent full adminimport race passes3.850s before the final migrations; final combined affected controls also pass afterward.
- [ ] Finish normal current-source census controls; preserve prior incomplete race runs.
- [ ] Create the frozen local candidate and run full `make verify BASE=origin/release/v1.4.4`; no qualified push or readiness claim before actual results.
