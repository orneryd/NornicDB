# PR #1655 duplicate review

The advisory inventory covered 724 added/rewritten functions and reported seven
refactor targets plus 39 reuse candidates against release `15f1a502`. Reciprocal
pairs are counted separately by that report. This document records their actual
owner changes and the reasons for retaining distinct operations. It does not
claim a zero-finding checker result or measured performance for untested designs.
Checker rules, thresholds and exclusions were not changed.

## Consolidated mechanics

| Inventory rows | One current owner | Contract preserved |
| --- | --- | --- |
| 1–2 | `TracedEngine.startOperation` | Constant span names, context, kind and attributes; each required typed Engine method defers span end across its actual backing call, including panic |
| 3–7 | Edge-mutation runner and seed-list ID-seek fixture | Direct and explicit transaction paths, endpoint/type indexes, original graph populations and precondition assertions; all ID equality/compound/IN outcomes remain |
| 8, 10, 13, 19–20, 30 | Shared count-cycle, metrics, MVCC adjacency-history, snapshot-write and directional-read fixtures | Literal count expectations, original labels/properties, distinct peer histories, exact version-qualified edge IDs and rollback boundaries |
| 9, 36 | One privilege mutation-fault adapter and one auth mutation-fault adapter | Second CREATE/DELETE/UPDATE rejection and content-selected user rejection; actual native transactions and original rollback/cache/audit assertions |
| 11–12, 15–16, 18 | One post-success write barrier, shared worker persistence fixture and stale-refresh scenario | Collection stores share their arm across opened engines; DB retains its separate pre-delete arm; original workers, polling, newer-write ordering and terminal search/storage assertions remain |
| 14, 21, 23, 26–28, 33–34, 40–41 | Cohesive composite, timestamp WAL, stale label-transition, suppression, adjacency conflict, HTTP stream, WAL delegation, query-node and rebuild fixtures | Schema results versus missing updates; create versus update clocks; node and edge suppression; corruption isolation; all stream terminal scenarios; supported and unsupported delegation; cache/vector/concurrent checks; explicit rebuild retention/version inputs |
| 17 | `mvccBenchHeadVersion` | Repeated untimed head observation is shared; live update histories and alternating delete/recreate histories remain distinct |
| 25 | `runNumericConversions` | Original mixed-property float64 query, float32 comparison, int SUM, int32 AVG and numeric-string → same-node nonnumeric-string UPDATE/TypeError sequence |
| 24, 29 | `SchemaManager.ApplyUniqueValue` | One constraint lookup, key projection and mutex; validated acquisition, recovered acquisition and owner-checked retirement are explicit actions. Old registration/unregistration APIs are retired |
| 31–32 | Standard `maps.Copy` | Shared property merge; native APOC adapter retains actual Before and persistent update, while APOC InMemoryStorage mutates its owned record |
| 35 | `checkEdgeWriteConflict` plus existing `snapshotHeadConflict` | Identical CREATE/UPDATE edge-head bodies are collapsed; both callers use the same private checker with no forwarding alias |
| 37 | `PointsService.admitCollection` | One typed name-validation/authorization owner. All 20 public Points methods preserve order and read/write authority. Empty QueryBatch still authorizes the empty name and accepts an empty batch when permitted |
| 38 | Four-result `LookupUniqueConstraintValue` | Holder, found, constraint existence and cache completeness are explicit; planning and validation adapters are retired and callers migrated |
| 42 | Filtered `GetConstraintsForLabels` | Temporal selection shares schema enumeration/type filtering; redundant type checks removed |
| 45 | `saveUser(user, transaction)` | One record mutation body. Nil owns transaction admission; an already admitted view is used directly. `saveUserInTransaction` is retired |

The auth admission distinction is demonstrated, not hypothetical. A naive
collapse that re-entered `WithStoreTransaction` reset a custom staged view's
mutation counter and failed `TestCoordinatedRoleRenameFailureChangesNothing`.
The corrected explicit-view call preserves the original rejection of the second
staged UPDATE. Replacing that fault with a pre-write scan failure was rejected.
The numeric fixture likewise retains the original mixed-property float64 query
and updates the same string-valued node before its second TypeError assertion.

## Retained operations and concrete reasons

| Operations | Reason they remain distinct after shared mechanics converge |
| --- | --- |
| Typed tracing Engine methods | Their signatures and backing calls implement Engine. The remaining bodies contain that dispatch and its span lifetime; they contain no copied span-construction algorithm. No unmeasured callback overhead is asserted as justification |
| Live/tombstone builders and CREATE/UPDATE benchmarks (17, 22) | Shared head observations are untimed. A live chain records update versions; a tombstone chain records separate live and deleted versions. CREATE times formatted new IDs and growing population; UPDATE times one preseeded ID. Sharing a timed callback or operation branch would change the benchmark workload. Both use real Badger in-memory storage, not a map-only mock |
| APOC native and InMemory backends (31–32) | Native adapter resolves integer IDs, retains immutable Before and publishes a persistent mutation. InMemoryStorage returns/mutates its owned pointer and has **no mutex**. Property merging is already shared through maps.Copy; introducing a persistence callback into its map operation adds a new backend layer without removing a remaining merge algorithm |
| Edge version and endpoint validity (35) | Edge checker compares the relationship's current version. Endpoint checker examines both node identities, staged creates/deletes, tombstones and acknowledged buffered recreates, distinguishing invalid endpoints from peer-deletion conflict. Both use canonical head comparison where applicable; substituting one operation for the other loses a required validation stage |
| Migration marker read/write (39) | Read asks whether stored status equals complete; write persists status plus destination. They already share namespace and key owners. The review found both completion callers discarded write errors: both now return those errors. Four rejection cases cover empty/legacy data and UPDATE rejection and injected ErrNotFound → CREATE rejection routes; copied completion control is not waived |
| Temporal exact lookup and mutation eligibility (42) | Both use filtered schema enumeration. Lookup addresses one exact three-property key/start/end triple. Mutation selects applicable node rules including composite keys via splitTemporalKeySpec. Reusing the node-only mutation predicate for lookup would add a different entity-type restriction; reusing exact lookup would discard composite rules |
| Bulk create/delete policy (43) | Transaction lifetime already belongs to inBulkTransaction; accepted capture belongs to inMutationTransaction. Empty create checks engine-open state, nil/empty create identities are errors and entity constraints run before admission. Empty delete returns immediately; ordinary delete filters duplicates/empty IDs and tolerates missing records, while accepted capture retains input order. These are different remaining policies, not copied transaction lifecycles |
| Buffered-generation lease versus statement tracker release (44) | Generation CAS covers a list of commit leases only after queue retirement and last BEGIN pin. Statement release atomically detaches one tracker pointer before ending/pooling it. A once-bit cannot replace pointer transfer, and a pointer pool cannot replace generation pin ownership |
| Cypher/HTTP transaction memory tests (46) | Only the public configuration call is shared. Cypher uses private runningTransaction/root accounting to assert both pool ceilings; HTTP uses authenticated admin reload, a running statement barrier and subsequent request failure. A test helper cannot import another package's private test controls; production configuration/accounting is already common. Both independent boundary drivers are retained |

Different expected results alone were not accepted as reasons to copy setup or
control flow. The repeated mechanics above were consolidated while the distinct
assertions and failure schedules remain. Record final acceptance and gate results on the PR with the checked source and base; focused or canceled runs are
not full-suite qualification.

## Fresh869-function inventory — integrated follow-up

The later unchanged advisory checker identified one recognized move, five refactor targets and39reuse candidates. Reciprocal pairs remain separate rows in the saved44-row inventory. This supplements the earlier724-function inventory; neither report is a zero-finding or full-test result. Current dirty-source convergence still requires final qualification.

| Fresh rows | Shared owner and exact preserved boundary |
|---|---|
|1,6,8|Canonical disk/in-memory constructor registration and snapshot graph fixture; original configurations, namespace and cleanup retained.|
|2,35|mustReadNode takes the actual query/full/latest reader; original vector/cache/current-image assertions and concurrent schedule retained.|
|3–5|ChangeUserState typed action plus mutateStoredUser locked clone/persist/cache/audit boundary; Enable clears disabled/count/lock, Disable only disabled, Unlock count/lock preserving disabled. Password completion follows committed success audit under lock.|
|7,13|MutationFailureEngine owns shared selector and Create/Update/Delete interception with explicitly supplied auth.WithStoreTransaction. Original Nth2 staged failures/Nth0 rejection retained; RejectUpdate short-circuits before selector counting. Old mutationFault/selector retired; no fallback transaction implementation.|
|9–12,25|fieldIndexOperation owns field-index workflow; batchEachChild owns fan-out/name overwrite/results/error. Search/Recommend nil→empty and QueryBatch nil→original Query dispatch are explicit policies. Original admission/empty-list behavior retained. Field-name extraction is lazy after admission, preserving nil-request denial ordering.|
|14|testfixture.SpanAttrs projects one typed attribute slice; SDK span and exporter-stub consumers select their own attributes, preserving provider and consumer assertions without a runtime type switch.|
|15,23,29|Directional/MVCC read fixtures retain exact edge sets, snapshot versions, pending overlays and returned-copy controls.|
|16,18|pauseConsumerUpsert owns original seed/arm/goroutine/wait sequence; peer delete/newer-upsert and baseline/final assertions remain case inputs.|
|17,20|Untimed head observation/real engine fixture shared; live update vs tombstone history and create-new-ID vs update-preseeded-ID timed workload preserved without performance claims for unmeasured designs.|
|19,21,22|Seed-list ID seek, numericUniqueClaimFixture and stale label-transition fixture share setup; original IN/equality populations, int1/float641 committed versus pending controls and opposite label transitions remain.|
|24,30,31|maps.Copy owns property merge; native immutable Before/persistent publication and in-memory owned pointer remain their backend boundaries.|
|26,27|HTTP stream request/status/decode owner retains65exhaustions×3modes, six terminal scenarios, actual disconnect and repeated discard controls.|
|28,32|Canonical edge-head comparison and concrete nodeConstraintValidation own repeated policy. Physical publishing txn and transaction pending/current reader methods remain unchanged. nodeConstraintKeyValues preserves property order, composite UNIQUE null skip and NODE KEY first-null error; native/transaction localized error/cause construction remains distinct as before.|
|33,42|snapshotEdgeWriteCase retains BEGIN-before-peer and true tombstone versus never-existing error controls.|
|34,37–41,43|Marker read/write transition, bulk admission, exact temporal addressability, generation lease vs detached statement tracker, MERGE holder observations before/after key lock and private/public memory drivers keep their actual canonical owners and required authority boundaries.|
|36|mustNewAuthenticator shares construction/assertion with explicit original configurations and fault-owned storage.|
|44|PauseGate.Pause(armed,entered) owns post-observation/post-success/pre-delete CAS/signal/wait. Each adapter supplies its original trigger arm/signal; one release remains idempotent. Pre-delete still pauses before backing Delete.|

Static source review found no new product defect in the parent-integrated constraint, Qdrant nil-policy or privilege transaction changes. The selector and pre-delete gate copies identified during the final review were corrected and re-inspected. No claim of a new checker pass or full dirty-head qualification is made. Final source, original assertion populations and full verification receipts must be reconciled before marking these findings complete.

## Later379-function advisory — integrated owners, qualification pending

This29-row inventory (one refactor target,28reuse) now has current source owners for span query/lookup/provider lifetime, scoped accessor readers, orphan observation races, sole-match graph cutting/continuation draining, vector/service/GPU/clustering fixtures, populated user-read fixtures, durable audit observation, constructor success assertions and HTTP mutation rejection. Original input populations, timeouts, nil/error/negative controls, allocation budget7, measured-region boundaries and per-case assertions remain required.

| Rows | Inspected current owner and retained boundary |
|---|---|
|1–5,25|spanRecorder/recordedSpan/InstallTracerProvider and registerAccessorSnapshot. Panic-ended storage span remains distinct from real Cypher→storage dispatch while sharing actual provider lifetime. ParseError also uses recordedSpan and retains original failure/status assertions.|
|6,8|runOrphanObservation declares original images/ErrNotFound-vs-first-read/timeouts; cutSoleMatchLinks and collectSoleMatchContinuation own graph lock/cut/drain. seedSoleMatchNodes owns1000-document ID/target/vector/persistence with explicit original decoration and immediate-index callback versus IVF later BuildIndexes.|
|9,11,12,15,22,24,26|newVectorService/ownVectorService/indexVectorNodes/disabledSearchGPUManager/newRoutingProfileFixture share actual setup and lifetime. Metrics/persistence/dimensions, single/mixed scripts, raw index boundary and BM25/routing finite oracles remain explicit original inputs; timed regions unchanged.|
|10,16,19,21,27|populatedUserReadFixture outside timing, observeStoredUserAudit and mustNewAuthenticator own repeated mechanics. Expected-failure constructor remains explicit; HTTP uses shared MutationFailureEngine+RejectUpdate while retaining authenticated PUT/400/stored/live controls.|
|13|Both named allowlist tests now use newSystemEngine with independent stores and original seed/copy/nil/reload/CreatedAt controls; raw-storage LoadAndHasData test remains its original boundary.|
|7,14,17,18|Canonical immutable second-UPDATE rejection case, pauseConsumerUpsert, numericUniqueClaimFixture and mustReadNode already preserve their original history/assertion controls.|
|20,23,28,29|Atomic combined-account orchestration invokes canonical operations and publishes after whole transaction; configs remain explicit constructor inputs; constraint facades retain original native/transaction read authority through one policy owner.|

The three remaining mechanics identified in review were corrected and re-inspected. No further copied mechanism or product defect is established by this bounded29-row source review; no fresh checker result is implied. Staged555a failed a stale census and the census was regenerated; no later full-gate pass or all-resolved readiness conclusion is made here. Refresh this appendix after corrections and final integrated proof.

## Lifetime fixture failure reconciliation

The archived555a full run failed both the generated census check and
`TestPR1655BeginPresenceDoesNotRetainFutureGenerations`; the earlier maintainer
summary that named only the census was incorrect. Repeating the unchanged
lifetime test reproduced three failures in100 runs.

A forced background-drain schedule showed the acknowledged generation had one
commit, was retired with its lease released, and left the expected1149-byte
initial-reader charge. The original test then sampled a different, empty active
buffer and incorrectly asserted that this replacement buffer had been retired.
A long initial interval does not prevent adaptive background draining after a
manual flush.

The lifetime cases now use the existing manual-drain test fixture with their real
memory policy and low-memory disk configuration. All16future writes, original
pin/charge/retirement assertions and cleanup controls remain. Other fixture callers
retain their background workers by default. This correction changes no production
retirement algorithm, memory-policy contract or storage format. The original and
forced failure logs are preserved; final qualification is recorded on the PR.
