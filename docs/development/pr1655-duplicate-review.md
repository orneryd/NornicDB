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
