# Durable cache publication: duplicate review

PR #1548 / NOR-34, target `release/v1.4.4`.

The complete initial PR-wide inventory analyzed 587 symbols and reported 87
candidates against the ingested release baseline
`343ca141f4e2516883e5f83cac12efe022a4a3b1`. The analyzer and hook were not
modified. Its SHA-256 is
`e8a5f56a53f2ee9ab3abc6a3ee0e499491f55eca4aa5992a718d0be729fd51d7`.

Candidates were compared with their actual callers, state, ownership, errors
and admission/publication ordering. Similarity scores were not used to dismiss
findings. Reciprocal rows reference the same correction. A retained boundary
means the common implementation is already shared or the remaining operations
have different contracts; it does not mean a performance exemption.

**Verification status:** final integrated `make test` passes (134 packages;
9 fresh and 125 Go-cached), source unchanged. The final remote scan timed out;
the maintainer-authorized completed inventories below remain the review guide,
not a clean final-source analyzer result. See the
[qualification and measured costs](../performance/pr1548-durable-cache-qualification.md).
The unchanged ARM SIMD header has an extraction limitation; its symbols were not
analyzed, although the Mac build compiles that header.

## Candidate-by-candidate decisions

| ID | Candidate and reference | Decision / common owner |
| --- | --- | --- |
| 0 | `pkg/storage/serialization_bench_test.go`: BenchmarkSerializeEdge() → BenchmarkSerializeNode(), BenchmarkDeserializeEdge() | Shared encode benchmark transaction kernel; decode fixture setup shared separately. |
| 1 | `pkg/storage/publication_decision.go`: WithPublicationDecision() → withQueryText() | Both typed setters already use context.WithValue, with distinct unexported keys and consumers. Publication decision carries a sealed abortable commit callback; search carries a trimmed disposable routing hint. Their only repeated primitive is nil-context normalization; a new generic setter would add a forwarding layer, without sharing arbitration or routing behavior. |
| 2 | `pkg/storage/badger_mvcc.go`: .rebuildNodeMVCCHeadsFromVersions() → .rebuildEdgeMVCCHeadsFromVersions() | One typed MVCC head rebuild scanner and batch writer. |
| 3 | `pkg/storage/badger_mvcc.go`: .rebuildEdgeMVCCHeadsFromVersions() → .rebuildNodeMVCCHeadsFromVersions() | Reciprocal of 2. |
| 4 | `pkg/storage/badger_mvcc.go`: .bootstrapNodeMVCCFromCurrentState() → .bootstrapEdgeMVCCFromCurrentState() | One typed MVCC bootstrap algorithm; typed payload batches preserve layout. |
| 5 | `pkg/storage/badger_mvcc.go`: .bootstrapEdgeMVCCFromCurrentState() → .bootstrapNodeMVCCFromCurrentState() | Reciprocal of 4. |
| 6 | `pkg/storage/commit_buffer.go`: .AppendNode() → .AppendEdge() | One Append(BufferedMutation) with canonical consumers; private replay fields/layout preserved, redundant private kind aliases also retired. Invalid requests have no side effects; nil/empty/rotation/retired lifecycle retained. |
| 7 | `pkg/storage/commit_buffer.go`: .AppendEdge() → .AppendNode() | One Append(BufferedMutation) with canonical consumers; private replay fields/layout preserved, redundant private kind aliases also retired. Invalid requests have no side effects; nil/empty/rotation/retired lifecycle retained. |
| 8 | `pkg/storage/badger_managed_failure_test.go`: TestManagedBadger_OpenFailsWhenRecoveryCannotWrite() → TestManagedBadger_LargeCommitCannotStartOnReadOnlyStore() | Shared managed-store fixture; interrupted-recovery and read-only-start assertions remain distinct. |
| 9 | `pkg/storage/badger_managed_failure_test.go`: TestManagedBadger_LargeCommitCannotStartOnReadOnlyStore() → TestManagedBadger_OpenFailsWhenRecoveryCannotWrite() | Reciprocal of 8. |
| 10 | `pkg/storage/schema.go`: .AddFulltextIndex() → .AddFulltextRelationshipIndex() | Canonical full-text creation accepts explicit entity kind; relationship forwarding entry point retired. |
| 11 | `pkg/storage/schema.go`: .AddFulltextRelationshipIndex() → .AddFulltextIndex() | Reciprocal of 10. |
| 12 | `pkg/storage/exact_case_862_branches_test.go`: writeRawNode() → writeRawEdge() | Shared raw-record test seeding; corrupt body inputs remain scenario-specific. |
| 13 | `pkg/storage/exact_case_862_branches_test.go`: writeRawEdge() → writeRawNode() | Reciprocal of 12. |
| 14 | `pkg/storage/schema_knowledgepolicy.go`: .ShowPromotionPolicies() → .ShowPromotionProfiles() | One scoped policy mutation/rollback owner retains affected maps and binding-table state on rejected persistence. A per-manager publication gate precedes schema.mu for direct persistent mutations and spans native physical commit through live schema publication; known-token fast paths avoid it. Thirteen mutation forms and six intervening schema families are exercised. Shared pointer sorting and canonical pointer-taking cloners replace the regressing large-value sort. The smaller scalar promotion-profile copy/sort stays direct because the all-pointer candidate added 14.5% bytes and a second allocation at 1,024 entries; both correct candidates were measured at 0/1/32/1,024 entries. Final integrated make test passes; measured SHOW costs are recorded below. |
| 15 | `pkg/storage/serialization_bench_test.go`: BenchmarkSerializeNode() → BenchmarkSerializeEdge() | Shared encode benchmark transaction kernel. |
| 16 | `pkg/storage/badger_edge_type_count.go`: .repairEdgeTypeCountLocked() → .repairLabelCountLocked() | One named-count repair and entry counter; literal family/key-builder choice retains compiler proof. |
| 17 | `pkg/storage/badger_label_count.go`: .repairLabelCountLocked() → .repairEdgeTypeCountLocked() | Reciprocal of 16. |
| 18 | `pkg/storage/prepared_entity_commit_prototype.go`: .RetainAcceptedAuthorityWithContext() → .RetainAcceptedAuthority() | Single RetainAcceptedAuthority(ctx); canonical callers migrated. |
| 19 | `pkg/storage/migration_v1_to_v2.go`: .migrateV1ToV2Nodes() → .migrateV1ToV2Edges() | One typed V1-to-V2 body migration with unchanged codec and key choices. |
| 20 | `pkg/storage/migration_v1_to_v2.go`: .migrateV1ToV2Edges() → .migrateV1ToV2Nodes() | Reciprocal of 19. |
| 21 | `pkg/storage/badger_cache_fill_outside_lock_test.go`: BenchmarkNodeCacheFillParallel() → BenchmarkNodeCacheFillParallelStale() | Shared untimed node-cache fixture; timed generation loads remain exactly scenario-specific. |
| 22 | `pkg/storage/badger_cache_fill_outside_lock_test.go`: BenchmarkNodeCacheFillParallelStale() → BenchmarkNodeCacheFillParallel() | Reciprocal of 21. |
| 23 | `pkg/storage/badger_cache.go`: .adjCacheStoreOutgoing() → .adjCacheStoreIncoming() | One adjacency-cache load/store owner with explicit direction. |
| 24 | `pkg/storage/badger_cache.go`: .adjCacheStoreIncoming() → .adjCacheStoreOutgoing() | Reciprocal of 23. |
| 25 | `pkg/storage/badger_edge_type_count.go`: .applyEdgeTypeCountDeltasLocked() → .applyEdgeTypeLabelCountDeltasLocked() | Six derived-count batch shells share applyCountDeltas; family-specific sign/key decoding and caller locks remain. Integrated storage tests and compiler census pass; paired count controls preserve allocations; final make test passes. |
| 26 | `pkg/storage/badger_mvcc.go`: .AppendNodeTombstone() → .AppendEdgeTombstone() | Typed MVCC tombstone entry points compose existing generic version and head writers; no second encoder or head algorithm. AppendNodeTombstone declares writtenEntities.nodes and calls shared writeMVCCVersionInTxn/writeMVCCHeadInTxn through typed node specializations. AppendEdgeTombstone declares edges and uses edge key specializations. Both keep version+head atomic in withUpdate. Sharing only the admission shell requires a new type-marker/request layer while the actual tombstone/head persistence is already canonical; no performance claim or API-compatibility veto is used. Existing MVCC append/fallback/visibility/public API tests cover both families. |
| 27 | `pkg/storage/badger_managed.go`: .beginLargeCommitContext() → .beginLargeCommit() | Retired beginLargeCommit forwarder; callers invoke beginLargeCommitContext. |
| 28 | `pkg/storage/badger_managed.go`: .update() → .Update() | badgerKV interface adapter Update(fn) supplies the fixed callback signature shared with unmanaged *badger.DB; update(ctx,fn,publication) is the sole transaction implementation. Removing the adapter requires replacing the common third-party interface, not collapsing a second implementation. Metadata method-value adaptation likewise must preserve metadata-only scope. |
| 29 | `pkg/storage/badger_write_behind.go`: .persistReplayTxnCounters() → .persistTxnCounters() | persistTxnCounters(batchTarget,drain) is the sole write/flush/mark-durable owner; canonical callers select ordinary versus held-large target. No persistDrain forwarding wrapper retained. |
| 30 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeNodeGraphitiVector() → BenchmarkDeserializeNodeGraphitiVectorProjectedScalars() | Shared untimed encoded-body fixture; distinct full/projected decode loops and assertions remain. |
| 31 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeNodeGraphitiVectorProjectedScalars() → BenchmarkDeserializeNodeGraphitiVector() | Reciprocal of 30. |
| 32 | `pkg/storage/badger_edge_type_count.go`: .decrementEdgeTypeCounts() → .decrementEdgeTypeLabelCounts() | Six derived-count batch shells share applyCountDeltas; family-specific sign/key decoding and caller locks remain. Integrated storage tests and compiler census pass; paired count controls preserve allocations; final make test passes. |
| 33 | `pkg/storage/id_dictionary_freelist_abort_test.go`: TestNumericIDCommittedWinnerSurvivesRejectedAdopter() → TestNumericIDRejectedAdopterPreservesLiveWinner() | Shared numeric-adoption fixture; winner/adopter commit order and assertions remain separate. |
| 34 | `testing/storagecensus/call_targets_test.go`: dispatchProgram() → coverageProgram() | One SSA fixture builder with explicit package and builder mode. |
| 35 | `pkg/storage/badger_temporal_helpers_extra_test.go`: TestRebuildTemporalNodeInTxn_Branches() → TestTemporalCurrentNode_Branches() | Shared temporal-constraint engine fixture; different temporal operations retain their assertions. |
| 36 | `pkg/storage/badger_write_behind.go`: .FlushWriteBehindContext() → .FlushWriteBehind() | One FlushWriteBehind(ctx), migrated consumers and guards; public BEGIN cancellation additionally reproduced and fixed. |
| 37 | `pkg/storage/badger_mvcc_internal_extra_test.go`: TestLoadAndWriteMVCCHeadForLogicalKeyInTxn_Errors() → TestWriteAndLoadMVCCHeadForLogicalKeyInTxn_NodeAndEdge() | Shared logical MVCC key builder; malformed-input and round-trip assertions remain. |
| 38 | `pkg/storage/badger_index_tombstone.go`: .WriteIndexTombstones() → .DeleteIndexTombstones() | Canonical SetIndexTombstones(keys,present); old write/delete APIs retired and callers/docs migrated. Empty no-op and marker bytes preserved. |
| 39 | `pkg/storage/badger_edges_namespace_error_branches_test.go`: seedRawNodeForEdgeNamespaceTests() → seedRawEdgeForNamespaceTests() | Shared raw-record seeding. |
| 40 | `pkg/storage/badger_edges_namespace_error_branches_test.go`: seedRawEdgeForNamespaceTests() → seedRawNodeForEdgeNamespaceTests() | Reciprocal of 39. |
| 41 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeEdge() → BenchmarkSerializeEdge() | Shared encode/decode fixture setup; encoding measures update transactions, decoding reads a prepared immutable body. |
| 42 | `pkg/storage/badger_label_index_backfill_test.go`: TestLabelIndexBackfill_LegacyMarkerRebuildsCurrentScheme() → TestLabelIndexBackfill_EmptyStoreMarksCurrentScheme() | Shared backfill fixture, exact historical and empty marker scenarios preserved. |
| 43 | `pkg/storage/property_codec_edge_test.go`: TestEncodeTokenizedProperties_ZeroLengthBuffer() → TestEncodeTokenizedProperties_Empty() | Shared property codec transaction fixture; zero/nil/empty encoding assertions preserved. |
| 44 | `pkg/storage/property_codec_edge_test.go`: TestNamespaceIsolation_PropKeyDict() → TestPropertyKeyDict_NamespaceIsolation() | Shared property dictionary transaction fixture; replaced a non-asserting cross-namespace probe with actual namespace-local stability assertions. |
| 45 | `pkg/storage/property_key_dictionary_test.go`: TestPropertyKeyDict_NamespaceIsolation() → TestNamespaceIsolation_PropKeyDict() | Reciprocal of 44. |
| 46 | `pkg/storage/buffered_write_order.go`: .lockMixedContext() → .lockMixed() | Deleted unused error-dropping lockMixed wrapper. Also retired sibling lock wrapper and migrated tests with explicit error assertions. |
| 47 | `pkg/storage/badger_publication_stage.go`: appendNodePublicationKeys() → appendEdgePublicationKeys() | Node scalar uniqueness and relationship composite uniqueness retain distinct canonical value codecs and affected-fact payloads. Their common inline-first rule addressing now uses publicationConstraintView.ruleAt directly (no callback iterator or unmeasured allocation objection). Node label before-images and edge endpoint/type before-images remain required, different facts. |
| 48 | `pkg/storage/badger_publication_stage.go`: appendEdgePublicationKeys() → appendNodePublicationKeys() | Same pair and disposition as 47. |
| 49 | `pkg/storage/serializer_migration.go`: MigrateBadgerToMsgpack() → MigrateBadgerToMsgpackWithDB() | Owned versus borrowed handle Both public migrations already call migrateBadgerToMsgpack. The owning entry opens managed Badger, recovers interrupted commits and closes it; the borrowed entry rejects nil and uses the unmanaged caller-owned WriteBatch. Moving open/close into the shared scanner would close borrowed handles or bypass managed recovery. |
| 50 | `pkg/storage/schema_knowledgepolicy.go`: .AlterDecayProfileBinding() → .CreateDecayProfileBinding() | One scoped policy mutation/rollback owner retains affected maps and binding-table state on rejected persistence. A per-manager publication gate precedes schema.mu for direct persistent mutations and spans native physical commit through live schema publication; known-token fast paths avoid it. Thirteen mutation forms and six intervening schema families are exercised. Shared pointer sorting and canonical pointer-taking cloners replace the regressing large-value sort. The smaller scalar promotion-profile copy/sort stays direct because the all-pointer candidate added 14.5% bytes and a second allocation at 1,024 entries; both correct candidates were measured at 0/1/32/1,024 entries. Final integrated make test passes; measured SHOW costs are recorded below. |
| 51 | `pkg/storage/entity_expansion_conflict_test.go`: TestNativeCompleteFootprintWaitsForSuppressionCleanup() → TestNativeCompleteFootprintCloseDrainsQueuedSuppression() | Shared suppression cleanup fixture; ordinary publication and queued-close order remain separate. |
| 52 | `pkg/storage/badger_mvcc_archive_materialize_test.go`: TestArchivePrimaryIntoMVCCVersionInTxn_NoHistoryOrMissing_NoOp() → TestArchivePrimaryIntoMVCCVersionInTxn_NodeAndEdge() | Shared archive fixture; no-op and node/edge archive assertions remain separate. |
| 53 | `pkg/storage/label_scan_bounded_test.go`: TestLabelScanLargeTailPreservesReadErrors() → TestLabelScanLargeTailStaysStreaming() | Shared bounded label-tail fixture; streaming and injected failure assertions remain separate. |
| 54 | `pkg/storage/embedding_key_scan_test.go`: TestEmbeddingRangeReaderRetainedKeysAndLongCursor() → TestEmbeddingRangeReaderSnapshotsAndStagedWrites() | Distinct range-read scenarios Both use the existing memory-engine fixture and visitEmbeddingPrefixKeys. The long-key case retains owned cursor/key images and prunes child shards; the other holds an old snapshot, applies deletes/TTL/staged writes and tests resume/stop/error. No repeated body-building or collection operation remains to share. |
| 55 | `pkg/storage/badger_transaction_publication_controls_test.go`: publishReservedPublication() → stageReservedPublication() | Two deliberate protocol phases stageReservedPublication validates/allocates/materializes/flushes but deliberately leaves publication pending. publishReservedPublication commits and closes. These consecutive operations create the externally observed reservation window; neither reimplements the other. |
| 56 | `pkg/storage/badger_close_commit_race_test.go`: TestBadgerEngine_HelperUpdateTailSurvivesConcurrentClose() → TestBadgerEngine_NonTransactionalWriteTailSurvivesConcurrentClose() | Shared concurrent-close fixture closeWhileTailRuns already owns hook installation and peer Close scheduling. The helper-write test checks post-close refusal; CreateNode test reopens and checks the separately persisted numeric high-water mark. Different writes/outcome assertions remain; shared scheduling already has one owner. |
| 57 | `pkg/storage/badger_cache.go`: .cacheLoadEdgeForRead() → .cacheLoadEdge() | Unversioned versus pinned cache admission cacheLoadEdge reads the immutable current edge map under edgeCacheMu. cacheLoadEdgeForRead additionally holds durableCaches.mu, reads edgeCacheSources and refuses zero/newer versions than the captured transaction. They have no common state transition; extracting one map lookup would only rename that primitive. No performance-based separation claim is made. |
| 58 | `pkg/storage/badger.go`: .writeSchemaVersion() → .readSchemaVersion() | Record read versus write readSchemaVersion and writeSchemaVersion already reuse accessSchemaVersionInTxn. Read opens a View and accepts the historical one-byte marker; write stages exactly one big-endian value under metadata publication. Moving either transaction direction/acceptance rule into the codec would mix transaction ownership with representation. |
| 59 | `pkg/storage/badger_mvcc_bootstrap_extra_test.go`: TestApplyEdgeBootstrapBatch_NilAndEmpty() → TestApplyEdgeBootstrapBatch_SkipsExistingHead() | No-op versus retained-head assertion Nil/empty batch test has no entity seeding; the other seeds endpoints and an edge, records its head, re-applies bootstrap and asserts its version unchanged. Engine creation already has a common fixture, and both call the same canonical bootstrap owner. |
| 60 | `pkg/storage/badger_queries.go`: projectCachedNodeForRead() → copyNodeProjected() | Storage cache projection owns/canonicalizes selected mutable values, labels and optionally embedding payloads across cache/buffer reads. Cypher fallback selects properties on already read-only stream input and preserves its input value representation. Both use existing storage/public input owners; forcing canonicalization into the borrowed fallback changes the contract, while a new exported selectable-ownership facade adds a second policy surface just for a three-line selection loop. No throughput or allocation objection is inferred. |
| 61 | `pkg/storage/schema_knowledgepolicy.go`: .AlterPromotionPolicyDefinition() → .AlterPromotionPolicy() | One scoped policy mutation/rollback owner retains affected maps and binding-table state on rejected persistence. A per-manager publication gate precedes schema.mu for direct persistent mutations and spans native physical commit through live schema publication; known-token fast paths avoid it. Thirteen mutation forms and six intervening schema families are exercised. Shared pointer sorting and canonical pointer-taking cloners replace the regressing large-value sort. The smaller scalar promotion-profile copy/sort stays direct because the all-pointer candidate added 14.5% bytes and a second allocation at 1,024 entries; both correct candidates were measured at 0/1/32/1,024 entries. Final integrated make test passes; measured SHOW costs are recorded below. |
| 62 | `pkg/storage/badger_managed.go`: .waitPublishedContext() → .waitPublished() | Single waitPublished(ctx,ts); non-cancellable success tails pass nil explicitly. |
| 63 | `testing/storagecensus/selector.go`: knownArgument() → conditionKnown() | Shared integerConstant distinguishes integer kind from representability; branch predicates, known arguments and variadic-slice bounds use it. New overflow control proves a literal cannot fall back to an assumed selector value. |
| 64 | `pkg/storage/badger_nodes_edges_mvcc_extra_test.go`: TestBadgerEngine_DeleteEdgeFamily_AdjacencyLoadErrors() → TestBadgerEngine_QueryHelpers_Extra() | Parent rejected optional deferral: extracted all three corrupt-edge fixtures; separate query-helper test retains label/cache/early-stop assertions. |
| 65 | `pkg/storage/badger_managed.go`: .Set() → .Set() | Physical batch versus logical staging managedWriteBatch.Set invokes batchWriter.write and physical cache projection before Txn.Set; stagedKV.Set removes pendingDeletes and assigns pendingWrites without a physical write. Their only common input shape is key/value; sharing the state mutation would change visibility, ownership and flush semantics. |
| 66 | `pkg/storage/badger_commit_writer.go`: .finish() → .finish() | Commit orchestration versus large-commit completion commitWriter.finish persists tokens and selects ordinary versus large completion. The large branch calls largeCommit.finish, which syncs batches, removes the intent, flushes, registers publication, releases the gate and applies. Replacing either with the other loses its required protocol stage; actual token/batch/publication primitives are already shared. |
| 67 | `pkg/storage/badger_managed.go`: .finish() → .finish() | Reciprocal Same pair and disposition as 66. |
| 68 | `pkg/storage/badger_count_deltas.go`: .foldCountDeltaKeysChunked() → .foldCountDeltaKeys() | Bounded driver over atomic fold foldCountDeltaKeysChunked only partitions keys by countDeltaFoldChunk and calls foldCountDeltaKeys. The latter sums/deletes exactly one partition and writes base counts atomically under its selected admission. The common fold body is already singular; inlining it into the driver would duplicate direct unchunked consumers or remove transaction bounds. |
| 69 | `pkg/storage/badger_cache.go`: .cacheStoreNode() → .cacheStoreNodeBody() | Different retained cache representations Full-node storage uses copyNode, installs immutable embedding-owner identity, invalidates node generation, inserts full state and removes a body-only entry. Body-only storage refuses unreconstructible inline vectors, uses copyNodeWithoutEmbeddings, enforces byte/entry caps and LRU accounting. Both already use node-floor/copy/cache-generation primitives; no duplicate installation body remains. No unmeasured performance objection is used. |
| 70 | `pkg/storage/badger_transaction.go`: .GetMetadata() → .SetMetadata() | Metadata input versus output SetMetadata admits the transaction, validates the byte cap and merges owned entries into retained state. GetMetadata returns an owned independent map without mutating the transaction. Both call ownedEntries; that is the entire common ownership algorithm. Admission and directional assignment are intentionally not repeated. |
| 71 | `pkg/storage/badger_commit_writer.go`: .VisitAffectedFacts() → .Visit() | writerCacheFacts composes publication keys, raw physical projections and declared cache scope through their existing VisitAffectedFacts owner. resultmetadata.Facts.Visit dispatches three known/not-applicable/unresolved outcomes, copies known statement metadata, and carries errors. No duplicated fact derivation or result-outcome decision exists between them. |
| 72 | `pkg/storage/badger_label_index_backfill.go`: .markIndexBackfillReady() → .indexBackfillReady() | Backfill marker read versus write Both select the same existing family key constants. Read checks exact label scheme byte or edge-key presence in View; write emits established marker bytes under metadata withUpdate. Sharing a key-constant selection alone would add an indirection without sharing either operation or acceptance rule. |
| 73 | `pkg/storage/badger_txn_helpers.go`: .beginOrderedWrite() → .beginBarrierWrite() | Removed orderedWrite adapter; existing writeBarrierLease now owns notification completion. |
| 74 | `pkg/storage/badger.go`: .reopenForPostMigrationCompaction() → .loadFromBadger() | Lifecycle orchestration versus dictionary load reopenForPostMigrationCompaction closes/reopens the managed handle and calls both dictionary loaders, resets WAL identity, MVCC sequence and counts, closing the new handle on any error. idDictionary.loadFromBadger only reconstructs dictionary indexes from the supplied handle. Already actual caller-to-owner reuse; no alternate dictionary scanner exists here. |
| 75 | `pkg/storage/badger_managed.go`: .commit() → .commitShared() | Completion wait over shared physical commit commit calls commitShared, handles its error, and waits for publication of its timestamp. commitShared owns gate/timestamp/durable-write/stage registration and returns timestamp to consumers that perform their own completion. This is required success-tail orchestration, not a second commit implementation. Both paths already share every physical action. |
| 76 | `pkg/storage/badger_txn_helpers.go`: .beginBarrierWrite() → .beginOrderedWrite() | Reciprocal of 73. |
| 77 | `pkg/storage/wal_entity_commit_prototype_test.go`: assertAcceptedStoredImage() → assertStoredNodeImage() | Different image equivalence contracts assertStoredNodeImage compares whole Node after dropping embedding identity, including runtime database/storage-lane fields. assertAcceptedStoredImage compares exported Node/Edge durable semantic fields and time.Equal timestamps, explicitly checks suppression but excludes runtime-only fields. Sharing the full-struct assertion would reject legitimate WAL hydration; sharing the reflection assertion would weaken runtime-field assertions. Beyond require comparisons, there is no identical normalization operation. |
| 78 | `testing/storagecensus/enforce.go`: .flow() → .mutexField() | flow is cached CFG acquisition gen/kill analysis and already calls mutexField to identify each lock receiver. mutexField resolves one SSA value to a unanimous field identity; it does not compute hold spans. Predicate parsing/selector implication and call-target resolution are already shared and tested with ambiguity/ignored-error mutants. |
| 79 | `pkg/storage/adjacency_read.go`: readAdjacency() → .captureAdjacencyReader() | readAdjacency routes namespace/request/direction and partial-error ordering, and is the only caller of captureAdjacencyReader. The latter captures one physical view, generations, caches and overlay, returning a reader/close capability. Combining capture into routing would erase native provider ownership; there is no second capture body to remove. |
| 80 | `pkg/storage/affected_facts.go`: .VisitAffectedFacts() → .Visit() | publicationKeys translates NodeID/EdgeID/AffectedFact inputs into affected-scope facts; resultmetadata.Facts.Visit dispatches outcome variants with statement copies/errors. Payload, callback arity, error and ownership contracts differ; callback invocation itself is the shared language primitive, not a duplicated algorithm. |
| 81 | `pkg/storage/badger_commit_writer.go`: .runPublication() → .publication() | Publication description versus execution publication constructs the key/owner/metadata/callback descriptor or nil. runPublication executes stored effects, pending embeddings, callback and final cache images. The descriptor directly reuses runPublication as its callback. Combining them would run effects while merely constructing an uncommitted descriptor. |
| 82 | `testing/storagecensus/call_targets_test.go`: lockCall() → lockCall() | Census lockCall finds the single SSA call named Lock in a test fixture. Inference lockCall classifies an AST statement/defer against a receiver string via existing lockeffect.Classify. Different IR/result/consumers; neither contains a second AST or SSA classification implementation. |
| 83 | `testing/storagecensus/enforce.go`: .syncParam() → .syncParamOf() | syncParam resolves actual call-site target/receiver argument mapping and async boundaries, then calls syncParamOf. The latter memoizes whether one parameter escapes by calling syncValue. Existing direct delegation shares the only parameter proof; recursion and dynamic dispatch are distinct parts of that proof. |
| 84 | `pkg/storage/admission_wait_observer.go`: setAdmissionWaitObserver() → .SetObserver() | The storage observer setter swaps/restores one process-global atomic test callback for admission wait controls. Registry.SetObserver installs a per-registry production metrics observer, handles nil and immediately seeds count/retained bytes. Atomic publication is already the common primitive; registration lifetime and required initial notification differ. |
| 85 | `pkg/storage/badger_commit_writer.go`: .newCommitWriter() → .commitEngineWrite() | Constructor reused by commit owner newCommitWriter initializes ctx/engine/db/batchWriter only. commitEngineWrite calls it, owns transaction lifetime, executes write units, aborts on failure, commits and persists counters. Constructor has no duplicate commit, error or publication logic to merge. |
| 86 | `pkg/storage/wal_engine.go`: NewWALEngine() → .logger() | Logger access versus engine construction logger returns the configured WAL logger or discard fallback, without creating state. NewWALEngine constructs cancellation lifetime, typed engine/key registry, WAL binding and initial compaction watermark. No common initialization or logging implementation exists in these two functions; this conclusion is based on complete bodies, not similarity score. |

## Corrections established by this review

Rejected policy persistence previously left a changed live map, and could
carry the rejected edit into a later durable definition. Policy changes now
stage only the affected family, retain published state on rejection, and compile
bindings only after success. Constraint-add rollback preserves existing runtime
index/value caches instead of reconstructing an unrelated empty schema.

An independent control also reproduced an acknowledged direct policy write
disappearing across a native publication gap. The current owner takes engine
admission before the schema lock, persists under that lock and releases both
before invoking callbacks. The checked-in ordering test pauses before publication
and verifies memory and reopen outcomes. Other schema-mutator investigations
that passed did not result in speculative product changes.

Public Cypher BEGIN now carries cancellation into its physical flush admission.
The fail-first public regression and race-enabled checks cover this actual path,
rather than only the flush helper.

Go API migrations are documented in [replicated entity commits](replicated-entity-commits.md)
and [full-text schema creation](schema-fulltext-go-api.md). Badger keys, stored
payloads, WAL bytes and the storage version are unchanged.

A rejected temporal constraint or compiled-contract drop also lost history-index
trust marks. The fix retains the exact prior complete/incomplete state in the
existing rollback action. Its public regression first demonstrates an overlapping
node being accepted by the old code, then verifies rejection and scan enforcement
after the correction. Successful drops still clear both marks.

The initial generic SHOW projection reduced allocations but regressed large decay
families because it passed whole values through an identity callback. That
candidate is not performance-qualified; a measured correction is required before
readiness. Count-batch consolidation retains allocation parity across empty,
single-key, 32-key and 1024-key workloads.

## Qualified SHOW convergence choice

Apple M2 Max, Go 1.26.9, GOMAXPROCS=1. The external paired experiment
compiled the correct pre-convergence SHOW loops, the first shared owner and two
pointer-order candidates against the same package checkout using source overlays.
The reference overlay also contains older policy admission/count code, which is
not called by the timed SHOW loops: every arm seeds the same private maps directly. Ordering,
non-nil empty output, input/output ownership and existing policy tests passed.
Fixture construction was followed by GC before timing. Twelve alternating
samples covered each size (0/1/32/1,024); eight separate-process samples
rechecked the 1,024-definition shapes with normal GC and GC disabled.

The integrated candidate sorts source pointers for the three larger families,
then creates the owned results using the canonical cloners. It keeps the scalar
promotion-profile value copy/sort direct. There is no forwarding clone adapter.

| SHOW workload, 1,024 definitions | Correct pre-convergence median | Integrated candidate median | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Decay bundles and bindings, normal GC | 729,502.5 ns | 412,709.5 ns | 442,784 → 461,312 | 3,080 → 3,076 |
| Promotion policies, normal GC | 570,691 ns | 408,376 ns | 442,552 → 451,840 | 6,148 → 6,146 |
| Promotion profiles, normal GC | 187,720.5 ns | 172,901.5 ns | 65,672 → 65,536 | 4 → 1 |
| Promotion profiles, GC disabled | 126,357 ns | 129,686 ns | 65,672 → 65,536 | 4 → 1 |

The retained scalar path has a 2.6% median timing increase in the GC-disabled
control and improves in the normal-GC runs; it is not claimed faster in every
configuration. The all-pointer profile candidate is faster but uses 75,008
bytes/op and two allocations, versus 65,536 and one for the chosen path.
That concrete buffer cost, rather than assumed coupling cost, is why the small
scalar family does not use pointer sorting. Rich-family pointer buffers add
4.2%/2.1% bytes at this size while reducing time and allocation count.

The local Qwen server was resident but idle, Docker had no running containers,
and the advisory checker ran concurrently. These are paired local workload
measurements, not a completely isolated system benchmark or Northwind result.
Raw samples, source hashes, candidate patches and earlier rejected experiments
are preserved in the task's external readiness evidence. The final complete `make test` now qualifies the integrated runtime/test source;
see the linked qualification report for the checked tree and cache usage.

## Refreshed whole-PR inventory

The next unchanged run analyzed 980 functions on staged tree
`a9689668c79bd8e8b836acd9f76d340d3b5bfc81`: 75 reuse candidates, no refactor
targets, cap skips or unsearched symbols. Index and ingested baseline stayed
unchanged during that run. This snapshot predates the final SHOW, schema API,
fixture and reviewed-population updates, and the final remote scan timed out. The maintainer authorized the completed
inventory as the review guide. Every candidate is mapped below; an initial-row reference also retains
its complete contract comparison above.

| Refreshed ID | Candidate | Disposition |
| --- | --- | --- |
| 0 | `pkg/storage/publication_decision.go`: WithPublicationDecision() | Initial inventory 1: Both typed setters already use context.WithValue, with distinct unexported keys and consumers. Publication decision carries a sealed abortable commit callback; search carries a trimmed disposable routing hint. Their only repeated primitive is nil-context normalization; a new generic setter would add a forwarding layer, without sharing arbitration or routing behavior. |
| 1 | `pkg/cypher/call_compat.go`: .callDbIndexFulltextCreateNodeIndex() | One typed full-text procedure owner; dispatch calls it directly with entity kind, preserving procedure/argument/error names and row shapes. Old handlers retired. Focused public and argument tests plus reverted entity-swap mutation prove dispatch sensitivity. |
| 2 | `pkg/cypher/call_compat.go`: .callDbIndexFulltextCreateRelationshipIndex() | Reciprocal of refreshed row 1; same canonical full-text procedure owner. |
| 3 | `pkg/cypher/transaction_script_test.go`: TestExecuteInTransaction_SuccessPath() | Shared successful execute-in-transaction assertion/scenario; namespace, query, columns and value stay caller inputs. Named tests retained with mutation controls. |
| 4 | `pkg/cypher/transaction_script_test.go`: TestExecuteInTransaction_SuccessPath_NoNamespace() | Reciprocal of refreshed row 3. |
| 5 | `pkg/cypher/transaction_script_test.go`: TestTransactionHandleRollback_ErrorBranchAfterClosedTx() | Shared closed-transaction arrangement/assertion owner; commit versus rollback handler and error substring remain explicit inputs. Distinct assertions retained. |
| 6 | `pkg/cypher/transaction_script_test.go`: TestTransactionHandleCommit_ErrorBranchAfterClosedTx() | Reciprocal of refreshed row 5. |
| 7 | `pkg/cypher/administration_review_test.go`: TestRejectedSystemStatementFailsTransaction() | Shared administration executor/BEGIN/accepted-grant setup; rejected statement and commit/grant outcomes remain in each named test. |
| 8 | `pkg/cypher/administration_routing_test.go`: TestAdministrationTransactionOpenedOnUserDatabase() | Reciprocal of refreshed row 7. |
| 9 | `pkg/storage/id_dictionary_freelist_abort_test.go`: newAdoptionStore() | Retired newAdoptionStore; callers use existing testfixture.OwnResult directly. In-memory input preserved because disk fixture starts background cleanup/count workers and changes the freelist TTL schedule. Shared lifetime owner, no manual cleanup wrapper. |
| 10 | `pkg/storage/badger_write_behind_largegen_test.go`: TestWriteBehind_LargeGenerationFlushSpansBadgerBatches() | One 25,000-node write-behind staging/commit/flush/drain sequence with exact per-case node factories. Both named tests and fresh-property-token input retained; focused and race runs pass. |
| 11 | `pkg/storage/schema_fulltext_relationship_test.go`: TestFulltextIndex_DropIndex_PreservesSiblings() | Shared fulltextSiblingIndexFixture creates both index kinds; each named test retains its own drop direction and exact survivor assertions. Parent rejected the claim that differing outcomes prohibit shared setup. |
| 12 | `pkg/storage/schema_fulltext_relationship_test.go`: TestFulltextIndex_DropIndex_DropNodePreservesRel() | Reciprocal of refreshed row 11. |
| 13 | `pkg/storage/badger_mvcc.go`: .AppendNodeTombstone() | Initial inventory 26: Typed MVCC tombstone entry points compose existing generic version and head writers; no second encoder or head algorithm. |
| 14 | `pkg/storage/badger_managed.go`: .update() | Initial inventory 28: badgerKV interface adapter |
| 15 | `pkg/storage/badger_label_index_backfill_test.go`: openBackfillStore() | Retired openBackfillStore; callers use existing openBackfillTestEngine(t,dir,false). Constructor option equivalence verified; interval is read only when write-behind is enabled. Reopen/marker assertions retained. |
| 16 | `pkg/storage/badger_index_tombstone_test.go`: TestIndexTombstone_LabelIndexSkipsTombstoned() | One label-index tombstone lookup/write fixture owner takes label and node identity. Each test retains its seed population, reveal override and read assertions; original compact two-node seed retained. |
| 17 | `pkg/cypher/range_semantics_test.go`: TestUnwindRangeExplicitTransactionFiltersBeforeMaterializingRows() | Shared range query and allocation/materialization assertion; explicit namespace/BEGIN/rollback and auto-commit parser warm-up remain separate. Named tests and sensitivity controls retained. |
| 18 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeEdge() | Initial inventory 41: Shared encode/decode fixture setup; encoding measures update transactions, decoding reads a prepared immutable body. |
| 19 | `pkg/cypher/transaction_visibility_test.go`: TestExplicitTransactionCreatesDependentWritesFromAcknowledgedState() | Shared acknowledged write plus BEGIN fixture; read result, dependent writes and terminal commit/rollback behavior remain in the owning tests. |
| 20 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeNodeGraphitiVector() | Initial inventory 30: Shared untimed encoded-body fixture; distinct full/projected decode loops and assertions remain. |
| 21 | `pkg/storage/badger_mvcc_internal_extra_test.go`: TestWriteAndLoadMVCCHeadForLogicalKeyInTxn_NodeAndEdge() | Existing mvccHeadLogicalKey is the common key encoder; both use newTestEngine. Round-trip write/read verifies actual versions and tombstones; malformed/unknown-prefix inputs test error classifications. No second key/codec implementation remains. |
| 22 | `pkg/storage/badger_mvcc_bootstrap_extra_test.go`: TestBootstrapNodeMVCCFromCurrentStateInTxn_PopulatesHead() | Shared bootstrap update-transaction and persisted-head read fixture owners. Missing-head deletion/ErrNotFound and existing-head before/after equality remain distinct assertions. Focused and race cases pass. |
| 23 | `pkg/storage/serialization_bench_test.go`: BenchmarkDeserializeNodeGraphitiVectorProjectedScalars() | Initial inventory 31: Reciprocal of 30. |
| 24 | `pkg/storage/id_dictionary_freelist_abort_test.go`: TestNumericIDCommittedWinnerSurvivesRejectedAdopter() | Initial inventory 33: Shared numeric-adoption fixture; winner/adopter commit order and assertions remain separate. |
| 25 | `pkg/server/hidden_relationship_delete_test.go`: TestSuppressedRelationshipReMergeKeepsCounts() | Shared hiddenDeleteServer success/error assertion and physical flush owners; protocol transport already shared. Quota, physical counter/reopen and multi-row ON MATCH behavior retain separate assertions; focused group passes. |
| 26 | `pkg/storage/badger_edge_type_count.go`: .applyEdgeTypeCountDeltasLocked() | Initial inventory 25: Six derived-count batch shells share applyCountDeltas; family-specific sign/key decoding and caller locks remain. Integrated storage tests and compiler census pass; paired count controls preserve allocations; final make test passes. |
| 27 | `pkg/storage/serializer_migration.go`: MigrateBadgerToMsgpack() | Initial inventory 49: Owned versus borrowed handle |
| 28 | `pkg/storage/pr1655_staged_targets_acceptance_test.go`: TestPR1655StagedTargetVanishedBeforeCommit() | Existing acceptance engine, seed, writer and scenario owners are shared. Deletion after staging versus during staging changes peer/snapshot/flush populations and conflict phase. Each owns distinct commit-veto, no-resurrection and no-dangling-edge observations, not a duplicate mutation algorithm. |
| 29 | `pkg/storage/pr1655_staged_targets_acceptance_test.go`: TestPR1655VanishedTargetAtStaging() | Reciprocal of refreshed row 28. |
| 30 | `pkg/storage/schema_knowledgepolicy.go`: alterKnowledgeProfile() | Create and alter now call one publishKnowledgeProfile validation/stage/commit owner. Creation handles duplicate-name/IF NOT EXISTS; alteration handles lookup/options on a local value copy. No repeated accepted/rejected state transition. |
| 31 | `pkg/storage/embedding_key_scan_test.go`: TestEmbeddingRangeReaderRetainedKeysAndLongCursor() | Initial inventory 54: Distinct range-read scenarios |
| 32 | `pkg/storage/schema_knowledgepolicy.go`: createKnowledgeProfile() | Reciprocal of refreshed row 30. |
| 33 | `pkg/storage/badger_publication_stage.go`: appendNodePublicationKeys() | Initial inventory 47: Node scalar uniqueness and relationship composite uniqueness retain distinct canonical value codecs and affected-fact payloads. Their common inline-first rule addressing now uses publicationConstraintView.ruleAt directly (no callback iterator or unmeasured allocation objection). Node label before-images and edge endpoint/type before-images remain required, different facts. |
| 34 | `pkg/storage/badger_cache_fill_outside_lock_test.go`: BenchmarkNodeCacheFillParallel() | Initial inventory 21: Shared untimed node-cache fixture; timed generation loads remain exactly scenario-specific. |
| 35 | `pkg/storage/badger_transaction_publication_controls_test.go`: publishReservedPublication() | Initial inventory 55: Two deliberate protocol phases |
| 36 | `pkg/storage/badger_close_commit_race_test.go`: TestBadgerEngine_HelperUpdateTailSurvivesConcurrentClose() | Initial inventory 56: Shared concurrent-close fixture |
| 37 | `pkg/storage/badger_cache_fill_outside_lock_test.go`: BenchmarkNodeCacheFillParallelStale() | Initial inventory 22: Reciprocal of 21. |
| 38 | `pkg/storage/badger_cache.go`: .cacheLoadEdgeForRead() | Initial inventory 57: Unversioned versus pinned cache admission |
| 39 | `pkg/storage/serialization_bench_test.go`: BenchmarkSerializeEdge() | Initial inventory 0: Shared encode benchmark transaction kernel; decode fixture setup shared separately. |
| 40 | `pkg/storage/property_codec_edge_test.go`: TestEncodeTokenizedProperties_ZeroLengthBuffer() | Initial inventory 43: Shared property codec transaction fixture; zero/nil/empty encoding assertions preserved. |
| 41 | `pkg/storage/badger_queries.go`: projectCachedNodeForRead() | Initial inventory 60: Storage cache projection owns/canonicalizes selected mutable values, labels and optionally embedding payloads across cache/buffer reads. Cypher fallback selects properties on already read-only stream input and preserves its input value representation. Both use existing storage/public input owners; forcing canonicalization into the borrowed fallback changes the contract, while a new exported selectable-ownership facade adds a second policy surface just for a three-line selection loop. No throughput or allocation objection is inferred. |
| 42 | `pkg/storage/label_scan_bounded_test.go`: TestLabelScanLargeTailStaysStreaming() | Both use existing labelScanFixture and labelTailScan snapshot lifetime. Streaming test verifies no tail decode/read-ahead and early-stop order; error test injects deleted/corrupt tail rows across snapshot/projection modes. Setup/read scanner is already canonical. |
| 43 | `pkg/storage/badger.go`: .writeSchemaVersion() | Initial inventory 58: Record read versus write |
| 44 | `pkg/storage/schema_knowledgepolicy.go`: .ShowPromotionProfiles() | Measured all-pointer candidate adds 14.5% bytes and a second allocation for the 64-byte scalar profile at 1,024 entries. Integrated mixed candidate uses shared pointer sort/deep cloners for rich families and direct scalar value sort; correct alternatives, all sizes and two replicates recorded. |
| 45 | `pkg/storage/badger_mvcc_bootstrap_extra_test.go`: TestApplyEdgeBootstrapBatch_NilAndEmpty() | Initial inventory 59: No-op versus retained-head assertion |
| 46 | `pkg/storage/schema_knowledgepolicy.go`: .AlterPromotionPolicyDefinition() | Initial inventory 61: One scoped knowledge-policy mutation/rollback owner retains affected map and binding-table state on rejected persistence; engine admission precedes schema.mu and excludes native publication gaps. Shared typed SHOW clone/sort owner preserves per-family copy depth. Thirteen-form tests and paired cost qualification pass; costs and limits are recorded in the qualification report. |
| 47 | `pkg/storage/badger_mvcc_archive_materialize_test.go`: TestArchivePrimaryIntoMVCCVersionInTxn_NoHistoryOrMissing_NoOp() | Initial inventory 52: Shared archive fixture; no-op and node/edge archive assertions remain separate. |
| 48 | `pkg/storage/property_codec_edge_test.go`: TestNamespaceIsolation_PropKeyDict() | Initial inventory 44: Shared property dictionary transaction fixture; replaced a non-asserting cross-namespace probe with actual namespace-local stability assertions. |
| 49 | `testing/storagecensus/integer_constant_test.go`: TestIntegerSelectorLiteralBoundaries() | SSA constant kind/representability and CFG assumption inference versus executed Cypher decimal/hex/octal value semantics. No shared parser/classifier/fixture implementation; tests already use the language/SSA owners. A generic assertion wrapper would share only testing primitives. |
| 50 | `pkg/storage/badger_managed.go`: .Set() | Initial inventory 65: Physical batch versus logical staging |
| 51 | `pkg/storage/badger_write_behind.go`: .persistReplayTxnCounters() | Initial inventory 29: persistTxnCounters(batchTarget,drain) is the sole write/flush/mark-durable owner; canonical callers select ordinary versus held-large target. No persistDrain forwarding wrapper retained. |
| 52 | `pkg/storage/badger_managed.go`: .finish() | Initial inventory 67: Reciprocal |
| 53 | `pkg/storage/badger_count_deltas.go`: .foldCountDeltaKeysChunked() | Initial inventory 68: Bounded driver over atomic fold |
| 54 | `pkg/storage/badger_commit_writer.go`: .finish() | Initial inventory 66: Commit orchestration versus large-commit completion |
| 55 | `pkg/storage/badger_cache.go`: .cacheStoreNode() | Initial inventory 69: Different retained cache representations |
| 56 | `testing/storagecensus/selector.go`: knownArgument() | Initial inventory 63: Shared integerConstant distinguishes integer kind from representability; branch predicates, known arguments and variadic-slice bounds use it. New overflow control proves a literal cannot fall back to an assumed selector value. |
| 57 | `pkg/storage/badger_transaction.go`: .GetMetadata() | Initial inventory 70: Metadata input versus output |
| 58 | `pkg/storage/badger_commit_writer.go`: .VisitAffectedFacts() | Initial inventory 71: writerCacheFacts composes publication keys, raw physical projections and declared cache scope through their existing VisitAffectedFacts owner. resultmetadata.Facts.Visit dispatches three known/not-applicable/unresolved outcomes, copies known statement metadata, and carries errors. No duplicated fact derivation or result-outcome decision exists between them. |
| 59 | `pkg/storage/schema_policy_persistence_regression_test.go`: newPolicyPersistenceSeeded() | newPolicyPersistenceSeeded is genuinely reused by the thirteen-form rejected-persistence fixture. Hook test must start with a bare manager to assert exactly the two profiles around SetPersister; broad policy seed would change its required definition. NewSchemaManager is already the shared constructor. |
| 60 | `pkg/storage/badger_label_index_backfill.go`: .markIndexBackfillReady() | Initial inventory 72: Backfill marker read versus write |
| 61 | `pkg/storage/schema.go`: .addPropertyTypeConstraint() | One AddPropertyTypeConstraint with typed PropertyTypeConstraintOptions; WithOptions/private forwarding entry points retired and callers/docs migrated. Existing admission/staging/persist owner unchanged. Five consumer packages and focused schema races pass. |
| 62 | `pkg/storage/schema.go`: .AddVectorIndexForEntity() | One explicit-kind AddVectorIndex; ForEntity/node-only wrapper retired. Callers/docs migrated, existing zero-kind normalization and stored representation preserved. No performance objection or forwarding layer retained. |
| 63 | `pkg/storage/badger.go`: .reopenForPostMigrationCompaction() | Initial inventory 74: Lifecycle orchestration versus dictionary load |
| 64 | `pkg/storage/badger_managed.go`: .commit() | Initial inventory 75: Completion wait over shared physical commit |
| 65 | `pkg/storage/wal_entity_commit_prototype_test.go`: assertAcceptedStoredImage() | Initial inventory 77: Different image equivalence contracts |
| 66 | `testing/storagecensus/enforce.go`: .flow() | Initial inventory 78: flow is cached CFG acquisition gen/kill analysis and already calls mutexField to identify each lock receiver. mutexField resolves one SSA value to a unanimous field identity; it does not compute hold spans. Predicate parsing/selector implication and call-target resolution are already shared and tested with ambiguity/ignored-error mutants. |
| 67 | `pkg/storage/adjacency_read.go`: readAdjacency() | Initial inventory 79: readAdjacency routes namespace/request/direction and partial-error ordering, and is the only caller of captureAdjacencyReader. The latter captures one physical view, generations, caches and overlay, returning a reader/close capability. Combining capture into routing would erase native provider ownership; there is no second capture body to remove. |
| 68 | `pkg/storage/affected_facts.go`: .VisitAffectedFacts() | Initial inventory 80: publicationKeys translates NodeID/EdgeID/AffectedFact inputs into affected-scope facts; resultmetadata.Facts.Visit dispatches outcome variants with statement copies/errors. Payload, callback arity, error and ownership contracts differ; callback invocation itself is the shared language primitive, not a duplicated algorithm. |
| 69 | `pkg/storage/badger_commit_writer.go`: .runPublication() | Initial inventory 81: Publication description versus execution |
| 70 | `testing/storagecensus/call_targets_test.go`: lockCall() | Initial inventory 82: Census lockCall finds the single SSA call named Lock in a test fixture. Inference lockCall classifies an AST statement/defer against a receiver string via existing lockeffect.Classify. Different IR/result/consumers; neither contains a second AST or SSA classification implementation. |
| 71 | `testing/storagecensus/enforce.go`: .syncParam() | Initial inventory 83: syncParam resolves actual call-site target/receiver argument mapping and async boundaries, then calls syncParamOf. The latter memoizes whether one parameter escapes by calling syncValue. Existing direct delegation shares the only parameter proof; recursion and dynamic dispatch are distinct parts of that proof. |
| 72 | `pkg/storage/admission_wait_observer.go`: setAdmissionWaitObserver() | Initial inventory 84: The storage observer setter swaps/restores one process-global atomic test callback for admission wait controls. Registry.SetObserver installs a per-registry production metrics observer, handles nil and immediately seeds count/retained bytes. Atomic publication is already the common primitive; registration lifetime and required initial notification differ. |
| 73 | `pkg/storage/badger_commit_writer.go`: .newCommitWriter() | Initial inventory 85: Constructor reused by commit owner |
| 74 | `pkg/storage/wal_engine.go`: NewWALEngine() | Initial inventory 86: Logger access versus engine construction |

The fixture correction also retired duplicated ignored Close cleanup from
owned memory fixtures. AST binding, exact closure-body and cleanup-order guards
preserve explicit closes/reopen tests and other cleanup effects. The existing
`testfixture.Own` remains the canonical lifetime owner; no replacement owner was
introduced. All 136 affected test functions and lifetime guards passed focused
checks; final full-suite make test qualification also passes.


## Final scanner and suite status

The final frozen remote scan failed with `/nornicdb/search: timed out`; it did
not produce a complete report. Per the maintainer's explicit fallback direction,
the completed 980-function/75-candidate inventory and initial 87-row review above
remain the investigation record. This is not a clean final-source checker pass.
No analyzer or hook changes were made.

The first completed final `make test` failed only its wall-clock fallback cost
assertion. The replacement measures allocated work in the actual Badger lookup
at unchanged hub degrees 1,000/16,000 and ratio bound 4. Three repetitions use
28 allocations in either case. A deliberate outgoing-only hub scan fails at
15,038/240,055 allocations (15.96x); runtime source was restored afterwards.
Result/error assertions remain, and the existing short-mode skip was removed.
This proves sensitivity to the original allocating hub scan; it is not a general
CPU bound against arbitrary allocation-free alternative algorithms. The subsequent full make test passes on unchanged source, including this
correction; its exact inputs and cache usage are in the qualification report.


The normal commit hook on implementation commit `7189928a8` also ended with
`Duplicate comparison deadline reached` in advisory mode. It did not emit a clean
result. The complete inventories and concrete dispositions above remain the
explicitly authorized guide; no hook or analyzer was altered to commit.
