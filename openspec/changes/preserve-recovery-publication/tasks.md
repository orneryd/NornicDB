## Recovery publication corrections (report items 13, 45, 46, 53.5)

Scope: journaled-commit recovery publication and streaming-snapshot vector
fidelity. The follow-up adds optional admitted-mode metadata to the existing WAL
record map; framing and semantic payloads remain unchanged. No snapshot format,
Badger key layout, entity payload, storage version, hook,
analyzer, pre-journal admission, WAL migration or search-refresh change.

- [x] Source review (report 13): the replayed record is the accepted semantic
  `entity_commit`, applied by `RecoverBufferedCommitsAtOpen` through
  `SubmitAcceptedEntityTransaction`; its change events are written only in the
  capture mode the engine reports (`changeCaptureLocked`/`beginChangeCaptureLocked`).
  The persisted `txLogEnrichment` option reached the engine only from
  `multidb.NewDatabaseManager` (server, after `nornicdb.Open` returned), so a
  commit replayed by `Open` was applied with capture OFF and a feed cursor read
  before the crash never listed it. `TestChangeCaptureOfACommitRecoveredFromTheJournal`
  covers the storage call with the mode set by hand, not the open path.
- [x] Correction: `multidb.LoadChangeCaptureModes` reads the persisted catalog
  record through the same reader `loadMetadata` uses and settles every
  database's mode; `nornicdb.Open` calls it before `RecoverBufferedCommitsAtOpen`.
  The later manager load settles the same modes again (idempotent).
- [x] Source review (reports 45, 53.5): the affected snapshot is the automatic
  streaming snapshot (`createSnapshotAndCompact` -> `SaveStreamingSnapshot` ->
  `WriteSnapshot`). It streamed stored nodes with `StreamNodesOptions{}`; a
  stream without `WithEmbeddings` decodes the body alone, so a worker's sidecar
  vectors were never read and an older inline image survived, and
  `snapshotNodeVectorMetadata` dropped the metadata that would have said the
  node needs embedding. The journal truncation after the snapshot leaves the
  snapshot as the last copy.
- [x] Correction: `WriteSnapshot` streams with `WithEmbeddings: true`, so a
  node's managed vectors are exported as its reads return them;
  `snapshotNodeVectorMetadata` still settles a body that flags separate storage
  and carries no vectors. The canonical recovery-format note records the new
  contract and the larger snapshot.
- [ ] Capture fresh failing-then-passing evidence for
  `TestRecoveredCommitPublishesItsChangeEvent` (pkg/nornicdb) with
  `go test -tags=noui,nolocalllm ./pkg/nornicdb -run TestRecoveredCommitPublishesItsChangeEvent -count=1 -v`.
- [ ] Capture fresh evidence for
  `TestPeriodicSnapshotPreservesTheVectorsReadsReturn` and
  `TestWriteSnapshotReadsNodeVectors` (pkg/storage) with
  `go test -tags=noui,nolocalllm ./pkg/storage -run 'TestPeriodicSnapshotPreservesTheVectorsReadsReturn|TestWriteSnapshotReadsNodeVectors' -count=1 -v`,
  then the retained rebuild suite
  `-run 'TestRebuildDoesNotPresentReplacedVectorsAsCurrent|TestJournaledEmbeddingClearRecordsEachClearedNode'`.
- [ ] Profile the automatic snapshot after the correction: each node's sidecar
  vectors are now read and written. Report snapshot bytes and time on one named
  fixture; no measurement exists yet in this checkout.
- [x] Report46 correction: retain each node's existing write authority across
  the reset record and its transaction. The public reset plus concurrent update
  and snapshot/journal replay regression reproduces the old vector resurrection.
  Parent uses the canonical lock-wait hook to prove the interleaving, without a
  timing-only negative observation. Five repeats pass; no whole-store lock or
  storage format change.

## Parent validation

- [x] Fail-first on release ce22592d2: CDC feed missing recovered node, periodic snapshot returns stale/missing vectors.
- [x] Managed public query writer avoids dependence on the separate embedded-journal fix.
- [x] Focused storage/nornicdb/multidb checks pass under default and ANTLR parsers.
- [ ] Snapshot vector I/O/memory/record-size qualification.
- [x] Clear/update replay ordering (report46), including post-reset retry and replay comparison.
- [x] Correct nil-label body-copy drift; cold/warm snapshots and stored-stream encoders agree.
- [x] Full snapshot vector/metadata assertions replace the superseded embedding-free expectations. Missing-vector metadata component controls retain no-false-availability coverage.
- [x] Fresh integrated focused storage checks (2.007s) and public recovered-commit/embedding-clear checks (1.387s) pass on the working tree.
- [ ] Journaled-but-unapplied capture-option changes remain correctly ordered with recovered commits.
- [ ] Exact-head CI, race/census and integrated release qualification.

- [x] Reproduce hidden persisted catalog failure in both pre-recovery capture-mode loading and manager reload; share the stored-inclusive catalog read owner and verify options/catalog preservation.

## Approved CI correction follow-up

- [x] Reproduce suppressed catalog transition rejection; reuse stored-inclusive transaction reads without clearing suppression. Full multidb package passes.
- [x] Prove exact single-error fmt.Errorf propagation, retaining lookalike, side-effect, swallowed-error and sparse-argument rejection controls. Sparse-array negative control fails before tightening and passes after.
- [x] Register the one reviewed catalog data-reader and two exact non-network expressions; lexical population and endpoint inventories pass. Refresh the renamed snapshot evidence test reference without changing its classification.
- [x] Validate complete snapshot vectors instead of the obsolete sub-MiB size assumption; focused compaction tests pass.
- [x] Correct prune progress assertions to count committed deletions; independent three-repeat layout regression passes.
- [ ] Complete integrated affected-package checks, rebase on current release, push and run CI.

## Stored catalog disposition ownership

- [x] Move catalog insert/update selection into BadgerTransaction.UpsertGraphNode, retaining the existing create/update kernels and accounting boundary order.
- [x] Preserve the stored predecessor and creation timestamp for suppressed catalog updates. Public catalog regression and new transaction create/suppressed-update/refusal/cancellation controls pass.
- [x] Full multidb package passes32.338s; storage fixture constructor inventory passes56.045s.
- [ ] Recheck the catalog finding in exact-head CI; do not infer census success from focused runtime tests.
- [ ] Two constructor cleanup-error paths need an additional approved proof scope: they compose, rather than discard, the original startup error. Existing direct-wrap approval does not cover captured error cells and deferred cleanup. No extra analyzer change applied while that decision is pending.

## Approved cleanup proofs and CI follow-ups

- [x] Explicit operator approval covers error-preserving constructor cleanup and the exact catalog abort pattern.
- [x] Constructor proof uses the real compose.Outcomes owner; escaped local/result writes remain rejected. Parent fail-first controls caught and corrected an overly broad nil-store allowance.
- [x] Catalog proof verifies canonical rollback, exact gate release and the same failed transaction. A different transaction of the same type is rejected; normalization errors without a proven transaction relation remain unclaimed.
- [x] Parent focused controls pass across census/storage/Bolt/observability (10.686s/27.522s/1.391s/2.555s); actual constructor proof also passes7.169s.
- [x] UpsertGraphNode create/update publication scenarios pass and its explicitly approved classification points at that scenario.
- [x] Retired catalog endpoint fingerprints are removed only for expressions eliminated by the transaction-owned extraction; no new exemptions. Endpoint guard passes.
- [x] Provider timeout/cleanup test is ordered after RUN admission instead of racing a250ms BEGIN timer;30 repetitions pass. Actual deadline-clock behavior remains covered by separate lifecycle tests.
- [ ] Fresh exact-head CI, canonical artifact freshness and the remaining recovery-publication AC.

## Pending catalog-option recovery correction

- [x] Reproduce public DIFF-to-FULL catalog update followed by a data update
  using an earlier closed store and the later WAL. Replayed catalog is FULL,
  but the event is DIFF and omits an unchanged property (parent fail-first1.065s).
- [x] Prototype a commit-owned decision in optional WAL metadata. Both native
  and prepared writers persist OFF/DIFF/FULL; both replay routes use the existing
  accepted-transaction application owner. No post-hoc event rewriting.
- [x] Settle recovered catalog options for future statements after replay.
- [x] Initial public regression passes1.034s; focused semantic/prepared/native
  storage controls pass3.852s; codec/native mode controls pass1.121s.
- [x] Before/after-option and OFF controls fail independently on pre-fix source (1.426s), then pass three repeated default runs4.024s and ANTLR2.931s. Final runtime mode and exact second-restart feed are asserted. An independently applied catalog with earlier outstanding data is not qualified by this sequential crash-copy fixture.
- [x] Independent source review of both writer/replay paths finds no blocking
  route/lifecycle/conflict defect; comments and Go keyed-literal guidance updated.
- [x] Parent race controls pass nornicdb3.618s/storage7.666s, alongside default
  and ANTLR public checks. Codec adds ten encoded bytes, no extra allocations.
- [ ] CI on the pushed correction; snapshot cost qualification remains open.
- [x] Earlier source36077272c passed all seven make-test shards, lint and aggregate
  in CI43635884110. This does not qualify this newer uncommitted correction.

## Approved closure refresh and bounded snapshot qualification

- [x] Operator approved only the exact rollback closure fingerprint refresh
  after the CDC WAL change. All thirteen pinned sources, two compensation
  edges and caller sets are unchanged; compensation passes a nil journal.
  Installed positive and stale-source/new-caller controls pass108.948s.
- [x] Actual sidecar benchmark: 128/1024 nodes, 0.5/4 MiB raw vectors, complete
  vector read-back before timing. Three samples show approximately linear work;
  methodology and allocations are in the performance guide.
- [x] A17,408,000-byte sidecar round-trips every vector/metadata value, and the
  explicit16MiB record limit refuses it. Large-record/cancellation controls
  pass3.835s. Existing byte-counter fixture is shared, not duplicated.
- [ ] Foreground snapshot/write overlap and original production-size
  performance remain unqualified; no peak-heap or full release claim.
