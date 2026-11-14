## Recovery publication corrections (report items 13, 45, 46, 53.5)

Scope: journaled-commit recovery publication and streaming-snapshot vector
fidelity. No WAL/snapshot format, Badger key layout, wire payload, hook,
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
