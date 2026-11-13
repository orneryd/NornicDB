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
