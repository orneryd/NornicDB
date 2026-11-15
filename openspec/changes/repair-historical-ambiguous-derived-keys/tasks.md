# Repair historical ambiguous derived physical keys

- [x] Record the failing baseline before the change: the previous revision of
      `pkg/storage/wal_v4_ambiguous_key_upgrade_test.go` produced only the refusal
      for both fixtures (`applied=false`: 5 damaged keys, 1 ambiguous record
      without durable proof; `applied=true`: 5 damaged keys, 0 without proof).
      That output is preserved in the test file's header.
- [x] Write the fail-first success assertions and run them against the unchanged
      source: `TestV4UpgradeRepairsAmbiguousHistoricalWALKeys` failed for both
      fixtures at the file-only preflight
      (`wal: historical physical key contains ambiguous UTF-8 replacement`) and
      `TestV4UpgradeRepairsMergedHistoricalCollisionClasses` failed at the engine
      open with
      `V4 WAL migration and recovery: wal: historical physical key contains ambiguous UTF-8 replacement`.
- [x] Move the derivation owner from the test into the migration:
      `repairV4BufferedCommitPhysicalKeys` in
      `pkg/storage/wal_v4_buffered_key_repair.go` is the one canonical
      reconstruction of the eight derived key families, and the tests now call it.
- [x] Invoke that decision from the existing conversion owner
      (`v4UpgradeRecords.convert`), which validation and the conversion pass share,
      so the file-only preflight, the V4 validation and the conversion all take the
      same pure decision before any ambiguity or frame-size decision and before any
      file write.
- [x] Split encoding losslessness from named-property authority in
      `decodeBufferedWALRecord`, and mark repaired records with the explicit
      `PhysicalPropertyImages` field so a repaired legacy record's lossy `Ops`
      property values stay non-authoritative.
- [x] Reuse the existing owners: `labelIndexKey`/outgoing/incoming/edge-type/
      between-set/between-head builders, the reverse-dictionary key layouts,
      `decodeStoredNodeLabels`, `decodeEdgeCompactHeaderInto`,
      `adjacencyValueFromRecord`, `decodeNodeRefNum`, and the storage key-family
      inventory.
- [x] Repair whole collision classes, including entries the old JSON object merged
      into one key (`TestV4UpgradeRepairsMergedHistoricalCollisionClasses`: 146
      damaged keys, 144 merged entries, every entry restored byte for byte).
- [x] Refuse the whole record, with the source journal unchanged and no backup
      published, when the record does not prove every entry: unknown family,
      known but non-derived family, missing body, missing record-local binding
      (the record-local-only case, `TestV4UpgradeRefusesAmbiguousHistoricalKeysWithoutRecordLocalProof`),
      damaged deletion, shared between-head key, unproven damaged key.
- [x] Prove the public engine `Open` path and a second open for the durable and
      the unapplied fixture: label scan, both adjacency directions, the between
      lookup, the exact property image, and the preserved pre-upgrade backup.
- [x] Keep the genuine historical fixtures as the unchanged control
      (`TestV4StartupUpgradeMigratesGenuineWAL`,
      `TestV4StartupConvertsOlderWALWhenSchemaAlreadyV4`,
      `TestV4StartupUpgradeRestoresHistoricalPendingNewIDs`).
- [x] Confirm no Badger key layout, payload format or storage version change, no
      new JSON WAL writing and no gob path; `WALMessagePack` and the framing codec
      are unchanged.
- [x] Parent focused historical repair/refusal checks pass (4.296s).
- [ ] Independently rerun affected race checks on the final rebased head.
- [ ] Parent independent acceptance.
- [ ] Exact-head CI and remote duplicate review.

## Remaining, deliberately out of scope

- A record whose damaged keys need facts it does not carry still refuses; no
  durable-dictionary callback was added and no entry is inferred.
- [x] External-package public startup fixture opens with `nornicdb.Open`, verifies
  both recovered labels through a count query and the recovered relationship
  through MATCH, closes, and repeats the same public reads after a second open.
- [x] Independent fail-first collision-class control rejects a missing body even
  when a surviving member still names the collapsed damaged key.
- [ ] Exact property-binding owner inventory entry awaits operator approval.
