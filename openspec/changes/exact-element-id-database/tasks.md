# Exact element-ID database identity

- [x] Record approval to retire disposable database qualifiers on plain stores.
- [x] Reproduce foreign qualifier matches through public equality, IN, writes and graph utility paths before correction.
- [x] Preserve qualified references through canonical storage identity resolution; remove permissive comparison domains and their source maps.
- [x] Reproduce and fix path algorithms reinterpreting a foreign ID as an opaque local ID.
- [x] Verify both parsers, autocommit/explicit transactions, returned-ID round trips, raw-ID and native opaque-ID controls, and no writes from foreign IDs.
- [x] Verify composite identity consumers, pinned traversals and affected race checks.
- [x] Document the compatibility change without stored-key, payload or WAL changes.
- [x] Review lexical delta and refresh stored-entity census locations and the renamed lock scratch writer from release PR1756; classifications unchanged.
- [ ] Resolve the blocking context census finding in WAL recovery logging; retain the census as a gate.
- [ ] Complete fresh full make-test CI on the follow-up head.
- [ ] Reconcile duplicate advisory/review feedback and update Linear.
