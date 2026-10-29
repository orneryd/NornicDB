## Duplicate-check inventory for the MessagePack checkpoint

Checked staged tree `d0405685eedbba78b1c186273e25711854fa9763` against the delta from PR head `7de84c4c20260311e68e69e2340cc8964a18fe04`; the release graph was ingested at `a9265090a5d7bd8e34932cf8f504765c01aa147e`. The checker analyzed 182 functions and reported 7 reuse candidates and 0 refactor targets, with 0 candidate-cap skips.

The graph baseline differs from the checker comparison head. Graphify also reported that `pkg/simd/neon_simd_arm64.h` was partially extracted due to a syntax error. These findings are open follow-up work; this is not a checker pass.

| # | Kind | Candidate | Matched owner(s) |
| --- | --- | --- | --- |
| 1 | reuse | `pkg/linkpredict/graph_builder_test.go:281` — `TestGraphBuilder_CacheInvalidation()` | `pkg/linkpredict/graph_builder_test.go:305` — `TestGraphBuilder_CacheExpiration()` |
| 2 | reuse | `pkg/storage/wal_v4_gob_entity.go:56` — `decodeV4GobValue()` | `pkg/storage/badger_serialization.go:154` — `decodeValue()` |
| 3 | reuse | `pkg/replication/transaction_wire_prototype_test.go:51` — `wireCommand()` | `pkg/replication/transaction_wire_prototype_test.go:28` — `wireChange()` |
| 4 | reuse | `pkg/nornicdb/db.go:1601` — `.maybeEnableReplication()` | `pkg/replication/storage_adapter.go:70` — `NewStorageAdapterWithWAL()` |
| 5 | reuse | `pkg/storage/transport_property_values.go:225` — `readTransportInteger()` | `pkg/storage/property_codec.go:511` — `decodeStoredInteger()` |
| 6 | reuse | `pkg/storage/unique_lock_request_reuse_test.go:12` — `TestUniqueLockRequestReusePreservesOwnership()` | `pkg/storage/unique_key_lock_owner_test.go:16` — `TestUniqueKeyLocksAreOwned()` |
| 7 | reuse | `pkg/storage/replication_image_wire.go:87` — `.storedNode()` | `pkg/storage/badger_transaction_images.go:16` — `.StoredNodeImage()` |

The earlier full PR delta inventory contains 85 findings and is tracked separately in `openspec/changes/prepare-remote-ddl-release/checker-followup.md`; the two runs cover different staged deltas and should not be treated as an unchanged combined pass.

## Follow-up in progress

- Candidates 1, 2, 3, 5 and 6 have local consolidations with focused race checks.
- The historical gob decoder consolidation also corrected trailing zero-byte acceptance in WAL and caller-owned snapshot conversion. Existing stored-body decoding retains its prior permissive framing; fail-first and focused race tests cover both contracts.
- The checker passed staged tree `8c1ab3d5865e39b105fe16e9d6b0a4c9c8d01118`, analyzing 31 functions in the delta from `35ed741ebb0c213e8f1a3278c495a784d6d631e1` with no findings. The graph and header extraction limitations above still apply.
- Candidates 4 and 7 and the earlier inventory remain under review: they were not reanalyzed by the later incremental run. That run is not a pass for the complete PR delta.
