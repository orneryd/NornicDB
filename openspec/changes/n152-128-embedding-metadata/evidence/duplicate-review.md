# N152-128 duplicate review

Applied nornicdb-duplicate-review. Current HEAD and origin/release/v1.5.2 both
f77ec7706de099588ac232b32b6908860511d3da. DeepSeek source-only owner-map/proposal
files are independently owned and are not overwritten here.

- Canonical production owner: badger_helpers.go encodeNodeInTxn; direct create,
  bulk and native transaction commit reach it. Add missing count only after a
  top-level maps.Clone; no new helper or wrapper path.
- Existing top-level map edit: snapshot_vector_metadata.go uses maps.Clone before
  deleting metadata entries. Same ownership operation, different stored/export
  projection; standard-library clone is reused directly without combining these
  otherwise different operations.
- copyCallerMetadata/property_copy.go is the deep retention/delivery contract,
  including nested values and CallerCopy protocols. Encoding only inserts one
  top-level field, reads nested values and does not retain this map; deep copying
  would change work and ownership beyond this correction.
- encodeNodeV1Body has the same old shallow-map insertion, but repository-wide
  callers of encodeNodeV1 are only property_codec_test, byte_property_roundtrip_test
  and migration_v1_to_v2_test fixture builders. Direct body callers are two legacy
  tests. Actual migration_v1_to_v2.go:440 decodes V1 and :446 calls encodeNodeInTxn.
  Parent approved leaving the test-only legacy fixture writer unchanged, retaining
  its controls; no supported production V1 writer is excluded from correction.
- Existing canonical vector fixtures remain in embedding_chunk_decodes_test.go;
  the ticket's copy is archived only, never copied over the repository owner.

Staged checker was invoked and failed analysis/ingestion readiness at
localhost:7474/nornicdbcode; no graph-clean claim. Focused normal and stored-image
consumer checks passed; see verification.md for exact selectors and remaining
checks. DeepSeek worker proposal recommends a deep ownedEntries copy; parent
retained maps.Clone because the encoder only changes one top-level entry and
never mutates/retains nested metadata. Its nil/count-present/byte/error concerns
are covered by the existing measured-encoding matrix, with the caller-mutation
assertion corrected under parent approval. Existing sidecar reuse and ten stored
image/reopen shapes passed; no added sidecar implementation or nested ownership
contract is introduced.
