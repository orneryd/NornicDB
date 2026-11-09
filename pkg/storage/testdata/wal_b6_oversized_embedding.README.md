# Genuine b6 oversized embedding audit WAL

`wal_b6_oversized_embedding.wal.gz` was produced by the unmodified WAL writer at
`b6d8e762ab9a9bd1e954aca5dd4f4257805fa8c2` on TrueNAS, job
`i20261109181749-75456-bc39`. Only an untracked test exporter was overlaid; no
production source or writer limit was changed.

The exporter calls `NewWAL`, then `AppendWithDatabase` for a `create_node`
(`before`), a full-node `update_embedding` (2,048 chunks x 2,048 dimensions,
each value 0.12345678), and another `create_node` (`after`), then closes the WAL.
It compresses the resulting `wal.log` with gzip. All records use b6's native
version-3 framing, CRC32-C checksums, trailer and alignment.

Uncompressed length: 27,971,992 bytes.
Uncompressed SHA256: `0de00f9722ae1ba02c9052d9b299fd67fc11b0c596eb87671c30bb2af152581e`.
Compressed SHA256: `6e0a7d7443ce53d5415618ad3485d49625c24f481840aab4694bd2f2e67b58c2`.

`oversizedAuditFixture` verifies the source hash and every frame's version. Its
current-format variant changes only the outer version bytes to the current
raw-Data framing version; the complete envelopes, data, checksums, trailer,
alignment and semantic suffix remain unchanged. Legacy JSON recovery payloads
remain supported by the current recovery decoder.
