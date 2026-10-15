# Recovery format compatibility in v1.4.3

Recovery records preserve node and relationship timestamps, embedding metadata,
inline vectors, visibility flags, relationship confidence, and exact integer
property values. Client-facing node and relationship JSON is unchanged.

## Write-ahead logs

The outer WAL format remains version 3. Entity payloads retain their previous
JSON fields and add `_nornicdb_encoding: "storage-msgpack-v1"` and a base64
`_nornicdb_data` field containing the complete typed storage representation.
Current readers use that sidecar. Control records retain their previous JSON
representation. Current readers also accept older version 1–3 WAL records
without a sidecar.

Older binaries ignore the sidecar and recover their previous JSON projection.
They consequently lose the same internal metadata and integer precision that
older recovery already lost. This is baseline readability, not a lossless
rollback guarantee. Do not downgrade a database that requires those fields;
restore a pre-upgrade backup or export through a compatible application route.
The sidecar duplicates publicly visible properties on disk to avoid a dangerous
format transition: older startup repair treated an unsupported WAL version as a
corrupt tail and could truncate it. New startup repair rejects an unsupported
version without truncating any bytes.

## Snapshots

Both `SaveSnapshot` and `WriteSnapshot` now write framed `NDS2` version 2
snapshots. Entity frames contain typed storage records; framing, checksums, and
completion counts detect truncation and corruption. Materialized snapshots
preserve their collection timestamp and version in the completion frame.
`LoadSnapshot` and streaming recovery accept legacy JSON snapshots and `NDS2`
version 1 snapshots as well as version 2.

Older streaming readers reject version 2 before visiting any entities. Older
materialized JSON readers also reject it. Keep a pre-upgrade snapshot and its
matching WAL before upgrading; an old binary cannot restore a new version 2
snapshot. No automatic rewrite of existing backups or user data is required.

Materialized snapshots retain support for entity records above the 16 MiB
streaming-export limit. File-based load/recovery accepts framing lengths up to
`uint32` (4 GiB minus 1 byte per encoded entity); practical storage limits may
be lower. Public `ReadSnapshot` and streaming export retain the 16 MiB bound.
Large file payloads are read progressively rather than preallocating a claimed
length from an untrusted header.

Snapshot identifiers use the established `database:local-id` representation.
An ID containing a colon is treated as qualified, not as a bare default-database
ID; bare IDs without colons use the configured default database on restore.
Callers constructing snapshots must qualify local IDs that themselves contain
colons. This preserves existing snapshot qualification semantics.

Streaming snapshot export retains its established embedding-free enumeration
contract: separately stored vectors are not fetched by that route. The new
codec preserves all fields supplied by the chosen enumeration route; it does
not change which sidecars that route loads.

## Local validation

Regression tests cover legacy formats, exact types and metadata, ordinary and
bulk redo/undo, public JSON shape, invalid sidecars, checksums, truncated
snapshots, and unsupported WAL versions without truncation. A disposable
fixture can be generated with `NORNICDB_RECOVERY_FIXTURE_DIR` and
`TestRecoveryGenerateOldReaderFixtures`, then opened by an isolated old release
reader to check its baseline WAL recovery and snapshot rejection. Run
`scripts/verify-recovery-compatibility.sh <old-release-revision>` to perform
that comparison using disposable worktrees and fixtures.

`BenchmarkRecoveryCodecDocFixture` compares legacy JSON and the compatible
sidecar on a named small Doc fixture with nested integer properties, two 2D
vectors, metadata, and timestamps. Its wire-bytes and B/op measurements describe
this fixture, not database-wide memory consumption or throughput.

On Apple M2 Max, Go 1.26.6, `GOMAXPROCS=2`, the Doc fixture grows from
115 to 601 wire bytes (+423%); encode allocations grow from 464 to 4,337 B/op,
and decode allocations from 1,312 to 2,952 B/op. A retained 2,000-entity batch
measured about 944 versus 1,767 heap bytes per entity (+87%, GC-based estimate).
These increases exceed the usual memory budget and are explicitly justified
here by restoring previously omitted timestamps, vectors, metadata and flags,
plus exact integer properties; the legacy projection remains duplicated for
old-reader safety. The tiny metadata-heavy fixture exaggerates fixed overhead.
These are storage/allocation measurements, not latency improvement claims.
