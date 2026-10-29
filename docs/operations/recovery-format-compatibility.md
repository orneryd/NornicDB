# Recovery format compatibility

Recovery records preserve node and relationship timestamps, embedding metadata,
inline vectors, visibility flags, relationship confidence, and exact integer
property values. Client-facing node and relationship JSON is unchanged.

## Write-ahead logs

### v1.4.4 backup and restore boundary

Starting with v1.4.4, buffered-commit WAL payloads use the typed MessagePack
recovery codec and may be compressed. They preserve arbitrary Badger key bytes
that JSON cannot represent losslessly. The WAL change does not change existing
Badger keys or require a separate Badger storage-version bump; it is included in
the existing V4 upgrade arm.

Before upgrading, make and verify a backup of the database and its matching WAL.
Start v1.4.4 with `--upgrade-storage` to authorize the storage upgrade. The V4
startup migration validates and converts supported older WAL frames, including
retained segments, to the current framing. It replays acknowledged buffered
writes before transforming the V4 catalog. The migration preserves historical
application payload bytes; newly written buffered records use the typed codec. Old
JSON cannot reconstruct physical key bytes that an older writer already
replaced, so an ambiguous record is rejected instead of silently applied.

Before replacing WAL files, the migration keeps a synced copy in a protected
`wal.before-v4` directory. After a successful upgrade it is retained as
`wal.pre-v4-<id>` beside the active WAL. It may contain unencrypted application
data and credentials: protect it like the original database and remove it only
after the upgrade and independent backup have been verified. If the upgrade is
interrupted, restart with `--upgrade-storage` to resume it. Unsupported future
formats and corrupt records are rejected; a partial final frame in the active
WAL is handled as a torn tail. Retained segments are validated strictly.

Older binaries cannot read the new WAL frames. For rollback, restore the
pre-upgrade database backup and its matching old WAL together; do not mix files
from different database backups. Standalone current-version WAL readers accept
current frames only, so use the matching old binary to inspect an unconverted
archive.

### v1.4.3 recovery records

The v1.4.3 entity WAL payload used JSON fields plus a base64
`_nornicdb_data` sidecar with the typed storage representation. The V4 upgrade
retains those historical payload bytes inside converted frames.

Legacy JSON recovery keeps numeric lexemes before interpreting property values,
including nested lists/maps and buffered-commit operations. Integer lexemes become
`int64`, or `uint64` when a larger nonnegative value fits that range; fractional
and exponent lexemes become `float64`. Values outside those integer ranges are
rejected rather than rounded. This preserves `9007199254740993` exactly, but old
JSON cannot reveal whether an integer-looking value originally came from a Go
integer or an integral float. The typed representation retains that distinction.
The same reader is used by legacy JSON and version 1 snapshot frames.

Older binaries ignore the sidecar and recover their previous JSON projection.
They consequently lose the same internal metadata and integer precision that
older recovery already lost. This is baseline readability, not a lossless
rollback guarantee. Do not downgrade a database that requires those fields;
restore a pre-upgrade backup or export through a compatible application route.
That historical sidecar duplicated publicly visible properties on disk. An
older startup repair may treat an unsupported WAL version as a corrupt tail,
so keep the original files and use the binary that wrote them.

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
codec does not change which sidecars that route loads. Exported nodes clear the
separate-storage flag, and metadata no longer advertises omitted chunk vectors or
unavailable embeddings. Inline vectors and unrelated metadata remain intact;
the source node is unchanged.

## Local validation

Go callers reading individual WAL files use `storage.ReadWALEntries(path,
logger)`, passing `nil` to discard diagnostics. This replaces the former
one-argument `ReadWALEntries` and separate `ReadWALEntriesWithLogger` entry
points; migrate callers when updating to this release. Use
`storage.ReadWALEntriesAfter(path, sequence, storage.WALReadFile)` for one file
or `storage.WALReadDirectory` for its active WAL and retained segments; this
replaces the separate `ReadWALEntriesAfterFromDir` operation.

Recovery tests exercise exact types and metadata, redo/undo, checksums and
truncated snapshots. Validate a backup and its matching WAL with the version
that produced them before relying on them for recovery.

The following historical `BenchmarkRecoveryCodecDocFixture` measurements
compare v1.4.3 JSON and its sidecar on a small Doc fixture with nested integer
properties, two 2D vectors, metadata, and timestamps. They are not v1.4.4
measurements or database-wide memory and throughput figures.

On Apple M2 Max, Go 1.26.6, `GOMAXPROCS=2`, the Doc fixture grows from
115 to 601 wire bytes (+423%); encode allocations grow from 464 to 4,337 B/op,
and decode allocations from 1,312 to 2,952 B/op. A retained 2,000-entity batch
measured about 944 versus 1,767 heap bytes per entity (+87%, GC-based estimate).
These increases exceed the usual memory budget and are explicitly justified
here by restoring previously omitted timestamps, vectors, metadata and flags,
plus exact integer properties in that historical release. The tiny
metadata-heavy fixture exaggerates fixed overhead.
These are storage/allocation measurements, not latency improvement claims.
