# WAL snapshots, compaction, and retention

The application WAL records recovery intent. Compaction persists a snapshot before removing history it covers. It does not replace a backup or select acknowledgement sync policy; see [Durability](durability.md).

## Server configuration

```yaml
database:
  wal_auto_compaction_enabled: true
  wal_snapshot_interval: 300000 # integer milliseconds: five minutes
  wal_snapshot_retention_max_count: 3
  wal_snapshot_retention_max_age: "0s"
  wal_retention_max_segments: 0
  wal_retention_max_age: "0s"
```

| Environment setting | Default | Effect |
| --- | --- | --- |
| `NORNICDB_WAL_AUTO_COMPACTION_ENABLED` | `true` | Enable periodic snapshot/compaction checks. |
| `NORNICDB_WAL_SNAPSHOT_INTERVAL` | `300000` | Milliseconds between server checks. |
| `NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT` | Storage default: 3 | Snapshot file count. Server value 0 leaves the storage default in effect. |
| `NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_AGE` | `0s` | Snapshot age limit; 0 disables age pruning. |
| `NORNICDB_WAL_RETENTION_MAX_SEGMENTS` | `0` | Covered sealed-segment count limit when retention is enabled. |
| `NORNICDB_WAL_RETENTION_MAX_AGE` | `0s` | Covered sealed-segment age limit when retention is enabled. |
| `NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS` | `false` | With both segment limits unset, opt in to 24 segments and seven days. |

The server uses `<dataDir>/wal` and `<dataDir>/snapshots`. `wal_dir`, `wal_snapshot_dir`, `wal_batch_sync_interval`, `wal_max_file_size`, and `wal_max_entries` are not server YAML knobs. Direct `storage.WALConfig` callers have additional controls: its default snapshot interval is one hour, overridden to five minutes by the server.

## Compaction boundary

The automatic worker:

1. Acquires the mutation barrier and checks that accepted native publication permits a snapshot.
2. Skips unchanged intervals after a completed compaction.
3. Captures the WAL sequence/checkpoint and flushes acknowledged write-behind generations into Badger.
4. Streams entities to a temporary snapshot, syncs it, renames it, and applies platform directory-sync policy.
5. Calls `ManageSnapshot(ctx, sequence, storage.WALSnapshotTruncate)` to retire covered history.
6. Prunes snapshot files after successful compaction.

The snapshot and sequence must describe the same publication cut. Snapshot creation can delay writers behind the barrier and performs I/O proportional to stored data. It is not constant-time. Automatic snapshots use binary streaming format version 2 despite their `.json` suffix.

Incomplete snapshots are not recovery points. Persistence errors prevent corresponding truncation. If truncation fails after a snapshot is saved, the snapshot can remain usable while the WAL stays larger. Invalid manifests, unsupported formats, and unreadable records are not permission to discard history.

## Retained segments

Zero/zero segment limits mean compaction drops covered entries; they do not enable an unlimited ledger. Setting either enables retention. Compaction seals the active segment and preserves uncovered history. The current retention routine protects covered segments whose creation time is inside the age window, then processes the remaining covered candidates. The count branch keeps the newest configured count only when that candidate population exceeds the count; candidates at or below the count are retired. A count setting alone therefore does not guarantee retention of that many covered segments. With age protection enabled, total retained segments can exceed the count setting. Qualify the required history on the actual store rather than treating these settings as a minimum ledger guarantee.

```yaml
database:
  wal_retention_max_segments: 24
  wal_retention_max_age: "168h"
```

Uncovered segments remain necessary regardless of age. Retention is not a disk-byte cap. WAL retention, recovery snapshots, MVCC history, CDC, and backups have separate lifecycles.

Disable periodic compaction only with a separate snapshot and disk-capacity plan:

```bash
export NORNICDB_WAL_AUTO_COMPACTION_ENABLED=false
```

This stops the worker, not ordinary rotation or manually invoked maintenance.

## Embedded maintenance

Use `WALEngine.EnableAutoCompaction(snapshotDir)` / `DisableAutoCompaction` on a WAL-wrapped engine. `GetSnapshotStats()` returns `(totalSnapshots int64, lastSnapshotTime time.Time)`, not a struct with disk savings.

`WAL.ManageSnapshot` provides:

- `storage.WALSnapshotTruncate`: validate/truncate covered active history, or rotate/apply configured retention.
- `storage.WALSnapshotRetention`: prune sealed, covered segments without flushing/rotating the active WAL. Sequence zero is a no-op.

Raw `WAL.CreateSnapshot` builds an in-memory snapshot. It neither persists it nor establishes the wrapper's publication/mutation barrier. Do not follow it with truncation on a live database without matching coordination and successful persistence. Prefer the coordinated WAL engine lifecycle.

## Inspect the transaction log

You can query WAL entries directly via Cypher:

```cypher
// The 1,000 most recent entries of the current database
CALL db.txlog.entries() YIELD txId, db, kind, seq, timestamp, payload
RETURN seq, kind, txId, timestamp, payload
ORDER BY seq;

// Entries from sequence 1000 to 1200 (inclusive)
CALL db.txlog.entries(1000, 1200) YIELD seq, kind, txId
RETURN seq, kind, txId
ORDER BY seq;

// Entries of one transaction, at most 200 of them
CALL db.txlog.byTxId('tx-123', 200) YIELD txId, db, kind, seq, timestamp, payload
RETURN seq, kind, txId, timestamp, payload
ORDER BY seq;
```

- `db.txlog.entries(fromSeq = null, toSeq = null)` returns the current database's entries from `fromSeq` (inclusive, at least 1) to `toSeq` (inclusive; `null` or `0` is no upper bound). Without either, it returns the 1,000 most recent entries.
- `db.txlog.byTxId(txId, limit = null)` returns the current database's entries of one transaction; `limit` caps how many (`null`, `0` or less is no limit).
- Both yield `txId` (the entry's transaction ID, `""` when it has none), `db`, `kind` (the operation, such as `create_node` or `tx_commit`), `seq` (the WAL sequence), `timestamp` (RFC 3339, UTC) and `payload` (the entry's JSON data).
- Arguments may be literals, parameters or variables.

### Reading compressed WAL records offline

The WAL is a sequence of individually framed records, not a single compressed
archive. Write-behind commits use the typed MessagePack recovery representation;
payloads of at least 512 bytes are normally compressed with Zstandard at its
default speed level. Smaller payloads, and payloads written when the compressor
is unavailable, are stored uncompressed. Other record types and property
dictionary controls have their own payloads. Each frame carries its own length,
checksums and completion marker, so running `zstd -d` on `wal.log` as a whole
will not produce readable records.

For an offline inspection, first stop the database or take a consistent copy
of its complete WAL directory, including `wal.log` and `segments/`. Keep the
original backup and WAL together. From a NornicDB source checkout matching the
version that wrote the WAL, run:

```bash
go run ./scripts/wal-read -dir /path/to/copied/wal -from 1 -to 100 > wal.jsonl
```

The reader uses the same frame, decompression and typed-payload decoders as
NornicDB and writes one JSON object per entry. `-from` and `-to` are inclusive;
use `-to 0` to leave the upper bound open. The output is for inspection, not a
restorable backup. Buffered records render physical `writes` as a list of
base64-encoded `key` and `value` bytes and `deletes` as base64-encoded keys;
these are binary Badger keys, not text. To inspect a key as hexadecimal, copy
its base64 value from the output and run:

```bash
python3 -c 'import base64,sys; print(base64.b64decode(sys.argv[1]).hex())' 'BASE64_KEY'
```

An `entity_commit` payload contains a base64-encoded semantic transaction.
Decode that value to bytes and pass it to the Go
`storage.DecodeEntityTransaction` API to inspect its logical operations.
Property-dictionary authority controls are used by the reader and are not
emitted as ordinary entries.

The WAL may contain application data. Store the JSON output with the same
access controls as the database. A read may stop at an incomplete tail or skip
a damaged regenerable embedding entry, so an inspection is not by itself proof
that a backup is complete; check recovery diagnostics and verify the backup.

---

## Operations

Monitor complete WAL/snapshot directories, successful snapshot time, free space, retention, and failed flush/sync/truncation messages. Short intervals add I/O and writer latency; long intervals retain replay work. Measure recovery on a copy instead of assuming fixed timings or percentage savings.

If disk use grows, check snapshot completion, uncovered segments, and intentional retention. Check permissions/free space before retrying. Never delete the active WAL as a memory or disk-space workaround.

## Format upgrades and rollback

Preserve a verified pre-upgrade backup with matching WAL/search/replication state. Authorize required migrations with `--upgrade-storage`; retain protected migration copies until validation succeeds. Restore matching pre-upgrade state for rollback. See [Recovery format compatibility](recovery-format-compatibility.md).

## Implementation reference

[Coordinated compaction](../../pkg/storage/wal_engine.go), [configuration/retention](../../pkg/storage/wal.go), [streaming publication](../../pkg/storage/snapshot_stream.go), and [temporary-file handling](../../pkg/storage/snapshot_temp.go).
