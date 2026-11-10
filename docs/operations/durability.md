# Durability and crash recovery

A successful write, a visible write, a flushed WAL record, and a synced file are different boundaries. Choose the required boundary before tuning throughput.

This guide describes persistent `nornicdb.Open` / `nornicdb serve` deployments. `Open("", cfg)` uses in-memory storage and has no restart durability. Direct `pkg/storage` callers must construct and close their own storage/WAL stack.

## Choose a write policy

| Policy | Configuration | Acknowledgement and failure boundary |
| --- | --- | --- |
| Default persistent | Async writes off; WAL `batch`, `100ms` | Transactions apply through WAL + Badger without write-behind. WAL sync is periodic, not per acknowledgement. |
| Strict persistent | `strict_durability: true` | Forces immediate WAL sync, enables Badger `SyncWrites`, and disables write-behind even if async writes were requested. |
| Opt-in buffered writes | Async writes on; strict off | Eligible implicit commits acknowledge before background Badger application. Buffered-commit records are flushed to the OS before acknowledgement; this is not a per-commit `fsync`. |
| WAL `none` | `wal_sync_mode: none`; strict off | WAL sync calls flush userspace buffers without syncing the WAL file. Recent writes can be lost. |

Strict mode is the supported configuration when writes must cross synchronous persistence boundaries. It cannot protect against failed hardware, a filesystem that does not honor sync, loss of the storage volume, or loss of encryption keys. Keep independent backups and test recovery.

```yaml
database:
  strict_durability: true
  async_writes_enabled: false
  wal_auto_compaction_enabled: true
```

Equivalent strict-mode environment setting:

```bash
export NORNICDB_STRICT_DURABILITY=true
```

### Transaction completion

- Explicit native transactions request a Badger sync after physical commit. The current commit tail logs a sync failure without rolling back the already committed transaction. A successful response alone does not prove that no storage error occurred; investigate `fsync failed after commit` warnings.
- Implicit transactions can coalesce sync work. An autocommit result or receipt is not an independent guarantee that every file was synced at that moment.
- The optional eventual query path reports HTTP `202 Accepted`, `X-NornicDB-Consistency: eventual`, and `optimistic` metadata. Other mutations can still use the transactional path when async writes are enabled. See [query consistency](configuration.md#async-write-settings).
- Once a semantic command is accepted into a log, cancellation or a client timeout does not necessarily discard it. Application/recovery may complete after the client stops waiting. Reconcile uncertain outcomes and use application idempotency keys before retrying writes.

Atomicity and MVCC visibility are separate from power-loss durability. See [Transactions](../user-guides/transactions.md).

## WAL sync settings

| Setting | Server default | Meaning |
| --- | --- | --- |
| `database.strict_durability` / `NORNICDB_STRICT_DURABILITY` | `false` | Forces immediate WAL sync and disables write-behind. |
| `database.wal_sync_mode` / `NORNICDB_WAL_SYNC_MODE` | `batch` | `batch`, `immediate`, or `none`. |
| `database.wal_sync_interval` / `NORNICDB_WAL_SYNC_INTERVAL` | `100ms` | Duration between batch-sync attempts. |
| `database.async_writes_enabled` / `NORNICDB_ASYNC_WRITES_ENABLED` | `false` | Enables the native buffer for eligible commits. |
| `database.async_flush_interval` / `NORNICDB_ASYNC_FLUSH_INTERVAL` | `auto` | Initial adaptive buffer rotation delay. A positive value also overrides the batch WAL interval when async batch mode is enabled. |

Ordinary batch appends flush the userspace WAL buffer; the background sync loop performs file syncs. Async batch mode may defer ordinary append flushes until a sync tick. The buffered-commit path explicitly flushes its full recovery record before acknowledgement even with that setting.

Automatic native buffer rotation starts at 50 ms and adapts between 5 ms and 1 s. Its delay and the WAL sync interval are different controls. Neither is a hard upper bound on acknowledged data loss or replay latency: scheduling, I/O stalls, errors, and write-path selection matter. There is no workload-independent throughput multiplier or recovery-time guarantee.

## Persistent files

| Artifact | Purpose |
| --- | --- |
| Badger files in the data directory | Current graph, dictionaries, schemas, MVCC metadata, counts, and system records. |
| `wal/wal.log`, `wal/segments/`, manifest and property-authority files | Recovery intent and retained mutation history. Keep the whole WAL directory together. |
| `snapshots/snapshot-*.json` | Recovery snapshots. Current automatic snapshots are binary streaming files despite the suffix. |
| `search/` | Search generations/checkpoints, with a readiness and rebuild lifecycle separate from graph commit completion. |
| Replication data directory | Cluster log/state; may be outside the graph data directory. |

Current WAL frames use format **6**, typed recovery payloads, checksums, and completion markers. Larger buffered payloads can be compressed. These detect invalid/truncated records; alignment and checksums do not make arbitrary disk writes atomic or repair arbitrary corruption. See [Recovery format compatibility](recovery-format-compatibility.md) for upgrade and rollback rules.

## Startup and recovery

1. Startup validates WAL format before opening or migrating storage. Supported historical formats require an authorized storage upgrade; unknown future formats fail validation.
2. With usable Badger storage, startup replays accepted semantic transactions and buffered commits before traffic. Persisted transaction identities prevent already applied commands from being applied as new writes.
3. A valid one-use clean-shutdown marker permits reuse of temporal indexes and MVCC heads. It is consumed before service resumes. An unclean start uses conservative reconstruction.
4. A qualifying Badger corruption/open failure can trigger reconstruction from a valid snapshot and subsequent WAL entries, or sufficient WAL-only history. This is separate from ordinary accepted-commit replay.
5. Search services load compatible checkpoints or rebuild indexes. Graph recovery does not imply semantic search is warm.

`NORNICDB_AUTO_RECOVER_ON_CORRUPTION` controls the corruption-rebuild attempt. The default uses corruption heuristics; explicit `true` requests an attempt for otherwise unmatched open errors, and `false` disables it. Automatic corruption reconstruction is not used when database encryption is enabled. An unfinished recovery manifest also blocks an ordinary reopen rather than pretending recovery completed. Reconstruction preserves the original store as `<dataDir>.corrupted-<timestamp>`. Permissions, disk space, encryption configuration, and usable recovery artifacts still determine success.

Native restore writes `native-restore.marker`. Corruption recovery must not use an older snapshot/WAL history to overwrite restored state; a newer recovery snapshot is required for that boundary. See [Backup and restore](backup-restore.md).

## Shutdown and incidents

Stop traffic, let in-flight work finish, and call `DB.Close` or allow the server a graceful termination window. Close joins database-owned work and returns its retirement outcome. Forced termination can interrupt buffering, recovery, or index persistence; large stores may need a longer orchestrator termination timeout.

After a failed start or sync warning:

1. Stop restart loops. Preserve the complete data directory, matching WAL, search files, configuration, and external replication directory.
2. Record the binary version, error, disk space, mount type, and termination reason. Process termination and machine power failure are different failure models.
3. Diagnose using a copy. Do not delete or truncate WAL, sealed segments, dictionaries, or snapshots to make startup succeed.
4. Restore a verified backup to an isolated target if recovery cannot establish a valid store. Validate counts and application invariants before traffic.

See [WAL lifecycle](wal-compaction.md), [memory management](memory-management.md), and [troubleshooting](troubleshooting.md#data-integrity-recovery).

## Implementation reference

- [Server storage and durability options](../../pkg/nornicdb/db.go): `resolveDurabilityOptions`, startup replay, wrappers.
- [Native publication](../../pkg/storage/badger_transaction_publication.go): explicit sync and sync-error handling.
- [Buffered acknowledgement](../../pkg/storage/badger_write_behind.go) and [WAL append/sync](../../pkg/storage/wal.go).
- [Accepted commands](../../pkg/storage/wal_entity_commit.go) and [corruption recovery](../../pkg/nornicdb/storage_recovery.go).
- [Cluster acknowledgements](../user-guides/clustering.md#write-acknowledgment): local sync and replication policies are independent. A replica or quorum is not a backup.
