# Backup and restore

Use a native backup for an online persistent-store copy, or stop the database before copying complete files. Automatic WAL snapshots support local recovery; they are not independent backups.

## Choose a backup

| Method | Boundary | Preserve separately |
| --- | --- | --- |
| Native `DB.Backup` / admin API | Flushes accepted write-behind data, streams a consistent Badger backup, and syncs the output. | Configuration, encryption/provider material, model/plugin files, and required external replication state. |
| Legacy/custom-engine JSON export | Graph nodes/edges; not a native full-store replacement. | Account/schema/config state not represented by exported entities. Quiesce writes for a controlled export. |
| Cold directory copy | Successful shutdown followed by copying the complete data directory and external state. | Keys and provider credentials needed to reopen it. |
| Filesystem/volume snapshot | Depends on provider consistency and application quiescing. | All separately mounted data/WAL/search/replication volumes at the matching point. |

Native full-store backups include persisted system/account records. Graph-only JSON exports do not provide equivalent account/schema coverage. A native `.backup` does not package surrounding WAL, search files, configuration, or models.

## Online backup

The destination is on the server, inside permitted filesystem roots; the response does not download it. Create the directory with suitable ownership and restrict backup access.

```bash
curl --fail-with-body -X POST http://localhost:7474/admin/backup \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"path":"/backups/nornicdb.backup"}'
```

Admin permission is required. For remote calls use the configured HTTPS origin and trusted CA. Copy only successfully completed files to independent storage; failure can leave partial output that must not enter the restore set.

```go
if err := db.Backup(ctx, "/backups/nornicdb.backup"); err != nil {
    return err
}
```

Native Badger backups are streaming protobuf, including the built-in `MemoryEngine`, which delegates backup to its Badger owner. JSON is the compatibility/fallback path when native backup is not implemented. The engine capability, not an empty data-directory argument or a filename suffix, selects the format. There are no `nornicdb backup` / `nornicdb restore` CLI subcommands.

## Restore

Restore into an isolated deployment first, with matching binary and encryption configuration. Stop application traffic and close long-lived transactions before restoring a serving store.

```bash
curl --fail-with-body -X POST http://localhost:7474/admin/restore \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"path":"/backups/nornicdb.backup"}'
```

```go
if err := db.Restore(ctx, "/backups/nornicdb.backup"); err != nil {
    return err
}
```

Native restore replaces storage under read/write barriers. It waits up to 30 seconds for existing readers/transactions; failure to drain stops replacement. New reads/writes during replacement receive a transient storage-restoring error. Treat replacement/I/O failures as incidents; retain the original backup and inspect the error before retrying.

Storage reloads dictionaries, schemas, counters, caches, and MVCC state. The DB layer rebuilds temporal indexes/MVCC heads, resets search services, and lets embedding work rescan. Search can need warming or rebuilding afterward. `native-restore.marker` prevents corruption recovery from restoring an older recovery snapshot over the replacement.

JSON fallback inserts/updates entities without clearing all unrelated records. Use an empty target for graph imports; this is not native full-store replacement.

## Cold Docker volume copy

This assumes all state is in `nornicdb-data`, mounted at `/data`. Include other volumes when state lives elsewhere.

```bash
# Choose an adequate grace period and confirm shutdown completed.
docker stop --time 120 nornicdb

docker run --rm \
  -v nornicdb-data:/data:ro \
  -v "$PWD/backups:/backup" \
  busybox tar czf /backup/nornicdb-data.tar.gz -C /data .

docker start nornicdb
```

A live writable-store `tar` is not a consistent backup. If the stop deadline forced termination, record that and test crash recovery of the copy.

Restore to a new empty volume, retain the original, extract there, and start an isolated instance against it. Do not overlay unrelated backup sets.

## Scheduling and remote storage

A scheduler can call the admin API, check completion, then upload the server-side file. Keep credentials out of scripts/logs. In Kubernetes use a native backup or a quiesced CSI `VolumeSnapshot` covering required volumes; a live file copy does not establish consistency.

Set retention on the independent destination. WAL retention and the default three local recovery snapshots do not enforce backup retention or provide off-machine redundancy.

## Validate the restore set

1. Record binary version/build commit, method, digest, completion result, and required keys/configuration.
2. Restore to a separate directory/volume without production traffic.
3. Verify databases, account access, constraints/indexes, counts, representative properties, and application invariants.
4. Exercise writes/reads, reopen the DB, and verify search readiness separately.
5. Measure duration, replay work, and peak memory for the real dataset. There is no implementation-supplied recovery-time objective.

For rollback retain an independent pre-upgrade copy with matching WAL/search/replication state. Do not attach an original deployment's old WAL to a separately restored native backup. Follow [Recovery format compatibility](recovery-format-compatibility.md).

## Implementation reference

[DB backup/restore and JSON fallback](../../pkg/nornicdb/db_admin.go), [native backup/replacement](../../pkg/storage/badger_backup.go), [buffered visibility tests](../../pkg/storage/backup_buffered_visibility_test.go), and [post-restore recovery boundary](../../pkg/nornicdb/storage_recovery.go).
