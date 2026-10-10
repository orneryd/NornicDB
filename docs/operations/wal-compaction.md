# WAL Compaction and Truncation

**Managing Write-Ahead Log growth in NornicDB**

Last Updated: December 2025

---

## Overview

NornicDB's Write-Ahead Log (WAL) supports automatic compaction to prevent unbounded growth. Without compaction, the WAL would grow indefinitely in long-running databases, consuming disk space and slowing recovery.

**Problem Solved**: WAL grows forever until manual snapshot + delete
**Solution**: Automatic periodic snapshots with WAL truncation

---

## Features

### 1. Automatic Compaction (Recommended)

Automatic compaction is the recommended approach for production deployments. Enable it through configuration:

**YAML configuration:**

```yaml
database:
  wal_dir: "data/wal"
  wal_sync_mode: "batch"
  wal_snapshot_interval: 300000     # Milliseconds; check every five minutes
  wal_auto_compaction_enabled: true  # Enabled by default
  wal_snapshot_dir: "data/snapshots"
```

**Environment variables:**

```bash
export NORNICDB_WAL_SNAPSHOT_INTERVAL=300000
export NORNICDB_WAL_AUTO_COMPACTION_ENABLED=true
```

**Behavior:**

- WAL changes are checked at the configured interval (default: 5 minutes)
- Idle intervals do not create snapshots, scan nodes, or append checkpoints
- WAL truncated after each successful snapshot (unless retention is configured; see Retention Settings)
- Failures logged but don't crash the database
- Automatic retry on next interval
- Old snapshots saved to the snapshot directory as `snapshot-<timestamp>.json`

### 2. Manual WAL Truncation

For development or special cases, you can trigger truncation manually. Create a snapshot and then truncate the WAL to remove all entries before the snapshot point.

**Safety Guarantees:**

- Atomic rename (crash-safe)
- Old WAL remains intact until truncation succeeds
- Can retry truncation if it fails
- Recovery works from partial truncations

### 3. Disable Automatic Compaction

```yaml
database:
  wal_auto_compaction_enabled: false
```

```bash
export NORNICDB_WAL_AUTO_COMPACTION_ENABLED=false
```

### 4. Retention Settings (Immutable Segments)

NornicDB stores WAL as immutable segments with a manifest. You can retain sealed segments
for audit/ledger use cases.

**YAML configuration:**

```yaml
database:
  wal_retention_max_segments: 24
  wal_retention_max_age: "168h" # 7 days
```

**Environment variables:**

```bash
export NORNICDB_WAL_RETENTION_MAX_SEGMENTS=24
export NORNICDB_WAL_RETENTION_MAX_AGE=168h
export NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS=true
```

These settings retain sealed WAL segments **after snapshots**. Auto-compaction remains
enabled by default to preserve existing behavior; retention is **opt-in**.

With either setting, each compaction seals the active WAL into a segment instead of
dropping the entries the snapshot covers. Retention then removes only covered segments past
their age (or beyond the newest N), so `db.txlog.entries` and replication can read entries
for the retention period. Recovery still replays only the entries after the latest snapshot.
Without retention, compaction drops covered entries as before.

### 5. Txlog Query Procedures

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

---

## How It Works

### Compaction Process

When a compaction cycle runs, NornicDB flushes any pending writes, then creates a point-in-time snapshot of the current database state. Once the snapshot is safely persisted, the WAL is rewritten to contain only entries that arrived after the snapshot. The old WAL file is replaced via an atomic rename so the operation is crash-safe — at no point can a crash leave the WAL in a partial or corrupt state.

### Crash Safety

The truncation process is crash-safe at every step:

- **Before rename**: Old WAL is intact
- **During rename**: Atomic operation (old or new, never partial)
- **After rename**: New WAL is complete and synced

If a crash occurs:

- Before rename: Old WAL used on recovery (full history)
- After rename: New WAL used on recovery (snapshot + delta)

### Recovery

With auto-compaction enabled:

```
Recovery = Latest Snapshot + Post-Snapshot WAL Entries
```

Example timeline:

```
T=0:   Database starts
T=1h:  Snapshot 1 created (100 nodes), WAL truncated
T=2h:  Snapshot 2 created (150 nodes), WAL truncated
T=2.5h: Crash occurs (170 nodes in database)

Recovery:
  Load Snapshot 2 (150 nodes)
  + Replay WAL since T=2h (20 new nodes)
  = 170 nodes recovered
```

---

## Performance Impact

### Disk Space

**Before compaction:**

```
WAL size grows unbounded:
  After 1 day:  ~10GB
  After 1 week: ~70GB
  After 1 month: ~300GB
```

**After compaction (hourly):**

```
WAL size bounded by interval:
  Maximum size: ~500MB (1 hour of writes)
  Average size: ~250MB
  Disk savings: 99%+
```

### Recovery Time

**Before compaction:**

```
Recovery time = O(total history)
  1 day:  ~30 seconds
  1 week: ~3 minutes
  1 month: ~15 minutes
```

**After compaction:**

```
Recovery time = Snapshot load + O(interval writes)
  Load snapshot: ~2 seconds
  Replay WAL:    ~1 second
  Total:         ~3 seconds (constant!)
```

### Runtime Overhead

- **Snapshot creation**: ~2-5ms per 1000 nodes (async, doesn't block writes)
- **WAL truncation**: ~10-50ms (happens every hour, negligible amortized cost)
- **Total overhead**: <0.001% of runtime

---

## Configuration

### WAL Settings

| Setting | Default | Description |
|---|---|---|
| `wal_dir` | `data/wal` | WAL directory |
| `wal_sync_mode` | `batch` | Sync mode: `immediate`, `batch`, or `none` |
| `wal_batch_sync_interval` | `100ms` | Batch sync frequency |
| `wal_max_file_size` | `100MB` | File rotation trigger (bytes) |
| `wal_max_entries` | `100000` | File rotation trigger (count) |
| `wal_snapshot_interval` | `300000` | Milliseconds between WAL change checks |
| `wal_auto_compaction_enabled` | `true` | Enable/disable auto-compaction |

### Tuning Snapshot Interval

**Aggressive (every 15 minutes):**

- Minimal WAL size
- Faster recovery
- More snapshot overhead
- Good for: High-write, limited disk space

**Moderate (every hour — default):**

- Balanced disk usage
- Good recovery time
- Low overhead
- Good for: Most use cases

**Conservative (every 6 hours):**

- Larger WAL size
- Slower recovery
- Minimal overhead
- Good for: Low-write, plenty of disk space

---

## Monitoring

NornicDB exposes compaction metrics that you can use to monitor WAL health:

- **Total snapshots created** — number of successful compaction cycles since startup
- **Last snapshot time** — timestamp of the most recent snapshot
- **WAL entry count** — current number of entries in the active WAL
- **WAL bytes written** — total bytes in the active WAL

These metrics are available through the server's diagnostics and can be monitored via the admin UI or log output.

---

## Troubleshooting

### Issue: WAL still growing despite auto-compaction

**Check:**

1. Verify auto-compaction is enabled in your configuration (`wal_auto_compaction_enabled: true`)

2. Check snapshot directory for recent files:

   ```bash
   ls -lh data/snapshots/
   # Should see snapshot-<timestamp>.json files
   ```

3. Check WAL size:
   ```bash
   ls -lh data/wal/wal.log
   ```

### Issue: Truncation errors

**Symptom**: Logs show "failed to truncate WAL"

**Causes:**

- Disk full
- Permission issues
- WAL file locked by another process

**Solution:**

```bash
# Check disk space
df -h

# Check permissions
ls -l data/wal/
chmod 644 data/wal/wal.log

# Check for locks
lsof | grep wal.log
```

### Issue: Slow recovery after crash

**Check snapshot age:**

```bash
ls -lt data/snapshots/ | head -1
```

If snapshot is old, auto-compaction may not be running. Verify your configuration and check server logs for compaction errors.

---

## Best Practices

1. **Always enable auto-compaction in production** — this is the default and should not be disabled unless you have a specific reason.

2. **Monitor snapshot creation** — check server logs or metrics to confirm snapshots are being created at the expected interval.

3. **Rotate old snapshots** to avoid filling disk with historical snapshots:

   ```bash
   find data/snapshots -name "snapshot-*.json" -mtime +7 -delete
   ```

4. **Test recovery regularly** — periodically verify that your latest snapshot can be loaded and that the WAL replays correctly.
