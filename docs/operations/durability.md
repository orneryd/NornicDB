# Durability Configuration

**Configure data safety vs performance trade-offs for your workload.**

## Overview

NornicDB provides configurable durability settings that balance data safety with write performance. The default settings are optimized for most workloads, but you can enable **Strict Durability Mode** for critical data like financial transactions.

## Quick Start

```bash
# Default: Good balance (recommended for most workloads)
# No configuration needed - these are the defaults

# Strict: Maximum safety for financial/critical data
NORNICDB_STRICT_DURABILITY=true
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_STRICT_DURABILITY` | `false` | Enable maximum safety mode (2-5x slower) |
| `NORNICDB_WAL_SYNC_MODE` | `batch` | WAL sync strategy: `batch`, `immediate`, or `none` |
| `NORNICDB_WAL_SYNC_INTERVAL` | `100ms` | Interval for batch sync mode |

### Sync Modes Explained

```
┌─────────────────────────────────────────────────────────────────┐
│                    DURABILITY SPECTRUM                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  FASTEST ◄─────────────────────────────────────────► SAFEST    │
│                                                                 │
│  "none"            "batch" (DEFAULT)         "immediate"        │
│  (no fsync)        (100ms fsync)             (every write)      │
│                                                                 │
│  Performance: 10-100x          1x                 0.1-0.5x      │
│  Data risk:   HIGH          ~100ms loss            NONE         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### `batch` (Default)

```bash
NORNICDB_WAL_SYNC_MODE=batch
NORNICDB_WAL_SYNC_INTERVAL=100ms
```

- **Behavior**: Buffers writes and fsyncs every 100ms
- **Data Risk**: Up to 100ms of data loss on crash
- **Performance**: Good throughput for most workloads
- **Use Case**: General purpose, web applications, caching

#### `immediate`

```bash
NORNICDB_WAL_SYNC_MODE=immediate
# Or simply:
NORNICDB_STRICT_DURABILITY=true
```

- **Behavior**: fsync after every write
- **Data Risk**: **NONE** - every committed write is durable
- **Performance**: 2-5x slower writes
- **Use Case**: Financial data, transactions, audit logs

#### `none`

```bash
NORNICDB_WAL_SYNC_MODE=none
```

- **Behavior**: No fsync - relies on OS buffer cache
- **Data Risk**: **HIGH** - entire buffer can be lost on crash
- **Performance**: 10-100x faster writes
- **Use Case**: Testing, development, temporary data

## Strict Durability Mode

For maximum data safety, enable strict durability:

```bash
NORNICDB_STRICT_DURABILITY=true
```

This automatically enables:

| Setting | Value | Effect |
|---------|-------|--------|
| WAL Sync Mode | `immediate` | fsync every write |
| Badger SyncWrites | `true` | Sync underlying storage |
| AsyncEngine Flush | `10ms` | More frequent cache flushes |

### When to Use Strict Mode

✅ **Use Strict Durability for:**
- Financial transactions
- Payment processing
- Audit logging
- Compliance-critical data (HIPAA, SOX, PCI-DSS)
- Data that cannot be regenerated

❌ **Don't need Strict Durability for:**
- Caches
- Session data
- Search indexes
- Analytics
- Embeddings (regenerable)

## Performance Comparison

Benchmark results on Apple M3 Max with NVMe SSD:

| Mode | Writes/sec | Latency (p99) | Data Safety |
|------|------------|---------------|-------------|
| `none` | 150,000 | 0.1ms | Low |
| `batch` (default) | 50,000 | 2ms | Medium |
| `immediate` | 15,000 | 10ms | High |

## Docker Configuration

```yaml
# docker-compose.yml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    environment:
      # Default mode (recommended)
      - NORNICDB_WAL_SYNC_MODE=batch
      - NORNICDB_WAL_SYNC_INTERVAL=100ms
      
      # OR: Strict mode for financial data
      # - NORNICDB_STRICT_DURABILITY=true
```

## Programmatic Configuration

```go
import "github.com/orneryd/nornicdb/pkg/storage"

// Default: batch mode (good balance)
cfg := storage.DefaultWALConfig()
// cfg.SyncMode = "batch"
// cfg.BatchSyncInterval = 100ms

// Strict: immediate sync
cfg := &storage.WALConfig{
    Dir:      "/data/wal",
    SyncMode: "immediate",
}

wal, err := storage.NewWAL("/data/wal", cfg)
```

## Crash Recovery

NornicDB includes robust crash recovery regardless of sync mode with multiple layers of corruption prevention:

### WAL Record Format (v2)

Each WAL entry uses an atomic binary format designed to detect any form of corruption:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         WAL RECORD FORMAT (v2)                           │
├──────────┬─────────┬────────┬─────────────┬────────┬──────────┬─────────┤
│ Magic    │ Version │ Length │ Payload     │ CRC32  │ Trailer  │ Padding │
│ "WALE"   │   2     │ uint32 │ JSON bytes  │ uint32 │ 8 bytes  │ 0-7 B   │
│ 4 bytes  │ 1 byte  │ 4 bytes│ N bytes     │ 4 bytes│ canary   │ align   │
└──────────┴─────────┴────────┴─────────────┴────────┴──────────┴─────────┘
                                                          ↑          ↑
                                              0xDEADBEEFFEEDFACE    8-byte
                                              detects incomplete   prevents
                                                  writes          torn headers
```

### Corruption Prevention Layers

1. **Magic Header (`WALE`)**: Validates record boundaries and format detection
2. **Version Byte**: Forward/backward compatibility between WAL versions
3. **Length Prefix**: Detects truncated payloads from partial writes
4. **CRC32-C Checksum**: Hardware-accelerated detection of bit flips and corruption
5. **Trailer Canary (`0xDEADBEEFFEEDFACE`)**: Confirms complete write - if missing or wrong, the write was interrupted
6. **8-Byte Alignment**: Prevents torn headers on sector boundaries (512B/4KB sectors)
7. **Transaction Rollback**: Incomplete transactions are automatically rolled back on recovery

### How Each Layer Protects Your Data

| Failure Mode | Detection Mechanism |
|--------------|---------------------|
| Crash mid-header write | Partial magic or missing length |
| Crash mid-payload write | Length mismatch or truncated read |
| Crash after payload, before CRC | Missing CRC bytes detected |
| Crash after CRC, before trailer | Missing trailer canary |
| Bit flip in payload | CRC32-C mismatch |
| Sector-aligned torn write | 8-byte alignment ensures atomic header |
| Incomplete transaction | Missing commit marker triggers rollback |

### Recovery Behavior

On startup, NornicDB scans the WAL and:

1. ✅ **Commits complete entries** - All layers validated
2. ⚠️ **Skips incomplete entries** - Detected via missing trailer/CRC
3. 🔄 **Rolls back partial transactions** - No commit marker found
4. 🔁 **Regenerates embeddings** - Corrupted embedding entries skipped (safe to regenerate)

The sync mode only affects **how much data** might be lost on crash:

| Mode | Maximum Data Loss | Recovery Time |
|------|-------------------|---------------|
| `immediate` | 0 | < 1 second |
| `batch` | ~100ms | < 1 second |
| `none` | OS buffer (seconds) | < 1 second |

## Monitoring

Monitor durability metrics via Prometheus:

```promql
# WAL write latency
histogram_quantile(0.99, rate(nornicdb_wal_write_duration_seconds_bucket[5m]))

# Sync operations per second
rate(nornicdb_wal_syncs_total[5m])

# Flush errors (should be 0)
nornicdb_async_engine_flush_errors_total
```

## See Also

- [WAL Compaction](wal-compaction.md) - Automatic disk space management
- [Backup & Restore](backup-restore.md) - Data protection strategies
- [Monitoring](monitoring.md) - Prometheus metrics reference

---

**Configure for your workload** → Use defaults for most cases, strict for critical data.
