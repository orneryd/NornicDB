# Storage Serialization (gob vs msgpack)

NornicDB stores nodes, edges, and embeddings in BadgerDB using a pluggable serializer. The default is now **msgpack** for better performance and lower allocations on graph‑heavy workloads.

## Supported serializers

### 1) `gob`
**Pros**
- Native Go types (no extra tags)
- Backwards compatible with older NornicDB data
- Good for quick prototypes

**Cons**
- Slower encode/decode (especially for nodes/edges)
- Higher allocations
- Go‑specific wire format

### 2) `msgpack`
**Pros**
- Significantly faster for nodes/edges
- Lower allocations and smaller payloads
- Better latency on read‑heavy workloads

**Cons**
- Embedding serialization can be slightly slower to encode
- Requires a migration step if your DB was created with gob

## Default

New databases default to **msgpack**.

MVCC note:

- MVCC version records use Msgpack
- MVCC head metadata uses Msgpack
- new MVCC/internal metadata does not use gob

If an existing database is detected to use gob, NornicDB will:
1) log a warning about the mismatch  
2) continue using gob for that database  
3) still apply msgpack to **new** databases

## Configuration

### Environment variable
```bash
export NORNICDB_STORAGE_SERIALIZER=msgpack
```

### YAML
```yaml
database:
  storage_serializer: msgpack
```

Accepted values: `gob`, `msgpack`.

For current releases, `storage_serializer` controls the primary storage format for the broader storage layer, but MVCC internal hot-path metadata is always Msgpack.

---

## Migration: gob → msgpack

**Important:** stop the NornicDB server before migrating. This is an offline, in‑place conversion.

### 1) Backup first

```bash
# While the server is running, trigger an admin backup over HTTP
curl -sf -X POST http://localhost:7474/admin/backup \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"output\":\"/backups/backup-$(date +%Y%m%d).json\"}"

# Or after stopping the server, copy the data directory
tar czf backup-$(date +%Y%m%d).tar.gz ./data
```

### 2) Dry‑run (recommended)
```bash
go run scripts/migrate_storage_serializer/main.go \
  --data-dir ./data \
  --to msgpack \
  --dry-run
```

### 3) Apply conversion
```bash
go run scripts/migrate_storage_serializer/main.go \
  --data-dir ./data \
  --to msgpack
```

### 4) Update config (so new writes stay msgpack)
```bash
export NORNICDB_STORAGE_SERIALIZER=msgpack
```

### 5) Restart NornicDB

If the database is already msgpack, the script will report “already target” and exit without changes.

---

## How the migration works

The migration script:
- scans all node, edge, and embedding records
- decodes existing gob data
- re‑encodes it in msgpack **in place**

Legacy gob records (without headers) are detected and converted safely. The script is idempotent; re‑running it will skip already‑converted values.

---

## Script usage reference

```bash
go run scripts/migrate_storage_serializer/main.go \
  --data-dir /path/to/data \
  --to msgpack \
  --batch-size 1000 \
  --dry-run
```

Flags:
- `--data-dir` (required): BadgerDB directory
- `--to`: `gob` or `msgpack`
- `--batch-size`: write batch size (default 1000)
- `--dry-run`: scan only, no writes

