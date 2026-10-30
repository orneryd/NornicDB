---
name: nornicdb-qdrant-migration
description: Migrate from Qdrant to NornicDB end-to-end through NornicDB's Qdrant-compatible gRPC surface. Covers connection setup, collection→database mapping, point→node mapping, the vector-config and named-vector replication, point upsert in batches, count verification, and what (deliberately) does not transfer (snapshots, HNSW tuning, quantization). Points to runnable Python/Go/Node scripts.
---

# Migrating from Qdrant to NornicDB

NornicDB's Qdrant-compatible gRPC service means migration is a **same-API replay**: the source and target speak the same wire, so the script reading from Qdrant can write into NornicDB through the same client library. There is no protocol bridge to build, no schema translation to invent.

This skill covers the migration surface itself. Runnable scripts are in [`scripts/migration/qdrant/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/qdrant).

## Prerequisites

1. **Enable the gRPC server on NornicDB.** Off by default:
   ```bash
   export NORNICDB_QDRANT_GRPC_ENABLED=true
   export NORNICDB_QDRANT_GRPC_LISTEN_ADDR=":6334"
   ```
2. **Match dimension limits.** If your source has vectors above `4096`, raise `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM` before running.
3. **Match auth.** If your source enforces auth, your migration script needs an API key (Qdrant) and your target needs the same if `Auth.Enabled=true`.

## Migration phases

### 1. List source collections

Use the source SDK's standard collection listing:

- Python: `client.get_collections().collections`
- Go: `client.ListCollections(ctx)`
- Node: `client.getCollections().collections`

Pick a subset with a `--collections a,b` flag if the migration is partial.

### 2. Replicate vector config per collection

Read the source collection's `VectorParams` (or named-vector `dict`/`map`) and apply them to NornicDB. Both sides accept the same shape:

```python
client.create_collection(
    collection_name="docs",
    vectors_config=m.VectorParams(size=1024, distance=m.Distance.COSINE),
)
```

Named-vector collections are passed as a dict/map where each entry has its own `size` and `distance`. NornicDB stores each named vector independently in `node.NamedEmbeddings[<vectorName>]`.

If the target already has a collection of the same name, skip the create with `--skip-existing`.

### 3. Stream points and upsert

Scroll source points in batches and upsert into target:

```python
points, offset = src.scroll(name, limit=batch, offset=offset, with_payload=True, with_vectors=True)
upserts = [m.PointStruct(id=p.id, vector=p.vector, payload=p.payload) for p in points]
dst.upsert(collection_name=name, points=upserts, wait=True)
```

Batch size guidance:
- Default `256` — safe for 1024-dim vectors.
- Tune up for shorter vectors (small payloads), down for very large ones — the gRPC `MaxRecvMsgSize` is 64 MB.
- Don't exceed `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS` (default `1000`) per upsert.

`Upsert` is idempotent by point ID. Re-running the migration refreshes existing points without creating duplicates.

### 4. Verify

After every collection: `count(name, exact=True)` on both sides should match. The scripts print `✓` / `✗` per collection and surface mismatches loudly.

## Mapping reference

| Qdrant concept | NornicDB equivalent |
|---|---|
| Collection name | Database (namespace) — `DatabaseManager.GetStorage(<name>)` |
| Point ID | Node ID `qdrant:point:<rawID>` |
| Point payload | `node.Properties` (internal `_qdrant_*` keys are filtered out on read) |
| Single vector | `node.NamedEmbeddings["default"]` |
| Named vectors | `node.NamedEmbeddings[<vectorName>]` |
| Field index | NornicDB property index |
| `score_threshold` on Search | `min_similarity` on the Cypher equivalent |

## What deliberately does NOT transfer

- **Snapshots and replication settings.** NornicDB manages persistence/replication internally; source values are advisory. Take a fresh snapshot on the target after migration if you need one.
- **HNSW tuning (M, ef_construct, ef_search).** NornicDB auto-tunes; source overrides are ignored.
- **Quantization configs.** Not currently supported on the NornicDB side. Vectors are stored full-precision after migration.
- **Per-collection RBAC.** Re-authored against NornicDB's `Auth.Enabled` + `qdrant_grpc_rbac` model if needed.

## Cutover playbook

1. **Enable NornicDB gRPC** with the env vars above.
2. **Dry-run** with `--dry-run` against a representative subset to confirm collection configs port cleanly.
3. **Migrate cold collections first** (highest cardinality, lowest QPS). Verify counts before moving to hot ones.
4. **Drain in-flight writes on Qdrant** (or pause the writer fleet) before migrating hot collections.
5. **Re-run with `--skip-existing`** to top up any incremental writes that arrived between dry-run and cutover.
6. **Flip read traffic** to NornicDB once `count` parity is observed for every collection.
7. **Keep Qdrant up read-only for a rollback window**; the migration is non-destructive on the source.
8. **Decommission** when satisfied.

## When the same SDK can't migrate cleanly

- **Quantization-only collections** with no full-precision vectors stored cannot be migrated point-by-point — re-embed from your text/image source if available, otherwise plan a re-index pass.
- **Geographic or sparse vector types** are not on NornicDB's surface today. Convert before migration or postpone.
- **Sharded multi-region Qdrant deployments** require running the migration against each shard or rebuilding the corpus on the target. There is no cross-shard replay built into the scripts.

## Running the scripts

```bash
# Python (gRPC end-to-end — recommended)
python scripts/migration/qdrant/migrate.py \
    --source-url http://qdrant.prod:6333 \
    --target-host nornicdb.local --target-grpc-port 6334 \
    --batch-size 512

# Go (gRPC end-to-end)
go run ./scripts/migration/qdrant/migrate.go \
    --source-host qdrant.prod --source-port 6334 \
    --target-host nornicdb.local --target-port 6334 \
    --batch-size 512

# Node (Qdrant REST source → NornicDB Bolt target)
# Each Qdrant point becomes a node on label `${labelPrefix}_${collection}`
# with its vector on n.embedding and payload merged into n.Properties.
# Named-vector collections are not handled on this path — use Python or Go.
node scripts/migration/qdrant/migrate.mjs \
    --source-url http://qdrant.prod:6333 \
    --target-url bolt://nornicdb.local:7687 \
    --target-user neo4j --target-pass <pw>
```

## Verification afterwards

```python
from qdrant_client import QdrantClient
c = QdrantClient(host="nornicdb.local", grpc_port=6334, prefer_grpc=True)
for col in c.get_collections().collections:
    info = c.get_collection(col.name)
    cnt = c.count(col.name, exact=True).count
    print(f"{col.name}: dim={info.config.params.vectors.size} count={cnt}")
```

## See also
- [`grpc.skill.md`](grpc.skill.md) — the gRPC surface the migration calls.
- [`scripts/migration/qdrant/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/qdrant) — Python, Go, Node migration scripts.
- [`docs/user-guides/qdrant-grpc.md`](../user-guides/qdrant-grpc.md) — full Qdrant proto compatibility matrix.
