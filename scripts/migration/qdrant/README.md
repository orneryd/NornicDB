# Qdrant → NornicDB Migration Scripts

Three runnable migrations to move collections + points from a source Qdrant instance into NornicDB. NornicDB exposes a Qdrant-compatible gRPC surface, so most of these scripts use the **same** Qdrant client library on both ends — only the connection target differs.

| Script | Stack | Source protocol | Target protocol |
|---|---|---|---|
| [`migrate.py`](migrate.py)  | Python 3.10+, `qdrant-client` | gRPC (`prefer_grpc=True`) | gRPC (`prefer_grpc=True`) |
| [`migrate.go`](migrate.go)  | Go 1.21+, `github.com/qdrant/go-client v1.18+` | gRPC | gRPC |
| [`migrate.mjs`](migrate.mjs) | Node 20+, `@qdrant/js-client-rest` + `neo4j-driver` | REST (Qdrant) | Bolt (NornicDB), since there is no Qdrant-compatible REST surface on NornicDB and no maintained Qdrant gRPC client for Node |

The Python and Go scripts implement a **same-API replay**: list collections, replicate vector configs (single and named), scroll points, upsert with `wait=true`, verify counts. Re-running is idempotent — `Upsert` is keyed by point ID, and existing collections can be skipped with `--skip-existing`.

The Node script implements a **bridge**: it reads from Qdrant over REST and writes into NornicDB over Bolt as nodes (one label per source collection, vector on `n.embedding`, payload merged into the node properties). Named-vector collections are not handled by the Node script — use Python or Go for that case.

## Choosing a script

- **Python** — quickest to set up, talks gRPC end-to-end. Use this unless you need otherwise.
- **Go** — no Python runtime, vendorable for CI pipelines. Talks gRPC end-to-end.
- **Node** — pick this only if you already have a Node/TS pipeline. Different shape: writes into NornicDB through Bolt rather than gRPC, so the result is graph nodes (with vector + payload) rather than Qdrant-collection-shaped data.

## Common flags

| Flag | Default | Effect |
|---|---|---|
| `--source-*` / `--target-*` | (required) | Connection details for both ends |
| `--collections a,b,c` | all | Migrate only the named collections |
| `--batch-size N` | 256 | Points per scroll/upsert batch |
| `--skip-existing` | off | Skip collections already present in target |
| `--dry-run` | off | Print the plan without writing |
| `--*-api-key TOKEN` | none | Bearer token if `Auth.Enabled=true` on either side |

## Example

```bash
# Python, gRPC end-to-end
python scripts/migration/qdrant/migrate.py \
    --source-url http://qdrant.prod:6333 \
    --target-host nornicdb.local --target-grpc-port 6334 \
    --batch-size 512 --skip-existing
```

## What gets migrated

- Vector configs (single and named vectors), distance metric, dimension.
- All points: ID, vector(s), payload.
- Idempotently: re-running upserts the same IDs, no duplicates.

## What does NOT get migrated

- Snapshots, replication settings, sharding parameters — NornicDB manages these internally; the source values are advisory.
- HNSW tuning parameters (M, ef_construct, ef_search) — NornicDB auto-tunes.
- Quantization configs — NornicDB does not currently use Qdrant-shaped quantization.
- Per-collection RBAC — re-author against NornicDB's `Auth.Enabled` model if needed.

## Verification after migration

```bash
# Spot-check a few points
python -c "
from qdrant_client import QdrantClient
c = QdrantClient(host='nornicdb.local', grpc_port=6334, prefer_grpc=True)
print(c.get_collection('docs'))
print(c.count('docs', exact=True))
points, _ = c.scroll('docs', limit=5, with_vectors=True, with_payload=True)
for p in points:
    print(p.id, len(p.vector or []), list((p.payload or {}).keys()))
"
```

## See also
- [`docs/skills/grpc.skill.md`](../../../docs/skills/grpc.skill.md) — the gRPC surface the scripts call.
- [`docs/skills/qdrant-migration.skill.md`](../../../docs/skills/qdrant-migration.skill.md) — the consumer-facing migration skill.
- [`docs/user-guides/qdrant-grpc.md`](../../../docs/user-guides/qdrant-grpc.md) — full proto compatibility matrix.
