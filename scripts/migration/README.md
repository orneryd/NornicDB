# NornicDB Migration Scripts

Runnable migrations from common sources into NornicDB. Each subdirectory ships **the same migration in three languages** so you can pick whichever fits your existing tooling.

| Source | Directory | Skill |
|---|---|---|
| Neo4j 5 (Bolt → Bolt, all three languages) | [`neo4j/`](neo4j/) | [`docs/skills/neo4j-migration.skill.md`](../../docs/skills/neo4j-migration.skill.md) |
| Qdrant (Python and Go: gRPC → gRPC; Node: Qdrant REST → NornicDB Bolt) | [`qdrant/`](qdrant/) | [`docs/skills/qdrant-migration.skill.md`](../../docs/skills/qdrant-migration.skill.md) |

## Common shape

Every script:

- Connects to a source and a target with the same SDK (`neo4j-driver`/`neo4j-go-driver/v5`/`neo4j` for Neo4j; `qdrant-client`/`go-client`/`@qdrant/js-client-rest` for Qdrant).
- Streams data in batches large enough for throughput but small enough to stay on NornicDB's hot-path executor routes.
- Replays into NornicDB using query shapes pinned to [`docs/performance/hot-path-query-cookbook.md`](../../docs/performance/hot-path-query-cookbook.md).
- Is **idempotent** when re-run — `MERGE` for nodes, `Upsert` for points, `IF NOT EXISTS` for schema.
- Supports `--dry-run` to preview the plan without writing.

## Pick a language

| Language | When to pick it |
|---|---|
| Python | Default. Quickest to set up, all dependencies on PyPI. |
| Go | No Python runtime, vendorable into CI. |
| Node | You already have a Node/TS data pipeline. |

Each subdirectory's `README.md` lists the language-specific flags. Within a single source, flag names are kept aligned across languages where the wire shape allows it — the Qdrant Node script is the one exception, since it talks Bolt to the target instead of gRPC.

## See also

- [`docs/skills/`](../../docs/skills/) — agent-facing skill files for every NornicDB surface.
- [`docs/performance/hot-path-query-cookbook.md`](../../docs/performance/hot-path-query-cookbook.md) — the query shapes the migrations target.
