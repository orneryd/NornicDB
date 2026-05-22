# Configuration Guide

This guide covers all configuration options for NornicDB, including the new async write settings and search similarity configuration.

## Configuration File

NornicDB uses a YAML configuration file (typically `nornicdb.yaml`) that can be specified via:

```bash
./nornicdb serve --config /path/to/nornicdb.yaml
```

Or via environment variables (see Environment Variables section).

Canonical inventory (all runtime-referenced names): [Environment Variables Reference](environment-variables.md)

### Config file discovery (when `--config` is not provided)

NornicDB searches for a config file in this order:

1. `NORNICDB_CONFIG` (explicit path)
2. `~/.nornicdb/config.yaml`
3. next to the binary: `config.yaml` or `nornicdb.yaml`
4. current working directory: `config.yaml` or `nornicdb.yaml`
5. container mount path: `/config/nornicdb.yaml` or `/config/config.yaml`
6. OS user config dirs:
   - macOS: `~/Library/Application Support/NornicDB/config.yaml`
   - Linux: `~/.config/nornicdb/config.yaml`

To avoid ambiguity in Docker/Kubernetes, prefer:

```bash
export NORNICDB_CONFIG=/config/nornicdb.yaml
```

## Core Configuration

## Retention Policies Opt-In

Runtime retention enforcement is disabled by default. Enable it explicitly with `compliance.retention_enabled: true` or `NORNICDB_RETENTION_ENABLED=true`.

When retention is disabled:

- no retention manager is created
- no retention sweep background worker starts
- no retention policy file is persisted on shutdown
- retention admin endpoints return `503 Service Unavailable`

Minimal opt-in example:

```yaml
compliance:
  retention_enabled: true
  retention_policy_days: 30
  retention_auto_delete: false

retention:
  sweep_interval: 3600
  default_policies: false
  excluded_labels: ["AuditLog", "System"]
```

The `retention:` block is inert until retention is enabled.

`retention.sweep_interval` must be specified as an integer number of seconds. Shorthand duration strings are not supported.

### Database Settings

```yaml
# Database storage and basic settings
database:
  path: /data/nornicdb.db
  default_database: "nornic" # Default database name (like Neo4j's "neo4j")
  max_connections: 100
  connection_timeout: 30s
  storage_serializer: msgpack # default: msgpack; MVCC metadata uses msgpack on the hot path
  mvcc_retention_max_versions: 1
  mvcc_retention_ttl: 168h
```

**Multi-Database Support:**

- Default database name: `"nornic"` (configurable)
- System database: `"system"` (for metadata, not user-accessible)
- Multiple databases can be created via `CREATE DATABASE` command
- Each database is completely isolated (multi-tenancy)
- **Database Aliases**: Create alternate names for databases (`CREATE ALIAS`, `DROP ALIAS`, `SHOW ALIASES`)
- **Resource Limits**: Set per-database resource limits (`ALTER DATABASE SET LIMIT`, `SHOW LIMITS`)
- **Automatic migration**: Existing data is automatically migrated to the default database on first startup after upgrading
- Configuration precedence: CLI args > Env vars > Config file > Defaults

**Environment Variables:**

- `NORNICDB_DEFAULT_DATABASE` - Set default database name
- `NEO4J_dbms_default__database` - Neo4j-compatible env var (backwards compat)
- `NORNICDB_STORAGE_SERIALIZER` - Storage serializer (`gob` or `msgpack`)
- `NORNICDB_MVCC_RETENTION_MAX_VERSIONS` - Default historical version cap per key
- `NORNICDB_MVCC_RETENTION_TTL` - Protect recent MVCC history from pruning

### MVCC Historical Retention

NornicDB keeps MVCC history for snapshot and temporal reads. The retention policy controls the default pruning behavior for that history.

```yaml
database:
  storage_serializer: msgpack
  mvcc_retention_max_versions: 1
  mvcc_retention_ttl: "168h"
```

Semantics:

- `mvcc_retention_max_versions` applies to closed historical versions
- the current head is preserved separately and is never pruned
- `mvcc_retention_ttl` protects versions newer than `now - ttl`
- these settings define defaults for maintenance calls; they do not start background pruning on their own

Recommended starting points:

- default deployment: `100` versions, no TTL
- moderate churn: `50` versions, `24h` TTL
- audit-focused: `100` versions, `168h` TTL

For query examples and maintenance usage, see [Historical Reads & MVCC Retention](../user-guides/historical-reads-mvcc-retention.md).

### Provider-backed at-rest encryption

NornicDB supports provider-backed storage encryption for Badger using a wrapped data-encryption key persisted in the data directory.

Supported provider modes:

- `password`
- `local`
- `aws-kms`
- `azure-keyvault`
- `gcp-cloudkms`

Example:

```yaml
database:
  encryption_enabled: true
  encryption_provider: "aws-kms"
  encryption_aws_region: "us-east-1"
  encryption_aws_kms_key_id: "arn:aws:kms:us-east-1:123456789012:key/..."
  encryption_audit_sign_events: true
  encryption_audit_sign_key: "replace-with-hmac-signing-key"
  encryption_rotation_enabled: true
  encryption_rotation_interval: "2160h"
```

See:

- [CMEK Setup](../encryption/cmek-setup.md)
- [HSM Integration](../encryption/hsm-integration.md)
- [Compliance Evidence](../encryption/compliance-evidence.md)

### Per-database configuration overrides

Instance-level configuration (env, config file) is the **default** for every database. You can override specific settings **per database** so that embedding, search, HNSW, k-means, and related options can differ by database.

- **Precedence:** For a given database, effective config = global defaults (env + config file) **overlaid by** per-database overrides. Any key not set in overrides uses the global value.
- **Storage:** Overrides are stored in the **system database** (same pattern as RBAC allowlist/privileges). They are loaded at startup and on every `PUT` so all nodes see the same view.
- **Management:**
  - **Admin API:** `GET /admin/databases/{dbName}/config` returns `overrides` and `effective`; `PUT /admin/databases/{dbName}/config` with body `{ "overrides": { "NORNICDB_EMBEDDING_MODEL": "bge-m3", ... } }` saves overrides. `GET /admin/databases/config/keys` returns the list of allowed keys and their types/categories. All require admin authentication.
  - **UI:** On the **Databases** page, users with the admin role see a settings (cog) button on each database card. Clicking it opens a configuration modal where you can set or clear overrides per key; "Use default" means that key is not overridden.
- **Allowed keys:** Embedding (`NORNICDB_EMBEDDING_*`, including `NORNICDB_EMBEDDING_API_KEY`), search (`NORNICDB_SEARCH_*`, including `NORNICDB_SEARCH_RERANK_API_KEY`), HNSW (`NORNICDB_VECTOR_*`), k-means (`NORNICDB_KMEANS_*`), auto-links (`NORNICDB_AUTO_LINKS_*`), Auto-TLP (`NORNICDB_AUTO_TLP_*`), and embed worker (`NORNICDB_EMBED_*`). All listed keys can be set per database.
- **Effect:** Search service creation (vector dimensions, min similarity) and, where implemented, other per-DB behaviour use the resolved config for that database. Changing overrides takes effect for new operations; existing search service caches may need a restart or index rebuild for index-related settings to apply fully.
- **Search pipeline and query embedding:** The search pipeline must embed the **query** using the same effective config (and thus dimensions) as the **index** for that database to avoid vector dimension mismatches. The HTTP search handler uses per-database resolved config when embedding the query: it validates that the global embedder's output dimensions match the database's resolved embedding dimensions. If they differ (e.g. you set a per-DB override for embedding dimensions that does not match the global embedder), the API returns `400 Bad Request` with a clear message instead of returning empty vector results. Align global embedding dimensions with per-DB overrides, or leave per-DB embedding dimensions unset so they match global.
- **Remote embedding providers (OpenAI, Ollama) per database:** You can set per-DB overrides for `NORNICDB_EMBEDDING_PROVIDER`, `NORNICDB_EMBEDDING_MODEL`, `NORNICDB_EMBEDDING_API_URL`, `NORNICDB_EMBEDDING_API_KEY`, and `NORNICDB_EMBEDDING_DIMENSIONS` so different databases use different models, endpoints, or API keys. When a database uses provider `openai` (or another provider that requires a key), the **resolved** API key for that database (global default or per-DB override) is used. Ensure the effective API key is set and valid for any database that uses a provider requiring it. Ollama typically does not require an API key; per-DB URL and model work without change.

### Per-database search index control

Two orthogonal axes per index, four keys total.

#### Precedence ladder

Configuration values for these flags resolve through a fixed ladder, lowest → highest:

1. **Built-in defaults** — bm25=true, vector=true, both warming=startup.
2. **Global config** — `cfg.Memory.Search*` populated by YAML and env vars (`NORNICDB_SEARCH_*`).
3. **Per-DB overrides** — admin API (`PUT /admin/databases/{name}/config`) and the YAML `databases:` block.
4. **CLI overrides** — `--search-bm25-enabled`, `--search-bm25-warming`, `--search-vector-enabled`, `--search-vector-warming` on `nornicdb serve`. Only flags explicitly typed on the command line participate; unset flags inherit the next level down.

**Why CLI trumps per-DB.** CLI is the operational kill switch. When an operator types `--search-bm25-enabled=false` at boot — typically during an OOM incident, disk-pressure recovery, or debug session — that intent must take effect even if a tenant's per-DB store entry says otherwise. Env and YAML do **not** get this treatment because they're declarative config that's easy to forget in a compose file or k8s manifest, and a stale env shouldn't be able to silently revert a tenant's intentional per-DB configuration. CLI is what an operator types when something is on fire; it's unambiguous.

In all other directions: per-DB overrides win over global, in both directions. An override of `true` turns on a globally-disabled index; an override of `false` turns off a globally-enabled one. Same for warming. The "always wins" guarantee is what makes the multi-tenant story work (one DB needs search, the rest don't).

| Key                              | Type    | Default   | Meaning                                                                                                                                                                                                                                       |
| -------------------------------- | ------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_SEARCH_BM25_ENABLED`   | boolean | `true`    | Master switch for BM25 fulltext search. When false, no BM25 build runs.                                                                                                                                                                     |
| `NORNICDB_SEARCH_BM25_WARMING`   | enum    | `startup` | When BM25 is enabled, choose `startup` (build at boot) or `lazy` (defer until first search query, which blocks synchronously while warming).                                                                                                |
| `NORNICDB_SEARCH_VECTOR_ENABLED` | boolean | `true`    | Master switch for vector search across every ANN strategy (HNSW, IVF-HNSW, brute-force, GPU, Metal, Qdrant). When false, node embeddings are NOT iterated into the in-memory ANN substrate — strongest available memory-pressure lever.   |
| `NORNICDB_SEARCH_VECTOR_WARMING` | enum    | `startup` | When vector is enabled, choose `startup` or `lazy`. See the BM25 warming description.                                                                                                                                                       |

Behavior summary (all combinations supported):

| BM25         | Vector       | First search request                                                            |
| ------------ | ------------ | ------------------------------------------------------------------------------- |
| on / startup | on / startup | Hybrid (today's default).                                                       |
| on / startup | on / lazy    | Synchronous wait while vector warms; first response includes vector results.    |
| on / lazy    | on / lazy    | Synchronous wait while both warm; first response is fully ranked.                |
| on / startup | off / —      | Lexical-only 200.                                                               |
| off / —      | on / startup | Vector-only 200 (HNSW falls back to random insertion order).                    |
| off / —      | off / —      | 503 `search_disabled_for_database`, `retryable: false` — permanent.            |

Configure via (in order of effective precedence; later sources override earlier ones):

- **YAML global** (`memory.search_*` block in `nornicdb.yaml`): sets the global default. Affects all databases that don't have a per-DB override or a CLI flag set.
- **Env vars** (`NORNICDB_SEARCH_*` at boot): override YAML on the global default.
- **YAML per-DB** (`databases:` map keyed by database name; see the [yaml schema example](#yaml-databases-map)): seeded into the dbconfig store on first boot only.
- **Admin API** (`PUT /admin/databases/{name}/config`): per-DB overrides applied at runtime; flipping `enabled=true→false` triggers an immediate teardown of the affected index.
- **CLI** (`--search-bm25-enabled`, `--search-bm25-warming`, `--search-vector-enabled`, `--search-vector-warming` on `nornicdb serve`): explicit boot-time overrides. Top of the precedence ladder — these trump per-DB store entries (the kill-switch contract above). Only flags explicitly set on the command line participate.

Health checks **must not** target `/nornicdb/search` for `warming=lazy` or any `*_enabled=false` database — use `/nornicdb/health` (DB-agnostic) or `/admin/databases/{name}/config` (lookup-only) instead. Probing search on a lazy DB triggers the build on every probe; probing a disabled DB streams 503s that look like real failures in monitoring.

#### yaml databases: map

```yaml
databases:
  hot_app_db: {}                # both indexes default (enabled, startup)

  analytics:
    NORNICDB_SEARCH_BM25_ENABLED:   "false"
    NORNICDB_SEARCH_VECTOR_WARMING: "lazy"

  audit_logs:
    NORNICDB_SEARCH_BM25_ENABLED:   "false"
    NORNICDB_SEARCH_VECTOR_ENABLED: "false"

  exports_only:
    NORNICDB_SEARCH_BM25_ENABLED:   "true"
    NORNICDB_SEARCH_VECTOR_ENABLED: "false"  # write embeddings; never load in-process
```

The yaml `databases:` map is read into `dbconfig.Store` **only on first boot** for each `(dbName, key)` pair. Once an admin has PUT a value via `/admin/databases/{name}/config`, that value is authoritative across restarts and yaml changes for the same key are ignored. Operators who want yaml to win again can either delete the `_DbConfig` node from the system database or PUT the desired value back via the admin API.

### Server Settings

```yaml
server:
  bolt_enabled: true
  bolt_port: 7687
  bolt_address: "0.0.0.0"
  bolt_server_announcement: "" # Optional compatibility override for the Bolt HELLO server string
  bolt_tls_enabled: false

  http_enabled: true
  http_port: 7474
  http_address: "0.0.0.0"
  http_tls_enabled: false
```

### Bolt announcement override for strict Neo4j clients

Some Bolt clients, especially `cypher-shell`, check the Bolt `HELLO` success metadata and reject connections unless the announced server looks like Neo4j. NornicDB can override that announcement when you explicitly opt in.

Use this only when a client blocks on the server identity string.

Environment variable:

```bash
export NORNICDB_BOLT_SERVER_ANNOUNCEMENT="Neo4j/5.26.0"
```

YAML:

```yaml
server:
  bolt_server_announcement: "Neo4j/5.26.0"
```

Typical `cypher-shell` workflow:

```bash
export NORNICDB_BOLT_SERVER_ANNOUNCEMENT="Neo4j/5.26.0"
./nornicdb serve
cypher-shell -a bolt://localhost:7687 -u neo4j -p password
```

Notes:

- This changes only the Bolt `server` metadata returned during `HELLO`.
- It does not claim full Neo4j product identity or feature parity.
- Leave it unset unless a strict client requires it.

## Async Write Settings ⭐ New

The async write engine provides write-behind caching for improved throughput. Async-eligible auto-commit writes return immediately after updating the cache and are flushed to disk asynchronously. Mutations that still need the implicit transactional path continue to execute synchronously and produce durable receipts.

Current behavior:

- Pure auto-commit `CREATE` statements are the primary eventual-consistency path
- Schema commands, system commands, and read-modify-write mutations such as `MATCH ... CREATE`, `CREATE ... SET`, `MERGE`, `DELETE`, and `SET` remain on the durable transactional path
- Eventual HTTP responses return `202 Accepted`, `X-NornicDB-Consistency: eventual`, and `optimistic` metadata instead of a durable `receipt`

### Configuration

```yaml
# === Async Write Settings ===
# These control the async write-behind cache for better throughput
async_writes:
  enabled: true # Enable async writes (default: true)
  flush_interval: 50ms # How often to flush pending writes
  max_node_cache_size: 50000 # Max nodes to buffer before forcing flush
  max_edge_cache_size: 100000 # Max edges to buffer before forcing flush
```

### Environment Variables

| Variable                             | Default  | Description                       |
| ------------------------------------ | -------- | --------------------------------- |
| `NORNICDB_ASYNC_WRITES_ENABLED`      | `true`   | Enable/disable async writes       |
| `NORNICDB_ASYNC_FLUSH_INTERVAL`      | `50ms`   | Control flush frequency           |
| `NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE` | `50000`  | Limit memory usage for node cache |
| `NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE` | `100000` | Limit memory usage for edge cache |

### Performance Tuning

**For High Throughput (bulk operations):**

```yaml
async_writes:
  enabled: true
  flush_interval: 200ms # Larger = better throughput
  max_node_cache_size: 100000 # Increase for bulk inserts
  max_edge_cache_size: 200000
```

**For Lower Staleness Window (still eventual consistency):**

```yaml
async_writes:
  enabled: true
  flush_interval: 10ms # Smaller = more consistent
  max_node_cache_size: 1000 # Smaller = less memory risk
  max_edge_cache_size: 2000
```

**For Maximum Durability:**

```yaml
async_writes:
  enabled: false # Disable async writes
```

With `enabled: true`, do not assume every mutation becomes eventual. The setting enables the async write-behind engine and lets eligible queries use it; durable transactional mutations still return `200 OK` and include `receipt` metadata.

### Memory Management

The cache size limits prevent unbounded memory growth during bulk operations:

- **Set to 0** for unlimited cache size (not recommended for production)
- **Monitor memory usage** during bulk operations
- **Adjust based on available RAM** and operation patterns

## Vector Search Configuration

### Embedding Settings

```yaml
embeddings:
  provider: local # or ollama, openai
  model: bge-m3
  dimensions: 1024
```

> Note: embedding generation is **disabled by default** in current releases. Enable it explicitly with `NORNICDB_EMBEDDING_ENABLED=true` (or `nornicdb serve --embedding-enabled`) to get semantic search without manually storing vectors.

### Embedding text: which properties are used

By default, the embedding worker builds text from **all node properties** plus **node labels**. Managed embedding metadata is stored internally (in `EmbedMeta`, not `Properties`). You can restrict this so that only specific properties are embedded, or exclude certain properties.

Use this when you want to:

- **Embed only one field** (e.g. only `content`) to avoid re-embedding stored embeddings or noisy fields.
- **Exclude internal or large fields** (e.g. `internal_id`, `raw_html`) from the embedding text.

**YAML** (under `embedding_worker`):

```yaml
embedding_worker:
  # Optional: only these property keys are used when building embedding text (empty = all).
  properties_include: [content] # e.g. only "content", or [content, title, description]
  # Optional: these property keys are never used (in addition to built-in metadata skips).
  properties_exclude: [internal_id, raw_html]
  # Whether to prepend node labels to the embedding text (default: true).
  include_labels: true
```

**Environment variables:**

| Variable                                | Default | Description                                                                                                                                                        |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `NORNICDB_EMBEDDING_PROPERTIES_INCLUDE` | (empty) | Comma-separated list of property keys to use. If set, **only** these keys (and optionally labels) are embedded. Example: `content` or `content,title,description`. |
| `NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE` | (empty) | Comma-separated list of property keys to exclude from embedding text. Example: `internal_id,raw_html`.                                                             |
| `NORNICDB_EMBEDDING_INCLUDE_LABELS`     | `true`  | Set to `false` to omit node labels from the embedding text (e.g. when embedding only a single field).                                                              |

**Behavior:**

- If **properties_include** is set, only those keys (minus any in the exclude list) are used.
- **properties_exclude** is always applied; it is also applied when include is set (so an excluded key is never embedded even if listed in include).
- Precedence: defaults → config file → environment variables (env wins).

### Search Similarity ⭐ New

Configure minimum similarity thresholds for vector search:

```yaml
search:
  min_similarity: 0.5 # Default threshold (0.0-1.0)
```

### Programmatic Configuration

You can also configure similarity settings programmatically:

```go
// Set default minimum similarity
searchService.SetDefaultMinSimilarity(0.7)

// Get current default
current := searchService.GetDefaultMinSimilarity()

// Per-search override
results, err := searchService.Search(ctx, &SearchOptions{
    Query: "machine learning",
    MinSimilarity: &[]float64{0.8}[0], // Override for this search only
})
```

### Apple Intelligence Compatibility

For Apple Intelligence integration, use lower similarity thresholds:

```yaml
search:
  min_similarity: 0.3 # Lower threshold for AI assistants
```

### Search index persistence

By default, BM25 (full-text) and vector (HNSW) search indexes are built in memory on startup by scanning storage. When **persist search indexes** is enabled, NornicDB saves these indexes to disk under the data directory and loads them on startup when present, skipping the full scan and speeding up startup for large graphs.

> **Experimental:** Search index persistence is currently experimental.
> If persisted artifacts are missing/incompatible and a rebuild is required, startup can still be long on large datasets.
> Observed reference point: rebuilding IVF-HNSW for ~1M embeddings can take ~30 minutes on startup (hardware dependent).

**When to use:**

- Large databases where rebuilding indexes on every startup is slow.
- Restarts or deployments where you want search to be ready immediately after storage recovery.

**YAML** (under `database`):

```yaml
database:
  persist_search_indexes: true # EXPERIMENTAL. Default: false. Requires data_dir to be set.
```

**Environment variable:**

| Variable                          | Default | Description                                                                                                                                                                                                                |
| --------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_PERSIST_SEARCH_INDEXES` | `false` | **EXPERIMENTAL.** When `true`, save and load BM25, vector, and HNSW indexes under `DataDir/search/<dbname>/` (e.g. `bm25.gob`, `vectors`, `hnsw`). Has no effect if `NORNICDB_DATA_DIR` (or config `data_dir`) is not set. |

**Behavior:**

- Indexes are written under `data_dir/search/<database_name>/` (e.g. `bm25.gob`, `vectors`, `hnsw`).
- After node index/remove operations, changes are persisted after a short debounce delay (configurable via `NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC`); on graceful shutdown, indexes are flushed to disk.
- On startup, if both index files exist and are compatible with the current format version, they are loaded and the full storage iteration is skipped; otherwise indexes are rebuilt as usual.
- Storage recovery (WAL) runs first; search indexes are built or loaded after storage is consistent.

### Vector search strategy and HNSW tuning

Vector search chooses a strategy automatically based on dataset size and features (GPU, clustering). All thresholds and HNSW parameters are configurable via environment variables.

Runtime transition behavior:

- Strategy checks run on index mutations (`IndexNode` / `RemoveNode`).
- The service switches among `CPU brute-force`, `GPU brute-force`, and `global HNSW` using threshold crossings.
- Brute-force CPU/GPU switches do not rebuild ANN graphs.
- Brute-force/HNSW transitions run with debounced scheduling and background build/swap so query serving continues.
- Writes during transition are replayed before cutover to keep the target index current.

**Strategy selection (order of precedence):**

1. **GPU brute-force** – when GPU is enabled and vector count is in `[NORNICDB_VECTOR_GPU_BRUTE_MIN_N, NORNICDB_VECTOR_GPU_BRUTE_MAX_N]`.
2. **CPU brute-force** – when vector count is under 5000 (fixed constant `NSmallMax`; no env override).
3. **Cluster-based** (IVF-HNSW or k-means) – when clustering is enabled and built.
4. **Global HNSW** – when vector count is 5000 or more and neither GPU nor clustering is chosen.

So the switch from CPU brute-force to HNSW happens at **5000 vectors**. If you see slow search with more than 5k vectors, ensure the HNSW index is being used (e.g. check logs for `HNSW index created`) and consider the HNSW quality/efSearch settings below.

| Variable                                       | Default  | Description                                                                                                                                       |
| ---------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Strategy thresholds**                        |          |                                                                                                                                                   |
| `NORNICDB_VECTOR_GPU_BRUTE_MIN_N`              | `5000`   | Min vector count to use GPU brute-force (exact search).                                                                                           |
| `NORNICDB_VECTOR_GPU_BRUTE_MAX_N`              | `15000`  | Max vector count for GPU brute-force; above this, HNSW is preferred by default.                                                                   |
| `NORNICDB_VECTOR_IVF_HNSW_ENABLED`             | `false`  | When clustered, use IVF-HNSW (per-cluster HNSW) when available. Disabled by default.                                                              |
| `NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE`    | `200`    | Min cluster size to build a cluster HNSW index.                                                                                                   |
| `NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS`        | `1024`   | Max number of clusters for IVF-HNSW.                                                                                                              |
| **Hybrid lexical-semantic routing**            |          |                                                                                                                                                   |
| `NORNICDB_VECTOR_ROUTING_MODE`                 | `hybrid` | Cluster routing mode: `hybrid` (lexical+semantic) or `semantic` (centroid-only).                                                                  |
| `NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM`         | `0.7`    | Weight for semantic (centroid) routing score.                                                                                                     |
| `NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX`         | `0.3`    | Weight for lexical (BM25-term profile) routing score.                                                                                             |
| `NORNICDB_VECTOR_HYBRID_ROUTING_LEX_TOP_TERMS` | `64`     | Number of high-value terms kept per cluster lexical profile.                                                                                      |
| **K-means clustering**                         |          |                                                                                                                                                   |
| `NORNICDB_KMEANS_NUM_CLUSTERS`                 | (auto)   | Number of clusters. Unset or 0 = **auto** from dataset size at run time (√(n/2), clamped 10–8192). Set to a positive value to fix K (e.g. `500`). |
| `NORNICDB_KMEANS_MAX_ITERATIONS`               | `5`      | Max k-means iterations (early stop when stable). Minimum is clamped to 5.                                                                         |
| `NORNICDB_KMEANS_SEED_MAX_TERMS`               | `256`    | Max BM25 high-IDF terms used to build seed candidates when always-on lexical seeding is attempted.                                                |
| `NORNICDB_KMEANS_SEED_DOCS_PER_TERM`           | `1`      | Max seed docs selected per term when always-on lexical seeding is attempted.                                                                      |
| **HNSW index (quality preset)**                |          |                                                                                                                                                   |
| `NORNICDB_VECTOR_ANN_QUALITY`                  | `fast`   | Preset: `fast` \| `balanced` \| `accurate` \| `compressed`. See tables below.                                                                     |
| `NORNICDB_VECTOR_HNSW_M`                       | (preset) | Max connections per node (e.g. 16 or 32). Overrides preset.                                                                                       |
| `NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION`         | (preset) | Candidate list size during index build. Overrides preset.                                                                                         |
| `NORNICDB_VECTOR_HNSW_EF_SEARCH`               | (preset) | Candidate list size during search; higher = better recall, slower. Overrides preset.                                                              |
| `NORNICDB_VECTOR_PQ_SEGMENTS`                  | `16`     | Compressed mode only. Number of PQ segments (`dimensions` must be divisible by this).                                                             |
| `NORNICDB_VECTOR_PQ_BITS`                      | `8`      | Compressed mode only. Bits per PQ code (currently clamped to 4-8).                                                                                |
| `NORNICDB_VECTOR_IVFPQ_NPROBE`                 | `16`     | Compressed mode only. Number of IVF lists probed per query.                                                                                       |
| `NORNICDB_VECTOR_IVFPQ_RERANK_TOPK`            | `200`    | Compressed mode only. Max candidates sent to exact rerank.                                                                                        |
| `NORNICDB_VECTOR_IVFPQ_TRAINING_SAMPLE_MAX`    | `200000` | Compressed mode only. Maximum vectors sampled for IVFPQ training.                                                                                 |
| **HNSW Metal (GPU)**                           |          |                                                                                                                                                   |
| `NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES`    | `0`      | If greater than 0, use Metal for HNSW search when candidate count meets threshold. `0` = disabled.                                                |
| **HNSW maintenance**                           |          |                                                                                                                                                   |
| `NORNICDB_HNSW_MAINT_INTERVAL_MS`              | `30000`  | Interval (ms) for HNSW maintenance (tombstone checks).                                                                                            |
| `NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC`       | `60`     | Min interval between HNSW rebuilds (seconds).                                                                                                     |
| `NORNICDB_HNSW_TOMBSTONE_REBUILD_RATIO`        | `0.50`   | Rebuild when tombstone ratio exceeds this.                                                                                                        |
| `NORNICDB_HNSW_MAX_TOMBSTONE_OVERHEAD_FACTOR`  | `2.0`    | Max tombstone overhead before rebuild.                                                                                                            |
| `NORNICDB_HNSW_REBUILD_ENABLED`                | `true`   | Enable periodic HNSW rebuilds when tombstone ratio is high.                                                                                       |
| **Index persistence**                          |          |                                                                                                                                                   |
| `NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC`      | `30`     | Debounce delay (seconds) before writing BM25/vector indexes to disk after updates.                                                                |

**HNSW quality presets:**

| Preset     | M   | efConstruction | efSearch | Use case                      |
| ---------- | --- | -------------- | -------- | ----------------------------- |
| `fast`     | 16  | 100            | 50       | Faster queries, lower recall. |
| `balanced` | 16  | 200            | 100      | Good balance.                 |
| `accurate` | 32  | 400            | 200      | Higher recall, slower search. |

To reduce latency (e.g. if search is ~4s), try `NORNICDB_VECTOR_ANN_QUALITY=fast` or lower `NORNICDB_VECTOR_HNSW_EF_SEARCH` (e.g. `50`). Ensure you have ≥ 5000 vectors so the pipeline uses HNSW instead of CPU brute-force.

### Compressed ANN mode (IVFPQ)

`NORNICDB_VECTOR_ANN_QUALITY=compressed` enables the compressed ANN path designed to reduce memory footprint at large scale.

How it works:

1. Build/load an IVF/PQ index (coarse lists + compressed codes).
2. Probe a bounded number of lists (`nprobe`) for approximate candidates.
3. Score candidates in compressed space.
4. Re-rank a bounded top window using exact vectors for result quality.

This mode is integrated end-to-end: build, persist, startup load/rebuild, search query path, and compatibility checks.

**Safety behavior:**

- If compressed mode prerequisites are not satisfied for a run (for example, invalid dimensions/segments combination or insufficient vectors for current profile), NornicDB logs a clear diagnostic and safely falls back to the standard path instead of failing the service.

**Quick enable (environment):**

```bash
export NORNICDB_VECTOR_ANN_QUALITY=compressed
export NORNICDB_VECTOR_PQ_SEGMENTS=16
export NORNICDB_VECTOR_PQ_BITS=8
export NORNICDB_VECTOR_IVFPQ_NPROBE=16
export NORNICDB_VECTOR_IVFPQ_RERANK_TOPK=200
export NORNICDB_VECTOR_IVFPQ_TRAINING_SAMPLE_MAX=200000
```

**Tuning guidance (compressed mode):**

- Increase `NORNICDB_VECTOR_IVFPQ_NPROBE` to improve recall (usually increases latency).
- Increase `NORNICDB_VECTOR_IVFPQ_RERANK_TOPK` to improve final quality consistency (usually increases latency and exact-score IO).
- Increase `NORNICDB_VECTOR_PQ_SEGMENTS` or `NORNICDB_VECTOR_PQ_BITS` to improve compressed-space fidelity (increases memory/build cost).
- Keep `NORNICDB_VECTOR_PQ_SEGMENTS` a divisor of embedding dimensions.

#### Tradeoff snapshot (latest benchmark matrix)

Reference run: `BenchmarkANNQueryPipelineChunked`, `benchtime=2s`, `count=3`, Apple M3 Max.

Latency (ns/op), averaged:

```text
Query Latency vs Dataset Size (ns/op)
Scale: 1 block ~= 2,000 ns

N=1500
HNSW   5,810  |███
IVFPQ 22,995  |███████████

N=3000
HNSW   5,748  |███
IVFPQ 42,643  |█████████████████████

N=6000
HNSW   5,828  |███
IVFPQ 38,894  |███████████████████

N=12000
HNSW   5,642  |███
IVFPQ 48,735  |████████████████████████
```

Memory tradeoff (current implementation):

1. **Per-query working-set memory** (`heap delta`, MiB), averaged from the same runs:

| Dataset size | HNSW heap delta | IVFPQ heap delta |
| -----------: | --------------: | ---------------: |
|       `1500` |          `1.56` |           `1.57` |
|       `3000` |          `1.57` |           `2.08` |
|       `6000` |          `1.57` |           `2.08` |
|      `12000` |          `1.58` |           `2.08` |

```text
Query Heap Delta vs Dataset Size (MiB)
Scale: 1 block ~= 0.25 MiB

N=1500
HNSW   1.56 MiB |██████
IVFPQ  1.57 MiB |██████

N=3000
HNSW   1.57 MiB |██████
IVFPQ  2.08 MiB |████████

N=6000
HNSW   1.57 MiB |██████
IVFPQ  2.08 MiB |████████

N=12000
HNSW   1.58 MiB |██████
IVFPQ  2.08 MiB |████████
```

Per-query memory summary:

- HNSW path: ~`1.56-1.59 MiB`
- Compressed IVFPQ path: ~`1.57-2.08 MiB`

2. **Index-size economics** (how many embeddings fit per node):

Reference run: `BenchmarkANNQualityMatrixChunked/full_n=12000` (same hardware family).

| Metric                                         |       HNSW | Compressed IVFPQ |
| ---------------------------------------------- | ---------: | ---------------: |
| Build-time index heap (`heap_build_mib`)       | `3.61 MiB` |       `1.08 MiB` |
| Relative embeddings per same index-heap budget |     `1.0x` |         `~3.34x` |

Current compressed profile internals (benchmark profile: `dims=32`, `PQ segments=16`):

- Raw float32 vector payload: `32 * 4 = 128 bytes/vector`
- Compressed code payload (current IVFPQ accounting): `16 + 2 = 18 bytes/vector`
- Payload ratio: `~7.1x` smaller compressed representation

Interpretation (current state only):

- Compressed ANN currently trades higher query latency for better index memory economics.
- In current measurements, compressed query memory pressure is now close to HNSW at smaller slices and remains bounded at larger slices.
- In current measurements, compressed mode supports roughly `3.34x` more embeddings per equivalent in-memory index budget on this benchmark shape.
- Use compressed mode when your primary objective is ANN scale economics and predictable bounded candidate behavior; use `fast|balanced|accurate` when lowest query latency is the primary objective.

### Search timing diagnostics

Use these flags to identify whether latency comes from embedding, vector retrieval, BM25, or fusion:

| Variable                       | Default | Description                                                                                       |
| ------------------------------ | ------: | ------------------------------------------------------------------------------------------------- |
| `NORNICDB_SEARCH_LOG_TIMINGS`  | `false` | Logs per-search service stage timing (`vector_ms`, `bm25_ms`, `fusion_ms`, candidates, fallback). |
| `NORNICDB_SEARCH_DIAG_TIMINGS` | `false` | Logs HTTP-handler timing breakdown (`embed_total`, `search_total`, `embed_calls`, chunk stats).   |

Typical diagnostic pattern for fulltext-only/fallback traffic after optimization:

- `embed_calls=0`
- `embed_total=0s`
- `search_total` in microseconds

Observed reference on Apple M3 Max (64GB RAM), varied cache-busting queries:

- **Embedding-query path:** sequential p50 ~11.28ms, p95 ~25.84ms; concurrent (8 workers) p50 ~76.36ms, p95 ~87.41ms
- **Fulltext-only path:** sequential p50 ~0.57ms, p95 ~2.77ms; handler-internal `total` commonly ~15-100us (HTTP overhead can be higher)

### A/B testing hybrid routing on/off

Use the same data directory and run two profiles to compare startup/build time and query latency.

**Profile A: hybrid routing ON (default)**

```bash
export NORNICDB_PERSIST_SEARCH_INDEXES=true
export NORNICDB_VECTOR_ANN_QUALITY=fast
export NORNICDB_KMEANS_MAX_ITERATIONS=5
export NORNICDB_VECTOR_ROUTING_MODE=hybrid
export NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM=0.7
export NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX=0.3
./bin/nornicdb serve
```

**Profile B: hybrid routing OFF (semantic-only routing)**

```bash
export NORNICDB_PERSIST_SEARCH_INDEXES=true
export NORNICDB_VECTOR_ANN_QUALITY=fast
export NORNICDB_KMEANS_MAX_ITERATIONS=5
export NORNICDB_VECTOR_ROUTING_MODE=semantic
./bin/nornicdb serve
```

Watch logs for:

- `Vector search strategy: IVF-HNSW` or `k-means` routing mode
- k-means completion and `iterations=...`
- first query latency and p95 latency under representative load

## Heimdall AI Assistant

Heimdall is the cognitive guardian and AI chat assistant. It supports **local** (GGUF BYOM), **ollama**, and **openai** providers—matching the embedding subsystem style.

| Variable                     | Default     | Description                                                       |
| ---------------------------- | ----------- | ----------------------------------------------------------------- |
| `NORNICDB_HEIMDALL_ENABLED`  | `false`     | Enable the AI assistant                                           |
| `NORNICDB_HEIMDALL_PROVIDER` | `local`     | Backend: `local`, `ollama`, or `openai`                           |
| `NORNICDB_HEIMDALL_API_URL`  | (see below) | API base URL for ollama/openai (ollama: `http://localhost:11434`) |
| `NORNICDB_HEIMDALL_API_KEY`  | (empty)     | API key for OpenAI (required when provider=openai)                |
| `NORNICDB_HEIMDALL_MODEL`    | (varies)    | Model name (GGUF file, Ollama model, or OpenAI model)             |

Streaming (SSE) is supported for chat completions when the client requests it; the OpenAI and Ollama providers stream tokens as they are generated.

See [Heimdall AI Assistant](../user-guides/heimdall-ai-assistant.md) for full configuration, provider examples, and YAML. To expose MCP memory tools (store, recall, link, etc.) in the Bifrost agentic loop, set `NORNICDB_HEIMDALL_MCP_ENABLE=true` and optionally `NORNICDB_HEIMDALL_MCP_TOOLS` (comma-separated allowlist); see [Enabling MCP tools in the agentic loop](../user-guides/heimdall-mcp-tools.md).

## Search Rerank (Stage-2 Reranking)

Stage-2 reranking improves vector/hybrid search by re-scoring top candidates with a reranker model. It is **independent of Heimdall** and supports **local** (GGUF, like embeddings) or **external** (ollama/openai/http) providers.

| Variable                          | Default     | Description                                                                                                                |
| --------------------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_SEARCH_RERANK_ENABLED`  | `false`     | Enable Stage-2 reranking for vector/hybrid search                                                                          |
| `NORNICDB_SEARCH_RERANK_PROVIDER` | `local`     | Backend: `local` (GGUF), `ollama`, `openai`, or `http`                                                                     |
| `NORNICDB_SEARCH_RERANK_MODEL`    | (see below) | For **local**: GGUF filename (e.g. `bge-reranker-v2-m3-Q4_K_M.gguf`). For **API**: model name (e.g. `rerank-english-v3.0`) |
| `NORNICDB_SEARCH_RERANK_API_URL`  | (see below) | Rerank API URL for non-local (default for `ollama`: `http://localhost:11434/rerank`)                                       |
| `NORNICDB_SEARCH_RERANK_API_KEY`  | (empty)     | API key for Cohere, OpenAI, etc.                                                                                           |

Local models live in `NORNICDB_MODELS_DIR` (default `./models`). Download the default reranker with `make download-bge-reranker`.

**Env var invocation:** Use `export NORNICDB_SEARCH_RERANK_ENABLED=true` (and other vars) before running `./nornicdb serve`, or put all vars on one logical line with backslashes—otherwise the shell may run each line as a separate command and only the last line’s vars are passed to the process.

**YAML:**

```yaml
search_rerank:
  enabled: true
  provider: local # local | ollama | openai | http
  model: bge-reranker-v2-m3-Q4_K_M.gguf
  api_url: ""
  api_key: ""
```

See [Cross-Encoder Reranking](../features/cross-encoder-reranking.md) for full configuration, local GGUF vs external API, and examples.

## Memory Decay Configuration

```yaml
decay:
  enabled: true
  recalculate_interval: 1h
  decay_rate: 0.1 # How quickly memories fade
```

## Auto-Link Configuration

```yaml
auto_links:
  enabled: true
  similarity_threshold: 0.82 # Threshold for automatic relationships
```

## Encryption Configuration

```yaml
encryption:
  enabled: false
  password: "your-secure-password" # Use environment variable in production
```

## Environment Variables

All configuration options can be set via environment variables using the pattern `NORNICDB_<SECTION>_<KEY>`:

```bash
# Server configuration
export NORNICDB_SERVER_BOLT_PORT=7687
export NORNICDB_SERVER_HTTP_PORT=7474

# Async writes
export NORNICDB_ASYNC_WRITES_ENABLED=true
export NORNICDB_ASYNC_FLUSH_INTERVAL=50ms

# Search
export NORNICDB_SEARCH_MIN_SIMILARITY=0.5
export NORNICDB_PERSIST_SEARCH_INDEXES=true   # Save/load BM25 and vector indexes (requires data_dir)

# Embeddings
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=local           # local | ollama | openai
export NORNICDB_EMBEDDING_MODEL=bge-m3
export NORNICDB_EMBEDDING_DIMENSIONS=1024
export NORNICDB_EMBEDDING_API_URL=http://localhost:11434
export NORNICDB_MODELS_DIR=./models                # used by provider=local
```

## Qdrant gRPC Endpoint (Qdrant SDK Compatibility)

NornicDB can expose a **Qdrant-compatible gRPC endpoint** so existing Qdrant SDKs can connect without modification.

User guide: `docs/user-guides/qdrant-grpc.md`

### Configuration (YAML)

```yaml
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":6334"
  qdrant_grpc_max_vector_dim: 4096
  qdrant_grpc_max_batch_points: 1000
  qdrant_grpc_max_top_k: 1000

  # Optional: override required permissions per RPC (advanced)
  qdrant_grpc_rbac:
    methods:
      # Key format: "<Service>/<Method>" (short service name)
      # Values: read, write, create, delete, admin, schema, user_manage
      "Points/Upsert": "write"
      "Points/Search": "read"
```

### Environment variables

| Variable                                | Default | Description                              |
| --------------------------------------- | ------: | ---------------------------------------- |
| `NORNICDB_QDRANT_GRPC_ENABLED`          | `false` | Enable the Qdrant-compatible gRPC server |
| `NORNICDB_QDRANT_GRPC_LISTEN_ADDR`      | `:6334` | gRPC listen address                      |
| `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM`   |  `4096` | Maximum vector dimension                 |
| `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS` |  `1000` | Max points per upsert                    |
| `NORNICDB_QDRANT_GRPC_MAX_TOP_K`        |  `1000` | Max search results per query             |

### Embedding ownership

- If `NORNICDB_EMBEDDING_ENABLED=true`, NornicDB owns embeddings; Qdrant vector mutation RPCs may be rejected to avoid conflicting sources of truth.
- If you want Qdrant clients to upsert/update/delete vectors directly, set `NORNICDB_EMBEDDING_ENABLED=false`.

## Configuration Validation

NornicDB validates configuration on startup and will:

1. **Reject invalid values** (e.g., negative cache sizes)
2. **Apply sensible defaults** for missing settings
3. **Log warnings** for potentially problematic combinations
4. **Fail fast** on critical configuration errors

## Performance Impact

### Async Write Settings

| Setting                | Impact                               | Recommendation            |
| ---------------------- | ------------------------------------ | ------------------------- |
| `enabled: true`        | 3-10x write throughput improvement   | Enable for most workloads |
| `flush_interval: 50ms` | Balance of consistency vs throughput | Default works well        |
| `cache_size: 50000`    | Memory usage vs bulk performance     | Adjust based on RAM       |

### Search Similarity

| Threshold | Use Case       | Impact                               |
| --------- | -------------- | ------------------------------------ |
| `0.7-1.0` | High precision | Fewer, more relevant results         |
| `0.5-0.7` | Balanced       | Good for most applications           |
| `0.3-0.5` | High recall    | More results, good for AI assistants |

## Troubleshooting

### Common Issues

**High memory usage:**

- Reduce `max_node_cache_size` and `max_edge_cache_size`
- Monitor during bulk operations
- Consider disabling async writes for memory-constrained environments

**Stale data reads:**

- Reduce `flush_interval` for more frequent writes
- Disable async writes if strong consistency is required
- Monitor WAL size and compaction

**Poor search results:**

- Adjust `min_similarity` based on your embedding model
- Consider model-specific thresholds (e.g., lower for Apple Intelligence)
- Test with your specific embedding provider

### Monitoring

Monitor these metrics to optimize configuration:

```bash
# Async write performance
curl http://localhost:7474/metrics | grep async

# Cache hit rates
curl http://localhost:7474/metrics | grep cache

# Search performance
curl http://localhost:7474/metrics | grep search
```

## Example Configurations

### Development Environment

```yaml
async_writes:
  enabled: true
  flush_interval: 10ms # Fast feedback
  max_node_cache_size: 1000
  max_edge_cache_size: 2000

search:
  min_similarity: 0.3 # More results for testing
```

### Production High-Throughput

```yaml
async_writes:
  enabled: true
  flush_interval: 100ms # Better throughput
  max_node_cache_size: 100000
  max_edge_cache_size: 200000

search:
  min_similarity: 0.7 # Higher precision
```

### Memory-Constrained

```yaml
async_writes:
  enabled: false # Disable to save memory
  # or small cache sizes:
  # max_node_cache_size: 500
  # max_edge_cache_size: 1000

search:
  min_similarity: 0.8 # Reduce result processing
```
