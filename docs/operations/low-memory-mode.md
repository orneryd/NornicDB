# Low-memory storage profile

Low-memory mode reduces Badger's configured arenas/caches. It does not set a total RAM cap, enable admission, select strict durability, or guarantee that a dataset fits in 512 MiB. Start with [memory accounting and limits](memory-management.md).

## Enable the profile

```bash
nornicdb serve --low-memory
# Or:
NORNICDB_LOW_MEMORY=true nornicdb serve
```

The `serve` shortcut selects `storage.mode: low`, sets the hot-node cache to 1,000 entries, and caps cached edge types at 10 before opening storage. For YAML or embedding, choose those controls explicitly:

```yaml
storage:
  mode: low
database:
  badger_node_cache_max_entries: 1000
  badger_edge_type_cache_max_types: 10
  async_writes_enabled: false
```

There is no current `--badger-high-performance` flag or `NORNICDB_BADGER_HIGH_PERFORMANCE` switch. Normal server storage uses the high-performance Badger profile unless storage mode is `low`; the default does not detect available host RAM automatically.

## What changes

| Badger option | Normal server profile | Low profile |
| --- | --- | --- |
| Memtable size | 128 MiB | 8 MiB |
| Number of memtables | 5 | 1 |
| Value-log file size | 256 MiB | 32 MiB |
| Block cache | 256 MiB | 8 MiB |
| Index cache | 128 MiB | 4 MiB |
| Compactors | Explicitly 4 | Badger default |

These capacities/file-sizing options are not an additive RSS estimate. Value-log file size does not imply resident RAM of that size. The profile also changes L0 thresholds and value placement; smaller buffers can increase compaction pressure and latency. Measure the actual workload rather than assuming fixed savings.

## Container deployment

Set the container limit separately:

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-amd64-cpu:latest
    mem_limit: 2g
    environment:
      NORNICDB_LOW_MEMORY: "true"
      NORNICDB_ASYNC_WRITES_ENABLED: "false"
    volumes:
      - nornicdb-data:/data
volumes:
  nornicdb-data:
```

The automatic Go target derives from a finite cgroup ceiling; native models, mappings, indexes, and query concurrency still need headroom. Docker Desktop VM capacity and the container limit are separate controls, with no universal default assumed here.

## Defer search loading

```yaml
memory:
  search_bm25_warming: lazy
  search_vector_warming: lazy
```

Lazy warming defers index load/build until first use, then retains the warmed working set. Concurrent first searches share the build. Database overrides use `db.nornic.search.bm25.warming` and `db.nornic.search.vector.warming`; see [search controls](configuration.md#per-database-search-index-control).

Use health endpoints for liveness, not searches that activate lazy warming. Disable an unneeded index through supported database settings instead of treating lazy mode as eviction.

## Embedded use

Set `cfg.Storage.Mode = "low"` and cache-entry limits on `nornicdb.DefaultConfig()` before `Open`. Direct storage users can choose `storage.BadgerOptions{LowMemory: true}`; they own WAL, recovery, and shutdown integration. See [Embedding NornicDB](../development/embedding.md).

For OOM incidents follow [memory diagnostics](memory-management.md#diagnose-before-raising-caps). Never delete WAL files to reduce startup memory.

## Implementation reference

[CLI shortcut](../../cmd/nornicdb/main.go), [server profile selection](../../pkg/nornicdb/db.go), and [Badger capacities](../../pkg/storage/badger.go).
