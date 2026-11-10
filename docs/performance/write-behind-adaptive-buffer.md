# Write-Behind Adaptive Buffer

The write-behind commit buffer acknowledges committed statements in memory and
replays them into Badger with a single background flusher. Its rotation delay
and buffer size **auto-scale to the measured drain latency and throughput**, so
the flush cadence follows real disk and workload conditions instead of a static
guess.

## Durable by default

Write-behind is disabled by default. Persistent statements use the native WAL/Badger path; whether acknowledgement waits for synchronous persistence depends on transaction mode and sync configuration. See [durability boundaries](../operations/durability.md).

```yaml
database:
  async_writes_enabled: false
```

For buffered commits, the full recovery record is flushed to the OS before acknowledgement, allowing replay after a process kill when the kernel/file data survives. This is not per-commit file sync or a bound on power-loss exposure. An overlay makes accepted buffered state visible before background Badger application; snapshot/backup/explicit transaction boundaries drain the relevant accepted work.

## Enabling the auto-scaling buffer

Environment:

```bash
export NORNICDB_ASYNC_WRITES_ENABLED=true
export NORNICDB_ASYNC_FLUSH_INTERVAL=auto   # default; runtime adapts the delay
```

YAML:

```yaml
database:
  async_writes_enabled: true
  async_flush_interval: auto   # or e.g. 50ms as the initial delay
```

With `StrictDurability` enabled the buffer stays off even when
`async_writes_enabled` is true — durability always wins.

## How the auto-scaling works

After every successful flush the buffer measures two things:

1. **Drain latency** — how long the generation took to land in Badger. The
   rotation delay moves toward this value (EWMA), clamped to 5ms–1s.
2. **Drain throughput** — operations per second. The generation size threshold
   becomes `throughput × delay`, clamped to `[1,000 ops, max ops]`.

Adaptive sizing controls cadence and operation count. It does not guarantee one generation of backlog, a fixed loss window, or a byte cap: payload sizes, retained generations, and slow/failing drains matter. The default maximum is 100,000 operations; use [memory accounting](../operations/memory-management.md) to budget actual retained work.

### Manual caps and warnings

- `NORNICDB_ASYNC_MAX_OPS` (YAML `async_max_ops`) is the primary cap on the
  buffered generation size. The default (`auto` = 0) selects the researched
  100,000-op cap; a positive value overrides it for unusually fast or slow
  storage.
- `NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE` / `..._EDGE_...` are deprecated legacy
  knobs; when `async_max_ops` is unset and either is set, their sum is used as
  the cap.
- When a fixed initial interval is set (`async_flush_interval: 50ms`) it is a
  starting point, not a straitjacket — the runtime still adapts.
- If the size or timing configuration persistently mismatches the measured
  throughput, the engine logs a warning once per state change, e.g.:

  ```
  write-behind sizing: fixed max 50000 ops binds at ~120000 ops per flush interval (50ms); effective flush delay ~20.8ms — a smaller max increases flush frequency and per-flush overhead
  write-behind flush delay pinned at 1s: measured drain latency 45s exceeds the bound; the flusher cannot keep up with the write rate
  ```

## Measured throughput

`go test ./pkg/cypher -run '^$' -bench 'ZZAsyncStrip' -benchtime=2s -benchmem`
(M2 Max, buffered vs synchronous WAL baseline):

| case | sync | write-behind | speedup |
|---|---|---|---|
| create1 | 60.0µs | 20.9µs | 2.9× |
| create2 | 82.6µs | 28.7µs | 2.9× |
| create_rel | 133.8µs | 41.7µs | 3.2× |
| unwind100 | 1.66ms | 0.94ms | 1.8× (10.6µs/node) |

The benchmark runner (`scripts/benchmark_northwind_vs_neo4j.sh`) enables the
auto-scaling buffer by default (`NORNIC_ASYNC_WRITES=0` benchmarks the durable
baseline).
