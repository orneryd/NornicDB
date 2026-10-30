# Write-Behind Adaptive Buffer

The write-behind commit buffer acknowledges committed statements in memory and
replays them into Badger with a single background flusher. Its rotation delay
and buffer size **auto-scale to the measured drain latency and throughput**, so
the flush cadence follows real disk and workload conditions instead of a static
guess.

## Durable by default

Durable writes are the default. Statements commit synchronously to Badger and
the WAL, exactly as before this feature existed.

```yaml
database:
  async_writes_enabled: false   # default: durable synchronous commits
```

Enabling the buffer is an explicit throughput-for-durability trade. Each
acknowledged commit is written to the WAL before the ACK and replayed on
restart, so it survives a process kill; a power loss or OS crash can lose up
to one flush interval of acknowledged-but-unflushed commits that the OS had not
yet written to disk. Read queries are unaffected — buffered writes stay visible
through an in-memory overlay, and `Close` / explicit `BEGIN` drain the buffer
first. Validation,
constraints, WAL markers and commit receipts all still run synchronously before
an ACK.

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

Because draining a generation is deterministic (size ÷ rate = time), the
backlog stays at one interval of work: bounded memory, bounded loss window, and
no generation is so small that per-flush fixed costs (Badger commit, count
locks) dominate.

The 1s ceiling on the delay follows the industry flush cadence: Redis AOF
`appendfsync everysec`, MySQL InnoDB `innodb_flush_log_at_timeout=1s` and
MongoDB's 100ms journal group-commit all flush around the 100ms–1s band. The
default maximum generation size is **100,000 ops** (~50–200MB of unflushed
memory, in the same band as InnoDB's 16MB redo-log buffer and RocksDB's 64MB
write buffer), so oversized hardware can not balloon the buffer.

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
