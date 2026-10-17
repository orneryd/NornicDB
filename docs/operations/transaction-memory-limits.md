# Transaction Memory Limits

NornicDB can limit how much memory transactions use, with Neo4j's settings
and errors. A query that would go over a limit fails with a transient error.
The server stays up, and other transactions keep running.

Without a limit, several heavy queries at once can push the process past its
container's memory limit, and the container is OOM-killed. With limits set,
the queries that go over fail cleanly, and the client can retry them later.

## Settings

| Setting | Scope | Default | Dynamic | Set with |
|---|---|---|---|---|
| `db.memory.transaction.max` | One transaction of a database | `0` (unlimited) | yes | `NORNICDB_MEMORY_TRANSACTION_MAX`, YAML `memory.transaction_max`, or the database's config override |
| `db.memory.transaction.total.max` | All transactions of one database together | `0` (unlimited) | yes | The database's config override |
| `dbms.memory.transaction.total.max` | All transactions of the process together | `0` (unlimited) | yes, via `dbms.setConfigValue` | `NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX`, YAML `memory.transaction_total_max`, or `CALL dbms.setConfigValue(...)` while the server runs |

- **Units:** bytes. Use a `k`, `m`, `g` or `t` suffix for kibibytes,
  mebibytes, gibibytes or tebibytes: `512m`, `2g`. `0` means unlimited.
- **The global pool's default is unlimited.** This deviates from Neo4j,
  which defaults it to 70% of the heap. NornicDB's estimates still count
  some shared values once per row that references them (for example a
  `CALL` subquery's outer row once per inner row), so on real statements the
  counted use can be many times the memory actually held. With a 70%
  default, statements that used a fraction of the memory limit were
  rejected with `MemoryPoolOutOfMemoryError`. Until the accounting tracks
  real use, set `dbms.memory.transaction.total.max` explicitly to enable
  the pool. The Go memory limit (`memory.runtime_limit` /
  `NORNICDB_MEMORY_LIMIT`, `GOMEMLIMIT`, or the cgroup-derived default; see
  [The Go memory limit](#the-go-memory-limit)) no longer sets it.
- **Dynamic settings** apply to the statements that start after the change,
  with no restart. Set a database's own values through the per-database
  config API (`PUT /admin/databases/<db>/config` with
  `{"overrides": {"db.memory.transaction.max": "1g"}}`). A database's own
  `db.memory.transaction.max` replaces the process default for that
  database. Change the global pool with
  `CALL dbms.setConfigValue('dbms.memory.transaction.total.max', '2g')`;
  the change lasts until the process restarts. An empty value
  (`CALL dbms.setConfigValue('dbms.memory.transaction.total.max', '')`)
  resets it to the value the process was configured with
  (`NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX` or YAML, else unlimited).
  A running statement keeps the limits it started with, and one that
  started with no limit is not counted toward a newly set pool limit.
- **Visibility:** `SHOW SETTINGS` and `CALL dbms.listConfig()` list all
  three settings with their current values in bytes. While a limit is set,
  `SHOW TRANSACTIONS YIELD estimatedUsedHeapMemory` shows each running
  statement's current estimate.

### Example: an 8 GiB container

```yaml
memory:
  runtime_limit: "6144"          # Go memory limit, MB
  transaction_total_max: "4g"    # all transactions together
  transaction_max: "1g"          # any single transaction
```

or, as environment variables:

```bash
NORNICDB_MEMORY_LIMIT=6144
NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX=4g
NORNICDB_MEMORY_TRANSACTION_MAX=1g
```

Leave headroom: the limits cover query working memory (below), not caches,
indexes or the idle baseline.

## What is counted

Memory is an estimate in bytes, as Neo4j's memory tracker keeps one, not a
measurement of the Go heap. A statement counts what it keeps while it runs:

- materialised rows between clauses (MATCH, OPTIONAL MATCH, WITH, UNWIND);
- aggregation groups, and the lists `collect()` builds;
- DISTINCT keys, both `WITH DISTINCT` and `collect(DISTINCT …)`;
- ORDER BY sort keys;
- paths found by relationship and variable-length expansion;
- rows that `CALL { … }` subqueries return to the outer query.

A node or relationship in a row or list counts as a reference: the storage
layer and its caches own the entity. Its properties count when the query
copies them into values of its own (`n.text`, `properties(n)`, a map
projection).

A statement's memory is released when it ends, whether it succeeded, failed
or was rolled back. In an explicit transaction each statement's memory is
released when that statement ends. A result already handed to the client and
a transaction's uncommitted writes are not counted.

With no limit configured, nothing is tracked and there is no overhead.

## Client-visible contract

### Error codes, messages and GQL status

| Limit exceeded | Neo4j status code | Message |
|---|---|---|
| `db.memory.transaction.max` | `Neo.TransientError.General.TransactionMemoryLimit` | `The allocation of an extra <A> would use more than the limit <L>. Currently using <U>. db.memory.transaction.max threshold reached` |
| `db.memory.transaction.total.max` | `Neo.TransientError.General.MemoryPoolOutOfMemoryError` | `The allocation of an extra <A> would use more than the limit <L>. Currently using <U>. db.memory.transaction.total.max threshold reached` |
| `dbms.memory.transaction.total.max` | `Neo.TransientError.General.MemoryPoolOutOfMemoryError` | `The allocation of an extra <A> would use more than the limit <L>. Currently using <U>. dbms.memory.transaction.total.max threshold reached` |

- `<A>` is the allocation that went over, `<L>` the limit and `<U>` the bytes
  in use: for the per-transaction limit, the transaction's own use; for a
  pool, the pool's use by all transactions. Sizes are formatted as Neo4j
  formats them: `512 B`, `64.0 KiB`, `197.7 MiB`, `1.0 GiB`. Example from
  Neo4j 5.26: `The allocation of an extra 197.7 MiB would use more than the
  limit 1.0 GiB. Currently using 922.8 MiB. dbms.memory.transaction.total.max
  threshold reached`.
- **GQL status** (Bolt FAILURE metadata `gql_status`): `50N42`, for all three
  limits. This is what Neo4j 5.26 reports for these errors: its generic
  "general processing exception" status.
- **Retrying:** all three are `TransientError`s. Drivers' managed
  transactions (`ExecuteRead` / `ExecuteWrite` / `session.run` with retry)
  retry them automatically. To surface a clean failure instead, run the
  query outside a retrying function, or catch the code after the retries
  run out.
- **Matching the error:** match on the code. The message text is localized
  with the server's locale; the English text above is the default.

### Bolt

- **Auto-commit query:** `FAILURE {code, message, gql_status: "50N42"}`. The
  session stays usable after the driver's `RESET`: the next query on the
  same session runs normally.
- **Explicit transaction:** the failing statement fails the transaction.
  The transaction is rolled back, and every write it made is discarded. The
  driver's transaction object can no longer be used; commit fails. The
  session stays usable for new transactions.

### HTTP (`/db/<db>/tx/...`)

- **Status:** `200 OK`, as for any statement error.
- **Body:** the failing statement's result has its columns and no rows, and
  `errors` holds one entry, with no GQL field (Neo4j's HTTP API has none):

```json
{
  "results": [{"columns": ["n"], "data": []}],
  "errors": [{
    "code": "Neo.TransientError.General.TransactionMemoryLimit",
    "message": "The allocation of an extra 64.0 KiB would use more than the limit 1.0 MiB. Currently using 1023.9 KiB. db.memory.transaction.max threshold reached"
  }]
}
```

- **Open transaction** (`POST /db/<db>/tx/<id>`): the transaction is rolled
  back. Its writes are discarded, and later requests to it get `404` with
  `Neo.ClientError.Transaction.TransactionNotFound`.
- **Auto-commit** (`/tx/commit`): nothing is written. The next request runs
  normally.

### What other transactions see

A statement that goes over a limit only fails itself. Concurrent statements
keep running and keep their results. A pool limit makes the statement that
would overflow the pool fail, whichever it is; small statements that fit
keep succeeding. When the failed statement ends, its memory goes back to the
pool at once.

## The Go memory limit

The server's soft memory limit (Go's `GOMEMLIMIT`) makes the garbage
collector work harder as the process approaches it. It only protects the
process while it sits below the hard limit the kernel enforces: a soft limit
of 7 GiB in a 6 GiB container lets the heap grow until the process is
OOM-killed.

`nornicdb serve` therefore settles the limit against the container's cgroup
memory limit at startup (cgroup v2 `memory.max`, cgroup v1
`memory.limit_in_bytes`, the tightest of the process's cgroup and its
ancestors):

| `memory.runtime_limit` / `NORNICDB_MEMORY_LIMIT` / `--memory-limit` | In a container with a memory limit | Without one |
|---|---|---|
| unset or `auto` (default) | 85% of the container limit; a `GOMEMLIMIT` from the environment is used instead when it fits | `GOMEMLIMIT`, or unlimited |
| a value in MB at or below the container limit | that value | that value |
| a value in MB above the container limit | lowered to 85% of the container limit, with a startup warning | that value |
| `0` | unlimited | unlimited |

The startup banner shows the outcome, for example
`Memory limit:    5.10 GB (85% of the container limit 6.00 GB)`, or the
warning `Warning: memory limit 7.00 GB is above the container's memory limit
6.00 GB and could not prevent an OOM kill; using 5.10 GB`.

The 15% of headroom is for memory the Go limit doesn't cover but the
container is charged for: cgo allocations (llama.cpp embedding models, GPU
drivers), C thread stacks, kernel memory such as socket buffers, and the
brief overshoot of the soft limit while a GC cycle catches up. A server that
loads a large local model may need a lower explicit limit.
