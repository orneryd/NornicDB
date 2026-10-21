# Transaction Memory Limits

NornicDB can reject tracked transaction growth with Neo4j-compatible settings
and errors. These are cooperative soft limits, not a guarantee that the server
cannot exhaust its container: allocations outside the tracked paths, native
memory, storage and index work, and sampling delay can still consume headroom.

With no limits configured, admission and transaction tracking remain off.

## Settings

| Setting | Scope | Default | Dynamic | Set with |
|---|---|---|---|---|
| `db.memory.transaction.max` | One transaction of a database | `0` (unlimited) | yes | `NORNICDB_MEMORY_TRANSACTION_MAX`, YAML `memory.transaction_max`, or the database's config override |
| `db.memory.transaction.total.max` | All transactions of one database together | `0` (unlimited) | yes | The database's config override |
| `dbms.memory.transaction.total.max` | All transactions of the process together | `0` (unlimited) | no (restart) | `NORNICDB_MEMORY_TRANSACTION_TOTAL_MAX`, YAML `memory.transaction_total_max` |
| Application soft max | Sampled process/cgroup admission | disabled | no (restart) | `NORNICDB_MEMORY_APPLICATION_SOFT_MAX`, YAML `memory.application_soft_max` |

- **Units:** bytes. Use a `k`, `m`, `g` or `t` suffix for kibibytes,
  mebibytes, gibibytes or tebibytes: `512m`, `2g`. `0` means unlimited.
- **Go GC target:** `NORNICDB_GO_MEMORY_LIMIT_MIB` sets Go's soft runtime
  memory target. The deprecated `NORNICDB_MEMORY_LIMIT` retains this same
  meaning; neither setting turns on transaction or application admission.
- **Dynamic settings** apply to the statements that start after the change,
  with no restart. Set a database's own values through the per-database
  config API (`PUT /admin/databases/<db>/config` with
  `{"overrides": {"db.memory.transaction.max": "1g"}}`). A database's own
  `db.memory.transaction.max` replaces the process default for that
  database.
- **Visibility:** `SHOW SETTINGS` and `CALL dbms.listConfig()` list all
  three settings with their current values in bytes. While a limit is set,
  `SHOW TRANSACTIONS YIELD estimatedUsedHeapMemory` shows each running
  statement's current estimate.

### Example: an 8 GiB container

```yaml
memory:
  go_runtime_limit: "6144"       # Go memory target, MiB
  application_soft_max: "6g"    # opt-in sampled admission threshold
  transaction_total_max: "4g"    # all transactions together
  transaction_max: "1g"          # any single transaction
```

or, as environment variables:

```bash
NORNICDB_GO_MEMORY_LIMIT_MIB=6144
NORNICDB_MEMORY_APPLICATION_SOFT_MAX=6g
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

A statement's temporary working memory is released when it ends. Staged
writes in an explicit transaction remain charged until commit or rollback;
memory failure rolls them back immediately while preserving the failure for
the client. Materialized result rows are tracked during execution. HTTP
transaction requests retain their application forecast until response
assembly ends. Bolt read results retain an additional forecast through PULL,
DISCARD, RESET or disconnect, including out-of-order explicit-transaction
streams. Embedded results and completed autocommit write results do not yet
retain an application lease after execution. The application policy remains
a soft admission forecast, not a precise bound on returned row allocations.

With no limit configured, nothing is tracked and there is no overhead.

## Client-visible contract

### Error codes, messages and GQL status

| Limit exceeded | Neo4j status code | Message |
|---|---|---|
| `db.memory.transaction.max` | `Neo.ClientError.General.TransactionOutOfMemoryError` | `The allocation of an extra <A> would use more than the limit <L>. Currently using <U>. db.memory.transaction.max threshold reached` |
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
- **Retrying:** the per-transaction error is a `ClientError`; aggregate pool
  errors are `TransientError`s. Retry only when the client can safely repeat
  the transaction and capacity is expected to recover.
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
    "code": "Neo.ClientError.General.TransactionOutOfMemoryError",
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
