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
- **Go GC target:** `NORNICDB_GO_MEMORY_LIMIT_BYTES` accepts an integer number of bytes and sets Go's soft runtime
  memory target. The deprecated `NORNICDB_MEMORY_LIMIT` retains this same
  meaning; neither setting turns on transaction or application admission.
- **Dynamic settings** apply to the statements that start after the change,
  with no restart. Set a database's own values through the per-database
  config API (`PUT /admin/databases/<db>/config` with
  `{"overrides": {"db.memory.transaction.max": "1g"}}`). A database's own
  `db.memory.transaction.max` replaces the process default for that
  database.
- **Running statements:** each statement snapshots its transaction and pool
  ceilings at start. A later reload does not revoke or relax those ceilings.
  Pool usage counters remain shared across statement generations, including
  retained staged writes, so new statements enforce their new ceiling against
  all existing charges. An explicit transaction's next statement takes a new
  snapshot without discarding its retained charge.
- **Visibility:** `SHOW SETTINGS` and `CALL dbms.listConfig()` list all
  three settings with their current values in bytes. While a limit is set,
  `SHOW TRANSACTIONS YIELD estimatedUsedHeapMemory` shows each running
  statement's current estimate.

### Example: an 8 GiB container

```yaml
memory:
  go_runtime_limit_bytes: "6442450944" # Go memory target, bytes
  application_soft_max: "6g"    # opt-in sampled admission threshold
  transaction_total_max: "4g"    # all transactions together
  transaction_max: "1g"          # any single transaction
```

or, as environment variables:

```bash
NORNICDB_GO_MEMORY_LIMIT_BYTES=6442450944
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

When write-behind buffering is enabled, each accepted implicit commit reserves
an application forecast before its replayable WAL record is written. The
reservation stays with the buffered generation through a replay failure and
ends after successful retirement. An over-limit commit is rolled back before
WAL publication. This forecast does not replace the physical cgroup sample;
the bytes already allocated while staging the write may briefly exceed it.
Startup WAL recovery applies batches targeting 8 MiB of estimated retained
payload instead of retaining the whole pending WAL. It may also retain the
record currently being decoded; a single oversized commit is applied alone.
This batching target does not bound decoder expansion or total process memory.

With no limit configured, nothing is tracked and there is no overhead.

## Application pressure and recovery

When application admission is enabled, one shared policy owns pre-decode wire
reservations. Bolt HELLO, BEGIN, RUN, PULL and DISCARD reserve before PackStream
decoding. Both Bolt readers reserve the accumulated message size as chunks
arrive, before growing their buffers, and recheck before dispatch. The bound
comes from currently available policy capacity, using the existing four-times
wire forecast and 64 KiB minimum, rather than a new fixed Bolt size cap. An
incomplete message that exceeds capacity receives
`Neo.TransientError.General.MemoryPoolOutOfMemoryError` and its connection closes;
the unread remainder is never decoded. Reservations follow queued messages and
are released after setup/handling, on read failure, or when the connection ends.
Valid bodyless RESET, ROLLBACK, COMMIT and GOODBYE can still release accepted work.

HTTP metadata admission encloses the complete application middleware chain,
before tracing header extraction, security query decoding, localization, proxy or authentication parsing.
The security checks share one parsed query map. HTTP request bodies use that
same owner before routing to JSON decoders,
including transaction, authentication, search, configuration, MCP, GraphQL and
Heimdall handlers. Enabled admission also applies the existing HTTP request-size
setting to their bodies. Request-line/query and header bytes are charged in
addition to the body; the body limit never clamps those known sizes. Body reads
grow the same reservation before returning bytes to a decoder. The reservation
lasts until the handler actually ends,
including workers that outlive an HTTP timeout. Bodyless health, rollback and
bare explicit commit requests remain available only when their raw query is
empty (the exemption cap is **0 bytes**), Content-Length is zero and there is no
Transfer-Encoding. Classification inspects only method, path and those framing
fields; it does not parse query parameters or headers. Adding any query or body
removes the exemption and requires normal admission. The existing security and
authentication checks still run for exempt requests. Timeout workers retain the
shared owner before they are started; returning a timeout response cannot release
credit while a worker still decodes. Bolt-over-WebSocket also reserves its HTTP
upgrade metadata as it arrives, before header/cookie parsing and upgrade setup.
With application admission
disabled, ordinary request admission is unchanged: in particular Bolt has no
new default aggregate message cap, so existing large parameter maps and bulk
UNWIND payloads remain accepted by the framing layer. A general configurable
Bolt cap is outside the 1.4.4 contract.

Only bounded native framing and protocol setup precedes application admission:
request headers and HTTP2-Settings decoding retain the implicit
[Go 1 MiB header limit](https://github.com/golang/go/blob/go1.26.9/src/net/http/server.go#L892-L906),
and HTTP/2 derives its header-list bound from it
([including 320 bytes of field overhead](https://github.com/golang/net/blob/v0.59.0/http2/config.go#L118-L126)).
Before the h2c adapter, one outer selector permits only bodyless HTTP/1.1
Upgrade requests: the upgrade-body cap is **0 bytes**, with no Transfer-Encoding.
Any nonzero or unknown Content-Length, or any transfer encoding, declines the h2c
upgrade and processes the request as ordinary HTTP/1.1 through the same admission
and request-body limits. This protocol-selection rule also applies with the
application policy disabled; it does not impose a new body-size cap on ordinary
HTTP/1.1 requests. The adapter cannot buffer a client payload before admission.
Bodyless h2c upgrades, prior-knowledge h2c and TLS h2 remain supported. Application
metadata/body credit belongs to each request or HTTP/2 stream, never to an entire
upgraded connection. See the
[listener-to-admission audit](https://github.com/orneryd/NornicDB/blob/release/v1.4.4/openspec/changes/dynamic-memory-soft-limits/transport-admission-audit.md).

GraphQL GET variables in the query string also pass through request admission.
Standalone GraphQL handlers use the same body/query/header admission helper;
delegated handlers borrow the HTTP request's owner. Known byte counts are never
clamped to the HTTP body limit, including query strings larger than that limit.
Upgraded GraphQL WebSocket messages use gqlgen's supported transport adapter:
it reads framing without buffering the payload, reserves through the same
policy, and grows that lease before accumulating each part of the message.
The available policy capacity bounds the message before gqlgen decodes its
envelope, initialization payload or operation variables. The lease lasts through
synchronous decode/setup and ends before waiting for the next message, or on
error/disconnect. An enclosing handler scope closes the upgraded connection on
every transport return, including invalid initialization; cancellation unblocks
the socket read before the scope ends. Policy rejection closes with code `1013` and reason
`Neo.TransientError.General.MemoryPoolOutOfMemoryError`. The existing gqlgen
payload read limit still applies independently. Policy-disabled GraphQL uses
the unchanged default gqlgen transport. See the
[pre-decode consumer audit](https://github.com/orneryd/NornicDB/blob/release/v1.4.4/openspec/changes/dynamic-memory-soft-limits/predecode-consumer-audit.md).

`NORNICDB_MEMORY_APPLICATION_SOFT_MAX` (`memory.application_soft_max`) enables
physical cgroup admission separately from the transaction estimates. Its default
is `0` (disabled), as are both transaction limits. The automatic Go target of
85% of a finite cgroup ceiling does not enable any of these admission checks.
The policy samples every 250 ms; readings older than two seconds are unavailable.
It keeps the greater of 128 MiB or 10% of the application maximum as headroom
below both the configured maximum and the tightest measured ancestor ceiling.

Go may finish collecting a rejected query while retaining its idle heap pages.
Those pages still count against `memory.current`, even though the live heap is
small. After physical pressure rejects an admission, the shared sampler can run
one collection with an explicit return of unused heap pages to the OS. It then
reads the cgroup again. HTTP, Bolt and buffered storage commits all use that
policy; request handlers do not collect or substitute a live-heap estimate for
physical usage.

Collections never overlap within the policy and are spaced by
`memory.recovery_interval_ms` (`NORNICDB_MEMORY_RECOVERY_INTERVAL_MS`, default
5000 ms). After its first attempt, another requires an admitted lease release or
`memory.recovery_rearm_ms` (`NORNICDB_MEMORY_RECOVERY_REARM_MS`, default 30000 ms)
since the previous attempt started. Both are startup-only integer milliseconds
>= 1, with rearm >= interval; invalid values fail startup. The longer interval allows
recovery from allocations that never held a lease. Both paths require a fresh
physical-pressure reading; rejected client retries never collect or shorten the
cooldowns. Live objects, file
cache or other processes can keep admission closed after reclamation; only a
fresh physical sample with sufficient capacity admits new work. An unavailable
sample continues to reject. This is a soft limit, with cooperative accounting and
sampling delay, rather than a guarantee against an OS OOM kill.

No cgroup-derived transaction or application admission default is enabled. In the
8 GiB concurrency reproduction, a 5 GiB aggregate estimate limit let two copies
finish but did not prevent the three/four-copy OOM kills. A 2 GiB pool rejected
even one copy that succeeds with defaults. An application threshold alone also
failed to stop already admitted concurrent work. See the
[8 GiB investigation](https://github.com/orneryd/NornicDB/pull/1618#issuecomment-6320191226)
for the measured compatibility trade-off.

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
