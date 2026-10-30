# Neo4j-Compatible Streaming Driver + Multi-Language ORM Plan

## Objective

Add true incremental result delivery to NornicDB while preserving the current Neo4j-compatible HTTP and Bolt behavior.

The end state is:

- Bolt `RUN` returns fields quickly and `PULL n` drains rows from a live stream instead of a fully materialized result.
- HTTP has a new streaming endpoint for non-Bolt clients.
- Existing `/db/{db}/tx/commit`, existing Bolt clients, and existing Cypher callers keep working unchanged.
- Go, Rust, and TypeScript users get first-party client/ORM packages on top of the same streaming contracts.

## Reality Check

These constraints come from the current code and from Neo4j/Bolt protocol behavior.

1. Bolt cannot stream the query text itself.
   The client sends a complete framed `RUN` message. NornicDB can start execution immediately after decoding that message, but it cannot start before the full `RUN` frame arrives.

2. Neo4j HTTP `/tx/commit` should remain materialized.
   It returns a single JSON transaction response. True HTTP streaming needs a new endpoint. Changing `/tx/commit` to stream would break compatibility.

3. Streaming all Cypher shapes immediately is not realistic.
   `ORDER BY`, global `DISTINCT`, aggregation, `collect`, some subqueries, writes with returned rows, and many APPLY shapes require buffering or commit-time guarantees. These must remain materialized until each shape gets a proven streaming operator.

4. Read-only scans are feasible now.
   Storage already has `storage.StreamingEngine`, `StreamNodes`, `StreamEdges`, `StreamNodeChunks`, `PrefixStreamingEngine`, and `LabelNodeIDLookupEngine`. The executor currently uses those only to collect bounded slices, not to expose rows incrementally.

5. Fabric already has a partial iterator layer.
   `pkg/fabric/result.go` defines `RowIterator`, bounded prefetch, concat, distinct, and row-view helpers. `pkg/fabric/executor.go` has `executeRows`, but top-level Fabric execution still returns `ResultStream{Rows [][]interface{}}`, and local/remote fragments still materialize through Cypher or remote transport APIs.

## Current Code Facts

### Materialized Result Boundary

- `pkg/cypher/types.go` defines `ExecuteResult` with `Rows [][]interface{}`.
- `pkg/cypher/executor.go` exposes `StorageExecutor.Execute(ctx, query, params) (*ExecuteResult, error)`.
- Most Cypher files append to `result.Rows`; streaming must be added as an alternate path, not by rewriting every executor first.

### Storage Streaming Exists

- `pkg/storage/types.go` defines `StreamingEngine` and fallback helpers.
- `pkg/storage/label_nodeid_lookup.go` defines label-ID streaming helpers.
- `pkg/cypher/match_multi.go` has `collectNodesWithStreaming`, but it still returns `[]*storage.Node`.

### Bolt Is Protocol-Streaming, Not Execution-Streaming Yet

- `pkg/bolt/server.go` stores `lastResult *QueryResult` and `resultIndex` on `Session`.
- `handleRun` executes the full query before sending `SUCCESS` with fields.
- `handlePull` slices `lastResult.Rows` and writes records, so fetch size only controls network batching, not executor memory.

### HTTP Is Fully Materialized

- `pkg/server/server_db.go` handles `/db/{db}/tx/commit` by building a `TransactionResponse` in memory.
- `appendStatementResult` converts all `ExecuteResult.Rows` into all `QueryResult.Data` rows before writing JSON.

### Fabric Has Useful Building Blocks But Still Materializes At Boundaries

- `pkg/fabric/result.go` has `RowIterator` and bounded prefetch.
- `pkg/fabric/local_executor.go` exposes `ExecuteRows`, but it wraps materialized `ExecuteWithRecord`.
- `pkg/fabric/remote_executor.go` exposes `ExecuteRows`, but it wraps materialized remote results.
- `pkg/cypher/executor_fabric.go` calls `fabricExecutor.Execute` and converts the returned `ResultStream` to `ExecuteResult`.

## Architecture Decisions

### 1. Add A Shared Row Stream Package

Do not put the base stream interface in `pkg/cypher`; `pkg/fabric` intentionally avoids importing `pkg/cypher` to prevent cycles. Add a small shared package:

Files:

- `pkg/rowstream/rowstream.go`
- `pkg/rowstream/materialized.go`
- `pkg/rowstream/channel.go`
- `pkg/rowstream/rowstream_test.go`

Target API:

```go
package rowstream

type Row struct {
    Values []interface{}
}

type Summary struct {
    Stats    map[string]int64
    Metadata map[string]interface{}
}

type RowStream interface {
    Columns() []string
    Next(ctx context.Context) (Row, error) // io.EOF on completion
    Summary() Summary                      // valid after EOF or Close
    Close() error
}
```

Adapters:

- `FromRows(columns []string, rows [][]interface{}, summary Summary) RowStream`
- `Drain(ctx context.Context, stream RowStream, maxRows int) (columns []string, rows [][]interface{}, summary Summary, err error)`
- `NewChannelStream(ctx context.Context, columns []string, buffer int, produce func(context.Context, EmitFunc) Summary) RowStream`

Reasoning:

- Cypher, Fabric, Bolt, Server, and RemoteEngine can all import this without cycles.
- Existing materialized paths can be adapted immediately.
- Channel streams give bounded backpressure when storage APIs are callback-based.

### 2. Add Streaming Beside Existing APIs

Keep existing public APIs stable first:

- Keep `StorageExecutor.Execute` unchanged.
- Add `StorageExecutor.ExecuteStream` in a new file.
- Keep `bolt.QueryExecutor.Execute` unchanged.
- Add optional `bolt.StreamingQueryExecutor`.
- Keep `fabric.ResultStream` unchanged initially.
- Add Fabric streaming methods and bridge them back to `ResultStream` where old callers still need it.

This gives a safe migration path and keeps tests from exploding in the first PR.

### 3. Stream Read-Only Shapes First

Initial streaming-eligible shapes:

- `MATCH (n) RETURN n`
- `MATCH (n:Label) RETURN n`
- `MATCH (n) WHERE <simple node predicate> RETURN <simple projection>`
- `MATCH ... RETURN ... SKIP ... LIMIT ...` when no ordering/aggregation is required
- `UNWIND $rows AS row RETURN row` and simple projections from parameter lists
- `CALL db.labels()` style catalog procedures only after their procedure implementation is iterator-backed

Materialized fallback remains required for:

- `ORDER BY` without a bounded top-k optimization
- global `DISTINCT`
- aggregation and `collect`
- variable-length traversal returning paths
- `OPTIONAL MATCH` shapes not proven row-by-row
- writes that return rows
- mixed read/write queries
- Fabric APPLY shapes not covered by the existing batched lookup iterator path

### 4. Bolt PULL Must Own Backpressure

The session should only read from the executor stream when a `PULL` arrives. If a producer goroutine is needed under the stream, it must use a bounded channel. `PULL n` must send at most `n` records.

To produce correct `has_more`, the Bolt session needs one-row lookahead:

- Drain up to `n` records from the stream.
- If exactly `n` rows were sent, read at most one extra row into `pendingRow`.
- If `pendingRow` exists, send `SUCCESS {has_more: true}`.
- If `io.EOF`, send completion metadata.

### 5. HTTP Streaming Is A New Endpoint

Add `/db/{db}/tx/stream` and keep `/db/{db}/tx/commit` unchanged.

Initial endpoint limitations:

- One statement per request.
- Read-only queries first.
- Materialized fallback allowed only when caller opts in with `allowMaterializedFallback: true`.
- Response format is newline-delimited JSON (`application/x-ndjson`) because it is simple for Go, Rust, and TypeScript clients.

Event model:

```json
{"type":"columns","columns":["n"]}
{"type":"row","row":[{"id":"1","labels":["Person"],"properties":{"name":"Alice"}}]}
{"type":"summary","stats":{},"metadata":{"db":"nornic"}}
{"type":"error","code":"Neo.ClientError.Statement.SyntaxError","message":"..."}
```

## Server Implementation Plan

### Phase 0: Baselines And Safety Gates

Files:

- `pkg/bolt/streaming_bench_test.go`
- `pkg/server/server_stream_bench_test.go` (new)
- `pkg/cypher/streaming_bench_test.go` (new or expanded)
- `docs/performance/streaming-baseline.md` (new)

Tasks:

- Add benchmarks for first-row latency, full-result latency, and peak allocated bytes.
- Benchmark at least:
  - `MATCH (n:Bench) RETURN n` with 10k, 100k rows
  - `MATCH (n:Bench) RETURN n LIMIT 100`
  - materialized fallback shape with `ORDER BY`
- Add cancellation tests for:
  - Bolt disconnect during stream
  - HTTP client cancellation during stream
  - stream `Close` before EOF

Acceptance:

- Baseline numbers committed in `docs/performance/streaming-baseline.md`.
- `go test ./pkg/rowstream ./pkg/cypher ./pkg/fabric ./pkg/bolt ./pkg/server` passes.

### Phase 1: Shared RowStream Core

Files:

- `pkg/rowstream/rowstream.go` (new)
- `pkg/rowstream/materialized.go` (new)
- `pkg/rowstream/channel.go` (new)
- `pkg/rowstream/rowstream_test.go` (new)
- `pkg/cypher/stream.go` (new)

Tasks:

- Implement `rowstream.RowStream`, materialized adapter, drain adapter, bounded channel stream.
- Add `func (e *StorageExecutor) ExecuteStream(ctx context.Context, query string, params map[string]interface{}) (rowstream.RowStream, error)`.
- First implementation may call `Execute` and wrap rows. This is a compatibility bridge, not the optimized path.
- Add `func ExecuteResultFromStream(ctx context.Context, s rowstream.RowStream) (*ExecuteResult, error)` for old callers and tests.

Acceptance:

- `ExecuteStream` returns the same columns/rows/metadata as `Execute` through the materialized adapter.
- `Close` is idempotent.
- `Next` after `Close` returns `io.EOF` or a stable terminal error.
- Context cancellation unblocks a channel-backed stream.

### Phase 2: Cypher Read Streaming

Files:

- `pkg/cypher/executor_stream.go` (new)
- `pkg/cypher/match_stream.go` (new)
- `pkg/cypher/match.go`
- `pkg/cypher/match_multi.go`
- `pkg/cypher/types.go`

Tasks:

- Add a stream classifier before materialized execution:
  - reject writes with existing mutation detection
  - reject aggregation, `ORDER BY`, global `DISTINCT`, subquery, and complex traversal shapes
  - accept simple read scan shapes only
- Factor the simple `MATCH` parsing already used by `tryFastPathSimpleMatchReturnLimit` into reusable helpers.
- Implement node scan stream using:
  - `storage.LabelNodeIDLookupEngine` for label scans where possible
  - `storage.StreamingEngine.StreamNodes` for full scans
  - `storage.StreamNodesWithFallback` only when the engine lacks native streaming
- Apply simple `WHERE`, `SKIP`, and `LIMIT` in the stream producer.
- Keep `Execute` implemented as `ExecuteResultFromStream` only for shapes handled by `ExecuteStream`; otherwise leave the current materialized code path.

Acceptance:

- `MATCH (n:Bench) RETURN n LIMIT 1` produces the first row without collecting all `Bench` nodes.
- `MATCH (n:Bench) RETURN n` streams with bounded memory on Badger and Async storage.
- Materialized and streaming results are byte-for-byte equivalent after Neo4j conversion for supported shapes.

### Phase 3: Fabric Streaming Boundary

Files:

- `pkg/fabric/result.go`
- `pkg/fabric/executor.go`
- `pkg/fabric/local_executor.go`
- `pkg/fabric/remote_executor.go`
- `pkg/cypher/executor_fabric.go`

Tasks:

- Keep existing `RowIterator` but add bridges to `rowstream.RowStream`.
- Add `FabricExecutor.ExecuteStream(ctx, tx, fragment, params, authToken) (rowstream.RowStream, error)`.
- Promote current unexported `executeRows` behavior to the stream path instead of immediately materializing at the top.
- Add optional local interface:

```go
type StreamingCypherExecutor interface {
    ExecuteQueryStream(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) (rowstream.RowStream, error)
}
```

- `LocalFragmentExecutor.ExecuteWithRecordRows` should use `StreamingCypherExecutor` when available, then fall back to materialized rows.
- Add remote streaming support in `RemoteFragmentExecutor.ExecuteRows` only after `RemoteEngine.QueryCypherStream` exists.
- Leave APPLY operators materialized except the existing batched lookup iterator path, then convert that path to emit a stream instead of accumulating into `ResultStream.Rows`.

Acceptance:

- Top-level `USE composite.alias MATCH ... RETURN ...` can stream when every leaf query is streamable.
- Fabric still enforces many-read/one-write transaction rules from `pkg/fabric/transaction.go`.
- Unsupported APPLY and UNION shapes explicitly expose `metadata.streamingFallback = "materialized"`.

### Phase 4: Remote Engine Streaming

Files:

- `pkg/storage/remote_engine.go`
- `pkg/storage/remote_engine_test.go`

Tasks:

- Add `QueryCypherStream(ctx, statement, params) (rowstream.RowStream, error)` to `RemoteEngine`.
- Bolt transport: use the Neo4j Go driver's `ResultWithContext.Next(ctx)` as the row source instead of collecting rows.
- HTTP transport: use `/db/{db}/tx/stream` when the remote server supports it.
- Fallback to `QueryCypher` materialization when the remote does not support streaming.

Acceptance:

- Remote Fabric constituents can participate in streaming for read-only leaf fragments.
- Closing the returned stream closes the underlying Neo4j session/transaction result resources.

### Phase 5: Bolt Incremental Execution Streaming

Files:

- `pkg/bolt/server.go`
- `pkg/bolt/streaming_bench_test.go`
- `pkg/bolt/server_extra_test.go`
- `pkg/bolt/integration_test.go`

Tasks:

- Add optional interface:

```go
type StreamingQueryExecutor interface {
    ExecuteStream(ctx context.Context, query string, params map[string]any) (rowstream.RowStream, error)
}
```

- Session state changes:
  - keep `lastResult *QueryResult` for fallback
  - add `lastStream rowstream.RowStream`
  - add `pendingRow *rowstream.Row`
  - add stream summary metadata captured at EOF
- `handleRun` should prefer `StreamingQueryExecutor` when the executor implements it.
- `handleRun` must send fields from `stream.Columns()` without draining the stream.
- `handlePull` should drain `lastStream` according to `n` and use one-row lookahead for `has_more`.
- `handleDiscard` should call `lastStream.Close()` and then emit completion metadata.
- Preserve existing deferred flush behavior for write queries. Initial streaming path should reject writes so current write semantics remain unchanged.
- Preserve the existing Bolt reader-loop cancellation model: normal pipelined `PULL` must not cancel the active stream; only `RESET`, `GOODBYE`, read error/EOF, timeout, or explicit stream close should cancel it.

Acceptance:

- Neo4j Go and TypeScript drivers observe incremental row delivery with fetch size.
- `PULL {n: 1}` does not cause full result materialization.
- Disconnect, `RESET`, and `GOODBYE` cancel the active stream.
- Existing Bolt materialized tests still pass.

### Phase 6: HTTP Streaming Endpoint

Files:

- `pkg/server/server_db.go`
- `pkg/server/server_db_stream.go` (new)
- `pkg/server/server_router.go`
- `pkg/server/server_stream_test.go` (new)

Routing detail:

- `/db/{db}/tx/stream` currently routes as `txId == "stream"` because `handleTransactionEndpoint` treats one path segment after `tx` as a transaction ID.
- Add the `stream` case before the generic `len(remaining) == 1` transaction-ID case.

Request:

```json
{
  "statement": "MATCH (n:Person) RETURN n",
  "parameters": {},
  "format": "ndjson",
  "allowMaterializedFallback": false
}
```

Tasks:

- Reuse auth, RBAC, database resolution, query normalization, and mutation checks from `handleImplicitTransaction`.
- Write `columns` before rows.
- Flush after each row or after a small configurable batch.
- Write terminal `summary` on success and terminal `error` on failure after streaming has started.
- Respect `r.Context()` cancellation and close the stream.

Acceptance:

- `curl -N` receives row events progressively.
- Existing `/db/{db}/tx/commit` tests are unchanged.
- HTTP clients can cancel without leaving executor goroutines running.

## Client And ORM Architecture

The server should remain Neo4j-compatible at the wire level. The first-party clients should be thin, language-native layers over Bolt and HTTP streaming, not separate database engines.

### Shared Client Concepts

All language clients expose the same conceptual layers:

1. Driver layer:
   - `Driver`, `Session`, `Transaction`, `Result`, `Record`, `Summary`
   - compatible with common Neo4j driver workflows where each language has an established Neo4j API

2. Streaming layer:
   - `RunStream` or equivalent returns an async/lazy row stream
   - Bolt is default when available
   - HTTP NDJSON stream is fallback and works through proxies/firewalls

3. ORM mapper layer:
   - maps `Record` values to typed structs/classes
   - does not hide Cypher or invent a new query language in v1
   - supports explicit query methods, parameters, and result mapping

4. Schema helper layer:
   - optional helpers for constraints and indexes
   - no automatic destructive migrations in v1

### Go Package

Files:

- `pkg/client/neo4jcompat/driver.go` (new)
- `pkg/client/neo4jcompat/session.go` (new)
- `pkg/client/neo4jcompat/result.go` (new)
- `pkg/client/orm/mapper.go` (new)
- `pkg/client/orm/query.go` (new)
- `pkg/client/neo4jcompat/*_test.go` (new)

Dependencies:

- Reuse `github.com/neo4j/neo4j-go-driver/v5`, already present in `go.mod`.
- Do not implement a second Bolt client unless the official driver cannot express a Nornic-specific feature.

API shape:

```go
driver, err := neo4jcompat.NewDriver(ctx, neo4jcompat.Config{
    URI:      "bolt://localhost:7687",
    Username: "neo4j",
    Password: "password",
    Database: "nornic",
})

session := driver.NewSession(ctx, neo4jcompat.SessionConfig{Database: "nornic"})
result, err := session.Run(ctx, "MATCH (p:Person) RETURN p.name AS name", nil)
for result.Next(ctx) {
    name, _ := result.Record().Get("name")
    _ = name
}
summary, err := result.Consume(ctx)
```

ORM mapper shape:

```go
type Person struct {
    ID   string `nornic:"id"`
    Name string `nornic:"name"`
}

people, err := orm.QueryAll[Person](ctx, session,
    "MATCH (p:Person) RETURN elementId(p) AS id, p.name AS name",
    nil,
)
```

Go acceptance:

- Works with current materialized Bolt path.
- Streams automatically once Bolt server streaming lands.
- Tests include first-row timing against a local NornicDB Bolt server.

### TypeScript / npm Package

Files:

- `clients/typescript/package.json` (new)
- `clients/typescript/src/driver.ts` (new)
- `clients/typescript/src/httpStream.ts` (new)
- `clients/typescript/src/orm.ts` (new)
- `clients/typescript/test/*.test.ts` (new)

Package name options:

- Preferred: `@nornicdb/client`
- Fallback if scope unavailable: `nornicdb`

Dependencies:

- Use `neo4j-driver` for Bolt. The UI already depends on `neo4j-driver`.
- Use native `fetch` and `ReadableStream` for HTTP NDJSON streaming.

API shape:

```ts
import { createDriver, queryAll } from "@nornicdb/client";

const driver = createDriver({
  uri: "bolt://localhost:7687",
  auth: { username: "neo4j", password: "password" },
  database: "nornic",
});

for await (const record of driver
  .session()
  .runStream("MATCH (p:Person) RETURN p.name AS name")) {
  console.log(record.get("name"));
}

type Person = { id: string; name: string };
const people = await queryAll<Person>(driver, {
  cypher: "MATCH (p:Person) RETURN elementId(p) AS id, p.name AS name",
});
```

TypeScript acceptance:

- ESM package with generated `.d.ts` types.
- Works in Node 20+.
- Optional browser HTTP-stream mode for the UI; Bolt remains Node/server-side.

### Rust Crate

Files:

- `clients/rust/Cargo.toml` (new)
- `clients/rust/src/lib.rs` (new)
- `clients/rust/src/http_stream.rs` (new)
- `clients/rust/src/orm.rs` (new)
- `clients/rust/tests/*.rs` (new)

Crate name options:

- Preferred: `nornicdb`
- If unavailable: `nornicdb-client`

Dependencies:

- HTTP streaming first with `reqwest`, `serde`, and `futures`.
- Optional `bolt` feature can use `neo4rs` or another maintained Bolt client, but Rust should not block server streaming work.

API shape:

```rust
use futures::TryStreamExt;
use nornicdb::{Client, Query};

let client = Client::http("http://localhost:7474")
    .database("nornic")
    .basic_auth("neo4j", "password")
    .build()?;

let mut rows = client.stream(Query::new("MATCH (p:Person) RETURN p.name AS name")).await?;
while let Some(record) = rows.try_next().await? {
    let name: String = record.get("name")?;
}
```

ORM mapper shape:

```rust
#[derive(serde::Deserialize)]
struct Person {
    id: String,
    name: String,
}

let people: Vec<Person> = client
    .query_as("MATCH (p:Person) RETURN elementId(p) AS id, p.name AS name")
    .collect()
    .await?;
```

Rust acceptance:

- Async stream implements `futures::Stream<Item = Result<Record, Error>>`.
- HTTP streaming works before any Rust Bolt dependency is selected.
- Optional Bolt feature is separately gated in CI.

## Compatibility Contract

### Bolt

- Existing Neo4j drivers must continue to connect.
- Existing materialized query behavior must remain valid.
- Fetch size must affect memory once `StreamingQueryExecutor` is active.
- Errors after partial row delivery must surface through `Result.Err()` / `Consume()` semantics.

### HTTP

- `/db/{db}/tx/commit` remains Neo4j HTTP-compatible and materialized.
- `/db/{db}/tx/stream` is NornicDB-specific and explicitly documented.
- HTTP stream errors are terminal events once response headers are sent.

### Transactions

- Read-only autocommit streams can emit rows immediately.
- Explicit read transactions can stream until commit/rollback/close.
- Writes initially remain materialized.
- Streamed writes are out of scope until commit visibility and failure semantics are proven.

### Fabric

- Preserve many-read/one-write enforcement.
- Preserve strict `USE` and composite scoping.
- Remote streaming is best-effort; fallback must be visible through metadata.

## Testing Strategy

### Unit Tests

- `pkg/rowstream`: EOF, cancellation, close idempotence, drain limits, channel backpressure.
- `pkg/cypher`: classifier accepts only proven streamable shapes.
- `pkg/fabric`: iterator-to-stream adapters, close propagation, materialized fallback metadata.
- `pkg/bolt`: lookahead behavior, `PULL n`, `DISCARD`, EOF metadata.
- `pkg/server`: NDJSON framing and cancellation.

### Integration Tests

- Large node scan over Bolt with small fetch size.
- Large node scan over HTTP stream with client cancellation after N rows.
- Composite query with local-only streamable leaves.
- Remote Fabric query using Bolt stream transport.
- Explicit transaction rollback while a read stream is open.

### Compatibility Tests

- Neo4j Go driver: `Result.Next`, `Record`, `Err`, `Consume`.
- TypeScript `neo4j-driver`: async iteration or `subscribe` path, depending on driver support.
- Existing HTTP `/tx/commit` tests unchanged.

### Performance Tests

Targets for streaming-eligible read scans:

- First-row latency: at least 50% lower than materialized baseline.
- Peak memory: at least 40% lower on 100k-row scans.
- Full-result throughput: no regression greater than 10% compared with materialized delivery.

## Rollout Order

1. `pkg/rowstream` plus materialized adapter.
2. `StorageExecutor.ExecuteStream` bridge with no behavior change.
3. Simple Cypher read stream for `MATCH ... RETURN` scans.
4. Bolt `StreamingQueryExecutor` and `PULL`-driven stream drain.
5. HTTP `/tx/stream` endpoint.
6. Fabric top-level streaming for FragmentExec and streamable Union.
7. RemoteEngine streaming.
8. Go client package.
9. TypeScript npm package.
10. Rust crate.
11. More Cypher operators: bounded top-k ORDER BY, UNWIND streaming, procedure streaming, selected APPLY streaming.

## Definition Of Done

- Existing `Execute`, Bolt, and HTTP materialized behavior is unchanged for unsupported shapes.
- Supported read-only query classes do not require full-result buffering in the Bolt path.
- HTTP streaming delivers valid NDJSON events and handles cancellation cleanly.
- Fabric can stream at least simple local composite reads without top-level materialization.
- Go, TypeScript, and Rust packages expose a consistent record-stream and typed-mapping story.
- Docs clearly state which query shapes stream and which intentionally fall back.
