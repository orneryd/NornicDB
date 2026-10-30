# Durable Cross-Protocol QID Continuation Plan

## Objective

Extend NornicDB's Neo4j-compatible Bolt result-stream contract into one durable,
protocol-neutral continuation mechanism for ranked search.

The end state is:

- Bolt preserves standard `RUN`, `PULL {qid, n}`, and `DISCARD {qid}` behavior.
- A connection-local Bolt `qid` can alias the same retained result population
  used by HTTP, native gRPC, and Cypher.
- Extension-aware Bolt clients can obtain an opaque durable qid and resume the
  result after reconnecting or through another supported protocol.
- HTTP, native gRPC, and `db.retrieve` expose the same start, pull, and discard
  semantics instead of separate page and release APIs.
- Search continuation incrementally extends the final canonical outer-RRF
  population produced by `search.SearchTextChunks`; the initial retrieval depth
  is not a total-result ceiling.
- No database transaction, storage lock, search-index lock, or global registry
  lock remains held while the client decides whether to request another page.
- Search calls that do not request continuation retain their current behavior
  and performance.

This plan supersedes the API shape proposed by PR #353. Its `ranked`,
`ranked_then_id`, `id`, complete filtered scan, grouping, exhaustion, and
mutation-epoch semantics are carried forward through the shared qid contract.
Its separate `db.retrieve.page`/`db.retrieve.release` procedures, cursor store,
global locking, and deep-copy-heavy population representation are not.

## Contract

All protocols implement the same logical operations:

```text
START(query, options, n) -> records, qid, has_more
PULL(qid, n)             -> records, qid, has_more
DISCARD(qid)             -> released
```

Properties of this contract:

- `n` is only the maximum number of records returned by one operation.
- In continuation mode, `limit` is the initial retrieval depth and expansion
  quantum, not the maximum number of results the caller may consume.
- Optional `max_results` lets a caller request an explicit total ceiling. When
  omitted, pulls continue until the searchable population is exhausted or a
  configured server safety limit rejects further expansion.
- A successful partial response returns a qid representing the next position.
- Repeating the same durable qid is idempotent and returns the same page.
- `DISCARD` releases the complete retained population, regardless of which
  position token from that population is supplied.
- A terminal response has `has_more: false` and no next qid; `completion`
  identifies whether it ended by exhaustion, candidate budget, or catalogue
  population exhaustion.
- Pulling does not extend the fixed expiry time.
- Later pulls never rerun chunking or embedding. They may deepen ANN and BM25
  retrieval and recompute fusion over the expanded candidate prefixes when the
  retained unseen-result buffer cannot satisfy `n`.

The optional `mode` selects the declared population:

- `ranked` (default) progressively enumerates canonical ranked search results.
- `ranked_then_id` emits a ranked prefix followed by every other eligible
  member in ascending logical-ID order. Tail rows are explicitly unscored.
- `id` bypasses retrieval and enumerates every eligible member in ascending
  logical-ID order.

`group_by` optionally collapses eligible child nodes into logical parents
before pagination while retaining the matching children in each parent's
`passages` collection. One parent consumes one page slot, but callers still
receive every matching passage for that parent. `ranked_limit` is an explicit boundary for the ranked phase;
it is distinct from `limit`, `n`, and `max_results`. When `ranked_limit` is
omitted, a ranked phase ends only when every retrieval branch reports
exhaustion. This preserves the rule that the initial `limit` is never a silent
lifetime ceiling. `ranked_then_id` cannot begin its catalogue tail before that
rank boundary is known.

"Infinite" continuation means that no initial client-selected batch or top-K
silently becomes the stream's lifetime ceiling. It does not mean unbounded
memory, CPU, token lifetime, or results beyond the finite searchable corpus.
Every pull remains subject to explicit per-operation and per-principal resource
budgets, and a client may refresh an expired qid from its last logical position
only through a new search.

## Identifier Model

Neo4j-compatible Bolt and durable continuation need related but distinct wire
identifiers.

### Connection-Local Bolt QID

Bolt `qid` remains a nonnegative `int64` allocated within an explicit
transaction. It is an alias in the session's statement map and obeys Neo4j's
existing rules:

- `RUN` in an explicit transaction returns a zero-based numeric `qid`.
- `PULL {qid, n}` and `DISCARD {qid, n}` address that statement.
- An omitted `qid` addresses the latest statement.
- Commit, rollback, reset, timeout, or connection teardown removes the alias.
- Ordinary clients observe no protocol behavior change.

A numeric Bolt qid is not globally unique, authenticated, or suitable for
reconnection. It must not become the durable identifier itself.

### Durable QID

The durable qid is an opaque, signed, base64url token. It addresses the same
underlying result stream but is safe to carry across requests, connections, and
supported protocols.

The token contains only:

- format version;
- owning server-instance or cursor-store identifier;
- random stream identifier;
- unsigned position;
- fixed expiry;
- HMAC-SHA256 authentication tag.

It contains no query, filters, embedding, node IDs, scores, properties, roles,
or credentials. The registry entry binds the stream to its canonical database
and authenticated owner and retains the compact normalized search state needed
to deepen retrieval without embedding the query again.

The name `qid` is used by HTTP, gRPC, and Cypher for the durable token. Bolt
continues to use its numeric `qid` field and exposes the opaque form as
`durable_qid` extension metadata when durable continuation was requested.

## Current Code Facts

### Bolt Streams Are Materialized

- `pkg/bolt/server.go` defines `resultStream` as a pointer to a complete
  `QueryResult` plus a row index.
- `pkg/bolt/session_messages.go` stores explicit-transaction streams in
  `Session.resultStreams map[int64]*resultStream`.
- `handlePull` slices or walks `QueryResult.Rows`; it does not ask the executor
  for additional rows.
- `clearExplicitTransactionState` removes all numeric qid aliases.

This already provides Neo4j-compatible wire streaming, but not lazy execution
or reconnect-safe continuation.

### Search Has One Canonical Text Path

- `pkg/search/chunked_search.go` owns text chunking, independent embedding and
  search, deduplication, and outer RRF.
- HTTP, native gRPC, Cypher retrieval, MCP, and Heimdall use this canonical
  search behavior.
- Continuation must be attached after the final fused `SearchResponse` is
  ordered.

### Cypher Retrieval Returns Rows

`pkg/cypher/call_rag.go` currently returns these columns:

```text
node, score, rrf_score, vector_rank, bm25_rank,
search_method, fallback_triggered
```

Calls without continuation options must retain this result shape. A durable
continuation call may add a stable qid column or metadata, but it must still
represent an empty page and an exhausted page without inventing fake nodes.

## Architecture

### 1. Shared Result-Stream Abstraction

Introduce a small protocol-neutral package rather than placing the base
contract in Bolt, Cypher, or HTTP.

Proposed package:

```text
pkg/resultstream/
  stream.go
  materialized.go
  registry.go
  token.go
  compact_search.go
```

Target API:

```go
type Position uint64

type Page struct {
    Rows     [][]any
    Position Position
    Next     Position
    HasMore bool
    Metadata map[string]any
}

type Stream interface {
    Columns() []string
    Pull(ctx context.Context, position Position, n int) (*Page, error)
    Close() error
}
```

Required implementations:

- `MaterializedStream` adapts existing `QueryResult.Rows` and preserves current
  behavior while Bolt is migrated.
- `SearchStream` owns an immutable compact ranked population and hydrates only
  the requested page.

This should align with the separate execution-streaming work in
`docs/plans/neo4j-compatible-streaming-driver-and-server-plan.md`. If that plan
lands first, reuse its shared row-stream interface and add positional durable
registry semantics rather than creating a competing abstraction.

### 2. One Registry Shared By Protocol Adapters

Use a process-level registry that can be reached by Bolt, HTTP, native gRPC,
and Cypher adapters. Entries are immutable after publication.

Conceptual entry:

```go
type entry struct {
  streamID       [16]byte
  ownerHash      [32]byte
  databaseID     uint64
  expiresUnix    int64
  columns        []string
  descriptors    []compactHit
  stringArena    []byte
  searchState    compactSearchState
  emitted        uint64
  retrievalDepth uint32
  exhausted      bool
  bytes          int64
}
```

`compactHit` stores offsets and lengths into one string arena plus only the
numeric ranking fields needed to reproduce the search row. Grouped hits also
store a compact range into the retained child-passage descriptors; they do not
retain full child nodes or property maps. `compactSearchState`
stores normalized filters/options, query chunks, and their embeddings so later
pulls do not call the chunker or embedding provider again. It does not retain:

- `storage.Node` values;
- property maps or label slices;
- complete `SearchResult` objects;
- storage transactions;
- index handles or locks.

Full nodes are batch-hydrated for the requested page through normal storage and
authorization paths. Storage engines that implement
`BatchGetNodesWithoutEmbeddings` should use that lighter contract first so
continuation page hydration does not reload stored vector payloads that are not
returned on the wire. Missing IDs are omitted, matching `BatchGetNodes`
semantics. Wrappers such as `NamespacedEngine` may report whether their current
inner engine supports the light batch read; callers fall back to `BatchGetNodes`
or per-node `GetNodeWithoutEmbeddings`/`GetNode` when the capability is absent.

### 3. Locking Model

Avoid PR #353's single mutex around cloning, expiry scans, page copying, and
token signing.

- Split the registry into 32 or 64 shards selected by stream ID.
- Hold a shard lock only for map lookup, insertion, or deletion.
- Build, validate, size, and pack a population before acquiring a shard lock.
- Verify token HMAC and fixed fields before acquiring a shard lock.
- After lookup, retain the entry reference and release the shard lock before
  slicing descriptors, hydrating nodes, authorizing results, encoding the next
  token, or deepening retrieval.
- Serialize expansion only per entry. Pulls that can be served from buffered
  descriptors remain concurrent; one pull expands while followers recheck the
  buffer after that expansion completes.
- Publish expanded descriptors with a short entry lock or immutable snapshot
  swap. Never hold that lock while ANN, BM25, fusion, reranking, or storage
  hydration runs.
- Use atomic counters or a short accounting lock for global and per-owner byte
  and session admission.
- Make release idempotent at the registry level where possible. A release racing
  with an already-admitted pull may finish that pull; future pulls fail.
- Never scan every entry on every pull.

Expired entries should be reclaimed by a bounded periodic shard sweep or timing
wheel. Shutdown must stop the reaper cleanly. Tests should use an injected clock
and explicit sweep hook rather than sleeps.

### 4. Progressive Search Ownership

The initial request executes the existing canonical path and retains the
normalized chunk vectors and options:

```text
chunk -> embed -> per-chunk search -> outer RRF -> buffer -> first pull
```

When a pull cannot be satisfied from buffered unseen descriptors, the stream
increases retrieval depth geometrically, reruns the search branches with the
retained vectors and normalized options, performs canonical outer RRF over the
larger prefixes, removes IDs already emitted or buffered, and appends newly
discovered results. Expansion stops when enough rows are buffered, all branches
report exhaustion, repeated deeper requests stop increasing an approximate
vector or hybrid ranked prefix, the optional `max_results` is reached, or a
server resource budget rejects the operation. A repeated non-growing approximate
prefix is reported as a candidate-budget boundary, not as eligible-population
exhaustion.

Repeated deepening is the first correctness-oriented implementation because it
does not retain index locks or mutable index iterators across requests. It must
be hidden behind a producer interface so BM25 keyset iteration and safe
vector-index frontier snapshots can replace repeated work after profiling.

The `maxChunkCandidateLimit` constant is an ordinary one-shot search safeguard,
not a valid lifetime ceiling for a continued stream. Candidate depth may grow to
the current searchable cardinality unless a caller supplies a lower
`MaxCandidateLimit`. Candidate-generator implementations must report whether a
returned prefix is exhausted; a short approximate response must not be
presented as proof that the corpus is exhausted. Repeated non-growth from an
approximate vector or hybrid producer during deeper continuation requests is
treated as an honest bounded candidate pool so continuation does not repeat
equivalent producer work indefinitely. Exact BM25 and opaque producers still
need proven exhaustion, an explicit candidate-budget signal, or an explicit
ceiling.

If all retrieval branches establish exhaustion and the requested first page
consumes every result, return it directly without registering a durable stream.

### 5. Complete Collection And Grouped Populations

Complete collection modes use a separate materialized catalogue producer behind
the same `resultstream.Stream` and registry. They do not alter qid encoding,
authentication, replay, expiry, or protocol adapters.

Population semantics are:

| Mode             | Membership                                                      | Stable order                            | Collection exhaustive |
| ---------------- | --------------------------------------------------------------- | --------------------------------------- | --------------------- |
| `ranked`         | progressively discovered ranked hits                            | append-only canonical ranked expansions | no                    |
| `ranked_then_id` | bounded/exhausted ranked phase plus every other eligible member | ranked phase, then unscored logical ID  | yes                   |
| `id`             | every eligible member; no ANN, BM25, embedding, or reranking    | logical ID                              | yes                   |

For `ranked_then_id`, a caller may set `ranked_limit` to request the original PR's
fixed ranked-prefix behavior explicitly. Without it, the service progressively
deepens retrieval until branch-native exhaustion before transitioning to the
catalogue tail. `max_results` remains a total stream ceiling and must never be
misinterpreted as ranked-phase completion.

The initial complete-mode build:

1. reserves a bounded build slot and snapshots the database mutation revision;
2. prepares the ranked phase when the mode requires one;
3. streams storage once with `storage.StreamNodesWithFallback`;
4. applies canonical type, property, visibility, temporal, decay, and
   result-authorization eligibility;
5. records only compact IDs, group keys, phase, ranking diagnostics, and the
   ordered child-passage descriptors needed by grouped results;
6. verifies the mutation revision is unchanged;
7. sorts and publishes one immutable population through the shared registry.

All scanned nodes count against `max_scanned_nodes`, including filter rejects.
Member, byte, build-time, and concurrent-build limits fail the request; they do
not truncate an allegedly complete population. Engines without a database-scoped
`storage.GraphMutationVersionProvider` must reject complete modes unless a future
engine-specific snapshot contract provides an equivalent guarantee. The
`AllNodes` fallback is functionally valid but may materialize the source graph;
production guidance must identify whether the selected engine truly streams.

Complete populations bind their starting storage revision and policy generation
to the stream. A revision change during construction prevents publication. A
change before any later pull invalidates the stream instead of returning mixed
membership or false `eligible_count` values. This stricter rule applies only to
complete/grouped populations; ordinary progressive ranked streams retain their
existing current-hydration behavior.

Grouping occurs after eligibility and ranked-phase selection but before paging:

- `group_by` names one flat property whose value must be a nonempty valid UTF-8
  string on every eligible node;
- filters and authorization apply to child nodes, not an implicit parent join;
- a ranked group uses its highest-scoring ranked child; equal scores choose the
  lowest child ID, and its `passages` collection contains every ranked matching
  child in score-descending, child-ID order;
- an unranked group uses its lowest eligible child ID, and its `passages`
  collection contains every metadata-eligible child in child-ID order;
- ranked groups sort by score descending, then group key and representative ID;
- catalogue groups sort by group key, then representative ID;
- counts and `n` refer to distinct groups, while each row returns the
  representative node ID, `group_key`, and nested `passages` descriptors.

The top-level representative controls parent ordering and preserves the best
ranked diagnostics. It is not the complete grouped payload. For example, a
video asset appears once in a page while `passages` contains all relevant
moments selected for that video. Catalogue passages remain explicitly
unscored. Passage descriptors count toward per-build and retained-byte limits;
they are never silently dropped to admit a group.

Missing, empty, non-string, or invalid grouping values fail the build instead of
being skipped. Because a later ranked discovery can replace a group's best
representative, grouped modes materialize the declared ranked phase and complete
eligible scan before returning the first page. This is the deliberate latency
tradeoff required for deterministic grouping and replay.

Complete/grouped pages add protocol-neutral metadata without changing the qid
operations: `mode`, per-row `phase`, optional `group_key`, optional nested
`passages`, `ranked_count`,
`eligible_count`, `ranked_pool_exhausted`, `collection_exhausted`, and
`completion`. `completion` is one of `more_results`,
`candidate_pool_exhausted`, `eligible_population_exhausted`, or
`max_results_reached`.
`candidate_pool_exhausted` means a configured candidate boundary stopped ranked
expansion without proving full ranked exhaustion. `max_results_reached` means
the caller's explicit result ceiling stopped the stream before full collection
exhaustion. A short ANN response never claims collection or ranked-pool
exhaustion by itself.

### 6. Consistency Contract

Continuation guarantees:

- already emitted logical result IDs never repeat;
- already emitted order never changes;
- retained scores and rank metadata for emitted rows remain unchanged;
- search method and fallback diagnostics.

Because deeper approximate or multi-list retrieval can discover a result whose
recomputed score would have placed it in an earlier page, the contract is
append-only progressive ranking, not a claim that every emitted page is a
prefix of one omniscient global ordering. New results are appended in their
order within the latest canonical fused expansion.

It does not freeze node properties or provide an MVCC snapshot. Each pull:

- verifies the current authenticated principal owns the stream;
- verifies current read access to the bound database;
- hydrates the current node state;
- applies result-level authorization before returning a node;
- skips nodes that were deleted or are no longer visible.

Ordinary node writes do not invalidate all cursors. This avoids adding mutation
hooks and write-path contention across storage wrappers. Invalidation is
reserved for service shutdown, explicit release, expiry, incompatible ranking
policy replacement, or an administrator reconfiguration that changes cursor
security or resource policy.

The documented guarantee is stable ranked membership and order, not stable
properties or transactional snapshot isolation.

## Protocol Adapters

### Bolt

Keep the Neo4j contract unchanged for ordinary clients. Change the internal
`resultStream` to hold a shared `resultstream.Stream` or materialized adapter.

When a search request explicitly enables durable continuation:

1. `RUN` creates or attaches to a registry entry.
2. The session assigns its normal numeric qid as a local alias.
3. `PULL {qid, n}` invokes the shared stream pull operation.
4. A partial `PULL` reports standard `has_more: true` and adds the opaque
   `durable_qid` to extension metadata.
5. `DISCARD {qid}` closes the local alias and releases the durable entry when
   the request selected durable-discard behavior.

The standard Bolt state machine does not allow a normal driver to issue a bare
`PULL` on a new connection without first establishing a result. Reconnection
therefore uses one of these supported paths:

- call `db.retrieve({qid: $qid, n: $n})` through a standard Neo4j driver; or
- use a future negotiated Bolt extension that attaches a durable qid to a new
  local numeric qid.

Do not change the type or scope of the standard Bolt `qid` field.

### HTTP Search

Extend the existing `/nornicdb/search` JSON contract additively.

Initial request:

```json
{
  "database": "nornic",
  "query": "sunset beach",
  "labels": ["Image"],
  "mode": "ranked_then_id",
  "group_by": "asset_id",
  "limit": 500,
  "ranked_limit": 5000,
  "n": 50
}
```

Pull request:

```json
{
  "qid": "opaque-signed-token",
  "n": 50
}
```

Discard request:

```json
{
  "qid": "opaque-signed-token",
  "discard": true
}
```

The response adds `qid`, `has_more`, `position`, `returned`, `discovered`,
`exhausted`, `expires_at`, `mode`, per-row `phase`, optional `group_key`,
optional per-row `passages`,
`ranked_count`, `eligible_count`, `ranked_pool_exhausted`,
`collection_exhausted`, and `completion`. `total` is omitted until terminal
completion for progressive `ranked` mode because the eventual searchable result
count is not known. `completion="candidate_pool_exhausted"` is terminal for the
stream but does not prove `ranked_pool_exhausted`. Complete modes know `total`
and `eligible_count` after initial materialization. `completion="max_results_reached"`
is terminal but leaves `collection_exhausted=false` when the requested result
ceiling is lower than the eligible population. When no continuation fields are
supplied, the current request and response behavior remains unchanged.

On pull and discard, the token selects its canonical database. If a request
also supplies `database`, it must resolve to the same database or fail closed.

### Native gRPC

Append fields without renumbering existing protobuf fields:

```proto
message SearchTextRequest {
  // Existing fields 1-5 remain unchanged.
  string qid = 6;
  int32 n = 7;
  bool discard = 8;
  optional int64 max_results = 9;
  string mode = 10;
  string group_by = 11;
  optional int64 ranked_limit = 12;
}

message SearchTextResponse {
  // Existing fields 1-5 remain unchanged.
  string qid = 6;
  bool has_more = 7;
  int64 position = 8;
  int32 returned = 9;
  optional int64 total = 10;
  google.protobuf.Timestamp expires_at = 11;
  bool released = 12;
  string mode = 13;
  int64 ranked_count = 14;
  optional int64 eligible_count = 15;
  bool ranked_pool_exhausted = 16;
  bool collection_exhausted = 17;
  // more_results, candidate_pool_exhausted, eligible_population_exhausted, or
  // max_results_reached.
  string completion = 18;
}
```

Each returned search result also gains additive `phase`, `group_key`, and
repeated `passages` fields. A passage carries its child ID and ranking
diagnostics; catalogue passages have `phase: "catalog"` and unscored values.

Before enabling durable qids, native gRPC must expose an authenticated
principal and canonical database through request context using the same server
authorization policy as HTTP and Bolt. Metadata supplied by the caller must not
be accepted as a trusted owner identity.

### Cypher `db.retrieve`

Extend the existing request map:

```cypher
CALL db.retrieve({query: $query, limit: 500, n: 50})
CALL db.retrieve({query: $query, mode: 'ranked_then_id',
                  group_by: 'asset_id', ranked_limit: 5000, n: 50})
CALL db.retrieve({mode: 'id', filters: $filters, n: 50})
CALL db.retrieve({qid: $qid, n: 50})
CALL db.retrieve({qid: $qid, discard: true})
```

Do not add `db.retrieve.page` or `db.retrieve.release`.

Calls without `n`, `qid`, or `discard` retain the existing columns and row
behavior. Continuation-enabled calls need a page envelope or procedure metadata
that can represent empty and exhausted pages. The implementation phase must
select one additive Cypher shape and add compatibility tests before changing
`call_rag.go`. Preferred shape:

```text
page = {
  results: [{node, score, rrf_score, vector_rank, bm25_rank,
             group_key, phase, passages: [{node, score, rrf_score,
                                            vector_rank, bm25_rank, phase}]}],
  search_method: ...,
  fallback_triggered: ...,
  qid: ...,
  has_more: ...,
  position: ...,
  returned: ...,
  discovered: ...,
  total: ..., // present only after exhaustion
  expires_at: ...
}
```

This avoids repeating page metadata on every result and preserves metadata for
an empty page. It is activated only by continuation options, so existing
`YIELD node, score, ...` calls remain unchanged.

## Security And Topology

- Authenticate every start, pull, and discard operation.
- Derive owner identity from trusted server context. Prefer immutable subject ID
  over username or bearer-token text.
- Canonicalize validated identities as subject, username, then email. Never
  parse an unvalidated bearer token or use a raw Authorization header as owner
  identity. A forwarded credential without a validated principal fails closed.
- Compute the retained owner/database thumbprint with HMAC-SHA-256 under the
  registry's random process secret. Retain only this keyed digest; never retain
  a password, bearer token, API key, or Authorization header in stream state.
- Reauthentication or token refresh may resume a stream only when it resolves
  to the same validated principal and current database authorization still
  permits the pull.
- Bind the entry to the canonical database, not a user-supplied alias.
- Recheck database read access and node visibility on every pull.
- Compare owner hashes in constant time.
- Return one generic invalid-qid response for malformed, forged, wrong-owner,
  and unknown tokens where distinguishing them would leak state.
- Log detailed internal reasons without logging complete tokens or query data.
- Rate-limit invalid token attempts independently from valid pulls.

Phase one is process-local and survives connection loss, not process loss. A
multi-instance deployment requires affinity to the instance encoded in the
token. A wrong instance returns a distinct retryable routing error without
revealing whether the stream exists.

A later shared registry may store the compact serialized population in a
distributed cache. It must preserve immutable entries and position-bearing
tokens rather than serialize Go object graphs or live iterators.

## Resource Policy

Initial defaults should be conservative and configurable:

- fixed TTL: 5 minutes;
- maximum page size `n`: 500;
- maximum buffered unseen descriptors per stream;
- maximum cumulative results may be configured by an administrator as a safety
  ceiling, but is independent of the initial `limit` and defaults to searchable
  corpus exhaustion where deployment capacity permits;
- maximum active streams per principal;
- maximum retained bytes per principal;
- maximum active streams globally;
- maximum retained bytes globally;
- maximum concurrent population builds;
- maximum bytes for one population.

Admission rejects the new stream when any limit is exceeded. It does not evict
an unrelated live stream. Exact retained bytes are calculated from descriptor
and arena capacities, with fixed entry/map overhead included. Expose gauges and
counters for active streams, retained bytes, admission failures, expiry,
release, pull outcomes, and wrong-instance tokens.

## Implementation Phases

### Phase 0: Contract Tests And Baselines

- [x] Add black-box contract tests for START, repeated PULL, replay, exhaustion,
      and DISCARD independent of any transport.
- [ ] Capture ordinary search and current Bolt PULL latency, allocations, and
      retained heap before adding the registry.
- [ ] Add benchmark fixtures that consume 10, 100, 1,000, 5,000, and more than
      5,000 ranked hits from one stream.
- [x] Verify a stream can return more results than its initial `limit` without
      duplicates or repeated embedding calls.

### Phase 1: Shared Stream And Compact Registry

- [x] Add the shared stream interface and materialized adapter.
- [ ] Add compact search descriptors, normalized resumable search state, and a
      contiguous string arena.
- [x] Add signed position-bearing durable qids.
- [x] Add sharded immutable registry entries and bounded admission accounting.
- [x] Add fixed expiry and bounded cleanup.
- [x] Add owner/database binding and error taxonomy.
- [ ] Prove with tests that no transaction or storage/search lock survives
      publication.

### Phase 2: Search Integration

- [x] Buffer the initial final `SearchTextChunks` outer-RRF population and
      progressively deepen it when unseen rows run low.
- [ ] Preserve explicit-vector single-search behavior.
- [x] Hydrate and authorize only the requested page.
- [x] Avoid registry insertion for one-page populations.
- [x] Ensure pulls never call the chunker or embedder.
- [ ] Add branch exhaustion reporting and continuation-specific retrieval depth
      beyond one-shot candidate caps.
- [x] Preserve emitted-prefix stability while deduplicating expanded results.

### Phase 2B: Complete And Grouped Populations

- [x] Add `mode`, `group_by`, and explicit `ranked_limit` to the normalized
      continuation request shared by every adapter.
- [x] Add a bounded build-admission path separate from ordinary stream
      admission.
- [x] Implement compact `ranked_then_id` and `id` catalogue builders behind the
      shared stream interface.
- [x] Apply canonical eligibility and trusted result authorization during the
      scan; never expose unauthorized IDs through results or counts.
- [x] Bind complete populations to database mutation and policy generations;
      reject unsupported engines and invalidate changed populations.
- [x] Implement deterministic representative selection and grouping before
      pagination.
- [x] Preserve every matching child passage under its grouped asset, with
      deterministic passage ordering and byte accounting.
- [x] Add ranked-pool and eligible-population exhaustion metadata consistently
      to HTTP, gRPC, Cypher, and extension-aware Bolt.
- [x] Prove scan-order-independent grouping and exact 20,000-member enumeration
      without duplicates.
- [x] Benchmark native streaming and `AllNodes` fallback separately; document
      that fallback memory is not bounded by descriptor admission alone.

### Phase 3: Bolt Adapter

- [ ] Wrap current materialized results in the shared stream interface.
- [ ] Map each numeric transaction-local qid to a stream handle.
- [x] Preserve latest-qid fallback and existing invalid-qid failures.
- [x] Preserve bounded `PULL` and `DISCARD` behavior for multiple active streams.
- [x] Emit `durable_qid` only when durable continuation was requested.
- [ ] Cover commit, rollback, reset, timeout, disconnect, and reconnect paths.
- [x] Confirm ordinary Bolt compatibility tests pass unchanged.

### Phase 4: HTTP And Native gRPC

- [x] Add `qid`, `n`, and `discard` to the existing HTTP search request.
- [x] Add continuation metadata to the existing HTTP response.
- [x] Append protobuf fields and regenerate checked-in Go bindings.
- [x] Add native gRPC principal/database context integration.
- [x] Map common continuation errors consistently to HTTP, native gRPC, and Bolt statuses.
- [x] Add cross-protocol tests that start in one protocol and pull or discard in
      another.

### Phase 5: Existing Cypher Procedure

- [x] Extend `db.retrieve` request parsing with `qid`, `n`, and `discard`.
- [x] Add the continuation-only page envelope without changing ordinary columns.
- [x] Derive owner and canonical database from trusted execution context.
- [ ] Verify standard Neo4j drivers can resume a durable qid after reconnect by
      calling the existing procedure.
- [x] Verify empty and exhausted pages retain continuation metadata.

### Phase 6: Operations And Cluster Readiness

- [x] Add bounded-label metrics and privacy-safe structured continuation lifecycle events.
- [x] Add operator configuration and protocol-specific continuation errors.
- [x] Add shutdown and reconfiguration behavior.
- [x] Document process-local durability and load-balancer affinity requirements.
- [ ] Design, but do not require, a shared-registry provider interface for a
      later cluster-durable implementation.

## Performance Validation

### Implemented Primitive Baseline And Tuning

Measured on Apple M3 Max (`darwin/arm64`, five runs, steady-state median):

| Operation                    |                     Before |                     After |                              Allocation change |
| ---------------------------- | -------------------------: | ------------------------: | ---------------------------------------------: |
| Signed token encode + verify |   710 ns/op (~1.41M ops/s) |  432 ns/op (~2.31M ops/s) |        1,472 B / 18 allocs to 224 B / 2 allocs |
| Buffered registry pull       | 1,150 ns/op (~0.87M ops/s) |  721 ns/op (~1.39M ops/s) |        2,168 B / 30 allocs to 400 B / 6 allocs |
| Buffered stream page         |  31.7 ns/op (~31.5M ops/s) | 31.7 ns/op (~31.5M ops/s) |                  unchanged at 120 B / 2 allocs |
| Complete ID build, 20k nodes |                    30.0 ms |                   20.2 ms | 30.0 MB / 305k allocs to 26.9 MB / 245k allocs |

The tuned path keeps HMAC-SHA-256 and constant-time comparison. It uses
fixed-size token buffers and fixed-size HMAC computation for the known token
layout, plus per-registry pools for keyed scope hashers and cleared scope-input
buffers. Ungrouped complete builds collect compact descriptors in fixed chunks
before one exact flattening allocation. Registry locks still cover lookup only;
stream paging and expansion execute outside shard locks.

Reproduce with:

```bash
go test ./pkg/resultstream -run '^$' \
  -bench 'Benchmark(TokenRoundTrip|ProgressiveBufferedPull|RegistryBufferedPull)$' \
  -benchmem -count=5
```

Required microbenchmarks:

- population packing for 10, 100, 1,000, 5,000, and 20,000 hits;
- first-page publication with page sizes 10, 50, and 500;
- pull at the beginning, middle, and end of a population;
- expansion at 2x, 4x, and 8x the initial retrieval depth;
- token decode/verify and next-token encode;
- page hydration with batch and per-node fallback storage;
- release and expiry cleanup;
- parallel pulls of the same token;
- concurrent starts, pulls, and releases across registry shards.
- complete scans with 20,000, 100,000, and 1,000,000 source nodes at multiple
  filter selectivities;
- grouped and ungrouped population builds with ranked-prefix hit rates of 0%,
  10%, and 100%;
- representative replacement and final sort cost for high- and low-cardinality
  group keys;
- grouped passage fan-out at 1, 10, 100, and 1,000 passages per logical asset;
- native `StreamingEngine` versus `AllNodes` fallback peak heap and build time.

Required load tests:

- ordinary non-continuation search;
- continuation with immediate draining;
- continuation with realistic client think time;
- abandoned cursors retained until expiry;
- mixed start/pull/discard traffic at admission limits;
- one principal attempting to exhaust per-owner limits;
- 1, 8, 32, and 128 concurrent clients where the test host permits.

Report:

- p50, p95, and p99 operation latency;
- operations per second;
- `B/op` and allocations per operation;
- registry lock wait and hold time;
- retained heap and RSS at configured capacity;
- cleanup duration and reclaimed bytes;
- storage hydration cost separately from registry lookup.

Acceptance criteria:

- Search without continuation has no statistically significant latency or
  allocation regression beyond measurement noise.
- No lock hold duration grows with total population size or requested page size.
- Registry lookup and token validation perform no population-sized work.
- Buffered pulls allocate only bounded response/hydration state; retained
  descriptors are not copied.
- Expansion cost is reported separately and amortized over newly discovered
  results; no expansion performs work while holding registry or entry locks.
- Memory remains bounded under abandoned-cursor and adversarial admission tests.
- HTTP, gRPC, Cypher, and extension-aware Bolt return identical IDs and rank
  order when reading the same retained population.
- Grouped protocol responses return the same ordered passage IDs for each
  logical asset without consuming additional page slots.
- Replaying one durable qid returns the same position and result IDs.
- `go test -race` passes concurrent pull, release, expiry, shutdown, and
  reconfiguration tests.

## Explicit Non-Goals For The First Release

- Making standard numeric Bolt qids globally unique or reconnect-safe.
- Retaining explicit database transactions across disconnections.
- Surviving server process restart or instance failure.
- Unbounded export: complete modes remain bounded materializations and fail
  rather than silently truncate when scan, member, byte, or time limits are hit.
- Graph-traversal grouping or parent joins; `group_by` is a flat child property.
- Holding a snapshot of mutable node properties.
- Claiming globally exact ranking across progressively discovered approximate
  candidates.
- Adding separate `.page` or `.release` procedure namespaces.

## Decisions Required During Implementation

1. Choose the shared package boundary after reconciling this work with the
   existing row-streaming plan; there must be only one base stream abstraction.
2. Verify whether supported Neo4j drivers expose unknown Bolt `PULL` success
   metadata. If they do not, `durable_qid` remains available through
   `db.retrieve` for standard drivers and through raw/extension-aware Bolt.
3. Select the exact trusted principal identifier and authorization-revision
   source for all four transports.
4. Decide whether a policy/index generation change invalidates existing ranked
   populations or only affects newly started streams.
5. Establish measured defaults for per-owner and global byte/session limits from
   benchmark heap profiles rather than copying PR #353's estimates.
