# Secure Search Continuation Architecture

Search continuation in NornicDB is a protocol-neutral START/PULL/DISCARD
contract for search result streams. It is not OFFSET pagination with a friendlier
name. The design goal is to let a client page ranked and complete search results
without re-running expensive query preparation on every page, without leaking
cursors across users or databases, and without letting normal result caching
bypass cursor lifecycle checks.

The user-facing contract is documented in
[Search Continuation](../user-guides/search-continuation.md). This note covers
the architecture, the performance and caching hurdles, and the tradeoffs against
other continuation models.

## The Problem

Deep search pagination has two hard cases:

- Ranked hybrid search has unstable boundaries. If page 2 is implemented as
  `offset + limit`, approximate vector retrieval can shift ordering between
  requests and return duplicates or skip rows.
- Complete catalogue traversal must be exact. A client may ask for "ranked
  prefix, then everything else by ID" or for pure ID traversal, and those modes
  need deterministic page membership, explicit completion labels, and stable
  ownership checks.

The continuation API therefore separates page size from retrieval depth:
`n` is the page size, while `limit` is the initial ranked retrieval depth.
`max_results` is the optional stream ceiling. `ranked_limit` can pin the ranked
prefix for `ranked_then_id`.

## External Baselines

Different systems use the word "continuation" for different workloads:

| System | Continuation shape | Main tradeoff |
| --- | --- | --- |
| Qdrant | Scroll returns points page by page in ID order with `next_page_offset`; ranked search uses `offset` and `limit`. Qdrant documents that large offsets can be expensive because search internally retrieves `offset + limit`, and approximate HNSW pagination can duplicate or skip rows. See [Qdrant search pagination](https://qdrant.tech/documentation/search/) and [Qdrant scroll](https://api.qdrant.tech/api-reference/points/scroll-points). | Good ID-ordered traversal and familiar ranked offsets, but ranked pagination is still offset-based and approximate-order sensitive. |
| Weaviate | `after` is a cursor for sequential object listing, but it is not compatible with `where`, `near*`, `bm25`, `hybrid`, or similar searches. Those use `offset` and `limit`, and Weaviate documents that offset pagination is not stateful and gets more expensive as the offset grows. See [Weaviate additional operators](https://docs.weaviate.io/weaviate/api/graphql/additional-operators). | Strong simple listing cursor, but ranked/vector/hybrid searches still use offset semantics. |
| Milvus | `SearchIterator` and `QueryIterator` expose paginated iterator APIs. The search iterator is aimed at retrieving more ANN hits than a single request limit; clients set a batch size and total top-K. See [Milvus Search Iterator](https://milvus.io/docs/with-iterators.md) and [Milvus QueryIterator](https://milvus.io/docs/v2.6.x/get-and-scalar-query.md). | Better than OFFSET for large ANN retrieval, but still framed around collection/vector search iterators rather than a graph-aware, cross-protocol qid shared by HTTP, Bolt/Cypher, and gRPC. |
| Azure Cosmos DB | Query continuation tokens are server-stateless bookmarks. Cosmos DB documents that tokens can resume query progress later, do not expire as long as the same SDK version is used, and are not available for some state-heavy shapes such as `GROUP BY`. See [Cosmos DB query pagination](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/pagination). | Excellent stateless query continuation, but the token resumes a document query plan, not a retained hybrid ranking state with rerank budgets, grouped graph hydration, and per-protocol search metadata. |
| DynamoDB | `Query` and `Scan` return `LastEvaluatedKey`; clients pass it back as `ExclusiveStartKey`. Scans page by 1 MB chunks, and filtering is applied after reading. See [DynamoDB Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) and [DynamoDB Scan pagination](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html). | Efficient key-ordered table/index continuation, but it is keyset pagination over items, not score-ordered search continuation. |

NornicDB sits in a different spot: it is a graph database with BM25, vector,
hybrid RRF, optional reranking, grouped child passages, and Neo4j-compatible
Bolt/Cypher surfaces. We chose a bounded process-local cursor because the state
that makes ranked continuation correct is not only "last primary key"; it can
also include prepared query chunks, embeddings, a ranked prefix, seen ranked
IDs, completion evidence, and compact catalogue descriptors.

## Core Design

The shared cursor registry lives in
[pkg/resultstream/registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L41-L49).
It bounds active streams by process, owner, page size, retained bytes, and
per-owner retained bytes. A `Scope` binds each cursor to one owner and one
canonical database
([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L27-L31)).

Qids are opaque signed tokens. The token stores version, instance ID, stream ID,
position, expiry, and MAC
([token.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/token.go#L22-L31)); decoding checks length,
version, instance, and MAC
([token.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/token.go#L34-L51)). The owner and database
are not trusted from the client token. They are stored server-side as keyed
digests and checked on every pull/discard
([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L229-L260),
[registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L263-L315)).

`Start` pulls the first page before publishing the stream. If there is no next
page, the stream is closed immediately and no qid is issued
([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L148-L160)). If there is more
data, registry admission accounts for the stream before insertion and emits a
new qid at the returned position
([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L162-L225)).

The registry is deliberately small and narrow:

- stream lookup/removal uses 32 shards
  ([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L16-L16),
  [registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L397-L418));
- `Pull` resolves and validates the qid under registry locks, then invokes the
  stream without holding a registry shard lock
  ([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L263-L315));
- admission and retained-byte accounting are centralized under `admissionMu`
  ([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L420-L478));
- expiry is handled by a reaper that removes expired entries and closes streams
  outside the shard lock
  ([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L514-L552)).

## Ranked Mode

`ranked` mode retains an append-only ranked prefix. It starts with one real
search call, compacts the returned hits, and registers a progressive stream
([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L285-L383)).

The progressive stream does geometric lookahead only when the buffered rows
cannot prove whether another page exists. The important implementation detail is
that expansion work runs outside the stream lock; only publication of the new
rows is locked
([progressive.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/progressive.go#L38-L129)). Concurrent
pulls for the same cursor join the in-flight expansion instead of issuing their
own provider/search request
([progressive.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/progressive.go#L88-L109)).

Ranked continuation avoids repeated query preparation:

- query chunking is guarded by `sync.Once`
  ([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L433-L443));
- embeddings are memoized per chunk and copied before retention
  ([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L446-L463));
- later expansions reuse those wrappers
  ([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L332-L343)).

Producer exhaustion is explicit. `SearchResponse.RetrievalExhausted` means every
participating branch is actually exhausted; a short approximate result is not
enough. `CandidateBudgetReached` means the producer hit a configured candidate
budget and cannot expose a deeper ranked prefix without changing that budget
([search.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/search.go#L253-L262)). Stage-2 reranking sets the
candidate-budget signal only once the request has reached the rerank top-K and
the pre-rerank candidate list is still larger than that budget
([search.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/search.go#L4280-L4309)).

Approximate vector and hybrid producers can also plateau: if repeated deeper
requests do not increase the prefix, continuation treats that as a bounded
candidate pool for the known approximate methods
([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L106-L156)). This
prevents unbounded provider or ANN work when a deeper `limit` cannot expose more
ranked rows, while avoiding the earlier bug of treating one short batch as
completion.

## ID And Ranked-Then-ID Modes

`id` and `ranked_then_id` need a complete eligible population. The builder lives
in
[pkg/search/text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L152-L356).

For `ranked_then_id`, NornicDB first prepares the ranked prefix. If
`ranked_limit` is omitted, the ranked request deepens until it sees true
retrieval exhaustion, a declared candidate budget, a bounded approximate
plateau, or an explicit depth limit
([text_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_continuation.go#L216-L273)).
Then the complete builder scans eligible nodes and merges the ranked prefix with
the catalogue tail.

The complete builder is bounded by policy:

- maximum scanned nodes, logical members, child passages, retained bytes,
  duration, and concurrent builds are defined in
  [text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L23-L42);
- active complete builds are admitted with an atomic counter
  ([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L73-L90));
- `max_results` is labeled as a caller ceiling, not collection exhaustion
  ([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L323-L354)).

Complete modes also bind the graph mutation revision. The stream captures the
revision before and after the build and fails if the graph changed during
materialization
([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L160-L168),
[text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L272-L275)).
Each pull rechecks the graph revision and continuation policy generation before
hydrating a page
([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L473-L534)).

## Memory And Hydration

Continuation does not retain full node payloads or embedding vectors for every
page. Search hits are compacted before retention by clearing display fields,
properties, and child passages where they can be reconstructed later
([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L359-L372)).
On pull, compact descriptors are hydrated from current storage
([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L537-L593)).

Two storage extension points keep this cheap:

- complete builds use projected prefix scanning when no per-node authorization
  callback requires full node access
  ([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L114-L150));
- pull hydration prefers batched reads without embeddings
  ([text_catalog_continuation.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/text_catalog_continuation.go#L596-L607),
  [types.go](https://github.com/orneryd/NornicDB/blob/main/pkg/storage/types.go#L496-L531)).

The Badger implementation can stream only selected user properties while
iterating nodes
([badger_stats.go](https://github.com/orneryd/NornicDB/blob/main/pkg/storage/badger_stats.go#L663-L695)). That matters
for large vector corpora: complete-mode eligibility and grouping do not need to
decode stored embedding arrays just to decide page membership.

## Caching And Ownership

The durable qid must always reach the registry. Ordinary Cypher result caching
is therefore disabled for `CALL db.retrieve(...)`, including when the qid,
discard flag, or continuation options are hidden in parameters
([cache_policy.go](https://github.com/orneryd/NornicDB/blob/main/pkg/cypher/cache_policy.go#L15-L50)). This avoids two
bad outcomes: returning a cached START token to a different owner, and returning
a cached PULL page after the cursor was discarded.

The protocol adapters all converge on `SearchTextContinuation`:

- HTTP parses continuation fields, resolves the qid database binding before
  selecting a service, and serializes the continuation envelope
  ([server_nornicdb.go](https://github.com/orneryd/NornicDB/blob/main/pkg/server/server_nornicdb.go#L303-L340),
  [server_nornicdb.go](https://github.com/orneryd/NornicDB/blob/main/pkg/server/server_nornicdb.go#L454-L552),
  [server_nornicdb.go](https://github.com/orneryd/NornicDB/blob/main/pkg/server/server_nornicdb.go#L648-L703));
- Cypher `db.retrieve` constructs the same request object and returns one
  `page` value for continuation calls
  ([call_rag.go](https://github.com/orneryd/NornicDB/blob/main/pkg/cypher/call_rag.go#L184-L350));
- native gRPC uses the same request fields and maps continuation errors to
  protocol statuses
  ([search_service.go](https://github.com/orneryd/NornicDB/blob/main/pkg/nornicgrpc/search_service.go#L120-L188),
  [search_service.go](https://github.com/orneryd/NornicDB/blob/main/pkg/nornicgrpc/search_service.go#L242-L300)).

Database services share one process registry so a qid can move across adapters
inside the same process and database namespace
([search_services.go](https://github.com/orneryd/NornicDB/blob/main/pkg/nornicdb/search_services.go#L275-L282),
[search_services.go](https://github.com/orneryd/NornicDB/blob/main/pkg/nornicdb/search_services.go#L393-L425)).
Operators enable the feature by setting a positive `memory.search_cursor_max`
or `NORNICDB_SEARCH_CURSOR_MAX`; zero disables durable search continuation
([config.go](https://github.com/orneryd/NornicDB/blob/main/pkg/config/config.go#L682-L686),
[configuration.md](../operations/configuration.md)).

## Observability

The registry exposes cursor lifecycle and aggregate usage events through a small
observer interface
([registry.go](https://github.com/orneryd/NornicDB/blob/main/pkg/resultstream/registry.go#L33-L37)). Search metrics bind
that observer when metrics are attached
([observability.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/observability.go#L63-L83)).
Cursor events are bounded to known outcomes, and the usage gauges track active
cursors and retained bytes
([observability.go](https://github.com/orneryd/NornicDB/blob/main/pkg/search/observability.go#L91-L114)).

## Tradeoffs

What NornicDB gains:

- forward-only ranked pages without deep OFFSET recomputation;
- stable qids across HTTP, Bolt/Cypher, and native gRPC in one process;
- owner and database binding on every pull and discard;
- no repeated chunking or embedding while a ranked stream deepens;
- bounded retained state with stream-count and retained-byte admission;
- explicit completion labels for "more", "candidate budget", "caller ceiling",
  and "eligible population exhausted";
- exact catalogue modes that detect graph mutation instead of lying about an
  eligible count.

What NornicDB pays:

- cursors are process-local; multi-instance deployments need affinity;
- qids are fixed-expiry and do not survive process restart;
- the registry retains bounded server memory instead of using a purely
  stateless token;
- complete `id` and `ranked_then_id` starts may scan the eligible population up
  front, which is deliberate so later pages are cheap and deterministic;
- continuation is forward-only, not random page access;
- ranked membership/order is retained, but page hydration reads current node
  display fields, so a mutation can change hydrated metadata unless the mode is
  one of the complete revision-bound streams.

That trade is intentional. NornicDB is optimizing for secure, deterministic
forward paging of graph search results, including hybrid/reranked/grouped
results over Neo4j-compatible protocols. Systems such as Cosmos DB and
DynamoDB show how effective stateless query tokens are for key/document query
plans; Qdrant, Weaviate, and Milvus show the available vector-search continuum.
NornicDB uses a process-local retained stream because its continuation boundary
has to preserve more than a key, and because normal query caching must never be
allowed to short-circuit cursor ownership, discard, expiry, or invalidation.
