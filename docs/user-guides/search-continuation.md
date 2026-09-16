# Search Continuation

NornicDB exposes one durable START/PULL/DISCARD contract through HTTP, native
gRPC, and the existing `db.retrieve` Cypher procedure. The opaque qid is shared
by these adapters when they use the same server process, authenticated
principal, and canonical database.

## Operations

| Operation | Fields                 | Result                                       |
| --------- | ---------------------- | -------------------------------------------- |
| START     | query/options plus `n` | First page and a qid when more results exist |
| PULL      | `qid`, `n`             | Deterministic page at the token position     |
| DISCARD   | `qid`, `discard: true` | Releases the retained stream                 |

`n` is a page size. In `ranked` mode, `limit` is the initial retrieval depth,
not a lifetime ceiling. `max_results` is the optional explicit lifetime
ceiling. A terminal response has `has_more: false` and no next qid; inspect
`completion` to distinguish true exhaustion from a configured candidate budget
or a requested result ceiling.

## Modes

| Mode             | Population                                         | Order                          |
| ---------------- | -------------------------------------------------- | ------------------------------ |
| `ranked`         | Progressively discovered canonical ranked hits     | Append-only ranked expansions  |
| `ranked_then_id` | Ranked prefix, then every other eligible result    | Ranked prefix, then logical ID |
| `id`             | Every eligible result without embedding or ranking | Logical ID                     |

`ranked_limit` fixes the ranked prefix in `ranked_then_id`. `group_by` names a
flat property containing a nonempty UTF-8 string. Grouping happens before
pagination: one logical asset consumes one page slot, while its ordered
`passages` collection retains matching child nodes.

`ranked_pool_exhausted` is conservative. It is `true` for `id` mode or when all
participating retrieval branches establish exhaustion without truncating their
candidate prefixes. A short approximate, filtered, or fused result does not
establish exhaustion. `ranked_limit` still selects a fixed ranked prefix; it
does not prove that further ranked candidates do not exist. When a producer
reaches an explicit candidate budget, such as a Stage-2 rerank top-K boundary,
the stream ends with `completion: "candidate_pool_exhausted"` and
`ranked_pool_exhausted: false`. Rerank producers declare that boundary only
after the requested retrieval depth reaches the configured top-K and the
pre-rerank candidate list still contains more rows. A shallow `limit` below the
top-K can continue deepening inside the same rerank budget.

When `ranked` mode reaches the natural end of an exact producer without a
candidate-budget boundary, the terminal page reports
`completion: "eligible_population_exhausted"`,
`ranked_pool_exhausted: true`, and `collection_exhausted: true`. This is the
same terminal reason used by complete modes when their full eligible population
has been emitted.

When `max_results` ends a stream before the complete eligible population is
emitted, the terminal page reports `completion: "max_results_reached"` and
`collection_exhausted: false`. `eligible_count` remains the discovered complete
population for `id` and `ranked_then_id`, so clients can distinguish a caller
ceiling from true collection exhaustion.

Progressive ranked continuation deepens short batches using the prepared query
embeddings, including each chunk of a multi-chunk query. A single short batch is
not treated as completion. If repeated deeper requests do not increase an
approximate vector or hybrid ranked producer's prefix, the stream stops at the
stable prefix and reports `completion: "candidate_pool_exhausted"` with
`ranked_pool_exhausted: false`. Exact BM25 producers and opaque integrations
still require proven exhaustion, an explicit candidate-budget signal, or an
explicit ceiling. The engine does not impose an arbitrary candidate ceiling:
retrieval may deepen to the searchable population. Go and Cypher callers may set
`MaxCandidateLimit` to impose a lower per-request budget. If that budget is
reached without proven exhaustion, an explicit candidate-budget signal, repeated
approximate-producer non-growth, or the requested `max_results`, the operation
fails with the existing capacity error instead of returning a falsely exhausted
page. HNSW establishes exhaustion only when its pre-filter candidate heap covers
the live index (excluding deleted entries) and the returned prefix is not
truncated. Unavailable embedding or retrieval branches do not prevent an exact,
selected BM25 fallback from completing. Replaying a candidate-budgeted request
does not increase its budget.

## HTTP

```http
POST /nornicdb/search
Content-Type: application/json

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

Pull and discard use the same endpoint:

```json
{ "qid": "opaque-signed-token", "n": 50 }
```

```json
{ "qid": "opaque-signed-token", "discard": true }
```

When no continuation fields are supplied, HTTP preserves its legacy array
response and exposes a fallback reason, when present, in the
`X-NornicDB-Search-Fallback-Reason` header. A continuation request returns an
object containing `results`, `qid`,
`has_more`, `position`, `returned`, `discovered`, `total`, `expires_at`,
`search_method`, `fallback_triggered`, `fallback_reason`, `mode`, `ranked_count`,
`eligible_count`, `ranked_pool_exhausted`, `collection_exhausted`, and
`completion`.

## Native gRPC

Set `SearchTextRequest.n` to start. Send `SearchTextResponse.qid` in a later
request with a new `n`, or set `discard`. The additive request fields are
`qid`, `n`, `discard`, `max_results`, `mode`, `group_by`, and `ranked_limit`.
`SearchHit` adds `phase`, `group_key`, and repeated `passages`. Native gRPC
uses the same `SearchHit` message for grouped child passages, so per-hit
metadata is preserved on both parent and child results.

## Cypher and Bolt drivers

Continuation-enabled `db.retrieve` calls return one `page` column:

```cypher
CALL db.retrieve({query: $query, mode: 'ranked', limit: 50, n: 10})
YIELD page RETURN page
```

Resume after reconnecting with any standard Neo4j driver:

```cypher
CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page
```

Ordinary `db.retrieve` calls without continuation options retain their existing
`node`, `score`, `rrf_score`, `vector_rank`, `bm25_rank`, `search_method`, and
`fallback_triggered` columns, plus `fallback_reason` when the requested search
path changed. Bolt's standard numeric qid remains local to a
connection and transaction; it is not the opaque durable qid. A continuation
START also publishes the opaque token as additive `durable_qid` metadata on
Bolt's `RUN` `SUCCESS` message when more results exist.

`fallback_reason` is a stable diagnostic code, not a provider error string.
Current values are `query_embedding_failed`, `query_embedding_unavailable`,
`no_embedder`, `no_hybrid_results`, and `hybrid_search_failed`. This keeps
provider credentials and response bodies out of caller-visible metadata while
the corresponding warning log retains the operational error.

## Security, consistency, and lifetime

Durable continuation is disabled by default. Operators enable it by setting
`memory.search_cursor_max` to a positive process-wide cursor limit, or by
setting `NORNICDB_SEARCH_CURSOR_MAX`. Zero means disabled.
`memory.search_cursor_ttl` (or `NORNICDB_SEARCH_CURSOR_TTL`) sets the fixed
lifetime in milliseconds and defaults to `300000` (five minutes). Ordinary
searches without continuation fields are unaffected when the feature is
disabled.

Qids are signed, fixed-expiry, process-local tokens. Registry state stores a
keyed owner/database digest, not credentials. Pull and discard require the same
validated principal and canonical database. A different server instance cannot
resume a process-local qid, so multi-instance deployments require affinity.

Ranked streams preserve emitted membership, order, and score metadata while
hydrating current node values. Complete `id` and `ranked_then_id` populations
also bind the graph mutation revision and continuation policy generation; a
change invalidates the qid rather than returning an inexact eligible count.

Complete builds fail instead of truncating when scan, member, passage, retained
descriptor byte, build-duration, or concurrent-build limits are exceeded.
The shared registry additionally enforces global and per-owner active-stream
and reported retained-byte limits. Complete streams report their compact
descriptor bytes to this admission layer; streams that do not implement byte
reporting are still governed by stream-count limits.

Continuation errors have stable protocol mappings:

| Condition                             | HTTP                                                   | Native gRPC          | Bolt/Cypher over Bolt                            |
| ------------------------------------- | ------------------------------------------------------ | -------------------- | ------------------------------------------------ |
| Disabled                              | `409 continuation_disabled`                            | `FailedPrecondition` | `Neo.ClientError.Statement.UnsupportedOperation` |
| Capacity saturated                    | `503 continuation_saturated`, `Retry-After`, retryable | `ResourceExhausted`  | `Neo.TransientError.General.DatabaseUnavailable` |
| Expired, released, or invalidated qid | `410 continuation_gone`                                | `NotFound`           | `Neo.ClientError.Statement.EntityNotFound`       |
| Malformed or wrong-scope qid          | `400 continuation_invalid`                             | `InvalidArgument`    | client error                                     |

Wrong-scope qids are deliberately indistinguishable from malformed qids; the
server does not reveal whether another owner or database has a matching cursor.

Prometheus exposes `nornicdb_search_cursors_active`,
`nornicdb_search_cursor_retained_bytes`, and
`nornicdb_search_cursor_events_total{outcome}`. The outcome label is a closed
set and structured lifecycle events omit qids, owners, queries, and database
identifiers.

Complete builds prefer projected prefix streaming when the storage engine
supports it. During materialization the builder only decodes properties needed
for eligibility: `group_by`, filter keys, `type`, and temporal-constraint
fields. Page pulls still hydrate full nodes before returning results. Requests
with an `AuthorizeNode` callback use full-node streaming because the callback
may inspect arbitrary properties. Engines without streaming support use
`AllNodes` fallback, whose temporary full node slice is outside retained
descriptor admission; operators using a fallback engine must budget for that
additional build-time memory.
