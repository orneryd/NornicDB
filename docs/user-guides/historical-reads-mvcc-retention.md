# Historical Reads and MVCC Retention

NornicDB keeps multi-version history for nodes and edges so you can ask historical questions without disturbing current reads. This is backed by MVCC (multi-version concurrency control): each logical record can have a chain of committed versions plus a persisted current head.

This guide covers:

- what MVCC history gives you
- how to query historical state with `db.temporal.asOf`
- what a historical read can and cannot return
- how retention and pruning work
- which knobs control history size and age
- how MVCC interacts with search and serialization

## What This Feature Does

MVCC history enables:

- snapshot-consistent reads of nodes, edges, labels, and relationship topology
- historical reads of retained states of entity incarnations that still exist
- pruning of older versions without deleting the current head
- tombstone-chain compaction when it is safe to do so

Important behavior:

- the current head is always preserved
- search indexes stay current-only; pruning historical versions does not change current search results
- retention settings define the default pruning policy, but they do not start a background pruning job by themselves

## Isolation and Snapshot Semantics

NornicDB uses MVCC in two related ways:

- explicit MVCC snapshot selectors let callers read historical committed state by commit time and commit sequence
- storage transactions capture a read snapshot at `BeginTransaction()` and keep committed reads pinned to that snapshot for the life of the transaction

That means:

- MVCC snapshot reads resolve against committed versions only
- storage transactions provide snapshot isolation, atomic commit, rollback, and read-your-writes
- uncommitted changes from one transaction are not visible to other transactions
- repeated point reads and label scans inside one transaction stay on the same committed graph snapshot

Use explicit MVCC snapshot selectors when you need a specific historical view. Use storage transactions when you need a stable current snapshot across a unit of work.

## Graph Snapshot Consistency

MVCC applies to both nodes and edges. When you read nodes, labels, edge types, and traversals with the **same** MVCC version selector, NornicDB resolves them against the same commit-order view of the graph.

That means:

- node and edge visibility are evaluated against the same `MVCCVersion`
- transactional commits assign one commit version to all node, edge, and property mutations in that commit
- label scans, edge-type scans, and edge-between scans have snapshot-visible APIs, not just point lookups

This is the intended consistency model for graph snapshots: pick one snapshot version, then use that same selector across all node and edge reads involved in the query.

Atomic commit visibility contract:

- a single MVCC Version ID covers all node, edge, and property mutations produced by one committed transaction
- snapshot queries that use one selector are guaranteed to be cross-entity consistent for that committed version

If a requested object was tombstoned at that snapshot, or if the requested snapshot predates the retained floor for that logical key, the read returns not found.

## Querying Historical State

### Cypher Historical Reads

The Cypher entry point for historical reads is `db.temporal.asOf`:

```cypher
CALL db.temporal.asOf(at, query, parameters)
YIELD row
```

- `at` is the commit-time cutoff for the retained entity states selected by the query.
- `query` is an ordinary read-only `MATCH` query.
- `parameters` is an optional map bound to that query's `$parameters`; it defaults to `{}`.
- each yielded `row` is one row of the query's result, resolved against the historical view.

```cypher
CALL db.temporal.asOf(
  datetime('2026-03-01T00:00:00Z'),
  'MATCH (a:Account) WHERE a.accountId = $accountId
   RETURN a.accountId AS accountId, a.balance AS balance',
  {accountId: 'acct-42'}
) YIELD row
RETURN row
```

`at` is read the same way the `TEMPORAL NO OVERLAP` constraint reads its bounds: `datetime()`, `date()` and `localdatetime()` values, ISO 8601 strings, and Unix seconds are accepted, with a date or local date-time taken as UTC.

#### What the historical view returns

A `db.temporal.asOf` read resolves properties, labels, and relationships as they were at `at` for the entity incarnations that exist now:

- property values, labels, and relationship state come from retained history, not from the current values
- only entities that exist in the current graph are readable: a deleted node or relationship is not resurrected, even when it existed at `at`
- an identifier that was deleted and later reused does not expose the earlier incarnation's state
- a current entity that did not exist at `at` has no state at `at` to report
- the read is independent of temporal constraints: `db.temporal.asOf` needs no interval constraint, and an interval constraint does not restrict it
- no physical graph snapshot is created or materialized: the procedure does not copy the graph or build an in-memory snapshot of it

#### Retention is required

Historical reads depend on retained MVCC history. When the requested `at` is not covered by retained history — for example after pruning — the call fails with an explicit error instead of returning empty rows, partial rows, or current state.

Size `mvcc_retention_max_versions` and `mvcc_retention_ttl` for the longest lookback you need, and keep the prune schedule inside that window. Historical reads need no storage migration: they use the MVCC history the database already retains.

#### Historical embeddings are not available

MVCC version records keep logical graph state without embedding payloads. A `db.temporal.asOf` read therefore returns historical properties, labels, and relationships only: no historical embedding vector is stored to return. Current vector and BM25 search stay current-only (see [Search Behavior](#search-behavior)), so a historical read cannot provide the embeddings a version once had.

#### Deprecated positional form

Before 1.5.0 the procedure took positional arguments: `CALL db.temporal.asOf(label, keyProp, keyValue, validFromProp, validToProp, asOf [, systemTime [, systemSequence]]) YIELD node`. That form is deprecated starting in 1.5.0; it is still accepted for compatibility and no removal version is set.

It reads one label and one key value per call and yields a single `node`. Without `systemTime` and `systemSequence` it selects among the label's current nodes by their validity windows — the answer is a current node, not that node's state at `asOf`. With them, the read is pinned to an MVCC snapshot. New queries should use the `at`/`query`/`parameters` form above. The older tuple form of the temporal constraint is deprecated in the same release; see [Managing Constraints](managing-constraints.md).

### Embedded Go APIs

If you embed NornicDB, the storage and DB layers expose explicit MVCC visibility methods:

```go
head, err := engine.GetNodeCurrentHead(nodeID)
if err != nil {
    return err
}

node, err := engine.GetNodeVisibleAt(nodeID, head.Version)
if err != nil {
    return err
}
```

For label and relationship scans against a snapshot, use:

- `GetNodesByLabelVisibleAt`
- `GetEdgesByTypeVisibleAt`
- `ReadAdjacentEdgesVisibleAt` (a between read is `AdjacentBetween`)

## Retention Policy

NornicDB now has a formal retention policy surface for MVCC history.

Defaults:

- `MaxVersionsPerKey = 0` (head-only; enable historical retention explicitly)
- `TTL = 0` which means no age-based protection

Semantics:

- `MaxVersionsPerKey` applies to closed historical versions
- the current head is preserved separately and is never deleted by pruning
- `TTL` protects versions newer than `now - TTL` from pruning
- pruning is head-safe and tombstone-aware

## Pruning Guarantees

Pruning is designed to reduce history without corrupting the visible head state.

NornicDB tracks a retained floor per logical key. In documentation and operations terms, treat this as the **Minimum Retained Snapshot (MRS)** for that key: older history is no longer guaranteed to exist.

Guaranteed behavior:

- the current head is always preserved
- pruning rewrites the persisted head metadata with a retained floor anchor
- snapshot reads at or above the retained floor continue to resolve correctly
- tombstone-chain compaction is only allowed when there are no active MVCC snapshot readers and no age-based protection keeping older tombstones alive

Important non-guarantees:

- pruning does **not** preserve every historical snapshot forever
- a new historical query cannot recover versions already removed before read admission
- retention settings define the pruning policy; historical readers register their cutoff for the read lifetime

Reader-aware grace period:

- active snapshot readers prevent the most aggressive physical tombstone compaction path
- registered readers protect the versions they need while active; close or cancel abandoned readers promptly
- `asOf` reports unavailable history explicitly when its requested state has already been pruned

Operationally, this means you should size `mvcc_retention_max_versions` and `mvcc_retention_ttl` for the longest historical lookback you actually need, and schedule prune maintenance accordingly.

### YAML Configuration

```yaml
database:
  storage_serializer: msgpack
  mvcc_retention_max_versions: 1
  mvcc_retention_ttl: "168h"
```

### Environment Variables

```bash
export NORNICDB_STORAGE_SERIALIZER=msgpack
export NORNICDB_MVCC_RETENTION_MAX_VERSIONS=100
export NORNICDB_MVCC_RETENTION_TTL=168h
```

### Choosing Values

Use a lower version cap when:

- the same keys are updated frequently
- oldest-snapshot lookup latency matters more than long audit depth
- your workload produces long tombstone/recreate chains

Use a TTL when:

- you need a guaranteed recent history window even under heavy churn
- operators need predictable lookback such as the last 24 hours or 7 days

Practical starting points:

- default workload: `0` versions, no TTL; historical lookback is not retained
- example history-enabled workload: `100` versions, no TTL
- moderate write churn with recent debugging needs: `50` versions, `24h` TTL
- audit-heavy but bounded history: `100` versions, `168h` TTL

## Pruning History

Retention settings define the default policy, but pruning is a maintenance action.

For embedded deployments, call the DB maintenance API:

```go
deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{})
if err != nil {
    return err
}

log.Printf("pruned %d historical versions", deleted)
```

Passing zero values uses the configured engine retention policy.

You can also override the configured defaults for a one-off maintenance run:

```go
deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{
    MaxVersionsPerKey: 50,
    MinRetentionAge:   24 * time.Hour,
})
```

If you need to reconstruct head state from stored history, use:

```go
if err := db.RebuildMVCCHeads(ctx); err != nil {
    return err
}
```

## Failure Modes and Operational Limits

The main failure and degradation modes to plan for are:

1. snapshots older than the retained floor
  Reads below the Minimum Retained Snapshot for a logical key return `ErrNotFound`. This is expected after pruning and should be treated as history no longer being retained.

2. long-running historical queries during aggressive pruning
  Active snapshot readers prevent the most aggressive tombstone compaction path, but pruning still enforces retention policy. If a query depends on versions outside the retained window, it can still lose access to those older versions.

3. startup and restore rebuild cost
  `RebuildMVCCHeads` is correctness-first. The current implementation scans stored MVCC version records and then current records to restore missing head state. On large stores, that can increase startup or restore time.

4. async write visibility
  Async writes are an eventual-consistency mode. They improve throughput, but they are a separate visibility model from explicit MVCC historical reads.

## Startup and Recovery Behavior

NornicDB rebuilds MVCC heads during startup warmup and after restore so search and snapshot reads have consistent head metadata available.

Current implementation characteristics:

- rebuild is a foreground correctness step before search warmup completes
- rebuild is based on scanning existing MVCC version records, then bootstrapping from current records where needed
- rebuild cost is $O(N)$ in the number of versioned records that must be scanned

This is safe, but it means startup cost can scale directly with stored MVCC history. On very large datasets, full MVCC head reconstruction can become a material startup or restore event. If you operate very large stores with deep history, treat MVCC head rebuild time as an operational budget item and benchmark it in your environment.

## Search Behavior

Search indexing remains current-only.

That means:

- historical MVCC versions are not added to current search indexes
- pruning historical versions does not change current vector or BM25 search results
- HNSW and current candidate selection continue to operate on the latest visible graph state

This separation is intentional: MVCC history is for temporal correctness and historical inspection, not for duplicating search corpus entries.

The current search benchmark and prune regression should be read as a **structural integrity smoke test**: they verify that adding history depth and pruning retained history do not perturb current-only search candidate selection in the tested fixture. They are not a blanket claim about every workload, corpus, or ranking metric.

## Serialization Notes

`storage_serializer` still controls the primary storage format for main records in the broader storage layer.

For MVCC specifically:

- MVCC version records use Msgpack on the hot path
- MVCC head metadata uses Msgpack
- new MVCC/internal metadata does not use gob

For new deployments, keep `storage_serializer: msgpack`.

## Operational Guidance

Watch for these signals:

- frequent updates to the same logical keys
- a growing gap between current-read latency and oldest-snapshot latency
- long delete/recreate sequences on the same IDs

When those appear:

1. lower `mvcc_retention_max_versions`
2. add an `mvcc_retention_ttl` if you need a recent lookback guarantee
3. run `PruneMVCCVersions` during maintenance windows or scheduled operator workflows

## Related Docs

- [Operations Configuration](../operations/configuration.md)
- [Environment Variables](../operations/environment-variables.md)
- [Storage Serialization](../operations/storage-serialization.md)
- [Managing Constraints](managing-constraints.md)
- [Cypher Queries](cypher-queries.md)

See [temporal read and constraint costs](../performance/temporal-read-and-constraint-costs.md) for N-arity examples, applicable indexes, scaling costs and scoped measurements.
