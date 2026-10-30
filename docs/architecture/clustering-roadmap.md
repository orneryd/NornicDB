# Clustering Architecture & Roadmap

> **Current capabilities and design notes for distribution and sharding.**

## Current Capabilities

- **Standalone Mode** — Single node, embedded or server.
- **Hot Standby** — 2-node primary/standby with WAL replication.
- **Raft Cluster** — 3-5 node strong consistency cluster.
- **Multi-Region** — Per-region Raft clusters with async cross-region replication.
- **Composite Databases (Sharding)** — Horizontal sharding via composite databases that span multiple constituent databases. Local *and* remote constituents are supported, so a single composite database can shard data across multiple NornicDB servers.

## Deployment Tiers

| Tier | Nodes | Approach | Status |
|------|-------|----------|--------|
| Embedded | 1 | Library mode | ✅ Available |
| Standalone | 1-2 | Single binary, optional standby | ✅ Available |
| Raft Cluster | 3-5 | Strong-consistency Raft per database | ✅ Available |
| Multi-Region | 6+ | Per-region Raft + async cross-region | ✅ Available |
| **Composite-sharded** | N | Composite DB over local/remote constituents | ✅ Available |

## Horizontal Sharding via Composite Databases

NornicDB achieves horizontal sharding through **composite databases**. A composite database is a virtual top-level database that routes Cypher subqueries to constituent physical databases. Constituents may be local (same server) or **remote** (a different NornicDB server reached over HTTP).

### What Ships Today

- Composite database creation: `CREATE COMPOSITE DATABASE <name> ALIAS <alias> FOR DATABASE <db> ...`.
- Local constituents: composite root delegates `USE <composite>.<alias>` subqueries to the local engine for that database.
- **Remote constituents**: `AT '<url>'` clause routes a constituent to another NornicDB server. Auth is forwarded via `OIDC CREDENTIAL FORWARDING` (default) or explicit `USER/PASSWORD`.
- Distributed transaction coordination with the **many-read / one-write** rule per transaction (writes pinned to a single constituent shard).
- Schema introspection and DDL targeted at a constituent via `USE <composite>.<alias>`.
- Schema merging across constituents for `SHOW CONSTRAINTS` / `SHOW INDEXES` views.
- Per-database resource limits (`ALTER DATABASE ... SET LIMIT max_nodes/max_edges/max_bytes/...`) — applied per shard.

See:

- [Multi-Database User Guide → Composite Databases](../user-guides/multi-database.md#composite-databases)
- [Infinigraph Topology](../user-guides/infinigraph-topology.md) — design pattern guide for sharded deployments

### Sharding Strategies (in user code)

Sharding strategy is expressed by **how you define your constituent databases and your `shard_key` property contract**, not by a built-in router. The patterns are:

- **Label-based sharding** — separate constituent databases per label class (e.g. `riskgraph.identity`, `riskgraph.tx_us_0`). Read-heavy joins use bounded candidate extraction in the source shard plus key-based enrichment lookups in the destination shard. See [Infinigraph Topology → Bounded Cross-Constituent Query Flow](../user-guides/infinigraph-topology.md#step-3-bounded-cross-constituent-query-flow).
- **Hash-based sharding** — write a deterministic `shard_key` property derived from the entity's stable identifier; hash to one of N constituents named `shard_0`, `shard_1`, … . Application code chooses the constituent in the `USE <composite>.<alias>` clause.
- **Region-based sharding** — combine with multi-region: each region runs its own composite over its local constituents; cross-region access uses remote constituents.

The composite layer guarantees identity propagation, the many-read/one-write rule, and search correctness; the user is responsible for the routing key.

## Vector Index Per Shard

Each constituent database carries its own vector index. There is no automatic cross-shard `db.index.vector.queryNodes` aggregation; for cross-shard semantic search, fan out via `CALL { USE <composite>.<alias> ... }` blocks and merge in the outer query.

## Heterogeneous Constituents

Because constituents are independent NornicDB processes, they can run on different hardware. Concretely:

- A constituent on a Raspberry Pi can host a smaller dataset and skip embeddings entirely (`NORNICDB_EMBEDDING_ENABLED=false`).
- A constituent on a GPU server can host the embedding-bearing data and run vector search against its local index.
- A constituent on a CUDA server can run K-means clustering for IVF candidate generation while a separate constituent stays CPU-only.

This is "heterogeneous clusters" in practice: the unit of homogeneity is the **constituent**, not the composite. There is no shared/centralized capability registry; routing decisions live in user code via `USE <composite>.<alias>`.

## Live Rebalancing

Composite databases do not currently support automatic live shard migration. To rebalance:

1. Provision a new constituent.
2. Run a migration (Cypher copy, JSON backup/restore, or one of the scripts in [`scripts/migration/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration)) to move data across.
3. `ALTER COMPOSITE DATABASE` to add/remove the constituent alias.
4. Switch application traffic.

This is a documented operational pattern, not an automated feature. Automated dual-write-and-cutover is out of scope for the current implementation.

## Multi-Region

Geographic distribution with async cross-region replication:

- ✅ Per-region Raft clusters (strong local consistency)
- ✅ Cross-region WAL streaming (async replication)
- ✅ Conflict resolution strategies (`last_write_wins`, `manual`)
- ✅ Configurable cross-region sync modes (`async`, `quorum`)
- ✅ Region failover and promotion

### Chaos Testing

Tested under representative network conditions:

- **Extreme latency:** 2000-3000ms spikes (cross-region scenarios)
- **Packet loss:** up to 20% packet loss handling
- **Data corruption:** detection and recovery
- **Connection drops:** automatic reconnection
- **Byzantine failures:** malicious data, replay attacks
- **Reordering:** out-of-order packet handling

See **[Clustering Guide → Multi-Region](../user-guides/clustering.md#mode-3-multi-region)** for setup instructions.

## Technical References

- [Cassandra Architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/)
- [Dgraph Sharding](https://dgraph.io/docs/design-concepts/)
- [Raft Consensus](https://raft.github.io/)

## See Also

- **[Clustering Guide](../user-guides/clustering.md)** — Operator-facing setup
- **[Replication Architecture](replication.md)** — Internal replication details
- **[Multi-Database User Guide](../user-guides/multi-database.md)** — Composite databases, constituents, aliases
- **[Infinigraph Topology](../user-guides/infinigraph-topology.md)** — Sharded composite design pattern
- **[Composite DB Analysis](composite-db-analysis.md)** — Capability comparison
- **[Scaling Guide](../operations/scaling.md)** — Current scaling knobs
