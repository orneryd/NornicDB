# NornicDB Clustering Guide

This guide covers how to set up, configure, and operate NornicDB in clustered configurations for high availability.

## Table of Contents

1. [Overview](#overview)
2. [Choosing a Replication Mode](#choosing-a-replication-mode)
3. [Mode 1: Hot Standby (2 Nodes)](#mode-1-hot-standby-2-nodes)
4. [Mode 2: Raft Cluster (3+ Nodes)](#mode-2-raft-cluster-3-nodes)
5. [Mode 3: Multi-Region](#mode-3-multi-region)
6. [Client Connection](#client-connection)
7. [Monitoring](#monitoring)
8. [Configuration Reference](#configuration-reference)
9. [Best Practices](#best-practices)
10. [Technical Deep Dive](#technical-deep-dive)

---

## Overview

NornicDB supports multiple replication modes to meet different availability and consistency requirements:

| Mode             | Nodes | Consistency  | Use Case                               |
| ---------------- | ----- | ------------ | -------------------------------------- |
| **Standalone**   | 1     | N/A          | Development, testing, small workloads  |
| **Hot Standby**  | 2     | Eventual     | Simple HA, fast failover               |
| **Raft Cluster** | 3-5   | Strong       | Production HA, consistent reads        |
| **Multi-Region** | 6+    | Configurable | Global distribution, disaster recovery |

### Architecture Diagram

```
MODE 1: HOT STANDBY (2 nodes)
┌─────────────┐      WAL Stream      ┌─────────────┐
│   Primary   │ ──────────────────►  │   Standby   │
│  (writes)   │    (async/quorum)    │  (failover) │
└─────────────┘                      └─────────────┘

MODE 2: RAFT CLUSTER (3-5 nodes)
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Leader    │◄──►│  Follower   │◄──►│  Follower   │
│  (writes)   │    │  (reads)    │    │  (reads)    │
└─────────────┘    └─────────────┘    └─────────────┘
        │                  │                  │
        └──────────────────┴──────────────────┘
                   Raft Consensus

MODE 3: MULTI-REGION (Raft clusters + cross-region HA)
┌─────────────────────────┐      ┌─────────────────────────┐
│      US-EAST REGION     │      │      EU-WEST REGION     │
│  ┌───┐ ┌───┐ ┌───┐     │ WAL  │     ┌───┐ ┌───┐ ┌───┐  │
│  │ L │ │ F │ │ F │     │◄────►│     │ L │ │ F │ │ F │  │
│  └───┘ └───┘ └───┘     │async │     └───┘ └───┘ └───┘  │
│     Raft Cluster A      │      │      Raft Cluster B    │
└─────────────────────────┘      └─────────────────────────┘
```

---

## Choosing a Replication Mode

### Hot Standby (2 nodes)

**Choose this when:**

- You need simple, fast failover.
- You have exactly 2 nodes available.
- Eventual consistency is acceptable.
- You want minimal operational complexity.

**Trade-offs:**

- ✅ Simple setup and operation.
- ✅ Low resource overhead.
- ⚠️ Only 2 nodes (no quorum).
- ⚠️ Risk of data loss on async failover.

### Raft Cluster (3-5 nodes)

**Choose this when:**

- You need strong consistency guarantees.
- You want automatic leader election.
- You can deploy 3+ nodes.
- Data integrity is critical.

**Trade-offs:**

- ✅ Strong consistency (linearizable).
- ✅ Automatic leader election.
- ✅ No data loss on failover.
- ⚠️ Higher latency (quorum writes).
- ⚠️ Requires odd number of nodes.

### Multi-Region (6+ nodes)

**Choose this when:**

- You need geographic distribution.
- You want disaster recovery across regions.
- You have users in multiple geographic areas.
- You can tolerate cross-region latency.

**Trade-offs:**

- ✅ Geographic redundancy.
- ✅ Local read performance.
- ⚠️ Complex setup.
- ⚠️ Cross-region latency for writes.

---

## Mode 1: Hot Standby (2 Nodes)

Hot Standby provides simple high availability with one primary node handling all writes and one standby node receiving replicated data.

### Runbook Summary

- Connect all **writes** (GraphQL/HTTP/Bolt/Qdrant gRPC) to the **primary** node.
- The **standby is read-only** and applies replicated WAL batches from the primary.
- There is currently **no automatic write forwarding** from standby to primary.
- Replication traffic uses a **separate internal TCP port** (`NORNICDB_CLUSTER_BIND_ADDR`, default `0.0.0.0:7000`) and should not overlap with Bolt/HTTP/Qdrant gRPC ports.

### Required Configuration

| Variable                            | Primary        | Standby        | Description            |
| ----------------------------------- | -------------- | -------------- | ---------------------- |
| `NORNICDB_CLUSTER_MODE`             | `ha_standby`   | `ha_standby`   | Replication mode       |
| `NORNICDB_CLUSTER_NODE_ID`          | `primary-1`    | `standby-1`    | Unique node identifier |
| `NORNICDB_CLUSTER_HA_ROLE`          | `primary`      | `standby`      | Node role              |
| `NORNICDB_CLUSTER_HA_PEER_ADDR`     | `standby:7000` | `primary:7000` | Peer address           |
| `NORNICDB_CLUSTER_HA_AUTO_FAILOVER` | `true`         | `true`         | Enable auto-failover   |

### Deployment (Docker Compose)

Create a `docker-compose.ha.yml`:

```yaml
version: "3.8"

services:
  nornicdb-primary:
    image: nornicdb:latest
    container_name: nornicdb-primary
    hostname: primary
    ports:
      - "7474:7474"   # HTTP API
      - "7687:7687"   # Bolt protocol
    volumes:
      - primary-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: ha_standby
      NORNICDB_CLUSTER_NODE_ID: primary-1
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_HA_ROLE: primary
      NORNICDB_CLUSTER_HA_PEER_ADDR: standby:7000
      NORNICDB_CLUSTER_HA_SYNC_MODE: async
      NORNICDB_CLUSTER_HA_HEARTBEAT_MS: 1000
      NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT: 30s
      NORNICDB_CLUSTER_HA_AUTO_FAILOVER: "true"
    networks:
      - nornicdb-cluster
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  nornicdb-standby:
    image: nornicdb:latest
    container_name: nornicdb-standby
    hostname: standby
    ports:
      - "7475:7474"   # HTTP API on a different host port
      - "7688:7687"   # Bolt on a different host port
    volumes:
      - standby-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: ha_standby
      NORNICDB_CLUSTER_NODE_ID: standby-1
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_HA_ROLE: standby
      NORNICDB_CLUSTER_HA_PEER_ADDR: primary:7000
      NORNICDB_CLUSTER_HA_SYNC_MODE: async
      NORNICDB_CLUSTER_HA_HEARTBEAT_MS: 1000
      NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT: 30s
      NORNICDB_CLUSTER_HA_AUTO_FAILOVER: "true"
    networks:
      - nornicdb-cluster
    depends_on:
      - nornicdb-primary
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  primary-data:
  standby-data:

networks:
  nornicdb-cluster:
    driver: bridge
```

### Start / Stop

```bash
# Start the HA cluster
docker compose -f docker-compose.ha.yml up -d

# Check container status
docker compose -f docker-compose.ha.yml ps

# View logs
docker compose -f docker-compose.ha.yml logs -f
```

### Write Acknowledgment

| Mode        | Description                 | Latency | Data Safety       |
| ----------- | --------------------------- | ------- | ----------------- |
| `async`     | Acknowledge immediately     | Lowest  | Risk of data loss |
| `quorum`    | Wait for standby to apply   | Highest | Strongest         |

`NORNICDB_CLUSTER_HA_SYNC_MODE` controls this behavior. Default is `async`.

### Local Dev (Two Processes)

Primary:
```bash
NORNICDB_CLUSTER_HA_SYNC_MODE=async go run ./cmd/nornicdb serve \
  --data-dir /tmp/nornicdb-node1 \
  --http-port 7474 --bolt-port 7687 \
  --cluster-mode ha_standby \
  --cluster-node-id node1 \
  --cluster-bind-addr 127.0.0.1:9001 \
  --cluster-advertise-addr 127.0.0.1:9001 \
  --cluster-ha-role primary \
  --cluster-ha-peer-addr 127.0.0.1:9002
```

Standby:
```bash
NORNICDB_CLUSTER_HA_SYNC_MODE=async go run ./cmd/nornicdb serve \
  --data-dir /tmp/nornicdb-node2 \
  --http-port 7475 --bolt-port 7688 \
  --cluster-mode ha_standby \
  --cluster-node-id node2 \
  --cluster-bind-addr 127.0.0.1:9002 \
  --cluster-advertise-addr 127.0.0.1:9002 \
  --cluster-ha-role standby \
  --cluster-ha-peer-addr 127.0.0.1:9001
```

### Failover

When `NORNICDB_CLUSTER_HA_AUTO_FAILOVER=true` (the default), the standby promotes itself automatically once the primary stops sending heartbeats for `NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT`. The promotion happens inside the running standby process — no admin endpoint or external action is required.

For a manual restart-driven failover, stop the standby, change `NORNICDB_CLUSTER_HA_ROLE=primary`, and restart it; clients then need to be repointed to the new primary's address.

---

## Mode 2: Raft Cluster (3+ Nodes)

Raft provides strong consistency with automatic leader election. Data is replicated to **all** nodes (leader + followers), so every node has a full copy of the data.

### Reads vs writes

| Operation | Any node? | Notes |
| --------- | --------- | ----- |
| **Query / search / read** | Yes | All nodes have the same data; you can send reads to any node (e.g. via a load balancer). |
| **Write** | Yes | If the request hits a follower, the server **automatically forwards** it to the current Raft leader and returns the leader's response. No client-side leader routing is required. |

### Configuration Overview

| Variable                          | Node 1    | Node 2    | Node 3    | Description       |
| --------------------------------- | --------- | --------- | --------- | ----------------- |
| `NORNICDB_CLUSTER_MODE`           | `raft`    | `raft`    | `raft`    | Replication mode  |
| `NORNICDB_CLUSTER_NODE_ID`        | `node-1`  | `node-2`  | `node-3`  | Unique node ID    |
| `NORNICDB_CLUSTER_RAFT_BOOTSTRAP` | `true`    | `false`   | `false`   | Bootstrap cluster |
| `NORNICDB_CLUSTER_RAFT_PEERS`     | See below | See below | See below | Peer addresses    |

### Docker Compose Setup

Create a `docker-compose.raft.yml`:

```yaml
version: "3.8"

services:
  nornicdb-node1:
    image: nornicdb:latest
    container_name: nornicdb-node1
    hostname: node1
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - node1-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: raft
      NORNICDB_CLUSTER_NODE_ID: node-1
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: node1:7000
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: my-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "true"
      NORNICDB_CLUSTER_RAFT_PEERS: "node-2:node2:7000,node-3:node3:7000"
      NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT: 1s
      NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT: 100ms
      NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL: 300
      NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD: 10000
    networks:
      - nornicdb-cluster
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  nornicdb-node2:
    image: nornicdb:latest
    container_name: nornicdb-node2
    hostname: node2
    ports:
      - "7475:7474"
      - "7688:7687"
    volumes:
      - node2-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: raft
      NORNICDB_CLUSTER_NODE_ID: node-2
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: node2:7000
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: my-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "false"
      NORNICDB_CLUSTER_RAFT_PEERS: "node-1:node1:7000,node-3:node3:7000"
      NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT: 1s
      NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT: 100ms
    networks:
      - nornicdb-cluster
    depends_on:
      - nornicdb-node1

  nornicdb-node3:
    image: nornicdb:latest
    container_name: nornicdb-node3
    hostname: node3
    ports:
      - "7476:7474"
      - "7689:7687"
    volumes:
      - node3-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: raft
      NORNICDB_CLUSTER_NODE_ID: node-3
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: node3:7000
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: my-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "false"
      NORNICDB_CLUSTER_RAFT_PEERS: "node-1:node1:7000,node-2:node2:7000"
      NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT: 1s
      NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT: 100ms
    networks:
      - nornicdb-cluster
    depends_on:
      - nornicdb-node1

volumes:
  node1-data:
  node2-data:
  node3-data:

networks:
  nornicdb-cluster:
    driver: bridge
```

### Starting the Cluster

```bash
docker compose -f docker-compose.raft.yml up -d

# Wait for leader election
sleep 5

# Verify each node responds
for port in 7474 7475 7476; do
  curl -s http://localhost:$port/health
done
```

### Raft Tuning Parameters

| Parameter                                     | Default | Description                   |
| --------------------------------------------- | ------- | ----------------------------- |
| `NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT`      | `1s`    | Time before starting election |
| `NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT`     | `100ms` | Leader heartbeat interval     |
| `NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL`     | `300`   | Seconds between snapshots     |
| `NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD`    | `10000` | Log entries before snapshot   |

### Failover

Raft handles failover automatically through leader election:

1. When the leader fails, followers detect missing heartbeats.
2. After `NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT`, a follower starts an election.
3. The node with the most up-to-date log wins the election.
4. Drivers configured with multi-host routing automatically rediscover the new leader on the next request.

---

## Mode 3: Multi-Region

Multi-Region deployment combines Raft clusters within each region with asynchronous replication between regions.

### Configuration Overview

| Variable                          | US-East                       | EU-West                       | Description       |
| --------------------------------- | ----------------------------- | ----------------------------- | ----------------- |
| `NORNICDB_CLUSTER_MODE`           | `multi_region`                | `multi_region`                | Replication mode  |
| `NORNICDB_CLUSTER_REGION_ID`      | `us-east`                     | `eu-west`                     | Region identifier |
| `NORNICDB_CLUSTER_REMOTE_REGIONS` | `eu-west:eu-coordinator:7000` | `us-east:us-coordinator:7000` | Remote regions    |

### Docker Compose Setup (US-East Region)

Create `docker-compose.us-east.yml`:

```yaml
version: "3.8"

services:
  us-east-node1:
    image: nornicdb:latest
    container_name: us-east-node1
    hostname: us-east-node1
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - us-east-node1-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: multi_region
      NORNICDB_CLUSTER_NODE_ID: us-east-node-1
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: us-east-node1:7000
      NORNICDB_CLUSTER_REGION_ID: us-east
      NORNICDB_CLUSTER_REMOTE_REGIONS: "eu-west:eu-west-node1:7000"
      NORNICDB_CLUSTER_CROSS_REGION_MODE: async
      NORNICDB_CLUSTER_CONFLICT_STRATEGY: last_write_wins
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: us-east-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "true"
      NORNICDB_CLUSTER_RAFT_PEERS: "us-east-node-2:us-east-node2:7000,us-east-node-3:us-east-node3:7000"
    networks:
      - nornicdb-global

  us-east-node2:
    image: nornicdb:latest
    container_name: us-east-node2
    hostname: us-east-node2
    ports:
      - "7475:7474"
      - "7688:7687"
    volumes:
      - us-east-node2-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: multi_region
      NORNICDB_CLUSTER_NODE_ID: us-east-node-2
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: us-east-node2:7000
      NORNICDB_CLUSTER_REGION_ID: us-east
      NORNICDB_CLUSTER_REMOTE_REGIONS: "eu-west:eu-west-node1:7000"
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: us-east-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "false"
      NORNICDB_CLUSTER_RAFT_PEERS: "us-east-node-1:us-east-node1:7000,us-east-node-3:us-east-node3:7000"
    networks:
      - nornicdb-global
    depends_on:
      - us-east-node1

  us-east-node3:
    image: nornicdb:latest
    container_name: us-east-node3
    hostname: us-east-node3
    ports:
      - "7476:7474"
      - "7689:7687"
    volumes:
      - us-east-node3-data:/data
    environment:
      NORNICDB_CLUSTER_MODE: multi_region
      NORNICDB_CLUSTER_NODE_ID: us-east-node-3
      NORNICDB_CLUSTER_BIND_ADDR: 0.0.0.0:7000
      NORNICDB_CLUSTER_ADVERTISE_ADDR: us-east-node3:7000
      NORNICDB_CLUSTER_REGION_ID: us-east
      NORNICDB_CLUSTER_REMOTE_REGIONS: "eu-west:eu-west-node1:7000"
      NORNICDB_CLUSTER_RAFT_CLUSTER_ID: us-east-cluster
      NORNICDB_CLUSTER_RAFT_BOOTSTRAP: "false"
      NORNICDB_CLUSTER_RAFT_PEERS: "us-east-node-1:us-east-node1:7000,us-east-node-2:us-east-node2:7000"
    networks:
      - nornicdb-global
    depends_on:
      - us-east-node1

volumes:
  us-east-node1-data:
  us-east-node2-data:
  us-east-node3-data:

networks:
  nornicdb-global:
    driver: bridge
```

### Cross-Region Replication Modes

| Mode        | Description                    | Latency | Consistency |
| ----------- | ------------------------------ | ------- | ----------- |
| `async`     | Fire-and-forget replication    | Lowest  | Eventual    |
| `quorum`    | Wait for remote acknowledgment | Higher  | Stronger    |

### Conflict Resolution Strategies

| Strategy          | Description               | Use Case          |
| ----------------- | ------------------------- | ----------------- |
| `last_write_wins` | Latest timestamp wins     | Most applications |
| `manual`          | Require manual resolution | Financial data    |

---

## Client Connection

### Hot Standby

```javascript
// Node.js with neo4j-driver
const neo4j = require("neo4j-driver");

// Connect to primary for writes
const primaryDriver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("admin", "admin")
);

// For high availability, give the driver multiple hosts
const haDriver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("admin", "admin"),
  {
    resolver: (address) => [
      "localhost:7687", // Primary host port
      "localhost:7688", // Standby host port
    ],
  }
);
```

### Raft Cluster

```javascript
const neo4j = require("neo4j-driver");

const driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("admin", "admin"),
  {
    resolver: (address) => [
      "localhost:7687", // Node 1
      "localhost:7688", // Node 2
      "localhost:7689", // Node 3
    ],
  }
);

const session = driver.session({ defaultAccessMode: neo4j.session.WRITE });
await session.run("CREATE (n:Person {name: $name})", { name: "Alice" });

const readSession = driver.session({ defaultAccessMode: neo4j.session.READ });
const result = await readSession.run("MATCH (n:Person) RETURN n");
```

### Multi-Region

```javascript
// Connect to nearest region
const usEastDriver = neo4j.driver(
  "bolt://us-east-node1:7687",
  neo4j.auth.basic("admin", "admin"),
  {
    resolver: (address) => [
      "us-east-node1:7687",
      "us-east-node2:7687",
      "us-east-node3:7687",
    ],
  }
);

// For cross-region failover, list both regions
const globalDriver = neo4j.driver(
  "bolt://us-east-node1:7687",
  neo4j.auth.basic("admin", "admin"),
  {
    resolver: (address) => [
      // Primary region
      "us-east-node1:7687",
      "us-east-node2:7687",
      "us-east-node3:7687",
      // Failover region
      "eu-west-node1:7687",
      "eu-west-node2:7687",
      "eu-west-node3:7687",
    ],
  }
);
```

### Python Connection

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("admin", "admin"),
    resolver=lambda _: [
        ("localhost", 7687),
        ("localhost", 7688),
        ("localhost", 7689),
    ],
)

with driver.session() as session:
    result = session.run("MATCH (n) RETURN count(n)")
    print(result.single()[0])
```

---

## Monitoring

### Health Endpoint

```bash
curl http://localhost:7474/health
```

Returns `{"status": "healthy"}` when the server is up. Use container logs (`docker logs nornicdb-node1`) for cluster-state diagnostics.

### Docker Health Checks

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
```

---

## Configuration Reference

### Environment Variables

| Variable                          | Default              | Description                         |
| --------------------------------- | -------------------- | ----------------------------------- |
| `NORNICDB_CLUSTER_MODE`           | `standalone`         | Replication mode                    |
| `NORNICDB_CLUSTER_NODE_ID`        | auto-generated       | Unique node identifier              |
| `NORNICDB_CLUSTER_BIND_ADDR`      | `0.0.0.0:7000`       | Address to bind for cluster traffic |
| `NORNICDB_CLUSTER_ADVERTISE_ADDR` | same as bind         | Address advertised to peers         |
| `NORNICDB_CLUSTER_DATA_DIR`       | `./data/replication` | Directory for replication state     |

The cluster transport listens on its own port (default `7000`) — separate from the Bolt port (`7687`) and the HTTP port (`7474`).

#### Hot Standby

| Variable                               | Default | Description                             |
| -------------------------------------- | ------- | --------------------------------------- |
| `NORNICDB_CLUSTER_HA_ROLE`             | -       | `primary` or `standby` (required)       |
| `NORNICDB_CLUSTER_HA_PEER_ADDR`        | -       | Address of peer node (required)         |
| `NORNICDB_CLUSTER_HA_SYNC_MODE`        | `async` | Write ack mode: `async`, `quorum`       |
| `NORNICDB_CLUSTER_HA_HEARTBEAT_MS`     | `1000`  | Heartbeat interval in ms                |
| `NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT` | `30s`   | Time before failover                    |
| `NORNICDB_CLUSTER_HA_AUTO_FAILOVER`    | `true`  | Enable automatic failover               |

#### Raft

| Variable                                   | Default    | Description               |
| ------------------------------------------ | ---------- | ------------------------- |
| `NORNICDB_CLUSTER_RAFT_CLUSTER_ID`         | `nornicdb` | Cluster identifier        |
| `NORNICDB_CLUSTER_RAFT_BOOTSTRAP`          | `false`    | Bootstrap new cluster     |
| `NORNICDB_CLUSTER_RAFT_PEERS`              | -          | Comma-separated peers     |
| `NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT`   | `1s`       | Election timeout          |
| `NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT`  | `100ms`    | Heartbeat timeout         |
| `NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL`  | `300`      | Seconds between snapshots |
| `NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD` | `10000`    | Entries before snapshot   |

#### Multi-Region

| Variable                             | Default           | Description                  |
| ------------------------------------ | ----------------- | ---------------------------- |
| `NORNICDB_CLUSTER_REGION_ID`         | -                 | Region identifier (required) |
| `NORNICDB_CLUSTER_REMOTE_REGIONS`    | -                 | Remote region addresses      |
| `NORNICDB_CLUSTER_CROSS_REGION_MODE` | `async`           | Cross-region sync mode: `async`, `quorum` |
| `NORNICDB_CLUSTER_CONFLICT_STRATEGY` | `last_write_wins` | Conflict resolution          |

### Securing Replication Traffic

The cluster port carries WAL batches, Raft votes and appends, fencing and forwarded writes. Protect it with a shared secret and TLS on every node:

| Variable                                    | Default  | Description |
| ------------------------------------------- | -------- | ----------- |
| `NORNICDB_CLUSTER_REPLICATION_SECRET`       | -        | Shared secret, at least 32 characters, identical on every node. Every cluster message is signed with it, and messages without a valid signature are refused. |
| `NORNICDB_CLUSTER_TLS_ENABLED`              | `false`  | TLS on the cluster port and on connections to peers. Needs `NORNICDB_CLUSTER_TLS_CERT_FILE` and `NORNICDB_CLUSTER_TLS_KEY_FILE`. |
| `NORNICDB_CLUSTER_TLS_CA_FILE`              | -        | CA that peer certificates must chain to. |
| `NORNICDB_CLUSTER_TLS_VERIFY_CLIENT`        | `true`   | Peers must present a client certificate from the CA (mutual TLS). Needs `NORNICDB_CLUSTER_TLS_CA_FILE`. |
| `NORNICDB_CLUSTER_TLS_INSECURE_SKIP_VERIFY` | `false`  | Skip verification of peer server certificates. Testing only. |

Boolean settings accept `true`/`1`/`yes`/`on` and `false`/`0`/`no`/`off`. Any other value, or a certificate, key or CA that can't be read, stops replication from starting rather than letting it run unprotected.

**Replay protection.** When the replication secret is configured, every message must carry a fresh 16-byte nonce and `SignatureV2`, covering the message type, sender, timestamp, nonce and payload. Replay protection is always enabled for signed traffic. Old-format (v1 or nonce-less) messages are rejected. Each receiver remembers a sender's nonce until the message leaves the 30-second timestamp window; a replay within that window is rejected. The cache is per receiving node and starts empty after a restart.

**Cluster upgrade requirement.** Upgrade every node in a cluster before resuming replication with this version. The cluster wire format is now MessagePack; older nodes use gob. Nodes running this version also reject old-format signed messages. Plan a coordinated cluster upgrade with an interruption to replication; a mixed-version rolling upgrade does not keep the cluster operational.

During a mixed-version window, peers cannot decode each other's cluster messages and close the connection. Heartbeats, WAL replication, fencing and promotion between old and upgraded nodes fail; standbys stop catching up, Raft peers can lose quorum, and forwarded writes across the version boundary fail. Resume normal cluster service only after every node has been upgraded.

---

## Best Practices

### Network Configuration

1. **Use a dedicated network for cluster traffic.** Separate user traffic from replication.
2. **Low latency between nodes.** Aim for < 1 ms for Raft, < 100 ms for HA standby.
3. **Reliable network.** Packet loss causes leader elections.

### Capacity Planning

| Mode         | Min Nodes | Recommended | Max Practical |
| ------------ | --------- | ----------- | ------------- |
| Hot Standby  | 2         | 2           | 2             |
| Raft         | 3         | 3-5         | 7             |
| Multi-Region | 6         | 9+          | No limit      |

### Monitoring

1. **Watch container logs** during failover testing — cluster state changes are logged at INFO level.
2. **Use Docker health checks** against `/health` so the orchestrator restarts unresponsive nodes.
3. **Test failover regularly** in non-production environments.

---

## Technical Deep Dive

This section explains the internal architecture of NornicDB clustering for users who want to understand how the system works.

### Network Architecture

NornicDB uses two separate network protocols:

```
┌─────────────────────────────────────────────────────────────┐
│                     NornicDB Node                           │
├─────────────────────────────┬───────────────────────────────┤
│      Client Traffic         │      Cluster Traffic          │
│   ┌─────────────────────┐   │   ┌─────────────────────────┐ │
│   │  Bolt Protocol      │   │   │  Cluster Protocol       │ │
│   │  (Port 7687)        │   │   │  (Port 7000)            │ │
│   ├─────────────────────┤   │   ├─────────────────────────┤ │
│   │ • Cypher queries    │   │   │ • Raft consensus        │ │
│   │ • Result streaming  │   │   │   - RequestVote RPC     │ │
│   │ • Transactions      │   │   │   - AppendEntries RPC   │ │
│   │ • Neo4j compatible  │   │   │ • WAL streaming (HA)    │ │
│   └─────────────────────┘   │   │ • Heartbeats            │ │
│                             │   │ • Failover coordination │ │
│                             │   └─────────────────────────┘ │
└─────────────────────────────┴───────────────────────────────┘
```

**Port 7687 (Bolt Protocol):**

- Standard Neo4j Bolt protocol for client connections.
- Used by all Neo4j drivers (Python, Java, Go, JavaScript, etc.).
- Handles Cypher query execution and result streaming.
- In a cluster, writes sent to a follower are automatically forwarded to the leader over the cluster protocol.

**Port 7000 (Cluster Protocol):**

- Binary protocol optimized for low-latency cluster communication.
- Length-prefixed JSON messages over TCP.
- Handles all cluster coordination and replication.
- Should be firewalled from external access.

### Message Flow Diagrams

#### Hot Standby Write Flow

```
Client                  Primary                 Standby
  │                        │                        │
  │─── WRITE (Bolt) ───────►                        │
  │                        │                        │
  │                        │── WALBatch ───────────►│
  │                        │                        │
  │                        │◄─ WALBatchResponse ────│
  │                        │                        │
  │◄── SUCCESS ────────────│                        │
```

#### Raft Consensus Write Flow

```
Client                  Leader               Follower 1         Follower 2
  │                        │                     │                   │
  │── WRITE (Bolt) ────────►                     │                   │
  │                        │── AppendEntries ───►│                   │
  │                        │── AppendEntries ────┼──────────────────►│
  │                        │◄── Success ─────────│                   │
  │                        │◄── Success ──────────────────────────────│
  │                        │ (quorum reached)    │                   │
  │◄── SUCCESS ────────────│                     │                   │
```

### Wire Protocol

The cluster protocol uses length-prefixed MessagePack messages:

```
┌─────────────┬─────────────────────────────────────┐
│ Length (4B) │       MessagePack Payload           │
│ Big Endian  │                                     │
└─────────────┴─────────────────────────────────────┘
```

### Configuration Parameter Details

#### Election Timeout (`NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT`)

Time a follower waits without hearing from the leader before starting an election.

```
Recommended values:
  - Same datacenter: 150ms - 500ms
  - Same region:     500ms - 2s
  - Cross-region:    2s - 10s (higher than network RTT)

Formula: election_timeout > 2 × heartbeat_timeout + max_network_RTT
```

**Too low:** Frequent unnecessary elections, cluster instability.
**Too high:** Slow failure detection, longer downtime during failover.

#### Heartbeat Timeout (`NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT`)

How often the leader sends heartbeats to maintain authority.

```
Recommended values:
  - Same datacenter: 50ms - 100ms
  - Same region:     100ms - 500ms
  - Cross-region:    500ms - 2s

Formula: heartbeat_timeout < election_timeout / 2
```

#### Write Ack Mode (`NORNICDB_CLUSTER_HA_SYNC_MODE`)

| Mode        | Acknowledgment    | Data Safety       | Latency |
| ----------- | ----------------- | ----------------- | ------- |
| `async`     | Primary only      | Risk of data loss | Lowest  |
| `quorum`    | Standby applied   | Strongest         | Highest |

Default is `async` for lowest write latency; opt into `quorum` only when you need replication-acknowledged writes.

### Connection Pool Configuration

For applications connecting to clusters, configure your driver's connection pool:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://node1:7687",
    auth=("admin", "admin"),
    max_connection_pool_size=50,
    connection_acquisition_timeout=60,
    max_transaction_retry_time=30,
)
```

```go
driver, _ := neo4j.NewDriver(
    "bolt://node1:7687",
    neo4j.BasicAuth("admin", "admin", ""),
    func(config *neo4j.Config) {
        config.MaxConnectionPoolSize = 50
        config.ConnectionAcquisitionTimeout = time.Minute
        config.MaxTransactionRetryTime = 30 * time.Second
    },
)
```

---

## Next Steps

- [Cluster Security](../operations/cluster-security.md) — Authentication for clusters.
- [Replication Architecture](../architecture/replication.md) — Internal architecture details.
- [Clustering Roadmap](../architecture/clustering-roadmap.md) — Future sharding plans.
- [Complete Examples](./complete-examples.md) — End-to-end usage examples.
- [System Design](../architecture/system-design.md) — Overall system architecture.
