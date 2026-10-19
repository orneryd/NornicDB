# Replication Architecture

This document describes the internal architecture of NornicDB's replication system for contributors and advanced users.

> **For user documentation, see [Clustering Guide](../user-guides/clustering.md)**

## Overview

NornicDB supports three replication modes to meet different availability and consistency requirements:

| Mode | Nodes | Consistency | Use Case |
|------|-------|-------------|----------|
| **Standalone** | 1 | N/A | Development, testing, small workloads |
| **Hot Standby** | 2 | Eventual | Simple HA, fast failover |
| **Raft Cluster** | 3-5 | Strong | Production HA, consistent reads |
| **Multi-Region** | 6+ | Configurable | Global distribution, disaster recovery |

## Cluster upgrades

Upgrade every node in a cluster before resuming replication with this version.
Signed cluster messages always require a nonce and `SignatureV2`; old-format
(v1 or nonce-less) messages are rejected. During a mixed-version window, peers
reject messages and close connections: heartbeats, WAL replication, Raft consensus
and forwarded writes across the version boundary fail. Plan a coordinated upgrade
with a replication interruption. See [Securing Replication Traffic](../user-guides/clustering.md#securing-replication-traffic)
for the operator-visible errors and replay-cache behavior.

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    NORNICDB REPLICATION ARCHITECTURE                            │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  MODE 1: HOT STANDBY (2 nodes)                                                 │
│  ┌─────────────┐      WAL Stream      ┌─────────────┐                         │
│  │   Primary   │ ──────────────────►  │   Standby   │                         │
│  │  (writes)   │    (async/quorum)    │  (failover) │                         │
│  └─────────────┘                      └─────────────┘                         │
│                                                                                 │
│  MODE 2: RAFT CLUSTER (3-5 nodes)                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                        │
│  │   Leader    │◄──►│  Follower   │◄──►│  Follower   │                        │
│  │  (writes)   │    │  (reads)    │    │  (reads)    │                        │
│  └─────────────┘    └─────────────┘    └─────────────┘                        │
│         │                  │                  │                                │
│         └──────────────────┴──────────────────┘                                │
│                    Raft Consensus                                              │
│                                                                                 │
│  MODE 3: MULTI-REGION (Raft clusters + cross-region HA)                        │
│  ┌─────────────────────────┐      ┌─────────────────────────┐                 │
│  │      US-EAST REGION     │      │      EU-WEST REGION     │                 │
│  │  ┌───┐ ┌───┐ ┌───┐     │      │     ┌───┐ ┌───┐ ┌───┐  │                 │
│  │  │ L │ │ F │ │ F │     │ WAL  │     │ L │ │ F │ │ F │  │                 │
│  │  └───┘ └───┘ └───┘     │◄────►│     └───┘ └───┘ └───┘  │                 │
│  │     Raft Cluster A      │async │      Raft Cluster B    │                 │
│  └─────────────────────────┘      └─────────────────────────┘                 │
│                                                                                 │
└────────────────────────────────────────────────────────────────────────────────┘
```

## Package Structure

```
pkg/replication/
├── config.go           # Configuration loading and validation
├── replicator.go       # Core Replicator interface and factory
├── transport.go        # ClusterTransport over TCP (gob-framed messages)
├── ha_standby.go       # Hot Standby implementation
├── raft.go             # Raft consensus implementation
├── multi_region.go     # Multi-region coordinator
├── codec.go            # Gob codecs (node/edge payloads, helpers)
├── handlers.go         # Cluster message dispatch/handler registration
├── chaos_test.go       # Chaos testing infrastructure
├── scenario_test.go    # E2E scenario tests
└── replication_test.go # Unit tests
```

## Core Interfaces

### Replicator Interface

All replication modes implement this interface:

```go
type Replicator interface {
    // Start starts the replicator
    Start(ctx context.Context) error
    
    // Apply applies a write operation (routes to leader if needed)
    Apply(cmd *Command, timeout time.Duration) error
    
    // IsLeader returns true if this node can accept writes
    IsLeader() bool
    
    // LeaderAddr returns the address of the current leader
    LeaderAddr() string
    
    // LeaderID returns the ID of the current leader
    LeaderID() string
    
    // Health returns health status
    Health() *HealthStatus
    
    // WaitForLeader blocks until a leader is elected
    WaitForLeader(timeout time.Duration) error
    
    // Mode returns the replication mode
    Mode() ReplicationMode
    
    // NodeID returns this node's ID
    NodeID() string
    
    // Shutdown gracefully shuts down
    Shutdown() error
}
```

## Current Implementation Notes

- **Transport**: `pkg/replication/transport.go` uses a plain TCP listener with framed **gob** messages for cluster RPCs (heartbeat, WAL batch, Raft messages).
- **Payloads**: `pkg/replication/codec.go` defines replication-safe payloads for nodes/edges. Node payloads include embeddings (`NamedEmbeddings`, `ChunkEmbeddings`) so embeddings replicate in HA mode.
- **Write Path**: the base storage engine is wrapped by `pkg/replication/replicated_engine.go`, which converts writes into `replication.Command` and routes them through the active `Replicator`.
- **WAL Streaming**: `pkg/replication/storage_adapter.go` maintains a persistent WAL (restart/recovery) plus an in-memory WAL slice to avoid repeated full-file scans during streaming, and uses an async WAL write queue for lower per-write latency.

### Transport Interface

Node-to-node communication:

```go
type Transport interface {
    // Connect establishes a connection to a peer
    Connect(ctx context.Context, addr string) (PeerConnection, error)
    
    // Listen accepts incoming connections
    Listen(ctx context.Context, addr string, handler ConnectionHandler) error
    
    // Close shuts down the transport
    Close() error
}

type PeerConnection interface {
    // WAL streaming (Hot Standby)
    SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error)
    SendHeartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error)
    SendFence(ctx context.Context, req *FenceRequest) (*FenceResponse, error)
    SendPromote(ctx context.Context, req *PromoteRequest) (*PromoteResponse, error)
    
    // Raft consensus
    SendRaftVote(ctx context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error)
    SendRaftAppendEntries(ctx context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error)
    
    Close() error
    IsConnected() bool
}
```

### Storage Interface

Replication layer's view of storage:

```go
type Storage interface {
    // Commands
    ApplyCommand(cmd *Command) error
    
    // WAL position tracking
    GetWALPosition() (uint64, error)
    SetWALPosition(pos uint64) error
    
    // Node/Edge operations (used by WAL applier)
    CreateNode(node *Node) error
    UpdateNode(node *Node) error
    DeleteNode(id NodeID) error
    CreateEdge(edge *Edge) error
    DeleteEdge(from, to NodeID, relType string) error
    SetProperty(nodeID NodeID, key string, value interface{}) error
}
```

## Network Protocol

### Port Allocation

| Port | Protocol | Purpose |
|------|----------|---------|
| 7474 | HTTP | REST API, Admin, Health checks |
| 7687 | Bolt | Neo4j-compatible client queries |
| 7000 | Cluster | Replication, Raft consensus (default; configurable via `NORNICDB_CLUSTER_BIND_ADDR`) |

### Wire Format

The cluster protocol uses length-prefixed `gob`-encoded payloads over TCP. Encoder/decoder live in `pkg/replication/codec.go`.

```
┌─────────────┬─────────────────────────────────────┐
│ Length (4B) │           gob Payload               │
│ Big Endian  │                                     │
└─────────────┴─────────────────────────────────────┘
```

### Message Types

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| VoteRequest | 1 | Candidate → Follower | Request vote in election |
| VoteResponse | 2 | Follower → Candidate | Grant/deny vote |
| AppendEntries | 3 | Leader → Follower | Replicate log entries |
| AppendEntriesResponse | 4 | Follower → Leader | Acknowledge entries |
| WALBatch | 5 | Primary → Standby | Stream WAL entries |
| WALBatchResponse | 6 | Standby → Primary | Acknowledge WAL |
| Heartbeat | 7 | Primary → Standby | Health check |
| HeartbeatResponse | 8 | Standby → Primary | Health status |
| Fence | 9 | Standby → Primary | Fence old primary |
| FenceResponse | 10 | Primary → Standby | Acknowledge fence |
| Promote | 11 | Admin → Standby | Promote to primary |
| PromoteResponse | 12 | Standby → Admin | Promotion status |

## Mode 1: Hot Standby

### Components

- **Primary**: Accepts writes, streams WAL to standby
- **Standby**: Receives WAL, ready for failover
- **WALStreamer**: Manages WAL position and batching
- **WALApplier**: Applies WAL entries to storage

### Write Flow

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

### Sync Modes

| Mode | Acknowledgment | Data Safety | Latency |
|------|----------------|-------------|---------|
| `async` | Primary only | Risk of data loss | Lowest |
| `quorum` | Standby applied | Strongest | Highest |

### Failover Process

1. Standby detects missing heartbeats
2. After `FAILOVER_TIMEOUT`, standby attempts to fence primary
3. Standby promotes itself to primary
4. Clients reconnect to new primary

## Mode 2: Raft Consensus

### Components

- **RaftReplicator**: Main Raft node implementation
- **Election Timer**: Triggers leader election on timeout
- **Log**: In-memory Raft log with commit tracking
- **Heartbeat Loop**: Leader sends heartbeats to maintain authority

### State Machine

```
                 ┌────────────┐
                 │  Follower  │◄────────────────┐
                 └─────┬──────┘                 │
                       │ election timeout       │
                       ▼                        │
                 ┌────────────┐                 │
          ┌─────►│ Candidate  │─────────────────┤
          │      └─────┬──────┘ loses election  │
          │            │                        │
          │            │ wins election          │
          │            ▼                        │
          │      ┌────────────┐                 │
          │      │   Leader   │─────────────────┘
          │      └─────┬──────┘ discovers higher term
          │            │
          └────────────┘
           starts new election
```

### Leader Election

1. Follower's election timer expires
2. Increments term, transitions to Candidate
3. Votes for self, requests votes from peers
4. If majority votes received → becomes Leader
5. Sends heartbeats to maintain leadership

### Log Replication

1. Client sends write to Leader
2. Leader appends entry to log
3. Leader sends AppendEntries to all followers
4. When majority acknowledge → entry committed
5. Leader applies to state machine, responds to client

### Raft RPC Messages

**RequestVote:**
```go
type RaftVoteRequest struct {
    Term         uint64
    CandidateID  string
    LastLogIndex uint64
    LastLogTerm  uint64
}

type RaftVoteResponse struct {
    Term        uint64
    VoteGranted bool
    VoterID     string
}
```

**AppendEntries:**
```go
type RaftAppendEntriesRequest struct {
    Term         uint64
    LeaderID     string
    LeaderAddr   string
    PrevLogIndex uint64
    PrevLogTerm  uint64
    Entries      []*RaftLogEntry
    LeaderCommit uint64
}

type RaftAppendEntriesResponse struct {
    Term          uint64
    Success       bool
    MatchIndex    uint64
    ConflictIndex uint64
    ConflictTerm  uint64
}
```

## Mode 3: Multi-Region

### Components

- **MultiRegionReplicator**: Coordinates local Raft + cross-region
- **Local Raft Cluster**: Strong consistency within region
- **Cross-Region Streamer**: Async WAL replication between regions

### Write Flow

1. Write arrives at region's Raft leader
2. Raft commits locally (strong consistency)
3. Async replication to remote regions
4. Remote regions apply WAL entries

### Conflict Resolution

When async replication causes conflicts:

| Strategy | Description |
|----------|-------------|
| `last_write_wins` | Latest timestamp wins |
| `first_write_wins` | Earliest timestamp wins |
| `manual` | Flag for manual resolution |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_CLUSTER_MODE` | `standalone` | `standalone`, `ha_standby`, `raft`, `multi_region` |
| `NORNICDB_CLUSTER_NODE_ID` | auto | Unique node identifier |
| `NORNICDB_CLUSTER_BIND_ADDR` | `0.0.0.0:7000` | Cluster port binding |
| `NORNICDB_CLUSTER_ADVERTISE_ADDR` | same as bind | Address advertised to peers |

See [Clustering Guide](../user-guides/clustering.md) for complete configuration reference.

## Testing

### Test Categories

| File | Purpose |
|------|---------|
| `replication_test.go` | Unit tests for each component |
| `scenario_test.go` | E2E tests for all modes (A/B/C/D scenarios) |
| `chaos_test.go` | Network failure simulation |

### Chaos Testing

The chaos testing infrastructure simulates:

- Packet loss
- High latency (2000ms+)
- Connection drops
- Data corruption
- Packet duplication
- Packet reordering
- Byzantine failures

### Running Tests

```bash
# All replication tests
go test ./pkg/replication/... -v

# Specific test
go test ./pkg/replication/... -run TestScenario_Raft -v

# With race detection
go test ./pkg/replication/... -race

# Skip long-running tests
go test ./pkg/replication/... -short
```

## Implementation Status

| Component | Status | Details |
|-----------|--------|---------|
| Hot Standby | ✅ Complete | 2-node HA with auto-failover |
| Raft Cluster | ✅ Complete | 3-5 node strong consistency |
| Multi-Region | ✅ Complete | Async cross-region replication |
| Chaos Testing | ✅ Complete | Extreme latency, packet loss, Byzantine failures |

## See Also

- [Clustering Guide](../user-guides/clustering.md) - User documentation
- [System Design](./system-design.md) - Overall architecture
- [Plugin System](./plugin-system.md) - APOC plugin architecture
