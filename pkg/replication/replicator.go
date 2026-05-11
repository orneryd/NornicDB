package replication

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Errors returned by replication operations.
var (
	// ErrNotLeader is returned when a write is attempted on a non-leader node.
	ErrNotLeader = errors.New("not leader")

	// ErrNoLeader is returned when no leader is available in the cluster.
	ErrNoLeader = errors.New("no leader available")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("operation timed out")

	// ErrClosed is returned when operating on a closed replicator.
	ErrClosed = errors.New("replicator is closed")

	// ErrStandbyMode is returned when writes are attempted on a standby node.
	ErrStandbyMode = errors.New("node is in standby mode")

	// ErrNotReady is returned when the replicator hasn't finished initialization.
	ErrNotReady = errors.New("replicator not ready")
)

// Replicator is the unified interface for all replication modes.
// It abstracts the complexity of different replication strategies behind
// a simple interface that the rest of NornicDB uses.
//
// The Replicator handles:
//   - Write routing (to leader)
//   - Read routing (based on consistency level)
//   - Health monitoring
//   - Failover orchestration
//
// Example:
//
//	replicator, _ := replication.NewReplicator(config, storage)
//	replicator.Start(ctx)
//	defer replicator.Shutdown()
//
//	// All writes go through replicator
//	if err := replicator.Apply(cmd); err != nil {
//	    if errors.Is(err, replication.ErrNotLeader) {
//	        // Forward to leader
//	    }
//	}
type Replicator interface {
	// Start initializes and starts the replicator.
	// This should be called after the storage engine is ready.
	Start(ctx context.Context) error

	// Apply applies a write command to the cluster.
	// Returns ErrNotLeader if this node cannot accept writes.
	// The command is replicated according to the replication mode.
	Apply(cmd *Command, timeout time.Duration) error

	// ApplyBatch applies multiple commands atomically.
	ApplyBatch(cmds []*Command, timeout time.Duration) error

	// IsLeader returns true if this node can accept writes.
	// For standalone mode, this always returns true.
	// For HA standby, this returns true only on primary.
	// For Raft, this returns true only on the Raft leader.
	IsLeader() bool

	// LeaderAddr returns the address of the current leader.
	// Returns empty string if unknown or in standalone mode.
	LeaderAddr() string

	// LeaderID returns the ID of the current leader.
	LeaderID() string

	// Health returns the current health status of the replicator.
	Health() *HealthStatus

	// WaitForLeader blocks until a leader is elected or context is cancelled.
	WaitForLeader(ctx context.Context) error

	// Shutdown gracefully shuts down the replicator.
	// This should be called before closing the storage engine.
	Shutdown() error

	// Mode returns the current replication mode.
	Mode() ReplicationMode

	// NodeID returns this node's unique identifier.
	NodeID() string
}

// Command represents a write operation to be replicated.
type Command struct {
	// Type identifies the operation type.
	Type CommandType

	// Data is the serialized operation data.
	Data []byte

	// Timestamp when the command was created.
	Timestamp time.Time

	// RequestID for idempotency/deduplication.
	RequestID string
}

// CommandType identifies the type of write operation.
type CommandType uint8

const (
	// CmdUnknown is an unknown command type.
	CmdUnknown CommandType = iota

	// CmdCreateNode creates a new node.
	CmdCreateNode

	// CmdUpdateNode updates an existing node.
	CmdUpdateNode

	// CmdDeleteNode deletes a node.
	CmdDeleteNode

	// CmdCreateEdge creates a new edge.
	CmdCreateEdge

	// CmdDeleteEdge deletes an edge.
	CmdDeleteEdge

	// CmdUpdateEdge updates an existing edge.
	CmdUpdateEdge

	// CmdSetProperty sets a property on a node.
	CmdSetProperty

	// CmdBatchWrite is a batch of multiple writes.
	CmdBatchWrite

	// CmdCypher is a Cypher write query.
	CmdCypher

	// CmdVoteRequest is a Raft vote request.
	CmdVoteRequest

	// CmdVoteResponse is a Raft vote response.
	CmdVoteResponse

	// CmdAppendEntries is a Raft append entries request.
	CmdAppendEntries

	// CmdAppendEntriesResponse is a Raft append entries response.
	CmdAppendEntriesResponse

	// CmdDeleteByPrefix deletes all nodes/edges under an ID prefix (e.g. database drop).
	CmdDeleteByPrefix

	// CmdBulkCreateNodes creates multiple nodes.
	CmdBulkCreateNodes

	// CmdBulkCreateEdges creates multiple edges.
	CmdBulkCreateEdges

	// CmdBulkDeleteNodes deletes multiple nodes.
	CmdBulkDeleteNodes

	// CmdBulkDeleteEdges deletes multiple edges.
	CmdBulkDeleteEdges
)

// HealthStatus represents the health state of the replicator.
type HealthStatus struct {
	// Mode is the replication mode.
	Mode ReplicationMode `json:"mode"`

	// NodeID is this node's identifier.
	NodeID string `json:"node_id"`

	// Role is the current role (leader, follower, standby, etc.).
	Role string `json:"role"`

	// IsLeader indicates if this node accepts writes.
	IsLeader bool `json:"is_leader"`

	// LeaderID is the current leader's ID (if known).
	LeaderID string `json:"leader_id,omitempty"`

	// LeaderAddr is the current leader's address.
	LeaderAddr string `json:"leader_addr,omitempty"`

	// State is the current state (ready, initializing, etc.).
	State string `json:"state"`

	// Healthy indicates overall health.
	Healthy bool `json:"healthy"`

	// ReplicationLag is the lag behind leader (for followers).
	ReplicationLag time.Duration `json:"replication_lag,omitempty"`

	// LastContact is the last successful contact with leader/peer.
	LastContact time.Time `json:"last_contact,omitempty"`

	// Peers contains status of peer nodes.
	Peers []PeerStatus `json:"peers,omitempty"`

	// Region is the region ID (for multi-region).
	Region string `json:"region,omitempty"`

	// CommitIndex is the last committed log index (for Raft).
	CommitIndex uint64 `json:"commit_index,omitempty"`

	// AppliedIndex is the last applied log index.
	AppliedIndex uint64 `json:"applied_index,omitempty"`

	// Term is the current Raft term.
	Term uint64 `json:"term,omitempty"`
}

// PeerStatus represents the status of a peer node.
type PeerStatus struct {
	// ID is the peer's identifier.
	ID string `json:"id"`

	// Address is the peer's network address.
	Address string `json:"address"`

	// Healthy indicates if the peer is reachable.
	Healthy bool `json:"healthy"`

	// Lag is the replication lag for this peer.
	Lag uint64 `json:"lag,omitempty"`

	// LastContact is the last successful contact with this peer.
	LastContact time.Time `json:"last_contact,omitempty"`

	// State is the peer's current state.
	State string `json:"state,omitempty"`
}

// Storage is the interface that the storage engine must implement
// to work with replication. This is a subset of the full storage.Engine
// interface, containing only what replication needs.
type Storage interface {
	// ApplyCommand applies a replicated command to storage.
	ApplyCommand(cmd *Command) error

	// GetWALPosition returns the current WAL position.
	GetWALPosition() (uint64, error)

	// GetWALEntries returns WAL entries starting from the given position.
	GetWALEntries(fromPosition uint64, maxEntries int) ([]*WALEntry, error)

	// WriteSnapshot writes a full snapshot to the given writer.
	WriteSnapshot(w SnapshotWriter) error

	// RestoreSnapshot restores state from a snapshot.
	RestoreSnapshot(r SnapshotReader) error
}

// WALEntry represents a write-ahead log entry.
type WALEntry struct {
	// Position is the unique, monotonically increasing position.
	Position uint64

	// Timestamp when the entry was created.
	Timestamp int64

	// Command is the replicated command.
	Command *Command
}

// SnapshotWriter is used to write snapshot data.
type SnapshotWriter interface {
	Write(p []byte) (n int, err error)
}

// SnapshotReader is used to read snapshot data.
type SnapshotReader interface {
	Read(p []byte) (n int, err error)
}

// NewReplicator creates the appropriate Replicator based on configuration.
// This is the main factory function for creating replicators.
//
// For standalone mode, this returns a no-op replicator that has zero overhead.
// For other modes, it returns the appropriate distributed replicator.
//
// Example:
//
//	config := replication.LoadFromEnv()
//	if err := config.Validate(); err != nil {
//	    log.Fatal(err)
//	}
//
//	replicator, err := replication.NewReplicator(config, storage)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewReplicator(config *Config, storage Storage) (Replicator, error) {
	switch config.Mode {
	case ModeStandalone:
		return NewStandaloneReplicator(config, storage), nil

	case ModeHAStandby:
		return NewHAStandbyReplicator(config, storage)

	case ModeRaft:
		return NewRaftReplicator(config, storage)

	case ModeMultiRegion:
		return NewMultiRegionReplicator(config, storage)

	default:
		return nil, errors.New("unknown replication mode: " + string(config.Mode))
	}
}

// StandaloneReplicator is a no-op replicator for single-node operation.
// It implements the Replicator interface with zero overhead.
// All writes are applied directly to storage without any replication.
type StandaloneReplicator struct {
	config  *Config
	storage Storage
	mu      sync.RWMutex
	started bool
	closed  bool

	// metrics is the Plan-04-06 observation seam. Standalone never
	// publishes per-peer or transition metrics (no peers, no role
	// transitions), but accepts the bag for callsite uniformity.
	metrics metricsHolder
}

// SetReplicatorMetrics implements MetricsAware. Standalone uses metrics
// only to emit an initial Role=standalone (well, follower with no peers)
// observation at Start; the bag is otherwise quiescent.
func (r *StandaloneReplicator) SetReplicatorMetrics(bag *observability.ReplicationMetrics, tracker *PeerTracker) {
	r.metrics.set(newReplicatorMetrics(bag, tracker))
}

// NewStandaloneReplicator creates a new standalone (no-op) replicator.
func NewStandaloneReplicator(config *Config, storage Storage) *StandaloneReplicator {
	return &StandaloneReplicator{
		config:  config,
		storage: storage,
	}
}

// Start initializes the standalone replicator (no-op).
func (r *StandaloneReplicator) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrClosed
	}

	r.started = true
	return nil
}

// Apply applies a command directly to storage.
func (r *StandaloneReplicator) Apply(cmd *Command, timeout time.Duration) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrClosed
	}
	if !r.started {
		r.mu.RUnlock()
		return ErrNotReady
	}
	r.mu.RUnlock()

	return r.storage.ApplyCommand(cmd)
}

// ApplyBatch applies multiple commands directly to storage.
func (r *StandaloneReplicator) ApplyBatch(cmds []*Command, timeout time.Duration) error {
	for _, cmd := range cmds {
		if err := r.Apply(cmd, timeout); err != nil {
			return err
		}
	}
	return nil
}

// IsLeader always returns true for standalone mode.
func (r *StandaloneReplicator) IsLeader() bool {
	return true
}

// LeaderAddr returns empty string for standalone mode.
func (r *StandaloneReplicator) LeaderAddr() string {
	return ""
}

// LeaderID returns this node's ID for standalone mode.
func (r *StandaloneReplicator) LeaderID() string {
	return r.config.NodeID
}

// Health returns health status.
func (r *StandaloneReplicator) Health() *HealthStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state := "initializing"
	if r.started {
		state = "ready"
	}
	if r.closed {
		state = "closed"
	}

	return &HealthStatus{
		Mode:     ModeStandalone,
		NodeID:   r.config.NodeID,
		Role:     "standalone",
		IsLeader: true,
		LeaderID: r.config.NodeID,
		State:    state,
		Healthy:  r.started && !r.closed,
	}
}

// WaitForLeader returns immediately for standalone mode.
func (r *StandaloneReplicator) WaitForLeader(ctx context.Context) error {
	return nil
}

// Shutdown stops the standalone replicator.
func (r *StandaloneReplicator) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	return nil
}

// Mode returns the replication mode.
func (r *StandaloneReplicator) Mode() ReplicationMode {
	return ModeStandalone
}

// NodeID returns this node's ID.
func (r *StandaloneReplicator) NodeID() string {
	return r.config.NodeID
}

// Ensure StandaloneReplicator implements Replicator.
var _ Replicator = (*StandaloneReplicator)(nil)
