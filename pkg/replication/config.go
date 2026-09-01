// Package replication provides distributed replication for NornicDB.
//
// This package implements a Cassandra-style shared-nothing architecture with
// Raft consensus for strong consistency. It supports multiple deployment modes:
//
//   - Standalone: Single node, no replication (default)
//   - HAStandby: Hot standby with automatic failover (2 nodes)
//   - Raft: Raft consensus cluster (3+ nodes)
//   - MultiRegion: Raft clusters with cross-region async replication
//
// The design follows these principles:
//   - Shared-nothing: Each node owns its data, no shared storage
//   - Tunable consistency: Choose between AP (fast) and CP (safe) per query
//   - Linear scalability: Add nodes for increased capacity
//   - Operational simplicity: Minimal configuration, sensible defaults
//
// Environment Variables (NORNICDB_CLUSTER_*):
//
//	NORNICDB_CLUSTER_MODE=standalone|ha_standby|raft|multi_region
//	NORNICDB_CLUSTER_NODE_ID=node-1
//	NORNICDB_CLUSTER_BIND_ADDR=0.0.0.0:7000
//	NORNICDB_CLUSTER_ADVERTISE_ADDR=192.168.1.10:7000
//	NORNICDB_CLUSTER_REPLICATION_SECRET=your-shared-secret-min-32-chars
//
// Hot Standby:
//
//	NORNICDB_CLUSTER_HA_ROLE=primary|standby
//	NORNICDB_CLUSTER_HA_PEER_ADDR=standby-host:7000
//	NORNICDB_CLUSTER_HA_SYNC_MODE=async|quorum
//	NORNICDB_CLUSTER_HA_HEARTBEAT_MS=1000
//	NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT=30s
//	NORNICDB_CLUSTER_HA_AUTO_FAILOVER=true
//
// Raft Cluster:
//
//	NORNICDB_CLUSTER_RAFT_PEERS=node-2:7000,node-3:7000
//	NORNICDB_CLUSTER_RAFT_BOOTSTRAP=true
//	NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT=1s
//	NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT=100ms
//	NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL=300
//	NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD=10000
//
// Multi-Region:
//
//	NORNICDB_CLUSTER_REGION_ID=us-east
//	NORNICDB_CLUSTER_REMOTE_REGIONS=eu-west:coord1:7000,ap-south:coord2:7000
//	NORNICDB_CLUSTER_CROSS_REGION_MODE=async|quorum
//
// Example Usage:
//
//	// Load config from environment
//	config := replication.LoadFromEnv()
//
//	// Create replicator based on mode
//	replicator, err := replication.NewReplicator(config, storage)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Start replication
//	if err := replicator.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Apply writes through replicator
//	if err := replicator.Apply(cmd); err != nil {
//		if errors.Is(err, replication.ErrNotLeader) {
//			// Forward to leader
//			leaderAddr := replicator.LeaderAddr()
//		}
//	}
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you have a diary that you want to keep safe:
//
//  1. **Standalone**: You have one diary. Simple, but if you lose it, everything's gone.
//
//  2. **Hot Standby**: You write in your diary, and your friend copies everything
//     you write into their diary. If you're sick, they can take over.
//
//  3. **Raft Cluster**: You and 2 friends all have diaries. When you want to write
//     something, everyone votes. If most agree, everyone writes the same thing.
//     Even if one person is absent, the group still works.
//
//  4. **Multi-Region**: Like Raft, but your friends are in different cities.
//     Within each city, the Raft voting happens. Between cities, changes are
//     copied but not voted on (it would be too slow).
//
// The "shared-nothing" part means each person has their own complete diary -
// no one shares pages with anyone else. This makes it easy to add more friends
// without things getting complicated.
package replication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ReplicationMode defines how the node participates in replication.
type ReplicationMode string

const (
	// ModeStandalone is single-node operation with no replication (default).
	// This mode has zero overhead and is suitable for development, testing,
	// and small deployments where high availability is not required.
	ModeStandalone ReplicationMode = "standalone"

	// ModeHAStandby is hot standby replication between 2 nodes.
	// The primary accepts all writes and streams WAL to the standby.
	// Automatic failover promotes standby if primary fails.
	// Best for: Simple HA without the complexity of consensus.
	ModeHAStandby ReplicationMode = "ha_standby"

	// ModeRaft is Raft consensus with 3+ nodes.
	// Provides strong consistency with automatic leader election.
	// All writes go through Raft log for guaranteed ordering.
	// Best for: Production deployments requiring strong consistency.
	ModeRaft ReplicationMode = "raft"

	// ModeMultiRegion combines Raft clusters with cross-region replication.
	// Each region runs a Raft cluster for local strong consistency.
	// Cross-region replication is async for performance.
	// Best for: Global deployments with regional read replicas.
	ModeMultiRegion ReplicationMode = "multi_region"
)

// SyncMode defines the synchronization strategy for replication.
type SyncMode string

const (
	// SyncAsync acknowledges writes immediately without waiting for replication.
	// Lowest latency, but potential data loss on failure.
	SyncAsync SyncMode = "async"

	// SyncQuorum waits until the replica acknowledges it has applied the write.
	// Highest durability, highest latency.
	SyncQuorum SyncMode = "quorum"
)

// ConsistencyLevel defines read/write consistency requirements.
// Follows Cassandra naming conventions.
type ConsistencyLevel string

const (
	// ConsistencyOne requires acknowledgment from one node.
	ConsistencyOne ConsistencyLevel = "ONE"

	// ConsistencyQuorum requires acknowledgment from majority (N/2+1).
	ConsistencyQuorum ConsistencyLevel = "QUORUM"

	// ConsistencyAll requires acknowledgment from all nodes.
	ConsistencyAll ConsistencyLevel = "ALL"

	// ConsistencyLocalOne requires acknowledgment from one node in local region.
	ConsistencyLocalOne ConsistencyLevel = "LOCAL_ONE"

	// ConsistencyLocalQuorum requires quorum from local region only.
	ConsistencyLocalQuorum ConsistencyLevel = "LOCAL_QUORUM"
)

// Config holds all replication configuration.
// Designed to integrate with NornicDB's existing config patterns.
type Config struct {
	// Logger receives structured replication lifecycle events.
	Logger *slog.Logger
	// Localizer renders replication lifecycle prose. Nil preserves source English.
	Localizer *localization.Manager

	// Mode selects the replication mode (default: standalone)
	// Environment: NORNICDB_CLUSTER_MODE
	Mode ReplicationMode

	// NodeID uniquely identifies this node in the cluster.
	// Auto-generated if not set.
	// Environment: NORNICDB_CLUSTER_NODE_ID
	NodeID string

	// BindAddr is the address to bind the replication server.
	// Environment: NORNICDB_CLUSTER_BIND_ADDR
	BindAddr string

	// AdvertiseAddr is the address advertised to other nodes.
	// Defaults to BindAddr if not set.
	// Environment: NORNICDB_CLUSTER_ADVERTISE_ADDR
	AdvertiseAddr string

	// ReplicationSecret is a shared secret used to authenticate cluster messages.
	// When set, nodes must present valid HMAC signatures for replication traffic.
	// Environment: NORNICDB_CLUSTER_REPLICATION_SECRET
	ReplicationSecret string

	// DataDir for replication state (Raft logs, snapshots).
	// Defaults to main database DataDir + "/replication"
	// Environment: NORNICDB_CLUSTER_DATA_DIR
	DataDir string

	// HAStandby holds hot standby configuration.
	HAStandby HAStandbyConfig

	// Raft holds Raft consensus configuration.
	Raft RaftConfig

	// MultiRegion holds multi-region configuration.
	MultiRegion MultiRegionConfig

	// Consistency holds default consistency levels.
	Consistency ConsistencyConfig

	// TLS holds security configuration for encrypted connections.
	TLS TLSConfig
}

func (c *Config) logEvent(ctx context.Context, level slog.Level, event localization.LogEvent) {
	if c == nil {
		logReplicationEvent(ctx, nil, nil, level, event)
		return
	}
	logReplicationEvent(ctx, c.Logger, c.Localizer, level, event)
}

func (c *Config) logPrintf(ctx context.Context, level slog.Level, format string, args ...any) {
	c.logEvent(ctx, level, localization.ReplicationOperatorEvent(format, args...))
}

func logReplicationEvent(ctx context.Context, logger *slog.Logger, localizer *localization.Manager, level slog.Level, event localization.LogEvent) {
	if logger == nil {
		logger = slog.Default()
	}
	if localizer != nil {
		localizer.Log(ctx, logger, level, event)
		return
	}
	attrs := append([]slog.Attr{slog.String("event_id", string(event.ID))}, event.Attrs...)
	logger.LogAttrs(ctx, level, event.Message.Fallback, attrs...)
}

func logReplicationPrintf(ctx context.Context, level slog.Level, format string, args ...any) {
	logReplicationEvent(ctx, nil, nil, level, localization.ReplicationOperatorEvent(format, args...))
}

// TLSConfig configures TLS/mTLS for secure replication connections.
// ALL replication traffic should use TLS in production environments.
type TLSConfig struct {
	// Enabled enables TLS for all replication connections.
	// STRONGLY RECOMMENDED for production.
	// Environment: NORNICDB_CLUSTER_TLS_ENABLED
	Enabled bool

	// CertFile is path to the server certificate (PEM format).
	// Environment: NORNICDB_CLUSTER_TLS_CERT_FILE
	CertFile string

	// KeyFile is path to the server private key (PEM format).
	// Environment: NORNICDB_CLUSTER_TLS_KEY_FILE
	KeyFile string

	// CAFile is path to the CA certificate for client verification (mTLS).
	// If set, client certificates will be required and verified.
	// Environment: NORNICDB_CLUSTER_TLS_CA_FILE
	CAFile string

	// VerifyClient requires client certificate verification (mTLS).
	// Should be true in production for mutual authentication.
	// Environment: NORNICDB_CLUSTER_TLS_VERIFY_CLIENT
	VerifyClient bool

	// InsecureSkipVerify skips server certificate verification.
	// WARNING: Only use for testing. Never in production!
	// Environment: NORNICDB_CLUSTER_TLS_INSECURE_SKIP_VERIFY
	InsecureSkipVerify bool

	// ServerName is the expected server name for certificate verification.
	// Environment: NORNICDB_CLUSTER_TLS_SERVER_NAME
	ServerName string

	// MinVersion is the minimum TLS version (default: TLS 1.2).
	// Environment: NORNICDB_CLUSTER_TLS_MIN_VERSION
	MinVersion string // "1.2" or "1.3"

	// CipherSuites restricts allowed cipher suites (optional).
	// Leave empty to use secure defaults.
	// Environment: NORNICDB_CLUSTER_TLS_CIPHER_SUITES
	CipherSuites []string
}

// HAStandbyConfig configures 2-node hot standby replication.
type HAStandbyConfig struct {
	// Role is "primary" or "standby".
	// Environment: NORNICDB_CLUSTER_HA_ROLE
	Role string

	// PeerAddr is the address of the other node.
	// Environment: NORNICDB_CLUSTER_HA_PEER_ADDR
	PeerAddr string

	// SyncMode controls replication synchronization.
	// Environment: NORNICDB_CLUSTER_HA_SYNC_MODE
	SyncMode SyncMode

	// HeartbeatInterval is how often to send heartbeats.
	// Environment: NORNICDB_CLUSTER_HA_HEARTBEAT_MS (in milliseconds)
	HeartbeatInterval time.Duration

	// FailoverTimeout is how long to wait before triggering failover.
	// Environment: NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT
	FailoverTimeout time.Duration

	// AutoFailover enables automatic failover on primary failure.
	// Environment: NORNICDB_CLUSTER_HA_AUTO_FAILOVER
	AutoFailover bool

	// WALBatchSize is max entries per WAL batch.
	// Environment: NORNICDB_CLUSTER_HA_WAL_BATCH_SIZE
	WALBatchSize int

	// WALBatchTimeout is max time to wait for batch fill.
	// Environment: NORNICDB_CLUSTER_HA_WAL_BATCH_TIMEOUT
	WALBatchTimeout time.Duration

	// ReconnectInterval is how often to retry connection to peer.
	// Environment: NORNICDB_CLUSTER_HA_RECONNECT_INTERVAL
	ReconnectInterval time.Duration

	// MaxReconnectBackoff is maximum backoff for reconnection.
	// Environment: NORNICDB_CLUSTER_HA_MAX_RECONNECT_BACKOFF
	MaxReconnectBackoff time.Duration
}

// RaftConfig configures Raft consensus clustering.
type RaftConfig struct {
	// ClusterID identifies the Raft cluster.
	// Environment: NORNICDB_CLUSTER_RAFT_CLUSTER_ID
	ClusterID string

	// Bootstrap initializes a new cluster (only on first node).
	// Environment: NORNICDB_CLUSTER_RAFT_BOOTSTRAP
	Bootstrap bool

	// Peers is the initial set of peer addresses.
	// Format: "node-id:addr,node-id:addr"
	// Environment: NORNICDB_CLUSTER_RAFT_PEERS
	Peers []PeerConfig

	// ElectionTimeout is the time before starting an election.
	// Environment: NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT
	ElectionTimeout time.Duration

	// HeartbeatTimeout is the interval for leader heartbeats.
	// Environment: NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT
	HeartbeatTimeout time.Duration

	// LeaderLeaseTimeout is how long a leader maintains its lease.
	// Environment: NORNICDB_CLUSTER_RAFT_LEADER_LEASE_TIMEOUT
	LeaderLeaseTimeout time.Duration

	// SnapshotInterval is seconds between automatic snapshots.
	// Environment: NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL
	SnapshotInterval int

	// SnapshotThreshold is log entries before triggering snapshot.
	// Environment: NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD
	SnapshotThreshold uint64

	// TrailingLogs is entries to retain after snapshot.
	// Environment: NORNICDB_CLUSTER_RAFT_TRAILING_LOGS
	TrailingLogs uint64

	// MaxAppendEntries is max entries per AppendEntries RPC.
	// Environment: NORNICDB_CLUSTER_RAFT_MAX_APPEND_ENTRIES
	MaxAppendEntries uint64

	// CommitTimeout is max time to wait for commit.
	// Environment: NORNICDB_CLUSTER_RAFT_COMMIT_TIMEOUT
	CommitTimeout time.Duration

	// MaxInflightLogs is max in-flight log entries for replication.
	// Environment: NORNICDB_CLUSTER_RAFT_MAX_INFLIGHT_LOGS
	MaxInflightLogs int

	// SnapshotRetain is number of snapshots to retain.
	// Environment: NORNICDB_CLUSTER_RAFT_SNAPSHOT_RETAIN
	SnapshotRetain int
}

// PeerConfig holds configuration for a single peer node.
type PeerConfig struct {
	// ID is the unique identifier for this peer.
	ID string

	// Addr is the network address (host:port).
	Addr string
}

// MultiRegionConfig configures cross-region replication.
type MultiRegionConfig struct {
	// RegionID identifies this region.
	// Environment: NORNICDB_CLUSTER_REGION_ID
	RegionID string

	// LocalCluster is the local Raft cluster configuration.
	LocalCluster RaftConfig

	// RemoteRegions are other regions to replicate to.
	// Environment: NORNICDB_CLUSTER_REMOTE_REGIONS (format: "region:host:port,...")
	RemoteRegions []RemoteRegionConfig

	// CrossRegionSyncMode controls cross-region replication mode.
	// Environment: NORNICDB_CLUSTER_CROSS_REGION_MODE
	CrossRegionSyncMode SyncMode

	// CrossRegionBatchSize is entries per cross-region batch.
	// Environment: NORNICDB_CLUSTER_CROSS_REGION_BATCH_SIZE
	CrossRegionBatchSize int

	// CrossRegionBatchTimeout is max time to wait for batch fill.
	// Environment: NORNICDB_CLUSTER_CROSS_REGION_BATCH_TIMEOUT
	CrossRegionBatchTimeout time.Duration

	// ConflictStrategy handles write conflicts between regions.
	// Environment: NORNICDB_CLUSTER_CONFLICT_STRATEGY
	ConflictStrategy string // "last_write_wins", "manual"
}

// RemoteRegionConfig holds configuration for a remote region.
type RemoteRegionConfig struct {
	// RegionID identifies the remote region.
	RegionID string

	// Addrs are coordinator addresses for the remote region.
	Addrs []string

	// Priority for failover (lower = higher priority).
	Priority int
}

// ConsistencyConfig holds default consistency levels.
type ConsistencyConfig struct {
	// DefaultWriteConsistency for write operations.
	// Environment: NORNICDB_CLUSTER_WRITE_CONSISTENCY
	DefaultWriteConsistency ConsistencyLevel

	// DefaultReadConsistency for read operations.
	// Environment: NORNICDB_CLUSTER_READ_CONSISTENCY
	DefaultReadConsistency ConsistencyLevel
}

// DefaultConfig returns a Config with sensible defaults for standalone mode.
// All values are tuned for single-node operation with zero overhead.
func DefaultConfig() *Config {
	return &Config{
		Mode:          ModeStandalone,
		BindAddr:      "0.0.0.0:7000",
		AdvertiseAddr: "127.0.0.1:7000", // Default to localhost for tests
		DataDir:       "./data/replication",

		HAStandby: HAStandbyConfig{
			SyncMode:            SyncAsync,
			HeartbeatInterval:   1000 * time.Millisecond,
			FailoverTimeout:     30 * time.Second,
			AutoFailover:        true,
			WALBatchSize:        1000,
			WALBatchTimeout:     10 * time.Millisecond,
			ReconnectInterval:   5 * time.Second,
			MaxReconnectBackoff: 30 * time.Second,
		},

		Raft: RaftConfig{
			ElectionTimeout:    1 * time.Second,
			HeartbeatTimeout:   100 * time.Millisecond,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			SnapshotInterval:   300,
			SnapshotThreshold:  10000,
			TrailingLogs:       10000,
			MaxAppendEntries:   64,
			CommitTimeout:      50 * time.Millisecond,
			MaxInflightLogs:    512,
			SnapshotRetain:     3,
		},

		MultiRegion: MultiRegionConfig{
			CrossRegionSyncMode:     SyncAsync,
			CrossRegionBatchSize:    100,
			CrossRegionBatchTimeout: 100 * time.Millisecond,
			ConflictStrategy:        "last_write_wins",
		},

		Consistency: ConsistencyConfig{
			DefaultWriteConsistency: ConsistencyQuorum,
			DefaultReadConsistency:  ConsistencyOne,
		},
	}
}

// LoadFromEnv loads replication configuration from environment variables.
// Uses NORNICDB_CLUSTER_* prefix for all replication settings.
//
// Example environment for HA Standby:
//
//	NORNICDB_CLUSTER_MODE=ha_standby
//	NORNICDB_CLUSTER_NODE_ID=primary-1
//	NORNICDB_CLUSTER_HA_ROLE=primary
//	NORNICDB_CLUSTER_HA_PEER_ADDR=standby-1:7000
//	NORNICDB_CLUSTER_HA_AUTO_FAILOVER=true
//
// Example environment for Raft:
//
//	NORNICDB_CLUSTER_MODE=raft
//	NORNICDB_CLUSTER_NODE_ID=node-1
//	NORNICDB_CLUSTER_RAFT_BOOTSTRAP=true
//	NORNICDB_CLUSTER_RAFT_PEERS=node-2:node2:7000,node-3:node3:7000
func LoadFromEnv() *Config {
	config := DefaultConfig()

	// Core settings
	config.Mode = ReplicationMode(envutil.Get("NORNICDB_CLUSTER_MODE", string(ModeStandalone)))
	config.NodeID = envutil.Get("NORNICDB_CLUSTER_NODE_ID", generateNodeID())
	config.BindAddr = envutil.Get("NORNICDB_CLUSTER_BIND_ADDR", "0.0.0.0:7000")
	config.AdvertiseAddr = envutil.Get("NORNICDB_CLUSTER_ADVERTISE_ADDR", config.BindAddr)
	config.DataDir = envutil.Get("NORNICDB_CLUSTER_DATA_DIR", "./data/replication")
	config.ReplicationSecret = envutil.Get("NORNICDB_CLUSTER_REPLICATION_SECRET", "")

	// HA Standby settings
	config.HAStandby.Role = envutil.Get("NORNICDB_CLUSTER_HA_ROLE", "")
	config.HAStandby.PeerAddr = envutil.Get("NORNICDB_CLUSTER_HA_PEER_ADDR", "")
	config.HAStandby.SyncMode = SyncMode(envutil.Get("NORNICDB_CLUSTER_HA_SYNC_MODE", string(SyncAsync)))
	config.HAStandby.HeartbeatInterval = getEnvDurationMs("NORNICDB_CLUSTER_HA_HEARTBEAT_MS", 1000)
	config.HAStandby.FailoverTimeout = envutil.GetDuration("NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT", 30*time.Second)
	config.HAStandby.AutoFailover = envutil.GetBoolLoose("NORNICDB_CLUSTER_HA_AUTO_FAILOVER", true)
	config.HAStandby.WALBatchSize = envutil.GetInt("NORNICDB_CLUSTER_HA_WAL_BATCH_SIZE", 1000)
	config.HAStandby.WALBatchTimeout = envutil.GetDuration("NORNICDB_CLUSTER_HA_WAL_BATCH_TIMEOUT", 10*time.Millisecond)
	config.HAStandby.ReconnectInterval = envutil.GetDuration("NORNICDB_CLUSTER_HA_RECONNECT_INTERVAL", 5*time.Second)
	config.HAStandby.MaxReconnectBackoff = envutil.GetDuration("NORNICDB_CLUSTER_HA_MAX_RECONNECT_BACKOFF", 30*time.Second)

	// Raft settings
	config.Raft.ClusterID = envutil.Get("NORNICDB_CLUSTER_RAFT_CLUSTER_ID", "nornicdb")
	config.Raft.Bootstrap = envutil.GetBoolLoose("NORNICDB_CLUSTER_RAFT_BOOTSTRAP", false)
	config.Raft.Peers = parsePeers(envutil.Get("NORNICDB_CLUSTER_RAFT_PEERS", ""))
	config.Raft.ElectionTimeout = envutil.GetDuration("NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT", 1*time.Second)
	config.Raft.HeartbeatTimeout = envutil.GetDuration("NORNICDB_CLUSTER_RAFT_HEARTBEAT_TIMEOUT", 100*time.Millisecond)
	config.Raft.LeaderLeaseTimeout = envutil.GetDuration("NORNICDB_CLUSTER_RAFT_LEADER_LEASE_TIMEOUT", 500*time.Millisecond)
	config.Raft.SnapshotInterval = envutil.GetInt("NORNICDB_CLUSTER_RAFT_SNAPSHOT_INTERVAL", 300)
	config.Raft.SnapshotThreshold = uint64(envutil.GetInt("NORNICDB_CLUSTER_RAFT_SNAPSHOT_THRESHOLD", 10000))
	config.Raft.TrailingLogs = uint64(envutil.GetInt("NORNICDB_CLUSTER_RAFT_TRAILING_LOGS", 10000))
	config.Raft.MaxAppendEntries = uint64(envutil.GetInt("NORNICDB_CLUSTER_RAFT_MAX_APPEND_ENTRIES", 64))
	config.Raft.CommitTimeout = envutil.GetDuration("NORNICDB_CLUSTER_RAFT_COMMIT_TIMEOUT", 50*time.Millisecond)
	config.Raft.MaxInflightLogs = envutil.GetInt("NORNICDB_CLUSTER_RAFT_MAX_INFLIGHT_LOGS", 512)
	config.Raft.SnapshotRetain = envutil.GetInt("NORNICDB_CLUSTER_RAFT_SNAPSHOT_RETAIN", 3)

	// Multi-region settings
	config.MultiRegion.RegionID = envutil.Get("NORNICDB_CLUSTER_REGION_ID", "")
	config.MultiRegion.RemoteRegions = parseRemoteRegions(envutil.Get("NORNICDB_CLUSTER_REMOTE_REGIONS", ""))
	config.MultiRegion.CrossRegionSyncMode = SyncMode(envutil.Get("NORNICDB_CLUSTER_CROSS_REGION_MODE", string(SyncAsync)))
	config.MultiRegion.CrossRegionBatchSize = envutil.GetInt("NORNICDB_CLUSTER_CROSS_REGION_BATCH_SIZE", 100)
	config.MultiRegion.CrossRegionBatchTimeout = envutil.GetDuration("NORNICDB_CLUSTER_CROSS_REGION_BATCH_TIMEOUT", 100*time.Millisecond)
	config.MultiRegion.ConflictStrategy = envutil.Get("NORNICDB_CLUSTER_CONFLICT_STRATEGY", "last_write_wins")
	// Copy Raft config for local cluster
	config.MultiRegion.LocalCluster = config.Raft

	// Consistency settings
	config.Consistency.DefaultWriteConsistency = ConsistencyLevel(envutil.Get("NORNICDB_CLUSTER_WRITE_CONSISTENCY", string(ConsistencyQuorum)))
	config.Consistency.DefaultReadConsistency = ConsistencyLevel(envutil.Get("NORNICDB_CLUSTER_READ_CONSISTENCY", string(ConsistencyOne)))

	// TLS settings (STRONGLY RECOMMENDED for production)
	config.TLS.Enabled = envutil.GetBoolLoose("NORNICDB_CLUSTER_TLS_ENABLED", false)
	config.TLS.CertFile = envutil.Get("NORNICDB_CLUSTER_TLS_CERT_FILE", "")
	config.TLS.KeyFile = envutil.Get("NORNICDB_CLUSTER_TLS_KEY_FILE", "")
	config.TLS.CAFile = envutil.Get("NORNICDB_CLUSTER_TLS_CA_FILE", "")
	config.TLS.VerifyClient = envutil.GetBoolLoose("NORNICDB_CLUSTER_TLS_VERIFY_CLIENT", true) // Default to secure
	config.TLS.InsecureSkipVerify = envutil.GetBoolLoose("NORNICDB_CLUSTER_TLS_INSECURE_SKIP_VERIFY", false)
	config.TLS.ServerName = envutil.Get("NORNICDB_CLUSTER_TLS_SERVER_NAME", "")
	config.TLS.MinVersion = envutil.Get("NORNICDB_CLUSTER_TLS_MIN_VERSION", "1.2")
	config.TLS.CipherSuites = parseCSV(envutil.Get("NORNICDB_CLUSTER_TLS_CIPHER_SUITES", ""))

	return config
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeStandalone:
		// No validation needed for standalone
		return nil

	case ModeHAStandby:
		if c.HAStandby.Role == "" {
			return localizedError(localization.ReplicationConfigHARoleRequired(), nil)
		}
		if c.HAStandby.Role != "primary" && c.HAStandby.Role != "standby" {
			return localizedError(localization.ReplicationConfigInvalidHARole(c.HAStandby.Role), nil)
		}
		if c.HAStandby.PeerAddr == "" {
			return localizedError(localization.ReplicationConfigHAPeerAddressRequired(), nil)
		}
		if c.HAStandby.SyncMode == "" {
			c.HAStandby.SyncMode = SyncAsync
		}
		if c.HAStandby.SyncMode != SyncAsync && c.HAStandby.SyncMode != SyncQuorum {
			return localizedError(localization.ReplicationConfigInvalidHASyncMode(string(c.HAStandby.SyncMode)), nil)
		}

	case ModeRaft:
		if c.Raft.Bootstrap && len(c.Raft.Peers) == 0 {
			// Bootstrap node can start alone, others will join
		} else if !c.Raft.Bootstrap && len(c.Raft.Peers) == 0 {
			return localizedError(localization.ReplicationConfigRaftPeersRequired(), nil)
		}

	case ModeMultiRegion:
		if c.MultiRegion.RegionID == "" {
			return localizedError(localization.ReplicationConfigRegionIDRequired(), nil)
		}
		if c.MultiRegion.CrossRegionSyncMode == "" {
			c.MultiRegion.CrossRegionSyncMode = SyncAsync
		}
		if c.MultiRegion.CrossRegionSyncMode != SyncAsync && c.MultiRegion.CrossRegionSyncMode != SyncQuorum {
			return localizedError(localization.ReplicationConfigInvalidCrossRegionSyncMode(string(c.MultiRegion.CrossRegionSyncMode)), nil)
		}

	default:
		return localizedError(localization.ReplicationConfigUnknownMode(c.Mode), nil)
	}

	if c.NodeID == "" {
		return localizedError(localization.ReplicationConfigNodeIDRequired(), nil)
	}

	// TLS validation for non-standalone modes
	if c.Mode != ModeStandalone {
		if err := c.validateTLS(); err != nil {
			return err
		}
	}

	if c.ReplicationSecret != "" && len(c.ReplicationSecret) < 32 {
		return localizedError(localization.ReplicationConfigSecretTooShort(32), nil)
	}

	return nil
}

// validateTLS validates TLS configuration.
func (c *Config) validateTLS() error {
	// Warn if TLS is disabled in clustered mode (but don't fail - allow testing)
	if !c.TLS.Enabled {
		// TLS should be enabled for production but we allow disabling for testing
		return nil
	}

	// If TLS is enabled, cert and key are required
	if c.TLS.CertFile == "" {
		return localizedError(localization.ReplicationConfigTLSCertRequired(), nil)
	}
	if c.TLS.KeyFile == "" {
		return localizedError(localization.ReplicationConfigTLSKeyRequired(), nil)
	}

	// Verify mTLS configuration
	if c.TLS.VerifyClient && c.TLS.CAFile == "" {
		return localizedError(localization.ReplicationConfigTLSCARequired(), nil)
	}

	// Warn about insecure configurations (but allow for testing)
	if c.TLS.InsecureSkipVerify {
		// Log warning: InsecureSkipVerify should only be used for testing
	}

	// Validate min version
	switch c.TLS.MinVersion {
	case "", "1.2", "1.3":
		// Valid
	default:
		return localizedError(localization.ReplicationConfigInvalidTLSMinVersion(c.TLS.MinVersion), nil)
	}

	return nil
}

// IsStandalone returns true if running in standalone mode.
func (c *Config) IsStandalone() bool {
	return c.Mode == ModeStandalone
}

// String returns a safe string representation (no sensitive data).
func (c *Config) String() string {
	return fmt.Sprintf("ReplicationConfig{Mode: %s, NodeID: %s, Bind: %s}",
		c.Mode, c.NodeID, c.BindAddr)
}

func getEnvDurationMs(key string, defaultMs int) time.Duration {
	ms := envutil.GetInt(key, defaultMs)
	return time.Duration(ms) * time.Millisecond
}

func generateNodeID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano()%10000)
}

// parsePeers parses peer configuration string.
// Format: "id1:addr1,id2:addr2" or just "addr1,addr2" (auto-generate IDs)
func parsePeers(s string) []PeerConfig {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	peers := make([]PeerConfig, 0, len(parts))

	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// Check if format is "id:addr" or just "addr"
		if idx := strings.Index(p, ":"); idx > 0 {
			// Could be "id:host:port" or just "host:port"
			// Check if there are two colons (id:host:port)
			colonCount := strings.Count(p, ":")
			if colonCount >= 2 {
				// Format: id:host:port
				firstColon := strings.Index(p, ":")
				peers = append(peers, PeerConfig{
					ID:   p[:firstColon],
					Addr: p[firstColon+1:],
				})
			} else {
				// Format: host:port (auto-generate ID)
				peers = append(peers, PeerConfig{
					ID:   fmt.Sprintf("peer-%d", i+1),
					Addr: p,
				})
			}
		} else {
			// Just hostname, assume default port
			peers = append(peers, PeerConfig{
				ID:   fmt.Sprintf("peer-%d", i+1),
				Addr: p + ":7000",
			})
		}
	}

	return peers
}

// parseRemoteRegions parses remote region configuration.
// Format: "region1:host1:port1,region2:host2:port2"
func parseRemoteRegions(s string) []RemoteRegionConfig {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	regions := make([]RemoteRegionConfig, 0)
	regionMap := make(map[string]*RemoteRegionConfig)

	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// Format: region:host:port
		subparts := strings.SplitN(p, ":", 2)
		if len(subparts) < 2 {
			continue
		}

		regionID := subparts[0]
		addr := subparts[1]

		if existing, ok := regionMap[regionID]; ok {
			existing.Addrs = append(existing.Addrs, addr)
		} else {
			region := &RemoteRegionConfig{
				RegionID: regionID,
				Addrs:    []string{addr},
				Priority: i + 1,
			}
			regionMap[regionID] = region
			regions = append(regions, *region)
		}
	}

	// Update regions with accumulated addresses
	for i := range regions {
		if updated, ok := regionMap[regions[i].RegionID]; ok {
			regions[i] = *updated
		}
	}

	return regions
}

// parseCSV parses a comma-separated string into a slice.
func parseCSV(s string) []string {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
