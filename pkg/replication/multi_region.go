package replication

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// MultiRegionReplicator implements multi-region replication.
// Each region runs a Raft cluster, with async replication between regions.
//
// Architecture:
// - Each region has its own Raft cluster for strong local consistency
// - Cross-region replication is asynchronous via WAL streaming
// - One region is designated as "primary" for write coordination
// - Failover promotes a remote region to primary
type MultiRegionReplicator struct {
	config  *Config
	storage Storage

	// Local Raft cluster
	localRaft *RaftReplicator

	mu sync.RWMutex

	// Cross-region replication
	remoteConns     map[string]PeerConnection // regionID -> connection
	remoteStreaming map[string]bool           // regionID -> actively streaming
	walPosition     uint64                    // Last WAL position sent to remotes

	// Transport for cross-region communication
	transport Transport

	// State
	started   atomic.Bool
	closed    atomic.Bool
	isPrimary atomic.Bool // Is this the primary region?
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewMultiRegionReplicator creates a new multi-region replicator.
func NewMultiRegionReplicator(config *Config, storage Storage) (*MultiRegionReplicator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Create local Raft cluster with the local cluster config
	localConfig := *config
	localConfig.Mode = ModeRaft
	localConfig.Raft = config.MultiRegion.LocalCluster

	localRaft, err := NewRaftReplicator(&localConfig, storage)
	if err != nil {
		return nil, err
	}

	r := &MultiRegionReplicator{
		config:          config,
		storage:         storage,
		localRaft:       localRaft,
		remoteConns:     make(map[string]PeerConnection),
		remoteStreaming: make(map[string]bool),
		stopCh:          make(chan struct{}),
	}

	// First region listed is primary by default
	r.isPrimary.Store(len(config.MultiRegion.RemoteRegions) == 0)

	return r, nil
}

// SetTransport sets the transport for cross-region communication.
func (r *MultiRegionReplicator) SetTransport(transport Transport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transport = transport
	r.localRaft.SetTransport(transport)
}

// Start initializes and starts the multi-region replicator.
func (r *MultiRegionReplicator) Start(ctx context.Context) error {
	if r.started.Load() {
		return nil
	}

	if r.transport == nil {
		transport, err := NewDefaultTransportFromConfig(r.config)
		if err != nil {
			return err
		}
		r.transport = transport
		r.localRaft.SetTransport(transport)
	}

	// Start local Raft cluster
	if err := r.localRaft.Start(ctx); err != nil {
		return err
	}

	// Start cross-region replication if we have remote regions
	if len(r.config.MultiRegion.RemoteRegions) > 0 {
		r.wg.Add(1)
		go r.runCrossRegionReplication(ctx)
	}

	r.started.Store(true)
	r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Started in region %s", r.config.NodeID, r.config.MultiRegion.RegionID)
	return nil
}

// runCrossRegionReplication manages async WAL streaming to remote regions.
func (r *MultiRegionReplicator) runCrossRegionReplication(ctx context.Context) {
	defer r.wg.Done()

	// Connect to remote regions
	r.connectToRemoteRegions(ctx)

	// Start WAL streaming loop
	ticker := time.NewTicker(100 * time.Millisecond) // Batch every 100ms for cross-region
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			// Only stream if we're the local Raft leader
			if r.localRaft.IsLeader() {
				r.streamWALToRemoteRegions(ctx)
			}
		}
	}
}

// connectToRemoteRegions establishes connections to remote region coordinators.
func (r *MultiRegionReplicator) connectToRemoteRegions(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.transport == nil {
		r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] No transport configured for cross-region replication", r.config.NodeID)
		return
	}

	for _, remote := range r.config.MultiRegion.RemoteRegions {
		if len(remote.Addrs) == 0 {
			continue
		}

		// Try to connect to first available address
		for _, addr := range remote.Addrs {
			conn, err := r.transport.Connect(ctx, addr)
			if err != nil {
				r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] Failed to connect to region %s at %s: %v",
					r.config.NodeID, remote.RegionID, addr, err)
				continue
			}

			r.remoteConns[remote.RegionID] = conn
			r.remoteStreaming[remote.RegionID] = true
			r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Connected to remote region %s at %s",
				r.config.NodeID, remote.RegionID, addr)
			break
		}
	}
}

// streamWALToRemoteRegions streams WAL entries to all connected remote regions.
func (r *MultiRegionReplicator) streamWALToRemoteRegions(ctx context.Context) {
	r.mu.RLock()
	conns := make(map[string]PeerConnection, len(r.remoteConns))
	for k, v := range r.remoteConns {
		conns[k] = v
	}
	walPos := r.walPosition
	r.mu.RUnlock()

	if len(conns) == 0 {
		return
	}

	// Get pending WAL entries from local storage
	currentPos, err := r.storage.GetWALPosition()
	if err != nil || currentPos <= walPos {
		return
	}

	// Create WAL entries to send
	entries := make([]*WALEntry, 0, currentPos-walPos)
	for pos := walPos + 1; pos <= currentPos; pos++ {
		entries = append(entries, &WALEntry{
			Position:  pos,
			Timestamp: time.Now().UnixNano(),
			// Data would come from actual WAL storage
		})
	}

	if len(entries) == 0 {
		return
	}

	// Send to each remote region asynchronously
	var wg sync.WaitGroup
	for regionID, conn := range conns {
		if !conn.IsConnected() {
			continue
		}

		wg.Add(1)
		go func(rid string, c PeerConnection) {
			defer wg.Done()

			sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second) // Longer timeout for cross-region
			defer cancel()

			resp, err := c.SendWALBatch(sendCtx, entries)
			if err != nil {
				r.config.logPrintf(ctx, slog.LevelError, "[MultiRegion %s] Failed to stream WAL to region %s: %v",
					r.config.NodeID, rid, err)
				return
			}

			r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Streamed %d WAL entries to region %s (acked: %d)",
				r.config.NodeID, len(entries), rid, resp.AckedPosition)
		}(regionID, conn)
	}
	wg.Wait()

	// Update WAL position
	r.mu.Lock()
	r.walPosition = currentPos
	r.mu.Unlock()
}

// Apply applies a command through the local Raft cluster.
func (r *MultiRegionReplicator) Apply(cmd *Command, timeout time.Duration) error {
	if r.closed.Load() {
		return ErrClosed
	}
	if !r.started.Load() {
		return ErrNotReady
	}

	// Apply through local Raft
	return r.localRaft.Apply(cmd, timeout)
}

// ApplyBatch applies multiple commands.
func (r *MultiRegionReplicator) ApplyBatch(cmds []*Command, timeout time.Duration) error {
	for _, cmd := range cmds {
		if err := r.Apply(cmd, timeout); err != nil {
			return err
		}
	}
	return nil
}

// IsLeader returns true if this node is the local Raft leader.
func (r *MultiRegionReplicator) IsLeader() bool {
	return r.localRaft.IsLeader()
}

// LeaderAddr returns the address of the local leader.
func (r *MultiRegionReplicator) LeaderAddr() string {
	return r.localRaft.LeaderAddr()
}

// LeaderID returns the ID of the local leader.
func (r *MultiRegionReplicator) LeaderID() string {
	return r.localRaft.LeaderID()
}

// Health returns health status.
func (r *MultiRegionReplicator) Health() *HealthStatus {
	localHealth := r.localRaft.Health()

	state := "initializing"
	if r.started.Load() {
		state = "ready"
	}
	if r.closed.Load() {
		state = "closed"
	}

	// Build peer status for remote regions
	r.mu.RLock()
	peers := make([]PeerStatus, 0, len(r.remoteConns))
	for regionID, conn := range r.remoteConns {
		peers = append(peers, PeerStatus{
			ID:      regionID,
			Address: "", // Remote region coordinator
			Healthy: conn.IsConnected(),
		})
	}
	r.mu.RUnlock()

	return &HealthStatus{
		Mode:         ModeMultiRegion,
		NodeID:       r.config.NodeID,
		Role:         localHealth.Role,
		IsLeader:     localHealth.IsLeader,
		LeaderID:     localHealth.LeaderID,
		LeaderAddr:   localHealth.LeaderAddr,
		State:        state,
		Healthy:      r.started.Load() && !r.closed.Load(),
		Region:       r.config.MultiRegion.RegionID,
		CommitIndex:  localHealth.CommitIndex,
		AppliedIndex: localHealth.AppliedIndex,
		Term:         localHealth.Term,
		Peers:        peers,
	}
}

// WaitForLeader blocks until a leader is elected.
func (r *MultiRegionReplicator) WaitForLeader(ctx context.Context) error {
	return r.localRaft.WaitForLeader(ctx)
}

// Shutdown stops the replicator.
func (r *MultiRegionReplicator) Shutdown() error {
	if r.closed.Load() {
		return nil
	}

	r.closed.Store(true)
	close(r.stopCh)

	// Wait for cross-region replication to stop
	r.wg.Wait()

	// Close all remote connections
	r.mu.Lock()
	for regionID, conn := range r.remoteConns {
		if err := conn.Close(); err != nil {
			r.config.logPrintf(context.Background(), slog.LevelWarn, "[MultiRegion %s] Error closing connection to region %s: %v",
				r.config.NodeID, regionID, err)
		}
	}
	r.remoteConns = make(map[string]PeerConnection)
	r.mu.Unlock()

	r.config.logPrintf(context.Background(), slog.LevelInfo, "[MultiRegion %s] Shutting down", r.config.NodeID)
	return r.localRaft.Shutdown()
}

// Mode returns the replication mode.
func (r *MultiRegionReplicator) Mode() ReplicationMode {
	return ModeMultiRegion
}

// NodeID returns this node's ID.
func (r *MultiRegionReplicator) NodeID() string {
	return r.config.NodeID
}

// RegionID returns this region's ID.
func (r *MultiRegionReplicator) RegionID() string {
	return r.config.MultiRegion.RegionID
}

// IsPrimaryRegion returns true if this is the primary region.
func (r *MultiRegionReplicator) IsPrimaryRegion() bool {
	return r.isPrimary.Load()
}

// RegionFailover promotes this region to primary.
func (r *MultiRegionReplicator) RegionFailover(ctx context.Context) error {
	if r.isPrimary.Load() {
		return nil
	}

	// Must be local Raft leader to become primary region
	if !r.localRaft.IsLeader() {
		return ErrNotLeader
	}

	r.isPrimary.Store(true)

	// Notify other regions of failover
	r.notifyRegionsOfFailover(ctx)

	r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Region failover complete - now primary region", r.config.NodeID)
	return nil
}

// notifyRegionsOfFailover notifies all connected remote regions that this region is now primary.
func (r *MultiRegionReplicator) notifyRegionsOfFailover(ctx context.Context) {
	r.mu.RLock()
	conns := make(map[string]PeerConnection, len(r.remoteConns))
	for k, v := range r.remoteConns {
		conns[k] = v
	}
	r.mu.RUnlock()

	for regionID, conn := range conns {
		if !conn.IsConnected() {
			continue
		}

		// Send failover notification via heartbeat
		// Role field indicates primary status
		go func(rid string, c PeerConnection) {
			notifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			_, err := c.SendHeartbeat(notifyCtx, &HeartbeatRequest{
				NodeID:    r.config.NodeID,
				Role:      "primary_region", // Signal this is now primary region
				Timestamp: time.Now().UnixNano(),
			})
			if err != nil {
				r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] Failed to notify region %s of failover: %v",
					r.config.NodeID, rid, err)
			} else {
				r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Notified region %s of failover", r.config.NodeID, rid)
			}
		}(regionID, conn)
	}
}

// Ensure MultiRegionReplicator implements Replicator.
var _ Replicator = (*MultiRegionReplicator)(nil)
