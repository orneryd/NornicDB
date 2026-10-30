package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
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

	mu       sync.RWMutex
	starting *lifecycle.OnceCompletion // the current start attempt; a retry is a new attempt
	work     lifecycle.WorkLifetime    // admission-free stop signal and outcomes of this replicator

	// Cross-region replication
	remoteConns     map[string]PeerConnection // regionID -> connection
	remoteStreaming map[string]bool           // regionID -> actively streaming
	walPosition     uint64                    // Last WAL position sent to remotes

	// Transport for cross-region communication
	transport Transport

	// State
	started   atomic.Bool
	isPrimary atomic.Bool // Is this the primary region?
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
	}

	// First region listed is primary by default
	r.isPrimary.Store(len(config.MultiRegion.RemoteRegions) == 0)

	return r, nil
}

// LifetimeContext returns the MultiRegionReplicator owner's immutable shutdown context.
func (r *MultiRegionReplicator) LifetimeContext() context.Context { return r.work.Context() }

// SetTransport sets the transport for cross-region communication.
func (r *MultiRegionReplicator) SetTransport(transport Transport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transport = transport
	r.localRaft.SetTransport(transport)
}

// Start initializes and starts the multi-region replicator.
func (r *MultiRegionReplicator) Start(ctx context.Context) (startErr error) {
	r.mu.Lock()
	if r.work.Closing() {
		r.mu.Unlock()
		return ErrClosed
	}
	if attempt := r.starting; attempt != nil {
		r.mu.Unlock()
		<-attempt.Done()
		return attempt.Err()
	}
	if r.started.Load() {
		r.mu.Unlock()
		return nil
	}
	attempt := &lifecycle.OnceCompletion{}
	r.starting = attempt
	r.mu.Unlock()
	release, accepted := r.enter()
	if !accepted {
		r.mu.Lock()
		r.starting = nil
		r.mu.Unlock()
		attempt.Do(func() error { return ErrClosed })
		return ErrClosed
	}
	defer func() {
		r.mu.Lock()
		r.starting = nil
		r.mu.Unlock()
		attempt.Do(func() error { return startErr })
		release()
	}()
	raftCtx := ctx // the local Raft owner signals its own stop at its existing phase
	// Initialization observes its caller; running work belongs to the
	// immutable context created before this replicator was published.
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.work.Closing() {
		return ErrClosed
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
	if err := r.localRaft.Start(raftCtx); err != nil {
		return err
	}

	ctx = r.work.Context()
	// Start cross-region replication if we have remote regions
	if len(r.config.MultiRegion.RemoteRegions) > 0 {
		r.goWork(func() { r.runCrossRegionReplication(ctx) })
	}

	r.started.Store(true)
	r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Started in region %s", r.config.NodeID, r.config.MultiRegion.RegionID)
	return nil
}

// runCrossRegionReplication manages async WAL streaming to remote regions.
func (r *MultiRegionReplicator) runCrossRegionReplication(ctx context.Context) {

	// Connect to remote regions
	r.connectToRemoteRegions(ctx)

	// Start WAL streaming loop
	ticker := time.NewTicker(100 * time.Millisecond) // Batch every 100ms for cross-region
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.work.Context().Done():
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
	r.mu.RLock()
	transport := r.transport
	r.mu.RUnlock()

	if transport == nil {
		r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] No transport configured for cross-region replication", r.config.NodeID)
		return
	}

	for _, remote := range r.config.MultiRegion.RemoteRegions {
		if len(remote.Addrs) == 0 {
			continue
		}

		// Try to connect to first available address
		for _, addr := range remote.Addrs {
			conn, err := transport.Connect(ctx, addr)
			if err != nil {
				r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] Failed to connect to region %s at %s: %v",
					r.config.NodeID, remote.RegionID, addr, err)
				continue
			}

			r.mu.Lock()
			r.remoteConns[remote.RegionID] = conn
			r.remoteStreaming[remote.RegionID] = true
			r.mu.Unlock()
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
	if err := checkApplyReadiness(r.work.Closing(), r.started.Load()); err != nil {
		return err
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
	if r.work.Closing() {
		state = "closed"
	}

	// Build peer status for remote regions
	r.mu.RLock()
	connections := make(map[string]PeerConnection, len(r.remoteConns))
	for id, conn := range r.remoteConns {
		connections[id] = conn
	}
	r.mu.RUnlock()
	peers := make([]PeerStatus, 0, len(connections))
	for regionID, conn := range connections {
		peers = append(peers, PeerStatus{
			ID:      regionID,
			Address: "", // Remote region coordinator
			Healthy: conn.IsConnected(),
		})
	}

	return &HealthStatus{
		Mode:         ModeMultiRegion,
		NodeID:       r.config.NodeID,
		Role:         localHealth.Role,
		IsLeader:     localHealth.IsLeader,
		LeaderID:     localHealth.LeaderID,
		LeaderAddr:   localHealth.LeaderAddr,
		State:        state,
		Healthy:      r.started.Load() && !r.work.Closing(),
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
	r.config.logPrintf(context.Background(), slog.LevelInfo, "[MultiRegion %s] Shutting down", r.config.NodeID)
	return r.work.Shutdown(context.Background(), r.retire)
}

// retire runs once, after every accepted unit of work has returned: it closes
// the remote connections, then the local Raft owner.
func (r *MultiRegionReplicator) retire() error {
	r.mu.Lock()
	connections := r.remoteConns
	r.remoteConns = make(map[string]PeerConnection)
	r.mu.Unlock()
	var closeErr error
	for regionID, conn := range connections {
		if err := conn.Close(); err != nil {
			closeErr = compose.Outcomes(closeErr, fmt.Errorf("multi-region %s connection close: %w", regionID, err))
			r.config.logPrintf(context.Background(), slog.LevelWarn, "[MultiRegion %s] Error closing connection to region %s: %v",
				r.config.NodeID, regionID, err)
		}
	}
	return compose.Outcomes(closeErr, r.localRaft.Shutdown())
}

// enter admits one unit of accepted work. It is refused once shutdown has
// sealed admission; the release is called on the work's actual return.
func (r *MultiRegionReplicator) enter() (func(), bool) {
	_, release, accepted := r.work.Acquire(r.work.Context())
	return release, accepted
}

// goWork launches fn as accepted work: enrolled before the goroutine exists,
// released when fn actually returns. It reports false once admission is sealed.
func (r *MultiRegionReplicator) goWork(fn func()) bool {
	release, accepted := r.enter()
	if !accepted {
		return false
	}
	go func() {
		defer release()
		fn()
	}()
	return true
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
	release, accepted := r.enter()
	if !accepted {
		return ErrClosed
	}
	defer release()
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
		rid, c := regionID, conn
		// The admitted failover enrolls this child before launch.
		r.goWork(func() {
			notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			stop := context.AfterFunc(r.work.Context(), cancel)
			defer stop()

			_, err := c.SendHeartbeat(notifyCtx, &HeartbeatRequest{
				NodeID:    r.config.NodeID,
				Role:      "primary_region", // Signal this is now primary region
				Timestamp: time.Now().UnixNano(),
			})
			if err != nil {
				outcome := fmt.Errorf("multi-region %s failover notification: %w", rid, err)
				r.mu.Lock()
				r.work.RecordError("", outcome)
				r.mu.Unlock()
				r.config.logPrintf(ctx, slog.LevelWarn, "[MultiRegion %s] Failed to notify region %s of failover: %v",
					r.config.NodeID, rid, err)
			} else {
				r.config.logPrintf(ctx, slog.LevelInfo, "[MultiRegion %s] Notified region %s of failover", r.config.NodeID, rid)
			}
		})
	}
}

// Ensure MultiRegionReplicator implements Replicator.
var _ Replicator = (*MultiRegionReplicator)(nil)
