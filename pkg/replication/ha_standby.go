package replication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// HAStandbyReplicator implements hot standby replication between 2 nodes.
// One node is primary (accepts writes), the other is standby (receives WAL).
// Supports automatic failover when primary fails.
type HAStandbyReplicator struct {
	config  *Config
	storage Storage

	mu       sync.RWMutex
	starting *lifecycle.OnceCompletion // the current start attempt; a retry is a new attempt
	work     lifecycle.WorkLifetime    // admission-free stop signal and outcomes of this replicator

	// Role state
	role       string // "primary", "standby" or "fenced" (protected by mu)
	isPrimary  atomic.Bool
	isPromoted atomic.Bool

	// promoting is the in-flight Promote, if any (protected by promoteMu).
	promoteMu sync.Mutex
	promoting *lifecycle.OnceCompletion // the current promotion; a retry is a new attempt

	// Primary-side state
	walStreamer *WALStreamer
	standbyConn PeerConnection

	// Standby-side state
	walApplier     *WALApplier
	primaryConn    PeerConnection
	lastPrimaryHB  time.Time
	primaryHealthy atomic.Bool

	// Shared state
	started atomic.Bool

	// metrics is the Plan-04-06 observation seam (D-15a per-event role
	// updates + per-peer lag/RTT/last_contact). nil-safe — production code
	// injects via SetReplicatorMetrics; existing tests work unchanged.
	metrics metricsHolder

	// Transport for peer communication
	transport Transport
}

// Transport is the interface for peer-to-peer communication.
// This allows mocking in tests.
type Transport interface {
	// Connect establishes a connection to a peer.
	Connect(ctx context.Context, addr string) (PeerConnection, error)

	// Listen starts accepting connections from peers.
	Listen(ctx context.Context, addr string, handler ConnectionHandler) error

	// Close shuts down the transport.
	Close() error
}

// PeerConnection represents a connection to a peer node.
type PeerConnection interface {
	// SendWALBatch sends a batch of WAL entries to the peer.
	SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error)

	// SendHeartbeat sends a heartbeat to the peer.
	SendHeartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error)

	// SendFence sends a fence request to prevent split-brain.
	SendFence(ctx context.Context, req *FenceRequest) (*FenceResponse, error)

	// SendPromote notifies the peer to prepare for promotion.
	SendPromote(ctx context.Context, req *PromoteRequest) (*PromoteResponse, error)

	// SendRaftVote sends a Raft vote request and returns the response.
	SendRaftVote(ctx context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error)

	// SendRaftAppendEntries sends Raft append entries and returns the response.
	SendRaftAppendEntries(ctx context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error)

	// Close closes the connection.
	Close() error

	// IsConnected returns true if the connection is active.
	IsConnected() bool
}

// RaftVoteRequest is a Raft RequestVote RPC request.
type RaftVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RaftVoteResponse is a Raft RequestVote RPC response.
type RaftVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     string `json:"voter_id"`
}

// RaftAppendEntriesRequest is a Raft AppendEntries RPC request.
type RaftAppendEntriesRequest struct {
	Term         uint64          `json:"term"`
	LeaderID     string          `json:"leader_id"`
	LeaderAddr   string          `json:"leader_addr"`
	PrevLogIndex uint64          `json:"prev_log_index"`
	PrevLogTerm  uint64          `json:"prev_log_term"`
	Entries      []*RaftLogEntry `json:"entries"`
	LeaderCommit uint64          `json:"leader_commit"`
	CodecVersion uint32          `json:"codec_version,omitempty"`
}

// RaftAppendEntriesResponse is a Raft AppendEntries RPC response.
type RaftAppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"match_index"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ResponderID   string `json:"responder_id"`
	CodecVersion  uint32 `json:"codec_version,omitempty"`
}

// ConnectionHandler handles incoming connections. ClusterTransport calls it
// once per inbound connection, after the first message read from it has been
// verified and before that message is dispatched.
type ConnectionHandler func(conn PeerConnection)

// WALBatchResponse is the response to a WAL batch.
type WALBatchResponse struct {
	AckedPosition    uint64
	ReceivedPosition uint64
}

// HeartbeatRequest is a heartbeat message.
type HeartbeatRequest struct {
	NodeID      string
	Role        string
	WALPosition uint64
	Timestamp   int64
}

// HeartbeatResponse is the response to a heartbeat.
type HeartbeatResponse struct {
	NodeID      string
	Role        string
	WALPosition uint64
	Lag         int64
}

// FenceRequest requests the peer to stop accepting writes.
type FenceRequest struct {
	Reason    string
	RequestID string
}

// FenceResponse is the response to a fence request.
type FenceResponse struct {
	Fenced bool
}

// PromoteRequest notifies the peer to prepare for promotion.
type PromoteRequest struct {
	Reason string
}

// PromoteResponse is the response to a promote request.
type PromoteResponse struct {
	Ready bool
}

// NewHAStandbyReplicator creates a new HA standby replicator.
func NewHAStandbyReplicator(config *Config, storage Storage) (*HAStandbyReplicator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	r := &HAStandbyReplicator{
		config:  config,
		storage: storage,
		role:    config.HAStandby.Role,
	}

	r.isPrimary.Store(config.HAStandby.Role == "primary")

	// Create WAL components
	r.walStreamer = NewWALStreamer(storage, config.HAStandby.WALBatchSize)
	r.walApplier = NewWALApplier(storage)

	return r, nil
}

// LifetimeContext returns the HAStandbyReplicator owner's immutable shutdown context.
func (r *HAStandbyReplicator) LifetimeContext() context.Context { return r.work.Context() }

// SetTransport sets the transport for peer communication.
//
// Safe to call before or after Start(). The chaos / partition-recovery
// tests swap transports on a running replicator to simulate network
// failure and recovery; r.mu serializes those writes against the
// connectToStandbyLoop / Listen reads of r.transport.
func (r *HAStandbyReplicator) SetTransport(t Transport) {
	r.mu.Lock()
	r.transport = t
	r.mu.Unlock()
}

// currentRole returns the role under r.mu: HandleFence and Promote change it
// while the heartbeat loop and RPC handlers read it.
func (r *HAStandbyReplicator) currentRole() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

// SetReplicatorMetrics implements MetricsAware — Plan-04-06 D-15a
// observation seam. Idempotent. Calling with nil disables observation.
func (r *HAStandbyReplicator) SetReplicatorMetrics(bag *observability.ReplicationMetrics, tracker *PeerTracker) {
	r.metrics.set(newReplicatorMetrics(bag, tracker))
}

// Start initializes and starts the HA standby replicator.
func (r *HAStandbyReplicator) Start(ctx context.Context) (startErr error) {
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
	r.started.Store(true)
	r.mu.Unlock()
	release, accepted := r.enter()
	if !accepted {
		r.mu.Lock()
		r.started.Store(false)
		r.starting = nil
		r.mu.Unlock()
		attempt.Do(func() error { return ErrClosed })
		return ErrClosed
	}
	defer func() {
		r.mu.Lock()
		if startErr != nil {
			r.started.Store(false)
		}
		r.starting = nil
		r.mu.Unlock()
		attempt.Do(func() error { return startErr })
		release()
	}()
	// Initialization observes its caller; running work belongs to the
	// immutable context created before this replicator was published.
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.work.Closing() {
		return ErrClosed
	}

	// Use default transport if not set. r.mu serializes against any
	// concurrent SetTransport (chaos tests do this on a running
	// replicator to simulate network failures).
	r.mu.Lock()
	if r.transport == nil {
		transport, err := NewDefaultTransportFromConfig(r.config)
		if err != nil {
			r.mu.Unlock()
			return localizedError(localization.ReplicationHAInitTransportFailed(err), err)
		}
		r.transport = transport
	}
	r.mu.Unlock()

	r.config.logPrintf(ctx, slog.LevelInfo, "[HA] Starting in role=%s peer=%s sync_mode=%s wal_batch_size=%d wal_batch_timeout=%s",
		r.config.HAStandby.Role,
		r.config.HAStandby.PeerAddr,
		r.config.HAStandby.SyncMode,
		r.config.HAStandby.WALBatchSize,
		r.config.HAStandby.WALBatchTimeout,
	)
	if r.work.Closing() {
		return ErrClosed
	}

	ctx = r.work.Context()
	if r.isPrimary.Load() {
		if err := r.startPrimary(ctx); err != nil {
			r.started.Store(false)
			return localizedError(localization.ReplicationHAStartPrimaryFailed(err), err)
		}
	} else {
		if err := r.startStandby(ctx); err != nil {
			r.started.Store(false)
			return localizedError(localization.ReplicationHAStartStandbyFailed(err), err)
		}
	}

	role := r.currentRole()
	r.config.logPrintf(ctx, slog.LevelInfo, "[HA] Started as %s, peer: %s", role, r.config.HAStandby.PeerAddr)

	// Plan 04-06-03 D-15a: emit initial role gauge at the same lifecycle
	// log site that announces start-up. HA primary maps to "leader",
	// standby maps to "standby".
	roleStr := "standby"
	if role == "primary" {
		roleStr = "leader"
	}
	r.metrics.get().observeRoleTransition(roleStr, 0, 0, 0)

	return nil
}

// startPrimary starts the primary node.
func (r *HAStandbyReplicator) startPrimary(ctx context.Context) error {
	// Connect to standby
	r.goWork(func() { r.connectToStandbyLoop(ctx) })

	// Start WAL streaming
	r.goWork(func() { r.streamWALLoop(ctx) })

	// Start heartbeat
	r.goWork(func() { r.primaryHeartbeatLoop(ctx) })

	return nil
}

// startStandby starts the standby node.
func (r *HAStandbyReplicator) startStandby(ctx context.Context) error {
	// Start listening for incoming connections
	r.goWork(func() { r.listenForPrimary(ctx) })

	// Start primary health monitoring
	r.goWork(func() { r.monitorPrimaryHealth(ctx) })

	return nil
}

// connectToStandbyLoop continuously tries to connect to the standby.
func (r *HAStandbyReplicator) connectToStandbyLoop(ctx context.Context) {

	backoff := r.config.HAStandby.ReconnectInterval

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.work.Context().Done():
			return
		default:
		}

		r.mu.RLock()
		current := r.standbyConn
		r.mu.RUnlock()
		if current != nil && current.IsConnected() {
			time.Sleep(r.config.HAStandby.HeartbeatInterval)
			continue
		}

		r.config.logPrintf(ctx, slog.LevelInfo, "[HA Primary] Connecting to standby: %s", r.config.HAStandby.PeerAddr)

		r.mu.RLock()
		transport := r.transport
		r.mu.RUnlock()
		conn, err := transport.Connect(ctx, r.config.HAStandby.PeerAddr)
		if err != nil {
			r.config.logPrintf(ctx, slog.LevelWarn, "[HA Primary] Failed to connect to standby: %v", err)
			time.Sleep(backoff)
			backoff = min(backoff*2, r.config.HAStandby.MaxReconnectBackoff)
			continue
		}

		r.mu.Lock()
		r.standbyConn = conn
		r.mu.Unlock()

		backoff = r.config.HAStandby.ReconnectInterval
		r.config.logPrintf(ctx, slog.LevelInfo, "[HA Primary] Connected to standby")
	}
}

// streamWALLoop continuously streams WAL to standby.
func (r *HAStandbyReplicator) streamWALLoop(ctx context.Context) {

	ticker := time.NewTicker(r.config.HAStandby.WALBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.work.Context().Done():
			return
		case <-ticker.C:
			r.streamPendingWAL(ctx)
		}
	}
}

// streamPendingWAL sends pending WAL entries to standby.
func (r *HAStandbyReplicator) streamPendingWAL(ctx context.Context) {
	r.mu.RLock()
	conn := r.standbyConn
	r.mu.RUnlock()

	if conn == nil || !conn.IsConnected() {
		return
	}

	entries, err := r.walStreamer.GetPendingEntries(r.config.HAStandby.WALBatchSize)
	if err != nil || len(entries) == 0 {
		return
	}

	resp, err := conn.SendWALBatch(ctx, entries)
	if err != nil {
		r.config.logPrintf(ctx, slog.LevelError, "[HA Primary] Failed to send WAL batch: %v", err)
		return
	}

	r.walStreamer.AcknowledgePosition(resp.AckedPosition)
}

// primaryHeartbeatLoop sends heartbeats to standby.
func (r *HAStandbyReplicator) primaryHeartbeatLoop(ctx context.Context) {

	ticker := time.NewTicker(r.config.HAStandby.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.work.Context().Done():
			return
		case <-ticker.C:
			r.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat sends a heartbeat to the peer.
func (r *HAStandbyReplicator) sendHeartbeat(ctx context.Context) {
	r.mu.RLock()
	conn := r.standbyConn
	r.mu.RUnlock()

	if conn == nil || !conn.IsConnected() {
		return
	}

	walPos, _ := r.storage.GetWALPosition()

	req := &HeartbeatRequest{
		NodeID:      r.config.NodeID,
		Role:        r.currentRole(),
		WALPosition: walPos,
		Timestamp:   time.Now().UnixNano(),
	}

	_, err := conn.SendHeartbeat(ctx, req)
	if err != nil {
		r.config.logPrintf(ctx, slog.LevelWarn, "[HA Primary] Heartbeat failed: %v", err)
	}
}

// listenForPrimary listens for incoming connections from primary.
func (r *HAStandbyReplicator) listenForPrimary(ctx context.Context) {

	// Tie listener lifetime to shutdown signal as well as parent context.
	listenCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.goWork(func() {
		select {
		case <-r.work.Context().Done():
			cancel()
		case <-listenCtx.Done():
		}
	})

	handler := func(conn PeerConnection) {
		r.mu.Lock()
		r.primaryConn = conn
		r.primaryHealthy.Store(true)
		r.lastPrimaryHB = time.Now()
		r.mu.Unlock()

		r.config.logPrintf(ctx, slog.LevelInfo, "[HA Standby] Primary connected")
	}

	r.mu.RLock()
	transport := r.transport
	r.mu.RUnlock()
	if err := transport.Listen(listenCtx, r.config.BindAddr, handler); err != nil {
		if listenCtx.Err() == nil {
			r.config.logPrintf(ctx, slog.LevelError, "[HA Standby] Listen error: %v", err)
		}
	}
}

// monitorPrimaryHealth monitors primary health and triggers failover.
func (r *HAStandbyReplicator) monitorPrimaryHealth(ctx context.Context) {

	ticker := time.NewTicker(r.config.HAStandby.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.work.Context().Done():
			return
		case <-ticker.C:
			r.checkPrimaryHealth(ctx)
		}
	}
}

// checkPrimaryHealth checks if primary is healthy.
func (r *HAStandbyReplicator) checkPrimaryHealth(ctx context.Context) {
	r.mu.RLock()
	lastHB := r.lastPrimaryHB
	wasHealthy := r.primaryHealthy.Load()
	r.mu.RUnlock()

	timeSinceHB := time.Since(lastHB)

	if timeSinceHB > r.config.HAStandby.FailoverTimeout {
		if wasHealthy {
			r.config.logPrintf(ctx, slog.LevelWarn, "[HA Standby] Primary appears down, last heartbeat: %v ago", timeSinceHB)
			r.primaryHealthy.Store(false)

			if r.config.HAStandby.AutoFailover && !r.isPromoted.Load() {
				// monitorPrimaryHealth's admitted work enrolls this child before launch.
				r.goWork(func() {
					r.work.RecordError("", r.triggerAutoFailover(ctx))
				})
			}
		}
	}
}

// triggerAutoFailover initiates automatic promotion to primary.
func (r *HAStandbyReplicator) triggerAutoFailover(ctx context.Context) error {
	r.config.logPrintf(ctx, slog.LevelWarn, "[HA Standby] Initiating auto-failover")

	// Try to fence old primary
	fenceErr := r.fenceOldPrimary(ctx)

	// Promote self
	if err := r.Promote(ctx); err != nil {
		r.config.logPrintf(ctx, slog.LevelError, "[HA Standby] Auto-failover failed")
		return compose.Outcomes(fenceErr, err)
	}

	r.config.logPrintf(ctx, slog.LevelInfo, "[HA Standby] Auto-failover completed, now primary")
	return fenceErr
}

// fenceOldPrimary attempts to stop the old primary from accepting writes.
func (r *HAStandbyReplicator) fenceOldPrimary(ctx context.Context) error {
	r.mu.RLock()
	conn := r.primaryConn
	r.mu.RUnlock()

	if conn == nil {
		return nil
	}

	req := &FenceRequest{
		Reason:    "standby_promotion",
		RequestID: fmt.Sprintf("fence-%d", time.Now().UnixNano()),
	}

	if _, err := conn.SendFence(ctx, req); err != nil {
		r.config.logPrintf(ctx, slog.LevelWarn, "[HA Standby] Failed to fence old primary: %v", err)
		return fmt.Errorf("HA primary fencing: %w", err)
	}
	return nil
}

// Promote promotes this standby to primary.
//
// A manual Promote and the auto-failover goroutine can both get here; only
// one may promote, or the primary loops start twice. The others wait for it
// and return its result, so none reports success before the WAL is flushed
// and primary status is published. After a failure the next call retries.
func (r *HAStandbyReplicator) Promote(ctx context.Context) error {
	release, accepted := r.enter()
	if !accepted {
		return ErrClosed
	}
	defer release()
	if r.isPrimary.Load() {
		return nil
	}

	r.promoteMu.Lock()
	if p := r.promoting; p != nil {
		r.promoteMu.Unlock()
		<-p.Done()
		return p.Err()
	}
	if r.isPromoted.Load() {
		r.promoteMu.Unlock()
		return nil
	}
	p := &lifecycle.OnceCompletion{}
	r.promoting = p
	r.promoteMu.Unlock()

	err := r.promote(ctx)

	r.promoteMu.Lock()
	r.promoting = nil
	r.promoteMu.Unlock()
	return p.Do(func() error { return err })
}

// promote runs one promotion; Promote ensures only one runs at a time.
func (r *HAStandbyReplicator) promote(ctx context.Context) error {
	// Flush any pending WAL
	if err := r.walApplier.Flush(ctx); err != nil {
		return localizedError(localization.ReplicationHAFlushWALFailed(err), err)
	}

	r.mu.Lock()
	r.role = "primary"
	r.mu.Unlock()

	r.isPrimary.Store(true)
	r.isPromoted.Store(true)

	r.config.logPrintf(ctx, slog.LevelInfo, "[HA] Promoted to primary")

	// Plan 04-06-03 D-15a: emit role transition at the same site that
	// emits the "Promoted to primary" log line.
	r.metrics.get().observeRoleTransition("leader", 0, 0, 0)

	// Restart as primary
	return r.startPrimary(ctx)
}

// Apply applies a command to storage. Its native submission observes this
// replicator's work context.
func (r *HAStandbyReplicator) Apply(cmd *Command, timeout time.Duration) error {
	return r.applyEnrolled(r.work.Context(), cmd, timeout)
}

// applyEnrolled is Apply with the caller's work context.
func (r *HAStandbyReplicator) applyEnrolled(ctx context.Context, cmd *Command, timeout time.Duration) error {
	if err := checkApplyReadiness(r.work.Closing(), r.started.Load()); err != nil {
		return err
	}

	if !r.isPrimary.Load() {
		return ErrStandbyMode
	}

	if cmd != nil {
		if handled, err := routeLeaderEntityCommand(r.storage, r, cmd, timeout); handled || err != nil {
			return err
		}
	}
	traceWrites := os.Getenv("NORNICDB_CLUSTER_TRACE_WRITES") != ""
	start := time.Now()
	if err := r.storage.ApplyCommand(ctx, cmd); err != nil {
		return err
	}
	applyDur := time.Since(start)

	// Honor write-ack mode: optionally wait for standby acknowledgement.
	walPos, _ := r.storage.GetWALPosition()
	ackStart := time.Now()
	ackErr := r.waitForReplicationAck(walPos, timeout)
	ackDur := time.Since(ackStart)
	if traceWrites {
		r.config.logPrintf(context.Background(), slog.LevelInfo, "[HA Primary] Apply type=%d apply=%s ack=%s total=%s mode=%s wal=%d err=%v",
			cmd.Type, applyDur, ackDur, time.Since(start), r.config.HAStandby.SyncMode, walPos, ackErr)
	}
	return ackErr
}

// ApplyBatch applies multiple commands.
func (r *HAStandbyReplicator) ApplyBatch(cmds []*Command, timeout time.Duration) error {
	for _, cmd := range cmds {
		if err := r.Apply(cmd, timeout); err != nil {
			return err
		}
	}
	return nil
}

// waitForReplicationAck enforces the configured write-ack behavior after a local apply.
// For async mode, it returns immediately. For quorum mode, it waits until the
// standby has acknowledged WAL up to the target position or the timeout elapses.
func (r *HAStandbyReplicator) waitForReplicationAck(target uint64, timeout time.Duration) error {
	mode := r.config.HAStandby.SyncMode
	if mode == SyncAsync || target == 0 || r.walStreamer == nil {
		return nil
	}
	if mode != SyncQuorum {
		// Config validation should prevent this, but keep async semantics as a safe default.
		return nil
	}

	// Quorum: wait for ack up to timeout.
	// Default timeout if caller did not supply one.
	if timeout <= 0 {
		// Use a conservative bound: 2x heartbeat interval or 500ms minimum.
		timeout = r.config.HAStandby.HeartbeatInterval * 2
		if timeout == 0 {
			timeout = 500 * time.Millisecond
		} else if timeout < 250*time.Millisecond {
			timeout = 250 * time.Millisecond
		}
	}

	ctx, cancel := context.WithTimeout(r.work.Context(), timeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if r.walStreamer.LastAcked() >= target {
			return nil
		}

		// Proactively attempt to send any pending WAL to reduce wait time.
		r.streamPendingWAL(ctx)

		select {
		case <-ctx.Done():
			return localizedError(localization.ReplicationHAAckTimedOut(string(mode), target, ctx.Err()), ctx.Err())
		case <-ticker.C:
		}
	}
}

// IsLeader returns true if this is the primary.
func (r *HAStandbyReplicator) IsLeader() bool {
	return r.isPrimary.Load()
}

// LeaderAddr returns the primary address.
func (r *HAStandbyReplicator) LeaderAddr() string {
	if r.isPrimary.Load() {
		return r.config.AdvertiseAddr
	}
	return r.config.HAStandby.PeerAddr
}

// LeaderID returns the primary's node ID.
func (r *HAStandbyReplicator) LeaderID() string {
	if r.isPrimary.Load() {
		return r.config.NodeID
	}
	return "" // Unknown until we receive heartbeat
}

// Health returns health status.
func (r *HAStandbyReplicator) Health() *HealthStatus {
	r.mu.RLock()
	role, lastContact, standby := r.role, r.lastPrimaryHB, r.standbyConn
	r.mu.RUnlock()

	state := "initializing"
	if r.started.Load() {
		state = "ready"
	}
	if r.work.Closing() {
		state = "closed"
	}

	if r.isPromoted.Load() && role == "standby" {
		role = "primary (promoted)"
	}

	return &HealthStatus{
		Mode:        ModeHAStandby,
		NodeID:      r.config.NodeID,
		Role:        role,
		IsLeader:    r.isPrimary.Load(),
		State:       state,
		Healthy:     r.started.Load() && !r.work.Closing(),
		LastContact: lastContact,
		Peers: []PeerStatus{
			{
				ID:          "peer",
				Address:     r.config.HAStandby.PeerAddr,
				Healthy:     standby != nil && standby.IsConnected() || r.primaryHealthy.Load(),
				LastContact: lastContact,
			},
		},
	}
}

// WaitForLeader blocks until primary is available.
func (r *HAStandbyReplicator) WaitForLeader(ctx context.Context) error {
	if r.isPrimary.Load() {
		return nil
	}

	// For standby, wait until we're connected to primary
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if r.primaryHealthy.Load() || r.isPrimary.Load() {
				return nil
			}
		}
	}
}

// Shutdown stops the replicator.
func (r *HAStandbyReplicator) Shutdown() error {
	return r.work.Shutdown(context.Background(), r.retire)
}

// retire runs once, after every accepted unit of work has returned: it closes
// the connections and transport the loops used.
func (r *HAStandbyReplicator) retire() error {
	r.mu.Lock()
	standby, primary := r.standbyConn, r.primaryConn
	transport := r.transport
	r.mu.Unlock()

	var closeErr error
	if standby != nil {
		if err := standby.Close(); err != nil {
			closeErr = compose.Outcomes(closeErr, fmt.Errorf("HA standby connection close: %w", err))
		}
	}
	if primary != nil {
		if err := primary.Close(); err != nil {
			closeErr = compose.Outcomes(closeErr, fmt.Errorf("HA primary connection close: %w", err))
		}
	}
	if transport != nil {
		if err := transport.Close(); err != nil {
			closeErr = compose.Outcomes(closeErr, fmt.Errorf("HA transport close: %w", err))
		}
	}
	return closeErr
}

// enter admits one unit of accepted work. It is refused once shutdown has
// sealed admission; the release is called on the work's actual return.
func (r *HAStandbyReplicator) enter() (func(), bool) {
	_, release, accepted := r.work.Acquire(r.work.Context())
	return release, accepted
}

// goWork launches fn as accepted work: enrolled before the goroutine exists,
// released when fn actually returns. It reports false once admission is sealed.
func (r *HAStandbyReplicator) goWork(fn func()) bool {
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
func (r *HAStandbyReplicator) Mode() ReplicationMode {
	return ModeHAStandby
}

// NodeID returns this node's ID.
func (r *HAStandbyReplicator) NodeID() string {
	return r.config.NodeID
}

// HandleWALBatch handles incoming WAL batch (for standby).
func (r *HAStandbyReplicator) HandleWALBatch(entries []*WALEntry) (*WALBatchResponse, error) {
	if r.isPrimary.Load() {
		return nil, localizedError(localization.ReplicationHAPrimaryCannotReceiveWAL(), nil)
	}

	r.mu.Lock()
	r.lastPrimaryHB = time.Now()
	r.primaryHealthy.Store(true)
	r.mu.Unlock()

	lastApplied, err := r.walApplier.ApplyBatch(r.work.Context(), entries)
	if err != nil {
		return nil, err
	}

	return &WALBatchResponse{
		AckedPosition:    lastApplied,
		ReceivedPosition: lastApplied,
	}, nil
}

// HandleHeartbeat handles incoming heartbeat (for standby).
func (r *HAStandbyReplicator) HandleHeartbeat(req *HeartbeatRequest) (*HeartbeatResponse, error) {
	r.mu.Lock()
	r.lastPrimaryHB = time.Now()
	r.primaryHealthy.Store(true)
	r.mu.Unlock()

	walPos, _ := r.storage.GetWALPosition()
	lag := int64(req.WALPosition) - int64(walPos)
	if lag < 0 {
		lag = 0
	}

	return &HeartbeatResponse{
		NodeID:      r.config.NodeID,
		Role:        r.currentRole(),
		WALPosition: walPos,
		Lag:         lag,
	}, nil
}

// HandleFence handles incoming fence request (for primary).
func (r *HAStandbyReplicator) HandleFence(req *FenceRequest) (*FenceResponse, error) {
	r.config.logPrintf(context.Background(), slog.LevelWarn, "[HA Primary] Received fence request: %s", req.Reason)

	// Stop accepting writes
	r.isPrimary.Store(false)
	r.mu.Lock()
	r.role = "fenced"
	r.mu.Unlock()

	return &FenceResponse{Fenced: true}, nil
}

// HandlePromote handles an incoming promote request from the peer.
// Today this is used as a lightweight coordination hook; the actual promotion
// decision remains local (based on health/auto-failover config).
func (r *HAStandbyReplicator) HandlePromote(req *PromoteRequest) (*PromoteResponse, error) {
	_ = req
	return &PromoteResponse{Ready: true}, nil
}

// Ensure HAStandbyReplicator implements Replicator.
var _ Replicator = (*HAStandbyReplicator)(nil)

// WALStreamer manages WAL streaming from primary.
type WALStreamer struct {
	storage      Storage
	batchSize    int
	lastAckedPos uint64
	mu           sync.Mutex
}

// NewWALStreamer creates a new WAL streamer.
func NewWALStreamer(storage Storage, batchSize int) *WALStreamer {
	return &WALStreamer{
		storage:   storage,
		batchSize: batchSize,
	}
}

// GetPendingEntries returns WAL entries that haven't been acknowledged.
func (w *WALStreamer) GetPendingEntries(maxEntries int) ([]*WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.storage.GetWALEntries(w.lastAckedPos, maxEntries)
}

// AcknowledgePosition marks entries up to this position as acknowledged.
func (w *WALStreamer) AcknowledgePosition(pos uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if pos > w.lastAckedPos {
		w.lastAckedPos = pos
	}

	// Allow storage to discard already-acknowledged in-memory WAL entries.
	if pruner, ok := w.storage.(interface{ PruneWALEntries(uptoPosition uint64) }); ok {
		pruner.PruneWALEntries(pos)
	}
}

// LastAcked returns the last acknowledged WAL position.
func (w *WALStreamer) LastAcked() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastAckedPos
}

// WALApplier applies WAL entries to storage.
type WALApplier struct {
	storage      Storage
	lastApplied  uint64
	pendingBatch []*WALEntry
	mu           sync.Mutex
}

// NewWALApplier creates a new WAL applier.
func NewWALApplier(storage Storage) *WALApplier {
	return &WALApplier{storage: storage}
}

// ApplyBatch applies a batch of WAL entries; ctx is the caller's work context.
func (a *WALApplier) ApplyBatch(ctx context.Context, entries []*WALEntry) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var lastApplied uint64
	for _, entry := range entries {
		if entry.Position <= a.lastApplied {
			continue // Already applied
		}

		if err := a.storage.ApplyCommand(ctx, entry.Command); err != nil {
			return lastApplied, err
		}

		a.lastApplied = entry.Position
		lastApplied = entry.Position
	}

	return lastApplied, nil
}

// Flush ensures all pending entries are applied; ctx is the caller's work
// context.
func (a *WALApplier) Flush(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, entry := range a.pendingBatch {
		if err := a.storage.ApplyCommand(ctx, entry.Command); err != nil {
			return err
		}
	}

	a.pendingBatch = nil
	return nil
}

// NewDefaultTransport creates a ClusterTransport from a transport config.
// See transport.go for the full implementation.
//
// Deprecated: it applies only what config carries, not the replication
// secret or TLS settings of a replication Config. Build a node's transport
// with NewDefaultTransportFromConfig.
func NewDefaultTransport(config *ClusterTransportConfig) Transport {
	return NewClusterTransport(config)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
