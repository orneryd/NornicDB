package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// RaftState represents the current state of a Raft node.
type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
)

func (s RaftState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// CurrentCodecVersion is the codec generation this binary speaks. Pre-version
// peers omit the field (wire value 0). Version 1 is the first versioned frame
// that supports the optional traceparent field added in Phase 8.
const CurrentCodecVersion uint32 = 1

// RaftLogEntry represents an entry in the Raft log.
type RaftLogEntry struct {
	Index   uint64   `json:"index"`
	Term    uint64   `json:"term"`
	Command *Command `json:"command"`
}

// VoteRequest is sent by candidates to request votes.
type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// VoteResponse is the response to a vote request.
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     string `json:"voter_id"`
}

// AppendEntriesRequest is sent by the leader to replicate log entries.
type AppendEntriesRequest struct {
	Term         uint64          `json:"term"`
	LeaderID     string          `json:"leader_id"`
	LeaderAddr   string          `json:"leader_addr"`
	PrevLogIndex uint64          `json:"prev_log_index"`
	PrevLogTerm  uint64          `json:"prev_log_term"`
	Entries      []*RaftLogEntry `json:"entries"`
	LeaderCommit uint64          `json:"leader_commit"`
	// CodecVersion identifies the wire format generation (TRC-21/TRC-22).
	// 0 (or absent via omitempty) = pre-version frame; 1 = first versioned frame.
	// Rolling-upgrade compat: receivers treat absent/0 as pre-version and accept
	// gracefully; leaders omit the field when any peer is at version 0.
	CodecVersion uint32 `json:"codec_version,omitempty"`
}

// AppendEntriesResponse is the response to an append entries request.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"match_index"`
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
	ResponderID   string `json:"responder_id"`
	// CodecVersion echoes the responder's supported codec version so the leader
	// can track peer capabilities for mixed-cluster compat (TRC-22).
	CodecVersion uint32 `json:"codec_version,omitempty"`
}

// RaftRPCType identifies the type of Raft RPC message.
type RaftRPCType uint8

const (
	RPCVoteRequest RaftRPCType = iota + 1
	RPCVoteResponse
	RPCAppendEntries
	RPCAppendEntriesResponse
)

// RaftRPCMessage wraps all Raft RPC messages for transport.
type RaftRPCMessage struct {
	Type    RaftRPCType `json:"type"`
	Payload []byte      `json:"payload"`
}

// RaftReplicator implements full Raft consensus-based replication.
// This provides strong consistency with automatic leader election.
type RaftReplicator struct {
	config  *Config
	storage Storage

	// Raft state (protected by mu)
	mu          sync.RWMutex
	state       RaftState
	currentTerm uint64
	votedFor    string
	leaderID    string
	leaderAddr  string
	starting    *lifecycleAttempt
	shutdownErr error
	workContext context.Context
	stopWork    context.CancelFunc

	// applyMu serializes physical application without holding logMu across
	// storage callbacks. Acquire it before logMu.
	applyMu sync.Mutex

	// Log (protected by logMu)
	logMu       sync.RWMutex
	log         []*RaftLogEntry
	commitIndex uint64
	lastApplied uint64
	applyErr    error // committed entry awaiting successful state-machine application

	// Peer state (leader only) and config.Raft.Peers (protected by peerMu).
	// Lock order: mu, then logMu, then peerMu.
	peerMu            sync.RWMutex
	nextIndex         map[string]uint64
	matchIndex        map[string]uint64
	peerConns         map[string]PeerConnection
	peerCodecVersions map[string]uint32

	// Pending futures for in-flight applies
	futuresMu sync.Mutex
	futures   map[uint64]*applyFuture

	// Transport
	transport Transport

	// Channels
	stopCh      chan struct{}
	applyCh     chan *applyFuture
	commitCh    chan struct{}
	heartbeatCh chan struct{}
	rpcCh       chan *rpcRequest

	// State tracking
	started atomic.Bool
	closed  atomic.Bool
	wg      sync.WaitGroup

	// Random source for election timeouts
	rand *rand.Rand

	// metrics is the Plan-04-06 observation seam (D-15a per-event role
	// gauge updates + per-peer lag/RTT/last_contact). nil-safe — production
	// code injects via SetReplicatorMetrics; existing tests work unchanged.
	metrics metricsHolder
}

// SetReplicatorMetrics implements MetricsAware — Plan-04-06 D-15a
// observation seam. Idempotent. Calling with nil disables observation.
func (r *RaftReplicator) SetReplicatorMetrics(bag *observability.ReplicationMetrics, tracker *PeerTracker) {
	r.metrics.set(newReplicatorMetrics(bag, tracker))
}

// peerCodecVersion returns the codec version to use when sending to peerID.
// If the peer has never responded (version 0 / unknown), we send at version 0
// (pre-version frame) for rolling-upgrade safety. Once a peer echoes a version
// >= CurrentCodecVersion, we send at CurrentCodecVersion.
func (r *RaftReplicator) peerCodecVersion(peerID string) uint32 {
	r.peerMu.RLock()
	v := r.peerCodecVersions[peerID]
	r.peerMu.RUnlock()
	if v >= CurrentCodecVersion {
		return CurrentCodecVersion
	}
	return 0
}

// peerConfigs returns a copy of the configured peers. AddVoter and
// RemoveServer rewrite config.Raft.Peers under peerMu.
func (r *RaftReplicator) peerConfigs() []PeerConfig {
	r.peerMu.RLock()
	defer r.peerMu.RUnlock()
	peers := make([]PeerConfig, len(r.config.Raft.Peers))
	copy(peers, r.config.Raft.Peers)
	return peers
}

// peerCount returns the number of configured peers.
func (r *RaftReplicator) peerCount() int {
	r.peerMu.RLock()
	defer r.peerMu.RUnlock()
	return len(r.config.Raft.Peers)
}

// applyFuture represents a pending apply operation.
type applyFuture struct {
	cmd      *Command
	index    uint64
	term     uint64
	errCh    chan error
	accepted *raftAcceptedOutcome
}

// rpcRequest represents an incoming RPC request.
type rpcRequest struct {
	message *RaftRPCMessage
	conn    PeerConnection
	respCh  chan []byte
}

// HandleRaftVote handles an incoming RequestVote RPC via the cluster transport.
func (r *RaftReplicator) HandleRaftVote(req *RaftVoteRequest) (*RaftVoteResponse, error) {
	r.mu.RLock()
	if r.closed.Load() {
		r.mu.RUnlock()
		return nil, ErrClosed
	}
	r.wg.Add(1)
	r.mu.RUnlock()
	defer r.wg.Done()
	if req == nil {
		return nil, localizedError(localization.ReplicationRaftVoteRequestRequired(), nil)
	}
	resp := r.handleVoteRequest(&VoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &RaftVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
		VoterID:     resp.VoterID,
	}, nil
}

// HandleRaftAppendEntries handles an incoming AppendEntries RPC via the cluster transport.
func (r *RaftReplicator) HandleRaftAppendEntries(req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	r.mu.RLock()
	if r.closed.Load() {
		r.mu.RUnlock()
		return nil, ErrClosed
	}
	r.wg.Add(1)
	r.mu.RUnlock()
	defer r.wg.Done()
	if req == nil {
		return nil, localizedError(localization.ReplicationRaftAppendRequestRequired(), nil)
	}
	resp := r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		LeaderAddr:   req.LeaderAddr,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      req.Entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &RaftAppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		MatchIndex:    resp.MatchIndex,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
		ResponderID:   resp.ResponderID,
	}, nil
}

// NewRaftReplicator creates a new Raft replicator.
func NewRaftReplicator(config *Config, storage Storage) (*RaftReplicator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	workContext, stopWork := context.WithCancel(context.Background())
	r := &RaftReplicator{
		workContext: workContext,
		stopWork:    stopWork,
		config:      config,
		storage:     storage,
		state:       StateFollower,
		currentTerm: 0,
		votedFor:    "",
		leaderID:    "",
		leaderAddr:  "",
		log:         make([]*RaftLogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		peerConns:   make(map[string]PeerConnection),
		futures:     make(map[uint64]*applyFuture),
		stopCh:      make(chan struct{}),
		applyCh:     make(chan *applyFuture, 256),
		commitCh:    make(chan struct{}, 1),
		heartbeatCh: make(chan struct{}, 1),
		rpcCh:       make(chan *rpcRequest, 256),
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Initialize log with a dummy entry at index 0 (standard Raft practice)
	r.log = append(r.log, &RaftLogEntry{Index: 0, Term: 0, Command: nil})

	return r, nil
}

// LifetimeContext returns the RaftReplicator owner's immutable shutdown context.
func (r *RaftReplicator) LifetimeContext() context.Context { return r.workContext }

// SetTransport sets the transport for peer communication.
func (r *RaftReplicator) SetTransport(t Transport) {
	r.mu.Lock()
	r.transport = t
	r.mu.Unlock()
}

// Start initializes and starts the Raft replicator.
func (r *RaftReplicator) Start(ctx context.Context) (startErr error) {
	r.mu.Lock()
	if r.closed.Load() {
		r.mu.Unlock()
		return ErrClosed
	}
	if attempt := r.starting; attempt != nil {
		r.mu.Unlock()
		<-attempt.done
		return attempt.err
	}
	if r.started.Load() {
		r.mu.Unlock()
		return nil
	}
	attempt := &lifecycleAttempt{done: make(chan struct{})}
	r.starting = attempt
	transport := r.transport
	r.wg.Add(1)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		attempt.err = startErr
		r.starting = nil
		close(attempt.done)
		r.mu.Unlock()
		r.wg.Done()
	}()
	// Initialization observes its caller; running work belongs to the
	// immutable context created before this replicator was published.
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.closed.Load() {
		return ErrClosed
	}

	r.config.logPrintf(ctx, slog.LevelInfo, "[Raft %s] Starting node", r.config.NodeID)

	if transport == nil {
		if r.closed.Load() {
			return ErrClosed
		}
		created, err := NewDefaultTransportFromConfig(r.config)
		if err != nil {
			return localizedError(localization.ReplicationRaftInitTransportFailed(err), err)
		}
		r.mu.Lock()
		if r.closed.Load() {
			r.mu.Unlock()
			closeErr := created.Close()
			r.mu.Lock()
			r.shutdownErr = errors.Join(r.shutdownErr, closeErr)
			r.mu.Unlock()
			return errors.Join(ErrClosed, closeErr)
		}
		r.transport = created
		r.mu.Unlock()
	}
	if r.closed.Load() {
		return ErrClosed
	}

	// Initialize peer tracking for all configured peers
	r.peerMu.Lock()
	for _, peer := range r.config.Raft.Peers {
		r.nextIndex[peer.ID] = 1
		r.matchIndex[peer.ID] = 0
	}
	r.peerMu.Unlock()

	// Start background goroutines
	ctx = r.workContext
	r.wg.Add(5)
	go r.runElectionTimer(ctx)
	go r.runApplyLoop(ctx)
	go r.runCommitLoop(ctx)
	go r.runPeerConnector(ctx)
	go r.runRPCHandler(ctx)

	// If bootstrap mode with no peers, immediately become leader
	if r.config.Raft.Bootstrap && r.peerCount() == 0 {
		r.mu.Lock()
		r.state = StateLeader
		r.leaderID = r.config.NodeID
		r.leaderAddr = r.config.AdvertiseAddr
		r.currentTerm = 1
		r.mu.Unlock()
		r.config.logPrintf(ctx, slog.LevelInfo, "[Raft %s] Bootstrap: became leader (term 1)", r.config.NodeID)
		// Plan 04-06-03 D-15a: observe role transition at the same log site.
		r.metrics.get().observeRoleTransition("leader", 1, 0, 0)
	}

	// Start listening for peer connections if transport available
	if r.transport != nil {
		r.wg.Add(1)
		go r.listenForPeers(ctx)
	}

	r.started.Store(true)
	r.config.logPrintf(ctx, slog.LevelInfo, "[Raft %s] Started in %s state", r.config.NodeID, r.getState())

	return nil
}

// runElectionTimer manages election timeouts and triggers elections.
func (r *RaftReplicator) runElectionTimer(ctx context.Context) {
	defer r.wg.Done()

	minTimeout := r.config.Raft.ElectionTimeout
	if minTimeout <= 0 {
		minTimeout = time.Second // Sensible default
	}
	maxTimeout := minTimeout * 2

	randomTimeout := func() time.Duration {
		diff := int64(maxTimeout - minTimeout)
		if diff <= 0 {
			return minTimeout
		}
		return minTimeout + time.Duration(r.rand.Int63n(diff))
	}

	timer := time.NewTimer(randomTimeout())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-r.heartbeatCh:
			// Reset timer when we receive heartbeat from leader
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomTimeout())
		case <-timer.C:
			r.mu.RLock()
			state := r.state
			r.mu.RUnlock()

			if state != StateLeader {
				// Election timeout - start new election
				r.startElection(ctx)
			}
			timer.Reset(randomTimeout())
		}
	}
}

// startElection initiates a leader election.
func (r *RaftReplicator) startElection(ctx context.Context) {
	r.mu.Lock()
	r.state = StateCandidate
	r.currentTerm++
	term := r.currentTerm
	r.votedFor = r.config.NodeID // Vote for self
	r.mu.Unlock()

	r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Starting election for term %d", r.config.NodeID, term)

	// Get last log info for vote request
	r.logMu.RLock()
	lastLogIndex := r.log[len(r.log)-1].Index
	lastLogTerm := r.log[len(r.log)-1].Term
	r.logMu.RUnlock()

	// Count votes - we already voted for ourselves
	var votesMu sync.Mutex
	votesReceived := 1
	peers := r.peerConfigs()
	totalVoters := len(peers) + 1
	votesNeeded := totalVoters/2 + 1

	// If single node, we win immediately
	if len(peers) == 0 {
		r.becomeLeader(ctx, term)
		return
	}

	// Request votes from all peers in parallel
	voteReq := &VoteRequest{
		Term:         term,
		CandidateID:  r.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		r.wg.Add(1)
		go func(p PeerConfig) {
			defer wg.Done()
			defer r.wg.Done()
			r.requestVoteFromPeer(ctx, p, voteReq, term, &votesMu, &votesReceived, votesNeeded)
		}(peer)
	}

	// Wait for vote requests with timeout
	done := make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(r.config.Raft.ElectionTimeout):
		r.config.logPrintf(context.Background(), slog.LevelWarn, "[Raft %s] Election timed out for term %d", r.config.NodeID, term)
	case <-ctx.Done():
	case <-r.stopCh:
	}
}

// requestVoteFromPeer sends a vote request to a single peer.
func (r *RaftReplicator) requestVoteFromPeer(ctx context.Context, peer PeerConfig, req *VoteRequest, term uint64, votesMu *sync.Mutex, votesReceived *int, votesNeeded int) {
	// Get or establish connection
	conn, err := r.getOrConnectPeer(ctx, peer)
	if err != nil {
		r.config.logPrintf(context.Background(), slog.LevelWarn, "[Raft %s] Cannot connect to %s for vote: %v", r.config.NodeID, peer.ID, err)
		return
	}

	// Send vote request
	resp, err := r.sendVoteRequestRPC(ctx, conn, req)
	if err != nil {
		r.config.logPrintf(context.Background(), slog.LevelWarn, "[Raft %s] Vote request to %s failed: %v", r.config.NodeID, peer.ID, err)
		return
	}

	// Process response
	r.mu.Lock()

	// Check if we've moved on (higher term discovered or already won/lost)
	if r.closed.Load() || r.currentTerm != term || r.state != StateCandidate {
		r.mu.Unlock()
		return
	}

	// If response has higher term, step down
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.state = StateFollower
		r.votedFor = ""
		r.leaderID = ""
		r.leaderAddr = ""
		r.mu.Unlock()
		r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Stepping down: discovered higher term %d from %s", r.config.NodeID, resp.Term, peer.ID)
		// Plan 04-06-03 D-15a: observe role transition. commitIndex /
		// lastApplied are protected by logMu (separate from r.mu) — read
		// under RLock to satisfy -race.
		r.logMu.RLock()
		ci, ai := r.commitIndex, r.lastApplied
		r.logMu.RUnlock()
		r.metrics.get().observeRoleTransition("follower", resp.Term, ci, ai)
		return
	}

	if resp.VoteGranted {
		votesMu.Lock()
		*votesReceived++
		votes := *votesReceived
		votesMu.Unlock()
		lead := votes >= votesNeeded && r.state == StateCandidate && r.currentTerm == term
		r.mu.Unlock()

		r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Received vote from %s (%d/%d needed)", r.config.NodeID, resp.VoterID, votes, votesNeeded)

		if lead {
			r.becomeLeader(ctx, term)
		}
		return
	}
	r.mu.Unlock()
}

// sendVoteRequestRPC sends a vote request and waits for response.
func (r *RaftReplicator) sendVoteRequestRPC(ctx context.Context, conn PeerConnection, req *VoteRequest) (*VoteResponse, error) {
	// Convert to transport type
	raftReq := &RaftVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	// Send actual Raft vote request via peer connection
	raftResp, err := conn.SendRaftVote(ctx, raftReq)
	if err != nil {
		return nil, localizedError(localization.ReplicationRaftSendVoteFailed(err), err)
	}

	// Convert response back to internal type
	return &VoteResponse{
		Term:        raftResp.Term,
		VoteGranted: raftResp.VoteGranted,
		VoterID:     raftResp.VoterID,
	}, nil
}

// becomeLeader transitions this node to leader state.
func (r *RaftReplicator) becomeLeader(ctx context.Context, term uint64) {
	r.mu.Lock()
	if r.closed.Load() || r.currentTerm != term || r.state == StateLeader {
		r.mu.Unlock()
		return
	}

	r.state = StateLeader
	r.leaderID = r.config.NodeID
	r.leaderAddr = r.config.AdvertiseAddr

	// Initialize nextIndex for all peers to last log index + 1
	r.logMu.RLock()
	lastIndex := r.log[len(r.log)-1].Index
	r.logMu.RUnlock()

	r.peerMu.Lock()
	for _, peer := range r.config.Raft.Peers {
		r.nextIndex[peer.ID] = lastIndex + 1
		r.matchIndex[peer.ID] = 0
	}
	r.peerMu.Unlock()

	r.mu.Unlock()

	r.config.logEvent(context.Background(), slog.LevelInfo,
		localization.ReplicationRaftBecameLeaderEvent(r.config.NodeID, term))

	// Plan 04-06-03 D-15a: observe role transition at the same log site.
	// commit/apply indexes read-locked outside r.mu to avoid double-locking;
	// stale by a few µs is acceptable for the gauge.
	r.logMu.RLock()
	commitIdx := r.commitIndex
	applyIdx := r.lastApplied
	r.logMu.RUnlock()
	r.metrics.get().observeRoleTransition("leader", term, commitIdx, applyIdx)

	// Start heartbeat routine
	r.wg.Add(1)
	go r.runHeartbeats(ctx, term)

	// Send immediate heartbeat to establish leadership
	r.sendHeartbeatsToAllPeers(ctx, term)
}

// runHeartbeats sends periodic heartbeats to all peers while leader.
func (r *RaftReplicator) runHeartbeats(ctx context.Context, term uint64) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.Raft.HeartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.RLock()
			isLeader := r.state == StateLeader
			currentTerm := r.currentTerm
			r.mu.RUnlock()

			if !isLeader || currentTerm != term {
				return // No longer leader for this term
			}

			r.sendHeartbeatsToAllPeers(ctx, term)
		}
	}
}

// sendHeartbeatsToAllPeers sends AppendEntries RPCs to all peers.
func (r *RaftReplicator) sendHeartbeatsToAllPeers(ctx context.Context, term uint64) {
	for _, peer := range r.peerConfigs() {
		r.wg.Add(1)
		go func(peer PeerConfig) { defer r.wg.Done(); r.replicateLogToPeer(ctx, peer, term) }(peer)
	}
}

// replicateLogToPeer sends AppendEntries to a single peer.
func (r *RaftReplicator) replicateLogToPeer(ctx context.Context, peer PeerConfig, term uint64) {
	conn, err := r.getOrConnectPeer(ctx, peer)
	if err != nil {
		return
	}

	// Get next index for this peer (guarded by peerMu, not logMu)
	r.peerMu.RLock()
	nextIdx := r.nextIndex[peer.ID]
	r.peerMu.RUnlock()

	// Build AppendEntries request
	r.logMu.RLock()
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && int(prevLogIndex) < len(r.log) {
		prevLogTerm = r.log[prevLogIndex].Term
	}

	// Get entries to send
	var entries []*RaftLogEntry
	if int(nextIdx) < len(r.log) {
		// Make a copy of entries to send
		entries = make([]*RaftLogEntry, len(r.log)-int(nextIdx))
		copy(entries, r.log[nextIdx:])
	}
	commitIndex := r.commitIndex
	r.logMu.RUnlock()

	// TRC-20: leader-side append span.
	ctx, appendSpan := startAppendSpan(ctx, peer.ID, term, len(entries))
	defer appendSpan.End()

	req := &AppendEntriesRequest{
		Term:         term,
		LeaderID:     r.config.NodeID,
		LeaderAddr:   r.config.AdvertiseAddr,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
		CodecVersion: r.peerCodecVersion(peer.ID),
	}

	// Send AppendEntries RPC
	resp, err := r.sendAppendEntriesRPC(ctx, conn, req)
	if err != nil {
		return
	}

	r.handleAppendEntriesResponse(peer.ID, term, req, resp)
}

// sendAppendEntriesRPC sends an AppendEntries request and returns the response.
func (r *RaftReplicator) sendAppendEntriesRPC(ctx context.Context, conn PeerConnection, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	// Convert internal log entries to transport type
	var transportEntries []*RaftLogEntry
	if len(req.Entries) > 0 {
		transportEntries = make([]*RaftLogEntry, len(req.Entries))
		for i, entry := range req.Entries {
			transportEntries[i] = entry
		}
	}

	// Create transport request
	raftReq := &RaftAppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		LeaderAddr:   req.LeaderAddr,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      transportEntries,
		LeaderCommit: req.LeaderCommit,
		CodecVersion: req.CodecVersion,
	}

	// Send actual Raft AppendEntries RPC via peer connection
	raftResp, err := conn.SendRaftAppendEntries(ctx, raftReq)
	if err != nil {
		return nil, localizedError(localization.ReplicationRaftSendAppendFailed(err), err)
	}

	// Convert response back to internal type
	return &AppendEntriesResponse{
		Term:          raftResp.Term,
		Success:       raftResp.Success,
		MatchIndex:    raftResp.MatchIndex,
		ConflictIndex: raftResp.ConflictIndex,
		ConflictTerm:  raftResp.ConflictTerm,
		ResponderID:   raftResp.ResponderID,
	}, nil
}

// handleAppendEntriesResponse processes a response to AppendEntries.
func (r *RaftReplicator) handleAppendEntriesResponse(peerID string, term uint64, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	r.mu.Lock()
	if r.closed.Load() {
		r.mu.Unlock()
		return
	}

	// If response term is higher, step down
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.state = StateFollower
		r.leaderID = ""
		r.leaderAddr = ""
		r.votedFor = ""
		r.mu.Unlock()
		r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Stepping down: discovered higher term %d", r.config.NodeID, resp.Term)
		// Plan 04-06-03 D-15a: observe role transition. commitIndex and
		// lastApplied are guarded by logMu, not r.mu — read-lock them
		// here. Slight staleness is acceptable for the gauge.
		r.logMu.RLock()
		ci, ai := r.commitIndex, r.lastApplied
		r.logMu.RUnlock()
		r.metrics.get().observeRoleTransition("follower", resp.Term, ci, ai)
		return
	}
	defer r.mu.Unlock()

	// Ignore if we're no longer leader or term changed
	if r.state != StateLeader || r.currentTerm != term {
		return
	}

	r.peerMu.Lock()
	defer r.peerMu.Unlock()

	// TRC-22: record the peer's codec version for mixed-cluster compat.
	if r.peerCodecVersions == nil {
		r.peerCodecVersions = make(map[string]uint32)
	}
	r.peerCodecVersions[peerID] = resp.CodecVersion

	if resp.Success {
		// Update matchIndex and nextIndex
		if resp.MatchIndex > r.matchIndex[peerID] {
			r.matchIndex[peerID] = resp.MatchIndex
			r.nextIndex[peerID] = resp.MatchIndex + 1
		}

		// Signal to check commit
		select {
		case r.commitCh <- struct{}{}:
		default:
		}
	} else {
		// Decrement nextIndex and retry
		if resp.ConflictIndex > 0 {
			r.nextIndex[peerID] = resp.ConflictIndex
		} else if r.nextIndex[peerID] > 1 {
			r.nextIndex[peerID]--
		}
	}
}

// runApplyLoop processes incoming Apply requests.
func (r *RaftReplicator) runApplyLoop(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case future := <-r.applyCh:
			r.processApply(ctx, future)
		}
	}
}

// processApply handles a single apply request.
// raftApplyQueue marks the private queue-transfer phase. The accepted
// future already owns its receipt and producer lease; ordinary requests retain
// their original separate enqueue and response timeout budgets.
type raftApplyQueue struct{ timeout time.Duration }

func (r *RaftReplicator) processApply(ctx context.Context, future *applyFuture, queue ...raftApplyQueue) error {
	if len(queue) > 0 {
		if future.accepted != nil {
			select {
			case r.applyCh <- future:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-r.stopCh:
				return ErrClosed
			}
		}
		timeout := queue[0].timeout
		select {
		case r.applyCh <- future:
		case <-r.stopCh:
			return ErrClosed
		case <-time.After(timeout):
			return localizedError(localization.ReplicationRaftApplyQueueFull(), nil)
		}
		select {
		case err := <-future.errCh:
			return err
		case <-r.stopCh:
			return ErrClosed
		case <-time.After(timeout):
			return localizedError(localization.ReplicationRaftApplyTimedOut(), nil)
		}
	}

	r.mu.RLock()
	var admissionErr error
	if future.accepted != nil {
		admissionErr = r.acceptedAdmissionErrorLocked(future.accepted.term)
	} else if r.state != StateLeader {
		admissionErr = ErrNotLeader
	}
	if admissionErr != nil {
		r.mu.RUnlock()
		future.discardBeforeCommit(admissionErr)
		return nil
	}
	term := r.currentTerm
	// Retain the role/term lease through append and future registration.
	// Shutdown and leadership changes cannot pass a half-transferred command.
	r.logMu.Lock()
	entry := &RaftLogEntry{Index: r.log[len(r.log)-1].Index + 1, Term: term, Command: future.cmd}
	r.log = append(r.log, entry)
	future.index, future.term = entry.Index, term
	r.futuresMu.Lock()
	r.futures[entry.Index] = future
	r.futuresMu.Unlock()
	r.logMu.Unlock()
	r.mu.RUnlock()

	// If single node, commit immediately through the same ordered owner.
	if r.peerCount() == 0 {
		r.applyMu.Lock()
		r.logMu.Lock()
		r.commitIndex = entry.Index
		r.advanceCommitIndex(raftCommittedFrontier{index: r.commitIndex})
		r.logMu.Unlock()
		r.applyMu.Unlock()
		return nil
	}

	// Trigger replication to peers
	r.mu.RLock()
	currentTerm := r.currentTerm
	r.mu.RUnlock()
	r.sendHeartbeatsToAllPeers(ctx, currentTerm)

	// Accepted and ordinary requests share one deadline observer. Only the
	// accepted coordinator retains logical ownership beyond that deadline.
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.waitForCommit(ctx, future) }()

	return nil
}

// waitForCommit waits for an entry to be committed.
func (r *RaftReplicator) waitForCommit(ctx context.Context, future *applyFuture) {
	timer := time.NewTimer(r.config.Raft.CommitTimeout)
	defer timer.Stop()
	var ctxDone, acceptedDone <-chan struct{}
	var tick <-chan time.Time
	if future.accepted != nil {
		acceptedDone = future.accepted.receipt.Done()
	} else {
		ctxDone = ctx.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		var err error
		remove := false
		select {
		case <-acceptedDone:
			return
		case <-ctxDone:
			err = ctx.Err()
		case <-r.stopCh:
			err = ErrClosed
		case <-timer.C:
			err = localizedError(localization.ReplicationRaftCommitTimedOut(r.config.Raft.CommitTimeout), nil)
			remove = true
		case <-tick:
			r.logMu.RLock()
			applied := r.lastApplied >= future.index
			r.logMu.RUnlock()
			if !applied {
				continue
			}
		}
		if future.accepted != nil {
			_ = future.accepted.owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: err})
		} else {
			if remove {
				r.futuresMu.Lock()
				delete(r.futures, future.index)
				r.futuresMu.Unlock()
			}
			future.reportRecorded(err)
		}
		return
	}
}

// runCommitLoop checks for entries that can be committed.
func (r *RaftReplicator) runCommitLoop(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-r.commitCh:
			r.advanceCommitIndex()
		}
	}
}

// advanceCommitIndex checks if any entries can be committed.
// raftCommittedFrontier is a private held-lock phase: the caller already
// selected a committed frontier under applyMu then logMu. It cannot promote
// consensus; only the zero-argument leader phase computes a quorum frontier.
type raftCommittedFrontier struct{ index uint64 }

func (r *RaftReplicator) advanceCommitIndex(known ...raftCommittedFrontier) {
	if len(known) == 0 {
		r.mu.RLock()
		isLeader := r.state == StateLeader
		term := r.currentTerm
		r.mu.RUnlock()
		if !isLeader {
			return
		}
		r.applyMu.Lock()
		defer r.applyMu.Unlock()
		r.logMu.Lock()
		defer r.logMu.Unlock()

		// Find highest index replicated to majority of servers
		for n := r.commitIndex + 1; n < uint64(len(r.log)); n++ {
			// Only commit entries from current term (Raft safety property)
			if r.log[n].Term != term {
				continue
			}

			// Count replications (including self)
			replicatedTo := 1
			r.peerMu.RLock()
			for _, peer := range r.config.Raft.Peers {
				if r.matchIndex[peer.ID] >= n {
					replicatedTo++
				}
			}
			totalServers := len(r.config.Raft.Peers) + 1
			r.peerMu.RUnlock()

			// Check for majority
			if replicatedTo > totalServers/2 {
				r.commitIndex = n
			}
		}

	}
	frontier := r.commitIndex
	if len(known) > 0 {
		frontier = known[0].index
	}
	// This is the sole application owner for leader, follower and single-node
	// commits. Native application runs without logMu; failures retain committed
	// ownership and successful-only lastApplied advancement for ordered retry.

	for r.lastApplied < frontier {
		index := r.lastApplied + 1
		entry := r.log[index]
		var err error
		if entry.Command != nil {
			r.logMu.Unlock()
			err = r.storage.ApplyCommand(entry.Command)
			if err != nil {
				r.config.logPrintf(context.Background(), slog.LevelError, "[Raft %s] Failed to apply entry %d", r.config.NodeID, entry.Index)
			}
			r.logMu.Lock()
		}
		if err != nil {
			r.applyErr = err
		} else {
			r.lastApplied = index
		}
		// A transport waiter can already have timed out; its buffered result
		// must not park the committed application owner while holding logMu.
		// Command ownership remains in the committed log on an apply error.
		r.futuresMu.Lock()
		future := r.futures[index]
		if err == nil || future == nil || future.accepted == nil {
			delete(r.futures, index)
		}
		r.futuresMu.Unlock()
		if future != nil {
			future.reportRecorded(err)
		}

		if err != nil {
			return
		}
	}
	r.applyErr = nil

}

// runPeerConnector maintains connections to peers.
func (r *RaftReplicator) runPeerConnector(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.maintainPeerConnections(ctx)
		}
	}
}

// maintainPeerConnections ensures we have connections to all peers.
func (r *RaftReplicator) maintainPeerConnections(ctx context.Context) {
	if r.transport == nil {
		return
	}

	for _, peer := range r.peerConfigs() {
		if _, err := r.getOrConnectPeer(ctx, peer); err != nil {
			r.config.logPrintf(ctx, slog.LevelWarn, "[Raft %s] Peer %s connection attempt: %v", r.config.NodeID, peer.ID, err)
		}
	}
}

// getOrConnectPeer gets an existing connection or creates a new one.
func (r *RaftReplicator) getOrConnectPeer(ctx context.Context, peer PeerConfig) (PeerConnection, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	r.peerMu.RLock()
	conn := r.peerConns[peer.ID]
	r.peerMu.RUnlock()

	if conn != nil && conn.IsConnected() {
		return conn, nil
	}

	if r.transport == nil {
		return nil, localizedError(localization.ReplicationRaftTransportNotConfigured(), nil)
	}

	newConn, err := r.transport.Connect(ctx, peer.Addr)
	if err != nil {
		return nil, localizedError(localization.ReplicationRaftConnectPeerFailed(peer.Addr, err), err)
	}

	r.peerMu.Lock()
	if r.closed.Load() {
		r.peerMu.Unlock()
		closeErr := newConn.Close()
		if closeErr != nil {
			closeErr = fmt.Errorf("raft rejected peer %s close: %w", peer.ID, closeErr)
		}
		r.mu.Lock()
		r.shutdownErr = errors.Join(r.shutdownErr, closeErr)
		r.mu.Unlock()
		return nil, errors.Join(ErrClosed, closeErr)
	}
	r.peerConns[peer.ID] = newConn
	r.peerMu.Unlock()

	return newConn, nil
}

// runRPCHandler processes incoming RPC requests.
func (r *RaftReplicator) runRPCHandler(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case req := <-r.rpcCh:
			r.handleRPC(req)
		}
	}
}

// handleRPC dispatches an RPC request to the appropriate handler.
func (r *RaftReplicator) handleRPC(req *rpcRequest) {
	var respData []byte

	switch req.message.Type {
	case RPCVoteRequest:
		var voteReq VoteRequest
		if err := json.Unmarshal(req.message.Payload, &voteReq); err != nil {
			return
		}
		resp := r.handleVoteRequest(&voteReq)
		respData, _ = json.Marshal(resp)

	case RPCAppendEntries:
		var aeReq AppendEntriesRequest
		if err := json.Unmarshal(req.message.Payload, &aeReq); err != nil {
			return
		}
		resp := r.handleAppendEntriesRequest(&aeReq)
		respData, _ = json.Marshal(resp)
	}

	if req.respCh != nil {
		req.respCh <- respData
	}
}

// handleVoteRequest processes an incoming vote request.
func (r *RaftReplicator) handleVoteRequest(req *VoteRequest) *VoteResponse {
	r.mu.Lock()

	resp := &VoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
		VoterID:     r.config.NodeID,
	}
	candidate, requestedTerm := req.CandidateID, req.Term
	defer func() {
		r.mu.Unlock()
		if resp.VoteGranted {
			r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Granted vote to %s for term %d", r.config.NodeID, candidate, requestedTerm)
		}
	}()
	if r.closed.Load() {
		return resp
	}

	// If request term is stale, reject
	if req.Term < r.currentTerm {
		return resp
	}

	// If request term is newer, update our term and become follower
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = StateFollower
		r.votedFor = ""
		r.leaderID = ""
		r.leaderAddr = ""
	}

	resp.Term = r.currentTerm

	// Check if we can grant vote
	canVote := r.votedFor == "" || r.votedFor == req.CandidateID

	if canVote {
		// Check if candidate's log is at least as up-to-date as ours
		r.logMu.RLock()
		lastLogIndex := r.log[len(r.log)-1].Index
		lastLogTerm := r.log[len(r.log)-1].Term
		r.logMu.RUnlock()

		logIsUpToDate := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		if logIsUpToDate {
			r.votedFor = req.CandidateID
			resp.VoteGranted = true

			// Reset election timer
			select {
			case r.heartbeatCh <- struct{}{}:
			default:
			}

		}
	}

	return resp
}

// handleAppendEntriesRequest processes an incoming AppendEntries request.
func (r *RaftReplicator) handleAppendEntriesRequest(req *AppendEntriesRequest) *AppendEntriesResponse {
	// TRC-20: follower-side apply span.
	_, applySpan := startApplySpan(context.Background(), req.LeaderID, req.Term, req.PrevLogIndex, len(req.Entries))
	defer applySpan.End()

	r.mu.Lock()

	resp := &AppendEntriesResponse{
		Term:         r.currentTerm,
		Success:      false,
		ResponderID:  r.config.NodeID,
		CodecVersion: CurrentCodecVersion,
	}

	if r.closed.Load() {
		r.mu.Unlock()
		return resp
	}

	// If request term is stale, reject
	if req.Term < r.currentTerm {
		r.mu.Unlock()
		return resp
	}

	// If request term is newer or equal, recognize sender as leader
	if req.Term >= r.currentTerm {
		if req.Term > r.currentTerm {
			r.currentTerm = req.Term
			r.votedFor = ""
		}
		r.state = StateFollower
		r.leaderID = req.LeaderID
		r.leaderAddr = req.LeaderAddr
	}

	resp.Term = r.currentTerm
	r.mu.Unlock()

	// Reset election timer - we heard from leader
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	// Serialize physical application and log replacement in that order.
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	// Check log consistency
	r.logMu.Lock()
	defer r.logMu.Unlock()

	// Check if we have the entry at PrevLogIndex with matching term
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) >= len(r.log) {
			// We don't have this entry
			resp.ConflictIndex = uint64(len(r.log))
			return resp
		}
		if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			// Terms don't match - find conflicting term's first index
			conflictTerm := r.log[req.PrevLogIndex].Term
			resp.ConflictTerm = conflictTerm
			for i := int(req.PrevLogIndex); i >= 1; i-- {
				if r.log[i].Term != conflictTerm {
					resp.ConflictIndex = uint64(i + 1)
					break
				}
				if i == 1 {
					resp.ConflictIndex = 1
				}
			}
			return resp
		}
	}

	// Append new entries (with conflict resolution)
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if int(idx) < len(r.log) {
			if r.log[idx].Term != entry.Term {
				// A committed prefix is immutable. Only uncommitted entries
				// can be proven discarded by a new leader's replacement.
				if idx <= r.commitIndex {
					resp.ConflictIndex = r.commitIndex + 1
					return resp
				}
				r.futuresMu.Lock()
				var discarded []*applyFuture
				for index, future := range r.futures {
					if index >= idx {
						discarded = append(discarded, future)
						delete(r.futures, index)
					}
				}
				r.futuresMu.Unlock()
				for _, future := range discarded {
					r.wg.Add(1)
					go func(future *applyFuture) { defer r.wg.Done(); future.discardBeforeCommit(ErrNotLeader) }(future)
				}
				clear(r.log[idx:])
				r.log = r.log[:idx]
				r.log = append(r.log, entry)
			}
			// Entry matches - skip
		} else {
			// New entry
			r.log = append(r.log, entry)
		}
	}

	// Update match index
	if len(req.Entries) > 0 {
		resp.MatchIndex = req.Entries[len(req.Entries)-1].Index
	} else {
		resp.MatchIndex = req.PrevLogIndex
	}

	// Update commit index
	if req.LeaderCommit > r.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}
	}

	r.advanceCommitIndex(raftCommittedFrontier{index: r.commitIndex})

	resp.Success = true
	return resp
}

// listenForPeers listens for incoming peer connections.
func (r *RaftReplicator) listenForPeers(ctx context.Context) {
	defer r.wg.Done()

	if r.transport == nil {
		return
	}

	err := r.transport.Listen(ctx, r.config.AdvertiseAddr, func(conn PeerConnection) {
		r.handleIncomingConnection(ctx, conn)
	})
	if err != nil && ctx.Err() == nil {
		r.config.logPrintf(context.Background(), slog.LevelError, "[Raft %s] Listen error: %v", r.config.NodeID, err)
	}
}

// handleIncomingConnection handles an incoming peer connection.
func (r *RaftReplicator) handleIncomingConnection(ctx context.Context, conn PeerConnection) {
	// Signal that we received communication from a peer
	// This helps reset election timer
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}
}

// forwardApplySender is implemented by cluster connections that support forwarding writes to the leader.
type forwardApplySender interface {
	SendForwardApply(ctx context.Context, cmd *Command, timeout time.Duration) error
}

// HandleForwardApply applies a command received from a follower (write forwarding).
// Only the leader should receive these; it applies the command through Raft as usual.
func (r *RaftReplicator) HandleForwardApply(cmd *Command, timeout time.Duration) error {
	return r.HandleForwardApplyWithContext(nil, cmd, timeout)
}

// HandleForwardApplyWithContext binds the actual inbound handler lifetime.
func (r *RaftReplicator) HandleForwardApplyWithContext(ctx context.Context, cmd *Command, timeout time.Duration) error {
	return r.ApplyWithContext(ctx, cmd, timeout)
}

// Apply applies a command through Raft consensus.
// If this node is not the leader, it forwards the command to the leader automatically.
func (r *RaftReplicator) Apply(cmd *Command, timeout time.Duration) error {
	return r.ApplyWithContext(nil, cmd, timeout)
}

// ApplyWithContext retains the request through forwarding and local admission.
func (r *RaftReplicator) ApplyWithContext(ctx context.Context, cmd *Command, timeout time.Duration) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	// Plan 04-06-03 D-15a: observe apply latency at the function-exit
	// chokepoint via the pre-bound MET-25 observer (zero alloc when ctx
	// has no sampled span). Wrapped at the outer entry so retries /
	// forwards / queue waits all roll up into the single histogram.
	startApply := time.Now()
	defer func() {
		if m := r.metrics.get(); m != nil {
			m.observeApplyDuration(context.Background(), time.Since(startApply).Seconds())
		}
	}()

	if err := checkApplyReadiness(r.closed.Load(), r.started.Load()); err != nil {
		return err
	}
	if ctx == nil {
		ctx = r.LifetimeContext()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	isLeader := r.state == StateLeader
	leaderAddr := r.leaderAddr
	r.mu.RUnlock()

	if !isLeader {
		// Forward write to leader so clients can send writes to any node
		if leaderAddr == "" {
			return ErrNoLeader
		}
		if r.transport == nil {
			return ErrNotLeader
		}
		fwdCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		conn, err := r.transport.Connect(fwdCtx, leaderAddr)
		if err != nil {
			return localizedError(localization.ReplicationRaftForwardToLeaderFailed(err), err)
		}
		if fa, ok := conn.(forwardApplySender); ok {
			err := fa.SendForwardApply(fwdCtx, cmd, timeout)
			if err == nil {
				return nil
			}
			// Once sent, a failed reply cannot establish that the leader did
			// not accept it. Preserve that uncertainty through the Go API.
			return &storage.AcceptedCommitError{State: storage.CommitRecoveryPending, LogicalID: cmd.RequestID, Err: err}
		}
		return ErrNotLeader
	}

	if handled, err := routeLeaderEntityCommand(ctx, r.storage, r, cmd, timeout); handled {
		return err
	}

	future := &applyFuture{cmd: cmd, errCh: make(chan error, 1)}
	return r.processApply(ctx, future, raftApplyQueue{timeout: timeout})
}

// ApplyBatch applies multiple commands atomically.
func (r *RaftReplicator) ApplyBatch(cmds []*Command, timeout time.Duration) error {
	return r.ApplyBatchWithContext(nil, cmds, timeout)
}

// ApplyBatchWithContext retains the request while submitting each command.
func (r *RaftReplicator) ApplyBatchWithContext(ctx context.Context, cmds []*Command, timeout time.Duration) error {
	for _, cmd := range cmds {
		if err := r.ApplyWithContext(ctx, cmd, timeout); err != nil {
			return err
		}
	}
	return nil
}

// IsLeader returns true if this node is the Raft leader.
func (r *RaftReplicator) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == StateLeader
}

// LeaderAddr returns the address of the current leader.
func (r *RaftReplicator) LeaderAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state == StateLeader {
		return r.config.AdvertiseAddr
	}
	return r.leaderAddr
}

// LeaderID returns the ID of the current leader.
func (r *RaftReplicator) LeaderID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state == StateLeader {
		return r.config.NodeID
	}
	return r.leaderID
}

// Health returns health status.
func (r *RaftReplicator) Health() *HealthStatus {
	r.mu.RLock()
	state := r.state
	term := r.currentTerm
	leaderID := r.leaderID
	leaderAddr := r.leaderAddr
	r.mu.RUnlock()

	stateStr := "initializing"
	if r.started.Load() {
		stateStr = "ready"
	}
	if r.closed.Load() {
		stateStr = "closed"
	}

	role := state.String()
	isLeader := state == StateLeader

	if isLeader {
		leaderID = r.config.NodeID
		leaderAddr = r.config.AdvertiseAddr
	}

	// Collect peer status
	r.peerMu.RLock()
	configured := append([]PeerConfig(nil), r.config.Raft.Peers...)
	connections := make(map[string]PeerConnection, len(r.peerConns))
	for id, conn := range r.peerConns {
		connections[id] = conn
	}
	r.peerMu.RUnlock()
	peers := make([]PeerStatus, 0, len(configured))
	for _, peer := range configured {
		conn := connections[peer.ID]
		peerState := "disconnected"
		healthy := false
		if conn != nil && conn.IsConnected() {
			peerState = "connected"
			healthy = true
		}
		peers = append(peers, PeerStatus{
			ID:      peer.ID,
			Address: peer.Addr,
			Healthy: healthy,
			State:   peerState,
		})
	}

	r.logMu.RLock()
	commitIdx := r.commitIndex
	appliedIdx := r.lastApplied
	applyFailed := r.applyErr != nil
	r.logMu.RUnlock()

	if applyFailed && !r.closed.Load() {
		stateStr = "apply_failed"
	}
	return &HealthStatus{
		Mode:         ModeRaft,
		NodeID:       r.config.NodeID,
		Role:         role,
		IsLeader:     isLeader,
		LeaderID:     leaderID,
		LeaderAddr:   leaderAddr,
		State:        stateStr,
		Healthy:      r.started.Load() && !r.closed.Load() && !applyFailed,
		Term:         term,
		CommitIndex:  commitIdx,
		AppliedIndex: appliedIdx,
		Peers:        peers,
	}
}

// WaitForLeader blocks until a leader is elected or context cancelled.
func (r *RaftReplicator) WaitForLeader(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return ErrClosed
		case <-ticker.C:
			r.mu.RLock()
			hasLeader := r.state == StateLeader || r.leaderID != ""
			r.mu.RUnlock()

			if hasLeader {
				return nil
			}
		}
	}
}

// Shutdown gracefully shuts down the replicator.
func (r *RaftReplicator) Shutdown() error {
	r.mu.Lock()
	if !r.closed.CompareAndSwap(false, true) {
		r.mu.Unlock()
		return nil
	}
	close(r.stopCh)
	transport := r.transport
	stopWork := r.stopWork
	r.mu.Unlock()
	if stopWork != nil {
		stopWork()
	}

	r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Shutting down", r.config.NodeID)
	var closeErr error

	// Close transport to stop listener (unblocks listenForPeers goroutine)
	if transport != nil {
		if err := transport.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("raft transport close: %w", err))
		}
	}

	// Close all peer connections
	r.peerMu.Lock()
	peers := make(map[string]PeerConnection, len(r.peerConns))
	for id, conn := range r.peerConns {
		peers[id] = conn
	}
	r.peerMu.Unlock()
	for id, conn := range peers {
		if conn != nil {
			if err := conn.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("raft peer %s close: %w", id, err))
			}
		}
	}

	// Stop queue consumers before classifying remaining ownership. Commands
	// that never entered the log are a definite discard; committed entries
	// remain recovery-pending even while their unused native snapshot is freed.
	r.wg.Wait()
	for {
		select {
		case future := <-r.applyCh:
			future.discardBeforeCommit(ErrClosed)
		default:
			goto queueDrained
		}
	}
queueDrained:
	r.logMu.RLock()
	committed := r.commitIndex
	r.futuresMu.Lock()
	pending := r.futures
	r.futures = make(map[uint64]*applyFuture)
	r.futuresMu.Unlock()
	r.logMu.RUnlock()
	for index, future := range pending {
		if future.accepted == nil || index > committed {
			future.discardBeforeCommit(ErrClosed)
			continue
		}
		if local := future.cmd.accepted; local != nil && !local.started.Load() {
			local.prepared.CloseNative() // release only physical staging, not logical ownership
		}
		closeErr = errors.Join(closeErr, future.accepted.owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: ErrClosed}))
	}

	r.mu.Lock()
	closeErr = errors.Join(closeErr, r.shutdownErr)
	r.mu.Unlock()
	return closeErr
}

// Mode returns the replication mode.
func (r *RaftReplicator) Mode() ReplicationMode {
	return ModeRaft
}

// NodeID returns this node's ID.
func (r *RaftReplicator) NodeID() string {
	return r.config.NodeID
}

// AddVoter adds a voting member to the cluster.
func (r *RaftReplicator) AddVoter(id, addr string) error {
	r.mu.RLock()
	if r.closed.Load() {
		r.mu.RUnlock()
		return ErrClosed
	}
	isLeader := r.state == StateLeader
	if isLeader {
		r.wg.Add(1)
	}
	r.mu.RUnlock()

	if !isLeader {
		return ErrNotLeader
	}
	defer r.wg.Done()

	// Read the log before taking peerMu: the commit path holds logMu while
	// it takes peerMu, so the reverse order deadlocks.
	r.logMu.RLock()
	lastIdx := r.log[len(r.log)-1].Index
	r.logMu.RUnlock()

	r.peerMu.Lock()

	// Check if already exists
	for _, peer := range r.config.Raft.Peers {
		if peer.ID == id {
			r.peerMu.Unlock()
			return nil // Already a member
		}
	}

	// Add new peer
	r.config.Raft.Peers = append(r.config.Raft.Peers, PeerConfig{
		ID:   id,
		Addr: addr,
	})

	// Initialize tracking for new peer
	r.nextIndex[id] = lastIdx + 1
	r.matchIndex[id] = 0
	r.peerMu.Unlock()

	r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Added voter %s at %s", r.config.NodeID, id, addr)
	return nil
}

// RemoveServer removes a server from the cluster.
func (r *RaftReplicator) RemoveServer(id string) error {
	r.mu.RLock()
	if r.closed.Load() {
		r.mu.RUnlock()
		return ErrClosed
	}
	isLeader := r.state == StateLeader
	if isLeader {
		r.wg.Add(1)
	}
	r.mu.RUnlock()

	if !isLeader {
		return ErrNotLeader
	}
	defer r.wg.Done()

	r.peerMu.Lock()

	// Find and remove peer
	newPeers := make([]PeerConfig, 0, len(r.config.Raft.Peers))
	for _, peer := range r.config.Raft.Peers {
		if peer.ID != id {
			newPeers = append(newPeers, peer)
		}
	}
	r.config.Raft.Peers = newPeers

	// Close connection and clean up tracking
	conn := r.peerConns[id]
	delete(r.peerConns, id)
	delete(r.nextIndex, id)
	delete(r.matchIndex, id)
	r.peerMu.Unlock()
	var closeErr error
	if conn != nil {
		closeErr = conn.Close()
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("raft removed peer %s close: %w", id, closeErr)
	}
	r.mu.Lock()
	r.shutdownErr = errors.Join(r.shutdownErr, closeErr)
	r.mu.Unlock()

	r.config.logPrintf(context.Background(), slog.LevelInfo, "[Raft %s] Removed server %s", r.config.NodeID, id)
	return closeErr
}

// GetConfiguration returns the current cluster configuration.
func (r *RaftReplicator) GetConfiguration() ([]PeerStatus, error) {
	r.mu.RLock()
	state := r.state
	r.mu.RUnlock()

	r.peerMu.RLock()
	configured := append([]PeerConfig(nil), r.config.Raft.Peers...)
	connections := make(map[string]PeerConnection, len(r.peerConns))
	for id, conn := range r.peerConns {
		connections[id] = conn
	}
	r.peerMu.RUnlock()

	result := make([]PeerStatus, 0, len(configured)+1)

	// Add self
	result = append(result, PeerStatus{
		ID:      r.config.NodeID,
		Address: r.config.AdvertiseAddr,
		Healthy: true,
		State:   state.String(),
	})

	// Add peers
	for _, peer := range configured {
		conn := connections[peer.ID]
		peerState := "follower"
		healthy := false
		if conn != nil && conn.IsConnected() {
			peerState = "connected"
			healthy = true
		} else {
			peerState = "disconnected"
		}
		result = append(result, PeerStatus{
			ID:      peer.ID,
			Address: peer.Addr,
			Healthy: healthy,
			State:   peerState,
		})
	}

	return result, nil
}

// getState returns the current state thread-safely.
func (r *RaftReplicator) getState() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// Ensure RaftReplicator implements Replicator.
var _ Replicator = (*RaftReplicator)(nil)
