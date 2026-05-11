package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
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

	// Log (protected by logMu)
	logMu       sync.RWMutex
	log         []*RaftLogEntry
	commitIndex uint64
	lastApplied uint64

	// Peer state (leader only)
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

// applyFuture represents a pending apply operation.
type applyFuture struct {
	cmd    *Command
	index  uint64
	term   uint64
	errCh  chan error
	doneCh chan struct{}
}

// rpcRequest represents an incoming RPC request.
type rpcRequest struct {
	message *RaftRPCMessage
	conn    PeerConnection
	respCh  chan []byte
}

// HandleRaftVote handles an incoming RequestVote RPC via the cluster transport.
func (r *RaftReplicator) HandleRaftVote(req *RaftVoteRequest) (*RaftVoteResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil vote request")
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
	if req == nil {
		return nil, fmt.Errorf("nil append entries request")
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

	r := &RaftReplicator{
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

// SetTransport sets the transport for peer communication.
func (r *RaftReplicator) SetTransport(t Transport) {
	r.transport = t
}

// Start initializes and starts the Raft replicator.
func (r *RaftReplicator) Start(ctx context.Context) error {
	if r.started.Load() {
		return nil
	}

	log.Printf("[Raft %s] Starting node", r.config.NodeID)

	if r.transport == nil {
		transport, err := NewDefaultTransportFromConfig(r.config)
		if err != nil {
			return fmt.Errorf("init transport: %w", err)
		}
		r.transport = transport
	}

	// Initialize peer tracking for all configured peers
	r.peerMu.Lock()
	for _, peer := range r.config.Raft.Peers {
		r.nextIndex[peer.ID] = 1
		r.matchIndex[peer.ID] = 0
	}
	r.peerMu.Unlock()

	// Start background goroutines
	r.wg.Add(5)
	go r.runElectionTimer(ctx)
	go r.runApplyLoop(ctx)
	go r.runCommitLoop(ctx)
	go r.runPeerConnector(ctx)
	go r.runRPCHandler(ctx)

	// If bootstrap mode with no peers, immediately become leader
	if r.config.Raft.Bootstrap && len(r.config.Raft.Peers) == 0 {
		r.mu.Lock()
		r.state = StateLeader
		r.leaderID = r.config.NodeID
		r.leaderAddr = r.config.AdvertiseAddr
		r.currentTerm = 1
		r.mu.Unlock()
		log.Printf("[Raft %s] Bootstrap: became leader (term 1)", r.config.NodeID)
		// Plan 04-06-03 D-15a: observe role transition at the same log site.
		r.metrics.get().observeRoleTransition("leader", 1, 0, 0)
	}

	// Start listening for peer connections if transport available
	if r.transport != nil {
		r.wg.Add(1)
		go r.listenForPeers(ctx)
	}

	r.started.Store(true)
	log.Printf("[Raft %s] Started in %s state", r.config.NodeID, r.getState())

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

	log.Printf("[Raft %s] Starting election for term %d", r.config.NodeID, term)

	// Get last log info for vote request
	r.logMu.RLock()
	lastLogIndex := r.log[len(r.log)-1].Index
	lastLogTerm := r.log[len(r.log)-1].Term
	r.logMu.RUnlock()

	// Count votes - we already voted for ourselves
	var votesMu sync.Mutex
	votesReceived := 1
	totalVoters := len(r.config.Raft.Peers) + 1
	votesNeeded := totalVoters/2 + 1

	// If single node, we win immediately
	if len(r.config.Raft.Peers) == 0 {
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
	for _, peer := range r.config.Raft.Peers {
		wg.Add(1)
		go func(p PeerConfig) {
			defer wg.Done()
			r.requestVoteFromPeer(ctx, p, voteReq, term, &votesMu, &votesReceived, votesNeeded)
		}(peer)
	}

	// Wait for vote requests with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(r.config.Raft.ElectionTimeout):
		log.Printf("[Raft %s] Election timed out for term %d", r.config.NodeID, term)
	case <-ctx.Done():
	case <-r.stopCh:
	}
}

// requestVoteFromPeer sends a vote request to a single peer.
func (r *RaftReplicator) requestVoteFromPeer(ctx context.Context, peer PeerConfig, req *VoteRequest, term uint64, votesMu *sync.Mutex, votesReceived *int, votesNeeded int) {
	// Get or establish connection
	conn, err := r.getOrConnectPeer(ctx, peer)
	if err != nil {
		log.Printf("[Raft %s] Cannot connect to %s for vote: %v", r.config.NodeID, peer.ID, err)
		return
	}

	// Send vote request
	resp, err := r.sendVoteRequestRPC(ctx, conn, req)
	if err != nil {
		log.Printf("[Raft %s] Vote request to %s failed: %v", r.config.NodeID, peer.ID, err)
		return
	}

	// Process response
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if we've moved on (higher term discovered or already won/lost)
	if r.currentTerm != term || r.state != StateCandidate {
		return
	}

	// If response has higher term, step down
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.state = StateFollower
		r.votedFor = ""
		r.leaderID = ""
		r.leaderAddr = ""
		log.Printf("[Raft %s] Stepping down: discovered higher term %d from %s", r.config.NodeID, resp.Term, peer.ID)
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

		log.Printf("[Raft %s] Received vote from %s (%d/%d needed)", r.config.NodeID, resp.VoterID, votes, votesNeeded)

		if votes >= votesNeeded && r.state == StateCandidate && r.currentTerm == term {
			r.mu.Unlock()
			r.becomeLeader(ctx, term)
			r.mu.Lock()
		}
	}
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
		return nil, fmt.Errorf("send vote request: %w", err)
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
	if r.currentTerm != term || r.state == StateLeader {
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

	log.Printf("[Raft %s] Became leader for term %d", r.config.NodeID, term)

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
	r.peerMu.RLock()
	peers := make([]PeerConfig, len(r.config.Raft.Peers))
	copy(peers, r.config.Raft.Peers)
	r.peerMu.RUnlock()

	for _, peer := range peers {
		go r.replicateLogToPeer(ctx, peer, term)
	}
}

// replicateLogToPeer sends AppendEntries to a single peer.
func (r *RaftReplicator) replicateLogToPeer(ctx context.Context, peer PeerConfig, term uint64) {
	conn, err := r.getOrConnectPeer(ctx, peer)
	if err != nil {
		return
	}

	// Get next index for this peer
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
		return nil, fmt.Errorf("send append entries: %w", err)
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
	defer r.mu.Unlock()

	// If response term is higher, step down
	if resp.Term > r.currentTerm {
		r.currentTerm = resp.Term
		r.state = StateFollower
		r.leaderID = ""
		r.leaderAddr = ""
		r.votedFor = ""
		log.Printf("[Raft %s] Stepping down: discovered higher term %d", r.config.NodeID, resp.Term)
		// Plan 04-06-03 D-15a: observe role transition. commitIndex and
		// lastApplied are guarded by logMu, not r.mu — read-lock them
		// here. Slight staleness is acceptable for the gauge.
		r.logMu.RLock()
		ci, ai := r.commitIndex, r.lastApplied
		r.logMu.RUnlock()
		r.metrics.get().observeRoleTransition("follower", resp.Term, ci, ai)
		return
	}

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
func (r *RaftReplicator) processApply(ctx context.Context, future *applyFuture) {
	r.mu.RLock()
	isLeader := r.state == StateLeader
	term := r.currentTerm
	r.mu.RUnlock()

	if !isLeader {
		future.errCh <- ErrNotLeader
		return
	}

	// Append to local log
	r.logMu.Lock()
	entry := &RaftLogEntry{
		Index:   r.log[len(r.log)-1].Index + 1,
		Term:    term,
		Command: future.cmd,
	}
	r.log = append(r.log, entry)
	future.index = entry.Index
	future.term = term
	r.logMu.Unlock()

	// Track this future for commit notification
	r.futuresMu.Lock()
	r.futures[entry.Index] = future
	r.futuresMu.Unlock()

	// If single node, commit immediately
	if len(r.config.Raft.Peers) == 0 {
		r.logMu.Lock()
		r.commitIndex = entry.Index
		r.logMu.Unlock()

		// Apply to state machine
		err := r.storage.ApplyCommand(future.cmd)

		r.logMu.Lock()
		r.lastApplied = entry.Index
		r.logMu.Unlock()

		r.futuresMu.Lock()
		delete(r.futures, entry.Index)
		r.futuresMu.Unlock()

		future.errCh <- err
		return
	}

	// Trigger replication to peers
	r.mu.RLock()
	currentTerm := r.currentTerm
	r.mu.RUnlock()
	r.sendHeartbeatsToAllPeers(ctx, currentTerm)

	// Wait for commit in background
	go r.waitForCommit(ctx, future)
}

// waitForCommit waits for an entry to be committed.
func (r *RaftReplicator) waitForCommit(ctx context.Context, future *applyFuture) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(r.config.Raft.CommitTimeout)

	for {
		select {
		case <-ctx.Done():
			future.errCh <- ctx.Err()
			return
		case <-r.stopCh:
			future.errCh <- ErrClosed
			return
		case <-timeout:
			r.futuresMu.Lock()
			delete(r.futures, future.index)
			r.futuresMu.Unlock()
			future.errCh <- fmt.Errorf("commit timeout after %v", r.config.Raft.CommitTimeout)
			return
		case <-ticker.C:
			r.logMu.RLock()
			committed := r.commitIndex >= future.index
			applied := r.lastApplied >= future.index
			r.logMu.RUnlock()

			if applied {
				future.errCh <- nil
				return
			}

			if committed {
				// Entry is committed but not yet applied - wait for apply
				continue
			}
		}
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
func (r *RaftReplicator) advanceCommitIndex() {
	r.mu.RLock()
	isLeader := r.state == StateLeader
	term := r.currentTerm
	r.mu.RUnlock()

	if !isLeader {
		return
	}

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

	// Apply committed entries to state machine
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		applyIdx := r.lastApplied

		if int(applyIdx) < len(r.log) {
			entry := r.log[applyIdx]
			if entry.Command != nil {
				if err := r.storage.ApplyCommand(entry.Command); err != nil {
					log.Printf("[Raft %s] Failed to apply entry %d: %v", r.config.NodeID, entry.Index, err)
				}
			}
		}

		// Notify any waiting futures
		r.futuresMu.Lock()
		if future, ok := r.futures[applyIdx]; ok {
			delete(r.futures, applyIdx)
			r.futuresMu.Unlock()
			future.errCh <- nil
		} else {
			r.futuresMu.Unlock()
		}
	}
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

	r.peerMu.RLock()
	peers := make([]PeerConfig, len(r.config.Raft.Peers))
	copy(peers, r.config.Raft.Peers)
	r.peerMu.RUnlock()

	for _, peer := range peers {
		r.peerMu.RLock()
		conn := r.peerConns[peer.ID]
		r.peerMu.RUnlock()

		if conn == nil || !conn.IsConnected() {
			newConn, err := r.transport.Connect(ctx, peer.Addr)
			if err != nil {
				continue
			}
			r.peerMu.Lock()
			r.peerConns[peer.ID] = newConn
			r.peerMu.Unlock()
		}
	}
}

// getOrConnectPeer gets an existing connection or creates a new one.
func (r *RaftReplicator) getOrConnectPeer(ctx context.Context, peer PeerConfig) (PeerConnection, error) {
	r.peerMu.RLock()
	conn := r.peerConns[peer.ID]
	r.peerMu.RUnlock()

	if conn != nil && conn.IsConnected() {
		return conn, nil
	}

	if r.transport == nil {
		return nil, fmt.Errorf("no transport configured")
	}

	newConn, err := r.transport.Connect(ctx, peer.Addr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", peer.Addr, err)
	}

	r.peerMu.Lock()
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
	defer r.mu.Unlock()

	resp := &VoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
		VoterID:     r.config.NodeID,
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

			log.Printf("[Raft %s] Granted vote to %s for term %d", r.config.NodeID, req.CandidateID, req.Term)
		}
	}

	return resp
}

// handleAppendEntriesRequest processes an incoming AppendEntries request.
func (r *RaftReplicator) handleAppendEntriesRequest(req *AppendEntriesRequest) *AppendEntriesResponse {
	r.mu.Lock()

	resp := &AppendEntriesResponse{
		Term:         r.currentTerm,
		Success:      false,
		ResponderID:  r.config.NodeID,
		CodecVersion: CurrentCodecVersion,
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
				// Conflict: delete this and all following entries
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

	// Apply committed entries to state machine
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if int(r.lastApplied) < len(r.log) {
			entry := r.log[r.lastApplied]
			if entry.Command != nil {
				if err := r.storage.ApplyCommand(entry.Command); err != nil {
					log.Printf("[Raft %s] Failed to apply entry %d: %v", r.config.NodeID, entry.Index, err)
				}
			}
		}
	}

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
		log.Printf("[Raft %s] Listen error: %v", r.config.NodeID, err)
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
	return r.Apply(cmd, timeout)
}

// Apply applies a command through Raft consensus.
// If this node is not the leader, it forwards the command to the leader automatically.
func (r *RaftReplicator) Apply(cmd *Command, timeout time.Duration) error {
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

	if r.closed.Load() {
		return ErrClosed
	}
	if !r.started.Load() {
		return ErrNotReady
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
		fwdCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		conn, err := r.transport.Connect(fwdCtx, leaderAddr)
		if err != nil {
			return fmt.Errorf("forward to leader: %w", err)
		}
		if fa, ok := conn.(forwardApplySender); ok {
			return fa.SendForwardApply(fwdCtx, cmd, timeout)
		}
		return ErrNotLeader
	}

	future := &applyFuture{
		cmd:   cmd,
		errCh: make(chan error, 1),
	}

	select {
	case r.applyCh <- future:
	case <-time.After(timeout):
		return fmt.Errorf("apply queue full")
	}

	select {
	case err := <-future.errCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("apply timeout")
	}
}

// ApplyBatch applies multiple commands atomically.
func (r *RaftReplicator) ApplyBatch(cmds []*Command, timeout time.Duration) error {
	for _, cmd := range cmds {
		if err := r.Apply(cmd, timeout); err != nil {
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
	peers := make([]PeerStatus, 0, len(r.config.Raft.Peers))
	for _, peer := range r.config.Raft.Peers {
		conn := r.peerConns[peer.ID]
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
	r.peerMu.RUnlock()

	r.logMu.RLock()
	commitIdx := r.commitIndex
	appliedIdx := r.lastApplied
	r.logMu.RUnlock()

	return &HealthStatus{
		Mode:         ModeRaft,
		NodeID:       r.config.NodeID,
		Role:         role,
		IsLeader:     isLeader,
		LeaderID:     leaderID,
		LeaderAddr:   leaderAddr,
		State:        stateStr,
		Healthy:      r.started.Load() && !r.closed.Load(),
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
	if r.closed.Load() {
		return nil
	}

	log.Printf("[Raft %s] Shutting down", r.config.NodeID)
	r.closed.Store(true)
	close(r.stopCh)

	// Close transport to stop listener (unblocks listenForPeers goroutine)
	if r.transport != nil {
		r.transport.Close()
	}

	// Close all peer connections
	r.peerMu.Lock()
	for _, conn := range r.peerConns {
		if conn != nil {
			conn.Close()
		}
	}
	r.peerMu.Unlock()

	// Fail any pending futures
	r.futuresMu.Lock()
	for _, future := range r.futures {
		select {
		case future.errCh <- ErrClosed:
		default:
		}
	}
	r.futures = make(map[uint64]*applyFuture)
	r.futuresMu.Unlock()

	// Wait for goroutines
	r.wg.Wait()

	return nil
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
	isLeader := r.state == StateLeader
	r.mu.RUnlock()

	if !isLeader {
		return ErrNotLeader
	}

	r.peerMu.Lock()
	defer r.peerMu.Unlock()

	// Check if already exists
	for _, peer := range r.config.Raft.Peers {
		if peer.ID == id {
			return nil // Already a member
		}
	}

	// Add new peer
	r.config.Raft.Peers = append(r.config.Raft.Peers, PeerConfig{
		ID:   id,
		Addr: addr,
	})

	// Initialize tracking for new peer
	r.logMu.RLock()
	lastIdx := r.log[len(r.log)-1].Index
	r.logMu.RUnlock()

	r.nextIndex[id] = lastIdx + 1
	r.matchIndex[id] = 0

	log.Printf("[Raft %s] Added voter %s at %s", r.config.NodeID, id, addr)
	return nil
}

// RemoveServer removes a server from the cluster.
func (r *RaftReplicator) RemoveServer(id string) error {
	r.mu.RLock()
	isLeader := r.state == StateLeader
	r.mu.RUnlock()

	if !isLeader {
		return ErrNotLeader
	}

	r.peerMu.Lock()
	defer r.peerMu.Unlock()

	// Find and remove peer
	newPeers := make([]PeerConfig, 0, len(r.config.Raft.Peers))
	for _, peer := range r.config.Raft.Peers {
		if peer.ID != id {
			newPeers = append(newPeers, peer)
		}
	}
	r.config.Raft.Peers = newPeers

	// Close connection and clean up tracking
	if conn := r.peerConns[id]; conn != nil {
		conn.Close()
		delete(r.peerConns, id)
	}
	delete(r.nextIndex, id)
	delete(r.matchIndex, id)

	log.Printf("[Raft %s] Removed server %s", r.config.NodeID, id)
	return nil
}

// GetConfiguration returns the current cluster configuration.
func (r *RaftReplicator) GetConfiguration() ([]PeerStatus, error) {
	r.mu.RLock()
	state := r.state
	r.mu.RUnlock()

	r.peerMu.RLock()
	defer r.peerMu.RUnlock()

	result := make([]PeerStatus, 0, len(r.config.Raft.Peers)+1)

	// Add self
	result = append(result, PeerStatus{
		ID:      r.config.NodeID,
		Address: r.config.AdvertiseAddr,
		Healthy: true,
		State:   state.String(),
	})

	// Add peers
	for _, peer := range r.config.Raft.Peers {
		conn := r.peerConns[peer.ID]
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
