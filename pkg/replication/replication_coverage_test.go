package replication

import (
	"fmt"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCoverageRaftReplicator(t *testing.T) (*RaftReplicator, *MockStorage) {
	t.Helper()
	store := NewMockStorage()
	cfg := DefaultConfig()
	cfg.Mode = ModeRaft
	cfg.NodeID = "raft-node"
	cfg.AdvertiseAddr = "127.0.0.1:17000"
	cfg.Raft.Bootstrap = true
	r, err := NewRaftReplicator(cfg, store)
	require.NoError(t, err)
	return r, store
}

type coverageReplicator struct {
	applyErr        error
	forwardApplyErr error
	isLeader        bool
	nodeID          string

	applied        []*Command
	lastTimeout    time.Duration
	lastForwardCmd *Command

	walEntries   []*WALEntry
	heartbeatReq *HeartbeatRequest
	fenceReq     *FenceRequest
	promoteReq   *PromoteRequest
	voteReq      *RaftVoteRequest
	appendReq    *RaftAppendEntriesRequest
}

type raftCoveragePeerConn struct {
	connected  bool
	voteResp   *RaftVoteResponse
	voteErr    error
	appendResp *RaftAppendEntriesResponse
	appendErr  error
}

func (c *raftCoveragePeerConn) SendWALBatch(context.Context, []*WALEntry) (*WALBatchResponse, error) {
	return &WALBatchResponse{}, nil
}
func (c *raftCoveragePeerConn) SendHeartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return &HeartbeatResponse{}, nil
}
func (c *raftCoveragePeerConn) SendFence(context.Context, *FenceRequest) (*FenceResponse, error) {
	return &FenceResponse{Fenced: true}, nil
}
func (c *raftCoveragePeerConn) SendPromote(context.Context, *PromoteRequest) (*PromoteResponse, error) {
	return &PromoteResponse{Ready: true}, nil
}
func (c *raftCoveragePeerConn) SendRaftVote(context.Context, *RaftVoteRequest) (*RaftVoteResponse, error) {
	if c.voteErr != nil {
		return nil, c.voteErr
	}
	if c.voteResp != nil {
		return c.voteResp, nil
	}
	return &RaftVoteResponse{Term: 1, VoteGranted: true, VoterID: "peer"}, nil
}
func (c *raftCoveragePeerConn) SendRaftAppendEntries(context.Context, *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	if c.appendErr != nil {
		return nil, c.appendErr
	}
	if c.appendResp != nil {
		return c.appendResp, nil
	}
	return &RaftAppendEntriesResponse{Term: 1, Success: true, MatchIndex: 0, ResponderID: "peer"}, nil
}
func (c *raftCoveragePeerConn) Close() error      { c.connected = false; return nil }
func (c *raftCoveragePeerConn) IsConnected() bool { return c.connected }

type raftCoverageTransport struct {
	conn       PeerConnection
	connectErr error
}

func (t *raftCoverageTransport) Connect(context.Context, string) (PeerConnection, error) {
	if t.connectErr != nil {
		return nil, t.connectErr
	}
	return t.conn, nil
}
func (t *raftCoverageTransport) Listen(context.Context, string, ConnectionHandler) error { return nil }
func (t *raftCoverageTransport) Close() error                                            { return nil }

type raftForwardPeerConn struct {
	raftCoveragePeerConn
	forwardErr error
}

func (c *raftForwardPeerConn) SendForwardApply(context.Context, *Command, time.Duration) error {
	return c.forwardErr
}

func (m *coverageReplicator) Start(context.Context) error { return nil }
func (m *coverageReplicator) Apply(cmd *Command, timeout time.Duration) error {
	m.applied = append(m.applied, cmd)
	m.lastTimeout = timeout
	return m.applyErr
}
func (m *coverageReplicator) ApplyBatch([]*Command, time.Duration) error { return nil }
func (m *coverageReplicator) IsLeader() bool                             { return m.isLeader }
func (m *coverageReplicator) LeaderAddr() string                         { return "" }
func (m *coverageReplicator) LeaderID() string                           { return "leader" }
func (m *coverageReplicator) Health() *HealthStatus                      { return &HealthStatus{} }
func (m *coverageReplicator) WaitForLeader(context.Context) error        { return nil }
func (m *coverageReplicator) LifetimeContext() context.Context           { return context.Background() }
func (m *coverageReplicator) Shutdown() error                            { return nil }
func (m *coverageReplicator) Mode() ReplicationMode                      { return ModeStandalone }
func (m *coverageReplicator) NodeID() string                             { return m.nodeID }
func (m *coverageReplicator) HandleWALBatch(entries []*WALEntry) (*WALBatchResponse, error) {
	m.walEntries = entries
	return &WALBatchResponse{AckedPosition: 42, ReceivedPosition: 42}, nil
}
func (m *coverageReplicator) HandleHeartbeat(req *HeartbeatRequest) (*HeartbeatResponse, error) {
	m.heartbeatReq = req
	return &HeartbeatResponse{NodeID: "peer", Role: "primary"}, nil
}
func (m *coverageReplicator) HandleFence(req *FenceRequest) (*FenceResponse, error) {
	m.fenceReq = req
	return &FenceResponse{Fenced: true}, nil
}
func (m *coverageReplicator) HandlePromote(req *PromoteRequest) (*PromoteResponse, error) {
	m.promoteReq = req
	return &PromoteResponse{Ready: true}, nil
}
func (m *coverageReplicator) HandleRaftVote(req *RaftVoteRequest) (*RaftVoteResponse, error) {
	m.voteReq = req
	return &RaftVoteResponse{Term: req.Term, VoteGranted: true}, nil
}
func (m *coverageReplicator) HandleRaftAppendEntries(req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	m.appendReq = req
	return &RaftAppendEntriesResponse{Term: req.Term, Success: true, MatchIndex: req.PrevLogIndex}, nil
}
func (m *coverageReplicator) HandleForwardApply(cmd *Command, timeout time.Duration) error {
	m.lastForwardCmd = cmd
	m.lastTimeout = timeout
	return m.forwardApplyErr
}

func TestReplicatedEngine_MethodCoverage(t *testing.T) {
	t.Parallel()
	t.Run("standalone backing", func(t *testing.T) {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { _ = base.Close() })

		engine := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: 5 * time.Second}).engine
		var err error

		_, err = engine.CreateNode(&storage.Node{ID: "db1:n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "db2:n2", Labels: []string{"Person"}})
		require.NoError(t, err)

		// Write must be visible on the underlying engine.
		_, err = base.GetNode("db1:n1")
		require.NoError(t, err)

		// Drop prefix must remove only matching IDs.
		_, _, err = engine.DeleteByPrefix("db1:")
		require.NoError(t, err)
		_, err = base.GetNode("db1:n1")
		require.ErrorIs(t, err, storage.ErrNotFound)
		_, err = base.GetNode("db2:n2")
		require.NoError(t, err)

	})

	var nilEngine *ReplicatedEngine
	assert.True(t, nilEngine.IsLeader())

	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })

	noRepl := NewReplicatedEngine(base, nil, 0)
	assert.True(t, noRepl.IsLeader())

	mock := &coverageReplicator{isLeader: false}
	engine := NewReplicatedEngine(base, mock, 0)
	assert.False(t, engine.IsLeader())

	_, err := engine.CreateNode(nil)
	require.ErrorContains(t, err, "nil node")
	require.ErrorContains(t, engine.UpdateNode(nil), "nil node")
	require.ErrorContains(t, engine.CreateEdge(nil), "nil edge")
	require.ErrorContains(t, engine.UpdateEdge(nil), "nil edge")

	createdID, err := engine.CreateNode(&storage.Node{ID: storage.NodeID("created-1"), Labels: []string{"Person"}})
	require.NoError(t, err)
	assert.Equal(t, storage.NodeID("created-1"), createdID)

	node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person"}}
	require.NoError(t, engine.UpdateNode(node))
	require.NoError(t, engine.DeleteNode("n1"))
	require.Len(t, mock.applied, 3)
	assert.Equal(t, CmdCreateNode, mock.applied[0].Type)
	assert.Equal(t, CmdUpdateNode, mock.applied[1].Type)
	assert.Equal(t, CmdDeleteNode, mock.applied[2].Type)
	assert.Equal(t, 30*time.Second, mock.lastTimeout)

	edge := &storage.Edge{ID: storage.EdgeID("e1"), StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.UpdateEdge(edge))
	require.NoError(t, engine.DeleteEdge("e1"))
	require.Len(t, mock.applied, 6)
	assert.Equal(t, CmdCreateEdge, mock.applied[3].Type)
	assert.Equal(t, CmdUpdateEdge, mock.applied[4].Type)
	assert.Equal(t, CmdDeleteEdge, mock.applied[5].Type)

	require.NoError(t, engine.BulkCreateNodes([]*storage.Node{
		{ID: storage.NodeID("n3"), Labels: []string{"L"}},
		{ID: storage.NodeID("n4"), Labels: []string{"L"}},
	}))
	require.NoError(t, engine.BulkCreateEdges([]*storage.Edge{
		{ID: storage.EdgeID("e2"), StartNode: "n3", EndNode: "n4", Type: "REL"},
	}))
	require.NoError(t, engine.BulkDeleteNodes([]storage.NodeID{"n3"}))
	require.NoError(t, engine.BulkDeleteEdges([]storage.EdgeID{"e2"}))
	_, _, err = engine.DeleteByPrefix("db1:")
	require.NoError(t, err)

	require.NoError(t, storage.DecodeCommandPayload(mock.applied[6].Data, &[][]byte{}))
	require.NoError(t, storage.DecodeCommandPayload(mock.applied[7].Data, &[][]byte{}))
	require.NoError(t, storage.DecodeCommandPayload(mock.applied[8].Data, &[]storage.NodeID{}))
	require.NoError(t, storage.DecodeCommandPayload(mock.applied[9].Data, &[]storage.EdgeID{}))

	mock.applyErr = errors.New("replicate failed")
	_, err = engine.CreateNode(&storage.Node{ID: "n-fail-create", Labels: []string{"L"}})
	require.ErrorContains(t, err, "replicate failed")
	err = engine.DeleteNode("n-fail")
	require.ErrorContains(t, err, "replicate failed")
	_, _, err = engine.DeleteByPrefix("db-fail:")
	require.ErrorContains(t, err, "replicate failed")
	require.ErrorContains(t, engine.DeleteEdge("e-fail"), "replicate failed")
	require.ErrorContains(t, engine.BulkDeleteNodes([]storage.NodeID{"n-fail"}), "replicate failed")
	require.ErrorContains(t, engine.BulkDeleteEdges([]storage.EdgeID{"e-fail"}), "replicate failed")
	_, err = engine.CreateNode(&storage.Node{
		ID:     "n-bad-encode",
		Labels: []string{"L"},
		Properties: map[string]any{
			"bad": make(chan int),
		},
	})
	require.ErrorContains(t, err, "encode node")

	require.ErrorContains(t, engine.BulkCreateNodes([]*storage.Node{nil}), "encode node")
	require.ErrorContains(t, engine.BulkCreateEdges([]*storage.Edge{nil}), "encode edge")
}

func TestRegisterClusterHandlers_AndDispatch(t *testing.T) {
	t.Parallel()

	tr := NewClusterTransport(&ClusterTransportConfig{NodeID: "node-a"})
	mock := &coverageReplicator{isLeader: true, nodeID: "node-a"}

	RegisterClusterHandlers(nil, mock)
	RegisterClusterHandlers(tr, nil)

	RegisterClusterHandlers(tr, mock)
	require.Len(t, tr.handlers, 7)

	walPayload, err := storage.EncodeCommandPayload([]*WALEntry{{Position: 7, Command: &Command{Type: CmdCreateNode}}})
	require.NoError(t, err)
	walResp, err := tr.handlers[ClusterMsgWALBatch](context.Background(), "peer-1", &ClusterMessage{Payload: walPayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgWALBatchResponse, walResp.Type)
	require.Len(t, mock.walEntries, 1)
	assert.Equal(t, uint64(7), mock.walEntries[0].Position)

	heartbeatPayload, err := storage.EncodeCommandPayload(HeartbeatRequest{NodeID: "peer-1", Role: "standby"})
	require.NoError(t, err)
	heartbeatResp, err := tr.handlers[ClusterMsgHeartbeat](context.Background(), "peer-1", &ClusterMessage{Payload: heartbeatPayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgHeartbeatResponse, heartbeatResp.Type)
	require.NotNil(t, mock.heartbeatReq)
	assert.Equal(t, "peer-1", mock.heartbeatReq.NodeID)

	_, err = tr.handlers[ClusterMsgHeartbeat](context.Background(), "peer-1", &ClusterMessage{Payload: []byte("bad")})
	require.ErrorContains(t, err, "decode heartbeat")

	fencePayload, err := storage.EncodeCommandPayload(FenceRequest{Reason: "failover"})
	require.NoError(t, err)
	fenceResp, err := tr.handlers[ClusterMsgFence](context.Background(), "peer-1", &ClusterMessage{Payload: fencePayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgFenceResponse, fenceResp.Type)
	assert.Equal(t, "failover", mock.fenceReq.Reason)

	promotePayload, err := storage.EncodeCommandPayload(PromoteRequest{Reason: "planned"})
	require.NoError(t, err)
	promoteResp, err := tr.handlers[ClusterMsgPromote](context.Background(), "peer-1", &ClusterMessage{Payload: promotePayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgPromoteResponse, promoteResp.Type)
	assert.Equal(t, "planned", mock.promoteReq.Reason)

	votePayload, err := storage.EncodeCommandPayload(RaftVoteRequest{Term: 5, CandidateID: "cand"})
	require.NoError(t, err)
	voteResp, err := tr.handlers[ClusterMsgVoteRequest](context.Background(), "peer-1", &ClusterMessage{Payload: votePayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgVoteResponse, voteResp.Type)
	assert.Equal(t, uint64(5), mock.voteReq.Term)

	appendPayload, err := storage.EncodeCommandPayload(RaftAppendEntriesRequest{Term: 7, LeaderID: "l1", PrevLogIndex: 9})
	require.NoError(t, err)
	appendResp, err := tr.handlers[ClusterMsgAppendEntries](context.Background(), "peer-1", &ClusterMessage{Payload: appendPayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgAppendEntriesResponse, appendResp.Type)
	assert.Equal(t, uint64(9), mock.appendReq.PrevLogIndex)

	forwardPayload, err := storage.EncodeCommandPayload(Command{Type: CmdCreateNode, Data: []byte("x")})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	forwardResp, err := tr.handlers[ClusterMsgForwardApply](ctx, "peer-1", &ClusterMessage{Payload: forwardPayload})
	require.NoError(t, err)
	assert.Equal(t, ClusterMsgForwardApplyResponse, forwardResp.Type)
	require.NotNil(t, mock.lastForwardCmd)
	assert.Equal(t, CmdCreateNode, mock.lastForwardCmd.Type)
	assert.Positive(t, mock.lastTimeout)
	assert.LessOrEqual(t, mock.lastTimeout, 30*time.Second)

	mock.forwardApplyErr = errors.New("forward apply failed")
	forwardResp, err = tr.handlers[ClusterMsgForwardApply](context.Background(), "peer-1", &ClusterMessage{Payload: forwardPayload})
	require.NoError(t, err)
	var fr forwardApplyResponse
	require.NoError(t, storage.DecodeCommandPayload(forwardResp.Payload, &fr))
	assert.Equal(t, "forward apply failed", fr.Err)
}

func TestStorageAdapter_ApplyCommand_UpdateEdgeAndBulkCommands(t *testing.T) {
	t.Parallel()

	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })

	_, err := adapter.engine.CreateNode(&storage.Node{ID: "n1", Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = adapter.engine.CreateNode(&storage.Node{ID: "n2", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, adapter.engine.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}))

	updatedEdge := &storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "KNOWS",
		Properties: map[string]any{"weight": float64(2)},
	}
	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdUpdateEdge,
		Data:      runtimeFixtureOwner.Encode(t, storage.EncodeEdgePayload, updatedEdge),
		Timestamp: time.Now(),
	}))
	gotEdge, err := adapter.engine.GetEdge("e1")
	require.NoError(t, err)
	assert.Equal(t, float64(2), gotEdge.Properties["weight"])

	nodeBytes1 := runtimeFixtureOwner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "n3", Labels: []string{"L"}})
	nodeBytes2 := runtimeFixtureOwner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: "n4", Labels: []string{"L"}})
	bulkNodesPayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, [][]byte{nodeBytes1, nodeBytes2})
	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdBulkCreateNodes,
		Data:      bulkNodesPayload,
		Timestamp: time.Now(),
	}))
	_, err = adapter.engine.GetNode("n3")
	require.NoError(t, err)
	_, err = adapter.engine.GetNode("n4")
	require.NoError(t, err)

	edgeBytes := runtimeFixtureOwner.Encode(t, storage.EncodeEdgePayload, &storage.Edge{ID: "e2", StartNode: "n3", EndNode: "n4", Type: "REL"})
	bulkEdgesPayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, [][]byte{edgeBytes})
	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdBulkCreateEdges,
		Data:      bulkEdgesPayload,
		Timestamp: time.Now(),
	}))
	_, err = adapter.engine.GetEdge("e2")
	require.NoError(t, err)

	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdBulkDeleteNodes,
		Data:      runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, []storage.NodeID{"n4"}),
		Timestamp: time.Now(),
	}))
	_, err = adapter.engine.GetNode("n4")
	require.ErrorIs(t, err, storage.ErrNotFound)

	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdBulkDeleteEdges,
		Data:      runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, []storage.EdgeID{"e2"}),
		Timestamp: time.Now(),
	}))
	_, err = adapter.engine.GetEdge("e2")
	require.ErrorIs(t, err, storage.ErrNotFound)

	err = adapter.ApplyCommand(&Command{Type: CmdBulkDeleteEdges, Data: []byte("bad"), Timestamp: time.Now()})
	require.ErrorContains(t, err, "decode bulk delete edges")
}

func TestStandaloneReplicator_MetadataMethods(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Mode = ModeStandalone
	cfg.NodeID = "solo-1"
	r := NewStandaloneReplicator(cfg, NewMockStorage())

	assert.Equal(t, "", r.LeaderAddr())
	assert.Equal(t, "solo-1", r.LeaderID())
	assert.Equal(t, "solo-1", r.NodeID())
}

func TestHAStandbyReplicator_UncoveredHelpers(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "ha-1"
	cfg.AdvertiseAddr = "127.0.0.1:17001"
	cfg.HAStandby.Role = "standby"
	cfg.HAStandby.PeerAddr = "127.0.0.1:17002"
	cfg.HAStandby.SyncMode = SyncAsync
	cfg.HAStandby.AutoFailover = false
	cfg.HAStandby.FailoverTimeout = 1 * time.Millisecond

	store := NewMockStorage()
	r, err := NewHAStandbyReplicator(cfg, store)
	require.NoError(t, err)
	r.transport = NewMockTransport()
	r.started.Store(true)

	cmds := []*Command{
		{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()},
		{Type: CmdUpdateNode, Data: []byte("y"), Timestamp: time.Now()},
	}
	require.Error(t, r.ApplyBatch(cmds, 50*time.Millisecond))

	r.isPrimary.Store(true)
	assert.Equal(t, cfg.NodeID, r.LeaderID())
	assert.NoError(t, r.WaitForLeader(context.Background()))

	r.isPrimary.Store(false)
	assert.Equal(t, "", r.LeaderID())

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.WaitForLeader(waitCtx), context.DeadlineExceeded)

	r.primaryHealthy.Store(true)
	require.NoError(t, r.WaitForLeader(context.Background()))

	resp, err := r.HandlePromote(&PromoteRequest{Reason: "manual"})
	require.NoError(t, err)
	assert.True(t, resp.Ready)

	r.lastPrimaryHB = time.Now().Add(-time.Second)
	r.primaryHealthy.Store(true)
	r.checkPrimaryHealth(context.Background())
	assert.False(t, r.primaryHealthy.Load())

	mockConn := &MockPeerConn{connected: true}
	r.primaryConn = mockConn
	r.fenceOldPrimary(context.Background())
	assert.Equal(t, 1, mockConn.fenceCalls)

	cancelCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	r.isPromoted.Store(false)
	r.triggerAutoFailover(cancelCtx)
	assert.True(t, r.isPromoted.Load())
	assert.True(t, r.isPrimary.Load())

	assert.NotNil(t, NewDefaultTransport(DefaultClusterTransportConfig()))
}

func TestRaftReplicator_UncoveredHandlersAndAdminMethods(t *testing.T) {
	t.Parallel()

	r, store := newCoverageRaftReplicator(t)

	_, err := r.HandleRaftVote(nil)
	require.ErrorContains(t, err, "nil vote request")
	_, err = r.HandleRaftAppendEntries(nil)
	require.ErrorContains(t, err, "nil append entries request")

	r.currentTerm = 3
	r.votedFor = ""
	r.log = []*RaftLogEntry{
		{Index: 0, Term: 0, Command: nil},
		{Index: 1, Term: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("a"), Timestamp: time.Now()}},
	}
	respVote := r.handleVoteRequest(&VoteRequest{Term: 2, CandidateID: "c1", LastLogIndex: 1, LastLogTerm: 2})
	assert.False(t, respVote.VoteGranted)

	respVote = r.handleVoteRequest(&VoteRequest{Term: 4, CandidateID: "c1", LastLogIndex: 1, LastLogTerm: 2})
	assert.True(t, respVote.VoteGranted)
	assert.Equal(t, uint64(4), respVote.Term)

	respVote = r.handleVoteRequest(&VoteRequest{Term: 4, CandidateID: "c2", LastLogIndex: 1, LastLogTerm: 2})
	assert.False(t, respVote.VoteGranted)

	r.currentTerm = 5
	appendResp := r.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 4, LeaderID: "l1"})
	assert.False(t, appendResp.Success)

	r.currentTerm = 5
	appendResp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         6,
		LeaderID:     "l1",
		LeaderAddr:   "127.0.0.1:17010",
		PrevLogIndex: 99,
		PrevLogTerm:  3,
	})
	assert.False(t, appendResp.Success)
	assert.Greater(t, appendResp.ConflictIndex, uint64(0))

	r.log = []*RaftLogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("n1"), Timestamp: time.Now()}},
	}
	r.commitIndex = 0
	r.lastApplied = 0
	appendResp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         7,
		LeaderID:     "l1",
		LeaderAddr:   "127.0.0.1:17010",
		PrevLogIndex: 1,
		PrevLogTerm:  2,
		Entries: []*RaftLogEntry{
			{Index: 2, Term: 7, Command: &Command{Type: CmdUpdateNode, Data: []byte("n1"), Timestamp: time.Now()}},
		},
		LeaderCommit: 2,
	})
	assert.True(t, appendResp.Success)
	assert.Equal(t, uint64(2), appendResp.MatchIndex)
	assert.Equal(t, 2, store.GetApplyCount())

	r.started.Store(true)
	r.state = StateFollower
	r.leaderAddr = ""
	assert.ErrorIs(t, r.HandleForwardApply(&Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}, 20*time.Millisecond), ErrNoLeader)
	assert.ErrorIs(t, r.ApplyBatch([]*Command{{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}}, 20*time.Millisecond), ErrNoLeader)

	r.state = StateLeader
	assert.Equal(t, r.config.NodeID, r.LeaderID())
	assert.Equal(t, r.config.NodeID, r.NodeID())

	r.state = StateFollower
	r.leaderID = "leader-2"
	assert.Equal(t, "leader-2", r.LeaderID())

	assert.ErrorIs(t, r.AddVoter("p1", "127.0.0.1:17011"), ErrNotLeader)
	r.state = StateLeader
	require.NoError(t, r.AddVoter("p1", "127.0.0.1:17011"))
	require.NoError(t, r.AddVoter("p1", "127.0.0.1:17011"))
	require.Len(t, r.config.Raft.Peers, 1)

	r.peerConns["p1"] = &MockPeerConn{connected: true}
	conf, err := r.GetConfiguration()
	require.NoError(t, err)
	require.Len(t, conf, 2)

	require.NoError(t, r.RemoveServer("p1"))
	require.Empty(t, r.config.Raft.Peers)
}

func TestMultiRegionReplicator_UncoveredWrapperMethods(t *testing.T) {
	t.Parallel()

	local, _ := newCoverageRaftReplicator(t)
	local.started.Store(true)
	local.state = StateFollower
	local.leaderAddr = "127.0.0.1:17050"
	local.leaderID = "leader-x"

	cfg := DefaultConfig()
	cfg.Mode = ModeMultiRegion
	cfg.NodeID = "region-node-1"
	cfg.MultiRegion.RegionID = "us-east"

	r := &MultiRegionReplicator{
		config:      cfg,
		storage:     NewMockStorage(),
		localRaft:   local,
		remoteConns: make(map[string]PeerConnection),
	}
	r.started.Store(true)

	assert.Equal(t, "127.0.0.1:17050", r.LeaderAddr())
	assert.Equal(t, "leader-x", r.LeaderID())
	assert.Equal(t, "region-node-1", r.NodeID())

	err := r.ApplyBatch([]*Command{{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}}, 10*time.Millisecond)
	require.ErrorIs(t, err, ErrNotLeader)

	remote := &MockPeerConn{connected: true}
	r.remoteConns["eu-west"] = remote
	store := r.storage.(*MockStorage)
	store.walPosition = 3
	r.streamWALToRemoteRegions(context.Background())
	assert.Equal(t, 1, remote.GetWALBatchCalls())
	assert.Equal(t, uint64(3), r.walPosition)

	r.notifyRegionsOfFailover(context.Background())
	time.Sleep(25 * time.Millisecond)
	assert.GreaterOrEqual(t, remote.GetHeartbeatCalls(), 1)
}

func TestStorageAdapter_PruneWALEntries(t *testing.T) {
	t.Parallel()

	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })

	adapter.memWAL = []*WALEntry{
		{Position: 1}, {Position: 2}, {Position: 3}, {Position: 4},
	}

	adapter.PruneWALEntries(2)
	require.Len(t, adapter.memWAL, 2)
	assert.Equal(t, uint64(3), adapter.memWAL[0].Position)

	adapter.PruneWALEntries(10)
	assert.Empty(t, adapter.memWAL)
}

func TestRaftReplicator_InternalCoverage(t *testing.T) {
	t.Parallel()

	r, store := newCoverageRaftReplicator(t)
	r.started.Store(true)

	voteReq := &VoteRequest{Term: 1, CandidateID: "c1", LastLogIndex: 0, LastLogTerm: 0}
	voteConn := &raftCoveragePeerConn{connected: true, voteResp: &RaftVoteResponse{Term: 1, VoteGranted: true, VoterID: "peer-1"}}
	voteResp, err := r.sendVoteRequestRPC(context.Background(), voteConn, voteReq)
	require.NoError(t, err)
	assert.True(t, voteResp.VoteGranted)

	_, err = r.sendVoteRequestRPC(context.Background(), &raftCoveragePeerConn{connected: true, voteErr: errors.New("vote down")}, voteReq)
	require.ErrorContains(t, err, "send vote request")

	appendReq := &AppendEntriesRequest{Term: 1, LeaderID: "l1", PrevLogIndex: 0}
	appendConn := &raftCoveragePeerConn{connected: true, appendResp: &RaftAppendEntriesResponse{Term: 1, Success: true, MatchIndex: 1, ResponderID: "peer-1"}}
	appendResp, err := r.sendAppendEntriesRPC(context.Background(), appendConn, appendReq)
	require.NoError(t, err)
	assert.True(t, appendResp.Success)

	_, err = r.sendAppendEntriesRPC(context.Background(), &raftCoveragePeerConn{connected: true, appendErr: errors.New("append down")}, appendReq)
	require.ErrorContains(t, err, "send append entries")

	peer := PeerConfig{ID: "p1", Addr: "127.0.0.1:17111"}
	r.peerConns["p1"] = &raftCoveragePeerConn{connected: true}
	gotConn, err := r.getOrConnectPeer(context.Background(), peer)
	require.NoError(t, err)
	require.NotNil(t, gotConn)

	r.peerConns["p2"] = nil
	_, err = r.getOrConnectPeer(context.Background(), PeerConfig{ID: "p2", Addr: "127.0.0.1:17112"})
	require.ErrorContains(t, err, "no transport configured")

	r.transport = &raftCoverageTransport{connectErr: errors.New("dial fail")}
	_, err = r.getOrConnectPeer(context.Background(), PeerConfig{ID: "p2", Addr: "127.0.0.1:17112"})
	require.ErrorContains(t, err, "connect to 127.0.0.1:17112")

	newConn := &raftCoveragePeerConn{connected: true}
	r.transport = &raftCoverageTransport{conn: newConn}
	gotConn, err = r.getOrConnectPeer(context.Background(), PeerConfig{ID: "p2", Addr: "127.0.0.1:17112"})
	require.NoError(t, err)
	assert.Equal(t, newConn, gotConn)

	r.state = StateCandidate
	r.currentTerm = 2
	r.log = []*RaftLogEntry{{Index: 0, Term: 0}}
	votes := 1
	var mu sync.Mutex
	r.requestVoteFromPeer(context.Background(), peer, voteReq, 2, &mu, &votes, 2)
	assert.Equal(t, 2, votes)

	r.state = StateCandidate
	r.currentTerm = 3
	r.peerConns["p1"] = nil
	r.transport = &raftCoverageTransport{conn: &raftCoveragePeerConn{connected: true, voteResp: &RaftVoteResponse{Term: 5, VoteGranted: false, VoterID: "peer-1"}}}
	r.requestVoteFromPeer(context.Background(), peer, &VoteRequest{Term: 3}, 3, &mu, &votes, 3)
	assert.Equal(t, StateFollower, r.state)
	assert.Equal(t, uint64(5), r.currentTerm)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	r.mu.Lock()
	r.state = StateCandidate
	r.currentTerm = 7
	r.mu.Unlock()
	r.peerMu.Lock()
	r.config.Raft.Peers = []PeerConfig{{ID: "p3", Addr: "127.0.0.1:17113"}}
	r.peerMu.Unlock()
	r.logMu.Lock()
	r.log = []*RaftLogEntry{{Index: 0, Term: 0}, {Index: 1, Term: 7}}
	r.logMu.Unlock()
	r.becomeLeader(cancelCtx, 7)
	r.mu.RLock()
	assert.Equal(t, StateLeader, r.state)
	assert.Equal(t, r.config.NodeID, r.leaderID)
	r.mu.RUnlock()

	r.mu.Lock()
	r.state = StateLeader
	r.currentTerm = 9
	r.mu.Unlock()
	r.peerMu.Lock()
	r.matchIndex["p3"] = 0
	r.nextIndex["p3"] = 1
	r.peerMu.Unlock()
	r.handleAppendEntriesResponse("p3", 9, appendReq, &AppendEntriesResponse{Term: 9, Success: true, MatchIndex: 2})
	r.peerMu.RLock()
	assert.Equal(t, uint64(2), r.matchIndex["p3"])
	assert.Equal(t, uint64(3), r.nextIndex["p3"])
	r.peerMu.RUnlock()

	r.handleAppendEntriesResponse("p3", 9, appendReq, &AppendEntriesResponse{Term: 9, Success: false, ConflictIndex: 1})
	r.peerMu.RLock()
	assert.Equal(t, uint64(1), r.nextIndex["p3"])
	r.peerMu.RUnlock()

	r.mu.Lock()
	r.state = StateLeader
	r.currentTerm = 10
	r.mu.Unlock()
	r.handleAppendEntriesResponse("p3", 10, appendReq, &AppendEntriesResponse{Term: 11})
	r.mu.RLock()
	assert.Equal(t, StateFollower, r.state)
	assert.Equal(t, uint64(11), r.currentTerm)
	r.mu.RUnlock()

	// Mutate state through the same locks production code uses. The
	// becomeLeader call above started a heartbeat goroutine that reads
	// r.log / r.commitIndex under r.logMu and r.nextIndex / r.matchIndex
	// under r.peerMu, so unlocked field assignment here triggers a
	// real data race when -race is enabled. Production code never
	// rewrites r.log / r.commitIndex without holding r.logMu, and never
	// rewrites r.matchIndex without holding r.peerMu — match that here.
	r.mu.Lock()
	r.state = StateLeader
	r.currentTerm = 12
	r.mu.Unlock()
	r.peerMu.Lock()
	r.config.Raft.Peers = []PeerConfig{{ID: "p3", Addr: "127.0.0.1:17113"}}
	r.matchIndex["p3"] = 1
	r.peerMu.Unlock()
	r.logMu.Lock()
	r.log = []*RaftLogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 12, Command: &Command{Type: CmdCreateNode, Data: []byte("a"), Timestamp: time.Now()}},
	}
	r.commitIndex = 0
	r.lastApplied = 0
	r.logMu.Unlock()
	r.advanceCommitIndex()
	assert.Equal(t, uint64(1), r.commitIndex)
	assert.Equal(t, 1, store.GetApplyCount())

	future := &applyFuture{index: 3, errCh: make(chan error, 1)}
	r.config.Raft.CommitTimeout = 10 * time.Millisecond
	r.waitForCommit(context.Background(), future)
	require.ErrorContains(t, <-future.errCh, "commit timeout")

	reqVotePayload, err := json.Marshal(VoteRequest{Term: 12, CandidateID: "cand", LastLogIndex: 1, LastLogTerm: 12})
	require.NoError(t, err)
	respCh := make(chan []byte, 1)
	r.handleRPC(&rpcRequest{message: &RaftRPCMessage{Type: RPCVoteRequest, Payload: reqVotePayload}, respCh: respCh})
	require.NotEmpty(t, <-respCh)

	reqAppendPayload, err := json.Marshal(AppendEntriesRequest{Term: 12, LeaderID: "leader", LeaderAddr: "addr", PrevLogIndex: 1, PrevLogTerm: 12})
	require.NoError(t, err)
	respCh = make(chan []byte, 1)
	r.handleRPC(&rpcRequest{message: &RaftRPCMessage{Type: RPCAppendEntries, Payload: reqAppendPayload}, respCh: respCh})
	require.NotEmpty(t, <-respCh)

	r.handleIncomingConnection(context.Background(), &raftCoveragePeerConn{connected: true})
	select {
	case <-r.heartbeatCh:
	default:
		t.Fatalf("expected heartbeat signal")
	}
}

func TestHAStandbyReplicator_StartAndAckPaths(t *testing.T) {
	t.Parallel()

	primaryCfg := DefaultConfig()
	primaryCfg.Mode = ModeHAStandby
	primaryCfg.NodeID = "ha-primary"
	primaryCfg.BindAddr = "127.0.0.1:17120"
	primaryCfg.AdvertiseAddr = "127.0.0.1:17120"
	primaryCfg.HAStandby.Role = "primary"
	primaryCfg.HAStandby.PeerAddr = "127.0.0.1:17121"
	primaryCfg.HAStandby.SyncMode = SyncQuorum

	primaryStore := NewMockStorage()
	primary, err := NewHAStandbyReplicator(primaryCfg, primaryStore)
	require.NoError(t, err)
	primary.SetTransport(NewMockTransport())
	require.NoError(t, primary.Start(context.Background()))
	require.NoError(t, primary.Start(context.Background()))
	t.Cleanup(func() { _ = primary.Shutdown() })

	primary.walStreamer.AcknowledgePosition(5)
	require.NoError(t, primary.waitForReplicationAck(5, 20*time.Millisecond))

	err = primary.waitForReplicationAck(10, 20*time.Millisecond)
	require.ErrorContains(t, err, "replication ack timeout")

	standbyCfg := DefaultConfig()
	standbyCfg.Mode = ModeHAStandby
	standbyCfg.NodeID = "ha-standby"
	standbyCfg.BindAddr = "127.0.0.1:17122"
	standbyCfg.AdvertiseAddr = "127.0.0.1:17122"
	standbyCfg.HAStandby.Role = "standby"
	standbyCfg.HAStandby.PeerAddr = "127.0.0.1:17120"

	standbyStore := NewMockStorage()
	standby, err := NewHAStandbyReplicator(standbyCfg, standbyStore)
	require.NoError(t, err)
	transport := NewMockTransport()
	standby.SetTransport(transport)
	require.NoError(t, standby.Start(context.Background()))
	t.Cleanup(func() { _ = standby.Shutdown() })

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		transport.mu.RLock()
		hasHandler := transport.handler != nil
		transport.mu.RUnlock()
		if hasHandler {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	transport.SimulateIncomingConnection()
	time.Sleep(25 * time.Millisecond)
	assert.True(t, standby.primaryHealthy.Load())
}

func TestRaftReplicator_ApplyBranchCoverage(t *testing.T) {
	t.Parallel()

	r, _ := newCoverageRaftReplicator(t)
	cmd := &Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}

	r.work.CloseAdmission()
	require.ErrorIs(t, r.Apply(cmd, 10*time.Millisecond), ErrClosed)

	// Owner admission never reopens: a fresh replicator is the not-started case.
	r2, _ := newCoverageRaftReplicator(t)
	require.ErrorIs(t, r2.Apply(cmd, 10*time.Millisecond), ErrNotReady)

	r.started.Store(true)
	r.state = StateFollower
	r.leaderAddr = ""
	require.ErrorIs(t, r.Apply(cmd, 10*time.Millisecond), ErrNoLeader)

	r.leaderAddr = "127.0.0.1:17130"
	r.transport = nil
	require.ErrorIs(t, r.Apply(cmd, 10*time.Millisecond), ErrNotLeader)

	r.transport = &raftCoverageTransport{connectErr: errors.New("dial failed")}
	err := r.Apply(cmd, 10*time.Millisecond)
	require.ErrorContains(t, err, "forward to leader")

	fwdConn := &raftForwardPeerConn{raftCoveragePeerConn: raftCoveragePeerConn{connected: true}}
	r.transport = &raftCoverageTransport{conn: fwdConn}
	require.NoError(t, r.Apply(cmd, 10*time.Millisecond))

	fwdConn.forwardErr = errors.New("leader rejected")
	err = r.Apply(cmd, 10*time.Millisecond)
	require.ErrorContains(t, err, "leader rejected")

	r.state = StateLeader
	r.applyCh = make(chan *applyFuture)
	err = r.Apply(cmd, 10*time.Millisecond)
	require.ErrorContains(t, err, "apply queue full")
}

func TestRaftReplicator_HandleTransportWrappers(t *testing.T) {
	t.Parallel()

	r, _ := newCoverageRaftReplicator(t)
	r.currentTerm = 1
	r.log = []*RaftLogEntry{{Index: 0, Term: 0}}

	voteResp, err := r.HandleRaftVote(&RaftVoteRequest{
		Term:         2,
		CandidateID:  "cand",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	require.NoError(t, err)
	assert.True(t, voteResp.VoteGranted)

	appendResp, err := r.HandleRaftAppendEntries(&RaftAppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader",
		LeaderAddr:   "127.0.0.1:17140",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	require.NoError(t, err)
	assert.True(t, appendResp.Success)
}

func TestStorageAdapter_TypedMixedTransactionReplacesLegacyBatch(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := adapter.engine.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: cmdRetiredBatchWrite, Data: []byte("bad")}), ErrRetiredCommand)
	ops := []storage.Operation{{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:n-batch", Labels: []string{"L"}}}, {Type: storage.OpCreateEdge, Edge: &storage.Edge{ID: "test:e-batch", StartNode: "test:a", EndNode: "test:b", Type: "REL"}}}
	payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "test", LogicalID: "mixed-replacement", Operations: ops})
	require.NoError(t, err)
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(adapter.engine, payload))
	_, err = adapter.engine.GetNode("n-batch")
	require.NoError(t, err)
	_, err = adapter.engine.GetEdge("e-batch")
	require.NoError(t, err)
}

func TestNewDefaultTransportFromConfig_Branches(t *testing.T) {
	t.Parallel()

	tr, err := NewDefaultTransportFromConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, tr)

	cfg := DefaultConfig()
	cfg.NodeID = "node-tls"
	cfg.BindAddr = "127.0.0.1:17150"
	cfg.ReplicationSecret = "secret"
	tr, err = NewDefaultTransportFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, tr)

	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "missing-cert.pem"
	cfg.TLS.KeyFile = "missing-key.pem"
	_, err = NewDefaultTransportFromConfig(cfg)
	require.ErrorContains(t, err, "load TLS cert/key")
}

func TestHAStandbyReplicator_Start_DefaultTransportInitFailure(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "ha-fail"
	cfg.HAStandby.Role = "primary"
	cfg.HAStandby.PeerAddr = "127.0.0.1:17190"

	store := NewMockStorage()
	r, err := NewHAStandbyReplicator(cfg, store)
	require.NoError(t, err)
	r.transport = nil
	r.config.TLS.Enabled = true
	r.config.TLS.CertFile = "missing-cert.pem"
	r.config.TLS.KeyFile = "missing-key.pem"

	err = r.Start(context.Background())
	require.ErrorContains(t, err, "init transport")
	assert.False(t, r.started.Load(), "failed native construction must not publish readiness")
}

func TestHAStandbyReplicator_Start_DefaultTransportSuccessPaths(t *testing.T) {
	t.Parallel()

	t.Run("primary with default transport", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Mode = ModeHAStandby
		cfg.NodeID = "ha-default-primary"
		cfg.BindAddr = "127.0.0.1:0"
		cfg.AdvertiseAddr = "127.0.0.1:0"
		cfg.HAStandby.Role = "primary"
		cfg.HAStandby.PeerAddr = "127.0.0.1:65534"
		cfg.HAStandby.SyncMode = SyncAsync

		r, err := NewHAStandbyReplicator(cfg, NewMockStorage())
		require.NoError(t, err)
		err = r.Start(context.Background())
		require.NoError(t, err)
		require.NoError(t, r.Shutdown())
	})

	t.Run("standby with default transport", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Mode = ModeHAStandby
		cfg.NodeID = "ha-default-standby"
		cfg.BindAddr = "127.0.0.1:0"
		cfg.AdvertiseAddr = "127.0.0.1:0"
		cfg.HAStandby.Role = "standby"
		cfg.HAStandby.PeerAddr = "127.0.0.1:65534"
		cfg.HAStandby.SyncMode = SyncAsync

		r, err := NewHAStandbyReplicator(cfg, NewMockStorage())
		require.NoError(t, err)
		err = r.Start(context.Background())
		require.NoError(t, err)
		require.NoError(t, r.Shutdown())
	})
}

func TestStorageAdapter_RestoreSnapshot_ErrorPaths(t *testing.T) {
	t.Parallel()

	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })

	badReader := io.NopCloser(io.Reader(&errReader{err: errors.New("read boom")}))
	err := adapter.RestoreSnapshot(badReader)
	require.ErrorContains(t, err, "read snapshot")

	err = adapter.RestoreSnapshot(io.NopCloser(bytes.NewBufferString("bad-snapshot")))
	require.ErrorContains(t, err, "decode snapshot")

	node := &storage.Node{ID: "n1", Labels: []string{"L"}}
	snapshot := storage.ReplicationSnapshot{
		WALPosition: 3,
		Nodes:       []*storage.Node{node, node},
	}
	payload, encErr := storage.EncodeCommandPayload(snapshot)
	require.NoError(t, encErr)
	err = adapter.RestoreSnapshot(io.NopCloser(bytes.NewReader(payload)))
	require.ErrorContains(t, err, "restore node")
}

func TestRaftReplicator_WaitForCommitCancelAndStop(t *testing.T) {
	t.Parallel()

	r, _ := newCoverageRaftReplicator(t)
	r.config.Raft.CommitTimeout = 100 * time.Millisecond

	f1 := &applyFuture{index: 1, errCh: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.waitForCommit(ctx, f1)
	require.ErrorIs(t, <-f1.errCh, context.Canceled)

	f2 := &applyFuture{index: 1, errCh: make(chan error, 1)}
	r.work.Cancel()
	r.waitForCommit(context.Background(), f2)
	require.ErrorIs(t, <-f2.errCh, ErrClosed)
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func TestStorageAdapter_ApplyCommand_SyncWALFallback(t *testing.T) {
	t.Parallel()
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			adapter := setupTestAdapter(t).adapter
			// Stop the actual writer before replacing its queue, preserving the race-safe
			// rendezvous that forces the non-blocking enqueue's synchronous fallback.
			adapter.stopWALWriterForTest()
			adapter.walQueue = make(chan *walWriteRequest)
			id := storage.NodeID("fallback-n1")
			if closed {
				require.NoError(t, adapter.wal.Close())
				id = "fallback-fail"
			}
			command := &Command{Type: CmdCreateNode, Data: runtimeFixtureOwner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: id, Labels: []string{"L"}}), Timestamp: time.Now()}
			err := adapter.ApplyCommand(command)
			if closed && err != nil {
				require.ErrorContains(t, err, "failed to append to WAL")
				return
			}
			require.NoError(t, err)
			_, err = adapter.engine.GetNode(id)
			require.NoError(t, err)
		})
	}
}

func TestClusterTransport_SendMethods_ErrorMatrix(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	server := NewClusterTransport(&ClusterTransportConfig{NodeID: "server-errors", BindAddr: "127.0.0.1:0"})
	for request, response := range map[ClusterMessageType]ClusterMessageType{ClusterMsgWALBatch: ClusterMsgWALBatchResponse, ClusterMsgHeartbeat: ClusterMsgHeartbeatResponse, ClusterMsgFence: ClusterMsgFenceResponse, ClusterMsgPromote: ClusterMsgPromoteResponse, ClusterMsgVoteRequest: ClusterMsgVoteResponse, ClusterMsgAppendEntries: ClusterMsgAppendEntriesResponse} {
		server.RegisterHandler(request, func(context.Context, string, *ClusterMessage) (*ClusterMessage, error) {
			return &ClusterMessage{Type: response, Payload: []byte("bad")}, nil
		})
	}
	server.RegisterHandler(ClusterMsgForwardApply, func(context.Context, string, *ClusterMessage) (*ClusterMessage, error) {
		return &ClusterMessage{Type: ClusterMsgForwardApplyResponse, Payload: runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, forwardApplyResponse{Err: "leader apply failed"})}, nil
	})
	listenDone := make(chan struct{})
	go func() { defer close(listenDone); _ = server.Listen(ctx, server.bindAddr, nil) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case <-listenDone:
		case <-time.After(5 * time.Second):
			t.Error("server listen did not join")
		}
	})
	var address string
	require.Eventually(t, func() bool {
		server.resources.mu.RLock()
		defer server.resources.mu.RUnlock()
		address = server.bindAddr
		return server.resources.listener != nil
	}, 2*time.Second, 10*time.Millisecond)
	client := NewClusterTransport(&ClusterTransportConfig{NodeID: "client-errors"})
	t.Cleanup(func() { _ = client.Close() })
	conn, err := client.Connect(ctx, address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.True(t, waitForConnected(conn, 2*time.Second))
	cc := conn.(*ClusterConnection)
	cases := []struct {
		name        string
		send        func(context.Context) error
		decodeError string
	}{
		{"WALBatch", func(c context.Context) error { _, e := cc.SendWALBatch(c, []*WALEntry{{Position: 1}}); return e }, "decode"},
		{"Heartbeat", func(c context.Context) error { _, e := cc.SendHeartbeat(c, &HeartbeatRequest{NodeID: "n1"}); return e }, "decode"},
		{"Fence", func(c context.Context) error { _, e := cc.SendFence(c, &FenceRequest{Reason: "r"}); return e }, "decode"},
		{"Promote", func(c context.Context) error { _, e := cc.SendPromote(c, &PromoteRequest{Reason: "r"}); return e }, "decode"},
		{"RaftVote", func(c context.Context) error { _, e := cc.SendRaftVote(c, &RaftVoteRequest{Term: 1}); return e }, "decode"},
		{"RaftAppendEntries", func(c context.Context) error {
			_, e := cc.SendRaftAppendEntries(c, &RaftAppendEntriesRequest{Term: 1})
			return e
		}, "decode"},
		{"ForwardApply", func(c context.Context) error {
			return cc.SendForwardApply(c, &Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}, 100*time.Millisecond)
		}, "leader apply failed"},
	}
	for _, canceled := range []bool{false, true} {
		callCtx := ctx
		if canceled {
			var stop context.CancelFunc
			callCtx, stop = context.WithCancel(ctx)
			stop()
		}
		for _, test := range cases {
			t.Run(fmt.Sprintf("%s/canceled=%v", test.name, canceled), func(t *testing.T) {
				err := test.send(callCtx)
				if canceled {
					require.Error(t, err)
				} else {
					require.ErrorContains(t, err, test.decodeError)
				}
			})
		}
	}
}

func TestHAStandby_HelperBranches(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "ha-helper"
	cfg.AdvertiseAddr = "127.0.0.1:17210"
	cfg.HAStandby.Role = "standby"
	cfg.HAStandby.PeerAddr = "127.0.0.1:17211"
	cfg.HAStandby.SyncMode = SyncQuorum

	store := NewMockStorage()
	r, err := NewHAStandbyReplicator(cfg, store)
	require.NoError(t, err)
	r.transport = NewMockTransport()

	// LeaderAddr branch for standby and primary.
	r.isPrimary.Store(false)
	assert.Equal(t, cfg.HAStandby.PeerAddr, r.LeaderAddr())
	r.isPrimary.Store(true)
	assert.Equal(t, cfg.AdvertiseAddr, r.LeaderAddr())

	// Default-timeout branch (timeout<=0) with quorum mode and no ack.
	r.walStreamer = NewWALStreamer(store, 8)
	err = r.waitForReplicationAck(1, 0)
	require.ErrorContains(t, err, "replication ack timeout")

	// triggerAutoFailover failure path: Promote fails because WAL flush fails.
	r.isPrimary.Store(false)
	r.isPromoted.Store(false)
	store.SetApplyError(errors.New("flush failed"))
	r.walApplier.pendingBatch = []*WALEntry{
		{Position: 1, Command: &Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}},
	}
	r.triggerAutoFailover(context.Background())
	assert.False(t, r.isPromoted.Load())

	assert.Equal(t, 5*time.Millisecond, min(5*time.Millisecond, 10*time.Millisecond))
	assert.Equal(t, 10*time.Millisecond, min(20*time.Millisecond, 10*time.Millisecond))
}

func TestHAStandbyReplicator_Apply_TraceWritesPath(t *testing.T) {
	t.Setenv("NORNICDB_CLUSTER_TRACE_WRITES", "1")

	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "ha-trace"
	cfg.AdvertiseAddr = "127.0.0.1:17220"
	cfg.HAStandby.Role = "primary"
	cfg.HAStandby.PeerAddr = "127.0.0.1:17221"
	cfg.HAStandby.SyncMode = SyncAsync

	store := NewMockStorage()
	r, err := NewHAStandbyReplicator(cfg, store)
	require.NoError(t, err)
	r.started.Store(true)
	r.isPrimary.Store(true)

	err = r.Apply(&Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}, 100*time.Millisecond)
	require.NoError(t, err)
}

func TestCodec_DecodePayloadErrorPaths(t *testing.T) {
	t.Parallel()

	_, err := storage.DecodeNodePayload([]byte("bad-node-payload"))
	require.Error(t, err)

	_, err = storage.DecodeEdgePayload([]byte("bad-edge-payload"))
	require.Error(t, err)
}

func TestStorageAdapter_ApplyHelpers_ErrorAndFallbackBranches(t *testing.T) {
	t.Parallel()

	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })

	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdCreateNode, Data: []byte("bad")}), "decode node")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdUpdateNode, Data: []byte("bad")}), "decode node")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdCreateEdge, Data: []byte("bad")}), "decode edge")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdUpdateEdge, Data: []byte("bad")}), "decode edge")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdDeleteEdge, Data: []byte("bad")}), "decode delete edge request")
	require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: cmdRetiredSetProperty, Data: []byte("bad")}), ErrRetiredCommand)
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdDeleteByPrefix, Data: []byte("bad")}), "decode delete by prefix request")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdBulkCreateNodes, Data: []byte("bad")}), "decode bulk create nodes")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdBulkCreateEdges, Data: []byte("bad")}), "decode bulk create edges")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdBulkDeleteNodes, Data: []byte("bad")}), "decode bulk delete nodes")
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdBulkDeleteEdges, Data: []byte("bad")}), "decode bulk delete edges")

	_, err := adapter.engine.CreateNode(&storage.Node{ID: "raw-fallback-node", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, adapter.ApplyCommand(&Command{Type: CmdDeleteNode, Data: []byte("raw-fallback-node")}))
	_, err = adapter.engine.GetNode("raw-fallback-node")
	require.ErrorIs(t, err, storage.ErrNotFound)

	emptyPrefixPayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, struct{ Prefix string }{Prefix: ""})
	require.ErrorContains(t, adapter.ApplyCommand(&Command{Type: CmdDeleteByPrefix, Data: emptyPrefixPayload}), "prefix is required")

	badNodePayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, [][]byte{[]byte("bad-node")})
	require.Error(t, adapter.ApplyCommand(&Command{Type: CmdBulkCreateNodes, Data: badNodePayload}))

	badEdgePayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, [][]byte{[]byte("bad-edge")})
	require.Error(t, adapter.ApplyCommand(&Command{Type: CmdBulkCreateEdges, Data: badEdgePayload}))

	// Edge create should fail when endpoints don't exist.
	validEdgeBytes := runtimeFixtureOwner.Encode(t, storage.EncodeEdgePayload, &storage.Edge{ID: "edge-missing-nodes", StartNode: "no-a", EndNode: "no-b", Type: "REL"})
	edgeOnlyBatch := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, struct {
		Nodes [][]byte
		Edges [][]byte
	}{
		Edges: [][]byte{validEdgeBytes},
	})
	require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: cmdRetiredBatchWrite, Data: edgeOnlyBatch}), ErrRetiredCommand)

	emptyCypherPayload := runtimeFixtureOwner.Encode[any](t, storage.EncodeCommandPayload, struct {
		Query  string
		Params map[string]interface{}
	}{})
	require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: cmdRetiredCypher, Data: emptyCypherPayload}), ErrRetiredCommand)
}

func TestStorageAdapter_GetWALEntries_ReadErrorPath(t *testing.T) {
	t.Parallel()

	adapter := setupTestAdapter(t).adapter
	t.Cleanup(func() { _ = adapter.Close() })

	// Force persistent WAL read failure branch (path is not a directory).
	adapter.walDir = filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(adapter.walDir, []byte("file"), 0o600))
	_, err := adapter.GetWALEntries(0, 10)
	require.ErrorContains(t, err, "failed to read WAL entries")
}

func TestStorageAdapter_WriteSnapshot_Failures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		encodeFailure bool
		want          string
	}{
		{name: "writer", want: "write failed"},
		{name: "codec", encodeFailure: true, want: "encode snapshot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := setupTestAdapter(t).adapter
			var writer io.Writer = &errWriter{err: errors.New("write failed")}
			if test.encodeFailure {
				adapter.engine = &snapshotEncodeErrorEngine{}
				writer = &bytes.Buffer{}
			}
			err := adapter.WriteSnapshot(writer)
			require.ErrorContains(t, err, test.want)
		})
	}
}

type errWriter struct{ err error }

func (w *errWriter) Write([]byte) (int, error) { return 0, w.err }

type snapshotEncodeErrorEngine struct {
	storage.Engine
}

func (e *snapshotEncodeErrorEngine) AllNodes() ([]*storage.Node, error) {
	return []*storage.Node{
		{
			ID:     "bad-node",
			Labels: []string{"L"},
			Properties: map[string]any{
				"bad": make(chan int),
			},
		},
	}, nil
}

func (e *snapshotEncodeErrorEngine) AllEdges() ([]*storage.Edge, error) {
	return nil, nil
}

// The snapshot streams stored nodes: the fake serves AllNodes' node.
func (e *snapshotEncodeErrorEngine) StreamNodesWithOptions(_ context.Context, _ storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	nodes, _ := e.AllNodes()
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func TestTransport_WireReadWriteHelpers(t *testing.T) {
	t.Parallel()

	msg := &ClusterMessage{Type: ClusterMsgHeartbeat, Payload: []byte("ok")}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	require.NoError(t, transferClusterMessage(msg, nil, bw, 0))
	require.NoError(t, bw.Flush())

	readMsg := new(ClusterMessage)
	err := transferClusterMessage(readMsg, bufio.NewReader(bytes.NewReader(out.Bytes())), nil, 1024)
	require.NoError(t, err)
	require.Equal(t, ClusterMsgHeartbeat, readMsg.Type)
	require.Equal(t, []byte("ok"), readMsg.Payload)

	errBuf := bufio.NewWriter(&errWriter{err: errors.New("write boom")})
	require.NoError(t, transferClusterMessage(msg, nil, errBuf, 0))
	require.ErrorContains(t, errBuf.Flush(), "write boom")

	err = transferClusterMessage(new(ClusterMessage), bufio.NewReader(bytes.NewReader(nil)), nil, 1024)
	require.Error(t, err)

	var tooLarge bytes.Buffer
	require.NoError(t, binary.Write(&tooLarge, binary.BigEndian, uint32(4096)))
	err = transferClusterMessage(new(ClusterMessage), bufio.NewReader(bytes.NewReader(tooLarge.Bytes())), nil, 64)
	require.ErrorContains(t, err, "message too large")

	var truncated bytes.Buffer
	require.NoError(t, binary.Write(&truncated, binary.BigEndian, uint32(8)))
	truncated.Write([]byte{1, 2, 3})
	err = transferClusterMessage(new(ClusterMessage), bufio.NewReader(bytes.NewReader(truncated.Bytes())), nil, 1024)
	require.Error(t, err)

	var badPayload bytes.Buffer
	payload := []byte("not-gob")
	require.NoError(t, binary.Write(&badPayload, binary.BigEndian, uint32(len(payload))))
	badPayload.Write(payload)
	err = transferClusterMessage(new(ClusterMessage), bufio.NewReader(bytes.NewReader(badPayload.Bytes())), nil, 1024)
	require.Error(t, err)

	cc := &ClusterConnection{}
	require.NoError(t, cc.Close())
}

func TestRaftReplicator_ProcessApply_AndPeerMaintenance(t *testing.T) {
	t.Parallel()

	r, store := newCoverageRaftReplicator(t)

	// Non-leader apply path.
	r.state = StateFollower
	f1 := &applyFuture{cmd: &Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}, errCh: make(chan error, 1)}
	r.processApply(context.Background(), f1)
	require.ErrorIs(t, <-f1.errCh, ErrNotLeader)

	// Leader single-node immediate commit/apply path.
	r.state = StateLeader
	r.currentTerm = 2
	r.config.Raft.Peers = nil
	f2 := &applyFuture{cmd: &Command{Type: CmdCreateNode, Data: []byte("y"), Timestamp: time.Now()}, errCh: make(chan error, 1)}
	r.processApply(context.Background(), f2)
	require.NoError(t, <-f2.errCh)
	assert.GreaterOrEqual(t, r.commitIndex, uint64(1))
	assert.GreaterOrEqual(t, r.lastApplied, uint64(1))
	assert.Equal(t, 1, store.GetApplyCount())

	// Peer maintenance path: reconnect disconnected peer.
	r.config.Raft.Peers = []PeerConfig{{ID: "p1", Addr: "127.0.0.1:17300"}}
	r.peerConns["p1"] = &raftCoveragePeerConn{connected: false}
	r.transport = &raftCoverageTransport{conn: &raftCoveragePeerConn{connected: true}}
	r.maintainPeerConnections(context.Background())

	conn := r.peerConns["p1"]
	require.NotNil(t, conn)
	assert.True(t, conn.IsConnected())
}

func TestRaftReplicator_HandleAppendEntriesRequest_Branches(t *testing.T) {
	t.Parallel()

	r, _ := newCoverageRaftReplicator(t)
	r.currentTerm = 5
	r.log = []*RaftLogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("a"), Timestamp: time.Now()}},
		{Index: 2, Term: 2, Command: &Command{Type: CmdUpdateNode, Data: []byte("b"), Timestamp: time.Now()}},
	}

	// Stale term is rejected.
	resp := r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         4,
		LeaderID:     "l1",
		LeaderAddr:   "addr",
		PrevLogIndex: 2,
		PrevLogTerm:  2,
	})
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)

	// Conflict index when prev log index is missing.
	resp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         6,
		LeaderID:     "l1",
		LeaderAddr:   "addr",
		PrevLogIndex: 99,
		PrevLogTerm:  2,
	})
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(len(r.log)), resp.ConflictIndex)

	// Conflict term/index when prev term mismatches.
	resp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         6,
		LeaderID:     "l1",
		LeaderAddr:   "addr",
		PrevLogIndex: 2,
		PrevLogTerm:  99,
	})
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(2), resp.ConflictTerm)
	assert.GreaterOrEqual(t, resp.ConflictIndex, uint64(1))

	// Overwrite conflicting entry and commit with leaderCommit < lastNewIndex.
	resp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         7,
		LeaderID:     "l1",
		LeaderAddr:   "addr",
		PrevLogIndex: 1,
		PrevLogTerm:  2,
		Entries: []*RaftLogEntry{
			{Index: 2, Term: 7, Command: &Command{Type: CmdDeleteNode, Data: []byte("c"), Timestamp: time.Now()}},
			{Index: 3, Term: 7, Command: &Command{Type: CmdCreateEdge, Data: []byte("d"), Timestamp: time.Now()}},
		},
		LeaderCommit: 2,
	})
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(3), resp.MatchIndex)
	require.GreaterOrEqual(t, len(r.log), 4)
	assert.Equal(t, uint64(7), r.log[2].Term)
	assert.Equal(t, uint64(2), r.commitIndex)

	// Commit with leaderCommit > lastNewIndex should clamp to lastNewIndex.
	resp = r.handleAppendEntriesRequest(&AppendEntriesRequest{
		Term:         7,
		LeaderID:     "l1",
		LeaderAddr:   "addr",
		PrevLogIndex: 3,
		PrevLogTerm:  7,
		Entries: []*RaftLogEntry{
			{Index: 4, Term: 7, Command: &Command{Type: CmdUpdateEdge, Data: []byte("e"), Timestamp: time.Now()}},
		},
		LeaderCommit: 100,
	})
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(4), resp.MatchIndex)
	assert.Equal(t, uint64(4), r.commitIndex)
}

func TestRaftReplicator_ListenForPeers(t *testing.T) {
	t.Parallel()

	r, _ := newCoverageRaftReplicator(t)
	mt := NewMockTransport()
	r.transport = mt
	r.config.AdvertiseAddr = "127.0.0.1:17310"

	ctx, cancel := context.WithCancel(context.Background())
	listenDone := make(chan struct{})
	go func() { defer close(listenDone); r.listenForPeers(ctx) }()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mt.mu.RLock()
		active := mt.listenAddr != ""
		mt.mu.RUnlock()
		if active {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mt.SimulateIncomingConnection()
	cancel()
	_ = mt.Close()
	<-listenDone
}

func TestApplyBatchWrappers_SuccessAndErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("standalone apply batch returns apply error", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Mode = ModeStandalone
		cfg.NodeID = "solo-batch"
		store := NewMockStorage()
		store.SetApplyError(errors.New("apply fail"))
		r := NewStandaloneReplicator(cfg, store)
		require.NoError(t, r.Start(context.Background()))
		err := r.ApplyBatch([]*Command{{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}}, time.Second)
		require.ErrorContains(t, err, "apply fail")
	})

	t.Run("ha apply batch success", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Mode = ModeHAStandby
		cfg.NodeID = "ha-batch"
		cfg.HAStandby.Role = "primary"
		cfg.HAStandby.PeerAddr = "peer:7000"
		cfg.HAStandby.SyncMode = SyncAsync
		store := NewMockStorage()
		r, err := NewHAStandbyReplicator(cfg, store)
		require.NoError(t, err)
		r.started.Store(true)
		r.isPrimary.Store(true)
		err = r.ApplyBatch([]*Command{
			{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()},
			{Type: CmdUpdateNode, Data: []byte("y"), Timestamp: time.Now()},
		}, 100*time.Millisecond)
		require.NoError(t, err)
	})

	t.Run("raft apply batch success", func(t *testing.T) {
		r, _ := newCoverageRaftReplicator(t)
		r.started.Store(true)
		r.state = StateLeader
		r.config.Raft.Peers = nil

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		applyDone := make(chan struct{})
		go func() { defer close(applyDone); r.runApplyLoop(ctx) }()
		defer func() {
			cancel()
			<-applyDone
		}()

		err := r.ApplyBatch([]*Command{
			{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()},
			{Type: CmdUpdateNode, Data: []byte("y"), Timestamp: time.Now()},
		}, time.Second)
		require.NoError(t, err)
	})

	t.Run("multi-region apply batch success", func(t *testing.T) {
		local, _ := newCoverageRaftReplicator(t)
		local.started.Store(true)
		local.state = StateLeader
		local.config.Raft.Peers = nil

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		applyDone := make(chan struct{})
		go func() { defer close(applyDone); local.runApplyLoop(ctx) }()
		defer func() {
			cancel()
			<-applyDone
		}()

		cfg := DefaultConfig()
		cfg.Mode = ModeMultiRegion
		cfg.NodeID = "mr-batch"
		cfg.MultiRegion.RegionID = "us-east"
		r := &MultiRegionReplicator{
			config:      cfg,
			storage:     NewMockStorage(),
			localRaft:   local,
			remoteConns: make(map[string]PeerConnection),
		}
		r.started.Store(true)

		err := r.ApplyBatch([]*Command{
			{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()},
			{Type: CmdUpdateNode, Data: []byte("y"), Timestamp: time.Now()},
		}, time.Second)
		require.NoError(t, err)
	})
}
