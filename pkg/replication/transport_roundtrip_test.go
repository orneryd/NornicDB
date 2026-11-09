package replication

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterTransport_HeartbeatRoundTrip(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewClusterTransport(&ClusterTransportConfig{
		AuthSecret: []byte(replayTestSecret),
		NodeID:     "server-1",
		BindAddr:   "127.0.0.1:0",
	})

	defer server.Close()

	var handled atomic.Bool
	server.RegisterHandler(ClusterMsgHeartbeat, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		handled.Store(true)
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: msg.Payload}, nil
	})

	go func() {
		_ = server.Listen(ctx, server.bindAddr, nil)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var boundAddr string
	for time.Now().Before(deadline) {
		server.resources.mu.RLock()
		ln := server.resources.listener
		boundAddr = server.bindAddr
		server.resources.mu.RUnlock()
		if ln != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	server.resources.mu.RLock()
	ln := server.resources.listener
	boundAddr = server.bindAddr
	server.resources.mu.RUnlock()
	if ln == nil {
		t.Fatalf("server did not start listening")
	}

	client := NewClusterTransport(&ClusterTransportConfig{NodeID: "client-1", AuthSecret: []byte(replayTestSecret)})
	defer client.Close()
	conn, err := client.Connect(ctx, boundAddr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	if !waitForConnected(conn, 2*time.Second) {
		t.Fatalf("client connection did not become connected")
	}

	// Ensure RPC path works end-to-end (request routed to handler and response read back).
	_, err = conn.SendHeartbeat(ctx, &HeartbeatRequest{
		NodeID:      "client-1",
		Role:        "test",
		WALPosition: 123,
		Timestamp:   time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}
	if !handled.Load() {
		t.Fatalf("expected heartbeat handler to run")
	}
}

func waitForConnected(conn PeerConnection, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if conn.IsConnected() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return conn.IsConnected()
}

func TestClusterTransport_AllRPCSendersRoundTrip(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewClusterTransport(&ClusterTransportConfig{
		AuthSecret: []byte(replayTestSecret),
		NodeID:     "server-2",
		BindAddr:   "127.0.0.1:0",
	})

	defer server.Close()

	server.RegisterHandler(ClusterMsgWALBatch, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in []*WALEntry
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(&WALBatchResponse{AckedPosition: 11, ReceivedPosition: 11})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgWALBatchResponse, Payload: out}, nil
	})
	server.RegisterHandler(ClusterMsgFence, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in FenceRequest
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(&FenceResponse{Fenced: true})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgFenceResponse, Payload: out}, nil
	})
	server.RegisterHandler(ClusterMsgPromote, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in PromoteRequest
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(&PromoteResponse{Ready: true})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgPromoteResponse, Payload: out}, nil
	})
	server.RegisterHandler(ClusterMsgVoteRequest, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in RaftVoteRequest
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(&RaftVoteResponse{Term: in.Term, VoteGranted: true, VoterID: "server-2"})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgVoteResponse, Payload: out}, nil
	})
	server.RegisterHandler(ClusterMsgAppendEntries, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in RaftAppendEntriesRequest
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(&RaftAppendEntriesResponse{Term: in.Term, Success: true, MatchIndex: in.PrevLogIndex})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgAppendEntriesResponse, Payload: out}, nil
	})
	server.RegisterHandler(ClusterMsgForwardApply, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		var in Command
		require.NoError(t, storage.DecodeCommandPayload(msg.Payload, &in))
		out, err := storage.EncodeCommandPayload(forwardApplyResponse{})
		require.NoError(t, err)
		return &ClusterMessage{Type: ClusterMsgForwardApplyResponse, Payload: out}, nil
	})

	go func() {
		_ = server.Listen(ctx, server.bindAddr, nil)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var boundAddr string
	for time.Now().Before(deadline) {
		server.resources.mu.RLock()
		ln := server.resources.listener
		boundAddr = server.bindAddr
		server.resources.mu.RUnlock()
		if ln != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	client := NewClusterTransport(&ClusterTransportConfig{NodeID: "client-2", AuthSecret: []byte(replayTestSecret)})
	defer client.Close()
	conn, err := client.Connect(ctx, boundAddr)
	require.NoError(t, err)
	defer conn.Close()
	require.True(t, waitForConnected(conn, 2*time.Second))

	cc := conn.(*ClusterConnection)

	walResp, err := cc.SendWALBatch(ctx, []*WALEntry{{Position: 1}})
	require.NoError(t, err)
	require.Equal(t, uint64(11), walResp.AckedPosition)

	fenceResp, err := cc.SendFence(ctx, &FenceRequest{Reason: "test"})
	require.NoError(t, err)
	require.True(t, fenceResp.Fenced)

	promoteResp, err := cc.SendPromote(ctx, &PromoteRequest{Reason: "test"})
	require.NoError(t, err)
	require.True(t, promoteResp.Ready)

	voteResp, err := cc.SendRaftVote(ctx, &RaftVoteRequest{Term: 9, CandidateID: "cand"})
	require.NoError(t, err)
	require.True(t, voteResp.VoteGranted)

	appendResp, err := cc.SendRaftAppendEntries(ctx, &RaftAppendEntriesRequest{Term: 9, PrevLogIndex: 5})
	require.NoError(t, err)
	require.True(t, appendResp.Success)

	require.NoError(t, cc.SendForwardApply(ctx, &Command{Type: CmdCreateNode, Data: []byte("x"), Timestamp: time.Now()}, 100*time.Millisecond))
	require.Error(t, cc.SendForwardApply(ctx, nil, 100*time.Millisecond))
}
