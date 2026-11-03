package replication

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Done observes the public sender reaching its wait. Before the correction this
// happened after writing the second request; the single-flight owner waits
// before admitting it to the wire. No sleeps or scheduler assumptions are used.
type rpcObservedWait struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *rpcObservedWait) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func gatedRPCPeer(t *testing.T, replyToWAL bool) (PeerConnection, <-chan struct{}, func(), *atomic.Int32) {
	t.Helper()
	server := NewClusterTransport(&ClusterTransportConfig{BindAddr: "127.0.0.1:0"})
	gate := make(chan struct{})
	arrived := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var walCalls atomic.Int32
	server.RegisterHandler(ClusterMsgHeartbeat, func(ctx context.Context, _ string, msg *ClusterMessage) (*ClusterMessage, error) {
		var request HeartbeatRequest
		if err := storage.DecodeCommandPayload(msg.Payload, &request); err != nil {
			return nil, err
		}
		if request.WALPosition == 41 {
			close(arrived)
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		payload, err := storage.EncodeCommandPayload(&HeartbeatResponse{WALPosition: request.WALPosition})
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: payload}, err
	})
	server.RegisterHandler(ClusterMsgWALBatch, func(_ context.Context, _ string, msg *ClusterMessage) (*ClusterMessage, error) {
		walCalls.Add(1)
		if replyToWAL {
			var entries []*WALEntry
			if err := storage.DecodeCommandPayload(msg.Payload, &entries); err != nil {
				return nil, err
			}
			position := entries[len(entries)-1].Position
			payload, err := storage.EncodeCommandPayload(&WALBatchResponse{AckedPosition: position, ReceivedPosition: position})
			return &ClusterMessage{Type: ClusterMsgWALBatchResponse, Payload: payload}, err
		}
		// This request's caller already canceled; suppress its reply so the
		// regression isolates whether the request was wrongly sent at all.
		return nil, nil
	})
	listened := make(chan error, 1)
	go func() { listened <- server.Listen(t.Context(), server.bindAddr, nil) }()
	t.Cleanup(func() {
		release()
		require.NoError(t, server.Close())
		awaitReplicationValue(t, listened)
	})
	require.Eventually(t, func() bool {
		server.resources.mu.RLock()
		defer server.resources.mu.RUnlock()
		return server.resources.listener != nil
	}, 5*time.Second, time.Millisecond)
	server.resources.mu.RLock()
	addr := server.bindAddr
	server.resources.mu.RUnlock()
	client := NewClusterTransport(nil)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	peer, err := client.Connect(t.Context(), addr)
	require.NoError(t, err)
	return peer, arrived, release, &walCalls
}

func TestClusterRPCQueuedCancellationDoesNotPublishOrConsumeAnotherReply(t *testing.T) {
	peer, arrived, release, walCalls := gatedRPCPeer(t, false)
	defer release()
	first := make(chan error, 1)
	go func() {
		response, err := peer.SendHeartbeat(t.Context(), &HeartbeatRequest{WALPosition: 41})
		if err == nil && response.WALPosition != 41 {
			err = ErrNotReady
		}
		first <- err
	}()
	awaitReplicationValue(t, arrived)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wait := &rpcObservedWait{Context: ctx, waiting: make(chan struct{})}
	second := make(chan error, 1)
	go func() {
		_, err := peer.SendWALBatch(wait, []*WALEntry{{Position: 99}})
		second <- err
	}()
	awaitReplicationValue(t, wait.waiting)
	cancel()
	require.ErrorIs(t, awaitReplicationValue(t, second), context.Canceled)
	release()
	require.NoError(t, awaitReplicationValue(t, first), "the first response belongs only to its own heartbeat")
	// Server dispatch is ordered. Receiving this sentinel proves that every
	// earlier wire message was handled, including any wrongly published WAL.
	response, err := peer.SendHeartbeat(t.Context(), &HeartbeatRequest{WALPosition: 42})
	require.NoError(t, err)
	require.Equal(t, uint64(42), response.WALPosition)
	require.Zero(t, walCalls.Load(), "a canceled queued RPC must never enter the wire")
	require.True(t, peer.IsConnected(), "canceling an unsent waiter must preserve the active connection")
}

func TestClusterRPCInflightCancellationRetiresUncorrelatedConnection(t *testing.T) {
	peer, arrived, release, _ := gatedRPCPeer(t, false)
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := peer.SendHeartbeat(ctx, &HeartbeatRequest{WALPosition: 41})
		result <- err
	}()
	awaitReplicationValue(t, arrived)
	cancel()
	require.ErrorIs(t, awaitReplicationValue(t, result), context.Canceled)
	require.False(t, peer.IsConnected(),
		"without wire request IDs an abandoned reply must never reach a later request")
	_, err := peer.SendWALBatch(t.Context(), []*WALEntry{{Position: 99}})
	require.Error(t, err)
}

type rpcObservedWrite struct {
	net.Conn
	once    sync.Once
	writing chan struct{}
}

func (c *rpcObservedWrite) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(data)
}

func TestClusterRPCBlockedWriteHonorsCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			local, remote := net.Pipe()
			t.Cleanup(func() { require.NoError(t, remote.Close()) })
			socket := &rpcObservedWrite{Conn: local, writing: make(chan struct{})}
			transport := NewClusterTransport(nil)
			t.Cleanup(func() { require.NoError(t, transport.Close()) })
			peer := transport.createConnection("blocked-write", socket)
			t.Cleanup(func() { require.NoError(t, peer.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			expected := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				expected = context.DeadlineExceeded
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := peer.SendHeartbeat(ctx, &HeartbeatRequest{WALPosition: 17})
				result <- err
			}()
			awaitReplicationValue(t, socket.writing)
			if !deadline {
				cancel()
			}
			require.ErrorIs(t, awaitReplicationValue(t, result), expected)
			require.False(t, peer.IsConnected(), "a partial/abandoned frame cannot be reused")
		})
	}
}

func TestClusterRPCCloseReleasesActiveAndQueuedSenders(t *testing.T) {
	peer, arrived, release, walCalls := gatedRPCPeer(t, false)
	defer release()
	active := make(chan error, 1)
	go func() {
		_, err := peer.SendHeartbeat(t.Context(), &HeartbeatRequest{WALPosition: 41})
		active <- err
	}()
	awaitReplicationValue(t, arrived)
	wait := &rpcObservedWait{Context: t.Context(), waiting: make(chan struct{})}
	queued := make(chan error, 1)
	go func() {
		_, err := peer.SendWALBatch(wait, []*WALEntry{{Position: 99}})
		queued <- err
	}()
	awaitReplicationValue(t, wait.waiting)
	require.NoError(t, peer.Close())
	require.Error(t, awaitReplicationValue(t, active))
	require.Error(t, awaitReplicationValue(t, queued))
	require.Zero(t, walCalls.Load())
}

func TestClusterRPCConcurrentHeartbeatAndWALKeepOwnReplies(t *testing.T) {
	peer, arrived, release, walCalls := gatedRPCPeer(t, true)
	defer release()
	type heartbeatResult struct {
		response *HeartbeatResponse
		err      error
	}
	type walResult struct {
		response *WALBatchResponse
		err      error
	}
	first := make(chan heartbeatResult, 1)
	go func() {
		response, err := peer.SendHeartbeat(t.Context(), &HeartbeatRequest{WALPosition: 41})
		first <- heartbeatResult{response, err}
	}()
	awaitReplicationValue(t, arrived)
	wait := &rpcObservedWait{Context: t.Context(), waiting: make(chan struct{})}
	second := make(chan walResult, 1)
	go func() {
		response, err := peer.SendWALBatch(wait, []*WALEntry{{Position: 99}})
		second <- walResult{response, err}
	}()
	awaitReplicationValue(t, wait.waiting)
	release()
	heartbeat := awaitReplicationValue(t, first)
	require.NoError(t, heartbeat.err)
	require.Equal(t, uint64(41), heartbeat.response.WALPosition)
	wal := awaitReplicationValue(t, second)
	require.NoError(t, wal.err)
	require.Equal(t, uint64(99), wal.response.AckedPosition)
	require.Equal(t, uint64(99), wal.response.ReceivedPosition)
	require.Equal(t, int32(1), walCalls.Load())
	require.True(t, peer.IsConnected())
}
