package replication

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise authenticated TCP dispatch and the real registered Raft handler,
// rather than calling the new context method directly. The inbound listener
// lifetime is cancelled while native preparation is paused, before acceptance.
func TestForwardedAdmissionContextCancellation(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := "leaf"
		if bulk {
			name = "bulk"
		}
		t.Run(name, func(t *testing.T) {
			b, adapter, r, barrier := admissionReplicatorFixture(t, ModeRaft, context.Background())
			inbound, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := NewClusterTransport(&ClusterTransportConfig{
				NodeID: "admission-server", BindAddr: "127.0.0.1:0",
				AuthSecret: []byte(strings.Repeat("x", 32)),
			})
			RegisterClusterHandlers(server, r)
			listenDone := make(chan error, 1)
			go func() { listenDone <- server.Listen(inbound, "127.0.0.1:0", nil) }()
			t.Cleanup(func() {
				cancel()
				require.NoError(t, server.Close())
				select {
				case <-listenDone:
				case <-time.After(5 * time.Second):
					t.Error("inbound listener did not stop")
				}
			})
			var addr string
			require.Eventually(t, func() bool {
				server.resources.mu.RLock()
				defer server.resources.mu.RUnlock()
				if server.resources.listener == nil {
					return false
				}
				addr = server.bindAddr
				return true
			}, 5*time.Second, time.Millisecond)
			client := NewClusterTransport(&ClusterTransportConfig{
				NodeID: "admission-client", AuthSecret: []byte(strings.Repeat("x", 32)),
			})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			wireContext, stopWire := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopWire()
			peer, err := client.Connect(wireContext, addr)
			require.NoError(t, err)
			before, err := adapter.GetWALPosition()
			require.NoError(t, err)
			command := admissionNodeCommand(t, bulk)
			barrier.armed.Store(true)
			result := make(chan error, 1)
			go func() { result <- peer.(*ClusterConnection).SendForwardApply(wireContext, command, 5*time.Second) }()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				close(barrier.resume)
				t.Fatal("registered inbound command did not reach native preparation")
			}
			cancel()
			close(barrier.resume)
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("forwarded request did not settle")
			}
			nodes, readErr := b.GetStoredNodesByLabel("ContextCandidate")
			require.NoError(t, readErr)
			after, readErr := adapter.GetWALPosition()
			require.NoError(t, readErr)
			t.Logf("bulk=%v forwarded=%v candidates=%d WAL=%d->%d", bulk, err, len(nodes), before, after)
			require.ErrorContains(t, err, context.Canceled.Error())
			require.Empty(t, nodes)
			require.Equal(t, before, after, "rejected inbound work cannot enter recovery")
		})
	}
}

// This public producer control cancels in the actual bulk encoding loop, before
// Apply is entered; a successful native preparation check alone cannot prove it.
func TestReplicatedInputEncodingCancellation(t *testing.T) {
	b, adapter, r, _ := admissionReplicatorFixture(t, ModeStandalone, context.Background())
	engine := NewReplicatedEngine(b, r, time.Second)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &admissionEncodingContext{Context: parent, cancel: cancel}
	nodes := make([]*storage.Node, 20)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID("test:encoding" + string(rune('a'+i))), Labels: []string{"ContextCandidate"}}
	}
	before, err := adapter.GetWALPosition()
	require.NoError(t, err)
	err = storage.BulkCreateNodesWithContext(ctx, engine, nodes)
	t.Logf("bulk-encoder observations=%d result=%v", ctx.hits, err)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	stored, err := b.GetStoredNodesByLabel("ContextCandidate")
	require.NoError(t, err)
	require.Empty(t, stored)
	after, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, storage.BulkCreateNodesWithContext(context.Background(), engine, nodes))
	stored, err = b.GetStoredNodesByLabel("ContextCandidate")
	require.NoError(t, err)
	require.Len(t, stored, len(nodes))
}

// A reusable connection can receive a reverse request without an inbound
// listener context. Registration must supply the real replicator lifetime.
func TestReverseForwardedAdmissionLifetimeCancellation(t *testing.T) {
	b, adapter, r, barrier := admissionReplicatorFixture(t, ModeRaft, context.Background())
	remoteContext, stopRemote := context.WithCancel(context.Background())
	defer stopRemote()
	remote := NewClusterTransport(&ClusterTransportConfig{NodeID: "reverse-sender", AuthSecret: []byte(strings.Repeat("x", 32))})
	remote.RegisterHandler(ClusterMsgHeartbeat, func(_ context.Context, _ string, msg *ClusterMessage) (*ClusterMessage, error) {
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: msg.Payload}, nil
	})
	accepted := make(chan PeerConnection, 1)
	listenDone := make(chan error, 1)
	go func() {
		listenDone <- remote.Listen(remoteContext, "127.0.0.1:0", func(c PeerConnection) { accepted <- c })
	}()
	t.Cleanup(func() {
		stopRemote()
		require.NoError(t, remote.Close())
		select {
		case <-listenDone:
		case <-time.After(5 * time.Second):
			t.Error("remote listener did not stop")
		}
	})
	var addr string
	require.Eventually(t, func() bool {
		remote.resources.mu.RLock()
		defer remote.resources.mu.RUnlock()
		if remote.resources.listener == nil {
			return false
		}
		addr = remote.bindAddr
		return true
	}, 5*time.Second, time.Millisecond)
	receiver := NewClusterTransport(&ClusterTransportConfig{NodeID: "reverse-receiver", AuthSecret: []byte(strings.Repeat("x", 32))})
	RegisterClusterHandlers(receiver, r)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	wire, stopWire := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopWire()
	connection, err := receiver.Connect(wire, addr)
	require.NoError(t, err)
	_, err = connection.(*ClusterConnection).SendHeartbeat(wire, &HeartbeatRequest{NodeID: "reverse-receiver"})
	require.NoError(t, err)
	var sender PeerConnection
	select {
	case sender = <-accepted:
	case <-wire.Done():
		t.Fatal("authenticated reverse peer was not announced")
	}
	before, err := adapter.GetWALPosition()
	require.NoError(t, err)
	command := admissionNodeCommand(t, true)
	barrier.armed.Store(true)
	result := make(chan error, 1)
	go func() { result <- sender.(*ClusterConnection).SendForwardApply(wire, command, 5*time.Second) }()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		close(barrier.resume)
		t.Fatal("reverse request did not reach native preparation")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- r.Shutdown() }()
	if owner, ok := r.(interface{ LifetimeContext() context.Context }); ok {
		select {
		case <-owner.LifetimeContext().Done():
		case <-time.After(5 * time.Second):
			close(barrier.resume)
			t.Fatal("Shutdown did not cancel the registered lifetime")
		}
	} else {
		select {
		case err = <-stopped:
			require.NoError(t, err)
			stopped = nil
		case <-time.After(5 * time.Second):
			close(barrier.resume)
			t.Fatal("old public Shutdown did not finish")
		}
	}
	close(barrier.resume)
	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("reverse request did not settle")
	}
	if stopped != nil {
		select {
		case stopErr := <-stopped:
			require.NoError(t, stopErr)
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown did not join")
		}
	}
	nodes, readErr := b.GetStoredNodesByLabel("ContextCandidate")
	require.NoError(t, readErr)
	after, readErr := adapter.GetWALPosition()
	require.NoError(t, readErr)
	t.Logf("reverse=%v candidates=%d WAL=%d->%d", err, len(nodes), before, after)
	require.ErrorContains(t, err, context.Canceled.Error())
	require.Empty(t, nodes)
	require.Equal(t, before, after)
}
