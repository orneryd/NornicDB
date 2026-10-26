package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net"
	"sync"
	"testing"
	"time"
)

type closeRendezvousConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
	closes chan struct{}
}

func (c *closeRendezvousConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	if c.closes != nil {
		c.closes <- struct{}{}
	}
	return err
}
func TestClusterTransportCloseJoinsInflightResponseWithoutAdmissionLocks(t *testing.T) {
	server := NewClusterTransport(&ClusterTransportConfig{NodeID: "closing"})
	left, right := net.Pipe()
	observed := &closeRendezvousConn{Conn: left, closed: make(chan struct{})}
	conn := server.createConnection("peer", observed)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	server.RegisterHandler(ClusterMsgHeartbeat, func(context.Context, string, *ClusterMessage) (*ClusterMessage, error) {
		close(entered)
		<-release
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse}, nil
	})
	server.resources.mu.Lock()
	server.resources.connections["peer"] = conn
	conn.wg.Add(1)
	server.resources.mu.Unlock()
	go conn.readLoop()
	payload := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, &ClusterMessage{Type: ClusterMsgHeartbeat})
	var frame bytes.Buffer
	require.NoError(t, binary.Write(&frame, binary.BigEndian, uint32(len(payload))))
	_, err := frame.Write(payload)
	require.NoError(t, err)
	writeDone := make(chan error, 1)
	go func() { _, err := right.Write(frame.Bytes()); writeDone <- err }()
	closeDone := make(chan struct{})
	var closeErr error
	var startOnce sync.Once
	startClose := func() { startOnce.Do(func() { go func() { closeErr = server.Close(); close(closeDone) }() }) }
	t.Cleanup(func() {
		releaseHandler()
		_ = right.Close()
		_ = observed.Close()
		startClose()
		select {
		case <-writeDone:
		case <-time.After(2 * time.Second):
			t.Error("frame writer did not join")
		}
		select {
		case <-closeDone:
		case <-time.After(2 * time.Second):
			t.Error("close worker did not join")
		}
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("response handler did not start")
	}
	startClose()
	// Network Close is inside the existing Close owner, so the handler cannot
	// race ahead of the close path and accidentally miss its mutex ordering.
	select {
	case <-observed.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("transport did not close network")
	}
	releaseHandler()
	select {
	case <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for handler while retaining its admission mutex")
	}
	require.NoError(t, server.Close())

}

// An accepted socket can reach its registration owner after Close started.
// Its existing transport worker count must join without registering a new loop.
func TestClusterTransportCloseRejectsDelayedIncomingRegistration(t *testing.T) {
	server := NewClusterTransport(&ClusterTransportConfig{NodeID: "delayed-peer"})
	left, right := net.Pipe()
	defer right.Close()
	observed := &closeRendezvousConn{Conn: left, closed: make(chan struct{})}
	server.wg.Add(1)
	closeDone := make(chan struct{})
	go func() { _ = server.Close(); close(closeDone) }()
	<-server.closeCh
	server.handleIncoming(context.Background(), observed, nil)
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join admitted peer")
	}
	select {
	case <-observed.closed:
	default:
		t.Fatal("delayed peer socket was not closed")
	}
	server.resources.mu.RLock()
	count := len(server.resources.connections)
	server.resources.mu.RUnlock()
	require.Zero(t, count, "closed admission cannot retain a newly registered peer")
}
