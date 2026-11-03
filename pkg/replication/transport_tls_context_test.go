package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A real TCP peer consumes ClientHello but never answers the TLS handshake.
// Cancellation must end the actual exported Connect, not wait for its separate
// five-second transport timeout. Every peer/socket/goroutine is joined even on
// the old failing implementation.
func TestClusterTransportTLSConnectCancelsStalledHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	release := make(chan struct{})
	entered := make(chan struct{})
	peerDone := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	go func() {
		defer close(peerDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var hello [1]byte
		if _, err := io.ReadFull(conn, hello[:]); err != nil {
			return
		}
		close(entered)
		<-release
	}()
	transport := NewClusterTransport(&ClusterTransportConfig{NodeID: "tls-context", DialTimeout: 5 * time.Second, TLSClient: &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12}})
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		conn PeerConnection
		err  error
	}
	result := make(chan outcome, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		c, err := transport.Connect(request, listener.Addr().String())
		result <- outcome{c, err}
	}()
	t.Cleanup(func() {
		cancel()
		unblock()
		err := listener.Close()
		if err != nil {
			require.ErrorIs(t, err, net.ErrClosed)
		}
		<-peerDone
		<-joined
		require.NoError(t, transport.Close())
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("public Connect did not start the real TLS handshake")
	}
	cancel()
	select {
	case actual := <-result:
		require.Nil(t, actual.conn)
		require.ErrorIs(t, actual.err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Error("public TLS Connect ignored cancellation for 100ms after ClientHello")
		// Release the intentionally stalled peer before joining the failed call.
		unblock()
		actual := <-result
		require.Nil(t, actual.conn)
	}
	require.Empty(t, transport.resources.connections, "a canceled handshake must never publish a connection")
}

// Expiration before entry must not be replaced by the independent dial timeout.
func TestClusterTransportTLSConnectRetainsExpiredCallerDeadline(t *testing.T) {
	transport := NewClusterTransport(&ClusterTransportConfig{NodeID: "tls-expired", DialTimeout: time.Second, TLSClient: &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	caller, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	conn, err := transport.Connect(caller, "127.0.0.1:1")
	require.Nil(t, conn)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expired caller must retain its cause, got %v", err)
}

// The context-aware dial must retain configured certificate verification and
// authenticated cluster traffic; it cannot turn a canceled dial fix into a
// trust/authentication bypass or change the successful RPC result path.
func TestClusterTransportTLSConnectPreservesVerifiedPeerRPC(t *testing.T) {
	cert, key, ca := writeSelfSignedTLSMaterial(t, t.TempDir())
	serverTLS, clientTLS, err := buildTLSConfigs(TLSConfig{CertFile: cert, KeyFile: key, CAFile: ca, VerifyClient: true})
	require.NoError(t, err)
	clientTLS.ServerName = "localhost"
	server := NewClusterTransport(&ClusterTransportConfig{NodeID: "tls-server", TLSServer: serverTLS, AuthSecret: []byte(peerAuthTestSecret)})
	var handled atomic.Int32
	server.RegisterHandler(ClusterMsgHeartbeat, func(_ context.Context, _ string, msg *ClusterMessage) (*ClusterMessage, error) {
		handled.Add(1)
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: msg.Payload}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	listened := make(chan error, 1)
	go func() { listened <- server.Listen(ctx, "127.0.0.1:0", nil) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, server.Close())
		err := <-listened
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
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
	for _, name := range []string{"verified-peer", "wrong-secret", "untrusted-certificate"} {
		t.Run(name, func(t *testing.T) {
			configured := clientTLS.Clone()
			secret := []byte(peerAuthTestSecret)
			if name == "wrong-secret" {
				secret = []byte("wrong-secret-of-forty-characters!!!!!!!!")
			}
			if name == "untrusted-certificate" {
				configured.RootCAs = x509.NewCertPool()
			}
			client := NewClusterTransport(&ClusterTransportConfig{NodeID: "tls-client", TLSClient: configured, AuthSecret: secret})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			request, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			conn, err := client.Connect(request, addr)
			if name == "untrusted-certificate" {
				require.Error(t, err)
				require.Nil(t, conn)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, conn)
			prior := handled.Load()
			response, err := conn.SendHeartbeat(request, &HeartbeatRequest{NodeID: "tls-client", Timestamp: time.Now().UnixNano()})
			if name == "wrong-secret" {
				require.Error(t, err)
				require.Equal(t, prior, handled.Load(), "unverified message must not reach cluster handler")
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
				require.Equal(t, prior+1, handled.Load())
			}
		})
	}
}
