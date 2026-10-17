package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tlsShutdownTestAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func tlsShutdownTestConfig(t *testing.T) *Config {
	t.Helper()
	certFile, keyFile, caFile := writeSelfSignedTLSMaterial(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "standby-1"
	cfg.BindAddr = tlsShutdownTestAddr(t)
	cfg.HAStandby.Role = "standby"
	cfg.HAStandby.PeerAddr = "127.0.0.1:1"
	cfg.HAStandby.AutoFailover = false
	cfg.ReplicationSecret = "replication-secret-of-forty-characters!!"
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = certFile
	cfg.TLS.KeyFile = keyFile
	cfg.TLS.CAFile = caFile
	cfg.TLS.VerifyClient = true
	return cfg
}

func waitTCPListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "nothing listening on %s", addr)
}

// Cancelling Listen's context ends it even on a TLS listener, which gets no
// accept deadline.
func TestClusterTransportListen_TLSReturnsWhenCancelled(t *testing.T) {
	cfg := tlsShutdownTestConfig(t)
	transport, err := NewDefaultTransportFromConfig(cfg)
	require.NoError(t, err)
	defer transport.Close()

	ctx, cancel := context.WithCancel(context.Background())
	accepted := make(chan struct{}, 16)
	done := make(chan error, 1)
	go func() {
		done <- transport.Listen(ctx, cfg.BindAddr, func(PeerConnection) { accepted <- struct{}{} })
	}()
	waitTCPListening(t, cfg.BindAddr)
	// Every probe connection has been accepted and Listen is blocked in
	// Accept again, so only the cancellation can end it.
	<-accepted
	time.Sleep(200 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Listen on a TLS listener did not return after its context was cancelled")
	}
}

// An HA standby with TLS on shuts down promptly: Shutdown waits for the
// listener before it closes the transport.
func TestHAStandbyShutdown_TLSIsPrompt(t *testing.T) {
	cfg := tlsShutdownTestConfig(t)
	r, err := NewHAStandbyReplicator(cfg, NewMockStorage())
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background()))
	waitTCPListening(t, cfg.BindAddr)
	// Let the listener take the probe connection and block in Accept again,
	// so only Shutdown can end it.
	time.Sleep(200 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- r.Shutdown() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("HA standby with TLS did not shut down")
	}
}
