package observability

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTelemetryListener_StartObservesContextCancellation covers the
// ctx.Done() path of telemetryListener.Start.
//
// Background: Plan 01-04 made lifecycle.Run the canonical supervisor.
// lifecycle.Run blocks on g.Wait() before invoking each Component's
// Shutdown — so each Component.Start MUST observe its ctx and trigger
// graceful shutdown internally, otherwise the supervisor deadlocks on
// SIGTERM. Plan 01-03 originally shipped a Start that ignored ctx;
// this test guards against regressing back.
func TestTelemetryListener_StartObservesContextCancellation(t *testing.T) {
	env := NewTestEnv(t)
	l, port := newTestListener(t, env)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- l.Start(ctx) }()

	// Wait for the listener to be reachable, proving Serve is running.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(port)), 50*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "listener never came up")

	// Cancel ctx — Start must observe it, trigger internal Shutdown,
	// and return cleanly within ShutdownTimeout.
	cancel()
	select {
	case err := <-startDone:
		require.NoError(t, err, "Start returned error after ctx cancel: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Start did not return within 5s of ctx cancel — supervisor deadlock regression")
	}

	// Listener port must be unreachable now.
	c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(port)), 100*time.Millisecond)
	if err == nil {
		_ = c.Close()
		t.Fatalf("listener port %d still accepting connections after Start returned", port)
	}
}

// TestPprofListener_StartObservesContextCancellation covers the
// ctx.Done() path of pprofListener.Start.
func TestPprofListener_StartObservesContextCancellation(t *testing.T) {
	cfg := PprofConfig{Enabled: true, Listen: "127.0.0.1:0"}
	l, err := NewPprofListener(cfg)
	require.NoError(t, err)
	require.NotNil(t, l)

	addr := l.ln.Addr().(*net.TCPAddr)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- l.Start(ctx) }()

	// Verify it serves /debug/pprof/ before cancellation.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr.String() + "/debug/pprof/")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Millisecond, "pprof listener never came up")

	cancel()
	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("pprof Start did not return within 5s of ctx cancel — supervisor deadlock regression")
	}
}

// itoa is shared with listener_test.go.
