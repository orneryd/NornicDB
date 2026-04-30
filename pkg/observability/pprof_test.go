package observability

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPprofListener_OptInAndDefault127 — table-driven coverage of OBS-06:
//   - Disabled cfg returns (nil, nil) so the caller skips registration.
//   - Empty Listen with Enabled=true defaults to "127.0.0.1:9091" (ADR §A9).
//   - Operator-set Listen overrides the default.
//
// We construct against an ephemeral port for the actual bind to avoid
// colliding with a real :9091 on the host; the literal "127.0.0.1:9091"
// default is asserted against srv.Addr without binding (we substitute via a
// public helper that exposes the resolved bind without dialing).
func TestPprofListener_OptInAndDefault127(t *testing.T) {
	t.Run("disabled returns nil listener", func(t *testing.T) {
		l, err := NewPprofListener(PprofConfig{Enabled: false})
		require.NoError(t, err)
		require.Nil(t, l, "disabled cfg must return nil so caller skips registration")
	})

	t.Run("default bind is literal 127.0.0.1:9091 when unset", func(t *testing.T) {
		// Use a helper to inspect resolved bind address without trying to bind 9091.
		got := resolvePprofListen(PprofConfig{Enabled: true, Listen: ""})
		require.Equal(t, "127.0.0.1:9091", got, "OBS-06 / ADR §A9: default bind must be the literal 127.0.0.1:9091")
	})

	t.Run("explicit operator override is honored", func(t *testing.T) {
		got := resolvePprofListen(PprofConfig{Enabled: true, Listen: ":9091"})
		require.Equal(t, ":9091", got, "operator-explicit Listen must override default")
	})
}

// TestPprofListener_HandlersRegistered — start the listener on an ephemeral
// loopback port and verify the documented pprof URLs return 200. Skips
// /profile and /trace because they take 30s+ by default.
func TestPprofListener_HandlersRegistered(t *testing.T) {
	l, err := NewPprofListener(PprofConfig{Enabled: true, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NotNil(t, l)

	startErr := make(chan error, 1)
	go func() { startErr <- l.Start(context.Background()) }()
	t.Cleanup(func() {
		_ = l.Shutdown(context.Background())
		<-startErr
	})

	addr := l.ln.Addr().(*net.TCPAddr)
	base := "http://" + net.JoinHostPort("127.0.0.1", itoa(addr.Port))

	// Wait for the listener to accept connections.
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(addr.Port)), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/symbol"} {
		resp, err := http.Get(base + path)
		require.NoError(t, err, "GET %s", path)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s expected 200", path)
	}
}

// TestPprofListener_NotImportedFromDefaultMux — ensures we did NOT use
// `import _ "net/http/pprof"` (which would register on
// http.DefaultServeMux). The DefaultServeMux's handler for /debug/pprof/
// must be the 404 stub (or otherwise not the pprof Index).
func TestPprofListener_NotImportedFromDefaultMux(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	http.DefaultServeMux.ServeHTTP(rec, req)
	body := rec.Body.String()
	// pprof.Index emits HTML containing "pprof". If absent, the side-effect import
	// did not occur. Accept either an explicit 404 or any non-pprof response.
	require.False(t, rec.Code == http.StatusOK && strings.Contains(strings.ToLower(body), "<title>/debug/pprof/</title>"),
		"pkg/observability must NOT register pprof on http.DefaultServeMux (no `import _ \"net/http/pprof\"`)")
}
