package observability

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
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

// TestPprofListener_NameAndBindError covers the Name() contract and a
// constructor failure path (already-bound port). We also smoke the bind
// error by attempting to listen on the same port twice.
func TestPprofListener_NameAndBindError(t *testing.T) {
	l, err := NewPprofListener(PprofConfig{Enabled: true, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	t.Cleanup(func() {
		if l != nil {
			_ = l.Shutdown(context.Background())
		}
	})
	require.Equal(t, "pprof", l.Name())

	// Listen on the same port again → bind error.
	addr := l.ln.Addr().(*net.TCPAddr)
	_, err = NewPprofListener(PprofConfig{Enabled: true, Listen: addr.String()})
	require.Error(t, err)
}

// TestPprofListener_NotImportedFromDefaultMux — verifies that pkg/observability
// does NOT use `import _ "net/http/pprof"` (the side-effect form). We need
// `net/http/pprof` for its handler symbols (pprof.Index, etc.), but we must
// import it for its symbols, not for its init() side-effect.
//
// The runtime DefaultServeMux state is necessarily polluted by net/http/pprof's
// init() once the package is in the binary at all — that's a property of Go's
// stdlib, not of our wiring. The contract we actually care about is that our
// :9091 listener uses a CUSTOM mux, which is asserted by
// TestPprofListener_HandlersRegistered (the listener's mux serves the
// handlers) and by source-level grep for `_ "net/http/pprof"` in the verify
// gate. This test asserts the source-level property directly.
func TestPprofListener_NotImportedFromDefaultMux(t *testing.T) {
	src, err := os.ReadFile("pprof.go")
	require.NoError(t, err)
	// Strip line comments so commentary mentioning the forbidden form does not
	// false-positive.
	var stripped strings.Builder
	for _, line := range strings.Split(string(src), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		stripped.WriteString(line)
		stripped.WriteByte('\n')
	}
	code := stripped.String()
	require.NotContains(t, code, `_ "net/http/pprof"`,
		"pkg/observability/pprof.go must NOT use the side-effect `import _ \"net/http/pprof\"` form")
	// Sanity: it MUST import the package by name (we use pprof.Index etc.).
	require.Contains(t, code, `"net/http/pprof"`,
		"pkg/observability/pprof.go is expected to import net/http/pprof for pprof.Index/etc.")
}
