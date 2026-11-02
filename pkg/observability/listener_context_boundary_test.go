package observability

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// listenerContextKey marks a caller value that must survive the shared
// listener context boundary.
type listenerContextKey struct{}

// publicListenerBoundary exposes one command HTTP service through its public
// lifecycle entry points plus the shared owner those entries normalize through.
type publicListenerBoundary struct {
	owner      *listenerLifetime
	srv        *http.Server
	ln         net.Listener
	beforeStop func(context.Context) error
	addr       string
	probe      string
	start      func(context.Context) error
	shutdown   func(context.Context) error
	wait       func(context.Context) error
}

func newTelemetryBoundary(t *testing.T) publicListenerBoundary {
	t.Helper()
	env := NewTestEnv(t)
	l, port := newTestListener(t, env)
	t.Cleanup(func() { _ = l.Shutdown(context.Background()) })
	return publicListenerBoundary{
		owner:      &l.retirement,
		srv:        l.srv,
		ln:         l.ln,
		beforeStop: l.prov.Shutdown,
		addr:       net.JoinHostPort("127.0.0.1", itoa(port)),
		probe:      "/livez",
		start:      l.Start,
		shutdown:   l.Shutdown,
		wait:       l.WaitStopped,
	}
}

func newPprofBoundary(t *testing.T) publicListenerBoundary {
	t.Helper()
	l, err := NewPprofListener(PprofConfig{Enabled: true, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NotNil(t, l)
	t.Cleanup(func() { _ = l.Shutdown(context.Background()) })
	return publicListenerBoundary{
		owner:    &l.retirement,
		srv:      l.srv,
		ln:       l.ln,
		addr:     l.ln.Addr().String(),
		probe:    "/debug/pprof/",
		start:    l.Start,
		shutdown: l.Shutdown,
		wait:     l.WaitStopped,
	}
}

func forEachListenerBoundary(t *testing.T, drive func(t *testing.T, b publicListenerBoundary)) {
	t.Helper()
	t.Run("telemetry", func(t *testing.T) { drive(t, newTelemetryBoundary(t)) })
	t.Run("pprof", func(t *testing.T) { drive(t, newPprofBoundary(t)) })
}

// TestListenerLifetimeInit_ContextBoundary pins the shared owner's boundary:
// the optional nil caller context is normalized once to a live non-nil context,
// a caller's own context is returned unchanged (identity, value, deadline and
// cancellation), and neither call re-binds the once-owned transport resources.
func TestListenerLifetimeInit_ContextBoundary(t *testing.T) {
	forEachListenerBoundary(t, func(t *testing.T, b publicListenerBoundary) {
		owner := b.owner
		require.Nil(t, owner.listener, "fresh listener owner must not be bound yet")

		insideActive := 0
		b.srv.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
			insideActive = owner.work.Active()
			out.WriteHeader(http.StatusNoContent)
		})

		// nil: one normalization to a live context — no deadline, no
		// cancellation, and none of the work lifetime's stop trigger.
		got := owner.init(nil, b.srv, b.ln, b.beforeStop)
		require.NotNil(t, got)
		require.NoError(t, got.Err())
		require.Nil(t, got.Done(), "nil caller context must behave like context.Background")
		_, hasDeadline := got.Deadline()
		require.False(t, hasDeadline)
		require.NotNil(t, owner.listener, "nil caller context must still bind the owned resources")

		boundListener := owner.listener

		// non-nil: the same caller context comes back, unsubstituted.
		callerDeadline := time.Now().Add(30 * time.Second)
		callerCtx, cancel := context.WithDeadline(
			context.WithValue(context.Background(), listenerContextKey{}, "caller-value"),
			callerDeadline,
		)
		defer cancel()

		got = owner.init(callerCtx, b.srv, b.ln, b.beforeStop)
		require.True(t, got == callerCtx, "init must return the caller's context unchanged")
		require.Equal(t, "caller-value", got.Value(listenerContextKey{}))
		gotDeadline, ok := got.Deadline()
		require.True(t, ok)
		require.Equal(t, callerDeadline, gotDeadline)
		require.Equal(t, callerCtx.Done(), got.Done())

		// The bound resources stay once-owned and caller-independent.
		require.Equal(t, boundListener, owner.listener)
		response := httptest.NewRecorder()
		b.srv.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusNoContent, response.Code)
		require.Equal(t, 1, insideActive, "the handler must be admitted once, not wrapped again")
		require.Zero(t, owner.work.Active(), "handler admission must release on return")

		cancel()
		<-callerCtx.Done()
		require.Equal(t, context.Canceled, got.Err())

		// Steady state (initOnce already done): the boundary choice adds no
		// allocation.
		allocs := testing.AllocsPerRun(200, func() {
			_ = owner.init(nil, b.srv, b.ln, b.beforeStop)
		})
		require.LessOrEqualf(t, allocs, 0.0,
			"steady-state context boundary must not allocate; got %.1f allocs/op", allocs)
	})
}

// TestListener_PublicNilContextDefaults proves the public nil default for both
// services: nil Start serves until Shutdown, nil Shutdown drains, and nil
// WaitStopped joins the completed retirement instead of failing.
func TestListener_PublicNilContextDefaults(t *testing.T) {
	forEachListenerBoundary(t, func(t *testing.T, b publicListenerBoundary) {
		client := &http.Client{Timeout: time.Second}
		t.Cleanup(client.CloseIdleConnections)
		startErr := make(chan error, 1)
		go func() { startErr <- b.start(nil) }()

		require.Eventually(t, func() bool {
			resp, err := client.Get("http://" + b.addr + b.probe)
			if err != nil {
				return false
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			return resp.StatusCode == http.StatusOK
		}, 2*time.Second, 10*time.Millisecond,
			"Start(nil) must behave like context.Background and keep serving")

		shutdownErr := make(chan error, 1)
		go func() { shutdownErr <- b.shutdown(nil) }()
		select {
		case err := <-shutdownErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown(nil) did not return within 5s")
		}

		select {
		case err := <-startErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Start(nil) did not return after Shutdown(nil)")
		}

		require.NoError(t, b.wait(nil), "WaitStopped(nil) must observe the completed retirement")

		conn, err := net.DialTimeout("tcp", b.addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("listener %s still accepting connections after Shutdown(nil)", b.addr)
		}
	})
}

// TestListener_WaitStoppedHonorsCallerContext proves a non-nil caller context
// reaches the private wait unchanged: an expired deadline and a cancellation
// each bound the entry wait instead of being replaced with Background.
func TestListener_WaitStoppedHonorsCallerContext(t *testing.T) {
	forEachListenerBoundary(t, func(t *testing.T, b publicListenerBoundary) {
		expired, cancelExpired := context.WithDeadline(
			context.WithValue(context.Background(), listenerContextKey{}, "caller-value"),
			time.Now().Add(-time.Second),
		)
		defer cancelExpired()
		require.ErrorIs(t, expired.Err(), context.DeadlineExceeded)

		waitErr := make(chan error, 1)
		go func() { waitErr <- b.wait(expired) }()
		select {
		case err := <-waitErr:
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"WaitStopped must honor the caller's deadline")
		case <-time.After(2 * time.Second):
			t.Fatal("WaitStopped did not honor the caller's non-nil deadline")
		}

		cancelled, cancel := context.WithCancel(
			context.WithValue(context.Background(), listenerContextKey{}, "caller-value"),
		)
		cancel()
		go func() { waitErr <- b.wait(cancelled) }()
		select {
		case err := <-waitErr:
			require.ErrorIs(t, err, context.Canceled,
				"WaitStopped must honor the caller's cancellation")
		case <-time.After(2 * time.Second):
			t.Fatal("WaitStopped did not honor the caller's cancellation")
		}
	})
}
