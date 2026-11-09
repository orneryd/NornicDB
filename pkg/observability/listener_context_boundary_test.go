package observability

import (
	"context"
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

		first := t.Context()
		got := owner.init(first, b.srv, b.ln, b.beforeStop)
		require.True(t, got == first, "init must return the caller's context unchanged")
		require.NotNil(t, owner.listener, "init must bind the owned resources")

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
			_ = owner.init(first, b.srv, b.ln, b.beforeStop)
		})
		require.LessOrEqualf(t, allocs, 0.0,
			"steady-state context boundary must not allocate; got %.1f allocs/op", allocs)
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
