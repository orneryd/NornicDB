package lifecycle

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// deadlineRecorder captures the deadline of a ctx passed to a component's
// Shutdown. Used by TestRun_FreshShutdownContext to prove the shutdown ctx
// is decoupled from the cancelled supervisor ctx (OBS-09).
type deadlineRecorder struct {
	mu          sync.Mutex
	deadline    time.Time
	hasDeadline bool
}

func (r *deadlineRecorder) record(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadline, r.hasDeadline = ctx.Deadline()
}

// TestRun_SupervisesAllComponents covers OBS-07: Run starts all registered
// components and returns once the supervisor ctx is cancelled.
func TestRun_SupervisesAllComponents(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")
	c := NewFakeComponent("c")

	parent, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- Run(parent, a, b, c) }()

	// Give the goroutines a moment to enter Start.
	require.Eventually(t, func() bool {
		return a.StartCount() == 1 && b.StartCount() == 1 && c.StartCount() == 1
	}, 2*time.Second, 5*time.Millisecond, "all components should have Start called")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "Run should return nil when parent ctx is cancelled cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s of parent cancel")
	}

	require.Equal(t, int32(1), a.ShutdownCount())
	require.Equal(t, int32(1), b.ShutdownCount())
	require.Equal(t, int32(1), c.ShutdownCount())
}

// TestRun_ReverseDrainOrder covers OBS-08: components shut down in REVERSE
// registration order. Registered A, B, C → shutdown C, B, A.
func TestRun_ReverseDrainOrder(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")
	c := NewFakeComponent("c")

	parent, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(parent, a, b, c) }()

	require.Eventually(t, func() bool {
		return a.StartCount() == 1 && b.StartCount() == 1 && c.StartCount() == 1
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	// Reverse-order property: c < b < a (each later in real time means earlier
	// in the comparison, since we registered a, b, c in forward order).
	require.True(t, c.ShutdownBefore(b),
		"c shutdownAt=%d must be before b shutdownAt=%d", c.ShutdownAtNanos(), b.ShutdownAtNanos())
	require.True(t, b.ShutdownBefore(a),
		"b shutdownAt=%d must be before a shutdownAt=%d", b.ShutdownAtNanos(), a.ShutdownAtNanos())
}

// TestRun_FreshShutdownContext is the OBS-09 keystone: shutdown ctx has
// a fresh ~30s deadline, NOT inherited from the cancelled supervisor ctx.
func TestRun_FreshShutdownContext(t *testing.T) {
	// Parent ctx is ALREADY cancelled — simulates the case where the
	// errgroup ctx is cancelled (by signal or component error) before
	// Shutdown runs.
	parent, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	recorder := &deadlineRecorder{}

	fc := NewFakeComponent("recorder")
	// Start returns immediately (so Run proceeds to drain) since gctx
	// will already be Done.
	fc.OnStart = func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}
	fc.OnShutdown = func(ctx context.Context) error {
		recorder.record(ctx)
		return nil
	}

	err := Run(parent, fc)
	// Run may surface the parent cancellation as an error wrapping
	// context.Canceled; accept either nil or a wrapped Canceled error.
	if err != nil {
		require.True(t, errors.Is(err, context.Canceled),
			"unexpected non-cancel error: %v", err)
	}

	require.True(t, recorder.hasDeadline, "shutdown ctx must have a deadline")
	delta := time.Until(recorder.deadline)
	require.Greater(t, delta, 25*time.Second,
		"shutdown ctx must be a fresh ~%s timeout, not derived from cancelled parent; got Δ=%v",
		ShutdownTimeout, delta)
	require.LessOrEqual(t, delta, ShutdownTimeout+time.Second,
		"shutdown ctx Δ=%v should be ≤ %s", delta, ShutdownTimeout)
}

// TestRun_ComponentErrorTriggersDrain proves Pitfall 5 from RESEARCH:
// when one component's Start errors, ALL components still get Shutdown.
func TestRun_ComponentErrorTriggersDrain(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")
	c := NewFakeComponent("c")

	bErr := errors.New("b boom")
	b.OnStart = func(ctx context.Context) error { return bErr }

	err := Run(context.Background(), a, b, c)
	require.Error(t, err)
	require.True(t, errors.Is(err, bErr), "returned err should wrap b's start error: %v", err)

	require.Equal(t, int32(1), a.ShutdownCount(), "a must be shutdown")
	require.Equal(t, int32(1), b.ShutdownCount(), "b must be shutdown")
	require.Equal(t, int32(1), c.ShutdownCount(), "c must be shutdown")
}

// TestRun_SignalTriggersDrain confirms signal.NotifyContext-driven cancel
// runs the drain. Table-driven across SIGINT and SIGTERM.
func TestRun_SignalTriggersDrain(t *testing.T) {
	cases := []struct {
		name string
		sig  syscall.Signal
	}{
		{"SIGINT", syscall.SIGINT},
		{"SIGTERM", syscall.SIGTERM},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFakeComponent("svc")
			done := make(chan error, 1)
			go func() { done <- Run(context.Background(), fc) }()

			require.Eventually(t, func() bool { return fc.StartCount() == 1 },
				2*time.Second, 5*time.Millisecond, "Start should be entered")

			// Send the signal to ourselves.
			proc, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, proc.Signal(tc.sig))

			select {
			case err := <-done:
				// Run normally returns nil after a signal-driven clean drain;
				// accept nil as success.
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return within 5s of signal")
			}
			require.Equal(t, int32(1), fc.ShutdownCount())
		})
	}
}

// TestRun_ReturnsErrorsJoined: when Start returns errA AND a different
// component's Shutdown returns errB, the returned error wraps BOTH.
func TestRun_ReturnsErrorsJoined(t *testing.T) {
	startErr := errors.New("start failure")
	shutdownErr := errors.New("shutdown failure")

	a := NewFakeComponent("a")
	a.OnShutdown = func(ctx context.Context) error { return shutdownErr }

	b := NewFakeComponent("b")
	b.OnStart = func(ctx context.Context) error { return startErr }

	err := Run(context.Background(), a, b)
	require.Error(t, err)
	require.True(t, errors.Is(err, startErr), "expected wrapped start err, got: %v", err)
	require.True(t, errors.Is(err, shutdownErr), "expected wrapped shutdown err, got: %v", err)
}
