package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFakeComponent_RecordsCalls confirms FakeComponent counts Start and
// Shutdown invocations correctly under concurrent use.
func TestFakeComponent_RecordsCalls(t *testing.T) {
	fc := NewFakeComponent("svc")
	require.Equal(t, "svc", fc.Name())

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = fc.Start(ctx)
	}()

	require.Eventually(t, func() bool { return fc.StartCount() == 1 },
		1*time.Second, 5*time.Millisecond, "Start should be entered")

	cancel()
	wg.Wait()

	require.NoError(t, fc.Shutdown(context.Background()))
	require.Equal(t, int32(1), fc.StartCount())
	require.Equal(t, int32(1), fc.ShutdownCount())
}

// TestFakeComponent_StartedBefore checks ordering helpers work.
func TestFakeComponent_StartedBefore(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = a.Start(ctx) }()
	require.Eventually(t, func() bool { return a.StartCount() == 1 },
		1*time.Second, 5*time.Millisecond)

	// Sleep guarantees a strictly later monotonic timestamp on b.
	time.Sleep(2 * time.Millisecond)

	go func() { _ = b.Start(ctx) }()
	require.Eventually(t, func() bool { return b.StartCount() == 1 },
		1*time.Second, 5*time.Millisecond)

	require.True(t, a.StartedBefore(b),
		"a startedAt=%d must be < b startedAt=%d", a.StartedAtNanos(), b.StartedAtNanos())
	require.False(t, b.StartedBefore(a))
}

// TestFakeComponent_OverridesHonored confirms OnStart/OnShutdown overrides
// are respected when set.
func TestFakeComponent_OverridesHonored(t *testing.T) {
	startErr := errors.New("start override")
	shutdownErr := errors.New("shutdown override")

	fc := NewFakeComponent("override")
	fc.OnStart = func(ctx context.Context) error { return startErr }
	fc.OnShutdown = func(ctx context.Context) error { return shutdownErr }

	require.ErrorIs(t, fc.Start(context.Background()), startErr)
	require.ErrorIs(t, fc.Shutdown(context.Background()), shutdownErr)
}
