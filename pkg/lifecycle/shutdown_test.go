package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestShutdown_TimeoutBudget is a constant test that catches accidental
// edits to the 30-second drain budget mandated by ADR-0001 §2.8.1 / A10a.
func TestShutdown_TimeoutBudget(t *testing.T) {
	require.Equal(t, 30*time.Second, ShutdownTimeout,
		"ShutdownTimeout must remain 30s per ADR-0001 §2.8.1 A10a")
}

// TestShutdown_DrainContinuesAfterError: when one component's Shutdown
// returns an error, the drain loop continues and shuts down the others.
func TestShutdown_DrainContinuesAfterError(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")
	c := NewFakeComponent("c")

	bErr := errors.New("b shutdown boom")
	b.OnShutdown = func(ctx context.Context) error { return bErr }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := DrainReverse(ctx, []Component{a, b, c})
	require.Error(t, err)
	require.True(t, errors.Is(err, bErr), "drain err should wrap b's err: %v", err)

	// All three must have had Shutdown called even though b errored.
	require.Equal(t, int32(1), a.ShutdownCount(), "a must still be shut down")
	require.Equal(t, int32(1), b.ShutdownCount())
	require.Equal(t, int32(1), c.ShutdownCount())
}

// TestShutdown_ReverseOrder verifies DrainReverse calls Shutdown in
// reverse slice order (last → first).
func TestShutdown_ReverseOrder(t *testing.T) {
	a := NewFakeComponent("a")
	b := NewFakeComponent("b")
	c := NewFakeComponent("c")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, DrainReverse(ctx, []Component{a, b, c}))
	require.True(t, c.ShutdownBefore(b), "c should shutdown before b")
	require.True(t, b.ShutdownBefore(a), "b should shutdown before a")
}
