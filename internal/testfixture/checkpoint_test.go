package testfixture

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointContextWorkBoundary(t *testing.T) {
	var observed int
	c := NewCheckpointContext(3, func() { observed++ })
	defer c.Cancel()
	require.NoError(t, c.Err())
	require.NoError(t, c.Err())
	require.ErrorIs(t, c.Err(), context.Canceled)
	require.ErrorIs(t, c.Err(), context.Canceled)
	require.Equal(t, 3, c.Checks())
	require.Equal(t, 1, observed)
	select {
	case <-c.Done():
	default:
		t.Fatal("underlying context must actually be canceled")
	}
	for _, limit := range []int{0, -1} {
		live := NewCheckpointContext(limit, nil)
		for range 5 {
			require.NoError(t, live.Err())
		}
		require.Equal(t, 5, live.Checks())
		live.Cancel()
		require.ErrorIs(t, live.Err(), context.Canceled)
		require.Equal(t, 5, live.Checks(), "manual cancellation must not advance work")
	}
}

func TestCheckpointContextConcurrentObservations(t *testing.T) {
	var observed atomic.Int64
	c := NewCheckpointContext(3, func() { observed.Add(1) })
	defer c.Cancel()
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Err(); _ = c.Checks() }()
	}
	wg.Wait()
	require.ErrorIs(t, c.Err(), context.Canceled)
	require.Equal(t, int64(1), observed.Load())
	require.GreaterOrEqual(t, c.Checks(), 3)
}
