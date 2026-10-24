package bulk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserveStepsObservesOncePerStride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// An item of Stride/16 steps: the first item and every sixteenth after it.
	c := NewCheckpoint(ctx)
	var observed []int
	for item := 0; item < 48; item++ {
		if err := c.ObserveSteps(Stride / 16); err != nil {
			require.ErrorIs(t, err, context.Canceled)
			observed = append(observed, item)
		}
	}
	require.Equal(t, []int{0, 16, 32}, observed)

	// One step is Observe.
	c = NewCheckpoint(ctx)
	require.Error(t, c.ObserveSteps(1))
	require.NoError(t, c.ObserveSteps(1))

	// A single row's checkpoint never observes; nor does a nil one.
	single := RowsCheckpoint(ctx, 1)
	require.NoError(t, single.ObserveSteps(Stride))
	require.NoError(t, (*Checkpoint)(nil).ObserveSteps(Stride))
}
