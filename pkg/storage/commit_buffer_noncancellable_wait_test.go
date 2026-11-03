package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An already-claimed generation represents accepted replay owned elsewhere.
// The observer advances that actual wait deterministically, without a worker
// or scheduler-dependent sleep in the fixture.
func claimedFlushWaitFixture() (*WriteBehindBuffer, *CommitBuffer) {
	generation := newCommitBuffer()
	generation.claimDrain()
	return &WriteBehindBuffer{active: newCommitBuffer(), draining: []*CommitBuffer{generation}, flushCh: make(chan struct{}, 1)}, generation
}

func TestFlushNoncancellableWaitAllocationParity(t *testing.T) {
	request, cancel := context.WithCancel(context.Background())
	detached := context.WithoutCancel(request)
	cancel()
	allocations := func(ctx context.Context) float64 {
		return testing.AllocsPerRun(10, func() {
			w, g := claimedFlushWaitFixture()
			waits := 0
			restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
				if owner == w {
					waits++
					if waits == 5 {
						g.markApplied()
					}
				}
			})
			err := w.flushContext(ctx, false, true)
			restore()
			require.NoError(t, err)
			require.Equal(t, 5, waits)
			require.Empty(t, w.draining)
		})
	}
	base := allocations(context.Background())
	for name, ctx := range map[string]context.Context{"background": context.Background(), "detached": detached} {
		actual := allocations(ctx)
		t.Logf("five blocked reclaim waits: detached=%.0f %s=%.0f allocations", base, name, actual)
		require.LessOrEqual(t, actual, base+1, "noncancellable context must not allocate a timer per wait")
	}
}

func TestFlushClaimedWaitCancellationKeepsReplayOwnership(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			w, g := claimedFlushWaitFixture()
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			waits := 0
			restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
				if owner == w {
					waits++
					if !deadline {
						cancel()
					}
				}
			})
			defer restore()
			err := w.flushContext(ctx, false, true)
			if deadline {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			// A real deadline may expire before the first wait under suite load.
			// The observer-cancel case proves cancellation inside the actual wait.
			if !deadline {
				require.Positive(t, waits)
			}
			require.True(t, g.draining.Load(), "wait cancellation cannot steal accepted replay")
			require.False(t, g.IsApplied())
			require.Equal(t, []*CommitBuffer{g}, w.draining)
		})
	}
}
