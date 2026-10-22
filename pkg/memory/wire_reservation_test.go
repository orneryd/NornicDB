package memory

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWireReservationGrowsOneOwnerAndReleases(t *testing.T) {
	now := time.Unix(100, 0)
	policy := NewPolicy(1<<20, 0, time.Second, func() (Sample, error) { return Sample{Used: 1}, nil }, func() time.Time { return now })
	require.NoError(t, policy.Refresh())
	lease, err := policy.ReserveWireRequest(1, 1)
	require.NoError(t, err)
	require.EqualValues(t, 64<<10, policy.Reserved())
	limit, err := lease.WireLimit()
	require.NoError(t, err)
	require.EqualValues(t, ((1<<20)-1)/4, limit)
	require.NoError(t, lease.GrowWireRequest(100<<10, 100<<10))
	require.EqualValues(t, 400<<10, policy.Reserved(), "growth charges only the increase")
	require.ErrorIs(t, lease.GrowWireRequest(300<<10, 300<<10), ErrLimitExceeded)
	require.EqualValues(t, 400<<10, policy.Reserved())
	now = now.Add(2 * time.Second)
	require.ErrorIs(t, lease.GrowWireRequest(1, 1), ErrStaleSample, "even a smaller forecast needs fresh telemetry")
	lease.Release()
	lease.Release()
	require.Zero(t, policy.Reserved())
	require.ErrorIs(t, lease.GrowWireRequest(1, 1), ErrLimitExceeded)
}

func TestWireReservationConcurrentGrowthAndRelease(t *testing.T) {
	policy := NewPolicy(1<<20, 0, time.Minute, func() (Sample, error) { return Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	for i := 0; i < 32; i++ {
		lease, err := policy.ReserveWireRequest(1, 1)
		require.NoError(t, err)
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); _ = lease.GrowWireRequest(100<<10, 100<<10) }()
		go func() { defer group.Done(); lease.Release() }()
		group.Wait()
		require.Zero(t, policy.Reserved())
	}
}
