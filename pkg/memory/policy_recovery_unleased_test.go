package memory

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Regression for the review's slow-collector/unleased-pressure state machine:
// no admitted work exists to release a lease after the first reclamation.
func TestPolicyPressureRecoveryRearmsWithoutReleasedLease(t *testing.T) {
	var used, clockNanos atomic.Int64
	var collections atomic.Int32
	used.Store(950)
	start := time.Unix(100, 0)
	clockNanos.Store(start.UnixNano())
	now := func() time.Time { return time.Unix(0, clockNanos.Load()) }
	policy := NewPolicy(1000, 100, 2*time.Second, func() (Sample, error) {
		return Sample{Used: used.Load()}, nil
	}, now)
	started, finish := make(chan struct{}), make(chan struct{})
	policy.recovery.reclaim = func() {
		if collections.Add(1) == 1 {
			close(started)
			<-finish
		}
		used.Store(100)
	}
	require.NoError(t, policy.Refresh())
	_, err := policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	done := make(chan error, 1)
	go func() { done <- policy.Refresh() }()
	<-started
	clockNanos.Add(int64(3 * time.Second))
	request := make(chan error, 1)
	go func() { _, err := policy.Reserve(1); request <- err }()
	select {
	case err = <-request:
		require.ErrorIs(t, err, ErrStaleSample)
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("Reserve waited for the sampler reclamation")
	}
	close(finish)
	require.NoError(t, <-done)

	// A direct read/search/background task allocates without a Policy lease.
	// Repeated fresh samples and rejected requests must not bypass the cooldown.
	used.Store(950)
	for elapsed := 5 * time.Second; elapsed < 30*time.Second; elapsed += 5 * time.Second {
		clockNanos.Store(start.Add(elapsed).UnixNano())
		require.NoError(t, policy.Refresh())
		for i := 0; i < 100; i++ {
			_, err = policy.Reserve(1)
			require.ErrorIs(t, err, ErrLimitExceeded)
		}
		require.EqualValues(t, 1, collections.Load())
	}
	clockNanos.Store(start.Add(30*time.Second - time.Nanosecond).UnixNano())
	require.NoError(t, policy.Refresh())
	require.EqualValues(t, 1, collections.Load())
	clockNanos.Add(1)
	// Even at the boundary, requests cannot invoke the collector themselves.
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.EqualValues(t, 1, collections.Load())
	require.NoError(t, policy.Refresh())
	require.EqualValues(t, 2, collections.Load(), "unleased pressure must rearm after thirty seconds")
	policy.mu.Lock()
	released := policy.recovery.released
	policy.mu.Unlock()
	require.Zero(t, released, "the second collection must not depend on an admitted lease")
	lease, err := policy.Reserve(1)
	require.NoError(t, err, "only the post-collection physical sample reopens admission")
	lease.Release()
	require.Zero(t, policy.Reserved())
}

func TestPolicyPressureRecoveryUnleasedRetryRequiresFreshPressure(t *testing.T) {
	now := time.Unix(100, 0)
	sample := Sample{Used: 950}
	var sampleErr error
	policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) {
		return sample, sampleErr
	}, func() time.Time { return now })
	collections := 0
	policy.recovery.reclaim = func() { collections++ }
	require.NoError(t, policy.Refresh())
	_, err := policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, policy.Refresh())
	require.Equal(t, 1, collections)

	now = now.Add(30 * time.Second)
	sampleErr = errors.New("unreadable cgroup")
	require.ErrorIs(t, policy.Refresh(), ErrStaleSample)
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrStaleSample)
	require.Equal(t, 1, collections, "elapsed time cannot replace a fresh physical sample")
	sampleErr = nil
	sample.Used = 100
	require.NoError(t, policy.Refresh())
	_, err = policy.Reserve(1 << 30)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, policy.Refresh())
	require.Equal(t, 1, collections, "forecast-only rejection cannot use the timed allowance")
	sample.Used = 950
	require.NoError(t, policy.Refresh())
	require.Equal(t, 2, collections)
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded, "an unsuccessful reclamation must keep live pressure closed")
}
