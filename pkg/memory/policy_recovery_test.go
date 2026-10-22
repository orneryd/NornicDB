package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPolicyPressureRecoveryKeepsLiveAndAncestorPressureClosed(t *testing.T) {
	for _, sample := range []Sample{
		{Used: 950, Ceiling: 1000},
		{Used: 100, Ceiling: 1000, HasHeadroom: true, Headroom: 50},
	} {
		now := time.Unix(100, 0)
		policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) { return sample, nil }, func() time.Time { return now })
		collections := 0
		var last time.Time
		policy.recovery.reclaim = func() {
			if collections > 0 {
				require.GreaterOrEqual(t, now.Sub(last), pressureRecoveryRetryInterval)
			}
			last = now
			collections++
		}
		require.NoError(t, policy.Refresh())
		for i := 0; i < 100; i++ {
			_, err := policy.Reserve(1)
			require.ErrorIs(t, err, ErrLimitExceeded)
			now = now.Add(pressureRecoveryInterval)
			require.NoError(t, policy.Refresh())
		}
		require.Equal(t, 17, collections, "sustained pressure permits only one unleased attempt per thirty seconds")
		require.Zero(t, policy.Reserved())
	}
}

func TestPolicyPressureRecoveryRearmsAfterCompletedWorkAndCooldown(t *testing.T) {
	now := time.Unix(100, 0)
	used := int64(100)
	policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) { return Sample{Used: used}, nil }, func() time.Time { return now })
	collections := 0
	policy.recovery.reclaim = func() {
		collections++
		if collections == 2 {
			used = 100
		}
	}
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(200)
	require.NoError(t, err)
	used = 950
	require.NoError(t, policy.Refresh())
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, policy.Refresh())
	require.Equal(t, 1, collections)
	work.Release()
	work.Release()
	now = now.Add(pressureRecoveryInterval - time.Millisecond)
	require.NoError(t, policy.Refresh())
	require.Equal(t, 1, collections)
	now = now.Add(time.Millisecond)
	require.NoError(t, policy.Refresh())
	require.Equal(t, 2, collections)
	lease, err := policy.Reserve(500)
	require.NoError(t, err)
	lease.Release()
}

func TestPolicyPressureRecoveryDoesNotHoldAdmissionLockOrOverlap(t *testing.T) {
	var used, clockNanos atomic.Int64
	used.Store(950)
	clockNanos.Store(time.Unix(100, 0).UnixNano())
	policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) { return Sample{Used: used.Load()}, nil },
		func() time.Time { return time.Unix(0, clockNanos.Load()) })
	started, finish := make(chan struct{}), make(chan struct{})
	var collections atomic.Int32
	policy.recovery.reclaim = func() {
		collections.Add(1)
		close(started)
		<-finish
		used.Store(100)
	}
	require.NoError(t, policy.Refresh())
	_, err := policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	done := make(chan error, 1)
	go func() { done <- policy.Refresh() }()
	<-started
	clockNanos.Add(int64(2 * pressureRecoveryRetryInterval))
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			require.NoError(t, policy.Refresh())
			_, err := policy.Reserve(1)
			require.ErrorIs(t, err, ErrLimitExceeded)
		}()
	}
	group.Wait()
	require.EqualValues(t, 1, collections.Load())
	close(finish)
	require.NoError(t, <-done)
	lease, err := policy.Reserve(500)
	require.NoError(t, err)
	lease.Release()
}

func TestPolicyPressureRecoveryRequiresFreshPhysicalProof(t *testing.T) {
	fail := false
	policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) {
		if fail {
			return Sample{}, errors.New("unreadable cgroup")
		}
		return Sample{Used: 950}, nil
	}, time.Now)
	policy.recovery.reclaim = func() { fail = true }
	require.NoError(t, policy.Refresh())
	_, err := policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.ErrorIs(t, policy.Refresh(), ErrStaleSample)
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrStaleSample)
}

func TestPolicyDoesNotCollectForForecastOnlyRejection(t *testing.T) {
	policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) { return Sample{Used: 100}, nil }, time.Now)
	policy.recovery.reclaim = func() { t.Fatal("forecast rejection does not establish physical pressure") }
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(800)
	require.NoError(t, err)
	_, err = policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, policy.Refresh())
	work.Release()
	_, err = policy.Reserve(1 << 30)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, policy.Refresh())
}
