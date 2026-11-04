// Author: #1683 implementation lane (Codex), 2026-11-01.
package memory

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetainedBudgetUsesCanonicalCapacity(t *testing.T) {
	policy := NewPolicy(1<<20, math.MaxInt64, time.Hour, func() (Sample, error) { return Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	var released atomic.Bool
	cache := RegisterRetainedCache(policy, func(Budget) { released.Store(true) })
	defer cache.Close()
	_, allowed := cache.Admission(1, true)
	require.False(t, allowed, "refill must not wrap a large configured admission headroom")
	require.True(t, released.Load())
	_, err := policy.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
}

func TestRetainedCopiesReleasedBeforeAdmission(t *testing.T) {
	for _, entry := range []string{"reserve", "wire_growth", "wire_limit"} {
		t.Run(entry, func(t *testing.T) {
			var used atomic.Int64
			used.Store(64 << 10)
			policy := NewPolicy(1<<20, 64<<10, time.Hour, func() (Sample, error) { return Sample{Used: used.Load()}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			var released atomic.Bool
			cache := RegisterRetainedCache(policy, func(budget Budget) {
				// This observation must be possible without a recursive admission
				// mutex: cache retirement is outside the policy's reservation lock.
				_ = policy.Reserved()
				if budget.Source == "application" {
					released.Store(true)
				}
			})
			defer cache.Close()
			lease, err := policy.ReserveWireRequest(1, 1024)
			require.NoError(t, err)
			defer lease.Release()
			switch entry {
			case "reserve":
				_, err = policy.Reserve(2 << 20)
			case "wire_growth":
				err = lease.GrowWireRequest(1<<20, 1<<20)
			case "wire_limit":
				used.Store(1 << 20)
				require.NoError(t, policy.Refresh())
				_, err = lease.WireLimit()
			}
			require.ErrorIs(t, err, ErrLimitExceeded)
			require.True(t, released.Load(), "another workload was denied while optional data was still installed")
		})
	}
}
