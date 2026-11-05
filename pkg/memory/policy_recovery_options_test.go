package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPolicyCustomRecoveryTimings(t *testing.T) {
	now := time.Unix(100, 0)
	used := int64(100)
	p := NewPolicy(1000, 100, time.Minute, func() (Sample, error) {
		return Sample{Used: used}, nil
	}, func() time.Time { return now }, WithRecoveryTimings(7*time.Millisecond, 11*time.Millisecond))
	collections := 0
	p.recovery.reclaim = func() { collections++ }
	require.NoError(t, p.Refresh())
	lease, err := p.Reserve(1)
	require.NoError(t, err)
	used = 950
	require.NoError(t, p.Refresh())
	_, err = p.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.NoError(t, p.Refresh())
	require.Equal(t, 1, collections)
	lease.Release()
	now = now.Add(7*time.Millisecond - time.Nanosecond)
	require.NoError(t, p.Refresh())
	require.Equal(t, 1, collections)
	now = now.Add(time.Nanosecond)
	require.NoError(t, p.Refresh())
	require.Equal(t, 2, collections, "released work uses the custom minimum interval")
	now = now.Add(11*time.Millisecond - time.Nanosecond)
	require.NoError(t, p.Refresh())
	require.Equal(t, 2, collections)
	now = now.Add(time.Nanosecond)
	_, err = p.Reserve(1)
	require.ErrorIs(t, err, ErrLimitExceeded)
	require.Equal(t, 2, collections, "requests still never collect")
	require.NoError(t, p.Refresh())
	require.Equal(t, 3, collections, "unleased work uses the custom rearm duration")
	other := NewPolicy(0, 0, time.Minute, nil, nil)
	require.Equal(t, pressureRecoveryInterval, other.recovery.interval, "options belong to one policy")
	for _, values := range [][2]time.Duration{{0, time.Second}, {time.Second, time.Millisecond}} {
		require.Panics(t, func() { WithRecoveryTimings(values[0], values[1]) })
	}
}
