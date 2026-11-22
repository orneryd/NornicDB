package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestL9PhysicalPressureReclaimRunsBetweenSamples(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "physical_pressure"}[pressure], func(t *testing.T) {
			used := int64(100)
			if pressure {
				used = 950
			}
			var events []string
			policy := NewPolicy(1000, 100, time.Minute, func() (Sample, error) {
				events = append(events, "sample")
				return Sample{Used: used}, nil
			}, time.Now)
			policy.recovery.reclaim = func() {
				events = append(events, "reclaim")
				used = 100
			}
			require.NoError(t, policy.Refresh())
			if pressure {
				_, err := policy.Reserve(1)
				require.ErrorIs(t, err, ErrLimitExceeded)
			}
			events = nil
			require.NoError(t, policy.Refresh())
			t.Logf("refresh after pressure=%v: %v", pressure, events)
			if pressure {
				require.Equal(t, []string{"sample", "reclaim", "sample"}, events)
			} else {
				require.Equal(t, []string{"sample"}, events)
			}
			lease, err := policy.Reserve(1)
			require.NoError(t, err)
			lease.Release()
		})
	}
}
