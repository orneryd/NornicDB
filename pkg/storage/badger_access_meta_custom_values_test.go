package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// TestAccessMetaCustomValuesRoundTripThroughFlush stores custom counters and
// timestamps through the real flusher into the Badger engine (on disk and in
// memory), reads them back, and flushes again on top of the stored value.
// The flusher merges onto a stored value only when it decodes as an int64,
// so every magnitude must come back as int64 and the second flush must add
// (counters) or keep the latest (timestamps) instead of resetting.
func TestAccessMetaCustomValuesRoundTripThroughFlush(t *testing.T) {
	backends := []struct {
		name string
		open func(t *testing.T) *BadgerEngine
	}{
		{"disk", func(t *testing.T) *BadgerEngine {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { engine.Close() })
			return engine
		}},
		{"memory", newTestEngine},
	}
	values := []int64{
		1, 127, 128, 255, 256, 32767, 32768, 65535, 65536,
		math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32, math.MaxUint32 + 1, math.MaxInt64 - 5,
		-1, -32, -33, -128, -129, -32768, -32769, math.MinInt32, math.MinInt32 - 1, math.MinInt64,
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			engine := backend.open(t)
			acc := knowledgepolicy.NewAccessAccumulator(true, 0)
			flusher := knowledgepolicy.NewAccessFlusher(acc, engine, time.Hour)
			for _, value := range values {
				id := fmt.Sprintf("custom-%d", value)

				acc.IncrementCustom(id, "views", value)
				acc.SetTimestamp(id, "seenAt", value)
				flusher.Flush()
				stored, err := engine.GetAccessMeta(id)
				require.NoError(t, err)
				require.NotNil(t, stored, "value %d", value)
				t.Logf("%d: counter decodes as %T, timestamp as %T", value, stored.Overflow["views"], stored.Overflow["seenAt"])
				require.IsType(t, int64(0), stored.Overflow["views"], "counter %d decoded as %T", value, stored.Overflow["views"])
				require.Equal(t, value, stored.Overflow["views"], "counter %d", value)
				if value > 0 {
					// SetTimestamp keeps only positive timestamps.
					require.IsType(t, int64(0), stored.Overflow["seenAt"], "timestamp %d decoded as %T", value, stored.Overflow["seenAt"])
					require.Equal(t, value, stored.Overflow["seenAt"], "timestamp %d", value)
				}

				acc.IncrementCustom(id, "views", 5)
				acc.SetTimestamp(id, "seenAt", 1)
				flusher.Flush()
				stored, err = engine.GetAccessMeta(id)
				require.NoError(t, err)
				require.Equal(t, value+5, stored.Overflow["views"], "counter %d after +5", value)
				wantSeen := value
				if value <= 0 {
					wantSeen = 1
				}
				require.Equal(t, wantSeen, stored.Overflow["seenAt"], "timestamp %d after an earlier one", value)
			}
		})
	}
}
