package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Falling access reports a decreasing trend and speeds decay, independent of
// how fast the test machine happens to run.
func TestDecreasingAccessTrend(t *testing.T) {
	t.Run("query load", func(t *testing.T) {
		qlp := NewQueryLoadPredictor(DefaultLoadConfig())
		qlp.qpsFilter.SetState(10, -100)
		require.Equal(t, "decreasing", qlp.GetPrediction().Trend)
	})

	t.Run("node access rate", func(t *testing.T) {
		tracker := NewTracker(DefaultConfig())
		tracker.RecordAccess("n1")
		tracker.nodes["n1"].intervalFilter.SetState(10, -1)
		_, trend := tracker.GetAccessRateTrend("n1")
		require.Equal(t, "decreasing", trend)
	})

	t.Run("decay multiplier", func(t *testing.T) {
		di := NewDecayIntegration(DefaultDecayIntegrationConfig())
		require.InDelta(t, di.config.RareAccessPenalty*0.75, di.calculateVelocityMultiplier(0.25, "decreasing"), 1e-9)
	})
}
