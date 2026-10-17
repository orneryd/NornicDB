package inference

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The background recluster OnNodeUpdate starts is single-flight: while one is
// pending or running, further updates past the threshold don't start
// another; once it has finished, the next one does.
func TestOnNodeUpdateStartsOneBackgroundRecluster(t *testing.T) {
	cfg := testClusterConfig()
	cfg.AutoRecluster = true
	ci := NewClusterIntegration(testGPUManager(), cfg, testKMeansConfig(3), testEmbConfig())
	for i := 0; i < 20; i++ {
		require.NoError(t, ci.AddEmbedding(fmt.Sprintf("n-%d", i), generateRandomEmbedding(64)))
	}
	require.NoError(t, ci.OnIndexComplete())
	require.True(t, ci.IsClustered())

	// A recluster is pending: updates (100% of the nodes, past the 10%
	// threshold) must not start another one.
	require.True(t, ci.reclusterPending.CompareAndSwap(false, true))
	for i := 0; i < 20; i++ {
		require.NoError(t, ci.OnNodeUpdate(fmt.Sprintf("n-%d", i), generateRandomEmbedding(64)))
	}
	time.Sleep(100 * time.Millisecond) // a wrongly started recluster would finish by now
	require.True(t, ci.ShouldRecluster(), "no recluster may run while one is pending")

	// Once it has finished, the next update starts exactly one, which
	// clears the pending flag and the update backlog when it's done.
	ci.reclusterPending.Store(false)
	require.NoError(t, ci.OnNodeUpdate("n-0", generateRandomEmbedding(64)))
	require.Eventually(t, func() bool { return !ci.reclusterPending.Load() && !ci.ShouldRecluster() },
		30*time.Second, 5*time.Millisecond, "the update must start a background recluster that runs and finishes")
}
