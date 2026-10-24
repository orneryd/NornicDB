package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheMaintenanceIssuesBeforePublication(t *testing.T) {
	oracle := newCommitOracle(12)
	for _, restored := range []uint64{50, 3} {
		before := oracle.published.Load()
		var issued uint64
		oracle.advanceTo(restored, func(ts uint64) {
			require.Equal(t, before, oracle.published.Load(), "eligibility precedes visibility")
			require.Greater(t, ts, before, "old backup versions cannot reuse eligibility")
			require.Greater(t, ts, restored)
			issued = ts
		})
		require.Equal(t, issued, oracle.published.Load())
		ts, err := oracle.assign()
		require.NoError(t, err)
		require.Greater(t, ts, issued, "the next ordinary commit must retain eligibility")
		oracle.finish(ts)
	}
}
