package storage

import "testing"

// HistoricalAmbiguousUpgradeFixtureForTesting exposes only the disposable
// fixture directory to external public-startup tests; no runtime API is added.
func HistoricalAmbiguousUpgradeFixtureForTesting(t *testing.T) string {
	t.Helper()
	return historicalAmbiguousRecords(t, false, historicalAmbiguousFillerBatch(t), historicalAmbiguousKilledBatch(t)).dir
}
