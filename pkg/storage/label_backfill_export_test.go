package storage

import "testing"

// ReviewRemoveDerivedLabelIndex reuses the native fixture's established
// invalidation boundary for public Cypher tests in storage_test.
func ReviewRemoveDerivedLabelIndex(t *testing.T, b *BadgerEngine) {
	t.Helper()
	wipeLabelIndexAndMarker(t, b, prefixLabelIndex)
}

// ReviewPauseLabelBackfillChunks exposes only the existing maintenance seam
// to external-package public-path tests; it is absent from production builds.
func ReviewPauseLabelBackfillChunks(hook func()) func() {
	return setMaintenancePauseHook(func(point string) {
		if point == pauseIndexBackfillChunk {
			hook()
		}
	})
}

func ReviewLabelBackfillDone(t *testing.T, b *BadgerEngine) <-chan struct{} {
	t.Helper()
	worker := pendingLabelIndexBackfill(t, b)
	if worker == nil {
		return nil
	}
	return worker.done.Done()
}
