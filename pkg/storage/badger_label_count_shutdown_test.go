package storage

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLabelCountReadinessRestoreClose(t *testing.T) {
	testLabelCountReadinessRestoreClose(t, false)
}

func TestLabelCountInNamespaceReadinessRestoreClose(t *testing.T) {
	testLabelCountReadinessRestoreClose(t, true)
}

// Restore invalidates the ready proof while holding its write barrier. A
// public count can reach the cache-miss probe before Close starts, then resume
// after Restore releases that barrier and Close has released the handle.
func testLabelCountReadinessRestoreClose(t *testing.T, scoped bool) {
	b := createTestBadgerEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:doc", Labels: []string{"Doc"}})
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "counts.backup")
	require.NoError(t, b.Backup(backup))
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)

	replaced, resumeRestore := make(chan struct{}), make(chan struct{})
	probe, resumeCount := make(chan struct{}), make(chan struct{})
	var replaceOnce, probeOnce, releaseRestore, releaseCount sync.Once
	defer releaseCount.Do(func() { close(resumeCount) })
	defer releaseRestore.Do(func() { close(resumeRestore) })
	restoreHook := setMaintenancePauseHook(func(point string) {
		switch point {
		case pauseLabelIndexStoreReplaced:
			replaceOnce.Do(func() { close(replaced); <-resumeRestore })
		case pauseLabelIndexReadinessProbe:
			probeOnce.Do(func() { close(probe); <-resumeCount })
		}
	})
	defer restoreHook()
	restored := runBackfillOperation(func() error { return b.Restore(backup) })
	awaitBackfillSignal(t, replaced)
	_, state, _, _, known := b.cachedLabelIndexReadiness()
	require.False(t, known)
	require.Equal(t, labelIndexReadinessUnknown, state)
	var count int64
	counted := runBackfillOperation(func() error {
		var err error
		if scoped {
			count, err = b.NodeCountByLabelInNamespace("test", "Doc")
		} else {
			count, err = b.NodeCountByLabel("Doc")
		}
		return err
	})
	awaitBackfillSignal(t, probe)
	closed := runBackfillOperation(b.Close)
	require.Eventually(t, func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return b.closing
	}, 10*time.Second, time.Millisecond)
	releaseRestore.Do(func() { close(resumeRestore) })
	restoreResult := awaitBackfillResult(t, restored)
	require.Nil(t, restoreResult.panicked, restoreResult.stack)
	require.NoError(t, restoreResult.err)
	closeResult := awaitBackfillResult(t, closed)
	require.Nil(t, closeResult.panicked, closeResult.stack)
	require.NoError(t, closeResult.err)
	releaseCount.Do(func() { close(resumeCount) })
	result := awaitBackfillResult(t, counted)
	require.Nil(t, result.panicked, "public label count panicked: %v\n%s", result.panicked, result.stack)
	if result.err != nil {
		require.ErrorIs(t, result.err, ErrStorageClosed)
	} else {
		require.Equal(t, int64(1), count)
	}
}
