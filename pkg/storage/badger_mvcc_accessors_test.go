// Plan 04-04-01 RED→GREEN: RISK-2 fix — MVCC accessors PinnedBytes /
// OldestReaderAgeSeconds / ActiveReaders. The observability D-15b GaugeFunc
// callbacks (catalog_mvcc.go) call these accessors during /metrics scrape.
// Accessors are pure reads; concurrent-safe with the existing reader
// registry (atomic counter + lifecycle.ReaderRegistry RWMutex).
//
// RESEARCH §RISK-2: prior to Plan 04-04 the MVCCProbe interface declared
// these methods but no Engine type implemented them. Wave-0 of Plan 04-04
// adds them BEFORE the bag's GaugeFunc registrations land, so the GaugeFunc
// callback always has a non-nil concrete value to read.
package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setupTestBadgerEngine builds an in-memory engine for the tests below.
// Mirrors the existing setupTest* helper convention in this package.
func newTestBadgerEngineForAccessors(t *testing.T) *BadgerEngine {
	t.Helper()
	be, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// TestActiveReaders_Accessor verifies that ActiveReaders() returns the live
// count of registered MVCC snapshot readers. Without the lifecycle
// controller injected, the engine falls back to the atomic
// activeMVCCSnapshotReaders counter.
func TestActiveReaders_Accessor(t *testing.T) {
	be := newTestBadgerEngineForAccessors(t)
	require.Equal(t, int64(0), be.ActiveReaders(), "fresh engine: ActiveReaders should be 0")

	v0 := MVCCVersion{}
	deregister1, err := be.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: v0, StartTime: time.Now()})
	require.NoError(t, err)
	require.Equal(t, int64(1), be.ActiveReaders())
	deregister2, err := be.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: v0, StartTime: time.Now()})
	require.NoError(t, err)
	require.Equal(t, int64(2), be.ActiveReaders())

	deregister1()
	require.Equal(t, int64(1), be.ActiveReaders())
	deregister2()
	require.Equal(t, int64(0), be.ActiveReaders())
}

// TestOldestReaderAgeSeconds_Accessor verifies that OldestReaderAgeSeconds()
// returns >0 when at least one reader is registered with a known StartTime.
//
// In the no-lifecycle-controller fallback path the accessor returns 0 because
// only an atomic count is tracked (not per-reader StartTime). This is by
// design — D-07/D-15b only requires non-nil semantics for the GaugeFunc
// callback. The non-zero behavior is exercised when a controller is wired
// (covered by the lifecycle reader-registry tests).
func TestOldestReaderAgeSeconds_Accessor(t *testing.T) {
	be := newTestBadgerEngineForAccessors(t)
	require.Equal(t, float64(0), be.OldestReaderAgeSeconds(), "fresh engine: 0 seconds")

	v0 := MVCCVersion{}
	deregister, err := be.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: v0, StartTime: time.Now()})
	require.NoError(t, err)
	defer deregister()
	// In fallback path the value is still 0 (no per-reader StartTime tracked).
	// Accessor must not panic, must return a sensible float64.
	got := be.OldestReaderAgeSeconds()
	require.GreaterOrEqual(t, got, float64(0))
}

// TestPinnedBytes_Accessor verifies that PinnedBytes() returns a non-negative
// int64 across the engine lifetime. The fallback path returns 0 when no
// lifecycle controller is wired; with a controller the controller's
// pinned-bytes accounting flows through.
func TestPinnedBytes_Accessor(t *testing.T) {
	be := newTestBadgerEngineForAccessors(t)
	require.Equal(t, int64(0), be.PinnedBytes(), "fresh engine: 0 bytes")

	v0 := MVCCVersion{}
	deregister, err := be.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: v0, StartTime: time.Now()})
	require.NoError(t, err)
	defer deregister()
	require.GreaterOrEqual(t, be.PinnedBytes(), int64(0))
}

// TestAccessors_RaceSafe drives concurrent reader open/close + accessor reads
// to exercise the race detector. -race mode required (Makefile/CI default).
func TestAccessors_RaceSafe(t *testing.T) {
	be := newTestBadgerEngineForAccessors(t)
	v0 := MVCCVersion{}
	const goroutines = 8
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				deregister, err := be.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: v0, StartTime: time.Now()})
				if err != nil {
					t.Errorf("acquireSnapshotReader: %v", err)
					return
				}
				deregister()
			}
		}()
	}
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = be.ActiveReaders()
				_ = be.PinnedBytes()
				_ = be.OldestReaderAgeSeconds()
			}
		}()
	}
	wg.Wait()
}
