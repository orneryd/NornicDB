// Plan 04-04-07 tests: cmd/nornicdb wiring smoke for StorageMetrics +
// MVCCMetrics + bytes_metrics_sweeper.
//
// We don't boot a full server here — the orchestrator unit-tests the
// adapter helpers directly. Full server smoke is deferred to manual
// /metrics curl per the plan goal_backward.
package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestUnwrapBadgerEngine_DirectReturn covers the direct-return path.
func TestUnwrapBadgerEngine_DirectReturn(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	got := unwrapBadgerEngine(be)
	require.Same(t, be, got, "direct *BadgerEngine should round-trip")
}

// TestUnwrapBadgerEngine_NamespacedWrap covers a NamespacedEngine wrap.
func TestUnwrapBadgerEngine_NamespacedWrap(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	ns := storage.NewNamespacedEngine(be, "test")
	got := unwrapBadgerEngine(ns)
	require.Same(t, be, got, "wrapped *BadgerEngine should be unwrapped")
}

// TestBadgerStorageProbe_RoutesAccessors verifies the probe adapter
// returns int64 values from the engine accessors.
func TestBadgerStorageProbe_RoutesAccessors(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	probe := badgerStorageProbe{be: be}
	require.Equal(t, int64(0), probe.NodeCount())
	require.Equal(t, int64(0), probe.EdgeCount())
}

// TestBadgerMVCCProbe_RoutesAccessors verifies the MVCC probe adapter
// routes to the RISK-2 accessors.
func TestBadgerMVCCProbe_RoutesAccessors(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	probe := badgerMVCCProbe{be: be}
	require.GreaterOrEqual(t, probe.PinnedBytes(), int64(0))
	require.GreaterOrEqual(t, probe.OldestReaderAgeSeconds(), float64(0))
	require.GreaterOrEqual(t, probe.ActiveReaders(), int64(0))
}
