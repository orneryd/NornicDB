package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelAfterInitialCheckContext struct {
	context.Context
	checks int
}

func (c *cancelAfterInitialCheckContext) Err() error {
	c.checks++
	if c.checks > 1 {
		return context.Canceled
	}
	return nil
}

func TestCleanShutdownMarkerIsConsumedByNextStartup(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	engine, err := NewBadgerEngine(directory)
	require.NoError(t, err)

	clean, err := engine.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.False(t, clean)
	require.NoError(t, engine.MarkCleanShutdown(context.Background()))
	require.NoError(t, engine.Close())

	restarted, err := NewBadgerEngine(directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	clean, err = restarted.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.True(t, clean)

	clean, err = restarted.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.False(t, clean)
}

func TestCanceledMaintenanceMarkerOperationsLeaveStateConservative(t *testing.T) {
	engine := createTestBadgerEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clean, err := engine.ConsumeCleanShutdownMarker(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, clean)
	require.ErrorIs(t, engine.MarkCleanShutdown(ctx), context.Canceled)
}

func TestMaintenanceMarkerAcceptsNilContextAndRejectsClosedStorage(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	require.NoError(t, engine.MarkCleanShutdown(nil))
	clean, err := engine.ConsumeCleanShutdownMarker(nil)
	require.NoError(t, err)
	require.True(t, clean)
	require.NoError(t, engine.Close())

	clean, err = engine.ConsumeCleanShutdownMarker(context.Background())
	require.Error(t, err)
	require.False(t, clean)
	require.Error(t, engine.MarkCleanShutdown(context.Background()))
}

func TestMaintenanceMarkerHonorsCancellationAtStorageBoundary(t *testing.T) {
	engine := createTestBadgerEngine(t)
	consumeCtx := &cancelAfterInitialCheckContext{Context: context.Background()}
	clean, err := engine.ConsumeCleanShutdownMarker(consumeCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, clean)
	markCtx := &cancelAfterInitialCheckContext{Context: context.Background()}
	require.ErrorIs(t, engine.MarkCleanShutdown(markCtx), context.Canceled)
}

func TestCleanShutdownMarkerTraversesWALStorageStack(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	badgerEngine, err := NewBadgerEngine(directory)
	require.NoError(t, err)
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(directory, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	stack := NewWALEngine(badgerEngine, wal)
	_, err = stack.CreateNode(&Node{ID: "nornic:pending", Labels: []string{"Document"}})
	require.NoError(t, err)
	require.NoError(t, stack.MarkCleanShutdown(context.Background()))
	require.NoError(t, stack.Close())

	restarted, err := NewBadgerEngine(directory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	clean, err := restarted.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.True(t, clean)
	_, err = restarted.GetNode("nornic:pending")
	require.NoError(t, err)
}
