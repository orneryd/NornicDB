package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A node or relationship suppressed on demand is hidden from reads with
// decay disabled, stays hidden across a reopen, is visible in a reveal scope,
// shows again when unsuppressed, and is forgotten when deleted, so one created
// again with its ID isn't hidden (#1064).
func TestManualSuppression(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "CITES"}))
	require.False(t, engine.HasManualSuppressions())
	require.False(t, engine.visibilityFiltered())

	require.ErrorIs(t, engine.SetNodeManuallySuppressed("test:missing", true), ErrNotFound)
	require.ErrorIs(t, engine.SetEdgeManuallySuppressed("test:missing", true), ErrNotFound)

	require.NoError(t, engine.SetNodeManuallySuppressed("test:b", true))
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:r", true))
	require.True(t, engine.HasManualSuppressions())
	require.True(t, engine.visibilityFiltered())
	hidden := func(t *testing.T, engine *BadgerEngine) {
		t.Helper()
		_, err := engine.GetNode("test:b")
		require.ErrorIs(t, err, ErrNotFound)
		docs, err := engine.GetNodesByLabel("Doc")
		require.NoError(t, err)
		require.Len(t, docs, 1)
		outgoing, err := engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Empty(t, outgoing)
	}
	hidden(t, engine)

	// Kept across a reopen.
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	hidden(t, engine)

	// Visible in a reveal scope.
	end := engine.BeginQueryRevealScope(true)
	_, err = engine.GetNode("test:b")
	require.NoError(t, err)
	require.False(t, engine.visibilityFiltered())
	end()

	// Shown again.
	require.NoError(t, engine.SetNodeManuallySuppressed("test:b", false))
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:r", false))
	require.False(t, engine.HasManualSuppressions())
	_, err = engine.GetNode("test:b")
	require.NoError(t, err)
	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)

	// Deleted, then created again with the same ID: not hidden.
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:r", true))
	require.NoError(t, engine.DeleteEdge("test:r"))
	require.False(t, engine.manuallySuppressed(manualSuppressionEdge, "test:r"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "CITES"}))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.NoError(t, engine.SetNodeManuallySuppressed("test:a", true))
	require.NoError(t, engine.DeleteNode("test:a"))
	require.False(t, engine.manuallySuppressed(manualSuppressionNode, "test:a"))
	// The key's delete runs on its own; a reopen doesn't bring it back.
	require.Eventually(t, func() bool {
		fresh := &BadgerEngine{}
		require.NoError(t, fresh.loadManualSuppressions(engine.db))
		return !fresh.HasManualSuppressions()
	}, time.Second, 10*time.Millisecond)
}

// The namespaced and WAL engines pass suppression to the stored engine, the
// namespaced one under its namespace's IDs.
func TestManualSuppressionThroughWrappers(t *testing.T) {
	base := createTestBadgerEngine(t)
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	namespaced := NewNamespacedEngine(NewWALEngine(base, wal), "ns")
	_, err = namespaced.CreateNode(&Node{ID: "a", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, namespaced.CreateEdge(&Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "CITES"}))

	var suppressor ManualSuppressionEngine = namespaced
	require.NoError(t, suppressor.SetNodeManuallySuppressed("b", true))
	require.NoError(t, suppressor.SetEdgeManuallySuppressed("r", true))
	require.True(t, base.manuallySuppressed(manualSuppressionNode, "ns:b"))
	require.True(t, base.manuallySuppressed(manualSuppressionEdge, "ns:r"))
	require.NoError(t, suppressor.SetNodeManuallySuppressed("b", false))
	require.NoError(t, suppressor.SetEdgeManuallySuppressed("r", false))
	require.False(t, base.HasManualSuppressions())

	// A namespace over an engine without suppression.
	require.ErrorIs(t, NewNamespacedEngine(nil, "ns").SetNodeManuallySuppressed("x", true), ErrNotImplemented)
	require.ErrorIs(t, NewNamespacedEngine(nil, "ns").SetEdgeManuallySuppressed("x", true), ErrNotImplemented)
}

// unsuppressibleEngine exposes only the Engine interface: no on-demand
// suppression.
type unsuppressibleEngine struct{ Engine }

// Engines without on-demand suppression under the WAL engine report
// ErrNotImplemented; a closed engine fails (#1064).
func TestManualSuppressionUnsupportedAndClosed(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = backing.Close() })
	memory := unsuppressibleEngine{Engine: backing}
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	walEngine := NewWALEngine(memory, wal)
	require.ErrorIs(t, walEngine.SetNodeManuallySuppressed("x", true), ErrNotImplemented)
	require.ErrorIs(t, walEngine.SetEdgeManuallySuppressed("x", true), ErrNotImplemented)

	closed, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	if closed != nil {
		fixtureowner.Own(t, closed)
	}
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	require.Error(t, closed.SetNodeManuallySuppressed("test:a", true))
}
