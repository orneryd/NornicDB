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

	require.ErrorIs(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:missing", true), ErrNotFound)
	require.ErrorIs(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:missing", true), ErrNotFound)

	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:b", true))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:r", true))
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
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:b", false))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:r", false))
	require.False(t, engine.HasManualSuppressions())
	_, err = engine.GetNode("test:b")
	require.NoError(t, err)
	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)

	// Deleted, then created again with the same ID: not hidden.
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:r", true))
	require.NoError(t, engine.DeleteEdge("test:r"))
	require.False(t, engine.manuallySuppressed(manualSuppressionEdge, "test:r"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "CITES"}))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:a", true))
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

	sequenceBefore := wal.sequence.Load()
	var suppressor ManualSuppressionEngine = namespaced
	require.NoError(t, suppressor.SetManualSuppression(StoredEntityNodeKind, "b", true))
	require.NoError(t, suppressor.SetManualSuppression(StoredEntityEdgeKind, "r", true))
	require.True(t, base.manuallySuppressed(manualSuppressionNode, "ns:b"))
	require.True(t, base.manuallySuppressed(manualSuppressionEdge, "ns:r"))
	require.NoError(t, suppressor.SetManualSuppression(StoredEntityNodeKind, "b", false))
	require.NoError(t, suppressor.SetManualSuppression(StoredEntityEdgeKind, "r", false))
	require.False(t, base.HasManualSuppressions())
	require.Equal(t, sequenceBefore, wal.sequence.Load(), "manual metadata must not append graph WAL records")

	// A namespace over an engine without suppression.
	require.ErrorIs(t, NewNamespacedEngine(nil, "ns").SetManualSuppression(StoredEntityNodeKind, "x", true), ErrNotImplemented)
	require.ErrorIs(t, NewNamespacedEngine(nil, "ns").SetManualSuppression(StoredEntityEdgeKind, "x", true), ErrNotImplemented)
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
	require.ErrorIs(t, walEngine.SetManualSuppression(StoredEntityNodeKind, "x", true), ErrNotImplemented)
	require.ErrorIs(t, walEngine.SetManualSuppression(StoredEntityEdgeKind, "x", true), ErrNotImplemented)

	closed, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	if closed != nil {
		fixtureowner.Own(t, closed)
	}
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	require.Error(t, closed.SetManualSuppression(StoredEntityNodeKind, "test:a", true))
}

// Both public proxies preserve their actual wrapped capability and the existing
// namespace qualification policy; invalid enum values never reach that owner.
func TestManualSuppressionKindAndNamespacePolicy(t *testing.T) {
	for _, namespace := range []string{"ns", ""} {
		for _, kind := range []StoredEntityKind{StoredEntityNodeKind, StoredEntityEdgeKind} {
			for _, id := range []string{"x", "ns:x", "other:x"} {
				recorder := &manualSuppressionRecorder{}
				scoped := NewNamespacedEngine(recorder, namespace)
				require.NoError(t, scoped.SetManualSuppression(kind, id, true))
				require.Equal(t, qualifyEntityID(namespace+":", id), recorder.id)
				require.Equal(t, kind, recorder.kind)
				require.True(t, recorder.suppressed)
				require.Equal(t, 1, recorder.calls)
				require.ErrorIs(t, scoped.SetManualSuppression(StoredEntityKind(255), id, false), ErrInvalidData)
				require.Equal(t, 1, recorder.calls)
			}
		}
	}
	recorder := &manualSuppressionRecorder{}
	wrapped := &WALEngine{engine: recorder}
	require.NoError(t, wrapped.SetManualSuppression(StoredEntityEdgeKind, "other:x", true))
	require.Equal(t, "other:x", recorder.id)
	require.ErrorIs(t, wrapped.SetManualSuppression(StoredEntityKind(255), "x", false), ErrInvalidData)
	require.Equal(t, 1, recorder.calls)
	base := createTestBadgerEngine(t)
	_, err := base.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	require.ErrorIs(t, base.SetManualSuppression(StoredEntityKind(255), "test:a", true), ErrInvalidData)
	require.False(t, base.HasManualSuppressions())
	_, err = base.GetNode("test:a")
	require.NoError(t, err)
	for _, tc := range []struct {
		kind      StoredEntityKind
		persisted byte
	}{{StoredEntityNodeKind, 'n'}, {StoredEntityEdgeKind, 'e'}} {
		actual, err := manualSuppressionKind(tc.kind)
		require.NoError(t, err)
		require.Equal(t, tc.persisted, actual)
		require.Equal(t, []byte{prefixMVCCMeta, prefixMVCCMetaManualSuppression, tc.persisted, 'x'}, manualSuppressionKey(actual, "x"))
	}
}

type manualSuppressionRecorder struct {
	Engine
	kind       StoredEntityKind
	id         string
	suppressed bool
	calls      int
}

func (r *manualSuppressionRecorder) SetManualSuppression(kind StoredEntityKind, id string, suppressed bool) error {
	r.kind, r.id, r.suppressed = kind, id, suppressed
	r.calls++
	return nil
}
