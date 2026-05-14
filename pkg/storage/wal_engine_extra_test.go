package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWALEngine_GetInnerEngine_Delegation(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	// Engine returned must be the same instance we wrapped — callers
	// (e.g. nornicdb.DB) rely on this for downcasts to BadgerEngine.
	require.NotNil(t, w.GetInnerEngine())
	require.Same(t, Engine(base), w.GetInnerEngine())

	// Nil receiver returns nil rather than panicking — defensive
	// against partial-init.
	var nilWAL *WALEngine
	require.Nil(t, nilWAL.GetInnerEngine())
}

func TestWALEngine_Logger_FallsBackToDiscard(t *testing.T) {
	// Nil receiver / nil wal → returns the discard logger so callers
	// can always call .Info/Warn/etc. without nil-checking.
	var nilWAL *WALEngine
	require.NotNil(t, nilWAL.logger())

	w := &WALEngine{}
	require.NotNil(t, w.logger())
}

func TestWALEngine_TemporalDelegation(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	// IsCurrentTemporalNode delegates; with no temporal index the
	// underlying engine returns true (no temporal data ⇒ everything
	// "current"). The wrapper just plumbs that through.
	got, err := w.IsCurrentTemporalNode(&Node{ID: "test:n"}, time.Now())
	require.NoError(t, err)
	_ = got // either result is acceptable; what matters is no panic

	// RebuildTemporalIndexes delegates without error on a fresh store.
	require.NoError(t, w.RebuildTemporalIndexes(context.Background()))

	// PruneTemporalHistory returns 0 with no opts to clean up.
	count, err := w.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestWALEngine_LifecycleDelegation(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	// SetLifecycleSchedule delegates when the inner engine implements
	// MVCCLifecycleScheduleEngine. BadgerEngine does, so this should
	// not error even with no controller installed.
	require.NoError(t, w.SetLifecycleSchedule(5*time.Minute))

	// TopLifecycleDebtKeys delegates when implemented; with no
	// lifecycle controller installed BadgerEngine returns nil.
	require.Nil(t, w.TopLifecycleDebtKeys(10))
}

func TestWALEngine_NamespaceListing(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	// Empty store → no namespaces.
	require.Empty(t, w.ListNamespaces())

	// Adding a node populates the per-namespace count map; the
	// delegation must surface the namespace through the wrapper.
	_, err := base.CreateNode(&Node{
		ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	got := w.ListNamespaces()
	require.Contains(t, got, "test")
}

func TestWALEngine_MVCCHeadDelegation(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	_, err := base.CreateNode(&Node{
		ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{
		ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL",
	}))

	nodeHead, err := w.GetNodeCurrentHead("test:n")
	require.NoError(t, err)
	require.False(t, nodeHead.Tombstoned)
	require.False(t, nodeHead.Version.IsZero())

	edgeHead, err := w.GetEdgeCurrentHead("test:e")
	require.NoError(t, err)
	require.False(t, edgeHead.Tombstoned)
	require.False(t, edgeHead.Version.IsZero())
}

func TestWALEngine_LatestEffectiveAndVisible(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	_, err := base.CreateNode(&Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL",
	}))

	gotNode, err := w.GetNodeLatestEffective("test:n")
	require.NoError(t, err)
	require.NotNil(t, gotNode)
	require.Equal(t, NodeID("test:n"), gotNode.ID)

	gotEdge, err := w.GetEdgeLatestEffective("test:e")
	require.NoError(t, err)
	require.NotNil(t, gotEdge)
	require.Equal(t, EdgeID("test:e"), gotEdge.ID)
}
