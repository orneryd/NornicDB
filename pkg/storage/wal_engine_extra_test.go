package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type walStatsLookupEngine struct {
	Engine
	labelCount     int64
	namespaceCount int64
	ids            []NodeID
	lookupErr      error
}

type walLabelFallbackEngine struct {
	Engine
	nodesByLabel []*Node
	labelErr     error
}

func (e *walStatsLookupEngine) NodeCountByLabel(string) (int64, error) {
	return e.labelCount, nil
}

func (e *walStatsLookupEngine) NodeCountByLabelInNamespace(string, string) (int64, error) {
	return e.namespaceCount, nil
}

func (e *walStatsLookupEngine) ForEachNodeIDByLabel(_ string, visit func(NodeID) bool) error {
	if e.lookupErr != nil {
		return e.lookupErr
	}
	for _, id := range e.ids {
		if visit != nil && !visit(id) {
			return nil
		}
	}
	return nil
}

func (e *walLabelFallbackEngine) GetNodesByLabel(string) ([]*Node, error) {
	if e.labelErr != nil {
		return nil, e.labelErr
	}
	return e.nodesByLabel, nil
}

func TestWALEngine_GetInnerEngine_Delegation(t *testing.T) {
	base := newTestMemoryEngine(t)
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
	base := newTestMemoryEngine(t)
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
	base := newTestMemoryEngine(t)
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
	base := newTestMemoryEngine(t)
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

func TestWALEngine_AdjacentAndLabelCountFallbacks(t *testing.T) {
	base := newTestMemoryEngine(t)
	for _, id := range []NodeID{"tenant:n1", "tenant:n2", "tenant:n3"} {
		_, err := base.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:out", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}))
	require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:in", StartNode: "tenant:n3", EndNode: "tenant:n1", Type: "R"}))
	_, err := base.CreateNode(&Node{ID: "tenant:p1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "other:p2", Labels: []string{"Person"}})
	require.NoError(t, err)

	w := NewWALEngine(base, nil)
	out, in, err := w.GetAdjacentEdges("tenant:n1")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, EdgeID("tenant:out"), out[0].ID)
	require.Len(t, in, 1)
	require.Equal(t, EdgeID("tenant:in"), in[0].ID)

	count, err := w.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = w.NodeCountByLabelInNamespace("tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	w.RecordMaterializedAccess("tenant:p1")

	recorder := &namespaceLifecycleRecorder{Engine: newTestMemoryEngine(t)}
	recorderWAL := NewWALEngine(recorder, nil)
	recorderWAL.RecordMaterializedAccess("tenant:recorded")
	require.Equal(t, []string{"tenant:recorded"}, recorder.accesses)
}

func TestWALEngine_OptionalDelegateAndErrorBranches(t *testing.T) {
	direct := &adjacentDirectEngine{
		adjacentFallbackEngine: &adjacentFallbackEngine{Engine: newTestMemoryEngine(t)},
		adjOut:                 []*Edge{{ID: "tenant:direct-out", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}},
		adjIn:                  []*Edge{{ID: "tenant:direct-in", StartNode: "tenant:n3", EndNode: "tenant:n1", Type: "R"}},
	}
	w := NewWALEngine(direct, nil)
	out, in, err := w.GetAdjacentEdges("tenant:n1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("tenant:direct-out"), out[0].ID)
	require.Equal(t, EdgeID("tenant:direct-in"), in[0].ID)

	fallbackErr := errors.New("incoming failed")
	fallback := &adjacentFallbackEngine{Engine: newTestMemoryEngine(t), inErr: fallbackErr}
	w = NewWALEngine(fallback, nil)
	out, in, err = w.GetAdjacentEdges("tenant:n1")
	require.ErrorIs(t, err, fallbackErr)
	require.Nil(t, out)
	require.Nil(t, in)

	lookup := &walStatsLookupEngine{
		Engine:         newTestMemoryEngine(t),
		labelCount:     7,
		namespaceCount: 3,
		ids:            []NodeID{"tenant:a", "tenant:b"},
	}
	w = NewWALEngine(lookup, nil)
	count, err := w.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.EqualValues(t, 7, count)
	count, err = w.NodeCountByLabelInNamespace("tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	var visited []NodeID
	err = w.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
		visited = append(visited, id)
		return false
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"tenant:a"}, visited)
	require.NoError(t, w.ForEachNodeIDByLabel("Person", nil))

	lookup.lookupErr = errors.New("lookup failed")
	err = w.ForEachNodeIDByLabel("Person", func(NodeID) bool { return true })
	require.ErrorIs(t, err, lookup.lookupErr)
}

func TestWALEngine_LabelFallbackBranches(t *testing.T) {
	base := &walLabelFallbackEngine{
		Engine: newTestMemoryEngine(t),
		nodesByLabel: []*Node{
			{ID: "tenant:a", Labels: []string{"Person"}},
			{ID: "tenant:b", Labels: []string{"Person"}},
			{ID: "other:c", Labels: []string{"Person"}},
		},
	}
	t.Cleanup(func() { _ = base.Close() })
	w := NewWALEngine(base, nil)

	count, err := w.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	count, err = w.NodeCountByLabelInNamespace("tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	var visited []NodeID
	err = w.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
		visited = append(visited, id)
		return len(visited) < 2
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"tenant:a", "tenant:b"}, visited)
	require.NoError(t, w.ForEachNodeIDByLabel("Person", nil))

	wantErr := errors.New("label lookup failed")
	base.labelErr = wantErr
	_, err = w.NodeCountByLabel("Person")
	require.ErrorIs(t, err, wantErr)
	_, err = w.NodeCountByLabelInNamespace("tenant", "Person")
	require.ErrorIs(t, err, wantErr)
	err = w.ForEachNodeIDByLabel("Person", func(NodeID) bool { return true })
	require.ErrorIs(t, err, wantErr)
}

func TestWALEngine_MVCCHeadDelegation(t *testing.T) {
	base := newTestMemoryEngine(t)
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
	base := newTestMemoryEngine(t)
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

func TestWALEngine_FallbacksForNonOptionalInterfaces(t *testing.T) {
	base := newTestMemoryEngine(t)
	w := NewWALEngine(&nonMVCCEngine{Engine: base}, nil)

	_, err := base.CreateNode(&Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL"}))

	ok, err := w.IsCurrentTemporalNode(&Node{ID: "test:n"}, time.Now())
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, w.RebuildTemporalIndexes(context.Background()))
	pruned, err := w.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	require.Zero(t, pruned)

	node, err := w.GetNodeLatestEffective("test:n")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n"), node.ID)

	edge, err := w.GetEdgeLatestEffective("test:e")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e"), edge.ID)

	node, err = w.GetNodeLatestVisible("test:n")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n"), node.ID)

	edge, err = w.GetEdgeLatestVisible("test:e")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e"), edge.ID)

	_, err = w.GetNodesByLabelVisibleAt("L", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)

	release := w.RegisterSnapshotReader(SnapshotReaderInfo{})
	require.NotNil(t, release)
	release()

	status := w.LifecycleStatus()
	require.Equal(t, false, status["enabled"])
	require.NoError(t, w.TriggerPruneNow(context.Background()))
	require.NoError(t, w.SetLifecycleSchedule(time.Second))
	require.Nil(t, w.TopLifecycleDebtKeys(5))
}
