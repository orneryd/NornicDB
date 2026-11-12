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

func (e *walStatsLookupEngine) NodeCountByLabel(context.Context, string) (int64, error) {
	return e.labelCount, nil
}

func (e *walStatsLookupEngine) NodeCountByLabelInNamespace(context.Context, string, string) (int64, error) {
	return e.namespaceCount, nil
}

func (e *walStatsLookupEngine) ForEachNodeIDByLabel(ctx context.Context, _ string, visit func(NodeID) bool) error {
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

func (e *walLabelFallbackEngine) GetNodesByLabel(ctx context.Context, _ string) ([]*Node, error) {
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
	got, err := w.IsCurrentTemporalNode(context.Background(), &Node{ID: "test:n"}, time.Now())
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
	_, err := base.CreateNode(t.Context(), &Node{
		ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	got := w.ListNamespaces()
	require.Contains(t, got, "test")
}

func TestWALEngine_AdjacentAndLabelCountFallbacks(t *testing.T) {
	base := newTestMemoryEngine(t)
	for _, id := range []NodeID{"tenant:n1", "tenant:n2", "tenant:n3"} {
		_, err := base.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	require.NoError(t, base.CreateEdge(t.Context(), &Edge{ID: "tenant:out", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}))
	require.NoError(t, base.CreateEdge(t.Context(), &Edge{ID: "tenant:in", StartNode: "tenant:n3", EndNode: "tenant:n1", Type: "R"}))
	_, err := base.CreateNode(t.Context(), &Node{ID: "tenant:p1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), &Node{ID: "other:p2", Labels: []string{"Person"}})
	require.NoError(t, err)

	w := NewWALEngine(base, nil)
	both, err := w.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "tenant:n1"})
	require.NoError(t, err)
	require.Len(t, both, 2)
	require.Equal(t, EdgeID("tenant:out"), both[0].ID)
	require.Equal(t, EdgeID("tenant:in"), both[1].ID)

	count, err := w.NodeCountByLabel(t.Context(), "Person")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = w.NodeCountByLabelInNamespace(t.Context(), "tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	w.RecordMaterializedAccess("tenant:p1")

	recorder := &namespaceLifecycleRecorder{Engine: newTestMemoryEngine(t)}
	recorderWAL := NewWALEngine(recorder, nil)
	recorderWAL.RecordMaterializedAccess("tenant:recorded")
	require.Equal(t, []string{"tenant:recorded"}, recorder.accesses)
}

// walLabelCapabilityExpectation is the label-capability outcome one WAL
// substrate must report: counts from the delegating engines plus the fallback
// iteration prefix that remains after early stop.
type walLabelCapabilityExpectation struct {
	labelCount      int64
	namespaceCount  int64
	earlyStopVisits []NodeID
}

// assertWALLabelCapabilities asserts the label capabilities WALEngine delegates
// or falls back to, including the nil-visitor and early-stop iteration contract.
func assertWALLabelCapabilities(t *testing.T, w *WALEngine, want walLabelCapabilityExpectation) {
	t.Helper()
	count, err := w.NodeCountByLabel(t.Context(), "Person")
	require.NoError(t, err)
	require.EqualValues(t, want.labelCount, count)
	count, err = w.NodeCountByLabelInNamespace(t.Context(), "tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, want.namespaceCount, count)
	var visited []NodeID
	err = w.ForEachNodeIDByLabel(context.Background(), "Person", func(id NodeID) bool {
		visited = append(visited, id)
		return len(visited) < len(want.earlyStopVisits)
	})
	require.NoError(t, err)
	require.Equal(t, want.earlyStopVisits, visited)
	require.NoError(t, w.ForEachNodeIDByLabel(context.Background(), "Person", nil))
}

func TestWALEngine_OptionalDelegateAndErrorBranches(t *testing.T) {
	direct := &adjacentDirectEngine{
		adjacentFallbackEngine: &adjacentFallbackEngine{Engine: newTestMemoryEngine(t)},
		adjOut:                 []*Edge{{ID: "tenant:direct-out", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}},
		adjIn:                  []*Edge{{ID: "tenant:direct-in", StartNode: "tenant:n3", EndNode: "tenant:n1", Type: "R"}},
	}
	w := NewWALEngine(direct, nil)
	directBoth, err := w.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "tenant:n1"})
	require.NoError(t, err)
	require.Len(t, directBoth, 2)
	require.Equal(t, EdgeID("tenant:direct-out"), directBoth[0].ID)
	require.Equal(t, EdgeID("tenant:direct-in"), directBoth[1].ID)

	fallbackErr := errors.New("incoming failed")
	fallback := &adjacentFallbackEngine{Engine: newTestMemoryEngine(t), inErr: fallbackErr}
	w = NewWALEngine(fallback, nil)
	failedBoth, err := w.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "tenant:n1"})
	require.ErrorIs(t, err, fallbackErr)
	require.Nil(t, failedBoth)

	lookup := &walStatsLookupEngine{
		Engine:         newTestMemoryEngine(t),
		labelCount:     7,
		namespaceCount: 3,
		ids:            []NodeID{"tenant:a", "tenant:b"},
	}
	w = NewWALEngine(lookup, nil)
	assertWALLabelCapabilities(t, w, walLabelCapabilityExpectation{
		labelCount:      7,
		namespaceCount:  3,
		earlyStopVisits: []NodeID{"tenant:a"},
	})

	lookup.lookupErr = errors.New("lookup failed")
	err = w.ForEachNodeIDByLabel(context.Background(), "Person", func(NodeID) bool { return true })
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

	assertWALLabelCapabilities(t, w, walLabelCapabilityExpectation{
		labelCount:      3,
		namespaceCount:  2,
		earlyStopVisits: []NodeID{"tenant:a", "tenant:b"},
	})

	wantErr := errors.New("label lookup failed")
	base.labelErr = wantErr
	_, err := w.NodeCountByLabel(t.Context(), "Person")
	require.ErrorIs(t, err, wantErr)
	_, err = w.NodeCountByLabelInNamespace(t.Context(), "tenant", "Person")
	require.ErrorIs(t, err, wantErr)
	err = w.ForEachNodeIDByLabel(context.Background(), "Person", func(NodeID) bool { return true })
	require.ErrorIs(t, err, wantErr)
}

func TestWALEngine_MVCCHeadDelegation(t *testing.T) {
	base := newTestMemoryEngine(t)
	w := NewWALEngine(base, nil)

	_, err := base.CreateNode(t.Context(), &Node{
		ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), &Node{
		ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(t.Context(), &Edge{
		ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL",
	}))

	nodeHead, err := w.GetNodeCurrentHead(context.Background(), "test:n")
	require.NoError(t, err)
	require.False(t, nodeHead.Tombstoned)
	require.False(t, nodeHead.Version.IsZero())

	edgeHead, err := w.GetEdgeCurrentHead(context.Background(), "test:e")
	require.NoError(t, err)
	require.False(t, edgeHead.Tombstoned)
	require.False(t, edgeHead.Version.IsZero())
}

func TestWALEngine_LatestEffectiveAndVisible(t *testing.T) {
	base := newTestMemoryEngine(t)
	w := NewWALEngine(base, nil)

	_, err := base.CreateNode(t.Context(), &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), &Node{ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(t.Context(), &Edge{
		ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL",
	}))

	gotNode, err := w.GetNodeLatestEffective(context.Background(), "test:n")
	require.NoError(t, err)
	require.NotNil(t, gotNode)
	require.Equal(t, NodeID("test:n"), gotNode.ID)

	gotEdge, err := w.GetEdgeLatestEffective(context.Background(), "test:e")
	require.NoError(t, err)
	require.NotNil(t, gotEdge)
	require.Equal(t, EdgeID("test:e"), gotEdge.ID)
}

func TestWALEngine_FallbacksForNonOptionalInterfaces(t *testing.T) {
	base := newTestMemoryEngine(t)
	w := NewWALEngine(&nonMVCCEngine{Engine: base}, nil)

	_, err := base.CreateNode(t.Context(), &Node{ID: "test:n", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), &Node{ID: "test:m", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "REL"}))

	ok, err := w.IsCurrentTemporalNode(context.Background(), &Node{ID: "test:n"}, time.Now())
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, w.RebuildTemporalIndexes(context.Background()))
	pruned, err := w.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	require.Zero(t, pruned)

	node, err := w.GetNodeLatestEffective(context.Background(), "test:n")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n"), node.ID)

	edge, err := w.GetEdgeLatestEffective(context.Background(), "test:e")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e"), edge.ID)

	node, err = w.GetNodeLatestVisible(t.Context(), "test:n")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n"), node.ID)

	edge, err = w.GetEdgeLatestVisible(t.Context(), "test:e")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e"), edge.ID)

	_, err = w.GetNodesByLabelVisibleAt(context.Background(), "L", MVCCVersion{})
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
