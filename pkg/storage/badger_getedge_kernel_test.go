package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/stretchr/testify/require"
)

// TestBadgerEngine_GetEdgeKernel_DirectAndVisibleAgree pins the shared
// readEdgeBodyInTxn core: the direct read (GetEdge) and the MVCC current-head
// branch (GetEdgeVisibleAt at the head version) must return the identical
// edge, and missing IDs must behave identically on both paths.
func TestBadgerEngine_GetEdgeKernel_DirectAndVisibleAgree(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	start := NodeID(prefixTestID("gek-start"))
	end := NodeID(prefixTestID("gek-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("gek-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 1}}))
	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	direct, err := engine.GetEdge(edgeID)
	require.NoError(t, err)
	visible, err := engine.GetEdgeVisibleAt(edgeID, head.Version)
	require.NoError(t, err)
	require.Equal(t, direct, visible, "direct and visible-at reads must share the same decoded body")

	// A missing edge behaves identically on both paths.
	_, err = engine.GetEdge(EdgeID(prefixTestID("gek-missing")))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeVisibleAt(EdgeID(prefixTestID("gek-missing")), head.Version)
	require.ErrorIs(t, err, ErrNotFound)

	// Historical resolution still goes through the MVCC record path after the
	// current head has moved on.
	require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 2}}}))
	old, err := engine.GetEdgeVisibleAt(edgeID, head.Version)
	require.NoError(t, err)
	require.EqualValues(t, 1, old.Properties["weight"])
}

// BenchmarkBadger_GetEdge_DirectRead pins the direct edge read (decode +
// decay filter) after the shared-kernel extraction.
func BenchmarkBadger_GetEdge_DirectRead(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	}))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = engine.Close() }()

	start := NodeID(prefixTestID("gek-bench-start"))
	end := NodeID(prefixTestID("gek-bench-end"))
	if _, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}}); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}}); err != nil {
		b.Fatal(err)
	}
	edgeID := EdgeID(prefixTestID("gek-bench-edge"))
	if err := engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "KNOWS", Properties: map[string]any{"weight": 1, "note": "bench"}}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edge, err := engine.GetEdge(edgeID)
		if err != nil {
			b.Fatal(err)
		}
		if edge == nil {
			b.Fatal("nil edge")
		}
	}
}
