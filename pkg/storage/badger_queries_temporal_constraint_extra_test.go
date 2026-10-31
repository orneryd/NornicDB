package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestCompareValues_MixedAndFallbackBranches(t *testing.T) {
	tests := []struct {
		a, b any
		ok   bool
	}{
		{int(1), int64(1), true},
		{int64(2), float64(2), true},
		{float64(3), int(3), true},
		{"x", "x", true},
		{true, true, true},
		{nil, nil, true},
		{nil, 1, false},
		{[]int{1, 2}, []int{1, 2}, true},  // DeepEqual branch (non-comparable)
		{[]int{1, 2}, []int{2, 1}, false}, // DeepEqual false
		{map[string]int{"a": 1}, map[string]int{"a": 1}, true},
		{"1", 1, false},
	}
	for _, tc := range tests {
		require.Equal(t, tc.ok, compareValues(tc.a, tc.b))
	}
}

func TestBadgerEngine_TemporalIndexHelpers_ExtraBranches(t *testing.T) {
	engine := newTestEngine(t)
	ns := "test"
	sm := engine.GetSchemaForNamespace(ns)
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "temporal_role",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityNode,
		Label:      "Role",
		Properties: []string{"role", "valid_from", "valid_to"},
	}))

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n1 := &Node{ID: "test:n1", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": base, "valid_to": base.Add(24 * time.Hour)}}
	n2 := &Node{ID: "test:n2", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": base.Add(48 * time.Hour), "valid_to": base.Add(72 * time.Hour)}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	c := sm.GetConstraintsForLabels([]string{"Role"})[0]
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		// writeTemporalIndexForNodeInTxn !ok branch.
		target, err := engine.writeTemporalIndexForNodeInTxn(txn, ns, &Node{ID: "test:bad", Labels: []string{"Role"}, Properties: map[string]any{}}, c)
		require.NoError(t, err)
		require.Equal(t, "", target.constraint.Name)

		// deleteTemporalIndexForNodeInTxn !ok branch.
		target, err = engine.deleteTemporalIndexForNodeInTxn(txn, ns, &Node{ID: "test:bad", Labels: []string{"Role"}, Properties: map[string]any{}}, c)
		require.NoError(t, err)
		require.Equal(t, "", target.constraint.Name)

		t1, err := engine.writeTemporalIndexForNodeInTxn(txn, ns, n1, c)
		require.NoError(t, err)
		_, err = engine.writeTemporalIndexForNodeInTxn(txn, ns, n2, c)
		require.NoError(t, err)

		// Insert malformed key under temporal prefix to hit nodeID=="" skip branch.
		p := temporalHistoryPrefix(t1.desc)
		require.NoError(t, txn.Set(append([]byte{}, p...), []byte{}))

		prev, next, err := engine.visitTemporalHistoryNodeInTxn(txn, t1, temporalHistorySelection{at: base.Add(48 * time.Hour), adjacent: true})
		require.NoError(t, err)
		require.NotNil(t, prev)
		require.NotNil(t, next)

		// Excluding n2 should remove forward candidate.
		_, next, err = engine.visitTemporalHistoryNodeInTxn(txn, t1, temporalHistorySelection{at: base.Add(48 * time.Hour), excludeNodeID: "test:n2", adjacent: true})
		require.NoError(t, err)
		require.Nil(t, next)
		return nil
	}))
}

func TestBadgerEngine_RebuildTemporalIndexes_ClosedAndCanceled(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.Close())
	require.Error(t, engine.RebuildTemporalIndexes(context.Background()))

	engine2 := newTestEngine(t)
	_, err := engine2.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = engine2.RebuildTemporalIndexes(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// The adjacency read's indexed edge lookup: a cached body inside the read's
// view answers; one from a later source does not; a missing record is skipped;
// a corrupt record fails the read and is not admitted to the cache.
func TestBadgerEngine_ReadIndexedEdge_SourceViewMissAndDecodeError(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	stored := &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"from": "store"}}
	require.NoError(t, engine.CreateEdge(stored))
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:e-corrupt"), []byte("bad-edge-bytes"))
	}))

	cachedBody := copyEdge(stored)
	cachedBody.Properties = map[string]any{"from": "cache"}
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		gen := engine.edgeCacheGen.current()
		readTs := txn.ReadTs()

		// A body from a source after this view must not answer the read.
		engine.cacheDeleteEdge("test:e1")
		engine.cacheStoreEdge(cachedBody, readTs+1)
		later, err := engine.readIndexedEdgeInTxn(txn, "test:e1", gen, nil)
		require.NoError(t, err)
		require.Equal(t, "store", later.Properties["from"], "a later source must fall back to this view's record")

		// A body from inside this view answers, as the caller's own copy.
		engine.cacheDeleteEdge("test:e1")
		engine.cacheStoreEdge(cachedBody, readTs)
		hit, err := engine.readIndexedEdgeInTxn(txn, "test:e1", engine.edgeCacheGen.current(), nil)
		require.NoError(t, err)
		require.Equal(t, "cache", hit.Properties["from"])
		hit.Properties["from"] = "changed"
		again, err := engine.readIndexedEdgeInTxn(txn, "test:e1", engine.edgeCacheGen.current(), nil)
		require.NoError(t, err)
		require.Equal(t, "cache", again.Properties["from"], "a hit must not share the cached body")

		missing, err := engine.readIndexedEdgeInTxn(txn, "test:e-missing", engine.edgeCacheGen.current(), nil)
		require.NoError(t, err)
		require.Nil(t, missing)

		_, err = engine.readIndexedEdgeInTxn(txn, "test:e-corrupt", engine.edgeCacheGen.current(), nil)
		require.Error(t, err)
		_, admitted := engine.cacheLoadEdge("test:e-corrupt", edgeCacheRead{readTs: readTs})
		require.False(t, admitted)
		return nil
	}))
}
