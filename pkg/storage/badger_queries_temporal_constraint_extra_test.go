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

func TestBadgerEngine_MaterializeAdjEdges_CacheMissAndDecodeError(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)

	e1 := &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "R"}
	e2 := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "R"}
	require.NoError(t, engine.CreateEdge(e1))
	require.NoError(t, engine.CreateEdge(e2))

	// Force one cache hit path.
	engine.cacheStoreEdge(e1)

	// Corrupt records fail the read; missing records are still skipped.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:e-corrupt"), []byte("bad-edge-bytes"))
	}))

	got, err := engine.materializeAdjEdges([]EdgeID{"test:e1", "test:e2", "test:e-missing", "test:e-corrupt"})
	require.Error(t, err)
	require.Nil(t, got)
	got, err = engine.materializeAdjEdges([]EdgeID{"test:e1", "test:e2", "test:e-missing"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	ids := map[EdgeID]struct{}{}
	for _, e := range got {
		ids[e.ID] = struct{}{}
	}
	_, ok1 := ids["test:e1"]
	_, ok2 := ids["test:e2"]
	require.True(t, ok1)
	require.True(t, ok2)
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
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
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

		prev, next, err := engine.temporalAdjacentNodesInTxn(txn, t1, base.Add(48*time.Hour), "")
		require.NoError(t, err)
		require.NotNil(t, prev)
		require.NotNil(t, next)

		// Excluding n2 should remove forward candidate.
		_, next, err = engine.temporalAdjacentNodesInTxn(txn, t1, base.Add(48*time.Hour), "test:n2")
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
