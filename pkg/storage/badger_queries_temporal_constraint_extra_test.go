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
