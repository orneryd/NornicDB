package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTemporalHelperFunctions(t *testing.T) {
	constraint := Constraint{Type: ConstraintTemporal, Label: "Role", Properties: []string{"entity_id", "valid_from", "valid_to"}}
	now := time.Now().UTC()

	node := &Node{ID: "tenant:n1", Labels: []string{"Role"}, Properties: map[string]any{"entity_id": "u1", "valid_from": now.Add(-time.Hour), "valid_to": now.Add(time.Hour)}}
	key, start, end, hasEnd, ok := temporalNodeState(node, constraint)
	require.True(t, ok)
	require.Equal(t, "u1", key)
	require.True(t, hasEnd)
	require.Equal(t, now.Add(-time.Hour).Unix(), start.Unix())
	require.Equal(t, now.Add(time.Hour).Unix(), end.Unix())

	_, _, _, _, ok = temporalNodeState(nil, constraint)
	require.False(t, ok)
	_, _, _, _, ok = temporalNodeState(node, Constraint{Properties: []string{"only", "two"}})
	require.False(t, ok)

	target, start, ok := temporalTargetForNode("tenant", node, constraint)
	require.True(t, ok)
	require.Equal(t, now.Add(-time.Hour).Unix(), start.Unix())
	require.NotEmpty(t, temporalTargetMapKey(target))

	m := map[string]temporalRefreshTarget{}
	mergeTemporalTargets(m, target)
	require.Len(t, m, 1)
	mergeTemporalTargets(nil, target)

	historyKey := temporalHistoryKey(target.desc, start, node.ID)
	extracted := extractNodeIDFromTemporalHistoryKey(historyKey, len(temporalHistoryPrefix(target.desc)))
	require.Equal(t, node.ID, extracted)
	require.Equal(t, NodeID(""), extractNodeIDFromTemporalHistoryKey([]byte{1, 2}, 10))

	require.True(t, nodeMatchesTemporalLookup(node, constraint, "u1", now))
	require.False(t, nodeMatchesTemporalLookup(node, constraint, "u2", now))
	require.False(t, nodeMatchesTemporalLookup(node, constraint, "u1", now.Add(-2*time.Hour)))

	require.Equal(t, NodeID("tenant:abc"), qualifyTemporalNodeID("tenant", "abc"))
	require.Equal(t, NodeID("tenant:abc"), qualifyTemporalNodeID("tenant", "tenant:abc"))
	require.Equal(t, NodeID("abc"), qualifyTemporalNodeID("", "abc"))
}

func TestTemporalClearBadgerPrefix_ContextCancel(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte{prefixTemporalIndex, 0x01}, []byte("x"))
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := engine.clearBadgerPrefix(ctx, prefixTemporalIndex)
	require.Error(t, err)
}

func TestRebuildTemporalNodeInTxn_Branches(t *testing.T) {
	engine := newTestEngine(t)
	ns := "tenant"
	sm := engine.GetSchemaForNamespace(ns)
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "temporal_role",
		Type:       ConstraintTemporal,
		Label:      "Role",
		EntityType: ConstraintEntityNode,
		Properties: []string{"entity_id", "valid_from", "valid_to"},
	}))

	now := time.Now().UTC()
	older := &Node{ID: "tenant:old", Labels: []string{"Role"}, Properties: map[string]any{"entity_id": "u1", "valid_from": now.Add(-3 * time.Hour), "valid_to": now.Add(-time.Hour)}}
	newer := &Node{ID: "tenant:new", Labels: []string{"Role"}, Properties: map[string]any{"entity_id": "u1", "valid_from": now.Add(-30 * time.Minute), "valid_to": nil}}
	_, err := engine.CreateNode(older)
	require.NoError(t, err)
	_, err = engine.CreateNode(newer)
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.rebuildTemporalNodeInTxn(txn, nil, now))
		require.NoError(t, engine.rebuildTemporalNodeInTxn(txn, &Node{ID: "unprefixed", Labels: []string{"Role"}}, now))
		require.NoError(t, engine.rebuildTemporalNodeInTxn(txn, older, now))
		require.NoError(t, engine.rebuildTemporalNodeInTxn(txn, newer, now))
		return nil
	}))

	current, err := engine.GetTemporalNodeAsOfInNamespace(ns, "Role", "entity_id", "u1", "valid_from", "valid_to", now)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Equal(t, NodeID("tenant:new"), current.ID)
}
