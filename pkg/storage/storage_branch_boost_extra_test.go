package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type asyncErrQueryEngine struct {
	*MemoryEngine
	err error
}

func (e *asyncErrQueryEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) NodeCountByLabel(label string) (int64, error) {
	return 0, e.err
}

func (e *asyncErrQueryEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	return 0, e.err
}

func TestAsyncEngine_LowBranchHelpers(t *testing.T) {
	base := NewMemoryEngine()
	errEng := &asyncErrQueryEngine{MemoryEngine: base, err: ErrStorageClosed}
	ae := NewAsyncEngine(errEng, &AsyncEngineConfig{FlushInterval: time.Hour, TargetFlushSize: 1000})
	defer func() { _ = ae.Close() }()

	require.Nil(t, ae.GetEdgeBetween("test:a", "test:b", "R"))
	require.Equal(t, 0, ae.GetInDegree("test:a"))
	require.Equal(t, 0, ae.GetOutDegree("test:a"))

	count, err := ae.NodeCountByLabel("User")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Equal(t, int64(0), count)
	count, err = ae.NodeCountByLabelInNamespace("test", "User")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Equal(t, int64(0), count)

	require.False(t, (FlushResult{}).isStorageClosedOnly())
	require.True(t, (FlushResult{NodesFailed: 1, FirstNodeError: ErrStorageClosed.Error()}).isStorageClosedOnly())
	require.True(t, (FlushResult{EdgesFailed: 1, FirstEdgeError: "wrap: " + ErrStorageClosed.Error()}).isStorageClosedOnly())
	require.True(t, (FlushResult{DeletesFailed: 1, FirstDeleteError: "x " + ErrStorageClosed.Error()}).isStorageClosedOnly())
	require.False(t, (FlushResult{NodesFailed: 1, FirstNodeError: "other error"}).isStorageClosedOnly())
}

func TestBadgerQueries_EdgeFromTxn_ErrorBranches(t *testing.T) {
	engine := newTestEngine(t)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.edgeFromTxn(txn, "test:missing")
		require.Error(t, err)
		return nil
	}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:bad"), []byte("not-a-valid-edge"))
	}))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.edgeFromTxn(txn, "test:bad")
		require.Error(t, err)
		return nil
	}))

	require.NoError(t, engine.selfHealEdgeBetweenIndexes([]*Edge{
		nil,
		{ID: "test:e-no-op", StartNode: "test:a", EndNode: "test:b", Type: "R"},
	}))
}

func TestTemporalCurrentNode_Branches(t *testing.T) {
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

	asOf := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ok, err := engine.IsCurrentTemporalNode(&Node{ID: "unprefixed", Labels: []string{"Role"}}, asOf)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = engine.IsCurrentTemporalNode(nil, asOf)
	require.NoError(t, err)
	require.False(t, ok)

	nodeNoTemporal := &Node{ID: "test:n0", Labels: []string{"Role"}, Properties: map[string]any{"x": 1}}
	ok, err = engine.IsCurrentTemporalNodeInNamespace(ns, nodeNoTemporal, asOf)
	require.NoError(t, err)
	require.True(t, ok) // matched=false branch

	n1 := &Node{ID: "test:n1", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": asOf.Add(-2 * time.Hour), "valid_to": asOf.Add(2 * time.Hour)}}
	n2 := &Node{ID: "test:n2", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": asOf.Add(3 * time.Hour), "valid_to": asOf.Add(4 * time.Hour)}}
	_, err = engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine.RebuildTemporalIndexes(nil))

	ok, err = engine.IsCurrentTemporalNodeInNamespace(ns, n1, asOf)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = engine.IsCurrentTemporalNodeInNamespace(ns, n2, asOf)
	require.NoError(t, err)
	require.False(t, ok)
}
