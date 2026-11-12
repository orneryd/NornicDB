package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type asyncErrQueryEngine struct {
	*MemoryEngine
	err error
}

func (e *asyncErrQueryEngine) ReadAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) GetNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return nil, e.err
}

func (e *asyncErrQueryEngine) NodeCountByLabel(ctx context.Context, label string) (int64, error) {
	return 0, e.err
}

func (e *asyncErrQueryEngine) NodeCountByLabelInNamespace(ctx context.Context, namespace, label string) (int64, error) {
	return 0, e.err
}

func TestWALEngine_LowBranchHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	errEng := &asyncErrQueryEngine{MemoryEngine: base, err: ErrStorageClosed}
	wal := NewWALEngine(errEng, nil)

	edge, err := wal.GetEdgeBetween(t.Context(), "test:a", "test:b", "R")
	require.NoError(t, err)
	require.Nil(t, edge)
	degree, err := wal.GetInDegree(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, 0, degree)
	degree, err = wal.GetOutDegree(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, 0, degree)

	count, err := wal.NodeCountByLabel(t.Context(), "User")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Equal(t, int64(0), count)
	count, err = wal.NodeCountByLabelInNamespace(t.Context(), "test", "User")
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Equal(t, int64(0), count)
}

func TestBadgerQueries_EdgeFromTxn_ErrorBranches(t *testing.T) {
	engine := newTestEngine(t)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.edgeFromTxn(txn, "test:missing")
		require.Error(t, err)
		return nil
	}))

	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:bad"), []byte("not-a-valid-edge"))
	}))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.edgeFromTxn(txn, "test:bad")
		require.Error(t, err)
		return nil
	}))

	// Author: storage-format-owners lane (Codex), 1 November 2026.
	require.NoError(t, engine.selfHealEdgeBetweenIndexes(t.Context(), []*Edge{
		nil,
		{ID: "test:e-no-op", StartNode: "test:a", EndNode: "test:b", Type: "R"},
	}))
}

func TestTemporalCurrentNode_Branches(t *testing.T) {
	engine := newTemporalConstraintEngine(t, "test", "Role", "role", "valid_from", "valid_to")
	ns := "test"

	asOf := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ok, err := engine.IsCurrentTemporalNode(context.Background(), &Node{ID: "unprefixed", Labels: []string{"Role"}}, asOf)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = engine.IsCurrentTemporalNode(context.Background(), nil, asOf)
	require.NoError(t, err)
	require.False(t, ok)

	nodeNoTemporal := &Node{ID: "test:n0", Labels: []string{"Role"}, Properties: map[string]any{"x": 1}}
	ok, err = engine.IsCurrentTemporalNodeInNamespace(context.Background(), ns, nodeNoTemporal, asOf)
	require.NoError(t, err)
	require.True(t, ok) // matched=false branch

	n1 := &Node{ID: "test:n1", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": asOf.Add(-2 * time.Hour), "valid_to": asOf.Add(2 * time.Hour)}}
	n2 := &Node{ID: "test:n2", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": asOf.Add(3 * time.Hour), "valid_to": asOf.Add(4 * time.Hour)}}
	_, err = engine.CreateNode(t.Context(), n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), n2)
	require.NoError(t, err)
	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))

	ok, err = engine.IsCurrentTemporalNodeInNamespace(context.Background(), ns, n1, asOf)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = engine.IsCurrentTemporalNodeInNamespace(context.Background(), ns, n2, asOf)
	require.NoError(t, err)
	require.False(t, ok)
}
