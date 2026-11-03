package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdvanceImplicitSnapshotAfterBatch(t *testing.T) {
	engine := createTestBadgerEngine(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.GetNode(t.Context(), "test:child")
	require.ErrorIs(t, err, ErrNotFound)
	nodes, err := tx.GetNodesByLabel(context.Background(), "Child")
	require.NoError(t, err)
	require.Empty(t, nodes)
	peer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer peer.Rollback()
	require.NoError(t, peer.SetNamespace("test"))
	_, err = peer.CreateNode(t.Context(), &Node{ID: "test:child", Labels: []string{"Child"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, peer.Commit())
	_, err = tx.GetNode(t.Context(), "test:child")
	require.ErrorIs(t, err, ErrNotFound, "snapshot stays pinned until the batch boundary")
	require.NoError(t, tx.AdvanceImplicitSnapshot())
	node, err := tx.GetNode(t.Context(), "test:child")
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["v"])
	nodes, err = tx.GetNodesByLabel(context.Background(), "Child")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:child", Labels: []string{"Child"}, Properties: map[string]interface{}{"v": int64(2)}}, Policy: ClientNodeEdits}))
	require.ErrorIs(t, tx.AdvanceImplicitSnapshot(), ErrConflict)
	node, err = tx.GetNode(t.Context(), "test:child")
	require.NoError(t, err)
	require.Equal(t, int64(2), node.Properties["v"], "refused advancement preserves pending writes")
	require.NoError(t, tx.Rollback())
	require.Error(t, tx.AdvanceImplicitSnapshot())
	explicit, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer explicit.Rollback()
	require.ErrorIs(t, explicit.AdvanceImplicitSnapshot(), ErrConflict)
}
