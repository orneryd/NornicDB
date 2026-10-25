package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectedDeletePreparationCancellation(t *testing.T) {
	b := createTestBadgerEngine(t)
	for i := 0; i < 12; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:delete%d", i))})
		require.NoError(t, err)
	}
	prepare := func() *BadgerTransaction {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		for i := 0; i < 12; i++ {
			err := tx.DeleteGraphNode(NodeID(fmt.Sprintf("test:delete%d", i)), RejectConnected)
			require.NoError(t, err)
		}
		return tx
	}
	ctx := preparationContext(t, "resolveConnectedDeletesLocked", 2)
	err := prepare().CommitContext(ctx)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	_, err = b.GetStoredNode("test:delete0")
	require.NoError(t, err, "canceled preparation cannot publish deletes")
	require.NoError(t, prepare().CommitContext(context.Background()))
	_, err = b.GetStoredNode("test:delete0")
	require.ErrorIs(t, err, ErrNotFound)
}
