package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotCandidateSortPreparationCancellation(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, err := b.CreateNode(&Node{ID: "foreign:first"})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		_, err = b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:p%d", i))})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	// Exercise the supported logical snapshot branch of the public iterator.
	tx.mu.Lock()
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
		tx.snapshotTx = nil
	}
	tx.mu.Unlock()
	ctx := preparationContext(t, "sortIndexCandidatesWithContext", 3)
	seen := 0
	options := StreamNodesOptions{QueryNodes: true, Prefix: "test:"}
	err = tx.StreamNodesWithOptions(ctx, options, func(*Node) error { seen++; return nil })
	require.GreaterOrEqual(t, ctx.hits, 3, "cancel in candidate sorting after its admission check")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, seen)
	require.NoError(t, tx.StreamNodesWithOptions(context.Background(), options, func(*Node) error { seen++; return nil }))
	require.Equal(t, 20, seen)
}
