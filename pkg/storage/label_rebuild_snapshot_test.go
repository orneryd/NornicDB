package storage

import (
	"context"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A completed index is still part of a pinned transaction's snapshot when a
// later maintenance rebuild replaces its entries. Rebuilding must not erase
// the older physical versions that transaction still needs.
func TestLabelRebuildPreservesReadyTransactionSnapshot(t *testing.T) {
	engine, err := fixture.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"ReadySnapshot"}})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	before, err := tx.GetNodesByLabel(context.Background(), "ReadySnapshot")
	require.NoError(t, err)
	require.Len(t, before, 3)
	processed, err := engine.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, processed)
	after, err := tx.GetNodesByLabel(context.Background(), "ReadySnapshot")
	require.NoError(t, err)
	require.ElementsMatch(t, before, after, "rebuild preserves the ready snapshot's complete rows")
}
