package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntityWriterRejectsCrossNamespaceLock(t *testing.T) {
	engine := newTestMemoryEngine(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.LockNodeForWrite(t.Context(), "other:node")
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
	_, err = tx.LockEdgeForWrite(t.Context(), "other:edge")
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
	require.Empty(t, tx.entityLocks.held)
	require.Zero(t, tx.OperationCount())
}

func TestEntityWriterRefreshesAtSaturatedCommitSequence(t *testing.T) {
	engine := newTestMemoryEngine(t)
	ctx := t.Context()
	_, err := engine.CreateNode(ctx, &Node{ID: "test:n", Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	state, err := engine.namespaceMVCC("test")
	require.NoError(t, err)
	state.seq.Store(maxMVCCCommitSequence)
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.Equal(t, uint64(maxMVCCCommitSequence), tx.readTS.CommitSequence)
	node, err := engine.GetStoredNode(ctx, "test:n")
	require.NoError(t, err)
	node.Properties["v"] = int64(2)
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	latest, err := tx.LockNodeForWrite(ctx, "test:n")
	require.NoError(t, err)
	require.NotNil(t, latest, "equal saturated sequences do not prove an unchanged entity")
	require.Equal(t, int64(2), latest.Properties["v"])
}
