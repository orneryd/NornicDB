package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedCommitOwnerSurvivesPreparationWaitCancellation(t *testing.T) {
	b := newTestEngine(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:accepted-owner"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	p, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	// An accepted command's owner may decide Applied after its caller stops
	// waiting. The native completion API must not reuse that waiter's context.
	cancel()
	require.NoError(t, p.Commit(nil))
	node, err := b.GetStoredNode("test:accepted-owner")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:accepted-owner"), node.ID)
}
