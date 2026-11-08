package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The accepted owner retains its database and logical identity when it accepts a
// command, so a publication retry keeps the same key after native settlement has
// retired the prepared frame and released its authority.
func TestAcceptedIdentitySurvivesPreparedRetirement(t *testing.T) {
	b := newTestEngine(t)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("identity"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: "identity:a"})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	defer prepared.Abort()

	accepted, _, err := AcceptEntityCommit(context.Background(), b, prepared)
	require.NoError(t, err)
	namespace, logicalID := accepted.Identity()
	require.Equal(t, "identity", namespace)
	require.Equal(t, prepared.LogicalID(), logicalID)
	require.True(t, accepted.NativeFrameRetained())

	require.NoError(t, accepted.ApplyContext(context.Background(), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, accepted.native.Wait(ctx))
	namespace, logicalID = accepted.Identity()
	require.Equal(t, "identity", namespace)
	require.Equal(t, prepared.LogicalID(), logicalID)

	accepted.Terminal(nil)(CommitOutcome{State: CommitApplied})
	require.False(t, accepted.NativeFrameRetained())

	// The frame and its authority are gone; the accepted identity is not.
	namespace, logicalID = accepted.Identity()
	require.Equal(t, "identity", namespace)
	require.Equal(t, prepared.LogicalID(), logicalID)
	require.True(t, accepted.AppliesTo(b))
	node, err := b.GetStoredNode(t.Context(), "identity:a")
	require.NoError(t, err)
	require.Equal(t, NodeID("identity:a"), node.ID)
}
