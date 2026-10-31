package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Whether an accepted application
// re-checks relationship endpoints is one decision, whatever commit branch its
// mode selects: the write-behind buffered branch (implicit) and the direct
// branch (direct, explicit) agree. An ordinary implicit commit still fails on
// the same concurrent endpoint delete.
func TestAcceptedApplicationEndpointDecisionIsBranchIndependent(t *testing.T) {
	commit := func(t *testing.T, mode EntityCommitMode, accepted bool) error {
		e := newOwnerTestEngine(t, true)
		for _, id := range []NodeID{"test:a", "test:b"} {
			_, err := e.CreateNode(&Node{ID: id})
			require.NoError(t, err)
		}
		require.NoError(t, e.FlushWriteBehind(nil))
		tx, err := BeginEntityTransaction(e, mode)
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
		// The endpoint delete is acknowledged into the buffer after staging.
		peer, err := BeginEntityTransaction(e, CommitModeImplicit)
		require.NoError(t, err)
		require.NoError(t, peer.DeleteGraphNode("test:b", CascadeEdges))
		require.NoError(t, peer.Commit())
		require.NotZero(t, e.writeBehind.PendingOps(), "the delete must still be buffered")
		tx.acceptedApplication = accepted
		return tx.Commit()
	}
	outcomes := map[EntityCommitMode]error{}
	for _, mode := range []EntityCommitMode{CommitModeImplicit, CommitModeDirect, CommitModeExplicit} {
		outcomes[mode] = commit(t, mode, true)
	}
	require.NoError(t, outcomes[CommitModeImplicit], "buffered branch")
	require.NoError(t, outcomes[CommitModeDirect], "direct branch")
	require.NoError(t, outcomes[CommitModeExplicit], "explicit branch")
	require.ErrorIs(t, commit(t, CommitModeImplicit, false), ErrConflict, "ordinary commits keep endpoint revalidation")
}
