package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-31. A prepared commit decides its
// relationship endpoints before its durable decision, whatever branch its
// mode selects. With write-behind on, an endpoint deleted by a peer commit
// acknowledged into the buffer before the decision rejects the prepared commit
// as a conflict before any accepted command exists, and nothing is stored:
// the accepted application, which never re-checks endpoints, is never asked
// to publish a relationship without its endpoint.
func TestPreparedCommitDecidesEndpointsBeforeAcceptance(t *testing.T) {
	for _, mode := range []EntityCommitMode{CommitModeImplicit, CommitModeDirect, CommitModeExplicit} {
		t.Run(fmt.Sprintf("mode=%v", mode), func(t *testing.T) {
			e := newOwnerTestEngine(t, true)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := e.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, e.FlushWriteBehind(nil))
			tx, err := BeginEntityTransaction(e, mode)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
			// The endpoint delete is acknowledged into the buffer after staging.
			peer, err := BeginEntityTransaction(e, CommitModeImplicit)
			require.NoError(t, err)
			require.NoError(t, peer.DeleteGraphNode("test:b", CascadeEdges))
			require.NoError(t, peer.Commit())
			require.NotZero(t, e.writeBehind.PendingOps(), "the delete must still be buffered")
			prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "endpoint-decision"})
			if prepared != nil {
				prepared.Abort()
			}
			require.ErrorIs(t, err, ErrConflict, "the endpoint conflict rejects the commit before its durable decision")
			require.Nil(t, prepared, "no accepted command may exist for a rejected commit")
			require.NoError(t, e.FlushWriteBehind(nil))
			_, err = e.GetEdge("test:r")
			require.ErrorIs(t, err, ErrNotFound, "nothing was stored")
		})
	}
}
