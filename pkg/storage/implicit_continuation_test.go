package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImplicitContinuationPreservesWritesAndTokens(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[rollback], func(t *testing.T) {
			dir := t.TempDir()
			engine, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("continue"))
			require.NoError(t, tx.SetImplicit(true))
			_, err = tx.CreateNode(&Node{ID: "continue:before", Labels: []string{"Outer"}, Properties: map[string]interface{}{"before_token": "before"}})
			require.NoError(t, err)
			// Populate the old completed-label cache before the separate commit.
			_, err = tx.GetNodesByLabel("Batch")
			require.NoError(t, err)
			batch, err := engine.BeginTransaction()
			require.NoError(t, err)
			_, err = batch.CreateNode(&Node{ID: "continue:batch", Labels: []string{"Batch"}, Properties: map[string]interface{}{"batch_token": "batch"}})
			require.NoError(t, err)
			require.NoError(t, batch.Commit())
			require.NoError(t, tx.ResumeAfterIndependentCommit())
			rows, err := tx.GetNodesByLabel("Batch")
			require.NoError(t, err)
			require.Len(t, rows, 1)
			_, err = tx.GetNode("continue:before")
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "continue:after", Labels: []string{"Outer"}, Properties: map[string]interface{}{"after_token": "after"}})
			require.NoError(t, err)
			require.NoError(t, tx.CreateEdge(&Edge{ID: "continue:edge", StartNode: "continue:before", EndNode: "continue:after", Type: "OUTER", Properties: map[string]interface{}{"edge_token": "edge"}}))
			if rollback {
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, tx.Commit())
			}
			require.NoError(t, engine.Close())
			engine, err = NewBadgerEngine(dir)
			require.NoError(t, err)
			defer engine.Close()
			batchNode, err := engine.GetNode("continue:batch")
			require.NoError(t, err)
			require.Equal(t, "batch", batchNode.Properties["batch_token"])
			for _, id := range []NodeID{"continue:before", "continue:after"} {
				node, err := engine.GetNode(id)
				if rollback {
					require.ErrorIs(t, err, ErrNotFound)
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, node.Properties)
				}
			}
			edge, err := engine.GetEdge("continue:edge")
			if rollback {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, "edge", edge.Properties["edge_token"])
			}
		})
	}
}

func TestImplicitContinuationRetainsConflictChecks(t *testing.T) {
	for _, beforeResume := range []bool{false, true} {
		t.Run(map[bool]string{false: "after-resume", true: "before-resume"}[beforeResume], func(t *testing.T) {
			engine := NewMemoryEngine()
			defer engine.Close()
			_, err := engine.CreateNode(&Node{ID: "continue:same", Properties: map[string]interface{}{"value": int64(0)}})
			require.NoError(t, err)
			outer, err := engine.BeginTransaction()
			require.NoError(t, err)
			defer outer.Rollback()
			require.NoError(t, outer.SetNamespace("continue"))
			require.NoError(t, outer.SetImplicit(true))
			node, err := outer.GetNode("continue:same")
			require.NoError(t, err)
			node.Properties["value"] = int64(1)
			require.NoError(t, outer.UpdateNode(node))
			if !beforeResume {
				require.NoError(t, outer.ResumeAfterIndependentCommit())
			}
			peer, err := engine.BeginTransaction()
			require.NoError(t, err)
			node, err = peer.GetNode("continue:same")
			require.NoError(t, err)
			node.Properties["value"] = int64(2)
			require.NoError(t, peer.UpdateNode(node))
			require.NoError(t, peer.Commit())
			if beforeResume {
				require.ErrorIs(t, outer.ResumeAfterIndependentCommit(), ErrConflict)
			} else {
				require.ErrorIs(t, outer.Commit(), ErrConflict)
			}
			node, err = engine.GetNode("continue:same")
			require.NoError(t, err)
			require.Equal(t, int64(2), node.Properties["value"])
		})
	}
}
