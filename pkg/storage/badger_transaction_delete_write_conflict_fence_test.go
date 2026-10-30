// SPDX-License-Identifier: MIT
package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A delete conflicts with a peer that writes the same node or relationship
// after the delete's snapshot validation and before its commit, as an update
// does (TestTransactionNodeUpdatePreservesPostValidationPeerWrite): the
// delete fails with ErrConflict and the peer's write is kept. Without it,
// both commit and the peer's acknowledged write is lost.
func TestTransactionDeletePreservesPostValidationPeerWrite(t *testing.T) {
	for _, highPerformance := range []bool{false, true} {
		name := map[bool]string{false: "low_memory", true: "default_server"}[highPerformance]
		for _, target := range []string{"node", "connected_node", "relationship"} {
			t.Run(name+"/"+target, func(t *testing.T) {
				barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
				engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), HighPerformance: highPerformance, LowMemory: !highPerformance, Logger: slog.New(barrier)}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })

				nodeID := NodeID("test:delete-conflict")
				_, err = engine.CreateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"writer": "seed"}})
				require.NoError(t, err)
				otherID := NodeID("test:delete-conflict-other")
				_, err = engine.CreateNode(&Node{ID: otherID})
				require.NoError(t, err)
				edgeID := EdgeID("test:delete-conflict-edge")
				require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "R", Properties: map[string]interface{}{"writer": "seed"}}))

				deleter, err := engine.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = deleter.Rollback() })
				switch target {
				case "node":
					require.NoError(t, deleter.DeleteEdge(edgeID))
					require.NoError(t, deleter.DeleteGraphNode(nodeID, CascadeEdges))
				case "connected_node":
					require.NoError(t, deleter.DeleteGraphNode(nodeID, RejectConnected))
					require.NoError(t, deleter.DeleteEdge(edgeID))
				case "relationship":
					require.NoError(t, deleter.DeleteEdge(edgeID))
				}
				require.NoError(t, deleter.SetMetadata(map[string]interface{}{"validation_publication_control": true}))
				barrier.target = deleter.ID
				var release sync.Once
				unblock := func() { release.Do(func() { close(barrier.resume) }) }
				t.Cleanup(unblock)

				committed := make(chan error, 1)
				go func() { committed <- deleter.Commit() }()
				select {
				case <-barrier.reached:
				case err := <-committed:
					t.Fatalf("delete finished before the validation barrier: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("delete did not reach the post-validation barrier")
				}

				peer, err := engine.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = peer.Rollback() })
				if target == "relationship" {
					edge, err := peer.GetEdge(edgeID)
					require.NoError(t, err)
					edge.Properties["writer"] = "peer"
					require.NoError(t, peer.UpdateEdge(EdgeUpdateRequest{After: edge}))
				} else {
					node, err := peer.GetNode(nodeID)
					require.NoError(t, err)
					node.Properties["writer"] = "peer"
					require.NoError(t, peer.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
				}
				require.NoError(t, peer.Commit())
				unblock()

				select {
				case err := <-committed:
					require.ErrorIs(t, err, ErrConflict)
				case <-time.After(5 * time.Second):
					t.Fatal("delete did not finish")
				}
				if target == "relationship" {
					edge, err := engine.GetEdge(edgeID)
					require.NoError(t, err)
					require.Equal(t, "peer", edge.Properties["writer"])
				} else {
					node, err := engine.GetNode(nodeID)
					require.NoError(t, err)
					require.Equal(t, "peer", node.Properties["writer"])
				}
			})
		}
	}
}

// An update or delete whose committed key can't be added to the conflict set
// (here the Badger transaction is gone) fails with that read's error, and
// nothing is buffered.
func TestTransactionWriteFailsWhenConflictTrackingReadFails(t *testing.T) {
	for _, op := range []string{"update node", "delete node", "delete connected node", "update relationship", "delete relationship"} {
		t.Run(op, func(t *testing.T) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			nodeID, otherID, edgeID := NodeID("test:tracking"), NodeID("test:tracking-other"), EdgeID("test:tracking-edge")
			_, err = engine.CreateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"v": 1}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&Node{ID: otherID})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "R", Properties: map[string]interface{}{"v": 1}}))

			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			node, err := tx.GetNode(nodeID)
			require.NoError(t, err)
			edge, err := tx.GetEdge(edgeID)
			require.NoError(t, err)
			tx.badgerTx.Discard()
			switch op {
			case "update node":
				err = tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
			case "delete node":
				err = tx.DeleteGraphNode(nodeID, CascadeEdges)
			case "delete connected node":
				err = tx.DeleteGraphNode(nodeID, RejectConnected)
			case "update relationship":
				err = tx.UpdateEdge(EdgeUpdateRequest{After: edge})
			case "delete relationship":
				err = tx.DeleteEdge(edgeID)
			}
			require.ErrorIs(t, err, badger.ErrDiscardedTxn)
			require.ErrorContains(t, err, "write conflict")
			require.Zero(t, tx.OperationCount())
		})
	}
}
