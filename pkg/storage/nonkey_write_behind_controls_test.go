package storage

import (
	"errors"
	"fmt"

	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteBehindConflictBaselineControls(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			for _, mutation := range []string{"update", "changed_key", "delete", "embedding", "embedding_operation"} {
				t.Run(fmt.Sprintf("async=%t/implicit=%t/%s", async, implicit, mutation), func(t *testing.T) {
					engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: async, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					schema := engine.GetSchemaForNamespace("test")
					require.NoError(t, schema.AddConstraint(Constraint{Name: "doc_id", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}))
					require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(engine, "test"), schema))
					id, err := engine.CreateNode(&Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "original"}})
					require.NoError(t, err)
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetImplicit(implicit))
					node, err := tx.GetNode(id)
					require.NoError(t, err)
					before := copyNode(node)
					// The peer commits BEFORE the loser stages its write. A fresh
					// staging read cannot replace the loser's earlier observation.
					peer := copyNode(node)
					peer.Properties["summary_input"] = "peer"
					if mutation == "changed_key" {
						peer.Properties["id"] = "claimed"
						node.Properties["id"] = "claimed"
					}
					require.NoError(t, engine.UpdateNode(peer))
					published, err := engine.GetNode(id)
					require.NoError(t, err)
					require.Equal(t, "peer", published.Properties["summary_input"])
					if mutation == "delete" {
						require.NoError(t, tx.DeleteNode(id))
					} else {
						node.Properties["summary_input"] = "loser"
						if mutation == "embedding" {
							node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
						}
						require.NoError(t, tx.UpdateNodeObserved(before, node))
						node, err = tx.GetNode(id)
						require.NoError(t, err)
						node.Properties["summary_input"] = "second write"
						require.NoError(t, tx.UpdateNodeObserved(before, node))
						if mutation == "embedding_operation" {
							// The legacy dispatch kind has no current staging producer.
							// It must use the same recorded baseline if encountered.
							tx.operations[0].Type = OpUpdateEmbedding
						}
					}
					// Classification has no dependency on a readable physical
					// snapshot once the transaction has staged its observation.
					snapshot := tx.snapshotTx
					discarded := engine.db.testTxn(false)
					discarded.Discard()
					tx.snapshotTx = discarded
					var classified *ConstraintViolationError
					require.Equal(t, mutation == "changed_key", errors.As(tx.classifyUpdateConflictAsUniqueViolation(id), &classified))
					tx.snapshotTx = snapshot
					err = tx.Commit()
					require.ErrorIs(t, err, ErrConflict)
					var violation *ConstraintViolationError
					require.Equal(t, mutation == "changed_key", errors.As(err, &violation), "%v", err)
				})
			}
		}
	}
}
