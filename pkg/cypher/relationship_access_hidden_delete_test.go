package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Rebase integration: the public DELETE paths must preserve the incoming
// stored-incident contract while all relationship I/O stays with the owner.
func TestRelationshipAccessProgramHiddenDeleteIntegrity(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		for _, detach := range []bool{false, true} {
			t.Run(fmt.Sprintf("transaction_%t/detach_%t", transaction, detach), func(t *testing.T) {
				base, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				ns := storage.NewNamespacedEngine(base, "test")
				for _, id := range []storage.NodeID{"a", "b"} {
					_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"DeleteNode"}, Properties: map[string]interface{}{"key": string(id)}})
					require.NoError(t, err)
				}
				require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "hidden", StartNode: "a", EndNode: "b", Type: "T"}))
				require.NoError(t, base.SetEdgeManuallySuppressed("test:hidden", true))
				e := NewStorageExecutor(ns)
				ctx := context.Background()
				if transaction {
					_, err = e.Execute(WithTransactionControl(ctx), "BEGIN", nil)
					require.NoError(t, err)
				}
				word := "DELETE"
				if detach {
					word = "DETACH DELETE"
				}
				_, err = e.Execute(ctx, "MATCH (a:DeleteNode {key:'a'}) "+word+" a", nil)
				if detach || transaction {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "relationship")
				}
				if transaction {
					// Explicit DELETE defers residual-edge validation until
					// COMMIT so a later statement can remove those edges.
					_, err = e.Execute(WithTransactionControl(ctx), "COMMIT", nil)
					if detach {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "relationship")
					}
				}
				_, nodeErr := storage.GetStoredNode(ns, "a")
				_, edgeErr := storage.GetStoredEdge(ns, "hidden")
				if detach {
					require.ErrorIs(t, nodeErr, storage.ErrNotFound)
					require.ErrorIs(t, edgeErr, storage.ErrNotFound)
				} else {
					require.NoError(t, nodeErr)
					require.NoError(t, edgeErr)
				}
			})
		}
	}
}
