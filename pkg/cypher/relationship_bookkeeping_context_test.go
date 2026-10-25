package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1667 implementation lane (Codex). Public statements exercise the
// pre-acceptance endpoint-label and rollback-image preparation owners.
func TestRelationshipBookkeepingPreparationCancellation(t *testing.T) {
	for _, operation := range []string{"create", "delete", "detach", "update"} {
		owners := []string{"uniqueLabelsWithContext", "edgePositionalLabelDeltaKeysWithContext", "bufferEdgePositionalLabelDeltasWithContext"}
		if operation == "update" {
			owners = []string{"copyEdgeWithContext"}
		}
		for _, owner := range owners {
			t.Run(operation+"/"+owner, func(t *testing.T) {
				b, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				labels := make([]string, 20)
				for i := range labels {
					labels[i] = fmt.Sprintf("L%d", i)
				}
				_, err = b.CreateNode(&storage.Node{ID: "test:a", Labels: append([]string{"A"}, labels...)})
				require.NoError(t, err)
				_, err = b.CreateNode(&storage.Node{ID: "test:b", Labels: append([]string{"B"}, labels...)})
				require.NoError(t, err)
				properties := make(map[string]interface{}, 20)
				for i := 0; i < 20; i++ {
					properties[fmt.Sprintf("p%d", i)] = int64(i)
				}
				if operation != "create" {
					require.NoError(t, b.CreateEdge(&storage.Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: properties}))
				}
				e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
				query := map[string]string{
					"create": "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)",
					"delete": "MATCH ()-[r:R]->() DELETE r",
					"detach": "MATCH (a:A) DETACH DELETE a",
					"update": "MATCH ()-[r:R]->() SET r.p0 = 999",
				}[operation]
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
				_, err = e.Execute(ctx, query, nil)
				t.Logf("operation=%s owner=%s hits=%d execute=%v", operation, owner, ctx.hits, err)
				require.GreaterOrEqual(t, ctx.hits, 2, "cancel inside the actual preparation owner")
				require.ErrorIs(t, err, context.Canceled)
				edges, err := b.GetStoredEdgesByType("R")
				require.NoError(t, err)
				if operation == "create" {
					require.Empty(t, edges)
				} else {
					require.Len(t, edges, 1)
					require.Equal(t, int64(0), edges[0].Properties["p0"])
				}
				_, err = b.GetStoredNode("test:a")
				require.NoError(t, err, "failed preparation preserves the endpoint")
				_, err = e.Execute(context.Background(), query, nil)
				require.NoError(t, err, "same statement retries after canceled preparation")
			})
		}
	}
}
