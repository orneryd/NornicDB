package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOptimisticMetadataConvergesAcrossAPOCAndCallTail(t *testing.T) {
	for _, many := range []bool{false, true} {
		for _, tail := range []bool{false, true} {
			t.Run(fmt.Sprintf("many=%t/tail=%t", many, tail), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "optimistic_owner")
				executor := NewStorageExecutor(store)
				body := "CREATE (a:OptimisticOwner {kind:'a'})-[:OPTIMISTIC_OWNER]->(b:OptimisticOwner {kind:'b'}) RETURN 1 AS v"
				query := "CALL apoc.cypher.run($body,{}) YIELD value"
				if tail {
					query += " WITH value RETURN value.v AS v"
				}
				wantNodes, wantEdges := 2, 1
				if many {
					body += "; CREATE (a:OptimisticOwner {kind:'c'})-[:OPTIMISTIC_OWNER]->(b:OptimisticOwner {kind:'d'}) RETURN 2 AS v"
					query = "CALL apoc.cypher.runMany($body,{},{statistics:false}) YIELD row,result"
					if tail {
						query += " WITH row,result RETURN result.v AS v"
					}
					wantNodes, wantEdges = 4, 2
				}
				result, err := executor.Execute(t.Context(), query, map[string]interface{}{"body": body})
				require.NoError(t, err)
				meta, ok := result.Metadata["optimistic"].(*optimisticMutationMeta)
				require.True(t, ok, "scalar RETURN rows cannot be used to reconstruct created IDs")
				require.Len(t, meta.CreatedNodeIDs, wantNodes)
				require.Len(t, meta.CreatedRelationshipIDs, wantEdges)
				for _, id := range meta.CreatedNodeIDs {
					_, err := store.GetNode(t.Context(), storage.NodeID(id))
					require.NoError(t, err, "actual created node %q", id)
				}
				for _, id := range meta.CreatedRelationshipIDs {
					_, err := store.GetEdge(t.Context(), storage.EdgeID(id))
					require.NoError(t, err, "actual created relationship %q", id)
				}
				nodes, err := store.GetNodesByLabel(t.Context(), "OptimisticOwner")
				require.NoError(t, err)
				require.Len(t, nodes, wantNodes)
			})
		}
	}
}

func TestOptimisticMetadataMergeIsTypedIsolatedAndDeduplicated(t *testing.T) {
	source := &ExecuteResult{Metadata: map[string]interface{}{"plan": "private", "db": "inner", "receipt": "not-owned", "resolved_query_type": "w"}}
	addOptimisticNodeID(source, "node-one")
	addOptimisticRelationshipID(source, "edge-one")
	target := &ExecuteResult{Metadata: map[string]interface{}{"db": "outer", "receipt": "owned"}}
	addOptimisticNodeID(target, "node-two")
	mergeOptimisticMetadata(target, source)
	mergeOptimisticMetadata(target, source)
	meta := target.Metadata["optimistic"].(*optimisticMutationMeta)
	require.ElementsMatch(t, []string{"node-one", "node-two"}, meta.CreatedNodeIDs)
	require.Equal(t, []string{"edge-one"}, meta.CreatedRelationshipIDs)
	require.Equal(t, "outer", target.Metadata["db"])
	require.Equal(t, "owned", target.Metadata["receipt"])
	require.NotContains(t, target.Metadata, "plan")
	require.NotContains(t, target.Metadata, "resolved_query_type")
	original := source.Metadata["optimistic"].(*optimisticMutationMeta)
	require.Equal(t, []string{"node-one"}, original.CreatedNodeIDs)
	require.Len(t, original.seenNodeIDs, 1)
	mergeOptimisticMetadata(target, &ExecuteResult{Metadata: map[string]interface{}{"optimistic": map[string]interface{}{"createdNodeIds": []string{"untyped"}}}})
	require.ElementsMatch(t, []string{"node-one", "node-two"}, meta.CreatedNodeIDs)
}
