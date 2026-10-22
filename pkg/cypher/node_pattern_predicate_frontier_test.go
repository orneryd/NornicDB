package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A predicate must consume each complete MATCH row once. A source may evaluate
// a repeatable predicate early, but it cannot cap before a deferred binding or
// replay a volatile predicate after a candidate has consumed a window slot.
func TestNodePatternPredicateFrontier(t *testing.T) {
	ctx := context.Background()
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_frontier")
			executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			_, err := executor.Execute(ctx, "UNWIND range(1,16) AS id CREATE (:Sample {id:id})", nil)
			require.NoError(t, err)
			if indexed {
				_, err = executor.Execute(ctx, "CREATE INDEX sample_id FOR (n:Sample) ON (n.id)", nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"MATCH p=(n:Sample) WHERE nodes(p)[0].id > 8 RETURN n.id ORDER BY n.id LIMIT 1",
				"MATCH p=(n:Sample) WHERE nodes(p)[0].id < 9 RETURN n.id ORDER BY n.id DESC LIMIT 1",
				"MATCH p=(n:Sample) WHERE nodes(p)[0].id > 8 RETURN n.id ORDER BY n.id SKIP 1 LIMIT 1",
				"MATCH p=(n:Sample) WHERE nodes(p)[0].id > 8 WITH n ORDER BY n.id LIMIT 1 RETURN n.id",
			} {
				t.Run(query, func(t *testing.T) {
					result, err := executor.Execute(ctx, query, nil)
					require.NoError(t, err)
					want := int64(9)
					if strings.Contains(query, "DESC") {
						want = 8
					}
					if strings.Contains(query, "SKIP") {
						want = 10
					}
					require.Equal(t, [][]interface{}{{want}}, result.Rows)
				})
			}
		})
	}
}

func TestNodePatternVolatilePredicateFrontier(t *testing.T) {
	const population = 4096
	ctx := context.Background()
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "volatile_frontier")
			nodes := make([]*storage.Node, 0, population+1)
			edges := make([]*storage.Edge, 0, population)
			nodes = append(nodes, &storage.Node{ID: "end", Labels: []string{"End"}, Properties: map[string]interface{}{"id": int64(1)}})
			for id := 0; id < population; id++ {
				nodeID := storage.NodeID(fmt.Sprintf("n%d", id))
				nodes = append(nodes, &storage.Node{ID: nodeID, Labels: []string{"Sample"}, Properties: map[string]interface{}{"id": int64(id)}})
				edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", id)), StartNode: nodeID, EndNode: "end", Type: "R"})
			}
			require.NoError(t, store.BulkCreateNodes(nodes))
			require.NoError(t, store.BulkCreateEdges(edges))
			executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			if indexed {
				_, err := executor.Execute(ctx, "CREATE INDEX sample_id FOR (n:Sample) ON (n.id)", nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"MATCH (n:Sample) WHERE n.id IS NOT NULL AND rand()<0.5 RETURN n.id ORDER BY n.id LIMIT 4096",
				"MATCH (n:Sample) WHERE n.id IS NOT NULL AND rand()<0.5 RETURN n.id LIMIT 4096",
				"MATCH (n:Sample)-[:R]->(q:End) WHERE n.id IS NOT NULL AND rand()<0.5 RETURN n.id ORDER BY n.id LIMIT 4096",
				"MATCH (n:Sample)-[:R]->(q:End) WHERE n.id IS NOT NULL AND rand()<0.5 RETURN n.id",
			} {
				t.Run(query, func(t *testing.T) {
					result, err := executor.Execute(ctx, query, nil)
					require.NoError(t, err)
					t.Logf("accepted=%d population=%d", len(result.Rows), population)
					// The interval is about 16 standard deviations from the
					// correct mean, and separates one draw from two independent
					// draws. Independent executions need not return identical IDs.
					require.Greater(t, len(result.Rows), population*3/8)
					require.Less(t, len(result.Rows), population*5/8)
				})
			}
			// Reusing a source is permitted; reusing its volatile WHERE verdict
			// across input rows is not. Compare sets, not just total cardinality.
			result, err := executor.Execute(ctx, "UNWIND [1,2] AS x MATCH (n:Sample) WHERE n.id IS NOT NULL AND rand()<0.5 RETURN x,n.id ORDER BY x,n.id LIMIT 8192", nil)
			require.NoError(t, err)
			sets := map[int64][]interface{}{1: {}, 2: {}}
			for _, row := range result.Rows {
				key := row[0].(int64)
				sets[key] = append(sets[key], row[1])
			}
			require.Greater(t, len(sets[1]), population*3/8)
			require.Greater(t, len(sets[2]), population*3/8)
			require.NotEqual(t, sets[1], sets[2], "volatile verdicts must not be cached between input rows")
		})
	}
}
