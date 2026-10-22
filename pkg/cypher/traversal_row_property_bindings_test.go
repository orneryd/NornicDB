package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineTraversalRowPropertyBindings(t *testing.T) {
	queries := []struct {
		name, query string
		count       bool
		want        int64
	}{
		{"start", "UNWIND range(1,3) AS x MATCH (p:P {id: x})-[:R]->(q) RETURN q.id", false, 3},
		{"end", "UNWIND range(1,3) AS x MATCH (p:P)-[:R]->(q:Q {id: x}) RETURN q.id", false, 3},
		{"relationship", "UNWIND range(1,3) AS x MATCH (p:P)-[:R {value: x}]->(q) RETURN q.id", false, 3},
		{"both nodes", "UNWIND range(1,3) AS x MATCH (p:P {id: x})-[:R]->(q:Q {id: x}) RETURN q.id", false, 3},
		{"map value", "UNWIND [{id:1},{id:2},{id:3}] AS row MATCH (p:P {id: row.id})-[:R]->(q) RETURN q.id", false, 3},
		{"subscript", "UNWIND range(0,2) AS x MATCH (p:P {id: $ids[x]})-[:R]->(q) RETURN q.id", false, 3},
		{"chained", "UNWIND range(1,3) AS x MATCH (p:P {id: x})-[:R]->(q)-[:S]->(s) RETURN q.id", false, 3},
		{"optional", "UNWIND range(1,3) AS x OPTIONAL MATCH (p:P {id: x})-[:R]->(q) RETURN q.id", false, 3},
		{"count", "UNWIND range(1,3) AS x MATCH (p:P {id: x})-[:R]->(q) RETURN count(q)", true, 3},
		{"empty input", "UNWIND [] AS x MATCH (p:P {id: x})-[:R]->(q) RETURN count(q)", true, 0},
		{"null input", "UNWIND [null] AS x MATCH (p:P {id: x})-[:R]->(q) RETURN count(q)", true, 0},
		{"literal control", "MATCH (p:P {id: 1})-[:R]->(q) RETURN count(q)", true, 1},
		{"quantified path control", "MATCH ((p:P)-[:R]->(q:Q))+ RETURN count(*)", true, 3},
		{"quantified shortest path control", "MATCH p = SHORTEST 1 (a:P {id: 1})((x)-[:R]->(y))+(b:Q {id: 1}) RETURN count(*)", true, 1},
	}
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("index=%t/transaction=%t", indexed, explicit), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_property")
				for value := int64(1); value <= 3; value++ {
					p, q, s := fmt.Sprintf("p%d", value), fmt.Sprintf("q%d", value), fmt.Sprintf("s%d", value)
					for _, node := range []*storage.Node{
						{ID: storage.NodeID(p), Labels: []string{"P"}, Properties: map[string]interface{}{"id": value}},
						{ID: storage.NodeID(q), Labels: []string{"Q"}, Properties: map[string]interface{}{"id": value}},
						{ID: storage.NodeID(s), Labels: []string{"End"}},
					} {
						_, err := store.CreateNode(node)
						require.NoError(t, err)
					}
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", value)), StartNode: storage.NodeID(p), EndNode: storage.NodeID(q), Type: "R", Properties: map[string]interface{}{"value": value}}))
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("s%d", value)), StartNode: storage.NodeID(q), EndNode: storage.NodeID(s), Type: "S"}))
				}
				exec := NewStorageExecutor(store)
				ctx := context.Background()
				if indexed {
					_, err := exec.Execute(ctx, "CREATE INDEX row_property_id FOR (p:P) ON (p.id)", nil)
					require.NoError(t, err)
				}
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				for _, query := range queries {
					t.Run(query.name, func(t *testing.T) {
						for _, prefix := range []string{"", "PROFILE "} {
							result, err := exec.Execute(ctx, prefix+query.query, map[string]interface{}{"ids": []interface{}{int64(3), int64(1), int64(2)}})
							require.NoError(t, err, prefix+query.query)
							if query.count {
								require.Equal(t, [][]interface{}{{query.want}}, result.Rows)
							} else {
								require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
							}
						}
					})
				}
			})
		}
	}
}
