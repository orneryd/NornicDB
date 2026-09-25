package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSetPropertiesSetCountsWrites pins properties_set to Neo4j 5.26.30's
// rule (#678): a SET counts the properties it writes, not the ones whose value
// changed. Every expectation was read from Neo4j on the same graph, with key
// names never used before in that database (zz, q, b are fresh), so a null for
// a key the entity lacks counts 0 (setWrites).
func TestSetPropertiesSetCountsWrites(t *testing.T) {
	const setup = "CREATE (:T {id: 1, x: 5})-[:R {w: 1}]->(:M)"
	tests := []struct {
		query  string
		params map[string]interface{}
		want   int
	}{
		{query: "MATCH (n:T {id: 1}) SET n.x = 5", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.x = n.x", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.x = 6, n.x = 7", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.x = 6, n.y = 1, n.x = 7", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n.x = 1, n.y = 1, n.x = 2, n.y = 2", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n.x = 6, n:L, n.x = 7", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n.x = 6 SET n.x = 7", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n.x = null", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.zz = null", want: 0},
		{query: "MATCH (n:T {id: 1}) SET n.x = null, n.x = 3", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.zz = null, n.zz = 3", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n.zz = 3, n.zz = null", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n += {x: null}", want: 1},
		{query: "MATCH (n:T {id: 1}) SET n += {zz: null}", want: 0},
		{query: "MATCH (n:T {id: 1}) SET n += {x: 5, y: 1}", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n += {}", want: 0},
		{query: "MATCH (n:T {id: 1}) SET n = {x: 5}", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n = {a: 1, b: null}", want: 3},
		{query: "MATCH (n:T {id: 1}) SET n = {x: null}", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n = {}", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n = {x: 1}, n.x = 2", want: 3},
		{query: "MATCH (n:T {id: 1}) SET n.x = 2, n = {x: 1}", want: 3},
		{query: "MATCH (n:T {id: 1}) SET n.x = 1, n += {x: 2}", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n = {x: 2}, n = {x: 3}", want: 3},
		{query: "MATCH (n:T {id: 1}), (m:M) SET m = properties(n)", want: 2},
		{query: "MATCH (n:T {id: 1}) SET n += $m", params: map[string]interface{}{"m": map[string]interface{}{"x": int64(5), "zz": nil}}, want: 1},
		{query: "MATCH (n:T {id: 1}) SET n = $m", params: map[string]interface{}{"m": map[string]interface{}{"x": int64(5), "zz": nil}}, want: 2},
		{query: "MATCH (n:T {id: 1}) SET n.x = $p", params: map[string]interface{}{"p": int64(5)}, want: 1},
		{query: "MATCH ()-[r:R]->() SET r.w = 1", want: 1},
		{query: "MATCH ()-[r:R]->() SET r.zz = null", want: 0},
		{query: "MATCH ()-[r:R]->() SET r.w = null", want: 1},
		{query: "MATCH ()-[r:R]->() SET r.w = 2, r.w = 3", want: 1},
		{query: "MATCH ()-[r:R]->() SET r += {q: null}", want: 0},
		{query: "MATCH ()-[r:R]->() SET r += {w: null}", want: 1},
		{query: "MATCH ()-[r:R]->() SET r += {w: 1, q: 2}", want: 2},
		{query: "MATCH ()-[r:R]->() SET r = {v: 2, q: null}", want: 2},
		{query: "MATCH ()-[r:R]->() SET r = {w: 1}", want: 1},
		{query: "MATCH ()-[r:R]->() SET r = {}", want: 1},
		{query: "MATCH (n:T {id: 1})-[r:R]->() SET n.x = 1, r.w = 1, n.x = 2", want: 3},
		{query: "MATCH (n:T {id: 1}) UNWIND [1, 2] AS i SET n.x = i", want: 2},
		{query: "MERGE (m:T {id: 1}) ON MATCH SET m.x = 5", want: 1},
		{query: "MERGE (m:T {id: 1}) ON MATCH SET m += {x: 5, q: null}", want: 1},
		{query: "MERGE (m:T {id: 2}) ON CREATE SET m.x = 5, m.x = 6", want: 2},
		{query: "CREATE (m:T {id: 3}) SET m.x = 5, m.x = 6", want: 2},
		{query: "CREATE (m:T {id: 3}) SET m.id = 3", want: 2},
		{query: "UNWIND [{id: 1, x: 5}] AS row MERGE (n:T {id: row.id}) SET n.x = row.x", want: 1},
		{query: "UNWIND [{id: 1, x: 5}] AS row MERGE (n:T {id: row.id}) SET n += row", want: 2},
		{query: "MATCH (n:T {id: 1}) REMOVE n.x", want: 1},
		{query: "MATCH (n:T {id: 1}) REMOVE n.zz", want: 0},
	}
	for _, explicit := range []bool{false, true} {
		for _, tc := range tests {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "set678"))
			ctx := context.Background()
			_, err := executor.Execute(ctx, setup, nil)
			require.NoError(t, err)
			if explicit {
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			result, err := executor.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.want, result.Stats.PropertiesSet, "explicit=%v %s", explicit, tc.query)
			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		}
	}
}
