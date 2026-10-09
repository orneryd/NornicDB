package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The differential ratchet's #908 statements, with Neo4j 5.26.30's answers.

// A variable named twice in one chain binds one node.
func TestChainRepeatedNodeVariable(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "chain_repeat"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {i: 1})-[:R]->(b:N {i: 2})-[:S]->(:N {i: 3}), (b)-[:S]->(b), (d:N {i: 4})-[:R]->(:N {i: 5})-[:S]->(:N {i: 6})-[:T]->(d)", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a)-[:R]->(b)-[:S]->(b) RETURN count(*) AS c":                                 {{int64(1)}},
		"MATCH (a)-[:R]->(b)-[:S]->(b) RETURN a.i AS a, b.i AS b":                            {{int64(1), int64(2)}},
		"MATCH (a:N)-[:R]->(b:N)-[:S]->(b) RETURN a.i AS a":                                  {{int64(1)}},
		"MATCH (a)-[:R]->(b)-[:S]->(c) RETURN count(*) AS c":                                 {{int64(3)}},
		"MATCH (a)-[:R]->(b)-[:S]->(c)-[:T]->(a) RETURN a.i AS a":                            {{int64(4)}},
		"MATCH (a)-[:R]->(b)-[:S]->(c)-[:T]->(b) RETURN count(*) AS c":                       {{int64(0)}},
		"MATCH (b)-[:S]->(b)<-[:R]-(a) RETURN a.i AS a":                                      {{int64(1)}},
		"MATCH (x {i: 1}) MATCH (x)-[:R]->(b)-[:S]->(b) RETURN b.i AS b":                     {{int64(2)}},
		"MATCH (a)-[:R]->(b) WHERE EXISTS { MATCH (a)-[:R]->(b)-[:S]->(b) } RETURN a.i AS a": {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if os.Getenv("ZZ") != "" {
			fmt.Printf("%s %+v\n", query, exec.LastHotPathTrace())
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// ORDER BY a RETURN aggregate's expression, beside other terms, orders by
// the aggregate.
func TestOrderByRepeatedAggregate(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "order_aggregate"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:G {k: 'a'}), (:G {k: 'a'}), (:G {k: 'b'}), (:G {k: 'c'}), (:G {k: 'c'}), (:G {k: 'c'})", nil)
	require.NoError(t, err)
	descending := [][]interface{}{{"c", int64(3)}, {"a", int64(2)}, {"b", int64(1)}}
	for query, want := range map[string][][]interface{}{
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY count(*) DESC, g":                   descending,
		"MATCH (n:G) RETURN n.k AS g, count(n) AS c ORDER BY count(n) DESC, g":                   descending,
		"MATCH (n:G) RETURN n.k AS g, sum(1) AS c ORDER BY sum(1) DESC, g":                       descending,
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY COUNT(*) DESC, g":                   descending,
		"MATCH (n:G) RETURN n.k, count(*) AS c ORDER BY count(*) DESC, n.k":                      descending,
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY -count(*), g":                       descending,
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY count(*) DESC, g LIMIT 2":           descending[:2],
		"MATCH (n:G) RETURN n.k AS g, count(*) * 2 AS c ORDER BY count(*) * 2 DESC, g":           {{"c", int64(6)}, {"a", int64(4)}, {"b", int64(2)}},
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY g, count(*) DESC":                   {{"a", int64(2)}, {"b", int64(1)}, {"c", int64(3)}},
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY count(*) DESC":                      descending,
		"MATCH (n:G) RETURN DISTINCT n.k AS g, count(*) AS c ORDER BY count(*) DESC":             descending,
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY c DESC, g SKIP 1":                   descending[1:],
		"MATCH (n:G) WITH n.k AS g, count(*) AS c ORDER BY -count(*), g RETURN g, c":             descending,
		"MATCH (n:G) WITH n.k AS g, count(*) AS c ORDER BY Count( * ) DESC, g RETURN g, c":       descending,
		"MATCH (n:G) RETURN n.k AS g, count(DISTINCT n) AS c ORDER BY COUNT(distinct n) DESC, g": descending,
		"MATCH (n:G) RETURN n.k AS g, count(*) * 2 AS c ORDER BY COUNT(*)*2 DESC, g":             {{"c", int64(6)}, {"a", int64(4)}, {"b", int64(2)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if os.Getenv("ZZ") != "" {
			fmt.Printf("%s %+v\n", query, exec.LastHotPathTrace())
		}
		require.Equal(t, want, result.Rows, query)
	}
}
