package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelectedPathBoundRelationshipLists(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boundrelationships"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (a:S {id:1})-[:T]->(b:S {id:3}), (a)-[:T]->(:S {id:2})-[:T]->(b)", nil)
	require.NoError(t, err)
	for _, selector := range []string{"ANY SHORTEST", "ALL SHORTEST", "SHORTEST 1", "SHORTEST 2", "ANY 2"} {
		q := "MATCH (a:S {id:1})-[r:T*2]->(b:S {id:3}) WITH a,b,r MATCH p = " + selector + " (a)-[r:T*]->(b) RETURN length(p),size(r)"
		r, err := e.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		require.Equal(t, [][]interface{}{{int64(2), int64(2)}}, r.Rows, q)
	}
	for _, call := range []string{"shortestPath", "allShortestPaths"} {
		for _, q := range []string{
			"MATCH (a:S {id:1})-[r:T*2]->(b:S {id:3}) WITH a,b,r MATCH p = " + call + "((a)-[r:T*]->(b)) RETURN p",
			"MATCH (a:S {id:1})-[r:T*2]->(b:S {id:3}) WITH a,b,r RETURN " + call + "((a)-[r:T*]->(b))",
			"MATCH (a:Missing)-[r:T*]->(b) WITH a,b,r MATCH p = " + call + "((a)-[r:T*]->(b)) RETURN p",
			"MATCH (a:S {id:1})-[r:T*2]->(b:S {id:3}), p = " + call + "((a)-[r:T*]->(b)) RETURN p",
		} {
			requireStatus(t, e, q, nil, "Neo.ClientError.Statement.SyntaxError", "Bound relationships not allowed in shortestPath(...)")
		}
	}
}
