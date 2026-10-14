package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/localization"
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
			requireStatus(t, e, q, nil, "Neo.ClientError.Statement.SyntaxError", "Bound relationships not allowed in "+call+"(...)")
			requireStatus(t, e, "EXPLAIN "+q, nil, "Neo.ClientError.Statement.SyntaxError", "Bound relationships not allowed in "+call+"(...)")
		}
	}
}

func TestShortestPathIdenticalEndpointVariables(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "samevariables"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (a:S {id:1})-[:T]->(a)", nil)
	require.NoError(t, err)
	message := localization.CypherMatchingShortestPathCommonEndNodes().Fallback
	for _, call := range []string{"shortestPath", "allShortestPaths"} {
		for _, bounds := range []string{"0..", "0", "1..", ""} {
			for _, filter := range []string{"", " WHERE length(p)>0"} {
				q := "MATCH (a:S {id:1}) MATCH p = " + call + "((a)-[:T*" + bounds + "]->(a))" + filter + " RETURN p"
				requireStatus(t, e, q, nil, "Neo.DatabaseError.Statement.ExecutionFailed", message)
				requireStatus(t, e, "EXPLAIN "+q, nil, "Neo.DatabaseError.Statement.ExecutionFailed", message)
			}
			requireStatus(t, e, "MATCH (a:Missing) RETURN "+call+"((a)-[:T*"+bounds+"]->(a))", nil, "Neo.DatabaseError.Statement.ExecutionFailed", message)
		}
		r, err := e.Execute(ctx, "MATCH (a:S {id:1}), (b:S {id:1}) MATCH p = "+call+"((a)-[:T*0..]->(b)) RETURN length(p)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, r.Rows)
	}
}

func TestShortestExplainValidBindings(t *testing.T) {
	e, _ := newShortestSelectorExecutor(t)
	_, err := e.Execute(context.Background(), "EXPLAIN MATCH (a:S {id:1}), (b:S {id:4}) MATCH p = shortestPath((a)-[r:T*]->(b)) RETURN p", nil)
	require.NoError(t, err)
}
