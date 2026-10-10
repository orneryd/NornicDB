package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A function call's argument inside an inline pattern WHERE (size(kinds)) is
// not a node pattern, so a bound list there is not checked as a node.
// Recorded on Neo4j 5.26.30.
func TestInlinePatternWhereFunctionArgumentIsNotANode(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_function_argument"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ZG2 {id:'a'})-[:R2]->(:ZG2 {id:'b'})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (s:ZG2 {id:'a'}) WITH s, [] AS kinds MATCH (s)-[r:R2 WHERE size(kinds) = 0]->(t) RETURN t.id AS v",
		"MATCH (s:ZG2 {id:'a'}) WITH s, [1] AS kinds MATCH (s)-[r:R2]->(t:ZG2 WHERE size(kinds) > 0) RETURN t.id AS v",
		"MATCH (s:ZG2 {id:'a'}) WITH s, [1] AS kinds OPTIONAL MATCH (s)-[r:R2 WHERE size(kinds) = 1]->(t) RETURN t.id AS v",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{"b"}}, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "MATCH(s:ZG2 {id:'a'}) RETURN s.id AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
	require.Equal(t, []string{"s", "x", "t"}, extractNodeVariables("(s)-[r WHERE size(kinds) = 0 AND exists((x))]->(t)"))
	require.True(t, functionCallParenAt("size(kinds)", 4))
	require.False(t, functionCallParenAt("MATCH(n)", 5))
	require.False(t, functionCallParenAt("(n)", 0))
}
