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

// A parenthesised expression that starts with a subquery is not a node
// pattern named EXISTS / COUNT / COLLECT. Recorded on Neo4j 5.26.30.
func TestParenthesisedSubqueryOperandIsNotANode(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_subquery_operand"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})-[:R]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE (EXISTS { MATCH (a)-[:R]->(x) } AND (a)-[:R]->(b)) RETURN a.id AS x, b.id AS y ORDER BY x, y":          {{int64(1), int64(2)}, {int64(2), int64(3)}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE false OR (COUNT { (a)--() } > 1 OR (a)-->()) RETURN a.id AS x, b.id AS y ORDER BY x, y":              {{int64(1), int64(2)}, {int64(2), int64(3)}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE NOT (EXISTS { MATCH (a)-[:R]->(x) } AND (a)-[:R]->(b)) RETURN a.id AS x, b.id AS y ORDER BY x, y":      {},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE (COLLECT { MATCH (a)-->(x) RETURN x.id } = [b.id] AND (a)-->(b)) RETURN a.id AS x, b.id AS y ORDER BY x": {{int64(1), int64(2)}, {int64(2), int64(3)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	require.Equal(t, []string{"a", "b"}, extractNodeVariables("(EXISTS { MATCH (q) } AND (a)-[:R]->(b))"))
}
