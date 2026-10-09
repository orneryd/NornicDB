package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Whitespace inside a relationship arrow means nothing, as in Neo4j 5.26.30,
// whose answers these are (#907).
func TestArrowSpacing(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "arrow_spacing"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:AS {i: 1})-[:R]->(:AS {i: 2})", nil)
	require.NoError(t, err)
	run := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	forward := [][]interface{}{{int64(1), int64(2)}}
	for _, pattern := range []string{
		"(a:AS) -[r]->(b)", "(a:AS)- [r]->(b)", "(a:AS)-[r] ->(b)", "(a:AS)-[r]- >(b)", "(a:AS)-[r]-> (b)",
		"(b) <-[r]-(a:AS)", "(b)< -[r]-(a:AS)", "(b)<- [r]-(a:AS)", "(b)<-[r] -(a:AS)", "(b)<-[r]- (a:AS)",
		"(a:AS) -->(b)", "(a:AS)- ->(b)", "(a:AS)-- >(b)", "(b)< --(a:AS)", "(b)<- -(a:AS)",
		"(a:AS) - [r] -> (b)", "(b) < - [ r ] - (a:AS)", "(a:AS)  -/* c */[r]-\n>(b)",
		"(a:AS)-[r:R]->(b)",
	} {
		require.Equal(t, forward, run("MATCH "+pattern+" RETURN a.i, b.i"), pattern)
	}
	require.Equal(t, forward, run("OPTIONAL MATCH (b) <- [r] - (a:AS) RETURN a.i, b.i"))
	require.Equal(t, forward, run("MATCH (a:AS {i: 1}), (b:AS {i: 2}) MERGE (a) - [r:R] -> (b) RETURN a.i, b.i"))

	// In an expression a dash or a comparison keeps its spaces.
	for query, want := range map[string][][]interface{}{
		"WITH 1 AS x RETURN x < - -1":                                       {{false}},
		"MATCH (x:AS {i: 1}) RETURN (2) - [3][0], x.i - -1":                 {{int64(-1), int64(2)}},
		"MATCH (x:AS {i: 1}) WHERE (x.i) < - -2 OR x.i - -1 > 1 RETURN x.i": {{int64(1)}},
		"MERGE (x:AS {i: 1}) ON MATCH SET x.j = x.i - -1 RETURN x.j":        {{int64(2)}},
		"MERGE (x:AS {i: 9}) ON CREATE SET x.j = 1 - - 1 RETURN x.j":        {{int64(2)}},
		"MATCH (x:AS {i: 1}) CALL { WITH x RETURN x.i - -1 AS y } RETURN y": {{int64(2)}},
		"MATCH (x:AS {i: 1}) RETURN [y IN [1] WHERE y > - 1 | y - -1] AS l": {{[]interface{}{int64(2)}}},
		"MATCH (x:AS {i: 1}) RETURN x {.i, j: x.i - -1} AS m":               {{map[string]interface{}{"i": int64(1), "j": int64(2)}}},
		"MATCH (x:AS {i: 1}) RETURN 'a - >b' AS s":                          {{"a - >b"}},
	} {
		require.Equal(t, want, run(query), query)
	}
}

// The second canonicalization of a statement whose first rewrite is an arrow
// gap returns the memoized rewrite.
func TestArrowSpacingCanonicalMemo(t *testing.T) {
	query := "MATCH (b) <-[r]-(a:ASMemo) RETURN b"
	first, rewrite := canonicalizeQueryText(query)
	require.Equal(t, "MATCH (b)<-[r]-(a:ASMemo) RETURN b", first)
	require.NotNil(t, rewrite)
	second, memoized := canonicalizeQueryText(query)
	require.Equal(t, first, second)
	require.Same(t, rewrite, memoized)
}
