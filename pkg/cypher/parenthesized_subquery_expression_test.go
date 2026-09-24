package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestParenthesizedSubqueryExpressionsAreNotPatternMaps covers eshu #7042 cause D:
// an EXISTS/COUNT/COLLECT subquery expression brace that follows an unclosed
// '(' was misclassified as a node/relationship pattern property map by
// validateStaticMapKeys, raising "pattern property maps require key-value
// entries" for queries Neo4j accepts. Correctness is asserted by row counts,
// not merely the absence of an error.
func TestParenthesizedSubqueryExpressionsAreNotPatternMaps(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "eshu7042-causeD"))
	ctx := context.Background()

	// One WorkloadInstance with an outgoing edge, one without, plus 30 decoy
	// nodes so a full scan without the predicate would over-count.
	_, err := executor.Execute(ctx, "CREATE (a:WorkloadInstance {id: 'a'})-[:R]->(:Target)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (:WorkloadInstance {id: 'b'})", nil)
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		_, err = executor.Execute(ctx, "CREATE (:Decoy)", nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name  string
		query string
		want  int64
	}{
		{
			name:  "parenthesized EXISTS",
			query: "MATCH (i:WorkloadInstance) WHERE (EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  1,
		},
		{
			name:  "parenthesized COUNT comparison",
			query: "MATCH (i:WorkloadInstance) WHERE (COUNT { MATCH (i)-[:R]->() } > 0) RETURN count(i)",
			want:  1,
		},
		{
			name:  "nested parens around EXISTS",
			query: "MATCH (i:WorkloadInstance) WHERE ((EXISTS { MATCH (i)-[:R]->() })) RETURN count(i)",
			want:  1,
		},
		{
			name:  "OR combination inside parens",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id = 'b' OR EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  2,
		},
		{
			name:  "AND combination inside parens",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id = 'a' AND EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  1,
		},
		{
			name:  "eshu infraResourceScopePredicate shape",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id IN ['zzz'] OR i.id IN ['zzz'] OR EXISTS { MATCH (i)-[:R]->(t:Target) WHERE (t.id IN ['zzz'] OR t.id IN ['zzz']) }) RETURN count(i)",
			want:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err, test.query)
			require.Len(t, result.Rows, 1, test.query)
			require.Equal(t, test.want, result.Rows[0][0], test.query)
		})
	}
}

// TestParenthesizedCollectSubqueryIsNotAPatternMap covers the COLLECT {}
// brace of eshu #7042 cause D in its supported position (a RETURN
// projection, per TestCollectSubquery), wrapped in a paren so the '{'
// follows an unclosed '(' the way it would inside a WHERE predicate.
// Correctness is asserted by the collected values, not just no error.
func TestParenthesizedCollectSubqueryIsNotAPatternMap(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "eshu7042-causeD-collect"))
	ctx := context.Background()

	_, err := executor.Execute(ctx, "CREATE (a:WorkloadInstance {id: 'a'})-[:R]->(:Target {id: 't1'})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx,
		"MATCH (i:WorkloadInstance {id: 'a'}) RETURN (collect { MATCH (i)-[:R]->(t) RETURN t.id }) AS targets", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{"t1"}, result.Rows[0][0])
}

// TestParenthesizedSubqueryExpressionRejectsGenuineBadPatternMap proves the
// fix does not weaken validateStaticMapKeys: a real invalid pattern property
// map must still be rejected, including at nested paren depth.
func TestParenthesizedSubqueryExpressionRejectsGenuineBadPatternMap(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	tests := []string{
		"MATCH (n {a}) RETURN n",
		"MATCH (n) WHERE (true AND EXISTS((m {a}))) RETURN n",
	}
	for _, query := range tests {
		_, err := executor.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		semanticError, ok := err.(*SemanticError)
		require.True(t, ok, "%s: err = %#v", query, err)
		require.Equal(t, "UnexpectedSyntax", semanticError.Detail, query)
	}
}
