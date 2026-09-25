package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue507_CreateReturnCount(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue507"))

	result, err := executor.Execute(context.Background(), "CREATE (w:X) RETURN count(w) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"count"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	for _, query := range []string{
		"CREATE (w:SB {t: 1}) RETURN count(w) AS c",
		"CREATE (w:SB {t: 1}) RETURN count(*) AS c",
	} {
		result, err := executor.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, query)
	}
}

// TestIssue507_CountOverSeveralCreates covers count() over a statement with
// several CREATE clauses, which returned null in explicit transactions, and in
// auto-commit too when the first CREATE has a relationship pattern (#507).
func TestIssue507_CountOverSeveralCreates(t *testing.T) {
	queries := []struct {
		query string
		want  [][]interface{}
	}{
		{"CREATE (a:A) CREATE (b:B) RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"CREATE (a:A) CREATE (b:B) RETURN count(a) AS c", [][]interface{}{{int64(1)}}},
		{"CREATE (a:A) CREATE (b:B) CREATE (c:C) RETURN count(b) AS c", [][]interface{}{{int64(1)}}},
		{"CREATE (a:A)-[:R]->(b:B) CREATE (c:C) RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"CREATE (a:A {x: 1}) CREATE (b:B) RETURN collect(a.x) AS l, sum(a.x) AS s", [][]interface{}{{[]interface{}{int64(1)}, int64(1)}}},
	}
	for _, explicit := range []bool{false, true} {
		for _, tc := range queries {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue507multi"))
			ctx := context.Background()
			if explicit {
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			result, err := executor.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.want, result.Rows, "explicit=%v %s", explicit, tc.query)
			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		}
	}
}

func TestIssue508_AggregatesPatternComprehension(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue508"))
	ctx := context.Background()

	_, err := executor.Execute(ctx, "CREATE (a:A)-[:R]->(:B)", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, "MATCH (a:A) RETURN sum(size([(a)-[:R]->() | 1])) AS sum, max(size([(a)-[:R]->() | 1])) AS max", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sum", "max"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)
}

func TestIssue509_MergeWholePathCreatesRelationship(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue509"))

	result, err := executor.Execute(context.Background(), "MERGE (a:Start {id: 'a'})-[:R]->(b:End {id: 'b'}) RETURN count(*) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	result, err = executor.Execute(context.Background(), "MATCH (:Start {id: 'a'})-[r:R]->(:End {id: 'b'}) RETURN count(r) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

func TestIssue506_ExistsWithCountInExplicitTransaction(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue506"))
	ctx := context.Background()

	_, err := executor.Execute(ctx, "CREATE (a:A)-[:R]->(:B)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()

	result, err := executor.Execute(ctx, "MATCH (a:A) WHERE EXISTS { MATCH (a)-[:R]->() } RETURN count(a) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
