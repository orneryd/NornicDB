package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestChainedOptionalMatchesPreserveEarlierBindingsForAggregation(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer func() { require.NoError(t, base.Close()) }()
	store := storage.NewNamespacedEngine(base, "optional_aggregation")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (:DoesExist {num: 42})
CREATE (:DoesExist {num: 43})
CREATE (:DoesExist {num: 44})
CREATE (:AlsoExists {num: 99})`, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		query       string
		clauseCount int
		columns     []string
		rows        [][]interface{}
	}{
		{
			name: "two optional matches",
			query: `OPTIONAL MATCH (f:DoesExist)
OPTIONAL MATCH (n:DoesNotExist)
RETURN collect(DISTINCT n.num) AS missing, collect(DISTINCT f.num) AS present`,
			clauseCount: 3,
			columns:     []string{"missing", "present"},
			rows:        [][]interface{}{{[]interface{}{}, []interface{}{int64(42), int64(43), int64(44)}}},
		},
		{
			name: "three optional matches with an unmatched middle clause",
			query: `OPTIONAL MATCH (f:DoesExist)
OPTIONAL MATCH (n:DoesNotExist)
OPTIONAL MATCH (g:AlsoExists)
RETURN collect(DISTINCT f.num) AS first, collect(DISTINCT n.num) AS missing, collect(DISTINCT g.num) AS last`,
			clauseCount: 4,
			columns:     []string{"first", "missing", "last"},
			rows:        [][]interface{}{{[]interface{}{int64(42), int64(43), int64(44)}, []interface{}{}, []interface{}{int64(99)}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clauses, ok := canExecuteAsPipeline(tc.query)
			require.True(t, ok)
			require.Len(t, clauses, tc.clauseCount)

			pipelineResult, handled, err := exec.executePipeline(ctx, tc.query)
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, tc.rows, pipelineResult.Rows)

			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.columns, result.Columns)
			require.Equal(t, tc.rows, result.Rows)
		})
	}
}
