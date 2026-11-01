package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPatternPropertyAdmissionIsStructural(t *testing.T) {
	calls := 0
	withPluginFunctions(t, map[string]interface{}{
		"inspection.value": func() int64 { calls++; return 1 },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_admission"))
	ctx := context.Background()
	clauses := []pipelineClause{
		retainedClauseForTest(t.Context(), "MATCH (a:CodeIntel {id:inspection.value()})-[r:T]->()"),
		retainedClauseForTest(t.Context(), "RETURN r"),
	}
	_, handled, err := exec.tryExecutePipelineSimpleRelationshipCountPlan(ctx, clauses, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Zero(t, calls)
	clauses[0].text = "MATCH (a:CodeIntel {id:inspection.value()})"
	clauses[1].text = "RETURN a.id + 1"
	_, handled, err = exec.tryExecutePipelineSimpleNodeReadPlan(ctx, clauses, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Zero(t, calls)
	for _, projection := range []string{"RETURN a.id", "RETURN count(a)", "RETURN a.id LIMIT 2"} {
		clauses[1].text = projection
		_, handled, err = exec.tryExecutePipelineSimpleNodeReadPlan(ctx, clauses, nil)
		require.NoError(t, err)
		require.False(t, handled, "dynamic properties belong to the row source")
		require.Zero(t, calls)
	}
	for _, query := range []string{
		"MATCH (a:CodeIntel {id:inspection.value()}) RETURN a.id",
		"MATCH (a:CodeIntel {id:inspection.value()}) WITH a AS node RETURN node",
	} {
		_, handled = exec.tryFastPathMatchVectorCosine(ctx, query, upperASCII(query))
		require.False(t, handled)
		require.Zero(t, calls)
		_, handled = exec.tryFastPathMatchWithVectorCosineProjection(ctx, query, upperASCII(query))
		require.False(t, handled)
		require.Zero(t, calls)
	}
}

func TestPatternPropertyRowNullAndErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_null_error"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CodeIntel {id:1})-[:T {w:1}]->()", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"UNWIND [null] AS wanted MATCH (a:CodeIntel {id:wanted})-[r:T]->() RETURN count(r)",
		"UNWIND [null] AS wanted MATCH (a:CodeIntel) WHERE (a)-[:T {w:wanted}]->() RETURN count(a)",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	}
	_, err = exec.Execute(ctx, "UNWIND [0] AS wanted MATCH (a:CodeIntel) WHERE (a)-[:T {w:1 / wanted}]->() RETURN count(a)", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArithmeticError")
}
