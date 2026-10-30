package cypher

// explain_plan_delivery_test.go — unit coverage for the Neo4j-shaped plan maps
// Bolt and HTTP deliver to clients (#744 §2): EXPLAIN carries the plan tree
// without runtime counters, PROFILE adds rows/dbHits, ordinary queries carry
// neither.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// collectNeo4jPlanOperatorTypes walks a plan map tree and returns every
// operatorType.
func collectNeo4jPlanOperatorTypes(node map[string]any) []string {
	types := []string{node["operatorType"].(string)}
	if children, ok := node["children"].([]map[string]any); ok {
		for _, child := range children {
			types = append(types, collectNeo4jPlanOperatorTypes(child)...)
		}
	}
	return types
}

func TestNeo4jPlanMapExplainAndProfile(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "plan-delivery")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Person {name: 'a'})", nil)
	require.NoError(t, err)

	t.Run("EXPLAIN plan map", func(t *testing.T) {
		result, err := exec.Execute(ctx, "EXPLAIN MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"n.name"}, result.Columns)
		require.Empty(t, result.Rows, "EXPLAIN runs nothing")

		rawPlan, ok := result.Metadata["plan"]
		require.True(t, ok, "EXPLAIN attaches the plan metadata")
		plan, ok := rawPlan.(*ExecutionPlan)
		require.True(t, ok)
		require.Equal(t, ModeExplain, plan.Mode)

		planMap := Neo4jPlanMap(plan, false)
		require.Equal(t, "ProduceResults", planMap["operatorType"], "root operator")
		args, ok := planMap["args"].(map[string]interface{})
		require.True(t, ok, "root carries args")
		require.Contains(t, args, "EstimatedRows")
		require.Contains(t, collectNeo4jPlanOperatorTypes(planMap), "NodeByLabelScan", "chain reaches the node scan")
		_, hasRows := planMap["rows"]
		require.False(t, hasRows, "EXPLAIN carries no runtime counters")
		_, hasDBHits := planMap["dbHits"]
		require.False(t, hasDBHits)
	})

	t.Run("PROFILE plan map", func(t *testing.T) {
		result, err := exec.Execute(ctx, "PROFILE MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"n.name"}, result.Columns)
		require.NotEmpty(t, result.Rows, "PROFILE executes the query")

		rawPlan, ok := result.Metadata["plan"]
		require.True(t, ok)
		plan, ok := rawPlan.(*ExecutionPlan)
		require.True(t, ok)
		require.Equal(t, ModeProfile, plan.Mode)

		planMap := Neo4jPlanMap(plan, true)
		require.Equal(t, "ProduceResults", planMap["operatorType"])
		_, hasRows := planMap["rows"]
		require.True(t, hasRows, "PROFILE carries rows")
		_, hasDBHits := planMap["dbHits"]
		require.True(t, hasDBHits, "PROFILE carries dbHits")
	})

	t.Run("ordinary queries carry no plan", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
		require.NoError(t, err)
		require.Nil(t, result.Metadata["plan"], "no plan metadata on ordinary queries")
	})
}
