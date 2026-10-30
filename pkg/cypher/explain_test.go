package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExecutionMode(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedMode  ExecutionMode
		expectedQuery string
	}{
		{
			name:          "normal query",
			query:         "MATCH (n) RETURN n",
			expectedMode:  ModeNormal,
			expectedQuery: "MATCH (n) RETURN n",
		},
		{
			name:          "EXPLAIN query",
			query:         "EXPLAIN MATCH (n) RETURN n",
			expectedMode:  ModeExplain,
			expectedQuery: "MATCH (n) RETURN n",
		},
		{
			name:          "PROFILE query",
			query:         "PROFILE MATCH (n) RETURN n",
			expectedMode:  ModeProfile,
			expectedQuery: "MATCH (n) RETURN n",
		},
		{
			name:          "lowercase explain",
			query:         "explain MATCH (n) RETURN n",
			expectedMode:  ModeExplain,
			expectedQuery: "MATCH (n) RETURN n",
		},
		{
			name:          "lowercase profile",
			query:         "profile MATCH (n) RETURN n",
			expectedMode:  ModeProfile,
			expectedQuery: "MATCH (n) RETURN n",
		},
		{
			name:          "explain with whitespace",
			query:         "  EXPLAIN   MATCH (n) RETURN n  ",
			expectedMode:  ModeExplain,
			expectedQuery: "MATCH (n) RETURN n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, query := parseExecutionMode(tt.query)
			require.Equal(t, tt.expectedMode, mode)
			require.Equal(t, tt.expectedQuery, query)
		})
	}
}

func TestExplainBasicQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	exec.Execute(ctx, `CREATE (n:Person {name: 'Alice', age: 30})`, nil)
	exec.Execute(ctx, `CREATE (n:Person {name: 'Bob', age: 25})`, nil)

	t.Run("EXPLAIN MATCH query", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, []string{"n"}, result.Columns)
		require.Len(t, result.Rows, 0)

		// Check that plan is returned
		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "EXPLAIN")
		require.Contains(t, planStr, "Query Plan")

		// Check metadata
		require.NotNil(t, result.Metadata)
		require.Equal(t, "EXPLAIN", result.Metadata["planType"])
	})

	t.Run("EXPLAIN with WHERE", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) WHERE n.age > 20 RETURN n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Filter")
	})

	t.Run("EXPLAIN with ORDER BY", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n ORDER BY n.age`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Sort")
	})

	t.Run("EXPLAIN with LIMIT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n LIMIT 10`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Limit")
	})

	t.Run("EXPLAIN with aggregation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN count(n)`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "EagerAggregation")
	})

	t.Run("EXPLAIN CALL procedure", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN CALL db.labels()`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "ProcedureCall")
		require.Contains(t, planStr, "db.labels")
	})

	t.Run("EXPLAIN returns projected columns and no rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n.name AS name`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name"}, result.Columns)
		require.Len(t, result.Rows, 0)
	})

	t.Run("EXPLAIN CALL YIELD RETURN * infers YIELD columns", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN CALL db.labels() YIELD label RETURN *`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"label"}, result.Columns)
		require.Len(t, result.Rows, 0)
	})
}

func TestProfileBasicQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some test data
	exec.Execute(ctx, `CREATE (n:Person {name: 'Alice', age: 30})`, nil)
	exec.Execute(ctx, `CREATE (n:Person {name: 'Bob', age: 25})`, nil)
	exec.Execute(ctx, `CREATE (n:Person {name: 'Carol', age: 35})`, nil)

	t.Run("PROFILE MATCH query", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) RETURN n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, []string{"n"}, result.Columns)
		require.Len(t, result.Rows, 3)

		// Check that plan is returned with statistics
		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "PROFILE")
		require.Contains(t, planStr, "Query Plan")
		require.Contains(t, planStr, "Total Time:")
		require.Contains(t, planStr, "Total Rows:")
		require.Contains(t, planStr, "Total DB Hits:")

		// Check metadata
		require.NotNil(t, result.Metadata)
		require.Equal(t, "PROFILE", result.Metadata["planType"])

		// Check plan object in metadata
		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.NotNil(t, plan)
		require.Equal(t, ModeProfile, plan.Mode)
		require.Greater(t, plan.TotalRows, int64(0))
	})

	t.Run("PROFILE with WHERE filters results", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) WHERE n.age > 28 RETURN n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := result.Metadata["plan"].(*ExecutionPlan)
		// Should only return Alice (30) and Carol (35)
		require.Equal(t, int64(2), plan.TotalRows)
	})

	t.Run("PROFILE with LIMIT", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) RETURN n LIMIT 1`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.Equal(t, int64(1), plan.TotalRows)
	})

	t.Run("PROFILE with aggregation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) RETURN count(n) AS cnt`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.Equal(t, int64(1), plan.TotalRows) // One aggregation result
	})
}

func TestExplainComplexQueries(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph
	exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (b:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)`, nil)

	t.Run("EXPLAIN relationship query", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Expand")
	})

	t.Run("EXPLAIN shortestPath", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH p = shortestPath((a:Person)-[*]-(b:Person)) RETURN p`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "ShortestPath")
	})

	t.Run("EXPLAIN CREATE", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN CREATE (n:Test {name: 'test'})`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Create")
	})

	t.Run("EXPLAIN MERGE", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MERGE (n:Test {name: 'test'})`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Merge")
	})

	t.Run("EXPLAIN DETACH DELETE includes DetachDelete operator", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) DETACH DELETE n`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "DetachDelete")
	})
}

func TestExplainPlanStructure(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("Plan has correct operator types", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) WHERE n.age > 25 RETURN n ORDER BY n.name LIMIT 10`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.NotNil(t, plan.Root)

		// Collect all operator types
		ops := collectOperatorTypes(plan.Root)

		// Should contain expected operators
		// Note: NodeByLabelScan is used because the WHERE is handled by Filter operator
		require.Contains(t, ops, "NodeByLabelScan")
		require.Contains(t, ops, "Filter")
		require.Contains(t, ops, "Sort")
		require.Contains(t, ops, "Limit")
		require.Contains(t, ops, "ProduceResults")
	})

	t.Run("Plan shows estimated rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n`, nil)
		require.NoError(t, err)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Estimated Rows:")
	})
}

func TestProfileDBHits(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some data
	for i := 0; i < 10; i++ {
		exec.Execute(ctx, `CREATE (n:Node {id: $id})`, map[string]interface{}{"id": i})
	}

	t.Run("Profile shows DB hits", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Node) RETURN n`, nil)
		require.NoError(t, err)

		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.Greater(t, plan.TotalDBHits, int64(0))
	})
}

func TestExplainNoExecution(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("EXPLAIN does not execute CREATE", func(t *testing.T) {
		// Count nodes before
		before, _ := store.AllNodes()
		beforeCount := len(before)

		// EXPLAIN CREATE
		_, err := exec.Execute(ctx, `EXPLAIN CREATE (n:TestNode {name: 'should_not_exist'})`, nil)
		require.NoError(t, err)

		// Count nodes after - should be same
		after, _ := store.AllNodes()
		afterCount := len(after)

		require.Equal(t, beforeCount, afterCount, "EXPLAIN should not create nodes")
	})

	t.Run("EXPLAIN does not execute DELETE", func(t *testing.T) {
		// Create a node
		exec.Execute(ctx, `CREATE (n:ToDelete {name: 'test'})`, nil)
		before, _ := store.AllNodes()
		beforeCount := len(before)

		// EXPLAIN DELETE
		_, err := exec.Execute(ctx, `EXPLAIN MATCH (n:ToDelete) DELETE n`, nil)
		require.NoError(t, err)

		// Node should still exist
		after, _ := store.AllNodes()
		afterCount := len(after)

		require.Equal(t, beforeCount, afterCount, "EXPLAIN should not delete nodes")
	})
}

// Helper to collect all operator types from a plan
func collectOperatorTypes(op *PlanOperator) []string {
	if op == nil {
		return nil
	}

	types := []string{op.OperatorType}
	for _, child := range op.Children {
		types = append(types, collectOperatorTypes(child)...)
	}
	return types
}

func TestExplainVsProfileOutput(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create data
	exec.Execute(ctx, `CREATE (n:Person {name: 'Alice'})`, nil)

	t.Run("EXPLAIN shows estimated rows only", func(t *testing.T) {
		result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) RETURN n`, nil)
		require.NoError(t, err)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Estimated Rows:")
		require.NotContains(t, planStr, "Actual:")
	})

	t.Run("PROFILE shows both estimated and actual", func(t *testing.T) {
		result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) RETURN n`, nil)
		require.NoError(t, err)

		planStr := planStringFromResult(t, result)
		require.Contains(t, planStr, "Est:")
		require.Contains(t, planStr, "Actual:")
		require.Contains(t, planStr, "Hits:")
	})
}

func planStringFromResult(t *testing.T, result *ExecuteResult) string {
	t.Helper()
	require.NotNil(t, result)
	require.NotNil(t, result.Metadata)
	planStr, ok := result.Metadata["planString"].(string)
	require.True(t, ok, "planString metadata must be present")
	return planStr
}

func TestAnalyzeNodeScan(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name         string
		query        string
		expectedType string
	}{
		{
			name:         "AllNodesScan for no label",
			query:        "MATCH (n) RETURN n",
			expectedType: "AllNodesScan",
		},
		{
			name:         "NodeByLabelScan for label only",
			query:        "MATCH (n:Person) RETURN n",
			expectedType: "NodeByLabelScan",
		},
		{
			name:         "NodeIndexSeek for label with property",
			query:        "MATCH (n:Person {name: 'Alice'}) RETURN n",
			expectedType: "NodeIndexSeek",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := exec.analyzeNodeScan(originalBraceView(tt.query))
			require.Equal(t, tt.expectedType, op.OperatorType)
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello world", 8, "hello..."},
		{"hi", 2, "hi"},
		{"", 5, ""},
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.maxLen)
		expectedPrefix := tt.expected
		if len(expectedPrefix) > 3 {
			expectedPrefix = expectedPrefix[:3]
		}
		if !strings.HasPrefix(result, expectedPrefix) {
			t.Errorf("truncate(%q, %d) = %q, want prefix of %q", tt.input, tt.maxLen, result, tt.expected)
		}
	}

	// truncateQuery helper: trims spaces and only appends ellipsis when needed.
	assert.Equal(t, "MATCH (n) RETURN n", truncateQuery("  MATCH (n) RETURN n  ", 64))
	assert.Equal(t, "MATCH...", truncateQuery("MATCH (n) RETURN n", 5))
}

func TestExplainHelpers_InferColumnsAndDBHitsBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// YIELD * path.
	cols := exec.StatementColumns("CALL db.labels() YIELD *")
	require.NotNil(t, cols)

	// YIELD + RETURN * path (projects yield aliases/names).
	cols = exec.StatementColumns("CALL db.info() YIELD name AS dbName RETURN *")
	require.Equal(t, []string{"dbName"}, cols)

	// RETURN parsing path.
	cols = exec.StatementColumns("MATCH (n) RETURN n.name AS name, n.age")
	require.Equal(t, []string{"name", "n.age"}, cols)

	// estimateDBHits operator-type switch coverage.
	plan := &PlanOperator{
		OperatorType:  "AllNodesScan",
		EstimatedRows: 5,
		Children: []*PlanOperator{
			{OperatorType: "NodeIndexSeek", EstimatedRows: 4},
			{OperatorType: "Filter", EstimatedRows: 3},
			{OperatorType: "ShortestPath", EstimatedRows: 2},
		},
	}
	hits := exec.estimateDBHits(plan)
	require.Greater(t, hits, int64(0))
	require.Equal(t, hits, plan.DBHits)
}

func TestExplainHelpers_InferColumnsAndDBHits_MoreBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// YIELD items without RETURN should infer yielded column names.
	cols := exec.StatementColumns("CALL db.info() YIELD name, version")
	require.Equal(t, []string{"name", "version"}, cols)

	// YIELD aliases without RETURN should use aliases.
	cols = exec.StatementColumns("CALL db.info() YIELD name AS dbName, version AS ver")
	require.Equal(t, []string{"dbName", "ver"}, cols)

	// Nil plan branch.
	assert.Equal(t, int64(0), exec.estimateDBHits(nil))

	// Cover remaining operator-specific cost branches and recursive summation.
	plan := &PlanOperator{
		OperatorType:  "NodeByLabelScan",
		EstimatedRows: 3,
		Children: []*PlanOperator{
			{OperatorType: "AllNodesScan", EstimatedRows: 2},
			{OperatorType: "Expand", EstimatedRows: 4},
			{OperatorType: "Custom", EstimatedRows: 5},
		},
	}
	hits := exec.estimateDBHits(plan)
	// 3*2 + 2*2 + 4*3 + 5 = 27
	require.Equal(t, int64(27), hits)
	require.Equal(t, int64(27), plan.DBHits)
}
