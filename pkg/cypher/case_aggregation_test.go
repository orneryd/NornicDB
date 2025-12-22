package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCaseExpressionInAggregation tests CASE expressions inside aggregation functions
// Bug: count(CASE WHEN condition THEN 1 END) was returning total count instead of conditional count
func TestCaseExpressionInAggregation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup test data
	_, err := exec.Execute(ctx, `CREATE (e:Entry {status: 'approved', score: 90})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {status: 'approved', score: 75})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {status: 'approved', score: 60})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {status: 'reject', score: 85})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {status: 'reject', score: 50})`, nil)
	require.NoError(t, err)

	t.Run("count with CASE WHEN no ELSE", func(t *testing.T) {
		// count(CASE WHEN condition THEN 1 END) should only count matching rows
		// When condition is false, CASE returns NULL, and count() ignores NULLs
		result, err := exec.Execute(ctx, `
			MATCH (e:Entry)
			RETURN count(CASE WHEN e.status = 'approved' THEN 1 END) as approved
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Should be 3 (only approved entries), not 5 (all entries)
		approved := result.Rows[0][0]
		assert.Equal(t, int64(3), toInt64Value(approved), "count(CASE WHEN) should only count matching rows")
	})

	t.Run("sum with CASE WHEN ELSE 0", func(t *testing.T) {
		// sum(CASE WHEN condition THEN 1 ELSE 0 END) should work correctly
		result, err := exec.Execute(ctx, `
			MATCH (e:Entry)
			RETURN sum(CASE WHEN e.status = 'approved' THEN 1 ELSE 0 END) as approved
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		approved := result.Rows[0][0]
		assert.Equal(t, int64(3), toInt64Value(approved), "sum(CASE WHEN ELSE 0) should sum matching rows")
	})

	t.Run("multiple count CASE in same query", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Entry)
			RETURN 
				count(e) as total,
				count(CASE WHEN e.status = 'approved' THEN 1 END) as approved,
				count(CASE WHEN e.status = 'reject' THEN 1 END) as rejected,
				count(CASE WHEN e.score < 80 THEN 1 END) as lowScore
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		total := toInt64Value(result.Rows[0][0])
		approved := toInt64Value(result.Rows[0][1])
		rejected := toInt64Value(result.Rows[0][2])
		lowScore := toInt64Value(result.Rows[0][3])

		assert.Equal(t, int64(5), total, "total should be 5")
		assert.Equal(t, int64(3), approved, "approved should be 3")
		assert.Equal(t, int64(2), rejected, "rejected should be 2")
		assert.Equal(t, int64(3), lowScore, "lowScore should be 3 (scores: 75, 60, 50)")
	})

	t.Run("count CASE with compound condition", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Entry)
			RETURN count(CASE WHEN e.status = 'approved' AND e.score < 80 THEN 1 END) as approvedLowScore
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// approved entries with score < 80: score=75, score=60 = 2 entries
		approvedLowScore := toInt64Value(result.Rows[0][0])
		assert.Equal(t, int64(2), approvedLowScore, "should find 2 approved entries with low scores")
	})

	t.Run("count CASE with CONTAINS", func(t *testing.T) {
		// Create fresh store for this test to avoid interference
		baseStore2 := storage.NewMemoryEngine()
		store2 := storage.NewNamespacedEngine(baseStore2, "test")
		exec2 := NewStorageExecutor(store2)

		// Add entries with text content
		_, err := exec2.Execute(ctx, `CREATE (e:Entry {issues: 'informal tú usage'})`, nil)
		require.NoError(t, err)
		_, err = exec2.Execute(ctx, `CREATE (e:Entry {issues: 'other issue'})`, nil)
		require.NoError(t, err)
		_, err = exec2.Execute(ctx, `CREATE (e:Entry {issues: 'another tú problem'})`, nil)
		require.NoError(t, err)

		// First verify CONTAINS works in regular WHERE
		verifyResult, err := exec2.Execute(ctx, `
			MATCH (e:Entry)
			WHERE e.issues CONTAINS 'tú'
			RETURN count(e) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, verifyResult.Rows, 1)
		assert.Equal(t, int64(2), toInt64Value(verifyResult.Rows[0][0]), "WHERE CONTAINS should find 2 entries")

		// Test sum(CASE WHEN CONTAINS) - this should work since sum(CASE) works
		sumResult, err := exec2.Execute(ctx, `
			MATCH (e:Entry)
			RETURN sum(CASE WHEN e.issues CONTAINS 'tú' THEN 1 ELSE 0 END) as informalSum
		`, nil)
		require.NoError(t, err)
		require.Len(t, sumResult.Rows, 1)
		informalSum := toInt64Value(sumResult.Rows[0][0])
		assert.Equal(t, int64(2), informalSum, "sum(CASE WHEN CONTAINS) should find 2 entries with 'tú'")

		// Now test count(CASE WHEN CONTAINS)
		result, err := exec2.Execute(ctx, `
			MATCH (e:Entry)
			RETURN count(CASE WHEN e.issues CONTAINS 'tú' THEN 1 END) as informalCount
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		informalCount := toInt64Value(result.Rows[0][0])
		assert.Equal(t, int64(2), informalCount, "count(CASE WHEN CONTAINS) should find 2 entries with 'tú'")
	})
}

// TestUnionAllQuery tests UNION ALL queries
func TestUnionAllQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup
	_, err := exec.Execute(ctx, `CREATE (e:Entry {category: 'High', status: 'approved'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {category: 'High', status: 'approved'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {category: 'High', status: 'reject'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {category: 'Low', status: 'approved'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Entry {category: 'Low', status: 'reject'})`, nil)
	require.NoError(t, err)

	t.Run("UNION ALL with aggregation", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (e:Entry) WHERE e.status = 'approved'
			RETURN e.category as category, 'approved' as type, count(e) as count
			UNION ALL
			MATCH (e:Entry) WHERE e.status = 'reject'
			RETURN e.category as category, 'rejected' as type, count(e) as count
		`, nil)
		require.NoError(t, err)

		// Should have 4 rows: High/approved, Low/approved, High/rejected, Low/rejected
		// (or some subset depending on which combinations exist)
		require.GreaterOrEqual(t, len(result.Rows), 2, "should have multiple result rows from UNION ALL")

		// Verify columns are correct
		assert.Contains(t, result.Columns, "category")
		assert.Contains(t, result.Columns, "type")
		assert.Contains(t, result.Columns, "count")
	})
}

// TestEvaluateCaseExpressionDirectly tests evaluateCaseExpression directly
func TestEvaluateCaseExpressionDirectly(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Create a test node
	node := &storage.Node{
		ID:     "test-node",
		Labels: []string{"TestNode"},
		Properties: map[string]interface{}{
			"issues": "informal tú usage",
			"name":   "test",
		},
	}
	_, _ = store.CreateNode(node)

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		name     string
		expr     string
		expected interface{}
	}{
		{
			name:     "CASE with equality",
			expr:     "CASE WHEN n.name = 'test' THEN 'yes' ELSE 'no' END",
			expected: "yes",
		},
		{
			name:     "CASE with CONTAINS ascii",
			expr:     "CASE WHEN n.issues CONTAINS 'informal' THEN 'found' ELSE 'not found' END",
			expected: "found",
		},
		{
			name:     "CASE with CONTAINS unicode",
			expr:     "CASE WHEN n.issues CONTAINS 'tú' THEN 'found' ELSE 'not found' END",
			expected: "found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateCaseExpression(tt.expr, nodes, nil)
			assert.Equal(t, tt.expected, result, "evaluateCaseExpression(%q)", tt.expr)
		})
	}
}

// TestEvaluateConditionContains tests the evaluateCondition function with CONTAINS
func TestEvaluateConditionContains(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Create a test node
	node := &storage.Node{
		ID:     "test-node",
		Labels: []string{"TestNode"},
		Properties: map[string]interface{}{
			"issues": "informal tú usage",
			"name":   "test",
		},
	}
	_, _ = store.CreateNode(node)

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		name      string
		condition string
		expected  bool
	}{
		{
			name:      "CONTAINS with ascii",
			condition: "n.issues CONTAINS 'informal'",
			expected:  true,
		},
		{
			name:      "CONTAINS with unicode",
			condition: "n.issues CONTAINS 'tú'",
			expected:  true,
		},
		{
			name:      "CONTAINS not found",
			condition: "n.issues CONTAINS 'xyz'",
			expected:  false,
		},
		{
			name:      "equality check",
			condition: "n.name = 'test'",
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateCondition(tt.condition, nodes, nil)
			assert.Equal(t, tt.expected, result, "evaluateCondition(%q)", tt.condition)
		})
	}
}

// TestParseCaseExpressionUTF8 verifies CASE expression parsing with UTF-8 content
func TestParseCaseExpressionUTF8(t *testing.T) {
	tests := []struct {
		name              string
		expr              string
		expectedCondition string
		expectedResult    string
		expectedElse      string
	}{
		{
			name:              "CASE with CONTAINS and unicode",
			expr:              "CASE WHEN n.issues CONTAINS 'tú' THEN 'found' ELSE 'not found' END",
			expectedCondition: "n.issues CONTAINS 'tú'",
			expectedResult:    "'found'",
			expectedElse:      "'not found'",
		},
		{
			name:              "CASE with CONTAINS ascii",
			expr:              "CASE WHEN n.issues CONTAINS 'test' THEN 'found' ELSE 'not found' END",
			expectedCondition: "n.issues CONTAINS 'test'",
			expectedResult:    "'found'",
			expectedElse:      "'not found'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce, err := parseCaseExpression(tt.expr)
			require.NoError(t, err)
			require.Len(t, ce.whenClauses, 1)
			assert.Equal(t, tt.expectedCondition, ce.whenClauses[0].condition, "condition mismatch")
			assert.Equal(t, tt.expectedResult, ce.whenClauses[0].result, "result mismatch")
			assert.Equal(t, tt.expectedElse, ce.elseResult, "else mismatch")
		})
	}
}

// TestFindTopLevelKeywordUTF8 verifies UTF-8 handling in keyword detection
func TestFindTopLevelKeywordUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		keyword  string
		expected int
	}{
		{
			name:     "CONTAINS before unicode",
			input:    "n.issues CONTAINS 'tú'",
			keyword:  " CONTAINS ",
			expected: 8, // position of space before CONTAINS
		},
		{
			name:     "CONTAINS with ascii only",
			input:    "n.issues CONTAINS 'test'",
			keyword:  " CONTAINS ",
			expected: 8,
		},
		{
			name:     "keyword inside string should not match",
			input:    "'n.issues CONTAINS test'",
			keyword:  " CONTAINS ",
			expected: -1, // inside string literal
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findTopLevelKeyword(tt.input, tt.keyword)
			assert.Equal(t, tt.expected, result, "findTopLevelKeyword(%q, %q)", tt.input, tt.keyword)
		})
	}
}

// TestCaseConditionEvaluation tests the evaluateCondition function directly
func TestCaseConditionEvaluation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with issues property
	_, err := exec.Execute(ctx, `CREATE (n:TestNode {issues: 'informal tú usage', name: 'test'})`, nil)
	require.NoError(t, err)

	// Verify basic CASE WHEN works
	t.Run("CASE WHEN with equality", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.name = 'test' THEN 'yes' ELSE 'no' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "yes", result.Rows[0][0])
	})

	t.Run("CASE WHEN with CONTAINS ascii", func(t *testing.T) {
		// Test with ASCII-only content first
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.issues CONTAINS 'informal' THEN 'found' ELSE 'not found' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "found", result.Rows[0][0], "CASE WHEN CONTAINS (ascii) should work")
	})

	t.Run("CASE WHEN with CONTAINS unicode", func(t *testing.T) {
		// Test resolveReturnItem directly with the node we created
		nodes := exec.storage.GetAllNodes()

		var testNode *storage.Node
		for _, node := range nodes {
			for _, label := range node.Labels {
				if label == "TestNode" {
					testNode = node
					break
				}
			}
		}
		require.NotNil(t, testNode, "Should find TestNode")
		t.Logf("Found node with issues: %q", testNode.Properties["issues"])

		// Test evaluateExpression directly (this is what resolveReturnItem calls)
		expr := "CASE WHEN n.issues CONTAINS 'tú' THEN 'found' ELSE 'not found' END"
		directResult := exec.evaluateExpression(expr, "n", testNode)
		t.Logf("Direct evaluateExpression result: %v", directResult)
		assert.Equal(t, "found", directResult, "Direct evaluateExpression should work")

		// Now test via full query
		// First try without unicode to ensure CASE expression works at all in query context
		asciiResult, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.issues CONTAINS 'informal' THEN 'found' ELSE 'not found' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, asciiResult.Rows, 1)
		t.Logf("ASCII query result: %v", asciiResult.Rows[0][0])
		assert.Equal(t, "found", asciiResult.Rows[0][0], "CASE WHEN CONTAINS (ascii) in query should work")

		// Now test with unicode
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.issues CONTAINS 'tú' THEN 'found' ELSE 'not found' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Unicode query result: %v", result.Rows[0][0])
		assert.Equal(t, "found", result.Rows[0][0], "CASE WHEN CONTAINS (unicode) should work")
	})

	t.Run("CASE WHEN with STARTS WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.issues STARTS WITH 'informal' THEN 'found' ELSE 'not found' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "found", result.Rows[0][0], "CASE WHEN STARTS WITH should work")
	})

	t.Run("CASE WHEN with ENDS WITH", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:TestNode)
			RETURN CASE WHEN n.issues ENDS WITH 'usage' THEN 'found' ELSE 'not found' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "found", result.Rows[0][0], "CASE WHEN ENDS WITH should work")
	})
}

// Helper to convert various numeric types to int64
func toInt64Value(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case int32:
		return int64(val)
	default:
		return 0
	}
}

// TestUTF8StringOperations tests all string operations with UTF-8 content
func TestUTF8StringOperations(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with various UTF-8 content
	_, err := exec.Execute(ctx, `CREATE (n:Doc {text: 'Hello tú world'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Doc {text: '日本語テスト'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Doc {text: 'emoji 🎉 test'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Doc {text: 'café résumé'})`, nil)
	require.NoError(t, err)

	// Test CONTAINS with various UTF-8 strings
	t.Run("CONTAINS with Spanish character", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text CONTAINS 'tú' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("CONTAINS with Japanese characters", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text CONTAINS '日本' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("CONTAINS with emoji", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text CONTAINS '🎉' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("CONTAINS with French accents", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text CONTAINS 'café' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	// Test STARTS WITH with UTF-8
	t.Run("STARTS WITH UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text STARTS WITH '日本' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	// Test ENDS WITH with UTF-8
	t.Run("ENDS WITH UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text ENDS WITH 'résumé' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	// Test equality with UTF-8
	t.Run("equality with UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Doc) WHERE n.text = '日本語テスト' RETURN n.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
}

// TestCaseExpressionWithUTF8 tests CASE expressions with UTF-8 content in various positions
func TestCaseExpressionWithUTF8(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (n:Item {category: 'español', name: 'tú test'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {category: 'français', name: 'café'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Item {category: 'english', name: 'plain'})`, nil)
	require.NoError(t, err)

	t.Run("CASE with UTF-8 in comparison value", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN CASE WHEN n.category = 'español' THEN 'Spanish' ELSE 'Other' END as lang
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		// Count Spanish entries
		spanishCount := 0
		for _, row := range result.Rows {
			if row[0] == "Spanish" {
				spanishCount++
			}
		}
		assert.Equal(t, 1, spanishCount)
	})

	t.Run("CASE with UTF-8 in THEN result", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item) WHERE n.category = 'español'
			RETURN CASE WHEN n.name CONTAINS 'tú' THEN '¡Sí!' ELSE 'No' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "¡Sí!", result.Rows[0][0])
	})

	t.Run("CASE with UTF-8 in ELSE result", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item) WHERE n.category = 'english'
			RETURN CASE WHEN n.name CONTAINS 'xyz' THEN 'Found' ELSE '未找到' END as result
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "未找到", result.Rows[0][0])
	})

	t.Run("CASE with CONTAINS and UTF-8 search string", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Item)
			RETURN n.name, CASE WHEN n.name CONTAINS 'é' THEN 'has accent' ELSE 'no accent' END as hasAccent
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
	})
}

// TestAggregationWithUTF8 tests aggregation functions with UTF-8 content
func TestAggregationWithUTF8(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `CREATE (n:Product {type: 'café', price: 5})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Product {type: 'café', price: 6})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Product {type: 'thé', price: 4})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:Product {type: '日本茶', price: 10})`, nil)
	require.NoError(t, err)

	t.Run("GROUP BY with UTF-8 property", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Product)
			RETURN n.type as type, count(n) as cnt, sum(n.price) as total
			ORDER BY type
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3) // café, thé, 日本茶
	})

	t.Run("count with CASE and UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Product)
			RETURN count(CASE WHEN n.type = 'café' THEN 1 END) as cafeCount
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(2), toInt64Value(result.Rows[0][0]))
	})

	t.Run("sum with CASE and UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Product)
			RETURN sum(CASE WHEN n.type CONTAINS 'é' THEN n.price ELSE 0 END) as frenchTotal
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// café (5+6) + thé (4) = 15
		assert.Equal(t, int64(15), toInt64Value(result.Rows[0][0]))
	})

	t.Run("COLLECT with UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Product)
			WHERE n.type CONTAINS 'é'
			RETURN collect(n.type) as types
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		types := result.Rows[0][0].([]interface{})
		assert.Len(t, types, 3) // café, café, thé
	})
}

// TestMultipleReturnItemsWithUTF8 tests multiple RETURN items containing UTF-8
func TestMultipleReturnItemsWithUTF8(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Multi {a: 'tú', b: 'café', c: '日本'})`, nil)
	require.NoError(t, err)

	t.Run("multiple UTF-8 CASE expressions", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Multi)
			RETURN 
				CASE WHEN n.a CONTAINS 'tú' THEN 'es' ELSE 'other' END as lang1,
				CASE WHEN n.b CONTAINS 'café' THEN 'fr' ELSE 'other' END as lang2,
				CASE WHEN n.c CONTAINS '日本' THEN 'ja' ELSE 'other' END as lang3
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "es", result.Rows[0][0])
		assert.Equal(t, "fr", result.Rows[0][1])
		assert.Equal(t, "ja", result.Rows[0][2])
	})

	t.Run("mixed CASE and property access with UTF-8", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Multi)
			RETURN n.a, CASE WHEN n.b = 'café' THEN '✓' ELSE '✗' END as check, n.c
		`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "tú", result.Rows[0][0])
		assert.Equal(t, "✓", result.Rows[0][1])
		assert.Equal(t, "日本", result.Rows[0][2])
	})
}
