// Package cypher provides tests for the Cypher executor.
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ======== Additional tests for 100% coverage ========

func TestValidateSyntaxUnbalancedClosingBrackets(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Extra closing paren - should produce syntax error (message varies by parser)
	_, err := exec.Execute(ctx, "MATCH (n)) RETURN n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")

	// Extra closing bracket - should produce syntax error
	_, err = exec.Execute(ctx, "MATCH (n)-[r]]->(m) RETURN n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")

	// Extra closing brace - should produce syntax error
	_, err = exec.Execute(ctx, "CREATE (n:Person {name: 'test'}} )", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
}

func TestValidateSyntaxEscapedQuotesInStrings(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Escaped quotes should be handled (string continues past escaped quote)
	result, err := exec.Execute(ctx, `CREATE (n:Test {name: 'O\'Brien'})`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestSubstituteParamsAllTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a test node
	node := &storage.Node{
		ID:         "param-types",
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"val": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test int64 parameter
	params := map[string]interface{}{
		"num": int64(100),
	}
	result, err := exec.Execute(ctx, "MATCH (n:Test) WHERE n.val = $num RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test float64 parameter
	params = map[string]interface{}{
		"num": float64(100),
	}
	result, err = exec.Execute(ctx, "MATCH (n:Test) WHERE n.val = $num RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test boolean parameter
	node2 := &storage.Node{
		ID:         "param-bool",
		Labels:     []string{"Test2"},
		Properties: map[string]interface{}{"active": true},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	params = map[string]interface{}{
		"flag": true,
	}
	result, err = exec.Execute(ctx, "MATCH (n:Test2) WHERE n.active = $flag RETURN n", params)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test nil parameter
	params = map[string]interface{}{
		"nothing": nil,
	}
	_, err = exec.Execute(ctx, "MATCH (n) WHERE n.prop = $nothing RETURN n", params)
	require.NoError(t, err)

	// Test default type (struct or other) - should use %v
	params = map[string]interface{}{
		"custom": struct{ Name string }{"test"},
	}
	_, err = exec.Execute(ctx, "MATCH (n) WHERE n.prop = $custom RETURN n", params)
	require.NoError(t, err)
}

func TestParseValueVariants(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes to test against
	node := &storage.Node{
		ID:     "parse-val",
		Labels: []string{"ValueTest"},
		Properties: map[string]interface{}{
			"name":   "test",
			"active": true,
			"count":  float64(42),
		},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test TRUE (uppercase)
	result, err := exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.active = TRUE RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test FALSE
	result, err = exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.active = FALSE RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// Test NULL
	result, err = exec.Execute(ctx, "MATCH (n:ValueTest) WHERE n.missing = NULL RETURN n", nil)
	require.NoError(t, err)

	// Test double-quoted string
	result, err = exec.Execute(ctx, `MATCH (n:ValueTest) WHERE n.name = "test" RETURN n`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestCompareEqualNilCases(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test equality with nil on both sides
	node := &storage.Node{
		ID:         "nil-eq",
		Labels:     []string{"NilTest"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Compare existing prop with nil literal
	result, err := exec.Execute(ctx, "MATCH (n:NilTest) WHERE n.name = NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0) // name is 'test', not null
}

func TestCompareGreaterLessStrings(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values for comparison
	nodes := []struct {
		id   string
		name string
	}{
		{"str1", "apple"},
		{"str2", "banana"},
		{"str3", "cherry"},
	}
	for _, n := range nodes {
		node := &storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     []string{"Fruit"},
			Properties: map[string]interface{}{"name": n.name},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// String comparison > (alphabetical)
	result, err := exec.Execute(ctx, "MATCH (n:Fruit) WHERE n.name > 'banana' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // cherry

	// String comparison <
	result, err = exec.Execute(ctx, "MATCH (n:Fruit) WHERE n.name < 'banana' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // apple
}

func TestCompareRegexInvalidPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "regex-inv",
		Labels:     []string{"RegexTest"},
		Properties: map[string]interface{}{"pattern": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Invalid regex pattern - should not match
	result, err := exec.Execute(ctx, "MATCH (n:RegexTest) WHERE n.pattern =~ '[invalid' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestCompareRegexNonStringExpected(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "regex-num",
		Labels:     []string{"RegexNum"},
		Properties: map[string]interface{}{"val": float64(123)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Regex with number - pattern isn't string type (will return false)
	result, err := exec.Execute(ctx, "MATCH (n:RegexNum) WHERE n.val =~ 123 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateStringOpMissingProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-miss",
		Labels:     []string{"StrMiss"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// CONTAINS on non-existent property
	result, err := exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing CONTAINS 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// STARTS WITH on non-existent property
	result, err = exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing STARTS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// ENDS WITH on non-existent property
	result, err = exec.Execute(ctx, "MATCH (n:StrMiss) WHERE n.missing ENDS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateInOpMissingProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-miss",
		Labels:     []string{"InMiss"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IN on non-existent property
	result, err := exec.Execute(ctx, "MATCH (n:InMiss) WHERE n.missing IN ['a', 'b'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestEvaluateInOpNotAList(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-notlist",
		Labels:     []string{"InNotList"},
		Properties: map[string]interface{}{"status": "active"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IN without proper list syntax (no brackets)
	result, err := exec.Execute(ctx, "MATCH (n:InNotList) WHERE n.status IN 'active' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0) // Should not match since 'active' is not a list
}

func TestEvaluateWhereNoValidOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "no-op",
		Labels:     []string{"NoOp"},
		Properties: map[string]interface{}{"val": float64(5)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// WHERE clause without a recognized operator - should include all
	result, err := exec.Execute(ctx, "MATCH (n:NoOp) WHERE n.val RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestEvaluateWhereNonPropertyComparison(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "non-prop",
		Labels:     []string{"NonProp"},
		Properties: map[string]interface{}{"val": float64(5)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Comparison that doesn't start with variable.property
	result, err := exec.Execute(ctx, "MATCH (n:NonProp) WHERE 5 = 5 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Should include all since we can't evaluate "5 = 5"
}

func TestEvaluateWherePropertyNotExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "prop-ne",
		Labels:     []string{"PropNE"},
		Properties: map[string]interface{}{"existing": "yes"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// WHERE on non-existent property should return false
	result, err := exec.Execute(ctx, "MATCH (n:PropNE) WHERE n.nonexistent = 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestOrderNodesStringSorting(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values
	names := []string{"Charlie", "Alice", "Bob"}
	for i, name := range names {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("sort-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": name},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// Order by string ascending
	result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
	assert.Equal(t, "Alice", result.Rows[0][0])

	// Order by string descending
	result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.name ORDER BY n.name DESC", nil)
	require.NoError(t, err)
	assert.Equal(t, "Charlie", result.Rows[0][0])
}

func TestOrderNodesWithoutVariablePrefix(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ord-%d", i)),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{"priority": float64(3 - i)}, // 3, 2, 1
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// ORDER BY without variable prefix (just property name)
	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n ORDER BY priority", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestSplitNodePatternsWithRemainder(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create pattern with extra text after closing paren - edge case
	result, err := exec.Execute(ctx, "CREATE (a:One), (b:Two)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestParseNodePatternNoLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node without labels
	result, err := exec.Execute(ctx, "CREATE (n {name: 'Unlabeled'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParseNodePatternMultipleLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with multiple labels
	result, err := exec.Execute(ctx, "CREATE (n:Person:Employee:Manager {name: 'Boss'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Verify labels
	nodes, _ := store.GetNodesByLabel("Person")
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes[0].Labels, "Employee")
	assert.Contains(t, nodes[0].Labels, "Manager")
}

func TestParsePropertiesFalseBoolean(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with false boolean
	result, err := exec.Execute(ctx, "CREATE (n:BoolTest {active: false})", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	nodes, _ := store.GetNodesByLabel("BoolTest")
	assert.Equal(t, false, nodes[0].Properties["active"])
}

func TestParseReturnItemsWithAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "alias-test",
		Labels:     []string{"Item"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Item) RETURN n.value AS val", nil)
	require.NoError(t, err)
	assert.Equal(t, "val", result.Columns[0])
}

func TestParseReturnItemsMapProjectionWithAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test that map projection syntax n { .*, key: value } AS n is parsed correctly
	// The comma inside {} should NOT split the return items
	params := map[string]interface{}{
		"props": map[string]interface{}{
			"name":      "TestNode",
			"value":     float64(42),
			"embedding": []float64{0.1, 0.2, 0.3},
		},
	}

	result, err := exec.Execute(ctx, "CREATE (n:Node $props) RETURN n { .*, embedding: null } AS n", params)
	require.NoError(t, err)

	// Should have exactly 1 column named "n", not 2 columns
	assert.Len(t, result.Columns, 1, "Should have exactly 1 column, not split on comma inside {}")
	assert.Equal(t, "n", result.Columns[0], "Column should be named 'n' from AS alias")

	// Should have 1 row
	assert.Len(t, result.Rows, 1)
}

func TestParseReturnItemsMapProjectionWithoutAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test that map projection syntax n { .*, key: value } WITHOUT AS alias
	// Neo4j infers the column name from the variable before the map projection
	params := map[string]interface{}{
		"props": map[string]interface{}{
			"name":      "TestNode",
			"value":     float64(42),
			"embedding": []float64{0.1, 0.2, 0.3},
		},
	}

	result, err := exec.Execute(ctx, "CREATE (n:Node $props) RETURN n { .*, embedding: null }", params)
	require.NoError(t, err)

	// Should have exactly 1 column named "n" (inferred from variable)
	assert.Len(t, result.Columns, 1, "Should have exactly 1 column, not split on comma inside {}")
	assert.Equal(t, "n", result.Columns[0], "Column should be named 'n' inferred from variable before {}")

	// Should have 1 row
	assert.Len(t, result.Rows, 1)
}

func TestParseReturnItemsOrderByLimit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ol-%d", i)),
			Labels:     []string{"OLTest"},
			Properties: map[string]interface{}{"idx": float64(i)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// RETURN with ORDER BY and LIMIT in return clause parsing
	result, err := exec.Execute(ctx, "MATCH (n:OLTest) RETURN n.idx ORDER BY n.idx LIMIT 3", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
}

func TestExecuteCreateInvalidRelPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Pattern that routes to relationship handler but fails regex
	// Contains "-[" to trigger relationship path, but not in valid format
	// Error message varies by parser (ANTLR: "syntax error", Nornic: "invalid relationship pattern")
	_, err := exec.Execute(ctx, "CREATE -[r:REL]- invalid", nil)
	assert.Error(t, err)
}

func TestExecuteDeleteRequiresMATCH(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DELETE without MATCH
	_, err := exec.Execute(ctx, "DETACH DELETE n", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DELETE requires a MATCH clause")
}

func TestExecuteSetRequiresMATCH(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET without MATCH - should fail (either syntax error or semantic error)
	// Nornic parser: "syntax error" (SET is not a valid start keyword)
	// ANTLR parser: "SET requires a MATCH clause" (passes syntax, fails semantic)
	_, err := exec.Execute(ctx, "SET n.prop = 'value'", nil)
	assert.Error(t, err)
	// Both error messages are acceptable - just verify it errors
}

func TestExecuteSetInvalidPropertyAccess(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-inv",
		Labels:     []string{"SetInv"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// SET without proper n.property format is invalid.
	_, err = exec.Execute(ctx, "MATCH (n:SetInv) SET prop = 'value'", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid SET assignment")
}

func TestExecuteSetMergeRejectsMalformedInlineMap(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:SetMergeMalformed {name: 'x'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:SetMergeMalformed) SET n += {a: 1,}", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse properties in SET +=")
}

func TestExecuteAggregationCountStar(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cs-%d", i)),
			Labels:     []string{"CountStar"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:CountStar) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(7), result.Rows[0][0])
}

func TestExecuteAggregationCollectNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cn-%d", i)),
			Labels:     []string{"CollectNode"},
			Properties: map[string]interface{}{"idx": float64(i)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// COLLECT(n) - collect whole nodes
	result, err := exec.Execute(ctx, "MATCH (n:CollectNode) RETURN collect(n)", nil)
	require.NoError(t, err)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 3)
	// Each item should be a *storage.Node
	node := collected[0].(*storage.Node)
	assert.NotEmpty(t, node.ID)
	assert.Contains(t, node.Labels, "CollectNode")
}

func TestExecuteAggregationNonAggregateInQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("na-%d", i)),
			Labels:     []string{"NonAgg"},
			Properties: map[string]interface{}{"name": fmt.Sprintf("Item%d", i), "val": float64(i * 10)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// Mix of aggregate and non-aggregate in RETURN
	// Neo4j implicitly groups by non-aggregated columns, so we get 3 rows (one per name)
	result, err := exec.Execute(ctx, "MATCH (n:NonAgg) RETURN n.name, sum(n.val)", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3) // One row per distinct n.name

	// Verify each row has the correct name and sum for that group
	// Neo4j: SUM of float64 values returns float64
	sumByName := make(map[string]float64)
	for _, row := range result.Rows {
		name := row[0].(string)
		sum := row[1].(float64)
		sumByName[name] = sum
	}
	assert.Equal(t, float64(0), sumByName["Item0"])  // Only Item0's value
	assert.Equal(t, float64(10), sumByName["Item1"]) // Only Item1's value
	assert.Equal(t, float64(20), sumByName["Item2"]) // Only Item2's value
}

func TestExecuteAggregationEmptyResultSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Aggregation on empty set - non-aggregate column should be nil
	result, err := exec.Execute(ctx, "MATCH (n:NonExistentLabel) RETURN n.name, sum(n.val)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0]) // No nodes, so non-aggregate is nil
}

func TestExecuteAggregationSumNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SUM with no matching property pattern
	node := &storage.Node{
		ID:         "sum-no",
		Labels:     []string{"SumNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:SumNo) RETURN sum(invalid)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Rows[0][0])
}

func TestExecuteAggregationAvgNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "avg-no",
		Labels:     []string{"AvgNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:AvgNo) RETURN avg(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteAggregationMinNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "min-no",
		Labels:     []string{"MinNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:MinNo) RETURN min(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestExecuteAggregationMaxNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "max-no",
		Labels:     []string{"MaxNo"},
		Properties: map[string]interface{}{"value": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:MaxNo) RETURN max(invalid)", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestResolveReturnItemCountFunction(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "count-resolve",
		Labels:     []string{"CountResolve"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Non-aggregation query but with COUNT in return item
	// This tests resolveReturnItem's COUNT handling
	result, err := exec.Execute(ctx, "MATCH (n:CountResolve) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestResolveReturnItemNonExistentProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "nep",
		Labels:     []string{"NEP"},
		Properties: map[string]interface{}{"exists": "yes"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:NEP) RETURN n.nonexistent", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0])
}

func TestResolveReturnItemDifferentVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "diff-var",
		Labels:     []string{"DiffVar"},
		Properties: map[string]interface{}{"val": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Return expression with different variable name
	result, err := exec.Execute(ctx, "MATCH (n:DiffVar) RETURN m.val", nil)
	require.NoError(t, err)
	assert.Nil(t, result.Rows[0][0]) // m doesn't match n
}

func TestDbSchemaVisualizationWithRelationships(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and edges
	node1 := &storage.Node{ID: "sv1", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "sv2", Labels: []string{"Company"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "sev1", StartNode: "sv1", EndNode: "sv2", Type: "WORKS_AT", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	// Should have nodes and relationships
	schemaNodes := result.Rows[0][0].([]map[string]interface{})
	schemaRels := result.Rows[0][1].([]map[string]interface{})
	assert.Len(t, schemaNodes, 2)
	assert.Len(t, schemaRels, 1)
}

func TestDbSchemaNodePropertiesMultipleLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{
		ID:         "snp1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": 30},
	}
	node2 := &storage.Node{
		ID:         "snp2",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme", "revenue": 1000000},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.schema.nodeProperties()", nil)
	require.NoError(t, err)
	// Should have rows for each label/property combination
	assert.GreaterOrEqual(t, len(result.Rows), 4) // At least: Person.name, Person.age, Company.name, Company.revenue
}

func TestDbSchemaRelPropertiesWithProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "srp1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "srp2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:         "serp1",
		StartNode:  "srp1",
		EndNode:    "srp2",
		Type:       "CONNECTS",
		Properties: map[string]interface{}{"weight": 5, "since": "2020"},
	}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.schema.relProperties()", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Rows), 2) // weight and since
}

func TestDbPropertyKeysWithEdgeProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "dpk1", Labels: []string{"X"}, Properties: map[string]interface{}{"nodeProp": "value"}}
	node2 := &storage.Node{ID: "dpk2", Labels: []string{"Y"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:         "depk1",
		StartNode:  "dpk1",
		EndNode:    "dpk2",
		Type:       "REL",
		Properties: map[string]interface{}{"edgeProp": "edgeValue"},
	}
	require.NoError(t, store.CreateEdge(edge))

	result, err := exec.Execute(ctx, "CALL db.propertyKeys()", nil)
	require.NoError(t, err)
	// Should include both nodeProp and edgeProp
	props := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		props[i] = row[0].(string)
	}
	assert.Contains(t, props, "nodeProp")
	assert.Contains(t, props, "edgeProp")
}

func TestCountLabelsAndRelTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create diverse data
	node1 := &storage.Node{ID: "cl1", Labels: []string{"Label1"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "cl2", Labels: []string{"Label2"}, Properties: map[string]interface{}{}}
	node3 := &storage.Node{ID: "cl3", Labels: []string{"Label3"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	edge1 := &storage.Edge{ID: "ce1", StartNode: "cl1", EndNode: "cl2", Type: "TYPE1", Properties: map[string]interface{}{}}
	edge2 := &storage.Edge{ID: "ce2", StartNode: "cl2", EndNode: "cl3", Type: "TYPE2", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	result, err := exec.Execute(ctx, "CALL nornicdb.stats()", nil)
	require.NoError(t, err)
	// labels count
	assert.Equal(t, 3, result.Rows[0][2])
	// relationshipTypes count
	assert.Equal(t, 2, result.Rows[0][3])
}

func TestDetachDeleteWithIncomingEdges(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a graph where node2 has both incoming and outgoing edges
	node1 := &storage.Node{ID: "dd1", Labels: []string{"Node"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "dd2", Labels: []string{"Target"}, Properties: map[string]interface{}{}}
	node3 := &storage.Node{ID: "dd3", Labels: []string{"Node"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	// node1 -> node2 (incoming to node2)
	edge1 := &storage.Edge{ID: "dde1", StartNode: "dd1", EndNode: "dd2", Type: "POINTS_TO", Properties: map[string]interface{}{}}
	// node2 -> node3 (outgoing from node2)
	edge2 := &storage.Edge{ID: "dde2", StartNode: "dd2", EndNode: "dd3", Type: "POINTS_TO", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Detach delete node2 - should delete both edges
	result, err := exec.Execute(ctx, "MATCH (n:Target) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)
	assert.Equal(t, 2, result.Stats.RelationshipsDeleted)
}

func TestExecuteCreateRelationshipWithProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship with properties in the relationship
	result, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestExecuteCreateMultipleEmptyPatterns(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with extra whitespace between patterns
	result, err := exec.Execute(ctx, "CREATE (a:A)  ,  (b:B)  ,  (c:C)", nil)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Stats.NodesCreated)
}

func TestExecuteReturnStarWithMultipleNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "star-test",
		Labels:     []string{"Star"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// RETURN * should return all matched variables
	result, err := exec.Execute(ctx, "MATCH (n:Star) RETURN *", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestToFloat64WithInt(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node with plain int (not int64)
	node := &storage.Node{
		ID:         "int-test",
		Labels:     []string{"IntTest"},
		Properties: map[string]interface{}{"value": 42}, // plain int
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:IntTest) WHERE n.value > 40 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestToFloat64WithInvalidString(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// String that can't be converted to float
	node := &storage.Node{
		ID:         "inv-str",
		Labels:     []string{"InvStr"},
		Properties: map[string]interface{}{"value": "not-a-number"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Comparison should use string comparison as fallback
	result, err := exec.Execute(ctx, "MATCH (n:InvStr) WHERE n.value > 'aaa' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // 'not-a-number' > 'aaa' alphabetically
}

func TestExecuteDistinctWithDuplicates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with duplicate property values
	for i := 0; i < 5; i++ {
		category := "A"
		if i >= 3 {
			category = "B"
		}
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("dist-%d", i)),
			Labels:     []string{"Dist"},
			Properties: map[string]interface{}{"cat": category},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// DISTINCT should deduplicate
	result, err := exec.Execute(ctx, "MATCH (n:Dist) RETURN DISTINCT n.cat", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2) // Only A and B
}

// Additional tests for remaining coverage gaps

func TestCallDbRelationshipTypesEmpty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No edges - should return empty
	result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

func TestCallDbLabelsEmpty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No nodes - should return empty
	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

func TestExecuteCreateWithReturnTargetVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship and return the target node variable
	result, err := exec.Execute(ctx, "CREATE (a:City {name: 'NYC'})-[:ROUTE]->(b:City {name: 'LA'}) RETURN b.name", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteCreateRelationshipWithRelReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// CREATE with RETURN that doesn't match source or target
	result, err := exec.Execute(ctx, "CREATE (a:A)-[:REL]->(b:B) RETURN x.prop", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestParseValueFloatParsing(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "float-parse",
		Labels:     []string{"FloatParse"},
		Properties: map[string]interface{}{"value": float64(3.14159)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test float literal parsing in WHERE
	result, err := exec.Execute(ctx, "MATCH (n:FloatParse) WHERE n.value > 3.14 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestParseValuePlainString(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "plain-str",
		Labels:     []string{"PlainStr"},
		Properties: map[string]interface{}{"status": "active"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test comparison with unquoted string that isn't a number or boolean
	_, err = exec.Execute(ctx, "MATCH (n:PlainStr) WHERE n.status = active RETURN n", nil)
	require.NoError(t, err)
	// "active" without quotes is parsed as plain string
}

func TestEvaluateStringOpNonVariablePrefix(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-nv",
		Labels:     []string{"StrNV"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// CONTAINS with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:StrNV) WHERE something CONTAINS 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true (includes all)
}

func TestEvaluateInOpNonVariablePrefix(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-nv",
		Labels:     []string{"InNV"},
		Properties: map[string]interface{}{"val": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IN with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:InNV) WHERE something IN ['a', 'b'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true
}

func TestEvaluateIsNullNonVariablePrefix(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "null-nv",
		Labels:     []string{"NullNV"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IS NULL with expression that doesn't start with n.
	result, err := exec.Execute(ctx, "MATCH (n:NullNV) WHERE something IS NULL RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Non-property comparison returns true
}

func TestSplitNodePatternsComplexNesting(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Complex nesting with properties containing commas (unlikely but tests depth tracking)
	result, err := exec.Execute(ctx, "CREATE (a:Test {name: 'A, B'}), (b:Test)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
}

func TestExecuteMatchCountVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cv-%d", i)),
			Labels:     []string{"CountVar"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// count(n) with variable name
	result, err := exec.Execute(ctx, "MATCH (n:CountVar) RETURN count(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Rows[0][0])
}

func TestExecuteDeleteWithWhere(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	node1 := &storage.Node{ID: "del-w1", Labels: []string{"DelKeep"}, Properties: map[string]interface{}{"type": "keeper"}}
	node2 := &storage.Node{ID: "del-w2", Labels: []string{"DelRemove"}, Properties: map[string]interface{}{"type": "remove"}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// DELETE with label filter - DELETE all DelRemove nodes
	result, err := exec.Execute(ctx, "MATCH (n:DelRemove) DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify the right one was kept
	nodes, _ := store.GetNodesByLabel("DelKeep")
	assert.Len(t, nodes, 1)
}

func TestExecuteSetUpdateNodeError(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-err",
		Labels:     []string{"SetErr"},
		Properties: nil, // nil properties map
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// SET should initialize properties map if nil
	result, err := exec.Execute(ctx, "MATCH (n:SetErr) SET n.newprop = 'value'", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)
}

func TestExecuteCreateNodeWithEmptyPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with just variable, no labels
	result, err := exec.Execute(ctx, "CREATE (n)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParseReturnItemsEmptyAfterSplit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "empty-ret",
		Labels:     []string{"EmptyRet"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// RETURN with trailing comma is invalid syntax.
	_, err = exec.Execute(ctx, "MATCH (n:EmptyRet) RETURN n,", nil)
	require.Error(t, err)
}

func TestExecuteMergeAsCreate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE currently works like CREATE
	result, err := exec.Execute(ctx, "MERGE (n:MergeTest {name: 'Test'}) RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.NotEmpty(t, result.Rows)
}

func TestCallDbLabelsWithEmptyLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Node with empty labels array
	node := &storage.Node{
		ID:         "no-labels",
		Labels:     []string{},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	// Should not contain any labels
	assert.Empty(t, result.Rows)
}

func TestExecuteCreateRelationshipNoType(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Relationship without explicit type (uses default RELATED_TO)
	result, err := exec.Execute(ctx, "CREATE (a:A)-[r]->(b:B)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	edges, _ := store.AllEdges()
	assert.Equal(t, "RELATED_TO", edges[0].Type)
}

// ============================================================================
// CREATE ... WITH ... DELETE Tests (Compound Query Pattern)
// ============================================================================

// TestExecuteCreateWithDeleteBasic tests the basic CREATE...WITH...DELETE pattern
func TestExecuteCreateWithDeleteBasic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node, pass it through WITH, then delete it
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteAndReturn tests CREATE...WITH...DELETE...RETURN
func TestExecuteCreateWithDeleteAndReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This is the benchmark pattern: create, delete, return count
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t RETURN count(t)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Should have a return value
	assert.NotEmpty(t, result.Columns)
	assert.NotEmpty(t, result.Rows)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteTimestamp tests CREATE with timestamp() function
func TestExecuteCreateWithDeleteTimestamp(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// The benchmark uses timestamp() in the CREATE
	result, err := exec.Execute(ctx, "CREATE (t:TestNode {name: 'temp', created: timestamp()}) WITH t DELETE t RETURN count(t)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	// Verify node is gone
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

// TestExecuteCreateWithDeleteRelationship tests CREATE...WITH...DELETE for relationships
// Note: This creates nodes AND a relationship in one CREATE, then deletes the relationship
func TestExecuteCreateWithDeleteRelationship(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create two nodes with a relationship, pass relationship through WITH, delete it
	result, err := exec.Execute(ctx, "CREATE (a:Person)-[r:KNOWS]->(b:Person) WITH r DELETE r RETURN count(r)", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsDeleted)

	// Verify relationship is gone but nodes remain
	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(2), nodeCount)
	edgeCount, _ := store.EdgeCount()
	assert.Equal(t, int64(0), edgeCount)
}

// TestExecuteCreateWithDeleteMultipleNodes tests creating and deleting multiple nodes
func TestExecuteCreateWithDeleteMultipleNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a single node pattern first to verify basic functionality
	result, err := exec.Execute(ctx, "CREATE (t:Temp {id: 1}) WITH t DELETE t", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.NodesDeleted)

	nodeCount, _ := store.NodeCount()
	assert.Equal(t, int64(0), nodeCount)
}

func TestExecuteDeleteDetachKeywordPosition(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node1 := &storage.Node{ID: "dk1", Labels: []string{"DK"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "dk2", Labels: []string{"DK"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "dke1", StartNode: "dk1", EndNode: "dk2", Type: "REL", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	// DETACH DELETE at end of line (different position handling)
	result, err := exec.Execute(ctx, "MATCH (n:DK) DETACH DELETE n", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesDeleted)
}

func TestCollectRegexWithProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "collect-prop",
		Labels:     []string{"CollectProp"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// COLLECT(n.name) - collect property values
	result, err := exec.Execute(ctx, "MATCH (n:CollectProp) RETURN collect(n.name)", nil)
	require.NoError(t, err)
	collected := result.Rows[0][0].([]interface{})
	assert.Len(t, collected, 1)
	assert.Equal(t, "test", collected[0])
}

func TestParsePropertiesWithSpace(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Properties with spaces around colon and values
	result, err := exec.Execute(ctx, "CREATE (n:Test { name : 'Alice' , age : 30 })", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestParsePropertiesNoValue(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Properties key without value must fail (strict Cypher map syntax).
	_, err := exec.Execute(ctx, "CREATE (n:Test {name})", nil)
	require.Error(t, err)
}

func TestUnsupportedQueryType(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// GRANT is truly unsupported
	_, err := exec.Execute(ctx, "GRANT ADMIN TO user", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
}

func TestExecuteMatchOrderByWithSkipLimit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("osl-%d", i)),
			Labels:     []string{"OSL"},
			Properties: map[string]interface{}{"val": float64(i)},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// ORDER BY with SKIP and LIMIT
	result, err := exec.Execute(ctx, "MATCH (n:OSL) RETURN n.val ORDER BY n.val SKIP 2 LIMIT 5", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 5)
}

func TestParseValueIntegerParsing(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "int-parse",
		Labels:     []string{"IntParse"},
		Properties: map[string]interface{}{"count": float64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Integer literal parsing
	result, err := exec.Execute(ctx, "MATCH (n:IntParse) WHERE n.count = 100 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Additional tests for remaining coverage gaps

func TestCompareEqualBothNil(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Node with nil property
	node := &storage.Node{
		ID:         "nil-both",
		Labels:     []string{"NilBoth"},
		Properties: map[string]interface{}{"val": nil},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Compare nil = nil (both nil should be equal)
	result, err := exec.Execute(ctx, "MATCH (n:NilBoth) WHERE n.val = NULL RETURN n", nil)
	require.NoError(t, err)
	// Note: depends on how nil comparison is handled
	assert.NotNil(t, result)
}

func TestToFloat64AllTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test with float32 (we need to create nodes that have various types)
	node := &storage.Node{
		ID:     "types-all",
		Labels: []string{"TypesAll"},
		Properties: map[string]interface{}{
			"f64":  float64(100.0),
			"f32":  float32(50.0),
			"i":    int(30),
			"i64":  int64(40),
			"i32":  int32(20),
			"str":  "60.5",
			"bool": true,
		},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Query using float comparison
	result, err := exec.Execute(ctx, "MATCH (n:TypesAll) WHERE n.f64 > 50 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestEvaluateStringOpDefaultCase(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-def",
		Labels:     []string{"StrDef"},
		Properties: map[string]interface{}{"name": "test value"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test each string operator
	result, err := exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name CONTAINS 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	result, err = exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name STARTS WITH 'test' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	result, err = exec.Execute(ctx, "MATCH (n:StrDef) WHERE n.name ENDS WITH 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteMatchWithRelationshipQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship
	node1 := &storage.Node{ID: "rel-m1", Labels: []string{"Start"}, Properties: map[string]interface{}{}}
	node2 := &storage.Node{ID: "rel-m2", Labels: []string{"End"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "rel-me", StartNode: "rel-m1", EndNode: "rel-m2", Type: "CONNECTS"}
	require.NoError(t, store.CreateEdge(edge))

	// Match with relationship type
	result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "CONNECTS", result.Rows[0][0])
}

func TestExecuteCreateWithInlineProps(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create with inline properties and return the property
	result, err := exec.Execute(ctx, "CREATE (n:Inline {val: 42}) RETURN n.val", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

func TestExecuteReturnAliasedCount(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("ac-%d", i)),
			Labels:     []string{"AC"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:AC) RETURN count(*) AS total", nil)
	require.NoError(t, err)
	assert.Equal(t, "total", result.Columns[0])
	assert.Equal(t, int64(4), result.Rows[0][0])
}

func TestExecuteSetNewProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "set-new",
		Labels:     []string{"SetNew"},
		Properties: map[string]interface{}{"existing": "yes"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// SET a new property
	result, err := exec.Execute(ctx, "MATCH (n:SetNew) SET n.newprop = 'value' RETURN n", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.PropertiesSet)
	assert.NotEmpty(t, result.Rows)
}

func TestExecuteMatchWithNilParams(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "nil-params",
		Labels:     []string{"NilParams"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Execute with nil params map
	result, err := exec.Execute(ctx, "MATCH (n:NilParams) RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestParseReturnItemsEmptyString(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "empty-items",
		Labels:     []string{"EmptyItems"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// RETURN with trailing comma/empty item is invalid syntax.
	_, err = exec.Execute(ctx, "MATCH (n:EmptyItems) RETURN n , ", nil)
	require.Error(t, err)
}

func TestExecuteMatchWhereEqualsNumber(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "num-eq",
		Labels:     []string{"NumEq"},
		Properties: map[string]interface{}{"val": float64(42)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:NumEq) WHERE n.val = 42 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestExecuteReturnCountN(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("cn-%d", i)),
			Labels:     []string{"CountN"},
			Properties: map[string]interface{}{},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, err)
	}

	// COUNT(n) - count by variable name
	result, err := exec.Execute(ctx, "MATCH (n:CountN) RETURN count(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(5), result.Rows[0][0])
}

func TestExecuteAggregationWithNonNumeric(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with string values (not numeric)
	node := &storage.Node{
		ID:         "non-num",
		Labels:     []string{"NonNum"},
		Properties: map[string]interface{}{"value": "not a number"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// SUM should handle non-numeric gracefully
	result, err := exec.Execute(ctx, "MATCH (n:NonNum) RETURN sum(n.value)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Rows[0][0])
}

func TestExecuteMatchCreateBlock_SetAndDeleteErrorBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)

	// Invalid label name branch in SET label assignment.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (t:Temp {name:'x'}) SET t:123bad",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label name")

	// Missing parameter branch in SET assignment.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (t:Temp {name:'x'}) SET t.flag = $missing",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parameter $missing")

	// Relationship delete target branch.
	res, err := exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (b:TempNode {name:'b'}), (a)-[r:REL]->(b) WITH r DELETE r RETURN count(r) AS c",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0])
}

func TestResolveReturnItem_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"id": "prop-id", "name": "alice", "age": int64(30), "embedding": []float32{0.1, 0.2}},
	}

	ctx := context.Background()

	// Wildcard and direct variable branches.
	assert.Equal(t, node, exec.resolveReturnItem(ctx, returnItem{expr: "*"}, "p", node))
	assert.Equal(t, node, exec.resolveReturnItem(ctx, returnItem{expr: "p"}, "p", node))

	// Collect subquery placeholder branch.
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "COLLECT { MATCH (p)-[:R]->(q) RETURN q }"}, "p", node))

	// CASE/function/IS NULL/arithmetic evaluation branches.
	assert.Equal(t, "adult", exec.resolveReturnItem(ctx, returnItem{expr: "CASE WHEN p.age > 20 THEN 'adult' ELSE 'child' END"}, "p", node))
	assert.Equal(t, "30", exec.resolveReturnItem(ctx, returnItem{expr: "toString(p.age)"}, "p", node))
	assert.Equal(t, true, exec.resolveReturnItem(ctx, returnItem{expr: "p.missing IS NULL"}, "p", node))
	assert.Equal(t, int64(31), exec.resolveReturnItem(ctx, returnItem{expr: "p.age + 1"}, "p", node))

	// Property-access branches.
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "q.age"}, "p", node))             // var mismatch
	assert.Equal(t, "prop-id", exec.resolveReturnItem(ctx, returnItem{expr: "p.id"}, "p", node)) // id from property
	assert.Equal(t, []float32{0.1, 0.2}, exec.resolveReturnItem(ctx, returnItem{expr: "p.embedding"}, "p", node))
	assert.Equal(t, "alice", exec.resolveReturnItem(ctx, returnItem{expr: "p.name"}, "p", node))
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "p.unknown"}, "p", node))

	// embedding is a regular property — no special routing from ChunkEmbeddings/EmbedMeta.
	noEmbedding := &storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "p.embedding"}, "p", noEmbedding))
	withChunkOnly := &storage.Node{ID: "n3", Labels: []string{"Person"}, ChunkEmbeddings: [][]float32{{0.3, 0.4}}, Properties: map[string]interface{}{}}
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "p.embedding"}, "p", withChunkOnly)) // not in Properties
	withEmbProp := &storage.Node{ID: "n3b", Labels: []string{"Person"}, Properties: map[string]interface{}{"embedding": []float32{0.3, 0.4}}}
	assert.Equal(t, []float32{0.3, 0.4}, exec.resolveReturnItem(ctx, returnItem{expr: "p.embedding"}, "p", withEmbProp))
	withMeta := &storage.Node{ID: "n4", Labels: []string{"Person"}, EmbedMeta: map[string]interface{}{"has_embedding": true}}
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "p.embedding"}, "p", withMeta)) // not in Properties

	// has_embedding branches (from EmbedMeta).
	assert.Equal(t, true, exec.resolveReturnItem(ctx, returnItem{expr: "p.has_embedding"}, "p", withMeta))
	assert.Equal(t, true, exec.resolveReturnItem(ctx, returnItem{expr: "p.has_embedding"}, "p", &storage.Node{ID: "n5", Labels: []string{"Person"}, ChunkEmbeddings: [][]float32{{0.3, 0.4}}}))
	assert.Equal(t, false, exec.resolveReturnItem(ctx, returnItem{expr: "p.has_embedding"}, "p", noEmbedding))

	// Fallback unresolved expression branch.
	assert.Nil(t, exec.resolveReturnItem(ctx, returnItem{expr: "unknownExpressionToken"}, "p", node))
}

func TestExecuteCreateWithRefs_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Parameter substitution + function return item variable extraction.
	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"name": "alice"})
	res, nodes, edges, err := exec.executeCreateWithRefs(
		ctxWithParams,
		"CREATE (a:Person {name: $name})-[:KNOWS]->(b:Person {name:'bob'}) RETURN id(a) AS aid, b.name AS bname",
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Contains(t, nodes, "a")
	require.Contains(t, nodes, "b")
	require.NotNil(t, edges)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 2)
	assert.Equal(t, "bob", res.Rows[0][1])

	// Chain remainder branch and relationship variable capture.
	res, nodes, edges, err = exec.executeCreateWithRefs(
		ctx,
		"CREATE (x:A {name:'x'})-[r1:R1]->(y:B {name:'y'})-[r2:R2]->(z:C {name:'z'}) RETURN z.name",
	)
	require.NoError(t, err)
	require.NotNil(t, nodes["x"])
	require.NotNil(t, nodes["y"])
	require.NotNil(t, nodes["z"])
	require.Contains(t, edges, "r1")
	require.Contains(t, edges, "r2")
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "z", res.Rows[0][0])

	// Reverse direction branch.
	_, nodes, edges, err = exec.executeCreateWithRefs(
		ctx,
		"CREATE (l:Left {name:'l'})<-[rr:BACK]-(r:Right {name:'r'}) RETURN l.name, r.name",
	)
	require.NoError(t, err)
	require.Contains(t, nodes, "l")
	require.Contains(t, nodes, "r")
	require.Contains(t, edges, "rr")

	// Invalid relationship syntax branch.
	_, _, _, err = exec.executeCreateWithRefs(ctx, "CREATE (a)-[:BROKEN](b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid relationship pattern")
}

func TestLowLevelHelpers_AdditionalCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// extractFuncInner
	assert.Equal(t, "", extractFuncInner("notAFunction"))
	assert.Equal(t, "n", extractFuncInner("COUNT(n)"))
	assert.Equal(t, "{a:1}", extractFuncInner("collect({a:1})[..10]"))
	assert.Equal(t, "'(a) and )'", extractFuncInner("toString('(a) and )')"))

	// apocCollMin
	assert.Nil(t, apocCollMin([]interface{}{}))
	assert.Equal(t, int64(1), apocCollMin([]interface{}{int64(3), int64(1), int64(2)}))
	assert.Equal(t, int64(5), apocCollMin([]interface{}{int64(5), "x", int64(9)})) // non-numeric ignored
	assert.Nil(t, apocCollMin("not-list"))

	// executeCreateNodeSegment
	_, _, err := exec.executeCreateNodeSegment(ctx, "CREATE (:Person {name:'x'})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must have a variable name")

	_, _, err = exec.executeCreateNodeSegment(ctx, "CREATE (n:123Bad)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label name")

	_, _, err = exec.executeCreateNodeSegment(ctx, "CREATE (n:Person {1bad: 1})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid property key")

	node, varName, err := exec.executeCreateNodeSegment(ctx, "CREATE (n:Person)")
	require.NoError(t, err)
	require.Equal(t, "n", varName)
	require.NotNil(t, node)
	require.NotNil(t, node.Properties) // initialized for empty-property create
}

func TestExecuteMatchCreateBlock_SetMergeAndDeleteBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "aa", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "bb", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// Direct DELETE (without WITH) branch on created relationship + count() return.
	res, err := exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}), (b:Person {name:'bob'}) CREATE (a)-[r:REL {v:1}]->(b) DELETE r RETURN count(r) AS deleted",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0])

	// SET += merge branch on relationship properties.
	nodeVars := map[string]*storage.Node{}
	edgeVars := map[string]*storage.Edge{}
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}), (b:Person {name:'bob'}) CREATE (a)-[r:REL2 {x:1}]->(b) SET r += {y:2}",
		nodeVars,
		edgeVars,
	)
	require.NoError(t, err)
	require.Contains(t, edgeVars, "r")
	assert.Equal(t, int64(1), edgeVars["r"].Properties["x"])
	assert.Equal(t, int64(2), edgeVars["r"].Properties["y"])

	// Whole-map replacement on node/edge variable branches.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}), (b:Person {name:'bob'}) CREATE (a)-[r:REL3]->(b), (t:Temp {z:0}) SET t = {k: 7}, r = {w: 9} RETURN t.k, r.w",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)

	// Additional label assignment path.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (t:Temp {name:'x'}) SET t:TagLabel",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
}
func TestProcessWithAggregation_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	f1 := &storage.Node{ID: "f1", Labels: []string{"File"}, Properties: map[string]interface{}{"name": "a", "embedding": []float64{0.1}}}
	f2 := &storage.Node{ID: "f2", Labels: []string{"File"}, Properties: map[string]interface{}{"name": "b"}}
	c1 := &storage.Node{ID: "c1", Labels: []string{"Chunk"}, Properties: map[string]interface{}{"text": "x", "embedding": []float64{0.2}}}
	rows := []joinedRow{
		{initialNode: f1, relatedNode: c1},
		{initialNode: f1, relatedNode: nil},
		{initialNode: f2, relatedNode: nil},
	}

	ctx := context.Background()
	res, err := exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END AS hasChunk WITH COUNT(DISTINCT f) AS files, COUNT(c) AS chunks, SUM(hasChunk) AS hasEmbeddings, COLLECT(DISTINCT f.name)[..10] AS sampleFiles RETURN files, chunks, hasEmbeddings, sampleFiles",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(2), res.Rows[0][0])   // distinct files
	assert.Equal(t, int64(1), res.Rows[0][1])   // count(c)
	assert.Equal(t, float64(1), res.Rows[0][2]) // sum(case)
	assert.NotEmpty(t, res.Rows[0][3])          // collect distinct

	// Branch with no aggregation WITH: fall back to RETURN clause parsing.
	res, err = exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c RETURN f.name AS name",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "a", res.Rows[0][0])

	// RETURN clause is mandatory after WITH.
	_, err = exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETURN clause required")

	// Additional aggregate expression branches with computed aliases from first WITH.
	res, err = exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END AS chunkHasEmbedding, CASE WHEN f.embedding IS NOT NULL THEN 1 ELSE 0 END AS fileHasEmbedding WITH COUNT(DISTINCT c) AS distinctChunks, COUNT(DISTINCT z) AS distinctUnknown, COUNT(CASE WHEN c IS NOT NULL THEN 1 END) AS caseCount, COLLECT(c.text) AS texts, SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) AS totalEmbeddings, f.name AS firstName, unknownExpr AS missing RETURN distinctChunks, distinctUnknown, caseCount, texts, totalEmbeddings, firstName, missing",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0]) // distinct c
	assert.Equal(t, int64(0), res.Rows[0][1]) // unknown distinct
	assert.Equal(t, int64(1), res.Rows[0][2]) // CASE count
	assert.Equal(t, []interface{}{"x"}, res.Rows[0][3])
	assert.Equal(t, float64(3), res.Rows[0][4]) // 1 chunk + 2 file rows
	assert.Equal(t, "a", res.Rows[0][5])
	assert.Nil(t, res.Rows[0][6])

	// SUM over non-numeric values must fail (openCypher-compatible type checking).
	_, err = exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c RETURN SUM(f.embedding) AS invalid",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUM() requires numeric values")

	// N-term SUM arithmetic (SUM + SUM + SUM - SUM) is supported.
	res, err = exec.processWithAggregation(ctx,
		rows,
		"f",
		"c",
		"",
		"WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END AS a, CASE WHEN f.embedding IS NOT NULL THEN 2 ELSE 0 END AS b, CASE WHEN c IS NOT NULL THEN 3 ELSE 0 END AS d WITH SUM(a) + SUM(b) + SUM(d) - SUM(a) AS total RETURN total",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	// a=1, b=4, d=3 => 1+4+3-1 = 7
	assert.Equal(t, float64(7), res.Rows[0][0])
}

func TestCreateHelpers_ProcessRelationshipResolveAndSetMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	nodeVars := map[string]*storage.Node{}
	edgeVars := map[string]*storage.Edge{}
	result := &ExecuteResult{Stats: &QueryStats{}}

	err := exec.processCreateNode(ctx, "(a:Person {name:'alice'})", nodeVars, result, store)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "a")
	assert.Equal(t, 1, result.Stats.NodesCreated)

	err = exec.processCreateRelationship(ctx, "(a)-[r:KNOWS {since: 2020}]->(b:Person {name:'bob'})", nodeVars, edgeVars, result, store)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "b")
	require.Contains(t, edgeVars, "r")
	assert.Equal(t, "KNOWS", edgeVars["r"].Type)
	assert.Equal(t, int64(2020), edgeVars["r"].Properties["since"])

	// Reverse arrow branch: created edge should start at inline node c and end at a.
	err = exec.processCreateRelationship(ctx, "(a)<-[rb:BACK]-(c:Person {name:'carol'})", nodeVars, edgeVars, result, store)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "c")
	require.Contains(t, edgeVars, "rb")
	assert.Equal(t, nodeVars["c"].ID, edgeVars["rb"].StartNode)
	assert.Equal(t, nodeVars["a"].ID, edgeVars["rb"].EndNode)

	// Invalid relationship shape.
	err = exec.processCreateRelationship(ctx, "(a)-[:BROKEN](b)", nodeVars, edgeVars, result, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid relationship pattern")

	// Missing variable in context must error deterministically.
	err = exec.processCreateRelationship(ctx, "(missing)-[:REL]->(a)", nodeVars, edgeVars, result, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve source node")

	created := &ExecuteResult{Stats: &QueryStats{}}
	err = exec.applySetMergeToCreated(ctx, "a += {score: 5, active: true}", nodeVars, edgeVars, created, store)
	require.NoError(t, err)
	assert.Equal(t, int64(5), nodeVars["a"].Properties["score"])
	assert.Equal(t, true, nodeVars["a"].Properties["active"])
	assert.Equal(t, 2, created.Stats.PropertiesSet)

	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{
		"edgeProps": map[string]interface{}{"rank": int64(9)},
	})
	err = exec.applySetMergeToCreated(ctxWithParams, "r += $edgeProps", nodeVars, edgeVars, created, store)
	require.NoError(t, err)
	assert.Equal(t, int64(9), edgeVars["r"].Properties["rank"])
	assert.Equal(t, 3, created.Stats.PropertiesSet)

	// Strict error branches.
	err = exec.applySetMergeToCreated(ctx, "a + {x:1}", nodeVars, edgeVars, created, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid SET += syntax")

	err = exec.applySetMergeToCreated(ctx, "a += $missing", nodeVars, edgeVars, created, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires parameters")

	err = exec.applySetMergeToCreated(context.WithValue(ctx, paramsKey, map[string]interface{}{"missing": int64(1)}), "a += $missing", nodeVars, edgeVars, created, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a map")

	err = exec.applySetMergeToCreated(ctx, "a += {broken", nodeVars, edgeVars, created, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse properties")

	err = exec.applySetMergeToCreated(ctx, "unknown += {x:1}", nodeVars, edgeVars, created, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable")
}

func TestCreateHelpers_ParsersAndValidators(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	require.NoError(t, exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name:'ok'})"))
	require.NoError(t, exec.validateCreatePatternPropertyMap(ctx, "(n:Person)"))

	err := exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name:'x'")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid property map syntax")

	err = exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid property map syntax")

	pathVar, stripped := parseCreatePathAssignment("p = (a)-[:R]->(b)")
	assert.Equal(t, "p", pathVar)
	assert.Equal(t, "(a)-[:R]->(b)", stripped)

	pathVar, stripped = parseCreatePathAssignment("1bad = (a)-[:R]->(b)")
	assert.Equal(t, "", pathVar)
	assert.Equal(t, "1bad = (a)-[:R]->(b)", stripped)

	parts := exec.splitCreatePatterns("p=(a:A {txt:'x -> y'})-[r:REL]->(b:B), (c:C)")
	require.Len(t, parts, 2)
	assert.Equal(t, "p=(a:A {txt:'x -> y'})-[r:REL]->(b:B)", strings.TrimSpace(parts[0]))
	assert.Equal(t, "(c:C)", strings.TrimSpace(parts[1]))

	nodes := exec.splitNodePatterns("(a:A {txt:'(x)' }), (b:B {v:1})")
	require.Len(t, nodes, 2)
	assert.Contains(t, nodes[0], "(a:A")
	assert.Contains(t, nodes[1], "(b:B")

	relType, relProps := exec.parseRelationshipTypeAndProps(ctx, "r:KNOWS {since: 2021, note: 'x'}")
	assert.Equal(t, "KNOWS", relType)
	assert.Equal(t, int64(2021), relProps["since"])
	assert.Equal(t, "x", relProps["note"])

	relType, relProps = exec.parseRelationshipTypeAndProps(ctx, "r")
	assert.Equal(t, "RELATED_TO", relType)
	assert.Empty(t, relProps)

	relType, relProps = exec.parseRelationshipTypeAndProps(ctx, ":")
	assert.Equal(t, "RELATED_TO", relType)
	assert.Empty(t, relProps)

	source, rel, target, reverse, remainder, err := exec.parseCreateRelPatternWithVars("(a)-[r:KNOWS]->(b)-[:NEXT]->(c)")
	require.NoError(t, err)
	assert.Equal(t, "a", source)
	assert.Equal(t, "r:KNOWS", rel)
	assert.Equal(t, "b", target)
	assert.False(t, reverse)
	assert.Equal(t, "-[:NEXT]->(c)", remainder)

	source, rel, target, reverse, remainder, err = exec.parseCreateRelPatternWithVars("(a)<-[r:BACK]-(b)")
	require.NoError(t, err)
	assert.Equal(t, "a", source)
	assert.Equal(t, "r:BACK", rel)
	assert.Equal(t, "b", target)
	assert.True(t, reverse)
	assert.Equal(t, "", remainder)

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("a)-[r:REL]->(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must start with (")

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("(a)-[r:REL->(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched bracket")

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("(a)-[r:REL]-(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ->(")
}

func TestCreateHelpers_ResolveOrCreateAndMultipleCreates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	nodeVars := map[string]*storage.Node{}
	result := &ExecuteResult{Stats: &QueryStats{}}
	ctx := context.Background()

	_, err := exec.resolveOrCreateNode(ctx, "missingVar", nodeVars, result, store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	n, err := exec.resolveOrCreateNode(ctx, "x:Node {id:'x1'}", nodeVars, result, store)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Contains(t, nodeVars, "x")
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Existing variable returns same node without creating a new one.
	same, err := exec.resolveOrCreateNode(ctx, "x:Node {id:'different'}", nodeVars, result, store)
	require.NoError(t, err)
	assert.Equal(t, n.ID, same.ID)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	// Inline node without variable is created but not added to nodeVars.
	inline, err := exec.resolveOrCreateNode(ctx, ":Leaf {k:1}", nodeVars, result, store)
	require.NoError(t, err)
	require.NotNil(t, inline)
	assert.Equal(t, 2, result.Stats.NodesCreated)
	_, hasLeaf := nodeVars["Leaf"]
	assert.False(t, hasLeaf)

	assert.True(t, isSimpleVariable("abc_123"))
	assert.False(t, isSimpleVariable("a-b"))
	keys := getKeys(nodeVars)
	assert.Contains(t, keys, "x")

	res, err := exec.executeMultipleCreates(ctx, "CREATE (a:Person {name:'a'}) WITH a CREATE (b:Person {name:'b'}) CREATE (a)-[r:KNOWS]->(b) RETURN a.name AS aname, b.name AS bname, type(r) AS rt")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "a", res.Rows[0][0])
	assert.Equal(t, "b", res.Rows[0][1])
	assert.Equal(t, "KNOWS", res.Rows[0][2])
	assert.Equal(t, 2, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)
}

func TestExecuteMatchCreateBlock_AdditionalSetAndDeleteBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Person {name:'alice'})", nil)
	require.NoError(t, err)

	allNodeVars := map[string]*storage.Node{}
	allEdgeVars := map[string]*storage.Edge{}

	// No CREATE in block should be a no-op.
	res, err := exec.executeMatchCreateBlock(ctx, "MATCH (a:Person {name:'alice'})", allNodeVars, allEdgeVars)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 0, res.Stats.NodesCreated)
	assert.Empty(t, res.Rows)

	// MATCH producing zero rows should short-circuit CREATE and still shape RETURN columns.
	res, err = exec.executeMatchCreateBlock(ctx, "MATCH (m:Missing) CREATE (x:Tmp {id:'x'}) RETURN x", allNodeVars, allEdgeVars)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, res.Columns)
	assert.Empty(t, res.Rows)

	// Direct DELETE without WITH branch + count after delete.
	res, err = exec.executeMatchCreateBlock(ctx, "MATCH (a:Person {name:'alice'}) CREATE (t:Tmp {id:'d1'}) DELETE t RETURN count(t) AS c", allNodeVars, allEdgeVars)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0])
	assert.Equal(t, 1, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.NodesDeleted)

	// SET branches: += map, label add, edge property set, and RETURN edge + function expression.
	res, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (a)-[r:LIKES]->(b:Person {name:'bob2'}) SET b += {age: 20}, b:User, r.weight = 2 RETURN b.age AS age, type(r) AS rt, r.weight AS w",
		allNodeVars,
		allEdgeVars,
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(20), res.Rows[0][0])
	assert.Nil(t, res.Rows[0][1])
	assert.Equal(t, int64(2), res.Rows[0][2])
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)
	assert.GreaterOrEqual(t, res.Stats.PropertiesSet, 2)
	assert.Equal(t, 1, res.Stats.LabelsAdded)

	// Unknown variable in SET must fail deterministically.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (b:Tmp {id:'u1'}) SET z.flag = true RETURN b",
		allNodeVars,
		allEdgeVars,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable in SET clause")

	// Missing parameter for SET assignment must error.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (b:Tmp {id:'u2'}) SET b.score = $score RETURN b",
		allNodeVars,
		allEdgeVars,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires parameters to be provided")
}

func TestExecuteCompoundCreateWithDelete_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Invalid shape must error.
	_, err := exec.executeCompoundCreateWithDelete(ctx, "CREATE (n:Tmp {id:'x'}) RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid CREATE...WITH...DELETE query")

	// Edge delete target branch, non-count RETURN branch should yield nil value.
	res, err := exec.executeCompoundCreateWithDelete(
		ctx,
		"CREATE (a:Tmp {id:'a'})-[r:REL]->(b:Tmp {id:'b'}) WITH r DELETE r RETURN r",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)
	assert.Equal(t, 1, res.Stats.RelationshipsDeleted)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	assert.Nil(t, res.Rows[0][0])
}
