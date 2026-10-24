// Comprehensive unit tests for all Cypher functions in NornicDB.
// This file provides complete coverage for all scalar, aggregation, list, string,
// math, date/time, spatial, and vector functions.

package cypher

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Test Helper Functions
// ========================================

func setupTestExecutor(t *testing.T) *StorageExecutor {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	return NewStorageExecutor(store)
}

func createTestNode(t *testing.T, e *StorageExecutor, id string, labels []string, props map[string]interface{}) *storage.Node {
	node := &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     labels,
		Properties: props,
	}
	if _, err := e.storage.CreateNode(node); err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}
	return node
}

// ========================================
// Scalar Functions Tests
// ========================================

func TestFunctionId(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})

	ctx := context.Background()

	nodes := map[string]*storage.Node{"n": node}
	fixtureAdmission411Context, fixtureAdmission411 := expressionTestContext(ctx, "id(n)")
	_ = fixtureAdmission411
	result := e.evaluateExpressionWithContext(fixtureAdmission411, fixtureAdmission411Context, "id(n)", nodes, nil)

	if result != "node-1" {
		t.Errorf("id(n) = %v, want node-1", result)
	}
}

func TestFunctionElementId(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission412Context, fixtureAdmission412 := expressionTestContext(ctx, "elementId(n)")
	_ = fixtureAdmission412
	result := e.evaluateExpressionWithContext(fixtureAdmission412, fixtureAdmission412Context, "elementId(n)", nodes, nil)

	expected := "4:test:node-1"
	if result != expected {
		t.Errorf("elementId(n) = %v, want %v", result, expected)
	}
}

func TestFunctionLabels(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person", "Employee"}, nil)

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission413Context, fixtureAdmission413 := expressionTestContext(ctx, "labels(n)")
	_ = fixtureAdmission413
	result := e.evaluateExpressionWithContext(fixtureAdmission413, fixtureAdmission413Context, "labels(n)", nodes, nil)

	labels, ok := result.([]interface{})
	if !ok {
		t.Fatalf("labels(n) should return []interface{}, got %T", result)
	}
	if len(labels) != 2 {
		t.Errorf("labels(n) should return 2 labels, got %d", len(labels))
	}
}

func TestFunctionType(t *testing.T) {
	e := setupTestExecutor(t)

	rel := &storage.Edge{
		ID:   "rel-1",
		Type: "KNOWS",
	}
	rels := map[string]*storage.Edge{"r": rel}
	ctx := context.Background()

	fixtureAdmission414Context, fixtureAdmission414 := expressionTestContext(ctx, "type(r)")
	_ = fixtureAdmission414
	result := e.evaluateExpressionWithContext(fixtureAdmission414, fixtureAdmission414Context, "type(r)", nil, rels)

	if result != "KNOWS" {
		t.Errorf("type(r) = %v, want KNOWS", result)
	}
}

func TestFunctionKeys(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission415Context, fixtureAdmission415 := expressionTestContext(ctx, "keys(n)")
	_ = fixtureAdmission415
	result := e.evaluateExpressionWithContext(fixtureAdmission415, fixtureAdmission415Context, "keys(n)", nodes, nil)

	keys, ok := result.([]interface{})
	if !ok {
		t.Fatalf("keys(n) should return []interface{}, got %T", result)
	}
	if len(keys) != 2 {
		t.Errorf("keys(n) should return 2 keys, got %d", len(keys))
	}
}

func TestFunctionProperties(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission416Context, fixtureAdmission416 := expressionTestContext(ctx, "properties(n)")
	_ = fixtureAdmission416
	result := e.evaluateExpressionWithContext(fixtureAdmission416, fixtureAdmission416Context, "properties(n)", nodes, nil)

	props, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("properties(n) should return map, got %T", result)
	}
	if props["name"] != "Alice" {
		t.Errorf("properties(n)['name'] = %v, want Alice", props["name"])
	}
}

func TestFunctionCoalesce(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"coalesce(null, 'default')", "default"},
		{"coalesce('first', 'second')", "first"},
		{"coalesce(null, null, 'third')", "third"},
		{"coalesce(1, 2)", int64(1)},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission417Context, fixtureAdmission417 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission417
			result := e.evaluateExpressionWithContext(fixtureAdmission417, fixtureAdmission417Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestFunctionExists(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
	})

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected bool
	}{
		{"exists(n.name)", true},
		{"exists(n.missing)", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission418Context, fixtureAdmission418 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission418
			result := e.evaluateExpressionWithContext(fixtureAdmission418, fixtureAdmission418Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Functions Tests
// ========================================

func TestStringFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Case conversion
		{"toLower('HELLO')", "hello"},
		{"toUpper('hello')", "HELLO"},
		{"lower('WORLD')", "world"},
		{"upper('world')", "WORLD"},

		// Trimming
		{"trim('  hello  ')", "hello"},
		{"ltrim('  hello')", "hello"},
		{"rtrim('hello  ')", "hello"},
		{"btrim('xxhelloxx', 'x')", "hello"},

		// String manipulation
		{"replace('hello', 'l', 'L')", "heLLo"},
		{"reverse('hello')", "olleh"},
		{"left('hello', 2)", "he"},
		{"right('hello', 2)", "lo"},
		{"substring('hello', 1, 3)", "ell"},

		// Split
		{"toString(123)", "123"},
		{"toString(true)", "true"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission419Context, fixtureAdmission419 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission419
			result := e.evaluateExpressionWithContext(fixtureAdmission419, fixtureAdmission419Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestSplitFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Create a node with property for split test
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"data": "a,b,c",
		"sep":  ",",
	})
	nodes := map[string]*storage.Node{"n": node}

	ctx := context.Background()

	// Test with node properties
	fixtureAdmission420Context, fixtureAdmission420 := expressionTestContext(ctx, "split(n.data, n.sep)")
	_ = fixtureAdmission420
	result := e.evaluateExpressionWithContext(fixtureAdmission420, fixtureAdmission420Context, "split(n.data, n.sep)", nodes, nil)
	list, ok := result.([]interface{})
	if !ok {
		t.Fatalf("split should return list, got %T", result)
	}
	if len(list) != 3 {
		t.Errorf("split(n.data, n.sep) returned %d elements, want 3", len(list))
	}
}

func TestCharLengthFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int64
	}{
		{"char_length('hello')", 5},
		{"character_length('世界')", 2}, // Unicode characters
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission421Context, fixtureAdmission421 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission421
			result := e.evaluateExpressionWithContext(fixtureAdmission421, fixtureAdmission421Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestFunctionEvaluator_ArrayIndexSliceAndMapLiteral(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-eval-1", []string{"Person", "Employee"}, map[string]interface{}{"nums": []interface{}{int64(10), int64(20), int64(30)}})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()
	// Array/string indexing paths
	fixtureAdmission400Context, fixtureAdmission400 := expressionTestContext(ctx, "labels(n)[0]")
	_ = fixtureAdmission400Context
	assertEqual(t, "labels(n)[0]", "Person", e.evaluateExpressionWithContext(fixtureAdmission400, fixtureAdmission400Context, "labels(n)[0]", nodes, nil))
	fixtureAdmission401Context, fixtureAdmission401 := expressionTestContext(ctx, "labels(n)[-1]")
	_ = fixtureAdmission401Context
	assertEqual(t, "labels(n)[-1]", "Employee", e.evaluateExpressionWithContext(fixtureAdmission401, fixtureAdmission401Context, "labels(n)[-1]", nodes, nil))
	assertEqual(t, "substring('abc',1,1)", "b", typedNumericRead(t, e, "RETURN substring('abc',1,1) AS v", nil))
	_, err := e.Execute(ctx, "RETURN 'abc'[1] AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	// Slice notation paths
	fixtureAdmission425Context, fixtureAdmission425 := expressionTestContext(ctx, "labels(n)[..1]")
	_ = fixtureAdmission425
	s1 := e.evaluateExpressionWithContext(fixtureAdmission425, fixtureAdmission425Context, "labels(n)[..1]", nodes, nil)
	list1, ok := s1.([]interface{})
	if !ok || len(list1) != 1 || list1[0] != "Person" {
		t.Fatalf("labels(n)[..1] = %v, want [Person]", s1)
	}

	fixtureAdmission426Context, fixtureAdmission426 := expressionTestContext(ctx, "labels(n)[1..]")
	_ = fixtureAdmission426
	s2 := e.evaluateExpressionWithContext(fixtureAdmission426, fixtureAdmission426Context, "labels(n)[1..]", nodes, nil)
	list2, ok := s2.([]interface{})
	if !ok || len(list2) != 1 || list2[0] != "Employee" {
		t.Fatalf("labels(n)[1..] = %v, want [Employee]", s2)
	}

	// IN list must not be treated as indexing
	fixtureAdmission427Context, fixtureAdmission427 := expressionTestContext(ctx, "1 IN [1,2,3]")
	_ = fixtureAdmission427
	assertEqual(t, "1 IN [1,2,3]", true, e.evaluateExpressionWithContext(fixtureAdmission427, fixtureAdmission427Context, "1 IN [1,2,3]", nil, nil))

	// Parenthesized expression and map literal
	fixtureAdmission428Context, fixtureAdmission428 := expressionTestContext(ctx, "(1 + 2)")
	_ = fixtureAdmission428
	assertEqual(t, "(1 + 2)", int64(3), e.evaluateExpressionWithContext(fixtureAdmission428, fixtureAdmission428Context, "(1 + 2)", nil, nil))
	fixtureAdmission429Context, fixtureAdmission429 := expressionTestContext(ctx, "{x: 1, y: 'z'}")
	_ = fixtureAdmission429
	m := e.evaluateExpressionWithContext(fixtureAdmission429, fixtureAdmission429Context, "{x: 1, y: 'z'}", nil, nil)
	mm, ok := m.(map[string]interface{})
	if !ok {
		t.Fatalf("map literal should return map, got %T", m)
	}
	if mm["x"] != int64(1) || mm["y"] != "z" {
		t.Fatalf("map literal mismatch: %v", mm)
	}
}

func TestFunctionEvaluator_AdditionalBranches(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-eval-2", []string{"Person", "Employee"}, map[string]interface{}{
		"name":   "Alice",
		"values": []interface{}{int64(10), int64(20), int64(30), int64(40)},
	})
	rel := &storage.Edge{
		ID:         storage.EdgeID("rel-eval-1"),
		Type:       "KNOWS",
		StartNode:  node.ID,
		EndNode:    node.ID,
		Properties: map[string]interface{}{"weight": int64(1)},
	}
	if err := e.storage.CreateEdge(rel); err != nil {
		t.Fatalf("Failed to create test edge: %v", err)
	}

	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{"r": rel}
	ctx := context.Background()

	fixtureAdmission430Context, fixtureAdmission430 := expressionTestContext(ctx, "type(r)")
	_ = fixtureAdmission430
	assertEqual(t, "type(r)", "KNOWS", e.evaluateExpressionWithContext(fixtureAdmission430, fixtureAdmission430Context, "type(r)", nodes, rels))
	fixtureAdmission431Context, fixtureAdmission431 := expressionTestContext(ctx, "type({type:'KNOWS'})")
	_ = fixtureAdmission431
	assertEqual(t, "type({type:'KNOWS'})", nil, e.evaluateExpressionWithContext(fixtureAdmission431, fixtureAdmission431Context, "type({type:'KNOWS'})", nodes, rels))
	fixtureAdmission432Context, fixtureAdmission432 := expressionTestContext(ctx, "exists(n.missing)")
	_ = fixtureAdmission432
	assertEqual(t, "exists(n.missing)", false, e.evaluateExpressionWithContext(fixtureAdmission432, fixtureAdmission432Context, "exists(n.missing)", nodes, rels))
	fixtureAdmission433Context, fixtureAdmission433 := expressionTestContext(ctx, "head([])")
	_ = fixtureAdmission433
	assertEqual(t, "head([])", nil, e.evaluateExpressionWithContext(fixtureAdmission433, fixtureAdmission433Context, "head([])", nodes, rels))
	fixtureAdmission434Context, fixtureAdmission434 := expressionTestContext(ctx, "last([])")
	_ = fixtureAdmission434
	assertEqual(t, "last([])", nil, e.evaluateExpressionWithContext(fixtureAdmission434, fixtureAdmission434Context, "last([])", nodes, rels))
	fixtureAdmission435Context, fixtureAdmission435 := expressionTestContext(ctx, "reverse('stressed')")
	_ = fixtureAdmission435
	assertEqual(t, "reverse('stressed')", "desserts", e.evaluateExpressionWithContext(fixtureAdmission435, fixtureAdmission435Context, "reverse('stressed')", nodes, rels))
	fixtureAdmission436Context, fixtureAdmission436 := expressionTestContext(ctx, "indexOf([1,2,3], 9)")
	_ = fixtureAdmission436
	assertEqual(t, "indexOf([1,2,3], 9)", int64(-1), e.evaluateExpressionWithContext(fixtureAdmission436, fixtureAdmission436Context, "indexOf([1,2,3], 9)", nodes, rels))
	fixtureAdmission437Context, fixtureAdmission437 := expressionTestContext(ctx, "hasLabels(n, ['Person', 'Employee'])")
	_ = fixtureAdmission437
	assertEqual(t, "hasLabels(n, ['Person', 'Employee'])", true, e.evaluateExpressionWithContext(fixtureAdmission437, fixtureAdmission437Context, "hasLabels(n, ['Person', 'Employee'])", nodes, rels))
	fixtureAdmission438Context, fixtureAdmission438 := expressionTestContext(ctx, "hasLabels(n, ['Person', 'Missing'])")
	_ = fixtureAdmission438
	assertEqual(t, "hasLabels(n, ['Person', 'Missing'])", false, e.evaluateExpressionWithContext(fixtureAdmission438, fixtureAdmission438Context, "hasLabels(n, ['Person', 'Missing'])", nodes, rels))

	fixtureAdmission439Context, fixtureAdmission439 := expressionTestContext(ctx, "tail([1])")
	_ = fixtureAdmission439
	tail := e.evaluateExpressionWithContext(fixtureAdmission439, fixtureAdmission439Context, "tail([1])", nodes, rels)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 0 {
		t.Fatalf("tail([1]) = %v, want []", tail)
	}

	fixtureAdmission440Context, fixtureAdmission440 := expressionTestContext(ctx, "slice([1, 2, 3, 4], -3, -1)")
	_ = fixtureAdmission440
	sliced := e.evaluateExpressionWithContext(fixtureAdmission440, fixtureAdmission440Context, "slice([1, 2, 3, 4], -3, -1)", nodes, rels)
	sliceList, ok := sliced.([]interface{})
	if !ok || len(sliceList) != 2 || sliceList[0] != int64(2) || sliceList[1] != int64(3) {
		t.Fatalf("slice([1,2,3,4], -3, -1) = %v, want [2,3]", sliced)
	}

	fixtureAdmission441Context, fixtureAdmission441 := expressionTestContext(ctx, "length(p)")
	_ = fixtureAdmission441
	gotLength := e.evaluateExpressionWithContextFullFunctions(fixtureAdmission441, fixtureAdmission441Context,
		"length(p)",
		nodes,
		rels,
		map[string]*PathResult{"p": {Length: 4}},
		nil,
		nil,
		0,
	)
	assertEqual(t, "length(p)", int64(4), gotLength)

	fixtureAdmission442Context, fixtureAdmission442 := expressionTestContext(ctx, "length(route)")
	_ = fixtureAdmission442
	gotFallbackLength := e.evaluateExpressionWithContextFullFunctions(fixtureAdmission442, fixtureAdmission442Context, "length(route)", nodes, rels, nil, nil, nil, 6)
	assertEqual(t, "length(route)", int64(6), gotFallbackLength)
}

func assertEqual(t *testing.T, name string, expected interface{}, actual interface{}) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s = %v, want %v", name, actual, expected)
	}
}

// ========================================
// List Functions Tests
// ========================================

func TestListFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test head
	ctx := context.Background()
	fixtureAdmission443Context, fixtureAdmission443 := expressionTestContext(ctx, "head([1, 2, 3])")
	_ = fixtureAdmission443
	result := e.evaluateExpressionWithContext(fixtureAdmission443, fixtureAdmission443Context, "head([1, 2, 3])", nil, nil)
	if result != int64(1) {
		t.Errorf("head([1,2,3]) = %v, want 1", result)
	}

	// Test last
	fixtureAdmission444Context, fixtureAdmission444 := expressionTestContext(ctx, "last([1, 2, 3])")
	_ = fixtureAdmission444
	result = e.evaluateExpressionWithContext(fixtureAdmission444, fixtureAdmission444Context, "last([1, 2, 3])", nil, nil)
	if result != int64(3) {
		t.Errorf("last([1,2,3]) = %v, want 3", result)
	}

	// Test tail
	fixtureAdmission445Context, fixtureAdmission445 := expressionTestContext(ctx, "tail([1, 2, 3])")
	_ = fixtureAdmission445
	tail := e.evaluateExpressionWithContext(fixtureAdmission445, fixtureAdmission445Context, "tail([1, 2, 3])", nil, nil)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 2 {
		t.Errorf("tail([1,2,3]) = %v, want [2,3]", tail)
	}

	// Test reverse
	fixtureAdmission446Context, fixtureAdmission446 := expressionTestContext(ctx, "reverse([1, 2, 3])")
	_ = fixtureAdmission446
	rev := e.evaluateExpressionWithContext(fixtureAdmission446, fixtureAdmission446Context, "reverse([1, 2, 3])", nil, nil)
	revList, ok := rev.([]interface{})
	if !ok || len(revList) != 3 {
		t.Errorf("reverse([1,2,3]) failed")
	}
}

func TestRangeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int // length of result
	}{
		{"range(0, 5)", 6},     // 0,1,2,3,4,5
		{"range(0, 10, 2)", 6}, // 0,2,4,6,8,10
		{"range(5, 0, -1)", 6}, // 5,4,3,2,1,0
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission447Context, fixtureAdmission447 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission447
			result := e.evaluateExpressionWithContext(fixtureAdmission447, fixtureAdmission447Context, tt.expr, nil, nil)
			list, ok := result.([]interface{})
			if !ok {
				t.Fatalf("%s should return list", tt.expr)
			}
			if len(list) != tt.expected {
				t.Errorf("%s returned %d elements, want %d", tt.expr, len(list), tt.expected)
			}
		})
	}
}

func TestSizeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int64
	}{
		{"size([1, 2, 3])", 3},
		{"size('hello')", 5},
		{"length([1, 2])", 2},
		{"length('abc')", 3},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission448Context, fixtureAdmission448 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission448
			result := e.evaluateExpressionWithContext(fixtureAdmission448, fixtureAdmission448Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Math Functions Tests
// ========================================

func TestMathFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
		delta    float64 // for float comparisons
	}{
		{"abs(-5)", int64(5), 0},
		{"abs(5)", int64(5), 0},
		{"ceil(4.2)", float64(5), 0},
		{"floor(4.8)", float64(4), 0},
		{"round(4.5)", float64(5), 0},
		{"sign(10)", int64(1), 0},
		{"sign(-10)", int64(-1), 0},
		{"sign(0)", int64(0), 0},
		{"sqrt(16)", float64(4), 0.001},
		{"exp(0)", float64(1), 0.001},
		{"log(1)", float64(0), 0.001},
		{"log10(100)", float64(2), 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission449Context, fixtureAdmission449 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission449
			result := e.evaluateExpressionWithContext(fixtureAdmission449, fixtureAdmission449Context, tt.expr, nil, nil)

			if tt.delta > 0 {
				resultF, ok := result.(float64)
				if !ok {
					t.Fatalf("%s should return float64", tt.expr)
				}
				expectedF := tt.expected.(float64)
				if math.Abs(resultF-expectedF) > tt.delta {
					t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
				}
			} else {
				if result != tt.expected {
					t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
				}
			}
		})
	}
}

func TestTrigFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"sin(0)", 0, 0.001},
		{"cos(0)", 1, 0.001},
		{"tan(0)", 0, 0.001},
		{"asin(0)", 0, 0.001},
		{"acos(1)", 0, 0.001},
		{"atan(0)", 0, 0.001},
		{"atan2(0, 1)", 0, 0.001},
		{"radians(180)", math.Pi, 0.001},
		{"degrees(3.14159265)", 180, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission450Context, fixtureAdmission450 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission450
			result := e.evaluateExpressionWithContext(fixtureAdmission450, fixtureAdmission450Context, tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestHyperbolicFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"sinh(0)", 0, 0.001},
		{"sinh(1)", 1.1752011936438014, 0.001},
		{"sinh(0.7)", 0.7585837018395334, 0.001},
		{"cosh(0)", 1, 0.001},
		{"cosh(1)", 1.5430806348152437, 0.001},
		{"cosh(0.7)", 1.255169005630943, 0.001},
		{"tanh(0)", 0, 0.001},
		{"tanh(1)", 0.7615941559557649, 0.001},
		{"tanh(0.7)", 0.6043677771171636, 0.001},
		{"coth(1)", 1.3130352854993312, 0.001},
		{"coth(0.7)", 1.6546216358026298, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission451Context, fixtureAdmission451 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission451
			result := e.evaluateExpressionWithContext(fixtureAdmission451, fixtureAdmission451Context, tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestPowerFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"power(2, 3)", 8, 0.001},
		{"power(2, 10)", 1024, 0.001},
		{"power(10, 2)", 100, 0.001},
		{"power(4, 0.5)", 2, 0.001},
		{"power(27, 0.333333)", 3, 0.01},
		{"power(2, -1)", 0.5, 0.001},
		{"power(0, 5)", 0, 0.001},
		{"power(5, 0)", 1, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission452Context, fixtureAdmission452 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission452
			result := e.evaluateExpressionWithContext(fixtureAdmission452, fixtureAdmission452Context, tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestMathConstants(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test pi()
	fixtureAdmission453Context, fixtureAdmission453 := expressionTestContext(ctx, "pi()")
	_ = fixtureAdmission453
	pi := e.evaluateExpressionWithContext(fixtureAdmission453, fixtureAdmission453Context, "pi()", nil, nil)
	piF, ok := pi.(float64)
	if !ok || math.Abs(piF-math.Pi) > 0.0001 {
		t.Errorf("pi() = %v, want %v", pi, math.Pi)
	}

	// Test e()
	fixtureAdmission454Context, fixtureAdmission454 := expressionTestContext(ctx, "e()")
	_ = fixtureAdmission454
	eVal := e.evaluateExpressionWithContext(fixtureAdmission454, fixtureAdmission454Context, "e()", nil, nil)
	eF, ok := eVal.(float64)
	if !ok || math.Abs(eF-math.E) > 0.0001 {
		t.Errorf("e() = %v, want %v", eVal, math.E)
	}
}

func TestRandomFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test rand() returns value between 0 and 1
	fixtureAdmission455Context, fixtureAdmission455 := expressionTestContext(ctx, "rand()")
	_ = fixtureAdmission455
	result := e.evaluateExpressionWithContext(fixtureAdmission455, fixtureAdmission455Context, "rand()", nil, nil)
	randF, ok := result.(float64)
	if !ok {
		t.Fatalf("rand() should return float64")
	}
	if randF < 0 || randF > 1 {
		t.Errorf("rand() = %v, should be between 0 and 1", randF)
	}

	// Test randomUUID() returns a string
	fixtureAdmission456Context, fixtureAdmission456 := expressionTestContext(ctx, "randomUUID()")
	_ = fixtureAdmission456
	uuid := e.evaluateExpressionWithContext(fixtureAdmission456, fixtureAdmission456Context, "randomUUID()", nil, nil)
	uuidStr, ok := uuid.(string)
	if !ok {
		t.Fatalf("randomUUID() should return string")
	}
	if len(uuidStr) < 32 {
		t.Errorf("randomUUID() = %v, should be valid UUID format", uuidStr)
	}
}

// ========================================
// Type Conversion Functions Tests
// ========================================

func TestTypeConversionFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"toInteger('123')", int64(123)},
		{"toInteger(123.7)", int64(123)},
		{"toInt('456')", int64(456)},
		{"toFloat('3.14')", float64(3.14)},
		{"toFloat(42)", float64(42)},
		{"toBoolean('true')", true},
		{"toBoolean('false')", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission457Context, fixtureAdmission457 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission457
			result := e.evaluateExpressionWithContext(fixtureAdmission457, fixtureAdmission457Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// Null Check Functions Tests
// ========================================

func TestNullCheckFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"isEmpty([])", true},
		{"isEmpty([1])", false},
		{"isEmpty('')", true},
		{"isEmpty('a')", false},
		{"isNaN(0)", false},
		{"nullIf('a', 'a')", nil},
		{"nullIf('a', 'b')", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission458Context, fixtureAdmission458 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission458
			result := e.evaluateExpressionWithContext(fixtureAdmission458, fixtureAdmission458Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Timestamp Functions Tests
// ========================================

func TestTimestampFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test timestamp() returns a value
	fixtureAdmission459Context, fixtureAdmission459 := expressionTestContext(ctx, "timestamp()")
	_ = fixtureAdmission459
	ts := e.evaluateExpressionWithContext(fixtureAdmission459, fixtureAdmission459Context, "timestamp()", nil, nil)
	if ts == nil {
		t.Error("timestamp() should return a value")
	}

	// Test datetime() returns a value
	fixtureAdmission460Context, fixtureAdmission460 := expressionTestContext(ctx, "datetime()")
	_ = fixtureAdmission460
	dt := e.evaluateExpressionWithContext(fixtureAdmission460, fixtureAdmission460Context, "datetime()", nil, nil)
	if dt == nil {
		t.Error("datetime() should return a value")
	}

	// Test date()
	fixtureAdmission461Context, fixtureAdmission461 := expressionTestContext(ctx, "date()")
	_ = fixtureAdmission461
	d := e.evaluateExpressionWithContext(fixtureAdmission461, fixtureAdmission461Context, "date()", nil, nil)
	if d == nil {
		t.Error("date() should return a value")
	}

	// Test time()
	fixtureAdmission462Context, fixtureAdmission462 := expressionTestContext(ctx, "time()")
	_ = fixtureAdmission462
	tm := e.evaluateExpressionWithContext(fixtureAdmission462, fixtureAdmission462Context, "time()", nil, nil)
	if tm == nil {
		t.Error("time() should return a value")
	}
}

// ========================================
// Relationship Functions Tests
// ========================================

func TestRelationshipFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes and relationship
	node1 := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})
	node2 := createTestNode(t, e, "node-2", []string{"Person"}, map[string]interface{}{"name": "Bob"})

	rel := &storage.Edge{
		ID:         "rel-1",
		Type:       "KNOWS",
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Properties: map[string]interface{}{"since": 2020},
	}
	e.storage.CreateEdge(rel)

	nodes := map[string]*storage.Node{"a": node1, "b": node2}
	rels := map[string]*storage.Edge{"r": rel}
	ctx := context.Background()

	// Test startNode
	fixtureAdmission463Context, fixtureAdmission463 := expressionTestContext(ctx, "startNode(r)")
	_ = fixtureAdmission463
	result := e.evaluateExpressionWithContext(fixtureAdmission463, fixtureAdmission463Context, "startNode(r)", nodes, rels)
	if result == nil {
		t.Error("startNode(r) should return a value")
	}

	// Test endNode
	fixtureAdmission464Context, fixtureAdmission464 := expressionTestContext(ctx, "endNode(r)")
	_ = fixtureAdmission464
	result = e.evaluateExpressionWithContext(fixtureAdmission464, fixtureAdmission464Context, "endNode(r)", nodes, rels)
	if result == nil {
		t.Error("endNode(r) should return a value")
	}

	// Test type(r)
	fixtureAdmission465Context, fixtureAdmission465 := expressionTestContext(ctx, "type(r)")
	_ = fixtureAdmission465
	result = e.evaluateExpressionWithContext(fixtureAdmission465, fixtureAdmission465Context, "type(r)", nodes, rels)
	if result != "KNOWS" {
		t.Errorf("type(r) = %v, want KNOWS", result)
	}
}

// ========================================
// Vector Functions Tests
// ========================================

func TestVectorSimilarityFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes with vector embeddings
	node1 := createTestNode(t, e, "node-1", []string{"Doc"}, map[string]interface{}{
		"vec": []interface{}{float64(1), float64(0), float64(0)},
	})
	node2 := createTestNode(t, e, "node-2", []string{"Doc"}, map[string]interface{}{
		"vec": []interface{}{float64(1), float64(0), float64(0)},
	})

	nodes := map[string]*storage.Node{"a": node1, "b": node2}
	ctx := context.Background()

	// Test cosine similarity of identical vectors = 1
	fixtureAdmission466Context, fixtureAdmission466 := expressionTestContext(ctx, "vector.similarity.cosine(a.vec, b.vec)")
	_ = fixtureAdmission466
	result := e.evaluateExpressionWithContext(fixtureAdmission466, fixtureAdmission466Context, "vector.similarity.cosine(a.vec, b.vec)", nodes, nil)
	if result == nil {
		t.Error("vector.similarity.cosine should return a value")
	}

	// Test euclidean similarity
	fixtureAdmission467Context, fixtureAdmission467 := expressionTestContext(ctx, "vector.similarity.euclidean(a.vec, b.vec)")
	_ = fixtureAdmission467
	result = e.evaluateExpressionWithContext(fixtureAdmission467, fixtureAdmission467Context, "vector.similarity.euclidean(a.vec, b.vec)", nodes, nil)
	if result == nil {
		t.Error("vector.similarity.euclidean should return a value")
	}
}

// ========================================
// Spatial Functions Tests
// ========================================

func TestSpatialFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test point function
	fixtureAdmission468Context, fixtureAdmission468 := expressionTestContext(ctx, "point({x: 1.0, y: 2.0})")
	_ = fixtureAdmission468
	result := e.evaluateExpressionWithContext(fixtureAdmission468, fixtureAdmission468Context, "point({x: 1.0, y: 2.0})", nil, nil)
	if _, isPoint := result.(CypherPoint); !isPoint {
		t.Fatalf("point() should return a point, got %T", result)
	}
	pointMap, _ := spatialMap(result)
	if pointMap["x"] != float64(1.0) {
		t.Errorf("point().x = %v, want 1.0", pointMap["x"])
	}

	// Test distance function with x/y coordinates
	node1 := createTestNode(t, e, "node-1", []string{"Location"}, map[string]interface{}{
		"loc": map[string]interface{}{"x": float64(0), "y": float64(0)},
	})
	node2 := createTestNode(t, e, "node-2", []string{"Location"}, map[string]interface{}{
		"loc": map[string]interface{}{"x": float64(3), "y": float64(4)},
	})

	nodes := map[string]*storage.Node{"a": node1, "b": node2}
	fixtureAdmission469Context, fixtureAdmission469 := expressionTestContext(ctx, "distance(a.loc, b.loc)")
	_ = fixtureAdmission469
	dist := e.evaluateExpressionWithContext(fixtureAdmission469, fixtureAdmission469Context, "distance(a.loc, b.loc)", nodes, nil)
	distF, ok := dist.(float64)
	if !ok {
		t.Fatalf("distance() should return float64, got %T", dist)
	}
	// Distance from (0,0) to (3,4) should be 5
	if math.Abs(distF-5.0) > 0.001 {
		t.Errorf("distance(a.loc, b.loc) = %v, want 5.0", distF)
	}
}

// ========================================
// Reduce Function Tests
// ========================================

func TestReduceFunction(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// reduce(acc = 0, x IN [1,2,3] | acc + x) should return 6
	fixtureAdmission470Context, fixtureAdmission470 := expressionTestContext(ctx, "reduce(acc = 0, x IN [1, 2, 3] | acc + x)")
	_ = fixtureAdmission470
	result := e.evaluateExpressionWithContext(fixtureAdmission470, fixtureAdmission470Context, "reduce(acc = 0, x IN [1, 2, 3] | acc + x)", nil, nil)
	if result == nil {
		t.Error("reduce should return a value")
	}
}

// ========================================
// Property Access Tests
// ========================================

func TestFunctionPropertyAccess(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name":    "Alice",
		"age":     int64(30),
		"active":  true,
		"balance": float64(100.50),
	})

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"n.name", "Alice"},
		{"n.age", int64(30)},
		{"n.active", true},
		{"n.balance", float64(100.50)},
		{"n.missing", nil},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission471Context, fixtureAdmission471 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission471
			result := e.evaluateExpressionWithContext(fixtureAdmission471, fixtureAdmission471Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// Literals Tests
// ========================================

func TestLiterals(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"null", nil},
		{"true", true},
		{"false", false},
		{"123", int64(123)},
		{"3.14", float64(3.14)},
		{"'hello'", "hello"},
		{`"world"`, "world"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission472Context, fixtureAdmission472 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission472
			result := e.evaluateExpressionWithContext(fixtureAdmission472, fixtureAdmission472Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Concatenation Tests
// ========================================

func TestStringConcatenation(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"firstName": "John",
		"lastName":  "Doe",
	})

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission473Context, fixtureAdmission473 := expressionTestContext(ctx, "n.firstName + ' ' + n.lastName")
	_ = fixtureAdmission473
	result := e.evaluateExpressionWithContext(fixtureAdmission473, fixtureAdmission473Context, "n.firstName + ' ' + n.lastName", nodes, nil)
	if result != "John Doe" {
		t.Errorf("concatenation = %v, want 'John Doe'", result)
	}
}

// ========================================
// Count Function Tests
// ========================================

func TestCountFunction(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// count(*) in expression context should NOT be evaluated here
	// Aggregation functions must be handled by executeAggregation(ctx, ) or executeMatchWithRelationshipsWithPathSeeded()
	fixtureAdmission474Context, fixtureAdmission474 := expressionTestContext(ctx, "count(*)")
	_ = fixtureAdmission474
	result := e.evaluateExpressionWithContext(fixtureAdmission474, fixtureAdmission474Context, "count(*)", nil, nil)
	if result != nil {
		t.Errorf("count(*) in expression context should return nil, got %v", result)
	}

	// count(n) also should NOT be evaluated in expression context
	node := createTestNode(t, e, "node-1", []string{"Person"}, nil)
	nodes := map[string]*storage.Node{"n": node}
	fixtureAdmission475Context, fixtureAdmission475 := expressionTestContext(ctx, "count(n)")
	_ = fixtureAdmission475
	result = e.evaluateExpressionWithContext(fixtureAdmission475, fixtureAdmission475Context, "count(n)", nodes, nil)
	if result != nil {
		t.Errorf("count(n) in expression context should return nil, got %v", result)
	}
}

// ========================================
// Embedding Property Filter Tests
// ========================================
// OrNull Variants Tests
// ========================================

func TestOrNullVariants(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// toIntegerOrNull
		{"toIntegerOrNull('123')", int64(123)},
		{"toIntegerOrNull('invalid')", nil},
		{"toIntegerOrNull(45.6)", int64(45)},

		// toFloatOrNull
		{"toFloatOrNull('3.14')", float64(3.14)},
		{"toFloatOrNull('invalid')", nil},
		{"toFloatOrNull(42)", float64(42)},

		// toBooleanOrNull
		{"toBooleanOrNull('true')", true},
		{"toBooleanOrNull('false')", false},
		{"toBooleanOrNull('maybe')", nil},

		// toStringOrNull
		{"toStringOrNull(123)", "123"},
		{"toStringOrNull(null)", nil},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission476Context, fixtureAdmission476 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission476
			result := e.evaluateExpressionWithContext(fixtureAdmission476, fixtureAdmission476Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// List Conversion Functions Tests
// ========================================

func TestListConversionFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"intList":    []interface{}{"1", "2", "3"},
		"floatList":  []interface{}{"1.1", "2.2", "3.3"},
		"boolList":   []interface{}{"true", "false", "true"},
		"stringList": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// toIntegerList
	fixtureAdmission477Context, fixtureAdmission477 := expressionTestContext(ctx, "toIntegerList(n.intList)")
	_ = fixtureAdmission477
	result := e.evaluateExpressionWithContext(fixtureAdmission477, fixtureAdmission477Context, "toIntegerList(n.intList)", nodes, nil)
	if intList, ok := result.([]interface{}); ok {
		if len(intList) != 3 {
			t.Errorf("toIntegerList should return 3 elements, got %d", len(intList))
		}
		if intList[0] != int64(1) {
			t.Errorf("toIntegerList[0] = %v, want 1", intList[0])
		}
	} else {
		t.Errorf("toIntegerList should return []interface{}, got %T", result)
	}

	// toFloatList
	fixtureAdmission478Context, fixtureAdmission478 := expressionTestContext(ctx, "toFloatList(n.floatList)")
	_ = fixtureAdmission478
	result = e.evaluateExpressionWithContext(fixtureAdmission478, fixtureAdmission478Context, "toFloatList(n.floatList)", nodes, nil)
	if floatList, ok := result.([]interface{}); ok {
		if len(floatList) != 3 {
			t.Errorf("toFloatList should return 3 elements")
		}
	}

	// toBooleanList
	fixtureAdmission479Context, fixtureAdmission479 := expressionTestContext(ctx, "toBooleanList(n.boolList)")
	_ = fixtureAdmission479
	result = e.evaluateExpressionWithContext(fixtureAdmission479, fixtureAdmission479Context, "toBooleanList(n.boolList)", nodes, nil)
	if boolList, ok := result.([]interface{}); ok {
		if len(boolList) != 3 {
			t.Errorf("toBooleanList should return 3 elements")
		}
		if boolList[0] != true {
			t.Errorf("toBooleanList[0] = %v, want true", boolList[0])
		}
	}

	// toStringList
	fixtureAdmission480Context, fixtureAdmission480 := expressionTestContext(ctx, "toStringList(n.stringList)")
	_ = fixtureAdmission480
	result = e.evaluateExpressionWithContext(fixtureAdmission480, fixtureAdmission480Context, "toStringList(n.stringList)", nodes, nil)
	if stringList, ok := result.([]interface{}); ok {
		if len(stringList) != 3 {
			t.Errorf("toStringList should return 3 elements")
		}
		if stringList[0] != "1" {
			t.Errorf("toStringList[0] = %v, want '1'", stringList[0])
		}
	}
}

// ========================================
// valueType Function Tests
// ========================================

func TestValueTypeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected string
	}{
		{"valueType(null)", "NULL"},
		{"valueType(true)", "BOOLEAN NOT NULL"},
		{"valueType(123)", "INTEGER NOT NULL"},
		{"valueType(3.14)", "FLOAT NOT NULL"},
		{"valueType('hello')", "STRING NOT NULL"},
		{"valueType([1, 2, 3])", "LIST<INTEGER NOT NULL> NOT NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission481Context, fixtureAdmission481 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission481
			result := e.evaluateExpressionWithContext(fixtureAdmission481, fixtureAdmission481Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Aggregation Functions (Expression Context) Tests
// ========================================

func TestAggregationInExpressionContext(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{"val": int64(42)})
	nodes := map[string]*storage.Node{"n": node}

	// In single row context, aggregation functions just return the value
	tests := []string{"sum(n.val)", "avg(n.val)", "min(n.val)", "max(n.val)"}

	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission482Context, fixtureAdmission482 := expressionTestContext(ctx, expr)
			_ = fixtureAdmission482
			result := e.evaluateExpressionWithContext(fixtureAdmission482, fixtureAdmission482Context, expr, nodes, nil)
			if result != int64(42) {
				t.Errorf("%s = %v, want 42", expr, result)
			}
		})
	}

	// collect returns a list
	ctx := context.Background()
	fixtureAdmission483Context, fixtureAdmission483 := expressionTestContext(ctx, "collect(n.val)")
	_ = fixtureAdmission483
	result := e.evaluateExpressionWithContext(fixtureAdmission483, fixtureAdmission483Context, "collect(n.val)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 1 || list[0] != int64(42) {
			t.Errorf("collect(n.val) = %v, want [42]", list)
		}
	} else {
		t.Errorf("collect should return list, got %T", result)
	}
}

// ========================================
// List Predicate Functions Tests
// ========================================

func TestListPredicateFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with simple predicates that can be evaluated
	// Note: these functions parse the WHERE predicate and substitute values

	// Test none() with empty result - no match expected
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"empty": []interface{}{},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// none() on empty list should return true
	fixtureAdmission484Context, fixtureAdmission484 := expressionTestContext(ctx, "none(x IN n.empty WHERE true)")
	_ = fixtureAdmission484
	result := e.evaluateExpressionWithContext(fixtureAdmission484, fixtureAdmission484Context, "none(x IN n.empty WHERE true)", nodes, nil)
	if result != true {
		t.Errorf("none(x IN empty list) = %v, want true", result)
	}

	// Test basic parsing - ensure functions don't crash
	node2 := createTestNode(t, e, "node-2", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes2 := map[string]*storage.Node{"n": node2}

	// all() should not crash even if predicate evaluation is limited
	fixtureAdmission485Context, fixtureAdmission485 := expressionTestContext(ctx, "all(x IN n.nums WHERE x = x)")
	_ = fixtureAdmission485
	_ = e.evaluateExpressionWithContext(fixtureAdmission485, fixtureAdmission485Context, "all(x IN n.nums WHERE x = x)", nodes2, nil)

	// any() should not crash
	fixtureAdmission486Context, fixtureAdmission486 := expressionTestContext(ctx, "any(x IN n.nums WHERE x = x)")
	_ = fixtureAdmission486
	_ = e.evaluateExpressionWithContext(fixtureAdmission486, fixtureAdmission486Context, "any(x IN n.nums WHERE x = x)", nodes2, nil)

	// single() should not crash
	fixtureAdmission487Context, fixtureAdmission487 := expressionTestContext(ctx, "single(x IN n.nums WHERE x = x)")
	_ = fixtureAdmission487
	_ = e.evaluateExpressionWithContext(fixtureAdmission487, fixtureAdmission487Context, "single(x IN n.nums WHERE x = x)", nodes2, nil)
}

// ========================================
// Filter and Extract Functions Tests
// ========================================

func TestFilterAndExtractFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// Test extract() - transforms each element
	fixtureAdmission488Context, fixtureAdmission488 := expressionTestContext(ctx, "extract(x IN n.nums | x)")
	_ = fixtureAdmission488
	result := e.evaluateExpressionWithContext(fixtureAdmission488, fixtureAdmission488Context, "extract(x IN n.nums | x)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 5 {
			t.Errorf("extract should return 5 elements, got %d", len(list))
		}
	} else {
		t.Errorf("extract should return list, got %T", result)
	}

	// Test filter() - returns a list (predicate evaluation limited)
	fixtureAdmission489Context, fixtureAdmission489 := expressionTestContext(ctx, "filter(x IN n.nums WHERE true)")
	_ = fixtureAdmission489
	result = e.evaluateExpressionWithContext(fixtureAdmission489, fixtureAdmission489Context, "filter(x IN n.nums WHERE true)", nodes, nil)
	_, ok := result.([]interface{})
	if !ok {
		t.Errorf("filter should return list, got %T", result)
	}
}

// ========================================
// withinBBox Function Tests
// ========================================

func TestWithinBBoxFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Create points
	node := createTestNode(t, e, "node-1", []string{"Location"}, map[string]interface{}{
		"inside":  map[string]interface{}{"x": float64(5), "y": float64(5)},
		"outside": map[string]interface{}{"x": float64(15), "y": float64(15)},
		"ll":      map[string]interface{}{"x": float64(0), "y": float64(0)},
		"ur":      map[string]interface{}{"x": float64(10), "y": float64(10)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// Point inside bbox
	fixtureAdmission490Context, fixtureAdmission490 := expressionTestContext(ctx, "withinBBox(n.inside, n.ll, n.ur)")
	_ = fixtureAdmission490
	result := e.evaluateExpressionWithContext(fixtureAdmission490, fixtureAdmission490Context, "withinBBox(n.inside, n.ll, n.ur)", nodes, nil)
	if result != true {
		t.Errorf("withinBBox(inside) = %v, want true", result)
	}

	// Point outside bbox
	fixtureAdmission491Context, fixtureAdmission491 := expressionTestContext(ctx, "withinBBox(n.outside, n.ll, n.ur)")
	_ = fixtureAdmission491
	result = e.evaluateExpressionWithContext(fixtureAdmission491, fixtureAdmission491Context, "withinBBox(n.outside, n.ll, n.ur)", nodes, nil)
	if result != false {
		t.Errorf("withinBBox(outside) = %v, want false", result)
	}
}

// ========================================
// List Comprehension Tests
// ========================================

func TestListComprehension(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// [x IN list | x] - identity transformation
	fixtureAdmission492Context, fixtureAdmission492 := expressionTestContext(ctx, "[x IN n.nums | x]")
	_ = fixtureAdmission492
	result := e.evaluateExpressionWithContext(fixtureAdmission492, fixtureAdmission492Context, "[x IN n.nums | x]", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 3 {
			t.Errorf("list comprehension should return 3 elements, got %d", len(list))
		}
	}
}

// ========================================
// Slice Function Tests
// ========================================

func TestSliceFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"list": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// slice(list, 1, 3)
	fixtureAdmission493Context, fixtureAdmission493 := expressionTestContext(ctx, "slice(n.list, 1, 3)")
	_ = fixtureAdmission493
	result := e.evaluateExpressionWithContext(fixtureAdmission493, fixtureAdmission493Context, "slice(n.list, 1, 3)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 2 {
			t.Errorf("slice(list, 1, 3) should return 2 elements, got %d", len(list))
		}
	} else {
		t.Errorf("slice should return list, got %T", result)
	}

	// slice(list, 2) - to end
	fixtureAdmission494Context, fixtureAdmission494 := expressionTestContext(ctx, "slice(n.list, 2)")
	_ = fixtureAdmission494
	result = e.evaluateExpressionWithContext(fixtureAdmission494, fixtureAdmission494Context, "slice(n.list, 2)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 3 {
			t.Errorf("slice(list, 2) should return 3 elements, got %d", len(list))
		}
	}
}

// ========================================
// indexOf Function Tests
// ========================================

func TestIndexOfFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"list": []interface{}{"a", "b", "c", "d"},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// Found
	fixtureAdmission495Context, fixtureAdmission495 := expressionTestContext(ctx, "indexOf(n.list, 'b')")
	_ = fixtureAdmission495
	result := e.evaluateExpressionWithContext(fixtureAdmission495, fixtureAdmission495Context, "indexOf(n.list, 'b')", nodes, nil)
	if result != int64(1) {
		t.Errorf("indexOf(list, 'b') = %v, want 1", result)
	}

	// Not found
	fixtureAdmission496Context, fixtureAdmission496 := expressionTestContext(ctx, "indexOf(n.list, 'z')")
	_ = fixtureAdmission496
	result = e.evaluateExpressionWithContext(fixtureAdmission496, fixtureAdmission496Context, "indexOf(n.list, 'z')", nodes, nil)
	if result != int64(-1) {
		t.Errorf("indexOf(list, 'z') = %v, want -1", result)
	}
}

// ========================================
// Degree Functions Tests
// ========================================

func TestDegreeFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes
	node1 := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})
	node2 := createTestNode(t, e, "node-2", []string{"Person"}, map[string]interface{}{"name": "Bob"})
	node3 := createTestNode(t, e, "node-3", []string{"Person"}, map[string]interface{}{"name": "Carol"})

	// Create edges: Alice -> Bob, Carol -> Alice
	e.storage.CreateEdge(&storage.Edge{ID: "e1", Type: "KNOWS", StartNode: node1.ID, EndNode: node2.ID})
	e.storage.CreateEdge(&storage.Edge{ID: "e2", Type: "KNOWS", StartNode: node3.ID, EndNode: node1.ID})

	nodes := map[string]*storage.Node{"a": node1, "b": node2, "c": node3}
	ctx := context.Background()

	// Alice has 1 outgoing, 1 incoming = degree 2
	fixtureAdmission497Context, fixtureAdmission497 := expressionTestContext(ctx, "degree(a)")
	_ = fixtureAdmission497
	result := e.evaluateExpressionWithContext(fixtureAdmission497, fixtureAdmission497Context, "degree(a)", nodes, nil)
	if result != int64(2) {
		t.Errorf("degree(a) = %v, want 2", result)
	}

	// Alice has 1 outgoing
	fixtureAdmission498Context, fixtureAdmission498 := expressionTestContext(ctx, "outDegree(a)")
	_ = fixtureAdmission498
	result = e.evaluateExpressionWithContext(fixtureAdmission498, fixtureAdmission498Context, "outDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("outDegree(a) = %v, want 1", result)
	}

	// Alice has 1 incoming
	fixtureAdmission499Context, fixtureAdmission499 := expressionTestContext(ctx, "inDegree(a)")
	_ = fixtureAdmission499
	result = e.evaluateExpressionWithContext(fixtureAdmission499, fixtureAdmission499Context, "inDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("inDegree(a) = %v, want 1", result)
	}

	// Bob has 0 outgoing, 1 incoming
	fixtureAdmission500Context, fixtureAdmission500 := expressionTestContext(ctx, "degree(b)")
	_ = fixtureAdmission500
	result = e.evaluateExpressionWithContext(fixtureAdmission500, fixtureAdmission500Context, "degree(b)", nodes, nil)
	if result != int64(1) {
		t.Errorf("degree(b) = %v, want 1", result)
	}
}

// ========================================
// hasLabels Function Tests
// ========================================

func TestHasLabelsFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person", "Employee", "Manager"}, map[string]interface{}{
		"requiredLabels": []interface{}{"Person", "Employee"},
		"wrongLabels":    []interface{}{"Person", "Director"},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// Has all labels (using property with list)
	fixtureAdmission501Context, fixtureAdmission501 := expressionTestContext(ctx, "hasLabels(n, n.requiredLabels)")
	_ = fixtureAdmission501
	result := e.evaluateExpressionWithContext(fixtureAdmission501, fixtureAdmission501Context, "hasLabels(n, n.requiredLabels)", nodes, nil)
	if result != true {
		t.Errorf("hasLabels with matching labels = %v, want true", result)
	}

	// Missing a label
	fixtureAdmission502Context, fixtureAdmission502 := expressionTestContext(ctx, "hasLabels(n, n.wrongLabels)")
	_ = fixtureAdmission502
	result = e.evaluateExpressionWithContext(fixtureAdmission502, fixtureAdmission502Context, "hasLabels(n, n.wrongLabels)", nodes, nil)
	if result != false {
		t.Errorf("hasLabels with missing label = %v, want false", result)
	}
}

// ========================================
// Haversine Distance Tests
// ========================================

func TestHaversineDistance(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test distance with lat/lon coordinates
	node1 := createTestNode(t, e, "node-1", []string{"City"}, map[string]interface{}{
		"loc": map[string]interface{}{"latitude": float64(40.7128), "longitude": float64(-74.0060)}, // NYC
	})
	node2 := createTestNode(t, e, "node-2", []string{"City"}, map[string]interface{}{
		"loc": map[string]interface{}{"latitude": float64(34.0522), "longitude": float64(-118.2437)}, // LA
	})

	nodes := map[string]*storage.Node{"nyc": node1, "la": node2}
	fixtureAdmission503Context, fixtureAdmission503 := expressionTestContext(ctx, "distance(nyc.loc, la.loc)")
	_ = fixtureAdmission503
	dist := e.evaluateExpressionWithContext(fixtureAdmission503, fixtureAdmission503Context, "distance(nyc.loc, la.loc)", nodes, nil)

	if dist == nil {
		t.Fatal("haversine distance should return a value")
	}

	distF, ok := dist.(float64)
	if !ok {
		t.Fatalf("distance() should return float64, got %T", dist)
	}

	// Distance from NYC to LA is approximately 3,944 km or 3,944,000 meters
	if distF < 3800000 || distF > 4100000 {
		t.Errorf("haversine distance = %v, expected ~3944000 meters", distF)
	}
}

// ========================================
// CASE WHEN Expression Tests
// ========================================

func TestCaseWhenExpression(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Simple CASE expression
	fixtureAdmission504Context, fixtureAdmission504 := expressionTestContext(ctx, "CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
	_ = fixtureAdmission504
	result := e.evaluateExpressionWithContext(fixtureAdmission504, fixtureAdmission504Context, "CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", nil, nil)
	if result != "one" {
		t.Errorf("CASE 1 WHEN 1 = %v, want 'one'", result)
	}

	// Searched CASE expression
	fixtureAdmission505Context, fixtureAdmission505 := expressionTestContext(ctx, "CASE WHEN true THEN 'yes' ELSE 'no' END")
	_ = fixtureAdmission505
	result = e.evaluateExpressionWithContext(fixtureAdmission505, fixtureAdmission505Context, "CASE WHEN true THEN 'yes' ELSE 'no' END", nil, nil)
	if result != "yes" {
		t.Errorf("CASE WHEN true = %v, want 'yes'", result)
	}

	// CASE with ELSE
	fixtureAdmission506Context, fixtureAdmission506 := expressionTestContext(ctx, "CASE 3 WHEN 1 THEN 'one' ELSE 'other' END")
	_ = fixtureAdmission506
	result = e.evaluateExpressionWithContext(fixtureAdmission506, fixtureAdmission506Context, "CASE 3 WHEN 1 THEN 'one' ELSE 'other' END", nil, nil)
	if result != "other" {
		t.Errorf("CASE 3 WHEN 1 = %v, want 'other'", result)
	}

	// CASE without matching WHEN (no ELSE)
	fixtureAdmission507Context, fixtureAdmission507 := expressionTestContext(ctx, "CASE 5 WHEN 1 THEN 'one' END")
	_ = fixtureAdmission507
	result = e.evaluateExpressionWithContext(fixtureAdmission507, fixtureAdmission507Context, "CASE 5 WHEN 1 THEN 'one' END", nil, nil)
	if result != nil {
		t.Errorf("CASE without match = %v, want nil", result)
	}
}

func TestCaseWhenWithNode(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"status": "active",
		"score":  int64(85),
	})
	nodes := map[string]*storage.Node{"n": node}

	// CASE with property
	fixtureAdmission508Context, fixtureAdmission508 := expressionTestContext(ctx, "CASE n.status WHEN 'active' THEN 'online' ELSE 'offline' END")
	_ = fixtureAdmission508
	result := e.evaluateExpressionWithContext(fixtureAdmission508, fixtureAdmission508Context, "CASE n.status WHEN 'active' THEN 'online' ELSE 'offline' END", nodes, nil)
	if result != "online" {
		t.Errorf("CASE n.status = %v, want 'online'", result)
	}
}

// ========================================
// Logical Operators Tests
// ========================================

func TestLogicalOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// AND
		{"true AND true", true},
		{"true AND false", false},
		{"false AND true", false},

		// OR
		{"true OR false", true},
		{"false OR true", true},
		{"false OR false", false},

		// XOR
		{"true XOR false", true},
		{"true XOR true", false},
		{"false XOR false", false},

		// NOT
		{"NOT true", false},
		{"NOT false", true},

		// Combined
		{"true AND NOT false", true},
		{"false OR NOT false", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()
			fixtureAdmission509Context, fixtureAdmission509 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission509
			result := e.evaluateExpressionWithContext(fixtureAdmission509, fixtureAdmission509Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Comparison Operators Tests
// ========================================

func TestComparisonOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Equality
		{"1 = 1", true},
		{"1 = 2", false},
		{"'a' = 'a'", true},

		// Not equal
		{"1 <> 2", true},
		{"1 != 2", true},
		{"1 <> 1", false},

		// Less than
		{"1 < 2", true},
		{"2 < 1", false},

		// Greater than
		{"2 > 1", true},
		{"1 > 2", false},

		// Less than or equal
		{"1 <= 1", true},
		{"1 <= 2", true},
		{"2 <= 1", false},

		// Greater than or equal
		{"1 >= 1", true},
		{"2 >= 1", true},
		{"1 >= 2", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()
			fixtureAdmission510Context, fixtureAdmission510 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission510
			result := e.evaluateExpressionWithContext(fixtureAdmission510, fixtureAdmission510Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Arithmetic Operators Tests
// ========================================

func TestArithmeticOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Multiplication
		{"2 * 3", int64(6)},
		{"2.5 * 2", float64(5)},

		// Division retains integer type when both operands are integers.
		{"6 / 2", int64(3)},
		{"7 / 2", int64(3)},

		// Modulo
		{"7 % 3", int64(1)},
		{"10 % 5", int64(0)},

		// Subtraction
		{"5 - 3", int64(2)},
		{"3.5 - 1.5", float64(2)},

		// Unary minus
		{"-5", int64(-5)},
		{"-3.14", float64(-3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()
			fixtureAdmission511Context, fixtureAdmission511 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission511
			result := e.evaluateExpressionWithContext(fixtureAdmission511, fixtureAdmission511Context, tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestArithmeticWithProperties(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"a": int64(10),
		"b": int64(3),
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// n.a * n.b
	fixtureAdmission512Context, fixtureAdmission512 := expressionTestContext(ctx, "n.a * n.b")
	_ = fixtureAdmission512
	result := e.evaluateExpressionWithContext(fixtureAdmission512, fixtureAdmission512Context, "n.a * n.b", nodes, nil)
	if result != int64(30) {
		t.Errorf("n.a * n.b = %v, want 30", result)
	}

	// n.a - n.b
	fixtureAdmission513Context, fixtureAdmission513 := expressionTestContext(ctx, "n.a - n.b")
	_ = fixtureAdmission513
	result = e.evaluateExpressionWithContext(fixtureAdmission513, fixtureAdmission513Context, "n.a - n.b", nodes, nil)
	if result != int64(7) {
		t.Errorf("n.a - n.b = %v, want 7", result)
	}
}

// ========================================
// Combined Expression Tests
// ========================================

func TestCombinedExpressions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"age":    int64(25),
		"active": true,
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	// Comparison with property
	fixtureAdmission514Context, fixtureAdmission514 := expressionTestContext(ctx, "n.age > 18")
	_ = fixtureAdmission514
	result := e.evaluateExpressionWithContext(fixtureAdmission514, fixtureAdmission514Context, "n.age > 18", nodes, nil)
	if result != true {
		t.Errorf("n.age > 18 = %v, want true", result)
	}

	// Multiple conditions with AND
	fixtureAdmission515Context, fixtureAdmission515 := expressionTestContext(ctx, "n.age > 18 AND n.active = true")
	_ = fixtureAdmission515
	result = e.evaluateExpressionWithContext(fixtureAdmission515, fixtureAdmission515Context, "n.age > 18 AND n.active = true", nodes, nil)
	if result != true {
		t.Errorf("n.age > 18 AND n.active = %v, want true", result)
	}
}

// ========================================
// IS NULL / IS NOT NULL Tests
// ========================================

func TestIsNullPredicates(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"null IS NULL", true},
		{"null IS NOT NULL", false},
		{"'hello' IS NULL", false},
		{"'hello' IS NOT NULL", true},
		{"n.name IS NOT NULL", true},
		{"n.missing IS NULL", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission516Context, fixtureAdmission516 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission516
			result := e.evaluateExpressionWithContext(fixtureAdmission516, fixtureAdmission516Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Predicates Tests (STARTS WITH, ENDS WITH, CONTAINS)
// ========================================

func TestStringPredicates(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice Johnson",
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// STARTS WITH
		{"'hello world' STARTS WITH 'hello'", true},
		{"'hello world' STARTS WITH 'world'", false},
		{"n.name STARTS WITH 'Alice'", true},
		{"n.name STARTS WITH 'Bob'", false},

		// ENDS WITH
		{"'hello world' ENDS WITH 'world'", true},
		{"'hello world' ENDS WITH 'hello'", false},
		{"n.name ENDS WITH 'Johnson'", true},

		// CONTAINS
		{"'hello world' CONTAINS 'lo wo'", true},
		{"'hello world' CONTAINS 'xyz'", false},
		{"n.name CONTAINS 'ice'", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()
			fixtureAdmission517Context, fixtureAdmission517 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission517
			result := e.evaluateExpressionWithContext(fixtureAdmission517, fixtureAdmission517Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// IN Operator Tests
// ========================================

func TestInOperatorExpression(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"status": "active",
		"list":   []interface{}{"a", "b", "c"},
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"1 IN [1, 2, 3]", true},
		{"4 IN [1, 2, 3]", false},
		{"'a' IN ['a', 'b', 'c']", true},
		{"'x' IN ['a', 'b', 'c']", false},
		{"n.status IN ['active', 'pending']", true},
		{"n.status IN ['closed', 'cancelled']", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission518Context, fixtureAdmission518 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission518
			result := e.evaluateExpressionWithContext(fixtureAdmission518, fixtureAdmission518Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// BETWEEN Operator Tests
// ========================================

func TestBetweenOperator(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"age": int64(25),
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"5 BETWEEN 1 AND 10", true},
		{"15 BETWEEN 1 AND 10", false},
		{"1 BETWEEN 1 AND 10", true},  // inclusive
		{"10 BETWEEN 1 AND 10", true}, // inclusive
		{"n.age BETWEEN 18 AND 30", true},
		{"n.age BETWEEN 30 AND 40", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission519Context, fixtureAdmission519 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission519
			result := e.evaluateExpressionWithContext(fixtureAdmission519, fixtureAdmission519Context, tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// APOC Map Functions Tests
// ========================================

func TestApocMapMerge(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"map1": map[string]interface{}{"a": int64(1), "b": int64(2)},
		"map2": map[string]interface{}{"b": int64(3), "c": int64(4)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission520Context, fixtureAdmission520 := expressionTestContext(ctx, "apoc.map.merge(n.map1, n.map2)")
	_ = fixtureAdmission520
	result := e.evaluateExpressionWithContext(fixtureAdmission520, fixtureAdmission520Context, "apoc.map.merge(n.map1, n.map2)", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		// map2's "b" should override map1's "b"
		if m["a"] != int64(1) {
			t.Errorf("merged map should have a=1, got %v", m["a"])
		}
		if m["b"] != int64(3) {
			t.Errorf("merged map should have b=3 (from map2), got %v", m["b"])
		}
		if m["c"] != int64(4) {
			t.Errorf("merged map should have c=4, got %v", m["c"])
		}
	} else {
		t.Errorf("apoc.map.merge should return map, got %T", result)
	}
}

func TestApocMapSetKey(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"myMap": map[string]interface{}{"a": int64(1)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission521Context, fixtureAdmission521 := expressionTestContext(ctx, "apoc.map.setKey(n.myMap, 'b', 2)")
	_ = fixtureAdmission521
	result := e.evaluateExpressionWithContext(fixtureAdmission521, fixtureAdmission521Context, "apoc.map.setKey(n.myMap, 'b', 2)", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		if m["a"] != int64(1) {
			t.Errorf("map should still have a=1, got %v", m["a"])
		}
		// The '2' was parsed as a string, so we need to check for string or int
		if m["b"] == nil {
			t.Errorf("map should have b set")
		}
	} else {
		t.Errorf("apoc.map.setKey should return map, got %T", result)
	}
}

func TestApocMapRemoveKey(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"myMap": map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission522Context, fixtureAdmission522 := expressionTestContext(ctx, "apoc.map.removeKey(n.myMap, 'b')")
	_ = fixtureAdmission522
	result := e.evaluateExpressionWithContext(fixtureAdmission522, fixtureAdmission522Context, "apoc.map.removeKey(n.myMap, 'b')", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		if _, exists := m["b"]; exists {
			t.Errorf("map should not have key 'b' after removeKey")
		}
		if m["a"] != int64(1) {
			t.Errorf("map should still have a=1")
		}
		if m["c"] != int64(3) {
			t.Errorf("map should still have c=3")
		}
	} else {
		t.Errorf("apoc.map.removeKey should return map, got %T", result)
	}
}

func TestFunctionAdditionalMathAndStringCoverage(t *testing.T) {
	e := setupTestExecutor(t)
	nodes := map[string]*storage.Node{
		"n": {
			ID: "n1",
			Properties: map[string]interface{}{
				"txt": "go",
				"x":   int64(42),
			},
		},
	}

	tests := []struct {
		expr string
		want interface{}
	}{
		{"cot(1)", 0.6420926159343306}, // Neo4j 5.26.30's value (#907)
		{"haversin(1)", (1 - math.Cos(1)) / 2},
		{"normalize('cafe')", "cafe"},
		{"lpad('go', 5, '.')", "...go"},
		{"rpad('go', 5, '.')", "go..."},
		{"percentileCont(n.x, 0.5)", int64(42)},
		{"percentileDisc(n.x, 0.5)", int64(42)},
		{"stDev(n.x)", float64(0)},
		{"stDevP(n.x)", float64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			fixtureAdmission523Context, fixtureAdmission523 := expressionTestContext(ctx, tt.expr)
			_ = fixtureAdmission523
			got := e.evaluateExpressionWithContext(fixtureAdmission523, fixtureAdmission523Context, tt.expr, nodes, nil)
			if got != tt.want {
				t.Errorf("%s = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

func TestFunctionAdditionalGeometryAndPathCoverage(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// linestring / polygon literals
	fixtureAdmission524Context, fixtureAdmission524 := expressionTestContext(ctx, "lineString([point({x:0,y:0}), point({x:1,y:1})])")
	_ = fixtureAdmission524
	line := e.evaluateExpressionWithContext(fixtureAdmission524, fixtureAdmission524Context, "lineString([point({x:0,y:0}), point({x:1,y:1})])", nil, nil)
	lineMap, ok := line.(map[string]interface{})
	if !ok {
		t.Fatalf("lineString should return map, got %T", line)
	}
	if lineMap["type"] != "linestring" {
		t.Fatalf("lineString type = %v, want linestring", lineMap["type"])
	}

	fixtureAdmission525Context, fixtureAdmission525 := expressionTestContext(ctx, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])")
	_ = fixtureAdmission525
	poly := e.evaluateExpressionWithContext(fixtureAdmission525, fixtureAdmission525Context, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])", nil, nil)
	polyMap, ok := poly.(map[string]interface{})
	if !ok {
		t.Fatalf("polygon should return map, got %T", poly)
	}
	if polyMap["type"] != "polygon" {
		t.Fatalf("polygon type = %v, want polygon", polyMap["type"])
	}

	// insufficient points branches
	fixtureAdmission526Context, fixtureAdmission526 := expressionTestContext(ctx, "lineString([point({x:0,y:0})])")
	_ = fixtureAdmission526
	if v := e.evaluateExpressionWithContext(fixtureAdmission526, fixtureAdmission526Context, "lineString([point({x:0,y:0})])", nil, nil); v != nil {
		t.Fatalf("lineString with <2 points should be nil, got %v", v)
	}
	fixtureAdmission527Context, fixtureAdmission527 := expressionTestContext(ctx, "polygon([point({x:0,y:0}), point({x:1,y:1})])")
	_ = fixtureAdmission527
	if v := e.evaluateExpressionWithContext(fixtureAdmission527, fixtureAdmission527Context, "polygon([point({x:0,y:0}), point({x:1,y:1})])", nil, nil); v != nil {
		t.Fatalf("polygon with <3 points should be nil, got %v", v)
	}

	// nodes(path) coverage using explicit path context
	pathNodes := []*storage.Node{
		{ID: "a", Properties: map[string]interface{}{"name": "a"}},
		{ID: "b", Properties: map[string]interface{}{"name": "b"}},
	}
	pathEdges := []*storage.Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]interface{}{"w": 1}},
	}
	path := &PathResult{
		Nodes:         pathNodes,
		Relationships: pathEdges,
		Length:        1,
	}
	fixtureAdmission528Context, fixtureAdmission528 := expressionTestContext(ctx, "nodes(p)")
	_ = fixtureAdmission528
	gotNodes := e.evaluateExpressionWithContextFull(fixtureAdmission528, fixtureAdmission528Context, "nodes(p)", nil, nil, map[string]*PathResult{"p": path}, nil, nil, 0)
	if arr, ok := gotNodes.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) expected 2 entries, got %T %#v", gotNodes, gotNodes)
	}

	// fallback via allPathNodes/allPathEdges
	fixtureAdmission529Context, fixtureAdmission529 := expressionTestContext(ctx, "nodes(p)")
	_ = fixtureAdmission529
	gotNodesFallback := e.evaluateExpressionWithContextFull(fixtureAdmission529, fixtureAdmission529Context, "nodes(p)", nil, nil, nil, nil, pathNodes, 0)
	if arr, ok := gotNodesFallback.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) fallback expected 2 entries, got %T %#v", gotNodesFallback, gotNodesFallback)
	}
	fixtureAdmission530Context, fixtureAdmission530 := expressionTestContext(ctx, "relationships(p)")
	_ = fixtureAdmission530
	gotRelsFallback := e.evaluateExpressionWithContextFull(fixtureAdmission530, fixtureAdmission530Context, "relationships(p)", nil, nil, nil, pathEdges, nil, 0)
	if arr, ok := gotRelsFallback.([]interface{}); !ok || len(arr) != 1 || arr[0] != pathEdges[0] {
		t.Fatalf("relationships(p) fallback expected 1 entry, got %T %#v", gotRelsFallback, gotRelsFallback)
	}
}

func TestFunctionAdditionalListMapAndDegreeCoverage(t *testing.T) {
	e := setupTestExecutor(t)

	a := createTestNode(t, e, "fn-a", []string{"Person", "Employee"}, map[string]interface{}{
		"requiredLabels": []interface{}{"Person", "Employee"},
		"cleanMap":       map[string]interface{}{"a": int64(1), "b": int64(2), "c": nil},
	})
	b := createTestNode(t, e, "fn-b", []string{"Person"}, map[string]interface{}{})
	if err := e.storage.CreateEdge(&storage.Edge{
		ID:        "fn-rel-1",
		Type:      "KNOWS",
		StartNode: a.ID,
		EndNode:   b.ID,
	}); err != nil {
		t.Fatalf("create edge failed: %v", err)
	}

	nodes := map[string]*storage.Node{"a": a, "b": b}
	ctx := context.Background()

	// list/map helpers in evaluateExpressionWithContextFullFunctions.
	fixtureAdmission531Context, fixtureAdmission531 := expressionTestContext(ctx, "indexOf([10,20,30], 20)")
	_ = fixtureAdmission531
	assertEqual(t, "indexOf([10,20,30], 20)", int64(1), e.evaluateExpressionWithContext(fixtureAdmission531, fixtureAdmission531Context, "indexOf([10,20,30], 20)", nodes, nil))
	fixtureAdmission532Context, fixtureAdmission532 := expressionTestContext(ctx, "range(1,3)")
	_ = fixtureAdmission532
	gotRange := e.evaluateExpressionWithContext(fixtureAdmission532, fixtureAdmission532Context, "range(1,3)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, gotRange) {
		t.Fatalf("range(1,3) = %#v, want [1 2 3]", gotRange)
	}
	fixtureAdmission533Context, fixtureAdmission533 := expressionTestContext(ctx, "range(5,1,-2)")
	_ = fixtureAdmission533
	gotRangeNeg := e.evaluateExpressionWithContext(fixtureAdmission533, fixtureAdmission533Context, "range(5,1,-2)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(5), int64(3), int64(1)}, gotRangeNeg) {
		t.Fatalf("range(5,1,-2) = %#v, want [5 3 1]", gotRangeNeg)
	}
	fixtureAdmission534Context, fixtureAdmission534 := expressionTestContext(ctx, "range(1,3,0)")
	_ = fixtureAdmission534
	gotRangeZero := e.evaluateExpressionWithContext(fixtureAdmission534, fixtureAdmission534Context, "range(1,3,0)", nodes, nil)
	if gotRangeZero != nil {
		t.Fatalf("range(1,3,0) = %#v, want invalid evaluation", gotRangeZero)
	}
	fixtureAdmission535Context, fixtureAdmission535 := expressionTestContext(ctx, "slice([1,2,3], 2, 1)")
	_ = fixtureAdmission535
	gotSliceEmpty := e.evaluateExpressionWithContext(fixtureAdmission535, fixtureAdmission535Context, "slice([1,2,3], 2, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceEmpty) {
		t.Fatalf("slice([1,2,3],2,1) = %#v, want []", gotSliceEmpty)
	}
	fixtureAdmission536Context, fixtureAdmission536 := expressionTestContext(ctx, "slice('not-list', 0, 1)")
	_ = fixtureAdmission536
	gotSliceNonList := e.evaluateExpressionWithContext(fixtureAdmission536, fixtureAdmission536Context, "slice('not-list', 0, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceNonList) {
		t.Fatalf("slice('not-list',0,1) = %#v, want []", gotSliceNonList)
	}
	fixtureAdmission537Context, fixtureAdmission537 := expressionTestContext(ctx, "inDegree(missing)")
	_ = fixtureAdmission537
	assertEqual(t, "inDegree(missing)", int64(0), e.evaluateExpressionWithContext(fixtureAdmission537, fixtureAdmission537Context, "inDegree(missing)", nodes, nil))
	fixtureAdmission538Context, fixtureAdmission538 := expressionTestContext(ctx, "outDegree(missing)")
	_ = fixtureAdmission538
	assertEqual(t, "outDegree(missing)", int64(0), e.evaluateExpressionWithContext(fixtureAdmission538, fixtureAdmission538Context, "outDegree(missing)", nodes, nil))
	fixtureAdmission539Context, fixtureAdmission539 := expressionTestContext(ctx, "hasLabels(a, 'bad')")
	_ = fixtureAdmission539
	assertEqual(t, "hasLabels(a, 'bad')", false, e.evaluateExpressionWithContext(fixtureAdmission539, fixtureAdmission539Context, "hasLabels(a, 'bad')", nodes, nil))
	fixtureAdmission540Context, fixtureAdmission540 := expressionTestContext(ctx, "hasLabels(missing, ['Person'])")
	_ = fixtureAdmission540
	assertEqual(t, "hasLabels(missing, ['Person'])", false, e.evaluateExpressionWithContext(fixtureAdmission540, fixtureAdmission540Context, "hasLabels(missing, ['Person'])", nodes, nil))

	fixtureAdmission541Context, fixtureAdmission541 := expressionTestContext(ctx, "apoc.map.fromPairs([['a',1],['b',2]])")
	_ = fixtureAdmission541
	fromPairs := e.evaluateExpressionWithContext(fixtureAdmission541, fixtureAdmission541Context, "apoc.map.fromPairs([['a',1],['b',2]])", nodes, nil)
	fromPairsMap, ok := fromPairs.(map[string]interface{})
	if !ok {
		t.Fatalf("fromPairs result type = %T, want map[string]interface{}", fromPairs)
	}
	if fromPairsMap["a"] != int64(1) || fromPairsMap["b"] != int64(2) {
		t.Fatalf("unexpected fromPairs result: %#v", fromPairsMap)
	}

	fixtureAdmission542Context, fixtureAdmission542 := expressionTestContext(ctx, "apoc.map.clean(a.cleanMap, ['b'], [null])")
	_ = fixtureAdmission542
	clean := e.evaluateExpressionWithContext(fixtureAdmission542, fixtureAdmission542Context, "apoc.map.clean(a.cleanMap, ['b'], [null])", nodes, nil)
	cleanMap, ok := clean.(map[string]interface{})
	if !ok {
		t.Fatalf("clean result type = %T, want map[string]interface{}", clean)
	}
	if cleanMap["a"] != int64(1) {
		t.Fatalf("clean map missing expected a=1: %#v", cleanMap)
	}
	_, hasB := cleanMap["b"]
	if hasB {
		t.Fatalf("clean map should not contain key b: %#v", cleanMap)
	}
	_, hasC := cleanMap["c"]
	if hasC {
		t.Fatalf("clean map should not contain key c: %#v", cleanMap)
	}

	fixtureAdmission543Context, fixtureAdmission543 := expressionTestContext(ctx, "apoc.map.fromPairs('bad')")
	_ = fixtureAdmission543
	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext(fixtureAdmission543, fixtureAdmission543Context, "apoc.map.fromPairs('bad')", nodes, nil)) {
		t.Fatalf("apoc.map.fromPairs('bad') should return empty map")
	}
	fixtureAdmission544Context, fixtureAdmission544 := expressionTestContext(ctx, "apoc.map.clean('bad')")
	_ = fixtureAdmission544
	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext(fixtureAdmission544, fixtureAdmission544Context, "apoc.map.clean('bad')", nodes, nil)) {
		t.Fatalf("apoc.map.clean('bad') should return empty map")
	}
}

func TestFunctionAdditionalMathNilAndFallbackBranches(t *testing.T) {
	ctx := context.Background()
	e := setupTestExecutor(t)
	a := createTestNode(t, e, "m-a", []string{"T"}, map[string]interface{}{"name": "a"})
	b := createTestNode(t, e, "m-b", []string{"T"}, map[string]interface{}{"name": "b"})
	rel := &storage.Edge{ID: "m-r1", Type: "REL", StartNode: a.ID, EndNode: b.ID}
	if err := e.storage.CreateEdge(rel); err != nil {
		t.Fatalf("create edge failed: %v", err)
	}
	nodes := map[string]*storage.Node{"a": a, "b": b}
	rels := map[string]*storage.Edge{"r": rel}

	nilExprs := []string{
		"sin('x')",
		"atan2(1)",
		"power(2)",
		"startNode(missing)",
		"endNode(missing)",
		"nullIf('a')",
		"btrim()",
		"char_length(123)",
		"character_length(123)",
		"normalize(123)",
		"percentileCont(1)",
		"percentileDisc(1)",
		"reduce(acc 0, x IN [1,2] | acc + x)", // malformed reduce syntax
	}
	for _, expr := range nilExprs {
		fixtureAdmission545Context, fixtureAdmission545 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission545
		if got := e.evaluateExpressionWithContext(fixtureAdmission545, fixtureAdmission545Context, expr, nodes, rels); got != nil {
			t.Fatalf("%s should be nil, got %T %#v", expr, got, got)
		}
	}

	// isNaN of a string is a type error in Neo4j; the expression evaluator
	// reports errors as null.
	fixtureAdmission546Context, fixtureAdmission546 := expressionTestContext(ctx, "isNaN('x')")
	_ = fixtureAdmission546
	if got := e.evaluateExpressionWithContext(fixtureAdmission546, fixtureAdmission546Context, "isNaN('x')", nodes, rels); got != nil {
		t.Fatalf("isNaN('x') = %#v, want nil", got)
	}
	// isEmpty of a number is Neo4j's TypeError, null in this evaluator.
	fixtureAdmission547Context, fixtureAdmission547 := expressionTestContext(ctx, "isEmpty(123)")
	_ = fixtureAdmission547
	if got := e.evaluateExpressionWithContext(fixtureAdmission547, fixtureAdmission547Context, "isEmpty(123)", nodes, rels); got != nil {
		t.Fatalf("isEmpty(123) = %#v, want nil", got)
	}
	fixtureAdmission548Context, fixtureAdmission548 := expressionTestContext(ctx, "isEmpty(NULL)")
	_ = fixtureAdmission548
	if got := e.evaluateExpressionWithContext(fixtureAdmission548, fixtureAdmission548Context, "isEmpty(NULL)", nodes, rels); got != nil {
		t.Fatalf("isEmpty(NULL) = %#v, want nil (Neo4j)", got)
	}

	fixtureAdmission549Context, fixtureAdmission549 := expressionTestContext(ctx, "coth(0)")
	_ = fixtureAdmission549
	if got := e.evaluateExpressionWithContext(fixtureAdmission549, fixtureAdmission549Context, "coth(0)", nodes, rels); !math.IsNaN(got.(float64)) {
		t.Fatalf("coth(0) should be NaN, got %#v", got)
	}

	// nodes()/relationships() empty fallback branches.
	fixtureAdmission550Context, fixtureAdmission550 := expressionTestContext(ctx, "nodes(missingPath)")
	_ = fixtureAdmission550
	if got := e.evaluateExpressionWithContext(fixtureAdmission550, fixtureAdmission550Context, "nodes(missingPath)", nodes, rels); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("nodes(missingPath) = %#v, want []", got)
	}
	fixtureAdmission551Context, fixtureAdmission551 := expressionTestContext(ctx, "relationships(missingPath)")
	_ = fixtureAdmission551
	if got := e.evaluateExpressionWithContext(fixtureAdmission551, fixtureAdmission551Context, "relationships(missingPath)", nodes, rels); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("relationships(missingPath) = %#v, want []", got)
	}
}

func TestFunctionFullMathAdditionalCoverage(t *testing.T) {
	e := setupTestExecutor(t)
	a := createTestNode(t, e, "fm-a", []string{"T"}, map[string]interface{}{"name": "a"})
	b := createTestNode(t, e, "fm-b", []string{"T"}, map[string]interface{}{"name": "b"})
	rel := &storage.Edge{ID: "fm-r", Type: "REL", StartNode: a.ID, EndNode: b.ID, Properties: map[string]interface{}{"w": int64(2)}}
	if err := e.storage.CreateEdge(rel); err != nil {
		t.Fatalf("create edge failed: %v", err)
	}

	nodes := map[string]*storage.Node{"a": a, "b": b}
	rels := map[string]*storage.Edge{"r": rel}
	paths := map[string]*PathResult{
		"p": {Nodes: []*storage.Node{a, b}, Relationships: []*storage.Edge{rel}},
	}

	eval := func(expr string, allEdges []*storage.Edge, allNodes []*storage.Node, pathLen int) interface{} {
		ctx := context.Background()

		fixtureAdmission552Context, fixtureAdmission552 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission552
		return e.evaluateExpressionWithContextFullFunctions(fixtureAdmission552, fixtureAdmission552Context,
			expr,

			nodes,
			rels,
			paths,
			allEdges,
			allNodes,
			pathLen,
		)
	}

	if got := eval("pi()", nil, nil, 0).(float64); math.Abs(got-math.Pi) > 0.0001 {
		t.Fatalf("pi() = %v", got)
	}
	if got := eval("e()", nil, nil, 0).(float64); math.Abs(got-math.E) > 0.0001 {
		t.Fatalf("e() = %v", got)
	}
	// acos() reads musl's acos, the one every route uses (#907).
	if got := eval("acos(0.5)", nil, nil, 0).(float64); math.Abs(got-math.Pi/3) > 1e-15 {
		t.Fatalf("acos(0.5) = %v", got)
	}
	if got := eval("startNode(r)", nil, nil, 0).(*storage.Node); got.ID != a.ID {
		t.Fatalf("startNode(r) = %s, want %s", got.ID, a.ID)
	}
	if got := eval("endNode(r)", nil, nil, 0).(*storage.Node); got.ID != b.ID {
		t.Fatalf("endNode(r) = %s, want %s", got.ID, b.ID)
	}

	gotNodes := eval("nodes(p)", nil, nil, 0).([]interface{})
	if len(gotNodes) != 2 {
		t.Fatalf("nodes(p) len = %d, want 2", len(gotNodes))
	}
	gotNodes = eval("nodes(missing)", nil, []*storage.Node{a, b}, 0).([]interface{})
	if len(gotNodes) != 2 {
		t.Fatalf("nodes(allPathNodes) len = %d, want 2", len(gotNodes))
	}
	gotNodes = eval("nodes(a)", nil, nil, 0).([]interface{})
	if len(gotNodes) != 1 {
		t.Fatalf("nodes(a) len = %d, want 1", len(gotNodes))
	}

	gotRels := eval("relationships(p)", nil, nil, 0).([]interface{})
	if len(gotRels) != 1 {
		t.Fatalf("relationships(p) len = %d, want 1", len(gotRels))
	}
	gotRels = eval("relationships(missing)", []*storage.Edge{rel}, nil, 0).([]interface{})
	if len(gotRels) != 1 {
		t.Fatalf("relationships(allPathEdges) len = %d, want 1", len(gotRels))
	}
	gotRels = eval("relationships(r)", nil, nil, 0).([]interface{})
	if len(gotRels) != 1 {
		t.Fatalf("relationships(r) len = %d, want 1", len(gotRels))
	}

	if got := eval("point.x(point({x: 3, y: 4}))", nil, nil, 0); got != float64(3) {
		t.Fatalf("point.x = %#v", got)
	}
	if got := eval("point.y(point({x: 3, y: 4}))", nil, nil, 0); got != float64(4) {
		t.Fatalf("point.y = %#v", got)
	}
	if got := eval("point.srid(point({latitude: 1, longitude: 2}))", nil, nil, 0); got != int64(4326) {
		t.Fatalf("point.srid(lat/lon) = %#v", got)
	}
	if got := eval("point.srid(point({x: 1, y: 2}))", nil, nil, 0); got != int64(7203) {
		t.Fatalf("point.srid(cartesian default) = %#v", got)
	}
	if got := eval("point.srid(point({x: 1, y: 2, z: 3, srid: 9157}))", nil, nil, 0); got != int64(9157) {
		t.Fatalf("point.srid(explicit) = %#v", got)
	}
	if got := eval("point.crs(point({x: 1, y: 2, z: 3}))", nil, nil, 0); got != "cartesian-3d" {
		t.Fatalf("point.crs = %#v", got)
	}
	if got := eval("point.crs(point({latitude: 1, longitude: 2}))", nil, nil, 0); got != "wgs-84" {
		t.Fatalf("point.crs(wgs84) = %#v", got)
	}
	if got := eval("point.crs(point({latitude: 1, longitude: 2, height: 3}))", nil, nil, 0); got != "wgs-84-3d" {
		t.Fatalf("point.crs(wgs84-3d) = %#v", got)
	}
	if got := eval("point.crs({x: 1, y: 2, crs: 'custom'})", nil, nil, 0); got != "custom" {
		t.Fatalf("point.crs(custom) = %#v", got)
	}
	if got := eval("point.height({altitude: 7})", nil, nil, 0); got != float64(7) {
		t.Fatalf("point.height = %#v", got)
	}
	if got := eval("point.height({z: 8})", nil, nil, 0); got != float64(8) {
		t.Fatalf("point.height(z) = %#v", got)
	}
	if got := eval("point.height({height: 9})", nil, nil, 0); got != float64(9) {
		t.Fatalf("point.height(height) = %#v", got)
	}
	if got := eval("point.z({z: 10})", nil, nil, 0); got != float64(10) {
		t.Fatalf("point.z = %#v", got)
	}
	if got := eval("point.latitude(point({latitude: 12.5, longitude: 22.5}))", nil, nil, 0); got != float64(12.5) {
		t.Fatalf("point.latitude = %#v", got)
	}
	if got := eval("point.longitude(point({latitude: 12.5, longitude: 22.5}))", nil, nil, 0); got != float64(22.5) {
		t.Fatalf("point.longitude = %#v", got)
	}
	if got := eval("distance(point({x:0,y:0}), point({x:3,y:4}))", nil, nil, 0); got != float64(5) {
		t.Fatalf("distance = %#v", got)
	}
	if got := eval("distance(point({latitude:0,longitude:0}), point({latitude:0,longitude:0.01}))", nil, nil, 0); got == nil {
		t.Fatalf("distance(lat/lon) should not be nil")
	}
	if got := eval("point.withinBBox(point({x:1,y:1}), point({x:0,y:0}), point({x:2,y:2}))", nil, nil, 0); got != true {
		t.Fatalf("point.withinBBox = %#v", got)
	}
	if got := eval("withinBBox(point({latitude:1,longitude:1}), point({latitude:0,longitude:0}), point({latitude:2,longitude:2}))", nil, nil, 0); got != true {
		t.Fatalf("withinBBox(lat/lon) = %#v", got)
	}
	if got := eval("point.withinDistance(point({x:1,y:1}), point({x:0,y:0}), 2)", nil, nil, 0); got != true {
		t.Fatalf("point.withinDistance = %#v", got)
	}
	if got := eval("point.withinDistance(point({latitude:1,longitude:1}), point({latitude:1,longitude:1}), 1)", nil, nil, 0); got != true {
		t.Fatalf("point.withinDistance(lat/lon) = %#v", got)
	}
	if got := eval("polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:2,y:2})])", nil, nil, 0); got == nil {
		t.Fatalf("polygon should not be nil")
	}
	if got := eval("lineString([point({x:0,y:0}), point({x:2,y:2})])", nil, nil, 0); got == nil {
		t.Fatalf("lineString should not be nil")
	}
	if got := eval("point.intersects(point({x:1,y:1}), polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:2,y:2})]))", nil, nil, 0); got != true {
		t.Fatalf("point.intersects(...) = %#v", got)
	}
	if got := eval("point.contains(polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:2,y:2})]), point({x:1,y:1}))", nil, nil, 0); got != true {
		t.Fatalf("point.contains(...) = %#v", got)
	}
	fixtureAdmission553Context, fixtureAdmission553 := expressionTestContext(context.Background(), "vector.similarity.cosine([1.0,0.0], [1.0,0.0])")
	_ = fixtureAdmission553
	if got := e.evaluateExpressionWithContextFull(fixtureAdmission553, fixtureAdmission553Context, "vector.similarity.cosine([1.0,0.0], [1.0,0.0])", nodes, rels, paths, nil, nil, 0); got == nil {
		t.Fatal("vector.similarity.cosine should not be nil")
	}
	fixtureAdmission554Context, fixtureAdmission554 := expressionTestContext(context.Background(), "vector.similarity.euclidean([1.0,0.0], [1.0,0.0])")
	_ = fixtureAdmission554
	if got := e.evaluateExpressionWithContextFull(fixtureAdmission554, fixtureAdmission554Context, "vector.similarity.euclidean([1.0,0.0], [1.0,0.0])", nodes, rels, paths, nil, nil, 0); got == nil {
		t.Fatal("vector.similarity.euclidean should not be nil")
	}
	if got := eval("all(x IN [1,2,3] WHERE x > 0)", nil, nil, 0); got != true {
		t.Fatalf("all(...) = %#v", got)
	}
	if got := eval("any(x IN [1,2,3] WHERE x = 2)", nil, nil, 0); got != true {
		t.Fatalf("any(...) = %#v", got)
	}
	if got := eval("none(x IN [1,2,3] WHERE x < 0)", nil, nil, 0); got != true {
		t.Fatalf("none(...) = %#v", got)
	}
	if got := eval("single(x IN [1,2,3] WHERE x = 2)", nil, nil, 0); got != true {
		t.Fatalf("single(...) = %#v", got)
	}
	if got := eval("filter(x IN [1,2,3] WHERE x >= 2)", nil, nil, 0).([]interface{}); len(got) != 2 {
		t.Fatalf("filter(...) len = %d, want 2", len(got))
	}
	if got := eval("extract(x IN [1,2,3] | x)", nil, nil, 0).([]interface{}); len(got) != 3 {
		t.Fatalf("extract(...) len = %d, want 3", len(got))
	}
	if got := eval("[x IN [1,2,3] WHERE x > 1]", nil, nil, 0).([]interface{}); len(got) != 2 {
		t.Fatalf("list comp filter len = %d, want 2", len(got))
	}
	if got := eval("[x IN [1,2,3] WHERE x >= 2]", nil, nil, 0).([]interface{}); len(got) != 2 {
		t.Fatalf("list comp >= len = %d, want 2", len(got))
	}
	if got := eval("[x IN [1,2,3] WHERE x = 2]", nil, nil, 0).([]interface{}); len(got) != 1 {
		t.Fatalf("list comp = len = %d, want 1", len(got))
	}
	if got := eval("[x IN [1,2,3] WHERE x != 2]", nil, nil, 0).([]interface{}); len(got) != 2 {
		t.Fatalf("list comp != len = %d, want 2", len(got))
	}
	if got := eval("[x IN [1,2,3]]", nil, nil, 0).([]interface{}); len(got) != 3 {
		t.Fatalf("list comp identity len = %d, want 3", len(got))
	}
	if got := eval("[r IN relationships(p) | type(r)]", nil, nil, 0).([]interface{}); len(got) != 1 || got[0] != "REL" {
		t.Fatalf("relationship transform result = %#v", got)
	}
}

func TestFunctionEvaluator_KalmanAdditionalBranches(t *testing.T) {
	e := setupTestExecutor(t)
	n := createTestNode(t, e, "kal-n", []string{"Kalman"}, map[string]interface{}{})
	nodes := map[string]*storage.Node{"n": n}
	ctx := context.Background()

	fixtureAdmission555Context, fixtureAdmission555 := expressionTestContext(ctx, "kalman.init()")
	_ = fixtureAdmission555
	state := e.evaluateExpressionWithContext(fixtureAdmission555, fixtureAdmission555Context, "kalman.init()", nodes, nil)
	stateStr, ok := state.(string)
	if !ok || stateStr == "" {
		t.Fatalf("kalman.init() should return non-empty state string, got %#v", state)
	}
	n.Properties["state"] = stateStr

	fixtureAdmission556Context, fixtureAdmission556 := expressionTestContext(ctx, "kalman.process(10, n.state)")
	_ = fixtureAdmission556
	processed := e.evaluateExpressionWithContext(fixtureAdmission556, fixtureAdmission556Context, "kalman.process(10, n.state)", nodes, nil)
	processedMap, ok := processed.(map[string]interface{})
	if !ok || processedMap["state"] == nil || processedMap["value"] == nil {
		t.Fatalf("kalman.process should return state/value map, got %#v", processed)
	}
	if gotState, ok := processedMap["state"].(string); ok && gotState != "" {
		n.Properties["state"] = gotState
	}

	fixtureAdmission557Context, fixtureAdmission557 := expressionTestContext(ctx, "kalman.predict(n.state, 2)")
	_ = fixtureAdmission557
	if got := e.evaluateExpressionWithContext(fixtureAdmission557, fixtureAdmission557Context, "kalman.predict(n.state, 2)", nodes, nil); got == nil {
		t.Fatalf("kalman.predict should not be nil")
	}
	fixtureAdmission558Context, fixtureAdmission558 := expressionTestContext(ctx, "kalman.state(n.state)")
	_ = fixtureAdmission558
	if got := e.evaluateExpressionWithContext(fixtureAdmission558, fixtureAdmission558Context, "kalman.state(n.state)", nodes, nil); got == nil {
		t.Fatalf("kalman.state should not be nil")
	}
	fixtureAdmission559Context, fixtureAdmission559 := expressionTestContext(ctx, "kalman.reset(n.state)")
	_ = fixtureAdmission559
	if got := e.evaluateExpressionWithContext(fixtureAdmission559, fixtureAdmission559Context, "kalman.reset(n.state)", nodes, nil); got == nil {
		t.Fatalf("kalman.reset should not be nil")
	}

	fixtureAdmission560Context, fixtureAdmission560 := expressionTestContext(ctx, "kalman.velocity.init()")
	_ = fixtureAdmission560
	vstate := e.evaluateExpressionWithContext(fixtureAdmission560, fixtureAdmission560Context, "kalman.velocity.init()", nodes, nil)
	vstateStr, ok := vstate.(string)
	if !ok || vstateStr == "" {
		t.Fatalf("kalman.velocity.init() should return state string, got %#v", vstate)
	}
	n.Properties["vstate"] = vstateStr
	fixtureAdmission561Context, fixtureAdmission561 := expressionTestContext(ctx, "kalman.velocity.process(5, n.vstate)")
	_ = fixtureAdmission561
	if got := e.evaluateExpressionWithContext(fixtureAdmission561, fixtureAdmission561Context, "kalman.velocity.process(5, n.vstate)", nodes, nil); got == nil {
		t.Fatalf("kalman.velocity.process should not be nil")
	}
	fixtureAdmission562Context, fixtureAdmission562 := expressionTestContext(ctx, "kalman.velocity.predict(n.vstate, 3)")
	_ = fixtureAdmission562
	if got := e.evaluateExpressionWithContext(fixtureAdmission562, fixtureAdmission562Context, "kalman.velocity.predict(n.vstate, 3)", nodes, nil); got == nil {
		t.Fatalf("kalman.velocity.predict should not be nil")
	}

	fixtureAdmission563Context, fixtureAdmission563 := expressionTestContext(ctx, "kalman.adaptive.init()")
	_ = fixtureAdmission563
	astate := e.evaluateExpressionWithContext(fixtureAdmission563, fixtureAdmission563Context, "kalman.adaptive.init()", nodes, nil)
	astateStr, ok := astate.(string)
	if !ok || astateStr == "" {
		t.Fatalf("kalman.adaptive.init() should return state string, got %#v", astate)
	}
	n.Properties["astate"] = astateStr
	fixtureAdmission564Context, fixtureAdmission564 := expressionTestContext(ctx, "kalman.adaptive.process(5, n.astate)")
	_ = fixtureAdmission564
	if got := e.evaluateExpressionWithContext(fixtureAdmission564, fixtureAdmission564Context, "kalman.adaptive.process(5, n.astate)", nodes, nil); got == nil {
		t.Fatalf("kalman.adaptive.process should not be nil")
	}
}

func TestFunctionEvaluator_ArrayIndexingAdditionalBranches(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "idx-n", []string{"Person"}, map[string]interface{}{
		"arrAny": []interface{}{int64(7), int64(8), int64(9)},
		"arrStr": []string{"aa", "bb", "cc"},
		"text":   "hello",
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission402Context, fixtureAdmission402 := expressionTestContext(ctx, "n.arrAny[1]")
	_ = fixtureAdmission402Context
	assertEqual(t, "n.arrAny[1]", int64(8), e.evaluateExpressionWithContext(fixtureAdmission402, fixtureAdmission402Context, "n.arrAny[1]", nodes, nil))
	fixtureAdmission403Context, fixtureAdmission403 := expressionTestContext(ctx, "n.arrAny[-1]")
	_ = fixtureAdmission403Context
	assertEqual(t, "n.arrAny[-1]", int64(9), e.evaluateExpressionWithContext(fixtureAdmission403, fixtureAdmission403Context, "n.arrAny[-1]", nodes, nil))
	fixtureAdmission422Context, fixtureAdmission422 := expressionTestContext(ctx, "n.arrStr[0]")
	_ = fixtureAdmission422Context
	assertEqual(t, "n.arrStr[0]", "aa", e.evaluateExpressionWithContext(fixtureAdmission422, fixtureAdmission422Context, "n.arrStr[0]", nodes, nil))
	fixtureAdmission423Context, fixtureAdmission423 := expressionTestContext(ctx, "n.arrStr[-1]")
	_ = fixtureAdmission423Context
	assertEqual(t, "n.arrStr[-1]", "cc", e.evaluateExpressionWithContext(fixtureAdmission423, fixtureAdmission423Context, "n.arrStr[-1]", nodes, nil))
	assertEqual(t, "substring(n.text,1,1)", "e", typedNumericRead(t, e, "MATCH (n:Person) RETURN substring(n.text,1,1) AS v", nil))
	assertEqual(t, "right(n.text,1)", "o", typedNumericRead(t, e, "MATCH (n:Person) RETURN right(n.text,1) AS v", nil))
	for _, expression := range []string{"n.text[1]", "n.text[-1]"} {
		_, err := e.Execute(ctx, "MATCH (n:Person) RETURN "+expression+" AS v", nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	}

	fixtureAdmission571Context, fixtureAdmission571 := expressionTestContext(ctx, "n.arrAny[99]")
	_ = fixtureAdmission571
	if got := e.evaluateExpressionWithContext(fixtureAdmission571, fixtureAdmission571Context, "n.arrAny[99]", nodes, nil); got != nil {
		t.Fatalf("n.arrAny[99] should be nil, got %#v", got)
	}
	fixtureAdmission572Context, fixtureAdmission572 := expressionTestContext(ctx, "1 IN [1,2,3]")
	_ = fixtureAdmission572
	if got := e.evaluateExpressionWithContext(fixtureAdmission572, fixtureAdmission572Context, "1 IN [1,2,3]", nodes, nil); got != true {
		t.Fatalf("1 IN [1,2,3] should be true, got %#v", got)
	}
	fixtureAdmission573Context, fixtureAdmission573 := expressionTestContext(ctx, "n.arrAny[..2]")
	_ = fixtureAdmission573
	if got := e.evaluateExpressionWithContext(fixtureAdmission573, fixtureAdmission573Context, "n.arrAny[..2]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(7), int64(8)}, got) {
		t.Fatalf("n.arrAny[..2] unexpected: %#v", got)
	}
	fixtureAdmission574Context, fixtureAdmission574 := expressionTestContext(ctx, "n.arrAny[1..]")
	_ = fixtureAdmission574
	if got := e.evaluateExpressionWithContext(fixtureAdmission574, fixtureAdmission574Context, "n.arrAny[1..]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(8), int64(9)}, got) {
		t.Fatalf("n.arrAny[1..] unexpected: %#v", got)
	}
}

func TestFunctionEvaluator_ConversionAndStringFallbackBranches(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "conv-n", []string{"Person"}, map[string]interface{}{
		"arrAny": []interface{}{int64(1), int64(2), int64(3), int64(4)},
		"mixed":  []interface{}{int64(1), float64(2.9), "3", "bad", true, nil},
		"mixedF": []interface{}{float32(1.25), int64(2), "3.5", "bad", true},
		"mixedB": []interface{}{true, "false", "bad", int64(1)},
		"text":   "xy",
	})
	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	fixtureAdmission575Context, fixtureAdmission575 := expressionTestContext(ctx, "")
	_ = fixtureAdmission575
	if got := e.evaluateExpressionWithContextFullFunctions(fixtureAdmission575, fixtureAdmission575Context, "", nodes, nil, nil, nil, nil, 0); got != nil {
		t.Fatalf("empty expr should return nil, got %#v", got)
	}

	// Additional array-indexing/slicing branches.
	fixtureAdmission576Context, fixtureAdmission576 := expressionTestContext(ctx, "n.arrAny[-3..-1]")
	_ = fixtureAdmission576
	if got := e.evaluateExpressionWithContext(fixtureAdmission576, fixtureAdmission576Context, "n.arrAny[-3..-1]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(2), int64(3)}, got) {
		t.Fatalf("n.arrAny[-3..-1] unexpected: %#v", got)
	}
	fixtureAdmission577Context, fixtureAdmission577 := expressionTestContext(ctx, "n.arrAny[-99..2]")
	_ = fixtureAdmission577
	if got := e.evaluateExpressionWithContext(fixtureAdmission577, fixtureAdmission577Context, "n.arrAny[-99..2]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(1), int64(2)}, got) {
		t.Fatalf("n.arrAny[-99..2] unexpected: %#v", got)
	}
	fixtureAdmission578Context, fixtureAdmission578 := expressionTestContext(ctx, "n.arrAny[3..1]")
	_ = fixtureAdmission578
	if got := e.evaluateExpressionWithContext(fixtureAdmission578, fixtureAdmission578Context, "n.arrAny[3..1]", nodes, nil); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("n.arrAny[3..1] unexpected: %#v", got)
	}
	// Neo4j slices a non-list value as a list of that value (#907).
	fixtureAdmission579Context, fixtureAdmission579 := expressionTestContext(ctx, "n.text[..1]")
	_ = fixtureAdmission579
	if got := e.evaluateExpressionWithContext(fixtureAdmission579, fixtureAdmission579Context, "n.text[..1]", nodes, nil); !reflect.DeepEqual([]interface{}{"xy"}, got) {
		t.Fatalf("n.text[..1] unexpected: %#v", got)
	}
	// Neo4j list indices are integers, without implicit numeric-string coercion.
	// Preserve the successful selection with an explicit conversion and retain
	// an error assertion for the original invalid expression.
	assertEqual(t, "n.arrAny[toInteger('2')]", int64(3), typedNumericRead(t, e, "MATCH (n:Person) RETURN n.arrAny[toInteger('2')] AS v", nil))
	_, err := e.Execute(ctx, "MATCH (n:Person) RETURN n.arrAny['2'] AS v", nil)
	// The literal STRING index is rejected during statement validation;
	// dynamic invalid indices are separately checked as runtime TypeErrors.
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	// Scalar conversions.
	fixtureAdmission581Context, fixtureAdmission581 := expressionTestContext(ctx, "toInteger(2.9)")
	_ = fixtureAdmission581
	if got := e.evaluateExpressionWithContext(fixtureAdmission581, fixtureAdmission581Context, "toInteger(2.9)", nodes, nil); got != int64(2) {
		t.Fatalf("toInteger(2.9) = %#v", got)
	}
	fixtureAdmission582Context, fixtureAdmission582 := expressionTestContext(ctx, "toInteger('bad')")
	_ = fixtureAdmission582
	if got := e.evaluateExpressionWithContext(fixtureAdmission582, fixtureAdmission582Context, "toInteger('bad')", nodes, nil); got != nil {
		t.Fatalf("toInteger('bad') should be nil, got %#v", got)
	}
	fixtureAdmission583Context, fixtureAdmission583 := expressionTestContext(ctx, "toInt(3)")
	_ = fixtureAdmission583
	if got := e.evaluateExpressionWithContext(fixtureAdmission583, fixtureAdmission583Context, "toInt(3)", nodes, nil); got != int64(3) {
		t.Fatalf("toInt(3) = %#v", got)
	}
	fixtureAdmission584Context, fixtureAdmission584 := expressionTestContext(ctx, "toInt('bad')")
	_ = fixtureAdmission584
	if got := e.evaluateExpressionWithContext(fixtureAdmission584, fixtureAdmission584Context, "toInt('bad')", nodes, nil); got != nil {
		t.Fatalf("toInt('bad') should be nil, got %#v", got)
	}
	fixtureAdmission585Context, fixtureAdmission585 := expressionTestContext(ctx, "toFloat(2)")
	_ = fixtureAdmission585
	if got := e.evaluateExpressionWithContext(fixtureAdmission585, fixtureAdmission585Context, "toFloat(2)", nodes, nil); got != float64(2) {
		t.Fatalf("toFloat(2) = %#v", got)
	}
	fixtureAdmission586Context, fixtureAdmission586 := expressionTestContext(ctx, "toFloat('3.5')")
	_ = fixtureAdmission586
	if got := e.evaluateExpressionWithContext(fixtureAdmission586, fixtureAdmission586Context, "toFloat('3.5')", nodes, nil); got != float64(3.5) {
		t.Fatalf("toFloat('3.5') = %#v", got)
	}
	fixtureAdmission587Context, fixtureAdmission587 := expressionTestContext(ctx, "toFloat('bad')")
	_ = fixtureAdmission587
	if got := e.evaluateExpressionWithContext(fixtureAdmission587, fixtureAdmission587Context, "toFloat('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloat('bad') should be nil, got %#v", got)
	}
	fixtureAdmission588Context, fixtureAdmission588 := expressionTestContext(ctx, "toBoolean('TRUE')")
	_ = fixtureAdmission588
	if got := e.evaluateExpressionWithContext(fixtureAdmission588, fixtureAdmission588Context, "toBoolean('TRUE')", nodes, nil); got != true {
		t.Fatalf("toBoolean('TRUE') = %#v", got)
	}
	fixtureAdmission589Context, fixtureAdmission589 := expressionTestContext(ctx, "toBoolean(1)")
	_ = fixtureAdmission589
	if got := e.evaluateExpressionWithContext(fixtureAdmission589, fixtureAdmission589Context, "toBoolean(1)", nodes, nil); got != true {
		t.Fatalf("toBoolean(1) should be true, got %#v", got)
	}

	// OrNull conversion variants.
	fixtureAdmission590Context, fixtureAdmission590 := expressionTestContext(ctx, "toIntegerOrNull('7')")
	_ = fixtureAdmission590
	if got := e.evaluateExpressionWithContext(fixtureAdmission590, fixtureAdmission590Context, "toIntegerOrNull('7')", nodes, nil); got != int64(7) {
		t.Fatalf("toIntegerOrNull('7') = %#v", got)
	}
	fixtureAdmission591Context, fixtureAdmission591 := expressionTestContext(ctx, "toIntegerOrNull('bad')")
	_ = fixtureAdmission591
	if got := e.evaluateExpressionWithContext(fixtureAdmission591, fixtureAdmission591Context, "toIntegerOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toIntegerOrNull('bad') should be nil, got %#v", got)
	}
	fixtureAdmission592Context, fixtureAdmission592 := expressionTestContext(ctx, "toFloatOrNull('2.25')")
	_ = fixtureAdmission592
	if got := e.evaluateExpressionWithContext(fixtureAdmission592, fixtureAdmission592Context, "toFloatOrNull('2.25')", nodes, nil); got != float64(2.25) {
		t.Fatalf("toFloatOrNull('2.25') = %#v", got)
	}
	fixtureAdmission593Context, fixtureAdmission593 := expressionTestContext(ctx, "toFloatOrNull('bad')")
	_ = fixtureAdmission593
	if got := e.evaluateExpressionWithContext(fixtureAdmission593, fixtureAdmission593Context, "toFloatOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloatOrNull('bad') should be nil, got %#v", got)
	}
	fixtureAdmission594Context, fixtureAdmission594 := expressionTestContext(ctx, "toBooleanOrNull('false')")
	_ = fixtureAdmission594
	if got := e.evaluateExpressionWithContext(fixtureAdmission594, fixtureAdmission594Context, "toBooleanOrNull('false')", nodes, nil); got != false {
		t.Fatalf("toBooleanOrNull('false') = %#v", got)
	}
	fixtureAdmission595Context, fixtureAdmission595 := expressionTestContext(ctx, "toBooleanOrNull('bad')")
	_ = fixtureAdmission595
	if got := e.evaluateExpressionWithContext(fixtureAdmission595, fixtureAdmission595Context, "toBooleanOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toBooleanOrNull('bad') should be nil, got %#v", got)
	}
	fixtureAdmission596Context, fixtureAdmission596 := expressionTestContext(ctx, "toStringOrNull(null)")
	_ = fixtureAdmission596
	if got := e.evaluateExpressionWithContext(fixtureAdmission596, fixtureAdmission596Context, "toStringOrNull(null)", nodes, nil); got != nil {
		t.Fatalf("toStringOrNull(null) should be nil, got %#v", got)
	}
	fixtureAdmission597Context, fixtureAdmission597 := expressionTestContext(ctx, "toStringOrNull(12)")
	_ = fixtureAdmission597
	if got := e.evaluateExpressionWithContext(fixtureAdmission597, fixtureAdmission597Context, "toStringOrNull(12)", nodes, nil); got != "12" {
		t.Fatalf("toStringOrNull(12) = %#v", got)
	}

	// List conversion functions.
	fixtureAdmission598Context, fixtureAdmission598 := expressionTestContext(ctx, "toIntegerList(n.mixed)")
	_ = fixtureAdmission598
	if got := e.evaluateExpressionWithContext(fixtureAdmission598, fixtureAdmission598Context, "toIntegerList(n.mixed)", nodes, nil); !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3), nil, int64(1), nil}, got) {
		t.Fatalf("toIntegerList(n.mixed) unexpected: %#v", got)
	}
	fixtureAdmission599Context, fixtureAdmission599 := expressionTestContext(ctx, "toIntegerList('bad')")
	_ = fixtureAdmission599
	if got := e.evaluateExpressionWithContext(fixtureAdmission599, fixtureAdmission599Context, "toIntegerList('bad')", nodes, nil); got != nil {
		t.Fatalf("toIntegerList('bad') should be nil, got %#v", got)
	}
	fixtureAdmission600Context, fixtureAdmission600 := expressionTestContext(ctx, "toFloatList(n.mixedF)")
	_ = fixtureAdmission600
	if got := e.evaluateExpressionWithContext(fixtureAdmission600, fixtureAdmission600Context, "toFloatList(n.mixedF)", nodes, nil); !reflect.DeepEqual([]interface{}{float64(1.25), float64(2), float64(3.5), nil, nil}, got) {
		t.Fatalf("toFloatList(n.mixedF) unexpected: %#v", got)
	}
	fixtureAdmission601Context, fixtureAdmission601 := expressionTestContext(ctx, "toFloatList('bad')")
	_ = fixtureAdmission601
	if got := e.evaluateExpressionWithContext(fixtureAdmission601, fixtureAdmission601Context, "toFloatList('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloatList('bad') should be nil, got %#v", got)
	}
	fixtureAdmission602Context, fixtureAdmission602 := expressionTestContext(ctx, "toBooleanList(n.mixedB)")
	_ = fixtureAdmission602
	if got := e.evaluateExpressionWithContext(fixtureAdmission602, fixtureAdmission602Context, "toBooleanList(n.mixedB)", nodes, nil); !reflect.DeepEqual([]interface{}{true, false, nil, true}, got) {
		t.Fatalf("toBooleanList(n.mixedB) unexpected: %#v", got)
	}
	fixtureAdmission603Context, fixtureAdmission603 := expressionTestContext(ctx, "toBooleanList('bad')")
	_ = fixtureAdmission603
	if got := e.evaluateExpressionWithContext(fixtureAdmission603, fixtureAdmission603Context, "toBooleanList('bad')", nodes, nil); got != nil {
		t.Fatalf("toBooleanList('bad') should be nil, got %#v", got)
	}
	fixtureAdmission604Context, fixtureAdmission604 := expressionTestContext(ctx, "toStringList([1, null, true])")
	_ = fixtureAdmission604
	if got := e.evaluateExpressionWithContext(fixtureAdmission604, fixtureAdmission604Context, "toStringList([1, null, true])", nodes, nil); !reflect.DeepEqual([]interface{}{"1", nil, "true"}, got) {
		t.Fatalf("toStringList([1, null, true]) unexpected: %#v", got)
	}
	fixtureAdmission605Context, fixtureAdmission605 := expressionTestContext(ctx, "toStringList('bad')")
	_ = fixtureAdmission605
	if got := e.evaluateExpressionWithContext(fixtureAdmission605, fixtureAdmission605Context, "toStringList('bad')", nodes, nil); got != nil {
		t.Fatalf("toStringList('bad') should be nil, got %#v", got)
	}

	// Utility + aggregate-in-expression branches.
	fixtureAdmission606Context, fixtureAdmission606 := expressionTestContext(ctx, "valueType({k: 1})")
	_ = fixtureAdmission606
	if got := e.evaluateExpressionWithContext(fixtureAdmission606, fixtureAdmission606Context, "valueType({k: 1})", nodes, nil); got != "MAP NOT NULL" {
		t.Fatalf("valueType({k:1}) = %#v", got)
	}
	fixtureAdmission607Context, fixtureAdmission607 := expressionTestContext(ctx, "valueType([1])")
	_ = fixtureAdmission607
	if got := e.evaluateExpressionWithContext(fixtureAdmission607, fixtureAdmission607Context, "valueType([1])", nodes, nil); got != "LIST<INTEGER NOT NULL> NOT NULL" {
		t.Fatalf("valueType([1]) = %#v", got)
	}
	fixtureAdmission608Context, fixtureAdmission608 := expressionTestContext(ctx, "valueType(n)")
	_ = fixtureAdmission608
	if got := e.evaluateExpressionWithContext(fixtureAdmission608, fixtureAdmission608Context, "valueType(n)", nodes, nil); got != "NODE NOT NULL" {
		t.Fatalf("valueType(n) = %#v", got)
	}
	fixtureAdmission609Context, fixtureAdmission609 := expressionTestContext(ctx, "collect(null)")
	_ = fixtureAdmission609
	if got := e.evaluateExpressionWithContext(fixtureAdmission609, fixtureAdmission609Context, "collect(null)", nodes, nil); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("collect(null) unexpected: %#v", got)
	}

	// String helper nil/fallback branches.
	for _, expr := range []string{"lower(1)", "upper(1)", "trim(1)", "ltrim(1)", "rtrim(1)"} {
		fixtureAdmission610Context, fixtureAdmission610 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission610
		if got := e.evaluateExpressionWithContext(fixtureAdmission610, fixtureAdmission610Context, expr, nodes, nil); got != nil {
			t.Fatalf("%s should be nil, got %#v", expr, got)
		}
	}
	fixtureAdmission611Context, fixtureAdmission611 := expressionTestContext(ctx, "replace('abc', 'a')")
	_ = fixtureAdmission611
	if got := e.evaluateExpressionWithContext(fixtureAdmission611, fixtureAdmission611Context, "replace('abc', 'a')", nodes, nil); got != nil {
		t.Fatalf("replace with missing arg should be nil, got %#v", got)
	}
	fixtureAdmission612Context, fixtureAdmission612 := expressionTestContext(ctx, "split('a')")
	_ = fixtureAdmission612
	if got := e.evaluateExpressionWithContext(fixtureAdmission612, fixtureAdmission612Context, "split('a')", nodes, nil); got != nil {
		t.Fatalf("split with missing delimiter should be nil, got %#v", got)
	}
	fixtureAdmission613Context, fixtureAdmission613 := expressionTestContext(ctx, "substring('abc', 99, 2)")
	_ = fixtureAdmission613
	if got := e.evaluateExpressionWithContext(fixtureAdmission613, fixtureAdmission613Context, "substring('abc', 99, 2)", nodes, nil); got != "" {
		t.Fatalf("substring start >= len should be empty, got %#v", got)
	}
	fixtureAdmission614Context, fixtureAdmission614 := expressionTestContext(ctx, "substring('abc')")
	_ = fixtureAdmission614
	if got := e.evaluateExpressionWithContext(fixtureAdmission614, fixtureAdmission614Context, "substring('abc')", nodes, nil); got != nil {
		t.Fatalf("substring with missing args should be nil, got %#v", got)
	}
	fixtureAdmission615Context, fixtureAdmission615 := expressionTestContext(ctx, "left('abc', 99)")
	_ = fixtureAdmission615
	if got := e.evaluateExpressionWithContext(fixtureAdmission615, fixtureAdmission615Context, "left('abc', 99)", nodes, nil); got != "abc" {
		t.Fatalf("left('abc',99) = %#v", got)
	}
	fixtureAdmission616Context, fixtureAdmission616 := expressionTestContext(ctx, "left('abc')")
	_ = fixtureAdmission616
	if got := e.evaluateExpressionWithContext(fixtureAdmission616, fixtureAdmission616Context, "left('abc')", nodes, nil); got != nil {
		t.Fatalf("left with missing args should be nil, got %#v", got)
	}
	fixtureAdmission617Context, fixtureAdmission617 := expressionTestContext(ctx, "right('abc', 99)")
	_ = fixtureAdmission617
	if got := e.evaluateExpressionWithContext(fixtureAdmission617, fixtureAdmission617Context, "right('abc', 99)", nodes, nil); got != "abc" {
		t.Fatalf("right('abc',99) = %#v", got)
	}
	fixtureAdmission618Context, fixtureAdmission618 := expressionTestContext(ctx, "right('abc')")
	_ = fixtureAdmission618
	if got := e.evaluateExpressionWithContext(fixtureAdmission618, fixtureAdmission618Context, "right('abc')", nodes, nil); got != nil {
		t.Fatalf("right with missing args should be nil, got %#v", got)
	}
	fixtureAdmission619Context, fixtureAdmission619 := expressionTestContext(ctx, "lpad('x', bad)")
	_ = fixtureAdmission619
	if got := e.evaluateExpressionWithContext(fixtureAdmission619, fixtureAdmission619Context, "lpad('x', bad)", nodes, nil); got != nil {
		t.Fatalf("lpad invalid length should be nil, got %#v", got)
	}
	fixtureAdmission620Context, fixtureAdmission620 := expressionTestContext(ctx, "rpad('x', bad)")
	_ = fixtureAdmission620
	if got := e.evaluateExpressionWithContext(fixtureAdmission620, fixtureAdmission620Context, "rpad('x', bad)", nodes, nil); got != nil {
		t.Fatalf("rpad invalid length should be nil, got %#v", got)
	}
	fixtureAdmission621Context, fixtureAdmission621 := expressionTestContext(ctx, "format()")
	_ = fixtureAdmission621
	if got := e.evaluateExpressionWithContext(fixtureAdmission621, fixtureAdmission621Context, "format()", nodes, nil); got != nil {
		t.Fatalf("format() should be nil, got %#v", got)
	}
}

func TestFunctionFullMath_AdditionalInvalidInputBranches(t *testing.T) {
	e := setupTestExecutor(t)
	nodes := map[string]*storage.Node{
		"n": {ID: "math-n", Properties: map[string]interface{}{"x": "bad"}},
	}

	nilExprs := []string{
		"cos('bad')",
		"tan('bad')",
		"cot('bad')",
		"asin('bad')",
		"acos('bad')",
		"atan('bad')",
		"atan2(1)",
		"exp('bad')",
		"log('bad')",
		"log10('bad')",
		"sqrt('bad')",
		"radians('bad')",
		"degrees('bad')",
		"haversin('bad')",
		"sinh('bad')",
		"cosh('bad')",
		"tanh('bad')",
		"power(2)",
		"lpad('x', bad)",
		"rpad('x', bad)",
		"point.x('bad')",
		"point.y('bad')",
		"point.z('bad')",
		"point.latitude('bad')",
		"point.longitude('bad')",
		"point.srid('bad')",
		"point.distance('bad', point({x:0,y:0}))",
		"point.height('bad')",
		"point.crs('bad')",
		"polygon([point({x:0,y:0}), point({x:1,y:1})])",
		"lineString([point({x:0,y:0})])",
	}
	ctx := context.Background()
	for _, expr := range nilExprs {
		fixtureAdmission622Context, fixtureAdmission622 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission622
		if got := e.evaluateExpressionWithContext(fixtureAdmission622, fixtureAdmission622Context, expr, nodes, nil); got != nil {
			t.Fatalf("%s should be nil, got %#v", expr, got)
		}
	}
	fixtureAdmission623Context, fixtureAdmission623 := expressionTestContext(ctx, "point.withinBBox('bad', point({x:0,y:0}), point({x:1,y:1}))")
	_ = fixtureAdmission623
	if got := e.evaluateExpressionWithContext(fixtureAdmission623, fixtureAdmission623Context, "point.withinBBox('bad', point({x:0,y:0}), point({x:1,y:1}))", nodes, nil); got != nil {
		t.Fatalf("point.withinBBox with an argument that isn't a point should be nil (Neo4j), got %#v", got)
	}
	fixtureAdmission624Context, fixtureAdmission624 := expressionTestContext(ctx, "point.withinDistance('bad', point({x:0,y:0}), 1)")
	_ = fixtureAdmission624
	if got := e.evaluateExpressionWithContext(fixtureAdmission624, fixtureAdmission624Context, "point.withinDistance('bad', point({x:0,y:0}), 1)", nodes, nil); got != false {
		t.Fatalf("point.withinDistance invalid input should be false, got %#v", got)
	}
	fixtureAdmission625Context, fixtureAdmission625 := expressionTestContext(ctx, "point.intersects(point({x:1,y:1}), 'bad')")
	_ = fixtureAdmission625
	if got := e.evaluateExpressionWithContext(fixtureAdmission625, fixtureAdmission625Context, "point.intersects(point({x:1,y:1}), 'bad')", nodes, nil); got != false {
		t.Fatalf("point.intersects invalid input should be false, got %#v", got)
	}
	fixtureAdmission626Context, fixtureAdmission626 := expressionTestContext(ctx, "point.contains('bad', point({x:1,y:1}))")
	_ = fixtureAdmission626
	if got := e.evaluateExpressionWithContext(fixtureAdmission626, fixtureAdmission626Context, "point.contains('bad', point({x:1,y:1}))", nodes, nil); got != false {
		t.Fatalf("point.contains invalid input should be false, got %#v", got)
	}

	fixtureAdmission627Context, fixtureAdmission627 := expressionTestContext(ctx, "isNaN('bad')")
	_ = fixtureAdmission627
	if got := e.evaluateExpressionWithContext(fixtureAdmission627, fixtureAdmission627Context, "isNaN('bad')", nodes, nil); got != nil {
		t.Fatalf("isNaN('bad') should be nil, got %#v", got)
	}
	// A string literal in a list position is rejected before evaluation
	// (validateListOperands); evaluated anyway, it is the one-element list
	// ['bad'], and 'bad' > 0 is null.
	for _, function := range []string{"all", "any", "none", "single"} {
		expression := function + "(x IN 'bad' WHERE x > 0)"
		fixtureAdmission628Context, fixtureAdmission628 := expressionTestContext(ctx, expression)
		_ = fixtureAdmission628
		if got := e.evaluateExpressionWithContext(fixtureAdmission628, fixtureAdmission628Context, expression, nodes, nil); got != nil {
			t.Fatalf("%s should be nil, got %#v", expression, got)
		}
	}
	fixtureAdmission629Context, fixtureAdmission629 := expressionTestContext(ctx, "filter(x IN 'bad' WHERE x > 0)")
	_ = fixtureAdmission629
	if got := e.evaluateExpressionWithContext(fixtureAdmission629, fixtureAdmission629Context, "filter(x IN 'bad' WHERE x > 0)", nodes, nil); !reflect.DeepEqual(got, []interface{}{}) {
		t.Fatalf("filter invalid input should return empty list, got %#v", got)
	}
	// extract is the list comprehension [x IN list | x], which reads a value
	// that isn't a list as a list of that one value, as UNWIND does.
	fixtureAdmission630Context, fixtureAdmission630 := expressionTestContext(ctx, "extract(x IN 'bad' | x)")
	_ = fixtureAdmission630
	if got := e.evaluateExpressionWithContext(fixtureAdmission630, fixtureAdmission630Context, "extract(x IN 'bad' | x)", nodes, nil); !reflect.DeepEqual(got, []interface{}{"bad"}) {
		t.Fatalf("extract of a single value should return it as a list, got %#v", got)
	}
}

func TestFunctionEval_APOCCollectionsConvertMetaAndMapBranches(t *testing.T) {
	e := setupTestExecutor(t)
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "fn-apoc",
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"pairs": []interface{}{[]interface{}{"k1", 1}, []interface{}{"k2", "v2"}},
			},
		},
	}
	ctx := context.Background()

	fixtureAdmission631Context, fixtureAdmission631 := expressionTestContext(ctx, "apoc.coll.pairs([1,2,3])")
	_ = fixtureAdmission631
	got := e.evaluateExpressionWithContext(fixtureAdmission631, fixtureAdmission631Context, "apoc.coll.pairs([1,2,3])", nodes, nil)
	pairs, ok := got.([]interface{})
	if !ok || len(pairs) != 3 {
		t.Fatalf("apoc.coll.pairs returned %#v", got)
	}

	fixtureAdmission632Context, fixtureAdmission632 := expressionTestContext(ctx, "apoc.coll.zip([1,2], ['a','b'])")
	_ = fixtureAdmission632
	got = e.evaluateExpressionWithContext(fixtureAdmission632, fixtureAdmission632Context, "apoc.coll.zip([1,2], ['a','b'])", nodes, nil)
	zipped, ok := got.([]interface{})
	if !ok || len(zipped) != 2 {
		t.Fatalf("apoc.coll.zip returned %#v", got)
	}

	fixtureAdmission633Context, fixtureAdmission633 := expressionTestContext(ctx, "apoc.coll.frequencies(['a','a','b'])")
	_ = fixtureAdmission633
	got = e.evaluateExpressionWithContext(fixtureAdmission633, fixtureAdmission633Context, "apoc.coll.frequencies(['a','a','b'])", nodes, nil)
	if !reflect.DeepEqual([]interface{}{
		map[string]interface{}{"item": "a", "count": int64(2)},
		map[string]interface{}{"item": "b", "count": int64(1)},
	}, got) {
		t.Fatalf("apoc.coll.frequencies returned %#v", got)
	}

	fixtureAdmission634Context, fixtureAdmission634 := expressionTestContext(ctx, "apoc.coll.occurrences([1,2,2,3],2)")
	_ = fixtureAdmission634
	if occ := e.evaluateExpressionWithContext(fixtureAdmission634, fixtureAdmission634Context, "apoc.coll.occurrences([1,2,2,3],2)", nodes, nil); occ != int64(2) {
		t.Fatalf("apoc.coll.occurrences returned %#v", occ)
	}
	fixtureAdmission635Context, fixtureAdmission635 := expressionTestContext(ctx, "apoc.coll.indexOf([1,2,3],2)")
	_ = fixtureAdmission635
	if idx := e.evaluateExpressionWithContext(fixtureAdmission635, fixtureAdmission635Context, "apoc.coll.indexOf([1,2,3],2)", nodes, nil); idx != int64(1) {
		t.Fatalf("apoc.coll.indexOf returned %#v", idx)
	}
	fixtureAdmission636Context, fixtureAdmission636 := expressionTestContext(ctx, "apoc.coll.split([1,2,3,2,4],2)")
	_ = fixtureAdmission636
	if split := e.evaluateExpressionWithContext(fixtureAdmission636, fixtureAdmission636Context, "apoc.coll.split([1,2,3,2,4],2)", nodes, nil); split == nil {
		t.Fatal("apoc.coll.split should return non-nil")
	}
	fixtureAdmission637Context, fixtureAdmission637 := expressionTestContext(ctx, "apoc.coll.partition([1,2,3,4],2)")
	_ = fixtureAdmission637
	if part := e.evaluateExpressionWithContext(fixtureAdmission637, fixtureAdmission637Context, "apoc.coll.partition([1,2,3,4],2)", nodes, nil); part == nil {
		t.Fatal("apoc.coll.partition should return non-nil")
	}

	fixtureAdmission638Context, fixtureAdmission638 := expressionTestContext(ctx, "apoc.convert.toJson({a: 1, b: 'x'})")
	_ = fixtureAdmission638
	got = e.evaluateExpressionWithContext(fixtureAdmission638, fixtureAdmission638Context, "apoc.convert.toJson({a: 1, b: 'x'})", nodes, nil)
	if s, ok := got.(string); !ok || !strings.Contains(s, "\"a\":1") {
		t.Fatalf("apoc.convert.toJson returned %#v", got)
	}

	fixtureAdmission639Context, fixtureAdmission639 := expressionTestContext(ctx, "apoc.convert.fromJsonMap(42)")
	_ = fixtureAdmission639
	if bad := e.evaluateExpressionWithContext(fixtureAdmission639, fixtureAdmission639Context, "apoc.convert.fromJsonMap(42)", nodes, nil); bad != nil {
		t.Fatalf("apoc.convert.fromJsonMap(non-string) should be nil, got %#v", bad)
	}
	fixtureAdmission640Context, fixtureAdmission640 := expressionTestContext(ctx, "apoc.convert.fromJsonList(42)")
	_ = fixtureAdmission640
	if bad := e.evaluateExpressionWithContext(fixtureAdmission640, fixtureAdmission640Context, "apoc.convert.fromJsonList(42)", nodes, nil); bad != nil {
		t.Fatalf("apoc.convert.fromJsonList(non-string) should be nil, got %#v", bad)
	}

	fixtureAdmission641Context, fixtureAdmission641 := expressionTestContext(ctx, "apoc.meta.type(42)")
	_ = fixtureAdmission641
	if typ := e.evaluateExpressionWithContext(fixtureAdmission641, fixtureAdmission641Context, "apoc.meta.type(42)", nodes, nil); typ != "INTEGER" {
		t.Fatalf("apoc.meta.type returned %#v", typ)
	}
	fixtureAdmission642Context, fixtureAdmission642 := expressionTestContext(ctx, "apoc.meta.isType(42,'INTEGER')")
	_ = fixtureAdmission642
	if isType := e.evaluateExpressionWithContext(fixtureAdmission642, fixtureAdmission642Context, "apoc.meta.isType(42,'INTEGER')", nodes, nil); isType != true {
		t.Fatalf("apoc.meta.isType should be true, got %#v", isType)
	}
	fixtureAdmission643Context, fixtureAdmission643 := expressionTestContext(ctx, "apoc.meta.isType(42,123)")
	_ = fixtureAdmission643
	if isType := e.evaluateExpressionWithContext(fixtureAdmission643, fixtureAdmission643Context, "apoc.meta.isType(42,123)", nodes, nil); isType != false {
		t.Fatalf("apoc.meta.isType(non-string type) should be false, got %#v", isType)
	}

	fixtureAdmission644Context, fixtureAdmission644 := expressionTestContext(ctx, "apoc.map.merge({a:1}, {b:2})")
	_ = fixtureAdmission644
	got = e.evaluateExpressionWithContext(fixtureAdmission644, fixtureAdmission644Context, "apoc.map.merge({a:1}, {b:2})", nodes, nil)
	merged, ok := got.(map[string]interface{})
	if !ok || merged["a"] != int64(1) || merged["b"] != int64(2) {
		t.Fatalf("apoc.map.merge returned %#v", got)
	}
	fixtureAdmission645Context, fixtureAdmission645 := expressionTestContext(ctx, "apoc.map.merge({a:1})")
	_ = fixtureAdmission645
	if got := e.evaluateExpressionWithContext(fixtureAdmission645, fixtureAdmission645Context, "apoc.map.merge({a:1})", nodes, nil); !reflect.DeepEqual(got, map[string]interface{}{}) {
		t.Fatalf("apoc.map.merge missing arg should be empty map, got %#v", got)
	}

	fixtureAdmission646Context, fixtureAdmission646 := expressionTestContext(ctx, "apoc.map.fromPairs(n.pairs)")
	_ = fixtureAdmission646
	got = e.evaluateExpressionWithContext(fixtureAdmission646, fixtureAdmission646Context, "apoc.map.fromPairs(n.pairs)", nodes, nil)
	fromPairsMap, ok := got.(map[string]interface{})
	if !ok || fmt.Sprint(fromPairsMap["k1"]) != "1" || fromPairsMap["k2"] != "v2" {
		t.Fatalf("apoc.map.fromPairs returned %#v", got)
	}
	fixtureAdmission647Context, fixtureAdmission647 := expressionTestContext(ctx, "apoc.map.fromLists(['x','y'], [10,20])")
	_ = fixtureAdmission647
	got = e.evaluateExpressionWithContext(fixtureAdmission647, fixtureAdmission647Context, "apoc.map.fromLists(['x','y'], [10,20])", nodes, nil)
	fromListsMap, ok := got.(map[string]interface{})
	if !ok || fmt.Sprint(fromListsMap["x"]) != "10" || fmt.Sprint(fromListsMap["y"]) != "20" {
		t.Fatalf("apoc.map.fromLists returned %#v", got)
	}
	fixtureAdmission648Context, fixtureAdmission648 := expressionTestContext(ctx, "apoc.map.fromLists(['x'])")
	_ = fixtureAdmission648
	if got := e.evaluateExpressionWithContext(fixtureAdmission648, fixtureAdmission648Context, "apoc.map.fromLists(['x'])", nodes, nil); got != nil {
		t.Fatalf("apoc.map.fromLists missing args should be nil, got %#v", got)
	}
}

func TestFunctionEval_SpatialGeometryAndKalmanAdditionalBranches(t *testing.T) {
	e := setupTestExecutor(t)
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "fn-geo",
			Labels: []string{"Geo"},
			Properties: map[string]interface{}{
				"polyPts": []interface{}{
					map[string]interface{}{"x": float64(0), "y": float64(0)},
					map[string]interface{}{"x": float64(2), "y": float64(0)},
					map[string]interface{}{"x": float64(0), "y": float64(2)},
				},
				"linePts": []interface{}{
					map[string]interface{}{"x": float64(0), "y": float64(0)},
					map[string]interface{}{"x": float64(1), "y": float64(1)},
				},
			},
		},
	}
	ctx := context.Background()
	fixtureAdmission649Context, fixtureAdmission649 := expressionTestContext(ctx, "point.distance(point({x:0,y:0}), point({x:3,y:4}))")
	_ = fixtureAdmission649
	if d := e.evaluateExpressionWithContext(fixtureAdmission649, fixtureAdmission649Context, "point.distance(point({x:0,y:0}), point({x:3,y:4}))", nodes, nil); d != float64(5) {
		t.Fatalf("point.distance cartesian returned %#v", d)
	}
	fixtureAdmission650Context, fixtureAdmission650 := expressionTestContext(ctx, "point.distance(point({latitude:0,longitude:0}), point({latitude:0,longitude:1}))")
	_ = fixtureAdmission650
	if d := e.evaluateExpressionWithContext(fixtureAdmission650, fixtureAdmission650Context, "point.distance(point({latitude:0,longitude:0}), point({latitude:0,longitude:1}))", nodes, nil); d == nil {
		t.Fatal("point.distance lat/lon should not be nil")
	}

	fixtureAdmission651Context, fixtureAdmission651 := expressionTestContext(ctx, "point.withinBBox(point({x:1,y:1}), point({x:0,y:0}), point({x:2,y:2}))")
	_ = fixtureAdmission651
	if ok := e.evaluateExpressionWithContext(fixtureAdmission651, fixtureAdmission651Context, "point.withinBBox(point({x:1,y:1}), point({x:0,y:0}), point({x:2,y:2}))", nodes, nil); ok != true {
		t.Fatalf("point.withinBBox cartesian returned %#v", ok)
	}
	fixtureAdmission652Context, fixtureAdmission652 := expressionTestContext(ctx, "point.withinBBox(point({latitude:1,longitude:1}), point({latitude:0,longitude:0}), point({latitude:2,longitude:2}))")
	_ = fixtureAdmission652
	if ok := e.evaluateExpressionWithContext(fixtureAdmission652, fixtureAdmission652Context, "point.withinBBox(point({latitude:1,longitude:1}), point({latitude:0,longitude:0}), point({latitude:2,longitude:2}))", nodes, nil); ok != true {
		t.Fatalf("point.withinBBox lat/lon returned %#v", ok)
	}
	fixtureAdmission653Context, fixtureAdmission653 := expressionTestContext(ctx, "point.withinBBox(1,2,3)")
	_ = fixtureAdmission653
	if ok := e.evaluateExpressionWithContext(fixtureAdmission653, fixtureAdmission653Context, "point.withinBBox(1,2,3)", nodes, nil); ok != nil {
		t.Fatalf("point.withinBBox with arguments that aren't points should be nil (Neo4j), got %#v", ok)
	}
	fixtureAdmission654Context, fixtureAdmission654 := expressionTestContext(ctx, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}), 2)")
	_ = fixtureAdmission654
	if ok := e.evaluateExpressionWithContext(fixtureAdmission654, fixtureAdmission654Context, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}), 2)", nodes, nil); ok != true {
		t.Fatalf("point.withinDistance returned %#v", ok)
	}
	fixtureAdmission655Context, fixtureAdmission655 := expressionTestContext(ctx, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}))")
	_ = fixtureAdmission655
	if ok := e.evaluateExpressionWithContext(fixtureAdmission655, fixtureAdmission655Context, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}))", nodes, nil); ok != false {
		t.Fatalf("point.withinDistance missing arg should be false, got %#v", ok)
	}

	fixtureAdmission656Context, fixtureAdmission656 := expressionTestContext(ctx, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])")
	_ = fixtureAdmission656
	poly := e.evaluateExpressionWithContext(fixtureAdmission656, fixtureAdmission656Context, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])", nodes, nil)
	if poly == nil {
		t.Fatal("polygon literal should not be nil")
	}
	fixtureAdmission657Context, fixtureAdmission657 := expressionTestContext(ctx, "polygon([point({x:0,y:0}), point({x:1,y:0})])")
	_ = fixtureAdmission657
	if badPoly := e.evaluateExpressionWithContext(fixtureAdmission657, fixtureAdmission657Context, "polygon([point({x:0,y:0}), point({x:1,y:0})])", nodes, nil); badPoly != nil {
		t.Fatalf("polygon with <3 points should be nil, got %#v", badPoly)
	}
	fixtureAdmission658Context, fixtureAdmission658 := expressionTestContext(ctx, "polygon(n.polyPts)")
	_ = fixtureAdmission658
	if polyVar := e.evaluateExpressionWithContext(fixtureAdmission658, fixtureAdmission658Context, "polygon(n.polyPts)", nodes, nil); polyVar == nil {
		t.Fatal("polygon(variable points) should not be nil")
	}

	fixtureAdmission659Context, fixtureAdmission659 := expressionTestContext(ctx, "lineString([point({x:0,y:0}), point({x:1,y:1})])")
	_ = fixtureAdmission659
	if line := e.evaluateExpressionWithContext(fixtureAdmission659, fixtureAdmission659Context, "lineString([point({x:0,y:0}), point({x:1,y:1})])", nodes, nil); line == nil {
		t.Fatal("lineString literal should not be nil")
	}
	fixtureAdmission660Context, fixtureAdmission660 := expressionTestContext(ctx, "lineString(n.linePts)")
	_ = fixtureAdmission660
	if lineVar := e.evaluateExpressionWithContext(fixtureAdmission660, fixtureAdmission660Context, "lineString(n.linePts)", nodes, nil); lineVar == nil {
		t.Fatal("lineString(variable points) should not be nil")
	}
	fixtureAdmission661Context, fixtureAdmission661 := expressionTestContext(ctx, "lineString([point({x:0,y:0})])")
	_ = fixtureAdmission661
	if badLine := e.evaluateExpressionWithContext(fixtureAdmission661, fixtureAdmission661Context, "lineString([point({x:0,y:0})])", nodes, nil); badLine != nil {
		t.Fatalf("lineString with <2 points should be nil, got %#v", badLine)
	}

	fixtureAdmission662Context, fixtureAdmission662 := expressionTestContext(ctx, "point.intersects(point({x:0.5,y:0.5}), polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})]))")
	_ = fixtureAdmission662
	if hit := e.evaluateExpressionWithContext(fixtureAdmission662, fixtureAdmission662Context, "point.intersects(point({x:0.5,y:0.5}), polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})]))", nodes, nil); hit != true {
		t.Fatalf("point.intersects should be true, got %#v", hit)
	}

	fixtureAdmission663Context, fixtureAdmission663 := expressionTestContext(ctx, "kalman.velocity.init(10, 2)")
	_ = fixtureAdmission663
	state := e.evaluateExpressionWithContext(fixtureAdmission663, fixtureAdmission663Context, "kalman.velocity.init(10, 2)", nodes, nil)
	stateJSON, ok := state.(string)
	if !ok || stateJSON == "" {
		t.Fatalf("kalman.velocity.init returned %#v", state)
	}
	fixtureAdmission664Context, fixtureAdmission664 := expressionTestContext(ctx, "kalman.velocity.process(12, '"+stateJSON+"')")
	_ = fixtureAdmission664
	if p := e.evaluateExpressionWithContext(fixtureAdmission664, fixtureAdmission664Context, "kalman.velocity.process(12, '"+stateJSON+"')", nodes, nil); p == nil {
		t.Fatal("kalman.velocity.process should not be nil")
	}
	fixtureAdmission665Context, fixtureAdmission665 := expressionTestContext(ctx, "kalman.velocity.predict('"+stateJSON+"', 3)")
	_ = fixtureAdmission665
	if p := e.evaluateExpressionWithContext(fixtureAdmission665, fixtureAdmission665Context, "kalman.velocity.predict('"+stateJSON+"', 3)", nodes, nil); p == nil {
		t.Fatal("kalman.velocity.predict should not be nil")
	}
}
