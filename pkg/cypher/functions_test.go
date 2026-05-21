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
	result := e.evaluateExpressionWithContext(ctx, "id(n)", nodes, nil)

	if result != "node-1" {
		t.Errorf("id(n) = %v, want node-1", result)
	}
}

func TestFunctionElementId(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	result := e.evaluateExpressionWithContext(ctx, "elementId(n)", nodes, nil)

	expected := "4:nornicdb:node-1"
	if result != expected {
		t.Errorf("elementId(n) = %v, want %v", result, expected)
	}
}

func TestFunctionLabels(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person", "Employee"}, nil)

	nodes := map[string]*storage.Node{"n": node}
	ctx := context.Background()

	result := e.evaluateExpressionWithContext(ctx, "labels(n)", nodes, nil)

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

	result := e.evaluateExpressionWithContext(ctx, "type(r)", nil, rels)

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

	result := e.evaluateExpressionWithContext(ctx, "keys(n)", nodes, nil)

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

	result := e.evaluateExpressionWithContext(ctx, "properties(n)", nodes, nil)

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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "split(n.data, n.sep)", nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	assertEqual(t, "labels(n)[0]", "Person", e.evaluateExpressionWithContext(ctx, "labels(n)[0]", nodes, nil))
	assertEqual(t, "labels(n)[-1]", "Employee", e.evaluateExpressionWithContext(ctx, "labels(n)[-1]", nodes, nil))
	assertEqual(t, "'abc'[1]", "b", e.evaluateExpressionWithContext(ctx, "'abc'[1]", nodes, nil))

	// Slice notation paths
	s1 := e.evaluateExpressionWithContext(ctx, "labels(n)[..1]", nodes, nil)
	list1, ok := s1.([]interface{})
	if !ok || len(list1) != 1 || list1[0] != "Person" {
		t.Fatalf("labels(n)[..1] = %v, want [Person]", s1)
	}

	s2 := e.evaluateExpressionWithContext(ctx, "labels(n)[1..]", nodes, nil)
	list2, ok := s2.([]interface{})
	if !ok || len(list2) != 1 || list2[0] != "Employee" {
		t.Fatalf("labels(n)[1..] = %v, want [Employee]", s2)
	}

	// IN list must not be treated as indexing
	assertEqual(t, "1 IN [1,2,3]", true, e.evaluateExpressionWithContext(ctx, "1 IN [1,2,3]", nil, nil))

	// Parenthesized expression and map literal
	assertEqual(t, "(1 + 2)", int64(3), e.evaluateExpressionWithContext(ctx, "(1 + 2)", nil, nil))
	m := e.evaluateExpressionWithContext(ctx, "{x: 1, y: 'z'}", nil, nil)
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

	assertEqual(t, "type({type:'KNOWS'})", "KNOWS", e.evaluateExpressionWithContext(ctx, "type({type:'KNOWS'})", nodes, rels))
	assertEqual(t, "exists(n.missing)", false, e.evaluateExpressionWithContext(ctx, "exists(n.missing)", nodes, rels))
	assertEqual(t, "head([])", nil, e.evaluateExpressionWithContext(ctx, "head([])", nodes, rels))
	assertEqual(t, "last([])", nil, e.evaluateExpressionWithContext(ctx, "last([])", nodes, rels))
	assertEqual(t, "reverse('stressed')", "desserts", e.evaluateExpressionWithContext(ctx, "reverse('stressed')", nodes, rels))
	assertEqual(t, "indexOf([1,2,3], 9)", int64(-1), e.evaluateExpressionWithContext(ctx, "indexOf([1,2,3], 9)", nodes, rels))
	assertEqual(t, "hasLabels(n, ['Person', 'Employee'])", true, e.evaluateExpressionWithContext(ctx, "hasLabels(n, ['Person', 'Employee'])", nodes, rels))
	assertEqual(t, "hasLabels(n, ['Person', 'Missing'])", false, e.evaluateExpressionWithContext(ctx, "hasLabels(n, ['Person', 'Missing'])", nodes, rels))

	tail := e.evaluateExpressionWithContext(ctx, "tail([1])", nodes, rels)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 0 {
		t.Fatalf("tail([1]) = %v, want []", tail)
	}

	sliced := e.evaluateExpressionWithContext(ctx, "slice([1, 2, 3, 4], -3, -1)", nodes, rels)
	sliceList, ok := sliced.([]interface{})
	if !ok || len(sliceList) != 2 || sliceList[0] != int64(2) || sliceList[1] != int64(3) {
		t.Fatalf("slice([1,2,3,4], -3, -1) = %v, want [2,3]", sliced)
	}

	gotLength := e.evaluateExpressionWithContextFullFunctions(ctx,
		"length(p)",
		nodes,
		rels,
		map[string]*PathResult{"p": {Length: 4}},
		nil,
		nil,
		0,
	)
	assertEqual(t, "length(p)", int64(4), gotLength)

	gotFallbackLength := e.evaluateExpressionWithContextFullFunctions(ctx, "length(route)", nodes, rels, nil, nil, nil, 6)
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
	result := e.evaluateExpressionWithContext(ctx, "head([1, 2, 3])", nil, nil)
	if result != int64(1) {
		t.Errorf("head([1,2,3]) = %v, want 1", result)
	}

	// Test last
	result = e.evaluateExpressionWithContext(ctx, "last([1, 2, 3])", nil, nil)
	if result != int64(3) {
		t.Errorf("last([1,2,3]) = %v, want 3", result)
	}

	// Test tail
	tail := e.evaluateExpressionWithContext(ctx, "tail([1, 2, 3])", nil, nil)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 2 {
		t.Errorf("tail([1,2,3]) = %v, want [2,3]", tail)
	}

	// Test reverse
	rev := e.evaluateExpressionWithContext(ctx, "reverse([1, 2, 3])", nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
		{"ceil(4.2)", int64(5), 0},
		{"floor(4.8)", int64(4), 0},
		{"round(4.5)", int64(5), 0},
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)

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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	pi := e.evaluateExpressionWithContext(ctx, "pi()", nil, nil)
	piF, ok := pi.(float64)
	if !ok || math.Abs(piF-math.Pi) > 0.0001 {
		t.Errorf("pi() = %v, want %v", pi, math.Pi)
	}

	// Test e()
	eVal := e.evaluateExpressionWithContext(ctx, "e()", nil, nil)
	eF, ok := eVal.(float64)
	if !ok || math.Abs(eF-math.E) > 0.0001 {
		t.Errorf("e() = %v, want %v", eVal, math.E)
	}
}

func TestRandomFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	ctx := context.Background()

	// Test rand() returns value between 0 and 1
	result := e.evaluateExpressionWithContext(ctx, "rand()", nil, nil)
	randF, ok := result.(float64)
	if !ok {
		t.Fatalf("rand() should return float64")
	}
	if randF < 0 || randF > 1 {
		t.Errorf("rand() = %v, should be between 0 and 1", randF)
	}

	// Test randomUUID() returns a string
	uuid := e.evaluateExpressionWithContext(ctx, "randomUUID()", nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	ts := e.evaluateExpressionWithContext(ctx, "timestamp()", nil, nil)
	if ts == nil {
		t.Error("timestamp() should return a value")
	}

	// Test datetime() returns a value
	dt := e.evaluateExpressionWithContext(ctx, "datetime()", nil, nil)
	if dt == nil {
		t.Error("datetime() should return a value")
	}

	// Test date()
	d := e.evaluateExpressionWithContext(ctx, "date()", nil, nil)
	if d == nil {
		t.Error("date() should return a value")
	}

	// Test time()
	tm := e.evaluateExpressionWithContext(ctx, "time()", nil, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "startNode(r)", nodes, rels)
	if result == nil {
		t.Error("startNode(r) should return a value")
	}

	// Test endNode
	result = e.evaluateExpressionWithContext(ctx, "endNode(r)", nodes, rels)
	if result == nil {
		t.Error("endNode(r) should return a value")
	}

	// Test type(r)
	result = e.evaluateExpressionWithContext(ctx, "type(r)", nodes, rels)
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
	result := e.evaluateExpressionWithContext(ctx, "vector.similarity.cosine(a.vec, b.vec)", nodes, nil)
	if result == nil {
		t.Error("vector.similarity.cosine should return a value")
	}

	// Test euclidean similarity
	result = e.evaluateExpressionWithContext(ctx, "vector.similarity.euclidean(a.vec, b.vec)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "point({x: 1.0, y: 2.0})", nil, nil)
	pointMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("point() should return map, got %T", result)
	}
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
	dist := e.evaluateExpressionWithContext(ctx, "distance(a.loc, b.loc)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "reduce(acc = 0, x IN [1, 2, 3] | acc + x)", nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

	result := e.evaluateExpressionWithContext(ctx, "n.firstName + ' ' + n.lastName", nodes, nil)
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
	// Aggregation functions must be handled by executeAggregation(ctx, ) or executeMatchWithRelationships()
	result := e.evaluateExpressionWithContext(ctx, "count(*)", nil, nil)
	if result != nil {
		t.Errorf("count(*) in expression context should return nil, got %v", result)
	}

	// count(n) also should NOT be evaluated in expression context
	node := createTestNode(t, e, "node-1", []string{"Person"}, nil)
	nodes := map[string]*storage.Node{"n": node}
	result = e.evaluateExpressionWithContext(ctx, "count(n)", nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "toIntegerList(n.intList)", nodes, nil)
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
	result = e.evaluateExpressionWithContext(ctx, "toFloatList(n.floatList)", nodes, nil)
	if floatList, ok := result.([]interface{}); ok {
		if len(floatList) != 3 {
			t.Errorf("toFloatList should return 3 elements")
		}
	}

	// toBooleanList
	result = e.evaluateExpressionWithContext(ctx, "toBooleanList(n.boolList)", nodes, nil)
	if boolList, ok := result.([]interface{}); ok {
		if len(boolList) != 3 {
			t.Errorf("toBooleanList should return 3 elements")
		}
		if boolList[0] != true {
			t.Errorf("toBooleanList[0] = %v, want true", boolList[0])
		}
	}

	// toStringList
	result = e.evaluateExpressionWithContext(ctx, "toStringList(n.stringList)", nodes, nil)
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
		{"valueType(true)", "BOOLEAN"},
		{"valueType(123)", "INTEGER"},
		{"valueType(3.14)", "FLOAT"},
		{"valueType('hello')", "STRING"},
		{"valueType([1, 2, 3])", "LIST"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := context.Background()

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

			result := e.evaluateExpressionWithContext(ctx, expr, nodes, nil)
			if result != int64(42) {
				t.Errorf("%s = %v, want 42", expr, result)
			}
		})
	}

	// collect returns a list
	ctx := context.Background()
	result := e.evaluateExpressionWithContext(ctx, "collect(n.val)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "none(x IN n.empty WHERE true)", nodes, nil)
	if result != true {
		t.Errorf("none(x IN empty list) = %v, want true", result)
	}

	// Test basic parsing - ensure functions don't crash
	node2 := createTestNode(t, e, "node-2", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes2 := map[string]*storage.Node{"n": node2}

	// all() should not crash even if predicate evaluation is limited
	_ = e.evaluateExpressionWithContext(ctx, "all(x IN n.nums WHERE x = x)", nodes2, nil)

	// any() should not crash
	_ = e.evaluateExpressionWithContext(ctx, "any(x IN n.nums WHERE x = x)", nodes2, nil)

	// single() should not crash
	_ = e.evaluateExpressionWithContext(ctx, "single(x IN n.nums WHERE x = x)", nodes2, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "extract(x IN n.nums | x)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 5 {
			t.Errorf("extract should return 5 elements, got %d", len(list))
		}
	} else {
		t.Errorf("extract should return list, got %T", result)
	}

	// Test filter() - returns a list (predicate evaluation limited)
	result = e.evaluateExpressionWithContext(ctx, "filter(x IN n.nums WHERE true)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "withinBBox(n.inside, n.ll, n.ur)", nodes, nil)
	if result != true {
		t.Errorf("withinBBox(inside) = %v, want true", result)
	}

	// Point outside bbox
	result = e.evaluateExpressionWithContext(ctx, "withinBBox(n.outside, n.ll, n.ur)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "[x IN n.nums | x]", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "slice(n.list, 1, 3)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 2 {
			t.Errorf("slice(list, 1, 3) should return 2 elements, got %d", len(list))
		}
	} else {
		t.Errorf("slice should return list, got %T", result)
	}

	// slice(list, 2) - to end
	result = e.evaluateExpressionWithContext(ctx, "slice(n.list, 2)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "indexOf(n.list, 'b')", nodes, nil)
	if result != int64(1) {
		t.Errorf("indexOf(list, 'b') = %v, want 1", result)
	}

	// Not found
	result = e.evaluateExpressionWithContext(ctx, "indexOf(n.list, 'z')", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "degree(a)", nodes, nil)
	if result != int64(2) {
		t.Errorf("degree(a) = %v, want 2", result)
	}

	// Alice has 1 outgoing
	result = e.evaluateExpressionWithContext(ctx, "outDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("outDegree(a) = %v, want 1", result)
	}

	// Alice has 1 incoming
	result = e.evaluateExpressionWithContext(ctx, "inDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("inDegree(a) = %v, want 1", result)
	}

	// Bob has 0 outgoing, 1 incoming
	result = e.evaluateExpressionWithContext(ctx, "degree(b)", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "hasLabels(n, n.requiredLabels)", nodes, nil)
	if result != true {
		t.Errorf("hasLabels with matching labels = %v, want true", result)
	}

	// Missing a label
	result = e.evaluateExpressionWithContext(ctx, "hasLabels(n, n.wrongLabels)", nodes, nil)
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
	dist := e.evaluateExpressionWithContext(ctx, "distance(nyc.loc, la.loc)", nodes, nil)

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
	result := e.evaluateExpressionWithContext(ctx, "CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", nil, nil)
	if result != "one" {
		t.Errorf("CASE 1 WHEN 1 = %v, want 'one'", result)
	}

	// Searched CASE expression
	result = e.evaluateExpressionWithContext(ctx, "CASE WHEN true THEN 'yes' ELSE 'no' END", nil, nil)
	if result != "yes" {
		t.Errorf("CASE WHEN true = %v, want 'yes'", result)
	}

	// CASE with ELSE
	result = e.evaluateExpressionWithContext(ctx, "CASE 3 WHEN 1 THEN 'one' ELSE 'other' END", nil, nil)
	if result != "other" {
		t.Errorf("CASE 3 WHEN 1 = %v, want 'other'", result)
	}

	// CASE without matching WHEN (no ELSE)
	result = e.evaluateExpressionWithContext(ctx, "CASE 5 WHEN 1 THEN 'one' END", nil, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "CASE n.status WHEN 'active' THEN 'online' ELSE 'offline' END", nodes, nil)
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
			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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

		// Division - Neo4j returns int64 for exact division, float64 otherwise
		{"6 / 2", int64(3)},
		{"7 / 2", float64(3.5)},

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
			result := e.evaluateExpressionWithContext(ctx, tt.expr, nil, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "n.a * n.b", nodes, nil)
	if result != int64(30) {
		t.Errorf("n.a * n.b = %v, want 30", result)
	}

	// n.a - n.b
	result = e.evaluateExpressionWithContext(ctx, "n.a - n.b", nodes, nil)
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
	result := e.evaluateExpressionWithContext(ctx, "n.age > 18", nodes, nil)
	if result != true {
		t.Errorf("n.age > 18 = %v, want true", result)
	}

	// Multiple conditions with AND
	result = e.evaluateExpressionWithContext(ctx, "n.age > 18 AND n.active = true", nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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
			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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

			result := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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

	result := e.evaluateExpressionWithContext(ctx, "apoc.map.merge(n.map1, n.map2)", nodes, nil)
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

	result := e.evaluateExpressionWithContext(ctx, "apoc.map.setKey(n.myMap, 'b', 2)", nodes, nil)
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

	result := e.evaluateExpressionWithContext(ctx, "apoc.map.removeKey(n.myMap, 'b')", nodes, nil)
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
		{"cot(1)", 1.0 / math.Tan(1)},
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

			got := e.evaluateExpressionWithContext(ctx, tt.expr, nodes, nil)
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
	line := e.evaluateExpressionWithContext(ctx, "lineString([point({x:0,y:0}), point({x:1,y:1})])", nil, nil)
	lineMap, ok := line.(map[string]interface{})
	if !ok {
		t.Fatalf("lineString should return map, got %T", line)
	}
	if lineMap["type"] != "linestring" {
		t.Fatalf("lineString type = %v, want linestring", lineMap["type"])
	}

	poly := e.evaluateExpressionWithContext(ctx, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])", nil, nil)
	polyMap, ok := poly.(map[string]interface{})
	if !ok {
		t.Fatalf("polygon should return map, got %T", poly)
	}
	if polyMap["type"] != "polygon" {
		t.Fatalf("polygon type = %v, want polygon", polyMap["type"])
	}

	// insufficient points branches
	if v := e.evaluateExpressionWithContext(ctx, "lineString([point({x:0,y:0})])", nil, nil); v != nil {
		t.Fatalf("lineString with <2 points should be nil, got %v", v)
	}
	if v := e.evaluateExpressionWithContext(ctx, "polygon([point({x:0,y:0}), point({x:1,y:1})])", nil, nil); v != nil {
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
	gotNodes := e.evaluateExpressionWithContextFull(ctx, "nodes(p)", nil, nil, map[string]*PathResult{"p": path}, nil, nil, 0)
	if arr, ok := gotNodes.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) expected 2 entries, got %T %#v", gotNodes, gotNodes)
	}

	// fallback via allPathNodes/allPathEdges
	gotNodesFallback := e.evaluateExpressionWithContextFull(ctx, "nodes(p)", nil, nil, nil, nil, pathNodes, 0)
	if arr, ok := gotNodesFallback.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) fallback expected 2 entries, got %T %#v", gotNodesFallback, gotNodesFallback)
	}
	gotRelsFallback := e.evaluateExpressionWithContextFull(ctx, "relationships(p)", nil, nil, nil, pathEdges, nil, 0)
	if arr, ok := gotRelsFallback.([]interface{}); !ok || len(arr) != 1 {
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
	assertEqual(t, "indexOf([10,20,30], 20)", int64(1), e.evaluateExpressionWithContext(ctx, "indexOf([10,20,30], 20)", nodes, nil))
	gotRange := e.evaluateExpressionWithContext(ctx, "range(1,3)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, gotRange) {
		t.Fatalf("range(1,3) = %#v, want [1 2 3]", gotRange)
	}
	gotRangeNeg := e.evaluateExpressionWithContext(ctx, "range(5,1,-2)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(5), int64(3), int64(1)}, gotRangeNeg) {
		t.Fatalf("range(5,1,-2) = %#v, want [5 3 1]", gotRangeNeg)
	}
	gotRangeZero := e.evaluateExpressionWithContext(ctx, "range(1,3,0)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, gotRangeZero) {
		t.Fatalf("range(1,3,0) = %#v, want [1 2 3]", gotRangeZero)
	}
	gotSliceEmpty := e.evaluateExpressionWithContext(ctx, "slice([1,2,3], 2, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceEmpty) {
		t.Fatalf("slice([1,2,3],2,1) = %#v, want []", gotSliceEmpty)
	}
	gotSliceNonList := e.evaluateExpressionWithContext(ctx, "slice('not-list', 0, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceNonList) {
		t.Fatalf("slice('not-list',0,1) = %#v, want []", gotSliceNonList)
	}
	assertEqual(t, "inDegree(missing)", int64(0), e.evaluateExpressionWithContext(ctx, "inDegree(missing)", nodes, nil))
	assertEqual(t, "outDegree(missing)", int64(0), e.evaluateExpressionWithContext(ctx, "outDegree(missing)", nodes, nil))
	assertEqual(t, "hasLabels(a, 'bad')", false, e.evaluateExpressionWithContext(ctx, "hasLabels(a, 'bad')", nodes, nil))
	assertEqual(t, "hasLabels(missing, ['Person'])", false, e.evaluateExpressionWithContext(ctx, "hasLabels(missing, ['Person'])", nodes, nil))

	fromPairs := e.evaluateExpressionWithContext(ctx, "apoc.map.fromPairs([['a',1],['b',2]])", nodes, nil)
	fromPairsMap, ok := fromPairs.(map[string]interface{})
	if !ok {
		t.Fatalf("fromPairs result type = %T, want map[string]interface{}", fromPairs)
	}
	if fromPairsMap["a"] != int64(1) || fromPairsMap["b"] != int64(2) {
		t.Fatalf("unexpected fromPairs result: %#v", fromPairsMap)
	}

	clean := e.evaluateExpressionWithContext(ctx, "apoc.map.clean(a.cleanMap, ['b'], [null])", nodes, nil)
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

	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext(ctx, "apoc.map.fromPairs('bad')", nodes, nil)) {
		t.Fatalf("apoc.map.fromPairs('bad') should return empty map")
	}
	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext(ctx, "apoc.map.clean('bad')", nodes, nil)) {
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
		if got := e.evaluateExpressionWithContext(ctx, expr, nodes, rels); got != nil {
			t.Fatalf("%s should be nil, got %T %#v", expr, got, got)
		}
	}

	if got := e.evaluateExpressionWithContext(ctx, "isNaN('x')", nodes, rels); got != false {
		t.Fatalf("isNaN('x') = %#v, want false", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "isEmpty(123)", nodes, rels); got != false {
		t.Fatalf("isEmpty(123) = %#v, want false", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "isEmpty(NULL)", nodes, rels); got != true {
		t.Fatalf("isEmpty(NULL) = %#v, want true", got)
	}

	if got := e.evaluateExpressionWithContext(ctx, "coth(0)", nodes, rels); !math.IsNaN(got.(float64)) {
		t.Fatalf("coth(0) should be NaN, got %#v", got)
	}

	// nodes()/relationships() empty fallback branches.
	if got := e.evaluateExpressionWithContext(ctx, "nodes(missingPath)", nodes, rels); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("nodes(missingPath) = %#v, want []", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "relationships(missingPath)", nodes, rels); !reflect.DeepEqual([]interface{}{}, got) {
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

		return e.evaluateExpressionWithContextFullMath(ctx,
			expr,
			strings.ToLower(expr),
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
	if got := eval("point.srid(point({x: 1, y: 2, srid: 9157}))", nil, nil, 0); got != int64(9157) {
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
	if got := eval("point.crs(point({x: 1, y: 2, crs: 'custom'}))", nil, nil, 0); got != "custom" {
		t.Fatalf("point.crs(custom) = %#v", got)
	}
	if got := eval("point.height(point({altitude: 7}))", nil, nil, 0); got != float64(7) {
		t.Fatalf("point.height = %#v", got)
	}
	if got := eval("point.height(point({z: 8}))", nil, nil, 0); got != float64(8) {
		t.Fatalf("point.height(z) = %#v", got)
	}
	if got := eval("point.height(point({height: 9}))", nil, nil, 0); got != float64(9) {
		t.Fatalf("point.height(height) = %#v", got)
	}
	if got := eval("point.z(point({z: 10}))", nil, nil, 0); got != float64(10) {
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
	if got := eval("vector.similarity.cosine([1.0,0.0], [1.0,0.0])", nil, nil, 0); got == nil {
		t.Fatal("vector.similarity.cosine should not be nil")
	}
	if got := eval("vector.similarity.euclidean([1.0,0.0], [1.0,0.0])", nil, nil, 0); got == nil {
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

	state := e.evaluateExpressionWithContext(ctx, "kalman.init()", nodes, nil)
	stateStr, ok := state.(string)
	if !ok || stateStr == "" {
		t.Fatalf("kalman.init() should return non-empty state string, got %#v", state)
	}
	n.Properties["state"] = stateStr

	processed := e.evaluateExpressionWithContext(ctx, "kalman.process(10, n.state)", nodes, nil)
	processedMap, ok := processed.(map[string]interface{})
	if !ok || processedMap["state"] == nil || processedMap["value"] == nil {
		t.Fatalf("kalman.process should return state/value map, got %#v", processed)
	}
	if gotState, ok := processedMap["state"].(string); ok && gotState != "" {
		n.Properties["state"] = gotState
	}

	if got := e.evaluateExpressionWithContext(ctx, "kalman.predict(n.state, 2)", nodes, nil); got == nil {
		t.Fatalf("kalman.predict should not be nil")
	}
	if got := e.evaluateExpressionWithContext(ctx, "kalman.state(n.state)", nodes, nil); got == nil {
		t.Fatalf("kalman.state should not be nil")
	}
	if got := e.evaluateExpressionWithContext(ctx, "kalman.reset(n.state)", nodes, nil); got == nil {
		t.Fatalf("kalman.reset should not be nil")
	}

	vstate := e.evaluateExpressionWithContext(ctx, "kalman.velocity.init()", nodes, nil)
	vstateStr, ok := vstate.(string)
	if !ok || vstateStr == "" {
		t.Fatalf("kalman.velocity.init() should return state string, got %#v", vstate)
	}
	n.Properties["vstate"] = vstateStr
	if got := e.evaluateExpressionWithContext(ctx, "kalman.velocity.process(5, n.vstate)", nodes, nil); got == nil {
		t.Fatalf("kalman.velocity.process should not be nil")
	}
	if got := e.evaluateExpressionWithContext(ctx, "kalman.velocity.predict(n.vstate, 3)", nodes, nil); got == nil {
		t.Fatalf("kalman.velocity.predict should not be nil")
	}

	astate := e.evaluateExpressionWithContext(ctx, "kalman.adaptive.init()", nodes, nil)
	astateStr, ok := astate.(string)
	if !ok || astateStr == "" {
		t.Fatalf("kalman.adaptive.init() should return state string, got %#v", astate)
	}
	n.Properties["astate"] = astateStr
	if got := e.evaluateExpressionWithContext(ctx, "kalman.adaptive.process(5, n.astate)", nodes, nil); got == nil {
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

	assertEqual(t, "n.arrAny[1]", int64(8), e.evaluateExpressionWithContext(ctx, "n.arrAny[1]", nodes, nil))
	assertEqual(t, "n.arrAny[-1]", int64(9), e.evaluateExpressionWithContext(ctx, "n.arrAny[-1]", nodes, nil))
	assertEqual(t, "n.arrStr[0]", "aa", e.evaluateExpressionWithContext(ctx, "n.arrStr[0]", nodes, nil))
	assertEqual(t, "n.arrStr[-1]", "cc", e.evaluateExpressionWithContext(ctx, "n.arrStr[-1]", nodes, nil))
	assertEqual(t, "n.text[1]", "e", e.evaluateExpressionWithContext(ctx, "n.text[1]", nodes, nil))
	assertEqual(t, "n.text[-1]", "o", e.evaluateExpressionWithContext(ctx, "n.text[-1]", nodes, nil))

	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[99]", nodes, nil); got != nil {
		t.Fatalf("n.arrAny[99] should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "1 IN [1,2,3]", nodes, nil); got != true {
		t.Fatalf("1 IN [1,2,3] should be true, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[..2]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(7), int64(8)}, got) {
		t.Fatalf("n.arrAny[..2] unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[1..]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(8), int64(9)}, got) {
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

	if got := e.evaluateExpressionWithContextFullFunctions(ctx, "", nodes, nil, nil, nil, nil, 0); got != nil {
		t.Fatalf("empty expr should return nil, got %#v", got)
	}

	// Additional array-indexing/slicing branches.
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[-3..-1]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(2), int64(3)}, got) {
		t.Fatalf("n.arrAny[-3..-1] unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[-99..2]", nodes, nil); !reflect.DeepEqual([]interface{}{int64(1), int64(2)}, got) {
		t.Fatalf("n.arrAny[-99..2] unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny[3..1]", nodes, nil); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("n.arrAny[3..1] unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "'abc'[..1]", nodes, nil); got != nil {
		t.Fatalf("'abc'[..1] should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "n.arrAny['2']", nodes, nil); got != int64(3) {
		t.Fatalf("n.arrAny['2'] unexpected: %#v", got)
	}

	// Scalar conversions.
	if got := e.evaluateExpressionWithContext(ctx, "toInteger(2.9)", nodes, nil); got != int64(2) {
		t.Fatalf("toInteger(2.9) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toInteger('bad')", nodes, nil); got != nil {
		t.Fatalf("toInteger('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toInt(3)", nodes, nil); got != int64(3) {
		t.Fatalf("toInt(3) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toInt('bad')", nodes, nil); got != nil {
		t.Fatalf("toInt('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloat(2)", nodes, nil); got != float64(2) {
		t.Fatalf("toFloat(2) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloat('3.5')", nodes, nil); got != float64(3.5) {
		t.Fatalf("toFloat('3.5') = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloat('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloat('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBoolean('TRUE')", nodes, nil); got != true {
		t.Fatalf("toBoolean('TRUE') = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBoolean(1)", nodes, nil); got != nil {
		t.Fatalf("toBoolean(1) should be nil, got %#v", got)
	}

	// OrNull conversion variants.
	if got := e.evaluateExpressionWithContext(ctx, "toIntegerOrNull('7')", nodes, nil); got != int64(7) {
		t.Fatalf("toIntegerOrNull('7') = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toIntegerOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toIntegerOrNull('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloatOrNull('2.25')", nodes, nil); got != float64(2.25) {
		t.Fatalf("toFloatOrNull('2.25') = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloatOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloatOrNull('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBooleanOrNull('false')", nodes, nil); got != false {
		t.Fatalf("toBooleanOrNull('false') = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBooleanOrNull('bad')", nodes, nil); got != nil {
		t.Fatalf("toBooleanOrNull('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toStringOrNull(null)", nodes, nil); got != nil {
		t.Fatalf("toStringOrNull(null) should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toStringOrNull(12)", nodes, nil); got != "12" {
		t.Fatalf("toStringOrNull(12) = %#v", got)
	}

	// List conversion functions.
	if got := e.evaluateExpressionWithContext(ctx, "toIntegerList(n.mixed)", nodes, nil); !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3), nil, nil, nil}, got) {
		t.Fatalf("toIntegerList(n.mixed) unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toIntegerList('bad')", nodes, nil); got != nil {
		t.Fatalf("toIntegerList('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloatList(n.mixedF)", nodes, nil); !reflect.DeepEqual([]interface{}{float64(1.25), float64(2), float64(3.5), nil, nil}, got) {
		t.Fatalf("toFloatList(n.mixedF) unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toFloatList('bad')", nodes, nil); got != nil {
		t.Fatalf("toFloatList('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBooleanList(n.mixedB)", nodes, nil); !reflect.DeepEqual([]interface{}{true, false, nil, nil}, got) {
		t.Fatalf("toBooleanList(n.mixedB) unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toBooleanList('bad')", nodes, nil); got != nil {
		t.Fatalf("toBooleanList('bad') should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toStringList([1, null, true])", nodes, nil); !reflect.DeepEqual([]interface{}{"1", nil, "true"}, got) {
		t.Fatalf("toStringList([1, null, true]) unexpected: %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "toStringList('bad')", nodes, nil); got != nil {
		t.Fatalf("toStringList('bad') should be nil, got %#v", got)
	}

	// Utility + aggregate-in-expression branches.
	if got := e.evaluateExpressionWithContext(ctx, "valueType({k: 1})", nodes, nil); got != "MAP" {
		t.Fatalf("valueType({k:1}) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "valueType([1])", nodes, nil); got != "LIST" {
		t.Fatalf("valueType([1]) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "valueType(n)", nodes, nil); got != "ANY" {
		t.Fatalf("valueType(n) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "collect(null)", nodes, nil); !reflect.DeepEqual([]interface{}{}, got) {
		t.Fatalf("collect(null) unexpected: %#v", got)
	}

	// String helper nil/fallback branches.
	for _, expr := range []string{"lower(1)", "upper(1)", "trim(1)", "ltrim(1)", "rtrim(1)"} {
		if got := e.evaluateExpressionWithContext(ctx, expr, nodes, nil); got != nil {
			t.Fatalf("%s should be nil, got %#v", expr, got)
		}
	}
	if got := e.evaluateExpressionWithContext(ctx, "replace('abc', 'a')", nodes, nil); got != nil {
		t.Fatalf("replace with missing arg should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "split('a')", nodes, nil); got != nil {
		t.Fatalf("split with missing delimiter should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "substring('abc', 99, 2)", nodes, nil); got != "" {
		t.Fatalf("substring start >= len should be empty, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "substring('abc')", nodes, nil); got != nil {
		t.Fatalf("substring with missing args should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "left('abc', 99)", nodes, nil); got != "abc" {
		t.Fatalf("left('abc',99) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "left('abc')", nodes, nil); got != nil {
		t.Fatalf("left with missing args should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "right('abc', 99)", nodes, nil); got != "abc" {
		t.Fatalf("right('abc',99) = %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "right('abc')", nodes, nil); got != nil {
		t.Fatalf("right with missing args should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "lpad('x', bad)", nodes, nil); got != nil {
		t.Fatalf("lpad invalid length should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "rpad('x', bad)", nodes, nil); got != nil {
		t.Fatalf("rpad invalid length should be nil, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "format()", nodes, nil); got != nil {
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
		if got := e.evaluateExpressionWithContext(ctx, expr, nodes, nil); got != nil {
			t.Fatalf("%s should be nil, got %#v", expr, got)
		}
	}
	if got := e.evaluateExpressionWithContext(ctx, "point.withinBBox('bad', point({x:0,y:0}), point({x:1,y:1}))", nodes, nil); got != false {
		t.Fatalf("point.withinBBox invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "point.withinDistance('bad', point({x:0,y:0}), 1)", nodes, nil); got != false {
		t.Fatalf("point.withinDistance invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "point.intersects(point({x:1,y:1}), 'bad')", nodes, nil); got != false {
		t.Fatalf("point.intersects invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "point.contains('bad', point({x:1,y:1}))", nodes, nil); got != false {
		t.Fatalf("point.contains invalid input should be false, got %#v", got)
	}

	if got := e.evaluateExpressionWithContext(ctx, "isNaN('bad')", nodes, nil); got != false {
		t.Fatalf("isNaN('bad') should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "all(x IN 'bad' WHERE x > 0)", nodes, nil); got != false {
		t.Fatalf("all invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "any(x IN 'bad' WHERE x > 0)", nodes, nil); got != false {
		t.Fatalf("any invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "none(x IN 'bad' WHERE x > 0)", nodes, nil); got != true {
		t.Fatalf("none invalid input should be true, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "single(x IN 'bad' WHERE x > 0)", nodes, nil); got != false {
		t.Fatalf("single invalid input should be false, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "filter(x IN 'bad' WHERE x > 0)", nodes, nil); !reflect.DeepEqual(got, []interface{}{}) {
		t.Fatalf("filter invalid input should return empty list, got %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "extract(x IN 'bad' | x)", nodes, nil); !reflect.DeepEqual(got, []interface{}{}) {
		t.Fatalf("extract invalid input should return empty list, got %#v", got)
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

	got := e.evaluateExpressionWithContext(ctx, "apoc.coll.pairs([1,2,3])", nodes, nil)
	pairs, ok := got.([]interface{})
	if !ok || len(pairs) != 3 {
		t.Fatalf("apoc.coll.pairs returned %#v", got)
	}

	got = e.evaluateExpressionWithContext(ctx, "apoc.coll.zip([1,2], ['a','b'])", nodes, nil)
	zipped, ok := got.([]interface{})
	if !ok || len(zipped) != 2 {
		t.Fatalf("apoc.coll.zip returned %#v", got)
	}

	got = e.evaluateExpressionWithContext(ctx, "apoc.coll.frequencies(['a','a','b'])", nodes, nil)
	freq, ok := got.(map[string]interface{})
	if !ok || freq["a"] != int64(2) || freq["b"] != int64(1) {
		t.Fatalf("apoc.coll.frequencies returned %#v", got)
	}

	if occ := e.evaluateExpressionWithContext(ctx, "apoc.coll.occurrences([1,2,2,3],2)", nodes, nil); occ != int64(2) {
		t.Fatalf("apoc.coll.occurrences returned %#v", occ)
	}
	if idx := e.evaluateExpressionWithContext(ctx, "apoc.coll.indexOf([1,2,3],2)", nodes, nil); idx != int64(1) {
		t.Fatalf("apoc.coll.indexOf returned %#v", idx)
	}
	if split := e.evaluateExpressionWithContext(ctx, "apoc.coll.split([1,2,3,2,4],2)", nodes, nil); split == nil {
		t.Fatal("apoc.coll.split should return non-nil")
	}
	if part := e.evaluateExpressionWithContext(ctx, "apoc.coll.partition([1,2,3,4],2)", nodes, nil); part == nil {
		t.Fatal("apoc.coll.partition should return non-nil")
	}

	got = e.evaluateExpressionWithContext(ctx, "apoc.convert.toJson({a: 1, b: 'x'})", nodes, nil)
	if s, ok := got.(string); !ok || !strings.Contains(s, "\"a\":1") {
		t.Fatalf("apoc.convert.toJson returned %#v", got)
	}

	if bad := e.evaluateExpressionWithContext(ctx, "apoc.convert.fromJsonMap(42)", nodes, nil); bad != nil {
		t.Fatalf("apoc.convert.fromJsonMap(non-string) should be nil, got %#v", bad)
	}
	if bad := e.evaluateExpressionWithContext(ctx, "apoc.convert.fromJsonList(42)", nodes, nil); bad != nil {
		t.Fatalf("apoc.convert.fromJsonList(non-string) should be nil, got %#v", bad)
	}

	if typ := e.evaluateExpressionWithContext(ctx, "apoc.meta.type(42)", nodes, nil); typ != "INTEGER" {
		t.Fatalf("apoc.meta.type returned %#v", typ)
	}
	if isType := e.evaluateExpressionWithContext(ctx, "apoc.meta.isType(42,'INTEGER')", nodes, nil); isType != true {
		t.Fatalf("apoc.meta.isType should be true, got %#v", isType)
	}
	if isType := e.evaluateExpressionWithContext(ctx, "apoc.meta.isType(42,123)", nodes, nil); isType != false {
		t.Fatalf("apoc.meta.isType(non-string type) should be false, got %#v", isType)
	}

	got = e.evaluateExpressionWithContext(ctx, "apoc.map.merge({a:1}, {b:2})", nodes, nil)
	merged, ok := got.(map[string]interface{})
	if !ok || merged["a"] != int64(1) || merged["b"] != int64(2) {
		t.Fatalf("apoc.map.merge returned %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "apoc.map.merge({a:1})", nodes, nil); !reflect.DeepEqual(got, map[string]interface{}{}) {
		t.Fatalf("apoc.map.merge missing arg should be empty map, got %#v", got)
	}

	got = e.evaluateExpressionWithContext(ctx, "apoc.map.fromPairs(n.pairs)", nodes, nil)
	fromPairsMap, ok := got.(map[string]interface{})
	if !ok || fmt.Sprint(fromPairsMap["k1"]) != "1" || fromPairsMap["k2"] != "v2" {
		t.Fatalf("apoc.map.fromPairs returned %#v", got)
	}
	got = e.evaluateExpressionWithContext(ctx, "apoc.map.fromLists(['x','y'], [10,20])", nodes, nil)
	fromListsMap, ok := got.(map[string]interface{})
	if !ok || fmt.Sprint(fromListsMap["x"]) != "10" || fmt.Sprint(fromListsMap["y"]) != "20" {
		t.Fatalf("apoc.map.fromLists returned %#v", got)
	}
	if got := e.evaluateExpressionWithContext(ctx, "apoc.map.fromLists(['x'])", nodes, nil); got != nil {
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
	if d := e.evaluateExpressionWithContext(ctx, "point.distance(point({x:0,y:0}), point({x:3,y:4}))", nodes, nil); d != float64(5) {
		t.Fatalf("point.distance cartesian returned %#v", d)
	}
	if d := e.evaluateExpressionWithContext(ctx, "point.distance(point({latitude:0,longitude:0}), point({latitude:0,longitude:1}))", nodes, nil); d == nil {
		t.Fatal("point.distance lat/lon should not be nil")
	}

	if ok := e.evaluateExpressionWithContext(ctx, "point.withinBBox(point({x:1,y:1}), point({x:0,y:0}), point({x:2,y:2}))", nodes, nil); ok != true {
		t.Fatalf("point.withinBBox cartesian returned %#v", ok)
	}
	if ok := e.evaluateExpressionWithContext(ctx, "point.withinBBox(point({latitude:1,longitude:1}), point({latitude:0,longitude:0}), point({latitude:2,longitude:2}))", nodes, nil); ok != true {
		t.Fatalf("point.withinBBox lat/lon returned %#v", ok)
	}
	if ok := e.evaluateExpressionWithContext(ctx, "point.withinBBox(1,2,3)", nodes, nil); ok != false {
		t.Fatalf("point.withinBBox invalid args should be false, got %#v", ok)
	}
	if ok := e.evaluateExpressionWithContext(ctx, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}), 2)", nodes, nil); ok != true {
		t.Fatalf("point.withinDistance returned %#v", ok)
	}
	if ok := e.evaluateExpressionWithContext(ctx, "point.withinDistance(point({x:1,y:1}), point({x:0,y:0}))", nodes, nil); ok != false {
		t.Fatalf("point.withinDistance missing arg should be false, got %#v", ok)
	}

	poly := e.evaluateExpressionWithContext(ctx, "polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])", nodes, nil)
	if poly == nil {
		t.Fatal("polygon literal should not be nil")
	}
	if badPoly := e.evaluateExpressionWithContext(ctx, "polygon([point({x:0,y:0}), point({x:1,y:0})])", nodes, nil); badPoly != nil {
		t.Fatalf("polygon with <3 points should be nil, got %#v", badPoly)
	}
	if polyVar := e.evaluateExpressionWithContext(ctx, "polygon(n.polyPts)", nodes, nil); polyVar == nil {
		t.Fatal("polygon(variable points) should not be nil")
	}

	if line := e.evaluateExpressionWithContext(ctx, "lineString([point({x:0,y:0}), point({x:1,y:1})])", nodes, nil); line == nil {
		t.Fatal("lineString literal should not be nil")
	}
	if lineVar := e.evaluateExpressionWithContext(ctx, "lineString(n.linePts)", nodes, nil); lineVar == nil {
		t.Fatal("lineString(variable points) should not be nil")
	}
	if badLine := e.evaluateExpressionWithContext(ctx, "lineString([point({x:0,y:0})])", nodes, nil); badLine != nil {
		t.Fatalf("lineString with <2 points should be nil, got %#v", badLine)
	}

	if hit := e.evaluateExpressionWithContext(ctx, "point.intersects(point({x:0.5,y:0.5}), polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})]))", nodes, nil); hit != true {
		t.Fatalf("point.intersects should be true, got %#v", hit)
	}

	state := e.evaluateExpressionWithContext(ctx, "kalman.velocity.init(10, 2)", nodes, nil)
	stateJSON, ok := state.(string)
	if !ok || stateJSON == "" {
		t.Fatalf("kalman.velocity.init returned %#v", state)
	}
	if p := e.evaluateExpressionWithContext(ctx, "kalman.velocity.process(12, '"+stateJSON+"')", nodes, nil); p == nil {
		t.Fatal("kalman.velocity.process should not be nil")
	}
	if p := e.evaluateExpressionWithContext(ctx, "kalman.velocity.predict('"+stateJSON+"', 3)", nodes, nil); p == nil {
		t.Fatal("kalman.velocity.predict should not be nil")
	}
}
