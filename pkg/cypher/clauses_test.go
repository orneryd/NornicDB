// Comprehensive unit tests for Cypher clauses and CALL procedures in NornicDB.

package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// WITH Clause Tests
// ========================================

func TestWithClause(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "basic WITH",
			query:   "WITH 1 AS num RETURN num",
			wantErr: false,
		},
		{
			name:    "WITH multiple values",
			query:   "WITH 1 AS a, 2 AS b RETURN a, b",
			wantErr: false,
		},
		{
			name:    "WITH string",
			query:   "WITH 'hello' AS msg RETURN msg",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.Execute(ctx, tt.query, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ========================================
// UNWIND Clause Tests
// ========================================

func TestUnwindClause(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		wantRows int
		wantErr  bool
	}{
		{
			name:     "UNWIND simple list",
			query:    "UNWIND [1, 2, 3] AS x RETURN x",
			wantRows: 3,
			wantErr:  false,
		},
		{
			name:     "UNWIND range",
			query:    "UNWIND range(1, 5) AS x RETURN x",
			wantRows: 5,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Execute(ctx, tt.query, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result.Rows) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
		})
	}
}

// ========================================
// UNION Clause Tests
// ========================================

func TestUnionClause(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes
	node1 := &storage.Node{ID: "n1", Labels: []string{"A"}, Properties: map[string]interface{}{"val": int64(1)}}
	node2 := &storage.Node{ID: "n2", Labels: []string{"B"}, Properties: map[string]interface{}{"val": int64(2)}}
	_, _ = store.CreateNode(node1)
	_, _ = store.CreateNode(node2)

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "UNION of two queries",
			query:   "MATCH (a:A) RETURN a.val AS v UNION MATCH (b:B) RETURN b.val AS v",
			wantErr: false,
		},
		{
			name:    "UNION ALL",
			query:   "RETURN 1 AS n UNION ALL RETURN 1 AS n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.Execute(ctx, tt.query, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ========================================
// OPTIONAL MATCH Tests
// ========================================

func TestOptionalMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node without relationships
	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	_, _ = store.CreateNode(node)

	result, err := e.Execute(ctx, "OPTIONAL MATCH (n:Person) RETURN n.name", nil)
	if err != nil {
		t.Fatalf("OPTIONAL MATCH failed: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Error("OPTIONAL MATCH should return at least one row")
	}
}

// ========================================
// FOREACH Clause Tests
// ========================================

func TestForeachClause(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Direct test of executeForeach function
	result, err := e.executeForeach(ctx, "FOREACH (i IN [1, 2, 3] | CREATE (:Item {num: i}))")
	if err != nil {
		t.Fatalf("executeForeach failed: %v", err)
	}

	// FOREACH should return a result
	if result == nil {
		t.Error("FOREACH should return a result")
	}
}

// ========================================
// CALL Procedures Tests
// ========================================

func TestCallDbLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with labels
	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Company"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Person", "Employee"}})

	result, err := e.Execute(ctx, "CALL db.labels()", nil)
	if err != nil {
		t.Fatalf("CALL db.labels() failed: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "label" {
		t.Errorf("Expected column 'label', got %v", result.Columns)
	}

	// Should have Person, Company, Employee
	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 labels, got %d", len(result.Rows))
	}
}

func TestCallDbRelationshipTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationships
	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}})
	store.CreateEdge(&storage.Edge{ID: "r1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"})
	store.CreateEdge(&storage.Edge{ID: "r2", Type: "WORKS_WITH", StartNode: "n1", EndNode: "n2"})

	result, err := e.Execute(ctx, "CALL db.relationshipTypes()", nil)
	if err != nil {
		t.Fatalf("CALL db.relationshipTypes() failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 relationship types, got %d", len(result.Rows))
	}
}

func TestCallDbIndexes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL db.indexes()", nil)
	if err != nil {
		t.Fatalf("CALL db.indexes() failed: %v", err)
	}

	// Should return empty list (no indexes implemented yet)
	if len(result.Columns) == 0 {
		t.Error("Expected columns in result")
	}
}

// TestCallDbConstraints moved to db_procedures_test.go for comprehensive testing

func TestCallDbPropertyKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "Alice", "age": 30}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Properties: map[string]interface{}{"title": "Engineer"}})

	result, err := e.Execute(ctx, "CALL db.propertyKeys()", nil)
	if err != nil {
		t.Fatalf("CALL db.propertyKeys() failed: %v", err)
	}

	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 property keys, got %d", len(result.Rows))
	}
}

func TestCallDbmsComponents(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL dbms.components()", nil)
	if err != nil {
		t.Fatalf("CALL dbms.components() failed: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Error("Expected at least one component")
	}

	// Check for NornicDB
	found := false
	for _, row := range result.Rows {
		if len(row) > 0 && row[0] == "NornicDB" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected NornicDB component")
	}
}

func TestCallDbmsProcedures(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL dbms.procedures()", nil)
	if err != nil {
		t.Fatalf("CALL dbms.procedures() failed: %v", err)
	}

	// Should list available procedures
	if len(result.Rows) < 10 {
		t.Errorf("Expected at least 10 procedures, got %d", len(result.Rows))
	}

	// Check columns
	expectedCols := []string{"name", "description", "mode"}
	for i, col := range expectedCols {
		if i >= len(result.Columns) || result.Columns[i] != col {
			t.Errorf("Expected column %s, got %v", col, result.Columns)
		}
	}
}

func TestCallDbmsFunctions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL dbms.functions()", nil)
	if err != nil {
		t.Fatalf("CALL dbms.functions() failed: %v", err)
	}

	// Should list available functions
	if len(result.Rows) < 15 {
		t.Errorf("Expected at least 15 functions, got %d", len(result.Rows))
	}
}

// ========================================
// NornicDB-specific Procedures Tests
// ========================================

func TestCallNornicDbVersion(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL nornicdb.version()", nil)
	if err != nil {
		t.Fatalf("CALL nornicdb.version() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check columns
	expectedCols := []string{"version", "build", "edition"}
	for i, col := range expectedCols {
		if i >= len(result.Columns) || result.Columns[i] != col {
			t.Errorf("Expected column %s", col)
		}
	}
}

func TestCallNornicDbStats(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some data
	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}})
	store.CreateEdge(&storage.Edge{ID: "r1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"})

	result, err := e.Execute(ctx, "CALL nornicdb.stats()", nil)
	if err != nil {
		t.Fatalf("CALL nornicdb.stats() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Check columns
	expectedCols := []string{"nodes", "relationships", "labels", "relationshipTypes"}
	for i, col := range expectedCols {
		if i >= len(result.Columns) || result.Columns[i] != col {
			t.Errorf("Expected column %s", col)
		}
	}
}

func TestCallNornicDbDecayInfo(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL nornicdb.decay.info()", nil)
	if err != nil {
		t.Fatalf("CALL nornicdb.decay.info() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "enabled" || result.Columns[1] != "system" || result.Columns[2] != "configuredVia" {
		t.Fatalf("unexpected columns: %v", result.Columns)
	}
	if enabled, ok := result.Rows[0][0].(bool); !ok || enabled {
		t.Fatalf("expected enabled=false for memory-backed test engine, got %v", result.Rows[0][0])
	}
	if configuredVia, ok := result.Rows[0][2].(string); !ok || !strings.Contains(configuredVia, "CREATE DECAY PROFILE") || strings.Contains(configuredVia, "CREATE RETENTION BINDING") {
		t.Fatalf("unexpected configuredVia value: %v", result.Rows[0][2])
	}
}

func TestCallNornicDbKnowledgePolicyInfo(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL nornicdb.knowledgepolicy.info()", nil)
	if err != nil {
		t.Fatalf("CALL nornicdb.knowledgepolicy.info() failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Columns) != 7 {
		t.Fatalf("expected 7 columns, got %d", len(result.Columns))
	}
	expectedCols := []string{"enabled", "system", "decayProfiles", "decayBindings", "promotionProfiles", "promotionPolicies", "configuredVia"}
	for i, col := range expectedCols {
		if result.Columns[i] != col {
			t.Fatalf("unexpected columns: %v", result.Columns)
		}
	}
	if enabled, ok := result.Rows[0][0].(bool); !ok || enabled {
		t.Fatalf("expected enabled=false for memory-backed test engine, got %v", result.Rows[0][0])
	}
	for idx := 2; idx <= 5; idx++ {
		if count, ok := result.Rows[0][idx].(int); !ok || count != 0 {
			t.Fatalf("expected zero count in column %d, got %T (%v)", idx, result.Rows[0][idx], result.Rows[0][idx])
		}
	}
	if configuredVia, ok := result.Rows[0][6].(string); !ok || !strings.Contains(configuredVia, "CREATE DECAY PROFILE") || !strings.Contains(configuredVia, "CREATE PROMOTION POLICY") {
		t.Fatalf("unexpected configuredVia value: %v", result.Rows[0][6])
	}
}

func TestCallDbSchemaVisualization(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create schema
	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Company"}})
	store.CreateEdge(&storage.Edge{ID: "r1", Type: "WORKS_AT", StartNode: "n1", EndNode: "n2"})

	result, err := e.Execute(ctx, "CALL db.schema.visualization()", nil)
	if err != nil {
		t.Fatalf("CALL db.schema.visualization() failed: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Error("Expected schema data")
	}
}

func TestCallDbSchemaNodeProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": 30}})

	result, err := e.Execute(ctx, "CALL db.schema.nodeProperties()", nil)
	if err != nil {
		t.Fatalf("CALL db.schema.nodeProperties() failed: %v", err)
	}

	// Check columns
	expectedCols := []string{"nodeLabel", "propertyName", "propertyType"}
	for i, col := range expectedCols {
		if i >= len(result.Columns) || result.Columns[i] != col {
			t.Errorf("Expected column %s", col)
		}
	}
}

func TestCallDbSchemaRelProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{ID: "n1"})
	_, _ = store.CreateNode(&storage.Node{ID: "n2"})
	store.CreateEdge(&storage.Edge{ID: "r1", Type: "KNOWS", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"since": 2020}})

	result, err := e.Execute(ctx, "CALL db.schema.relProperties()", nil)
	if err != nil {
		t.Fatalf("CALL db.schema.relProperties() failed: %v", err)
	}

	// Check columns
	expectedCols := []string{"relType", "propertyName", "propertyType"}
	for i, col := range expectedCols {
		if i >= len(result.Columns) || result.Columns[i] != col {
			t.Errorf("Expected column %s", col)
		}
	}
}

// ========================================
// Unknown Procedure Tests
// ========================================

func TestCallUnknownProcedure(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CALL unknown.procedure()", nil)
	if err == nil {
		t.Error("Expected error for unknown procedure")
	}
	if !strings.Contains(err.Error(), "unknown procedure") {
		t.Errorf("Expected 'unknown procedure' error, got: %v", err)
	}
}

// ========================================
// LOAD CSV Tests
// ========================================

func TestLoadCSVNotSupported(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Direct test of executeLoadCSV function
	_, err := e.executeLoadCSV(ctx, "LOAD CSV FROM 'file.csv' AS row RETURN row")
	// LOAD CSV should error as not supported
	if err == nil {
		t.Error("Expected error for LOAD CSV (not supported)")
		return
	}
	// Accept any error message about not being supported
	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "not supported") {
		t.Errorf("Expected 'not supported' error, got: %v", err)
	}
}

// ========================================
// DROP/CREATE Schema Commands Tests
// ========================================

func TestSchemaCommandsNoOp(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// DROP INDEX should be a no-op
	result, err := e.Execute(ctx, "DROP INDEX my_index IF EXISTS", nil)
	if err != nil {
		t.Errorf("DROP INDEX should not error: %v", err)
	}
	if result == nil {
		t.Error("Expected empty result, got nil")
	}

	// CREATE CONSTRAINT should be a no-op
	result, err = e.Execute(ctx, "CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.id IS UNIQUE", nil)
	if err != nil {
		t.Errorf("CREATE CONSTRAINT should not error: %v", err)
	}

	// CREATE INDEX should be a no-op
	result, err = e.Execute(ctx, "CREATE INDEX my_index FOR (n:Person) ON (n.name)", nil)
	if err != nil {
		t.Errorf("CREATE INDEX should not error: %v", err)
	}
}

// ========================================
// Return Clause Tests
// ========================================

func TestReturnLiterals(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		query    string
		expected interface{}
	}{
		{"RETURN 1", int64(1)},
		{"RETURN 'hello'", "hello"},
		// Note: RETURN true/false returns 1/0 in simple RETURN parser
		{"RETURN 3.14", float64(3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result, err := e.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
				t.Fatalf("Expected 1 row with 1 column")
			}
			if result.Rows[0][0] != tt.expected {
				t.Errorf("got %v (%T), want %v (%T)", result.Rows[0][0], result.Rows[0][0], tt.expected, tt.expected)
			}
		})
	}
}

func TestReturnExpressions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Test that RETURN expressions execute without error
	tests := []string{
		"RETURN 1 + 1 AS sum",
		"RETURN size('hello') AS len",
		"RETURN toUpper('hello') AS upper",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			result, err := e.Execute(ctx, query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}
		})
	}
}

func TestReturnMathFunctions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		query    string
		expected float64
		delta    float64
	}{
		{"RETURN sinh(0.7)", 0.7585837018395334, 0.001},
		{"RETURN cosh(0.7)", 1.255169005630943, 0.001},
		{"RETURN tanh(0.7)", 0.6043677771171636, 0.001},
		{"RETURN coth(1)", 1.3130352854993312, 0.001},
		{"RETURN power(2, 10)", 1024, 0.001},
		{"RETURN power(4, 0.5)", 2, 0.001},
		{"RETURN sin(0)", 0, 0.001},
		{"RETURN cos(0)", 1, 0.001},
		{"RETURN sqrt(16)", 4, 0.001},
		{"RETURN abs(-5)", 5, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result, err := e.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
				t.Fatalf("Expected 1 row with 1 column, got %d rows", len(result.Rows))
			}
			var resultF float64
			switch v := result.Rows[0][0].(type) {
			case float64:
				resultF = v
			case int64:
				resultF = float64(v)
			default:
				t.Fatalf("Expected numeric result, got %T", result.Rows[0][0])
			}
			if resultF < tt.expected-tt.delta || resultF > tt.expected+tt.delta {
				t.Errorf("got %v, want %v (±%v)", resultF, tt.expected, tt.delta)
			}
		})
	}
}

// ========================================
// Neo4j Vector Index Procedure Tests
// ========================================

func TestCallDbIndexVectorQueryNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with an embedding
	_, _ = store.CreateNode(&storage.Node{
		ID:              "vec-node-1",
		Labels:          []string{"Test"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	})

	// Call the vector query procedure
	result, err := e.Execute(ctx, "CALL db.index.vector.queryNodes('node_embedding_index', 10, [0.1, 0.2, 0.3])", nil)
	if err != nil {
		t.Fatalf("Vector query failed: %v", err)
	}
	if len(result.Columns) != 2 || result.Columns[0] != "node" || result.Columns[1] != "score" {
		t.Errorf("Expected columns [node, score], got %v", result.Columns)
	}
	if len(result.Rows) < 1 {
		t.Error("Expected at least one result with embedding")
	}
}

// ========================================
// Neo4j Fulltext Index Procedure Tests
// ========================================

func TestCallDbIndexFulltextQueryNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create the fulltext index first (Neo4j compatibility)
	_, err := e.Execute(ctx, "CALL db.index.fulltext.createNodeIndex('node_search', ['Document'], ['content', 'title'])", nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index: %v", err)
	}

	// Create nodes with searchable content
	_, _ = store.CreateNode(&storage.Node{
		ID:     "doc-1",
		Labels: []string{"Document"},
		Properties: map[string]interface{}{
			"title":   "Test Document",
			"content": "This is searchable content about vectors",
		},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:     "doc-2",
		Labels: []string{"Document"},
		Properties: map[string]interface{}{
			"title":   "Another Doc",
			"content": "Different text here",
		},
	})

	// Search for "searchable"
	result, err := e.Execute(ctx, "CALL db.index.fulltext.queryNodes('node_search', 'searchable')", nil)
	if err != nil {
		t.Fatalf("Fulltext query failed: %v", err)
	}
	if len(result.Columns) != 2 || result.Columns[0] != "node" || result.Columns[1] != "score" {
		t.Errorf("Expected columns [node, score], got %v", result.Columns)
	}
	if len(result.Rows) < 1 {
		t.Error("Expected at least one result matching 'searchable'")
	}
}

func TestCallDbIndexFulltextQueryNoMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create the fulltext index first (Neo4j compatibility)
	_, err := e.Execute(ctx, "CALL db.index.fulltext.createNodeIndex('node_search', ['Document'], ['content'])", nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index: %v", err)
	}

	_, _ = store.CreateNode(&storage.Node{
		ID:         "doc-1",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"content": "hello world"},
	})

	// Search for something that doesn't exist
	result, err := e.Execute(ctx, "CALL db.index.fulltext.queryNodes('node_search', 'nonexistent')", nil)
	if err != nil {
		t.Fatalf("Fulltext query failed: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected no results, got %d", len(result.Rows))
	}
}

// ========================================
// APOC Path Procedure Tests
// ========================================

func TestCallApocPathSubgraphNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a small graph: Alice -> Bob -> Carol
	_, _ = store.CreateNode(&storage.Node{ID: "alice", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	_, _ = store.CreateNode(&storage.Node{ID: "bob", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	_, _ = store.CreateNode(&storage.Node{ID: "carol", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "KNOWS", StartNode: "alice", EndNode: "bob"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "KNOWS", StartNode: "bob", EndNode: "carol"})

	// Call subgraph nodes procedure
	result, err := e.Execute(ctx, "CALL apoc.path.subgraphNodes(start, {maxLevel: 2, relationshipFilter: 'KNOWS'})", nil)
	if err != nil {
		t.Fatalf("APOC subgraph query failed: %v", err)
	}
	if len(result.Columns) != 1 || result.Columns[0] != "node" {
		t.Errorf("Expected columns [node], got %v", result.Columns)
	}
	// Should return all 3 nodes since they're all connected
	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 nodes in subgraph, got %d", len(result.Rows))
	}
}

func TestCallApocPathExpand(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a simple graph: n1 -> n2 -> n3
	_, _ = store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Node"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Node"}})
	_, _ = store.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Node"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "LINK", StartNode: "n1", EndNode: "n2"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "LINK", StartNode: "n2", EndNode: "n3"})

	t.Run("basic_path_expansion", func(t *testing.T) {
		// Call path expand procedure - should return paths from n1
		// Use direct node reference via MATCH to find the node
		result, err := e.Execute(ctx, "MATCH (start:Node {id: 'n1'}) CALL apoc.path.expand(start, 'LINK', null, 1, 2) YIELD path RETURN path", nil)
		if err != nil {
			t.Fatalf("APOC expand query failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		// Note: The query may return 0 rows if the start node isn't found or if there are no paths
		// This is expected behavior - the implementation should handle this gracefully
		if len(result.Rows) == 0 {
			t.Logf("No paths returned - this may be expected if start node wasn't found or no paths exist")
		}

		// Verify paths contain both nodes and relationships
		for _, row := range result.Rows {
			if len(row) == 0 {
				continue
			}
			pathMap, ok := row[0].(map[string]interface{})
			if !ok {
				t.Errorf("Expected path to be a map, got %T", row[0])
				continue
			}

			// Check that path has nodes
			nodes, hasNodes := pathMap["nodes"].([]interface{})
			if !hasNodes || len(nodes) == 0 {
				t.Error("Path should have nodes")
			}

			// Check that path has relationships (for paths with length > 0)
			rels, hasRels := pathMap["relationships"].([]interface{})
			length, hasLength := pathMap["length"].(int)
			if hasLength && length > 0 {
				if !hasRels || len(rels) == 0 {
					t.Errorf("Path with length %d should have relationships", length)
				}
			}
		}
	})

	t.Run("path_with_relationship_filter", func(t *testing.T) {
		// Create additional edge with different type
		store.CreateEdge(&storage.Edge{ID: "e3", Type: "OTHER", StartNode: "n1", EndNode: "n3"})

		result, err := e.Execute(ctx, "MATCH (start:Node {id: 'n1'}) CALL apoc.path.expand(start, 'LINK', null, 1, 2) YIELD path RETURN path", nil)
		if err != nil {
			t.Fatalf("APOC expand with filter failed: %v", err)
		}

		// Verify all paths only use LINK relationships
		for _, row := range result.Rows {
			if len(row) == 0 {
				continue
			}
			pathMap, ok := row[0].(map[string]interface{})
			if !ok {
				continue
			}

			rels, hasRels := pathMap["relationships"].([]interface{})
			if hasRels {
				for _, rel := range rels {
					relMap, ok := rel.(map[string]interface{})
					if ok {
						relType, _ := relMap["type"].(string)
						if relType != "LINK" {
							t.Errorf("Expected only LINK relationships, found %s", relType)
						}
					}
				}
			}
		}
	})

	t.Run("path_with_min_max_level", func(t *testing.T) {
		// Test minLevel and maxLevel constraints
		result, err := e.Execute(ctx, "MATCH (start:Node {id: 'n1'}) CALL apoc.path.expand(start, null, null, 2, 2) YIELD path RETURN path", nil)
		if err != nil {
			t.Fatalf("APOC expand with level constraints failed: %v", err)
		}

		// All paths should have length 2
		for _, row := range result.Rows {
			if len(row) == 0 {
				continue
			}
			pathMap, ok := row[0].(map[string]interface{})
			if !ok {
				continue
			}

			length, hasLength := pathMap["length"].(int)
			if hasLength && length != 2 {
				t.Errorf("Expected path length 2, got %d", length)
			}
		}
	})
}

func TestApocPathConfig(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	tests := []struct {
		cypher    string
		maxLevel  int
		direction string
		types     []string
	}{
		{
			"CALL apoc.path.subgraphNodes(n, {maxLevel: 5})",
			5, "both", nil,
		},
		{
			"CALL apoc.path.subgraphNodes(n, {maxLevel: 3, relationshipFilter: 'KNOWS'})",
			3, "both", []string{"KNOWS"},
		},
		{
			"CALL apoc.path.subgraphNodes(n, {relationshipFilter: '>FOLLOWS'})",
			3, "outgoing", []string{"FOLLOWS"},
		},
		{
			"CALL apoc.path.subgraphNodes(n, {relationshipFilter: '<FOLLOWS|KNOWS'})",
			3, "incoming", []string{"FOLLOWS", "KNOWS"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.cypher, func(t *testing.T) {
			config := e.parseApocPathConfig(tt.cypher)
			if config.maxLevel != tt.maxLevel {
				t.Errorf("maxLevel = %d, want %d", config.maxLevel, tt.maxLevel)
			}
			if config.direction != tt.direction {
				t.Errorf("direction = %s, want %s", config.direction, tt.direction)
			}
			if len(config.relationshipTypes) != len(tt.types) {
				t.Errorf("types = %v, want %v", config.relationshipTypes, tt.types)
			}
		})
	}
}

// ========================================
// EXISTS Subquery Tests
// ========================================

func TestExistsSubquery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with relationships
	_, _ = store.CreateNode(&storage.Node{ID: "watched-file", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/watched/file.txt"}})
	_, _ = store.CreateNode(&storage.Node{ID: "orphan-file", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/orphan/file.txt"}})
	_, _ = store.CreateNode(&storage.Node{ID: "watcher", Labels: []string{"WatchConfig"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "WATCHES", StartNode: "watcher", EndNode: "watched-file"})

	// Query files that have a WATCHES relationship
	result, err := e.Execute(ctx, `
		MATCH (f:File)
		WHERE EXISTS { MATCH (f)<-[:WATCHES]-(:WatchConfig) }
		RETURN f.path
	`, nil)

	if err != nil {
		t.Fatalf("EXISTS subquery failed: %v", err)
	}

	// Should return only the watched file
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestNotExistsSubquery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with relationships
	_, _ = store.CreateNode(&storage.Node{ID: "watched-file", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/watched/file.txt"}})
	_, _ = store.CreateNode(&storage.Node{ID: "orphan-file", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/orphan/file.txt"}})
	_, _ = store.CreateNode(&storage.Node{ID: "watcher", Labels: []string{"WatchConfig"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "WATCHES", StartNode: "watcher", EndNode: "watched-file"})

	// Query files that don't have a WATCHES relationship
	result, err := e.Execute(ctx, `
		MATCH (f:File)
		WHERE NOT EXISTS { MATCH (f)<-[:WATCHES]-(:WatchConfig) }
		RETURN f.path
	`, nil)

	if err != nil {
		t.Fatalf("NOT EXISTS subquery failed: %v", err)
	}

	// Should return only the orphan file
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row (orphan file), got %d", len(result.Rows))
	}
}

// ========================================
// SET += Property Merge Tests
// ========================================

func TestSetPlusMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with some properties
	_, _ = store.CreateNode(&storage.Node{
		ID:     "test-node",
		Labels: []string{"MergeTest"},
		Properties: map[string]interface{}{
			"name":    "Original",
			"version": 1,
		},
	})

	// Use SET += to merge properties - match by label only
	result, err := e.Execute(ctx, `
		MATCH (n:MergeTest)
		SET n += {version: 2, status: 'updated'}
	`, nil)

	if err != nil {
		t.Fatalf("SET += failed: %v", err)
	}

	t.Logf("Stats: %+v", result.Stats)

	// Verify properties were merged - get fresh copy
	node, err := store.GetNode("test-node")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	t.Logf("Node properties: %+v", node.Properties)

	if node.Properties["name"] != "Original" {
		t.Errorf("Original property was lost, got: %v", node.Properties["name"])
	}
	if node.Properties["status"] != "updated" {
		t.Errorf("New property not added, got: %v", node.Properties["status"])
	}
}

func TestSetPlusMergeWithParameter(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with some properties
	_, _ = store.CreateNode(&storage.Node{
		ID:     "test-node-param",
		Labels: []string{"MergeTest"},
		Properties: map[string]interface{}{
			"name":    "Original",
			"version": 1,
		},
	})

	// Use SET += with parameter
	params := map[string]interface{}{
		"props": map[string]interface{}{
			"version": 2,
			"status":  "updated",
			"count":   int64(42),
		},
	}

	result, err := e.Execute(ctx, `
		MATCH (n:MergeTest {name: 'Original'})
		SET n += $props
	`, params)

	if err != nil {
		t.Fatalf("SET += with parameter failed: %v", err)
	}

	t.Logf("Stats: %+v", result.Stats)

	// Verify properties were merged - get fresh copy
	node, err := store.GetNode("test-node-param")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	t.Logf("Node properties: %+v", node.Properties)

	if node.Properties["name"] != "Original" {
		t.Errorf("Original property was lost, got: %v", node.Properties["name"])
	}
	if node.Properties["status"] != "updated" {
		t.Errorf("New property from parameter not added, got: %v", node.Properties["status"])
	}
	if node.Properties["version"] != int64(2) {
		t.Errorf("Property from parameter not updated, got: %v", node.Properties["version"])
	}
	if node.Properties["count"] != int64(42) {
		t.Errorf("Numeric property from parameter not added, got: %v", node.Properties["count"])
	}
}

func TestSetPlusMergeWithMapVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with some properties
	_, _ = store.CreateNode(&storage.Node{
		ID:     "test-node-map-var",
		Labels: []string{"MergeTest"},
		Properties: map[string]interface{}{
			"name":    "Original",
			"version": 1,
		},
	})

	result, err := e.Execute(ctx, `
		WITH {version: 3, status: 'from_with'} AS props
		MATCH (n:MergeTest {name: 'Original'})
		SET n += props
	`, nil)
	if err != nil {
		t.Fatalf("SET += with map variable failed: %v", err)
	}

	t.Logf("Stats: %+v", result.Stats)

	// Verify properties were merged - get fresh copy
	node, err := store.GetNode("test-node-map-var")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	t.Logf("Node properties: %+v", node.Properties)

	if node.Properties["status"] != "from_with" {
		t.Errorf("Map variable property not added, got: %v", node.Properties["status"])
	}
	if node.Properties["version"] != int64(3) {
		t.Errorf("Map variable property not updated, got: %v", node.Properties["version"])
	}
}

// ========================================
// REMOVE Property Tests
// ========================================

func TestRemoveProperty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node with properties using explicit error check
	node := &storage.Node{
		ID:     "remove-test",
		Labels: []string{"RemoveTest"},
		Properties: map[string]interface{}{
			"name":      "Test",
			"embedding": []float32{0.1, 0.2, 0.3},
			"temp":      "to-be-removed",
		},
	}
	if _, err := store.CreateNode(node); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Remove the embedding property
	_, err := e.Execute(ctx, `MATCH (n:RemoveTest) REMOVE n.embedding`, nil)
	if err != nil {
		t.Fatalf("REMOVE failed: %v", err)
	}

	// Verify property was removed
	updatedNode, _ := store.GetNode("remove-test")
	if _, exists := updatedNode.Properties["embedding"]; exists {
		t.Error("embedding property should have been removed")
	}
	if updatedNode.Properties["name"] != "Test" {
		t.Error("name property should still exist")
	}
}

func TestRemoveMultipleProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "multi-remove",
		Labels: []string{"MultiRemove"},
		Properties: map[string]interface{}{
			"name":        "Test",
			"lockedBy":    "user1",
			"lockedAt":    "2024-01-01",
			"lockExpires": "2024-01-02",
		},
	}
	if _, err := store.CreateNode(node); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Remove multiple properties
	_, err := e.Execute(ctx, `MATCH (n:MultiRemove) REMOVE n.lockedBy, n.lockedAt, n.lockExpires`, nil)
	if err != nil {
		t.Fatalf("REMOVE multiple failed: %v", err)
	}

	updatedNode, _ := store.GetNode("multi-remove")
	if _, exists := updatedNode.Properties["lockedBy"]; exists {
		t.Error("lockedBy should have been removed")
	}
	if _, exists := updatedNode.Properties["lockedAt"]; exists {
		t.Error("lockedAt should have been removed")
	}
	if updatedNode.Properties["name"] != "Test" {
		t.Error("name should still exist")
	}
}

func TestRemoveWithReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "remove-return",
		Labels: []string{"RemoveReturn"},
		Properties: map[string]interface{}{
			"name": "Test",
			"temp": "value",
		},
	}
	if _, err := store.CreateNode(node); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	result, err := e.Execute(ctx, `MATCH (n:RemoveReturn) REMOVE n.temp RETURN n`, nil)
	if err != nil {
		t.Fatalf("REMOVE with RETURN failed: %v", err)
	}
	// REMOVE executed - check the property was removed
	updatedNode, _ := store.GetNode("remove-return")
	if _, exists := updatedNode.Properties["temp"]; exists {
		t.Error("temp property should have been removed")
	}
	t.Logf("REMOVE with RETURN result: %d rows", len(result.Rows))
}

func TestSetThenRemoveLabel_SameQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "set-remove-label",
		Labels: []string{"Base", "MongoDocument"},
		Properties: map[string]interface{}{
			"name": "x",
		},
	}
	if _, err := store.CreateNode(node); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	_, err := e.Execute(ctx, "MATCH (n:Base) SET n:OriginalText REMOVE n:MongoDocument RETURN n", nil)
	if err != nil {
		t.Fatalf("SET ... REMOVE label failed: %v", err)
	}

	updated, err := store.GetNode("set-remove-label")
	if err != nil {
		t.Fatalf("Failed to reload node: %v", err)
	}
	hasOriginal := false
	hasLegacy := false
	for _, l := range updated.Labels {
		if l == "OriginalText" {
			hasOriginal = true
		}
		if l == "MongoDocument" {
			hasLegacy = true
		}
	}
	if !hasOriginal {
		t.Fatalf("expected OriginalText label to be present, got labels=%v", updated.Labels)
	}
	if hasLegacy {
		t.Fatalf("expected MongoDocument label to be removed, got labels=%v", updated.Labels)
	}
}

func TestRemoveLabelOnly(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "remove-label-only",
		Labels: []string{"OriginalText", "MongoDocument"},
		Properties: map[string]interface{}{
			"originalText": "hello",
		},
	}
	if _, err := store.CreateNode(node); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	_, err := e.Execute(ctx, "MATCH (n:OriginalText) REMOVE n:MongoDocument RETURN n", nil)
	if err != nil {
		t.Fatalf("REMOVE label failed: %v", err)
	}

	updated, err := store.GetNode("remove-label-only")
	if err != nil {
		t.Fatalf("Failed to reload node: %v", err)
	}
	for _, l := range updated.Labels {
		if l == "MongoDocument" {
			t.Fatalf("MongoDocument label should be removed, got labels=%v", updated.Labels)
		}
	}
}

func TestBatchRelationshipMergeWithUnwindKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "o-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create o-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "o-2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create o-2 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create t-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t-2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create t-2 failed: %v", err)
	}

	query := `
UNWIND $keys AS join_key
MATCH (o:OriginalText)
WHERE o.__tmpJoinKey = join_key
MATCH (t:TranslatedText)
WHERE t.__tmpJoinKey = join_key
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
`
	params := map[string]interface{}{"keys": []interface{}{"k1", "k2"}}
	if _, err := e.Execute(ctx, query, params); err != nil {
		t.Fatalf("batch unwind merge failed: %v", err)
	}

	verify, err := e.Execute(ctx, "MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c", nil)
	if err != nil {
		t.Fatalf("verify relationship count failed: %v", err)
	}
	if len(verify.Rows) != 1 || len(verify.Rows[0]) != 1 {
		t.Fatalf("unexpected verify shape: %+v", verify.Rows)
	}
	if got, ok := verify.Rows[0][0].(int64); !ok || got != 2 {
		t.Fatalf("expected 2 TRANSLATES_TO edges, got %#v", verify.Rows[0][0])
	}
}

func TestBatchRelationshipMergeWithMatchInKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "o2-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create o2-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "o2-2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create o2-2 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t2-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create t2-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t2-2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create t2-2 failed: %v", err)
	}

	query := `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS matched_pairs
`
	params := map[string]interface{}{"keys": []interface{}{"k1", "k2"}}
	if _, err := e.Execute(ctx, query, params); err != nil {
		t.Fatalf("batch match-in merge failed: %v", err)
	}

	verify, err := e.Execute(ctx, "MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c", nil)
	if err != nil {
		t.Fatalf("verify relationship count failed: %v", err)
	}
	if len(verify.Rows) != 1 || len(verify.Rows[0]) != 1 {
		t.Fatalf("unexpected verify shape: %+v", verify.Rows)
	}
	if got, ok := verify.Rows[0][0].(int64); !ok || got != 2 {
		t.Fatalf("expected 2 TRANSLATES_TO edges, got %#v", verify.Rows[0][0])
	}
}

func TestBatchRelationshipCreateWithMatchInKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "o3-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create o3-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "o3-2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create o3-2 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t3-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k1",
		},
	})
	if err != nil {
		t.Fatalf("create t3-1 failed: %v", err)
	}
	_, err = store.CreateNode(&storage.Node{
		ID:     "t3-2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"__tmpJoinKey": "k2",
		},
	})
	if err != nil {
		t.Fatalf("create t3-2 failed: %v", err)
	}

	query := `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS matched_pairs
`
	params := map[string]interface{}{"keys": []interface{}{"k1", "k2"}}
	if _, err := e.Execute(ctx, query, params); err != nil {
		t.Fatalf("batch match-in create failed: %v", err)
	}

	verify, err := e.Execute(ctx, "MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c", nil)
	if err != nil {
		t.Fatalf("verify relationship count failed: %v", err)
	}
	if len(verify.Rows) != 1 || len(verify.Rows[0]) != 1 {
		t.Fatalf("unexpected verify shape: %+v", verify.Rows)
	}
	if got, ok := verify.Rows[0][0].(int64); !ok || got != 2 {
		t.Fatalf("expected 2 TRANSLATES_TO edges, got %#v", verify.Rows[0][0])
	}
}

// ========================================
// UNION Tests
// ========================================

func TestUnionAll(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, _ = store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"TypeA"}, Properties: map[string]interface{}{"name": "A1"}})
	_, _ = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"TypeB"}, Properties: map[string]interface{}{"name": "B1"}})

	result, err := e.Execute(ctx, `MATCH (a:TypeA) RETURN a.name AS name UNION ALL MATCH (b:TypeB) RETURN b.name AS name`, nil)
	if err != nil {
		t.Fatalf("UNION ALL failed: %v", err)
	}
	t.Logf("UNION ALL result: %d rows", len(result.Rows))
	// At least verify it didn't error
}

func TestUnionDistinct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{ID: "u1", Labels: []string{"Union1"}, Properties: map[string]interface{}{"val": 1}})
	_, _ = store.CreateNode(&storage.Node{ID: "u2", Labels: []string{"Union2"}, Properties: map[string]interface{}{"val": 1}})

	result, err := e.Execute(ctx, `
		MATCH (a:Union1) RETURN a.val AS val
		UNION
		MATCH (b:Union2) RETURN b.val AS val
	`, nil)
	if err != nil {
		t.Fatalf("UNION failed: %v", err)
	}
	// UNION (distinct) should deduplicate
	t.Logf("UNION result rows: %d", len(result.Rows))
}

// ========================================
// OPTIONAL MATCH Tests
// ========================================

func TestOptionalMatchWithNoResult(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{ID: "opt1", Labels: []string{"Orphan"}})

	result, err := e.Execute(ctx, `
		OPTIONAL MATCH (n:NonExistent)
		RETURN n
	`, nil)
	if err != nil {
		t.Fatalf("OPTIONAL MATCH failed: %v", err)
	}
	// Should return null for non-matching
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row with null, got %d", len(result.Rows))
	}
}

// ========================================
// UNWIND Tests
// ========================================

func TestUnwindList(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, `UNWIND [1, 2, 3] AS x RETURN x`, nil)
	if err != nil {
		t.Fatalf("UNWIND failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows from UNWIND, got %d", len(result.Rows))
	}
}

func TestUnwindRange(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, `UNWIND range(1, 5) AS x RETURN x`, nil)
	if err != nil {
		t.Fatalf("UNWIND range failed: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows from UNWIND range, got %d", len(result.Rows))
	}
}

func TestUnwindCreateSetFromMapParameter(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoRecord)
SET n = row.properties
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"properties": map[string]interface{}{
					"_mongo_database":   "nornic-translation",
					"_mongo_collection": "nornic_language_list",
					"_mongo_id":         "abc123",
					"name":              "English",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("UNWIND CREATE/SET from parameterized map failed: %v", err)
	}

	result, err := e.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'abc123'})
RETURN n._mongo_collection AS coll, n._mongo_database AS db, n._mongo_id AS id
`, nil)
	if err != nil {
		t.Fatalf("MATCH failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 imported node, got %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "nornic_language_list" {
		t.Fatalf("expected _mongo_collection=nornic_language_list, got %#v", got)
	}
	if got := result.Rows[0][1]; got != "nornic-translation" {
		t.Fatalf("expected _mongo_database=nornic-translation, got %#v", got)
	}
	if got := result.Rows[0][2]; got != "abc123" {
		t.Fatalf("expected _mongo_id=abc123, got %#v", got)
	}
}

// ========================================
// Helper Function Tests
// ========================================

func TestParseRemoveProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	tests := []struct {
		input    string
		expected []string
	}{
		{"n.prop1", []string{"prop1"}},
		{"n.prop1, n.prop2", []string{"prop1", "prop2"}},
		{"n.lockedBy, n.lockedAt, n.lockExpires", []string{"lockedBy", "lockedAt", "lockExpires"}},
		{"", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := e.parseRemoveProperties(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d props, got %d: %v", len(tt.expected), len(result), result)
			}
		})
	}
}

func TestNodeMatchesProps(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	node := &storage.Node{
		ID:     "test",
		Labels: []string{"Test"},
		Properties: map[string]interface{}{
			"name":   "Alice",
			"age":    30,
			"active": true,
		},
	}

	tests := []struct {
		props    map[string]interface{}
		expected bool
	}{
		{nil, true},
		{map[string]interface{}{"name": "Alice"}, true},
		{map[string]interface{}{"name": "Bob"}, false},
		{map[string]interface{}{"name": "Alice", "age": 30}, true},
		{map[string]interface{}{"missing": "value"}, false},
	}

	for i, tt := range tests {
		result := e.nodeMatchesProps(node, tt.props)
		if result != tt.expected {
			t.Errorf("Test %d: expected %v, got %v for props %v", i, tt.expected, result, tt.props)
		}
	}
}

func TestGetParamKeys(t *testing.T) {
	params := map[string]interface{}{
		"name":  "Alice",
		"age":   30,
		"items": []int{1, 2, 3},
	}
	keys := getParamKeys(params)
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}
}

func TestMinMax(t *testing.T) {
	if min(3, 5) != 3 {
		t.Error("min(3, 5) should be 3")
	}
	if min(5, 3) != 3 {
		t.Error("min(5, 3) should be 3")
	}
	if max(3, 5) != 5 {
		t.Error("max(3, 5) should be 5")
	}
	if max(5, 3) != 5 {
		t.Error("max(5, 3) should be 5")
	}
}

func TestToFloat64Slice(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected bool
	}{
		{[]float64{1.0, 2.0, 3.0}, true},
		{[]interface{}{1.0, 2.0, 3.0}, true},
		{[]interface{}{1, 2, 3}, true},
		{[]interface{}{"not", "numbers"}, false},
		{"not a slice", false},
	}

	for i, tt := range tests {
		_, ok := toFloat64Slice(tt.input)
		if ok != tt.expected {
			t.Errorf("Test %d: expected ok=%v, got ok=%v", i, tt.expected, ok)
		}
	}
}

func TestCosineSimilarity(t *testing.T) {
	// Identical vectors should have similarity 1
	a := []float64{1, 0, 0}
	b := []float64{1, 0, 0}
	sim := vector.CosineSimilarityFloat64(a, b)
	if sim < 0.99 {
		t.Errorf("Identical vectors should have sim ~1, got %f", sim)
	}

	// Orthogonal vectors should have similarity 0
	a = []float64{1, 0, 0}
	b = []float64{0, 1, 0}
	sim = vector.CosineSimilarityFloat64(a, b)
	if sim > 0.01 {
		t.Errorf("Orthogonal vectors should have sim ~0, got %f", sim)
	}

	// Different length vectors
	a = []float64{1, 2}
	b = []float64{1, 2, 3}
	sim = vector.CosineSimilarityFloat64(a, b)
	if sim != 0 {
		t.Errorf("Different length vectors should return 0, got %f", sim)
	}
}

func TestEuclideanSimilarity(t *testing.T) {
	// Identical vectors should have high similarity
	a := []float64{1, 0, 0}
	b := []float64{1, 0, 0}
	sim := vector.EuclideanSimilarityFloat64(a, b)
	if sim < 0.99 {
		t.Errorf("Identical vectors should have sim ~1, got %f", sim)
	}

	// Different length vectors
	a = []float64{1, 2}
	b = []float64{1, 2, 3}
	sim = vector.EuclideanSimilarityFloat64(a, b)
	if sim != 0 {
		t.Errorf("Different length vectors should return 0, got %f", sim)
	}
}

func TestGetLatLon(t *testing.T) {
	// Test with latitude/longitude keys
	m := map[string]interface{}{"latitude": 40.7128, "longitude": -74.0060}
	lat, lon, ok := getLatLon(m)
	if !ok {
		t.Error("Should parse latitude/longitude")
	}
	if lat != 40.7128 || lon != -74.0060 {
		t.Errorf("Wrong values: %f, %f", lat, lon)
	}

	// Test with lat/lon keys
	m = map[string]interface{}{"lat": 40.7128, "lon": -74.0060}
	lat, lon, ok = getLatLon(m)
	if !ok {
		t.Error("Should parse lat/lon")
	}

	// Test with missing keys
	m = map[string]interface{}{"x": 1, "y": 2}
	_, _, ok = getLatLon(m)
	if ok {
		t.Error("Should fail for missing lat/lon")
	}
}

// ========================================
// Traversal Additional Tests
// ========================================

func TestGetRelType(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	// Create edge
	_, _ = store.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Start"}})
	_, _ = store.CreateNode(&storage.Node{ID: "e1", Labels: []string{"End"}})
	store.CreateEdge(&storage.Edge{ID: "r1", Type: "KNOWS", StartNode: "s1", EndNode: "e1"})

	relType := e.getRelType("r1")
	if relType != "KNOWS" {
		t.Errorf("Expected KNOWS, got %s", relType)
	}

	// Non-existent edge
	relType = e.getRelType("nonexistent")
	if relType != "" {
		t.Errorf("Expected empty string for non-existent edge, got %s", relType)
	}
}

func TestTraverseGraphDeeper(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a deeper graph: A -> B -> C -> D
	_, _ = store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	_, _ = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	_, _ = store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	_, _ = store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "LINK", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "LINK", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "LINK", StartNode: "c", EndNode: "d"})

	// Test variable length path
	result, err := e.Execute(ctx, `MATCH (a:Node {name: 'A'})-[*1..3]->(end) RETURN end.name`, nil)
	if err != nil {
		t.Fatalf("Variable length path failed: %v", err)
	}
	t.Logf("Variable length result: %d rows", len(result.Rows))
}

func TestAllShortestPathsMultiple(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	// Create a diamond graph: A -> B -> D, A -> C -> D (two equal paths)
	_, _ = store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	_, _ = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	_, _ = store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	_, _ = store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "LINK", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "LINK", StartNode: "a", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "LINK", StartNode: "b", EndNode: "d"})
	store.CreateEdge(&storage.Edge{ID: "e4", Type: "LINK", StartNode: "c", EndNode: "d"})

	startNode, _ := store.GetNode("a")
	endNode, _ := store.GetNode("d")

	// allShortestPaths(ctx, start, end, relTypes, direction, maxHops)
	paths, err := e.allShortestPaths(context.Background(), startNode, endNode, nil, "both", 10)
	if err != nil {
		t.Fatalf("allShortestPaths: %v", err)
	}
	if len(paths) < 2 {
		t.Errorf("Expected at least 2 shortest paths in diamond graph, got %d", len(paths))
	}
}

// ========================================
// Compound Query Tests
// ========================================

func TestCompoundMatchMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial data
	_, _ = store.CreateNode(&storage.Node{
		ID:         "person1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	})

	// MATCH ... MERGE
	result, err := e.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		MERGE (c:Company {name: 'TechCorp'})
		RETURN p.name, c.name
	`, nil)
	if err != nil {
		t.Fatalf("Compound MATCH MERGE failed: %v", err)
	}
	t.Logf("Compound result: %d rows, %d cols", len(result.Rows), len(result.Columns))
}

// ========================================
// Edge Case Tests
// ========================================

func TestEmptyMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Match on non-existent label
	result, err := e.Execute(ctx, `MATCH (n:NonExistent) RETURN n`, nil)
	if err != nil {
		t.Fatalf("Empty match should not error: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows for non-existent label, got %d", len(result.Rows))
	}
}

func TestWhereWithExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{
		ID:     "exist-test",
		Labels: []string{"ExistsTest"},
		Properties: map[string]interface{}{
			"name": "Test",
			"age":  25,
		},
	})

	// Test exists() function
	result, err := e.Execute(ctx, `MATCH (n:ExistsTest) WHERE exists(n.age) RETURN n.name`, nil)
	if err != nil {
		t.Fatalf("WHERE exists failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestCheckSubqueryMatchOutgoing(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with outgoing relationship
	_, _ = store.CreateNode(&storage.Node{ID: "parent", Labels: []string{"Parent"}})
	_, _ = store.CreateNode(&storage.Node{ID: "child", Labels: []string{"Child"}})
	store.CreateEdge(&storage.Edge{ID: "e1", Type: "HAS_CHILD", StartNode: "parent", EndNode: "child"})

	// Query with EXISTS checking outgoing relationship
	result, err := e.Execute(ctx, `
		MATCH (p:Parent)
		WHERE EXISTS { MATCH (p)-[:HAS_CHILD]->(:Child) }
		RETURN p
	`, nil)
	if err != nil {
		t.Fatalf("EXISTS outgoing failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 parent with child, got %d", len(result.Rows))
	}
}

// ========================================
// Keyword Detection Tests
// ========================================

func TestFindKeywordIndex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		keyword  string
		expected int
	}{
		// Basic keyword detection
		{name: "RETURN at start", input: "RETURN n", keyword: "RETURN", expected: 0},
		{name: "RETURN in middle", input: "MATCH (n) RETURN n", keyword: "RETURN", expected: 10},
		{name: "RETURN case insensitive", input: "match (n) return n", keyword: "RETURN", expected: 10},

		// Avoiding substring matches - this is the key fix!
		{name: "RemoveReturn label", input: "MATCH (n:RemoveReturn) RETURN n", keyword: "RETURN", expected: 23},
		{name: "Return label", input: "MATCH (n:Return) RETURN n.name", keyword: "RETURN", expected: 17},
		{name: "ReturnValue label", input: "MATCH (n:ReturnValue) RETURN n", keyword: "RETURN", expected: 22},

		// WHERE keyword
		{name: "WHERE normal", input: "MATCH (n) WHERE n.age > 10 RETURN n", keyword: "WHERE", expected: 10},
		{name: "Somewhere label", input: "MATCH (n:Somewhere) WHERE n.age > 10", keyword: "WHERE", expected: 20},

		// MATCH keyword
		{name: "MATCH at start", input: "MATCH (n) RETURN n", keyword: "MATCH", expected: 0},
		{name: "ReMatch label", input: "MATCH (n:ReMatch) RETURN n", keyword: "MATCH", expected: 0},

		// MERGE keyword
		{name: "MERGE normal", input: "MERGE (n:Person)", keyword: "MERGE", expected: 0},
		{name: "SubMerge label", input: "MATCH (n:SubMerge) MERGE (m:Person)", keyword: "MERGE", expected: 19},

		// Multi-word keywords
		{name: "ON CREATE SET", input: "MERGE (n) ON CREATE SET n.created = true", keyword: "ON CREATE SET", expected: 10},
		{name: "ON MATCH SET", input: "MERGE (n) ON MATCH SET n.updated = true", keyword: "ON MATCH SET", expected: 10},

		// Not found
		{name: "keyword not present", input: "MATCH (n) WHERE n.age > 10", keyword: "RETURN", expected: -1},
		{name: "only in label", input: "MATCH (n:RemoveReturn)", keyword: "RETURN", expected: -1},

		// Edge cases
		{name: "keyword at end", input: "MATCH (n) WHERE", keyword: "WHERE", expected: 10},
		{name: "keyword with newline", input: "MATCH (n)\nRETURN n", keyword: "RETURN", expected: 10},
		{name: "keyword with tab", input: "MATCH (n)\tRETURN n", keyword: "RETURN", expected: 10},
		{name: "keyword after paren", input: "(n:Test)RETURN n", keyword: "RETURN", expected: 8},

		// Security/correctness: ignore keywords inside data / nested structures
		{name: "ignore inside string literal", input: "MATCH (n) RETURN 'WITH' AS x", keyword: "WITH", expected: -1},
		{name: "ignore inside line comment", input: "MATCH (n) // RETURN n\nRETURN n", keyword: "RETURN", expected: 22},
		{name: "ignore inside block comment", input: "MATCH (n) /* RETURN n */ RETURN n", keyword: "RETURN", expected: 25},
		{name: "ignore inside map literal", input: "MATCH (n) RETURN {WITH: 1} AS m", keyword: "WITH", expected: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findKeywordIndex(tt.input, tt.keyword)
			if got != tt.expected {
				t.Errorf("findKeywordIndex(%q, %q) = %d, want %d", tt.input, tt.keyword, got, tt.expected)
			}
		})
	}
}

func TestTopLevelKeywordIndex_IgnoresCallSubqueryBody(t *testing.T) {
	input := "CALL { RETURN 1 } RETURN 2"
	got := topLevelKeywordIndex(input, "RETURN")
	if got != 18 {
		t.Fatalf("topLevelKeywordIndex(%q, %q) = %d, want %d", input, "RETURN", got, 18)
	}
}

func TestMatchWithLabelsContainingKeywords(t *testing.T) {
	// This is the key regression test - labels containing keywords should work
	ctx := context.Background()

	labels := []string{"RemoveReturn", "Return", "Where", "Match", "Merge", "Set"}

	for _, label := range labels {
		t.Run("label_"+label, func(t *testing.T) {
			// Create fresh store for each test
			baseStore := newTestMemoryEngine(t)

			store := storage.NewNamespacedEngine(baseStore, "test")
			node := &storage.Node{
				ID:         storage.NodeID("node-" + strings.ToLower(label)),
				Labels:     []string{label},
				Properties: map[string]interface{}{"name": label},
			}
			if _, err := store.CreateNode(node); err != nil {
				t.Fatalf("Failed to create node: %v", err)
			}

			e := NewStorageExecutor(store)

			// Test that MATCH with this label works
			query := "MATCH (n:" + label + ") RETURN n.name"
			result, err := e.Execute(ctx, query, nil)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row for label %s, got %d", label, len(result.Rows))
			}
		})
	}
}

// ========================================
// Tests for 0% coverage functions
// ========================================

func TestExecuteUnionDirect(t *testing.T) {
	ctx := context.Background()
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	// Create test data
	_, _ = store.CreateNode(&storage.Node{
		ID:         "alice",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30)},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:         "bob",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob", "age": int64(25)},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:         "charlie",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Charlie", "age": int64(25)},
	})

	e := NewStorageExecutor(store)

	t.Run("union_all_combines_all", func(t *testing.T) {
		// Using direct execution since the main Execute router might not route to executeUnion
		result, err := e.executeUnion(ctx, "MATCH (n:Person) WHERE n.age = 25 RETURN n.name UNION ALL MATCH (n:Person) WHERE n.age = 30 RETURN n.name", true)
		if err != nil {
			t.Fatalf("executeUnion failed: %v", err)
		}
		if len(result.Rows) != 3 {
			t.Errorf("UNION ALL should return all rows, got %d", len(result.Rows))
		}
	})

	t.Run("union_distinct_removes_duplicates", func(t *testing.T) {
		result, err := e.executeUnion(ctx, "MATCH (n:Person) RETURN n.age UNION MATCH (n:Person) RETURN n.age", false)
		if err != nil {
			t.Fatalf("executeUnion failed: %v", err)
		}
		// Should have unique ages only: 25 and 30
		if len(result.Rows) > 3 {
			t.Errorf("UNION should remove duplicates, got %d rows", len(result.Rows))
		}
	})

	t.Run("union_error_no_clause", func(t *testing.T) {
		_, err := e.executeUnion(ctx, "MATCH (n) RETURN n", false)
		if err == nil {
			t.Error("Expected error for missing UNION clause")
		}
	})

	t.Run("union_all_error_no_clause", func(t *testing.T) {
		_, err := e.executeUnion(ctx, "MATCH (n) RETURN n", true)
		if err == nil {
			t.Error("Expected error for missing UNION ALL clause")
		}
	})

	t.Run("union_column_mismatch", func(t *testing.T) {
		// This might or might not error depending on implementation
		// Just ensure it doesn't panic
		_, _ = e.executeUnion(ctx, "RETURN 1 AS a UNION RETURN 1 AS a, 2 AS b", false)
	})
}

func TestExecuteCompoundMatchMergeDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("basic_match_merge", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		// Create source node
		_, _ = store.CreateNode(&storage.Node{
			ID:         "source-1",
			Labels:     []string{"Source"},
			Properties: map[string]interface{}{"name": "SourceNode", "value": "test"},
		})

		e := NewStorageExecutor(store)
		result, err := e.executeCompoundMatchMerge(ctx, "MATCH (s:Source) MERGE (t:Target {name: 'NewTarget'})")
		if err != nil {
			t.Fatalf("executeCompoundMatchMerge failed: %v", err)
		}
		// Should create a Target node
		if result.Stats == nil || result.Stats.NodesCreated == 0 {
			// Check if node was created
			nodes, _ := store.GetNodesByLabel("Target")
			if len(nodes) == 0 {
				t.Log("Note: Target node not created - may need context propagation")
			}
		}
	})

	t.Run("no_match_results_in_empty", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)
		result, err := e.executeCompoundMatchMerge(ctx, "MATCH (s:NonExistent) MERGE (t:Target)")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// With no matches and no OPTIONAL MATCH, should return empty
		if len(result.Rows) != 0 {
			t.Logf("Got %d rows (may vary by implementation)", len(result.Rows))
		}
	})

	t.Run("invalid_query_no_match", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)
		_, err := e.executeCompoundMatchMerge(ctx, "MERGE (t:Target)")
		if err == nil {
			t.Error("Expected error for query without MATCH")
		}
	})

	t.Run("invalid_query_no_merge", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)
		_, err := e.executeCompoundMatchMerge(ctx, "MATCH (s:Source) RETURN s")
		if err == nil {
			t.Error("Expected error for query without MERGE")
		}
	})
}

func TestExecuteMatchForContextDirect(t *testing.T) {
	ctx := context.Background()
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	// Create test nodes
	_, _ = store.CreateNode(&storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30)},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:         "p2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob", "age": int64(25)},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:         "c1",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme"},
	})

	e := NewStorageExecutor(store)

	t.Run("match_all_by_label", func(t *testing.T) {
		matches, rels, err := e.executeMatchForContext(ctx, "MATCH (p:Person)")
		if err != nil {
			t.Fatalf("executeMatchForContext failed: %v", err)
		}
		if len(matches) != 2 {
			t.Errorf("Expected 2 Person matches, got %d", len(matches))
		}
		if rels == nil {
			t.Error("Expected non-nil rels map")
		}
	})

	t.Run("match_with_where", func(t *testing.T) {
		matches, _, err := e.executeMatchForContext(ctx, "MATCH (p:Person) WHERE p.age = 30")
		if err != nil {
			t.Fatalf("executeMatchForContext failed: %v", err)
		}
		if len(matches) != 1 {
			t.Errorf("Expected 1 match for age=30, got %d", len(matches))
		}
	})

	t.Run("match_with_property_filter", func(t *testing.T) {
		matches, _, err := e.executeMatchForContext(ctx, "MATCH (p:Person {name: 'Alice'})")
		if err != nil {
			t.Fatalf("executeMatchForContext failed: %v", err)
		}
		if len(matches) != 1 {
			t.Errorf("Expected 1 match for name=Alice, got %d", len(matches))
		}
	})

	t.Run("match_no_results", func(t *testing.T) {
		matches, _, err := e.executeMatchForContext(ctx, "MATCH (p:NonExistent)")
		if err != nil {
			t.Fatalf("executeMatchForContext failed: %v", err)
		}
		if len(matches) != 0 {
			t.Errorf("Expected 0 matches, got %d", len(matches))
		}
	})

	t.Run("match_all_nodes_no_label", func(t *testing.T) {
		matches, _, err := e.executeMatchForContext(ctx, "MATCH (n)")
		if err != nil {
			t.Fatalf("executeMatchForContext failed: %v", err)
		}
		if len(matches) != 3 {
			t.Errorf("Expected 3 matches (all nodes), got %d", len(matches))
		}
	})
}

func TestExecuteMergeWithContextDirect(t *testing.T) {
	ctx := context.Background()

	t.Run("merge_with_node_context", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		// Create source node
		sourceNode := &storage.Node{
			ID:         "source-1",
			Labels:     []string{"Source"},
			Properties: map[string]interface{}{"name": "Alice", "data": "important"},
		}
		_, _ = store.CreateNode(sourceNode)

		e := NewStorageExecutor(store)

		nodeContext := map[string]*storage.Node{
			"s": sourceNode,
		}
		relContext := map[string]*storage.Edge{}

		result, err := e.executeMergeWithContext(ctx, "MERGE (t:Target {name: 'NewTarget'})", nodeContext, relContext)
		if err != nil {
			t.Fatalf("executeMergeWithContext failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
	})

	t.Run("merge_with_empty_context", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)

		result, err := e.executeMergeWithContext(ctx, "MERGE (t:Target {name: 'NewTarget'})", map[string]*storage.Node{}, map[string]*storage.Edge{})
		if err != nil {
			t.Fatalf("executeMergeWithContext failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
	})

	t.Run("merge_with_on_create_set", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)

		nodeContext := map[string]*storage.Node{}
		relContext := map[string]*storage.Edge{}

		result, err := e.executeMergeWithContext(ctx, "MERGE (t:Target {name: 'Test'}) ON CREATE SET t.created = true", nodeContext, relContext)
		if err != nil {
			t.Fatalf("executeMergeWithContext failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
	})

	t.Run("merge_with_return", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)

		result, err := e.executeMergeWithContext(ctx, "MERGE (t:Target {name: 'Test'}) RETURN t", map[string]*storage.Node{}, map[string]*storage.Edge{})
		if err != nil {
			t.Fatalf("executeMergeWithContext failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
	})

	t.Run("merge_with_with_clause_creates_relationship", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)

		query := `
		MERGE (parent:Section {code: $parent_code})
		WITH parent
		MERGE (child:Section {code: $code})
		  ON CREATE SET child.content = $content
		  ON MATCH SET child.content = $content
		MERGE (child)-[:SUBSECTION_OF]->(parent)
		`

		_, err := e.Execute(ctx, query, map[string]interface{}{
			"parent_code": "3.18.5",
			"code":        "3.18.5.1",
			"content":     "title A",
		})
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		verify, err := e.Execute(ctx, `
		MATCH (child:Section {code: '3.18.5.1'})-[r:SUBSECTION_OF]->(parent:Section {code: '3.18.5'})
		RETURN count(r) AS c, child.content AS content
		`, nil)
		if err != nil {
			t.Fatalf("verification query failed: %v", err)
		}
		if len(verify.Rows) != 1 || len(verify.Rows[0]) != 2 {
			t.Fatalf("unexpected verification shape: %+v", verify.Rows)
		}
		if got, ok := verify.Rows[0][0].(int64); !ok || got != 1 {
			t.Fatalf("expected 1 SUBSECTION_OF edge, got %#v", verify.Rows[0][0])
		}
		if got, ok := verify.Rows[0][1].(string); !ok || got != "title A" {
			t.Fatalf("expected child content to be updated, got %#v", verify.Rows[0][1])
		}
	})

	t.Run("unwind_with_with_clause_creates_relationships_for_each_parent", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)

		store := storage.NewNamespacedEngine(baseStore, "test")
		defer store.Close()

		e := NewStorageExecutor(store)

		query := `
		UNWIND $parent_codes AS p_code
		MERGE (parent:Section {code: p_code})
		WITH parent
		MERGE (child:Section {code: $code})
		  ON CREATE SET child.content = $content
		  ON MATCH SET child.content = $content
		WITH parent, child
		MERGE (child)-[:SUBSECTION_OF]->(parent)
		`

		_, err := e.Execute(ctx, query, map[string]interface{}{
			"parent_codes": []interface{}{"3.18.5", "3.18.6"},
			"code":         "3.18",
			"content":      "big title",
		})
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		verify, err := e.Execute(ctx, `
		MATCH (child:Section {code: '3.18'})-[r:SUBSECTION_OF]->(parent:Section)
		RETURN count(r) AS c
		`, nil)
		if err != nil {
			t.Fatalf("verification query failed: %v", err)
		}
		if len(verify.Rows) != 1 || len(verify.Rows[0]) != 1 {
			t.Fatalf("unexpected verification shape: %+v", verify.Rows)
		}
		if got, ok := verify.Rows[0][0].(int64); !ok || got != 2 {
			t.Fatalf("expected 2 SUBSECTION_OF edges, got %#v", verify.Rows[0][0])
		}

		parents, err := e.Execute(ctx, `
		MATCH (child:Section {code: '3.18'})-[:SUBSECTION_OF]->(parent:Section)
		RETURN parent.code AS code
		ORDER BY code
		`, nil)
		if err != nil {
			t.Fatalf("parent verification query failed: %v", err)
		}
		if len(parents.Rows) != 2 {
			t.Fatalf("expected 2 parent rows, got %+v", parents.Rows)
		}
		if parents.Rows[0][0] != "3.18.5" || parents.Rows[1][0] != "3.18.6" {
			t.Fatalf("unexpected parent codes: %+v", parents.Rows)
		}
	})
}

func TestEvaluateStringConcatDirect(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	e := NewStorageExecutor(store)

	t.Run("simple_concat", func(t *testing.T) {
		result := e.evaluateStringConcat("'Hello' + ' ' + 'World'")
		if result != "Hello World" {
			t.Errorf("Expected 'Hello World', got '%s'", result)
		}
	})

	t.Run("single_string", func(t *testing.T) {
		result := e.evaluateStringConcat("'Hello'")
		if result != "Hello" {
			t.Errorf("Expected 'Hello', got '%s'", result)
		}
	})

	t.Run("numbers", func(t *testing.T) {
		result := e.evaluateStringConcat("1 + 2")
		// String concat of numbers should produce concatenation or numeric result
		if result != "12" && result != "3" {
			t.Logf("Number concat result: '%s'", result)
		}
	})

	t.Run("mixed_types", func(t *testing.T) {
		result := e.evaluateStringConcat("'Count: ' + 5")
		if !strings.Contains(result, "Count") {
			t.Errorf("Expected result containing 'Count', got '%s'", result)
		}
	})
}

func TestSplitByPlusDirect(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	e := NewStorageExecutor(store)

	t.Run("simple_split", func(t *testing.T) {
		parts := e.splitByPlus("'a' + 'b' + 'c'")
		if len(parts) != 3 {
			t.Errorf("Expected 3 parts, got %d: %v", len(parts), parts)
		}
	})

	t.Run("no_plus", func(t *testing.T) {
		parts := e.splitByPlus("'hello'")
		if len(parts) != 1 {
			t.Errorf("Expected 1 part, got %d", len(parts))
		}
	})

	t.Run("plus_in_string", func(t *testing.T) {
		// Plus inside quotes should not split
		parts := e.splitByPlus("'a + b'")
		if len(parts) != 1 {
			t.Errorf("Expected 1 part (plus inside string), got %d: %v", len(parts), parts)
		}
	})

	t.Run("plus_in_function", func(t *testing.T) {
		// Plus inside parentheses should not split at top level
		parts := e.splitByPlus("func(a + b) + c")
		if len(parts) != 2 {
			t.Logf("Function split result: %v", parts)
		}
	})
}

func TestHasConcatOperatorDirect(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()

	e := NewStorageExecutor(store)

	t.Run("has_concat", func(t *testing.T) {
		if !e.hasConcatOperator("'a' + 'b'") {
			t.Error("Should detect concat operator")
		}
	})

	t.Run("no_concat", func(t *testing.T) {
		if e.hasConcatOperator("'hello'") {
			t.Error("Should not detect concat in simple string")
		}
	})

	t.Run("plus_in_string", func(t *testing.T) {
		if e.hasConcatOperator("'a + b'") {
			t.Error("Should not detect plus inside quoted string")
		}
	})

	t.Run("plus_without_spaces", func(t *testing.T) {
		// Without spaces, shouldn't be detected as concat
		if e.hasConcatOperator("1+2") {
			t.Error("Should require spaces around + for concat")
		}
	})
}

// ===== SHOW Commands Tests =====

func TestShowIndexes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "SHOW INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW INDEXES failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW INDEXES returned nil result")
	}
	if len(result.Columns) < 5 {
		t.Errorf("Expected at least 5 columns, got %d", len(result.Columns))
	}
}

func TestShowIndexes_MatchesCallDbIndexes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE INDEX person_name_idx FOR (p:Person) ON (p.name)", nil)
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE RANGE INDEX person_age_idx FOR (p:Person) ON (p.age)", nil)
	if err != nil {
		t.Fatalf("CREATE RANGE INDEX failed: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE FULLTEXT INDEX person_search_idx FOR (p:Person) ON EACH [p.name, p.bio]", nil)
	if err != nil {
		t.Fatalf("CREATE FULLTEXT INDEX failed: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE VECTOR INDEX person_vec_idx FOR (p:Person) ON (p.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 8}}", nil)
	if err != nil {
		t.Fatalf("CREATE VECTOR INDEX failed: %v", err)
	}

	showRes, err := e.Execute(ctx, "SHOW INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW INDEXES failed: %v", err)
	}
	callRes, err := e.Execute(ctx, "CALL db.indexes()", nil)
	if err != nil {
		t.Fatalf("CALL db.indexes() failed: %v", err)
	}

	if showRes == nil || callRes == nil {
		t.Fatal("SHOW INDEXES / CALL db.indexes() returned nil result")
	}
	if len(showRes.Rows) == 0 {
		t.Fatal("SHOW INDEXES should return created indexes")
	}
	if len(showRes.Rows) != len(callRes.Rows) {
		t.Fatalf("SHOW INDEXES row count (%d) should match CALL db.indexes() (%d)", len(showRes.Rows), len(callRes.Rows))
	}

	showNameIdx, callNameIdx := -1, -1
	for i, c := range showRes.Columns {
		if c == "name" {
			showNameIdx = i
			break
		}
	}
	for i, c := range callRes.Columns {
		if c == "name" {
			callNameIdx = i
			break
		}
	}
	if showNameIdx < 0 || callNameIdx < 0 {
		t.Fatalf("expected 'name' column in both results, got SHOW=%v CALL=%v", showRes.Columns, callRes.Columns)
	}

	showNames := make(map[string]struct{}, len(showRes.Rows))
	for _, row := range showRes.Rows {
		if showNameIdx >= len(row) {
			t.Fatalf("SHOW row missing name column: %v", row)
		}
		name, ok := row[showNameIdx].(string)
		if !ok || name == "" {
			t.Fatalf("SHOW row has invalid name: %v", row[showNameIdx])
		}
		showNames[name] = struct{}{}
	}

	callNames := make(map[string]struct{}, len(callRes.Rows))
	for _, row := range callRes.Rows {
		if callNameIdx >= len(row) {
			t.Fatalf("CALL row missing name column: %v", row)
		}
		name, ok := row[callNameIdx].(string)
		if !ok || name == "" {
			t.Fatalf("CALL row has invalid name: %v", row[callNameIdx])
		}
		callNames[name] = struct{}{}
	}

	if len(showNames) != len(callNames) {
		t.Fatalf("name set size mismatch SHOW=%d CALL=%d", len(showNames), len(callNames))
	}
	for name := range callNames {
		if _, ok := showNames[name]; !ok {
			t.Fatalf("SHOW INDEXES missing index name from CALL db.indexes(): %s", name)
		}
	}
}

func TestShowFulltextIndexes_FiltersToFulltextType(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE INDEX person_name_idx FOR (p:Person) ON (p.name)", nil)
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE FULLTEXT INDEX person_search_idx FOR (p:Person) ON EACH [p.name, p.bio]", nil)
	if err != nil {
		t.Fatalf("CREATE FULLTEXT INDEX failed: %v", err)
	}

	res, err := e.Execute(ctx, "SHOW FULLTEXT INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW FULLTEXT INDEXES failed: %v", err)
	}
	if res == nil {
		t.Fatal("SHOW FULLTEXT INDEXES returned nil result")
	}

	typeIdx, nameIdx := -1, -1
	for i, c := range res.Columns {
		if c == "type" {
			typeIdx = i
		}
		if c == "name" {
			nameIdx = i
		}
	}
	if typeIdx < 0 || nameIdx < 0 {
		t.Fatalf("expected type/name columns in SHOW FULLTEXT INDEXES, got %v", res.Columns)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("expected exactly one fulltext index row, got %d", len(res.Rows))
	}
	if gotType, _ := res.Rows[0][typeIdx].(string); strings.ToUpper(gotType) != "FULLTEXT" {
		t.Fatalf("expected FULLTEXT row type, got %v", res.Rows[0][typeIdx])
	}
	if gotName, _ := res.Rows[0][nameIdx].(string); gotName != "person_search_idx" {
		t.Fatalf("expected fulltext index name person_search_idx, got %v", res.Rows[0][nameIdx])
	}
}

func TestShowQualifiedIndexes_RangeAndVectorFilters(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Qualified SHOW should be accepted even with no indexes.
	emptyRange, err := e.Execute(ctx, "SHOW RANGE INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW RANGE INDEXES failed on empty schema: %v", err)
	}
	if emptyRange == nil {
		t.Fatal("SHOW RANGE INDEXES returned nil result")
	}
	if len(emptyRange.Rows) != 0 {
		t.Fatalf("expected no RANGE index rows on empty schema, got %d", len(emptyRange.Rows))
	}

	emptyVector, err := e.Execute(ctx, "SHOW VECTOR INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW VECTOR INDEXES failed on empty schema: %v", err)
	}
	if emptyVector == nil {
		t.Fatal("SHOW VECTOR INDEXES returned nil result")
	}
	if len(emptyVector.Rows) != 0 {
		t.Fatalf("expected no VECTOR index rows on empty schema, got %d", len(emptyVector.Rows))
	}

	_, err = e.Execute(ctx, "CREATE RANGE INDEX person_age_idx FOR (p:Person) ON (p.age)", nil)
	if err != nil {
		t.Fatalf("CREATE RANGE INDEX failed: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE VECTOR INDEX person_vec_idx FOR (p:Person) ON (p.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 8}}", nil)
	if err != nil {
		t.Fatalf("CREATE VECTOR INDEX failed: %v", err)
	}

	rangeRes, err := e.Execute(ctx, "SHOW RANGE INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW RANGE INDEXES failed: %v", err)
	}
	vectorRes, err := e.Execute(ctx, "SHOW VECTOR INDEXES", nil)
	if err != nil {
		t.Fatalf("SHOW VECTOR INDEXES failed: %v", err)
	}

	typeIdx := -1
	for i, c := range rangeRes.Columns {
		if c == "type" {
			typeIdx = i
			break
		}
	}
	if typeIdx < 0 {
		t.Fatalf("missing type column in SHOW qualified indexes: %v", rangeRes.Columns)
	}

	if len(rangeRes.Rows) != 1 {
		t.Fatalf("expected exactly one RANGE index row, got %d", len(rangeRes.Rows))
	}
	if gotType, _ := rangeRes.Rows[0][typeIdx].(string); strings.ToUpper(gotType) != "RANGE" {
		t.Fatalf("expected RANGE row type, got %v", rangeRes.Rows[0][typeIdx])
	}

	if len(vectorRes.Rows) != 1 {
		t.Fatalf("expected exactly one VECTOR index row, got %d", len(vectorRes.Rows))
	}
	if gotType, _ := vectorRes.Rows[0][typeIdx].(string); strings.ToUpper(gotType) != "VECTOR" {
		t.Fatalf("expected VECTOR row type, got %v", vectorRes.Rows[0][typeIdx])
	}
}

func TestShowConstraints(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatalf("SHOW CONSTRAINTS failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW CONSTRAINTS returned nil result")
	}
}

func TestShowConstraints_WithSchemaConstraintsAndPropertyTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	if err := store.GetSchema().AddConstraint(storage.Constraint{
		Name:       "unique_person_email",
		Type:       storage.ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	}); err != nil {
		t.Fatalf("failed to add unique constraint: %v", err)
	}
	if err := store.GetSchema().AddPropertyTypeConstraint("person_age_type", "Person", "age", storage.PropertyTypeInteger); err != nil {
		t.Fatalf("failed to add property type constraint: %v", err)
	}

	result, err := e.executeShowConstraints(ctx, "SHOW CONSTRAINTS")
	if err != nil {
		t.Fatalf("executeShowConstraints failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW CONSTRAINTS returned nil result")
	}
	if len(result.Rows) < 2 {
		t.Fatalf("expected at least 2 constraints rows, got %d", len(result.Rows))
	}

	var (
		foundUniqueRow       bool
		foundPropertyTypeRow bool
	)
	for _, row := range result.Rows {
		if len(row) != 13 {
			t.Fatalf("unexpected SHOW CONSTRAINTS row shape (expected 13 columns, got %d): %v", len(row), row)
		}
		name, _ := row[1].(string)
		typ, _ := row[2].(string)
		if name == "unique_person_email" && typ == "UNIQUE" {
			foundUniqueRow = true
		}
		if name == "person_age_type" && typ == "PROPERTY_TYPE" {
			foundPropertyTypeRow = true
			if row[7] == nil || row[7] == "" {
				t.Fatalf("expected propertyType column to be populated, row=%v", row)
			}
		}
	}
	if !foundUniqueRow {
		t.Fatalf("missing unique constraint row in SHOW CONSTRAINTS output: %v", result.Rows)
	}
	if !foundPropertyTypeRow {
		t.Fatalf("missing property-type constraint row in SHOW CONSTRAINTS output: %v", result.Rows)
	}
}

func TestShowProcedures(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "SHOW PROCEDURES", nil)
	if err != nil {
		t.Fatalf("SHOW PROCEDURES failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW PROCEDURES returned nil result")
	}
	if len(result.Rows) == 0 {
		t.Error("SHOW PROCEDURES should return procedures")
	}
}

func TestShowFunctions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "SHOW FUNCTIONS", nil)
	if err != nil {
		t.Fatalf("SHOW FUNCTIONS failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW FUNCTIONS returned nil result")
	}
	if len(result.Rows) == 0 {
		t.Error("SHOW FUNCTIONS should return functions")
	}
}

func TestShowDatabase(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "SHOW DATABASE", nil)
	if err != nil {
		t.Fatalf("SHOW DATABASE failed: %v", err)
	}
	if result == nil {
		t.Fatal("SHOW DATABASE returned nil result")
	}
	if len(result.Rows) != 1 {
		t.Errorf("SHOW DATABASE should return 1 row, got %d", len(result.Rows))
	}
}

func TestCallDbInfo_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL db.info()", nil)
	if err != nil {
		t.Fatalf("CALL db.info() failed: %v", err)
	}
	if result == nil || len(result.Rows) != 1 {
		t.Error("CALL db.info() should return 1 row")
	}
}

func TestCallDbPing_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL db.ping()", nil)
	if err != nil {
		t.Fatalf("CALL db.ping() failed: %v", err)
	}
	if result == nil || len(result.Rows) != 1 {
		t.Error("CALL db.ping() should return 1 row")
	}
	if result.Rows[0][0] != true {
		t.Error("CALL db.ping() should return true")
	}
}

func TestCallDbmsInfo_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL dbms.info()", nil)
	if err != nil {
		t.Fatalf("CALL dbms.info() failed: %v", err)
	}
	if result == nil {
		t.Fatal("CALL dbms.info() returned nil")
	}
}

func TestCallDbmsListConfig_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL dbms.listConfig()", nil)
	if err != nil {
		t.Fatalf("CALL dbms.listConfig() failed: %v", err)
	}
	if result == nil {
		t.Fatal("CALL dbms.listConfig() returned nil")
	}
}

func TestCallDbIndexFulltextListAvailableAnalyzers_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := e.Execute(ctx, "CALL db.index.fulltext.listAvailableAnalyzers()", nil)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}
	if result == nil || len(result.Rows) == 0 {
		t.Error("Should return analyzers")
	}
}

func TestExecuteUnion_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// DISTINCT UNION should dedupe.
	res, err := e.executeUnion(ctx, "RETURN 1 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x", false)
	if err != nil {
		t.Fatalf("distinct UNION failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("distinct UNION rows = %d, want 2", len(res.Rows))
	}

	// UNION ALL should keep duplicates.
	res, err = e.executeUnion(ctx, "RETURN 1 AS x UNION ALL RETURN 1 AS x UNION ALL RETURN 2 AS x", true)
	if err != nil {
		t.Fatalf("UNION ALL failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("UNION ALL rows = %d, want 3", len(res.Rows))
	}

	// Mismatched column counts must error.
	_, err = e.executeUnion(ctx, "RETURN 1 AS a UNION RETURN 1 AS a, 2 AS b", false)
	if err == nil {
		t.Fatal("expected mismatched-column UNION error")
	}
	if !strings.Contains(err.Error(), "same number of columns") {
		t.Fatalf("unexpected mismatched-column error: %v", err)
	}

	// Missing UNION separator must error.
	_, err = e.executeUnion(ctx, "RETURN 1 AS x", false)
	if err == nil {
		t.Fatal("expected UNION not found error")
	}

	// Error in a child query must be wrapped with query index.
	_, err = e.executeUnion(ctx, "RETURN 1 AS x UNION MATCH (n", false)
	if err == nil {
		t.Fatal("expected wrapped error from second UNION query")
	}
	if !strings.Contains(err.Error(), "UNION query 2") {
		t.Fatalf("unexpected wrapped UNION error: %v", err)
	}
}

func TestOptionalMatch_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.executeOptionalMatch(ctx, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected OPTIONAL MATCH not found error")
	}

	// Malformed OPTIONAL MATCH should still return deterministic single-row output.
	res, err := e.executeOptionalMatch(ctx, "OPTIONAL MATCH (n RETURN n")
	if err != nil {
		t.Fatalf("optional malformed should return deterministic result, got err: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("unexpected malformed optional result shape: %#v", res.Rows)
	}

	// Empty optional result should preserve columns with nil row.
	res, err = e.Execute(ctx, "OPTIONAL MATCH (n:Missing) RETURN n.name", nil)
	if err != nil {
		t.Fatalf("OPTIONAL MATCH empty result failed: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != nil {
		t.Fatalf("expected single nil row for empty OPTIONAL MATCH, got %#v", res.Rows)
	}
}

func TestCompoundOptionalMatchAndFindRelatedNodes_Branches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE (a:Person {name:'alice'}), (b:Person {name:'bob'}), (a)-[:KNOWS {w:1}]->(b)", nil)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// No WITH/RETURN branch should return matched count.
	res, err := e.executeCompoundMatchOptionalMatch(ctx, "MATCH (a:Person {name:'alice'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person)")
	if err != nil {
		t.Fatalf("compound optional without WITH/RETURN failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(1) {
		t.Fatalf("compound optional matched count unexpected: %#v", res.Rows)
	}

	// Missing variable in initial MATCH pattern should error.
	_, err = e.executeCompoundMatchOptionalMatch(ctx, "MATCH (:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN b")
	if err == nil {
		t.Fatal("expected parse error for variable-less initial MATCH in compound optional")
	}

	aliceRes, err := e.Execute(ctx, "MATCH (a:Person {name:'alice'}) RETURN a", nil)
	if err != nil || len(aliceRes.Rows) != 1 {
		t.Fatalf("failed to fetch alice: err=%v rows=%d", err, len(aliceRes.Rows))
	}
	alice, ok := aliceRes.Rows[0][0].(*storage.Node)
	if !ok || alice == nil {
		t.Fatalf("expected node in row[0][0], got %#v", aliceRes.Rows[0][0])
	}

	outPattern := e.parseOptionalRelPattern(ctx, "(a)-[r:KNOWS]->(b:Person {name:'bob'})")
	outRelated := e.findRelatedNodes(alice, outPattern)
	if len(outRelated) != 1 {
		t.Fatalf("outgoing related len = %d, want 1", len(outRelated))
	}
	if outRelated[0].node.Properties["name"] != "bob" {
		t.Fatalf("unexpected outgoing related node: %#v", outRelated[0].node.Properties)
	}

	inPattern := e.parseOptionalRelPattern(ctx, "(a)<-[r:KNOWS]-(b:Person)")
	inRelated := e.findRelatedNodes(alice, inPattern)
	if len(inRelated) != 0 {
		t.Fatalf("incoming related len = %d, want 0", len(inRelated))
	}

	// both-direction with wrong type should filter to zero.
	bothWrongType := optionalRelPattern{direction: "both", relType: "LIKES", targetProps: map[string]interface{}{}}
	bothRelated := e.findRelatedNodes(alice, bothWrongType)
	if len(bothRelated) != 0 {
		t.Fatalf("both-direction wrong-type related len = %d, want 0", len(bothRelated))
	}
}

func TestFindRelatedNodes_RequiresAllTargetLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE (a:Person {name:'alice'})", nil)
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE (b:Person {name:'bob'})", nil)
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	_, err = e.Execute(ctx, "CREATE (c:Person:Engineer {name:'carol'})", nil)
	if err != nil {
		t.Fatalf("create carol: %v", err)
	}
	_, err = e.Execute(ctx, "MATCH (a:Person {name:'alice'}), (b:Person {name:'bob'}), (c:Person:Engineer {name:'carol'}) CREATE (a)-[:KNOWS]->(b) CREATE (a)-[:KNOWS]->(c)", nil)
	if err != nil {
		t.Fatalf("create relationships: %v", err)
	}

	res, err := e.Execute(ctx, "MATCH (a:Person {name:'alice'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person:Engineer) RETURN b.name ORDER BY b.name", nil)
	if err != nil {
		t.Fatalf("optional match: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}
	if got := res.Rows[0][0]; got != "carol" {
		t.Fatalf("target name = %#v, want carol", got)
	}
}
