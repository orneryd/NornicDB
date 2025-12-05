package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCreateUniqueConstraint(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create unique constraint
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	// Verify constraint exists
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(constraints))
	}
	if constraints[0].Label != "Node" || constraints[0].Property != "id" {
		t.Errorf("Unexpected constraint: Label=%s, Property=%s", constraints[0].Label, constraints[0].Property)
	}

	// Test constraint enforcement - first node should succeed
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1', name: 'Test'})", nil)
	if err != nil {
		t.Fatalf("Failed to create first node: %v", err)
	}

	// Second node with same ID should fail
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1', name: 'Test2'})", nil)
	if err == nil {
		t.Fatal("Expected constraint violation, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "constraint violation") {
		t.Errorf("Expected constraint violation error, got: %v", err)
	}
}

func TestCreateIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create property index
	_, err := exec.Execute(ctx, "CREATE INDEX node_type IF NOT EXISTS FOR (n:Node) ON (n.type)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestCreateFulltextIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create fulltext index
	query := "CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.properties]"
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}

	idx := indexes[0].(map[string]interface{})
	if idx["type"] != "FULLTEXT" {
		t.Errorf("Expected FULLTEXT index, got: %v", idx["type"])
	}
}

func TestCreateVectorIndex(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create vector index
	query := `CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS
		FOR (n:Node) ON (n.embedding)
		OPTIONS {indexConfig: {` + "`vector.dimensions`" + `: 1024}}`
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create vector index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestMimirInitialization(t *testing.T) {
	// Test the actual Mimir initialization queries
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Unique constraint on node IDs
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_id_unique constraint: %v", err)
	}

	// Full-text search index
	_, err = exec.Execute(ctx, `CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.properties]`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_search index: %v", err)
	}

	// Type index for fast filtering
	_, err = exec.Execute(ctx, `CREATE INDEX node_type IF NOT EXISTS FOR (n:Node) ON (n.type)`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_type index: %v", err)
	}

	// Vector index
	_, err = exec.Execute(ctx, `CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`+"`vector.dimensions`"+`: 1024}}`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_embedding_index: %v", err)
	}

	// Verify all schemas created
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(constraints))
	}

	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(indexes))
	}

	// Test that constraint works
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1'})", nil)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Duplicate should fail
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1'})", nil)
	if err == nil {
		t.Fatal("Expected constraint violation for duplicate ID")
	}
}

func TestConstraintWithoutName(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint without explicit name
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	// Verify constraint exists with generated name
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(constraints))
	}
}

func TestIndexWithoutName(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create index without explicit name
	_, err := exec.Execute(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestIdempotentSchemaCreation(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint twice - should not error with IF NOT EXISTS
	query := "CREATE CONSTRAINT test_constraint IF NOT EXISTS FOR (n:Test) REQUIRE n.id IS UNIQUE"

	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("First constraint creation failed: %v", err)
	}

	_, err = exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Second constraint creation failed: %v", err)
	}

	// Should still have only one constraint
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(constraints))
	}
}

func TestSchemaErrorCases(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("InvalidConstraintSyntax", func(t *testing.T) {
		// Missing REQUIRE clause
		_, err := exec.Execute(ctx, "CREATE CONSTRAINT test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidIndexSyntax", func(t *testing.T) {
		// Missing ON clause
		_, err := exec.Execute(ctx, "CREATE INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidFulltextSyntax", func(t *testing.T) {
		// Missing ON EACH clause
		_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidVectorSyntax", func(t *testing.T) {
		// Missing ON clause
		_, err := exec.Execute(ctx, "CREATE VECTOR INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})
}

func TestVectorIndexWithDifferentOptions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name      string
		query     string
		wantDims  int
		wantSimFn string
	}{
		{
			name:      "WithOptions",
			query:     "CREATE VECTOR INDEX vec1 FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 512, `vector.similarity_function`: 'euclidean'}}",
			wantDims:  512,
			wantSimFn: "euclidean",
		},
		{
			name:      "DefaultOptions",
			query:     "CREATE VECTOR INDEX vec2 FOR (n:Node) ON (n.vec)",
			wantDims:  1024,     // default
			wantSimFn: "cosine", // default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Failed to create vector index: %v", err)
			}
		})
	}

	// Verify both were created
	indexes := store.GetSchema().GetIndexes()
	vectorCount := 0
	for _, idx := range indexes {
		m := idx.(map[string]interface{})
		if m["type"] == "VECTOR" {
			vectorCount++
		}
	}
	if vectorCount != 2 {
		t.Errorf("Expected 2 vector indexes, got %d", vectorCount)
	}
}

func TestFulltextIndexMultipleProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Index with multiple properties
	query := "CREATE FULLTEXT INDEX multi_search FOR (n:Document) ON EACH [n.title, n.content, n.description]"
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index with multiple properties: %v", err)
	}

	idx, exists := store.GetSchema().GetFulltextIndex("multi_search")
	if !exists {
		t.Fatal("Index not found")
	}

	if len(idx.Properties) != 3 {
		t.Errorf("Expected 3 properties, got %d", len(idx.Properties))
	}

	expectedProps := map[string]bool{"title": true, "content": true, "description": true}
	for _, prop := range idx.Properties {
		if !expectedProps[prop] {
			t.Errorf("Unexpected property: %s", prop)
		}
	}
}

func TestConstraintEnforcementMultipleProperties(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraints on different properties
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_email FOR (n:User) REQUIRE n.email IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create email constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE CONSTRAINT user_username FOR (n:User) REQUIRE n.username IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create username constraint: %v", err)
	}

	// Create user with both properties
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'test@example.com', username: 'testuser'})", nil)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Duplicate email should fail
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'test@example.com', username: 'different'})", nil)
	if err == nil {
		t.Error("Expected constraint violation for duplicate email")
	}

	// Duplicate username should fail
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'different@example.com', username: 'testuser'})", nil)
	if err == nil {
		t.Error("Expected constraint violation for duplicate username")
	}

	// Both different should succeed
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'another@example.com', username: 'anotheruser'})", nil)
	if err != nil {
		t.Errorf("Unexpected error for unique values: %v", err)
	}
}

// TestSchemaCommandsNoOp tests that schema commands don't error (they're no-ops)
func TestSchemaCommandExecution(t *testing.T) {
	store := storage.NewMemoryEngine()
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// These should all execute without error as no-ops
	tests := []struct {
		name  string
		query string
	}{
		{"constraint_neo4j5", "CREATE CONSTRAINT test IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE"},
		{"constraint_neo4j4", "CREATE CONSTRAINT IF NOT EXISTS ON (n:Node) ASSERT n.id IS UNIQUE"},
		{"index", "CREATE INDEX test_idx IF NOT EXISTS FOR (n:Node) ON (n.type)"},
		{"fulltext_index", "CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.content]"},
		{"vector_index", "CREATE VECTOR INDEX emb_idx IF NOT EXISTS FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1024}}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Errorf("%s failed: %v", tt.name, err)
			}
		})
	}
}
