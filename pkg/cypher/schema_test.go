package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type nilSchemaEngine struct {
	storage.Engine
}

func (n *nilSchemaEngine) GetSchema() *storage.SchemaManager { return nil }

func TestCreateUniqueConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create property index
	_, err := exec.Execute(ctx, "CREATE INDEX node_type IF NOT EXISTS FOR (n:Node) ON (n.type)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestCreateFulltextIndex(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create fulltext index
	query := "CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.properties]"
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index: %v", err)
	}

	// Verify index exists
	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}

	idx := indexes[0].(map[string]interface{})
	if idx["type"] != "FULLTEXT" {
		t.Errorf("Expected FULLTEXT index, got: %v", idx["type"])
	}
}

func TestCreateVectorIndex(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestSchemaInitialization(t *testing.T) {
	// Test the actual schema initialization queries
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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

	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) != 4 {
		t.Errorf("Expected 4 indexes including the constraint-owned index, got %d", len(indexes))
	}
	foundOwned := false
	for _, index := range indexes {
		entry := index.(map[string]interface{})
		if entry["name"] == "node_id_unique" {
			foundOwned = true
			if entry["type"] != "RANGE" || entry["owningConstraint"] != "node_id_unique" {
				t.Errorf("Unexpected owned index metadata: %v", entry)
			}
		}
	}
	if !foundOwned {
		t.Error("Expected a range index owned by node_id_unique")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create index without explicit name
	_, err := exec.Execute(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestIdempotentSchemaCreation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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

	t.Run("FulltextNoPropertiesFound", func(t *testing.T) {
		// Property token missing "n." should parse as zero extracted properties.
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX bad_ft FOR (n:Node) ON EACH [content]")
		if err == nil || !strings.Contains(err.Error(), "no properties found in fulltext index definition") {
			t.Fatalf("expected no properties error, got: %v", err)
		}
	})

	t.Run("DuplicateFulltextAndVectorIndex", func(t *testing.T) {
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Node) ON EACH [n.content]")
		if err != nil {
			t.Fatalf("failed to create baseline fulltext index: %v", err)
		}
		_, err = exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Node) ON EACH [n.content]")
		if err != nil {
			t.Fatalf("duplicate fulltext index should be idempotent, got error: %v", err)
		}

		_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX dup_vec FOR (n:Node) ON (n.embedding)")
		if err != nil {
			t.Fatalf("failed to create baseline vector index: %v", err)
		}
		_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX dup_vec FOR (n:Node) ON (n.embedding)")
		if err != nil {
			t.Fatalf("duplicate vector index should be idempotent, got error: %v", err)
		}
	})

	t.Run("FulltextRelationshipIndexSupportsMultipleTypes", func(t *testing.T) {
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX rel_ft_multi FOR ()-[r:OWNS|MANAGES]-() ON EACH [r.note, r.summary]")
		require.NoError(t, err)

		idx, ok := store.GetSchema().GetFulltextIndex("rel_ft_multi")
		require.True(t, ok)
		require.Equal(t, []string{"OWNS", "MANAGES"}, idx.RelationshipTypes)
		require.Equal(t, []string{"note", "summary"}, idx.Properties)
	})

	t.Run("FulltextRelationshipIndexRejectsInvalidTypeList", func(t *testing.T) {
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX rel_ft_bad FOR ()-[r:OWNS|]-() ON EACH [r.note]")
		require.Error(t, err)
		require.True(t, errors.Is(err, nerrors.ErrInvalidFulltextRelationshipTypes))
	})
}

func TestCreateExistsConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL", nil)
	if err != nil {
		t.Fatalf("Failed to create EXISTS constraint: %v", err)
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(allConstraints))
	}
	if allConstraints[0].Type != storage.ConstraintExists {
		t.Fatalf("Expected EXISTS constraint, got %s", allConstraints[0].Type)
	}

	_, err = exec.Execute(ctx, "CREATE (:Person {age: 42})", nil)
	if err == nil {
		t.Fatal("Expected EXISTS constraint violation, got nil")
	}
}

func TestCreateNodeKeyConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY", nil)
	if err != nil {
		t.Fatalf("Failed to create NODE KEY constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {username: 'alice', domain: 'example.com'})", nil)
	if err != nil {
		t.Fatalf("Failed to create first user: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {username: 'alice', domain: 'example.com'})", nil)
	if err == nil {
		t.Fatal("Expected NODE KEY constraint violation, got nil")
	}
}

func TestCanonicalBootstrapFactVersionConstraints(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		"CREATE CONSTRAINT fact_version_valid_from_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.valid_from IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_valid_to_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.valid_to IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_asserted_at_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.asserted_at IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_fact_key_valid_from_node_key IF NOT EXISTS FOR (n:FactVersion) REQUIRE (n.fact_key, n.valid_from) IS NODE KEY",
	}

	for _, query := range queries {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			t.Fatalf("Failed to execute canonical bootstrap constraint query %q: %v", query, err)
		}
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 1 {
		t.Fatalf("Expected 1 standard constraint (NODE KEY), got %d", len(allConstraints))
	}

	if allConstraints[0].Type != storage.ConstraintNodeKey {
		t.Fatalf("Expected NODE KEY constraint, got %s", allConstraints[0].Type)
	}

	propertyTypeConstraints := store.GetSchema().GetAllPropertyTypeConstraints()
	if len(propertyTypeConstraints) != 3 {
		t.Fatalf("Expected 3 property type constraints, got %d", len(propertyTypeConstraints))
	}
}

func TestCreateTemporalConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT fact_temporal IF NOT EXISTS FOR (v:FactVersion) REQUIRE (v.fact_key, v.valid_from, v.valid_to) IS TEMPORAL NO OVERLAP", nil)
	if err != nil {
		t.Fatalf("Failed to create TEMPORAL constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-01-01T00:00:00Z'), valid_to: datetime('2024-02-01T00:00:00Z')})", nil)
	if err != nil {
		t.Fatalf("Failed to create first FactVersion: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-01-15T00:00:00Z'), valid_to: datetime('2024-03-01T00:00:00Z')})", nil)
	if err == nil {
		t.Fatal("Expected TEMPORAL constraint violation, got nil")
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-02-01T00:00:00Z'), valid_to: datetime('2024-03-01T00:00:00Z')})", nil)
	if err != nil {
		t.Fatalf("Expected non-overlapping FactVersion to succeed: %v", err)
	}
}

func TestCreateTypeConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_age_type IF NOT EXISTS FOR (u:User) REQUIRE u.age IS :: INTEGER", nil)
	if err != nil {
		t.Fatalf("Failed to create type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {age: 30})", nil)
	if err != nil {
		t.Fatalf("Expected valid integer, got error: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {age: 'thirty'})", nil)
	if err == nil {
		t.Fatal("Expected type constraint violation, got nil")
	}
}

func TestCreateTypeConstraintWithTypedKeyword(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT event_ts_type IF NOT EXISTS FOR (e:Event) REQUIRE e.ts IS TYPED ZONED DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create type constraint with TYPED syntax: %v", err)
	}
}

func TestTemporalTypeConstraintSemantics_ZonedVsLocal(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT event_ts_zoned IF NOT EXISTS FOR (e:Event) REQUIRE e.ts IS :: ZONED DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create zoned datetime type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Event {ts: datetime('2025-11-27T10:30:00Z')})", nil)
	if err != nil {
		t.Fatalf("Expected zoned datetime value to satisfy zoned constraint, got: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Event {ts: localdatetime()})", nil)
	if err == nil {
		t.Fatal("Expected localdatetime() to violate ZONED DATETIME constraint")
	}

	_, err = exec.Execute(ctx, "CREATE CONSTRAINT meeting_local_type IF NOT EXISTS FOR (m:Meeting) REQUIRE m.start IS :: LOCAL DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create local datetime type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Meeting {start: localdatetime()})", nil)
	if err != nil {
		t.Fatalf("Expected localdatetime() to satisfy LOCAL DATETIME constraint, got: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Meeting {start: datetime('2025-11-27T10:30:00Z')})", nil)
	if err == nil {
		t.Fatal("Expected zoned datetime to violate LOCAL DATETIME constraint")
	}
}

func TestDropConstraint(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT drop_me IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "DROP CONSTRAINT drop_me", nil)
	if err != nil {
		t.Fatalf("Failed to drop constraint: %v", err)
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 0 {
		t.Fatalf("Expected 0 constraints after drop, got %d", len(allConstraints))
	}
}

func TestVectorIndexWithDifferentOptions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
			wantDims:  -1,       // executor default
			wantSimFn: "cosine", // default
		},
		{
			name:      "QuotedOptionKeys",
			query:     `CREATE VECTOR INDEX vec3 FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {"vector.dimensions": 384, "vector.similarity_function": "dot"}}`,
			wantDims:  384,
			wantSimFn: "dot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Failed to create vector index: %v", err)
			}

			idx, exists := store.GetSchema().GetVectorIndex(strings.Fields(tt.query)[3])
			if !exists {
				t.Fatalf("expected vector index %q to exist", strings.Fields(tt.query)[3])
			}
			wantDims := tt.wantDims
			if wantDims < 0 {
				wantDims = exec.GetDefaultEmbeddingDimensions()
			}
			if idx.Dimensions != wantDims {
				t.Fatalf("expected dimensions=%d, got=%d", wantDims, idx.Dimensions)
			}
			if idx.SimilarityFunc != tt.wantSimFn {
				t.Fatalf("expected similarity=%q, got=%q", tt.wantSimFn, idx.SimilarityFunc)
			}
		})
	}

	// Verify both were created
	indexes := userIndexes(store.GetSchema().GetIndexes())
	vectorCount := 0
	for _, idx := range indexes {
		m := idx.(map[string]interface{})
		if m["type"] == "VECTOR" {
			vectorCount++
		}
	}
	if vectorCount != 3 {
		t.Errorf("Expected 3 vector indexes, got %d", vectorCount)
	}
}

func TestFulltextIndexMultipleProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
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

func TestSchemaCommandDispatcherAndTypeParserBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Unknown schema command branch.
	_, err := exec.executeSchemaCommand(ctx, "CREATE UNKNOWN THING")
	if err == nil {
		t.Fatal("expected unknown schema command error")
	}

	// DROP CONSTRAINT IF EXISTS branch should swallow missing constraint errors.
	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT missing_name IF EXISTS")
	if err != nil {
		t.Fatalf("DROP CONSTRAINT IF EXISTS should not error for missing constraint: %v", err)
	}
	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT IF EXISTS missing_name")
	if err != nil {
		t.Fatalf("DROP CONSTRAINT IF EXISTS <name> should not error for missing constraint: %v", err)
	}

	typeCases := map[string]storage.PropertyType{
		"STRING":         storage.PropertyTypeString,
		"INT":            storage.PropertyTypeInteger,
		"INTEGER":        storage.PropertyTypeInteger,
		"FLOAT":          storage.PropertyTypeFloat,
		"BOOL":           storage.PropertyTypeBoolean,
		"BOOLEAN":        storage.PropertyTypeBoolean,
		"DATE":           storage.PropertyTypeDate,
		"DATETIME":       storage.PropertyTypeZonedDateTime,
		"ZONED DATETIME": storage.PropertyTypeZonedDateTime,
		"LOCAL DATETIME": storage.PropertyTypeLocalDateTime,
		"LOCALDATETIME":  storage.PropertyTypeLocalDateTime,
		"ZONEDDATETIME":  storage.PropertyTypeZonedDateTime,
	}
	for input, want := range typeCases {
		got, err := parsePropertyType(input)
		if err != nil {
			t.Fatalf("parsePropertyType(%q) unexpected error: %v", input, err)
		}
		if got != want {
			t.Fatalf("parsePropertyType(%q) = %q, want %q", input, got, want)
		}
	}
	if _, err := parsePropertyType("UNSUPPORTED_TYPE"); err == nil {
		t.Fatal("expected parsePropertyType unsupported type error")
	}
}

func TestCreateConstraint_SyntaxVariantCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	valid := []string{
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyExists) ASSERT exists(u.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyNotNull) ASSERT u.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyUnique) ASSERT u.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyNodeKey) ASSERT (u.a, u.b) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (t:TempUnnamed) REQUIRE (t.key, t.from, t.to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (p:TypedUnnamed) REQUIRE p.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (p:TypedLegacy) ASSERT p.age IS TYPED INTEGER",
	}
	for _, q := range valid {
		_, err := exec.Execute(ctx, q, nil)
		if err != nil {
			t.Fatalf("expected valid constraint syntax to pass for %q: %v", q, err)
		}
	}

	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_node_key FOR (u:BrokenNK) REQUIRE (u) IS NODE KEY")
	if err == nil || !strings.Contains(err.Error(), "NODE KEY constraint requires properties") {
		t.Fatalf("expected NODE KEY property validation error, got: %v", err)
	}

	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal FOR (t:BrokenTemporal) REQUIRE (t.key, t.from) IS TEMPORAL")
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL constraint requires 3 properties") {
		t.Fatalf("expected TEMPORAL property-count validation error, got: %v", err)
	}
}

func TestCreateConstraint_EachParserPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		// NODE KEY variants
		"CREATE CONSTRAINT nk_named IF NOT EXISTS FOR (n:NKNamed) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NKUnnamed) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NKAssert) ASSERT (n.k1, n.k2) IS NODE KEY",

		// TEMPORAL variants
		"CREATE CONSTRAINT t_named IF NOT EXISTS FOR (n:TNamed) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:TUnnamed) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",

		// EXISTS / NOT NULL variants
		"CREATE CONSTRAINT nn_named IF NOT EXISTS FOR (n:NNNamed) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NNUnnamed) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NNExists) ASSERT exists(n.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NNAssert) ASSERT n.email IS NOT NULL",

		// TYPE variants
		"CREATE CONSTRAINT tp_named IF NOT EXISTS FOR (n:TPNamed) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:TPUnnamed) REQUIRE n.age IS TYPED INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:TPAssert) ASSERT n.age IS :: INTEGER",

		// UNIQUE variants
		"CREATE CONSTRAINT uq_named IF NOT EXISTS FOR (n:UQNamed) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:UQUnnamed) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:UQAssert) ASSERT n.id IS UNIQUE",
	}

	for _, q := range queries {
		_, err := exec.executeCreateConstraint(ctx, q)
		if err != nil {
			t.Fatalf("expected query to match a CREATE CONSTRAINT parser pattern, got error for %q: %v", q, err)
		}
	}
}

func TestCreateConstraint_ValidationAndDuplicateErrorBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:DupUser {email:'a@x'}), (:DupUser {email:'a@x'})", nil)
	if err != nil {
		t.Fatalf("failed to seed duplicate users: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT uq_dup IF NOT EXISTS FOR (n:DupUser) REQUIRE n.email IS UNIQUE")
	if err == nil {
		t.Fatal("expected unique constraint validation error on existing duplicate data")
	}

	_, err = exec.Execute(ctx, "CREATE (:NullUser {name:'x'})", nil)
	if err != nil {
		t.Fatalf("failed to seed null property user: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT nn_dup IF NOT EXISTS FOR (n:NullUser) REQUIRE n.email IS NOT NULL")
	if err == nil {
		t.Fatal("expected NOT NULL constraint validation error on existing null property data")
	}

	_, err = exec.Execute(ctx, "CREATE (:TypedBad {age:'not-an-int'})", nil)
	if err != nil {
		t.Fatalf("failed to seed wrong-typed data: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT tp_dup IF NOT EXISTS FOR (n:TypedBad) REQUIRE n.age IS :: INTEGER")
	if err == nil {
		t.Fatal("expected type constraint validation error on existing wrong-typed data")
	}

}

func TestCreateIndex_BranchCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Named index branch (composite properties).
	_, err := exec.executeCreateIndex(ctx, "CREATE INDEX idx_person_name_age FOR (n:Person) ON (n.name, n.age)")
	if err != nil {
		t.Fatalf("expected named composite index creation to succeed: %v", err)
	}

	// Unnamed index branch (auto-generated name).
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:Product) ON (n.sku, n.region)")
	if err != nil {
		t.Fatalf("expected unnamed composite index creation to succeed: %v", err)
	}

	// Named no-properties branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_empty FOR (n:EmptyIdx) ON (n)")
	if err == nil {
		t.Fatal("expected no properties specified for named index")
	}

	// Unnamed no-properties branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:EmptyIdx2) ON (n)")
	if err == nil {
		t.Fatal("expected no properties specified for unnamed index")
	}

	// Invalid syntax fallback branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_invalid")
	if err == nil {
		t.Fatal("expected invalid CREATE INDEX syntax error")
	}

	relationshipIndexes := []string{
		"CREATE INDEX fact_namespace_idx IF NOT EXISTS FOR ()-[f:FACT]-() ON (f.namespace)",
		"CREATE INDEX fact_predicate_idx IF NOT EXISTS FOR ()-[f:FACT]-() ON (f.predicate)",
		"CREATE INDEX fact_namespace_predicate_idx IF NOT EXISTS FOR ()-[f:FACT]-() ON (f.namespace, f.predicate)",
	}
	for _, q := range relationshipIndexes {
		_, err = exec.executeCreateIndex(ctx, q)
		if err != nil {
			t.Fatalf("expected relationship CREATE INDEX syntax to succeed for %s; got %v", q, err)
		}
	}
}

func TestCreateIndex_Neo4jCompatibilitySyntax(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		"CREATE INDEX FOR (n:TestNode) ON (n.entity_id)",
		"CREATE INDEX IF NOT EXISTS FOR (n:`base`) ON (n.entity_id)",
		"CREATE INDEX ON :TestNode(entity_id)",
		"CREATE INDEX test_idx ON :TestNode(entity_id)",
		"CREATE RANGE INDEX FOR (n:TestNode) ON (n.entity_id)",
		"CREATE INDEX IF NOT EXISTS FOR (n:Test) ON n.entity_id",
		"CREATE INDEX rel_uuid IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.uuid)",
		"CREATE INDEX FOR ()<-[r:RELATES_TO]-() ON (r.uuid, r.group_id)",
		"CREATE INDEX `rel idx` IF NOT EXISTS FOR ()-[`r`:`RELATES_TO`]-() ON (`r`.`uuid`) OPTIONS {indexProvider: 'range-1.0'}",
	}
	for _, q := range queries {
		_, err := exec.executeSchemaCommand(ctx, q)
		if err != nil {
			t.Fatalf("expected query to succeed: %s; err=%v", q, err)
		}
	}

	indexes := userIndexes(store.GetSchema().GetIndexes())
	if len(indexes) == 0 {
		t.Fatal("expected created indexes to be present")
	}

	for _, raw := range indexes {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if label, ok := idx["label"].(string); ok && strings.Contains(label, ")") {
			t.Fatalf("label should not include closing parenthesis: %q", label)
		}
		if labels, ok := idx["labels"].([]string); ok {
			for _, label := range labels {
				if strings.Contains(label, ")") {
					t.Fatalf("label should not include closing parenthesis: %q", label)
				}
			}
		}
	}
}

func TestCreateRangeIndex_RelationshipPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// A RANGE index on a relationship property is valid Neo4j 5 DDL (#531).
	_, err := exec.executeSchemaCommand(ctx, "CREATE RANGE INDEX rel_rng FOR ()-[r:RELATES_TO]-() ON (r.uuid)")
	require.NoError(t, err)
}

func TestSchemaDDL_AllowsTrailingOptionsAndBacktickIdentifiers(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	ddl := []string{
		"CREATE CONSTRAINT `uq order id` IF NOT EXISTS FOR (`n`:`Order`) REQUIRE `n`.`id` IS UNIQUE OPTIONS {indexProvider: 'range-1.0'}",
		"CREATE INDEX `idx order created` IF NOT EXISTS FOR (`o`:`Order`) ON (`o`.`createdAt`) OPTIONS {indexProvider: 'range-1.0'}",
		"CREATE FULLTEXT INDEX `ft order` IF NOT EXISTS FOR (`o`:`Order`) ON EACH [`o`.`title`, `o`.`body`] OPTIONS {analyzer: 'standard'}",
	}
	for _, q := range ddl {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "expected DDL with OPTIONS/backticks to parse: %s", q)
	}

	_, err := exec.Execute(ctx, "DROP CONSTRAINT IF EXISTS `uq order id`", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "DROP CONSTRAINT `uq order id` IF EXISTS", nil)
	require.NoError(t, err)
}

func TestCreateConstraintRequireVariants_ANTLRMode(t *testing.T) {
	cleanup := config.WithANTLRParser()
	defer cleanup()

	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		"CREATE CONSTRAINT `uq order id antlr` IF NOT EXISTS FOR (`n`:`OrderANTLR`) REQUIRE `n`.`id` IS UNIQUE OPTIONS {indexProvider: 'range-1.0'}",
		"CREATE CONSTRAINT nk_antlr IF NOT EXISTS FOR (n:NKANTLR) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT nn_antlr IF NOT EXISTS FOR (n:NNANTLR) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT tp_antlr IF NOT EXISTS FOR (n:TPANTLR) REQUIRE n.ts IS TYPED ZONED DATETIME",
	}

	for _, query := range queries {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, "expected REQUIRE variant to execute via ANTLR parser: %s", query)
	}
}

func TestCreateIndex_RebuildsUsableEntriesAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	base1, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	store1 := storage.NewNamespacedEngine(base1, "test")
	exec1 := NewStorageExecutor(store1)

	_, err = exec1.Execute(ctx, "CREATE (n:MongoDocument {sourceId: 'src-001'})", nil)
	require.NoError(t, err)
	_, err = exec1.Execute(ctx, "CREATE (n:MongoDocument {sourceId: 'src-002'})", nil)
	require.NoError(t, err)

	_, err = exec1.Execute(ctx, "CREATE INDEX idx_source_id IF NOT EXISTS FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)

	pre := store1.GetSchema().PropertyIndexLookup("MongoDocument", "sourceId", "src-002")
	require.Len(t, pre, 1)

	require.NoError(t, base1.Close())

	base2, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	defer base2.Close()
	store2 := storage.NewNamespacedEngine(base2, "test")
	exec2 := NewStorageExecutor(store2)

	showRes, err := exec2.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotEmpty(t, showRes.Rows)

	post := store2.GetSchema().PropertyIndexLookup("MongoDocument", "sourceId", "src-002")
	require.Len(t, post, 1, "property index entries should remain usable after restart")
}

func TestCreateRelationshipIndex_PersistsAcrossRestart_E2E(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	base1, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	store1 := storage.NewNamespacedEngine(base1, "test")
	exec1 := NewStorageExecutor(store1)

	_, err = exec1.Execute(ctx, "CREATE (:Entity {uuid: 'a'})", nil)
	require.NoError(t, err)
	_, err = exec1.Execute(ctx, "CREATE (:Entity {uuid: 'b'})", nil)
	require.NoError(t, err)
	_, err = exec1.Execute(ctx, "MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1'}]->(b)", nil)
	require.NoError(t, err)

	const ddl = "CREATE INDEX rel_uuid_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.uuid)"
	_, err = exec1.Execute(ctx, ddl, nil)
	require.NoError(t, err)

	findIndexRow := func(result *ExecuteResult, name string) map[string]interface{} {
		for _, row := range result.Rows {
			if len(row) != len(result.Columns) {
				continue
			}
			rowMap := make(map[string]interface{}, len(result.Columns))
			for i, col := range result.Columns {
				rowMap[col] = row[i]
			}
			if n, _ := rowMap["name"].(string); n == name {
				return rowMap
			}
		}
		return nil
	}

	listHas := func(v interface{}, want string) bool {
		switch vals := v.(type) {
		case []string:
			for _, s := range vals {
				if s == want {
					return true
				}
			}
		case []interface{}:
			for _, s := range vals {
				if ss, ok := s.(string); ok && ss == want {
					return true
				}
			}
		}
		return false
	}

	show1, err := exec1.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	row1 := findIndexRow(show1, "rel_uuid_idx")
	require.NotNil(t, row1, "expected rel_uuid_idx in SHOW INDEXES before restart")
	require.Equal(t, "RELATIONSHIP", row1["entityType"])
	require.True(t, listHas(row1["labelsOrTypes"], "RELATES_TO"), "expected RELATES_TO in labelsOrTypes, got %#v", row1["labelsOrTypes"])
	require.True(t, listHas(row1["properties"], "uuid"), "expected uuid in properties, got %#v", row1["properties"])

	require.NoError(t, base1.Close())

	base2, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	defer base2.Close()
	store2 := storage.NewNamespacedEngine(base2, "test")
	exec2 := NewStorageExecutor(store2)

	show2, err := exec2.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	row2 := findIndexRow(show2, "rel_uuid_idx")
	require.NotNil(t, row2, "expected rel_uuid_idx in SHOW INDEXES after restart")
	require.Equal(t, "RELATIONSHIP", row2["entityType"])
	require.True(t, listHas(row2["labelsOrTypes"], "RELATES_TO"), "expected RELATES_TO in labelsOrTypes after restart, got %#v", row2["labelsOrTypes"])
	require.True(t, listHas(row2["properties"], "uuid"), "expected uuid in properties after restart, got %#v", row2["properties"])
}

func TestCreateFulltextIndex_CompatSyntaxWithoutParenthesizedPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeSchemaCommand(ctx, "CREATE FULLTEXT INDEX ft1 FOR n:base ON n.entity_id")
	if err != nil {
		t.Fatalf("expected fulltext compat syntax to succeed: %v", err)
	}

	indexes := userIndexes(store.GetSchema().GetIndexes())
	found := false
	for _, raw := range indexes {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := idx["name"].(string)
		idxType, _ := idx["type"].(string)
		if name == "ft1" && strings.EqualFold(idxType, "FULLTEXT") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected fulltext index ft1 to be persisted in schema")
	}
}

func TestCreateConstraint_DuplicateNamedDefinitionErrors(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name         string
		query        string
		queryWithINE string // same query with IF NOT EXISTS
	}{
		{
			name:         "exists_not_null",
			query:        "CREATE CONSTRAINT nn_dup_name FOR (n:DupExists) REQUIRE n.email IS NOT NULL",
			queryWithINE: "CREATE CONSTRAINT nn_dup_name IF NOT EXISTS FOR (n:DupExists) REQUIRE n.email IS NOT NULL",
		},
		{
			name:         "node_key",
			query:        "CREATE CONSTRAINT nk_dup_name FOR (n:DupNodeKey) REQUIRE (n.k1, n.k2) IS NODE KEY",
			queryWithINE: "CREATE CONSTRAINT nk_dup_name IF NOT EXISTS FOR (n:DupNodeKey) REQUIRE (n.k1, n.k2) IS NODE KEY",
		},
		{
			name:         "temporal",
			query:        "CREATE CONSTRAINT tp_dup_name FOR (n:DupTemporal) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
			queryWithINE: "CREATE CONSTRAINT tp_dup_name IF NOT EXISTS FOR (n:DupTemporal) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.executeCreateConstraint(ctx, tt.query)
			require.NoError(t, err, "first create should succeed")

			// Duplicate without IF NOT EXISTS should error
			_, err = exec.executeCreateConstraint(ctx, tt.query)
			require.Error(t, err, "duplicate without IF NOT EXISTS should error")
			require.Contains(t, err.Error(), "already exists")

			// Duplicate with IF NOT EXISTS should be no-op
			_, err = exec.executeCreateConstraint(ctx, tt.queryWithINE)
			require.NoError(t, err, "duplicate with IF NOT EXISTS should be no-op")
		})
	}
}

func TestCreateRangeIndex_ErrorBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age FOR (n:Person) ON (n.age)")
	if err != nil {
		t.Fatalf("failed to create baseline range index: %v", err)
	}
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age FOR (n:Person) ON (n.score)")
	if err != nil {
		t.Fatalf("expected conflicting named range index to be idempotent, got: %v", err)
	}

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Product) ON (n.price)")
	if err != nil {
		t.Fatalf("failed to create unnamed range index: %v", err)
	}
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Product) ON (n.cost)")
	if err != nil {
		t.Fatalf("expected second unnamed range index with different generated name to succeed, got: %v", err)
	}

	// A RANGE index is CREATE INDEX: composite and relationship forms too.
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_multi FOR (n:Person) ON (n.city, n.zip)")
	if err != nil {
		t.Fatalf("composite range index: %v", err)
	}
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_rel FOR ()-[r:KNOWS]-() ON (r.since)")
	if err != nil {
		t.Fatalf("relationship range index: %v", err)
	}

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX")
	if err == nil || !strings.Contains(err.Error(), "invalid CREATE") {
		t.Fatalf("expected invalid syntax error, got: %v", err)
	}
}

func TestCreateFulltextIndex_SchemaAndDuplicateErrors(t *testing.T) {
	ctx := context.Background()

	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Doc) ON EACH [n.title]")
	if err != nil {
		t.Fatalf("failed to create baseline fulltext index: %v", err)
	}
	_, err = exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Doc) ON EACH [n.body]")
	if err != nil {
		t.Fatalf("expected conflicting fulltext index to be idempotent, got: %v", err)
	}

	nilSchema := &nilSchemaEngine{Engine: store}
	execNilSchema := NewStorageExecutor(nilSchema)
	_, err = execNilSchema.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX nil_schema FOR (n:Doc) ON EACH [n.title]")
	if err == nil || !strings.Contains(err.Error(), "schema manager not available") {
		t.Fatalf("expected nil schema error, got: %v", err)
	}
}

func TestCreateVectorIndex_DuplicateErrorBranch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	query := "CREATE VECTOR INDEX vec_dup FOR (n:Doc) ON (n.embedding)"
	_, err := exec.executeCreateVectorIndex(ctx, query)
	if err != nil {
		t.Fatalf("failed to create baseline vector index: %v", err)
	}
	_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX vec_dup FOR (n:Doc) ON (n.altEmbedding)")
	if err != nil {
		t.Fatalf("expected conflicting vector index to be idempotent, got: %v", err)
	}
}

// =============================================================================
// Relationship Constraint DDL Tests
// =============================================================================

func TestRelationshipUniqueConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named single property", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_unique_order FOR ()-[r:SEQUEL_OF]-() REQUIRE r.order IS UNIQUE`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "rel_unique_order" {
				require.Equal(t, storage.ConstraintUnique, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "SEQUEL_OF", c.Label)
				require.Equal(t, []string{"order"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "constraint rel_unique_order not found")
	})

	t.Run("unnamed single property", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:WROTE]-() REQUIRE r.year IS UNIQUE`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "WROTE" && c.Type == storage.ConstraintUnique {
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, []string{"year"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "unnamed WROTE uniqueness constraint not found")
	})

	t.Run("named composite UNIQUE", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT prequel_composite FOR ()-[r:PREQUEL_OF]-() REQUIRE (r.order, r.author) IS UNIQUE`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "prequel_composite" {
				require.Equal(t, storage.ConstraintUnique, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "PREQUEL_OF", c.Label)
				require.Equal(t, []string{"order", "author"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "composite uniqueness constraint not found")
	})

	t.Run("IF NOT EXISTS is no-op for existing", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_unique_order IF NOT EXISTS FOR ()-[r:SEQUEL_OF]-() REQUIRE r.order IS UNIQUE`, nil)
		require.NoError(t, err)
	})
}

func TestRelationshipExistsConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT wrote_year_exists FOR ()-[r:WROTE]-() REQUIRE r.year IS NOT NULL`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "wrote_year_exists" {
				require.Equal(t, storage.ConstraintExists, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "WROTE", c.Label)
				require.Equal(t, []string{"year"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "constraint wrote_year_exists not found")
	})

	t.Run("unnamed", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "FOLLOWS" && c.Type == storage.ConstraintExists {
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				found = true
			}
		}
		require.True(t, found, "unnamed FOLLOWS exists constraint not found")
	})
}

func TestRelationshipPropertyTypeConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named INTEGER type", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT part_of_order_type FOR ()-[r:PART_OF]-() REQUIRE r.order IS :: INTEGER`, nil)
		require.NoError(t, err)

		allPtc := store.GetSchema().GetAllPropertyTypeConstraints()
		found := false
		for _, ptc := range allPtc {
			if ptc.Name == "part_of_order_type" {
				require.Equal(t, "PART_OF", ptc.Label)
				require.Equal(t, "order", ptc.Property)
				require.Equal(t, storage.PropertyTypeInteger, ptc.ExpectedType)
				found = true
			}
		}
		require.True(t, found, "constraint part_of_order_type not found")
	})

	t.Run("unnamed STRING type", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:KNOWS]-() REQUIRE r.how IS :: STRING`, nil)
		require.NoError(t, err)
	})
}

func TestRelationshipKeyConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named single property", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT owns_key FOR ()-[r:OWNS]-() REQUIRE r.ownershipId IS RELATIONSHIP KEY`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "owns_key" {
				require.Equal(t, storage.ConstraintRelationshipKey, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "OWNS", c.Label)
				require.Equal(t, []string{"ownershipId"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "constraint owns_key not found")
	})

	t.Run("named composite key", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT knows_key FOR ()-[r:KNOWS]-() REQUIRE (r.since, r.how) IS RELATIONSHIP KEY`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "knows_key" {
				require.Equal(t, storage.ConstraintRelationshipKey, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "KNOWS", c.Label)
				require.Equal(t, []string{"since", "how"}, c.Properties)
				found = true
			}
		}
		require.True(t, found, "constraint knows_key not found")
	})

	t.Run("unnamed single key", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:MANAGES]-() REQUIRE r.roleId IS RELATIONSHIP KEY`, nil)
		require.NoError(t, err)
	})

	t.Run("unnamed composite key", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:REPORTS_TO]-() REQUIRE (r.dept, r.level) IS RELATIONSHIP KEY`, nil)
		require.NoError(t, err)
	})
}

func TestRelationshipConstraint_ShowConstraints(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node constraint and a relationship constraint
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT node_unique FOR (n:Person) REQUIRE n.email IS UNIQUE`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE CONSTRAINT rel_unique FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
	require.NoError(t, err)

	// SHOW CONSTRAINTS should show both with correct entity types
	result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	entityTypes := map[string]string{}
	for _, row := range result.Rows {
		name := row[1].(string)
		entityType := row[3].(string)
		entityTypes[name] = entityType
	}

	require.Equal(t, "NODE", entityTypes["node_unique"])
	require.Equal(t, "RELATIONSHIP", entityTypes["rel_unique"])
}

func TestRelationshipConstraint_OwnedBackingIndex(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("uniqueness constraint creates owned index", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_u FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
		require.NoError(t, err)

		// Check SHOW CONSTRAINTS returns the owned index
		result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "rel_u", result.Rows[0][6]) // ownedIndex column
	})

	t.Run("dropping constraint drops owned index", func(t *testing.T) {
		// Verify index exists
		result, err := exec.Execute(ctx, "SHOW INDEXES", nil)
		require.NoError(t, err)
		indexFound := false
		for _, row := range result.Rows {
			if name, ok := row[1].(string); ok && name == "rel_u" {
				indexFound = true
			}
		}
		require.True(t, indexFound, "owned index should exist before drop")

		// Drop the constraint
		_, err = exec.Execute(ctx, "DROP CONSTRAINT rel_u", nil)
		require.NoError(t, err)

		// Verify index is gone
		result, err = exec.Execute(ctx, "SHOW INDEXES", nil)
		require.NoError(t, err)
		for _, row := range result.Rows {
			if name, ok := row[1].(string); ok {
				require.NotEqual(t, "rel_u", name, "owned index should be dropped with constraint")
			}
		}
	})

	t.Run("relationship key creates owned index", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_key FOR ()-[r:OWNS]-() REQUIRE r.ownershipId IS RELATIONSHIP KEY`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		for _, c := range all {
			if c.Name == "rel_key" {
				require.Equal(t, "rel_key", c.OwnedIndex)
			}
		}
	})

	t.Run("existence constraint does NOT create owned index", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_exists FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		for _, c := range all {
			if c.Name == "rel_exists" {
				require.Empty(t, c.OwnedIndex, "existence constraint should not have an owned index")
			}
		}
	})
}

func TestRelationshipConstraint_ConflictDetection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("same name different schema errors", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT dup_name FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, `CREATE CONSTRAINT dup_name FOR ()-[r:FOLLOWS]-() REQUIRE r.year IS UNIQUE`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("same schema same type different name errors without IF NOT EXISTS", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT another_name FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "equivalent constraint")
	})

	t.Run("same schema same type different name is no-op with IF NOT EXISTS", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT yet_another IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
		require.NoError(t, err) // IF NOT EXISTS — no-op
	})

	t.Run("uniqueness vs relationship key conflict", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT owns_unique FOR ()-[r:OWNS]-() REQUIRE r.ownershipId IS UNIQUE`, nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, `CREATE CONSTRAINT owns_key FOR ()-[r:OWNS]-() REQUIRE r.ownershipId IS RELATIONSHIP KEY`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "conflicting")
	})
}

func TestRelationshipConstraint_EnforcementOnWrite(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Set up test data
	_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Person {name: "Bob"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Person {name: "Charlie"})`, nil)
	require.NoError(t, err)

	t.Run("uniqueness enforcement on create", func(t *testing.T) {
		// Create uniqueness constraint
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT knows_since_u FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
		require.NoError(t, err)

		// Create a relationship — should succeed
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2020}]->(b)`, nil)
		require.NoError(t, err)

		// Create another relationship with duplicate 'since' — should fail
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Person {name: "Charlie"}) CREATE (a)-[:KNOWS {since: 2020}]->(c)`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("existence enforcement on create", func(t *testing.T) {
		// Create existence constraint
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT follows_reason FOR ()-[r:FOLLOWS]-() REQUIRE r.reason IS NOT NULL`, nil)
		require.NoError(t, err)

		// Create a relationship without the required property — should fail
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:FOLLOWS]->(b)`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Required property")

		// Create with the required property — should succeed
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:FOLLOWS {reason: "friend"}]->(b)`, nil)
		require.NoError(t, err)
	})
}

func TestRelationshipConstraint_ValidationOnCreation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and a relationship with a duplicate property value
	_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Person {name: "Bob"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Person {name: "Charlie"})`, nil)
	require.NoError(t, err)

	// Create relationships with duplicate 'since' values
	_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2020}]->(b)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (b:Person {name: "Charlie"}) CREATE (a)-[:KNOWS {since: 2020}]->(b)`, nil)
	require.NoError(t, err)

	// Creating a uniqueness constraint should fail because of duplicate values
	_, err = exec.Execute(ctx, `CREATE CONSTRAINT knows_since_unique FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNIQUE")
}

func TestDomainConstraint_DDL(t *testing.T) {
	t.Run("named node domain constraint", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive', 'pending']`, nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		nameIdx := -1
		typeIdx := -1
		entityIdx := -1
		for i, col := range result.Columns {
			switch col {
			case "name":
				nameIdx = i
			case "type":
				typeIdx = i
			case "entityType":
				entityIdx = i
			}
		}
		require.NotEqual(t, -1, nameIdx)
		require.Equal(t, "person_status_domain", result.Rows[0][nameIdx])
		require.Equal(t, string(storage.ConstraintDomain), result.Rows[0][typeIdx])
		require.Equal(t, "NODE", result.Rows[0][entityIdx])
	})

	t.Run("unnamed node domain constraint", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("creation fails when existing data violates domain", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice", status: "active"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Bob", status: "unknown"})`, nil)
		require.NoError(t, err)

		// Should fail — "unknown" is not in the allowed list
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT person_status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DOMAIN")
		require.Contains(t, err.Error(), "unknown")
	})

	t.Run("creation succeeds on clean data", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice", status: "active"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Bob", status: "inactive"})`, nil)
		require.NoError(t, err)

		_, err = exec.Execute(ctx, `CREATE CONSTRAINT person_status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)
	})

	t.Run("enforcement on node create", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		// Create constraint first
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)

		// Valid value should succeed
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Alice", status: "active"})`, nil)
		require.NoError(t, err)

		// Invalid value should fail
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Bob", status: "unknown"})`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DOMAIN")

		// NULL value should succeed (NULL is valid for domain constraints)
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Charlie"})`, nil)
		require.NoError(t, err)
	})

	t.Run("numeric domain values", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT priority_domain FOR (n:Task) REQUIRE n.priority IN [1, 2, 3]`, nil)
		require.NoError(t, err)

		// Valid priority
		_, err = exec.Execute(ctx, `CREATE (:Task {name: "fix bug", priority: 1})`, nil)
		require.NoError(t, err)

		// Invalid priority
		_, err = exec.Execute(ctx, `CREATE (:Task {name: "nice to have", priority: 5})`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DOMAIN")
	})

	t.Run("relationship domain constraint", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		// Create constraint on relationship
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_role_domain FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager', 'director']`, nil)
		require.NoError(t, err)

		// Create nodes
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Company {name: "Acme"})`, nil)
		require.NoError(t, err)

		// Valid role
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {role: "engineer"}]->(c)`, nil)
		require.NoError(t, err)

		// Invalid role
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {role: "intern"}]->(c)`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DOMAIN")
	})

	t.Run("relationship domain constraint creation fails on violating data", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Company {name: "Acme"})`, nil)
		require.NoError(t, err)

		// Create edge with a role not in the allowed list
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {role: "intern"}]->(c)`, nil)
		require.NoError(t, err)

		// Now try to add the constraint — should fail
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT rel_role_domain FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager']`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DOMAIN")
		require.Contains(t, err.Error(), "intern")
	})

	t.Run("different allowed values on same property conflict", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)

		// Different name, same property, different allowed values — should conflict
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT status_domain_v2 FOR (n:Person) REQUIRE n.status IN ['active', 'pending']`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "conflicting domain constraint")
	})

	t.Run("same name same values errors without IF NOT EXISTS", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)

		// Same name, same values, no IF NOT EXISTS — should error
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		// Same with IF NOT EXISTS — should be no-op
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT status_domain IF NOT EXISTS FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)
	})

	t.Run("same name different values errors", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']`, nil)
		require.NoError(t, err)

		// Same name but different values — should error
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'pending']`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "different allowed values")
	})
}

func TestRelationshipTemporalConstraint_DDL(t *testing.T) {
	t.Run("named temporal constraint with 4-property endpoint-pair form", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT employment_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.from_id, r.to_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err)

		// Verify constraint exists via SHOW CONSTRAINTS
		result, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		// Find columns
		nameIdx := -1
		typeIdx := -1
		entityIdx := -1
		propsIdx := -1
		for i, col := range result.Columns {
			switch col {
			case "name":
				nameIdx = i
			case "type":
				typeIdx = i
			case "entityType":
				entityIdx = i
			case "properties":
				propsIdx = i
			}
		}
		require.NotEqual(t, -1, nameIdx)
		require.Equal(t, "employment_temporal", result.Rows[0][nameIdx])
		require.Equal(t, string(storage.ConstraintTemporal), result.Rows[0][typeIdx])
		require.Equal(t, "RELATIONSHIP", result.Rows[0][entityIdx])
		require.Equal(t, []string{"from_id", "to_id", "valid_from", "valid_to"}, result.Rows[0][propsIdx])
	})

	t.Run("3-property single-key form still works", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT employment_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.employee_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("unnamed temporal constraint on relationship", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:WORKS_AT]-() REQUIRE (r.from_id, r.to_id, r.valid_from, r.valid_to) IS TEMPORAL`, nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})

	t.Run("rejects fewer than 3 properties", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least 3 properties")
	})

	t.Run("4-property creation fails on overlapping edges", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		// Create nodes
		_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Company {name: "Acme"})`, nil)
		require.NoError(t, err)

		// Create overlapping edges with same composite key (from_id, to_id)
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2020-01-01T00:00:00Z", valid_to: "2022-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2021-06-01T00:00:00Z", valid_to: "2023-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)

		// Creating temporal constraint should fail due to overlap on same composite key
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT employment_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.from_id, r.to_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TEMPORAL")
		require.Contains(t, err.Error(), "overlap")
	})

	t.Run("4-property creation succeeds when composite keys differ", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		_, err := exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Company {name: "Acme"})`, nil)
		require.NoError(t, err)

		// Same time range but different to_id — different composite key, no overlap
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2020-01-01T00:00:00Z", valid_to: "2022-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "other_co", valid_from: "2020-01-01T00:00:00Z", valid_to: "2022-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)

		// Creating constraint should succeed — different composite keys
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT employment_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.from_id, r.to_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err)
	})

	t.Run("4-property enforcement on write — overlapping edge rejected", func(t *testing.T) {
		baseStore := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(baseStore, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		// Create constraint first with 4-property form
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT employment_temporal FOR ()-[r:WORKS_AT]-() REQUIRE (r.from_id, r.to_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP`, nil)
		require.NoError(t, err)

		// Create nodes
		_, err = exec.Execute(ctx, `CREATE (:Person {name: "Alice"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Company {name: "Acme"})`, nil)
		require.NoError(t, err)

		// Create first employment edge
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2020-01-01T00:00:00Z", valid_to: "2022-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)

		// Try to create overlapping edge with same composite key — should fail
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2021-06-01T00:00:00Z", valid_to: "2023-01-01T00:00:00Z"}]->(c)`, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TEMPORAL")

		// Non-overlapping edge with same composite key should succeed
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "acme", valid_from: "2023-01-01T00:00:00Z", valid_to: "2024-01-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)

		// Different composite key (different to_id) should not conflict even with overlapping time
		_, err = exec.Execute(ctx, `MATCH (a:Person {name: "Alice"}), (c:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT {from_id: "alice", to_id: "other_co", valid_from: "2020-06-01T00:00:00Z", valid_to: "2022-06-01T00:00:00Z"}]->(c)`, nil)
		require.NoError(t, err)
	})
}

func TestShowIndexes_RelationshipBackingIndex(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create relationship uniqueness constraint (auto-creates owned backing index)
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT rel_uniq FOR ()-[r:KNOWS]-() REQUIRE r.since IS UNIQUE`, nil)
	require.NoError(t, err)

	// Create relationship key constraint with composite properties
	_, err = exec.Execute(ctx, `CREATE CONSTRAINT rel_comp_key FOR ()-[r:WORKS_AT]-() REQUIRE (r.dept, r.role) IS RELATIONSHIP KEY`, nil)
	require.NoError(t, err)

	// SHOW INDEXES should include the backing indexes with correct metadata
	result, err := exec.Execute(ctx, `SHOW INDEXES`, nil)
	require.NoError(t, err)

	foundUniq := false
	foundCompKey := false
	for _, row := range result.Rows {
		name := row[1].(string)
		entityType := row[5].(string)
		owningConstraint := row[9]

		switch name {
		case "rel_uniq":
			foundUniq = true
			require.Equal(t, "RELATIONSHIP", entityType, "backing index should have RELATIONSHIP entity type")
			require.Equal(t, "rel_uniq", owningConstraint, "backing index should reference owning constraint")
			props := row[7].([]string)
			require.Equal(t, []string{"since"}, props)
		case "rel_comp_key":
			foundCompKey = true
			require.Equal(t, "RELATIONSHIP", entityType)
			require.Equal(t, "rel_comp_key", owningConstraint)
			props := row[7].([]string)
			require.Equal(t, []string{"dept", "role"}, props, "composite key index should have all properties")
		}
	}
	require.True(t, foundUniq, "expected to find rel_uniq (the constraint's own name, #530) in SHOW INDEXES")
	require.True(t, foundCompKey, "expected to find rel_comp_key in SHOW INDEXES")
}
