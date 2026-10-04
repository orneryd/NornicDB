package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type updateErrorEngine struct {
	storage.Engine
	nodeErr error
	edgeErr error
}

func (e *updateErrorEngine) UpdateNode(node *storage.Node) error {
	if e.nodeErr != nil {
		return e.nodeErr
	}
	return e.Engine.UpdateNode(node)
}

func (e *updateErrorEngine) UpdateEdge(edge *storage.Edge) error {
	if e.edgeErr != nil {
		return e.edgeErr
	}
	return e.Engine.UpdateEdge(edge)
}

// TestCompositeIndex tests composite (multi-property) index creation
func TestCompositeIndex(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index with name
	_, err := exec.Execute(ctx, "CREATE INDEX person_name_age FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Verify index was created (via schema)
	schema := store.GetSchema()
	indexes := schema.GetIndexes()

	// Should have the composite index
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)

			if name == "person_name_age" && label == "Person" {
				if len(props) == 2 && props[0] == "firstName" && props[1] == "lastName" {
					found = true
					break
				}
			}
		}
	}

	if !found {
		t.Error("Composite index not found in schema")
	}
}

func TestMonster531CompositeAfterSinglePropertyIndex(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			for _, query := range []string{
				"CREATE (:P {a: 1, b: 2})",
				"CREATE INDEX ix_a FOR (n:P) ON (n.a)",
				"CREATE INDEX ix_ab FOR (n:P) ON (n.a, n.b)",
			} {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, properties WHERE name IN ['ix_a', 'ix_ab'] RETURN name, properties ORDER BY name", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			require.Equal(t, "ix_a", result.Rows[0][0])
			require.Equal(t, "ix_ab", result.Rows[1][0])
			require.ElementsMatch(t, []string{"a", "b"}, result.Rows[1][1])
			index, exists := exec.storage.GetSchema().GetRangeIndex("ix_ab")
			require.True(t, exists)
			require.Equal(t, []string{"a", "b"}, index.Properties)
			_, err = exec.Execute(ctx, "CREATE (:P {a: 3, b: 4})", nil)
			require.NoError(t, err)
			result, err = exec.Execute(ctx, "MATCH (n:P) RETURN n.a, n.b ORDER BY n.a", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(3), int64(4)}}, result.Rows)
		})
	}
}

func TestLegacyIndexProcedureCompatibilityBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// db.index.vector.createRelationshipIndex success + default similarity branch.
	res, err := exec.callDbIndexVectorCreateRelationshipIndex(ctx, "CALL db.index.vector.createRelationshipIndex('rel_vec_idx','KNOWS','embedding',128)")
	if err != nil {
		t.Fatalf("expected relationship vector index creation success: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][4] != "cosine" {
		t.Fatalf("expected default cosine similarity in result row, got %+v", res.Rows)
	}

	// invalid dimension branch
	_, err = exec.callDbIndexVectorCreateRelationshipIndex(ctx, "CALL db.index.vector.createRelationshipIndex('bad','KNOWS','embedding','x')")
	if err == nil || !strings.Contains(err.Error(), "invalid dimension") {
		t.Fatalf("expected invalid dimension error, got: %v", err)
	}

	// missing-args branch
	_, err = exec.callDbIndexVectorCreateRelationshipIndex(ctx, "CALL db.index.vector.createRelationshipIndex('too_few')")
	if err == nil {
		t.Fatal("expected too-few-args error for relationship vector index")
	}

	// fulltext create (node + relationship) success
	_, err = exec.callDbIndexFulltextCreateNodeIndex(ctx, "CALL db.index.fulltext.createNodeIndex('ft_node',['Doc'],['title','body'])")
	if err != nil {
		t.Fatalf("expected node fulltext index creation success: %v", err)
	}
	_, err = exec.callDbIndexFulltextCreateRelationshipIndex(ctx, "CALL db.index.fulltext.createRelationshipIndex('ft_rel',['KNOWS'],['note'])")
	if err != nil {
		t.Fatalf("expected relationship fulltext index creation success: %v", err)
	}

	// fulltext missing parenthesis/args branches
	_, err = exec.callDbIndexFulltextCreateNodeIndex(ctx, "CALL db.index.fulltext.createNodeIndex 'ft'")
	if err == nil {
		t.Fatal("expected missing parentheses error for fulltext node index")
	}
	_, err = exec.callDbIndexFulltextCreateRelationshipIndex(ctx, "CALL db.index.fulltext.createRelationshipIndex('ft_rel')")
	if err == nil {
		t.Fatal("expected too-few-args error for fulltext relationship index")
	}

	// vector drop branches
	_, err = exec.callDbIndexVectorDrop("CALL db.index.vector.drop('rel_vec_idx')")
	if err != nil {
		t.Fatalf("expected vector drop success: %v", err)
	}
	_, err = exec.callDbIndexVectorDrop("CALL db.index.vector.drop 'rel_vec_idx'")
	if err == nil {
		t.Fatal("expected missing parentheses error for vector drop")
	}
}

func TestVectorPropertyProcedureBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Node"},
		Properties: map[string]interface{}{},
	})
	if err != nil {
		t.Fatalf("seed node failed: %v", err)
	}
	err = store.CreateEdge(&storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n1",
		Type:       "SELF",
		Properties: map[string]interface{}{},
	})
	if err != nil {
		t.Fatalf("seed edge failed: %v", err)
	}

	// Success paths.
	result, err := exec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty('n1','embedding',[1.0,2.0])")
	if err != nil {
		t.Fatalf("setNodeVectorProperty success path failed: %v", err)
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Errorf("Expected void node setter result, got %#v", result)
	}
	result, err = exec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty('e1','embedding',[1.0,2.0])")
	if err != nil {
		t.Fatalf("setRelationshipVectorProperty success path failed: %v", err)
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Errorf("Expected void relationship setter result, got %#v", result)
	}

	// Argument/syntax errors.
	_, err = exec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty")
	if err == nil {
		t.Fatal("expected invalid syntax error for setNodeVectorProperty")
	}
	_, err = exec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty('n1')")
	if err == nil {
		t.Fatal("expected requires 3 arguments error for setNodeVectorProperty")
	}
	_, err = exec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty('n1','embedding')")
	if err == nil {
		t.Fatal("expected missing vector argument error for setNodeVectorProperty")
	}
	_, err = exec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty('missing','embedding',[1.0])")
	if err == nil || !strings.Contains(err.Error(), "node not found") {
		t.Fatalf("expected node not found error, got: %v", err)
	}

	_, err = exec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty")
	if err == nil {
		t.Fatal("expected invalid syntax error for setRelationshipVectorProperty")
	}
	_, err = exec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty('e1')")
	if err == nil {
		t.Fatal("expected requires 3 arguments error for setRelationshipVectorProperty")
	}
	_, err = exec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty('e1','embedding')")
	if err == nil {
		t.Fatal("expected missing vector argument error for setRelationshipVectorProperty")
	}
	_, err = exec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty('missing','embedding',[1.0])")
	if err == nil || !strings.Contains(err.Error(), "relationship not found") {
		t.Fatalf("expected relationship not found error, got: %v", err)
	}

	// Update failure branches.
	errStore := &updateErrorEngine{
		Engine:  store,
		nodeErr: errors.New("update node boom"),
		edgeErr: errors.New("update edge boom"),
	}
	errExec := NewStorageExecutor(errStore)

	_, err = errExec.callDbCreateSetNodeVectorProperty(ctx, "CALL db.create.setNodeVectorProperty('n1','embedding',[1.0])")
	if err == nil || !strings.Contains(err.Error(), "failed to update node") {
		t.Fatalf("expected failed to update node error, got: %v", err)
	}
	_, err = errExec.callDbCreateSetRelationshipVectorProperty(ctx, "CALL db.create.setRelationshipVectorProperty('e1','embedding',[1.0])")
	if err == nil || !strings.Contains(err.Error(), "failed to update relationship") {
		t.Fatalf("expected failed to update relationship error, got: %v", err)
	}
}

func TestLegacyIndexProcedureErrorBranches_Additional(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Node vector index: invalid keyword and missing parentheses.
	_, err := exec.callDbIndexVectorCreateNodeIndex(ctx, "CALL db.index.vector.nope('x')")
	if err == nil {
		t.Fatal("expected invalid createNodeIndex syntax error")
	}
	_, err = exec.callDbIndexVectorCreateNodeIndex(ctx, "CALL db.index.vector.createNodeIndex 'x'")
	if err == nil {
		t.Fatal("expected missing parentheses for createNodeIndex")
	}
	_, err = exec.callDbIndexVectorCreateNodeIndex(ctx, "CALL db.index.vector.createNodeIndex('x')")
	if err == nil {
		t.Fatal("expected too-few-args error for createNodeIndex")
	}

	// Relationship vector index: invalid keyword and missing parentheses.
	_, err = exec.callDbIndexVectorCreateRelationshipIndex(ctx, "CALL db.index.vector.nope('x')")
	if err == nil {
		t.Fatal("expected invalid createRelationshipIndex syntax error")
	}
	_, err = exec.callDbIndexVectorCreateRelationshipIndex(ctx, "CALL db.index.vector.createRelationshipIndex 'x'")
	if err == nil {
		t.Fatal("expected missing parentheses for createRelationshipIndex")
	}

	// Fulltext create node/relationship: invalid keyword.
	_, err = exec.callDbIndexFulltextCreateNodeIndex(ctx, "CALL db.index.fulltext.nope('x')")
	if err == nil {
		t.Fatal("expected invalid fulltext node index syntax error")
	}
	_, err = exec.callDbIndexFulltextCreateRelationshipIndex(ctx, "CALL db.index.fulltext.nope('x')")
	if err == nil {
		t.Fatal("expected invalid fulltext relationship index syntax error")
	}
}

// TestCompositeIndexUnnamed tests composite index without explicit name
func TestCompositeIndexUnnamed(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index without name
	_, err := exec.Execute(ctx, "CREATE INDEX FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create unnamed composite index: %v", err)
	}

	// Verify index was created with auto-generated name
	schema := store.GetSchema()
	indexes := schema.GetIndexes()

	// Should have the composite index (name will be auto-generated)
	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)

			if label == "Person" && len(props) == 2 {
				if props[0] == "firstName" && props[1] == "lastName" {
					found = true
					// Check auto-generated name (should be lowercase)
					if name != "index_Person_firstName_lastName" {
						t.Errorf("Unexpected auto-generated name: %s", name)
					}
					break
				}
			}
		}
	}

	if !found {
		t.Error("Unnamed composite index not found in schema")
	}
}

// TestCompositeIndexThreeProperties tests composite index with three properties
func TestCompositeIndexThreeProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index with three properties
	_, err := exec.Execute(ctx, "CREATE INDEX address_idx FOR (a:Address) ON (a.city, a.state, a.zipCode)", nil)
	if err != nil {
		t.Fatalf("Failed to create 3-property composite index: %v", err)
	}

	// Verify index
	schema := store.GetSchema()
	indexes := schema.GetIndexes()

	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)

			if name == "address_idx" && label == "Address" {
				if len(props) == 3 {
					if props[0] == "city" && props[1] == "state" && props[2] == "zipCode" {
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		t.Error("3-property composite index not found in schema")
	}
}

// TestCompositeIndexWithSpaces tests parsing with various spacing
func TestCompositeIndexWithSpaces(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{"minimal_spaces", "CREATE INDEX test1 FOR (n:Node) ON (n.a,n.b)"},
		{"extra_spaces", "CREATE INDEX test2 FOR (n:Node) ON (n.a , n.b)"},
		{"lots_of_spaces", "CREATE INDEX test3 FOR (n:Node) ON ( n.a , n.b , n.c )"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				t.Errorf("Failed to create index with spacing variation: %v", err)
			}
		})
	}
}

// TestCompositeIndexIfNotExists tests IF NOT EXISTS clause with composite indexes
func TestCompositeIndexIfNotExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index
	_, err := exec.Execute(ctx, "CREATE INDEX person_idx IF NOT EXISTS FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Create again with IF NOT EXISTS - should not error
	_, err = exec.Execute(ctx, "CREATE INDEX person_idx IF NOT EXISTS FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Errorf("IF NOT EXISTS should not error on duplicate: %v", err)
	}
}

// TestSinglePropertyIndexStillWorks tests that single-property indexes still work
func TestSinglePropertyIndexStillWorks(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create single-property index
	_, err := exec.Execute(ctx, "CREATE INDEX name_idx FOR (p:Person) ON (p.name)", nil)
	if err != nil {
		t.Fatalf("Failed to create single-property index: %v", err)
	}

	// Verify
	schema := store.GetSchema()
	indexes := schema.GetIndexes()

	found := false
	for _, idxInterface := range indexes {
		if idx, ok := idxInterface.(map[string]interface{}); ok {
			name, _ := idx["name"].(string)
			label, _ := idx["label"].(string)
			props, _ := idx["properties"].([]string)

			if name == "name_idx" && label == "Person" {
				if len(props) == 1 && props[0] == "name" {
					found = true
					break
				}
			}
		}
	}

	if !found {
		t.Error("Single-property index not found in schema")
	}
}

// TestCompositeIndexQueryOptimization tests that composite indexes can be used in queries
func TestCompositeIndexQueryOptimization(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create composite index
	_, err := exec.Execute(ctx, "CREATE INDEX person_name_idx FOR (p:Person) ON (p.firstName, p.lastName)", nil)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	// Create test data
	_, err = exec.Execute(ctx, `
		CREATE (p1:Person {firstName: 'John', lastName: 'Doe', age: 30}),
		       (p2:Person {firstName: 'Jane', lastName: 'Doe', age: 25}),
		       (p3:Person {firstName: 'John', lastName: 'Smith', age: 35})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Query using both properties (should benefit from composite index)
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE p.firstName = 'John' AND p.lastName = 'Doe'
		RETURN p.firstName, p.lastName, p.age
	`, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should find exactly one match
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}

	if result.Rows[0][0] != "John" || result.Rows[0][1] != "Doe" || result.Rows[0][2] != int64(30) {
		t.Errorf("Unexpected result: %v", result.Rows[0])
	}
}

// TestParseIndexProperties tests the parseIndexProperties helper function
func TestParseIndexProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"single", "n.name", []string{"name"}},
		{"two_props", "n.firstName, n.lastName", []string{"firstName", "lastName"}},
		{"three_props", "n.a, n.b, n.c", []string{"a", "b", "c"}},
		{"with_spaces", "n.a , n.b , n.c", []string{"a", "b", "c"}},
		{"no_spaces", "n.a,n.b,n.c", []string{"a", "b", "c"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := exec.parseIndexProperties(tc.input)

			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d properties, got %d", len(tc.expected), len(result))
				return
			}

			for i, prop := range result {
				if prop != tc.expected[i] {
					t.Errorf("Property %d: expected %s, got %s", i, tc.expected[i], prop)
				}
			}
		})
	}
}
