package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaManager(t *testing.T) {
	sm := NewSchemaManager()

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		err := sm.AddUniqueConstraint("test_constraint", "User", "email")
		if err != nil {
			t.Fatalf("Failed to add constraint: %v", err)
		}

		// Add again without IF NOT EXISTS - should error
		err = sm.AddUniqueConstraint("test_constraint", "User", "email")
		if err == nil {
			t.Fatalf("Expected error for duplicate constraint without IF NOT EXISTS")
		}

		// Add again with IF NOT EXISTS - should be idempotent
		err = sm.AddUniqueConstraint("test_constraint", "User", "email", true)
		if err != nil {
			t.Fatalf("Failed with IF NOT EXISTS: %v", err)
		}

		constraints := sm.GetConstraints()
		if len(constraints) != 1 {
			t.Errorf("Expected 1 constraint, got %d", len(constraints))
		}
	})

	t.Run("CheckUniqueConstraint", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddUniqueConstraint("email_unique", "User", "email")

		// First value should be fine
		err := sm.CheckUniqueConstraint("User", "email", "test@example.com", "")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Register the value
		sm.RegisterUniqueValue("User", "email", "test@example.com", "node1")

		// Same value should fail
		err = sm.CheckUniqueConstraint("User", "email", "test@example.com", "")
		if err == nil {
			t.Error("Expected constraint violation error")
		}

		// Same value with same node ID should be OK (update case)
		err = sm.CheckUniqueConstraint("User", "email", "test@example.com", "node1")
		if err != nil {
			t.Errorf("Unexpected error for same node: %v", err)
		}

		// No constraint on different property
		err = sm.CheckUniqueConstraint("User", "name", "test@example.com", "")
		if err != nil {
			t.Errorf("Unexpected error for unconstrained property: %v", err)
		}
	})

	t.Run("UniqueConstraintNumericKeysUseCompareValuesSemantics", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("age_unique", "User", "age"))

		sm.RegisterUniqueValue("User", "age", int(1), "node1")

		got, found, constrained := sm.LookupUniqueConstraintValue("User", "age", int64(1))
		require.True(t, constrained)
		require.True(t, found)
		assert.Equal(t, NodeID("node1"), got)

		err := sm.CheckUniqueConstraint("User", "age", float64(1), "")
		require.Error(t, err)

		sm.UnregisterUniqueValue("User", "age", int64(1))
		err = sm.CheckUniqueConstraint("User", "age", float64(1), "")
		require.NoError(t, err)
	})

	t.Run("LookupUniqueConstraintValue ignores non-comparable values", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("test_tags_unique", "User", "tags"))

		require.NotPanics(t, func() {
			nodeID, valueFound, constraintExists := sm.LookupUniqueConstraintValue("User", "tags", []string{"a"})
			require.Empty(t, nodeID)
			require.True(t, constraintExists)
			require.False(t, valueFound)
		})
	})

	t.Run("UnregisterUniqueValue", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddUniqueConstraint("id_unique", "Node", "id")
		sm.RegisterUniqueValue("Node", "id", "test-1", "node1")

		// Should fail before unregister
		err := sm.CheckUniqueConstraint("Node", "id", "test-1", "")
		if err == nil {
			t.Error("Expected constraint violation")
		}

		// Unregister
		sm.UnregisterUniqueValue("Node", "id", "test-1")

		// Should succeed after unregister
		err = sm.CheckUniqueConstraint("Node", "id", "test-1", "")
		if err != nil {
			t.Errorf("Unexpected error after unregister: %v", err)
		}
	})

	t.Run("AddPropertyIndex", func(t *testing.T) {
		sm := NewSchemaManager()
		err := sm.AddPropertyIndex("name_idx", "User", []string{"name"})
		if err != nil {
			t.Fatalf("Failed to add property index: %v", err)
		}

		// Idempotent
		err = sm.AddPropertyIndex("name_idx", "User", []string{"name"})
		if err != nil {
			t.Fatalf("Failed to add property index again: %v", err)
		}

		indexes := nonLookupIndexes(sm.GetIndexes())
		if len(indexes) != 1 {
			t.Errorf("Expected 1 index, got %d", len(indexes))
		}
	})

	t.Run("AddFulltextIndex", func(t *testing.T) {
		sm := NewSchemaManager()
		err := sm.AddFulltextIndex("search_idx", []string{"User", "Post"}, []string{"content", "title"})
		if err != nil {
			t.Fatalf("Failed to add fulltext index: %v", err)
		}

		// Idempotent
		err = sm.AddFulltextIndex("search_idx", []string{"User", "Post"}, []string{"content", "title"})
		if err != nil {
			t.Fatalf("Failed to add fulltext index again: %v", err)
		}

		idx, exists := sm.GetFulltextIndex("search_idx")
		if !exists {
			t.Error("Fulltext index not found")
		}
		if len(idx.Labels) != 2 {
			t.Errorf("Expected 2 labels, got %d", len(idx.Labels))
		}
		if len(idx.Properties) != 2 {
			t.Errorf("Expected 2 properties, got %d", len(idx.Properties))
		}

		// Non-existent index
		_, exists = sm.GetFulltextIndex("nonexistent")
		if exists {
			t.Error("Expected index not to exist")
		}
	})

	t.Run("AddVectorIndex", func(t *testing.T) {
		sm := NewSchemaManager()
		err := sm.AddVectorIndex("embedding_idx", "Document", "embedding", 1536, "cosine")
		if err != nil {
			t.Fatalf("Failed to add vector index: %v", err)
		}

		// Idempotent
		err = sm.AddVectorIndex("embedding_idx", "Document", "embedding", 1536, "cosine")
		if err != nil {
			t.Fatalf("Failed to add vector index again: %v", err)
		}

		idx, exists := sm.GetVectorIndex("embedding_idx")
		if !exists {
			t.Error("Vector index not found")
		}
		if idx.Dimensions != 1536 {
			t.Errorf("Expected 1536 dimensions, got %d", idx.Dimensions)
		}
		if idx.SimilarityFunc != "cosine" {
			t.Errorf("Expected cosine similarity, got %s", idx.SimilarityFunc)
		}

		// Non-existent index
		_, exists = sm.GetVectorIndex("nonexistent")
		if exists {
			t.Error("Expected index not to exist")
		}
	})

	t.Run("GetIndexes", func(t *testing.T) {
		sm := NewSchemaManager()

		sm.AddPropertyIndex("prop_idx", "User", []string{"name"})
		sm.AddFulltextIndex("ft_idx", []string{"User"}, []string{"bio"})
		sm.AddVectorIndex("vec_idx", "User", "embedding", 768, "euclidean")

		indexes := nonLookupIndexes(sm.GetIndexes())
		if len(indexes) != 3 {
			t.Errorf("Expected 3 indexes, got %d", len(indexes))
		}

		// Verify types
		types := make(map[string]int)
		for _, idx := range indexes {
			m := idx.(map[string]interface{})
			types[m["type"].(string)]++
		}
		if types["PROPERTY"] != 1 {
			t.Errorf("Expected 1 PROPERTY index, got %d", types["PROPERTY"])
		}
		if types["FULLTEXT"] != 1 {
			t.Errorf("Expected 1 FULLTEXT index, got %d", types["FULLTEXT"])
		}
		if types["VECTOR"] != 1 {
			t.Errorf("Expected 1 VECTOR index, got %d", types["VECTOR"])
		}
	})

	t.Run("MultipleConstraints", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddUniqueConstraint("user_email", "User", "email")
		sm.AddUniqueConstraint("user_username", "User", "username")
		sm.AddUniqueConstraint("post_id", "Post", "id")

		constraints := sm.GetConstraints()
		if len(constraints) != 3 {
			t.Errorf("Expected 3 constraints, got %d", len(constraints))
		}
	})
}

func TestSchemaManager_DropConstraintAndIndexHelpers(t *testing.T) {
	t.Run("DropConstraint handles standard and property type constraints", func(t *testing.T) {
		sm := NewSchemaManager()
		if err := sm.AddUniqueConstraint("user_email_unique", "User", "email"); err != nil {
			t.Fatalf("AddUniqueConstraint failed: %v", err)
		}
		if err := sm.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger); err != nil {
			t.Fatalf("AddPropertyTypeConstraint failed: %v", err)
		}

		if err := sm.DropConstraint("user_email_unique"); err != nil {
			t.Fatalf("DropConstraint(unique) failed: %v", err)
		}
		if err := sm.CheckUniqueConstraint("User", "email", "a@example.com", ""); err != nil {
			t.Fatalf("expected dropped unique constraint to stop enforcing, got %v", err)
		}

		if err := sm.DropConstraint("user_age_type"); err != nil {
			t.Fatalf("DropConstraint(property type) failed: %v", err)
		}
		if got := sm.GetAllPropertyTypeConstraints(); len(got) != 0 {
			t.Fatalf("expected property type constraints to be empty, got %d", len(got))
		}

		if err := sm.DropConstraint("missing"); err == nil {
			t.Fatal("expected missing constraint error")
		}
	})

	t.Run("DropConstraint rolls back on persist errors", func(t *testing.T) {
		sm := NewSchemaManager()
		if err := sm.AddUniqueConstraint("user_email_unique", "User", "email"); err != nil {
			t.Fatalf("AddUniqueConstraint failed: %v", err)
		}
		if err := sm.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger); err != nil {
			t.Fatalf("AddPropertyTypeConstraint failed: %v", err)
		}
		sm.persist = func(def *SchemaDefinition) error {
			return fmt.Errorf("persist failed")
		}

		if err := sm.DropConstraint("user_email_unique"); err == nil {
			t.Fatal("expected persist rollback error for unique constraint")
		}
		if len(sm.GetConstraints()) == 0 {
			t.Fatal("expected dropped unique constraint to be restored after persist error")
		}

		if err := sm.DropConstraint("user_age_type"); err == nil {
			t.Fatal("expected persist rollback error for property type constraint")
		}
		if len(sm.GetAllPropertyTypeConstraints()) == 0 {
			t.Fatal("expected dropped property type constraint to be restored after persist error")
		}
	})

	t.Run("GetRangeIndex and GetIndexStats expose registered indexes", func(t *testing.T) {
		sm := NewSchemaManager()
		if err := sm.AddPropertyIndex("user_email_idx", "User", []string{"email"}); err != nil {
			t.Fatalf("AddPropertyIndex failed: %v", err)
		}
		if err := sm.PropertyIndexInsert("User", "email", NodeID("u1"), "a@example.com"); err != nil {
			t.Fatalf("PropertyIndexInsert 1 failed: %v", err)
		}
		if err := sm.PropertyIndexInsert("User", "email", NodeID("u2"), "a@example.com"); err != nil {
			t.Fatalf("PropertyIndexInsert 2 failed: %v", err)
		}
		if err := sm.AddRangeIndex("user_age_idx", "User", "age"); err != nil {
			t.Fatalf("AddRangeIndex failed: %v", err)
		}
		if err := sm.RangeIndexInsert("user_age_idx", NodeID("u1"), 10); err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
		if err := sm.AddCompositeIndex("user_geo_idx", "User", []string{"country", "city"}); err != nil {
			t.Fatalf("AddCompositeIndex failed: %v", err)
		}
		if idx, ok := sm.compositeIndexes["user_geo_idx"]; ok {
			idx.fullIndex[fmt.Sprintf("%v", []interface{}{"US", "NYC"})] = []NodeID{"u1", "u2"}
		} else {
			t.Fatal("expected composite index to be registered")
		}
		if err := sm.AddFulltextIndex("user_text_idx", []string{"User"}, []string{"bio"}); err != nil {
			t.Fatalf("AddFulltextIndex failed: %v", err)
		}
		if err := sm.AddVectorIndex("user_vec_idx", "User", "embedding", 3, "cosine"); err != nil {
			t.Fatalf("AddVectorIndex failed: %v", err)
		}

		if idx, ok := sm.GetRangeIndex("user_age_idx"); !ok || idx == nil {
			t.Fatal("expected GetRangeIndex to return registered range index")
		}
		if idx, ok := sm.GetRangeIndex("missing"); ok || idx != nil {
			t.Fatal("expected missing range index lookup to fail")
		}

		stats := sm.GetIndexStats()
		if len(stats) != 5 {
			t.Fatalf("expected 5 index stats entries, got %d", len(stats))
		}

		seen := map[string]IndexStats{}
		for _, stat := range stats {
			seen[stat.Name] = stat
		}
		if seen["user_email_idx"].TotalEntries != 2 || seen["user_email_idx"].UniqueValues != 1 {
			t.Fatalf("unexpected property index stats: %+v", seen["user_email_idx"])
		}
		if seen["user_age_idx"].TotalEntries != 1 || seen["user_age_idx"].Type != "RANGE" {
			t.Fatalf("unexpected range index stats: %+v", seen["user_age_idx"])
		}
		if seen["user_geo_idx"].TotalEntries != 2 || seen["user_geo_idx"].Type != "COMPOSITE" {
			t.Fatalf("unexpected composite index stats: %+v", seen["user_geo_idx"])
		}
		if seen["user_text_idx"].Type != "FULLTEXT" || seen["user_vec_idx"].Type != "VECTOR" {
			t.Fatalf("unexpected fulltext/vector stats: fulltext=%+v vector=%+v", seen["user_text_idx"], seen["user_vec_idx"])
		}
	})
}

func TestSchemaManager_PersistRollbackAndRangeDeleteBranches(t *testing.T) {
	t.Run("AddConstraint rolls back both maps when persist fails", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.persist = func(def *SchemaDefinition) error {
			return fmt.Errorf("persist failed")
		}

		err := sm.AddConstraint(Constraint{
			Name:       "user_email_unique",
			Type:       ConstraintUnique,
			Label:      "User",
			Properties: []string{"email"},
		})
		if err == nil {
			t.Fatal("expected persist failure")
		}
		if len(sm.GetAllConstraints()) != 0 {
			t.Fatal("constraint map should be rolled back on persist failure")
		}
		if len(sm.GetConstraints()) != 0 {
			t.Fatal("unique constraints map should be rolled back on persist failure")
		}
	})

	t.Run("AddRangeIndex and AddVectorIndex roll back on persist failure", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.persist = func(def *SchemaDefinition) error {
			return fmt.Errorf("persist failed")
		}

		if err := sm.AddRangeIndex("ridx", "User", "age"); err == nil {
			t.Fatal("expected range index persist failure")
		}
		if _, ok := sm.GetRangeIndex("ridx"); ok {
			t.Fatal("range index should be rolled back on persist failure")
		}

		if err := sm.AddVectorIndex("vidx", "User", "embedding", 3, "cosine"); err == nil {
			t.Fatal("expected vector index persist failure")
		}
		if _, ok := sm.GetVectorIndex("vidx"); ok {
			t.Fatal("vector index should be rolled back on persist failure")
		}
	})

	t.Run("RangeIndexDelete no-op path and deleteEntryLocked miss path", func(t *testing.T) {
		sm := NewSchemaManager()
		if err := sm.AddRangeIndex("age_idx", "User", "age"); err != nil {
			t.Fatalf("AddRangeIndex failed: %v", err)
		}

		// Not present -> should be no-op.
		if err := sm.RangeIndexDelete("age_idx", "missing"); err != nil {
			t.Fatalf("RangeIndexDelete missing failed: %v", err)
		}

		if err := sm.RangeIndexInsert("age_idx", "u1", 10); err != nil {
			t.Fatalf("RangeIndexInsert failed: %v", err)
		}
		idx, ok := sm.GetRangeIndex("age_idx")
		if !ok || idx == nil {
			t.Fatal("expected range index")
		}
		idx.mu.Lock()
		removed := idx.deleteEntryLocked("nope", 10)
		idx.mu.Unlock()
		if removed {
			t.Fatal("deleteEntryLocked should return false for non-existent nodeID")
		}
	})
}

func TestMemoryEngineConstraintIntegration(t *testing.T) {
	t.Run("ConstraintEnforcementOnCreate", func(t *testing.T) {
		store := NewMemoryEngine()

		// Add constraint
		store.GetSchemaForNamespace("test").AddUniqueConstraint("email_unique", "User", "email")

		// First node succeeds
		node1 := &Node{
			ID:     NodeID(prefixTestID("user1")),
			Labels: []string{"User"},
			Properties: map[string]any{
				"email": "test@example.com",
				"name":  "Alice",
			},
		}
		_, err := store.CreateNode(node1)
		if err != nil {
			t.Fatalf("Failed to create first node: %v", err)
		}

		// Second node with same email fails
		node2 := &Node{
			ID:     NodeID(prefixTestID("user2")),
			Labels: []string{"User"},
			Properties: map[string]any{
				"email": "test@example.com",
				"name":  "Bob",
			},
		}
		_, err = store.CreateNode(node2)
		if err == nil {
			t.Fatal("Expected constraint violation error")
		}

		// Different email succeeds
		node3 := &Node{
			ID:     NodeID(prefixTestID("user3")),
			Labels: []string{"User"},
			Properties: map[string]any{
				"email": "different@example.com",
				"name":  "Charlie",
			},
		}
		_, err = store.CreateNode(node3)
		if err != nil {
			t.Fatalf("Failed to create node with different email: %v", err)
		}
	})

	t.Run("ConstraintOnMultipleLabels", func(t *testing.T) {
		store := NewMemoryEngine()

		store.GetSchemaForNamespace("test").AddUniqueConstraint("id_unique", "Entity", "id")

		// Node with Entity label
		node1 := &Node{
			ID:     NodeID(prefixTestID("n1")),
			Labels: []string{"Entity", "User"},
			Properties: map[string]any{
				"id": "unique-id-1",
			},
		}
		_, err := store.CreateNode(node1)
		if err != nil {
			t.Fatalf("Failed to create first node: %v", err)
		}

		// Another node with Entity label and same id should fail
		node2 := &Node{
			ID:     NodeID(prefixTestID("n2")),
			Labels: []string{"Entity", "Post"},
			Properties: map[string]any{
				"id": "unique-id-1",
			},
		}
		_, err = store.CreateNode(node2)
		if err == nil {
			t.Fatal("Expected constraint violation")
		}
	})

	t.Run("NoConstraintForDifferentLabel", func(t *testing.T) {
		store := NewMemoryEngine()

		store.GetSchemaForNamespace("test").AddUniqueConstraint("user_email", "User", "email")

		// Create User with email
		user := &Node{
			ID:     NodeID(prefixTestID("user1")),
			Labels: []string{"User"},
			Properties: map[string]any{
				"email": "test@example.com",
			},
		}
		store.CreateNode(user)

		// Post with same email property should succeed (different label)
		post := &Node{
			ID:     NodeID(prefixTestID("post1")),
			Labels: []string{"Post"},
			Properties: map[string]any{
				"email": "test@example.com",
			},
		}
		_, err := store.CreateNode(post)
		if err != nil {
			t.Errorf("Unexpected error for different label: %v", err)
		}
	})
}

// =============================================================================
// COMPOSITE INDEX TESTS
// =============================================================================

func TestCompositeKey(t *testing.T) {
	t.Run("CreateCompositeKey", func(t *testing.T) {
		key := NewCompositeKey("US", "NYC", "10001")

		if len(key.Values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(key.Values))
		}
		if key.Hash == "" {
			t.Error("Expected non-empty hash")
		}
		if key.Values[0] != "US" || key.Values[1] != "NYC" || key.Values[2] != "10001" {
			t.Errorf("Values mismatch: %v", key.Values)
		}
	})

	t.Run("CompositeKeyDeterminism", func(t *testing.T) {
		// Same values should produce same hash
		key1 := NewCompositeKey("US", "NYC", 10001)
		key2 := NewCompositeKey("US", "NYC", 10001)

		if key1.Hash != key2.Hash {
			t.Error("Expected identical hashes for same values")
		}
	})

	t.Run("CompositeKeyDifferentValues", func(t *testing.T) {
		key1 := NewCompositeKey("US", "NYC")
		key2 := NewCompositeKey("US", "LA")

		if key1.Hash == key2.Hash {
			t.Error("Expected different hashes for different values")
		}
	})

	t.Run("CompositeKeyTypeAwareness", func(t *testing.T) {
		// String "10" vs int 10 should produce different keys
		key1 := NewCompositeKey("10")
		key2 := NewCompositeKey(10)

		if key1.Hash == key2.Hash {
			t.Error("Expected different hashes for different types")
		}
	})

	t.Run("CompositeKeyString", func(t *testing.T) {
		key := NewCompositeKey("US", "NYC", 10001)
		str := key.String()

		if str != "US, NYC, 10001" {
			t.Errorf("Expected 'US, NYC, 10001', got '%s'", str)
		}
	})
}

func TestCompositeIndex(t *testing.T) {
	t.Run("CreateCompositeIndex", func(t *testing.T) {
		sm := NewSchemaManager()

		err := sm.AddCompositeIndex("location_idx", "User", []string{"country", "city", "zipcode"})
		if err != nil {
			t.Fatalf("Failed to create composite index: %v", err)
		}

		idx, exists := sm.GetCompositeIndex("location_idx")
		if !exists {
			t.Fatal("Composite index not found")
		}

		if idx.Name != "location_idx" {
			t.Errorf("Expected name 'location_idx', got '%s'", idx.Name)
		}
		if idx.Label != "User" {
			t.Errorf("Expected label 'User', got '%s'", idx.Label)
		}
		if len(idx.Properties) != 3 {
			t.Errorf("Expected 3 properties, got %d", len(idx.Properties))
		}
	})

	t.Run("CompositeIndexRequiresMultipleProperties", func(t *testing.T) {
		sm := NewSchemaManager()

		// Single property should fail
		err := sm.AddCompositeIndex("single_idx", "User", []string{"name"})
		if err == nil {
			t.Error("Expected error for single-property composite index")
		}

		// Empty should fail
		err = sm.AddCompositeIndex("empty_idx", "User", []string{})
		if err == nil {
			t.Error("Expected error for empty-property composite index")
		}
	})

	t.Run("CompositeIndexIdempotent", func(t *testing.T) {
		sm := NewSchemaManager()

		err := sm.AddCompositeIndex("idx", "User", []string{"a", "b"})
		if err != nil {
			t.Fatalf("First creation failed: %v", err)
		}

		// Adding again should succeed (idempotent)
		err = sm.AddCompositeIndex("idx", "User", []string{"a", "b"})
		if err != nil {
			t.Fatalf("Idempotent creation failed: %v", err)
		}

		// Verify only one exists
		indexes := sm.GetCompositeIndexesForLabel("User")
		if len(indexes) != 1 {
			t.Errorf("Expected 1 index, got %d", len(indexes))
		}
	})

	t.Run("GetCompositeIndexesForLabel", func(t *testing.T) {
		sm := NewSchemaManager()

		sm.AddCompositeIndex("user_loc", "User", []string{"country", "city"})
		sm.AddCompositeIndex("user_demo", "User", []string{"age", "gender"})
		sm.AddCompositeIndex("post_time", "Post", []string{"year", "month"})

		userIndexes := sm.GetCompositeIndexesForLabel("User")
		if len(userIndexes) != 2 {
			t.Errorf("Expected 2 User indexes, got %d", len(userIndexes))
		}

		postIndexes := sm.GetCompositeIndexesForLabel("Post")
		if len(postIndexes) != 1 {
			t.Errorf("Expected 1 Post index, got %d", len(postIndexes))
		}

		otherIndexes := sm.GetCompositeIndexesForLabel("Other")
		if len(otherIndexes) != 0 {
			t.Errorf("Expected 0 Other indexes, got %d", len(otherIndexes))
		}
	})
}

func TestCompositeIndexOperations(t *testing.T) {
	t.Run("IndexAndLookupFull", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city", "zipcode"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		// Index some nodes
		idx.IndexNode("user1", map[string]interface{}{
			"country": "US",
			"city":    "NYC",
			"zipcode": "10001",
		})
		idx.IndexNode("user2", map[string]interface{}{
			"country": "US",
			"city":    "NYC",
			"zipcode": "10002",
		})
		idx.IndexNode("user3", map[string]interface{}{
			"country": "US",
			"city":    "LA",
			"zipcode": "90001",
		})

		// Full lookup
		results := idx.LookupFull("US", "NYC", "10001")
		if len(results) != 1 || results[0] != "user1" {
			t.Errorf("Expected [user1], got %v", results)
		}

		// Non-existent lookup
		results = idx.LookupFull("US", "NYC", "99999")
		if results != nil {
			t.Errorf("Expected nil for non-existent key, got %v", results)
		}

		// Partial lookup should fail with LookupFull
		results = idx.LookupFull("US", "NYC")
		if results != nil {
			t.Errorf("Expected nil for partial key with LookupFull, got %v", results)
		}
	})

	t.Run("IndexAndLookupPrefix", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city", "zipcode"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		// Index some nodes
		idx.IndexNode("user1", map[string]interface{}{
			"country": "US",
			"city":    "NYC",
			"zipcode": "10001",
		})
		idx.IndexNode("user2", map[string]interface{}{
			"country": "US",
			"city":    "NYC",
			"zipcode": "10002",
		})
		idx.IndexNode("user3", map[string]interface{}{
			"country": "US",
			"city":    "LA",
			"zipcode": "90001",
		})
		idx.IndexNode("user4", map[string]interface{}{
			"country": "UK",
			"city":    "London",
			"zipcode": "SW1A",
		})

		// Prefix lookup: country only
		results := idx.LookupPrefix("US")
		if len(results) != 3 {
			t.Errorf("Expected 3 US users, got %d: %v", len(results), results)
		}

		// Prefix lookup: country + city
		results = idx.LookupPrefix("US", "NYC")
		if len(results) != 2 {
			t.Errorf("Expected 2 NYC users, got %d: %v", len(results), results)
		}

		// Full key via LookupPrefix should work too
		results = idx.LookupPrefix("US", "NYC", "10001")
		if len(results) != 1 {
			t.Errorf("Expected 1 user for full prefix, got %d", len(results))
		}

		// Empty prefix should return nil
		results = idx.LookupPrefix()
		if results != nil {
			t.Errorf("Expected nil for empty prefix, got %v", results)
		}
	})

	t.Run("RemoveNode", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		props := map[string]interface{}{
			"country": "US",
			"city":    "NYC",
		}

		// Index and verify
		idx.IndexNode("user1", props)
		results := idx.LookupFull("US", "NYC")
		if len(results) != 1 {
			t.Fatal("Index failed")
		}

		// Remove and verify
		idx.RemoveNode("user1", props)
		results = idx.LookupFull("US", "NYC")
		if results != nil {
			t.Errorf("Expected nil after removal, got %v", results)
		}

		// Prefix should also be gone
		results = idx.LookupPrefix("US")
		if results != nil {
			t.Errorf("Expected nil prefix after removal, got %v", results)
		}
	})

	t.Run("PartialProperties", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city", "zipcode"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		// Node with only some properties
		idx.IndexNode("user1", map[string]interface{}{
			"country": "US",
			"city":    "NYC",
			// No zipcode
		})

		// Should be findable by prefix
		results := idx.LookupPrefix("US")
		if len(results) != 1 {
			t.Errorf("Expected 1 result for prefix, got %d", len(results))
		}

		results = idx.LookupPrefix("US", "NYC")
		if len(results) != 1 {
			t.Errorf("Expected 1 result for full available prefix, got %d", len(results))
		}

		// But not findable by full lookup (missing property)
		results = idx.LookupFull("US", "NYC", "10001")
		if results != nil {
			t.Errorf("Expected nil for full lookup on partial data, got %v", results)
		}
	})

	t.Run("LookupWithFilter", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		// Index some nodes - we'll filter by ID for simplicity
		idx.IndexNode("user1", map[string]interface{}{"country": "US", "city": "NYC"})
		idx.IndexNode("user2", map[string]interface{}{"country": "US", "city": "NYC"})
		idx.IndexNode("user3", map[string]interface{}{"country": "US", "city": "NYC"})

		// Filter to only even-numbered users (user2)
		results := idx.LookupWithFilter(func(id NodeID) bool {
			return id == "user2"
		}, "US", "NYC")

		if len(results) != 1 || results[0] != "user2" {
			t.Errorf("Expected [user2], got %v", results)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("location_idx", "User", []string{"country", "city", "zipcode"})
		idx, _ := sm.GetCompositeIndex("location_idx")

		idx.IndexNode("user1", map[string]interface{}{"country": "US", "city": "NYC", "zipcode": "10001"})
		idx.IndexNode("user2", map[string]interface{}{"country": "US", "city": "LA", "zipcode": "90001"})

		stats := idx.Stats()

		if stats["name"] != "location_idx" {
			t.Errorf("Expected name 'location_idx', got %v", stats["name"])
		}
		if stats["fullIndexEntries"].(int) != 2 {
			t.Errorf("Expected 2 full index entries, got %v", stats["fullIndexEntries"])
		}
		// Prefix entries: user1 creates US, US|NYC, US|NYC|10001
		//                 user2 creates US (exists), US|LA, US|LA|90001
		// So unique prefixes: US, US|NYC, US|NYC|10001, US|LA, US|LA|90001 = 5
		// But US is shared, so there are 5 total prefix entries
		if stats["prefixEntries"].(int) < 4 {
			t.Errorf("Expected at least 4 prefix entries, got %v", stats["prefixEntries"])
		}
	})
}

func TestCompositeIndexConcurrency(t *testing.T) {
	sm := NewSchemaManager()
	sm.AddCompositeIndex("test_idx", "User", []string{"a", "b"})
	idx, _ := sm.GetCompositeIndex("test_idx")

	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()

			for i := 0; i < opsPerGoroutine; i++ {
				nodeID := NodeID(prefixTestID(fmt.Sprintf("node-%d-%d", gid, i)))
				props := map[string]interface{}{
					"a": fmt.Sprintf("a-%d", gid),
					"b": fmt.Sprintf("b-%d", i),
				}

				// Index
				idx.IndexNode(nodeID, props)

				// Lookup
				idx.LookupFull(fmt.Sprintf("a-%d", gid), fmt.Sprintf("b-%d", i))
				idx.LookupPrefix(fmt.Sprintf("a-%d", gid))

				// Remove every other
				if i%2 == 0 {
					idx.RemoveNode(nodeID, props)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify no panic occurred (test passes if we get here)
	stats := idx.Stats()
	t.Logf("Final stats: %v", stats)
}

func TestCompositeIndexInGetIndexes(t *testing.T) {
	sm := NewSchemaManager()

	sm.AddPropertyIndex("prop_idx", "User", []string{"name"})
	sm.AddCompositeIndex("comp_idx", "User", []string{"country", "city"})
	sm.AddFulltextIndex("ft_idx", []string{"User"}, []string{"bio"})
	sm.AddVectorIndex("vec_idx", "User", "embedding", 768, "cosine")

	indexes := nonLookupIndexes(sm.GetIndexes())
	if len(indexes) != 4 {
		t.Errorf("Expected 4 indexes, got %d", len(indexes))
	}

	// Count types
	types := make(map[string]int)
	for _, idx := range indexes {
		m := idx.(map[string]interface{})
		types[m["type"].(string)]++
	}

	if types["PROPERTY"] != 1 {
		t.Errorf("Expected 1 PROPERTY index, got %d", types["PROPERTY"])
	}
	if types["COMPOSITE"] != 1 {
		t.Errorf("Expected 1 COMPOSITE index, got %d", types["COMPOSITE"])
	}
	if types["FULLTEXT"] != 1 {
		t.Errorf("Expected 1 FULLTEXT index, got %d", types["FULLTEXT"])
	}
	if types["VECTOR"] != 1 {
		t.Errorf("Expected 1 VECTOR index, got %d", types["VECTOR"])
	}
}

func TestCompositeIndexEdgeCases(t *testing.T) {
	t.Run("NilValues", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "User", []string{"a", "b"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		// Indexing with nil value
		err := idx.IndexNode("user1", map[string]interface{}{
			"a": nil,
			"b": "test",
		})
		if err != nil {
			t.Errorf("Unexpected error with nil value: %v", err)
		}

		// Should be findable
		results := idx.LookupFull(nil, "test")
		if len(results) != 1 {
			t.Errorf("Expected 1 result with nil key, got %d", len(results))
		}
	})

	t.Run("EmptyStringValue", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "User", []string{"a", "b"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		idx.IndexNode("user1", map[string]interface{}{
			"a": "",
			"b": "test",
		})

		results := idx.LookupFull("", "test")
		if len(results) != 1 {
			t.Errorf("Expected 1 result with empty string, got %d", len(results))
		}
	})

	t.Run("ComplexTypes", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "Node", []string{"int_val", "float_val", "bool_val"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		idx.IndexNode("node1", map[string]interface{}{
			"int_val":   42,
			"float_val": 3.14,
			"bool_val":  true,
		})

		results := idx.LookupFull(42, 3.14, true)
		if len(results) != 1 {
			t.Errorf("Expected 1 result with complex types, got %d", len(results))
		}

		// Different types should not match
		results = idx.LookupFull("42", 3.14, true)
		if len(results) != 0 {
			t.Errorf("Expected 0 results for wrong type, got %d", len(results))
		}
	})

	t.Run("DuplicateIndexing", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "User", []string{"a", "b"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		props := map[string]interface{}{"a": "x", "b": "y"}

		// Index same node twice
		idx.IndexNode("user1", props)
		idx.IndexNode("user1", props)

		// Should still only have one entry
		results := idx.LookupFull("x", "y")
		if len(results) != 1 {
			t.Errorf("Expected 1 result (no duplicates), got %d", len(results))
		}
	})

	t.Run("RemoveNonexistent", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "User", []string{"a", "b"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		// Should not panic
		idx.RemoveNode("nonexistent", map[string]interface{}{"a": "x", "b": "y"})
	})

	t.Run("LargePrefixCount", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.AddCompositeIndex("test_idx", "User", []string{"a", "b", "c", "d", "e"})
		idx, _ := sm.GetCompositeIndex("test_idx")

		// Index a node with all 5 properties
		idx.IndexNode("user1", map[string]interface{}{
			"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
		})

		// Should create 5 prefix entries + 1 full entry
		stats := idx.Stats()
		if stats["prefixEntries"].(int) != 5 {
			t.Errorf("Expected 5 prefix entries, got %v", stats["prefixEntries"])
		}
		if stats["fullIndexEntries"].(int) != 1 {
			t.Errorf("Expected 1 full entry, got %v", stats["fullIndexEntries"])
		}
	})
}

func BenchmarkCompositeIndex(b *testing.B) {
	sm := NewSchemaManager()
	sm.AddCompositeIndex("bench_idx", "User", []string{"country", "city", "zipcode"})
	idx, _ := sm.GetCompositeIndex("bench_idx")

	// Pre-populate with some data
	for i := 0; i < 10000; i++ {
		idx.IndexNode(NodeID(prefixTestID(fmt.Sprintf("user-%d", i))), map[string]interface{}{
			"country": fmt.Sprintf("country-%d", i%100),
			"city":    fmt.Sprintf("city-%d", i%1000),
			"zipcode": fmt.Sprintf("zip-%d", i),
		})
	}

	b.Run("IndexNode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.IndexNode(NodeID(prefixTestID(fmt.Sprintf("bench-user-%d", i))), map[string]interface{}{
				"country": "US",
				"city":    "NYC",
				"zipcode": fmt.Sprintf("1000%d", i),
			})
		}
	})

	b.Run("LookupFull", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.LookupFull("country-1", "city-1", "zip-1")
		}
	})

	b.Run("LookupPrefix", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.LookupPrefix("country-1")
		}
	})

	b.Run("LookupPrefixTwoProps", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.LookupPrefix("country-1", "city-1")
		}
	})
}

func TestSchemaManager_PersistenceErrorRollback(t *testing.T) {
	persistErr := errors.New("disk full")

	t.Run("AddUniqueConstraint rolls back on persist error", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.SetPersister(func(def *SchemaDefinition) error { return persistErr })

		err := sm.AddUniqueConstraint("uc1", "Person", "email")
		require.ErrorIs(t, err, persistErr)

		// Constraint should NOT be registered
		constraints := sm.GetConstraints()
		assert.Len(t, constraints, 0)
	})

	t.Run("AddPropertyTypeConstraint rolls back on persist error", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.SetPersister(func(def *SchemaDefinition) error { return persistErr })

		err := sm.AddPropertyTypeConstraint("ptc1", "Person", "age", PropertyTypeInteger)
		require.ErrorIs(t, err, persistErr)

		// Should NOT be registered
		ptcs := sm.GetAllPropertyTypeConstraints()
		assert.Len(t, ptcs, 0)
	})

	t.Run("AddPropertyIndex rolls back on persist error", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.SetPersister(func(def *SchemaDefinition) error { return persistErr })

		err := sm.AddPropertyIndex("idx1", "Person", []string{"name"})
		require.ErrorIs(t, err, persistErr)

		// Should NOT be registered
		indexes := nonLookupIndexes(sm.GetIndexes())
		assert.Len(t, indexes, 0)
	})

	t.Run("AddCompositeIndex rolls back on persist error", func(t *testing.T) {
		sm := NewSchemaManager()
		sm.SetPersister(func(def *SchemaDefinition) error { return persistErr })

		err := sm.AddCompositeIndex("cidx1", "Person", []string{"first", "last"})
		require.ErrorIs(t, err, persistErr)

		// Should NOT be registered
		_, exists := sm.GetCompositeIndex("cidx1")
		assert.False(t, exists)
	})

	t.Run("AddPropertyTypeConstraint duplicate errors without IF NOT EXISTS", func(t *testing.T) {
		sm := NewSchemaManager()
		err := sm.AddPropertyTypeConstraint("ptc1", "Person", "age", PropertyTypeInteger)
		require.NoError(t, err)

		// Adding same name again without IF NOT EXISTS should error
		err = sm.AddPropertyTypeConstraint("ptc1", "Person", "age", PropertyTypeInteger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")

		// Adding same name with IF NOT EXISTS should be no-op
		err = sm.AddPropertyTypeConstraintWithOptions("ptc1", "Person", "age", PropertyTypeInteger, PropertyTypeConstraintOptions{IfNotExists: true})
		assert.NoError(t, err)
	})
}

func TestValidateConstraintOnCreation_UnknownType(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "bad",
		Type:       ConstraintType("INVALID"),
		Label:      "Person",
		Properties: []string{"name"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown constraint type")
}

func TestValidateConstraintOnCreation_UniqueViolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create two nodes with the same property value
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"email": "dup@test.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"email": "dup@test.com"}})
	require.NoError(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "unique_email",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	assert.Equal(t, ConstraintUnique, cve.Type)
}

func TestValidateConstraintOnCreation_ExistenceViolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create a node missing the required property
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "exists_name",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"name"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	assert.Equal(t, ConstraintExists, cve.Type)
}

func TestValidateConstraintOnCreation_UniqueMultipleProperties(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// UNIQUE constraint requires exactly 1 property
	err := ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "bad_unique",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"first", "last"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 1 property")
}

func TestValidateExistenceRelationshipConstraint(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes and an edge without the required property
	n1 := testNode("rel-n1")
	n2 := testNode("rel-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("rel-e1")), StartNode: n1.ID, EndNode: n2.ID,
		Type: "WORKS_AT", Properties: map[string]interface{}{},
	}))

	// Validate existence constraint on relationship property
	rc := RelationshipConstraint{
		Name:       "rel_since",
		RelType:    "WORKS_AT",
		Properties: []string{"since"},
	}
	err = engine.validateExistenceRelationshipConstraint(rc)
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	assert.Equal(t, ConstraintExists, cve.Type)
}

func TestValidateExistenceRelationshipConstraint_MultipleProperties(t *testing.T) {
	engine := createTestBadgerEngine(t)

	rc := RelationshipConstraint{
		Name:       "bad_rel",
		RelType:    "WORKS_AT",
		Properties: []string{"a", "b"},
	}
	err := engine.validateExistenceRelationshipConstraint(rc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 1 property")
}

func TestValidateConstraintOnCreation_NodeKeyViolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create two nodes with the same composite key
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John", "last": "Doe"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John", "last": "Doe"}})
	require.NoError(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "nk_person",
		Type:       ConstraintNodeKey,
		Label:      "Person",
		Properties: []string{"first", "last"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	assert.Equal(t, ConstraintNodeKey, cve.Type)
}

func TestValidateConstraintOnCreation_NodeKeyNullProperty(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John"}})
	require.NoError(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "nk_person2",
		Type:       ConstraintNodeKey,
		Label:      "Person",
		Properties: []string{"first", "last"},
	})
	// Null property on NODE KEY — should return violation
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	assert.Equal(t, ConstraintNodeKey, cve.Type)
}

func TestValidateConstraintOnCreation_TemporalOverlap(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create two nodes with overlapping temporal intervals for the same key
	_, err := engine.CreateNode(&Node{
		ID: "test:n1", Labels: []string{"Contract"},
		Properties: map[string]interface{}{
			"contract_id": "C1",
			"valid_from":  "2024-01-01T00:00:00Z",
			"valid_to":    "2024-12-31T00:00:00Z",
		},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{
		ID: "test:n2", Labels: []string{"Contract"},
		Properties: map[string]interface{}{
			"contract_id": "C1",
			"valid_from":  "2024-06-01T00:00:00Z",
			"valid_to":    "2025-06-01T00:00:00Z",
		},
	})
	require.NoError(t, err)

	err = ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "temporal_contract",
		Type:       ConstraintTemporal,
		Label:      "Contract",
		Properties: []string{"contract_id", "valid_from", "valid_to"},
	})
	require.Error(t, err)
}

func TestValidateConstraintOnCreation_TemporalBadProperties(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "bad_temporal",
		Type:       ConstraintTemporal,
		Label:      "Contract",
		Properties: []string{"only_one"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "3 properties")
}

func TestValidateConstraintOnCreation_ExistenceMultipleProperties(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := ValidateConstraintOnCreationForEngine(engine, Constraint{
		Name:       "bad_exists",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"a", "b"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 1 property")
}

func TestSchemaManager_AddFulltextIndex_PersistError(t *testing.T) {
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error { return errors.New("fail") })

	err := sm.AddFulltextIndex("ft1", []string{"Person"}, []string{"bio"})
	require.Error(t, err)

	// Should be rolled back
	indexes := nonLookupIndexes(sm.GetIndexes())
	assert.Len(t, indexes, 0)
}

func TestSchemaManager_AddVectorIndex_PersistError(t *testing.T) {
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error { return errors.New("fail") })

	err := sm.AddVectorIndex("vi1", "Document", "embedding", 128, "cosine")
	require.Error(t, err)

	// Should be rolled back
	indexes := nonLookupIndexes(sm.GetIndexes())
	assert.Len(t, indexes, 0)
}

func TestSchemaManager_AddRangeIndex_PersistError(t *testing.T) {
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error { return errors.New("fail") })

	err := sm.AddRangeIndex("ri1", "Person", "age")
	require.Error(t, err)

	// Should be rolled back
	indexes := nonLookupIndexes(sm.GetIndexes())
	assert.Len(t, indexes, 0)
}

func TestSchemaManager_AddVectorIndex_Idempotent(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddVectorIndex("vi1", "Document", "embedding", 128, "cosine"))
	require.NoError(t, sm.AddVectorIndex("vi1", "Document", "embedding", 128, "cosine")) // no-op
}

func TestSchemaManager_AddRangeIndex_Idempotent(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddRangeIndex("ri1", "Person", "age"))
	require.NoError(t, sm.AddRangeIndex("ri1", "Person", "age")) // no-op
}

func TestSchemaManager_AddFulltextIndex_Idempotent(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("ft1", []string{"Person"}, []string{"bio"}))
	require.NoError(t, sm.AddFulltextIndex("ft1", []string{"Person"}, []string{"bio"})) // no-op
}

func TestCompositeIndex_LookupWithFilter(t *testing.T) {
	idx := &CompositeIndex{
		Name:        "test_filter",
		Label:       "User",
		Properties:  []string{"country", "city"},
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}

	idx.IndexNode("test:n1", map[string]interface{}{"country": "US", "city": "NYC"})
	idx.IndexNode("test:n2", map[string]interface{}{"country": "US", "city": "LA"})
	idx.IndexNode("test:n3", map[string]interface{}{"country": "UK", "city": "LDN"})

	// Filter by prefix "US", then apply filter to only keep n1
	results := idx.LookupWithFilter(func(id NodeID) bool {
		return id == "test:n1"
	}, "US")
	assert.Len(t, results, 1)
	assert.Equal(t, NodeID("test:n1"), results[0])

	// Filter with no prefix match
	results = idx.LookupWithFilter(func(id NodeID) bool { return true }, "JP")
	assert.Nil(t, results)
}

func TestCompositeIndex_Stats(t *testing.T) {
	idx := &CompositeIndex{
		Name:        "stats_idx",
		Label:       "User",
		Properties:  []string{"country", "city"},
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}

	idx.IndexNode("test:n1", map[string]interface{}{"country": "US", "city": "NYC"})

	stats := idx.Stats()
	assert.Equal(t, "stats_idx", stats["name"])
	assert.Equal(t, "User", stats["label"])
	assert.Equal(t, 1, stats["fullIndexEntries"])
}

func TestSchemaManager_LookupPrefix_FullMatch(t *testing.T) {
	idx := &CompositeIndex{
		Name:        "prefix_full",
		Label:       "User",
		Properties:  []string{"country", "city"},
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}

	idx.IndexNode("test:n1", map[string]interface{}{"country": "US", "city": "NYC"})

	// LookupPrefix with all properties is a full match
	results := idx.LookupPrefix("US", "NYC")
	assert.Len(t, results, 1)

	// LookupPrefix with no values returns nil
	results = idx.LookupPrefix()
	assert.Nil(t, results)

	// LookupPrefix with too many values returns nil
	results = idx.LookupPrefix("US", "NYC", "extra")
	assert.Nil(t, results)
}

func TestCompositeIndex_RemoveNode(t *testing.T) {
	idx := &CompositeIndex{
		Name:        "test_idx",
		Label:       "User",
		Properties:  []string{"country", "city"},
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}

	// Index a node
	idx.IndexNode("test:n1", map[string]interface{}{"country": "US", "city": "NYC"})
	idx.IndexNode("test:n2", map[string]interface{}{"country": "US", "city": "LA"})

	// Verify indexed
	results := idx.LookupFull("US", "NYC")
	assert.Len(t, results, 1)
	prefixResults := idx.LookupPrefix("US")
	assert.Len(t, prefixResults, 2)

	// Remove n1
	idx.RemoveNode("test:n1", map[string]interface{}{"country": "US", "city": "NYC"})

	// Full lookup for NYC should return empty
	results = idx.LookupFull("US", "NYC")
	assert.Len(t, results, 0)

	// Prefix lookup for US should only have n2
	prefixResults = idx.LookupPrefix("US")
	assert.Len(t, prefixResults, 1)
}

func TestSchemaManager_PropertyIndexTopK(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("idx_src", "MongoDocument", []string{"sourceId"}))
	require.NoError(t, sm.PropertyIndexInsert("MongoDocument", "sourceId", NodeID("n3"), "src-003"))
	require.NoError(t, sm.PropertyIndexInsert("MongoDocument", "sourceId", NodeID("n1"), "src-001"))
	require.NoError(t, sm.PropertyIndexInsert("MongoDocument", "sourceId", NodeID("n2"), "src-002"))
	require.NoError(t, sm.PropertyIndexInsert("MongoDocument", "sourceId", NodeID("n4"), "src-004"))
	require.NoError(t, sm.PropertyIndexInsert("MongoDocument", "sourceId", NodeID("n-nil"), nil))

	asc := sm.PropertyIndexTopK("MongoDocument", "sourceId", 2, false)
	require.Equal(t, []NodeID{"n1", "n2"}, asc)

	desc := sm.PropertyIndexTopK("MongoDocument", "sourceId", 2, true)
	require.Equal(t, []NodeID{"n4", "n3"}, desc)
}
