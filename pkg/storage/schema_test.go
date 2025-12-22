package storage

import (
	"fmt"
	"sync"
	"testing"
)

func TestSchemaManager(t *testing.T) {
	sm := NewSchemaManager()

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		err := sm.AddUniqueConstraint("test_constraint", "User", "email")
		if err != nil {
			t.Fatalf("Failed to add constraint: %v", err)
		}

		// Add again - should be idempotent
		err = sm.AddUniqueConstraint("test_constraint", "User", "email")
		if err != nil {
			t.Fatalf("Failed to add constraint again: %v", err)
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

		indexes := sm.GetIndexes()
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

		indexes := sm.GetIndexes()
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

func TestMemoryEngineConstraintIntegration(t *testing.T) {
	t.Run("ConstraintEnforcementOnCreate", func(t *testing.T) {
		store := NewMemoryEngine()

		// Add constraint
		store.GetSchema().AddUniqueConstraint("email_unique", "User", "email")

		// First node succeeds
		node1 := &Node{
			ID: NodeID(prefixTestID("user1")),
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
			ID: NodeID(prefixTestID("user2")),
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
			ID: NodeID(prefixTestID("user3")),
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

		store.GetSchema().AddUniqueConstraint("id_unique", "Entity", "id")

		// Node with Entity label
		node1 := &Node{
			ID: NodeID(prefixTestID("n1")),
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
			ID: NodeID(prefixTestID("n2")),
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

		store.GetSchema().AddUniqueConstraint("user_email", "User", "email")

		// Create User with email
		user := &Node{
			ID: NodeID(prefixTestID("user1")),
			Labels: []string{"User"},
			Properties: map[string]any{
				"email": "test@example.com",
			},
		}
		store.CreateNode(user)

		// Post with same email property should succeed (different label)
		post := &Node{
			ID: NodeID(prefixTestID("post1")),
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

	indexes := sm.GetIndexes()
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
