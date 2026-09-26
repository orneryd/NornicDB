package storage

import (
	"fmt"
	"testing"
)

func TestSchemaManager_DropIndex_Property(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddPropertyIndex("idx_user_email", "User", []string{"email"}); err != nil {
		t.Fatalf("AddPropertyIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected 1 index, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	if err := sm.DropIndex("idx_user_email"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_Fulltext(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddFulltextIndex("ft_doc_content", []string{"Document"}, []string{"title", "body"}); err != nil {
		t.Fatalf("AddFulltextIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected 1 index, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	if err := sm.DropIndex("ft_doc_content"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_Range(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddRangeIndex("range_age", "Person", "age"); err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected 1 index, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	if err := sm.DropIndex("range_age"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_Vector(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddVectorIndex("vec_embedding", "Document", "embedding", 1024, "cosine"); err != nil {
		t.Fatalf("AddVectorIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected 1 index, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	if err := sm.DropIndex("vec_embedding"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_Composite(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddCompositeIndex("comp_user_loc", "User", []string{"country", "city"}); err != nil {
		t.Fatalf("AddCompositeIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected 1 index, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	if err := sm.DropIndex("comp_user_loc"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	if len(nonLookupIndexes(sm.GetIndexes())) != 0 {
		t.Fatalf("expected 0 indexes after drop, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_NotFound(t *testing.T) {
	sm := NewSchemaManager()
	err := sm.DropIndex("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent index")
	}
	if err.Error() != `index "nonexistent" does not exist` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSchemaManager_DropIndex_PersistRollback(t *testing.T) {
	sm := NewSchemaManager()
	if err := sm.AddRangeIndex("range_x", "Label", "prop"); err != nil {
		t.Fatalf("AddRangeIndex failed: %v", err)
	}

	// Set a persister that always fails.
	sm.SetPersister(func(def *SchemaDefinition) error {
		return fmt.Errorf("persist failure")
	})

	err := sm.DropIndex("range_x")
	if err == nil {
		t.Fatal("expected persist error")
	}

	// Index should still exist after rollback.
	if len(nonLookupIndexes(sm.GetIndexes())) != 1 {
		t.Fatalf("expected index to be rolled back, got %d indexes", len(nonLookupIndexes(sm.GetIndexes())))
	}
}

func TestSchemaManager_DropIndex_MultipleTypes(t *testing.T) {
	sm := NewSchemaManager()
	_ = sm.AddPropertyIndex("idx_a", "A", []string{"x"})
	_ = sm.AddFulltextIndex("ft_b", []string{"B"}, []string{"y"})
	_ = sm.AddRangeIndex("range_c", "C", "z")

	if len(nonLookupIndexes(sm.GetIndexes())) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(nonLookupIndexes(sm.GetIndexes())))
	}

	// Drop the fulltext one.
	if err := sm.DropIndex("ft_b"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	remaining := nonLookupIndexes(sm.GetIndexes())
	if len(remaining) != 2 {
		t.Fatalf("expected 2 indexes after drop, got %d", len(remaining))
	}

	// Verify the right ones remain.
	names := make(map[string]bool)
	for _, idx := range remaining {
		if m, ok := idx.(map[string]interface{}); ok {
			if n, ok := m["name"].(string); ok {
				names[n] = true
			}
		}
	}
	if !names["idx_a"] || !names["range_c"] {
		t.Fatalf("unexpected remaining indexes: %v", names)
	}
}
