// Package storage - Comprehensive constraint enforcement tests.
package storage

import (
	"fmt"
	"os"
	"testing"
)

// TestBadgerTransaction_FullScanUniqueConstraint tests UNIQUE constraint with full database scan.
func TestBadgerTransaction_FullScanUniqueConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create UNIQUE constraint
	engine.schema.AddConstraint(Constraint{
		Name:       "unique_email",
		Type:       ConstraintUnique,
		Label:      "User",
		Properties: []string{"email"},
	})

	// Insert first user (should succeed)
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	_, err = tx1.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "alice@example.com",
			"name":  "Alice",
		},
	})
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Try to insert duplicate email in NEW transaction (should fail with full scan)
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	_, err = tx2.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "alice@example.com", // DUPLICATE!
			"name":  "Alice Clone",
		},
	})

	if err == nil {
		t.Fatal("Expected constraint violation error, got nil")
	}

	// Verify error is a ConstraintViolationError
	if _, ok := err.(*ConstraintViolationError); !ok {
		t.Errorf("Expected ConstraintViolationError, got %T: %v", err, err)
	}

	tx2.Rollback()

	// Verify different email succeeds
	tx3, _ := engine.BeginTransaction()
	_, err = tx3.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-3")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "bob@example.com", // Different email
			"name":  "Bob",
		},
	})

	if err != nil {
		t.Errorf("Should allow different email: %v", err)
	}

	tx3.Commit()
}

// TestBadgerTransaction_FullScanNodeKeyConstraint tests NODE KEY constraint across transactions.
func TestBadgerTransaction_FullScanNodeKeyConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create NODE KEY constraint on (username, domain)
	engine.schema.AddConstraint(Constraint{
		Name:       "user_key",
		Type:       ConstraintNodeKey,
		Label:      "User",
		Properties: []string{"username", "domain"},
	})

	// Insert first user
	tx1, _ := engine.BeginTransaction()
	tx1.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"username": "alice",
			"domain":   "example.com",
		},
	})
	tx1.Commit()

	// Try to insert duplicate composite key in NEW transaction
	tx2, _ := engine.BeginTransaction()
	_, err := tx2.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"username": "alice",
			"domain":   "example.com", // Same composite key!
		},
	})

	if err == nil {
		t.Fatal("Expected NODE KEY violation, got nil")
	}

	tx2.Rollback()

	// Different domain should succeed
	tx3, _ := engine.BeginTransaction()
	_, err = tx3.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-3")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"username": "alice",
			"domain":   "other.com", // Different domain
		},
	})

	if err != nil {
		t.Errorf("Should allow different domain: %v", err)
	}

	tx3.Commit()
}

// TestBadgerEngine_ValidateConstraintOnCreation tests constraint validation when CREATE CONSTRAINT is executed.
func TestBadgerEngine_ValidateConstraintOnCreation(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Insert data with duplicates
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "duplicate@example.com",
		},
	})
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "duplicate@example.com", // Duplicate!
		},
	})
	tx.Commit()

	// Try to create UNIQUE constraint (should fail - existing duplicates)
	err := engine.ValidateConstraintOnCreation(Constraint{
		Name:       "unique_email",
		Type:       ConstraintUnique,
		Label:      "User",
		Properties: []string{"email"},
	})

	if err == nil {
		t.Fatal("Expected validation error for existing duplicates, got nil")
	}

	constraintErr, ok := err.(*ConstraintViolationError)
	if !ok {
		t.Errorf("Expected ConstraintViolationError, got %T", err)
	}

	if constraintErr != nil && constraintErr.Type != ConstraintUnique {
		t.Errorf("Expected UNIQUE constraint error, got %s", constraintErr.Type)
	}
}

// TestBadgerEngine_ValidateExistenceConstraintOnCreation tests EXISTS constraint validation.
func TestBadgerEngine_ValidateExistenceConstraintOnCreation(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Insert data with missing required property
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("person-1")),
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"age": 30,
			// Missing "name"
		},
	})
	tx.Commit()

	// Try to create EXISTS constraint (should fail)
	err := engine.ValidateConstraintOnCreation(Constraint{
		Name:       "require_name",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"name"},
	})

	if err == nil {
		t.Fatal("Expected validation error for missing property, got nil")
	}
}

// TestBadgerTransaction_UniqueConstraintWithinTransaction tests UNIQUE within single transaction.
func TestBadgerTransaction_UniqueConstraintWithinTransaction(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	engine.schema.AddConstraint(Constraint{
		Name:       "unique_id",
		Type:       ConstraintUnique,
		Label:      "Node",
		Properties: []string{"id"},
	})

	tx, _ := engine.BeginTransaction()

	// First node - OK
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("node-1")),
		Labels: []string{"Node"},
		Properties: map[string]interface{}{
			"id": "unique-123",
		},
	})

	// Second node with same ID - should fail immediately
	_, err := tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("node-2")),
		Labels: []string{"Node"},
		Properties: map[string]interface{}{
			"id": "unique-123", // Duplicate in same transaction!
		},
	})

	if err == nil {
		t.Fatal("Expected constraint violation within transaction, got nil")
	}

	tx.Rollback()
}

// TestBadgerTransaction_NullValuesAllowed tests that NULL values don't violate UNIQUE.
func TestBadgerTransaction_NullValuesAllowed(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	engine.schema.AddConstraint(Constraint{
		Name:       "unique_optional",
		Type:       ConstraintUnique,
		Label:      "Node",
		Properties: []string{"optionalField"},
	})

	tx, _ := engine.BeginTransaction()

	// Multiple nodes with NULL - should be allowed
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("node-1")),
		Labels:     []string{"Node"},
		Properties: map[string]interface{}{
			// optionalField is NULL
		},
	})

	_, err := tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("node-2")),
		Labels:     []string{"Node"},
		Properties: map[string]interface{}{
			// optionalField is NULL
		},
	})

	if err != nil {
		t.Errorf("NULL values should not violate UNIQUE: %v", err)
	}

	tx.Commit()
}

// TestBadgerTransaction_NodeKeyRequiresAllProperties tests NODE KEY rejects NULL.
func TestBadgerTransaction_NodeKeyRequiresAllProperties(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	engine.schema.AddConstraint(Constraint{
		Name:       "composite_key",
		Type:       ConstraintNodeKey,
		Label:      "Entity",
		Properties: []string{"part1", "part2"},
	})

	tx, _ := engine.BeginTransaction()

	// Missing part2 - should fail
	_, err := tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("entity-1")),
		Labels: []string{"Entity"},
		Properties: map[string]interface{}{
			"part1": "value1",
			// part2 is NULL - not allowed in NODE KEY!
		},
	})

	if err == nil {
		t.Fatal("Expected NODE KEY error for NULL property, got nil")
	}

	constraintErr, ok := err.(*ConstraintViolationError)
	if !ok {
		t.Errorf("Expected ConstraintViolationError, got %T", err)
	}

	if constraintErr != nil && constraintErr.Type != ConstraintNodeKey {
		t.Errorf("Expected NODE KEY error, got %s", constraintErr.Type)
	}
}

// TestBadgerTransaction_ExistenceConstraint tests EXISTS/NOT NULL constraint.
func TestBadgerTransaction_ExistenceConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	engine.schema.AddConstraint(Constraint{
		Name:       "require_email",
		Type:       ConstraintExists,
		Label:      "User",
		Properties: []string{"email"},
	})

	tx, _ := engine.BeginTransaction()

	// Missing required property - should fail
	_, err := tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"name": "Alice",
			// email is missing!
		},
	})

	if err == nil {
		t.Fatal("Expected EXISTS constraint violation, got nil")
	}

	tx.Rollback()

	// With required property - should succeed
	tx2, _ := engine.BeginTransaction()
	_, err = tx2.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"name":  "Bob",
			"email": "bob@example.com", // Required property present
		},
	})

	if err != nil {
		t.Errorf("Should allow node with required property: %v", err)
	}

	tx2.Commit()
}

// TestBadgerTransaction_MultipleConstraints tests multiple constraints on same label.
func TestBadgerTransaction_MultipleConstraints(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Add multiple constraints
	engine.schema.AddConstraint(Constraint{
		Name:       "unique_email",
		Type:       ConstraintUnique,
		Label:      "User",
		Properties: []string{"email"},
	})

	engine.schema.AddConstraint(Constraint{
		Name:       "require_name",
		Type:       ConstraintExists,
		Label:      "User",
		Properties: []string{"name"},
	})

	tx, _ := engine.BeginTransaction()

	// Violates EXISTS constraint
	_, err := tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "alice@example.com",
			// Missing name
		},
	})

	if err == nil {
		t.Fatal("Expected EXISTS constraint violation")
	}

	tx.Rollback()

	// Satisfies all constraints
	tx2, _ := engine.BeginTransaction()
	_, err = tx2.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"email": "bob@example.com",
			"name":  "Bob",
		},
	})

	if err != nil {
		t.Errorf("Should satisfy all constraints: %v", err)
	}

	tx2.Commit()
}

// TestCompareValues tests the compareValues function with different types.
func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"int equal", 42, 42, true},
		{"int not equal", 42, 43, false},
		{"int and int64 equal", 42, int64(42), true},
		{"int and float64 equal", 42, 42.0, true},
		{"string equal", "hello", "hello", true},
		{"string not equal", "hello", "world", false},
		{"bool equal", true, true, true},
		{"bool not equal", true, false, false},
		{"mixed types", 42, "42", false},
		{"float precision", 3.14, 3.14, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestBadgerTransaction_ConstraintAcrossCommits tests constraint enforcement across multiple commits.
func TestBadgerTransaction_ConstraintAcrossCommits(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	engine.schema.AddConstraint(Constraint{
		Name:       "unique_username",
		Type:       ConstraintUnique,
		Label:      "Account",
		Properties: []string{"username"},
	})

	// Transaction 1: Create alice
	tx1, _ := engine.BeginTransaction()
	tx1.CreateNode(&Node{
		ID: NodeID(prefixTestID("account-1")),
		Labels: []string{"Account"},
		Properties: map[string]interface{}{
			"username": "alice",
		},
	})
	tx1.Commit()

	// Transaction 2: Try to create another alice
	tx2, _ := engine.BeginTransaction()
	_, err := tx2.CreateNode(&Node{
		ID: NodeID(prefixTestID("account-2")),
		Labels: []string{"Account"},
		Properties: map[string]interface{}{
			"username": "alice", // Duplicate across transactions!
		},
	})

	if err == nil {
		t.Fatal("Expected UNIQUE constraint violation across commits")
	}

	tx2.Rollback()

	// Transaction 3: Different username should work
	tx3, _ := engine.BeginTransaction()
	_, err = tx3.CreateNode(&Node{
		ID: NodeID(prefixTestID("account-3")),
		Labels: []string{"Account"},
		Properties: map[string]interface{}{
			"username": "bob",
		},
	})

	if err != nil {
		t.Errorf("Different username should succeed: %v", err)
	}

	tx3.Commit()
}

// setupTestBadgerEngine creates a temporary BadgerDB for testing.
func setupTestBadgerEngine(t *testing.T) (*BadgerEngine, func()) {
	dir := fmt.Sprintf("/tmp/badger-test-%d", os.Getpid())

	engine, err := NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create BadgerEngine: %v", err)
	}

	cleanup := func() {
		engine.Close()
		os.RemoveAll(dir)
	}

	return engine, cleanup
}
