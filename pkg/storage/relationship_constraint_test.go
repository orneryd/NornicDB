// Package storage - Relationship property constraint tests.
package storage

import (
	"testing"
)

// TestBadgerEngine_RelationshipUniqueConstraint tests UNIQUE constraints on relationship properties.
func TestBadgerEngine_RelationshipUniqueConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create nodes first
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
	})
	tx.CreateNode(&Node{
		ID: NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
	})
	tx.Commit()

	// Create relationships with transaction IDs
	tx2, _ := engine.BeginTransaction()
	tx2.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("txn-1")),
		StartNode: NodeID(prefixTestID("user-1")),
		EndNode:   NodeID(prefixTestID("user-2")),
		Type:      "TRANSACTION",
		Properties: map[string]interface{}{
			"txid": "TX-12345",
		},
	})
	tx2.Commit()

	// Try to create duplicate transaction ID (should fail when constraint is checked)
	tx3, _ := engine.BeginTransaction()
	tx3.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("txn-2")),
		StartNode: NodeID(prefixTestID("user-2")),
		EndNode:   NodeID(prefixTestID("user-1")),
		Type:      "TRANSACTION",
		Properties: map[string]interface{}{
			"txid": "TX-12345", // Duplicate!
		},
	})
	tx3.Commit()

	// Validate constraint (simulating CREATE CONSTRAINT)
	err := engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "unique_txid",
		Type:       ConstraintUnique,
		RelType:    "TRANSACTION",
		Properties: []string{"txid"},
	})

	if err == nil {
		t.Fatal("Expected UNIQUE constraint violation on relationship, got nil")
	}

	constraintErr, ok := err.(*ConstraintViolationError)
	if !ok {
		t.Errorf("Expected ConstraintViolationError, got %T", err)
	}

	if constraintErr != nil && constraintErr.Type != ConstraintUnique {
		t.Errorf("Expected UNIQUE constraint error, got %s", constraintErr.Type)
	}
}

// TestBadgerEngine_RelationshipExistenceConstraint tests EXISTS constraints on relationships.
func TestBadgerEngine_RelationshipExistenceConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create nodes
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("person-1")), Labels: []string{"Person"}})
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("person-2")), Labels: []string{"Person"}})
	tx.Commit()

	// Create relationship WITH required property
	tx2, _ := engine.BeginTransaction()
	tx2.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("knows-1")),
		StartNode: NodeID(prefixTestID("person-1")),
		EndNode: NodeID(prefixTestID("person-2")),
		Type:      "KNOWS",
		Properties: map[string]interface{}{
			"since": "2020-01-01",
		},
	})
	tx2.Commit()

	// Create relationship WITHOUT required property
	tx3, _ := engine.BeginTransaction()
	tx3.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("knows-2")),
		StartNode: NodeID(prefixTestID("person-2")),
		EndNode: NodeID(prefixTestID("person-1")),
		Type:      "KNOWS",
		Properties: map[string]interface{}{
			// Missing "since"
		},
	})
	tx3.Commit()

	// Validate EXISTS constraint (should fail)
	err := engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "require_since",
		Type:       ConstraintExists,
		RelType:    "KNOWS",
		Properties: []string{"since"},
	})

	if err == nil {
		t.Fatal("Expected EXISTS constraint violation on relationship, got nil")
	}
}

// TestBadgerEngine_RelationshipConstraintValidTypes tests constraint validation only applies to matching relationship types.
func TestBadgerEngine_RelationshipConstraintValidTypes(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create nodes
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("user-1")), Labels: []string{"User"}})
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("post-1")), Labels: []string{"Post"}})
	tx.Commit()

	// Create CREATED relationship with transaction ID
	tx2, _ := engine.BeginTransaction()
	tx2.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("created-1")),
		StartNode: NodeID(prefixTestID("user-1")),
		EndNode: NodeID(prefixTestID("post-1")),
		Type:      "CREATED",
		Properties: map[string]interface{}{
			"txid": "TX-123",
		},
	})
	tx2.Commit()

	// Create LIKES relationship with same txid (different type - should be OK)
	tx3, _ := engine.BeginTransaction()
	tx3.CreateEdge(&Edge{
		ID: EdgeID(prefixTestID("likes-1")),
		StartNode: NodeID(prefixTestID("user-1")),
		EndNode: NodeID(prefixTestID("post-1")),
		Type:      "LIKES",
		Properties: map[string]interface{}{
			"txid": "TX-123", // Same as above but different relationship type
		},
	})
	tx3.Commit()

	// Validate UNIQUE constraint on CREATED type only (should pass)
	err := engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "unique_created_txid",
		Type:       ConstraintUnique,
		RelType:    "CREATED",
		Properties: []string{"txid"},
	})

	if err != nil {
		t.Errorf("UNIQUE constraint should only apply to matching relationship type: %v", err)
	}
}
