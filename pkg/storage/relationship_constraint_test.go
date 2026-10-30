// Package storage - Relationship property constraint tests.
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBadgerEngine_RelationshipUniqueConstraint tests UNIQUE constraints on relationship properties.
func TestBadgerEngine_RelationshipUniqueConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Create nodes first
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID:     NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
	})
	tx.CreateNode(&Node{
		ID:     NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
	})
	tx.Commit()

	// Create relationships with transaction IDs
	tx2, _ := engine.BeginTransaction()
	tx2.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("txn-1")),
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
		ID:        EdgeID(prefixTestID("txn-2")),
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
		ID:        EdgeID(prefixTestID("knows-1")),
		StartNode: NodeID(prefixTestID("person-1")),
		EndNode:   NodeID(prefixTestID("person-2")),
		Type:      "KNOWS",
		Properties: map[string]interface{}{
			"since": "2020-01-01",
		},
	})
	tx2.Commit()

	// Create relationship WITHOUT required property
	tx3, _ := engine.BeginTransaction()
	tx3.CreateEdge(&Edge{
		ID:         EdgeID(prefixTestID("knows-2")),
		StartNode:  NodeID(prefixTestID("person-2")),
		EndNode:    NodeID(prefixTestID("person-1")),
		Type:       "KNOWS",
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
		ID:        EdgeID(prefixTestID("created-1")),
		StartNode: NodeID(prefixTestID("user-1")),
		EndNode:   NodeID(prefixTestID("post-1")),
		Type:      "CREATED",
		Properties: map[string]interface{}{
			"txid": "TX-123",
		},
	})
	tx2.Commit()

	// Create LIKES relationship with same txid (different type - should be OK)
	tx3, _ := engine.BeginTransaction()
	tx3.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("likes-1")),
		StartNode: NodeID(prefixTestID("user-1")),
		EndNode:   NodeID(prefixTestID("post-1")),
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

func TestBadgerEngine_RelationshipConstraintValidationErrors(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: NodeID(prefixTestID("user-1")), Labels: []string{"User"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: NodeID(prefixTestID("user-2")), Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:         EdgeID(prefixTestID("rel-1")),
		StartNode:  NodeID(prefixTestID("user-1")),
		EndNode:    NodeID(prefixTestID("user-2")),
		Type:       "KNOWS",
		Properties: map[string]any{},
	}))
	require.NoError(t, tx.Commit())

	err = engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "unknown",
		Type:       ConstraintType("WEIRD"),
		RelType:    "KNOWS",
		Properties: []string{"since"},
	})
	require.ErrorContains(t, err, "unsupported relationship constraint type")

	err = engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "bad_unique_arity",
		Type:       ConstraintUnique,
		RelType:    "KNOWS",
		Properties: []string{"since", "weight"},
	})
	require.ErrorContains(t, err, "exactly 1 property")

	err = engine.ValidateRelationshipConstraint(RelationshipConstraint{
		Name:       "bad_exists_arity",
		Type:       ConstraintExists,
		RelType:    "KNOWS",
		Properties: nil,
	})
	require.ErrorContains(t, err, "exactly 1 property")
}

// TestConstraintEntityType verifies that the EntityType field defaults correctly
// and that EffectiveEntityType returns the right value for both node and relationship constraints.
func TestConstraintEntityType(t *testing.T) {
	// A constraint with no EntityType set should default to NODE.
	nodeConstraint := Constraint{
		Name:       "node_unique",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	}
	require.Equal(t, ConstraintEntityType(""), nodeConstraint.EntityType, "zero-value EntityType should be empty string")
	require.Equal(t, ConstraintEntityNode, nodeConstraint.EffectiveEntityType(), "EffectiveEntityType should default to NODE")

	// A constraint explicitly set to NODE.
	nodeConstraint.EntityType = ConstraintEntityNode
	require.Equal(t, ConstraintEntityNode, nodeConstraint.EffectiveEntityType())

	// A constraint set to RELATIONSHIP.
	relConstraint := Constraint{
		Name:       "rel_unique",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "KNOWS",
		Properties: []string{"since"},
	}
	require.Equal(t, ConstraintEntityRelationship, relConstraint.EffectiveEntityType())

	// PropertyTypeConstraint follows the same pattern.
	ptcNode := PropertyTypeConstraint{
		Name:         "ptc_node",
		Label:        "Person",
		Property:     "age",
		ExpectedType: PropertyTypeInteger,
	}
	require.Equal(t, ConstraintEntityNode, ptcNode.EffectiveEntityType(), "PropertyTypeConstraint should default to NODE")

	ptcRel := PropertyTypeConstraint{
		Name:         "ptc_rel",
		EntityType:   ConstraintEntityRelationship,
		Label:        "KNOWS",
		Property:     "since",
		ExpectedType: PropertyTypeInteger,
	}
	require.Equal(t, ConstraintEntityRelationship, ptcRel.EffectiveEntityType())
}

// TestConstraintEntityTypeInSchemaManager verifies that constraints stored in SchemaManager
// preserve their EntityType through AddConstraint and GetAllConstraints.
func TestConstraintEntityTypeInSchemaManager(t *testing.T) {
	sm := NewSchemaManager()

	// Add a node constraint (EntityType left empty — should default to NODE).
	err := sm.AddConstraint(Constraint{
		Name:       "node_unique_email",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	})
	require.NoError(t, err)

	// Add a relationship constraint.
	err = sm.AddConstraint(Constraint{
		Name:       "rel_unique_since",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "KNOWS",
		Properties: []string{"since"},
	})
	require.NoError(t, err)

	allConstraints := sm.GetAllConstraints()
	require.Len(t, allConstraints, 2)

	found := map[string]Constraint{}
	for _, c := range allConstraints {
		found[c.Name] = c
	}

	nodeC := found["node_unique_email"]
	require.Equal(t, ConstraintEntityNode, nodeC.EffectiveEntityType())

	relC := found["rel_unique_since"]
	require.Equal(t, ConstraintEntityRelationship, relC.EffectiveEntityType())
	require.Equal(t, ConstraintEntityRelationship, relC.EntityType)
}

// TestConstraintEntityTypePersistence verifies that EntityType survives export/import.
func TestConstraintEntityTypePersistence(t *testing.T) {
	sm := NewSchemaManager()

	// Add both node and relationship constraints.
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "node_exists",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"name"},
	}))
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "rel_unique",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "KNOWS",
		Properties: []string{"since"},
	}))

	// Export and reimport.
	def := sm.ExportDefinition()
	require.Len(t, def.Constraints, 2)

	sm2 := NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(def))

	all := sm2.GetAllConstraints()
	require.Len(t, all, 2)

	found := map[string]Constraint{}
	for _, c := range all {
		found[c.Name] = c
	}

	require.Equal(t, ConstraintEntityNode, found["node_exists"].EffectiveEntityType())
	require.Equal(t, ConstraintEntityRelationship, found["rel_unique"].EntityType)
}
