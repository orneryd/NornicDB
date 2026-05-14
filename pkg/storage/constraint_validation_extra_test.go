package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefreshUniqueConstraintValuesForEngine_NilArgs(t *testing.T) {
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(nil, nil))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(nil, NewSchemaManager()))

	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(e, nil))
}

func TestRefreshUniqueConstraintValuesForEngine_NoConstraints(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	schema := NewSchemaManager()
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(e, schema))
}

func TestRefreshUniqueConstraintValuesForEngine_RebuildsCache(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	schema := e.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name: "unique_email", Type: ConstraintUnique,
		Label: "User", Properties: []string{"email"},
	}))

	for _, n := range []*Node{
		{ID: "test:a", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}},
		{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"email": "b@example.com"}},
	} {
		_, err := e.CreateNode(n)
		require.NoError(t, err)
	}

	require.NoError(t, RefreshUniqueConstraintValuesForEngine(e, schema))
}

func TestValidateRelationshipConstraintOnCreationForEngine_DispatchesByType(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	for _, n := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(&Node{ID: n, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"v": "x"},
	}))

	// Unique relationship constraint passes when there's no duplicate.
	require.NoError(t, validateRelationshipConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintUnique, Label: "REL", Properties: []string{"v"},
	}))

	// PropertyType is handled separately, returns nil here.
	require.NoError(t, validateRelationshipConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintPropertyType, Label: "REL",
	}))

	// Unsupported relationship constraint type.
	err := validateRelationshipConstraintOnCreationForEngine(e, Constraint{
		Type: "BOGUS", Label: "REL",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

func TestValidateDomainConstraintOnCreationForEngine(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{
		ID: "test:n", Labels: []string{"Order"},
		Properties: map[string]any{"state": "active"},
	})
	require.NoError(t, err)

	// Pass: value in allow-list.
	require.NoError(t, validateDomainConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintDomain, Label: "Order", Properties: []string{"state"},
		AllowedValues: []interface{}{"active", "pending"},
	}))

	// Fail: value not in list.
	_, err = e.CreateNode(&Node{
		ID: "test:bad", Labels: []string{"Order"},
		Properties: map[string]any{"state": "deleted"},
	})
	require.NoError(t, err)
	err = validateDomainConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintDomain, Label: "Order", Properties: []string{"state"},
		AllowedValues: []interface{}{"active", "pending"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintDomain, cve.Type)

	// Validation rejects too-many properties.
	require.Error(t, validateDomainConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintDomain, Properties: []string{"a", "b"},
	}))
	// Validation rejects empty allow-list.
	require.Error(t, validateDomainConstraintOnCreationForEngine(e, Constraint{
		Type: ConstraintDomain, Properties: []string{"a"},
	}))
}

func TestValidateCardinalityOnCreationForEngine(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", StartNode: "a", EndNode: "x", Type: "OWNS"},
		{ID: "e2", StartNode: "a", EndNode: "y", Type: "OWNS"},
		{ID: "e3", StartNode: "a", EndNode: "z", Type: "OWNS"},
	}

	// Outgoing cardinality 5 → pass.
	require.NoError(t, validateCardinalityOnCreationForEngine(edges, Constraint{
		Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 5,
	}))

	// Outgoing cardinality 2 → fail (a has 3).
	err := validateCardinalityOnCreationForEngine(edges, Constraint{
		Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 2,
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
	require.Contains(t, cve.Message, "outgoing")

	// Incoming cardinality counts the END node side.
	incomingEdges := []*Edge{
		{ID: "e1", StartNode: "p", EndNode: "tgt", Type: "ASSIGNED"},
		{ID: "e2", StartNode: "q", EndNode: "tgt", Type: "ASSIGNED"},
	}
	err = validateCardinalityOnCreationForEngine(incomingEdges, Constraint{
		Type: ConstraintCardinality, Label: "ASSIGNED", Direction: "INCOMING", MaxCount: 1,
	})
	require.Error(t, err)
}
