package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestrictedUniqueConstraintDiagnosticPreservesClassification(t *testing.T) {
	const holder = NodeID("internal-holder")
	ctx := t.Context()
	ordinary := uniqueConstraintViolation(ctx, "Item", "key", "duplicate", holder)
	restricted := uniqueConstraintViolation(WithRestrictedConstraintDiagnostics(ctx), "Item", "key", "duplicate", holder)
	require.Equal(t, ordinary.Type, restricted.Type)
	require.Equal(t, ordinary.Label, restricted.Label)
	require.Equal(t, ordinary.Properties, restricted.Properties)
	require.ErrorContains(t, ordinary, string(holder))
	require.ErrorContains(t, restricted, "already exists")
	require.NotContains(t, restricted.Error(), string(holder))
}

func TestRestrictedRelationshipConstraintDiagnosticPreservesClassification(t *testing.T) {
	for _, kind := range []ConstraintType{ConstraintUnique, ConstraintRelationshipKey} {
		t.Run(string(kind), func(t *testing.T) {
			engine := relatedSynchronousKeyEngine(t, Constraint{
				Name: "link_secret_key", Type: kind, EntityType: ConstraintEntityRelationship,
				Label: "LINK", Properties: []string{"key"},
			})
			relatedEndpointNodes(t, engine)
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
				ID: "test:holder-edge", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
				Properties: map[string]any{"key": "held-rel-key"},
			}))
			duplicate := &Edge{
				ID: "test:probe-edge", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
				Properties: map[string]any{"key": "held-rel-key"},
			}
			plain := engine.CreateEdge(t.Context(), duplicate)
			require.ErrorContains(t, plain, "held-rel-key")
			require.ErrorContains(t, plain, "holder-edge")
			restricted := engine.CreateEdge(WithRestrictedConstraintDiagnostics(t.Context()), duplicate)
			var violation *ConstraintViolationError
			require.ErrorAs(t, restricted, &violation)
			require.Equal(t, kind, violation.Type)
			require.Equal(t, "LINK", violation.Label)
			require.Equal(t, []string{"key"}, violation.Properties)
			require.ErrorContains(t, restricted, "already exists")
			require.NotContains(t, restricted.Error(), "held-rel-key")
			require.NotContains(t, restricted.Error(), "holder-edge")

			ctx := WithRestrictedConstraintDiagnostics(t.Context())
			tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			staged := *duplicate
			staged.ID = "test:transaction-probe"
			err = tx.CreateEdge(ctx, &staged)
			if err == nil {
				err = tx.CommitContext(ctx)
			}
			require.ErrorAs(t, err, &violation)
			require.NotContains(t, err.Error(), "held-rel-key")
			require.NotContains(t, err.Error(), "holder-edge")
		})
	}
}

func TestRefreshUniqueConstraintValuesForEngine_NilArgs(t *testing.T) {
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), nil, nil))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), nil, NewSchemaManager()))

	e := newTestMemoryEngine(t)
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), e, nil))
}

func TestRefreshUniqueConstraintValuesForEngine_NoConstraints(t *testing.T) {
	e := newTestMemoryEngine(t)

	schema := NewSchemaManager()
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), e, schema))
}

func TestRefreshUniqueConstraintValuesForEngine_RebuildsCache(t *testing.T) {
	e := newTestMemoryEngine(t)

	schema := e.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name: "unique_email", Type: ConstraintUnique,
		Label: "User", Properties: []string{"email"},
	}))

	for _, n := range []*Node{
		{ID: "test:a", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}},
		{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"email": "b@example.com"}},
	} {
		_, err := e.CreateNode(t.Context(), n)
		require.NoError(t, err)
	}

	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), e, schema))
}

func TestValidateRelationshipConstraintOnCreationForEngine_DispatchesByType(t *testing.T) {
	e := newTestMemoryEngine(t)

	for _, n := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(t.Context(), &Node{ID: n, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(t.Context(), &Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"v": "x"},
	}))

	// Unique relationship constraint passes when there's no duplicate.
	require.NoError(t, validateRelationshipConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: ConstraintUnique, Label: "REL", Properties: []string{"v"},
	}))

	// PropertyType is handled separately, returns nil here.
	require.NoError(t, validateRelationshipConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: ConstraintPropertyType, Label: "REL",
	}))

	// Unsupported relationship constraint type.
	err := validateRelationshipConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: "BOGUS", Label: "REL",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

func TestValidateDomainConstraintOnCreationForEngine(t *testing.T) {
	e := newTestMemoryEngine(t)

	_, err := e.CreateNode(t.Context(), &Node{
		ID: "test:n", Labels: []string{"Order"},
		Properties: map[string]any{"state": "active"},
	})
	require.NoError(t, err)

	// Pass: value in allow-list.
	require.NoError(t, validateDomainConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: ConstraintDomain, Label: "Order", Properties: []string{"state"},
		AllowedValues: []interface{}{"active", "pending"},
	}))

	// Fail: value not in list.
	_, err = e.CreateNode(t.Context(), &Node{
		ID: "test:bad", Labels: []string{"Order"},
		Properties: map[string]any{"state": "deleted"},
	})
	require.NoError(t, err)
	err = validateDomainConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: ConstraintDomain, Label: "Order", Properties: []string{"state"},
		AllowedValues: []interface{}{"active", "pending"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintDomain, cve.Type)

	// Validation rejects too-many properties.
	require.Error(t, validateDomainConstraintOnCreationForEngine(context.Background(), e, Constraint{
		Type: ConstraintDomain, Properties: []string{"a", "b"},
	}))
	// Validation rejects empty allow-list.
	require.Error(t, validateDomainConstraintOnCreationForEngine(context.Background(), e, Constraint{
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
	require.NoError(t, validateCardinalityOnCreationForEngine(context.Background(), edges, Constraint{
		Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 5,
	}))

	// Outgoing cardinality 2 → fail (a has 3).
	err := validateCardinalityOnCreationForEngine(context.Background(), edges, Constraint{
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
	err = validateCardinalityOnCreationForEngine(context.Background(), incomingEdges, Constraint{
		Type: ConstraintCardinality, Label: "ASSIGNED", Direction: "INCOMING", MaxCount: 1,
	})
	require.Error(t, err)
}

func TestValidatePolicyOnCreationForEngine(t *testing.T) {
	e := newTestMemoryEngine(t)

	for _, node := range []*Node{
		{ID: "test:user", Labels: []string{"User"}, Properties: map[string]any{}},
		{ID: "test:secret", Labels: []string{"Secret"}, Properties: map[string]any{}},
		{ID: "test:report", Labels: []string{"Report"}, Properties: map[string]any{}},
	} {
		_, err := e.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:user", EndNode: "test:secret", Type: "READS"}))
	require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: "test:e2", StartNode: "test:user", EndNode: "test:report", Type: "READS"}))

	edges, err := e.AllEdges(context.Background())
	require.NoError(t, err)
	err = validatePolicyOnCreationForEngine(context.Background(), e, edges, Constraint{
		Type: ConstraintPolicy, Label: "READS", PolicyMode: "DISALLOWED", SourceLabel: "User", TargetLabel: "Secret",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "DISALLOWED policy")

	require.NoError(t, e.GetSchema().AddConstraint(Constraint{
		Name: "allow_reports", Type: ConstraintPolicy, Label: "READS", PolicyMode: "ALLOWED", SourceLabel: "User", TargetLabel: "Report",
	}))
	err = validatePolicyOnCreationForEngine(context.Background(), e, edges, Constraint{
		Type: ConstraintPolicy, Label: "READS", PolicyMode: "ALLOWED", SourceLabel: "User", TargetLabel: "Secret",
	})
	require.NoError(t, err)

	err = validatePolicyOnCreationForEngine(context.Background(), e, edges, Constraint{
		Type: ConstraintPolicy, Label: "READS", PolicyMode: "ALLOWED", SourceLabel: "Admin", TargetLabel: "Secret",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ALLOWED policy")
}

func TestValidateRelPropertyTypeOnCreationForEngine(t *testing.T) {
	e := newTestMemoryEngine(t)

	for _, n := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(t.Context(), &Node{ID: n, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: "test:ok", StartNode: "test:a", EndNode: "test:b", Type: "SCORED", Properties: map[string]any{"score": int64(10)}}))
	require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: "test:ignored", StartNode: "test:a", EndNode: "test:b", Type: "OTHER", Properties: map[string]any{"score": "bad"}}))

	require.NoError(t, validateRelPropertyTypeOnCreationForEngine(context.Background(), e, PropertyTypeConstraint{
		EntityType:   ConstraintEntityRelationship,
		Label:        "SCORED",
		Property:     "score",
		ExpectedType: PropertyTypeInteger,
	}))

	require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: "test:bad", StartNode: "test:a", EndNode: "test:b", Type: "SCORED", Properties: map[string]any{"score": "bad"}}))
	err := validateRelPropertyTypeOnCreationForEngine(context.Background(), e, PropertyTypeConstraint{
		EntityType:   ConstraintEntityRelationship,
		Label:        "SCORED",
		Property:     "score",
		ExpectedType: PropertyTypeInteger,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "relationship test:bad property score")
}
