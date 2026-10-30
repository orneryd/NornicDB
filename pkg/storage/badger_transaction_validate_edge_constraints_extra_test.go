package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ValidateEdgeConstraints_EarlyReturns(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	require.NoError(t, tx.validateEdgeConstraints(nil))
	require.NoError(t, tx.validateEdgeConstraints(&Edge{}))
	require.NoError(t, tx.validateEdgeConstraints(&Edge{ID: "test:e", Type: "REL"})) // namespace not pinned

	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.validateEdgeConstraints(&Edge{ID: "test:e", Type: "REL"})) // no constraints
}

func TestBadgerTransaction_ValidateEdgeConstraints_BranchCoverage(t *testing.T) {
	engine := newTestEngine(t)
	for _, n := range []*Node{
		{ID: "test:a", Labels: []string{"Person", "Employee"}, Properties: map[string]any{"dept": "eng"}},
		{ID: "test:b", Labels: []string{"Person", "Department"}, Properties: map[string]any{"dept": "sales"}},
		{ID: "test:c", Labels: []string{"Person", "Employee"}, Properties: map[string]any{"dept": "eng"}},
	} {
		_, err := engine.CreateNode(n)
		require.NoError(t, err)
	}

	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e-existing", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"token": "dup", "rank": int64(7)}}))

	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_exists", Type: ConstraintExists, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_domain", Type: ConstraintDomain, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"status"}, AllowedValues: []any{"ok", "warn"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_card", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "LINK", Direction: "OUTGOING", MaxCount: 1}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINK", PolicyMode: "DISALLOWED", SourceLabel: "Employee", TargetLabel: "Department"}))
	require.NoError(t, sm.AddPropertyTypeConstraint("rel_rank_type", "LINK", "rank", PropertyTypeInteger, ConstraintEntityRelationship))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "rel_temporal", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "TEMP_LINK", Properties: []string{"key", "valid_from", "valid_to"}}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))

	// EXISTS failure branch.
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-missing", StartNode: "test:c", EndNode: "test:c", Type: "LINK", Properties: map[string]any{}})
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintExists, violation.Type)

	// UNIQUE against committed branch.
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-dup", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "dup", "status": "ok", "rank": int64(3)}})
	require.Error(t, err)

	// DOMAIN branch.
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-domain", StartNode: "test:c", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "u2", "status": "bad", "rank": int64(1)}})
	require.Error(t, err)

	// CARDINALITY branch (existing outgoing from test:a already reaches max=1).
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-card", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "u3", "status": "ok", "rank": int64(1)}})
	require.Error(t, err)

	// POLICY branch (Employee -> Department disallowed).
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-policy", StartNode: "test:c", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"token": "u4", "status": "ok", "rank": int64(1)}})
	require.Error(t, err)

	// PROPERTY TYPE branch.
	err = tx.validateEdgeConstraints(&Edge{ID: "test:e-type", StartNode: "test:c", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "u5", "status": "ok", "rank": "not-int"}})
	require.Error(t, err)

	// TEMPORAL success + violation branches.
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, tx.validateEdgeConstraints(&Edge{ID: "test:t1", StartNode: "test:c", EndNode: "test:c", Type: "TEMP_LINK", Properties: map[string]any{"key": "k", "valid_from": start, "valid_to": end}}))
	tx.pendingEdges["test:t-pending"] = &Edge{ID: "test:t-pending", StartNode: "test:c", EndNode: "test:c", Type: "TEMP_LINK", Properties: map[string]any{"key": "k", "valid_from": start.Add(24 * time.Hour), "valid_to": end.Add(24 * time.Hour)}}
	err = tx.validateEdgeConstraints(&Edge{ID: "test:t2", StartNode: "test:c", EndNode: "test:c", Type: "TEMP_LINK", Properties: map[string]any{"key": "k", "valid_from": start, "valid_to": end}})
	require.Error(t, err)
}
