package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// txEdgeConstraintFixture spins up a BadgerEngine on a tempdir, registers the
// requested constraint in the "test" namespace, and seeds four endpoint nodes.
// The transaction-level constraint helpers (checkEdgeUniqueness,
// checkEdgeTemporalConstraint, checkEdgeCardinality, checkEdgePolicy) are
// reached through tx.CreateEdge → tx.Commit → tx.validateEdgeConstraints.
func txEdgeConstraintFixture(t *testing.T, c Constraint) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))

	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
		_, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Endpoint"},
			Properties: map[string]any{},
		})
		require.NoError(t, err, "CreateNode(%q)", id)
	}
	return engine
}

// TestTxCheckEdgeUniqueness_PendingDuplicate exercises the same-tx
// pendingEdges path: two edges with the same constrained property
// staged in one tx must conflict on Commit.
func TestTxCheckEdgeUniqueness_PendingDuplicate(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	}))

	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.Equal(t, "OWNS", cve.Label)
}

// TestTxCheckEdgeUniqueness_AgainstCommitted: an edge committed in an earlier
// tx must conflict with a same-token edge staged in a later tx.
func TestTxCheckEdgeUniqueness_AgainstCommitted(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	}))

	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.Contains(t, cve.Message, "test:e1",
		"violation message should reference the conflicting committed edge")
}

// TestTxCheckEdgeUniqueness_NullValueAllowed: a NULL constrained property
// must not block the write (Neo4j-style: NULL is unconstrained).
func TestTxCheckEdgeUniqueness_NullValueAllowed(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{},
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "OWNS", Properties: map[string]any{},
	}))
	require.NoError(t, tx.Commit())
}

// TestTxCheckEdgeUniqueness_CompositeKey exercises the multi-property branch.
func TestTxCheckEdgeUniqueness_CompositeKey(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "unique_pair",
		Type:       ConstraintRelationshipKey,
		EntityType: ConstraintEntityRelationship,
		Label:      "TAG",
		Properties: []string{"k1", "k2"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "TAG", Properties: map[string]any{"k1": "x", "k2": "y"},
	}))
	// Same composite — must violate.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "TAG", Properties: map[string]any{"k1": "x", "k2": "y"},
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, []string{"k1", "k2"}, cve.Properties)
}

// TestTxCheckEdgeUniqueness_CompositeKey_PartialNull: when not every key
// component is present, the constraint short-circuits to "not constrained".
func TestTxCheckEdgeUniqueness_CompositeKey_PartialNull(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "unique_pair",
		Type:       ConstraintRelationshipKey,
		EntityType: ConstraintEntityRelationship,
		Label:      "TAG",
		Properties: []string{"k1", "k2"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "TAG", Properties: map[string]any{"k1": "x"}, // k2 missing
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "TAG", Properties: map[string]any{"k1": "x"}, // k2 missing
	}))
	err = tx.Commit()
	// EXISTS-style key check on RELATIONSHIP_KEY rejects partial keys
	// before uniqueness gets a chance — that's also the correct
	// Cypher DDL behavior. Either way, missing k2 must NOT silently
	// succeed.
	require.Error(t, err)
}

// TestTxCheckEdgeTemporalConstraint_NoOverlap_Pending: two pending edges
// with overlapping intervals on the same key must conflict.
func TestTxCheckEdgeTemporalConstraint_NoOverlap_Pending(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Properties: []string{"role", "valid_from", "valid_to"},
	})

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t4 := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "captain", "valid_from": t1, "valid_to": t2,
		},
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "captain", "valid_from": t3, "valid_to": t4,
		},
	}))

	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintTemporal, cve.Type)
}

// TestTxCheckEdgeTemporalConstraint_NoOverlap_DifferentKey: different role
// values produce two non-overlapping intervals → both must succeed.
func TestTxCheckEdgeTemporalConstraint_NoOverlap_DifferentKey(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Properties: []string{"role", "valid_from", "valid_to"},
	})

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "captain", "valid_from": t1, "valid_to": t2,
		},
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "first-mate", "valid_from": t1, "valid_to": t2,
		},
	}))
	require.NoError(t, tx.Commit())
}

// TestTxCheckEdgeTemporalConstraint_AgainstCommitted: an existing committed
// edge must be checked against new pending edges.
func TestTxCheckEdgeTemporalConstraint_AgainstCommitted(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Properties: []string{"role", "valid_from", "valid_to"},
	})

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "captain", "valid_from": t1, "valid_to": t2,
		},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	t3 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t4 := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:d",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role": "captain", "valid_from": t3, "valid_to": t4,
		},
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintTemporal, cve.Type)
}

// TestTxCheckEdgeTemporalConstraint_NullKey: nil key prop is rejected.
func TestTxCheckEdgeTemporalConstraint_NullKey(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Properties: []string{"role", "valid_from", "valid_to"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"valid_from": time.Now(), "valid_to": time.Now().Add(time.Hour),
			// "role" missing → null → rejected by checkEdgeTemporalConstraint.
		},
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintTemporal, cve.Type)
	require.Contains(t, cve.Message, "role")
}

// TestTxCheckEdgeTemporalConstraint_NonDateStart: non-coercible start prop
// rejected with a clear message.
func TestTxCheckEdgeTemporalConstraint_NonDateStart(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Properties: []string{"role", "valid_from", "valid_to"},
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGNED",
		Properties: map[string]any{
			"role":       "captain",
			"valid_from": "not-a-date",
			"valid_to":   time.Now().Add(time.Hour),
		},
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Contains(t, cve.Message, "valid_from")
}

// TestTxCheckEdgeCardinality_OutgoingMaxExceeded:
// MaxCount=2 outgoing edges of type X on node a; the 3rd must fail.
func TestTxCheckEdgeCardinality_OutgoingMaxExceeded(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "max_2_outgoing",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "X",
		MaxCount:   2,
		Direction:  "OUTGOING",
	})
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "X",
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "X",
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:d", Type: "X",
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
	require.Contains(t, cve.Message, "outgoing")
	require.Contains(t, cve.Message, "test:a")
}

// TestTxCheckEdgeCardinality_PendingCount: two same-tx pending edges plus
// one already committed must trip the count.
func TestTxCheckEdgeCardinality_PendingCount(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "max_1_outgoing",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "X",
		MaxCount:   1,
		Direction:  "OUTGOING",
	})

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "X",
	}))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "X",
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
}

// TestTxCheckEdgeCardinality_IncomingDirection: incoming-direction
// constraints anchor on EndNode, not StartNode.
func TestTxCheckEdgeCardinality_IncomingDirection(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "max_1_incoming",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "X",
		MaxCount:   1,
		Direction:  "INCOMING",
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "X",
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:b", Type: "X",
	}))
	err = tx.Commit()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
	require.Contains(t, cve.Message, "incoming")
}

// TestTxCheckEdgeCardinality_DeletedExcluded: an edge deleted in the same tx
// must not count toward the cardinality budget.
func TestTxCheckEdgeCardinality_DeletedExcluded(t *testing.T) {
	engine := txEdgeConstraintFixture(t, Constraint{
		Name:       "max_1_outgoing",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "X",
		MaxCount:   1,
		Direction:  "OUTGOING",
	})
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "X",
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Drop the existing edge and add a fresh one — count stays at 1.
	require.NoError(t, tx.DeleteEdge("test:e1"))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "X",
	}))
	require.NoError(t, tx.Commit())
}

// TestTxGetNodeLabels_PendingThenCommittedThenDeleted exercises the three
// branches of tx.getNodeLabels in one test.
func TestTxGetNodeLabels_PendingCommittedDeleted(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	for _, id := range []NodeID{"test:a"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Committed branch: node "test:a" is already on disk.
	got, err := tx.getNodeLabels("test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"Endpoint"}, got)

	// Pending branch overrides committed.
	_, err = tx.CreateNode(&Node{ID: "test:new1", Labels: []string{"Pending"}})
	require.NoError(t, err)
	got, err = tx.getNodeLabels("test:new1")
	require.NoError(t, err)
	require.Equal(t, []string{"Pending"}, got)

	// Deleted branch: even though "test:a" exists committed, after
	// DeleteNode the lookup returns nil (deleted-set takes precedence).
	require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
	got, err = tx.getNodeLabels("test:a")
	require.NoError(t, err)
	require.Nil(t, got)
}
