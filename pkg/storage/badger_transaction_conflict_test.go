package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheckNodeCreateConflict_ConcurrentWriteAfterReadTS validates the
// snapshot-isolation conflict check: tx1 begins, tx2 commits a node create,
// tx1 tries to create that same node ID — must fail with ErrConflict.
func TestCheckNodeCreateConflict_ConcurrentWriteAfterReadTS(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	// tx1 is started BEFORE tx2 commits, so tx1.readTS < the new head.
	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx1.Rollback() })

	// tx2 (engine direct write here for simplicity) creates the node.
	_, err = engine.CreateNode(&Node{ID: "test:contended", Labels: []string{"L"}})
	require.NoError(t, err)

	// tx1 staging the same ID must fail at validateAllConstraints time —
	// the node now exists with a head version newer than tx1.readTS.
	_, err = tx1.CreateNode(&Node{ID: "test:contended", Labels: []string{"L"}})
	if err == nil {
		err = tx1.Commit()
	}
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrAlreadyExists),
		"expected conflict or already-exists; got %v", err)
}

// TestCheckEdgeCreateConflict_ConcurrentWriteAfterReadTS: same shape as the
// node test, for edges.
func TestCheckEdgeCreateConflict_ConcurrentWriteAfterReadTS(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx1.Rollback() })

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))

	err = tx1.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	})
	if err == nil {
		err = tx1.Commit()
	}
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrAlreadyExists),
		"expected conflict or already-exists; got %v", err)
}

// TestCheckNodeCreateConflict_NoExistingHead is the no-op path: a fresh ID
// passes the conflict check and the create succeeds.
func TestCheckNodeCreateConflict_NoExistingHead(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	_, err = tx.CreateNode(&Node{ID: "test:fresh-1", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// TestValidatePolicyOnNodeLabelChange_DisallowsViaUpdate: relabeling a node
// in-place must trigger DISALLOWED-policy validation against connected
// edges — Cypher SET/REMOVE label semantics.
func TestValidatePolicyOnNodeLabelChange_DisallowsViaUpdate(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:        "no_admin_to_user",
		Type:        ConstraintPolicy,
		EntityType:  ConstraintEntityRelationship,
		Label:       "MANAGES",
		SourceLabel: "Admin",
		TargetLabel: "User",
		PolicyMode:  "DISALLOWED",
	}))

	// Pre-existing valid graph: Admin->Analyst (allowed since Analyst != User).
	_, err = engine.CreateNode(&Node{ID: "test:admin1", Labels: []string{"Admin"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:analyst1", Labels: []string{"Analyst"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:admin1", EndNode: "test:analyst1", Type: "MANAGES",
	}))

	// Now relabel test:analyst1 → User. Resulting graph violates the
	// (Admin)-[MANAGES]->(User) DISALLOWED policy.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	err = tx.UpdateNode(&Node{
		ID: "test:analyst1", Labels: []string{"User"},
	})
	if err == nil {
		err = tx.Commit()
	}
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintPolicy, cve.Type)
	require.Contains(t, cve.Message, "DISALLOWED")
}

// TestValidatePolicyOnNodeLabelChange_NoMatchingPolicy: relabeling that
// produces no policy violations must succeed.
func TestValidatePolicyOnNodeLabelChange_NoMatchingPolicy(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:        "company_owns_dept",
		Type:        ConstraintPolicy,
		EntityType:  ConstraintEntityRelationship,
		Label:       "OWNS",
		SourceLabel: "Company",
		TargetLabel: "Department",
		PolicyMode:  "ALLOWED",
	}))

	_, err = engine.CreateNode(&Node{ID: "test:co1", Labels: []string{"Company"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:dept1", Labels: []string{"Department"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:co1", EndNode: "test:dept1", Type: "OWNS",
	}))

	// Add an extra label — relationship still matches the ALLOWED policy
	// because the original Company/Department labels remain.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.UpdateNode(&Node{
		ID: "test:co1", Labels: []string{"Company", "AcmeHolding"},
	}))
	require.NoError(t, tx.Commit())
}

// TestValidatePolicyOnNodeLabelChange_NoConstraintIsNoOp: with no policies
// in the schema, label change is a fast path that returns nil.
func TestValidatePolicyOnNodeLabelChange_NoConstraintIsNoOp(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"L1"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:n1", Labels: []string{"L2"}}))
	require.NoError(t, tx.Commit())
}

// TestGetCommittedEdgeLocked_SnapshotRead: when readTS is set, the lookup
// goes through the MVCC GetEdgeVisibleAt path — verify we get the expected
// edge body even though the in-tx snapshot is non-zero.
func TestGetCommittedEdgeLocked_SnapshotRead(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]any{"k": "v"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Read via the public GetEdge — internally calls getCommittedEdgeLocked.
	got, err := tx.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), got.ID)
	require.Equal(t, "REL", got.Type)
	require.Equal(t, "v", got.Properties["k"])
}
