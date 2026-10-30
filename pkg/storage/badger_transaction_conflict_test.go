package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheckNodeCreateConflict_ConcurrentWriteAfterReadTS validates the
// snapshot-isolation conflict check: tx1 begins, tx2 commits a node create,
// tx1 tries to create that same node ID — must fail with ErrConflict.
func TestCheckNodeCreateConflict_ConcurrentWriteAfterReadTS(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
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
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
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

// TestGetCommittedEdgeLocked_SnapshotRead: when readTS is set, the lookup
// goes through the MVCC GetEdgeVisibleAt path — verify we get the expected
// edge body even though the in-tx snapshot is non-zero.
func TestGetCommittedEdgeLocked_SnapshotRead(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
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

func TestCheckEdgeEndpointConflicts_TombstonedEndpointBranches(t *testing.T) {
	t.Run("endpoint deleted after transaction start returns conflict", func(t *testing.T) {
		engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		_, err = engine.CreateNode(&Node{ID: "test:start", Labels: []string{"L"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:target", Labels: []string{"L"}})
		require.NoError(t, err)

		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		require.NoError(t, engine.DeleteNode("test:target"))

		err = tx.checkEdgeEndpointConflicts(&Edge{ID: "test:e1", StartNode: "test:start", EndNode: "test:target", Type: "REL"})
		require.ErrorIs(t, err, ErrConflict)
		require.Contains(t, err.Error(), "deleted after transaction start")
	})

	t.Run("missing endpoint returns invalid edge", func(t *testing.T) {
		engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })

		_, err = engine.CreateNode(&Node{ID: "test:start", Labels: []string{"L"}})
		require.NoError(t, err)

		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		err = tx.checkEdgeEndpointConflicts(&Edge{ID: "test:e2", StartNode: "test:start", EndNode: "test:target", Type: "REL"})
		require.ErrorIs(t, err, ErrInvalidEdge)
	})
}

func TestClassifyUpdateConflictAsUniqueViolation(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "unique_name",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"name"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	nodeID := NodeID("test:peer-owned")
	_, err = engine.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	require.NoError(t, err)
	constraint := schema.uniqueConstraints["Person:name"]
	require.NotNil(t, constraint)
	constraint.mu.Lock()
	constraint.valuesCacheComplete = true
	constraint.mu.Unlock()

	tx.namespace = "test"
	tx.pendingNodes[nodeID] = &Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}

	err = tx.classifyUpdateConflictAsUniqueViolation(nodeID)
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintUnique, cve.Type)
	require.Equal(t, "Person", cve.Label)
	require.Equal(t, []string{"name"}, cve.Properties)
	require.Contains(t, cve.Message, "claimed by concurrent commit")
	require.ErrorIs(t, cve.Cause, ErrConflict)

	schema.ApplyUniqueValue("Person", "name", "Alice", nodeID, RetiredUniqueClaim)
	require.Nil(t, tx.classifyUpdateConflictAsUniqueViolation(nodeID))
}

// TestTransactionEntityPolicyScenarios owns policy validation across committed
// and pending endpoints, label updates, and the unconstrained create/update paths.
func TestTransactionEntityPolicyScenarios(t *testing.T) {
	disallowed := Constraint{Name: "no_admin_to_user", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "MANAGES", SourceLabel: "Admin", TargetLabel: "User", PolicyMode: "DISALLOWED"}
	allowed := Constraint{Name: "company_owns_dept", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "OWNS", SourceLabel: "Company", TargetLabel: "Department", PolicyMode: "ALLOWED"}
	for _, tc := range []struct {
		name           string
		policy         *Constraint
		committedNodes []*Node
		committedEdge  *Edge
		pendingNodes   []*Node
		update         *Node
		pendingEdge    *Edge
		violation      string
	}{
		{
			name:         "fresh ID has no existing head",
			pendingNodes: []*Node{{ID: "test:fresh-1", Labels: []string{"L"}}},
		},
		{
			name: "label update disallowed by connected edge", policy: &disallowed,
			committedNodes: []*Node{{ID: "test:admin1", Labels: []string{"Admin"}}, {ID: "test:analyst1", Labels: []string{"Analyst"}}},
			committedEdge:  &Edge{ID: "test:e1", StartNode: "test:admin1", EndNode: "test:analyst1", Type: "MANAGES"},
			update:         &Node{ID: "test:analyst1", Labels: []string{"User"}}, violation: "DISALLOWED",
		},
		{
			name: "label update retains matching allowed policy", policy: &allowed,
			committedNodes: []*Node{{ID: "test:co1", Labels: []string{"Company"}}, {ID: "test:dept1", Labels: []string{"Department"}}},
			committedEdge:  &Edge{ID: "test:e1", StartNode: "test:co1", EndNode: "test:dept1", Type: "OWNS"},
			update:         &Node{ID: "test:co1", Labels: []string{"Company", "AcmeHolding"}},
		},
		{
			name:           "label update without constraint is no-op validation",
			committedNodes: []*Node{{ID: "test:n1", Labels: []string{"L1"}}},
			update:         &Node{ID: "test:n1", Labels: []string{"L2"}},
		},
		{
			name: "edge between disallowed endpoints", policy: &disallowed,
			committedNodes: []*Node{{ID: "test:admin1", Labels: []string{"Admin"}}, {ID: "test:user1", Labels: []string{"User"}}},
			pendingEdge:    &Edge{ID: "test:e1", StartNode: "test:admin1", EndNode: "test:user1", Type: "MANAGES"}, violation: "DISALLOWED",
		},
		{
			name:           "edge has no allowed endpoint match",
			policy:         &Constraint{Name: "only_company_owns_dept", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "OWNS", SourceLabel: "Company", TargetLabel: "Department", PolicyMode: "ALLOWED"},
			committedNodes: []*Node{{ID: "test:p1", Labels: []string{"Person"}}, {ID: "test:dept1", Labels: []string{"Department"}}},
			pendingEdge:    &Edge{ID: "test:e1", StartNode: "test:p1", EndNode: "test:dept1", Type: "OWNS"}, violation: "ALLOWED",
		},
		{
			name: "edge policy reads pending endpoint labels", policy: &allowed,
			pendingNodes: []*Node{{ID: "test:co1", Labels: []string{"Company"}}, {ID: "test:dept1", Labels: []string{"Department"}}},
			pendingEdge:  &Edge{ID: "test:e1", StartNode: "test:co1", EndNode: "test:dept1", Type: "OWNS"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			if tc.policy != nil {
				require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(*tc.policy))
			}
			for _, node := range tc.committedNodes {
				_, err = engine.CreateNode(node)
				require.NoError(t, err)
			}
			if tc.committedEdge != nil {
				require.NoError(t, engine.CreateEdge(tc.committedEdge))
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			for _, node := range tc.pendingNodes {
				_, err = tx.CreateNode(node)
				require.NoError(t, err)
			}
			if tc.update != nil {
				err = tx.UpdateGraphNode(NodeUpdateRequest{After: tc.update, Policy: ClientNodeEdits})
				if tc.violation == "" {
					require.NoError(t, err)
				}
			}
			if tc.pendingEdge != nil {
				require.NoError(t, tx.CreateEdge(tc.pendingEdge))
			}
			if err == nil {
				err = tx.Commit()
			}
			if tc.violation == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				var cve *ConstraintViolationError
				require.True(t, errors.As(err, &cve))
				require.Equal(t, ConstraintPolicy, cve.Type)
				require.Contains(t, cve.Message, tc.violation)
			}
		})
	}
}
