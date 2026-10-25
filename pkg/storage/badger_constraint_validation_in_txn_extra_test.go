package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerConstraintValidation_NodeBranchesInTxn(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "k_user", Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "uid"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "e_name", Type: ConstraintExists, Label: "User", Properties: []string{"name"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "t_user", Type: ConstraintTemporal, Label: "User", Properties: []string{"k", "from", "to"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "d_state", Type: ConstraintDomain, Label: "User", Properties: []string{"state"}, AllowedValues: []any{"active", "pending"}}))
	require.NoError(t, schema.AddPropertyTypeConstraint("pt_age", "User", "age", PropertyTypeInteger))

	_, err := engine.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x", "tenant": "t", "uid": "1", "name": "A", "k": "k1", "from": time.Now().UTC(), "to": nil, "state": "active", "age": int64(10)}})
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		// nil guards
		require.NoError(t, engine.validateNodeConstraintsInTxn(context.Background(), txn, nil, schema, "test", ""))
		require.NoError(t, engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:nil", Labels: []string{"User"}}, nil, "test", ""))

		// UNIQUE violation via scan/cache path
		err := engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x", "tenant": "t", "uid": "2", "name": "B", "k": "k2", "from": time.Now().UTC(), "state": "active", "age": int64(11)}}, schema, "test", "")
		require.Error(t, err)

		// NODE KEY null property
		err = engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u3", Labels: []string{"User"}, Properties: map[string]any{"email": "c@x", "tenant": "t", "name": "C", "k": "k3", "from": time.Now().UTC(), "state": "active", "age": int64(12)}}, schema, "test", "")
		require.Error(t, err)

		// EXISTS missing (nil properties)
		err = engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u4", Labels: []string{"User"}, Properties: nil}, schema, "test", "")
		require.Error(t, err)

		// TEMPORAL invalid start type
		err = engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u5", Labels: []string{"User"}, Properties: map[string]any{"email": "e@x", "tenant": "t", "uid": "5", "name": "E", "k": "k5", "from": "bad", "to": nil, "state": "active", "age": int64(13)}}, schema, "test", "")
		require.Error(t, err)

		// DOMAIN violation
		err = engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u6", Labels: []string{"User"}, Properties: map[string]any{"email": "f@x", "tenant": "t", "uid": "6", "name": "F", "k": "k6", "from": time.Now().UTC(), "to": nil, "state": "blocked", "age": int64(14)}}, schema, "test", "")
		require.Error(t, err)

		// PROPERTY TYPE violation
		err = engine.validateNodeConstraintsInTxn(context.Background(), txn, &Node{ID: "test:u7", Labels: []string{"User"}, Properties: map[string]any{"email": "g@x", "tenant": "t", "uid": "7", "name": "G", "k": "k7", "from": time.Now().UTC(), "to": nil, "state": "active", "age": "bad"}}, schema, "test", "")
		require.Error(t, err)
		return nil
	}))
}

func TestBadgerConstraintValidation_EdgeBranchesInTxn(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_exists", Type: ConstraintExists, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_temporal", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"key", "from", "to"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_card", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "LINK", Direction: "OUTGOING", MaxCount: 1}))
	require.NoError(t, schema.AddPropertyTypeConstraint("rel_rank", "LINK", "rank", PropertyTypeInteger, ConstraintEntityRelationship))

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"User"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Secret"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:c", Labels: []string{"Report"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"token": "x", "key": "k", "from": time.Now().UTC(), "rank": int64(1)}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_disallowed", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINK", PolicyMode: "DISALLOWED", SourceLabel: "User", TargetLabel: "Secret"}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "rel_allowed", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINK", PolicyMode: "ALLOWED", SourceLabel: "User", TargetLabel: "Report"}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		// nil/empty type guard
		require.NoError(t, engine.validateEdgeConstraintsInTxn(txn, nil, schema, "test", ""))
		require.NoError(t, engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:empty", StartNode: "test:a", EndNode: "test:b", Type: ""}, schema, "test", ""))

		// EXISTS violation
		err := engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{}}, schema, "test", "")
		require.Error(t, err)

		// UNIQUE violation
		err = engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "x", "key": "k2", "from": time.Now().UTC(), "rank": int64(2)}}, schema, "test", "")
		require.Error(t, err)

		// TEMPORAL bad start type
		err = engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:e4", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "y", "key": "k3", "from": "bad", "rank": int64(3)}}, schema, "test", "")
		require.Error(t, err)

		// CARDINALITY violation (existing outgoing LINK already 1)
		err = engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:e5", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "z", "key": "k4", "from": time.Now().UTC(), "rank": int64(4)}}, schema, "test", "")
		require.Error(t, err)

		// PROPERTY TYPE violation
		err = engine.validateEdgeConstraintsInTxn(txn, &Edge{ID: "test:e6", StartNode: "test:a", EndNode: "test:c", Type: "LINK", Properties: map[string]any{"token": "zz", "key": "k5", "from": time.Now().UTC(), "rank": "bad"}}, schema, "test", "")
		require.Error(t, err)

		return nil
	}))
}
