package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Constraints for a label come back ordered by name, so an entity breaking
// several constraints always reports the same one.
func TestSchemaManager_GetConstraintsForLabelsOrderedByName(t *testing.T) {
	sm := NewSchemaManager()
	for _, name := range []string{"c_unique", "a_exists", "b_key", "d_other_label"} {
		label := "User"
		if name == "d_other_label" {
			label = "Admin"
		}
		require.NoError(t, sm.AddConstraint(Constraint{Name: name, Type: ConstraintExists, Label: label, Properties: []string{name}}))
	}
	var names []string
	for _, c := range sm.GetConstraintsForLabels([]string{"User"}) {
		names = append(names, c.Name)
	}
	require.Equal(t, []string{"a_exists", "b_key", "c_unique"}, names)
}

// A node without the unique property passes the uniqueness check; a node
// without properties reports the required property it is missing.
func TestBadgerConstraintValidation_NullUniqueValueAndMissingProperties(t *testing.T) {
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "e_name", Type: ConstraintExists, Label: "User", Properties: []string{"name"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.validateNodeConstraintsInTxn(txn, &Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"name": "A"}}, schema, "test", ""))

		err := engine.validateNodeConstraintsInTxn(txn, &Node{ID: "test:u2", Labels: []string{"User"}}, schema, "test", "")
		var violation *ConstraintViolationError
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintExists, violation.Type)
		return nil
	}))
}

// A pending node with another temporal key never overlaps the node checked.
func TestBadgerTransaction_TemporalConstraintIgnoresPendingNodesWithOtherKeys(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{Name: "person_temporal", Type: ConstraintTemporal, Label: "Person", Properties: []string{"account", "from", "to"}}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))

	base := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	otherID := NodeID(prefixTestID("other-account"))
	tx.pendingNodes[otherID] = &Node{ID: otherID, Labels: []string{"Person"}, Properties: map[string]any{"account": "other", "from": base, "to": base.Add(2 * time.Hour)}}
	node := &Node{ID: NodeID(prefixTestID("acct")), Labels: []string{"Person"}, Properties: map[string]any{"account": "acct", "from": base, "to": base.Add(time.Hour)}}
	require.NoError(t, tx.checkTemporalConstraint(node, constraint))
}
