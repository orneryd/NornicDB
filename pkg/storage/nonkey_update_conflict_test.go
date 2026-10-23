package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransactionUniqueClaimConflict(t *testing.T) {
	for _, tc := range []struct {
		name       string
		before     interface{}
		after      interface{}
		wantUnique bool
	}{
		{"unchanged_string", "original", "original", false},
		{"unchanged_number", int64(7), float64(7), false},
		{"unchanged_list", []interface{}{int64(7), "a"}, []interface{}{int64(7), "a"}, false},
		{"changed_key", "original", "claimed", true},
		{"added_key", nil, "claimed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddConstraint(Constraint{
				Name: "doc_id", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"},
			}))
			// The engine is empty: this mirrors the complete cache published
			// by CREATE CONSTRAINT, which the Execute/Bolt tests use.
			constraint := schema.uniqueConstraints["Doc:id"]
			constraint.mu.Lock()
			constraint.valuesCacheComplete = true
			constraint.mu.Unlock()
			nodeID, err := engine.CreateNode(&Node{
				ID: "test:doc", Labels: []string{"Doc"},
				Properties: map[string]interface{}{"id": tc.before, "summary_input": "before"},
			})
			require.NoError(t, err)
			loser, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = loser.Rollback() })
			peer, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Rollback() })
			for i, tx := range []*BadgerTransaction{loser, peer} {
				node, err := tx.GetNode(nodeID)
				require.NoError(t, err)
				node.Properties["id"] = tc.after
				node.Properties["summary_input"] = []string{"loser", "peer"}[i]
				require.NoError(t, tx.UpdateNode(node))
			}
			require.NoError(t, peer.Commit())
			err = loser.Commit()
			require.ErrorIs(t, err, ErrConflict)
			var violation *ConstraintViolationError
			require.Equal(t, tc.wantUnique, errors.As(err, &violation), "commit error: %v", err)
			if tc.wantUnique {
				require.Equal(t, ConstraintUnique, violation.Type)
				require.Contains(t, violation.Message, "claimed by concurrent commit")
			} else {
				require.Contains(t, err.Error(), "changed after transaction start")
			}
			stored, err := engine.GetNode(nodeID)
			require.NoError(t, err)
			require.Equal(t, "peer", stored.Properties["summary_input"])
		})
	}
}
