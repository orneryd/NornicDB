package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerConstraintValidation_MoreEdgeBranchCoverage(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"User"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Report"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:c", Labels: []string{"Secret"}})
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:e1",
		StartNode: "test:a",
		EndNode:   "test:b",
		Type:      "LINK",
		Properties: map[string]any{
			"token": "tok-1",
			"k":     "bucket-1",
			"a":     "x",
			"b":     "y",
			"from":  now,
			"to":    now.Add(2 * time.Hour),
		},
	}))

	t.Run("checkEdgeUniqueness single-key nil and composite branches", func(t *testing.T) {
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			// Single-key uniqueness: nil new value short-circuits as non-violating.
			err := engine.checkEdgeUniquenessInTxn(txn, &Edge{ID: "test:new-1", Type: "LINK", Properties: map[string]any{"token": nil}}, Constraint{Type: ConstraintUnique, Label: "LINK", Properties: []string{"token"}}, "test", "")
			require.NoError(t, err)

			// Composite uniqueness: missing component short-circuits as non-violating.
			err = engine.checkEdgeUniquenessInTxn(txn, &Edge{ID: "test:new-2", Type: "LINK", Properties: map[string]any{"a": "x", "b": nil}}, Constraint{Type: ConstraintUnique, Label: "LINK", Properties: []string{"a", "b"}}, "test", "")
			require.NoError(t, err)

			// Composite uniqueness: full match reports violation.
			err = engine.checkEdgeUniquenessInTxn(txn, &Edge{ID: "test:new-3", Type: "LINK", Properties: map[string]any{"a": "x", "b": "y"}}, Constraint{Type: ConstraintUnique, Label: "LINK", Properties: []string{"a", "b"}}, "test", "")
			require.Error(t, err)

			// Namespace filter skips when target namespace doesn't match existing edge IDs.
			err = engine.checkEdgeUniquenessInTxn(txn, &Edge{ID: "other:new", Type: "LINK", Properties: map[string]any{"token": "tok-1"}}, Constraint{Type: ConstraintUnique, Label: "LINK", Properties: []string{"token"}}, "other", "")
			require.NoError(t, err)
			return nil
		}))
	})

	t.Run("checkEdgeTemporal validation and overlap paths", func(t *testing.T) {
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			err := engine.checkEdgeTemporalInTxn(txn, &Edge{Type: "LINK", Properties: map[string]any{}}, Constraint{Type: ConstraintTemporal, Label: "LINK", Properties: []string{"k", "from"}}, "test", "")
			require.Error(t, err)

			err = engine.checkEdgeTemporalInTxn(txn, &Edge{Type: "LINK", Properties: map[string]any{"k": nil, "from": now, "to": now.Add(time.Hour)}}, Constraint{Type: ConstraintTemporal, Label: "LINK", Properties: []string{"k", "from", "to"}}, "test", "")
			require.Error(t, err)

			err = engine.checkEdgeTemporalInTxn(txn, &Edge{Type: "LINK", Properties: map[string]any{"k": "bucket-1", "from": "bad", "to": now.Add(time.Hour)}}, Constraint{Type: ConstraintTemporal, Label: "LINK", Properties: []string{"k", "from", "to"}}, "test", "")
			require.Error(t, err)

			err = engine.checkEdgeTemporalInTxn(txn, &Edge{Type: "LINK", Properties: map[string]any{"k": "bucket-1", "from": now.Add(30 * time.Minute), "to": now.Add(90 * time.Minute)}}, Constraint{Type: ConstraintTemporal, Label: "LINK", Properties: []string{"k", "from", "to"}}, "test", "")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("checkEdgeCardinality nil-prefix and violation branches", func(t *testing.T) {
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			// Unknown anchor node has no numeric ID, so prefix lookup returns nil and check is a no-op.
			err := engine.checkEdgeCardinalityInTxn(txn, &Edge{Type: "LINK", StartNode: "test:ghost", EndNode: "test:b"}, Constraint{Type: ConstraintCardinality, Label: "LINK", Direction: "OUTGOING", MaxCount: 1}, "test", "")
			require.NoError(t, err)

			// Existing outgoing LINK from test:a already reaches max count.
			err = engine.checkEdgeCardinalityInTxn(txn, &Edge{Type: "LINK", StartNode: "test:a", EndNode: "test:c"}, Constraint{Type: ConstraintCardinality, Label: "LINK", Direction: "OUTGOING", MaxCount: 1}, "test", "")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("checkEdgePolicy no-constraints and disallowed violation", func(t *testing.T) {
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			emptySchema := NewSchemaManager()
			require.NoError(t, engine.checkEdgePolicyInTxn(txn, &Edge{Type: "LINK", StartNode: "test:a", EndNode: "test:b"}, emptySchema, "test"))

			schema := NewSchemaManager()
			require.NoError(t, schema.AddConstraint(Constraint{
				Name:        "rel_disallowed",
				Type:        ConstraintPolicy,
				EntityType:  ConstraintEntityRelationship,
				Label:       "LINK",
				PolicyMode:  "DISALLOWED",
				SourceLabel: "User",
				TargetLabel: "Report",
			}))

			err := engine.checkEdgePolicyInTxn(txn, &Edge{Type: "LINK", StartNode: "test:a", EndNode: "test:b"}, schema, "test")
			require.Error(t, err)
			return nil
		}))
	})
}
