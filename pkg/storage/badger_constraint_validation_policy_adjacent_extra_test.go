package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerConstraintValidation_PolicyAdjacentEdgeBranches(t *testing.T) {
	t.Run("adjacent_no_prefix_noop", func(t *testing.T) {
		engine := newTestEngine(t)
		schema := engine.GetSchemaForNamespace("test")
		err := engine.withView(func(txn *badger.Txn) error {
			return engine.validatePolicyForAdjacentEdgesInTxn(txn, &Node{ID: "test:missing", Labels: []string{"A"}}, schema, "test")
		})
		require.NoError(t, err)
	})

	t.Run("edges_with_prefix_skip_malformed_lookup_and_reject_decode", func(t *testing.T) {
		engine := newTestEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"B"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:a")
			require.NotNil(t, prefix)
			// malformed key: extractEdgeNumIDFromOutgoingKey => !ok branch
			if err := txn.Set(append(append([]byte{}, prefix...), []byte("bad")...), []byte{}); err != nil {
				return err
			}
			// unknown edge num: lookupEdgeIDByNum => !ok branch
			num, ok := engine.idDict.lookupNodeNumID("test:a")
			require.True(t, ok)
			if err := txn.Set(outgoingIndexKey(num, 999999999), []byte{}); err != nil {
				return err
			}
			// A corrupt existing edge must fail validation, not bypass policy checks.
			return txn.Set(edgeKey("test:e1"), []byte("corrupt-edge"))
		}))

		schema := engine.GetSchemaForNamespace("test")
		err = engine.withView(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:a")
			require.NotNil(t, prefix)
			return engine.validatePolicyForEdgesWithPrefixInTxn(txn, prefix, &Node{ID: "test:a", Labels: []string{"A"}}, true, schema, "test")
		})
		require.ErrorContains(t, err, "edge body has unexpected format byte")
	})

	t.Run("edges_with_prefix_missing_other_node_labels_continue", func(t *testing.T) {
		engine := newTestEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"B"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			// Missing target node => readNodeLabelsInTxn error => continue path.
			return txn.Delete(nodeKey("test:b"))
		}))

		schema := engine.GetSchemaForNamespace("test")
		err = engine.withView(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:a")
			require.NotNil(t, prefix)
			return engine.validatePolicyForEdgesWithPrefixInTxn(txn, prefix, &Node{ID: "test:a", Labels: []string{"A"}}, true, schema, "test")
		})
		require.NoError(t, err)
	})

	t.Run("disallowed_and_allowed_policy_violations", func(t *testing.T) {
		engine := newTestEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"B"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

		schema := engine.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:        "rel_disallowed_ab",
			Type:        ConstraintPolicy,
			EntityType:  ConstraintEntityRelationship,
			Label:       "REL",
			SourceLabel: "A",
			TargetLabel: "B",
			PolicyMode:  "DISALLOWED",
		}))

		err = engine.withView(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:a")
			require.NotNil(t, prefix)
			return engine.validatePolicyForEdgesWithPrefixInTxn(txn, prefix, &Node{ID: "test:a", Labels: []string{"A"}}, true, schema, "test")
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "DISALLOWED policy")

		// Remove prior policy and add ALLOWED that does not match A->B.
		require.NoError(t, schema.DropConstraint("rel_disallowed_ab"))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:        "rel_allowed_cd",
			Type:        ConstraintPolicy,
			EntityType:  ConstraintEntityRelationship,
			Label:       "REL",
			SourceLabel: "C",
			TargetLabel: "D",
			PolicyMode:  "ALLOWED",
		}))

		err = engine.withView(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:a")
			require.NotNil(t, prefix)
			return engine.validatePolicyForEdgesWithPrefixInTxn(txn, prefix, &Node{ID: "test:a", Labels: []string{"A"}}, true, schema, "test")
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "ALLOWED policy")
	})
}
