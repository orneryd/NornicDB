// Author: Lane #1644 (Codex). A visible admission candidate's failed point
// read is not an absent holder. This exercises the public native admission API.
package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicMergeAdmissionRead(t *testing.T) {
	for _, properties := range [][]string{{"k"}, {"k", "part"}} {
		t.Run(properties[len(properties)-1], func(t *testing.T) {
			base := createTestBadgerEngine(t)
			tenant := NewNamespacedEngine(base, "test")
			schema := base.GetSchemaForNamespace("test")
			c := Constraint{Name: "admission_key", Type: ConstraintUnique, Label: "Doc", Properties: properties}
			require.NoError(t, schema.AddConstraint(c))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(tenant, schema))
			if len(properties) > 1 {
				index, ok := schema.ConstraintPropertyIndex(c.Name)
				require.True(t, ok)
				require.NoError(t, schema.BackfillCompositeIndex(index.Name, nil))
			}
			props := map[string]interface{}{"k": "caller-key", "part": "caller-part"}
			_, err := tenant.CreateNode(&Node{ID: "holder", Labels: []string{"Doc"}, Properties: props})
			require.NoError(t, err)
			control := beginMergeKeyTx(t, base)
			require.NoError(t, control.PrepareMergeNode(context.Background(), []string{"Doc"}, props))
			require.NoError(t, control.Rollback())
			require.NoError(t, base.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("test:holder"), []byte("invalid-node-envelope")) }))
			base.cacheDeleteNode("test:holder", nodeCacheAllForms)
			_, pointErr := base.GetNode("test:holder")
			require.Error(t, pointErr)
			require.NotErrorIs(t, pointErr, ErrNotFound)
			tx := beginMergeKeyTx(t, base)
			t.Cleanup(func() { _ = tx.Rollback() })
			err = tx.PrepareMergeNode(context.Background(), []string{"Doc"}, props)
			require.EqualError(t, err, pointErr.Error())
			require.NoError(t, tx.Rollback())
		})
	}
}
