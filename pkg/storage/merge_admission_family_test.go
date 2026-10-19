package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMergeAdmissionCanonicalTuple(t *testing.T) {
	for _, kind := range []ConstraintType{ConstraintUnique, ConstraintNodeKey} {
		t.Run(string(kind), func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			schema := engine.GetSchemaForNamespace("test")
			c := Constraint{Name: "tuple", Type: kind, Label: "Tuple", Properties: []string{"tenant", "id"}}
			require.NoError(t, schema.AddConstraint(c))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(engine, "test"), schema))
			idx, owned := schema.ConstraintPropertyIndex(c.Name)
			require.True(t, owned)
			require.NoError(t, schema.BackfillCompositeIndex(idx.Name, nil))
			props := map[string]interface{}{"tenant": int64(7), "id": int64(42)}
			keys := schema.uniqueMergeKeys([]string{"Tuple"}, props)
			require.Len(t, keys, 1)
			canonical, ok := nodeConstraintLockKey(c, &Node{Properties: props})
			require.True(t, ok)
			require.Equal(t, canonical, keys[0].lock)
			// Numeric aliases must use commit's same tuple, in schema order.
			require.Equal(t, canonical, schema.uniqueMergeKeys([]string{"Tuple"}, map[string]interface{}{"id": float64(42), "tenant": int(7)})[0].lock)
			require.Empty(t, schema.uniqueMergeKeys([]string{"Tuple"}, map[string]interface{}{"tenant": int64(7)}))
			require.Empty(t, schema.uniqueMergeKeys([]string{"Tuple"}, map[string]interface{}{"tenant": int64(7), "id": nil}))

			creator := beginMergeKeyTx(t, engine)
			require.NoError(t, creator.PrepareMergeNode(context.Background(), []string{"Tuple"}, props))
			// A shared column is not a shared tuple and must not block.
			disjoint := beginMergeKeyTx(t, engine)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, disjoint.PrepareMergeNode(ctx, []string{"Tuple"}, map[string]interface{}{"tenant": int64(7), "id": int64(43)}))
			require.NoError(t, disjoint.Rollback())

			// Commit uses the exact lock held by MERGE, so it must wait too.
			peer := beginMergeKeyTx(t, engine)
			id := NodeID(prefixTestID("tuple"))
			_, err := peer.CreateNode(&Node{ID: id, Labels: []string{"Tuple"}, Properties: props})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- peer.Commit() }()
			select {
			case err := <-done:
				t.Fatalf("commit bypassed MERGE tuple lock: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			require.NoError(t, creator.Rollback())
			require.NoError(t, <-done)

			fresh := beginMergeKeyTx(t, engine)
			stale := beginMergeKeyTx(t, engine)
			_, err = stale.CreateNode(&Node{ID: NodeID(prefixTestID("audit"))})
			require.NoError(t, err)
			newProps := map[string]interface{}{"tenant": int64(8), "id": int64(42)}
			writer := beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(&Node{ID: NodeID(prefixTestID("new-tuple")), Labels: []string{"Tuple"}, Properties: newProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit())
			require.NoError(t, fresh.PrepareMergeNode(context.Background(), []string{"Tuple"}, newProps))
			_, err = fresh.GetNode(NodeID(prefixTestID("new-tuple")))
			require.NoError(t, err)
			require.ErrorIs(t, stale.PrepareMergeNode(context.Background(), []string{"Tuple"}, newProps), ErrConflict)
			require.NoError(t, fresh.Rollback())
			require.NoError(t, stale.Rollback())
		})
	}
}
