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
			canonical, _, ok := nodeConstraintLockKey(c, &Node{Properties: props})
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

// Adjacent integers above 2^53 are distinct tuple values. Commit validation
// and MERGE admission share one exact holder test, although the generic
// composite index files both tuples under one rounded key.
func TestMergeAdmissionTupleIntegerPrecision(t *testing.T) {
	even, odd := int64(9007199254740992), int64(9007199254740993)
	require.False(t, compareValues(even, odd))
	require.False(t, compareValues(odd, float64(even)))
	require.True(t, compareValues(even, float64(even)))
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
			evenProps := map[string]interface{}{"tenant": int64(7), "id": even}
			oddProps := map[string]interface{}{"tenant": int64(7), "id": odd}

			// One transaction may hold both tuples; a second copy is still a duplicate.
			both := beginMergeKeyTx(t, engine)
			_, err := both.CreateNode(&Node{ID: NodeID(prefixTestID("pending-even")), Labels: []string{"Tuple"}, Properties: evenProps})
			require.NoError(t, err)
			_, err = both.CreateNode(&Node{ID: NodeID(prefixTestID("pending-odd")), Labels: []string{"Tuple"}, Properties: oddProps})
			require.NoError(t, err)
			_, err = both.CreateNode(&Node{ID: NodeID(prefixTestID("pending-copy")), Labels: []string{"Tuple"}, Properties: oddProps})
			require.Error(t, err)
			require.NoError(t, both.Rollback())

			evenID := NodeID(prefixTestID("even"))
			writer := beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(&Node{ID: evenID, Labels: []string{"Tuple"}, Properties: evenProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit())

			stale := beginMergeKeyTx(t, engine)
			_, err = stale.CreateNode(&Node{ID: NodeID(prefixTestID("audit"))})
			require.NoError(t, err)
			fresh := beginMergeKeyTx(t, engine)
			// The older adjacent tuple alone is not the holder: admission locks.
			probe := beginMergeKeyTx(t, engine)
			require.NoError(t, probe.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps))
			require.Len(t, probe.mergeKeyReleases, 1)
			require.NoError(t, probe.Rollback())

			oddID := NodeID(prefixTestID("odd"))
			writer = beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(&Node{ID: oddID, Labels: []string{"Tuple"}, Properties: oddProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit(), "adjacent tuple is not a duplicate")
			copyTx := beginMergeKeyTx(t, engine)
			_, err = copyTx.CreateNode(&Node{ID: NodeID(prefixTestID("copy")), Labels: []string{"Tuple"}, Properties: oddProps})
			if err == nil {
				err = copyTx.Commit()
			} else {
				_ = copyTx.Rollback()
			}
			require.Error(t, err, "the exact tuple remains unique")

			// The projected index hash finds both rounded neighbours; the exact
			// holder decides.
			keys := schema.uniqueMergeKeys([]string{"Tuple"}, oddProps)
			require.Len(t, keys, 1)
			require.ElementsMatch(t, []NodeID{evenID, oddID}, idx.lookupFullHash(keys[0].indexHash))
			holder, found, complete := fresh.mergeAdmissionHolder(schema, "test", keys[0], oddProps)
			require.True(t, found)
			require.True(t, complete)
			require.Equal(t, oddID, holder)
			plain := schema.uniqueMergeKeys([]string{"Tuple"}, map[string]interface{}{"tenant": int64(7), "id": int64(42)})
			require.Len(t, plain, 1)
			require.Equal(t, plain[0].lock.value, plain[0].indexHash, "a tuple without rounded values reuses the lock hash")
			require.Equal(t, idx.LookupFull(int64(7), int64(42)), idx.lookupFullHash(plain[0].indexHash))

			require.ErrorIs(t, stale.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps), ErrConflict)
			require.NoError(t, fresh.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps))
			got, err := fresh.GetNode(oddID)
			require.NoError(t, err)
			require.Equal(t, odd, got.Properties["id"])
			require.NoError(t, fresh.Rollback())
			require.NoError(t, stale.Rollback())
		})
	}
}
