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
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), NewNamespacedEngine(engine, "test"), schema))
			idx, owned := schema.ConstraintPropertyIndex(c.Name)
			require.True(t, owned)
			require.NoError(t, schema.BackfillCompositeIndex(t.Context(), idx.Name, nil))
			props := map[string]interface{}{"tenant": int64(7), "id": int64(42)}
			keys, keyErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, props)
			require.NoError(t, keyErr)
			require.Len(t, keys, 1)
			canonical, ok, canonicalErr := constraintPropertyLockKey(t.Context(), c, c.Label, props)
			require.NoError(t, canonicalErr)
			require.True(t, ok)
			require.Equal(t, canonical, keys[0].lock)
			// Numeric aliases must use commit's same tuple, in schema order.
			aliases, aliasesErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, map[string]interface{}{"id": float64(42), "tenant": int(7)})
			require.NoError(t, aliasesErr)
			require.Equal(t, canonical, aliases[0].lock)
			missing, missingErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, map[string]interface{}{"tenant": int64(7)})
			require.NoError(t, missingErr)
			require.Empty(t, missing)
			null, nullErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, map[string]interface{}{"tenant": int64(7), "id": nil})
			require.NoError(t, nullErr)
			require.Empty(t, null)

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
			_, err := peer.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Tuple"}, Properties: props})
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
			_, err = stale.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("audit"))})
			require.NoError(t, err)
			newProps := map[string]interface{}{"tenant": int64(8), "id": int64(42)}
			writer := beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("new-tuple")), Labels: []string{"Tuple"}, Properties: newProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit())
			require.NoError(t, fresh.PrepareMergeNode(context.Background(), []string{"Tuple"}, newProps))
			_, err = fresh.GetNode(t.Context(), NodeID(prefixTestID("new-tuple")))
			require.NoError(t, err)
			require.ErrorIs(t, stale.PrepareMergeNode(context.Background(), []string{"Tuple"}, newProps), ErrConflict)
			require.NoError(t, fresh.Rollback())
			require.NoError(t, stale.Rollback())
		})
	}
}

// Adjacent integers above 2^53 are distinct tuple values: the composite index,
// commit validation and MERGE admission all key them exactly.
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
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), NewNamespacedEngine(engine, "test"), schema))
			idx, owned := schema.ConstraintPropertyIndex(c.Name)
			require.True(t, owned)
			require.NoError(t, schema.BackfillCompositeIndex(t.Context(), idx.Name, nil))
			evenProps := map[string]interface{}{"tenant": int64(7), "id": even}
			oddProps := map[string]interface{}{"tenant": int64(7), "id": odd}

			// One transaction may hold both tuples; a second copy is still a duplicate.
			both := beginMergeKeyTx(t, engine)
			_, err := both.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("pending-even")), Labels: []string{"Tuple"}, Properties: evenProps})
			require.NoError(t, err)
			_, err = both.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("pending-odd")), Labels: []string{"Tuple"}, Properties: oddProps})
			require.NoError(t, err)
			_, err = both.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("pending-copy")), Labels: []string{"Tuple"}, Properties: oddProps})
			require.Error(t, err)
			require.NoError(t, both.Rollback())

			evenID := NodeID(prefixTestID("even"))
			writer := beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(t.Context(), &Node{ID: evenID, Labels: []string{"Tuple"}, Properties: evenProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit())

			stale := beginMergeKeyTx(t, engine)
			_, err = stale.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("audit"))})
			require.NoError(t, err)
			fresh := beginMergeKeyTx(t, engine)
			// The older adjacent tuple alone is not the holder: admission locks.
			probe := beginMergeKeyTx(t, engine)
			require.NoError(t, probe.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps))
			require.Len(t, probe.mergeKeyReleases, 1)
			require.NoError(t, probe.Rollback())

			oddID := NodeID(prefixTestID("odd"))
			writer = beginMergeKeyTx(t, engine)
			_, err = writer.CreateNode(t.Context(), &Node{ID: oddID, Labels: []string{"Tuple"}, Properties: oddProps})
			require.NoError(t, err)
			require.NoError(t, writer.Commit(), "adjacent tuple is not a duplicate")
			copyTx := beginMergeKeyTx(t, engine)
			_, err = copyTx.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("copy")), Labels: []string{"Tuple"}, Properties: oddProps})
			if err == nil {
				err = copyTx.Commit()
			} else {
				_ = copyTx.Rollback()
			}
			require.Error(t, err, "the exact tuple remains unique")

			// The lock hash is the index's exact key for the tuple: it lists
			// only the odd tuple's node, not its rounded neighbour.
			keys, keyErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, oddProps)
			require.NoError(t, keyErr)
			require.Len(t, keys, 1)
			hash, _ := keys[0].lock.value.(string)
			require.Equal(t, []NodeID{oddID}, checkedIndexRead[[]NodeID](t)(idx.lookupFullHash(t.Context(), hash)))
			require.Equal(t, []NodeID{evenID}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), int64(7), even)))
			holder, found, complete, holderErr := fresh.mergeAdmissionHolder(t.Context(), schema, "test", keys[0], oddProps)
			require.NoError(t, holderErr)
			require.True(t, found)
			require.True(t, complete)
			require.Equal(t, oddID, holder)
			plain, keyErr := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, map[string]interface{}{"tenant": int64(7), "id": int64(42)})
			require.NoError(t, keyErr)
			require.Len(t, plain, 1)
			plainHash, _ := plain[0].lock.value.(string)
			require.Equal(t, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), int64(7), int64(42))), checkedIndexRead[[]NodeID](t)(idx.lookupFullHash(t.Context(), plainHash)))

			require.ErrorIs(t, stale.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps), ErrConflict)
			require.NoError(t, fresh.PrepareMergeNode(context.Background(), []string{"Tuple"}, oddProps))
			got, err := fresh.GetNode(t.Context(), oddID)
			require.NoError(t, err)
			require.Equal(t, odd, got.Properties["id"])
			require.NoError(t, fresh.Rollback())
			require.NoError(t, stale.Rollback())
		})
	}
}
