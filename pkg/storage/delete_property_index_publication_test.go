package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerDeleteRemovesPropertyIndexPostings(t *testing.T) {
	for _, unique := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, evicted := range []bool{false, true} {
				t.Run(fmt.Sprintf("unique=%v/explicit=%v/evicted=%v", unique, explicit, evicted), func(t *testing.T) {
					b := newTestEngine(t)
					sm := b.GetSchemaForNamespace("tenant")
					if unique {
						require.NoError(t, sm.AddConstraint(Constraint{Name: "uq", Type: ConstraintUnique, Label: "L", Properties: []string{"p"}}))
					}
					require.NoError(t, sm.AddPropertyIndex("idx", "L", []string{"p"}))
					sm.MarkPropertyIndexesFilled() // DDL backfill is complete on this empty database.
					n := &Node{ID: "tenant:x", Labels: []string{"L"}, Properties: map[string]any{"p": "v"}}
					_, err := b.CreateNode(t.Context(), n)
					require.NoError(t, err)
					require.Equal(t, []NodeID{n.ID}, checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "L", "p", "v")))
					_, err = b.GetNode(t.Context(), n.ID)
					require.NoError(t, err)
					if evicted {
						b.cacheDeleteNode(n.ID, nodeCacheAllForms)
					}
					if explicit {
						tx, e := b.BeginTransaction(t.Context())
						require.NoError(t, e)
						require.NoError(t, tx.SetNamespace("tenant"))
						require.NoError(t, tx.DeleteGraphNode(t.Context(), n.ID, CascadeEdges))
						require.NoError(t, tx.Commit())
					} else {
						require.NoError(t, b.DeleteNode(t.Context(), n.ID))
					}
					_, err = b.GetNode(t.Context(), n.ID)
					require.ErrorIs(t, err, ErrNotFound)
					ids := checkedIndexRead[[]NodeID](t)(sm.PropertyIndexLookup(t.Context(), "L", "p", "v"))
					t.Logf("public PropertyIndexLookup after delete: %v", ids)
					require.Empty(t, ids, "public property index must remove committed deleted identity")
				})
			}
		}
	}
}
