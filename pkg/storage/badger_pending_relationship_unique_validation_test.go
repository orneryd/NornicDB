package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingRelationshipUniqueBucketsPreserveExactCompositeKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		first  any
		second any
		other  string
		clash  bool
	}{
		{"equal mixed number kinds", int64(42), float64(42), "same", true},
		{"different composite tails", int64(42), float64(42), "different", false},
		{"adjacent large integers", int64(1 << 53), int64(1<<53 + 1), "same", false},
		{"unindexed lists", []int64{1}, []float64{1}, "same", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			engine := txEdgeConstraintFixture(t, Constraint{
				Name: "unique_pair", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship,
				Label: "TAG", Properties: []string{"k1", "k2"},
			})
			tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			require.NoError(t, tx.CreateEdge(ctx, &Edge{
				ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "TAG",
				Properties: map[string]any{"k1": tc.first, "k2": "same"},
			}))
			require.NoError(t, tx.CreateEdge(ctx, &Edge{
				ID: "test:e2", StartNode: "test:c", EndNode: "test:d", Type: "TAG",
				Properties: map[string]any{"k1": tc.second, "k2": tc.other},
			}))

			err = tx.Commit()
			if tc.clash {
				var violation *ConstraintViolationError
				require.ErrorAs(t, err, &violation)
				require.Equal(t, ConstraintRelationshipKey, violation.Type)
				require.Equal(t, []string{"k1", "k2"}, violation.Properties)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
