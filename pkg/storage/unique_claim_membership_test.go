package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetainedUniqueClaimUsesCanonicalIdentity(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	for _, tc := range []struct {
		name          string
		before, after any
		retained      bool
	}{
		{"string", "stable", "stable", true},
		{"numeric_width", int64(7), float64(7), true},
		{"list_numeric_width", []any{int64(7), "k"}, []any{float64(7), "k"}, true},
		// Non-reflexive scalar NaN keys do not represent an equal claim.
		{"non_reflexive_nan", math.NaN(), math.NaN(), false},
		{"changed", "old", "new", false},
		{"removed", "old", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := &Node{Labels: []string{"Doc"}, Properties: map[string]any{"id": tc.after}}
			require.Equal(t, tc.retained, retainsNodeUniqueClaim(t.Context(), schema, "Doc", "id", tc.before, after))
			after.Labels = []string{"Other"}
			require.False(t, retainsNodeUniqueClaim(t.Context(), schema, "Doc", "id", tc.before, after))
			require.False(t, retainsNodeUniqueClaim(t.Context(), schema, "Doc", "id", tc.before, nil))
		})
	}
}
