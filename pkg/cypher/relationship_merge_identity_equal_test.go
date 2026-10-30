package cypher

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRelationshipMergeScalarFastPathMatchesGeneral: the scalar comparison
// gives the general comparison's answer for every pair it decides.
func TestRelationshipMergeScalarFastPathMatchesGeneral(t *testing.T) {
	values := []interface{}{
		"a", "b", "", "1", true, false, int64(0), int64(1), int64(-1), math.MaxInt64,
		0.0, math.Copysign(0, -1), 1.5, -1.5, math.NaN(), math.Inf(1), math.Inf(-1), 1.0, 2.0,
		int(1), int32(1), float32(1.5), nil, []interface{}{int64(1)}, map[string]interface{}{"k": "v"},
	}
	decidedPairs := 0
	for _, left := range values {
		for _, right := range values {
			got := normalizeRelationshipMergeIdentityValue(left)
			want := normalizeRelationshipMergeIdentityValue(right)
			equal, decided := relationshipMergeScalarsEqual(got, want)
			if !decided {
				continue
			}
			decidedPairs++
			require.Equal(t, relationshipMergeValuesEqualGeneral(got, want), equal, "%#v vs %#v", left, right)
		}
	}
	require.Greater(t, decidedPairs, 100)
}
