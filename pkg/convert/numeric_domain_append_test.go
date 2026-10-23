package convert

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericDomainTypedJSONLeaves(t *testing.T) {
	for _, value := range []int64{math.MinInt64, -9007199254740993, 0, 9007199254740993, math.MaxInt64} {
		want, err := CypherInputJSON.Marshal(value)
		require.NoError(t, err)
		require.Equal(t, append([]byte{'['}, want...), CypherInputJSON.AppendInteger([]byte{'['}, value))
	}
	for _, value := range []uint64{0, math.MaxInt64, uint64(math.MaxInt64) + 1, math.MaxUint64} {
		want, err := CypherInputJSON.Marshal(value)
		require.NoError(t, err)
		require.Equal(t, want, CypherInputJSON.AppendUnsigned(nil, value))
	}
	for _, bits := range []int{32, 64} {
		for _, value := range []float64{math.Copysign(0, -1), 2, 0.1, math.SmallestNonzeroFloat32, math.MaxFloat32} {
			var input any = value
			if bits == 32 {
				input = float32(value)
			}
			want, err := CypherInputJSON.Marshal(input)
			require.NoError(t, err)
			got, err := CypherInputJSON.AppendFloat(nil, value, bits)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		for _, value := range []float64{math.NaN(), math.Inf(-1), math.Inf(1)} {
			got, err := CypherInputJSON.AppendFloat([]byte{'['}, value, bits)
			require.Error(t, err)
			require.Equal(t, []byte{'['}, got)
		}
	}
}
