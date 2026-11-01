package models

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// FloatArray vector components follow the one external-number policy: an
// out-of-range FLOAT is +/-Infinity and an INTEGER outside int64 is rejected,
// for decoded JSON numbers and for JSON text alike.
func TestFloatArrayNumberPolicy(t *testing.T) {
	var vector FloatArray
	require.NoError(t, vector.UnmarshalGQL([]interface{}{json.Number("1e400"), json.Number("-1e400"), json.Number("2")}))
	require.Equal(t, FloatArray{float32(math.Inf(1)), float32(math.Inf(-1)), 2}, vector)
	require.NoError(t, vector.UnmarshalGQL(`[1e400, 0.5]`))
	require.Equal(t, FloatArray{float32(math.Inf(1)), 0.5}, vector)
	require.ErrorContains(t, vector.UnmarshalGQL([]interface{}{json.Number("9223372036854775808")}), "Unsupported type of Number 9223372036854775808")
	require.ErrorContains(t, vector.UnmarshalGQL(`[9223372036854775808]`), "Unsupported type of Number 9223372036854775808")
	require.NoError(t, vector.UnmarshalGQL(`null`))
	require.Nil(t, vector)
}
