package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// apoc.convert.fromJson* read external JSON with the same one number policy as
// Neo4j 5.26's HTTP parameters: an out-of-range FLOAT is +/-Infinity, an
// INTEGER outside int64 is rejected.
func TestFromJSONNumberPolicyMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	result, err := exec.Execute(ctx, `RETURN apoc.convert.fromJsonMap('{"a":1e400,"b":-1e400,"g":2.0,"h":9007199254740993}') AS m`, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"a": math.Inf(1), "b": math.Inf(-1), "g": float64(2), "h": int64(9007199254740993)}, result.Rows[0][0])
	result, err = exec.Execute(ctx, `RETURN apoc.convert.fromJsonList('[1e400,2.0,9007199254740993]') AS l`, nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{math.Inf(1), float64(2), int64(9007199254740993)}, result.Rows[0][0])
	for _, integer := range []string{"9223372036854775808", "18446744073709551616", "-9223372036854775809"} {
		_, err := exec.Execute(ctx, `RETURN apoc.convert.fromJsonMap('{"c":`+integer+`}') AS m`, nil)
		require.ErrorContains(t, err, "Unsupported type of Number "+integer, integer)
	}
}
