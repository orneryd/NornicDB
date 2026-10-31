package cypher

import (
	"context"
	"fmt"
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

// Every APOC JSON reader reached through Cypher reports the external-number
// policy's rejection instead of a partial or empty value, and reads an
// out-of-range FLOAT as +/-Infinity.
func TestAPOCJSONReadersNumberPolicyThroughCypher(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	// The APOC JSON readers Cypher registers. getJsonProperty,
	// getJsonPropertyMap and setJsonProperty are Go APIs only (Cypher reports
	// "unknown function" on release and here); apoc/convert tests cover them.
	readers := map[string]string{
		"fromJsonList": `apoc.convert.fromJsonList('[%s]')`,
		"fromJsonMap":  `apoc.convert.fromJsonMap('{"c":%s}')`,
	}
	for name, call := range readers {
		t.Run(name+"/rejected-integer", func(t *testing.T) {
			_, err := exec.Execute(ctx, "RETURN "+fmt.Sprintf(call, "9223372036854775808")+" AS v", nil)
			require.ErrorContains(t, err, "Unsupported type of Number 9223372036854775808")
		})
		t.Run(name+"/overflow-float", func(t *testing.T) {
			_, err := exec.Execute(ctx, "RETURN "+fmt.Sprintf(call, "1e400")+" AS v", nil)
			require.NoError(t, err)
		})
	}
	result, err := exec.Execute(ctx, `RETURN apoc.convert.fromJsonMap('{"c":-1e400}').c AS v`, nil)
	require.NoError(t, err)
	require.Equal(t, math.Inf(-1), result.Rows[0][0])
}
