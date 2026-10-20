package fabric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFabricExactNumericCollectDistinctProjection(t *testing.T) {
	for name, values := range map[string][]interface{}{
		"numbers": {int64(9007199254740992), float64(9007199254740992), int64(9007199254740993), int64(0), math.Copysign(0, -1)},
		"strings": {"key", "key", " key ", "\"key\""},
	} {
		t.Run(name, func(t *testing.T) {
			input := &ResultStream{Columns: []string{"v"}}
			for _, value := range values {
				input.Rows = append(input.Rows, []interface{}{value})
			}
			result, ok := executeApplyInMemoryProjection(input,
				"WITH collect({v: v}) AS rows UNWIND rows AS r WITH collect(DISTINCT r.v) AS keys RETURN keys")
			require.True(t, ok, "must exercise the Fabric in-memory DISTINCT projection")
			require.Equal(t, []string{"keys"}, result.Columns)
			require.Equal(t, [][]interface{}{{[]interface{}{values[0], values[2], values[3]}}}, result.Rows)
		})
	}
}
