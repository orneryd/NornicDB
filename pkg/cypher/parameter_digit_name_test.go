package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A parameter name may start with a digit ($0hello, $1abc, $00), as in Neo4j
// 2026.09, with either parser; a number glued to a word elsewhere stays a
// SyntaxError (#907).
func TestParameterNameStartingWithDigit(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			if parser == "antlr" {
				defer config.WithANTLRParser()()
			}
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "digit_parameter"))
			ctx := context.Background()
			params := map[string]interface{}{"0hello": int64(1), "1abc": "x", "00": true, "1_0": 2.5, "0": int64(9)}
			result, err := exec.Execute(ctx, "RETURN $0hello AS a, $1abc AS b, $00 AS c, $1_0 AS d, $0 AS e", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), "x", true, 2.5, int64(9)}}, result.Rows)

			_, err = exec.Execute(ctx, "RETURN $0hello AS v", nil)
			require.ErrorContains(t, err, "0hello")

			_, err = exec.Execute(ctx, "RETURN 1AS x", nil)
			require.Error(t, err)
			result, err = exec.Execute(ctx, "RETURN 0x1F AS v, [1, 2][0] AS w", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(31), int64(1)}}, result.Rows)
		})
	}
}
