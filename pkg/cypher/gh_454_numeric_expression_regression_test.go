package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH454_IntegerDivisionAndPowerSemantics(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh454"))
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			result, err := exec.Execute(ctx,
				"RETURN 7 / 2 AS int_div, 7.0 / 2 AS float_div, 7 % 3 AS m, 2 ^ 3 AS pow, 2 * 3 + 1 AS e", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(3), float64(3.5), int64(1), float64(8), int64(7)}}, result.Rows)

			result, err = exec.Execute(ctx,
				"WITH 2 AS a, 3 AS b RETURN a ^ b AS pow, a / b AS int_div", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{float64(8), int64(0)}}, result.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
