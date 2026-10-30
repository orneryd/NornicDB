// Author: Codex implementation lane for NOR-184, 2026-10-26.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCLcmValuesNOR184(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range []string{"apoc.math.lcm", "apoc.number.lcm"} {
		info, ok := apoc.GetInfo(name)
		require.True(t, ok)
		handlers[name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, tc := range []struct{ a, b, want int64 }{
			{0, 0, 0}, {0, 5, 0}, {5, 0, 0}, {12, 18, 36},
			{-12, 18, 36}, {12, -18, 36}, {-12, -18, 36},
			{4_000_000_000, 6_000_000_000, 12_000_000_000},
		} {
			for _, query := range []string{
				fmt.Sprintf("RETURN apoc.math.lcm(%d,%d) AS v, apoc.number.lcm(%d,%d) AS alias", tc.a, tc.b, tc.a, tc.b),
				"WITH $a AS a, $b AS b RETURN apoc.math.lcm(a,b) AS v, apoc.number.lcm(a,b) AS alias",
			} {
				t.Run(fmt.Sprintf("programsDisabled=%t/%s/%d/%d", disabled, query, tc.a, tc.b), func(t *testing.T) {
					result, err := exec.Execute(context.Background(), query, map[string]interface{}{"a": tc.a, "b": tc.b})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{tc.want, tc.want}}, result.Rows)
				})
			}
		}
	}
}
