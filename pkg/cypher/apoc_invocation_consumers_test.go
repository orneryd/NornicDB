// Author: Codex NOR-184 lane. Public overflow/error proof for every consumer class.
package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/apocintegers"
	"github.com/stretchr/testify/require"
)

func TestAPOCInvocationConsumersNOR184(t *testing.T) {
	consumers, cleanup, err := apocintegers.Consumers("cypher")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanup()) })
	handlers := map[string]interface{}{}
	for _, consumer := range consumers {
		handlers["nor184."+consumer.Class+"_"+consumer.Name] = consumer.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184_consumers"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, consumer := range consumers {
			name := "nor184." + consumer.Class + "_" + consumer.Name
			check := func(t *testing.T, query string, params map[string]interface{}, want int64, success bool, overflow bool) {
				t.Helper()
				var result *ExecuteResult
				var err error
				require.NotPanics(t, func() { result, err = exec.Execute(context.Background(), query, params) })
				if !success {
					require.Error(t, err)
					if err != nil {
						require.NotContains(t, strings.ToLower(err.Error()), "panic:")
						if overflow {
							require.NotContains(t, err.Error(), "unsupported function signature")
							require.NotContains(t, err.Error(), "nil handler")
						}
					}
					return
				}
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{want}}, result.Rows)
			}
			for _, pair := range [][2]int64{{0, 0}, {0, math.MinInt64}, {12, -18}, {-12, -18}, {math.MinInt64, 0}, {math.MinInt64, math.MinInt64}, {math.MaxInt64, 2}, {4_000_000_000, 6_000_000_000}} {
				want, fits := apocintegers.Expected(consumer.Operation, pair[0], pair[1])
				for route, query := range []string{fmt.Sprintf("RETURN %s(%d,%d) AS v", name, pair[0], pair[1]), fmt.Sprintf("UNWIND [$a] AS a WITH a,$b AS b RETURN %s(a,b) AS v", name)} {
					t.Run(fmt.Sprintf("%s/programsDisabled=%t/%d/%d/route=%d", name, disabled, pair[0], pair[1], route), func(t *testing.T) {
						check(t, query, map[string]interface{}{"a": pair[0], "b": pair[1]}, want, fits, !fits)
					})
				}
			}
			for _, input := range apocintegers.Inputs {
				for _, swap := range []bool{false, true} {
					params := map[string]interface{}{"a": input.Value, "b": int64(6)}
					if swap {
						params["a"], params["b"] = params["b"], params["a"]
					}
					want, fits := apocintegers.Expected(consumer.Operation, input.Integer, 6)
					for route, query := range []string{fmt.Sprintf("RETURN %s($a,$b) AS v", name), fmt.Sprintf("WITH $a AS a,$b AS b RETURN %s(a,b) AS v", name)} {
						t.Run(fmt.Sprintf("%s/programsDisabled=%t/%s/swap=%t/route=%d", name, disabled, input.Name, swap, route), func(t *testing.T) { check(t, query, params, want, input.Valid && fits, input.Valid && !fits) })
					}
				}
			}
			for _, args := range []string{"", "1", "1,2,3"} {
				t.Run(fmt.Sprintf("%s/programsDisabled=%t/arity=%s", name, disabled, args), func(t *testing.T) { check(t, "RETURN "+name+"("+args+") AS v", nil, 0, false, false) })
			}
		}
	}
}
