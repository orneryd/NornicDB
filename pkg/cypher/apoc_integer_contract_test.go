// Author: Codex NOR-184 implementation lane, 2026-10-29.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/apocintegers"
	"github.com/stretchr/testify/require"
)

func TestAPOCIntegerContractNOR184(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, namespace := range []string{"math", "number"} {
		for _, operation := range []string{"gcd", "lcm"} {
			name := "apoc." + namespace + "." + operation
			info, ok := apoc.GetInfo(name)
			require.True(t, ok)
			handlers[name] = info.Handler
		}
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor184_contract"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, namespace := range []string{"math", "number"} {
			for _, operation := range []string{"gcd", "lcm"} {
				name := "apoc." + namespace + "." + operation
				check := func(t *testing.T, query string, args map[string]interface{}, want int64, success bool) {
					t.Helper()
					var err error
					var result *ExecuteResult
					require.NotPanics(t, func() { result, err = exec.Execute(context.Background(), query, args) })
					if !success {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{want}}, result.Rows)
				}
				for _, a := range apocintegers.Values {
					for _, b := range apocintegers.Values {
						want, fits := apocintegers.Expected(operation, a, b)
						for route, query := range []string{fmt.Sprintf("RETURN %s(%d,%d) AS v", name, a, b), fmt.Sprintf("WITH $a AS a, $b AS b RETURN %s(a,b) AS v", name)} {
							t.Run(fmt.Sprintf("%s/programsDisabled=%t/%d/%d/route=%d", name, disabled, a, b, route), func(t *testing.T) { check(t, query, map[string]interface{}{"a": a, "b": b}, want, fits) })
						}
					}
				}
				for _, input := range apocintegers.Inputs {
					for _, other := range []int64{0, 6} {
						for _, swap := range []bool{false, true} {
							args := map[string]interface{}{"a": input.Value, "b": other}
							if swap {
								args["a"], args["b"] = args["b"], args["a"]
							}
							want, fits := apocintegers.Expected(operation, input.Integer, other)
							for route, query := range []string{fmt.Sprintf("RETURN %s($a,$b) AS v", name), fmt.Sprintf("WITH $a AS a, $b AS b RETURN %s(a,b) AS v", name)} {
								t.Run(fmt.Sprintf("%s/programsDisabled=%t/%s/%d/swap=%t/route=%d", name, disabled, input.Name, other, swap, route), func(t *testing.T) { check(t, query, args, want, input.Valid && fits) })
							}
						}
					}
				}
				for _, literal := range []struct {
					text    string
					integer int64
					valid   bool
				}{
					{"null", 0, false}, {"6.5", 0, false}, {"'12'", 0, false}, {"true", 0, false}, {"[12]", 0, false}, {"{n:12}", 0, false}, {"6.0", 6, true},
				} {
					want, fits := apocintegers.Expected(operation, literal.integer, 6)
					t.Run(fmt.Sprintf("%s/programsDisabled=%t/literal=%s", name, disabled, literal.text), func(t *testing.T) {
						check(t, fmt.Sprintf("RETURN %s(%s,6) AS v", name, literal.text), nil, want, literal.valid && fits)
					})
				}
			}
		}
	}
}

var integerContractBenchmarkResult *ExecuteResult

// Benchmarks use the same real Cypher invocation path on both signatures; no
// test-only legacy signature adapter is inserted into the measured call.
func BenchmarkAPOCIntegerContractNOR184(b *testing.B) {
	if err := apoc.Initialize(apocstorage.NewInMemoryStorage(), nil); err != nil {
		b.Fatal(err)
	}
	handlers := map[string]interface{}{}
	for _, name := range []string{"apoc.math.gcd", "apoc.math.lcm"} {
		info, ok := apoc.GetInfo(name)
		if !ok {
			b.Fatal(name)
		}
		handlers[name] = info.Handler
	}
	withPluginFunctions(b, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "nor184_bench"))
	for _, query := range []string{"RETURN apoc.math.gcd(12,-18) AS v", "RETURN apoc.math.lcm(12,18) AS v", "RETURN apoc.math.lcm(4000000000,6000000000) AS v"} {
		b.Run(query, func(b *testing.B) {
			for b.Loop() {
				var err error
				integerContractBenchmarkResult, err = exec.Execute(context.Background(), query, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Ordinary calls previously used scalar switches. Measure those public paths
// separately from the checked integer rows after invocation converges.
// Author: Codex NOR-184 lane.
func BenchmarkAPOCOrdinaryContractNOR184(b *testing.B) {
	if err := apoc.Initialize(apocstorage.NewInMemoryStorage(), nil); err != nil {
		b.Fatal(err)
	}
	handlers := map[string]interface{}{}
	for _, name := range []string{"apoc.text.reverse", "apoc.math.ceil", "apoc.scoring.normalize"} {
		info, ok := apoc.GetInfo(name)
		if !ok {
			b.Fatal(name)
		}
		handlers[name] = info.Handler
	}
	withPluginFunctions(b, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "nor184_ordinary_bench"))
	for _, query := range []string{"RETURN apoc.text.reverse('abcdef') AS v", "RETURN apoc.math.ceil(2.5) AS v", "RETURN apoc.scoring.normalize([1.0,2.0,3.0]) AS v"} {
		b.Run(query, func(b *testing.B) {
			for b.Loop() {
				var err error
				integerContractBenchmarkResult, err = exec.Execute(context.Background(), query, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
