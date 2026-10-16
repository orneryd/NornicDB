package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPreparationSyntaxCallerParity(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "syntax_caller_parity")
	for _, query := range []string{
		"RETURN 1 AS value", "RETURN 1 AS value;", "RETURN (1", "RETURN 1)", "RETURN [1,2", "RETURN {key:1}",
		"RETURN {1:2}", "RETURN 1 × 2", "RETURN '\\uZZZZ'", "RETURN 0xG", "RETURN 1e+", "MATCH (a) - > (b) RETURN a", "RETURN $",
		"RETURN 1 IN [1,2]", "RETURN 1 NOT IN [1,2]", "RETURN 1,", "RETURN 'a' 'b'", "RETURN n.", "RETURN [1,,2]", "RETURN 1 +",
		"SHOW DATABASES", "CYPHER 25 RETURN 1", "UNWIND [1,2] AS x WITH x RETURN x ORDER BY x",
	} {
		t.Run(query, func(t *testing.T) {
			legacy := NewStorageExecutor(engine).validateSyntax(query, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := pipelineCancellationForContext(ctx)
			checked := NewStorageExecutor(engine).validateSyntaxWithCancellation(query, false, probe)
			require.Equal(t, legacy == nil, checked == nil)
			if legacy != nil {
				require.Equal(t, legacy.Error(), checked.Error())
			}
			require.NoError(t, probe.err)
		})
	}
}

func TestPreparationSyntaxCallerCancellation(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "syntax_caller_cancel")
	query := "RETURN '" + strings.Repeat("value", 1025) + "' AS value"
	runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
		err := NewStorageExecutor(engine).validateSyntaxWithCancellation(query, false, probe)
		if probe.err != nil {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	probe := pipelineCancellationForContext(ctx)
	require.ErrorIs(t, NewStorageExecutor(engine).validateSyntaxWithCancellation(query, false, probe), context.Canceled)
}

func TestPreparationConfusableReaderCancellation(t *testing.T) {
	for _, query := range []string{
		"RETURN （" + strings.Repeat(" ", 257) + "1）",
		"MATCH (a) —" + strings.Repeat(" ", 257) + "> (b) RETURN a",
		"MATCH (a) ＜" + strings.Repeat(" ", 257) + "— (b) RETURN a",
		"RETURN '" + strings.Repeat("x'\"`\\", 257) + "'",
		"RETURN /*" + strings.Repeat("é", 257) + "*/ 1 //é\n",
		"RETURN \"é\" AS `é`",
		"RETURN \u20031\u2028",
		"RETURN '\xff'",
	} {
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			normalizeCypherSyntaxConfusablesWithCancellation(query, probe)
		})
	}
}

func TestPreparationValidatorBudgetBoundaries(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "validator_boundaries")
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for _, reader := range []struct {
		name string
		run  func(*StorageExecutor, string, *pipelineCancellation) error
	}{
		{"nornic", func(e *StorageExecutor, q string, p *pipelineCancellation) error {
			if p == nil {
				return e.validateSyntaxNornic(q)
			}
			return e.validateSyntaxNornicWithCancellation(q, p)
		}},
		{"antlr", func(e *StorageExecutor, q string, p *pipelineCancellation) error {
			if p == nil {
				return e.validateSyntaxANTLR(q, false)
			}
			return e.validateSyntaxANTLRWithCancellation(q, false, p)
		}},
	} {
		t.Run(reader.name, func(t *testing.T) {
			for _, query := range []string{"SHOW LIMITS", "RETURN 1 AS value", "RETURN '\\" + strings.Repeat("x", 129) + "' AS value", "RETURN n." + strings.Repeat(" ", 129) + "value", "RETURN 1 +" + strings.Repeat(" ", 129) + "*", "RETURN /*" + strings.Repeat("n. +*", 26) + "*/ 1 AS value"} {
				want := reader.run(NewStorageExecutor(engine), query, nil)
				runPreparationBudgetSamples(t, func(_ context.Context, probe *pipelineCancellation) {
					actual := reader.run(NewStorageExecutor(engine), query, probe)
					if probe.err != nil {
						require.ErrorIs(t, actual, context.Canceled)
					} else if want == nil {
						require.NoError(t, actual)
					} else {
						require.Error(t, actual)
					}
				}, func(calls int) []int {
					// Preserve all128 initial counter phases and sample admission,
					// an interval boundary, interior work and the final unwind.
					return []int{1, 2, 127, 128, 129, calls / 2, calls - 1, calls}
				}, phases...)
			}
		})
	}
}
