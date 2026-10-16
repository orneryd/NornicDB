package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParameterPreparationCancellationSites(t *testing.T) {
	values := make([]interface{}, 1024)
	for i := range values {
		values[i] = map[string][]int{"items": {i, i + 1}}
	}
	fields := make(map[string]interface{}, 1024)
	for i := range values {
		fields[fmt.Sprint(i)] = values[i]
	}
	for _, stage := range []struct {
		name string
		run  func(*pipelineCancellation)
	}{
		{"normalization", func(probe *pipelineCancellation) {
			normalizeQueryParametersWithCancellation(map[string]interface{}{"xs": values}, probe)
		}},
		{"typed_map", func(probe *pipelineCancellation) {
			normalizeParameterValueWithCancellation(map[string][]int{"xs": {1, 2}}, probe)
		}},
		{"typed_array", func(probe *pipelineCancellation) {
			normalizeParameterValueWithCancellation([1024]map[string]int{}, probe)
		}},
		{"row_list", func(probe *pipelineCancellation) { parameterRowValueWithCancellation(values, probe) }},
		{"row_map", func(probe *pipelineCancellation) { parameterRowValueWithCancellation(fields, probe) }},
		{"row_bindings", func(probe *pipelineCancellation) { parameterRowsOfWithCancellation(fields, true, probe) }},
	} {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				probe := pipelineCancellationForContext(ctx)
				stage.run(probe)
				return probe.err
			})
		})
	}
}

func TestParameterPreparationHealthyParity(t *testing.T) {
	for _, value := range []interface{}{nil, "text", int64(2), []int{1, 2}, map[string]int{"a": 1}, map[string]interface{}{"a": []int{1, 2}}, []interface{}{map[string]int{"a": 2}}, [2]map[string]int{{"a": 1}, {"b": 2}}} {
		ctx, cancel := context.WithCancel(context.Background())
		probe := &pipelineCancellation{ctx: ctx}
		expected, changed := normalizeParameterValue(value)
		actual, actualChanged := normalizeParameterValueWithCancellation(value, probe)
		require.Equal(t, expected, actual)
		require.Equal(t, changed, actualChanged)
		require.Equal(t, parameterRowValue(value), parameterRowValueWithCancellation(value, probe))
		cancel()
	}
}

func TestUnusedLargeParameterExactDeadline(t *testing.T) {
	values := make([]interface{}, 16<<20)
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "parameter_deadline"))
			readers := base.ActiveReaders()
			if explicit {
				_, err := executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				defer executor.handleRollback()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			_, err := executor.Execute(ctx, "RETURN 1", map[string]interface{}{"unused": values})
			returned := time.Now()
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
			if explicit {
				_, _ = executor.handleRollback()
			}
			require.Equal(t, readers, base.ActiveReaders())
			t.Logf("explicit=%t deadline=%s return=%s overrun=%s", explicit, deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(deadline))
		})
	}
}

type preparationLimitChecker struct {
	calls, released int
	deadline        time.Time
}

// Preserve the transaction and namespace wrapper chain while adding limits.
type preparationLimitedEngine struct {
	*storage.NamespacedEngine
	checker *preparationLimitChecker
}

func (engine preparationLimitedEngine) GetInnerEngine() storage.Engine {
	return engine.NamespacedEngine
}
func (engine preparationLimitedEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return engine.checker
}
func (checker *preparationLimitChecker) CheckQueryRate() error       { return nil }
func (checker *preparationLimitChecker) GetQueryLimits() interface{} { return nil }
func (checker *preparationLimitChecker) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	checker.calls++
	limited, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	checker.deadline, _ = limited.Deadline()
	return limited, func() { cancel(); checker.released++ }, nil
}

func TestConfiguredDeadlineIncludesParameterPreparation(t *testing.T) {
	values := make([]interface{}, 16<<20)
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			checker := &preparationLimitChecker{}
			executor := NewStorageExecutor(preparationLimitedEngine{NamespacedEngine: storage.NewNamespacedEngine(base, "configured_preparation"), checker: checker})
			readers := base.ActiveReaders()
			if explicit {
				_, err := executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
			}
			before := checker.calls
			_, err := executor.Execute(context.Background(), "RETURN 1", map[string]interface{}{"unused": values})
			returned := time.Now()
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, before+1, checker.calls, "one admission/context per statement")
			require.Equal(t, checker.calls, checker.released, "each admitted statement must release its permit")
			require.Less(t, returned.Sub(checker.deadline), 250*time.Millisecond)
			if explicit {
				_, _ = executor.handleRollback()
			}
			require.Equal(t, readers, base.ActiveReaders())
			t.Logf("background=true explicit=%t configured_deadline=%s returned=%s overrun=%s", explicit, checker.deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(checker.deadline))
		})
	}
}
