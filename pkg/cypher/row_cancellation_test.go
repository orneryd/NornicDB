package cypher

import (
	"cmp"
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/stretchr/testify/require"
)

type rowCheckCancellationContext struct {
	context.Context
	cancel context.CancelFunc
	after  int32
	checks atomic.Int32
}

func (ctx *rowCheckCancellationContext) Err() error {
	if ctx.checks.Add(1) >= ctx.after {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestMaterializedRowCancellationDuringSort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows := make([]int, 65536)
	for index := range rows {
		rows[index] = len(rows) - index
	}
	comparisons := 0
	err := stableSortRows(ctx, rows, func(left, right int) int {
		comparisons++
		if comparisons == 100 {
			cancel()
		}
		return cmp.Compare(left, right)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.LessOrEqual(t, comparisons, 100+rowCancellationInterval)
	require.PanicsWithValue(t, "comparator failure", func() {
		_ = stableSortRows(context.Background(), []int{2, 1}, func(int, int) int { panic("comparator failure") })
	})
}

func TestMaterializedRowCancellationDuringCollectAndValidation(t *testing.T) {
	executor := rowCancellationFixture(t, 1)
	rows := make([]pipelineRow, 4096)
	for index := range rows {
		rows[index] = pipelineRow{"x": []interface{}{int64(index)}}
	}
	for _, operator := range []string{"collect", "range", "subscript", "size", "with"} {
		t.Run(operator, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			checked := &rowCheckCancellationContext{Context: parent, cancel: cancel, after: 2}
			ctx := withExpressionFailureSlot(checked)
			var err error
			switch operator {
			case "collect":
				_, accepted := executor.evaluatePipelineAggregateWithContext(ctx, rows, "collect", "x", false)
				require.False(t, accepted)
				err = getExpressionFailure(ctx)
			case "range":
				err = executor.validatePipelineRangeArguments(ctx, rows, "RETURN range(0, 1)", "RETURN")
			case "subscript":
				err = executor.validatePipelineProjectionSubscripts(ctx, rows, "RETURN x[0]", "RETURN")
			case "size":
				err = executor.validatePipelineSizeArguments(ctx, rows, "RETURN size(x)", "RETURN")
			case "with":
				err = executor.validatePipelineWithRows(ctx, rows, "WITH x[0] AS y")
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, int32(2), checked.checks.Load())
		})
	}
}

func TestMaterializedRowCancellationStableOrder(t *testing.T) {
	executor := rowCancellationFixture(t, 1)
	rows := []pipelineRow{{"key": int64(2), "id": "first"}, {"key": int64(1), "id": "second"}, {"key": int64(2), "id": "third"}}
	require.True(t, executor.orderPipelineRowsWithScopes(context.Background(), rows, rows, []orderByTerm{{column: "key", descending: true}}))
	require.Equal(t, []string{"first", "third", "second"}, []string{rows[0]["id"].(string), rows[1]["id"].(string), rows[2]["id"].(string)})
}

func TestMaterializedRowCancellationDuringWithProjection(t *testing.T) {
	executor := rowCancellationFixture(t, 1)
	rows := make([]pipelineRow, 4096)
	for index := range rows {
		rows[index] = pipelineRow{"x": int64(index)}
	}
	for _, clause := range []string{"WITH x AS y", "WITH * ORDER BY x", "WITH x AS y ORDER BY y"} {
		t.Run(clause, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			checked := &rowCheckCancellationContext{Context: parent, cancel: cancel, after: 2}
			ctx := withExpressionFailureSlot(checked)
			_, accepted := executor.pipelineApplyWith(ctx, rows, clause)
			require.False(t, accepted)
			require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
			require.Equal(t, int32(2), checked.checks.Load())
		})
	}
}

func TestMaterializedKeyTraversalCheckpoint(t *testing.T) {
	encode := func(ctx context.Context, value interface{}) (key string, visited int) {
		checkpoint := bulk.NewCheckpoint(ctx)
		defer func() { visited = checkpoint.Steps() }()
		var stopped error
		defer bulk.RecoverStop(&stopped)
		return cypherValueKeyWithCheckpoint(value, &checkpoint, cypherEquivalenceContract), checkpoint.Steps()
	}
	for _, value := range []interface{}{nil, int64(7), "scalar", []interface{}{int64(1), map[string]interface{}{"b": false, "a": []interface{}{nil, "x"}}}, map[string]interface{}{"z": int64(2), "a": []interface{}{int64(1), true}}} {
		key, _ := encode(context.Background(), value)
		require.Equal(t, cypherValueKey(value, cypherEquivalenceContract), key, "checkpoints preserve the canonical bytes")
	}
	values := make([]interface{}, 4096)
	for i := range values {
		values[i] = int64(i)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	checked := &rowCheckCancellationContext{Context: parent, cancel: cancel, after: 2}
	ctx := withExpressionFailureSlot(checked)
	key, visited := encode(ctx, values)
	require.Empty(t, key, "an interrupted key must never be published")
	require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
	require.LessOrEqual(t, visited, rowCancellationInterval+1)
	for _, size := range []int{2, 4096} {
		fields := make(map[string]interface{}, size)
		for i := 0; i < size; i++ {
			fields[string(rune(i))] = int64(i)
		}
		parent, cancel := context.WithCancel(context.Background())
		checked := &rowCheckCancellationContext{Context: parent, cancel: cancel, after: 2}
		ctx := withExpressionFailureSlot(checked)
		key, _ := encode(ctx, fields)
		cancel()
		require.Empty(t, key)
		require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
	}
	require.PanicsWithValue(t, "key failure", func() {
		var stopped error
		defer bulk.RecoverStop(&stopped)
		panic("key failure")
	})
}

func TestMaterializedKeyNumericPolicyBytes(t *testing.T) {
	for _, value := range []interface{}{int64(1), 1.0, []interface{}{int64(1), 1.0}, map[string]interface{}{"k": []interface{}{1.0, int64(1)}}} {
		checkpoint := bulk.NewCheckpoint(context.Background())
		require.Equal(t, cypherValueKeyWithNumericPolicy(value, cypherNumericKinds, cypherEquivalenceContract), cypherValueKeyWithNumericCheckpoint(value, cypherNumericKinds, &checkpoint, cypherEquivalenceContract))
	}
}
