package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type pipelineUnvisitedKey struct{}

func (pipelineUnvisitedKey) GoString() string { panic("cancelled tuple visited its remaining column") }

func TestPipelineLegacyWideColumnCancellation(t *testing.T) {
	wide := pipelineRow{}
	for i := 0; i < 1025; i++ {
		wide[fmt.Sprintf("column%04d", i)] = i
	}
	runPipelineCancelPointStage(t, func(ctx context.Context) error {
		_ = pipelineColumns(ctx, []pipelineRow{wide}, true)
		return getExpressionFailure(ctx)
	})
}

func TestPipelineLegacyTupleStopsAfterNestedCancellation(t *testing.T) {
	for _, run := range []func(context.Context){
		func(ctx context.Context) {
			row := pipelineRow{"nested": make([]interface{}, 1025), "tail": pipelineUnvisitedKey{}}
			rows := []pipelineRow{row}
			_, _ = deduplicatePipelineRowsWithScopes(ctx, rows, rows, []string{"nested", "tail"})
		},
		func(ctx context.Context) {
			_ = deduplicatePipelineResultRows(ctx, [][]interface{}{{make([]interface{}, 1025), pipelineUnvisitedKey{}}})
		},
	} {
		ctx := withExpressionFailureSlot(&pipelineCountingContext{Context: context.Background(), stop: 2})
		require.NotPanics(t, func() { run(ctx) })
		require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
	}
}
