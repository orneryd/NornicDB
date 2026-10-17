package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A statement's result under its deadline (statementDeadlineOutcome).
func TestStatementDeadlineOutcome(t *testing.T) {
	result := &ExecuteResult{Columns: []string{"x"}}
	expired := func(t *testing.T) context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		ctx = withPipelineCancellation(ctx)
		cancel()
		probe := contextPipelineProbe(ctx)
		require.False(t, probe.poll(), "the probe sees the cancellation")
		return ctx
	}
	t.Run("a result built after a reader saw the cancellation is not returned", func(t *testing.T) {
		got, err := statementDeadlineOutcome(expired(t), result, nil)
		require.Nil(t, got)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("a committed statement keeps its result (review 4300778895)", func(t *testing.T) {
		ctx := expired(t)
		markStatementCommitted(ctx)
		got, err := statementDeadlineOutcome(ctx, result, nil)
		require.NoError(t, err)
		require.Same(t, result, got)
	})
	t.Run("a committed statement keeps its own error", func(t *testing.T) {
		ctx := expired(t)
		markStatementCommitted(ctx)
		own := errors.New("own")
		_, err := statementDeadlineOutcome(ctx, nil, own)
		require.Same(t, own, err)
	})
	t.Run("an error after the context ended is the context's error", func(t *testing.T) {
		_, err := statementDeadlineOutcome(expired(t), nil, errors.New("partial value"))
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("a live statement keeps its result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		got, err := statementDeadlineOutcome(withPipelineCancellation(ctx), result, nil)
		require.NoError(t, err)
		require.Same(t, result, got)
	})
	t.Run("commit marks every probe the statement reads", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		outer := withPipelineCancellation(ctx)
		admitted := admitStatementProbe(outer)
		markStatementCommitted(admitted)
		require.True(t, statementCommitted(admitted))
		require.True(t, statementCommitted(outer))
	})
}
