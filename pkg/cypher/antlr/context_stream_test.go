package antlr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelDuringLookahead struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringLookahead) Err() error {
	c.checks++
	if c.checks == 3 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestValidateContextCancellationReleasesValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	during := &cancelDuringLookahead{Context: ctx, cancel: cancel}
	require.ErrorIs(t, ValidateContext(during, "MATCH (n) WHERE n.x > 1 RETURN n"), context.Canceled)
	require.GreaterOrEqual(t, during.checks, 3, "cancellation must occur inside lookahead")
	require.NoError(t, Validate("RETURN 1"), "the interrupted pooled validator is reusable")
	require.Error(t, Validate("MATCH ("), "cancellation must not leave the next validation accepted")
}

func TestValidateContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ValidateContext(ctx, "RETURN 1"), context.Canceled)
}

type panicDuringLookahead struct {
	context.Context
	checks int
}

func (c *panicDuringLookahead) Err() error {
	c.checks++
	if c.checks == 3 {
		panic("unrelated prediction panic")
	}
	return c.Context.Err()
}

func TestValidateContextUnrelatedPanicPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	during := &panicDuringLookahead{Context: ctx}
	require.PanicsWithValue(t, "unrelated prediction panic", func() {
		_ = ValidateContext(during, "MATCH (n) WHERE n.x > 1 RETURN n")
	})
	require.NoError(t, Validate("RETURN 1"), "panic unwind releases the validator")
}
