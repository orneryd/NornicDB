package antlr

import (
	"context"
	"strings"
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
	if c.checks == 6 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestValidateContextCancellationReleasesValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	during := &cancelDuringLookahead{Context: ctx, cancel: cancel}
	require.ErrorIs(t, ValidateContext(during, "MATCH (n) WHERE n.x > 1 RETURN n"), context.Canceled)
	require.GreaterOrEqual(t, during.checks, 6, "cancellation must occur inside lookahead")
	func() {
		pooled := validatorPool.Get().(*pooledValidator)
		defer validatorPool.Put(pooled)
		var requestContext context.Context
		switch input := pooled.lexer.GetInputStream().(type) {
		case *contextCharStream:
			requestContext = input.ctx
		case *contextASCIICharStream:
			requestContext = input.ctx
		}
		if requestContext != nil {
			require.Equal(t, context.Background(), requestContext, "idle validators must release the statement context")
		}
	}()
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
	if c.checks == 6 {
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

type cancelAtCheck struct {
	context.Context
	cancel         context.CancelFunc
	checks, target int
}

func (c *cancelAtCheck) Err() error {
	c.checks++
	if c.checks == c.target {
		c.cancel()
	}
	return c.Context.Err()
}

func TestValidateContextSingleTokenCancellation(t *testing.T) {
	for _, query := range []string{
		"/*" + strings.Repeat("x", 1<<20) + "*/ RETURN 1",
		"RETURN '" + strings.Repeat("x", 1<<20) + "'",
		"RETURN " + strings.Repeat("x", 1<<20),
	} {
		ctx, cancel := context.WithCancel(t.Context())
		// Beyond the bounded ASCII preflight, but inside the first long token.
		during := &cancelAtCheck{Context: ctx, cancel: cancel, target: len(query)/1024 + 16}
		require.ErrorIs(t, ValidateContext(during, query), context.Canceled)
		require.GreaterOrEqual(t, during.checks, during.target)
		cancel()
		require.NoError(t, Validate("RETURN 1"))
	}
}

func TestLexContextSingleTokenCancellation(t *testing.T) {
	query := "/*" + strings.Repeat("x", 1<<20) + "*/ RETURN 1"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	during := &cancelAtCheck{Context: ctx, cancel: cancel, target: len(query)/1024 + 16}
	tokens, err := LexContext(during, query)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, tokens)
}

func TestContextUnicodeInputPreparationCancellation(t *testing.T) {
	query := "RETURN '" + strings.Repeat("界", 1<<20) + "'"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	during := &cancelAtCheck{Context: ctx, cancel: cancel, target: 32}
	require.ErrorIs(t, ValidateContext(during, query), context.Canceled)
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	during2 := &cancelAtCheck{Context: ctx2, cancel: cancel2, target: 32}
	tokens, err := LexContext(during2, query)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, tokens)
}

func TestContextWhitespacePreparationCancellation(t *testing.T) {
	for _, query := range []string{strings.Repeat(" ", 1<<20), strings.Repeat(" ", 1<<20)} {
		ctx, cancel := context.WithCancel(t.Context())
		during := &cancelAtCheck{Context: ctx, cancel: cancel, target: 16}
		require.ErrorIs(t, ValidateContext(during, query), context.Canceled)
		cancel()
	}
}
