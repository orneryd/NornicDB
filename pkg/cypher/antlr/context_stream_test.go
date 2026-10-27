package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"context"
	"strings"
	"testing"

	"github.com/antlr4-go/antlr/v4"
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
	require.ErrorIs(t, ValidateContext(during, "MATCH (n:ColdCancellationContext) WHERE n.x > 1 RETURN n", cyphertext.RawSource), context.Canceled)
	require.GreaterOrEqual(t, during.checks, 6, "cancellation must occur inside lookahead")
	require.NoError(t, Validate("RETURN 1"), "a canceled product must not contaminate later validation")
	require.Error(t, Validate("MATCH ("), "cancellation must not leave the next validation accepted")
}

func TestValidateContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ValidateContext(ctx, "RETURN 1", cyphertext.RawSource), context.Canceled)
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
		_ = ValidateContext(during, "MATCH (n:ColdCancellationContext) WHERE n.x > 1 RETURN n", cyphertext.RawSource)
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

// TestContextCancellationProtocols retains the lexer/validator assertions over
// one cancellation fixture. Each call gets its own context and counter; no
// canceled product may escape or contaminate later validation.
func TestContextCancellationProtocols(t *testing.T) {
	spaces := strings.Repeat(" ", 1<<20)
	cases := []struct {
		name, raw, accepted string
		target              int
		singleToken         bool
	}{
		{"comment", "/*" + strings.Repeat("x", 1<<20) + "*/ RETURN 1", "", 0, true},
		{"string", "RETURN '" + strings.Repeat("x", 1<<20) + "'", "", 0, true},
		{"identifier", "RETURN " + strings.Repeat("x", 1<<20), "", 0, true},
		{"unicode-preparation", "RETURN '" + strings.Repeat("界", 1<<20) + "'", "", 32, false},
		{"ASCII-whitespace", spaces, spaces, 16, false},
		{"Unicode-whitespace", strings.Repeat(" ", 1<<20), spaces, 16, false},
	}
	operations := []struct {
		name string
		lex  bool
		run  func(context.Context, string, SourceStage) ([]antlr.Token, error)
	}{
		{"validate", false, func(ctx context.Context, query string, stage SourceStage) ([]antlr.Token, error) {
			return nil, ValidateContext(ctx, query, stage)
		}},
		{"lex", true, LexContext},
	}
	for _, tc := range cases {
		for _, stage := range []SourceStage{RawSource, AcceptedSource} {
			for _, operation := range operations {
				stageName := "raw"
				query := tc.raw
				if stage == AcceptedSource {
					stageName = "accepted"
					if tc.accepted != "" {
						query = tc.accepted
					}
				}
				t.Run(tc.name+"/"+stageName+"/"+operation.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					target := tc.target
					if tc.singleToken {
						target = len(query)/1024 + 16
					}
					during := &cancelAtCheck{Context: ctx, cancel: cancel, target: target}
					tokens, err := operation.run(during, query, stage)
					require.ErrorIs(t, err, context.Canceled)
					if operation.lex {
						require.Nil(t, tokens)
					}
					if tc.singleToken && !operation.lex {
						require.GreaterOrEqual(t, during.checks, during.target)
						cancel()
						require.NoError(t, Validate("RETURN 1"))
					}
				})
			}
		}
	}
}

// Force the interruption inside Match, where ANTLR safeMatch swallows a
// non-recognition panic. Both streams must rethrow it at NextToken's LA.
func TestContextLexerUnwindEscapesSafeMatch(t *testing.T) {
	for _, ascii := range []bool{true, false} {
		for _, unrelated := range []bool{false, true} {
			ctx, cancel := context.WithCancel(t.Context())
			var during context.Context
			if unrelated {
				during = &panicDuringLookahead{Context: ctx}
			} else {
				during = &cancelDuringLookahead{Context: ctx, cancel: cancel}
			}
			query := "RETURN '" + strings.Repeat("界", 1<<16) + "'"
			if ascii {
				query = "RETURN '" + strings.Repeat("x", 1<<16) + "'"
			}
			source, _ := sourceInputStream(context.Background(), cyphertext.NewSource(t.Context(), query, cyphertext.AcceptedSource))
			input := &contextSourceCharStream{sourceCharStream: *source, contextCharCancellation: contextCharCancellation{ctx: during}}

			lex := func() (err error) {
				defer recoverPredictionCancellation(&err)
				lexer := NewCypherLexer(input)
				for lexer.NextToken().GetTokenType() != antlr.TokenEOF {
				}
				return nil
			}
			if unrelated {
				require.PanicsWithValue(t, "unrelated prediction panic", func() { _ = lex() })
			} else {
				require.ErrorIs(t, lex(), context.Canceled)
			}
			cancel()
		}
	}
}
