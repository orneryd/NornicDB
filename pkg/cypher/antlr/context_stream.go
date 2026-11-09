package antlr

import (
	"context"
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// predictionCanceled unwinds only this parse, through ANTLR's public stream
// interface. The runtime finishes an in-progress prediction step before it
// asks for lookahead again; no parser work is detached from the caller.
type predictionCanceled struct{ err error }

type contextTokenStream struct {
	antlr.TokenStream
	ctx context.Context
}

func (s *contextTokenStream) check() {
	if err := s.ctx.Err(); err != nil {
		panic(predictionCanceled{err})
	}
}

func (s *contextTokenStream) LA(k int) int {
	s.check()
	return s.TokenStream.LA(k)
}

func (s *contextTokenStream) LT(k int) antlr.Token {
	s.check()
	return s.TokenStream.LT(k)
}

func (s *contextTokenStream) Consume() {
	s.check()
	s.TokenStream.Consume()
}

// Shared cancellation owner for both character representations. ASCII consumes
// poll by input position; the Unicode adapter also bounds lexer lookahead.
type contextCharCancellation struct {
	ctx    context.Context
	checks uint
	unwind any
}

func (s *contextCharCancellation) check() {
	s.rethrow()
	if s.checks&1023 == 0 {
		s.poll()
	}
	s.checks++
}

func (s *contextCharCancellation) poll() {
	// ANTLR's lexer safeMatch recovers non-recognition panics without
	// rethrowing them. Preserve the outcome so NextToken's following LA,
	// outside safeMatch, unwinds synchronously instead of emitting a token
	// without consuming input. Preserve unrelated panics as well.
	defer func() {
		if p := recover(); p != nil {
			s.unwind = p
			panic(p)
		}
	}()
	if err := s.ctx.Err(); err != nil {
		panic(predictionCanceled{err})
	}
}

func (s *contextCharCancellation) rethrow() {
	if s.unwind != nil {
		panic(s.unwind)
	}
}

func recoverPredictionCancellation(err *error) {
	if p := recover(); p != nil {
		if canceled, ok := p.(predictionCanceled); ok {
			*err = canceled.err
		} else {
			panic(p)
		}
	}
}

// LexContext tokenizes for consumers such as log redaction using the same
// synchronous cancellation seam as validation. No partial tokens escape.
func LexContext(ctx context.Context, query string, stage cyphertext.SourceStage) (tokens []antlr.Token, err error) {
	defer func() {
		if err != nil {
			tokens = nil
		}
	}()
	defer recoverPredictionCancellation(&err)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	projection := cyphertext.NewSource(ctx, query, stage)
	if projection.Err != nil {
		return nil, projection.Err
	}
	_, input := sourceInputStream(ctx, projection)
	lexer := NewCypherLexer(input)
	listener := &parseErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)
	for {
		// The character stream bounds cancellation inside and between tokens.
		// Check once more at EOF instead of polling again for every token.
		token := lexer.NextToken()
		if len(listener.errors) != 0 {
			return nil, fmt.Errorf("invalid Cypher token: %s", listener.errors[0].Fallback)
		}
		if token == nil || token.GetTokenType() == antlr.TokenEOF {
			return tokens, ctx.Err()
		}
		tokens = append(tokens, token)
	}
}
