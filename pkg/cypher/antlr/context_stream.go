package antlr

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/antlr4-go/antlr/v4"
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

// contextCharStream checks within a token, rather than only between tokens.
// Poll at bounded intervals to keep large comments/strings cancellable without
// querying the context for every lexer lookahead.
type contextCharStream struct {
	antlr.CharStream
	ctx    context.Context
	checks uint
}

func (s *contextCharStream) check() {
	if s.checks&1023 == 0 {
		if err := s.ctx.Err(); err != nil {
			panic(predictionCanceled{err})
		}
	}
	s.checks++
}
func (s *contextCharStream) LA(k int) int { s.check(); return s.CharStream.LA(k) }
func (s *contextCharStream) Consume()     { s.check(); s.CharStream.Consume() }

func recoverPredictionCancellation(err *error) {
	if p := recover(); p != nil {
		if canceled, ok := p.(predictionCanceled); ok {
			*err = canceled.err
		} else {
			panic(p)
		}
	}
}

func contextInputStream(ctx context.Context, query string) antlr.CharStream {
	ascii := true
	for i := 0; i < len(query); i++ {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				panic(predictionCanceled{err})
			}
		}
		if query[i] >= 0x80 {
			ascii = false
			break
		}
	}
	var input antlr.CharStream
	if ascii {
		input = newASCIICharStream(query)
	} else if ctx.Done() != nil {
		input = antlr.NewIoStream(&contextInputReader{Reader: strings.NewReader(query), ctx: ctx})
	} else {
		input = antlr.NewInputStream(query)
	}
	if ctx.Done() != nil {
		input = &contextCharStream{CharStream: input, ctx: ctx}
	}
	return input
}

// LexContext tokenizes for consumers such as log redaction using the same
// synchronous cancellation seam as validation. No partial tokens escape.
func LexContext(ctx context.Context, query string) (tokens []antlr.Token, err error) {
	defer func() {
		if err != nil {
			tokens = nil
		}
	}()
	defer recoverPredictionCancellation(&err)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	lexer := NewCypherLexer(contextInputStream(ctx, query))
	listener := &parseErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		token := lexer.NextToken()
		if len(listener.errors) != 0 {
			return nil, fmt.Errorf("invalid Cypher token: %s", listener.errors[0])
		}
		if token == nil || token.GetTokenType() == antlr.TokenEOF {
			return tokens, ctx.Err()
		}
		tokens = append(tokens, token)
	}
}

type contextInputReader struct {
	io.Reader
	ctx context.Context
}

func (r *contextInputReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		panic(predictionCanceled{err})
	}
	return r.Reader.Read(p)
}
