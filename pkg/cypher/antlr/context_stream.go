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
	contextCharCancellation
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

func (s *contextCharCancellation) releaseContext() {
	s.ctx = context.Background()
	s.unwind = nil
}

func (s *contextCharStream) LA(k int) int { s.check(); return s.CharStream.LA(k) }
func (s *contextCharStream) Consume()     { s.check(); s.CharStream.Consume() }

// Embed the concrete ASCII stream so lookahead and text access retain their
// direct implementation. Lexer consumption polls every 1024 characters, even
// within one long token. Lookahead rethrows an interrupted match without
// polling the context or counting every character operation.
type contextASCIICharStream struct {
	asciiCharStream
	contextCharCancellation
}

func (s *contextASCIICharStream) LA(k int) int {
	s.rethrow()
	return s.asciiCharStream.LA(k)
}

func (s *contextASCIICharStream) Consume() {
	if s.i&1023 == 0 {
		s.poll()
	}
	s.asciiCharStream.Consume()
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
	if ascii {
		input := newASCIICharStream(query)
		if ctx.Done() != nil {
			stream := &contextASCIICharStream{asciiCharStream: *input}
			stream.ctx = ctx
			return stream
		}
		return input
	}
	if ctx.Done() != nil {
		stream := &contextCharStream{CharStream: antlr.NewIoStream(&contextInputReader{Reader: strings.NewReader(query), ctx: ctx})}
		stream.ctx = ctx
		return stream
	}
	return antlr.NewInputStream(query)
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
		// The character stream bounds cancellation inside and between tokens.
		// Check once more at EOF instead of polling again for every token.
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
