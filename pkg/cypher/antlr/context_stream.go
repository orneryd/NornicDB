package antlr

import (
	"context"

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
