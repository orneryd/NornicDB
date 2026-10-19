// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
//
// This package uses the official ANTLR Cypher grammar to parse queries
// into an AST that can be consumed by the executor.
package antlr

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ParseResult contains the parsed ANTLR tree and any errors
type ParseResult struct {
	Tree   IScriptContext
	Errors []string
}

// compositeKeyword recognizes administration-only words without reserving them
// in the lexer used by both parser paths and the canonical redaction owner.
func (p *CypherParser) compositeKeyword(word string) bool {
	return strings.EqualFold(p.GetCurrentToken().GetText(), word)
}

type pooledValidator struct {
	lexer         *CypherLexer
	tokens        *antlr.CommonTokenStream
	stream        *validationTokenStream
	parser        *CypherParser
	errorListener *parseErrorListener
}

var validatorPool = sync.Pool{
	New: func() interface{} {
		input := antlr.NewInputStream("")
		lexer := NewCypherLexer(input)
		tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		stream := &validationTokenStream{CommonTokenStream: tokens}
		parser := NewCypherParser(stream)

		errorListener := &parseErrorListener{}

		parser.RemoveErrorListeners()
		parser.AddErrorListener(errorListener)
		lexer.RemoveErrorListeners()
		lexer.AddErrorListener(errorListener)

		// Validation does not need parse tree listeners.
		parser.BuildParseTrees = false

		return &pooledValidator{
			lexer:         lexer,
			tokens:        tokens,
			stream:        stream,
			parser:        parser,
			errorListener: errorListener,
		}
	},
}

// Parse parses a Cypher query string using ANTLR and returns the parse tree
func Parse(query string) (*ParseResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		message := localization.CypherANTLRParseEmptyQuery()
		return nil, localization.NewLocalizedError(string(message.ID), message, nil)
	}

	// Create ANTLR input stream
	input := antlr.NewInputStream(query)

	// Create lexer
	lexer := NewCypherLexer(input)

	// Create token stream
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create parser
	parser := NewCypherParser(tokens)

	// Add error listener
	errorListener := &parseErrorListener{}
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	// Parse
	tree := parser.Script()

	result := &ParseResult{
		Tree:   tree,
		Errors: errorListener.errors,
	}

	// Check for errors
	if len(errorListener.errors) > 0 {
		message := localization.CypherANTLRParseSyntaxError(strings.Join(errorListener.errors, "; "))
		return result, localization.NewLocalizedError(string(message.ID), message, nil)
	}

	return result, nil
}

// Validate validates a Cypher query using pooled ANTLR objects (lexer/parser/token stream).
//
// This is the hot path used when NORNICDB_PARSER=antlr for strict syntax validation.
// It is optimized for throughput and reduced allocations and does not return a parse tree.
func Validate(query string) error {
	return ValidateContext(context.Background(), query)
}

// ValidateContext keeps native syntax validation within the statement's lifetime.
// Token consumption and prediction lookahead observe cancellation even when all
// tokens have already been buffered for an SLL-to-LL retry.
func ValidateContext(ctx context.Context, query string) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	query = strings.TrimSpace(query)
	if query == "" {
		message := localization.CypherANTLRValidateEmptyQuery()
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}

	v := validatorPool.Get().(*pooledValidator)
	v.errorListener.reset()
	v.stream.ctx, v.stream.done = ctx, ctx.Done()
	v.stream.deadline, v.stream.hasDeadline = ctx.Deadline()
	defer func() {
		v.stream.ctx, v.stream.done = nil, nil
		v.stream.deadline, v.stream.hasDeadline = time.Time{}, false
		validatorPool.Put(v)
		if caught := recover(); caught != nil {
			if canceled, ok := caught.(validationCancellation); ok {
				retErr = canceled.err
			} else {
				panic(caught)
			}
		}
	}()

	var input antlr.CharStream
	if isASCII(query) {
		input = newASCIICharStream(query)
	} else {
		input = antlr.NewInputStream(query)
	}
	v.lexer.SetInputStream(input)
	v.lexer.Reset()

	v.tokens.SetTokenSource(v.lexer)
	v.parser.SetTokenStream(v.stream)

	// Fast path: SLL prediction mode is usually faster; fall back to LL on errors.
	v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeSLL)
	_ = v.parser.Script()

	if len(v.errorListener.errors) > 0 {
		// Retry with LL prediction mode to avoid rejecting valid queries that
		// require full context prediction.
		v.errorListener.reset()
		v.tokens.Seek(0)
		v.parser.SetTokenStream(v.stream)
		v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeLL)
		_ = v.parser.Script()

		if len(v.errorListener.errors) > 0 {
			message := localization.CypherANTLRValidateSyntaxError(strings.Join(v.errorListener.errors, "; "))
			return localization.NewLocalizedError(string(message.ID), message, nil)
		}
	}
	return nil
}

// The runtime owns buffering and prediction; this adapter adds the statement's
// cancellation boundary without modifying generated grammar or runtime code.
type validationTokenStream struct {
	*antlr.CommonTokenStream
	ctx         context.Context
	done        <-chan struct{}
	deadline    time.Time
	hasDeadline bool
}
type validationCancellation struct{ err error }

func (s *validationTokenStream) check() {
	// Context timers may run after their deadline under CPU load. Prediction
	// must honor the same deadline without waiting for that timer goroutine.
	if s.hasDeadline && !time.Now().Before(s.deadline) {
		panic(validationCancellation{context.DeadlineExceeded})
	}
	if s.done != nil {
		select {
		case <-s.done:
			panic(validationCancellation{s.ctx.Err()})
		default:
		}
	}
}
func (s *validationTokenStream) LA(i int) int         { s.check(); return s.CommonTokenStream.LA(i) }
func (s *validationTokenStream) LT(i int) antlr.Token { s.check(); return s.CommonTokenStream.LT(i) }
func (s *validationTokenStream) Consume()             { s.check(); s.CommonTokenStream.Consume() }
func (s *validationTokenStream) Seek(i int)           { s.check(); s.CommonTokenStream.Seek(i) }

// parseErrorListener collects syntax errors during parsing
type parseErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (e *parseErrorListener) reset() {
	e.errors = e.errors[:0]
}

func (e *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, ex antlr.RecognitionException) {
	// Do not persist ANTLR's token/excerpt text: a malformed password can be
	// the offending token, including in lexer errors before a token exists.
	// Parse, Validate, and ParseResult share this structural diagnostic owner.
	message := localization.CypherANTLRUnexpectedInput(line, column)
	e.errors = append(e.errors, message.Fallback)
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
