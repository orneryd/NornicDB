// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
//
// This package uses the official ANTLR Cypher grammar to parse queries
// into an AST that can be consumed by the executor.
package antlr

import (
	"fmt"
	"strings"
	"sync"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ParseResult contains the parsed ANTLR tree and any errors
type ParseResult struct {
	Tree   IScriptContext
	Errors []string
}

type pooledValidator struct {
	lexer         *CypherLexer
	tokens        *antlr.CommonTokenStream
	parser        *CypherParser
	errorListener *parseErrorListener
}

var validatorPool = sync.Pool{
	New: func() interface{} {
		input := antlr.NewInputStream("")
		lexer := NewCypherLexer(input)
		tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		parser := NewCypherParser(tokens)

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
	return ValidateWithCancellation(query, nil)
}

// ValidationWork is the statement's cancellation owner, as validation sees
// it: CheckBoundary reports false, and Err the reason, once the statement
// is cancelled.
type ValidationWork interface {
	Err() error
	CheckBoundary() bool
}

// ValidateWithCancellation is Validate under the statement's cancellation.
// The upstream runtime gives a running parse no way to stop, so the deadline
// is checked at the parse's boundaries: before it, between the SLL pass and
// the LL retry, and after it. A cancelled statement returns work.Err(), never
// a syntax verdict. A nil work is Validate.
func ValidateWithCancellation(query string, work ValidationWork) error {
	if work != nil && !work.CheckBoundary() {
		return work.Err()
	}
	cancelled, err := validate(query, work)
	if cancelled {
		return err
	}
	if work != nil && !work.CheckBoundary() {
		return work.Err()
	}
	return err
}

// validate is Validate's body; cancelled reports that it stopped at the
// boundary between its passes, so err is work.Err().
func validate(query string, work ValidationWork) (cancelled bool, err error) {
	query = strings.TrimSpace(query)
	if query == "" {
		message := localization.CypherANTLRValidateEmptyQuery()
		return false, localization.NewLocalizedError(string(message.ID), message, nil)
	}

	v := validatorPool.Get().(*pooledValidator)
	v.errorListener.reset()
	defer validatorPool.Put(v)

	var input antlr.CharStream
	if isASCII(query) {
		input = newASCIICharStream(query)
	} else {
		input = antlr.NewInputStream(query)
	}
	v.lexer.SetInputStream(input)
	v.lexer.Reset()

	v.tokens.SetTokenSource(v.lexer)
	v.parser.SetTokenStream(v.tokens)

	// Fast path: SLL prediction mode is usually faster; fall back to LL on errors.
	v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeSLL)
	_ = v.parser.Script()

	if len(v.errorListener.errors) > 0 {
		if work != nil && !work.CheckBoundary() {
			return true, work.Err()
		}
		// Retry with LL prediction mode to avoid rejecting valid queries that
		// require full context prediction.
		v.errorListener.reset()
		v.tokens.Seek(0)
		v.parser.SetTokenStream(v.tokens)
		v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeLL)
		_ = v.parser.Script()

		if len(v.errorListener.errors) > 0 {
			message := localization.CypherANTLRValidateSyntaxError(strings.Join(v.errorListener.errors, "; "))
			return false, localization.NewLocalizedError(string(message.ID), message, nil)
		}
	}
	return false, nil
}

// parseErrorListener collects syntax errors during parsing
type parseErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (e *parseErrorListener) reset() {
	e.errors = e.errors[:0]
}

func (e *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, ex antlr.RecognitionException) {
	e.errors = append(e.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
