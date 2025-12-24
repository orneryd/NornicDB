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
		return nil, fmt.Errorf("empty query")
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
		return result, fmt.Errorf("syntax error: %s", strings.Join(errorListener.errors, "; "))
	}

	return result, nil
}

// Validate validates a Cypher query using pooled ANTLR objects (lexer/parser/token stream).
//
// This is the hot path used when NORNICDB_PARSER=antlr for strict syntax validation.
// It is optimized for throughput and reduced allocations and does not return a parse tree.
func Validate(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return fmt.Errorf("empty query")
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
		// Retry with LL prediction mode to avoid rejecting valid queries that
		// require full context prediction.
		v.errorListener.reset()
		v.tokens.Seek(0)
		v.parser.SetTokenStream(v.tokens)
		v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeLL)
		_ = v.parser.Script()

		if len(v.errorListener.errors) > 0 {
			return fmt.Errorf("syntax error: %s", strings.Join(v.errorListener.errors, "; "))
		}
	}
	return nil
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
