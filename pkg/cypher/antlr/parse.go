// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
//
// This package uses the official ANTLR Cypher grammar to parse queries
// into an AST that can be consumed by the executor.
package antlr

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

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

// ValidationWork borrows a statement's one cancellation budget. Boundary
// observations use that same owner's slow path, not another context guard.
type ValidationWork interface {
	Continue() bool
	Err() error
	CheckBoundary() bool
}
type validationCancelled struct{}

func ValidateWithCancellation(query string, work ValidationWork) error {
	return validateWithCancellation(query, work, nil)
}
func validateWithCancellation(query string, work ValidationWork, create func() *pooledValidator) (err error) {
	var v *pooledValidator
	var lexerSimulator *antlr.LexerATNSimulator
	completed := false
	var check func()
	if work != nil {
		check = func() {
			if !work.Continue() {
				panic(validationCancelled{})
			}
		}
	}
	defer func() {
		stopped := recover()
		if v != nil {
			v.parser.Interpreter.SetCancellationCheck(nil)
			if lexerSimulator != nil {
				lexerSimulator.SetCancellationCheck(nil)
			}
			v.tokens.SetCancellationCheck(nil)
			// SetTokenStream/SetInputStream detach before their fixed reset;
			// they cannot seek/fetch the interrupted query during cleanup.
			v.tokens.SetTokenSource(nil)
			v.parser.SetTokenStream(nil)
			v.lexer.SetInputStream(nil)
			v.errorListener.reset()
			v.errorListener.work = nil
		}
		if stopped != nil {
			if _, ok := stopped.(validationCancelled); !ok {
				panic(stopped)
			}
			err = work.Err()
			if err == nil {
				panic("validation cancellation without an error")
			}
			return
		}
		if completed && v != nil && create == nil {
			validatorPool.Put(v)
		}
	}()
	if work != nil && !work.CheckBoundary() {
		return work.Err()
	}
	query = trimValidationSpace(query, check)
	if query == "" {
		message := localization.CypherANTLRValidateEmptyQuery()
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}

	if create == nil {
		v = validatorPool.Get().(*pooledValidator)
	} else {
		v = create()
	}
	v.errorListener.reset()
	v.errorListener.work = check
	lexerSimulator = v.lexer.GetInterpreter().(*antlr.LexerATNSimulator)
	lexerSimulator.SetCancellationCheck(check)
	v.parser.Interpreter.SetCancellationCheck(check)
	v.tokens.SetCancellationCheck(check)

	var input antlr.CharStream
	if isASCIIWithWork(query, check) {
		input = newASCIICharStream(query)
	} else {
		input = antlr.NewInputStreamWithWork(query, check)
	}
	v.lexer.SetInputStream(input)
	v.lexer.Reset()

	v.tokens.SetTokenSource(v.lexer)
	v.parser.SetTokenStream(v.tokens)

	// Fast path: SLL prediction mode is usually faster; fall back to LL on errors.
	v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeSLL)
	_ = v.parser.Script()

	if v.errorListener.count() > 0 {
		// Retry with LL prediction mode to avoid rejecting valid queries that
		// require full context prediction.
		v.errorListener.reset()
		v.tokens.Seek(0)
		v.parser.SetTokenStream(v.tokens)
		v.parser.Interpreter.SetPredictionMode(antlr.PredictionModeLL)
		_ = v.parser.Script()

		if v.errorListener.count() > 0 {
			message := localization.CypherANTLRValidateSyntaxError(v.errorListener.join("; "))
			if work != nil && !work.CheckBoundary() {
				return work.Err()
			}
			completed = true
			return localization.NewLocalizedError(string(message.ID), message, nil)
		}
	}
	if work != nil && !work.CheckBoundary() {
		return work.Err()
	}
	completed = true
	return nil
}

// parseErrorListener collects syntax errors during parsing
type parseErrorListener struct {
	*antlr.DefaultErrorListener
	errors     []string
	work       func()
	head, tail *validationErrorPage
	errorCount int
}

type validationErrorPage struct {
	errors [64]string
	used   int
	next   *validationErrorPage
}

func (e *parseErrorListener) reset() {
	e.errors = nil
	e.head = nil
	e.tail = nil
	e.errorCount = 0
}

func (e *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, ex antlr.RecognitionException) {
	if e.work == nil {
		e.errors = append(e.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
		return
	}
	e.work()
	text := antlr.ConcatTextWithWork(e.work, "line ", strconv.Itoa(line), ":", strconv.Itoa(column), " ", msg)
	if e.tail == nil || e.tail.used == len(e.tail.errors) {
		page := &validationErrorPage{}
		e.work()
		if e.tail == nil {
			e.head = page
		} else {
			e.tail.next = page
		}
		e.tail = page
	}
	e.tail.errors[e.tail.used] = text
	e.tail.used++
	e.errorCount++
}

func (e *parseErrorListener) count() int { return len(e.errors) + e.errorCount }
func (e *parseErrorListener) join(separator string) string {
	if e.work == nil {
		return strings.Join(e.errors, separator)
	}
	length := 0
	for page := e.head; page != nil; page = page.next {
		for i := 0; i < page.used; i++ {
			e.work()
			length += len(page.errors[i])
		}
	}
	if e.errorCount > 1 {
		length += (e.errorCount - 1) * len(separator)
	}
	var result strings.Builder
	e.work()
	result.Grow(length)
	count := 0
	for page := e.head; page != nil; page = page.next {
		for i := 0; i < page.used; i++ {
			if count > 0 {
				result.WriteString(separator)
			}
			text := page.errors[i]
			for len(text) > 0 {
				e.work()
				size := min(len(text), 4096)
				result.WriteString(text[:size])
				text = text[size:]
			}
			count++
		}
	}
	return result.String()
}

func isASCII(s string) bool {
	return isASCIIWithWork(s, nil)
}
func isASCIIWithWork(s string, check func()) bool {
	for i := 0; i < len(s); i++ {
		if check != nil {
			check()
		}
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func trimValidationSpace(text string, check func()) string {
	if check == nil {
		return strings.TrimSpace(text)
	}
	for len(text) > 0 {
		check()
		character, size := utf8.DecodeRuneInString(text)
		if !unicode.IsSpace(character) {
			break
		}
		text = text[size:]
	}
	for len(text) > 0 {
		check()
		character, size := utf8.DecodeLastRuneInString(text)
		if !unicode.IsSpace(character) {
			break
		}
		text = text[:len(text)-size]
	}
	return text
}
