// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
//
// This package uses the official ANTLR Cypher grammar to parse queries
// into an AST that can be consumed by the executor.
package antlr

import (
	"context"
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ParseResult contains the parsed ANTLR tree and any errors
type ParseResult struct {
	Tree   IScriptContext
	Errors []string
}

// Parse parses a Cypher query string using ANTLR and returns the parse tree
func Parse(query string) (*ParseResult, error) {
	if strings.TrimSpace(query) == "" {
		message := localization.CypherANTLRParseEmptyQuery()
		return nil, localization.NewLocalizedError(string(message.ID), message, nil)
	}
	// Legacy AST consumers explicitly request their own mutable tree. They use
	// the same parser lifecycle, and never receive the retained immutable tree.
	result := &ParseResult{}
	data, err := parseStatementSyntax(context.Background(), query, StatementEntry, func(tree antlr.Tree) { result.Tree = tree.(IScriptContext) })
	if data != nil {
		result.Errors = append([]string(nil), data.diagnostics...)
	}
	return result, err
}

// Validate consumes the retained canonical statement product.
//
// Context-free convenience API; execution and logging use ValidateContext.
// It is optimized for throughput and reduced allocations and does not return a parse tree.
func Validate(query string) error {
	return ValidateContext(context.Background(), query)
}

// ValidateContext validates syntax while observing cancellation during lexer
// and parser lookahead, including cold prediction. Cancellation returns only
// after the parser has unwound and its prediction lease is released.
func ValidateContext(ctx context.Context, query string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	trimmed, err := TrimSpaceContext(ctx, query)
	if err != nil {
		return err
	}
	if trimmed == "" {
		message := localization.CypherANTLRValidateEmptyQuery()
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}
	syntax, err := ParseSyntax(ctx, query, StatementEntry, false)
	if err != nil && syntax.Valid() && len(syntax.data.diagnostics) != 0 {
		message := localization.CypherANTLRValidateSyntaxError(strings.Join(syntax.data.diagnostics, "; "))
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}
	return err
}

// parseErrorListener collects syntax errors during parsing
type parseErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (e *parseErrorListener) reset() {
	clear(e.errors)
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
