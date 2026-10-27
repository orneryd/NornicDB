// Package antlr provides ANTLR-based Cypher parsing for NornicDB.
//
// This package uses the official ANTLR Cypher grammar to parse queries
// into an AST that can be consumed by the executor.
package antlr

import (
	"context"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	data, err := parseStatementSyntax(context.Background(), cyphertext.NewSource(context.Background(), query, cyphertext.RawSource), StatementEntry, func(tree antlr.Tree) { result.Tree = tree.(IScriptContext) })
	if data != nil {
		result.Errors = diagnosticFallbacks(data.diagnostics)
	}
	return result, err
}

// Validate consumes the retained canonical statement product.
//
// Context-free convenience API; execution and logging use ValidateContext.
// It is optimized for throughput and reduced allocations and does not return a parse tree.
func Validate(query string) error {
	return ValidateContext(context.Background(), query, cyphertext.RawSource)
}

// ValidateContext validates syntax while observing cancellation during lexer
// and parser lookahead, including cold prediction. Cancellation returns only
// after the parser has unwound and its prediction lease is released.
func ValidateContext(ctx context.Context, query string, stage cyphertext.SourceStage) error {
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

	syntax, err := ParseSyntax(ctx, query, StatementEntry, false, stage)
	if err != nil && syntax.Valid() && len(syntax.data.diagnostics) != 0 {
		message := localization.CypherANTLRValidateSyntaxError(strings.Join(diagnosticFallbacks(syntax.data.diagnostics), "; "), syntax.data.diagnostics...)
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}
	return err
}

// parseErrorListener collects syntax errors during parsing
type parseErrorListener struct {
	*antlr.DefaultErrorListener
	errors   []localization.Message
	useError *UseReferenceError
}

func (e *parseErrorListener) fallbacks() []string {
	return diagnosticFallbacks(e.errors)
}

func diagnosticFallbacks(diagnostics []localization.Message) []string {
	if len(diagnostics) == 0 {
		return nil
	}
	result := make([]string, len(diagnostics))
	for i, message := range diagnostics {
		result[i] = message.Fallback
	}
	return result
}

func (e *parseErrorListener) reset() {
	clear(e.errors)
	e.errors = e.errors[:0]
	e.useError = nil
}

func (e *parseErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, ex antlr.RecognitionException) {
	if parser, ok := recognizer.(antlr.Parser); ok {
		for context := parser.GetParserRuleContext(); context != nil; {
			if context.GetRuleIndex() == CypherParserRULE_useClause {
				if token, ok := offendingSymbol.(antlr.Token); ok && e.useError == nil {
					e.useError = &UseReferenceError{Offset: token.GetStart()}
					if token.GetTokenType() == CypherParserFLOAT && strings.HasPrefix(token.GetText(), ".") {
						e.useError.NamePart = true
						e.useError.Offset = token.GetStart() + 1
					}
					if token.GetTokenType() == CypherParserDOT {
						e.useError.NamePart = true
						e.useError.Offset = parser.GetTokenStream().LT(2).GetStart()
					}
					if token.GetTokenType() == CypherParserLPAREN {
						e.useError.Function = true
						e.useError.Offset = parser.GetTokenStream().GetTokenSource().GetInputStream().Size()
					}

					if previous := parser.GetTokenStream().LT(-1); previous != nil {
						e.useError.NamePart = e.useError.NamePart || previous.GetTokenType() == CypherParserDOT
					}
				}
				break
			}
			if context.GetRuleIndex() == CypherParserRULE_useGraphArguments {
				if token, ok := offendingSymbol.(antlr.Token); ok {
					e.useError = &UseReferenceError{Offset: token.GetStart(), Function: true}
				}
				break
			}
			parent, _ := context.GetParent().(antlr.ParserRuleContext)
			context = parent
		}
	}
	// Do not persist ANTLR's token/excerpt text: a malformed password can be
	// the offending token, including in lexer errors before a token exists.
	// Parse, Validate, and ParseResult share this structural diagnostic owner.
	e.errors = append(e.errors, localization.CypherANTLRUnexpectedInput(line, column))
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
