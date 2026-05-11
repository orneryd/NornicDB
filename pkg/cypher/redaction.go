// Package cypher: D-04 query literal redactor for slow-query log emission (LOG-08).
//
// RedactLiterals walks the Cypher token stream, replacing STRING_LITERAL,
// INTEGER, and FLOAT tokens with the constant RedactedPlaceholder. Identifiers,
// keywords, and parameter REFERENCES ($name) are preserved verbatim because
// parameter VALUES bind separately at execution time and never appear as
// inline literals in the query text.
//
// The redactor is invoked at every log emission site that includes raw query
// text — currently the slow-query log (D-04c) — so PII stored in literal
// values (names, emails, passwords) cannot leak through unauthenticated
// /metrics or operator log surfaces. Phase 6 (TRC-04) calls this same helper
// before attaching query text to the nornicdb.cypher.plan span.
//
// On parse/lex failure the redactor returns RedactedPlaceholder (fail-closed
// per RESEARCH Pattern 5 line 660) — better to lose query readability than
// leak partial literal content from a half-tokenized input.
package cypher

import (
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	cantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// RedactedPlaceholder is the sentinel substituted for every redacted literal.
// Stable string contract: operators searching slow-query logs grep for it.
const RedactedPlaceholder = "<REDACTED>"

// redactSentinelError is consumed by the silent error listener so a syntax
// error short-circuits to fail-closed without printing to stderr.
type redactSilentErrorListener struct {
	*antlr4.DefaultErrorListener
	hadError bool
}

func (l *redactSilentErrorListener) SyntaxError(_ antlr4.Recognizer, _ interface{}, _, _ int, _ string, _ antlr4.RecognitionException) {
	l.hadError = true
}

// RedactLiterals returns the input query with literal tokens replaced by
// RedactedPlaceholder. STRING_LITERAL, INTEGER, and FLOAT token types are
// the redaction target set; all other tokens are emitted as their original
// text. Empty queries and parse failures return RedactedPlaceholder
// (fail-closed).
//
// Performance: this is NOT on the production hot path — it fires only on the
// slow-query log emission path (cypher.duration_ms >= SlowQueryThreshold),
// so per-call cost (~1 lexer pass) is acceptable.
func RedactLiterals(query string) string {
	if strings.TrimSpace(query) == "" {
		return RedactedPlaceholder
	}

	// Step 1: parser-level syntax check. Fail-closed on syntax errors —
	// better to lose query readability than to leak partial literal content
	// through a half-tokenized input that the lexer happily accepted.
	if _, err := cantlr.Parse(query); err != nil {
		return RedactedPlaceholder
	}

	// Step 2: token-stream walk. The grammar parsed cleanly, so any literal
	// tokens we encounter here are well-formed and safe to substitute. We
	// run a fresh lexer pass (not reusing the parser's token stream) so the
	// hot-path Parse cache stays untouched.
	input := antlr4.NewInputStream(query)
	lexer := cantlr.NewCypherLexer(input)
	listener := &redactSilentErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)

	var b strings.Builder
	b.Grow(len(query))

	for {
		tok := lexer.NextToken()
		if tok == nil {
			break
		}
		ttype := tok.GetTokenType()
		if ttype == antlr4.TokenEOF {
			break
		}
		if listener.hadError {
			return RedactedPlaceholder
		}
		switch ttype {
		case cantlr.CypherLexerSTRING_LITERAL,
			cantlr.CypherLexerINTEGER,
			cantlr.CypherLexerFLOAT:
			b.WriteString(RedactedPlaceholder)
		default:
			b.WriteString(tok.GetText())
		}
	}
	if listener.hadError {
		return RedactedPlaceholder
	}
	out := b.String()
	if out == "" {
		return RedactedPlaceholder
	}
	return out
}
