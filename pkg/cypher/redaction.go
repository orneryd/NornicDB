// Package cypher: D-04 query literal redactor for slow-query log emission (LOG-08).
//
// RedactLiterals walks the Cypher token stream, replacing STRING_LITERAL,
// CHAR_LITERAL (NornicDB's grammar accepts single-quoted strings as an
// alternate spelling of STRING_LITERAL — see CHAR_LITERAL in
// CypherLexer.g4), INTEGER, and FLOAT tokens with the constant
// RedactedPlaceholder. Identifiers, keywords, and parameter REFERENCES
// ($name) are preserved verbatim because parameter VALUES bind separately at
// execution time and never appear as inline literals in the query text.
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
// RedactedPlaceholder. STRING_LITERAL, CHAR_LITERAL, INTEGER, and FLOAT
// tokens are always replaced. Statements the grammar rejects also have
// their identifiers replaced (see below). Empty queries and malformed or
// truncated input return RedactedPlaceholder (fail-closed).
//
// NornicDB issue #563 defect 3: this used to return RedactedPlaceholder for
// any statement that failed a full cantlr.Parse. That parser rule does not
// cover every shape NornicDB's own executor accepts: CALL { ... UNION ... }
// subqueries, deep OR-chained CALL{UNION} branch fan-outs, and any
// statement ending in `LIMIT $limit` (a parameter literally named "limit",
// which the grammar's LIMIT clause rejects as a keyword collision; this is
// what collapsed the multi-MATCH variable-length (*m..n) traversal shape
// from the #563 scan, the *m..n syntax itself parses fine). Those
// statements execute successfully but were logged as the bare placeholder
// with no structure at all.
//
// The redactor now runs in two steps.
//
// Step 1 fails the whole statement closed (returns RedactedPlaceholder) on
// any of these lexer-level signals:
//  1. the lexer's own error listener fires;
//  2. the lexer emits an ERRCHAR token, ANTLR's catch-all for input it
//     cannot tokenize, e.g. a stray control character or the opening quote
//     of an unterminated string such as `MATCH (n {name: "ali`;
//  3. brackets do not nest: every `(`, `{`, and `[` must be closed by its
//     own type, in order. A closer with no open bracket (`) MATCH (n) (`), a
//     closer of the wrong type (`{pw: Secret) RETURN n}`), or an opener still
//     open at EOF (`MATCH ((((`) all fail closed. Brackets inside a string
//     literal or comment are part of that single token and never count;
//  4. two ID tokens appear back to back on the default channel, e.g. the
//     unquoted value in `{name: John Secret}`.
//
// Signal 4 also fires on a few valid statements. Keywords the lexer
// tokenizes never lex as ID, but some Cypher keywords (NULLS, FIRST, LAST,
// USE, LOAD, CSV, HEADERS, FROM, ERROR, CONTINUE, FINISH) are not lexer
// tokens, so statements such as `ORDER BY n.x DESC NULLS LAST` are redacted
// whole. That is the safe direction.
//
// Step 2 checks the statement against the grammar with cantlr.Validate.
// When the grammar accepts it, the output keeps every identifier and
// replaces only literals. When the grammar rejects it, the output keeps the
// statement's shape but replaces every identifier as well as every literal,
// because a bare word in text the grammar cannot place may be an unquoted
// value. In that mode only these stay: keywords, punctuation, whitespace,
// parameter names after `$`, and label or relationship-type names after `:`
// directly inside a `(` or `[`. Comments and hex/octal numbers are replaced
// too. The executor accepts some statements the grammar rejects (CALL {
// ... UNION ... } subqueries, `LIMIT $limit`, map projections, COUNT { }),
// so those keep their clause structure, labels, and parameter names instead
// of collapsing to the bare placeholder.
//
// Principle: when in doubt, redact. A false-closed redaction only costs log
// readability; a false-open one leaks the exact PII/secret content this
// redactor exists to protect.
//
// Performance: this is NOT on the production hot path — it fires only on the
// slow-query log emission path (cypher.duration_ms >= SlowQueryThreshold),
// so per-call cost (~1 lexer pass) is acceptable.
func RedactLiterals(query string) string {
	if strings.TrimSpace(query) == "" {
		return RedactedPlaceholder
	}
	toks, ok := redactLexChecked(query)
	if !ok {
		return RedactedPlaceholder
	}
	grammarValid := cantlr.Validate(query) == nil

	var b strings.Builder
	b.Grow(len(query))
	// stack tracks open bracket types so a label after `:` is kept only
	// inside a node or relationship pattern, never inside a map.
	var stack []int
	prev := noPrevToken
	for _, tok := range toks {
		ttype := tok.GetTokenType()
		text := tok.GetText()
		switch ttype {
		case cantlr.CypherLexerLPAREN, cantlr.CypherLexerLBRACE, cantlr.CypherLexerLBRACK:
			stack = append(stack, ttype)
		case cantlr.CypherLexerRPAREN, cantlr.CypherLexerRBRACE, cantlr.CypherLexerRBRACK:
			stack = stack[:len(stack)-1]
		}
		switch {
		case ttype == cantlr.CypherLexerSTRING_LITERAL,
			ttype == cantlr.CypherLexerCHAR_LITERAL,
			ttype == cantlr.CypherLexerINTEGER,
			ttype == cantlr.CypherLexerFLOAT:
			b.WriteString(RedactedPlaceholder)
		case !grammarValid && redactStructuralOnly(ttype, prev, stack):
			b.WriteString(RedactedPlaceholder)
		default:
			b.WriteString(text)
		}
		if tok.GetChannel() == antlr4.TokenDefaultChannel {
			prev = ttype
		}
	}
	out := b.String()
	if out == "" {
		return RedactedPlaceholder
	}
	return out
}

// noPrevToken is outside the valid ANTLR token-type range (all generated
// token types are >= 1), so it never collides with a real token type.
const noPrevToken = -1

// redactLexChecked lexes query and applies the step-1 fail-closed signals
// documented on RedactLiterals. It returns ok=false when the statement must
// be redacted whole.
func redactLexChecked(query string) ([]antlr4.Token, bool) {
	lexer := cantlr.NewCypherLexer(antlr4.NewInputStream(query))
	listener := &redactSilentErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)

	var toks []antlr4.Token
	var stack []int
	prev := noPrevToken
	for {
		tok := lexer.NextToken()
		if tok == nil {
			break
		}
		ttype := tok.GetTokenType()
		if ttype == antlr4.TokenEOF {
			break
		}
		if listener.hadError || ttype == cantlr.CypherLexerERRCHAR {
			return nil, false
		}
		switch ttype {
		case cantlr.CypherLexerLPAREN, cantlr.CypherLexerLBRACE, cantlr.CypherLexerLBRACK:
			stack = append(stack, ttype)
		case cantlr.CypherLexerRPAREN, cantlr.CypherLexerRBRACE, cantlr.CypherLexerRBRACK:
			if len(stack) == 0 || stack[len(stack)-1] != redactOpenerFor(ttype) {
				return nil, false
			}
			stack = stack[:len(stack)-1]
		}
		if tok.GetChannel() == antlr4.TokenDefaultChannel {
			if ttype == cantlr.CypherLexerID && prev == cantlr.CypherLexerID {
				return nil, false
			}
			prev = ttype
		}
		toks = append(toks, tok)
	}
	if listener.hadError || len(stack) != 0 {
		return nil, false
	}
	return toks, true
}

// redactOpenerFor returns the opening bracket token type that closer must
// match.
func redactOpenerFor(closer int) int {
	switch closer {
	case cantlr.CypherLexerRPAREN:
		return cantlr.CypherLexerLPAREN
	case cantlr.CypherLexerRBRACE:
		return cantlr.CypherLexerLBRACE
	default:
		return cantlr.CypherLexerLBRACK
	}
}

// redactStructuralOnly reports whether a token must be replaced when the
// grammar rejected the statement. prev is the previous default-channel token
// type and stack holds the bracket types open at this token.
func redactStructuralOnly(ttype, prev int, stack []int) bool {
	switch ttype {
	case cantlr.CypherLexerID, cantlr.CypherLexerESC_LITERAL:
		if prev == cantlr.CypherLexerDOLLAR {
			return false
		}
		if prev == cantlr.CypherLexerCOLON && len(stack) > 0 {
			top := stack[len(stack)-1]
			return top != cantlr.CypherLexerLPAREN && top != cantlr.CypherLexerLBRACK
		}
		return true
	case cantlr.CypherLexerDIGIT, cantlr.CypherLexerCOMMENT, cantlr.CypherLexerLINE_COMMENT:
		return true
	}
	return false
}
