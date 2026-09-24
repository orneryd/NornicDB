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
// token types are the redaction target set; all other tokens are emitted as
// their original text. Empty queries and genuinely malformed/truncated
// input return RedactedPlaceholder (fail-closed).
//
// eshu-7014-cause-C defect 3: this used to gate on a full cantlr.Parse of the
// statement (the parser's Script rule) before doing the token-level redaction
// below. That parser rule does not cover every shape NornicDB's own executor
// accepts — CALL { ... UNION ... } subqueries, deep OR-chained CALL{UNION}
// branch fan-outs, and multi-MATCH variable-length (*m..n) traversals with
// only list/param filters all round-trip through the executor successfully
// but failed cantlr.Parse's Script rule, so every one of these (valid,
// already-executed) queries collapsed to the bare placeholder with no
// structure at all. A full grammar parse is the wrong tool for "is this
// safe to redact": the redactor only needs to know whether the LEXER
// produced a genuinely broken token stream (one that could dribble out
// partial literal content), not whether the statement satisfies the whole
// parser grammar.
//
// Fail-closed now rests on three lexer-level signals instead:
//  1. the lexer's own error listener fires (hadError);
//  2. the lexer emits an ERRCHAR token — ANTLR's catch-all for input it
//     cannot tokenize at all, e.g. a stray control character;
//  3. paren/brace/bracket depth does not return to zero by EOF — this is
//     what actually flags a truncated/incomplete statement such as
//     `MATCH ((((` or an unterminated string like `MATCH (n {name: "ali`
//     (the unterminated quote route the lexer into ERRCHAR + ID tokens
//     rather than a well-formed STRING_LITERAL, so bullet 2 also catches
//     it; the brace never closes either, so bullet 3 is defense in depth).
//
// All three are cheap, bracket/brace/paren content inside a completed
// STRING_LITERAL or COMMENT token never perturbs the depth counter because
// those are consumed as a single atomic token by the lexer, not as
// individual characters.
//
// Performance: this is NOT on the production hot path — it fires only on the
// slow-query log emission path (cypher.duration_ms >= SlowQueryThreshold),
// so per-call cost (~1 lexer pass) is acceptable.
func RedactLiterals(query string) string {
	if strings.TrimSpace(query) == "" {
		return RedactedPlaceholder
	}

	input := antlr4.NewInputStream(query)
	lexer := cantlr.NewCypherLexer(input)
	listener := &redactSilentErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)

	var b strings.Builder
	b.Grow(len(query))
	depth := 0

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
			return RedactedPlaceholder
		}
		switch ttype {
		case cantlr.CypherLexerSTRING_LITERAL,
			cantlr.CypherLexerCHAR_LITERAL,
			cantlr.CypherLexerINTEGER,
			cantlr.CypherLexerFLOAT:
			b.WriteString(RedactedPlaceholder)
		case cantlr.CypherLexerLPAREN, cantlr.CypherLexerLBRACE, cantlr.CypherLexerLBRACK:
			depth++
			b.WriteString(tok.GetText())
		case cantlr.CypherLexerRPAREN, cantlr.CypherLexerRBRACE, cantlr.CypherLexerRBRACK:
			depth--
			b.WriteString(tok.GetText())
		default:
			b.WriteString(tok.GetText())
		}
	}
	if listener.hadError || depth != 0 {
		return RedactedPlaceholder
	}
	out := b.String()
	if out == "" {
		return RedactedPlaceholder
	}
	return out
}
