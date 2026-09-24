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
// NornicDB issue #563 defect 3: this used to gate on a full cantlr.Parse of
// the statement (the parser's Script rule) before doing the token-level
// redaction below. That parser rule does not cover every shape NornicDB's
// own executor accepts — CALL { ... UNION ... } subqueries, deep OR-chained
// CALL{UNION} branch fan-outs, and any statement ending in `LIMIT $limit`
// (a parameter literally named "limit", which the grammar's LIMIT clause
// rejects as a keyword collision — this is what actually collapsed the
// multi-MATCH variable-length (*m..n) traversal shape from the #563 scan;
// the *m..n traversal syntax itself parses fine) all round-trip through the
// executor successfully but failed cantlr.Parse's Script rule, so every one
// of these (valid, already-executed) queries collapsed to the bare
// placeholder with no structure at all. A full grammar parse is the wrong
// tool for "is this safe to redact": the redactor only needs to know
// whether the LEXER produced a genuinely broken or syntactically
// impossible token stream (one that could dribble out partial or
// unquoted literal content), not whether the statement satisfies the whole
// parser grammar.
//
// Fail-closed now rests on five lexer-level signals instead:
//  1. the lexer's own error listener fires (hadError);
//  2. the lexer emits an ERRCHAR token — ANTLR's catch-all for input it
//     cannot tokenize at all, e.g. a stray control character;
//  3. paren/brace/bracket depth does not return to zero by EOF — this is
//     what actually flags a truncated/incomplete statement such as
//     `MATCH ((((` or an unterminated string like `MATCH (n {name: "ali`
//     (the unterminated quote route the lexer into ERRCHAR + ID tokens
//     rather than a well-formed STRING_LITERAL, so bullet 2 also catches
//     it; the brace never closes either, so bullet 3 is defense in depth);
//  4. paren/brace/bracket depth goes negative at ANY point during the walk,
//     not just at EOF — a closer that appears before its opener (e.g.
//     `) MATCH (n {name:'secret'}) (`) can still end the statement at
//     depth 0 (it dips to -1 and climbs back), which bullet 3 alone would
//     miss and let the balanced-looking-but-invalid text leak verbatim;
//  5. two ID tokens appear back to back on the default channel with no
//     operator, punctuation, or keyword token between them (e.g. the
//     unquoted bare-word property value in `{name: John Secret}`) — every
//     keyword this grammar defines (MATCH, AS, IN, LIMIT, IS, NOT, ...) is
//     its own token type, never lexed as ID, so this sequence never occurs
//     in a statement the executor actually accepts; it only shows up when
//     an unquoted string was meant to be a quoted literal.
//
// All five are cheap. Bracket/brace/paren content inside a completed
// STRING_LITERAL or COMMENT token never perturbs the depth counter, and an
// ID inside such a token is never seen as a separate token by signal 5,
// because those are consumed as a single atomic token by the lexer, not as
// individual characters.
//
// Principle: when in doubt, redact the whole statement. A false-closed
// redaction only costs log readability; a false-open one leaks the exact
// PII/secret content this redactor exists to protect.
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
	// noPrevToken is outside the valid ANTLR token-type range (all generated
	// token types are >= 1), so it never collides with a real token type.
	const noPrevToken = -1
	prevDefaultChannelType := noPrevToken

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
		if tok.GetChannel() == antlr4.TokenDefaultChannel {
			// Signal 5: two bare identifiers with nothing between them (not
			// even a hidden-channel token resets this — WS/COMMENT tokens are
			// skipped by this channel check, so `John /*x*/ Secret` still
			// trips it) is not a token sequence the grammar's own parser
			// rules ever produce from valid input.
			if ttype == cantlr.CypherLexerID && prevDefaultChannelType == cantlr.CypherLexerID {
				return RedactedPlaceholder
			}
			prevDefaultChannelType = ttype
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
			if depth < 0 {
				// Signal 4: a closer before its opener. Depth can still
				// return to exactly 0 by EOF from here (dip to -1, climb
				// back), so the depth != 0 check below alone would miss
				// this. Fail closed the instant depth goes negative.
				return RedactedPlaceholder
			}
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
