// Package cypher: D-04 query literal redactor for slow-query log emission (LOG-08).
//
// RedactLiterals walks the Cypher token stream, replacing STRING_LITERAL,
// CHAR_LITERAL (NornicDB's grammar accepts single-quoted strings as an
// alternate spelling of STRING_LITERAL — see CHAR_LITERAL in
// CypherLexer.g4), INTEGER, FLOAT, and DIGIT (hex, octal) tokens with the
// constant RedactedPlaceholder. Identifiers, keywords, and parameter REFERENCES
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
	"context"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// RedactedPlaceholder is the sentinel substituted for every redacted literal.
// Stable string contract: operators searching slow-query logs grep for it.
const RedactedPlaceholder = "<REDACTED>"

// RedactLiterals returns the input query with literal tokens replaced by
// RedactedPlaceholder. STRING_LITERAL, CHAR_LITERAL, INTEGER, and FLOAT
// tokens, and DIGIT tokens (hex and octal integers such as 0xBEEF), are
// always replaced. Statements the grammar rejects also have
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
// USE, LOAD, CSV, HEADERS, FROM, ERROR, CONTINUE, FINISH, ZONED, LOCAL,
// DATETIME, TRAILING) are not lexer tokens, so statements such as
// `ORDER BY n.x DESC NULLS LAST` and `CREATE CONSTRAINT ... IS :: ZONED
// DATETIME` are redacted whole. That is the safe direction.
//
// Step 2 checks the statement against the grammar with cantlr.Validate,
// but only when it is at most redactGrammarCheckMaxBytes long; a longer
// statement is treated as one the grammar rejected, without parsing it.
// When the grammar accepts it, the output keeps every identifier and
// replaces only literals. When the grammar rejects it, the output keeps the
// statement's shape but replaces every identifier as well as every literal,
// because a bare word in text the grammar cannot place may be an unquoted
// value. In that mode only these stay: keywords, punctuation, whitespace,
// parameter names after `$`, and label or relationship-type names after `:`
// directly inside a `(` or `[`. Comments are replaced
// too. The executor accepts some statements the grammar rejects (CALL {
// ... UNION ... } subqueries, `LIMIT $limit`, map projections, COUNT { }),
// so those keep their clause structure, labels, and parameter names instead
// of collapsing to the bare placeholder.
//
// Principle: when in doubt, redact. A false-closed redaction only costs log
// readability; a false-open one leaks the exact PII/secret content this
// redactor exists to protect.
//
// Performance: this fires on the slow-query log path, i.e. exactly when the
// server is already under load, so its cost is bounded: one lexer pass over
// the statement, plus a grammar parse only for statements of at most
// redactGrammarCheckMaxBytes. A full parse of a large statement can allocate
// hundreds of megabytes of prediction state, and the log keeps only its
// first 500 characters. The executor's log sites go through
// (*StorageExecutor).redactForLog, which also reuses the executor's cached
// grammar verdict for the statement text, so the output is the same on both
// parsers.
func RedactLiterals(query string) string {
	return redactLiterals(query, redactGrammarAccepts)
}

// redactGrammarCheckMaxBytes is the largest statement RedactLiterals checks
// against the grammar. Larger statements keep only their structure (see
// RedactLiterals): the safe direction, since it redacts more.
const redactGrammarCheckMaxBytes = 8 << 10

// redactGrammarValidate is cantlr.ValidateContext; tests replace it to count the
// grammar parses the redactor runs.
var redactGrammarValidate = func(ctx context.Context, query string) error {
	return cantlr.ValidateContext(ctx, query, cyphertext.AcceptedSource)
}

// redactGrammarAccepts is the uncached grammar verdict RedactLiterals uses.
func redactGrammarAccepts(query string) bool {
	return len(query) <= redactGrammarCheckMaxBytes && redactGrammarValidate(context.Background(), query) == nil
}

// redactForLog is RedactLiterals for the executor's log sites. A statement
// the executor's own ANTLR validation (or an earlier redaction) already
// found grammar-valid is not parsed again; the verdict is the same either
// way, so the output does not depend on the parser or the cache.
func (e *StorageExecutor) redactForLog(ctx context.Context, query string) string {
	if ctx.Err() != nil {
		return RedactedPlaceholder
	}
	redacted := redactLiteralsContext(ctx, query, true, func(query string) bool {
		return e.redactGrammarAccepts(ctx, query)
	})
	if ctx.Err() != nil {
		return RedactedPlaceholder
	}
	return redacted
}

func (e *StorageExecutor) redactGrammarAccepts(ctx context.Context, query string) bool {
	if ctx.Err() != nil || len(query) > redactGrammarCheckMaxBytes {
		return false
	}
	if e.hasCachedValidSyntax(query, syntaxGrammarAccepted) {
		return true
	}
	if redactGrammarValidate(ctx, query) != nil {
		return false
	}
	e.markCachedValidSyntax(query, syntaxGrammarAccepted)
	return true
}

// redactLiterals is RedactLiterals with the grammar verdict supplied by the
// caller; grammarAccepts runs only after the lexer checks passed.
func redactLiterals(query string, grammarAccepts func(string) bool) string {
	return redactLiteralsContext(context.Background(), query, false, grammarAccepts)
}

func redactLiteralsContext(ctx context.Context, query string, stripComments bool, grammarAccepts func(string) bool) string {
	source := cyphertext.SourceFor(ctx, query)
	query = source.Text
	trimmed, err := cantlr.TrimSpaceContext(ctx, query)
	if err != nil || trimmed == "" {
		return RedactedPlaceholder
	}
	checked, ok := redactLexCheckedContext(ctx, query)
	if !ok {
		return RedactedPlaceholder
	}
	toks := checked.tokens
	// Strip comments from the same checked token stream; do not restart an
	// unbounded comment/quoted-text scan before observing cancellation.
	if stripComments && checked.hasComments {
		var normalized strings.Builder
		for i, tok := range toks {
			if i&1023 == 0 && ctx.Err() != nil {
				return RedactedPlaceholder
			}
			text := tok.GetText()
			if tok.GetTokenType() == cantlr.CypherLexerCOMMENT || tok.GetTokenType() == cantlr.CypherLexerLINE_COMMENT {
				var ok bool
				text, ok = cyphertext.CommentReplacement(text, func() bool { return ctx.Err() != nil })
				if !ok {
					return RedactedPlaceholder
				}
			}
			normalized.WriteString(text)
		}
		query = normalized.String()
	}
	grammarValid := grammarAccepts(query)
	// Grammar acceptance does not make every identifier public: a remote
	// credential may be an unquoted keyword or a quoted identifier. Consume
	// the retained grammar's credential roles before preserving identifiers.
	// A property named password remains ordinary query structure.
	if source.PasswordBoundary() >= 0 {
		if !grammarValid {
			return RedactedPlaceholder
		}
		syntax, err := cantlr.ParseSyntax(ctx, query, cantlr.StatementEntry, false, cyphertext.AcceptedSource)
		if err != nil {
			return RedactedPlaceholder
		}
		credential := false
		syntax.Walk(func(node cantlr.SyntaxNode) func() {
			if node.Rule() == cantlr.CypherParserRULE_credentialValue {
				credential = true
			}
			return nil
		})
		if credential {
			return RedactedPlaceholder
		}
	}

	var b strings.Builder
	b.Grow(len(query))
	// stack tracks open bracket types so a label after `:` is kept only
	// inside a node or relationship pattern, never inside a map.
	var stack []int
	prev := noPrevToken
	acceptedCursor, originalCursor := 0, 0
	for i, tok := range toks {
		if i&1023 == 0 && ctx.Err() != nil {
			return RedactedPlaceholder
		}
		ttype := tok.GetTokenType()
		text := tok.GetText()
		acceptedCursor += len(text)
		originalEnd, _ := source.OriginalOffset(acceptedCursor, false, cyphertext.SourceExtent)
		originalText := source.Original[originalCursor:originalEnd]
		originalCursor = originalEnd
		if stripComments && (ttype == cantlr.CypherLexerCOMMENT || ttype == cantlr.CypherLexerLINE_COMMENT) {
			var ok bool
			text, ok = cyphertext.CommentReplacement(text, func() bool { return ctx.Err() != nil })
			if !ok {
				return RedactedPlaceholder
			}
			b.WriteString(text)
			continue
		}
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
			ttype == cantlr.CypherLexerFLOAT,
			ttype == cantlr.CypherLexerDIGIT:
			b.WriteString(RedactedPlaceholder)
		case !grammarValid && redactStructuralOnly(ttype, prev, stack):
			b.WriteString(RedactedPlaceholder)
		default:
			b.WriteString(originalText)
		}
		if tok.GetChannel() == antlr4.TokenDefaultChannel {
			prev = ttype
		}
	}
	b.WriteString(source.Original[originalCursor:])
	out := b.String()
	if out == "" {
		return RedactedPlaceholder
	}
	return out
}

// noPrevToken is outside the valid ANTLR token-type range (all generated
// token types are >= 1), so it never collides with a real token type.
const noPrevToken = -1

type checkedRedactionTokens struct {
	tokens      []antlr4.Token
	hasComments bool
}

// redactLexCheckedContext applies the step-1 fail-closed signals and records
// comments in that same pass, avoiding a separate scan of the checked tokens.
func redactLexCheckedContext(ctx context.Context, query string) (checkedRedactionTokens, bool) {
	toks, err := cantlr.LexContext(ctx, query, cyphertext.AcceptedSource)
	if err != nil {
		return checkedRedactionTokens{}, false
	}
	checked := checkedRedactionTokens{tokens: toks}
	var stack []int
	prev := noPrevToken
	for i, tok := range toks {
		if i&1023 == 0 && ctx.Err() != nil {
			return checkedRedactionTokens{}, false
		}
		ttype := tok.GetTokenType()
		if ttype == cantlr.CypherLexerERRCHAR {
			return checkedRedactionTokens{}, false
		}
		if ttype == cantlr.CypherLexerCOMMENT || ttype == cantlr.CypherLexerLINE_COMMENT {
			checked.hasComments = true
		}
		switch ttype {
		case cantlr.CypherLexerLPAREN, cantlr.CypherLexerLBRACE, cantlr.CypherLexerLBRACK:
			stack = append(stack, ttype)
		case cantlr.CypherLexerRPAREN, cantlr.CypherLexerRBRACE, cantlr.CypherLexerRBRACK:
			if len(stack) == 0 || stack[len(stack)-1] != redactOpenerFor(ttype) {
				return checkedRedactionTokens{}, false
			}
			stack = stack[:len(stack)-1]
		}
		if tok.GetChannel() == antlr4.TokenDefaultChannel {
			if ttype == cantlr.CypherLexerID && prev == cantlr.CypherLexerID {
				return checkedRedactionTokens{}, false
			}
			prev = ttype
		}
	}
	if len(stack) != 0 || ctx.Err() != nil {
		return checkedRedactionTokens{}, false
	}
	return checked, true
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
	case cantlr.CypherLexerCOMMENT, cantlr.CypherLexerLINE_COMMENT:
		return true
	}
	return false
}
