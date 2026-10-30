package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"
)

// Author: #1663 implementation lane. The final contextual role grammar consumes
// the original lexical record. No consumer runs it on rewritten text.
type BraceRole uint8

const (
	// LiteralBrace is a map literal: {k: v, …}.
	LiteralBrace BraceRole = iota
	// ProjectionBrace follows a variable: n{.a, b, c: v, .*}.
	ProjectionBrace
	// PatternPropertyBrace is a node's or relationship's property map in a
	// pattern: (n {k: v}), [r {k: v}].
	PatternPropertyBrace
	// SubqueryBrace opens a query body: CALL {…}, CALL (…) {…}, EXISTS {…},
	// COUNT {…}, COLLECT {…}, a braced query part ({…} UNION {…}, NEXT {…})
	// and a block nested directly in any of them ({ { … } }).
	SubqueryBrace
	// QuantifierBrace repeats a path or relationship: ((a)-->(b)){1,3},
	// (a)-[r]->{1,3}(b).
	QuantifierBrace
	// DeclarationBrace belongs to the declaration grammar. Its embedded
	// Cypher expressions have their own original frames in this same product.
	DeclarationBrace
)

// The syntax scanner and entry grammar are shared with Fabric. Pattern
// context remains here, where statement scopes and pattern delimiters live.
type originalBraceRoleCursor struct {
	Query                string
	source               *cyphertext.Source
	Last, Prior, Earlier cyphertext.BraceToken
	expression           bool
	entryFacts           []cyphertext.BraceEntry
}

// Original statements can have CYPHER/version/options and execution-mode
// prefixes. Read their source span without rewriting text or losing offsets.
// An expression variable named cypher/EXPLAIN keeps its projection reading.

// mapBraceAt asks the owner about one brace in its original statement context.

// expressionBraceAt retains the fact that a caller has already isolated an
// expression. Its leading symbolic name cannot be a statement's clause keyword
// or DISTINCT modifier. Query-body introducers still go through the same scanner.

// distinctBraceNamesVariable resolves DISTINCT's ambiguous brace continuation.
// Keyed-only entries can follow the modifier; selectors require a variable.

// kind decides what the brace the scanner has just read opens. Its parameters
// are only for the pattern decision, which needs the statement's clauses.
func (scanner *originalBraceRoleCursor) kind(patternContext **mapPatternContext) BraceRole {
	query, brace, previous := scanner.Query, scanner.Last, scanner.Prior
	if previous.Is(query, ")") && previous.ParenOpen >= 0 && braceGroupedPath(scanner.source, previous.ParenOpen, previous.Start) && scanner.quantifierBody(brace.Start) {
		return QuantifierBrace
	}
	// After a relationship: -[r]->{1,3}, -[r]-{2}, -->{1,}, --{,3}.
	before := scanner.Earlier
	afterRelationship := previous.Is(query, ">") && before.Is(query, "-") ||
		previous.Is(query, "-") && (before.Is(query, "-") || before.Is(query, "]"))
	if afterRelationship && scanner.quantifierBody(brace.Start) {
		return QuantifierBrace
	}
	if *patternContext == nil {
		panic("cypher: original brace owner requires its retained token context")
	}
	if (*patternContext).insidePatternDelimiter(query, brace.Start) {
		return PatternPropertyBrace
	}
	if brace.Body {
		return SubqueryBrace
	}
	if scanner.projectionBase() {
		return ProjectionBrace
	}
	return LiteralBrace
}

// braceGroupedPath reports whether query[open:close+1] is a parenthesised path
// pattern: a node pattern, then a relationship arrow (((a)-->(b)){1,3}).
func braceGroupedPath(source *cyphertext.Source, open, close int) bool {
	query := source.Text
	node := queryGapEnd(query, open+1)
	if node >= close || query[node] != '(' {
		return false
	}
	extent, found, err := source.DelimiterSpan(node)
	if err != nil {
		panic(err)
	}
	if !found || extent.End > close {
		return false
	}
	arrow := queryGapEnd(query, extent.End)
	if arrow >= close {
		return false
	}
	next := queryGapEnd(query, arrow+1)
	if next >= close {
		return false
	}
	return query[arrow] == '-' && (query[next] == '-' || query[next] == '[' || query[next] == '>') || query[arrow] == '<' && query[next] == '-'
}

// braceQuantifierBody reports whether the brace at open holds a quantifier's
// bounds: {n}, {m,n}, {m,} or {,n}.
func (scanner *originalBraceRoleCursor) quantifierBody(open int) bool {
	span, found, err := scanner.source.DelimiterSpan(open)
	if err != nil {
		panic(err)
	}
	if !found {
		return false
	}
	_, valid := readBraceQuantifierBounds(scanner.Query, open, span.End-1)
	return valid
}

// projectionBase reports whether the token before the brace is a variable a
// map projection can follow. A keyword an expression follows (RETURN, IN,
// THEN, …) precedes a map literal instead, unless the keyword's own
// predecessor shows that it may be a variable's name (RETURN limit {.a},
// RETURN distinct {.a}): an ambiguous position keeps the reading that
// accepts the statement, as the parser decides the rest.
func (scanner *originalBraceRoleCursor) projectionBase() bool {
	query, previous := scanner.Query, scanner.Prior
	if previous.Kind == cyphertext.QuotedName {
		return true
	}
	if previous.Kind != cyphertext.Word {
		return false
	}
	word := previous.Text(query)
	if _, name := braceSymbolicName(word); !name {
		return false
	}
	upper := UpperASCII(word)
	before := scanner.Earlier
	if scanner.expression && before.Kind == 0 {
		return true
	}
	if previous.Keyword(query, "DISTINCT") &&
		(before.Keyword(query, "RETURN") || before.Keyword(query, "WITH")) {
		for _, entry := range scanner.entryFacts {
			if entry.Kind == cyphertext.PropertyBraceEntry || entry.Kind == cyphertext.WildcardBraceEntry || entry.Kind == cyphertext.VariableBraceEntry {
				return true
			}
		}
		return false
	}
	if _, keyword := LiteralLeadingKeywords[upper]; !keyword || !previous.Keyword(query, upper) {
		return true
	}
	switch {
	case before.Kind == cyphertext.Word:
		_, operandFollows := LiteralLeadingKeywords[UpperASCII(before.Text(query))]
		return operandFollows && before.Keyword(query, UpperASCII(before.Text(query)))
	case before.Kind == cyphertext.Symbol:
		return strings.IndexByte("(,[=+-*/%^<>|:", query[before.Start]) >= 0
	}
	return false
}

type braceBounds struct{ min, max, end int }

func readBraceQuantifierBounds(query string, at, close int) (braceBounds, bool) {
	bound := func(text string, missing int) (int, bool) {
		if text = strings.TrimSpace(text); text == "" {
			return missing, true
		}
		for _, digit := range text {
			if digit < '0' || digit > '9' {
				return 0, false
			}
		}
		value, err := strconv.Atoi(text)
		return value, err == nil && value >= 0
	}
	inner := braceStripComments(query[at+1 : close])
	lower, upper, ranged := strings.Cut(inner, ",")
	if !ranged {
		n, ok := bound(lower, -1)
		if !ok || n < 0 {
			return braceBounds{}, false
		}
		return braceBounds{min: n, max: n, end: close + 1}, true
	}
	min, minOK := bound(lower, 0)
	max, maxOK := bound(upper, -1)
	if !minOK || !maxOK {
		return braceBounds{}, false
	}
	return braceBounds{min: min, max: max, end: close + 1}, true
}

// Author: #1663 implementation lane. Shared bounds grammar after a retained paired extent.
func BraceQuantifierBounds(text string, open, close int) (int, int, bool) {
	value, ok := readBraceQuantifierBounds(text, open, close)
	return value.min, value.max, ok
}
func BraceGroupedPath(source *cyphertext.Source, open, close int) bool {
	return braceGroupedPath(source, open, close)
}
