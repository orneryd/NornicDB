package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

type mapBraceKind uint8

const (
	literalBrace mapBraceKind = iota
	projectionBrace
	subqueryBrace
	quantifiedBrace
)

// mapBraceSyntax is the sole owner of literal-versus-projection grammar.
// A completed projection base precedes a projection brace; entries never
// influence this decision. Pattern elements own literal property maps.
func mapBraceSyntax(query string, brace int, previous subqueryToken, subquery bool, context **mapPatternContext) mapBraceKind {
	if subquery {
		return subqueryBrace
	}
	if previous.Is(query, ")") && previous.parenOpen >= 0 && parenthesisedPathAt(query, previous.parenOpen, previous.Start) {
		return quantifiedBrace
	}
	base := previous.Kind == cyphertext.QuotedName || previous.Kind == cyphertext.Word || previous.Is(query, ")") || previous.Is(query, "]") || previous.Is(query, "}")
	if !base {
		return literalBrace
	}
	if previous.Kind == cyphertext.Word {
		if _, leadsExpression := literalLeadingKeywords[upperASCII(previous.Text(query))]; leadsExpression && !wordIsName(query, previous.Start) {
			return literalBrace
		}
		if _, symbolic := isOneSymbolicName(previous.Text(query)); !symbolic {
			return literalBrace
		}
	}
	if *context == nil {
		*context = newMapPatternContext(query)
	}
	if (*context).insidePatternDelimiter(query, brace) {
		return literalBrace
	}
	return projectionBrace
}

// mapBraceAt asks the same syntax owner when a consumer starts at a source
// position rather than already holding a forward reader's token history.
func mapBraceAt(query string, brace int) (mapBraceKind, bool) {
	scanner := newSubqueryBraceScanner(query, 0)
	var context *mapPatternContext
	for span, subquery, ok := scanner.next(); ok; span, subquery, ok = scanner.next() {
		if span.Start < brace {
			continue
		}
		if span.Start != brace || !span.Is(query, "{") {
			return literalBrace, false
		}
		return mapBraceSyntax(query, brace, scanner.prior, subquery, &context), true
	}
	return literalBrace, false
}

type braceEntryKind uint8

const (
	keyedBraceEntry braceEntryKind = iota
	propertyBraceEntry
	wildcardBraceEntry
	variableBraceEntry
)

type braceEntry struct {
	kind       braceEntryKind
	key, value string
}

// parseBraceEntry owns the entry grammar for both syntax validation and value
// materialization. Projection selectors are allowed only by a projection owner.
func parseBraceEntry(part string, owner mapBraceKind) (braceEntry, bool) {
	if key, value, keyed := mapEntryTokens(part); keyed {
		if !validMapLiteralKey(key) || cyphertext.TrimTrivia(value) == "" {
			return braceEntry{}, false
		}
		return braceEntry{kind: keyedBraceEntry, key: key, value: value}, true
	}
	if owner != projectionBrace {
		return braceEntry{}, false
	}
	if name, wildcard, selector := mapProjectionSelector(part); selector {
		kind := propertyBraceEntry
		if wildcard {
			kind = wildcardBraceEntry
		}
		return braceEntry{kind: kind, key: name}, true
	}
	if name := singleMapName(part); name != "" {
		return braceEntry{kind: variableBraceEntry, key: name, value: name}, true
	}
	return braceEntry{}, false
}

// enclosingProjectionBrace serves quoted-variable classification without a
// second reverse-text heuristic for which brace owns the current entry.
func enclosingProjectionBrace(query string, at int) bool {
	reader := cyphertext.NewReader(query, 0)
	var stack []cyphertext.Token
	for span, ok := reader.Next(); ok && span.Start < at; span, ok = reader.Next() {
		if span.Kind != cyphertext.Symbol {
			continue
		}
		switch span.Text(query) {
		case "(", "[", "{":
			stack = append(stack, span)
		case ")", "]", "}":
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	if len(stack) == 0 || !stack[len(stack)-1].Is(query, "{") {
		return false
	}
	owner, ok := mapBraceAt(query, stack[len(stack)-1].Start)
	return ok && owner == projectionBrace
}

func completeProjection(expression string) (string, string, bool) {
	reader := cyphertext.NewReader(expression, 0)
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if span.Kind != cyphertext.Symbol {
			continue
		}
		if span.Is(expression, "(") || span.Is(expression, "[") {
			closer := ')'
			if span.Is(expression, "[") {
				closer = ']'
			}
			if close := findMatchingDelimiter(expression, span.Start, rune(expression[span.Start]), closer); close >= 0 {
				reader.Seek(close + 1)
				continue
			}
		}
		if !span.Is(expression, "{") {
			continue
		}
		close := findMatchingDelimiter(expression, span.Start, '{', '}')
		if close < 0 {
			return "", "", false
		}
		owner, owned := mapBraceAt(expression, span.Start)
		if owned && owner == projectionBrace && cyphertext.SkipTrivia(expression, close+1) == len(expression) {
			return strings.TrimSpace(expression[:span.Start]), cyphertext.TrimTrivia(expression[span.End:close]), true
		}
		reader.Seek(close + 1)
	}
	return "", "", false
}

// Token history replaces reverse byte scans: the closing scope token carries
// the opening parenthesis and whether CALL introduced it, regardless of trivia.
type subqueryToken struct {
	cyphertext.Token
	parenOpen            int
	callImport, mapColon bool
}
type subqueryParen struct {
	open int
	call bool
}
type subqueryBraceScanner struct {
	query           string
	reader          cyphertext.Reader
	last, prior     subqueryToken
	currentSubquery bool
	parens          []subqueryParen
	depth           int
	buffer          [16]subqueryParen
}

func newSubqueryBraceScanner(query string, start int) subqueryBraceScanner {
	scanner := subqueryBraceScanner{query: query, reader: cyphertext.NewReader(query, 0)}
	// A consumer can begin inside an expression container. Preserve the lexical
	// context before that boundary: a map-value colon and a label colon have
	// different grammar even when the consumer starts after the opening brace.
	for start > 0 {
		lookahead := scanner
		token, _, ok := lookahead.next()
		if !ok || token.Start >= start {
			break
		}
		scanner = lookahead
	}
	return scanner
}
func (scanner *subqueryBraceScanner) next() (cyphertext.Token, bool, bool) {
	token, ok := scanner.reader.Next()
	if !ok {
		return token, false, false
	}
	query := scanner.query
	info := subqueryToken{Token: token, parenOpen: -1}
	subquery := token.Is(query, "{") && scanner.braceAfterPrevious(token.Start)
	switch {
	case token.Is(query, "("):
		parent := subqueryParen{token.Start, scanner.keyword("CALL")}
		if scanner.depth < len(scanner.buffer) {
			scanner.buffer[scanner.depth] = parent
		} else {
			scanner.parens = append(scanner.parens, parent)
		}
		scanner.depth++
	case token.Is(query, ")"):
		if scanner.depth > 0 {
			scanner.depth--
			var parent subqueryParen
			if scanner.depth < len(scanner.buffer) {
				parent = scanner.buffer[scanner.depth]
			} else {
				parent = scanner.parens[len(scanner.parens)-1]
				scanner.parens = scanner.parens[:len(scanner.parens)-1]
			}
			info.parenOpen, info.callImport = parent.open, parent.call
		}
	case token.Is(query, ":"):
		info.mapColon = (scanner.prior.Is(query, "{") || scanner.prior.Is(query, ",")) && validMapLiteralKey(scanner.last.Text(query))
	}
	scanner.prior, scanner.last = scanner.last, info
	scanner.currentSubquery = subquery
	return token, subquery, true
}
func (scanner *subqueryBraceScanner) keyword(word string) bool {
	return scanner.last.Kind == cyphertext.Word && strings.EqualFold(scanner.last.Text(scanner.query), word) && !scanner.prior.Is(scanner.query, ".") && (!scanner.prior.Is(scanner.query, ":") || scanner.prior.mapColon)
}
func (scanner *subqueryBraceScanner) braceAfterPrevious(brace int) bool {
	if scanner.last.Is(scanner.query, ")") {
		return scanner.last.callImport
	}
	if !scanner.keyword("CALL") && !scanner.keyword("EXISTS") && !scanner.keyword("COUNT") && !scanner.keyword("COLLECT") {
		return false
	}
	return subqueryBraceBodyStartsLikeClause(scanner.query, brace)
}
func (scanner *subqueryBraceScanner) skipBody(close int) {
	scanner.reader.Seek(close + 1)
	scanner.prior, scanner.last = scanner.last, subqueryToken{Token: cyphertext.Token{Start: close, End: close + 1, Kind: cyphertext.Symbol}, parenOpen: -1}
	scanner.currentSubquery = false
}
func (scanner *subqueryBraceScanner) at(brace int) bool {
	if brace < 0 || brace >= len(scanner.query) || scanner.query[brace] != '{' {
		return false
	}
	if scanner.last.End > brace {
		return scanner.last.Start == brace && scanner.currentSubquery
	}
	for token, subquery, ok := scanner.next(); ok; token, subquery, ok = scanner.next() {
		if token.Start >= brace {
			return token.Start == brace && subquery
		}
		if token.End > brace {
			return false
		}
	}
	return false
}
func precedingSubqueryExpressionKeyword(query string, brace int) bool {
	scanner := newSubqueryBraceScanner(query, 0)
	return scanner.at(brace)
}
func subqueryBraceBodyStartsLikeClause(query string, brace int) bool {
	reader := cyphertext.NewReader(query, brace+1)
	first, ok := reader.Next()
	if !ok {
		return false
	}
	if first.Is(query, "(") {
		return true
	}
	if first.Kind != cyphertext.Word {
		return false
	}
	next, _ := reader.Next()
	// A subquery's abbreviated pattern can bind a path before its first node.
	// Read the pattern prefix from the same tokens as the unbound alternative.
	if next.Is(query, "=") {
		if _, name := isOneSymbolicName(first.Text(query)); name {
			open, _ := reader.Next()
			return open.Is(query, "(")
		}
	}
	if next.Is(query, ":") || next.Is(query, ",") || next.Is(query, "}") {
		return false
	}
	for _, keyword := range queryStructureClauseStarts {
		if strings.EqualFold(first.Text(query), keyword) {
			return true
		}
	}
	return false
}
func colonIsMapKeySeparator(query string, colon int) bool {
	scanner := newSubqueryBraceScanner(query, 0)
	for token, _, ok := scanner.next(); ok; token, _, ok = scanner.next() {
		if token.Start >= colon {
			return token.Start == colon && token.Is(query, ":") && scanner.last.mapColon
		}
	}
	return false
}
