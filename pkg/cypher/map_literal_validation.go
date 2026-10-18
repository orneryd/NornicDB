package cypher

import "strings"

// validateStaticMapKeys rejects malformed symbolic map keys before either
// parser frontend or executor can normalize them. Braces without a top-level
// key/value separator are ignored because they may delimit a subquery.
func validateStaticMapKeys(query string) error {
	upper := upperASCII(strings.TrimSpace(query))
	if strings.HasPrefix(upper, "CREATE ") && strings.Contains(upper, "INDEX") && findKeywordIndex(query, "OPTIONS") >= 0 {
		return nil
	}
	if strings.HasPrefix(upper, "CREATE CONSTRAINT") && findKeywordIndex(query, "REQUIRE") >= 0 {
		return nil
	}
	var patternContext *mapPatternContext
	// Carry the predecessor through this lexical pass. Re-reading from byte
	// zero for each brace makes a flat list of ordinary maps quadratic.
	lastLive := -1
	for index := 0; index < len(query); index++ {
		if isCypherQuoteByte(query[index]) {
			index = skipCypherQuotedText(query, index, query[index]) - 1
			lastLive = index
			continue
		}
		if query[index] == '/' {
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end - 1
				continue
			}
		}
		if size := queryWhitespaceAt(query, index); size > 0 {
			index += size - 1
			continue
		}
		previous := lastLive
		lastLive = index
		if query[index] != '{' {
			continue
		}
		if subqueryExpressionAfterLiveByte(query, index, previous) {
			// Validate in the nearest statement scope: an outer RETURN must
			// not turn a nested MATCH property map into a map projection.
			if close := findMatchingDelimiter(query, index, '{', '}'); close >= 0 {
				if err := validateStaticMapKeys(query[index+1 : close]); err != nil {
					return err
				}
				index = close
				lastLive = close
			}
			continue
		}
		// A {m,n} after a parenthesised path quantifies it
		// ((a)-[r]-(b)){1,6}; it is not a map.
		if quantifiedGroupQuantifierAt(query, index) {
			continue
		}
		close := findMatchingDelimiter(query, index, '{', '}')
		if close < 0 {
			continue
		}
		parts := splitTopLevelComma(query[index+1 : close])
		mapLiteral := false
		for _, part := range parts {
			if findTopLevelMapKeyValueSeparator(part) >= 0 {
				mapLiteral = true
				break
			}
		}
		if !mapLiteral {
			if strings.TrimSpace(query[index+1:close]) == "" {
				continue
			}
			if patternContext == nil {
				patternContext = newMapPatternContext(query)
			}
			if patternContext.insidePatternDelimiter(query, index) && !inProjectionClause(query, index) {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"UnexpectedSyntax",
					"pattern property maps require key-value entries",
				)
			}
			continue
		}
		for _, part := range parts {
			trimmedPart := strings.TrimSpace(part)
			if _, selector := mapProjectionPropertySelector(trimmedPart); trimmedPart == ".*" || (strings.HasPrefix(trimmedPart, ".") && selector) {
				continue
			}
			separator := findTopLevelMapKeyValueSeparator(part)
			if separator <= 0 || !validMapLiteralKey(strings.TrimSpace(part[:separator])) {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"UnexpectedSyntax",
					"map literal keys must be valid symbolic names",
				)
			}
		}
	}
	return nil
}

// precedingSubqueryExpressionKeyword reports whether the '{' at braceIndex
// opens a brace-bodied subquery expression (EXISTS {, COUNT {, COLLECT {, or
// CALL {) rather than a node/relationship pattern property map. It finds the lexical
// predecessor outside whitespace/comments and matches its identifier
// case-insensitively against the subquery-introducing keywords the grammar
// allows before a bare '{'.
//
// A plain word match is not enough: "count", "exists", "collect", and
// "call" are also legal node-pattern variable names, label/type names, and
// map keys, e.g. `MATCH (count {bad})`, `MATCH (n:Count {bad})`, or
// `{call: 1}`. Two further checks narrow the match to genuine
// subquery-expression position:
//
//  1. The keyword must not itself be a quoted or typed identifier: a word
//     immediately preceded (after whitespace) by ':' is a label/type name,
//     and one preceded by '`' is a backtick-quoted identifier. Both are
//     rejected outright regardless of what follows the brace.
//  2. The brace body must start like a subquery clause -- MATCH, OPTIONAL,
//     CALL, WITH, UNWIND, RETURN, or a bare pattern element '(' -- which a
//     genuine pattern property map's key/value body never does. This is
//     what distinguishes `(EXISTS { MATCH ... })` (a grouped subquery
//     expression, keyword also preceded by '(') from `(count {bad})` (a
//     node pattern, invalid property map).
func precedingSubqueryExpressionKeyword(query string, braceIndex int) bool {
	scanner := newSubqueryBraceScanner(query, 0)
	return scanner.at(braceIndex)
}

// subqueryExpressionAfterLiveByte shares the same classification with callers
// that already know the last live byte from a forward lexical pass.
func subqueryExpressionAfterLiveByte(query string, braceIndex, index int) bool {
	// CALL (a, b) { … }: step back over the import list to the CALL.
	if index >= 0 && query[index] == ')' {
		depth := 0
		for ; index >= 0; index-- {
			if query[index] == ')' {
				depth++
			} else if query[index] == '(' {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		if index < 0 {
			return false
		}
		index--
		for index >= 0 && isCypherWhitespace(query[index]) {
			index--
		}
		wordEnd := index + 1
		for index >= 0 && isIdentByte(query[index]) {
			index--
		}
		return strings.EqualFold(query[index+1:wordEnd], "CALL")
	}
	end := index + 1
	for index >= 0 && isIdentByte(query[index]) {
		index--
	}
	start := index + 1
	if start >= end {
		return false
	}
	return subqueryExpressionKeywordBeforeBrace(query, start, end, braceIndex)
}

// subqueryExpressionKeywordBeforeBrace validates a known keyword token and its
// brace without rescanning its prefix when the forward reader already found it.
func subqueryExpressionKeywordBeforeBrace(query string, start, end, braceIndex int) bool {
	switch upperASCII(query[start:end]) {
	case "EXISTS", "COUNT", "COLLECT", "CALL":
	default:
		return false
	}
	precedingToken := start - 1
	for precedingToken >= 0 && isCypherWhitespace(query[precedingToken]) {
		precedingToken--
	}
	if precedingToken >= 0 {
		if isCypherBacktickByte(query[precedingToken]) {
			// Backtick-quoted identifier (`count` {bad}), never the keyword.
			return false
		}
		switch query[precedingToken] {
		case ':':
			// Label/type name (`:Count {bad}`), unless the colon separates
			// a map key from its value ({k: COUNT { … }}).
			if !colonIsMapKeySeparator(query, precedingToken) {
				return false
			}
		}
	}
	return subqueryBraceBodyStartsLikeClause(query, braceIndex)
}

// subqueryBraceBodyStartsLikeClause distinguishes a subquery's opening clause
// from an identically named map-projection selector. A selector ends at a comma
// or closing brace; a clause requires a following body. Standalone ordering and
// pagination also open valid expression subqueries and must reach normalization.
func subqueryBraceBodyStartsLikeClause(query string, braceIndex int) bool {
	index := queryGapEnd(query, braceIndex+1)
	if index >= len(query) {
		return false
	}
	if query[index] == '(' {
		return true
	}
	start := index
	for index < len(query) && isIdentByte(query[index]) {
		index++
	}
	if index == start {
		return false
	}
	// A clause keyword can name a map key or a standalone projection selector.
	if next := queryGapEnd(query, index); next < len(query) && (query[next] == ':' || query[next] == ',' || query[next] == '}') {
		return false
	}

	switch upperASCII(query[start:index]) {
	case "MATCH", "OPTIONAL", "CALL", "WITH", "UNWIND", "RETURN", "ORDER", "LIMIT", "SKIP", "OFFSET":
		return true
	default:
		return false
	}
}

func isCypherWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// inProjectionClause reports whether position lies in a RETURN, WITH, UNWIND
// or SET clause, where parentheses group an expression, such as a map
// projection (n {.k}).k, rather than delimit a node pattern.
func inProjectionClause(query string, position int) bool {

	switch mapClauseBefore(query[:position]) {
	case "RETURN", "WITH", "UNWIND", "SET":
		return true
	}
	return false
}

func validMapLiteralKey(key string) bool {
	if _, ok := isOneSymbolicName(key); ok {
		return true
	}
	return isWholeCypherQuotedString(key)
}

func undefinedStandaloneMapValue(expression string) string {
	inner, mapLiteral := stripEnclosingRowDelimiter(strings.TrimSpace(expression), '{', '}')
	if !mapLiteral {
		return ""
	}
	for _, part := range splitTopLevelComma(inner) {
		separator := findTopLevelMapKeyValueSeparator(part)
		if separator <= 0 {
			continue
		}
		value := strings.TrimSpace(part[separator+1:])
		if isValidIdentifier(value) && !strings.EqualFold(value, "null") &&
			!strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
			return value
		}
		if nested := undefinedStandaloneMapValue(value); nested != "" {
			return nested
		}
	}
	return ""
}

// colonIsMapKeySeparator reports whether the ':' at colon follows a map key
// ({key: …} or {…, key: …}) rather than a variable or label in a pattern.
func colonIsMapKeySeparator(query string, colon int) bool {
	if colon <= 0 || colon > len(query) {
		return false
	}
	// Test every possible entry boundary. Delimiter-looking bytes inside
	// strings, comments or quoted names cannot win: the shared symbolic-name
	// reader must consume the complete candidate between boundary and colon.
	for boundary := colon - 1; boundary >= 0; boundary-- {
		if query[boundary] != '{' && query[boundary] != ',' {
			continue
		}
		candidate := strings.TrimSpace(StripComments(query[boundary+1 : colon]))
		if _, ok := isOneSymbolicName(candidate); ok {
			return true
		}
	}
	return false
}

// subqueryBraceScanner retains lexical progress across increasing brace offsets.
// Each query byte is read once even when a statement contains many plain maps.
type subqueryBraceScanner struct {
	query              string
	position, lastLive int
}

func newSubqueryBraceScanner(query string, start int) subqueryBraceScanner {
	return subqueryBraceScanner{query: query, position: start, lastLive: start - 1}
}

func (scanner *subqueryBraceScanner) at(brace int) bool {
	if brace < 0 || brace >= len(scanner.query) || scanner.query[brace] != '{' {
		return false
	}
	for scanner.position < brace {
		i := scanner.position
		c := scanner.query[i]
		if isCypherQuoteByte(c) {
			scanner.position = skipCypherQuotedText(scanner.query, i, c)
			scanner.lastLive = scanner.position - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(scanner.query, i); end > i {
				scanner.position = end
				continue
			}
		}
		if size := queryWhitespaceAt(scanner.query, i); size > 0 {
			scanner.position += size
			continue
		}
		scanner.lastLive = i
		scanner.position++
	}
	return scanner.position == brace && subqueryExpressionAfterLiveByte(scanner.query, brace, scanner.lastLive)
}
