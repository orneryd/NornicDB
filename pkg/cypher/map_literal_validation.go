package cypher

import "strings"

// validateStaticMapKeys rejects malformed symbolic map keys before either
// parser frontend or executor can normalize them. Braces without a top-level
// key/value separator are ignored because they may delimit a subquery.
func validateStaticMapKeys(query string) error {
	return validateStaticMapKeysWithCancellation(query, nil)
}
func validateStaticMapKeysWithCancellation(query string, probe *pipelineCancellation) (err error) {
	defer func() {
		if probe != nil && probe.err != nil {
			err = probe.err
		}
	}()
	if probe != nil && !probe.check() {
		return probe.err
	}

	upper := upperASCII(trimSpaceWithCancellation(query, probe))
	if strings.HasPrefix(upper, "CREATE ") && containsTextWithCancellation(upper, "INDEX", probe) && keywordIndexFrom(query, "OPTIONS", 0, defaultKeywordScanOpts()) >= 0 {
		return nil
	}
	if strings.HasPrefix(upper, "CREATE CONSTRAINT") && keywordIndexFrom(query, "REQUIRE", 0, defaultKeywordScanOpts()) >= 0 {
		return nil
	}
	var patternContext *mapPatternContext
	// Carry the predecessor through this lexical pass. Re-reading from byte
	// zero for each brace makes a flat list of ordinary maps quadratic.
	lastLive := -1
	for index := 0; index < len(query); index++ {
		if probe != nil && !probe.check() {
			return probe.err
		}

		if query[index] == '\'' || query[index] == '"' || query[index] == '`' {
			index = numericValidationSkipQuoted(query, index) - 1
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
				if err := validateStaticMapKeysWithCancellation(query[index+1:close], probe); err != nil {
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
		parts := splitTopLevelComma(query[index+1:close])
		mapLiteral := false
		for _, part := range parts {
			if probe != nil && !probe.check() {
				return probe.err
			}

			if findTopLevelMapKeyValueSeparator(part) >= 0 {
				mapLiteral = true
				break
			}
		}
		if !mapLiteral {
			if trimSpaceWithCancellation(query[index+1:close], probe) == "" {
				continue
			}
			if patternContext == nil {
				if probe != nil && !probe.check() {
					return probe.err
				}
				patternContext = newMapPatternContext(query)
			}
			if probe != nil && !probe.check() {
				return probe.err
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
			if probe != nil && !probe.check() {
				return probe.err
			}

			trimmedPart := trimSpaceWithCancellation(part, probe)
			if _, selector := mapProjectionPropertySelectorWithCancellation(trimmedPart, probe); trimmedPart == ".*" || (strings.HasPrefix(trimmedPart, ".") && selector) {
				continue
			}
			separator := findTopLevelMapKeyValueSeparator(part)
			if separator <= 0 || !validMapLiteralKey(trimSpaceWithCancellation(part[:separator], probe)) {
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
	return precedingSubqueryExpressionKeywordWithCancellation(query, braceIndex, nil)
}
func precedingSubqueryExpressionKeywordWithCancellation(query string, braceIndex int, probe *pipelineCancellation) bool {
	scanner := newSubqueryBraceScanner(query, 0)
	return scanner.atWithCancellation(braceIndex, probe)
}

// subqueryExpressionAfterLiveByte shares the same classification with callers
// that already know the last live byte from a forward lexical pass.
func subqueryExpressionAfterLiveByte(query string, braceIndex, index int) bool {
	return subqueryExpressionAfterLiveByteWithCancellation(query, braceIndex, index, nil)
}
func subqueryExpressionAfterLiveByteWithCancellation(query string, braceIndex, index int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	// CALL (a, b) { … }: step back over the import list to the CALL.
	if index >= 0 && query[index] == ')' {
		depth := 0
		for ; index >= 0; index-- {
			if probe != nil && !probe.check() {
				return false
			}

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
			if probe != nil && !probe.check() {
				return false
			}

			index--
		}
		wordEnd := index + 1
		for index >= 0 && isIdentByte(query[index]) {
			if probe != nil && !probe.check() {
				return false
			}

			index--
		}
		return strings.EqualFold(query[index+1:wordEnd], "CALL")
	}
	end := index + 1
	for index >= 0 && isIdentByte(query[index]) {
		if probe != nil && !probe.check() {
			return false
		}

		index--
	}
	start := index + 1
	if start >= end {
		return false
	}
	return subqueryExpressionKeywordBeforeBraceWithCancellation(query, start, end, braceIndex, probe)
}

// subqueryExpressionKeywordBeforeBrace validates a known keyword token and its
// brace without rescanning its prefix when the forward reader already found it.
func subqueryExpressionKeywordBeforeBrace(query string, start, end, braceIndex int) bool {
	return subqueryExpressionKeywordBeforeBraceWithCancellation(query, start, end, braceIndex, nil)
}
func subqueryExpressionKeywordBeforeBraceWithCancellation(query string, start, end, braceIndex int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	switch upperASCII(query[start:end]) {
	case "EXISTS", "COUNT", "COLLECT", "CALL":
	default:
		return false
	}
	precedingToken := start - 1
	for precedingToken >= 0 && isCypherWhitespace(query[precedingToken]) {
		if probe != nil && !probe.check() {
			return false
		}

		precedingToken--
	}
	if precedingToken >= 0 {
		switch query[precedingToken] {
		case '`':
			// Backtick-quoted identifier (`` `count` {bad} ``), never the
			// keyword.
			return false
		case ':':
			// Label/type name (`:Count {bad}`), unless the colon separates
			// a map key from its value ({k: COUNT { … }}).
			if !colonIsMapKeySeparator(query, precedingToken) {
				return false
			}
		}
	}
	return subqueryBraceBodyStartsLikeClauseWithCancellation(query, braceIndex, probe)
}

// subqueryBraceBodyStartsLikeClause distinguishes a subquery's opening clause
// from an identically named map-projection selector. A selector ends at a comma
// or closing brace; a clause requires a following body. Standalone ordering and
// pagination also open valid expression subqueries and must reach normalization.
func subqueryBraceBodyStartsLikeClause(query string, braceIndex int) bool {
	return subqueryBraceBodyStartsLikeClauseWithCancellation(query, braceIndex, nil)
}
func subqueryBraceBodyStartsLikeClauseWithCancellation(query string, braceIndex int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	index := queryGapEnd(query, braceIndex+1)
	if index >= len(query) {
		return false
	}
	if query[index] == '(' {
		return true
	}
	start := index
	for index < len(query) && isIdentByte(query[index]) {
		if probe != nil && !probe.check() {
			return false
		}

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
	return inProjectionClauseWithCancellation(query, position, nil)
}

// inProjectionClauseWithCancellation keeps mapClauseBefore as the single
// clause reader; the probe bounds entry only, because that reader (in
// map_pattern_context.go) has no cancellation-aware variant yet.
func inProjectionClauseWithCancellation(query string, position int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	switch mapClauseBefore(query[:position]) {
	case "RETURN", "WITH", "UNWIND", "SET":
		return true
	}
	return false
}

// insidePatternDelimiter is a one-shot entry into mapPatternContext, the single
// owner of pattern-delimiter classification. The static map validator builds
// that context once per statement instead of calling this per brace.
func insidePatternDelimiter(query string, end int) bool {
	return insidePatternDelimiterWithCancellation(query, end, nil)
}
func insidePatternDelimiterWithCancellation(query string, end int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	return newMapPatternContext(query).insidePatternDelimiter(query, end)
}

func validMapLiteralKey(key string) bool { return validMapLiteralKeyWithCancellation(key, nil) }
func validMapLiteralKeyWithCancellation(key string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	if isValidIdentifier(key) {
		return true
	}
	if isWholeCypherQuotedString(key) {
		return true
	}
	if len(key) < 2 || key[0] != '`' || key[len(key)-1] != '`' {
		return false
	}
	for index := 1; index < len(key)-1; index++ {
		if probe != nil && !probe.check() {
			return false
		}

		if key[index] != '`' {
			continue
		}
		if index+1 >= len(key)-1 || key[index+1] != '`' {
			return false
		}
		index++
	}
	return true
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
	return colonIsMapKeySeparatorWithCancellation(query, colon, nil)
}
func colonIsMapKeySeparatorWithCancellation(query string, colon int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	index := colon - 1
	for index >= 0 && isCypherWhitespace(query[index]) {
		if probe != nil && !probe.check() {
			return false
		}

		index--
	}
	if index >= 0 && query[index] == '`' {
		index--
		for index >= 0 && query[index] != '`' {
			if probe != nil && !probe.check() {
				return false
			}

			index--
		}
		index--
	} else {
		end := index
		for index >= 0 && isIdentByte(query[index]) {
			if probe != nil && !probe.check() {
				return false
			}

			index--
		}
		if index == end {
			return false
		}
	}
	for index >= 0 && isCypherWhitespace(query[index]) {
		if probe != nil && !probe.check() {
			return false
		}

		index--
	}
	return index >= 0 && (query[index] == '{' || query[index] == ',')
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
	return scanner.atWithCancellation(brace, nil)
}

// atWithCancellation is the cancellation-aware form of at. A cancelled scan
// reports false; callers read the latched probe error.
func (scanner *subqueryBraceScanner) atWithCancellation(brace int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	if brace < 0 || brace >= len(scanner.query) || scanner.query[brace] != '{' {
		return false
	}
	for scanner.position < brace {
		if probe != nil && !probe.check() {
			return false
		}

		i := scanner.position
		c := scanner.query[i]
		if c == '\'' || c == '"' || c == '`' {
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
	return scanner.position == brace && subqueryExpressionAfterLiveByteWithCancellation(scanner.query, brace, scanner.lastLive, probe)
}
