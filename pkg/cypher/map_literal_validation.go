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

	upper := upperASCIIWithCancellation(trimSpaceWithCancellation(query, probe), probe)
	if strings.HasPrefix(upper, "CREATE ") && containsTextWithCancellation(upper, "INDEX", probe) && keywordIndexFromWithCancellation(query, "OPTIONS", 0, defaultKeywordScanOpts(), probe) >= 0 {
		return nil
	}
	if strings.HasPrefix(upper, "CREATE CONSTRAINT") && keywordIndexFromWithCancellation(query, "REQUIRE", 0, defaultKeywordScanOpts(), probe) >= 0 {
		return nil
	}
	for index := 0; index < len(query); index++ {
		if probe != nil && !probe.check() {
			return probe.err
		}

		if query[index] == '\'' || query[index] == '"' || query[index] == '`' {
			index = numericValidationSkipQuotedWithCancellation(query, index, probe) - 1
			continue
		}
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '/' {
			index += 2
			for index < len(query) && query[index] != '\n' && query[index] != '\r' {
				if probe != nil && !probe.check() {
					return probe.err
				}

				index++
			}
			continue
		}
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '*' {
			index += 2
			for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
				if probe != nil && !probe.check() {
					return probe.err
				}

				index++
			}
			index++
			continue
		}
		if query[index] != '{' {
			continue
		}
		if precedingSubqueryExpressionKeywordWithCancellation(query, index, probe) {
			continue
		}
		// A {m,n} after a parenthesised path quantifies it
		// ((a)-[r]-(b)){1,6}; it is not a map.
		if quantifiedGroupQuantifierAtWithCancellation(query, index, probe) {
			continue
		}
		close := findMatchingDelimiterWithCancellation(query, index, '{', '}', probe)
		if close < 0 {
			continue
		}
		parts := splitTopLevelCommaWithCancellation(query[index+1:close], probe)
		mapLiteral := false
		for _, part := range parts {
			if probe != nil && !probe.check() {
				return probe.err
			}

			if findTopLevelMapKeyValueSeparatorWithCancellation(part, probe) >= 0 {
				mapLiteral = true
				break
			}
		}
		if !mapLiteral {
			if trimSpaceWithCancellation(query[index+1:close], probe) != "" && insidePatternDelimiterWithCancellation(query, index, probe) && !inProjectionClauseWithCancellation(query, index, probe) {
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
			separator := findTopLevelMapKeyValueSeparatorWithCancellation(part, probe)
			if separator <= 0 || !validMapLiteralKeyWithCancellation(trimSpaceWithCancellation(part[:separator], probe), probe) {
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
// CALL {) rather than a node/relationship pattern property map. It scans
// backward over whitespace to the nearest identifier token and matches it
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
	if probe != nil && !probe.check() {
		return false
	}

	index := braceIndex - 1
	for index >= 0 && isCypherWhitespace(query[index]) {
		if probe != nil && !probe.check() {
			return false
		}

		index--
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
	switch upperASCIIWithCancellation(query[start:end], probe) {
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
			if !colonIsMapKeySeparatorWithCancellation(query, precedingToken, probe) {
				return false
			}
		}
	}
	return subqueryBraceBodyStartsLikeClauseWithCancellation(query, braceIndex, probe)
}

// subqueryBraceBodyStartsLikeClause reports whether the body of the brace at
// braceIndex opens with a subquery clause keyword (MATCH, OPTIONAL, CALL,
// WITH, UNWIND, RETURN) or a bare pattern element ('('), which is how every
// EXISTS/COUNT/COLLECT/CALL subquery body starts. A malformed pattern
// property map's body is a bare (invalid) key/value list and never starts
// this way, so this rejects `(count {bad})` and `(n:Count {bad})` even
// though the preceding-token check above cannot always tell a node-pattern
// '(' apart from an expression-grouping '(' (both precede the keyword with
// '(' in `(count {bad})` and `(EXISTS { MATCH ... })`).
func subqueryBraceBodyStartsLikeClause(query string, braceIndex int) bool {
	return subqueryBraceBodyStartsLikeClauseWithCancellation(query, braceIndex, nil)
}
func subqueryBraceBodyStartsLikeClauseWithCancellation(query string, braceIndex int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	index := braceIndex + 1
	for index < len(query) {
		if probe != nil && !probe.check() {
			return false
		}
		if isCypherWhitespace(query[index]) {
			index++
			continue
		}
		if query[index] == '/' {
			if end := queryCommentEndWithCancellation(query, index, probe); end > index {
				index = end
				continue
			}
		}
		break
	}
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
	// A clause keyword can name a map-projection entry too.
	if next := skipSpacesWithCancellation(query, index, probe); next < len(query) && query[next] == ':' {
		return false
	}

	switch upperASCIIWithCancellation(query[start:index], probe) {
	case "MATCH", "OPTIONAL", "CALL", "WITH", "UNWIND", "RETURN":
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
func inProjectionClauseWithCancellation(query string, position int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	clause, last := "", -1
	for _, keyword := range []string{"MATCH", "CREATE", "MERGE", "WHERE", "RETURN", "WITH", "UNWIND", "SET"} {
		if probe != nil && !probe.check() {
			return false
		}

		positions := findAllTopLevelPipelineKeywordPositionsWithCancellation(query[:position], keyword, probe)
		if len(positions) > 0 && positions[len(positions)-1] > last {
			clause, last = keyword, positions[len(positions)-1]
		}
	}
	switch clause {
	case "RETURN", "WITH", "UNWIND", "SET":
		return true
	}
	return false
}

func insidePatternDelimiter(query string, end int) bool {
	return insidePatternDelimiterWithCancellation(query, end, nil)
}
func insidePatternDelimiterWithCancellation(query string, end int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	var parentheses []bool
	brackets := 0
	for index := 0; index < end; index++ {
		if probe != nil && !probe.check() {
			return false
		}

		if query[index] == '\'' || query[index] == '"' || query[index] == '`' {
			index = numericValidationSkipQuotedWithCancellation(query, index, probe) - 1
			continue
		}
		switch query[index] {
		case '(':
			parentheses = append(parentheses, !functionCallParenAt(query, index))
		case ')':
			if len(parentheses) > 0 {
				parentheses = parentheses[:len(parentheses)-1]
			}
		case '[':
			brackets++
		case ']':
			if brackets > 0 {
				brackets--
			}
		}
	}
	for _, pattern := range parentheses {
		if pattern {
			return true
		}
	}
	return brackets > 0
}

func validMapLiteralKey(key string) bool { return validMapLiteralKeyWithCancellation(key, nil) }
func validMapLiteralKeyWithCancellation(key string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	if isValidIdentifierWithCancellation(key, probe) {
		return true
	}
	if isWholeCypherQuotedStringWithCancellation(key, probe) {
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
