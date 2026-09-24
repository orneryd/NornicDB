package cypher

import "strings"

// validateStaticMapKeys rejects malformed symbolic map keys before either
// parser frontend or executor can normalize them. Braces without a top-level
// key/value separator are ignored because they may delimit a subquery.
func validateStaticMapKeys(query string) error {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upper, "CREATE ") && strings.Contains(upper, "INDEX") && findKeywordIndex(query, "OPTIONS") >= 0 {
		return nil
	}
	if strings.HasPrefix(upper, "CREATE CONSTRAINT") && findKeywordIndex(query, "REQUIRE") >= 0 {
		return nil
	}
	for index := 0; index < len(query); index++ {
		if query[index] == '\'' || query[index] == '"' || query[index] == '`' {
			index = numericValidationSkipQuoted(query, index) - 1
			continue
		}
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '/' {
			index += 2
			for index < len(query) && query[index] != '\n' && query[index] != '\r' {
				index++
			}
			continue
		}
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '*' {
			index += 2
			for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
				index++
			}
			index++
			continue
		}
		if query[index] != '{' {
			continue
		}
		if precedingSubqueryExpressionKeyword(query, index) {
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
			if strings.TrimSpace(query[index+1:close]) != "" && insidePatternDelimiter(query, index) {
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
			if trimmedPart == ".*" || (strings.HasPrefix(trimmedPart, ".") && isValidIdentifier(strings.TrimPrefix(trimmedPart, "."))) {
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
	index := braceIndex - 1
	for index >= 0 && isCypherWhitespace(query[index]) {
		index--
	}
	end := index + 1
	for index >= 0 && isWordChar(query[index]) {
		index--
	}
	start := index + 1
	if start >= end {
		return false
	}
	switch strings.ToUpper(query[start:end]) {
	case "EXISTS", "COUNT", "COLLECT", "CALL":
	default:
		return false
	}
	precedingToken := start - 1
	for precedingToken >= 0 && isCypherWhitespace(query[precedingToken]) {
		precedingToken--
	}
	if precedingToken >= 0 {
		switch query[precedingToken] {
		case ':', '`':
			// Label/type name (`:Count {bad}`) or backtick-quoted
			// identifier (`` `count` {bad} ``), never the keyword.
			return false
		}
	}
	return subqueryBraceBodyStartsLikeClause(query, braceIndex)
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
	index := braceIndex + 1
	for index < len(query) && isCypherWhitespace(query[index]) {
		index++
	}
	if index >= len(query) {
		return false
	}
	if query[index] == '(' {
		return true
	}
	start := index
	for index < len(query) && isWordChar(query[index]) {
		index++
	}
	if index == start {
		return false
	}
	switch strings.ToUpper(query[start:index]) {
	case "MATCH", "OPTIONAL", "CALL", "WITH", "UNWIND", "RETURN":
		return true
	default:
		return false
	}
}

func isCypherWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func insidePatternDelimiter(query string, end int) bool {
	parentheses := 0
	brackets := 0
	for index := 0; index < end; index++ {
		if query[index] == '\'' || query[index] == '"' || query[index] == '`' {
			index = numericValidationSkipQuoted(query, index) - 1
			continue
		}
		switch query[index] {
		case '(':
			parentheses++
		case ')':
			if parentheses > 0 {
				parentheses--
			}
		case '[':
			brackets++
		case ']':
			if brackets > 0 {
				brackets--
			}
		}
	}
	return parentheses > 0 || brackets > 0
}

func validMapLiteralKey(key string) bool {
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
