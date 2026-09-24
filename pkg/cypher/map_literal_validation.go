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
// allows before a bare '{'. A property map is only ever preceded by a
// pattern element (identifier, label, ')', or ']'), never by these keywords,
// so this check is safe to apply at any paren/bracket nesting depth.
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
