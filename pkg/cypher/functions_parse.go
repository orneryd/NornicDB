package cypher

import "strings"

// parseFunctionCallWS parses a standalone function call expression, tolerating
// whitespace between the function name and the opening parenthesis.
//
// Examples:
//   - "toLower(n.name)"   -> ("toLower", "n.name", true)
//   - "toLower ( n.name)" -> ("toLower", " n.name", true)
//
// Returns ok=false if the expression is not a standalone function call.
func parseFunctionCallWS(expr string) (name string, inner string, ok bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}

	openIdx := strings.Index(expr, "(")
	if openIdx <= 0 {
		return "", "", false
	}

	var valid bool
	name, valid = canonicalCallableName(expr[:openIdx])
	if !valid {
		return "", "", false
	}

	closeIdx := findMatchingDelimiter(expr, openIdx, '(', ')')
	if closeIdx < 0 {
		return "", "", false
	}

	// Ensure no trailing non-space content after the matching close paren.
	if strings.TrimSpace(expr[closeIdx+1:]) != "" {
		return "", "", false
	}

	return name, expr[openIdx+1 : closeIdx], true
}

// functionNameStartsAt prevents a builtin scanner from matching only the last
// component of a qualified function name (custom.range is not range).
func functionNameStartsAt(text string, at int) bool {
	if at == 0 {
		return true
	}
	previous := at - 1
	if strings.Contains(text[:at], "/*") || strings.Contains(text[:at], "//") {
		// Comments can separate a namespace dot from its final component.
		// Use the query trivia reader; comment-like text inside quotes is data.
		previous = -1
		for index := 0; index < at; {
			if end := queryGapEnd(text, index); end > index {
				index = end
				continue
			}
			if isCypherQuoteByte(text[index]) {
				index = skipQuotedSemanticText(text, index)
				previous = index - 1
				continue
			}
			previous = index
			index++
		}
	} else {
		for previous >= 0 && isASCIIWhitespace(text[previous]) {
			previous--
		}
	}
	if previous >= 0 && text[previous] == '.' {
		return false
	}
	return !isIdentByte(text[at-1])
}

// canonicalCallableName shares the qualified identifier reader used by semantic
// validation, preserving component case while removing trivia around separators.
func canonicalCallableName(text string) (string, bool) {
	text = strings.TrimSpace(text)
	// Ordinary unquoted names need no normalization. Validate their lexical
	// components in one pass before using the trivia/quoted-token reader.
	if len(text) > 0 && isIdentStartByte(text[0]) {
		compact := true
		for index := 1; index < len(text); index++ {
			if isIdentByte(text[index]) {
				continue
			}
			if text[index] == '.' && index+1 < len(text) && isIdentStartByte(text[index+1]) {
				index++
				continue
			}
			compact = false
			break
		}
		if compact {
			return text, true
		}
	}
	name, end, ok := scanIdentifierToken(text, 0)
	if !ok {
		return "", false
	}
	if end == len(text) {
		return name, true
	}
	parts, end := dottedNameParts(text, end, true)
	if end != len(text) && queryGapEnd(text, end) != len(text) {
		return "", false
	}
	if len(name)+len(parts) == len(text) {
		return text, true
	}
	return name + parts, true
}
