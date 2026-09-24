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

	name = strings.TrimSpace(expr[:openIdx])
	if name == "" {
		return "", "", false
	}

	// Validate name is identifier-ish (letters/underscore start; alnum/underscore/dot after).
	for i, c := range name {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return "", "", false
			}
			continue
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			return "", "", false
		}
	}

	// Find matching closing paren.
	depth := 0
	inQuote := false
	quoteChar := rune(0)
	closeIdx := -1
	for i := openIdx; i < len(expr); i++ {
		ch := rune(expr[i])
		if inQuote {
			if ch == quoteChar && !isBackslashEscaped(expr, i) {
				inQuote = false
				quoteChar = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inQuote = true
			quoteChar = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeIdx = i
				i = len(expr) // break
			}
		}
	}
	if closeIdx == -1 {
		return "", "", false
	}

	// Ensure no trailing non-space content after the matching close paren.
	if strings.TrimSpace(expr[closeIdx+1:]) != "" {
		return "", "", false
	}

	return name, expr[openIdx+1 : closeIdx], true
}
