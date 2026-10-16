package cypher

// parseFunctionCallWS parses a standalone function call expression, tolerating
// whitespace between the function name and the opening parenthesis.
//
// Examples:
//   - "toLower(n.name)"   -> ("toLower", "n.name", true)
//   - "toLower ( n.name)" -> ("toLower", " n.name", true)
//
// Returns ok=false if the expression is not a standalone function call.
func parseFunctionCallWS(expr string) (string, string, bool) {
	return parseFunctionCallWSWithCancellation(expr, nil)
}
func parseFunctionCallWSWithCancellation(expr string, probe *pipelineCancellation) (name string, inner string, ok bool) {
	expr = trimSpaceWithCancellation(expr, probe)
	if expr == "" {
		return "", "", false
	}

	openIdx := indexByteWithCancellation(expr, '(', probe)
	if openIdx <= 0 {
		return "", "", false
	}

	name = trimSpaceWithCancellation(expr[:openIdx], probe)
	if name == "" {
		return "", "", false
	}

	// Validate name is identifier-ish (letters/underscore start; alnum/underscore/dot after).
	for i, c := range name {
		if probe != nil && !probe.check() {
			return "", "", false
		}
		if i == 0 {
			if !isIdentStartRune(c) {
				return "", "", false
			}
			continue
		}
		if !(isIdentRune(c) || c == '.') {
			return "", "", false
		}
	}

	// Find matching closing paren.
	depth := 0
	inQuote := false
	quoteChar := rune(0)
	closeIdx := -1
	for i := openIdx; i < len(expr); i++ {
		if probe != nil && !probe.check() {
			return "", "", false
		}
		ch := rune(expr[i])
		if inQuote {
			if ch == quoteChar && !isBackslashEscapedWithCancellation(expr, i, probe) {
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
	if trimSpaceWithCancellation(expr[closeIdx+1:], probe) != "" {
		return "", "", false
	}

	if probe != nil && probe.err != nil {
		return "", "", false
	}
	return name, expr[openIdx+1 : closeIdx], true
}
