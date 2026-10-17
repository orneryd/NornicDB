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

	var valid bool
	name, valid = canonicalCallableNameWithCancellation(expr[:openIdx], probe)
	if !valid {
		return "", "", false
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
	if trimSpaceWithCancellation(expr[closeIdx+1:], probe) != "" {
		return "", "", false
	}

	if probe != nil && probe.err != nil {
		return "", "", false
	}
	return name, expr[openIdx+1 : closeIdx], true
}

// functionNameStartsAt prevents a builtin scanner from matching only the last
// component of a qualified function name (custom.range is not range), and is
// the one test for a word that follows a dot: a property key (x . and) or a
// name component is never a keyword or a function of its own. Whitespace and
// comments may stand around the dot.
func functionNameStartsAt(text string, at int) bool {
	if at == 0 {
		return true
	}
	if isIdentByte(text[at-1]) {
		return false
	}
	previous := queryGapStart(text, at) - 1
	return previous < 0 || text[previous] != '.'
}

// canonicalCallableName shares the qualified identifier reader used by semantic
// validation, preserving component case while removing trivia around separators.
func canonicalCallableName(text string) (string, bool) {
	return canonicalCallableNameWithCancellation(text, nil)
}

func canonicalCallableNameWithCancellation(text string, probe *pipelineCancellation) (string, bool) {
	text = trimSpaceWithCancellation(text, probe)
	if probe != nil && probe.err != nil {
		return "", false
	}
	// Ordinary unquoted names need no normalization. Validate their lexical
	// components in one pass before using the trivia/quoted-token reader.
	if len(text) > 0 && isIdentStartByte(text[0]) {
		compact := true
		for index := 1; index < len(text); index++ {
			if probe != nil && !probe.check() {
				return "", false
			}
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
	name, end, ok := scanIdentifierTokenWithCancellation(text, 0, probe)
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
