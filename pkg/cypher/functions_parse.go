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
	call, ok := functionCallAt(expr, queryGapEnd(expr, 0))
	if !ok {
		return "", "", false
	}
	close := call.close(expr)
	if close < 0 || queryGapEnd(expr, close+1) != len(expr) {
		return "", "", false
	}
	return call.name, expr[call.open+1 : close], true
}

// functionCallSpan is the shared lexical identity of a call. Delimiter matching
// is lazy: name-only validation does not rescan every nested argument list.
type functionCallSpan struct {
	name        string
	start, open int
}

func (call functionCallSpan) close(text string) int {
	return findMatchingDelimiter(text, call.open, '(', ')')
}

// functionCallAt reads a name at a known code position, including quoted and
// qualified components and trivia before "(". Whole-text consumers must use
// nextFunctionCall so comments and string literals cannot become code.
func functionCallAt(text string, at int) (functionCallSpan, bool) {
	if at < 0 || at >= len(text) || !functionNameStartsAt(text, at) ||
		at > 0 && (text[at-1] == '$' || text[at-1] == ':') {
		return functionCallSpan{}, false
	}
	name, next, ok := scanIdentifierToken(text, at)
	if !ok {
		return functionCallSpan{}, false
	}
	parts, end := dottedNameParts(text, next, true)
	open := queryGapEnd(text, end)
	if open >= len(text) || text[open] != '(' {
		return functionCallSpan{}, false
	}
	return functionCallSpan{name: name + parts, start: at, open: open}, true
}

// nextFunctionCall is the lexical owner for static and runtime call readers.
// It advances into arguments so nested calls are visited, and excludes syntax,
// procedure/graph names, type annotations, comments and string literals.
func nextFunctionCall(text string, index int) (functionCallSpan, bool) {
	return nextFunctionCallIn(text, index, true)
}

func nextFunctionCallIn(text string, index int, descendSubqueries bool) (functionCallSpan, bool) {
	for index < len(text) {
		if end := queryGapEnd(text, index); end > index {
			index = end
			continue
		}
		if text[index] == '\'' || text[index] == '"' {
			index = skipCypherQuotedText(text, index, text[index])
			continue
		}
		if !descendSubqueries {
			if end := subqueryExpressionEnd(text, index); end > index {
				index = end
				continue
			}
		}
		name, next, ok := scanIdentifierToken(text, index)
		if !ok {
			index++
			continue
		}
		if text[index] != '`' && (strings.EqualFold(name, "CALL") || strings.EqualFold(name, "USE")) {
			index = skipCalledName(text, next)
			continue
		}
		call, called := functionCallAt(text, index)
		if called && !precededByTypeAnnotation(text, index) &&
			!isNonFunctionSemanticKeyword(lowerASCII(call.name)) {
			return call, true
		}
		index = next
	}
	return functionCallSpan{}, false
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
