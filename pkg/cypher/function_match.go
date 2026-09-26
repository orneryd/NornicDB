// Whitespace-tolerant function matching helpers for Cypher parsing.
// These helpers allow optional whitespace between function names and opening parentheses,
// making the parser compatible with formatted Cypher queries like "COUNT (n)" or "count  (n)".

package cypher

import (
	"strings"
)

type funcMatcher struct {
	funcName string
}

func (m *funcMatcher) MatchString(expr string) bool {
	if m == nil {
		return false
	}
	return hasFuncStart(expr, m.funcName)
}

func (m *funcMatcher) String() string {
	if m == nil {
		return ""
	}
	return "func:" + strings.ToLower(strings.TrimSpace(m.funcName))
}

// funcMatcherCache caches function-name matchers by lower-cased name
// (boundedCache).
var funcMatcherCache = newBoundedCache[string, *funcMatcher](1024)

func getFuncMatcher(funcName string) *funcMatcher {
	key := strings.ToLower(strings.TrimSpace(funcName))
	if key == "" {
		return &funcMatcher{funcName: funcName}
	}
	if cached, ok := funcMatcherCache.get(key); ok {
		return cached
	}
	m := &funcMatcher{funcName: key}
	funcMatcherCache.put(key, m)
	return m
}

// matchFuncStart checks if expr starts with funcName followed by optional whitespace and '('.
// Case-insensitive. Returns true if matched.
//
// Examples:
//   - matchFuncStart("count(n)", "count")     → true
//   - matchFuncStart("COUNT (n)", "count")    → true
//   - matchFuncStart("count  (n)", "count")   → true
//   - matchFuncStart("count\n(n)", "count")   → true
//   - matchFuncStart("countx(n)", "count")    → false (different function)
//   - matchFuncStart("xcount(n)", "count")    → false (prefix doesn't match)
func matchFuncStart(expr, funcName string) bool {
	return getFuncMatcher(funcName).MatchString(expr)
}

func hasFuncStart(expr, funcName string) bool {
	expr = strings.TrimSpace(expr)
	funcName = strings.TrimSpace(funcName)
	if expr == "" || funcName == "" {
		return false
	}
	if len(expr) < len(funcName)+1 {
		return false
	}
	if !strings.EqualFold(expr[:len(funcName)], funcName) {
		return false
	}
	// Ensure next token is optional whitespace then '('
	i := len(funcName)
	for i < len(expr) && isSpaceByte(expr[i]) {
		i++
	}
	return i < len(expr) && expr[i] == '('
}

// matchFuncStartAndSuffix checks if expr starts with funcName( and ends with ).
// Allows optional whitespace between function name and opening paren.
// This is the whitespace-tolerant replacement for:
//
//	strings.HasPrefix(lowerExpr, "funcname(") && strings.HasSuffix(expr, ")")
//
// Examples:
//   - matchFuncStartAndSuffix("count(n)", "count")    → true
//   - matchFuncStartAndSuffix("COUNT (n)", "count")   → true
//   - matchFuncStartAndSuffix("count(n) + 1", "count") → false (doesn't end with ))
func matchFuncStartAndSuffix(expr, funcName string) bool {
	return matchFuncStart(expr, funcName) && strings.HasSuffix(expr, ")")
}

// extractFuncArgs extracts the arguments string from a function call expression.
// Assumes the expression is already validated as a function call.
// Returns empty string if not a valid function call format.
//
// Examples:
//   - extractFuncArgs("count(n)", "count")           → "n"
//   - extractFuncArgs("COUNT (n, m)", "count")       → "n, m"
//   - extractFuncArgs("substring('hello', 0)", "substring") → "'hello', 0"
func extractFuncArgs(expr, funcName string) string {
	if !matchFuncStart(expr, funcName) {
		return ""
	}
	// Find the opening paren position
	idx := strings.IndexByte(expr, '(')
	if idx == -1 || !strings.HasSuffix(expr, ")") {
		return ""
	}
	// Return content between ( and )
	return strings.TrimSpace(expr[idx+1 : len(expr)-1])
}

// extractFuncArgsLen returns the arguments and the position of the opening paren.
// Useful when you need to know where the function name ends.
//
// Returns: (argsString, openParenIndex)
// If not matched, returns ("", -1)
func extractFuncArgsLen(expr, funcName string) (string, int) {
	if !matchFuncStart(expr, funcName) {
		return "", -1
	}
	idx := strings.IndexByte(expr, '(')
	if idx == -1 || !strings.HasSuffix(expr, ")") {
		return "", -1
	}
	return strings.TrimSpace(expr[idx+1 : len(expr)-1]), idx
}

// isFunctionCallWS is a whitespace-tolerant version of isFunctionCall.
// Checks if an expression is a standalone function call with balanced parentheses,
// allowing optional whitespace between function name and opening paren.
//
// Examples:
//   - isFunctionCallWS("count(n)", "count")           → true
//   - isFunctionCallWS("COUNT (n)", "count")          → true
//   - isFunctionCallWS("count (n) + 1", "count")      → false (not standalone)
//   - isFunctionCallWS("toLower(count (n))", "count") → false (nested)
func isFunctionCallWS(expr, funcName string) bool {
	if !matchFuncStart(expr, funcName) {
		return false
	}

	// Find the matching closing parenthesis for the opening one
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range expr {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
			if depth == 0 {
				// Found the matching closing parenthesis
				// Check if this is the end of the expression
				return i == len(expr)-1
			}
		}
	}
	return false
}

// extractFuncArgsWithSuffix extracts function arguments and any suffix after the function call.
// This properly handles cases like "collect({...})[..10]" where there's a suffix after the function.
//
// Returns:
//   - args: the content between the function's ( and matching )
//   - suffix: anything after the closing ), e.g., "[..10]"
//   - ok: whether extraction was successful
//
// Examples:
//   - extractFuncArgsWithSuffix("count(n)", "count")           → ("n", "", true)
//   - extractFuncArgsWithSuffix("collect({a: 1})[..10]", "collect") → ("{a: 1}", "[..10]", true)
//   - extractFuncArgsWithSuffix("sum(n.val) + 1", "sum")       → ("n.val", " + 1", true)
func extractFuncArgsWithSuffix(expr, funcName string) (args string, suffix string, ok bool) {
	if !matchFuncStart(expr, funcName) {
		return "", "", false
	}

	// Find the opening parenthesis
	openIdx := strings.IndexByte(expr, '(')
	if openIdx == -1 {
		return "", "", false
	}

	// Find the matching closing parenthesis
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range expr[openIdx:] {
		actualIdx := openIdx + i
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
			if depth == 0 {
				// Found the matching closing parenthesis
				args = strings.TrimSpace(expr[openIdx+1 : actualIdx])
				suffix = expr[actualIdx+1:]
				return args, suffix, true
			}
		}
	}
	return "", "", false
}

func isSpaceByte(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}
