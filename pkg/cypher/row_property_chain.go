package cypher

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"
)

// isLiteralKeyword reports whether name is one of Cypher's literal words:
// true, false, null, NaN or Infinity, in any case. In an expression those
// words are always the literal, even where a variable of that name was
// declared (WITH 1 AS null, MATCH (NaN)), as in Neo4j 5.26: such a variable
// can be declared but never read (#907).
func isLiteralKeyword(name string) bool {
	return literalKeywordKind(name) != literalKeywordNone
}

// rowExpressionBinding is the shared binding lookup for expressions. Literal
// words keep their native syntax role even when a CALL frame retains an import
// with the same name. Quoted references keep their distinct expression text.
func rowExpressionBinding[T any](values map[string]T, expression string) (T, bool) {
	value, bound := values[expression]
	return value, bound && literalKeywordKind(expression) == literalKeywordNone
}

type literalKeyword uint8

const (
	literalKeywordNone literalKeyword = iota
	literalKeywordNaN
	literalKeywordTrue
	literalKeywordNull
	literalKeywordFalse
	literalKeywordInfinity
)

// literalKeywordKind owns literal-word recognition for both binding and value
// readers. A binding lookup needs the syntax role without constructing a value.
func literalKeywordKind(name string) literalKeyword {
	switch len(name) {
	case 3:
		if strings.EqualFold(name, "NaN") {
			return literalKeywordNaN
		}
	case 4:
		if strings.EqualFold(name, "true") {
			return literalKeywordTrue
		}
		if strings.EqualFold(name, "null") {
			return literalKeywordNull
		}
	case 5:
		if strings.EqualFold(name, "false") {
			return literalKeywordFalse
		}
	case 8:
		if strings.EqualFold(name, "Infinity") {
			return literalKeywordInfinity
		}
	}
	return literalKeywordNone
}

// literalKeywordValue is the value of a literal word (isLiteralKeyword).
func literalKeywordValue(name string) (interface{}, bool) {
	switch literalKeywordKind(name) {
	case literalKeywordNaN:
		return math.NaN(), true
	case literalKeywordTrue:
		return true, true
	case literalKeywordNull:
		return nil, true
	case literalKeywordFalse:
		return false, true
	case literalKeywordInfinity:
		return math.Inf(1), true
	}
	return nil, false
}

// rowPropertyChainShape reports whether expr is exactly identifiers joined by
// dots (e.uuid, n.a.b), and returns the first identifier and the chain after
// it. Anything else (literals, backticks, calls, operators, spaces) is not,
// nor is a chain on a literal word (true, false, null, NaN, Infinity), which
// reads the literal (isLiteralKeyword).
func rowPropertyChainShape(expr string) (variable, chain string, ok bool) {
	dot := -1
	start := true
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch {
		case c == '.':
			if start || i == len(expr)-1 {
				return "", "", false
			}
			if dot < 0 {
				dot = i
			}
			start = true
		case isIdentStartByte(c):
			start = false
		case c >= '0' && c <= '9':
			if start {
				return "", "", false
			}
		default:
			return "", "", false
		}
	}
	if dot < 0 || isLiteralKeyword(expr[:dot]) {
		return "", "", false
	}
	return expr[:dot], expr[dot+1:], true
}
