package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
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

type literalKeyword = cyphertext.LiteralKeyword

const (
	literalKeywordNone     = cyphertext.LiteralKeywordNone
	literalKeywordNaN      = cyphertext.LiteralKeywordNaN
	literalKeywordTrue     = cyphertext.LiteralKeywordTrue
	literalKeywordNull     = cyphertext.LiteralKeywordNull
	literalKeywordFalse    = cyphertext.LiteralKeywordFalse
	literalKeywordInfinity = cyphertext.LiteralKeywordInfinity
)

func literalKeywordKind(name string) literalKeyword { return cyphertext.LiteralKeywordKind(name) }

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
