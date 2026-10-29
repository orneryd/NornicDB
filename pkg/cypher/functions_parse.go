package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

type functionCallPurpose uint8

const (
	qualifiedFunctionCall functionCallPurpose = iota
	namedFunctionCall
)

type functionCallFacts struct {
	name  string
	inner string
}

// inspectFunctionCall owns standalone call shape and matching delimiters.
// Qualified extraction trims its input and validates/canonicalizes qualified
// names. Named recognition preserves the original source extent and existing
// prefix matcher; it does not impose qualified-name admission. Facts are
// optional and populated on success, so boolean recognition needs no result
// product or additional allocation.
func inspectFunctionCall(source string, purpose functionCallPurpose, expectedName string, facts *functionCallFacts) bool {
	switch purpose {
	case qualifiedFunctionCall:
		source = strings.TrimSpace(source)
	case namedFunctionCall:
		if !hasFuncStart(source, expectedName) {
			return false
		}
	default:
		return false
	}
	var name string
	var open int
	if purpose == qualifiedFunctionCall {
		var end int
		var valid bool
		name, end, valid = qualifiedNameAt(source, 0)
		open = queryGapEnd(source, end)
		if !valid || open >= len(source) || source[open] != '(' {
			return false
		}
	} else {
		open = strings.IndexByte(source, '(')
		if open < 0 {
			return false
		}
	}
	close := cyphertext.FindMatchingDelimiter(source, open, '(', ')', cyphertext.OpeningDelimiter)
	if close != len(source)-1 {
		return false
	}
	if facts != nil {
		facts.name = name
		facts.inner = source[open+1 : close]
	}
	return true
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
		for previous >= 0 && cyphertext.IsWhitespaceAt(text, previous) {
			previous--
		}
	}
	if previous >= 0 && text[previous] == '.' {
		return false
	}
	return !cyphertext.NamePartAt(text, at-1)
}

// canonicalCallableName shares the qualified identifier reader used by semantic
// validation, preserving component case while removing trivia around separators.
func canonicalCallableName(text string) (string, bool) {
	text = strings.TrimSpace(text)
	name, end, ok := qualifiedNameAt(text, 0)
	return name, ok && queryGapEnd(text, end) == len(text)
}

// parseFunctionCallWS adapts the retained standalone-call shape owner.
func parseFunctionCallWS(source string) (name, inner string, ok bool) {
	var facts functionCallFacts
	ok = inspectFunctionCall(source, qualifiedFunctionCall, "", &facts)
	return facts.name, facts.inner, ok
}
