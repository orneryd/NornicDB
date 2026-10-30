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
	name      string
	inner     string
	arguments cyphertext.Span
}

// inspectFunctionCall owns standalone call shape and matching delimiters.
// Qualified extraction trims its input and validates/canonicalizes qualified
// names. Named recognition preserves the original source extent and existing
// prefix matcher; it does not impose qualified-name admission. Facts are
// optional and populated on success, so boolean recognition needs no result
// product or additional allocation.
func inspectFunctionCall(source string, purpose functionCallPurpose, expectedName string, facts *functionCallFacts) bool {
	left := 0
	switch purpose {
	case qualifiedFunctionCall:
		left = len(source) - len(strings.TrimLeftFunc(source, cyphertext.IsWhitespace))
		source = strings.TrimSpace(source)
	case namedFunctionCall:
		if !hasFuncStart(source, expectedName) {
			return false
		}
	default:
		return false
	}
	open := strings.IndexByte(source, '(')
	if open < 0 || (purpose == qualifiedFunctionCall && open == 0) {
		return false
	}
	var name string
	if purpose == qualifiedFunctionCall {
		var ok bool
		name, ok = canonicalCallableName(source[:open])
		if !ok {
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
		facts.arguments = cyphertext.Span{Start: left + open + 1, End: left + close}
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
	// Ordinary unquoted names need no normalization. Validate their lexical
	// components in one pass before using the trivia/quoted-token reader.
	if len(text) > 0 && cyphertext.NameStartAt(text, 0) {
		compact := true
		for index := 1; index < len(text); index++ {
			if cyphertext.NamePartAt(text, index) {
				continue
			}
			if text[index] == '.' && index+1 < len(text) && cyphertext.NameStartAt(text, index+1) {
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
	name, ok := cyphertext.ScanIdentifier(text, 0)
	if !ok {
		return "", false
	}
	if name.End == len(text) {
		return name.Value, true
	}
	parts, end := dottedNameParts(text, name.End, true)
	if end != len(text) && queryGapEnd(text, end) != len(text) {
		return "", false
	}
	if len(name.Value)+len(parts) == len(text) {
		return text, true
	}
	return name.Value + parts, true
}
