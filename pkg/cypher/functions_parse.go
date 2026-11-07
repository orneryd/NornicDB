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
	}
	return true
}

// isPropertyDotAt distinguishes a qualification/property separator from a
// slice/range token. Callers retain their own forward or predecessor extents.
func isPropertyDotAt(text string, at int) bool {
	return at >= 0 && at < len(text) && text[at] == '.' &&
		(at == 0 || text[at-1] != '.') && (at+1 == len(text) || text[at+1] != '.')
}

// functionNameStartsAt prevents a builtin scanner from matching only the last
// component of a qualified function name (custom.range is not range).
func functionNameStartsAt(text string, at int) bool {
	if at == 0 {
		return true
	}
	previous, _ := expressionPredecessor(text, at)
	// A single dot continues a qualified name; the adjacent '..' token
	// introduces a slice bound instead, where a builtin starts normally.
	if isPropertyDotAt(text, previous) {
		return false
	}
	return text[at-1] != '$' && !cyphertext.NamePartAt(text, at-1)
}

// expressionPredecessor returns the last non-trivia byte before limit and,
// when it belongs to a quoted token, that token's start. Ordinary adjacent
// bytes and horizontal gaps need no prefix scan. A comment terminator or a
// line break requires forward lexical ownership: reverse text cannot distinguish
// comment contents from names. The same pass locates string receivers, replacing
// their separate quote/comment walk.
func expressionPredecessor(expression string, limit int) (last, quotedStart int) {
	last = limit - 1
	if last < 0 {
		return -1, -1
	}
	// Horizontal ASCII gaps cannot terminate a line comment. Resolve this
	// common boundary without entering the full lexical-span walk; a slash,
	// quote, line break or Unicode gap still goes through that owner.
	for last >= 0 && cyphertext.IsHorizontalASCIIWhitespace(expression[last]) {
		last--
	}
	if last < 0 {
		return -1, -1
	}
	character := expression[last]
	if character > ' ' && character < 0x80 && character != '/' && !isCypherQuoteByte(character) {
		return last, -1
	}
	return scanExpressionPredecessor(expression, limit)
}

// scanExpressionPredecessor resolves the lexical spans needed for trivia or
// string receivers. Ordinary source runs are skipped as a unit; only the
// existing quote/comment owners interpret token boundaries.
func scanExpressionPredecessor(expression string, limit int) (last, quotedStart int) {
	last = limit - 1
	if last < 0 {
		return -1, -1
	}
	if character := expression[last]; character <= ' ' || character >= 0x80 {
		var found bool
		last, found = cyphertext.NeighborIndex(expression, limit, cyphertext.Before)
		if !found {
			return -1, -1
		}
	}
	if expression[last] != '/' && !isCypherQuoteByte(expression[last]) &&
		!strings.ContainsAny(expression[last+1:limit], "\r\n\u0085\u2028\u2029") {
		return last, -1
	}
	last, quotedStart = -1, -1
	for index := 0; index < limit; {
		if character := expression[index]; !isCypherQuoteByte(character) && character != '/' {
			offset := strings.IndexAny(expression[index:limit], "'\"`/")
			end := limit
			if offset >= 0 {
				end = index + offset
			}
			if previous, ok := cyphertext.NeighborIndex(expression, end, cyphertext.Before); ok && previous >= index {
				last, quotedStart = previous, -1
			}
			if offset < 0 {
				break
			}
			index = end
		}
		if isCypherQuoteByte(expression[index]) {
			end, closed := cyphertext.ScanQuotedText(expression, index)
			if !closed || end > limit {
				return -1, -1
			}
			last, quotedStart, index = end-1, index, end
			continue
		}
		if end, _ := cyphertext.GapEnd(expression, index); end > index {
			index = end
			continue
		}
		last, quotedStart = index, -1
		index++
	}
	return last, quotedStart
}

// callableReceiverStart reads the callable immediately before an argument
// group, or returns open when it is a group rather than a call. Receiver and
// label-expression consumers share the semantic keyword boundary and trivia
// owner instead of independently absorbing NOT, IN or comment text.
func callableReceiverStart(expression string, open int) int {
	end, quoted := expressionPredecessor(expression, open)
	if end < 0 || !isRowReceiverIdentifierAt(expression, end) {
		return open
	}
	last := end
	start := end
	qualified := false
	for {
		if quoted >= 0 && isCypherBacktickByte(expression[quoted]) {
			start = quoted
		} else {
			for start > 0 && cyphertext.NamePartAt(expression, start-1) {
				start--
			}
		}
		before, _ := expressionPredecessor(expression, start)
		if !isPropertyDotAt(expression, before) {
			break
		}
		end, quoted = expressionPredecessor(expression, before)
		if end < 0 || !isRowReceiverIdentifierAt(expression, end) {
			return open
		}
		qualified = true
		start = end
	}
	if !qualified && isNonFunctionSemanticKeyword(lowerASCII(expression[start:last+1])) {
		return open
	}
	return start
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
	parts, end := dottedNameParts(text, name.End, true, dottedNameIdentity)
	if end != len(text) && queryGapEnd(text, end) != len(text) {
		return "", false
	}
	if len(name.Value)+len(parts) == len(text) {
		return text, true
	}
	return name.Value + parts, true
}
