// Package cyphertext owns quotation and comment scanning shared by Cypher
// execution and adapters that must inspect raw Cypher text.
package cyphertext

import "strings"

// IsQuote reports whether character starts a Cypher string literal or quoted
// symbolic name.
func IsQuote(character byte) bool {
	return character == '\'' || character == '"' || character == '`'
}

// IsStringQuote reports whether character starts a Cypher string literal.
func IsStringQuote(character byte) bool { return character == '\'' || character == '"' }

// IsSingleQuote reports whether character is Cypher's single-string delimiter.
func IsSingleQuote(character byte) bool { return character == '\'' }

// IsDoubleQuote reports whether character is Cypher's double-string delimiter.
func IsDoubleQuote(character byte) bool { return character == '"' }

// IsBacktick reports whether character starts a quoted symbolic name.
func IsBacktick(character byte) bool { return character == '`' }

// ScanQuotedText returns the byte after the quoted token at start. Doubled
// delimiters escape every quote kind; backslash escapes apply to strings but
// not backtick symbolic names.
func ScanQuotedText(text string, start int) (int, bool) {
	end, _, closed := scanQuotedText(text, start, true, nil)
	return end, closed
}

// ScanStringToken preserves adjacent string literals as separate tokens for
// syntax-cache keys. It also reports backslash escapes that affect validity.
func ScanStringToken(text string, start int) (int, bool, bool) {
	if start < 0 || start >= len(text) || !IsStringQuote(text[start]) {
		return start, false, false
	}
	return scanQuotedText(text, start, false, nil)
}

// ScanInterpolatedText skips expression bodies before looking for the outer
// string terminator. expressionEnd owns the expression's structural grammar.
func ScanInterpolatedText(text string, start int, expressionEnd func(int) int) (int, bool) {
	end, _, closed := scanQuotedText(text, start, false, expressionEnd)
	return end, closed
}

func scanQuotedText(text string, start int, doubled bool, expressionEnd func(int) int) (int, bool, bool) {
	if start < 0 || start >= len(text) || !IsQuote(text[start]) {
		return start, false, false
	}
	quote := text[start]
	hasEscape := false
	for index := start + 1; index < len(text); index++ {
		if text[index] == '\\' && quote != '`' && index+1 < len(text) {
			hasEscape = true
			index++
			continue
		}
		if expressionEnd != nil && text[index] == '{' {
			end := expressionEnd(index)
			if end < index {
				return len(text), hasEscape, false
			}
			index = end
			continue
		}
		if text[index] != quote {
			continue
		}
		if doubled && index+1 < len(text) && text[index+1] == quote {
			index++
			continue
		}
		return index + 1, hasEscape, true
	}
	return len(text), hasEscape, false
}

// CommentEnd returns the end of the Cypher comment at index, or -1.
func CommentEnd(text string, index int) int {
	if index+1 >= len(text) || text[index] != '/' {
		return -1
	}
	switch text[index+1] {
	case '/':
		end := index + 2
		for end < len(text) && text[end] != '\n' && text[end] != '\r' {
			end++
		}
		return end
	case '*':
		if close := strings.Index(text[index+2:], "*/"); close >= 0 {
			return index + 2 + close + 2
		}
		return len(text)
	}
	return -1
}

// SkipToken skips one quoted token or comment at index.
func SkipToken(text string, index int) (int, bool) {
	if index < 0 || index >= len(text) {
		return index, false
	}
	if IsQuote(text[index]) {
		end, _ := ScanQuotedText(text, index)
		return end, true
	}
	if end := CommentEnd(text, index); end >= 0 {
		return end, true
	}
	return index, false
}

// SkipTokenRunes is SkipToken for callers that already decode the query to
// runes for Unicode normalization.
func SkipTokenRunes(text []rune, index int) (int, bool) {
	if index < 0 || index >= len(text) {
		return index, false
	}
	if text[index] == '\'' || text[index] == '"' || text[index] == '`' {
		quote := text[index]
		for end := index + 1; end < len(text); end++ {
			if text[end] == '\\' && quote != '`' && end+1 < len(text) {
				end++
				continue
			}
			if text[end] != quote {
				continue
			}
			if end+1 < len(text) && text[end+1] == quote {
				end++
				continue
			}
			return end + 1, true
		}
		return len(text), true
	}
	if index+1 < len(text) && text[index] == '/' {
		switch text[index+1] {
		case '/':
			end := index + 2
			for end < len(text) && text[end] != '\n' && text[end] != '\r' {
				end++
			}
			return end, true
		case '*':
			for end := index + 2; end+1 < len(text); end++ {
				if text[end] == '*' && text[end+1] == '/' {
					return end + 2, true
				}
			}
			return len(text), true
		}
	}
	return index, false
}
