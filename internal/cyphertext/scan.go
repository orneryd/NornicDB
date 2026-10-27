// Package cyphertext owns quotation and comment scanning shared by Cypher
// execution and adapters that must inspect raw Cypher text.
package cyphertext

import (
	"strings"
	"unicode/utf8"
)

// IsQuote reports whether character starts a Cypher string literal or quoted
// symbolic name.
func IsQuote(character byte) bool {
	return character == '\'' || character == '"' || character == '`'
}

// TrimQuoteWrapper removes a matching outer delimiter around a text span.
// Internal URL apostrophes remain data; this does not scan a Cypher token.
func TrimQuoteWrapper(text string, start, end int) int {
	if start > 0 && end > start {
		quote := text[start-1]
		if IsQuote(quote) && text[end-1] == quote {
			return end - 1
		}
	}
	return end
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
// backticks escape symbolic-name delimiters; backslash escapes apply to
// strings but not backtick symbolic names.
func ScanQuotedText(text string, start int) (int, bool) {
	end, _, closed := scanQuotedText(text, start, true, nil)
	return end, closed
}

// UnquoteBacktick returns the name inside one complete backtick token.
func UnquoteBacktick(text string) (string, bool) {
	if len(text) < 2 || !IsBacktick(text[0]) {
		return "", false
	}
	end, closed := ScanQuotedText(text, 0)
	if !closed || end != len(text) {
		return "", false
	}
	return strings.ReplaceAll(text[1:len(text)-1], "``", "`"), true
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
		if doubled && quote == '`' && index+1 < len(text) && text[index+1] == quote {
			index++
			continue
		}
		return index + 1, hasEscape, true
	}
	return len(text), hasEscape, false
}

// HasAdjacentStringLiterals reports two string tokens separated only by
// whitespace or comments. Cypher strings escape their delimiter with a
// backslash; unlike backtick symbolic names, doubled quote bytes close one
// string and start another.
func HasAdjacentStringLiterals(text string) bool {
	previousString := false
	for index := 0; index < len(text); {
		if end := CommentEnd(text, index); end >= 0 {
			index = end
			continue
		}
		if IsStringQuote(text[index]) {
			if previousString {
				return true
			}
			end, _, closed := ScanStringToken(text, index)
			if !closed {
				return false
			}
			index = end
			previousString = true
			continue
		}
		if IsBacktick(text[index]) {
			end, _ := ScanQuotedText(text, index)
			index = end
			previousString = false
			continue
		}
		if IsWhitespaceAt(text, index) {
			index++
		} else {
			previousString = false
			index++
		}
	}
	return false
}

// CommentEnd returns the end of the Cypher comment at index, or -1.
func CommentEnd(text string, index int) int {
	end, _ := CommentEndStatus(text, index)
	return end
}

// CommentEndStatus reads a comment through the shared lexical owner and
// reports whether it terminated. A line comment at EOF is complete.
func CommentEndStatus(text string, index int) (int, bool) {
	if index+1 >= len(text) || text[index] != '/' {
		return -1, false
	}
	switch text[index+1] {
	case '/':
		end := index + 2
		for end < len(text) && text[end] != '\n' && text[end] != '\r' {
			end++
		}
		return end, true
	case '*':
		if close := strings.Index(text[index+2:], "*/"); close >= 0 {
			return index + 2 + close + 2, true
		}
		return len(text), false
	}
	return -1, false
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
	suffix := string(text[index:])
	end, opaque := SkipToken(suffix, 0)
	if !opaque {
		return index, false
	}
	return index + utf8.RuneCountInString(suffix[:end]), true
}
