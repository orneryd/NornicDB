// Package cyphertext owns quotation and comment scanning shared by Cypher
// execution and adapters that inspect raw Cypher text.
package cyphertext

import "strings"

// IsQuote reports whether a byte starts a string literal or symbolic name.
func IsQuote(c byte) bool { return c == '\'' || c == '"' || c == '`' }

// ScanQuotedText returns the byte after a quoted token. Doubled delimiters
// escape all quote kinds; backslash escapes apply to strings, not names.
func ScanQuotedText(text string, start int) (int, bool) {
	if start < 0 || start >= len(text) || !IsQuote(text[start]) {
		return start, false
	}
	quote := text[start]
	for index := start + 1; index < len(text); index++ {
		if text[index] == '\\' && quote != '`' && index+1 < len(text) {
			index++
			continue
		}
		if text[index] != quote {
			continue
		}
		if index+1 < len(text) && text[index+1] == quote {
			index++
			continue
		}
		return index + 1, true
	}
	return len(text), false
}

// CommentEnd returns the end of a comment at index, or -1.
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

// SkipToken skips one quoted token or comment, leaving structural grammar
// to its caller.
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
