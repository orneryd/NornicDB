package cyphertext

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// WhitespaceAt returns the byte width of Cypher whitespace at index.
func WhitespaceAt(text string, index int) int {
	if index >= len(text) {
		return 0
	}
	switch c := text[index]; {
	case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
		return 1
	case c >= utf8.RuneSelf:
		r, width := utf8.DecodeRuneInString(text[index:])
		if unicode.IsSpace(r) {
			return width
		}
	}
	return 0
}

// GapEnd returns the end of trivia and the first unfinished comment, or -1.
func GapEnd(text string, index int) (int, int) {
	for index < len(text) {
		if width := WhitespaceAt(text, index); width > 0 {
			index += width
			continue
		}
		end, closed := CommentEndStatus(text, index)
		if end < 0 {
			break
		}
		if !closed {
			return end, index
		}
		index = end
	}
	return index, -1
}

// FindOutside searches outside quoted and comment spans, retaining lexical
// comment failure rather than silently discarding malformed source.
func FindOutside(text, needle string) (bool, int) {
	if needle == "" {
		return false, -1
	}
	for index := 0; index < len(text); {
		if IsQuote(text[index]) {
			end, _ := ScanQuotedText(text, index)
			index = end
			continue
		}
		if end, closed := CommentEndStatus(text, index); end >= 0 {
			if !closed {
				return false, index
			}
			index = end
			continue
		}
		if strings.HasPrefix(text[index:], needle) {
			return true, -1
		}
		index++
	}
	return false, -1
}
