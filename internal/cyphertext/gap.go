package cyphertext

import (
	"unicode"
	"unicode/utf8"
)

// WhitespaceAt and GapEnd are the existing statement gap phase. Connection
// frames and query normalization must not disagree about Unicode/comments.
var whitespaceClasses = func() (classes [256]byte) {
	for _, character := range " \t\n\r\f\v" {
		classes[character] = 1
	}
	for index := 128; index < len(classes); index++ {
		classes[index] = 2 // Needs Unicode classification, not a byte width.
	}
	return classes
}()

// spaceEnd is the whitespace-only phase, without GapEnd's comment policy.
// Keep the usual adjacent-token case separate from the whitespace loop.
func spaceEnd(text string, at int) int {
	if at < len(text) && whitespaceClasses[text[at]] != 0 {
		return spaceEndSlow(text, at)
	}
	return at
}

func spaceEndSlow(text string, at int) int {
	for at < len(text) {
		size := WhitespaceAt(text, at)
		if size == 0 {
			break
		}
		at += size
	}
	return at
}

func WhitespaceAt(query string, index int) int {
	if c := query[index]; c < utf8.RuneSelf {
		return int(whitespaceClasses[c])
	}
	return unicodeWhitespaceAt(query, index)
}

func unicodeWhitespaceAt(query string, index int) int {
	r, width := utf8.DecodeRuneInString(query[index:])
	if unicode.IsSpace(r) {
		return width
	}
	return 0
}

func GapEnd(query string, index int) int {
	end := index
	for end < len(query) {
		if query[end] == '/' {
			if commentEnd := CommentEnd(query, end); commentEnd >= 0 {
				end = commentEnd
				continue
			}
		}
		size := WhitespaceAt(query, end)
		if size == 0 {
			break
		}
		end += size
	}
	return end
}
