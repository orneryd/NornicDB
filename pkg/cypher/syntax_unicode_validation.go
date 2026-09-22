package cypher

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// validateUnicodeOperators rejects Unicode punctuation that visually mimics a
// Cypher operator. Such characters remain valid inside strings, escaped
// identifiers, and comments, where they are data rather than query syntax.
func validateUnicodeOperators(query string) error {
	const unicodeMinus = '−'
	if isLikelyPlainASCIICypher(query) {
		return nil
	}

	var quote byte
	inLineComment := false
	inBlockComment := false
	for offset := 0; offset < len(query); {
		if inLineComment {
			if query[offset] == '\n' || query[offset] == '\r' {
				inLineComment = false
			}
			offset++
			continue
		}
		if inBlockComment {
			if query[offset] == '*' && offset+1 < len(query) && query[offset+1] == '/' {
				inBlockComment = false
				offset += 2
				continue
			}
			_, size := utf8.DecodeRuneInString(query[offset:])
			offset += size
			continue
		}
		if quote != 0 {
			if query[offset] == '\\' {
				offset++
				if offset < len(query) {
					_, size := utf8.DecodeRuneInString(query[offset:])
					offset += size
				}
				continue
			}
			if query[offset] == quote {
				if offset+1 < len(query) && query[offset+1] == quote {
					offset += 2
					continue
				}
				quote = 0
				offset++
				continue
			}
			_, size := utf8.DecodeRuneInString(query[offset:])
			offset += size
			continue
		}

		if offset+1 < len(query) {
			switch query[offset : offset+2] {
			case "//":
				inLineComment = true
				offset += 2
				continue
			case "/*":
				inBlockComment = true
				offset += 2
				continue
			}
		}
		switch query[offset] {
		case '\'', '"', '`':
			quote = query[offset]
			offset++
			continue
		}

		r, size := utf8.DecodeRuneInString(query[offset:])
		if r == utf8.RuneError && size == 1 {
			return invalidUnicodeCharacterError(r, offset)
		}
		if r != '-' && (unicode.Is(unicode.Dash, r) || r == unicodeMinus) {
			return invalidUnicodeCharacterError(r, offset)
		}
		offset += size
	}
	return nil
}

func invalidUnicodeCharacterError(character rune, offset int) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidUnicodeCharacter",
		fmt.Sprintf("invalid Unicode character %q (U+%04X) at byte offset %d", character, character, offset),
	)
}
