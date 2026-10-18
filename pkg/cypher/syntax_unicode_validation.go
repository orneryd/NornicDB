package cypher

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateUnicodeOperators rejects Unicode punctuation that visually mimics a
// Cypher operator. Such characters remain valid inside strings, escaped
// identifiers, and comments, where they are data rather than query syntax.
// Every other non-ASCII character outside them is part of a name (the
// scanners read it as one, isIdentByte), so it must be one Neo4j takes there:
// isIdentifierStartRune where a name starts, isIdentifierPartRune after its
// first character. €a and a· are "Invalid input" as in Neo4j.
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
		if r >= utf8.RuneSelf {
			startsName := offset == 0 || !isIdentByte(query[offset-1])
			if startsName && !isIdentifierStartRune(r) || !startsName && !isIdentifierPartRune(r) {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
					localization.CypherCoreInvalidInput(string(r)))
			}
		}
		offset += size
	}
	return nil
}

// isUnquotedName reports whether name's characters can be written without
// backticks in Neo4j (`a—b` must stay quoted; ñ, éa and a€ need not), by the
// rules validateUnicodeOperators checks. ASCII is isIdentStartByte /
// isIdentByte's business.
func isUnquotedName(name string) bool {
	for offset, r := range name {
		if r < utf8.RuneSelf {
			continue
		}
		if offset == 0 && !isIdentifierStartRune(r) || offset > 0 && !isIdentifierPartRune(r) {
			return false
		}
	}
	return true
}

// isIdentifierStartRune reports whether a non-ASCII character can start an
// unquoted name in Neo4j 5.26: a letter, a letter number (Ⅰ) or a
// connector punctuation (‿), not a digit, mark or currency sign.
func isIdentifierStartRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.In(r, unicode.Nl, unicode.Pc)
}

// isIdentifierPartRune reports whether a non-ASCII character can continue an
// unquoted name in Neo4j 5.26 (Java's identifier part): a start character, a
// digit, a combining mark, a currency sign (a€), a format character, or a
// C1 control.
func isIdentifierPartRune(r rune) bool {
	return isIdentifierStartRune(r) || unicode.In(r, unicode.Nd, unicode.Mn, unicode.Mc, unicode.Sc, unicode.Cf) ||
		r >= 0x80 && r <= 0x9F
}

func invalidUnicodeCharacterError(character rune, offset int) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidUnicodeCharacter",
		fmt.Sprintf("invalid Unicode character %q (U+%04X) at byte offset %d", character, character, offset),
	)
}
