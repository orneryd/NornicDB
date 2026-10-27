package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateUnicodeOperators rejects Unicode punctuation that visually mimics a
// Cypher operator. Such characters remain valid inside strings, escaped
// identifiers, and comments, where they are data rather than query syntax.
// Non-ASCII name characters outside them must be ones Neo4j takes there:
// cyphertext.ValidNameStart where a name starts, cyphertext.ValidNamePart after its
// first character. €a and a· are "Invalid input" as in Neo4j.
func validateUnicodeOperators(query string) error {
	const unicodeMinus = '−'
	if isLikelyPlainASCIICypher(query) {
		return nil
	}

	for offset := 0; offset < len(query); {
		if end, quoted := skipCypherLexicalToken(query, offset); quoted {
			offset = end
			continue
		}

		r, size := utf8.DecodeRuneInString(query[offset:])
		if r == utf8.RuneError && size == 1 {
			return invalidUnicodeCharacterError(r, offset)
		}
		if r != '-' && (unicode.Is(unicode.Dash, r) || r == unicodeMinus) {
			return invalidUnicodeCharacterError(r, offset)
		}
		if r >= utf8.RuneSelf && !cyphertext.IsWhitespace(r) {
			startsName := offset == 0 || !cyphertext.NamePartAt(query, offset-1)
			if startsName && !cyphertext.ValidNameStart(r) || !startsName && !cyphertext.ValidNamePart(r) {
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
// rules validateUnicodeOperators checks. Token boundaries belong to cyphertext.
func isUnquotedName(name string) bool {
	for offset, r := range name {
		if r < utf8.RuneSelf {
			continue
		}
		if offset == 0 && !cyphertext.ValidNameStart(r) || offset > 0 && !cyphertext.ValidNamePart(r) {
			return false
		}
	}
	return true
}

func invalidUnicodeCharacterError(character rune, offset int) error {
	return newSourceOffsetError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidUnicodeCharacter",
		fmt.Sprintf("invalid Unicode character %q (U+%04X) at byte offset ", character, character), offset,
	)
}
