package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

func isWholeCypherQuotedString(raw string) bool { return cyphertext.IsWholeString(raw) }

func decodeCypherQuotedString(raw string) (string, bool) { return cyphertext.DecodeString(raw) }

func validateUnicodeStringLiterals(query string) error {
	if offset, invalid := cyphertext.InvalidUnicodeEscape(query); invalid {
		return invalidUnicodeLiteralError(offset)
	}
	return nil
}
func parseUnicodeEscape(text string, start, width int) (uint32, bool) {
	return cyphertext.ParseUnicodeEscape(text, start, width)
}

func invalidUnicodeLiteralError(offset int) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidUnicodeLiteral",
		fmt.Sprintf("invalid Unicode escape at byte offset %d", offset),
	)
}
