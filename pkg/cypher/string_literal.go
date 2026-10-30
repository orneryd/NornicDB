package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

func isWholeCypherQuotedString(raw string) bool { return cyphertext.IsWholeString(raw) }

func parseUnicodeEscape(text string, start, width int) (uint32, bool) {
	return cyphertext.ParseUnicodeEscape(text, start, width)
}

func invalidUnicodeLiteralError(offset int) error {
	return newSourceOffsetError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidUnicodeLiteral",
		"invalid Unicode escape at byte offset ", offset,
	)
}
