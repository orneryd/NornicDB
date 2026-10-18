package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode/utf8"
)

func isWholeCypherQuotedString(raw string) bool { return cyphertext.IsWholeString(raw) }

func decodeCypherQuotedString(raw string) (string, bool) { return cyphertext.DecodeString(raw) }

func validateUnicodeStringLiterals(query string) error {
	for index := 0; index < len(query); {
		if index+1 < len(query) && query[index] == '/' {
			if query[index+1] == '/' {
				index += 2
				for index < len(query) && query[index] != '\n' && query[index] != '\r' {
					index++
				}
				continue
			}
			if query[index+1] == '*' {
				index += 2
				for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
					index++
				}
				if index+1 < len(query) {
					index += 2
				}
				continue
			}
		}
		quote := query[index]
		if quote != '\'' && quote != '"' && quote != '`' {
			index++
			continue
		}
		index++
		for index < len(query) {
			if query[index] == quote {
				if index+1 < len(query) && query[index+1] == quote {
					index += 2
					continue
				}
				index++
				break
			}
			if query[index] != '\\' || quote == '`' {
				index++
				continue
			}
			escapeOffset := index
			index++
			if index >= len(query) {
				break
			}
			if query[index] != 'u' && query[index] != 'U' {
				index++
				continue
			}
			width := 4
			if query[index] == 'U' {
				width = 8
			}
			value, ok := parseUnicodeEscape(query, index+1, width)
			if !ok || !utf8.ValidRune(rune(value)) || value >= 0xD800 && value <= 0xDFFF {
				return invalidUnicodeLiteralError(escapeOffset)
			}
			index += 1 + width
		}
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
