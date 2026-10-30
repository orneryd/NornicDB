package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// =============================================================================
// APOC Helper Functions
// =============================================================================

// findMatchingParen finds the index of the closing parenthesis matching the one at startIdx.
func (e *StorageExecutor) findMatchingParen(s string, startIdx int) int {
	return cyphertext.FindMatchingDelimiter(s, startIdx, '(', ')', cyphertext.OpeningDelimiter)
}

// findMatchingBrace finds the index of the closing brace matching the one at startIdx.
func (e *StorageExecutor) findMatchingBrace(s string, startIdx int) int {
	return cyphertext.FindMatchingDelimiter(s, startIdx, '{', '}', cyphertext.OpeningDelimiter)
}

// splitBySemicolon splits a string by semicolons, respecting quotes.
func (e *StorageExecutor) splitBySemicolon(s string) []string {
	var result []string
	start := 0
	for position := 0; position < len(s); position++ {
		switch cypherLexicalByte(s[position]) {
		case cypherQuoteClass:
			position = skipCypherQuotedText(s, position, s[position]) - 1
		case '/':
			if position+1 < len(s) && (s[position+1] == '/' || s[position+1] == '*') {
				position = queryCommentEnd(s, position) - 1
			}
		case ';':
			result = append(result, s[start:position])
			start = position + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

