package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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

// extractProcedureName uses the same component reader as authorization,
// retaining exact case while accepting quoted names and spaced separators.
func extractProcedureName(ctx context.Context, source string) string {
	syntax, _ := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if !syntax.Valid() {
		return ""
	}
	call, found := syntax.LeadingCall()
	if !found {
		return ""
	}
	invocation := call.ChildRule(cypherantlr.CypherParserRULE_invocationName)
	if !invocation.Valid() {
		return ""
	}
	return invocation.QualifiedName()
}
