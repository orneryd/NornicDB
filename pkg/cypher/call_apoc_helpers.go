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
//
// The CALL fragment's parse error is the reader's error, not an absent name: a
// caller whose context ended between the statement's own admission and this
// read (a cancellation released at the statement's transaction BEGIN, for
// example) must report that cancellation, not look up the empty name a failed
// parse leaves behind. Callers that only classify a clause keep their
// established answer for a clause they could not read; the owner that runs the
// CALL reports the error.
func extractProcedureName(ctx context.Context, source string) (string, error) {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return "", err
	}
	if !syntax.Valid() {
		return "", nil
	}
	call, found := syntax.LeadingCall()
	if !found {
		return "", nil
	}
	invocation := call.ChildRule(cypherantlr.CypherParserRULE_invocationName)
	if !invocation.Valid() {
		return "", nil
	}
	return invocation.QualifiedName(), nil
}
