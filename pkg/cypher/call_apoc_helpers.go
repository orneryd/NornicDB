package cypher

// =============================================================================
// APOC Helper Functions
// =============================================================================

// findMatchingParen finds the index of the closing parenthesis matching the one at startIdx.
func (e *StorageExecutor) findMatchingParen(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '(', ')')
}

// findMatchingBrace finds the index of the closing brace matching the one at startIdx.
func (e *StorageExecutor) findMatchingBrace(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '{', '}')
}

// splitBySemicolon splits a string by semicolons, respecting quotes.
func (e *StorageExecutor) splitBySemicolon(s string) []string {
	return e.splitBySemicolonWithCancellation(s, nil)
}
func (e *StorageExecutor) splitBySemicolonWithCancellation(s string, probe *pipelineCancellation) []string {
	var result []string
	start := 0
	for position := 0; position < len(s); position++ {
		if probe != nil && !probe.check() {
			return nil
		}
		switch s[position] {
		case '\'', '"', '`':
			position = skipCypherQuotedText(s, position, s[position]) - 1
		case '/':
			if position+1 < len(s) && (s[position+1] == '/' || s[position+1] == '*') {
				position = queryCommentEnd(s, position) - 1
			}
		case ';':
			result = appendPipelineItem(result, s[start:position], probe)
			start = position + 1
		}
	}
	if start < len(s) {
		result = appendPipelineItem(result, s[start:], probe)
	}
	return result
}

// extractProcedureName uses the same component reader as authorization,
// retaining exact case while accepting quoted names and spaced separators.
func extractProcedureName(cypher string) string {
	if at := keywordIndexFrom(cypher, "CALL", 0, defaultKeywordScanOpts()); at >= 0 {
		if name, ok := procedureNameAt(cypher, at+len("CALL")); ok {
			return name
		}
	}
	// Fallback: return truncated query
	if len(cypher) > 60 {
		return cypher[:60] + "..."
	}
	return cypher
}
