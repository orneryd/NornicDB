package cypher

import (
	"regexp"
)

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
			position = skipCypherQuotedTextWithCancellation(s, position, s[position], probe) - 1
		case '/':
			if position+1 < len(s) && (s[position+1] == '/' || s[position+1] == '*') {
				position = queryCommentEndWithCancellation(s, position, probe) - 1
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

// extractProcedureName extracts the procedure name from a CALL statement for error messages.
// callProcedureNamePattern matches CALL followed by a procedure name
// ("CALL db.labels()" -> "db.labels"). It is compiled once: authorization
// looks up the procedure of every statement.
var callProcedureNamePattern = regexp.MustCompile(`(?i)CALL\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`)

func extractProcedureName(cypher string) string {
	matches := callProcedureNamePattern.FindStringSubmatch(cypher)
	if len(matches) > 1 {
		return matches[1]
	}
	// Fallback: return truncated query
	if len(cypher) > 60 {
		return cypher[:60] + "..."
	}
	return cypher
}
