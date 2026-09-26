package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// =============================================================================
// APOC Helper Functions
// =============================================================================

// findMatchingParen finds the index of the closing parenthesis matching the one at startIdx.
func (e *StorageExecutor) findMatchingParen(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '(', ')')
}

// findMatchingDelimiter finds the index of the closer matching the opener at startIdx,
// skipping quoted strings. It returns -1 when s[startIdx] is not the opener or no
// matching closer exists.
func findMatchingDelimiter(s string, startIdx int, opener, closer rune) int {
	if startIdx >= len(s) || rune(s[startIdx]) != opener {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := startIdx; i < len(s); i++ {
		c := rune(s[i])

		if inQuote {
			if c == quoteChar && !isBackslashEscaped(s, i) {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
		case opener:
			depth++
		case closer:
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// parseApocCypherRunArgs parses the arguments for apoc.cypher.run/runMany.
// Expected format: 'query string', {params} or 'query string', null
func (e *StorageExecutor) parseApocCypherRunArgs(ctx context.Context, argsStr string) (string, map[string]interface{}, error) {
	// Find the first quoted string (the query)
	query := ""
	params := make(map[string]interface{})

	// Find first quote
	quoteStart := -1
	quoteChar := rune(0)
	for i, c := range argsStr {
		if c == '\'' || c == '"' {
			quoteStart = i
			quoteChar = c
			break
		}
	}

	if quoteStart == -1 {
		return "", nil, fmt.Errorf("query string not found")
	}

	// Find matching closing quote
	quoteEnd := -1
	for i := quoteStart + 1; i < len(argsStr); i++ {
		if rune(argsStr[i]) == quoteChar && !isBackslashEscaped(argsStr, i) {
			quoteEnd = i
			break
		}
	}

	if quoteEnd == -1 {
		return "", nil, fmt.Errorf("unclosed query string")
	}

	query = argsStr[quoteStart+1 : quoteEnd]

	// Try to parse params after the query
	remaining := strings.TrimSpace(argsStr[quoteEnd+1:])
	if strings.HasPrefix(remaining, ",") {
		remaining = strings.TrimSpace(remaining[1:])

		// Skip 'null' or 'NULL'
		if len(remaining) >= 4 && strings.EqualFold(remaining[:4], "NULL") {
			return query, params, nil
		}

		// Try to parse as map literal {...}
		if strings.HasPrefix(remaining, "{") {
			mapEnd := e.findMatchingBrace(remaining, 0)
			if mapEnd > 0 {
				mapStr := remaining[:mapEnd+1]
				params = e.parseMapLiteral(ctx, mapStr)
			}
		}
	}

	return query, params, nil
}

// parseApocPeriodicIterateArgs parses arguments for apoc.periodic.iterate.
// Expected format: 'iterateQuery', 'actionQuery', {config}
func (e *StorageExecutor) parseApocPeriodicIterateArgs(ctx context.Context, argsStr string) (string, string, map[string]interface{}, error) {
	config := make(map[string]interface{})

	// Parse first query string
	iterateQuery, remaining, err := e.extractQuotedString(argsStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to parse iterate query: %w", err)
	}

	// Skip comma
	remaining = strings.TrimSpace(remaining)
	if !strings.HasPrefix(remaining, ",") {
		return "", "", nil, fmt.Errorf("expected comma after iterate query")
	}
	remaining = strings.TrimSpace(remaining[1:])

	// Parse second query string
	actionQuery, remaining, err := e.extractQuotedString(remaining)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to parse action query: %w", err)
	}

	// Try to parse config map
	remaining = strings.TrimSpace(remaining)
	if strings.HasPrefix(remaining, ",") {
		remaining = strings.TrimSpace(remaining[1:])
		if strings.HasPrefix(remaining, "{") {
			mapEnd := e.findMatchingBrace(remaining, 0)
			if mapEnd > 0 {
				mapStr := remaining[:mapEnd+1]
				config = e.parseMapLiteral(ctx, mapStr)
			}
		}
	}

	return iterateQuery, actionQuery, config, nil
}

// extractQuotedString extracts a quoted string from the start of s and returns it along with the remaining string.
func (e *StorageExecutor) extractQuotedString(s string) (string, string, error) {
	s = strings.TrimSpace(s)

	if len(s) == 0 {
		return "", "", fmt.Errorf("empty string")
	}

	quoteChar := rune(s[0])
	if quoteChar != '\'' && quoteChar != '"' {
		return "", "", fmt.Errorf("expected quote, got %c", quoteChar)
	}

	// Find matching closing quote
	for i := 1; i < len(s); i++ {
		if rune(s[i]) == quoteChar && !isBackslashEscaped(s, i) {
			return s[1:i], s[i+1:], nil
		}
	}

	return "", "", fmt.Errorf("unclosed quote")
}

// findMatchingBrace finds the index of the closing brace matching the one at startIdx.
func (e *StorageExecutor) findMatchingBrace(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '{', '}')
}

// parseMapLiteral parses a Cypher map literal like {key: value, key2: value2}.
func (e *StorageExecutor) parseMapLiteral(ctx context.Context, s string) map[string]interface{} {
	result := make(map[string]interface{})

	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return result
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return result
	}

	// Split map entries while respecting nested collections and quotes.
	pairs := splitTopLevelComma(inner)
	for _, pair := range pairs {
		colonIdx := findTopLevelMapKeyValueSeparator(pair)
		if colonIdx == -1 {
			continue
		}

		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		value := strings.TrimSpace(pair[colonIdx+1:])

		// Parse value
		result[key] = e.parseValue(ctx, value)
	}

	return result
}

// splitBySemicolon splits a string by semicolons, respecting quotes.
func (e *StorageExecutor) splitBySemicolon(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, c := range s {
		if inQuote {
			current.WriteRune(c)
			if c == quoteChar && !isBackslashEscaped(s, i) {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
			current.WriteRune(c)
		case ';':
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
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
