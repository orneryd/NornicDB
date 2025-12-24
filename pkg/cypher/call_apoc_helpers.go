package cypher

import (
	"fmt"
	"regexp"
	"strings"
)

// =============================================================================
// APOC Helper Functions
// =============================================================================

// findMatchingParen finds the index of the closing parenthesis matching the one at startIdx.
func (e *StorageExecutor) findMatchingParen(s string, startIdx int) int {
	if startIdx >= len(s) || s[startIdx] != '(' {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := startIdx; i < len(s); i++ {
		c := rune(s[i])

		if inQuote {
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
		case '(':
			depth++
		case ')':
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
func (e *StorageExecutor) parseApocCypherRunArgs(argsStr string) (string, map[string]interface{}, error) {
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
		if rune(argsStr[i]) == quoteChar && (i == 0 || argsStr[i-1] != '\\') {
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
				params = e.parseMapLiteral(mapStr)
			}
		}
	}

	return query, params, nil
}

// parseApocPeriodicIterateArgs parses arguments for apoc.periodic.iterate.
// Expected format: 'iterateQuery', 'actionQuery', {config}
func (e *StorageExecutor) parseApocPeriodicIterateArgs(argsStr string) (string, string, map[string]interface{}, error) {
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
				config = e.parseMapLiteral(mapStr)
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
		if rune(s[i]) == quoteChar && (i == 1 || s[i-1] != '\\') {
			return s[1:i], s[i+1:], nil
		}
	}

	return "", "", fmt.Errorf("unclosed quote")
}

// findMatchingBrace finds the index of the closing brace matching the one at startIdx.
func (e *StorageExecutor) findMatchingBrace(s string, startIdx int) int {
	if startIdx >= len(s) || s[startIdx] != '{' {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := startIdx; i < len(s); i++ {
		c := rune(s[i])

		if inQuote {
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// parseMapLiteral parses a Cypher map literal like {key: value, key2: value2}.
func (e *StorageExecutor) parseMapLiteral(s string) map[string]interface{} {
	result := make(map[string]interface{})

	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return result
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return result
	}

	// Simple key:value parsing (handles basic cases)
	pairs := e.splitMapPairs(inner)
	for _, pair := range pairs {
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		value := strings.TrimSpace(pair[colonIdx+1:])

		// Parse value
		result[key] = e.parseValue(value)
	}

	return result
}

// splitMapPairs splits a map literal's contents by commas, respecting nesting.
func (e *StorageExecutor) splitMapPairs(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range s {
		if inQuote {
			current.WriteRune(c)
			if c == quoteChar {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
			current.WriteRune(c)
		case '{', '[', '(':
			depth++
			current.WriteRune(c)
		case '}', ']', ')':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
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
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
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
func extractProcedureName(cypher string) string {
	// Match CALL followed by procedure name (e.g., "CALL db.labels()" -> "db.labels")
	re := regexp.MustCompile(`(?i)CALL\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`)
	matches := re.FindStringSubmatch(cypher)
	if len(matches) > 1 {
		return matches[1]
	}
	// Fallback: return truncated query
	if len(cypher) > 60 {
		return cypher[:60] + "..."
	}
	return cypher
}
