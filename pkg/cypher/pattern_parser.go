// Pattern parsing for NornicDB Cypher.
//
// This file contains functions for parsing Cypher node and relationship patterns.
// Patterns are the core syntax for describing graph structures in queries.
//
// # Pattern Syntax
//
// Node patterns:
//
//	(n)                    - Anonymous node
//	(n:Label)              - Node with label
//	(n:Label {prop: val})  - Node with label and properties
//	(:Label)               - Anonymous node with label
//
// Property patterns:
//
//	{name: 'Alice'}        - String property
//	{age: 30}              - Integer property
//	{active: true}         - Boolean property
//	{tags: ['a', 'b']}     - Array property
//
// # Parsing Process
//
//  1. parseNodePattern - Extract variable, labels, and properties
//  2. parseProperties - Parse {key: value, ...} syntax
//  3. parsePropertyValue - Convert string values to Go types
//  4. parseArrayValue - Handle array literals [1, 2, 3]
//
// # ELI12
//
// Pattern parsing is like reading a recipe:
//
//	"(alice:Person {name: 'Alice', age: 30})"
//
// The parser breaks this down:
//   - Variable: "alice" (what we'll call this in our query)
//   - Label: "Person" (what type of thing it is)
//   - Properties: name='Alice', age=30 (details about it)
//
// It's like reading "the red ball" and understanding:
//   - "ball" is what it is
//   - "red" describes it
//
// # Neo4j Compatibility
//
// Pattern parsing matches Neo4j Cypher syntax exactly for compatibility.

package cypher

import (
	"strconv"
	"strings"
)

// parseNodePattern parses a Cypher node pattern like (n:Label {prop: value}).
//
// # Parameters
//
//   - pattern: The node pattern string (with or without parentheses)
//
// # Returns
//
//   - nodePatternInfo containing variable, labels, and properties
//
// # Example
//
//	parseNodePattern("(n:Person {name: 'Alice'})")
//	// Returns: {variable: "n", labels: ["Person"], properties: {"name": "Alice"}}
//
//	parseNodePattern("(:Employee)")
//	// Returns: {variable: "", labels: ["Employee"], properties: {}}
func (e *StorageExecutor) parseNodePattern(pattern string) nodePatternInfo {
	info := nodePatternInfo{
		labels:     []string{},
		properties: make(map[string]interface{}),
	}

	// Remove outer parens
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(pattern, "(") && strings.HasSuffix(pattern, ")") {
		pattern = pattern[1 : len(pattern)-1]
	}

	// Extract properties
	braceIdx := strings.Index(pattern, "{")
	if braceIdx >= 0 {
		propsStr := pattern[braceIdx:]
		pattern = pattern[:braceIdx]
		info.properties = e.parseProperties(propsStr)
	}

	// Parse variable:Label:Label2
	parts := strings.Split(strings.TrimSpace(pattern), ":")
	if len(parts) > 0 && parts[0] != "" {
		info.variable = strings.TrimSpace(parts[0])
	}
	for i := 1; i < len(parts); i++ {
		if label := strings.TrimSpace(parts[i]); label != "" {
			info.labels = append(info.labels, label)
		}
	}

	return info
}

// parseProperties parses a Cypher property map like {key1: value1, key2: value2}.
//
// # Parameters
//
//   - propsStr: The property map string (with or without braces)
//
// # Returns
//
//   - Map of property names to values (converted to Go types)
//
// # Example
//
//	parseProperties("{name: 'Alice', age: 30}")
//	// Returns: {"name": "Alice", "age": int64(30)}
//
//	parseProperties("{tags: ['a', 'b'], active: true}")
//	// Returns: {"tags": []interface{}{"a", "b"}, "active": true}
func (e *StorageExecutor) parseProperties(propsStr string) map[string]interface{} {
	props := make(map[string]interface{})

	// Remove outer braces
	propsStr = strings.TrimSpace(propsStr)
	if strings.HasPrefix(propsStr, "{") && strings.HasSuffix(propsStr, "}") {
		propsStr = propsStr[1 : len(propsStr)-1]
	}
	propsStr = strings.TrimSpace(propsStr)

	if propsStr == "" {
		return props
	}

	// Parse key-value pairs using a state machine that respects quotes, brackets, and nested structures
	pairs := e.splitPropertyPairs(propsStr)

	for _, pair := range pairs {
		colonIdx := strings.Index(pair, ":")
		if colonIdx <= 0 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		valueStr := strings.TrimSpace(pair[colonIdx+1:])

		// Parse the value
		props[key] = e.parsePropertyValue(valueStr)
	}

	return props
}

// splitPropertyPairs splits a property string into key:value pairs,
// respecting quotes, brackets, and nested braces.
//
// # Parameters
//
//   - propsStr: The property pairs string (without outer braces)
//
// # Returns
//
//   - Slice of "key: value" strings
//
// # Example
//
//	splitPropertyPairs("name: 'Alice', age: 30")
//	// Returns: ["name: 'Alice'", "age: 30"]
//
//	splitPropertyPairs("tags: ['a', 'b'], data: {nested: true}")
//	// Returns: ["tags: ['a', 'b']", "data: {nested: true}"]
func (e *StorageExecutor) splitPropertyPairs(propsStr string) []string {
	var pairs []string
	var current strings.Builder
	depth := 0 // Track [], {} nesting
	inQuote := false
	quoteChar := rune(0)

	for i, c := range propsStr {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote (look back for \)
				escaped := false
				if i > 0 {
					// Count consecutive backslashes before this quote
					backslashes := 0
					for j := i - 1; j >= 0 && propsStr[j] == '\\'; j-- {
						backslashes++
					}
					escaped = backslashes%2 == 1
				}
				if !escaped {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '[' || c == '{' || c == '(') && !inQuote:
			depth++
			current.WriteRune(c)
		case (c == ']' || c == '}' || c == ')') && !inQuote:
			depth--
			current.WriteRune(c)
		case c == ',' && !inQuote && depth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				pairs = append(pairs, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	// Add final pair
	if s := strings.TrimSpace(current.String()); s != "" {
		pairs = append(pairs, s)
	}

	return pairs
}

// parsePropertyValue parses a single property value string into the appropriate Go type.
//
// Supported types:
//   - null → nil
//   - 'string' or "string" → string
//   - true/false → bool
//   - 123 → int64
//   - 1.23 → float64
//   - [1, 2, 3] → []interface{}
//   - {key: value} → map[string]interface{}
//   - function() → evaluated result
//
// # Parameters
//
//   - valueStr: The value string to parse
//
// # Returns
//
//   - The parsed Go value
//
// # Example
//
//	parsePropertyValue("'Alice'")  // "Alice"
//	parsePropertyValue("30")       // int64(30)
//	parsePropertyValue("true")     // true
//	parsePropertyValue("[1, 2]")   // []interface{}{int64(1), int64(2)}
func (e *StorageExecutor) parsePropertyValue(valueStr string) interface{} {
	valueStr = strings.TrimSpace(valueStr)

	if valueStr == "" {
		return nil
	}

	// Handle null
	if strings.EqualFold(valueStr, "null") {
		return nil
	}

	// Handle quoted strings
	if len(valueStr) >= 2 {
		first, last := valueStr[0], valueStr[len(valueStr)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			// Unescape the string content
			content := valueStr[1 : len(valueStr)-1]
			// Handle escaped quotes
			if first == '\'' {
				content = strings.ReplaceAll(content, "''", "'")
			} else {
				content = strings.ReplaceAll(content, "\\\"", "\"")
			}
			content = strings.ReplaceAll(content, "\\\\", "\\")
			return content
		}
	}

	// Handle booleans
	lowerVal := strings.ToLower(valueStr)
	if lowerVal == "true" {
		return true
	}
	if lowerVal == "false" {
		return false
	}

	// Handle integers
	if intVal, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return intVal
	}

	// Handle floats
	if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal
	}

	// Handle arrays
	if strings.HasPrefix(valueStr, "[") && strings.HasSuffix(valueStr, "]") {
		return e.parseArrayValue(valueStr)
	}

	// Handle nested maps (rare in properties, but possible)
	if strings.HasPrefix(valueStr, "{") && strings.HasSuffix(valueStr, "}") {
		return e.parseProperties(valueStr)
	}

	// Handle function calls like kalman.init(), toUpper('test'), etc.
	// A function call has the pattern: name(...) or name.sub.name(...)
	if looksLikeFunctionCall(valueStr) {
		result := e.evaluateExpressionWithContext(valueStr, nil, nil)
		// Only use the result if evaluation succeeded (not returned as original string)
		if result != nil && result != valueStr {
			return result
		}
	}

	// Otherwise return as string (handles unquoted identifiers, etc.)
	return valueStr
}

// parseArrayValue parses a Cypher array literal like [1, 2, 3] or ['a', 'b', 'c'].
//
// # Parameters
//
//   - arrayStr: The array literal string (with brackets)
//
// # Returns
//
//   - Slice of parsed values
//
// # Example
//
//	parseArrayValue("[1, 2, 3]")      // []interface{}{int64(1), int64(2), int64(3)}
//	parseArrayValue("['a', 'b']")    // []interface{}{"a", "b"}
//	parseArrayValue("[[1], [2]]")    // []interface{}{[]interface{}{1}, []interface{}{2}}
func (e *StorageExecutor) parseArrayValue(arrayStr string) []interface{} {
	// Remove brackets
	inner := strings.TrimSpace(arrayStr[1 : len(arrayStr)-1])
	if inner == "" {
		return []interface{}{}
	}

	// Split array elements respecting nested structures
	elements := e.splitArrayElements(inner)
	result := make([]interface{}, len(elements))

	for i, elem := range elements {
		result[i] = e.parsePropertyValue(strings.TrimSpace(elem))
	}

	return result
}

// splitArrayElements splits array contents by comma, respecting nested structures and quotes.
//
// # Parameters
//
//   - inner: The array contents (without brackets)
//
// # Returns
//
//   - Slice of element strings
//
// # Example
//
//	splitArrayElements("1, 2, 3")            // ["1", "2", "3"]
//	splitArrayElements("'a,b', 'c'")         // ["'a,b'", "'c'"]
//	splitArrayElements("[1, 2], [3, 4]")     // ["[1, 2]", "[3, 4]"]
func (e *StorageExecutor) splitArrayElements(inner string) []string {
	var elements []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, c := range inner {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				escaped := false
				if i > 0 && inner[i-1] == '\\' {
					escaped = true
				}
				if !escaped {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '[' || c == '{') && !inQuote:
			depth++
			current.WriteRune(c)
		case (c == ']' || c == '}') && !inQuote:
			depth--
			current.WriteRune(c)
		case c == ',' && !inQuote && depth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				elements = append(elements, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		elements = append(elements, s)
	}

	return elements
}

// looksLikeFunctionCall checks if a string looks like a function call.
//
// A function call matches: identifier(...) or namespace.function(...)
//
// # Parameters
//
//   - s: The string to check
//
// # Returns
//
//   - true if the string looks like a function call
//
// # Example
//
//	looksLikeFunctionCall("toUpper('test')")     // true
//	looksLikeFunctionCall("apoc.coll.sum([1])")  // true
//	looksLikeFunctionCall("'not a function'")    // false
//	looksLikeFunctionCall("x + y")               // false
func looksLikeFunctionCall(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	// Must end with )
	if !strings.HasSuffix(s, ")") {
		return false
	}

	// Find the opening parenthesis
	parenIdx := strings.Index(s, "(")
	if parenIdx <= 0 {
		return false
	}

	// The part before ( must be a valid identifier (possibly with dots for namespacing)
	name := s[:parenIdx]

	// Allow dots for namespaced functions like apoc.coll.sum
	for i, c := range name {
		if i == 0 {
			// First char must be letter or underscore
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			// Subsequent chars can be alphanumeric, underscore, or dot
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
				return false
			}
		}
	}

	return true
}
