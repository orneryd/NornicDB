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
	"context"
	"strings"
)

// containsReservedKeyword checks if a string contains Cypher reserved keywords
// or special characters that could indicate an injection attempt.
// Only matches WHOLE keywords (not substrings like "OR" in "Order").
func containsReservedKeyword(s string) bool {
	// Check for special characters that shouldn't be in identifiers
	dangerousChars := []string{"--", "//", "/*", "*/", ";", "`", "\"", "'", "(", ")", "[", "]", "{", "}", ","}
	for _, ch := range dangerousChars {
		if strings.Contains(s, ch) {
			return true
		}
	}
	// Only check for space (indicates multiple tokens which is invalid for identifiers)
	if strings.Contains(s, " ") {
		return true
	}
	return false
}

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
//	parseNodePattern(ctx, "(n:Person {name: 'Alice'})")
//	// Returns: {variable: "n", labels: ["Person"], properties: {"name": "Alice"}}
//
//	parseNodePattern(ctx, "(:Employee)")
//	// Returns: {variable: "", labels: ["Employee"], properties: {}}
func (e *StorageExecutor) parseNodePattern(ctx context.Context, pattern string) (nodePatternInfo, error) {
	return readNodePattern(ctx, pattern, e.parsePropertyMap)
}

func readNodePattern(ctx context.Context, pattern string, properties patternPropertyReader) (nodePatternInfo, error) {
	head, props := splitNodePatternProperties(pattern)
	info := nodePatternInfo{labels: []string{}}
	if props == "" {
		// No property span has no value phase, for either reader.
		info.properties = make(map[string]interface{})
	} else {
		var err error
		info.properties, err = properties(ctx, props, true)
		if err != nil {
			return nodePatternInfo{}, err
		}
	}
	info.variable, info.labels, info.labelErr = parseNodeHead(head)
	return info, nil
}

// splitNodePatternProperties unwraps a single node's path assignment and outer
// parentheses, then splits its property map outside backtick-quoted labels.
func splitNodePatternProperties(pattern string) (head, props string) {
	pattern = strings.TrimSpace(pattern)
	if extractPathAssignmentVariable(pattern) != "" {
		_, pattern, _ = splitPathAssignment(pattern)
	}
	if strings.HasPrefix(pattern, "(") && strings.HasSuffix(pattern, ")") {
		pattern = pattern[1 : len(pattern)-1]
	}
	if braceIdx := indexByteOutsideBackticks(pattern, '{'); braceIdx >= 0 {
		return pattern[:braceIdx], pattern[braceIdx:]
	}
	return pattern, ""
}

// parseNodeHead parses "n:L1:`L 2`" into the variable and label names
// (parseLabelChain) and the label-rule error, if any.
func parseNodeHead(head string) (variable string, labels []string, labelErr error) {
	variable, chain, hasLabels := splitNodeHead(head)
	if !hasLabels {
		return variable, []string{}, nil
	}
	labels, labelErr = parseLabelChain(chain)
	return variable, labels, labelErr
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
func (e *StorageExecutor) parseProperties(ctx context.Context, propsStr string) (map[string]interface{}, error) {
	return e.parsePropertyMap(ctx, propsStr, false)
}

func (e *StorageExecutor) parsePropertyMap(ctx context.Context, propsStr string, retainNull bool) (map[string]interface{}, error) {
	return e.materializePropertyMap(ctx, propsStr, e.fabricRecordBindings, retainNull, false)
}

func normalizePropertyKey(key string) string {
	key = strings.TrimSpace(key)
	if len(key) >= 2 {
		if strings.HasPrefix(key, "`") && strings.HasSuffix(key, "`") {
			return symbolicNameValue(key)
		}
		if (strings.HasPrefix(key, "'") && strings.HasSuffix(key, "'")) ||
			(strings.HasPrefix(key, "\"") && strings.HasSuffix(key, "\"")) {
			return key[1 : len(key)-1]
		}
	}
	return key
}

func findTopLevelMapKeyValueSeparator(pair string) int {
	return findTopLevelOperator(pair, ":", false, true)
}
func isBackslashEscaped(s string, idx int) bool {
	if idx <= 0 {
		return false
	}
	backslashes := 0
	for i := idx - 1; i >= 0 && s[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
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
func (e *StorageExecutor) parsePropertyValue(ctx context.Context, valueStr string) (interface{}, error) {
	return e.checkedPropertyExpression(ctx, valueStr, e.fabricRecordBindings)
}

func (e *StorageExecutor) evaluateScalarPropertyExpression(ctx context.Context, expr string) (interface{}, bool) {
	return e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(expr), e.fabricRecordBindings)
}

func (e *StorageExecutor) evaluateArithmeticLookupResult(op byte, left interface{}, leftOK bool, right interface{}, rightOK bool) (interface{}, bool) {
	if !leftOK || !rightOK {
		return nil, false
	}
	var value interface{}
	switch op {
	case '+':
		value = e.add(left, right)
	case '*':
		value = e.multiply(left, right)
	case '/':
		value = e.divide(left, right)
	case '%':
		value = e.modulo(left, right)
	case '-':
		value = e.subtract(left, right)
	}
	return value, value != nil
}

// invalidPropertyValue marks a property value that failed parsing validation
type invalidPropertyValue struct {
	raw string
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
func (e *StorageExecutor) parseArrayValue(ctx context.Context, arrayStr string) ([]interface{}, error) {
	value, err := e.checkedPropertyExpression(ctx, arrayStr, e.fabricRecordBindings)
	if err != nil {
		return nil, err
	}
	array, ok := value.([]interface{})
	if !ok {
		return nil, unresolvedExpressionError(arrayStr)
	}
	return array, nil
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
//	splitArrayElements("f(1, 2), 3")         // ["f(1, 2)", "3"]
func (e *StorageExecutor) splitArrayElements(inner string) []string {
	// The statement's own top-level comma split (quotes, parentheses,
	// brackets and braces nest): [substring('abc', 1, 1)] is one element
	// (#907). An empty element is dropped.
	parts := splitTopLevelComma(inner)
	elements := parts[:0]
	for _, part := range parts {
		if part != "" {
			elements = append(elements, part)
		}
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
			if !isIdentStartRune(c) {
				return false
			}
		} else {
			// Subsequent chars can be alphanumeric, underscore, or dot
			if !(isIdentRune(c) || c == '.') {
				return false
			}
		}
	}

	return true
}

// nodePatternStructure reads syntax only; it does not materialize property values.
func nodePatternStructure(pattern string) nodePatternInfo {
	head, _ := splitNodePatternProperties(pattern)
	variable, labels, err := parseNodeHead(head)
	return nodePatternInfo{variable: variable, labels: labels, labelErr: err}
}
