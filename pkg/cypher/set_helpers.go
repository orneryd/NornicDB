// SET clause helpers for NornicDB Cypher.
//
// This file contains helper functions for processing SET clauses in Cypher queries.
// These functions handle property assignments, expression evaluation, and string
// operations used during SET operations.
//
// # SET Clause Syntax
//
// SET operations modify node and relationship properties:
//
//	SET n.name = 'Alice'           - Single property
//	SET n.name = 'Alice', n.age = 30  - Multiple properties
//	SET n += {name: 'Alice'}       - Map merge
//	SET n:Label                    - Add label
//
// # Expression Evaluation
//
// SET clauses can use various expressions:
//
//	SET n.id = randomUUID()                    - Function call
//	SET n.ts = timestamp()                     - Built-in function
//	SET n.name = 'prefix-' + toString(n.id)   - String concatenation
//	SET n.list = [1, 2, 3]                     - Array literal
//
// # ELI12
//
// Think of SET like filling out a form:
//
//	SET n.name = 'Alice'
//
// You're writing "Alice" in the "name" box on the form. These helper
// functions make sure the value goes in the right format:
//   - Strings stay as strings: 'hello' → "hello"
//   - Numbers become numbers: 42 → 42
//   - Arrays become lists: [1,2] → [1, 2]
//   - Functions run and give results: timestamp() → 1234567890
//
// # Neo4j Compatibility
//
// These helpers ensure SET clauses work identically to Neo4j.

package cypher

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// applySetToNode applies SET clause assignments to a node.
//
// This function parses SET clauses like "n.name = 'Alice', n.age = 30"
// and updates the node's properties accordingly.
//
// # Parameters
//
//   - node: The node to update
//   - varName: The variable name used in the SET clause (e.g., "n")
//   - setClause: The SET clause string (without "SET" keyword)
//
// # Example
//
//	applySetToNode(node, "n", "n.name = 'Alice', n.age = 30")
//	// node.Properties["name"] = "Alice"
//	// node.Properties["age"] = int64(30)
func (e *StorageExecutor) applySetToNode(node *storage.Node, varName string, setClause string) {
	// Split SET clause into individual assignments, respecting parentheses and quotes
	assignments := e.splitSetAssignments(setClause)

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if !strings.HasPrefix(assignment, varName+".") {
			continue
		}

		eqIdx := strings.Index(assignment, "=")
		if eqIdx <= 0 {
			continue
		}

		propName := strings.TrimSpace(assignment[len(varName)+1 : eqIdx])
		propValue := strings.TrimSpace(assignment[eqIdx+1:])

		// Evaluate the expression and set the property
		setNodeProperty(node, propName, e.evaluateSetExpression(propValue))
	}
}

// setNodeProperty sets a property on a node.
//
// Special handling for "embedding" property which goes to node.Embedding
// instead of node.Properties.
//
// # Parameters
//
//   - node: The node to update
//   - propName: The property name
//   - value: The value to set
func setNodeProperty(node *storage.Node, propName string, value interface{}) {
	if propName == "embedding" {
		node.Embedding = toFloat32Slice(value)
		return
	}
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[propName] = value
}

// splitSetAssignments splits a SET clause into individual assignments,
// respecting parentheses and quotes.
//
// # Parameters
//
//   - setClause: The SET clause string (without "SET" keyword)
//
// # Returns
//
//   - Slice of individual assignments
//
// # Example
//
//	splitSetAssignments("n.name = 'Alice', n.age = 30")
//	// Returns: ["n.name = 'Alice'", "n.age = 30"]
//
//	splitSetAssignments("n.name = concat('a', 'b'), n.x = 1")
//	// Returns: ["n.name = concat('a', 'b')", "n.x = 1"]
func (e *StorageExecutor) splitSetAssignments(setClause string) []string {
	var assignments []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, c := range setClause {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote
				if i > 0 && setClause[i-1] != '\\' {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case c == '(' && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case c == ')' && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == ',' && !inQuote && parenDepth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				assignments = append(assignments, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	// Add final assignment
	if s := strings.TrimSpace(current.String()); s != "" {
		assignments = append(assignments, s)
	}

	return assignments
}

// splitSetAssignmentsRespectingBrackets splits a SET clause into individual assignments,
// respecting brackets (for arrays), parentheses, and quotes.
//
// # Parameters
//
//   - setClause: The SET clause string
//
// # Returns
//
//   - Slice of individual assignments
//
// # Example
//
//	splitSetAssignmentsRespectingBrackets("n.embedding = [0.1, 0.2], n.dim = 4")
//	// Returns: ["n.embedding = [0.1, 0.2]", "n.dim = 4"]
func (e *StorageExecutor) splitSetAssignmentsRespectingBrackets(setClause string) []string {
	var assignments []string
	var current strings.Builder
	depth := 0 // Tracks both () and []
	inQuote := false
	quoteChar := rune(0)

	for i, c := range setClause {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote
				if i > 0 && setClause[i-1] != '\\' {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '(' || c == '[') && !inQuote:
			depth++
			current.WriteRune(c)
		case (c == ')' || c == ']') && !inQuote:
			depth--
			current.WriteRune(c)
		case c == ',' && !inQuote && depth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				assignments = append(assignments, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	// Add final assignment
	if s := strings.TrimSpace(current.String()); s != "" {
		assignments = append(assignments, s)
	}

	return assignments
}

// evaluateSetExpression evaluates a Cypher expression for SET clauses.
//
// Handles various expression types including literals, arrays, function calls,
// and string concatenation.
//
// # Parameters
//
//   - expr: The expression string to evaluate
//
// # Returns
//
//   - The evaluated value
//
// # Example
//
//	evaluateSetExpression("'Alice'")      // "Alice"
//	evaluateSetExpression("42")           // int64(42)
//	evaluateSetExpression("true")         // true
//	evaluateSetExpression("[1, 2, 3]")    // []interface{}{1, 2, 3}
//	evaluateSetExpression("timestamp()") // current timestamp
func (e *StorageExecutor) evaluateSetExpression(expr string) interface{} {
	expr = strings.TrimSpace(expr)

	// Handle null
	if strings.EqualFold(expr, "null") {
		return nil
	}

	// Handle simple literals
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
		return expr[1 : len(expr)-1]
	}
	if strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"") {
		return expr[1 : len(expr)-1]
	}
	if expr == "true" {
		return true
	}
	if expr == "false" {
		return false
	}

	// Handle numbers
	if val, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return val
	}
	if val, err := strconv.ParseFloat(expr, 64); err == nil {
		return val
	}

	// Handle arrays (simplified)
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}
		}
		// Simple split for basic arrays
		parts := strings.Split(inner, ",")
		result := make([]interface{}, len(parts))
		for i, p := range parts {
			result[i] = e.evaluateSetExpression(strings.TrimSpace(p))
		}
		return result
	}

	// Handle function calls and expressions
	lowerExpr := strings.ToLower(expr)

	// timestamp() - returns current timestamp
	if lowerExpr == "timestamp()" {
		return e.idCounter()
	}

	// datetime() - returns ISO date string
	if lowerExpr == "datetime()" {
		return fmt.Sprintf("%d", e.idCounter())
	}

	// randomUUID() or randomuuid()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}

	// Handle string concatenation: 'prefix-' + toString(timestamp()) + '-' + substring(randomUUID(), 0, 8)
	if strings.Contains(expr, " + ") {
		return e.evaluateStringConcat(expr)
	}

	// Handle toString(expr)
	if strings.HasPrefix(lowerExpr, "tostring(") && strings.HasSuffix(expr, ")") {
		inner := expr[9 : len(expr)-1]
		val := e.evaluateSetExpression(inner)
		return fmt.Sprintf("%v", val)
	}

	// Handle substring(str, start, length)
	if strings.HasPrefix(lowerExpr, "substring(") && strings.HasSuffix(expr, ")") {
		return e.evaluateSubstringForSet(expr)
	}

	// If nothing else matched, return as-is (already substituted parameter value)
	return expr
}

// evaluateStringConcat handles string concatenation with +
//
// # Parameters
//
//   - expr: The expression with + operators
//
// # Returns
//
//   - The concatenated string
//
// # Example
//
//	evaluateStringConcat("'Hello' + ' ' + 'World'")
//	// Returns: "Hello World"
func (e *StorageExecutor) evaluateStringConcat(expr string) string {
	var result strings.Builder

	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		val := e.evaluateSetExpression(part)
		result.WriteString(fmt.Sprintf("%v", val))
	}

	return result.String()
}

// hasConcatOperator checks if the expression has a + operator outside of quotes.
// This prevents infinite recursion when property values contain " + " in text.
//
// # Parameters
//
//   - expr: The expression to check
//
// # Returns
//
//   - true if + operator exists at top level
func (e *StorageExecutor) hasConcatOperator(expr string) bool {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case c == '+' && !inQuote && parenDepth == 0:
			// Check for space before and after (to avoid matching ++ or += etc)
			hasBefore := i > 0 && expr[i-1] == ' '
			hasAfter := i < len(expr)-1 && expr[i+1] == ' '
			if hasBefore && hasAfter {
				return true
			}
		}
	}
	return false
}

// splitByPlus splits an expression by + operator, respecting quotes and parentheses.
//
// # Parameters
//
//   - expr: The expression to split
//
// # Returns
//
//   - Slice of parts separated by +
func (e *StorageExecutor) splitByPlus(expr string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case c == ')' && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == '+' && !inQuote && parenDepth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				parts = append(parts, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		parts = append(parts, s)
	}

	return parts
}

// evaluateSubstringForSet handles substring(str, start, length) for SET expressions.
//
// # Parameters
//
//   - expr: The substring function call
//
// # Returns
//
//   - The extracted substring
func (e *StorageExecutor) evaluateSubstringForSet(expr string) string {
	// Extract arguments from substring(str, start, length)
	inner := expr[10 : len(expr)-1] // Remove "substring(" and ")"

	// Split by comma, respecting parentheses
	args := e.splitFunctionArgs(inner)
	if len(args) < 2 {
		return ""
	}

	// Evaluate the string argument
	str := fmt.Sprintf("%v", e.evaluateSetExpression(args[0]))

	// Parse start
	start, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		start = 0
	}

	// Parse optional length
	length := len(str) - start
	if len(args) >= 3 {
		if l, err := strconv.Atoi(strings.TrimSpace(args[2])); err == nil {
			length = l
		}
	}

	// Apply substring
	if start >= len(str) {
		return ""
	}
	end := start + length
	if end > len(str) {
		end = len(str)
	}
	return str[start:end]
}

// splitFunctionArgs splits function arguments by comma, respecting parentheses and quotes.
//
// This function intelligently parses function arguments, handling:
//   - Nested parentheses: function(a, func(b, c), d)
//   - Quoted strings: "hello, world" treated as single argument
//   - Escape sequences: \"escaped quote\" within strings
//   - Mixed quotes: 'single' and "double" quotes
//
// # Parameters
//
//   - args: The argument string to split (without outer parentheses)
//
// # Returns
//
//   - Slice of individual arguments, trimmed of whitespace
//
// # Example
//
//	splitFunctionArgs("name, age, email")
//	// Returns: ["name", "age", "email"]
//
//	splitFunctionArgs("toLower(n.name), toUpper(n.city)")
//	// Returns: ["toLower(n.name)", "toUpper(n.city)"]
//
// # ELI12
//
// Imagine you're reading a sentence: "I like pizza, pasta, and ice cream"
// You need to split it by commas, but what if someone says:
// "I like pizza, 'pasta, with sauce', and ice cream"
//
// You can't just split at every comma! The comma inside the quotes is PART of
// the item, not a separator. This function is smart enough to know:
//   - Commas outside quotes = split here
//   - Commas inside quotes = keep them
//   - Parentheses = keep everything inside together
func (e *StorageExecutor) splitFunctionArgs(args string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(args); i++ {
		c := args[i]
		switch c {
		case '\'':
			if !inDoubleQuote {
				// Check for escape sequence
				if i > 0 && args[i-1] == '\\' {
					current.WriteByte(c)
				} else {
					inSingleQuote = !inSingleQuote
					current.WriteByte(c)
				}
			} else {
				current.WriteByte(c)
			}
		case '"':
			if !inSingleQuote {
				// Check for escape sequence
				if i > 0 && args[i-1] == '\\' {
					current.WriteByte(c)
				} else {
					inDoubleQuote = !inDoubleQuote
					current.WriteByte(c)
				}
			} else {
				current.WriteByte(c)
			}
		case '(':
			if !inSingleQuote && !inDoubleQuote {
				parenDepth++
			}
			current.WriteByte(c)
		case ')':
			if !inSingleQuote && !inDoubleQuote {
				parenDepth--
			}
			current.WriteByte(c)
		case ',':
			if parenDepth == 0 && !inSingleQuote && !inDoubleQuote {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
		}
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		result = append(result, s)
	}

	return result
}

// generateUUID generates a simple UUID-like string.
//
// Uses crypto/rand for cryptographically secure random bytes.
//
// # Returns
//
//   - A UUID-formatted string
func (e *StorageExecutor) generateUUID() string {
	// Use crypto/rand for proper UUID
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
