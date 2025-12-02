// Operator evaluation for NornicDB Cypher.
//
// This file contains functions for evaluating logical, comparison, and arithmetic
// operators in Cypher expressions. These operators follow Neo4j semantics for
// compatibility.
//
// # Operator Categories
//
// Logical Operators:
//   - AND: Logical conjunction (short-circuit evaluation)
//   - OR: Logical disjunction (short-circuit evaluation)
//   - XOR: Logical exclusive or
//   - NOT: Logical negation
//
// Comparison Operators:
//   - = : Equality
//   - <> : Inequality (also !=)
//   - < : Less than
//   - > : Greater than
//   - <= : Less than or equal
//   - >= : Greater than or equal
//   - =~ : Regular expression match
//
// Arithmetic Operators:
//   - + : Addition (also date + duration)
//   - - : Subtraction (also date - duration, date - date)
//   - * : Multiplication
//   - / : Division
//   - % : Modulo
//
// # Operator Precedence
//
// From highest to lowest:
//  1. Unary: NOT, -
//  2. Multiplicative: *, /, %
//  3. Additive: +, -
//  4. Comparison: =, <>, <, >, <=, >=, =~
//  5. Logical AND
//  6. Logical XOR
//  7. Logical OR
//
// # ELI12
//
// Operators are like action words in math:
//   - AND means "both must be true" (like needing both a ticket AND ID to enter)
//   - OR means "at least one" (like having either cash OR a card to pay)
//   - + means "add together" (like combining apples from two baskets)
//   - = means "are these the same?" (like checking if two puzzle pieces match)
//
// When you have multiple operators like "2 + 3 * 4", we follow PEMDAS rules:
// multiplication (*) happens before addition (+), so it's 2 + (3 * 4) = 14.
//
// # Neo4j Compatibility
//
// These operators match Neo4j behavior exactly:
//   - NULL propagation (NULL AND true = NULL)
//   - Type coercion rules (comparing integers to floats)
//   - Date arithmetic (date + duration)
//   - Regex matching (=~ operator)

package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Logical Operators
// ========================================

// hasLogicalOperator checks if the expression has a logical operator outside of quotes/parentheses.
//
// This function scans the expression for the given operator, ensuring it's not inside:
//   - String literals (single or double quotes)
//   - Nested parentheses
//
// # Parameters
//
//   - expr: The expression to check
//   - op: The logical operator to find (case-insensitive)
//
// # Returns
//
//   - true if the operator is found at the top level
//   - false if not found or only inside quotes/parentheses
//
// # Example
//
//	hasLogicalOperator("a = 1 AND b = 2", " AND ")      // true
//	hasLogicalOperator("name CONTAINS 'AND'", " AND ") // false (inside quotes)
//	hasLogicalOperator("(a AND b) OR c", " AND ")      // false (inside parens)
func (e *StorageExecutor) hasLogicalOperator(expr, op string) bool {
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)

	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i <= len(upperExpr)-len(upperOp); i++ {
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
		case !inQuote && parenDepth == 0:
			if upperExpr[i:i+len(upperOp)] == upperOp {
				return true
			}
		}
	}
	return false
}

// evaluateLogicalAnd evaluates expr1 AND expr2.
//
// This implements short-circuit evaluation: if any part is false,
// the entire expression is false without evaluating remaining parts.
//
// # Parameters
//
//   - expr: The expression containing AND operators
//   - nodes: Map of variable names to nodes for property access
//   - rels: Map of variable names to relationships for property access
//
// # Returns
//
//   - true if all parts evaluate to true
//   - false if any part evaluates to false
//   - nil if the expression cannot be split
//
// # Example
//
//	evaluateLogicalAnd("a = 1 AND b = 2", nodes, rels)  // true if both conditions match
//	evaluateLogicalAnd("true AND false", nodes, rels)  // false
func (e *StorageExecutor) evaluateLogicalAnd(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " AND ")
	if len(parts) < 2 {
		return nil
	}

	for _, part := range parts {
		result := e.evaluateExpressionWithContext(part, nodes, rels)
		if result != true {
			return false
		}
	}
	return true
}

// evaluateLogicalOr evaluates expr1 OR expr2.
//
// This implements short-circuit evaluation: if any part is true,
// the entire expression is true without evaluating remaining parts.
//
// # Parameters
//
//   - expr: The expression containing OR operators
//   - nodes: Map of variable names to nodes for property access
//   - rels: Map of variable names to relationships for property access
//
// # Returns
//
//   - true if any part evaluates to true
//   - false if all parts evaluate to false
//   - nil if the expression cannot be split
//
// # Example
//
//	evaluateLogicalOr("a = 1 OR b = 2", nodes, rels)  // true if either condition matches
//	evaluateLogicalOr("false OR true", nodes, rels)  // true
func (e *StorageExecutor) evaluateLogicalOr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " OR ")
	if len(parts) < 2 {
		return nil
	}

	for _, part := range parts {
		result := e.evaluateExpressionWithContext(part, nodes, rels)
		if result == true {
			return true
		}
	}
	return false
}

// evaluateLogicalXor evaluates expr1 XOR expr2.
//
// XOR (exclusive or) returns true when exactly one operand is true.
//
// # Parameters
//
//   - expr: The expression containing XOR operator
//   - nodes: Map of variable names to nodes for property access
//   - rels: Map of variable names to relationships for property access
//
// # Returns
//
//   - true if exactly one side is true
//   - false if both are true or both are false
//   - nil if the expression cannot be split into exactly 2 parts
//
// # Example
//
//	evaluateLogicalXor("true XOR false", nodes, rels)  // true
//	evaluateLogicalXor("true XOR true", nodes, rels)   // false
//	evaluateLogicalXor("false XOR false", nodes, rels) // false
func (e *StorageExecutor) evaluateLogicalXor(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " XOR ")
	if len(parts) != 2 {
		return nil
	}

	left := e.evaluateExpressionWithContext(parts[0], nodes, rels) == true
	right := e.evaluateExpressionWithContext(parts[1], nodes, rels) == true
	return left != right
}

// ========================================
// Comparison Operators
// ========================================

// hasComparisonOperator checks if the expression has a comparison operator.
//
// Operators checked (in order of specificity):
//   - <> (not equal)
//   - <= (less than or equal)
//   - >= (greater than or equal)
//   - =~ (regex match)
//   - != (not equal, alternative)
//   - = (equal)
//   - < (less than)
//   - > (greater than)
//
// # Parameters
//
//   - expr: The expression to check
//
// # Returns
//
//   - true if any comparison operator is found at the top level
//
// # Example
//
//	hasComparisonOperator("a = 1")          // true
//	hasComparisonOperator("a <= 5")         // true
//	hasComparisonOperator("name =~ '.*'")   // true
//	hasComparisonOperator("a + b")          // false
func (e *StorageExecutor) hasComparisonOperator(expr string) bool {
	ops := []string{"<>", "<=", ">=", "=~", "!=", "=", "<", ">"}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// hasOperatorOutsideQuotes checks if operator exists outside quotes and parentheses.
//
// This function handles special cases like ensuring "=" is not confused with
// "<=", ">=", "!=" or "=~".
//
// # Parameters
//
//   - expr: The expression to check
//   - op: The operator to find
//
// # Returns
//
//   - true if the operator is found at the top level
//
// # Example
//
//	hasOperatorOutsideQuotes("a = 1", "=")           // true
//	hasOperatorOutsideQuotes("a <= 1", "=")          // false (part of <=)
//	hasOperatorOutsideQuotes("name = 'test=1'", "=") // true (first = only)
func (e *StorageExecutor) hasOperatorOutsideQuotes(expr, op string) bool {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i <= len(expr)-len(op); i++ {
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
		case !inQuote && parenDepth == 0:
			if expr[i:i+len(op)] == op {
				// Make sure = is not part of <= or >=
				if op == "=" {
					if i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
						continue
					}
					if i < len(expr)-1 && expr[i+1] == '~' {
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

// evaluateComparisonExpr evaluates comparison expressions.
//
// Operators are evaluated in order of specificity to handle multi-character
// operators correctly (e.g., "<>" before "<" and ">").
//
// # Parameters
//
//   - expr: The comparison expression
//   - nodes: Map of variable names to nodes for property access
//   - rels: Map of variable names to relationships for property access
//
// # Returns
//
//   - true or false based on the comparison result
//   - nil if no valid comparison operator found
//
// # Example
//
//	evaluateComparisonExpr("5 > 3", nodes, rels)        // true
//	evaluateComparisonExpr("'abc' = 'abc'", nodes, rels) // true
//	evaluateComparisonExpr("n.age >= 18", nodes, rels)   // depends on n.age
func (e *StorageExecutor) evaluateComparisonExpr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	// Try operators in order of specificity
	ops := []struct {
		op   string
		eval func(left, right interface{}) bool
	}{
		{"<>", func(l, r interface{}) bool { return !e.compareEqual(l, r) }},
		{"!=", func(l, r interface{}) bool { return !e.compareEqual(l, r) }},
		{"<=", func(l, r interface{}) bool { return e.compareLess(l, r) || e.compareEqual(l, r) }},
		{">=", func(l, r interface{}) bool { return e.compareGreater(l, r) || e.compareEqual(l, r) }},
		{"=~", e.compareRegex},
		{"=", e.compareEqual},
		{"<", e.compareLess},
		{">", e.compareGreater},
	}

	for _, op := range ops {
		parts := e.splitByOperator(expr, op.op)
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			return op.eval(left, right)
		}
	}

	return nil
}

// ========================================
// Arithmetic Operators
// ========================================

// hasArithmeticOperator checks if the expression has arithmetic operators.
//
// Checks for: +, -, *, /, %
// Note: + and - are checked with surrounding spaces to distinguish
// from unary operators and signs in numbers.
//
// # Parameters
//
//   - expr: The expression to check
//
// # Returns
//
//   - true if any arithmetic operator is found at the top level
//
// # Example
//
//	hasArithmeticOperator("a + b")   // true
//	hasArithmeticOperator("a * b")   // true
//	hasArithmeticOperator("-5")      // false (unary minus)
//	hasArithmeticOperator("a - b")   // true
func (e *StorageExecutor) hasArithmeticOperator(expr string) bool {
	// Include + for date arithmetic (date + duration)
	ops := []string{" + ", "*", "/", "%", " - "}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// evaluateArithmeticExpr evaluates arithmetic expressions.
//
// Supports:
//   - Numeric operations: +, -, *, /, %
//   - Date arithmetic: date + duration, date - duration, date - date
//
// # Parameters
//
//   - expr: The arithmetic expression
//   - nodes: Map of variable names to nodes for property access
//   - rels: Map of variable names to relationships for property access
//
// # Returns
//
//   - The numeric result (int64 or float64)
//   - For date arithmetic: string (formatted date) or *CypherDuration
//   - nil if no valid arithmetic operator found
//
// # Example
//
//	evaluateArithmeticExpr("5 + 3", nodes, rels)             // int64(8)
//	evaluateArithmeticExpr("10 / 3", nodes, rels)            // float64(3.333...)
//	evaluateArithmeticExpr("date('2025-01-01') + duration('P5D')", ...) // "2025-01-06..."
func (e *StorageExecutor) evaluateArithmeticExpr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	// Handle + operator (date + duration, or numeric addition)
	if parts := e.splitByOperator(expr, " + "); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.add(left, right)
	}

	// Handle * operator
	if parts := e.splitByOperator(expr, "*"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.multiply(left, right)
	}

	// Handle / operator
	if parts := e.splitByOperator(expr, "/"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.divide(left, right)
	}

	// Handle % operator
	if parts := e.splitByOperator(expr, "%"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.modulo(left, right)
	}

	// Handle - operator (binary subtraction, not unary minus)
	if parts := e.splitByOperator(expr, " - "); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.subtract(left, right)
	}

	return nil
}

// splitByOperator splits expression by operator respecting quotes and parentheses.
//
// This function carefully handles:
//   - String literals (single and double quotes)
//   - Nested parentheses
//   - Special case for "=" not being part of "<=", ">=", "!="
//
// # Parameters
//
//   - expr: The expression to split
//   - op: The operator to split by (case-insensitive)
//
// # Returns
//
//   - A slice with exactly 2 elements if operator found
//   - A slice with 1 element (original expr) if not found
//
// # Example
//
//	splitByOperator("a + b", " + ")              // ["a", "b"]
//	splitByOperator("'a + b' + c", " + ")        // ["'a + b'", "c"]
//	splitByOperator("(a + b) + c", " + ")        // ["(a + b)", "c"]
func (e *StorageExecutor) splitByOperator(expr, op string) []string {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)

	for i := 0; i <= len(expr)-len(op); i++ {
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
		case !inQuote && parenDepth == 0:
			if upperExpr[i:i+len(upperOp)] == upperOp {
				// Additional check for = not being part of <= or >=
				if op == "=" {
					if i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
						continue
					}
				}
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+len(op):])
				return []string{left, right}
			}
		}
	}
	return []string{expr}
}

// ========================================
// Arithmetic Helper Functions
// ========================================

// add handles addition including date + duration.
//
// This function supports:
//   - Numeric addition (int64 + int64 = int64, otherwise float64)
//   - Date arithmetic (date + duration = date, duration + date = date)
//
// # Parameters
//
//   - left: Left operand
//   - right: Right operand
//
// # Returns
//
//   - int64 if both operands are integers
//   - float64 for other numeric types
//   - string (formatted date) for date + duration
//   - nil if operands cannot be added
//
// # Example
//
//	add(5, 3)                                    // int64(8)
//	add(5.0, 3)                                  // float64(8.0)
//	add("2025-01-01", &CypherDuration{Days: 5})  // "2025-01-06T00:00:00Z"
func (e *StorageExecutor) add(left, right interface{}) interface{} {
	// Handle date/datetime + duration
	if dur, ok := right.(*CypherDuration); ok {
		result := addDurationToDate(left, dur)
		if result != "" {
			return result
		}
	}
	// Handle duration + date/datetime (commutative)
	if dur, ok := left.(*CypherDuration); ok {
		result := addDurationToDate(right, dur)
		if result != "" {
			return result
		}
	}

	// Standard numeric addition
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l + r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

// multiply performs numeric multiplication.
//
// # Parameters
//
//   - left: Left operand
//   - right: Right operand
//
// # Returns
//
//   - int64 if both operands are integers
//   - float64 otherwise
//   - nil if operands cannot be multiplied
//
// # Example
//
//	multiply(5, 3)    // int64(15)
//	multiply(5.0, 3)  // float64(15.0)
func (e *StorageExecutor) multiply(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l * r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

// divide performs numeric division.
//
// Note: Division by zero returns nil (NULL in Cypher).
//
// # Parameters
//
//   - left: Dividend
//   - right: Divisor
//
// # Returns
//
//   - float64 result
//   - nil if divisor is zero or operands invalid
//
// # Example
//
//	divide(10, 3)  // float64(3.333...)
//	divide(10, 0)  // nil (division by zero)
func (e *StorageExecutor) divide(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return l / r
}

// modulo performs modulo operation (remainder after division).
//
// Note: Operands are converted to integers for the modulo operation.
//
// # Parameters
//
//   - left: Dividend
//   - right: Divisor
//
// # Returns
//
//   - int64 remainder
//   - nil if divisor is zero or operands invalid
//
// # Example
//
//	modulo(10, 3)  // int64(1)
//	modulo(10, 0)  // nil (division by zero)
func (e *StorageExecutor) modulo(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return int64(l) % int64(r)
}

// subtract handles subtraction including date arithmetic.
//
// This function supports:
//   - Numeric subtraction (int64 - int64 = int64, otherwise float64)
//   - Date - duration = date
//   - Date - date = duration
//
// # Parameters
//
//   - left: Left operand
//   - right: Right operand
//
// # Returns
//
//   - int64 if both operands are integers
//   - float64 for other numeric types
//   - string (formatted date) for date - duration
//   - *CypherDuration for date - date
//   - nil if operands cannot be subtracted
//
// # Example
//
//	subtract(10, 3)                                   // int64(7)
//	subtract("2025-01-06", &CypherDuration{Days: 5})  // "2025-01-01T00:00:00Z"
//	subtract("2025-01-06", "2025-01-01")              // &CypherDuration{Days: 5}
func (e *StorageExecutor) subtract(left, right interface{}) interface{} {
	// Handle date - duration = date
	if dur, ok := right.(*CypherDuration); ok {
		result := subtractDurationFromDate(left, dur)
		if result != "" {
			return result
		}
	}

	// Handle date - date = duration
	leftTime := parseDateTime(left)
	rightTime := parseDateTime(right)
	if !leftTime.IsZero() && !rightTime.IsZero() {
		return durationBetween(left, right)
	}

	// Standard numeric subtraction
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l - r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

// ========================================
// String Predicate Helper
// ========================================

// hasStringPredicate checks if expression has a string predicate (case-insensitive).
//
// String predicates include STARTS WITH, ENDS WITH, CONTAINS.
// This function respects quotes and nested structures.
//
// # Parameters
//
//   - expr: The expression to check
//   - predicate: The predicate to find (e.g., " CONTAINS ", " STARTS WITH ")
//
// # Returns
//
//   - true if the predicate is found at the top level
//
// # Example
//
//	hasStringPredicate("n.name CONTAINS 'test'", " CONTAINS ")  // true
//	hasStringPredicate("'CONTAINS' = n.name", " CONTAINS ")     // false (in quotes)
func (e *StorageExecutor) hasStringPredicate(expr, predicate string) bool {
	upperExpr := strings.ToUpper(expr)
	upperPred := strings.ToUpper(predicate)

	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	bracketDepth := 0

	for i := 0; i <= len(upperExpr)-len(upperPred); i++ {
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
		case c == '[' && !inQuote:
			bracketDepth++
		case c == ']' && !inQuote:
			bracketDepth--
		case !inQuote && parenDepth == 0 && bracketDepth == 0:
			if upperExpr[i:i+len(upperPred)] == upperPred {
				return true
			}
		}
	}
	return false
}
