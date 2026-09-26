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
	"context"
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

func asciiLowerByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func equalFoldASCII(text, other string) bool {
	if len(text) != len(other) {
		return false
	}
	for i := 0; i < len(text); i++ {
		if asciiLowerByte(text[i]) != asciiLowerByte(other[i]) {
			return false
		}
	}
	return true
}

func hasPrefixFoldASCII(text, prefix string) bool {
	return len(text) >= len(prefix) && equalFoldASCII(text[:len(prefix)], prefix)
}

func hasSuffixFoldASCII(text, suffix string) bool {
	return len(text) >= len(suffix) && equalFoldASCII(text[len(text)-len(suffix):], suffix)
}

func isASCIIWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func hasKeywordPrefixFoldASCII(text, keyword string) bool {
	if len(text) < len(keyword) || !equalFoldASCII(text[:len(keyword)], keyword) {
		return false
	}
	return len(text) == len(keyword) || isASCIIWhitespace(text[len(keyword)])
}

func operatorMatchesAt(expr, op string, idx int, caseInsensitive bool) bool {
	for offset := 0; offset < len(op); offset++ {
		exprByte := expr[idx+offset]
		opByte := op[offset]
		if caseInsensitive {
			exprByte = asciiLowerByte(exprByte)
			opByte = asciiLowerByte(opByte)
		}
		if exprByte != opByte {
			return false
		}
	}
	return true
}

func findTopLevelOperator(expr, op string, caseInsensitive, trackBrackets bool) int {
	if len(expr) < len(op) {
		return -1
	}

	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0

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
		case trackBrackets && c == '[' && !inQuote:
			bracketDepth++
		case trackBrackets && c == ']' && !inQuote:
			bracketDepth--
		case trackBrackets && c == '{' && !inQuote:
			braceDepth++
		case trackBrackets && c == '}' && !inQuote:
			braceDepth--
		case !inQuote && parenDepth == 0 && (!trackBrackets || (bracketDepth == 0 && braceDepth == 0)):
			if operatorMatchesAt(expr, op, i, caseInsensitive) {
				if op == "=" {
					if i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
						continue
					}
					if i < len(expr)-1 && expr[i+1] == '~' {
						continue
					}
				}
				return i
			}
		}
	}

	return -1
}

func splitByOperatorWithOptions(expr, op string, caseInsensitive, trackBrackets bool) (string, string, bool) {
	idx := findTopLevelOperator(expr, op, caseInsensitive, trackBrackets)
	if idx < 0 {
		return "", "", false
	}
	left := strings.TrimSpace(expr[:idx])
	right := strings.TrimSpace(expr[idx+len(op):])
	return left, right, true
}

func hasTopLevelExpressionOperator(expr string) bool {
	if expr == "" {
		return false
	}
	switch expr[0] {
	case '(':
		return false
	}
	if hasKeywordPrefixFoldASCII(expr, "case") {
		return false
	}
	if hasPrefixFoldASCII(expr, "not ") || hasSuffixFoldASCII(expr, " is null") || hasSuffixFoldASCII(expr, " is not null") {
		return true
	}
	for _, op := range []string{" BETWEEN ", " AND ", " OR ", " XOR ", " STARTS WITH ", " ENDS WITH ", " CONTAINS ", " NOT IN ", " IN "} {
		if findTopLevelOperator(expr, op, true, true) >= 0 {
			return true
		}
	}
	for _, op := range []string{"<>", "<=", ">=", "=~", "!=", "=", "<", ">", " + ", "+", "*", "/", "%", " - ", "-"} {
		if findTopLevelOperator(expr, op, false, true) >= 0 {
			return true
		}
	}
	return false
}

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
	return findTopLevelOperator(expr, op, true, false) >= 0
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
func (e *StorageExecutor) evaluateLogicalAnd(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	left, right, ok := splitByOperatorWithOptions(expr, " AND ", true, false)
	if !ok {
		return nil
	}

	if e.evaluateExpressionWithContext(ctx, left, nodes, rels) != true {
		return false
	}
	return e.evaluateExpressionWithContext(ctx, right, nodes, rels) == true
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
func (e *StorageExecutor) evaluateLogicalOr(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	left, right, ok := splitByOperatorWithOptions(expr, " OR ", true, false)
	if !ok {
		return nil
	}

	if e.evaluateExpressionWithContext(ctx, left, nodes, rels) == true {
		return true
	}
	return e.evaluateExpressionWithContext(ctx, right, nodes, rels) == true
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
func (e *StorageExecutor) evaluateLogicalXor(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " XOR ", true, false)
	if !ok {
		return nil
	}

	left := e.evaluateExpressionWithContext(ctx, leftExpr, nodes, rels) == true
	right := e.evaluateExpressionWithContext(ctx, rightExpr, nodes, rels) == true
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
	if len(expr) < len(op) || !strings.Contains(expr, op) {
		return false
	}
	return findTopLevelOperator(expr, op, false, false) >= 0
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
func (e *StorageExecutor) evaluateComparisonExpr(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (interface{}, bool) {
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
		leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, op.op, false, false)
		if ok {
			left := e.evaluateExpressionWithContextFull(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			right := e.evaluateExpressionWithContextFull(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if left == nil || right == nil {
				return nil, true
			}
			if op.op == "=~" {
				matched, err := cypherRegexMatch(left, right)
				if err != nil {
					recordExpressionFailure(ctx, err)
				}
				return matched, true
			}
			if op.op == "=" || op.op == "<>" || op.op == "!=" {
				equal := cypherEquality(left, right)
				if equal == nil {
					return nil, true
				}
				if op.op != "=" {
					return !equal.(bool), true
				}
				return equal, true
			}
			return op.eval(left, right), true
		}
	}

	return nil, false
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
	// Check with and without spaces for + and - operators
	ops := []string{" + ", "+", "*", "/", "%", " - ", "-"}
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
func (e *StorageExecutor) evaluateArithmeticExpr(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (interface{}, bool) {
	operands := func(leftExpr, rightExpr string) (interface{}, interface{}) {
		left := e.evaluateExpressionWithContextFull(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		right := e.evaluateExpressionWithContextFull(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return left, right
	}
	// result reports an operator's statement error (overflow, operand type)
	// and returns its value; a null result is a result, not "not arithmetic".
	result := func(op byte, left, right, value interface{}) (interface{}, bool) {
		if err := arithmeticError(op, left, right); err != nil {
			recordExpressionFailure(ctx, err)
			return nil, true
		}
		return value, true
	}
	// Cypher exponentiation always yields a floating-point value.
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "^", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		leftValue, rightValue := operands(leftExpr, rightExpr)
		left, leftOK := toFloat64(leftValue)
		right, rightOK := toFloat64(rightValue)
		if leftOK && rightOK {
			return result('^', leftValue, rightValue, math.Pow(left, right))
		}
		return result('^', leftValue, rightValue, nil)
	}
	// Handle + operator (date + duration, lists, strings, numbers).
	// Try with spaces first, then without
	for _, plus := range []string{" + ", "+"} {
		if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, plus, true, false); ok && binaryOperands(leftExpr, rightExpr) {
			left, right := operands(leftExpr, rightExpr)
			return result('+', left, right, e.add(left, right))
		}
	}

	// Handle * operator
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "*", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		left, right := operands(leftExpr, rightExpr)
		return result('*', left, right, e.multiply(left, right))
	}

	// Handle / operator
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "/", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		left, right := operands(leftExpr, rightExpr)
		if value, folded := foldedDivisionByZero(leftExpr, rightExpr, left, right); folded {
			return value, true
		}
		if divisionByZero('/', left, right) {
			recordExpressionFailure(ctx, divisionByZeroError())
		}
		return result('/', left, right, e.divide(left, right))
	}

	// Handle % operator
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "%", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		left, right := operands(leftExpr, rightExpr)
		if divisionByZero('%', left, right) {
			recordExpressionFailure(ctx, divisionByZeroError())
		}
		return result('%', left, right, e.modulo(left, right))
	}

	// Handle - operator (binary subtraction, not unary minus)
	// Try with spaces first, then without (but be careful with unary minus)
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " - ", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		left, right := operands(leftExpr, rightExpr)
		return result('-', left, right, e.subtract(left, right))
	}
	// For - without spaces, only split if both sides would be valid expressions
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, "-", true, false); ok && binaryOperands(leftExpr, rightExpr) {
		left, right := operands(leftExpr, rightExpr)
		if left != nil && right != nil {
			return result('-', left, right, e.subtract(left, right))
		}
	}

	return nil, false
}

// binaryOperands reports whether both sides of a binary operator are present;
// a bare "*" (RETURN *) or a leading sign is not an arithmetic expression.
func binaryOperands(left, right string) bool {
	return strings.TrimSpace(left) != "" && strings.TrimSpace(right) != ""
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
	left, right, ok := splitByOperatorWithOptions(expr, op, true, false)
	if !ok {
		return []string{expr}
	}
	return []string{left, right}
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
	// A null operand makes the sum null, for lists and strings too.
	if left == nil || right == nil {
		return nil
	}
	if result, handled := addTemporalValues(left, right); handled {
		return result
	}
	// Cypher list concatenation:
	// - list + list
	// - list + scalar
	// - scalar + list
	leftList, leftIsList := toInterfaceSlice(left)
	rightList, rightIsList := toInterfaceSlice(right)
	if leftIsList && rightIsList {
		out := make([]interface{}, 0, util.SafePreallocSum(len(leftList), len(rightList)))
		out = append(out, leftList...)
		out = append(out, rightList...)
		return out
	}
	if leftIsList {
		out := make([]interface{}, 0, util.SafePreallocSum(len(leftList), 1))
		out = append(out, leftList...)
		out = append(out, right)
		return out
	}
	if rightIsList {
		out := make([]interface{}, 0, util.SafePreallocSum(len(rightList), 1))
		out = append(out, left)
		out = append(out, rightList...)
		return out
	}

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

	// String concatenation: string + string, string + number, number + string.
	if text, isText := left.(string); isText {
		if other, ok := concatOperandText(right); ok {
			return text + other
		}
		return nil
	}
	if text, isText := right.(string); isText {
		if other, ok := concatOperandText(left); ok {
			return other + text
		}
		return nil
	}

	value, _, _ := numericArithmetic('+', left, right)
	return value
}

// concatOperandText is the text a value contributes to string + value:
// strings as they are, numbers as toString() formats them. Other values are
// not concatenated.
func concatOperandText(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case bool:
		return "", false
	}
	if _, isNumber := toFloat64(value); isNumber {
		return formatCypherValueString(value), true
	}
	return "", false
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
	if factor, ok := toFloat64(right); ok {
		if result, handled := scaleTemporalDuration(left, factor); handled {
			return result
		}
	}
	if factor, ok := toFloat64(left); ok {
		if result, handled := scaleTemporalDuration(right, factor); handled {
			return result
		}
	}
	value, _, _ := numericArithmetic('*', left, right)
	return value
}

// divide performs numeric division.
//
// Note: INTEGER division by zero returns nil here (the context-aware
// evaluators report "/ by zero"); a FLOAT operand gives ±Inf or NaN.
// Returns int64 if both operands are integers and division is exact.
//
// # Parameters
//
//   - left: Dividend
//   - right: Divisor
//
// # Returns
//
//   - int64 if exact integer division, float64 otherwise
//   - nil for an INTEGER division by zero or invalid operands
//
// # Example
//
//	divide(10, 2)  // int64(5)
//	divide(10, 3)  // float64(3.333...)
//	divide(10, 0)  // nil (division by zero)
func (e *StorageExecutor) divide(left, right interface{}) interface{} {
	if divisor, ok := toFloat64(right); ok && divisor != 0 {
		if result, handled := scaleTemporalDuration(left, 1/divisor); handled {
			return result
		}
	}
	// Integer division is exact and truncates toward zero; a floating operand
	// selects IEEE 754 division (0.0 / 0.0 is NaN). Integer division by zero
	// is null here; the context-aware evaluators report "/ by zero".
	value, _, _ := numericArithmetic('/', left, right)
	return value
}

// modulo performs modulo operation (remainder after division).
//
// Note: INTEGER modulo by zero returns nil here (the context-aware
// evaluators report "/ by zero"); a FLOAT operand by zero gives NaN.
//
// # Parameters
//
//   - left: Dividend
//   - right: Divisor
//
// # Returns
//
//   - int64 when both operands are integers, float64 otherwise
//   - nil for an INTEGER modulo by zero or invalid operands
//
// # Example
//
//	modulo(10, 3)  // int64(1)
//	modulo(10, 0)  // nil (division by zero)
//	modulo(1.5, 0) // NaN
func (e *StorageExecutor) modulo(left, right interface{}) interface{} {
	value, _, _ := numericArithmetic('%', left, right)
	return value
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
	if result, handled := subtractTemporalValues(left, right); handled {
		return result
	}
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

	value, _, _ := numericArithmetic('-', left, right)
	return value
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
	return findTopLevelOperator(expr, predicate, true, true) >= 0
}
