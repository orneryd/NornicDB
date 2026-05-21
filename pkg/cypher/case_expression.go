// CASE expression implementation for NornicDB Cypher.
// Supports both searched CASE and simple CASE expressions.
//
// Searched CASE:
//   CASE WHEN condition THEN result [WHEN ...] [ELSE default] END
//
// Simple CASE:
//   CASE expression WHEN value THEN result [WHEN ...] [ELSE default] END

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// caseWhenClause represents a single WHEN ... THEN clause in a CASE expression.
type caseWhenClause struct {
	condition string // WHEN condition (for searched CASE)
	value     string // WHEN value (for simple CASE)
	result    string // THEN result
}

// caseExpression represents a parsed CASE expression.
type caseExpression struct {
	isSimple       bool             // true for simple CASE, false for searched CASE
	testExpression string           // expression to test (simple CASE only)
	whenClauses    []caseWhenClause // list of WHEN clauses
	elseResult     string           // ELSE result (optional)
}

// isCaseExpression checks if an expression is a CASE expression.
func isCaseExpression(expr string) bool {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	return strings.HasPrefix(upper, "CASE") && strings.HasSuffix(upper, "END")
}

// parseCaseExpression parses a CASE expression into its components.
// Supports both searched and simple CASE expressions.
func parseCaseExpression(expr string) (*caseExpression, error) {
	expr = strings.TrimSpace(expr)
	upper := strings.ToUpper(expr)

	// Remove CASE and END keywords
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, fmt.Errorf("invalid CASE expression: must start with CASE and end with END")
	}

	// Extract the content between CASE and END
	content := strings.TrimSpace(expr[4 : len(expr)-3])

	ce := &caseExpression{
		whenClauses: []caseWhenClause{},
	}

	// Determine if this is a simple CASE or searched CASE
	// Simple CASE has an expression after CASE before the first WHEN
	firstWhenIdx := indexCaseInsensitive(content, "WHEN")
	if firstWhenIdx == -1 {
		return nil, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	beforeFirstWhen := strings.TrimSpace(content[:firstWhenIdx])
	if beforeFirstWhen != "" {
		// Simple CASE: CASE expression WHEN value THEN result ...
		ce.isSimple = true
		ce.testExpression = beforeFirstWhen
	}

	// Parse WHEN clauses and ELSE clause
	remaining := content[firstWhenIdx:]

	// Split by WHEN (but not within strings or nested expressions)
	whenSections := splitByKeyword(remaining, "WHEN")

	for i, section := range whenSections {
		if i == 0 && strings.TrimSpace(section) == "" {
			continue // Skip empty first section
		}

		section = strings.TrimSpace(section)
		if section == "" {
			continue
		}

		// Check if this section contains ELSE
		elseIdx := indexCaseInsensitive(section, "ELSE")
		if elseIdx >= 0 {
			// Split into WHEN part and ELSE part
			whenPart := strings.TrimSpace(section[:elseIdx])
			elsePart := strings.TrimSpace(section[elseIdx+4:])

			// Parse the WHEN clause if not empty
			if whenPart != "" {
				clause, err := parseWhenClause(whenPart, ce.isSimple)
				if err != nil {
					return nil, err
				}
				ce.whenClauses = append(ce.whenClauses, clause)
			}

			// Set ELSE result
			ce.elseResult = elsePart
			break // ELSE is always last
		} else {
			// Regular WHEN clause
			clause, err := parseWhenClause(section, ce.isSimple)
			if err != nil {
				return nil, err
			}
			ce.whenClauses = append(ce.whenClauses, clause)
		}
	}

	if len(ce.whenClauses) == 0 {
		return nil, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	return ce, nil
}

// parseWhenClause parses a single WHEN ... THEN ... clause.
func parseWhenClause(section string, isSimple bool) (caseWhenClause, error) {
	// Find THEN keyword
	thenIdx := indexCaseInsensitive(section, "THEN")
	if thenIdx == -1 {
		return caseWhenClause{}, fmt.Errorf("WHEN clause must have THEN: %s", section)
	}

	conditionPart := strings.TrimSpace(section[:thenIdx])
	resultPart := strings.TrimSpace(section[thenIdx+4:])

	clause := caseWhenClause{
		result: resultPart,
	}

	if isSimple {
		clause.value = conditionPart
	} else {
		clause.condition = conditionPart
	}

	return clause, nil
}

// evaluateCaseExpression evaluates a CASE expression and returns the result.
func (e *StorageExecutor) evaluateCaseExpression(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	ce, err := parseCaseExpression(expr)
	if err != nil {
		// Return nil if parsing fails
		return nil
	}

	if ce.isSimple {
		// Simple CASE: evaluate test expression once
		testValue := e.evaluateExpressionWithContext(ctx, ce.testExpression, nodes, rels)

		// Check each WHEN clause
		for _, clause := range ce.whenClauses {
			whenValue := e.evaluateExpressionWithContext(ctx, clause.value, nodes, rels)
			if compareValues(testValue, whenValue) {
				return e.evaluateExpressionWithContext(ctx, clause.result, nodes, rels)
			}
		}
	} else {
		// Searched CASE: evaluate each WHEN condition
		for _, clause := range ce.whenClauses {
			conditionResult := e.evaluateCondition(ctx, clause.condition, nodes, rels)
			if isTruthy(conditionResult) {
				return e.evaluateExpressionWithContext(ctx, clause.result, nodes, rels)
			}
		}
	}

	// No WHEN matched, return ELSE result or NULL
	if ce.elseResult != "" {
		return e.evaluateExpressionWithContext(ctx, ce.elseResult, nodes, rels)
	}
	return nil
}

// evaluateCondition evaluates a boolean condition expression.
func (e *StorageExecutor) evaluateCondition(ctx context.Context, condition string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) bool {
	condition = strings.TrimSpace(condition)
	upper := strings.ToUpper(condition)

	// Handle AND - split and evaluate both sides
	// Need to find AND at top level (not inside parentheses)
	andIdx := findTopLevelKeyword(condition, " AND ")
	if andIdx > 0 {
		left := strings.TrimSpace(condition[:andIdx])
		right := strings.TrimSpace(condition[andIdx+5:])
		return e.evaluateCondition(ctx, left, nodes, rels) && e.evaluateCondition(ctx, right, nodes, rels)
	}

	// Handle OR - split and evaluate both sides
	orIdx := findTopLevelKeyword(condition, " OR ")
	if orIdx > 0 {
		left := strings.TrimSpace(condition[:orIdx])
		right := strings.TrimSpace(condition[orIdx+4:])
		return e.evaluateCondition(ctx, left, nodes, rels) || e.evaluateCondition(ctx, right, nodes, rels)
	}

	// Handle NOT prefix
	if strings.HasPrefix(upper, "NOT ") {
		inner := strings.TrimSpace(condition[4:])
		return !e.evaluateCondition(ctx, inner, nodes, rels)
	}

	// Handle comparison operators: <, >, <=, >=, =, <>
	for _, op := range []string{"<=", ">=", "<>", "<", ">", "="} {
		if strings.Contains(condition, op) {
			parts := strings.SplitN(condition, op, 2)
			if len(parts) == 2 {
				left := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(parts[0]), nodes, rels)
				right := e.evaluateExpressionWithContext(ctx, strings.TrimSpace(parts[1]), nodes, rels)
				return compareWithOperator(left, right, op)
			}
		}
	}

	// Handle IS NULL / IS NOT NULL
	if strings.HasSuffix(upper, " IS NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-8])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return val == nil
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-12])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return val != nil
	}

	// Handle CONTAINS, STARTS WITH, ENDS WITH string predicates
	containsIdx := findTopLevelKeyword(condition, " CONTAINS ")
	if containsIdx > 0 {
		left := strings.TrimSpace(condition[:containsIdx])
		right := strings.TrimSpace(condition[containsIdx+10:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.Contains(leftStr, rightStr)
		}
		return false
	}
	if idx := findTopLevelKeyword(condition, " STARTS WITH "); idx > 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+13:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.HasPrefix(leftStr, rightStr)
		}
		return false
	}
	if idx := findTopLevelKeyword(condition, " ENDS WITH "); idx > 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+11:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.HasSuffix(leftStr, rightStr)
		}
		return false
	}

	// Handle label check: n:Label (returns true if node has the label)
	if colonIdx := strings.Index(condition, ":"); colonIdx > 0 {
		variable := strings.TrimSpace(condition[:colonIdx])
		label := strings.TrimSpace(condition[colonIdx+1:])
		// Check if this is a simple variable:Label pattern (no operators)
		if len(variable) > 0 && len(label) > 0 && !strings.ContainsAny(variable, " .(") && !strings.ContainsAny(label, " .(") {
			if node, ok := nodes[variable]; ok {
				for _, l := range node.Labels {
					if l == label {
						return true
					}
				}
				return false
			}
		}
	}

	// Otherwise evaluate as expression and check truthiness
	result := e.evaluateExpressionWithContext(ctx, condition, nodes, rels)
	return isTruthy(result)
}

// findTopLevelKeyword finds a keyword at the top level (not inside parentheses or strings)
// Returns the byte index in the original string, or -1 if not found.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func findTopLevelKeyword(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	depth := 0
	inString := false
	var stringChar byte
	for i := 0; i+len(keyword) <= len(s); i++ {
		ch := s[i]
		if ch == '\'' || ch == '"' {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.EqualFold(s[i:i+len(keyword)], keyword) {
			return i
		}
	}
	return -1
}

// compareValues compares two values for equality (used in simple CASE).
func compareValues(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	// Try numeric comparison
	numA, okA := toFloat64(a)
	numB, okB := toFloat64(b)
	if okA && okB {
		return numA == numB
	}

	// String comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// compareWithOperator compares two values using the given operator.
func compareWithOperator(left, right interface{}, op string) bool {
	// Handle NULL comparisons
	if left == nil || right == nil {
		switch op {
		case "=":
			return left == nil && right == nil
		case "<>":
			return !(left == nil && right == nil)
		default:
			return false // NULL comparisons with <, >, etc. are false
		}
	}

	// Try numeric comparison
	numLeft, okLeft := toFloat64(left)
	numRight, okRight := toFloat64(right)
	if okLeft && okRight {
		switch op {
		case "<":
			return numLeft < numRight
		case ">":
			return numLeft > numRight
		case "<=":
			return numLeft <= numRight
		case ">=":
			return numLeft >= numRight
		case "=":
			return numLeft == numRight
		case "<>":
			return numLeft != numRight
		}
	}

	// String comparison
	strLeft := fmt.Sprintf("%v", left)
	strRight := fmt.Sprintf("%v", right)
	switch op {
	case "<":
		return strLeft < strRight
	case ">":
		return strLeft > strRight
	case "<=":
		return strLeft <= strRight
	case ">=":
		return strLeft >= strRight
	case "=":
		return strLeft == strRight
	case "<>":
		return strLeft != strRight
	}

	return false
}

// isTruthy checks if a value is considered true in a boolean context.
func isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if num, ok := toFloat64(val); ok {
		return num != 0
	}
	if str, ok := val.(string); ok {
		return str != ""
	}
	return true
}

// indexCaseInsensitive finds the index of a keyword in a case-insensitive manner.
func indexCaseInsensitive(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	for i := 0; i+len(keyword) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(keyword)], keyword) {
			return i
		}
	}
	return -1
}

// splitByKeyword splits a string by a keyword, respecting string literals and nested expressions.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func splitByKeyword(s, keyword string) []string {
	var result []string
	var current strings.Builder
	var inString bool
	var stringChar byte
	var parenDepth int

	keywordLen := len(keyword)
	for i := 0; i < len(s); i++ {
		ch := s[i]

		// Track string literals
		if ch == '\'' || ch == '"' {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			current.WriteByte(ch)
			continue
		}

		// Track parentheses depth
		if !inString {
			if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
			}
		}

		// Check for keyword at current position
		if !inString && parenDepth == 0 && i+keywordLen <= len(s) {
			if strings.EqualFold(s[i:i+keywordLen], keyword) {
				// Check word boundary (not part of a longer word)
				validStart := i == 0 || !isAlphaNumeric(rune(s[i-1]))
				endPos := i + keywordLen
				validEnd := endPos >= len(s) || !isAlphaNumeric(rune(s[endPos]))

				if validStart && validEnd {
					// Found keyword at word boundary
					result = append(result, current.String())
					current.Reset()
					// Skip to the byte after keyword
					i = endPos - 1 // -1 because loop increments
					continue
				}
			}
		}

		current.WriteByte(ch)
	}

	// Add remaining content
	result = append(result, current.String())
	return result
}

// isAlphaNumeric checks if a character is alphanumeric or underscore.
func isAlphaNumeric(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}
