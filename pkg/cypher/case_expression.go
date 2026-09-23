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
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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

// leadingCaseExpressionEnd returns the exclusive end of a leading CASE ... END
// expression, including nested CASE expressions. A trailing operator or
// postfix expression is deliberately excluded so callers can treat CASE as an
// atomic operand without allowing operators inside WHEN predicates to escape.
func leadingCaseExpressionEnd(expr string) int {
	expr = strings.TrimSpace(expr)
	depth := 0
	for index := 0; index < len(expr); {
		if expr[index] == '\'' || expr[index] == '"' || expr[index] == '`' {
			index = numericValidationSkipQuoted(expr, index)
			continue
		}
		word, next, ok := scanIdentifierToken(expr, index)
		if !ok {
			index++
			continue
		}
		switch {
		case strings.EqualFold(word, "case"):
			depth++
		case strings.EqualFold(word, "end") && depth > 0:
			depth--
			if depth == 0 {
				return next
			}
		}
		index = next
	}
	return -1
}

// parseCaseExpression parses a CASE expression into its components.
// Supports both searched and simple CASE expressions.
func parseCaseExpression(expr string) (*caseExpression, error) {
	expr = strings.TrimSpace(expr)
	upper := strings.ToUpper(expr)

	// Remove CASE and END keywords
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, localizedError(localization.CypherCoreCaseEnvelopeInvalid(), nil)
	}

	// Extract the content between CASE and END
	content := strings.TrimSpace(expr[4 : len(expr)-3])

	ce := &caseExpression{
		whenClauses: []caseWhenClause{},
	}

	// Only keywords at the current CASE level delimit the outer expression.
	// A plain string search incorrectly treats WHEN/ELSE from a nested CASE as
	// belonging to its parent.
	firstWhenIdx := findCaseKeywordAtLevel(content, 0, "WHEN")
	if firstWhenIdx == -1 {
		return nil, localizedError(localization.CypherCoreCaseWhenRequired(), nil)
	}

	beforeFirstWhen := strings.TrimSpace(content[:firstWhenIdx])
	if beforeFirstWhen != "" {
		// Simple CASE: CASE expression WHEN value THEN result ...
		ce.isSimple = true
		ce.testExpression = beforeFirstWhen
	}

	for cursor := firstWhenIdx; cursor < len(content); {
		conditionStart := cursor + len("WHEN")
		thenIndex := findCaseKeywordAtLevel(content, conditionStart, "THEN")
		if thenIndex < 0 {
			return nil, localizedError(localization.CypherCoreCaseThenRequired(content[cursor:]), nil)
		}
		condition := strings.TrimSpace(content[conditionStart:thenIndex])
		resultStart := thenIndex + len("THEN")
		nextWhen := findCaseKeywordAtLevel(content, resultStart, "WHEN")
		nextElse := findCaseKeywordAtLevel(content, resultStart, "ELSE")
		resultEnd := len(content)
		next := len(content)
		if nextWhen >= 0 && nextWhen < resultEnd {
			resultEnd, next = nextWhen, nextWhen
		}
		if nextElse >= 0 && nextElse < resultEnd {
			resultEnd, next = nextElse, nextElse
		}
		result := strings.TrimSpace(content[resultStart:resultEnd])
		if condition == "" || result == "" {
			return nil, localizedError(localization.CypherCoreCaseThenRequired(content[cursor:]), nil)
		}
		clause := caseWhenClause{result: result}
		if ce.isSimple {
			clause.value = condition
		} else {
			clause.condition = condition
		}
		ce.whenClauses = append(ce.whenClauses, clause)

		if next == len(content) {
			break
		}
		if next == nextElse {
			ce.elseResult = strings.TrimSpace(content[nextElse+len("ELSE"):])
			break
		}
		cursor = nextWhen
	}

	if len(ce.whenClauses) == 0 {
		return nil, localizedError(localization.CypherCoreCaseWhenRequired(), nil)
	}

	return ce, nil
}

// findCaseKeywordAtLevel finds a CASE grammar keyword while ignoring quoted
// text, nested delimiters, and complete nested CASE ... END expressions.
func findCaseKeywordAtLevel(expression string, start int, keyword string) int {
	parenDepth, bracketDepth, braceDepth, caseDepth := 0, 0, 0, 0
	for index := start; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' || expression[index] == '`' {
			index = numericValidationSkipQuoted(expression, index)
			continue
		}
		switch expression[index] {
		case '(':
			parenDepth++
			index++
			continue
		case ')':
			parenDepth--
			index++
			continue
		case '[':
			bracketDepth++
			index++
			continue
		case ']':
			bracketDepth--
			index++
			continue
		case '{':
			braceDepth++
			index++
			continue
		case '}':
			braceDepth--
			index++
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || !isCaseWordStart(expression, index) {
			index++
			continue
		}
		end := index + 1
		for end < len(expression) && isNumericIdentifierByte(expression[end]) {
			end++
		}
		word := expression[index:end]
		if strings.EqualFold(word, "CASE") {
			caseDepth++
		} else if strings.EqualFold(word, "END") && caseDepth > 0 {
			caseDepth--
		} else if caseDepth == 0 && strings.EqualFold(word, keyword) {
			return index
		}
		index = end
	}
	return -1
}

func isCaseWordStart(expression string, index int) bool {
	return (index == 0 || !isNumericIdentifierByte(expression[index-1])) &&
		isASCIIIdentifierStart(expression[index])
}

// parseWhenClause parses a single WHEN ... THEN ... clause.
func parseWhenClause(section string, isSimple bool) (caseWhenClause, error) {
	// Find THEN keyword
	thenIdx := indexCaseInsensitive(section, "THEN")
	if thenIdx == -1 {
		return caseWhenClause{}, localizedError(localization.CypherCoreCaseThenRequired(section), nil)
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

	resolveComparisonOperand := func(operand string) interface{} {
		return e.evaluateExpressionWithContext(ctx, operand, nodes, rels)
	}
	if result, ok := evaluateComparisonChain(condition, resolveComparisonOperand, compareCypherPredicateValue); ok {
		matched, _ := result.(bool)
		return matched
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

// compareValues applies Cypher equality without coercing values across type
// families. Integer and floating-point values share Cypher's numeric family,
// while strings, booleans, and composite values retain their types.
func compareValues(a, b interface{}) bool {
	if a == nil || b == nil {
		return false
	}

	if equal, numeric := cypherNumericEquality(a, b); numeric {
		return equal
	}

	return reflect.DeepEqual(a, b)
}

func strictNumericValue(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
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
	if equal, temporal := compareTemporalValues(left, right); temporal {
		switch op {
		case "=":
			return equal
		case "<>":
			return !equal
		}
	}
	if comparison, temporal := compareTemporalOrdering(left, right); temporal {
		switch op {
		case "<":
			return comparison < 0
		case ">":
			return comparison > 0
		case "<=":
			return comparison <= 0
		case ">=":
			return comparison >= 0
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
