package cypher

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var compiledSimpleWhereCache sync.Map // map[string]func(*storage.Node) bool

func (e *StorageExecutor) filterNodes(ctx context.Context, nodes []*storage.Node, variable, whereClause string) []*storage.Node {
	if fastIN, ok := e.buildBoundInFastFilter(variable, whereClause); ok {
		return parallelFilterNodes(nodes, fastIN)
	}

	if compiled, ok := e.getCompiledSimpleWhere(ctx, variable, whereClause); ok {
		return parallelFilterNodes(nodes, compiled)
	}

	// Create filter function for parallel execution
	filterFn := func(node *storage.Node) bool {
		return e.evaluateWhere(ctx, node, variable, whereClause)
	}

	// Use parallel filtering for large datasets
	return parallelFilterNodes(nodes, filterFn)
}

// buildBoundInFastFilter compiles a high-frequency correlated predicate:
//
//	<var>.<prop> IN <bindingIdent>
//
// where bindingIdent resolves from fabricRecordBindings to a slice.
// This avoids per-row expression parsing/evaluation and converts membership checks
// to O(1) hash lookups for comparable keys.
func (e *StorageExecutor) buildBoundInFastFilter(variable, whereClause string) (FilterFunc, bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" || containsFold(clause, " AND ") || containsFold(clause, " OR ") || hasPrefixFold(clause, "NOT ") {
		return nil, false
	}

	inIdx := findTopLevelKeyword(clause, " IN ")
	if inIdx <= 0 {
		return nil, false
	}

	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+4:])
	varPrefix := variable + "."
	if !strings.HasPrefix(left, varPrefix) {
		return nil, false
	}

	propName := strings.TrimSpace(left[len(varPrefix):])
	if propName == "" || strings.ContainsAny(propName, " \t\r\n") {
		return nil, false
	}
	if strings.HasPrefix(right, "$") {
		right = strings.TrimSpace(right[1:])
	}
	if !isValidIdentifier(right) {
		return nil, false
	}

	boundList, ok := e.fabricRecordBindings[right]
	if !ok {
		return nil, false
	}
	items, ok := toInterfaceSlice(boundList)
	if !ok {
		return nil, false
	}

	comparableSet, nonComparable := buildComparableMembershipIndex(items)
	return func(node *storage.Node) bool {
		actual, exists := node.Properties[propName]
		if !exists || actual == nil {
			return false
		}
		return evaluateComparableMembership(actual, comparableSet, nonComparable, e.compareEqual)
	}, true
}

func (e *StorageExecutor) getCompiledSimpleWhere(ctx context.Context, variable, whereClause string) (func(*storage.Node) bool, bool) {
	key := variable + "\x00" + strings.TrimSpace(whereClause)
	if cached, ok := compiledSimpleWhereCache.Load(key); ok {
		if fn, okFn := cached.(func(*storage.Node) bool); okFn {
			return fn, true
		}
	}
	fn, ok := e.compileSimpleWhere(ctx, variable, whereClause)
	if ok {
		compiledSimpleWhereCache.Store(key, fn)
	}
	return fn, ok
}

func (e *StorageExecutor) compileSimpleWhere(ctx context.Context, variable, whereClause string) (func(*storage.Node) bool, bool) {
	whereClause = strings.TrimSpace(whereClause)

	// Keep this fast path strict to avoid semantic drift.
	if containsFold(whereClause, " AND ") ||
		containsFold(whereClause, " OR ") ||
		hasPrefixFold(whereClause, "NOT ") ||
		containsFold(whereClause, " CONTAINS ") ||
		containsFold(whereClause, " STARTS WITH ") ||
		containsFold(whereClause, " ENDS WITH ") ||
		strings.Contains(whereClause, "(") ||
		strings.Contains(whereClause, ")") {
		return nil, false
	}

	getProp := func(node *storage.Node, propName string) (any, bool) {
		return getNodePropertyValue(node, propName)
	}

	const prefixSep = "."
	varPrefix := variable + prefixSep

	// Simple IN fast-path: <var>.<prop> IN [<literal-list>]
	// Keeps semantics for the common membership predicate while avoiding
	// per-row expression parsing/evaluation.
	if inIdx := findTopLevelKeyword(whereClause, " IN "); inIdx > 0 {
		left := strings.TrimSpace(whereClause[:inIdx])
		right := strings.TrimSpace(whereClause[inIdx+4:])
		if strings.HasPrefix(left, varPrefix) {
			prop := strings.TrimSpace(left[len(varPrefix):])
			if prop != "" && !strings.ContainsAny(prop, " \t\r\n") {
				var listVal interface{}
				if isValidIdentifier(right) && len(e.fabricRecordBindings) > 0 {
					listVal = e.fabricRecordBindings[right]
				} else {
					listVal = e.parseValue(ctx, right)
				}
				if items, ok := toInterfaceSlice(listVal); ok {
					comparableSet, nonComparable := buildComparableMembershipIndex(items)
					return func(node *storage.Node) bool {
						actual, exists := getProp(node, prop)
						if !exists || actual == nil {
							return false
						}
						return evaluateComparableMembership(actual, comparableSet, nonComparable, e.compareEqual)
					}, true
				}
			}
		}
	}

	if hasSuffixFold(whereClause, " IS NOT NULL") {
		left := strings.TrimSpace(whereClause[:len(whereClause)-len(" IS NOT NULL")])
		if !strings.HasPrefix(left, varPrefix) {
			return nil, false
		}
		prop := strings.TrimSpace(left[len(varPrefix):])
		if prop == "" || strings.ContainsAny(prop, " \t\r\n") {
			return nil, false
		}
		return func(node *storage.Node) bool {
			val, exists := getProp(node, prop)
			return exists && val != nil
		}, true
	}

	if hasSuffixFold(whereClause, " IS NULL") {
		left := strings.TrimSpace(whereClause[:len(whereClause)-len(" IS NULL")])
		if !strings.HasPrefix(left, varPrefix) {
			return nil, false
		}
		prop := strings.TrimSpace(left[len(varPrefix):])
		if prop == "" || strings.ContainsAny(prop, " \t\r\n") {
			return nil, false
		}
		return func(node *storage.Node) bool {
			val, exists := getProp(node, prop)
			return !exists || val == nil
		}, true
	}

	// Restrict fast-path binary compilation to plain equality/inequality only.
	// Regex and comparison operators must use the full evaluator to preserve
	// Cypher semantics (e.g. "=~", "<", ">", "<=", ">="). Keep "<>"/"!="
	// in fast-path because they are simple inequality checks.
	hasNotEqual := e.hasOperatorOutsideQuotes(whereClause, "<>")
	hasLessEq := e.hasOperatorOutsideQuotes(whereClause, "<=")
	hasGreaterEq := e.hasOperatorOutsideQuotes(whereClause, ">=")
	hasBareLess := e.hasOperatorOutsideQuotes(whereClause, "<") && !hasNotEqual && !hasLessEq
	hasBareGreater := e.hasOperatorOutsideQuotes(whereClause, ">") && !hasNotEqual && !hasGreaterEq
	if e.hasOperatorOutsideQuotes(whereClause, "=~") ||
		hasLessEq ||
		hasGreaterEq ||
		hasBareLess ||
		hasBareGreater {
		return nil, false
	}

	parseBinary := func(op string, neg bool) (func(*storage.Node) bool, bool) {
		idx := strings.Index(whereClause, op)
		if idx < 0 {
			return nil, false
		}
		left := strings.TrimSpace(whereClause[:idx])
		right := strings.TrimSpace(whereClause[idx+len(op):])
		if !strings.HasPrefix(left, varPrefix) || right == "" {
			return nil, false
		}
		prop := strings.TrimSpace(left[len(varPrefix):])
		if prop == "" || strings.ContainsAny(prop, " \t\r\n") {
			return nil, false
		}
		expected := e.parseValue(ctx, right)
		return func(node *storage.Node) bool {
			actual, exists := getProp(node, prop)
			if !exists {
				return false
			}
			eq := e.compareEqual(actual, expected)
			if neg {
				return !eq
			}
			return eq
		}, true
	}

	// Order matters so we don't mis-split on "<>" / "!=".
	if fn, ok := parseBinary("<>", true); ok {
		return fn, true
	}
	if fn, ok := parseBinary("!=", true); ok {
		return fn, true
	}
	if fn, ok := parseBinary("=", false); ok {
		return fn, true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateWhere(ctx context.Context, node *storage.Node, variable, whereClause string) bool {
	whereClause = strings.TrimSpace(whereClause)

	// Handle parenthesized expressions - strip outer parens and recurse
	if strings.HasPrefix(whereClause, "(") && strings.HasSuffix(whereClause, ")") {
		// Verify these are matching outer parens, not separate groups
		depth := 0
		isOuterParen := true
		for i, ch := range whereClause {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			// If depth goes to 0 before the last char, these aren't outer parens
			if depth == 0 && i < len(whereClause)-1 {
				isOuterParen = false
				break
			}
		}
		if isOuterParen {
			return e.evaluateWhere(ctx, node, variable, whereClause[1:len(whereClause)-1])
		}
	}

	// CRITICAL: Handle AND/OR at top level FIRST before subqueries
	// This ensures "EXISTS {} AND COUNT {} >= 2" is properly split
	if andIdx := findTopLevelKeyword(whereClause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateWhere(ctx, node, variable, left) && e.evaluateWhere(ctx, node, variable, right)
	}

	// Handle OR at top level only.
	// Optimize catch-all patterns: ($p = '' OR field STARTS WITH $p)
	// When one branch is a tautology (e.g. '' = ''), short-circuit to true
	// without evaluating the other branch.
	if orIdx := findTopLevelKeyword(whereClause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(whereClause[:orIdx])
		right := strings.TrimSpace(whereClause[orIdx+4:])
		// Fast tautology check: if either branch is a constant-true equality
		// (e.g. '' = '' or 'x' = 'x'), the whole OR is true.
		if isConstantTrueEquality(left) || isConstantTrueEquality(right) {
			return true
		}
		return e.evaluateWhere(ctx, node, variable, left) || e.evaluateWhere(ctx, node, variable, right)
	}

	// Handle NOT EXISTS { } subquery FIRST (before other NOT handling)
	// Uses regex for whitespace-flexible matching
	if hasSubqueryPattern(whereClause, notExistsSubqueryRe) {
		return e.evaluateNotExistsSubquery(ctx, node, variable, whereClause)
	}

	// Handle EXISTS { } subquery (whitespace-flexible)
	if hasSubqueryPattern(whereClause, existsSubqueryRe) {
		return e.evaluateExistsSubquery(ctx, node, variable, whereClause)
	}

	// Handle COUNT { } subquery with comparison (whitespace-flexible)
	if hasSubqueryPattern(whereClause, countSubqueryRe) {
		return e.evaluateCountSubqueryComparison(node, variable, whereClause)
	}

	// Handle NOT prefix
	if hasPrefixFold(whereClause, "NOT ") {
		inner := strings.TrimSpace(whereClause[4:])
		return !e.evaluateWhere(ctx, node, variable, inner)
	}

	// Handle label check: n:Label or variable:Label
	if colonIdx := strings.Index(whereClause, ":"); colonIdx > 0 {
		labelVar := strings.TrimSpace(whereClause[:colonIdx])
		labelName := strings.TrimSpace(whereClause[colonIdx+1:])
		// Check if this looks like a simple variable:Label pattern
		if len(labelVar) > 0 && len(labelName) > 0 &&
			!strings.ContainsAny(labelVar, " .(") &&
			!strings.ContainsAny(labelName, " .(=<>") {
			// If the variable matches our node variable, check the label
			if labelVar == variable {
				for _, l := range node.Labels {
					if l == labelName {
						return true
					}
				}
				return false
			}
		}
	}

	// Handle string operators (case-insensitive check)
	if containsFold(whereClause, " CONTAINS ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "CONTAINS")
	}
	if containsFold(whereClause, " STARTS WITH ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "STARTS WITH")
	}
	if containsFold(whereClause, " ENDS WITH ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "ENDS WITH")
	}
	if containsFold(whereClause, " IN ") {
		return e.evaluateInOp(ctx, node, variable, whereClause)
	}
	if containsFold(whereClause, " IS NULL") {
		return e.evaluateIsNull(ctx, node, variable, whereClause, false)
	}
	if containsFold(whereClause, " IS NOT NULL") {
		return e.evaluateIsNull(ctx, node, variable, whereClause, true)
	}

	// Handle relationship patterns like (n)-[:TYPE]->() that may appear as "n)-[:TYPE]->()"
	// after stripping outer parens from NOT (n)-[:TYPE]->(). Must run BEFORE operator check
	// so "->" is not misinterpreted as ">" comparison.
	hasRelPattern := (strings.Contains(whereClause, "-[") && (strings.Contains(whereClause, "]->") || strings.Contains(whereClause, "<-")))
	refsVar := strings.Contains(whereClause, "("+variable+")") || strings.Contains(whereClause, "("+variable+":") ||
		strings.HasPrefix(whereClause, variable+")") || strings.HasPrefix(whereClause, variable+":")
	if hasRelPattern && refsVar {
		pattern := whereClause
		if strings.HasPrefix(whereClause, variable+")") || strings.HasPrefix(whereClause, variable+":") {
			pattern = "(" + whereClause // restore (n)-[:TYPE]->() for relationship check
		}
		return e.evaluateRelationshipPatternInWhere(node, variable, pattern)
	}

	// Determine operator and split accordingly
	var op string
	var opIdx int

	// Check operators in order of length (longest first to avoid partial matches)
	operators := []string{"<>", "!=", ">=", "<=", "=~", ">", "<", "="}
	for _, testOp := range operators {
		idx := strings.Index(whereClause, testOp)
		if idx >= 0 {
			op = testOp
			opIdx = idx
			break
		}
	}

	if op == "" {
		// No comparison operator - may be a boolean expression (e.g. exists(n.prop))
		return e.evaluateWhereAsBoolean(ctx, whereClause, variable, node)
	}

	left := strings.TrimSpace(whereClause[:opIdx])
	right := strings.TrimSpace(whereClause[opIdx+len(op):])

	// Handle id(variable) = value comparisons
	if hasPrefixFold(left, "id(") && strings.HasSuffix(left, ")") {
		// Extract variable name from id(varName)
		idVar := strings.TrimSpace(left[3 : len(left)-1])
		if idVar == variable {
			// Normalize expected value once to raw internal ID for comparison.
			expectedVal := normalizeNodeIDValue(e.parseValue(ctx, right))
			actualId := string(node.ID)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

	// Handle elementId(variable) = value comparisons
	if hasPrefixFold(left, "elementid(") && strings.HasSuffix(left, ")") {
		// Extract variable name from elementId(varName)
		idVar := strings.TrimSpace(left[10 : len(left)-1])
		if idVar == variable {
			// Normalize expected value once to raw internal ID for comparison.
			expectedVal := normalizeNodeIDValue(e.parseValue(ctx, right))
			actualId := string(node.ID)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

	// Extract property from left side (e.g., "n.name")
	if !strings.HasPrefix(left, variable+".") {
		// Left is not variable.prop (e.g. size(n.content), id(n)) - evaluate full expression
		return e.evaluateWhereAsBoolean(ctx, whereClause, variable, node)
	}

	propName := left[len(variable)+1:]

	// Get actual value - check EmbedMeta first for embedding metadata
	var actualVal any
	var exists bool
	if propName == "has_embedding" {
		// Check EmbedMeta first, then fall back to ChunkEmbeddings
		if node.EmbedMeta != nil {
			actualVal, exists = node.EmbedMeta["has_embedding"]
		}
		if !exists {
			// Fall back to native embedding field
			actualVal = len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0
			exists = true
		}
	} else {
		actualVal, exists = node.Properties[propName]
	}
	if !exists {
		return false
	}

	// Parse expected value (fast-path for correlated Fabric bind identifiers).
	var expectedVal any
	if len(e.fabricRecordBindings) > 0 && isValidIdentifier(right) {
		if bound, ok := e.fabricRecordBindings[right]; ok {
			expectedVal = bound
		}
	}
	if expectedVal == nil {
		expectedVal = e.parseValue(ctx, right)
	}

	// Perform comparison based on operator
	switch op {
	case "=":
		return e.compareEqual(actualVal, expectedVal)
	case "<>", "!=":
		return !e.compareEqual(actualVal, expectedVal)
	case ">":
		return e.compareGreater(actualVal, expectedVal)
	case ">=":
		return e.compareGreater(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
	case "<":
		return e.compareLess(actualVal, expectedVal)
	case "<=":
		return e.compareLess(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
	case "=~":
		return e.compareRegex(actualVal, expectedVal)
	default:
		return true
	}
}

func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func hasSuffixFold(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return strings.EqualFold(s[len(s)-len(suffix):], suffix)
}

func containsFold(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	if len(s) < len(sub) {
		return false
	}
	max := len(s) - len(sub)
	for i := 0; i <= max; i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

// normalizeNodeIDValue converts canonical element ID strings to raw internal IDs.
// Examples:
// - "4:nornicdb:abc-123" -> "abc-123"
// - "abc-123" -> "abc-123"
func normalizeNodeIDValue(v interface{}) interface{} {
	s, ok := v.(string)
	if !ok {
		return v
	}
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ":", 3)
	if len(parts) == 3 && parts[0] == "4" {
		return parts[2]
	}
	return s
}

// evaluateWhereAsBoolean evaluates a WHERE expression (e.g. size(n.content) > 10000, exists(n.prop))
// using the expression evaluator and returns a boolean. Used when evaluateWhere does not handle
// the condition as id(), elementId(), or variable.property.
func (e *StorageExecutor) evaluateWhereAsBoolean(ctx context.Context, whereClause, variable string, node *storage.Node) bool {
	nodes := map[string]*storage.Node{variable: node}
	result := e.evaluateExpressionWithContext(ctx, whereClause, nodes, nil)
	switch v := result.(type) {
	case bool:
		return v
	case nil:
		return false
	case int64:
		return v != 0
	case float64:
		return v != 0
	case int:
		return v != 0
	default:
		// Non-empty string, etc. - treat as true
		return result != nil
	}
}

// parseValue extracts the actual value from a Cypher literal
func (e *StorageExecutor) parseValue(ctx context.Context, s string) interface{} {
	s = strings.TrimSpace(s)

	// Handle arrays: [0.1, 0.2, 0.3]
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return e.parseArrayValue(ctx, s)
	}
	// Handle map literals: {key: value}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		return e.parseProperties(ctx, s)
	}

	// Handle quoted strings with escape sequence support
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		if decoded, ok := decodeCypherQuotedString(s); ok {
			return decoded
		}
	}

	// Handle booleans
	upper := strings.ToUpper(s)
	if upper == "TRUE" {
		return true
	}
	if upper == "FALSE" {
		return false
	}
	if upper == "NULL" {
		return nil
	}

	// Handle numbers - preserve int64 for integers, use float64 only for decimals
	// The comparison functions use toFloat64() which handles both types
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i // Keep as int64 for Neo4j compatibility
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	// Fabric correlated bindings: resolve bare identifier values from outer record context.
	if len(e.fabricRecordBindings) > 0 {
		isIdent := true
		for i, ch := range s {
			if i == 0 {
				if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
					isIdent = false
					break
				}
			} else {
				if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
					isIdent = false
					break
				}
			}
		}
		if isIdent {
			if v, ok := e.fabricRecordBindings[s]; ok {
				return v
			}
		}
	}

	return s
}

func cloneStringAnyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (e *StorageExecutor) resolveReturnItem(ctx context.Context, item returnItem, variable string, node *storage.Node) interface{} {
	expr := item.expr

	// Handle wildcard - return the whole node (Neo4j compatible: return *storage.Node)
	if expr == "*" || expr == variable {
		return node
	}

	// Check for COLLECT { } subquery FIRST (before other function checks)
	// This is a Neo4j 5.0+ feature that executes a subquery and collects results
	if hasSubqueryPattern(expr, collectSubqueryRe) {
		// We need context to execute the subquery, but resolveReturnItem doesn't have it
		// Return a placeholder that will be handled by the caller
		// This is a limitation - we'll need to handle collect { } at a higher level
		// For now, return nil and handle it in the calling code
		return nil // Will be handled by evaluateCollectSubquery in calling code
	}

	// Check for CASE expression FIRST (before property access check)
	// CASE expressions contain dots (like p.age) but should not be treated as property access
	if isCaseExpression(expr) {
		return e.evaluateExpression(ctx, expr, variable, node)
	}

	// Check for function calls - these should be evaluated, not treated as property access
	// e.g., coalesce(p.nickname, p.name), toString(p.age), etc.
	if strings.Contains(expr, "(") {
		return e.evaluateExpression(ctx, expr, variable, node)
	}

	// Check for IS NULL / IS NOT NULL - these need full evaluation
	upperExpr := strings.ToUpper(expr)
	if strings.Contains(upperExpr, " IS NULL") || strings.Contains(upperExpr, " IS NOT NULL") {
		return e.evaluateExpression(ctx, expr, variable, node)
	}

	// Check for arithmetic operators - need full evaluation
	if strings.ContainsAny(expr, "+-*/%") {
		return e.evaluateExpression(ctx, expr, variable, node)
	}

	// Handle property access: variable.property
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		varName := strings.TrimSpace(parts[0])
		propName := strings.TrimSpace(parts[1])

		// Check if variable matches
		if varName != variable {
			// Different variable - return nil (variable not in scope)
			return nil
		}

		// Handle special "id" property - return node's internal ID
		if propName == "id" {
			// Check if there's an "id" property first
			if val, ok := node.Properties["id"]; ok {
				return val
			}
			// Fall back to internal node ID
			return string(node.ID)
		}

		// has_embedding is stored in EmbedMeta by the managed embedding system
		// This supports queries like: WHERE f.has_embedding = true
		if propName == "has_embedding" {
			if node.EmbedMeta != nil {
				if val, ok := node.EmbedMeta["has_embedding"]; ok {
					return val
				}
			}
			return len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0
		}

		// Regular property access
		if val, ok := node.Properties[propName]; ok {
			return val
		}
		return nil
	}

	// Use the comprehensive expression evaluator for all expressions
	// This supports: id(n), labels(n), keys(n), properties(n), literals, etc.
	result := e.evaluateExpression(ctx, expr, variable, node)

	// If the result is just the expression string unchanged, return nil
	// (expression wasn't recognized/evaluated)
	if str, ok := result.(string); ok && str == expr && !strings.HasPrefix(expr, "'") && !strings.HasPrefix(expr, "\"") {
		return nil
	}

	return result
}

// isConstantTrueEquality returns true if the expression is a constant equality
// that always evaluates to true, e.g. ” = ” or 'x' = 'x' or "" = "".
// This is used to short-circuit OR branches when one side is a tautology
// (common in optional predicate patterns like $p = ” OR field STARTS WITH $p
// where $p has been substituted to ”).
func isConstantTrueEquality(expr string) bool {
	expr = strings.TrimSpace(expr)
	// Strip outer parens.
	for strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == expr {
			break
		}
		expr = inner
	}
	eqIdx := strings.Index(expr, "=")
	if eqIdx <= 0 || eqIdx >= len(expr)-1 {
		return false
	}
	// Reject !=, <=, >=, ==
	if eqIdx > 0 && (expr[eqIdx-1] == '!' || expr[eqIdx-1] == '<' || expr[eqIdx-1] == '>') {
		return false
	}
	if eqIdx+1 < len(expr) && expr[eqIdx+1] == '=' {
		return false
	}
	left := strings.TrimSpace(expr[:eqIdx])
	right := strings.TrimSpace(expr[eqIdx+1:])
	if left == "" || right == "" {
		return false
	}
	// Both sides must be identical quoted strings or identical unquoted literals.
	if left == right {
		// e.g. '' = '' or "" = "" or someValue = someValue (after param substitution)
		return true
	}
	return false
}
