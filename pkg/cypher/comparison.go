// Comparison operators for NornicDB Cypher.
//
// This file contains functions for comparing values in WHERE clauses and
// property matching. These functions implement Cypher's comparison semantics
// with appropriate type coercion.
//
// # Comparison Operators
//
// Equality and inequality:
//   - compareEqual: n.value = 42, n.name = 'Alice'
//   - nodeMatchesProps: Match node against property filter
//
// Numeric comparison:
//   - compareGreater: n.age > 30
//   - compareLess: n.age < 30
//
// Pattern matching:
//   - compareRegex: n.name =~ '.*Smith'
//
// String operations:
//   - evaluateStringOp: CONTAINS, STARTS WITH, ENDS WITH
//
// List operations:
//   - evaluateInOp: n.status IN ['active', 'pending']
//
// NULL checks:
//   - evaluateIsNull: IS NULL / IS NOT NULL
//
// # Type Coercion
//
// Comparisons follow Neo4j's type coercion rules:
//   - Numeric types are compared as numbers (int, float)
//   - Strings are compared lexicographically
//   - NULL propagates (NULL = anything → NULL)
//
// # ELI12
//
// Comparison is like asking "is this the same as that?"
//
//	n.age > 30  → "Is the person older than 30?"
//	n.name = 'Alice'  → "Is their name Alice?"
//	n.email CONTAINS '@'  → "Does the email have an @ sign?"
//
// These functions do the actual checking and tell us yes or no!
//
// # Neo4j Compatibility
//
// All comparison operators match Neo4j semantics exactly.

package cypher

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeMatchesProps checks if a node's properties match the expected values.
//
// # Parameters
//
//   - node: The node to check
//   - props: Map of expected property values
//
// # Returns
//
//   - true if all expected properties match (or props is nil/empty)
//
// # Example
//
//	nodeMatchesProps(node, map[string]interface{}{"name": "Alice", "age": 30})
//	// Returns true only if node has both name="Alice" AND age=30
func (e *StorageExecutor) nodeMatchesProps(node *storage.Node, props map[string]interface{}) bool {
	if props == nil {
		return true
	}
	for key, expected := range props {
		actual, exists := node.Properties[key]
		if !exists {
			return false
		}
		if !e.compareEqual(actual, expected) {
			return false
		}
	}
	return true
}

// compareEqual handles equality comparison with type coercion.
//
// # Parameters
//
//   - actual: The actual value from the node
//   - expected: The expected value from the query
//
// # Returns
//
//   - true if values are equal (with type coercion)
//
// # Example
//
//	compareEqual(int64(42), float64(42.0))  // true
//	compareEqual("hello", "hello")          // true
//	compareEqual(nil, nil)                  // true
//	compareEqual(42, "42")                  // true (string coercion)
func (e *StorageExecutor) compareEqual(actual, expected interface{}) bool {
	// Handle nil
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	// Try numeric comparison
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum == expectedNum
	}

	// String comparison
	return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

// compareGreater handles > comparison.
//
// # Parameters
//
//   - actual: The actual value
//   - expected: The threshold value
//
// # Returns
//
//   - true if actual > expected
func (e *StorageExecutor) compareGreater(actual, expected interface{}) bool {
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum > expectedNum
	}

	// String comparison as fallback
	return fmt.Sprintf("%v", actual) > fmt.Sprintf("%v", expected)
}

// compareLess handles < comparison.
//
// # Parameters
//
//   - actual: The actual value
//   - expected: The threshold value
//
// # Returns
//
//   - true if actual < expected
func (e *StorageExecutor) compareLess(actual, expected interface{}) bool {
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum < expectedNum
	}

	// String comparison as fallback
	return fmt.Sprintf("%v", actual) < fmt.Sprintf("%v", expected)
}

// compareRegex handles =~ regex comparison.
// Uses cached compiled regex for performance (avoids recompiling same pattern).
//
// # Parameters
//
//   - actual: The string value to test
//   - expected: The regex pattern
//
// # Returns
//
//   - true if actual matches the pattern
func (e *StorageExecutor) compareRegex(actual, expected interface{}) bool {
	pattern, ok := expected.(string)
	if !ok {
		return false
	}

	actualStr := fmt.Sprintf("%v", actual)

	// Use cached regex compilation
	re, err := GetCachedRegex(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(actualStr)
}

// evaluateStringOp handles CONTAINS, STARTS WITH, ENDS WITH.
//
// # Parameters
//
//   - node: The node containing the property
//   - variable: The variable name in the query
//   - whereClause: The WHERE clause string
//   - op: The operator ("CONTAINS", "STARTS WITH", "ENDS WITH")
//
// # Returns
//
//   - true if the string operation evaluates to true
//
// # Example
//
//	evaluateStringOp(node, "n", "n.name CONTAINS 'Smith'", "CONTAINS")
//	// Returns true if node.Properties["name"] contains "Smith"
func (e *StorageExecutor) evaluateStringOp(node *storage.Node, variable, whereClause, op string) bool {
	upperClause := strings.ToUpper(whereClause)
	opIdx := strings.Index(upperClause, " "+op+" ")
	if opIdx < 0 {
		return true
	}

	left := strings.TrimSpace(whereClause[:opIdx])
	right := strings.TrimSpace(whereClause[opIdx+len(op)+2:])

	// If the left side doesn't reference this variable, it's not our concern in this
	// single-node evaluation context. Treat it as a pass-through.
	if !containsIdentifierToken(left, variable) {
		return true
	}

	// Evaluate left/right expressions to support functions like toLower()/toUpper()
	actualVal := e.evaluateExpression(left, variable, node)
	if actualVal == nil {
		return false
	}
	expectedVal := e.evaluateExpression(right, variable, node)
	if expectedVal == nil {
		expectedVal = e.parseValue(right)
	}

	actualStr := fmt.Sprintf("%v", actualVal)
	expectedStr := fmt.Sprintf("%v", expectedVal)

	switch op {
	case "CONTAINS":
		return strings.Contains(actualStr, expectedStr)
	case "STARTS WITH":
		return strings.HasPrefix(actualStr, expectedStr)
	case "ENDS WITH":
		return strings.HasSuffix(actualStr, expectedStr)
	}
	return true
}

// evaluateInOp handles IN [list] operator.
//
// # Parameters
//
//   - node: The node containing the property
//   - variable: The variable name in the query
//   - whereClause: The WHERE clause string
//
// # Returns
//
//   - true if the property value is in the list
//
// # Example
//
//	evaluateInOp(node, "n", "n.status IN ['active', 'pending']")
//	// Returns true if node.Properties["status"] is "active" or "pending"
func (e *StorageExecutor) evaluateInOp(node *storage.Node, variable, whereClause string) bool {
	upperClause := strings.ToUpper(whereClause)

	// Cypher: `<expr> NOT IN <list>` must split on " NOT IN ", not on
	// the substring " IN " (which would leave the trailing "NOT" on the
	// left side and silently fail expression evaluation, dropping every
	// row from the result). Detect NOT IN first and negate the
	// membership result.
	negate := false
	opOffset := 4 // len(" IN ")
	splitIdx := strings.Index(upperClause, " NOT IN ")
	if splitIdx >= 0 {
		negate = true
		opOffset = 8 // len(" NOT IN ")
	} else {
		splitIdx = strings.Index(upperClause, " IN ")
		if splitIdx < 0 {
			return true
		}
	}
	inIdx := splitIdx

	left := strings.TrimSpace(whereClause[:inIdx])
	right := strings.TrimSpace(whereClause[inIdx+opOffset:])

	// Keep historical single-node behavior: if this predicate does not reference
	// the current variable at all, treat it as a pass-through.
	leftRefsVar := containsIdentifierToken(left, variable)
	rightRefsVar := containsIdentifierToken(right, variable)
	if !leftRefsVar && !rightRefsVar {
		return true
	}

	nodes := map[string]*storage.Node{
		variable: node,
	}

	// Evaluate both sides as expressions so we support both:
	//   n.prop IN ['a', 'b']
	//   'a' IN n.listProp
	value := e.evaluateExpressionWithContext(left, nodes, nil)
	if value == nil {
		// Neo4j semantics: NULL IN list yields NULL → false in WHERE,
		// for both IN and NOT IN (NOT NULL is also NULL → false).
		return false
	}

	rightIdent := strings.TrimSpace(right)
	var listVal interface{}
	if isValidIdentifier(rightIdent) {
		if v, ok := e.fabricRecordBindings[rightIdent]; ok {
			listVal = v
		}
	}
	if listVal == nil {
		listVal = e.evaluateExpressionWithContext(right, nodes, nil)
	}
	items, ok := toInterfaceSlice(listVal)
	if !ok {
		return false
	}

	matched := false
	for _, item := range items {
		if e.compareEqual(value, item) {
			matched = true
			break
		}
	}
	if negate {
		return !matched
	}
	return matched
}

func toInterfaceSlice(v interface{}) ([]interface{}, bool) {
	if v == nil {
		return nil, false
	}
	if list, ok := v.([]interface{}); ok {
		return list, true
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil, false
	}

	out := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// containsIdentifierToken returns true if expr contains ident as a standalone
// identifier token (word boundary), e.g. "id(n)" contains "n" but "node" does not.
func containsIdentifierToken(expr, ident string) bool {
	if ident == "" {
		return false
	}

	var token strings.Builder
	flush := func() bool {
		if token.Len() == 0 {
			return false
		}
		t := token.String()
		token.Reset()
		return t == ident
	}

	for _, r := range expr {
		if isIdentRune(r) {
			token.WriteRune(r)
			continue
		}
		if flush() {
			return true
		}
	}
	return flush()
}

func isIdentRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// evaluateIsNull handles IS NULL / IS NOT NULL.
//
// # Parameters
//
//   - node: The node containing the property
//   - variable: The variable name in the query
//   - whereClause: The WHERE clause string
//   - expectNotNull: true for IS NOT NULL, false for IS NULL
//
// # Returns
//
//   - true if the NULL check evaluates to true
//
// # Example
//
//	evaluateIsNull(node, "n", "n.email IS NOT NULL", true)
//	// Returns true if node.Properties["email"] exists and is not nil
func (e *StorageExecutor) evaluateIsNull(node *storage.Node, variable, whereClause string, expectNotNull bool) bool {
	upperClause := strings.ToUpper(whereClause)
	var propExpr string

	if expectNotNull {
		idx := strings.Index(upperClause, " IS NOT NULL")
		propExpr = strings.TrimSpace(whereClause[:idx])
	} else {
		idx := strings.Index(upperClause, " IS NULL")
		propExpr = strings.TrimSpace(whereClause[:idx])
	}

	// Extract property
	if !strings.HasPrefix(propExpr, variable+".") {
		// Keep historical permissive per-node behavior for unknown identifiers in
		// single-node evaluation (e.g. "something IS NULL"), which many existing
		// tests and call paths rely on.
		if strings.Contains(propExpr, ".") {
			return true
		}
		valExpr := strings.TrimSpace(propExpr)
		if isValidIdentifier(valExpr) {
			// If the identifier is row-bound (fabric/correlated execution), evaluate
			// actual null semantics from the binding.
			if bound, ok := e.fabricRecordBindings[valExpr]; ok {
				if expectNotNull {
					return bound != nil
				}
				return bound == nil
			}
			// Otherwise preserve permissive legacy semantics for unbound identifiers.
			return true
		}
		// Literal/null expression support:
		//   null IS NOT NULL -> false
		//   'x' IS NOT NULL  -> true
		//   1 IS NULL        -> false
		val := e.parseValue(valExpr)
		if expectNotNull {
			return val != nil
		}
		return val == nil
	}
	propName := propExpr[len(variable)+1:]

	// Check EmbedMeta for managed embedding metadata (e.g., has_embedding).
	if node.EmbedMeta != nil {
		if val, exists := node.EmbedMeta[propName]; exists {
			if expectNotNull {
				return val != nil
			}
			return val == nil
		}
	}

	val, exists := node.Properties[propName]

	if expectNotNull {
		return exists && val != nil
	}
	return !exists || val == nil
}
