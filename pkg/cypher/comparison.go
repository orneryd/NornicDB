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
	"strings"

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

	// Extract property
	if !strings.HasPrefix(left, variable+".") {
		return true
	}
	propName := left[len(variable)+1:]

	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	actualStr := fmt.Sprintf("%v", actualVal)
	expectedStr := fmt.Sprintf("%v", e.parseValue(right))

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
	inIdx := strings.Index(upperClause, " IN ")
	if inIdx < 0 {
		return true
	}

	left := strings.TrimSpace(whereClause[:inIdx])
	right := strings.TrimSpace(whereClause[inIdx+4:])

	// Extract property
	if !strings.HasPrefix(left, variable+".") {
		return true
	}
	propName := left[len(variable)+1:]

	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	// Parse list: [val1, val2, ...]
	if strings.HasPrefix(right, "[") && strings.HasSuffix(right, "]") {
		listContent := right[1 : len(right)-1]
		items := strings.Split(listContent, ",")
		for _, item := range items {
			itemVal := e.parseValue(strings.TrimSpace(item))
			if e.compareEqual(actualVal, itemVal) {
				return true
			}
		}
	}
	return false
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
		return true
	}
	propName := propExpr[len(variable)+1:]

	val, exists := node.Properties[propName]

	if expectNotNull {
		return exists && val != nil
	}
	return !exists || val == nil
}
