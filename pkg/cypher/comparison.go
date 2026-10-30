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
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	if len(props) == 0 {
		return true
	}
	if e.statementPropertyViews != nil {
		return nodePropertiesMatch(e.queryVisibleValue(node).(*storage.Node), props)
	}
	if !e.queryPropertyDecayEnabled() {
		return nodePropertiesMatch(node, props)
	}
	// A pattern only observes its named properties. Avoid scoring and reading
	// access metadata for every unrelated property on every candidate node.
	var local [8]string
	keys := local[:0]
	if len(props) > len(local) {
		keys = make([]string, 0, len(props))
	}
	for key := range props {
		keys = append(keys, key)
	}
	visible := storage.NewQueryResultProjection(e.storage).NodeProperties(node, keys)
	return nodePropertiesMatch(&storage.Node{Properties: visible}, props)
}

// nodePropertiesMatch is the one property test of a node pattern
// ((n {key: value}) in MATCH, OPTIONAL MATCH and MERGE): every property is
// present and equal to its value under Cypher equality, so 1 matches 1.0,
// [1, 2] matches a stored integer list, and a date matches the same date read
// back from storage. A null value matches nothing. Comparing Go values
// instead (reflect.DeepEqual, or MERGE's type-tagged form) missed those, and
// MERGE created a duplicate node (#846).
func nodePropertiesMatch(node *storage.Node, props map[string]interface{}) bool {
	for key, expected := range props {
		if expected == nil {
			return false
		}
		actual, exists := node.Properties[key]
		if !exists {
			return false
		}
		if matched, _ := cypherEquality(actual, expected).(bool); !matched {
			return false
		}
	}
	return true
}

// edgeMatchesProps checks if an edge's properties match the expected values.
func (e *StorageExecutor) edgeMatchesProps(edge *storage.Edge, props map[string]interface{}) bool {
	if len(props) == 0 {
		return true
	}
	properties := e.queryEntityProperties(edge)
	for key, expected := range props {
		actual, exists := properties[key]
		if !exists {
			return false
		}
		if !e.compareEqual(actual, expected) {
			return false
		}
	}
	return true
}

// compareEqual handles Cypher equality comparison.
//
// # Parameters
//
//   - actual: The actual value from the node
//   - expected: The expected value from the query
//
// # Returns
//
//   - true if values are equal under Cypher's type-family rules
//
// # Example
//
//	compareEqual(int64(42), float64(42.0))  // true
//	compareEqual("hello", "hello")          // true
//	compareEqual(nil, nil)                  // true
//	compareEqual(42, "42")                  // false (different type families)
func (e *StorageExecutor) compareEqual(actual, expected interface{}) bool {
	equal := cypherEquality(actual, expected)
	matched, _ := equal.(bool)
	return matched
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
	comparison, comparable := compareCypherOrderedValues(actual, expected)
	return comparable && comparison > 0
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
	comparison, comparable := compareCypherOrderedValues(actual, expected)
	return comparable && comparison < 0
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
	matched, err := cypherRegexMatch(actual, expected)
	return err == nil && matched == true
}

// evaluateInOpTruth evaluates a single-node `x IN list` / `x NOT IN list`
// WHERE leaf with the same three-valued membership as the expression
// evaluator (cypherMembership): null x, a null list, or a list that holds null
// without a match give unknown, and NOT IN of unknown stays unknown.
func (e *StorageExecutor) evaluateInOpTruth(ctx context.Context, node *storage.Node, variable, whereClause string) cypherTruth {
	upperClause := upperASCII(whereClause)

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
			return truthTrue
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
		return truthTrue
	}

	nodes := map[string]*storage.Node{
		variable: node,
	}

	// Evaluate both sides as expressions so we support both:
	//   n.prop IN ['a', 'b']
	//   'a' IN n.listProp
	value := e.evaluateExpressionWithContext(ctx, left, nodes, nil)
	rightIdent := strings.TrimSpace(right)
	var listVal interface{}
	if cyphertext.IsIdentifier(rightIdent) {
		if v, ok := e.boundValue(ctx, rightIdent); ok {
			listVal = v
		}
	}
	if listVal == nil {
		listVal = e.evaluateExpressionWithContext(ctx, right, nodes, nil)
	}
	membership, ok := cypherMembership(value, listVal)
	if !ok {
		return truthUnknown
	}
	truth := truthUnknown
	if matched, known := membership.(bool); known {
		truth = truthOf(matched)
	}
	if negate {
		return truth.not()
	}
	return truth
}

func toInterfaceSlice(v interface{}) ([]interface{}, bool) {
	return cypherListValue(v)
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
		if cyphertext.NamePart(r) {
			token.WriteRune(r)
			continue
		}
		if flush() {
			return true
		}
	}
	return flush()
}
