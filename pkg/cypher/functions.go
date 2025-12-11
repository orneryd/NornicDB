// Cypher function implementations for NornicDB.
// This file contains evaluateExpressionWithContext and all Cypher functions.

package cypher

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// PluginFunctionLookup is a callback to look up functions from loaded plugins.
// Set by pkg/nornicdb during database initialization.
// Returns the function handler and true if found, nil and false otherwise.
var PluginFunctionLookup func(name string) (handler interface{}, found bool)

// isFunctionCall checks if an expression is a standalone function call with balanced parentheses.
//
// This function validates that:
//  1. Expression starts with funcName + "("
//  2. All parentheses are properly balanced
//  3. Expression ends right after the closing parenthesis
//  4. Quotes are respected (parentheses inside quotes don't count)
//
// It's used to distinguish between standalone function calls like "date('2025-01-01')"
// and complex expressions like "date('2025-01-01') + duration('P5D')".
//
// Parameters:
//   - expr: The expression to check
//   - funcName: The function name to look for (case-insensitive)
//
// Returns:
//   - true if expr is a standalone call to funcName
//   - false if expr is part of a larger expression or not a function call
//
// Example 1 - Standalone Function Calls (returns true):
//
//	isFunctionCall("date('2025-01-01')", "date")           // true
//	isFunctionCall("toLower(n.name)", "tolower")           // true
//	isFunctionCall("count(n)", "count")                    // true
//	isFunctionCall("substring('hello', 0, 3)", "substring") // true
//
// Example 2 - Complex Expressions (returns false):
//
//	isFunctionCall("date('2025-01-01') + duration('P5D')", "date")  // false - has + after
//	isFunctionCall("toLower(n.name) + ' suffix'", "tolower")        // false - has + after
//	isFunctionCall("count(n) > 10", "count")                        // false - has > after
//
// Example 3 - Nested Calls (returns false for outer, true for specific):
//
//	expr := "toLower(substring(n.name, 0, 5))"
//	isFunctionCall(expr, "substring")  // false - it's wrapped in toLower
//	isFunctionCall(expr, "tolower")    // true - toLower is the outer function
//
// ELI12:
//
// Imagine you're reading math: "add(5, 3)"
// This function checks: "Is this JUST the add() function, or is there more?"
//
//   - "add(5, 3)" → YES, that's just the function
//   - "add(5, 3) * 2" → NO, there's multiplication after it
//   - "multiply(add(5, 3), 2)" → For "add": NO (it's inside multiply)
//     → For "multiply": YES (it's the whole thing)
//
// It's like asking "Is this sentence ONLY about one thing, or are there extra parts?"
//
// Use Cases:
//   - Distinguishing function calls from arithmetic expressions
//   - Parsing Cypher RETURN clauses
//   - Validating function arguments
func isFunctionCall(expr, funcName string) bool {
	// Use whitespace-tolerant matching - allows "count (n)" not just "count(n)"
	return isFunctionCallWS(expr, funcName)
}

// evaluateExpression evaluates an expression for a single node context.
func (e *StorageExecutor) evaluateExpression(expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(expr, map[string]*storage.Node{varName: node}, nil)
}

// evaluateExpressionWithPathContext evaluates an expression with full path context
// This is needed for path functions like relationships(path) and nodes(path) that need
// access to the full path data, not just individual nodes/relationships
func (e *StorageExecutor) evaluateExpressionWithPathContext(expr string, pathCtx PathContext) interface{} {
	return e.evaluateExpressionWithContextFull(expr, pathCtx.nodes, pathCtx.rels, pathCtx.paths, pathCtx.allPathEdges, pathCtx.allPathNodes, pathCtx.pathLength)
}

func (e *StorageExecutor) evaluateExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContextFull(expr, nodes, rels, nil, nil, nil, 0)
}

func (e *StorageExecutor) evaluateExpressionWithContextFull(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// ========================================
	// Parenthesized Expressions - strip outer parens
	// ========================================
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		// Check if these parentheses wrap the entire expression
		depth := 0
		allWrapped := true
		for i, ch := range expr {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			// If depth reaches 0 before the last character, parens don't wrap the whole thing
			if depth == 0 && i < len(expr)-1 {
				allWrapped = false
				break
			}
		}
		if allWrapped && depth == 0 {
			// Strip outer parentheses and re-evaluate
			return e.evaluateExpressionWithContext(expr[1:len(expr)-1], nodes, rels)
		}
	}

	// ========================================
	// Array Indexing - handle expr[index] patterns
	// ========================================
	// This handles expressions like labels(n)[0], collect(...)[..10], etc.
	if strings.HasSuffix(expr, "]") {
		// Find the matching opening bracket
		bracketEnd := len(expr) - 1
		depth := 1
		bracketStart := -1
		for i := bracketEnd - 1; i >= 0; i-- {
			if expr[i] == ']' {
				depth++
			} else if expr[i] == '[' {
				depth--
				if depth == 0 {
					bracketStart = i
					break
				}
			}
		}
		if bracketStart > 0 {
			baseExpr := expr[:bracketStart]
			indexExpr := expr[bracketStart+1 : bracketEnd]

			// Skip if this is an IN expression (e.g., "1 IN [1, 2, 3]")
			// The base would be "1 IN " which ends with " IN "
			baseUpper := strings.ToUpper(strings.TrimSpace(baseExpr))
			if strings.HasSuffix(baseUpper, " IN") || strings.HasSuffix(baseUpper, " NOT IN") {
				// This is an IN expression, not array indexing - skip this section
				goto skipArrayIndexing
			}

			// Check for slice notation [..N] or [N..M] or [N..]
			if strings.Contains(indexExpr, "..") {
				// This is a slice, not an index
				baseVal := e.evaluateExpressionWithContext(baseExpr, nodes, rels)
				if list, ok := baseVal.([]interface{}); ok {
					parts := strings.SplitN(indexExpr, "..", 2)
					startIdx := int64(0)
					endIdx := int64(len(list))

					if parts[0] != "" {
						startIdx, _ = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
					}
					if len(parts) > 1 && parts[1] != "" {
						endIdx, _ = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
					}

					// Handle negative indices
					if startIdx < 0 {
						startIdx = int64(len(list)) + startIdx
					}
					if endIdx < 0 {
						endIdx = int64(len(list)) + endIdx
					}
					// Clamp
					if startIdx < 0 {
						startIdx = 0
					}
					if endIdx > int64(len(list)) {
						endIdx = int64(len(list))
					}
					if startIdx >= endIdx {
						return []interface{}{}
					}
					return list[startIdx:endIdx]
				}
				return nil
			}

			// Single index access [N]
			baseVal := e.evaluateExpressionWithContext(baseExpr, nodes, rels)
			if baseVal == nil {
				return nil
			}

			// Evaluate the index
			index := e.evaluateExpressionWithContext(indexExpr, nodes, rels)
			var idx int64
			switch v := index.(type) {
			case int64:
				idx = v
			case int:
				idx = int64(v)
			case float64:
				idx = int64(v)
			case string:
				// Try to parse as number
				idx, _ = strconv.ParseInt(v, 10, 64)
			}

			// Apply the index to the base value
			switch list := baseVal.(type) {
			case []interface{}:
				if idx < 0 {
					idx = int64(len(list)) + idx
				}
				if idx >= 0 && idx < int64(len(list)) {
					return list[idx]
				}
				return nil
			case []string:
				if idx < 0 {
					idx = int64(len(list)) + idx
				}
				if idx >= 0 && idx < int64(len(list)) {
					return list[idx]
				}
				return nil
			case string:
				if idx < 0 {
					idx = int64(len(list)) + idx
				}
				if idx >= 0 && idx < int64(len(list)) {
					return string(list[idx])
				}
				return nil
			}
		}
	}
skipArrayIndexing:

	// ========================================
	// Map Literals - handle { key: value, ... } patterns
	// ========================================
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.evaluateMapLiteralFull(expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// ========================================
	// CASE Expressions (must be checked first)
	// ========================================
	if isCaseExpression(expr) {
		return e.evaluateCaseExpression(expr, nodes, rels)
	}

	lowerExpr := strings.ToLower(expr)

	// ========================================
	// Cypher Functions (Neo4j compatible)
	// ========================================

	// id(n) - return internal node/relationship ID
	if matchFuncStartAndSuffix(expr, "id") {
		inner := extractFuncArgs(expr, "id")
		if node, ok := nodes[inner]; ok {
			return string(node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return string(rel.ID)
		}
		return nil
	}

	// elementId(n) - same as id() for compatibility
	if matchFuncStartAndSuffix(expr, "elementid") {
		inner := extractFuncArgs(expr, "elementid")
		if node, ok := nodes[inner]; ok {
			return fmt.Sprintf("4:nornicdb:%s", node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return fmt.Sprintf("5:nornicdb:%s", rel.ID)
		}
		return nil
	}

	// labels(n) - return list of labels for a node
	if matchFuncStartAndSuffix(expr, "labels") {
		inner := extractFuncArgs(expr, "labels")
		// fmt.Printf("DEBUG labels(): inner=%q, nodes=%+v\n", inner, nodes)
		if node, ok := nodes[inner]; ok {
			// Return labels as a list of strings
			result := make([]interface{}, len(node.Labels))
			for i, label := range node.Labels {
				result[i] = label
			}
			return result
		}
		// Check if this is a nested expression like labels(f) where f is a value in nodes
		// Try to get the node if inner is a variable reference
		if innerVal := e.evaluateExpressionWithContext(inner, nodes, rels); innerVal != nil {
			if node, ok := innerVal.(*storage.Node); ok {
				result := make([]interface{}, len(node.Labels))
				for i, label := range node.Labels {
					result[i] = label
				}
				return result
			}
		}
		return nil
	}

	// type(r) - return relationship type
	if matchFuncStartAndSuffix(expr, "type") {
		inner := extractFuncArgs(expr, "type")
		if rel, ok := rels[inner]; ok {
			return rel.Type
		}
		// Also handle the case where inner is a map representation (from list comprehension)
		innerVal := e.evaluateExpressionWithContextFull(inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if mapVal, ok := innerVal.(map[string]interface{}); ok {
			if relType, ok := mapVal["type"]; ok {
				return relType
			}
		}
		return nil
	}

	// keys(n) - return list of property keys
	if matchFuncStartAndSuffix(expr, "keys") {
		inner := extractFuncArgs(expr, "keys")
		if node, ok := nodes[inner]; ok {
			keys := make([]interface{}, 0, len(node.Properties))
			for k := range node.Properties {
				if !e.isInternalProperty(k) {
					keys = append(keys, k)
				}
			}
			return keys
		}
		if rel, ok := rels[inner]; ok {
			keys := make([]interface{}, 0, len(rel.Properties))
			for k := range rel.Properties {
				keys = append(keys, k)
			}
			return keys
		}
		return nil
	}

	// properties(n) - return all properties as a map
	if matchFuncStartAndSuffix(expr, "properties") {
		inner := extractFuncArgs(expr, "properties")
		if node, ok := nodes[inner]; ok {
			props := make(map[string]interface{})
			for k, v := range node.Properties {
				if !e.isInternalProperty(k) {
					props[k] = v
				}
			}
			return props
		}
		if rel, ok := rels[inner]; ok {
			return rel.Properties
		}
		return nil
	}

	// Note: count(), sum(), avg(), etc. are aggregation functions and should NOT be
	// evaluated here. They must be handled by executeAggregation() in match.go or
	// executeMatchWithRelationships() in traversal.go. If we reach here with count(),
	// it means the query wasn't properly detected as an aggregation query - that's a bug
	// in the query router, not something we should handle here.

	// size(list) or size(string) - return length
	if matchFuncStartAndSuffix(expr, "size") {
		inner := extractFuncArgs(expr, "size")
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		case []string:
			return int64(len(v))
		}
		return int64(0)
	}

	// length(path) - same as size for compatibility, with special handling for paths
	if matchFuncStartAndSuffix(expr, "length") {
		inner := extractFuncArgs(expr, "length")

		// Check if this is a path variable - use the path length from context
		if paths != nil {
			if pathResult, ok := paths[inner]; ok && pathResult != nil {
				return int64(pathResult.Length)
			}
		}
		// Also check if inner is a path variable and we have allPathEdges (variable-length patterns)
		// For MATCH path = (a)-[*1..2]-(b), the path variable gives us the length
		if pathLength > 0 {
			// If pathLength is set and inner looks like a simple variable (no dots, no parens)
			if !strings.Contains(inner, ".") && !strings.Contains(inner, "(") && !strings.Contains(inner, "[") {
				// This might be a path variable reference, use stored pathLength
				return int64(pathLength)
			}
		}

		innerVal := e.evaluateExpressionWithContextFull(inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		}
		return int64(0)
	}

	// exists(n.prop) - check if property exists
	if matchFuncStartAndSuffix(expr, "exists") {
		inner := extractFuncArgs(expr, "exists")
		// Check for property access
		if dotIdx := strings.Index(inner, "."); dotIdx > 0 {
			varName := inner[:dotIdx]
			propName := inner[dotIdx+1:]
			if node, ok := nodes[varName]; ok {
				_, exists := node.Properties[propName]
				return exists
			}
		}
		return false
	}

	// coalesce(val1, val2, ...) - return first non-null value
	if matchFuncStartAndSuffix(expr, "coalesce") {
		inner := extractFuncArgs(expr, "coalesce")
		args := e.splitFunctionArgs(inner)
		for _, arg := range args {
			val := e.evaluateExpressionWithContext(strings.TrimSpace(arg), nodes, rels)
			if val != nil {
				return val
			}
		}
		return nil
	}

	// head(list) - return first element
	if matchFuncStartAndSuffix(expr, "head") {
		inner := extractFuncArgs(expr, "head")
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[0]
		}
		return nil
	}

	// last(list) - return last element
	if matchFuncStartAndSuffix(expr, "last") {
		inner := extractFuncArgs(expr, "last")
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[len(list)-1]
		}
		return nil
	}

	// tail(list) - return list without first element
	if matchFuncStartAndSuffix(expr, "tail") {
		inner := extractFuncArgs(expr, "tail")
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}

	// reverse(list) - return reversed list
	if matchFuncStartAndSuffix(expr, "reverse") {
		inner := extractFuncArgs(expr, "reverse")
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok {
			result := make([]interface{}, len(list))
			for i, v := range list {
				result[len(list)-1-i] = v
			}
			return result
		}
		if str, ok := innerVal.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
		return nil
	}

	// range(start, end) or range(start, end, step)
	if matchFuncStartAndSuffix(expr, "range") {
		inner := extractFuncArgs(expr, "range")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			start, _ := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
			end, _ := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			step := int64(1)
			if len(args) >= 3 {
				step, _ = strconv.ParseInt(strings.TrimSpace(args[2]), 10, 64)
			}
			if step == 0 {
				step = 1
			}
			var result []interface{}
			if step > 0 {
				for i := start; i <= end; i += step {
					result = append(result, i)
				}
			} else {
				for i := start; i >= end; i += step {
					result = append(result, i)
				}
			}
			return result
		}
		return []interface{}{}
	}

	// slice(list, start, end) - get sublist from start to end (exclusive)
	if matchFuncStartAndSuffix(expr, "slice") {
		inner := extractFuncArgs(expr, "slice")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			startIdx, _ := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			if list, ok := listVal.([]interface{}); ok {
				endIdx := int64(len(list))
				if len(args) >= 3 {
					endIdx, _ = strconv.ParseInt(strings.TrimSpace(args[2]), 10, 64)
				}
				if startIdx < 0 {
					startIdx = int64(len(list)) + startIdx
				}
				if endIdx < 0 {
					endIdx = int64(len(list)) + endIdx
				}
				if startIdx < 0 {
					startIdx = 0
				}
				if endIdx > int64(len(list)) {
					endIdx = int64(len(list))
				}
				if startIdx >= endIdx {
					return []interface{}{}
				}
				return list[startIdx:endIdx]
			}
		}
		return []interface{}{}
	}

	// indexOf(list, value) - get index of value in list, -1 if not found
	if matchFuncStartAndSuffix(expr, "indexof") {
		inner := extractFuncArgs(expr, "indexof")
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			listVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			searchVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if list, ok := listVal.([]interface{}); ok {
				for i, item := range list {
					if e.compareEqual(item, searchVal) {
						return int64(i)
					}
				}
			}
		}
		return int64(-1)
	}

	// degree(node) - total degree (in + out)
	if matchFuncStartAndSuffix(expr, "degree") {
		inner := extractFuncArgs(expr, "degree")
		if node, ok := nodes[inner]; ok {
			inDegree := e.storage.GetInDegree(node.ID)
			outDegree := e.storage.GetOutDegree(node.ID)
			return int64(inDegree + outDegree)
		}
		return int64(0)
	}

	// inDegree(node) - incoming edges count
	if matchFuncStartAndSuffix(expr, "indegree") {
		inner := extractFuncArgs(expr, "indegree")
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetInDegree(node.ID))
		}
		return int64(0)
	}

	// outDegree(node) - outgoing edges count
	if matchFuncStartAndSuffix(expr, "outdegree") {
		inner := extractFuncArgs(expr, "outdegree")
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetOutDegree(node.ID))
		}
		return int64(0)
	}

	// hasLabels(node, labels) - check if node has all specified labels
	if matchFuncStartAndSuffix(expr, "haslabels") {
		inner := extractFuncArgs(expr, "haslabels")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			if node, ok := nodes[strings.TrimSpace(args[0])]; ok {
				labelsVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
				if labels, ok := labelsVal.([]interface{}); ok {
					for _, reqLabel := range labels {
						labelStr, _ := reqLabel.(string)
						found := false
						for _, nodeLabel := range node.Labels {
							if nodeLabel == labelStr {
								found = true
								break
							}
						}
						if !found {
							return false
						}
					}
					return true
				}
			}
		}
		return false
	}

	// ========================================
	// APOC Map Functions
	// ========================================

	// apoc.map.fromPairs(list) - create map from [[key, value], ...] pairs
	if matchFuncStartAndSuffix(expr, "apoc.map.frompairs") {
		inner := extractFuncArgs(expr, "apoc.map.frompairs")
		pairsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pairs, ok := pairsVal.([]interface{}); ok {
			result := make(map[string]interface{})
			for _, pair := range pairs {
				if pairList, ok := pair.([]interface{}); ok && len(pairList) >= 2 {
					if key, ok := pairList[0].(string); ok {
						result[key] = pairList[1]
					}
				}
			}
			return result
		}
		return map[string]interface{}{}
	}

	// apoc.map.merge(map1, map2) - merge two maps
	if matchFuncStartAndSuffix(expr, "apoc.map.merge") {
		inner := extractFuncArgs(expr, "apoc.map.merge")
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			map1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			map2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			m1, ok1 := map1.(map[string]interface{})
			m2, ok2 := map2.(map[string]interface{})
			if ok1 && ok2 {
				result := make(map[string]interface{})
				for k, v := range m1 {
					result[k] = v
				}
				for k, v := range m2 {
					result[k] = v
				}
				return result
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.removeKey(map, key) - remove key from map
	if matchFuncStartAndSuffix(expr, "apoc.map.removekey") {
		inner := extractFuncArgs(expr, "apoc.map.removekey")
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			keyVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if m, ok := mapVal.(map[string]interface{}); ok {
				if key, ok := keyVal.(string); ok {
					result := make(map[string]interface{})
					for k, v := range m {
						if k != key {
							result[k] = v
						}
					}
					return result
				}
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.setKey(map, key, value) - set key in map
	if matchFuncStartAndSuffix(expr, "apoc.map.setkey") {
		inner := extractFuncArgs(expr, "apoc.map.setkey")
		args := e.splitFunctionArgs(inner)
		if len(args) == 3 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			keyVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			value := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)
			if m, ok := mapVal.(map[string]interface{}); ok {
				if key, ok := keyVal.(string); ok {
					result := make(map[string]interface{})
					for k, v := range m {
						result[k] = v
					}
					result[key] = value
					return result
				}
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.clean(map, keys, values) - remove specified keys and entries with specified values
	if matchFuncStartAndSuffix(expr, "apoc.map.clean") {
		inner := extractFuncArgs(expr, "apoc.map.clean")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			var keysToRemove []string
			var valuesToRemove []interface{}

			if len(args) >= 2 {
				if keys, ok := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels).([]interface{}); ok {
					for _, k := range keys {
						if ks, ok := k.(string); ok {
							keysToRemove = append(keysToRemove, ks)
						}
					}
				}
			}
			if len(args) >= 3 {
				if vals, ok := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels).([]interface{}); ok {
					valuesToRemove = vals
				}
			}

			if m, ok := mapVal.(map[string]interface{}); ok {
				result := make(map[string]interface{})
				for k, v := range m {
					// Skip if key is in keysToRemove
					skip := false
					for _, kr := range keysToRemove {
						if k == kr {
							skip = true
							break
						}
					}
					if skip {
						continue
					}
					// Skip if value is in valuesToRemove
					for _, vr := range valuesToRemove {
						if e.compareEqual(v, vr) {
							skip = true
							break
						}
					}
					if !skip {
						result[k] = v
					}
				}
				return result
			}
		}
		return map[string]interface{}{}
	}

	// ========================================
	// String Functions
	// ========================================

	// toString(value)
	if matchFuncStartAndSuffix(expr, "tostring") {
		inner := extractFuncArgs(expr, "tostring")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fmt.Sprintf("%v", val)
	}

	// toInteger(value)
	if matchFuncStartAndSuffix(expr, "tointeger") {
		inner := extractFuncArgs(expr, "tointeger")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil
	}

	// toInt(value) - alias for toInteger
	if matchFuncStartAndSuffix(expr, "toint") {
		inner := extractFuncArgs(expr, "toint")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil
	}

	// toFloat(value)
	if matchFuncStartAndSuffix(expr, "tofloat") {
		inner := extractFuncArgs(expr, "tofloat")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil
	}

	// toBoolean(value)
	if matchFuncStartAndSuffix(expr, "toboolean") {
		inner := extractFuncArgs(expr, "toboolean")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return strings.EqualFold(v, "true")
		}
		return nil
	}

	// ========================================
	// OrNull Variants (return null instead of error)
	// ========================================

	// toIntegerOrNull(value)
	if matchFuncStartAndSuffix(expr, "tointegerornull") {
		inner := extractFuncArgs(expr, "tointegerornull")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil // Return null instead of error
	}

	// toFloatOrNull(value)
	if matchFuncStartAndSuffix(expr, "tofloatornull") {
		inner := extractFuncArgs(expr, "tofloatornull")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil
	}

	// toBooleanOrNull(value)
	if matchFuncStartAndSuffix(expr, "tobooleanornull") {
		inner := extractFuncArgs(expr, "tobooleanornull")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			lower := strings.ToLower(v)
			if lower == "true" {
				return true
			}
			if lower == "false" {
				return false
			}
		}
		return nil
	}

	// toStringOrNull(value) - same as toString but explicit null handling
	if matchFuncStartAndSuffix(expr, "tostringornull") {
		inner := extractFuncArgs(expr, "tostringornull")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if val == nil {
			return nil
		}
		return fmt.Sprintf("%v", val)
	}

	// ========================================
	// List Conversion Functions
	// ========================================

	// toIntegerList(list)
	if matchFuncStartAndSuffix(expr, "tointegerlist") {
		inner := extractFuncArgs(expr, "tointegerlist")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case int64:
				result[i] = v
			case int:
				result[i] = int64(v)
			case float64:
				result[i] = int64(v)
			case string:
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					result[i] = n
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toFloatList(list)
	if matchFuncStartAndSuffix(expr, "tofloatlist") {
		inner := extractFuncArgs(expr, "tofloatlist")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case float64:
				result[i] = v
			case float32:
				result[i] = float64(v)
			case int64:
				result[i] = float64(v)
			case int:
				result[i] = float64(v)
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					result[i] = f
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toBooleanList(list)
	if matchFuncStartAndSuffix(expr, "tobooleanlist") {
		inner := extractFuncArgs(expr, "tobooleanlist")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case bool:
				result[i] = v
			case string:
				lower := strings.ToLower(v)
				if lower == "true" {
					result[i] = true
				} else if lower == "false" {
					result[i] = false
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toStringList(list)
	if matchFuncStartAndSuffix(expr, "tostringlist") {
		inner := extractFuncArgs(expr, "tostringlist")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			if item == nil {
				result[i] = nil
			} else {
				result[i] = fmt.Sprintf("%v", item)
			}
		}
		return result
	}

	// ========================================
	// Additional Utility Functions
	// ========================================

	// valueType(value) - returns the type of a value as a string
	if matchFuncStartAndSuffix(expr, "valuetype") {
		inner := extractFuncArgs(expr, "valuetype")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch val.(type) {
		case nil:
			return "NULL"
		case bool:
			return "BOOLEAN"
		case int, int64, int32:
			return "INTEGER"
		case float64, float32:
			return "FLOAT"
		case string:
			return "STRING"
		case []interface{}:
			return "LIST"
		case map[string]interface{}:
			return "MAP"
		default:
			return "ANY"
		}
	}

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// sum(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "sum") {
		inner := extractFuncArgs(expr, "sum")
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// avg(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "avg") {
		inner := extractFuncArgs(expr, "avg")
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// min(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "min") {
		inner := extractFuncArgs(expr, "min")
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// max(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "max") {
		inner := extractFuncArgs(expr, "max")
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// collect(expr) - in single row context, returns single-element list
	if matchFuncStartAndSuffix(expr, "collect") {
		inner := extractFuncArgs(expr, "collect")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if val == nil {
			return []interface{}{}
		}
		return []interface{}{val}
	}

	// toLower(string)
	if matchFuncStartAndSuffix(expr, "tolower") {
		inner := extractFuncArgs(expr, "tolower")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
		return nil
	}

	// lower(string) - alias for toLower
	if matchFuncStartAndSuffix(expr, "lower") {
		inner := extractFuncArgs(expr, "lower")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
		return nil
	}

	// toUpper(string)
	if matchFuncStartAndSuffix(expr, "toupper") {
		inner := extractFuncArgs(expr, "toupper")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
		return nil
	}

	// upper(string) - alias for toUpper
	if matchFuncStartAndSuffix(expr, "upper") {
		inner := extractFuncArgs(expr, "upper")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
		return nil
	}

	// trim(string) / ltrim(string) / rtrim(string)
	if matchFuncStartAndSuffix(expr, "trim") {
		inner := extractFuncArgs(expr, "trim")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimSpace(str)
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "ltrim") {
		inner := extractFuncArgs(expr, "ltrim")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimLeft(str, " \t\n\r")
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "rtrim") {
		inner := extractFuncArgs(expr, "rtrim")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimRight(str, " \t\n\r")
		}
		return nil
	}

	// replace(string, search, replacement)
	if matchFuncStartAndSuffix(expr, "replace") {
		inner := extractFuncArgs(expr, "replace")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 3 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			search := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			repl := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
			return strings.ReplaceAll(str, search, repl)
		}
		return nil
	}

	// split(string, delimiter)
	if matchFuncStartAndSuffix(expr, "split") {
		inner := extractFuncArgs(expr, "split")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			delim := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			parts := strings.Split(str, delim)
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result
		}
		return nil
	}

	// substring(string, start, [length])
	if matchFuncStartAndSuffix(expr, "substring") {
		inner := extractFuncArgs(expr, "substring")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			start, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			length := len(str) - start
			if len(args) >= 3 {
				length, _ = strconv.Atoi(strings.TrimSpace(args[2]))
			}
			if start >= len(str) {
				return ""
			}
			end := start + length
			if end > len(str) {
				end = len(str)
			}
			return str[start:end]
		}
		return nil
	}

	// left(string, n) - return first n characters
	if matchFuncStartAndSuffix(expr, "left") {
		inner := extractFuncArgs(expr, "left")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[:n]
		}
		return nil
	}

	// right(string, n) - return last n characters
	if matchFuncStartAndSuffix(expr, "right") {
		inner := extractFuncArgs(expr, "right")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[len(str)-n:]
		}
		return nil
	}

	// reverse(string) - reverse a string
	if matchFuncStartAndSuffix(expr, "reverse") {
		inner := extractFuncArgs(expr, "reverse")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
		return nil
	}

	// lpad(string, length, padString) - left-pad string to specified length
	if matchFuncStartAndSuffix(expr, "lpad") {
		inner := extractFuncArgs(expr, "lpad")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
				// Remove quotes if present
				padStr = strings.Trim(padStr, "'\"")
			}
			if len(str) >= length {
				return str[:length]
			}
			// Pad to the left
			padLen := length - len(str)
			padding := ""
			for len(padding) < padLen {
				padding += padStr
			}
			return padding[:padLen] + str
		}
		return nil
	}

	// rpad(string, length, padString) - right-pad string to specified length
	if matchFuncStartAndSuffix(expr, "rpad") {
		inner := extractFuncArgs(expr, "rpad")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
				// Remove quotes if present
				padStr = strings.Trim(padStr, "'\"")
			}
			if len(str) >= length {
				return str[:length]
			}
			// Pad to the right
			padLen := length - len(str)
			padding := ""
			for len(padding) < padLen {
				padding += padStr
			}
			return str + padding[:padLen]
		}
		return nil
	}

	// format(template, ...args) - string formatting (printf-style)
	if matchFuncStartAndSuffix(expr, "format") {
		inner := extractFuncArgs(expr, "format")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			template := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			// Remove quotes from template
			template = strings.Trim(template, "'\"")

			// Evaluate remaining arguments
			formatArgs := make([]interface{}, 0, len(args)-1)
			for i := 1; i < len(args); i++ {
				val := e.evaluateExpressionWithContext(strings.TrimSpace(args[i]), nodes, rels)
				formatArgs = append(formatArgs, val)
			}

			// Simple format string replacement
			// Supports %s (string), %d (integer), %f (float), %v (any)
			return fmt.Sprintf(template, formatArgs...)
		}
		return nil
	}

	// ========================================
	// Date/Time Functions (Neo4j compatible)
	// ========================================

	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return time.Now().UnixMilli()
	}

	// datetime() - current datetime as ISO 8601 string or parse from argument
	if isFunctionCall(expr, "datetime") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		if inner == "" {
			// No argument - return current datetime
			return time.Now().Format(time.RFC3339)
		}
		// Try to parse argument as ISO 8601 string
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			// Try parsing various formats
			for _, layout := range []string{
				time.RFC3339,
				"2006-01-02T15:04:05",
				"2006-01-02 15:04:05",
				"2006-01-02",
			} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format(time.RFC3339)
				}
			}
		}
		return nil
	}

	// localdatetime() - current local datetime
	if lowerExpr == "localdatetime()" {
		return time.Now().Format("2006-01-02T15:04:05")
	}

	// date() - current date or parse from argument
	if isFunctionCall(expr, "date") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if inner == "" {
			// No argument - return current date
			return time.Now().Format("2006-01-02")
		}
		// Try to parse argument
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return t.Format("2006-01-02")
			}
			// Try parsing datetime and extracting date
			for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format("2006-01-02")
				}
			}
		}
		return nil
	}

	// time() - current time or parse from argument
	if isFunctionCall(expr, "time") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if inner == "" {
			// No argument - return current time
			return time.Now().Format("15:04:05")
		}
		// Try to parse argument
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			// Try parsing various time formats
			for _, layout := range []string{"15:04:05", "15:04:05.000", "15:04"} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format("15:04:05")
				}
			}
		}
		return nil
	}

	// localtime() - current local time
	if lowerExpr == "localtime()" {
		return time.Now().Format("15:04:05")
	}

	// date.year(date), date.month(date), date.day(date) - extract components
	if matchFuncStartAndSuffix(expr, "date.year") {
		inner := extractFuncArgs(expr, "date.year")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Year())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "date.month") {
		inner := extractFuncArgs(expr, "date.month")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Month())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "date.day") {
		inner := extractFuncArgs(expr, "date.day")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Day())
			}
		}
		return nil
	}

	// date.week(date) - ISO week number (1-53)
	if matchFuncStartAndSuffix(expr, "date.week") {
		inner := extractFuncArgs(expr, "date.week")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				_, week := t.ISOWeek()
				return int64(week)
			}
		}
		return nil
	}

	// date.quarter(date) - quarter of year (1-4)
	if matchFuncStartAndSuffix(expr, "date.quarter") {
		inner := extractFuncArgs(expr, "date.quarter")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64((int(t.Month())-1)/3 + 1)
			}
		}
		return nil
	}

	// date.dayOfWeek(date) - day of week (1=Monday, 7=Sunday, ISO 8601)
	if matchFuncStartAndSuffix(expr, "date.dayofweek") {
		inner := extractFuncArgs(expr, "date.dayofweek")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				dow := int64(t.Weekday())
				if dow == 0 {
					dow = 7 // Sunday = 7 in ISO 8601
				}
				return dow
			}
		}
		return nil
	}

	// date.dayOfYear(date) - day of year (1-366)
	if matchFuncStartAndSuffix(expr, "date.dayofyear") {
		inner := extractFuncArgs(expr, "date.dayofyear")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.YearDay())
			}
		}
		return nil
	}

	// date.ordinalDay(date) - same as dayOfYear
	if matchFuncStartAndSuffix(expr, "date.ordinalday") {
		inner := extractFuncArgs(expr, "date.ordinalday")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.YearDay())
			}
		}
		return nil
	}

	// date.weekYear(date) - ISO week year (may differ from calendar year at year boundaries)
	if matchFuncStartAndSuffix(expr, "date.weekyear") {
		inner := extractFuncArgs(expr, "date.weekyear")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				year, _ := t.ISOWeek()
				return int64(year)
			}
		}
		return nil
	}

	// date.truncate(unit, date) - truncate date to specified unit
	if matchFuncStartAndSuffix(expr, "date.truncate") {
		inner := extractFuncArgs(expr, "date.truncate")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				if t, err := time.Parse("2006-01-02", str); err == nil {
					switch strings.ToLower(unit) {
					case "year":
						return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "quarter":
						q := (int(t.Month())-1)/3*3 + 1
						return time.Date(t.Year(), time.Month(q), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "month":
						return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "week":
						// Go back to Monday of current week
						offset := int(t.Weekday())
						if offset == 0 {
							offset = 7
						}
						return t.AddDate(0, 0, -(offset - 1)).Format("2006-01-02")
					case "day":
						return t.Format("2006-01-02")
					}
				}
			}
		}
		return nil
	}

	// datetime.truncate(unit, datetime) - truncate datetime to specified unit
	if matchFuncStartAndSuffix(expr, "datetime.truncate") {
		inner := extractFuncArgs(expr, "datetime.truncate")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				t := parseDateTime(str)
				if !t.IsZero() {
					switch strings.ToLower(unit) {
					case "year":
						return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "quarter":
						q := (int(t.Month())-1)/3*3 + 1
						return time.Date(t.Year(), time.Month(q), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "month":
						return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "week":
						offset := int(t.Weekday())
						if offset == 0 {
							offset = 7
						}
						return t.AddDate(0, 0, -(offset - 1)).Truncate(24 * time.Hour).Format(time.RFC3339)
					case "day":
						return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "hour":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "minute":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).Format(time.RFC3339)
					case "second":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()).Format(time.RFC3339)
					}
				}
			}
		}
		return nil
	}

	// time.truncate(unit, time) - truncate time to specified unit
	if matchFuncStartAndSuffix(expr, "time.truncate") {
		inner := extractFuncArgs(expr, "time.truncate")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				if t, err := time.Parse("15:04:05", str); err == nil {
					switch strings.ToLower(unit) {
					case "hour":
						return time.Date(0, 1, 1, t.Hour(), 0, 0, 0, time.UTC).Format("15:04:05")
					case "minute":
						return time.Date(0, 1, 1, t.Hour(), t.Minute(), 0, 0, time.UTC).Format("15:04:05")
					case "second":
						return t.Format("15:04:05")
					}
				}
			}
		}
		return nil
	}

	// datetime.hour(datetime), datetime.minute(datetime), datetime.second(datetime)
	if matchFuncStartAndSuffix(expr, "datetime.hour") {
		inner := extractFuncArgs(expr, "datetime.hour")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Hour())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "datetime.minute") {
		inner := extractFuncArgs(expr, "datetime.minute")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Minute())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "datetime.second") {
		inner := extractFuncArgs(expr, "datetime.second")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Second())
			}
		}
		return nil
	}

	// datetime.year(datetime), datetime.month(datetime), datetime.day(datetime)
	if matchFuncStartAndSuffix(expr, "datetime.year") {
		inner := extractFuncArgs(expr, "datetime.year")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Year())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "datetime.month") {
		inner := extractFuncArgs(expr, "datetime.month")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Month())
			}
		}
		return nil
	}
	if matchFuncStartAndSuffix(expr, "datetime.day") {
		inner := extractFuncArgs(expr, "datetime.day")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Day())
			}
		}
		return nil
	}

	// duration.inMonths(duration) - convert duration to months
	if matchFuncStartAndSuffix(expr, "duration.inmonths") {
		inner := extractFuncArgs(expr, "duration.inmonths")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.Years*12 + d.Months
		}
		return nil
	}

	// duration() - create duration from ISO 8601 string (P1Y2M3DT4H5M6S)
	// Returns a CypherDuration struct that can be used in arithmetic
	if isFunctionCall(expr, "duration") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			return parseDuration(str)
		}
		// Handle map format: duration({days: 5, hours: 3})
		if m, ok := val.(map[string]interface{}); ok {
			return durationFromMap(m)
		}
		return nil
	}

	// duration.between(d1, d2) - calculate duration between two dates/datetimes
	if matchFuncStartAndSuffix(expr, "duration.between") {
		inner := extractFuncArgs(expr, "duration.between")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			d1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			d2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			return durationBetween(d1, d2)
		}
		return nil
	}

	// duration.inDays(duration) - convert duration to days
	if matchFuncStartAndSuffix(expr, "duration.indays") {
		inner := extractFuncArgs(expr, "duration.indays")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalDays()
		}
		return nil
	}

	// duration.inSeconds(duration) - convert duration to seconds
	if matchFuncStartAndSuffix(expr, "duration.inseconds") {
		inner := extractFuncArgs(expr, "duration.inseconds")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalSeconds()
		}
		return nil
	}

	// ========================================
	// Math Functions
	// ========================================

	// abs(number)
	if matchFuncStartAndSuffix(expr, "abs") {
		inner := extractFuncArgs(expr, "abs")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			if v < 0 {
				return -v
			}
			return v
		case float64:
			if v < 0 {
				return -v
			}
			return v
		}
		return nil
	}

	// ceil(number)
	if matchFuncStartAndSuffix(expr, "ceil") {
		inner := extractFuncArgs(expr, "ceil")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}

	// floor(number)
	if matchFuncStartAndSuffix(expr, "floor") {
		inner := extractFuncArgs(expr, "floor")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}

	// round(number)
	if matchFuncStartAndSuffix(expr, "round") {
		inner := extractFuncArgs(expr, "round")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}

	// sign(number)
	if matchFuncStartAndSuffix(expr, "sign") {
		inner := extractFuncArgs(expr, "sign")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			if f > 0 {
				return int64(1)
			} else if f < 0 {
				return int64(-1)
			}
			return int64(0)
		}
		return nil
	}

	// randomUUID()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}

	// rand() - random float between 0 and 1
	if lowerExpr == "rand()" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		// Convert to float between 0 and 1
		val := float64(b[0]^b[1]^b[2]^b[3]) / 256.0
		return val
	}

	// ========================================

	// Plugin Functions (loaded dynamically from .so files)
	// ========================================

	// Try plugin functions for any namespaced function call (contains dots)
	// This is GENERIC - works for any plugin, not just a specific one
	if strings.Contains(lowerExpr, ".") && looksLikeFunctionCall(lowerExpr) {
		if result, handled := e.tryCallPluginFunction(expr, nodes, rels); handled {
			return result
		}
	}

	// apoc.create.uuid() - Generate a UUID (alias for randomUUID)
	// TODO: Move to plugin
	if lowerExpr == "apoc.create.uuid()" {
		return e.generateUUID()
	}

	// apoc.text.join(list, separator) - Join list elements with separator
	if isFunctionCall(expr, "apoc.text.join") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			sepVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			sep := ""
			if s, ok := sepVal.(string); ok {
				sep = strings.Trim(s, "'\"")
			}
			// Convert list to string slice
			var parts []string
			switch v := listVal.(type) {
			case []interface{}:
				for _, item := range v {
					parts = append(parts, fmt.Sprintf("%v", item))
				}
			case []string:
				parts = v
			}
			return strings.Join(parts, sep)
		}
		return nil
	}

	// apoc.coll.flatten(list) - Flatten nested lists into a single list
	if isFunctionCall(expr, "apoc.coll.flatten") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return flattenList(listVal)
	}

	// apoc.coll.toSet(list) - Remove duplicates from list
	if isFunctionCall(expr, "apoc.coll.toset") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return toSet(listVal)
	}

	// apoc.coll.sum(list) - Sum numeric values in list
	if isFunctionCall(expr, "apoc.coll.sum") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollSum(listVal)
	}

	// apoc.coll.avg(list) - Average of numeric values in list
	if isFunctionCall(expr, "apoc.coll.avg") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollAvg(listVal)
	}

	// apoc.coll.min(list) - Minimum value in list
	if isFunctionCall(expr, "apoc.coll.min") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollMin(listVal)
	}

	// apoc.coll.max(list) - Maximum value in list
	if isFunctionCall(expr, "apoc.coll.max") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollMax(listVal)
	}

	// apoc.coll.sort(list) - Sort list in ascending order
	if isFunctionCall(expr, "apoc.coll.sort") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollSort(listVal)
	}

	// apoc.coll.sortNodes(nodes, property) - Sort nodes by property
	if isFunctionCall(expr, "apoc.coll.sortnodes") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			propName := strings.Trim(args[1], "'\"")
			return apocCollSortNodes(listVal, propName)
		}
		return nil
	}

	// apoc.coll.reverse(list) - Reverse a list
	if isFunctionCall(expr, "apoc.coll.reverse") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollReverse(listVal)
	}

	// apoc.coll.union(list1, list2) - Union of two lists (removes duplicates)
	if isFunctionCall(expr, "apoc.coll.union") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollUnion(list1, list2)
		}
		return nil
	}

	// apoc.coll.unionAll(list1, list2) - Union of two lists (keeps duplicates)
	if isFunctionCall(expr, "apoc.coll.unionall") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollUnionAll(list1, list2)
		}
		return nil
	}

	// apoc.coll.intersection(list1, list2) - Intersection of two lists
	if isFunctionCall(expr, "apoc.coll.intersection") {
		inner := strings.TrimSpace(expr[23 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollIntersection(list1, list2)
		}
		return nil
	}

	// apoc.coll.subtract(list1, list2) - Elements in list1 but not in list2
	if isFunctionCall(expr, "apoc.coll.subtract") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollSubtract(list1, list2)
		}
		return nil
	}

	// apoc.coll.contains(list, value) - Check if list contains value
	if isFunctionCall(expr, "apoc.coll.contains") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContains(listVal, value)
		}
		return false
	}

	// apoc.coll.containsAll(list1, list2) - Check if list1 contains all elements of list2
	if isFunctionCall(expr, "apoc.coll.containsall") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContainsAll(list1, list2)
		}
		return false
	}

	// apoc.coll.containsAny(list1, list2) - Check if list1 contains any element of list2
	if isFunctionCall(expr, "apoc.coll.containsany") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContainsAny(list1, list2)
		}
		return false
	}

	// apoc.coll.indexOf(list, value) - Find index of value in list (-1 if not found)
	if isFunctionCall(expr, "apoc.coll.indexof") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollIndexOf(listVal, value)
		}
		return int64(-1)
	}

	// apoc.coll.split(list, value) - Split list at occurrences of value
	if isFunctionCall(expr, "apoc.coll.split") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollSplit(listVal, value)
		}
		return nil
	}

	// apoc.coll.partition(list, size) - Partition list into sublists of given size
	if isFunctionCall(expr, "apoc.coll.partition") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			sizeVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollPartition(listVal, sizeVal)
		}
		return nil
	}

	// apoc.coll.pairs(list) - Create pairs from consecutive elements [[a,b], [b,c], ...]
	if isFunctionCall(expr, "apoc.coll.pairs") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollPairs(listVal)
	}

	// apoc.coll.zip(list1, list2) - Zip two lists into pairs [[a1,b1], [a2,b2], ...]
	if isFunctionCall(expr, "apoc.coll.zip") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollZip(list1, list2)
		}
		return nil
	}

	// apoc.coll.frequencies(list) - Count frequency of each element
	if isFunctionCall(expr, "apoc.coll.frequencies") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollFrequencies(listVal)
	}

	// apoc.coll.occurrences(list, value) - Count occurrences of value in list
	if isFunctionCall(expr, "apoc.coll.occurrences") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollOccurrences(listVal, value)
		}
		return int64(0)
	}

	// apoc.convert.toJson(value) - Convert value to JSON string
	if isFunctionCall(expr, "apoc.convert.tojson") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return string(jsonBytes)
	}

	// apoc.convert.fromJsonMap(json) - Parse JSON string to map
	if isFunctionCall(expr, "apoc.convert.fromjsonmap") {
		inner := strings.TrimSpace(expr[24 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonStr, ok := val.(string)
		if !ok {
			return nil
		}
		jsonStr = strings.Trim(jsonStr, "'\"")
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
			return nil
		}
		return result
	}

	// apoc.convert.fromJsonList(json) - Parse JSON string to list
	if isFunctionCall(expr, "apoc.convert.fromjsonlist") {
		inner := strings.TrimSpace(expr[25 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonStr, ok := val.(string)
		if !ok {
			return nil
		}
		jsonStr = strings.Trim(jsonStr, "'\"")
		var result []interface{}
		if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
			return nil
		}
		return result
	}

	// apoc.meta.type(value) - Get the Cypher type name of a value
	if isFunctionCall(expr, "apoc.meta.type") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return getCypherType(val)
	}

	// apoc.meta.isType(value, typeName) - Check if value is of given type
	if isFunctionCall(expr, "apoc.meta.istype") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val := e.evaluateExpressionWithContext(args[0], nodes, rels)
			typeVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			typeName, ok := typeVal.(string)
			if !ok {
				return false
			}
			typeName = strings.Trim(typeName, "'\"")
			actualType := getCypherType(val)
			return strings.EqualFold(actualType, typeName)
		}
		return false
	}

	// apoc.map.merge(map1, map2) - Merge two maps (map2 values override map1)
	if isFunctionCall(expr, "apoc.map.merge") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			map1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			map2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return mergeMaps(map1, map2)
		}
		return nil
	}

	// apoc.map.fromPairs(list) - Create map from list of [key, value] pairs
	if isFunctionCall(expr, "apoc.map.frompairs") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fromPairs(listVal)
	}

	// apoc.map.fromLists(keys, values) - Create map from parallel lists
	if isFunctionCall(expr, "apoc.map.fromlists") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			keys := e.evaluateExpressionWithContext(args[0], nodes, rels)
			values := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return fromLists(keys, values)
		}
		return nil
	}

	// ========================================
	// Trigonometric Functions
	// ========================================

	// sin(x) - sine of x (radians)
	if matchFuncStartAndSuffix(expr, "sin") {
		inner := extractFuncArgs(expr, "sin")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sin(f)
		}
		return nil
	}

	// cos(x) - cosine of x (radians)
	if matchFuncStartAndSuffix(expr, "cos") {
		inner := extractFuncArgs(expr, "cos")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cos(f)
		}
		return nil
	}

	// tan(x) - tangent of x (radians)
	if matchFuncStartAndSuffix(expr, "tan") {
		inner := extractFuncArgs(expr, "tan")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tan(f)
		}
		return nil
	}

	// cot(x) - cotangent of x (radians)
	if matchFuncStartAndSuffix(expr, "cot") {
		inner := extractFuncArgs(expr, "cot")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return 1.0 / math.Tan(f)
		}
		return nil
	}

	// asin(x) - arc sine
	if matchFuncStartAndSuffix(expr, "asin") {
		inner := extractFuncArgs(expr, "asin")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Asin(f)
		}
		return nil
	}

	// acos(x) - arc cosine
	if matchFuncStartAndSuffix(expr, "acos") {
		inner := extractFuncArgs(expr, "acos")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Acos(f)
		}
		return nil
	}

	// atan(x) - arc tangent
	if matchFuncStartAndSuffix(expr, "atan") {
		inner := extractFuncArgs(expr, "atan")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Atan(f)
		}
		return nil
	}

	// atan2(y, x) - arc tangent of y/x
	if matchFuncStartAndSuffix(expr, "atan2") {
		inner := extractFuncArgs(expr, "atan2")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			y, ok1 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			x, ok2 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Atan2(y, x)
			}
		}
		return nil
	}

	// ========================================
	// Exponential and Logarithmic Functions
	// ========================================

	// exp(x) - e^x
	if matchFuncStartAndSuffix(expr, "exp") {
		inner := extractFuncArgs(expr, "exp")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Exp(f)
		}
		return nil
	}

	// log(x) - natural logarithm
	if matchFuncStartAndSuffix(expr, "log") {
		inner := extractFuncArgs(expr, "log")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log(f)
		}
		return nil
	}

	// log10(x) - base-10 logarithm
	if matchFuncStartAndSuffix(expr, "log10") {
		inner := extractFuncArgs(expr, "log10")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log10(f)
		}
		return nil
	}

	// sqrt(x) - square root
	if matchFuncStartAndSuffix(expr, "sqrt") {
		inner := extractFuncArgs(expr, "sqrt")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sqrt(f)
		}
		return nil
	}

	// ========================================
	// Angle Conversion Functions
	// ========================================

	// radians(degrees) - convert degrees to radians
	if matchFuncStartAndSuffix(expr, "radians") {
		inner := extractFuncArgs(expr, "radians")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * math.Pi / 180.0
		}
		return nil
	}

	// degrees(radians) - convert radians to degrees
	if matchFuncStartAndSuffix(expr, "degrees") {
		inner := extractFuncArgs(expr, "degrees")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * 180.0 / math.Pi
		}
		return nil
	}

	// haversin(x) - half of versine = (1 - cos(x))/2
	if matchFuncStartAndSuffix(expr, "haversin") {
		inner := extractFuncArgs(expr, "haversin")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return (1 - math.Cos(f)) / 2
		}
		return nil
	}

	// sinh(x) - hyperbolic sine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "sinh") {
		inner := extractFuncArgs(expr, "sinh")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sinh(f)
		}
		return nil
	}

	// cosh(x) - hyperbolic cosine (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "cosh") {
		inner := extractFuncArgs(expr, "cosh")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cosh(f)
		}
		return nil
	}

	// tanh(x) - hyperbolic tangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "tanh") {
		inner := extractFuncArgs(expr, "tanh")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tanh(f)
		}
		return nil
	}

	// coth(x) - hyperbolic cotangent (Neo4j 2025.06+)
	if matchFuncStartAndSuffix(expr, "coth") {
		inner := extractFuncArgs(expr, "coth")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			sinh := math.Sinh(f)
			if sinh == 0 {
				return math.NaN()
			}
			return math.Cosh(f) / sinh
		}
		return nil
	}

	// power(base, exponent) - raise base to power of exponent
	if matchFuncStartAndSuffix(expr, "power") {
		inner := extractFuncArgs(expr, "power")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			base, ok1 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			exp, ok2 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Pow(base, exp)
			}
		}
		return nil
	}

	// ========================================
	// Mathematical Constants
	// ========================================

	// pi() - mathematical constant π
	if lowerExpr == "pi()" {
		return math.Pi
	}

	// e() - mathematical constant e
	if lowerExpr == "e()" {
		return math.E
	}

	// ========================================
	// Relationship Functions
	// ========================================

	// startNode(r) - return start node of relationship
	if matchFuncStartAndSuffix(expr, "startnode") {
		inner := extractFuncArgs(expr, "startnode")
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.StartNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// endNode(r) - return end node of relationship
	if matchFuncStartAndSuffix(expr, "endnode") {
		inner := extractFuncArgs(expr, "endnode")
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.EndNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// nodes(path) - return list of nodes in a path
	if matchFuncStartAndSuffix(expr, "nodes") {
		inner := extractFuncArgs(expr, "nodes")
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner]; ok && pathResult != nil {
				var result []interface{}
				for _, node := range pathResult.Nodes {
					result = append(result, e.nodeToMap(node))
				}
				return result
			}
		}
		// Then check allPathNodes (for variable-length patterns without explicit path variable)
		if allPathNodes != nil && len(allPathNodes) > 0 {
			var result []interface{}
			for _, node := range allPathNodes {
				result = append(result, e.nodeToMap(node))
			}
			return result
		}
		// Fallback: return single node from node context
		if node, ok := nodes[inner]; ok {
			return []interface{}{e.nodeToMap(node)}
		}
		return []interface{}{}
	}

	// relationships(path) - return list of relationships in a path
	if matchFuncStartAndSuffix(expr, "relationships") {
		inner := extractFuncArgs(expr, "relationships")
		// First check if we have a full path by that variable name
		if paths != nil {
			if pathResult, ok := paths[inner]; ok && pathResult != nil {
				var result []interface{}
				for _, edge := range pathResult.Relationships {
					result = append(result, map[string]interface{}{
						"_edgeId":    string(edge.ID),
						"type":       edge.Type,
						"properties": edge.Properties,
					})
				}
				return result
			}
		}
		// Then check allPathEdges (for variable-length patterns without explicit path variable)
		if allPathEdges != nil && len(allPathEdges) > 0 {
			var result []interface{}
			for _, edge := range allPathEdges {
				result = append(result, map[string]interface{}{
					"_edgeId":    string(edge.ID),
					"type":       edge.Type,
					"properties": edge.Properties,
				})
			}
			return result
		}
		// Fallback: return single relationship from rel context
		if rel, ok := rels[inner]; ok {
			return []interface{}{map[string]interface{}{
				"_edgeId":    string(rel.ID),
				"type":       rel.Type,
				"properties": rel.Properties,
			}}
		}
		return []interface{}{}
	}

	// ========================================
	// Null Check Functions
	// ========================================

	// isEmpty(list/map/string) - check if empty
	if matchFuncStartAndSuffix(expr, "isempty") {
		inner := extractFuncArgs(expr, "isempty")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case nil:
			return true
		case string:
			return len(v) == 0
		case []interface{}:
			return len(v) == 0
		case map[string]interface{}:
			return len(v) == 0
		}
		return false
	}

	// isNaN(number) - check if not a number
	if matchFuncStartAndSuffix(expr, "isnan") {
		inner := extractFuncArgs(expr, "isnan")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.IsNaN(f)
		}
		return false
	}

	// nullIf(val1, val2) - return null if val1 = val2
	if matchFuncStartAndSuffix(expr, "nullif") {
		inner := extractFuncArgs(expr, "nullif")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			val2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2) {
				return nil
			}
			return val1
		}
		return nil
	}

	// ========================================
	// String Functions (additional)
	// ========================================

	// btrim(string) / btrim(string, chars) - trim both sides
	if matchFuncStartAndSuffix(expr, "btrim") {
		inner := extractFuncArgs(expr, "btrim")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			if len(args) >= 2 {
				chars := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
				return strings.Trim(str, chars)
			}
			return strings.TrimSpace(str)
		}
		return nil
	}

	// char_length(string)
	if matchFuncStartAndSuffix(expr, "char_length") {
		inner := extractFuncArgs(expr, "char_length")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// character_length(string) - alias for char_length
	if matchFuncStartAndSuffix(expr, "character_length") {
		inner := extractFuncArgs(expr, "character_length")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// normalize(string) - Unicode normalization
	if matchFuncStartAndSuffix(expr, "normalize") {
		inner := extractFuncArgs(expr, "normalize")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			// Simple normalization - just return the string (full Unicode normalization would require unicode package)
			return str
		}
		return nil
	}

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// percentileCont(expr, percentile) - continuous percentile
	if matchFuncStartAndSuffix(expr, "percentilecont") {
		inner := extractFuncArgs(expr, "percentilecont")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// percentileDisc(expr, percentile) - discrete percentile
	if matchFuncStartAndSuffix(expr, "percentiledisc") {
		inner := extractFuncArgs(expr, "percentiledisc")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// stDev(expr) - standard deviation
	if matchFuncStartAndSuffix(expr, "stdev") {
		inner := extractFuncArgs(expr, "stdev")
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// stDevP(expr) - population standard deviation
	if matchFuncStartAndSuffix(expr, "stdevp") {
		inner := extractFuncArgs(expr, "stdevp")
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// ========================================
	// Reduce Function
	// ========================================

	// reduce(acc = initial, x IN list | expr) - reduce a list
	if matchFuncStartAndSuffix(expr, "reduce") {
		inner := extractFuncArgs(expr, "reduce")

		// Parse: acc = initial, x IN list | expr
		eqIdx := strings.Index(inner, "=")
		commaIdx := strings.Index(inner, ",")
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		pipeIdx := strings.Index(inner, "|")

		if eqIdx > 0 && commaIdx > eqIdx && inIdx > commaIdx && pipeIdx > inIdx {
			accName := strings.TrimSpace(inner[:eqIdx])
			initialExpr := strings.TrimSpace(inner[eqIdx+1 : commaIdx])
			varName := strings.TrimSpace(inner[commaIdx+1 : inIdx])
			listExpr := strings.TrimSpace(inner[inIdx+4 : pipeIdx])
			reduceExpr := strings.TrimSpace(inner[pipeIdx+1:])

			// Get initial value
			acc := e.evaluateExpressionWithContext(initialExpr, nodes, rels)

			// Get list
			list := e.evaluateExpressionWithContext(listExpr, nodes, rels)

			var items []interface{}
			switch v := list.(type) {
			case []interface{}:
				items = v
			default:
				items = []interface{}{list}
			}

			// Apply reduce
			for _, item := range items {
				// Create context with acc and item
				tempNodes := make(map[string]*storage.Node)
				for k, v := range nodes {
					tempNodes[k] = v
				}

				// Store acc and item as pseudo-properties (simplified)
				// For a proper implementation, we'd need to handle this more comprehensively
				substitutedExpr := strings.ReplaceAll(reduceExpr, accName, fmt.Sprintf("%v", acc))
				substitutedExpr = strings.ReplaceAll(substitutedExpr, varName, fmt.Sprintf("%v", item))

				acc = e.evaluateExpressionWithContext(substitutedExpr, tempNodes, rels)
			}

			return acc
		}
		return nil
	}

	// ========================================
	// Kalman Filter Functions
	// ========================================

	// kalman.init() or kalman.init({processNoise: 0.1, measurementNoise: 88.0})
	if matchFuncStartAndSuffix(expr, "kalman.init") {
		inner := extractFuncArgs(expr, "kalman.init")
		var configMap map[string]interface{}
		if inner != "" {
			val := e.evaluateExpressionWithContext(inner, nodes, rels)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanInit(configMap)
	}

	// kalman.process(measurement, stateJson) or kalman.process(measurement, stateJson, target)
	if matchFuncStartAndSuffix(expr, "kalman.process") {
		inner := extractFuncArgs(expr, "kalman.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels).(string)
			target := 0.0
			if len(args) >= 3 {
				target, _ = toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
			}
			return kalmanProcess(measurement, stateJSON, target)
		}
		return nil
	}

	// kalman.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.predict") {
		inner := extractFuncArgs(expr, "kalman.predict")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels).(string)
			stepsFloat, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			steps := int(stepsFloat)
			return kalmanPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.state(stateJson) - get current state estimate
	if matchFuncStartAndSuffix(expr, "kalman.state") {
		inner := extractFuncArgs(expr, "kalman.state")
		stateJSON, _ := e.evaluateExpressionWithContext(inner, nodes, rels).(string)
		return kalmanStateValue(stateJSON)
	}

	// kalman.reset(stateJson) - reset to initial values
	if matchFuncStartAndSuffix(expr, "kalman.reset") {
		inner := extractFuncArgs(expr, "kalman.reset")
		stateJSON, _ := e.evaluateExpressionWithContext(inner, nodes, rels).(string)
		return kalmanReset(stateJSON)
	}

	// kalman.velocity.init() or kalman.velocity.init(initialPos, initialVel)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.init") {
		inner := extractFuncArgs(expr, "kalman.velocity.init")
		if inner == "" {
			return kalmanVelocityInit(0, 0, false)
		}
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			pos, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			vel, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			return kalmanVelocityInit(pos, vel, true)
		}
		return kalmanVelocityInit(0, 0, false)
	}

	// kalman.velocity.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.process") {
		inner := extractFuncArgs(expr, "kalman.velocity.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels).(string)
			return kalmanVelocityProcess(measurement, stateJSON)
		}
		return nil
	}

	// kalman.velocity.predict(stateJson, steps)
	if matchFuncStartAndSuffix(expr, "kalman.velocity.predict") {
		inner := extractFuncArgs(expr, "kalman.velocity.predict")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			stateJSON, _ := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels).(string)
			stepsFloat, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			steps := int(stepsFloat)
			return kalmanVelocityPredict(stateJSON, steps)
		}
		return nil
	}

	// kalman.adaptive.init() or kalman.adaptive.init({trendThreshold: 0.1, ...})
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.init") {
		inner := extractFuncArgs(expr, "kalman.adaptive.init")
		var configMap map[string]interface{}
		if inner != "" {
			val := e.evaluateExpressionWithContext(inner, nodes, rels)
			if m, ok := val.(map[string]interface{}); ok {
				configMap = m
			}
		}
		return kalmanAdaptiveInit(configMap)
	}

	// kalman.adaptive.process(measurement, stateJson)
	if matchFuncStartAndSuffix(expr, "kalman.adaptive.process") {
		inner := extractFuncArgs(expr, "kalman.adaptive.process")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			measurement, _ := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			stateJSON, _ := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels).(string)
			return kalmanAdaptiveProcess(measurement, stateJSON)
		}
		return nil
	}

	// ========================================
	// Vector Functions
	// ========================================

	// vector.similarity.cosine(v1, v2)
	if matchFuncStartAndSuffix(expr, "vector.similarity.cosine") {
		inner := extractFuncArgs(expr, "vector.similarity.cosine")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return vector.CosineSimilarityFloat64(vec1, vec2)
			}
		}
		return nil
	}

	// vector.similarity.euclidean(v1, v2)
	if matchFuncStartAndSuffix(expr, "vector.similarity.euclidean") {
		inner := extractFuncArgs(expr, "vector.similarity.euclidean")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return vector.EuclideanSimilarityFloat64(vec1, vec2)
			}
		}
		return nil
	}

	// ========================================
	// Point/Spatial Functions (basic support)
	// ========================================

	// point({x: val, y: val}) or point({latitude: val, longitude: val})
	if matchFuncStartAndSuffix(expr, "point") {
		inner := extractFuncArgs(expr, "point")
		// Return the point as a map
		if strings.HasPrefix(inner, "{") && strings.HasSuffix(inner, "}") {
			props := e.parseProperties(inner)
			return props
		}
		return nil
	}

	// distance(p1, p2) - Euclidean distance between two points
	if matchFuncStartAndSuffix(expr, "distance") {
		inner := extractFuncArgs(expr, "distance")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// withinBBox(point, lowerLeft, upperRight) - checks if point is within bounding box
	if matchFuncStartAndSuffix(expr, "withinbbox") {
		inner := extractFuncArgs(expr, "withinbbox")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Try x/y coordinates
		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		// Try lat/lon
		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.x(point) - get x coordinate
	if matchFuncStartAndSuffix(expr, "point.x") {
		inner := extractFuncArgs(expr, "point.x")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if x, ok := m["x"]; ok {
				if v, ok := toFloat64(x); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.y(point) - get y coordinate
	if matchFuncStartAndSuffix(expr, "point.y") {
		inner := extractFuncArgs(expr, "point.y")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if y, ok := m["y"]; ok {
				if v, ok := toFloat64(y); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.z(point) - get z coordinate (3D points)
	if matchFuncStartAndSuffix(expr, "point.z") {
		inner := extractFuncArgs(expr, "point.z")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.latitude(point) - get latitude
	if matchFuncStartAndSuffix(expr, "point.latitude") {
		inner := extractFuncArgs(expr, "point.latitude")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lat, ok := m["latitude"]; ok {
				if v, ok := toFloat64(lat); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.longitude(point) - get longitude
	if matchFuncStartAndSuffix(expr, "point.longitude") {
		inner := extractFuncArgs(expr, "point.longitude")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lon, ok := m["longitude"]; ok {
				if v, ok := toFloat64(lon); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.srid(point) - get SRID (Spatial Reference System Identifier)
	if matchFuncStartAndSuffix(expr, "point.srid") {
		inner := extractFuncArgs(expr, "point.srid")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if srid, ok := m["srid"]; ok {
				return srid
			}
			// Default SRID based on coordinate type
			if _, ok := m["latitude"]; ok {
				return int64(4326) // WGS84
			}
			return int64(7203) // Cartesian 2D
		}
		return nil
	}

	// point.distance(p1, p2) - alias for distance(p1, p2)
	if matchFuncStartAndSuffix(expr, "point.distance") {
		inner := extractFuncArgs(expr, "point.distance")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// point.withinBBox(point, lowerLeft, upperRight) - alias for withinBBox
	if matchFuncStartAndSuffix(expr, "point.withinbbox") {
		inner := extractFuncArgs(expr, "point.withinbbox")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.withinDistance(point, center, distance) - check if point is within distance of center
	if matchFuncStartAndSuffix(expr, "point.withindistance") {
		inner := extractFuncArgs(expr, "point.withindistance")
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		center := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		maxDist := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		cm, ok2 := center.(map[string]interface{})
		dist, ok3 := toFloat64(maxDist)
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Calculate distance between point and center
		x1, y1, hasXY1 := getXY(pm)
		x2, y2, hasXY2 := getXY(cm)
		if hasXY1 && hasXY2 {
			actualDist := math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
			return actualDist <= dist
		}

		lat1, lon1, hasLatLon1 := getLatLon(pm)
		lat2, lon2, hasLatLon2 := getLatLon(cm)
		if hasLatLon1 && hasLatLon2 {
			actualDist := haversineDistance(lat1, lon1, lat2, lon2)
			return actualDist <= dist
		}

		return false
	}

	// point.height(point) - get height/altitude (alias for z coordinate)
	if matchFuncStartAndSuffix(expr, "point.height") {
		inner := extractFuncArgs(expr, "point.height")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Try z first (3D Cartesian)
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
			// Try height (geographic)
			if h, ok := m["height"]; ok {
				if v, ok := toFloat64(h); ok {
					return v
				}
			}
			// Try altitude (alternative name)
			if alt, ok := m["altitude"]; ok {
				if v, ok := toFloat64(alt); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.crs(point) - get Coordinate Reference System name
	if matchFuncStartAndSuffix(expr, "point.crs") {
		inner := extractFuncArgs(expr, "point.crs")
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Check if CRS is explicitly set
			if crs, ok := m["crs"]; ok {
				return crs
			}
			// Infer CRS from coordinate type
			if _, ok := m["latitude"]; ok {
				if _, ok := m["height"]; ok {
					return "wgs-84-3d"
				}
				return "wgs-84"
			}
			if _, ok := m["z"]; ok {
				return "cartesian-3d"
			}
			return "cartesian"
		}
		return nil
	}

	// polygon(points) - create a polygon geometry from a list of points
	if matchFuncStartAndSuffix(expr, "polygon") {
		inner := extractFuncArgs(expr, "polygon")

		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}

			// Validate that we have at least 3 points for a valid polygon
			if len(pointList) < 3 {
				return nil
			}

			// Return a polygon structure
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}

		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 3 {
				return nil
			}
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}
		return nil
	}

	// lineString(points) - create a lineString geometry from a list of points
	if matchFuncStartAndSuffix(expr, "linestring") {
		inner := extractFuncArgs(expr, "linestring")

		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)

			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}

			// Validate that we have at least 2 points for a valid lineString
			if len(pointList) < 2 {
				return nil
			}

			// Return a lineString structure
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}

		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 2 {
				return nil
			}
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}
		return nil
	}

	// point.intersects(point, polygon) - check if point intersects with polygon
	if matchFuncStartAndSuffix(expr, "point.intersects") {
		inner := extractFuncArgs(expr, "point.intersects")
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}

		pointVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		polygonVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

		pm, ok1 := pointVal.(map[string]interface{})
		polygonMap, ok2 := polygonVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}

		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}

		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}

		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// point.contains(polygon, point) - check if polygon contains point
	if matchFuncStartAndSuffix(expr, "point.contains") {
		inner := extractFuncArgs(expr, "point.contains")
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}

		polygonVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		pointVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

		polygonMap, ok1 := polygonVal.(map[string]interface{})
		pm, ok2 := pointVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}

		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}

		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}

		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// ========================================
	// List Predicate Functions
	// ========================================

	// all(variable IN list WHERE predicate) - check if all elements match
	if matchFuncStartAndSuffix(expr, "all") {
		inner := extractFuncArgs(expr, "all")
		// Parse "variable IN list WHERE predicate"
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			// Create temporary context with variable
			tempNodes := make(map[string]*storage.Node)
			for k, v := range nodes {
				tempNodes[k] = v
			}
			// For simple values, we need to substitute in the predicate
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, tempNodes, rels)
			if result != true {
				return false
			}
		}
		return true
	}

	// any(variable IN list WHERE predicate) - check if any element matches
	if matchFuncStartAndSuffix(expr, "any") {
		inner := extractFuncArgs(expr, "any")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				return true
			}
		}
		return false
	}

	// none(variable IN list WHERE predicate) - check if no element matches
	if matchFuncStartAndSuffix(expr, "none") {
		inner := extractFuncArgs(expr, "none")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return true
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return true
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return true
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				return false
			}
		}
		return true
	}

	// single(variable IN list WHERE predicate) - check if exactly one element matches
	if matchFuncStartAndSuffix(expr, "single") {
		inner := extractFuncArgs(expr, "single")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		matchCount := 0
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				matchCount++
				if matchCount > 1 {
					return false
				}
			}
		}
		return matchCount == 1
	}

	// ========================================
	// Additional List Functions
	// ========================================

	// filter(variable IN list WHERE predicate) - filter list elements
	if matchFuncStartAndSuffix(expr, "filter") {
		inner := extractFuncArgs(expr, "filter")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, 0)
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			res := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if res == true {
				result = append(result, item)
			}
		}
		return result
	}

	// extract(variable IN list | expression) - transform list elements
	if matchFuncStartAndSuffix(expr, "extract") {
		inner := extractFuncArgs(expr, "extract")
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		pipeIdx := strings.Index(rest, " | ")
		if pipeIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:pipeIdx])
		transform := strings.TrimSpace(rest[pipeIdx+3:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, len(listVal))
		for i, item := range listVal {
			// Simple variable substitution for primitive values
			transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
			result[i] = e.evaluateExpressionWithContext(transformWithVal, nodes, rels)
		}
		return result
	}

	// [x IN list WHERE condition] - list comprehension with filter
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(strings.ToUpper(expr), " WHERE ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		upperInner := strings.ToUpper(inner)
		inIdx := strings.Index(upperInner, " IN ")
		whereIdx := strings.Index(upperInner, " WHERE ")

		if inIdx > 0 && whereIdx > inIdx {
			varName := strings.TrimSpace(inner[:inIdx])
			listExpr := strings.TrimSpace(inner[inIdx+4 : whereIdx])
			condition := strings.TrimSpace(inner[whereIdx+7:])

			// Evaluate the list expression
			list := e.evaluateExpressionWithContext(listExpr, nodes, rels)

			// Convert to []interface{} if needed
			var items []interface{}
			switch v := list.(type) {
			case []interface{}:
				items = v
			case []string:
				items = make([]interface{}, len(v))
				for i, s := range v {
					items[i] = s
				}
			default:
				return []interface{}{}
			}

			// Filter by condition
			result := make([]interface{}, 0, len(items))
			for _, item := range items {
				// Evaluate condition with item substituted
				itemStr := fmt.Sprintf("%v", item)

				// Handle different condition patterns
				matches := true
				if strings.Contains(condition, "<>") {
					parts := strings.SplitN(condition, "<>", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr != condVal
					}
				} else if strings.Contains(condition, "!=") {
					parts := strings.SplitN(condition, "!=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr != condVal
					}
				} else if strings.Contains(condition, ">=") {
					parts := strings.SplitN(condition, ">=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.TrimSpace(parts[1])
					if condVar == varName {
						if itemNum, ok := toFloat64(item); ok {
							if condNum, ok := toFloat64(e.parseValue(condVal)); ok {
								matches = itemNum >= condNum
							}
						}
					}
				} else if strings.Contains(condition, ">") {
					parts := strings.SplitN(condition, ">", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.TrimSpace(parts[1])
					if condVar == varName {
						if itemNum, ok := toFloat64(item); ok {
							if condNum, ok := toFloat64(e.parseValue(condVal)); ok {
								matches = itemNum > condNum
							}
						}
					}
				} else if strings.Contains(condition, "=") {
					parts := strings.SplitN(condition, "=", 2)
					condVar := strings.TrimSpace(parts[0])
					condVal := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if condVar == varName {
						matches = itemStr == condVal
					}
				}

				if matches {
					result = append(result, item)
				}
			}
			return result
		}
	}

	// [x IN list | expression] - list comprehension with transformation
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(expr, " | ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		if inIdx > 0 {
			varName := strings.TrimSpace(inner[:inIdx])
			rest := inner[inIdx+4:]
			pipeIdx := strings.Index(rest, " | ")
			if pipeIdx > 0 {
				listExpr := strings.TrimSpace(rest[:pipeIdx])
				transform := strings.TrimSpace(rest[pipeIdx+3:])

				// Use full context to properly evaluate path functions like relationships(path)
				list := e.evaluateExpressionWithContextFull(listExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				listVal, ok := list.([]interface{})
				if !ok {
					return []interface{}{}
				}

				result := make([]interface{}, len(listVal))
				for i, item := range listVal {
					// For simple function calls like type(r), handle the item directly
					// instead of string replacement which breaks for map types
					if matchFuncStartAndSuffix(transform, "type") {
						// Extract type from relationship map
						if mapItem, ok := item.(map[string]interface{}); ok {
							if relType, ok := mapItem["type"]; ok {
								result[i] = relType
								continue
							}
						}
						// Fallback: try to get type from storage.Edge
						if edge, ok := item.(*storage.Edge); ok {
							result[i] = edge.Type
							continue
						}
					}

					if matchFuncStartAndSuffix(transform, "id") {
						// Extract id from relationship map
						if mapItem, ok := item.(map[string]interface{}); ok {
							if id, ok := mapItem["_edgeId"]; ok {
								result[i] = id
								continue
							}
						}
					}

					// Fallback to string replacement (may not work for complex types)
					transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
					result[i] = e.evaluateExpressionWithContextFull(transformWithVal, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				}
				return result
			}
		}
	}

	// [x IN list] - simple list comprehension (identity)
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		upperInner := strings.ToUpper(inner)
		inIdx := strings.Index(upperInner, " IN ")
		// Only if no WHERE or | (those are handled above)
		if inIdx > 0 && !strings.Contains(upperInner, " WHERE ") && !strings.Contains(inner, " | ") {
			listExpr := strings.TrimSpace(inner[inIdx+4:])
			list := e.evaluateExpressionWithContext(listExpr, nodes, rels)

			switch v := list.(type) {
			case []interface{}:
				return v
			case []string:
				result := make([]interface{}, len(v))
				for i, s := range v {
					result[i] = s
				}
				return result
			default:
				return []interface{}{list}
			}
		}
	}

	// ========================================
	// CASE WHEN Expressions (must be before operators)
	// ========================================
	if strings.HasPrefix(lowerExpr, "case") && strings.HasSuffix(lowerExpr, "end") {
		return e.evaluateCaseExpression(expr, nodes, rels)
	}

	// ========================================
	// Boolean/Comparison Operators (must be before property access)
	// ========================================

	// NOT expr
	if strings.HasPrefix(lowerExpr, "not ") {
		inner := strings.TrimSpace(expr[4:])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		if b, ok := result.(bool); ok {
			return !b
		}
		return nil
	}

	// BETWEEN must be checked before AND (because BETWEEN x AND y uses AND)
	if e.hasStringPredicate(expr, " BETWEEN ") {
		betweenParts := e.splitByOperator(expr, " BETWEEN ")
		if len(betweenParts) == 2 {
			value := e.evaluateExpressionWithContext(betweenParts[0], nodes, rels)
			// For BETWEEN, we need to split by AND but only in the range part
			rangeParts := strings.SplitN(strings.ToUpper(betweenParts[1]), " AND ", 2)
			if len(rangeParts) == 2 {
				// Get the actual case-preserved parts
				andIdx := strings.Index(strings.ToUpper(betweenParts[1]), " AND ")
				minPart := strings.TrimSpace(betweenParts[1][:andIdx])
				maxPart := strings.TrimSpace(betweenParts[1][andIdx+5:])
				minVal := e.evaluateExpressionWithContext(minPart, nodes, rels)
				maxVal := e.evaluateExpressionWithContext(maxPart, nodes, rels)
				return (e.compareGreater(value, minVal) || e.compareEqual(value, minVal)) &&
					(e.compareLess(value, maxVal) || e.compareEqual(value, maxVal))
			}
		}
	}

	// AND operator
	if e.hasLogicalOperator(expr, " AND ") {
		return e.evaluateLogicalAnd(expr, nodes, rels)
	}

	// OR operator
	if e.hasLogicalOperator(expr, " OR ") {
		return e.evaluateLogicalOr(expr, nodes, rels)
	}

	// XOR operator
	if e.hasLogicalOperator(expr, " XOR ") {
		return e.evaluateLogicalXor(expr, nodes, rels)
	}

	// Comparison operators (=, <>, <, >, <=, >=)
	if e.hasComparisonOperator(expr) {
		return e.evaluateComparisonExpr(expr, nodes, rels)
	}

	// Arithmetic operators (*, /, %, -, +)
	// NOTE: Arithmetic is checked BEFORE string concatenation to support date/duration arithmetic
	if e.hasArithmeticOperator(expr) {
		result := e.evaluateArithmeticExpr(expr, nodes, rels)
		if result != nil {
			return result
		}
		// If arithmetic returned nil, fall through to string concatenation for + operator
	}

	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// This is a fallback when arithmetic didn't apply (e.g., string + string)
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(expr, nodes, rels)
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := result.(type) {
		case int64:
			return -v
		case float64:
			return -v
		case int:
			return -v
		}
	}

	// ========================================
	// Null Predicates (IS NULL, IS NOT NULL)
	// ========================================
	if strings.HasSuffix(lowerExpr, " is null") {
		inner := strings.TrimSpace(expr[:len(expr)-8])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		return result == nil
	}
	if strings.HasSuffix(lowerExpr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		return result != nil
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	if e.hasStringPredicate(expr, " STARTS WITH ") {
		parts := e.splitByOperator(expr, " STARTS WITH ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.HasPrefix(leftStr, rightStr)
			}
			return false
		}
	}
	if e.hasStringPredicate(expr, " ENDS WITH ") {
		parts := e.splitByOperator(expr, " ENDS WITH ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.HasSuffix(leftStr, rightStr)
			}
			return false
		}
	}
	if e.hasStringPredicate(expr, " CONTAINS ") {
		parts := e.splitByOperator(expr, " CONTAINS ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.Contains(leftStr, rightStr)
			}
			return false
		}
	}

	// ========================================
	// IN Operator (value IN list)
	// ========================================
	// NOT IN must be checked before IN (because "NOT IN" contains " IN ")
	if e.hasStringPredicate(expr, " NOT IN ") {
		parts := e.splitByOperator(expr, " NOT IN ")
		if len(parts) == 2 {
			value := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(parts[1], nodes, rels)

			// Convert to []interface{} if needed
			var list []interface{}
			switch v := listVal.(type) {
			case []interface{}:
				list = v
			case []string:
				list = make([]interface{}, len(v))
				for i, s := range v {
					list[i] = s
				}
			case []int64:
				list = make([]interface{}, len(v))
				for i, n := range v {
					list[i] = n
				}
			default:
				// Not a list, return true (value is not in non-list)
				return true
			}

			// Check if value is NOT in the list
			for _, item := range list {
				if e.compareEqual(value, item) {
					return false
				}
			}
			return true
		}
	}
	if e.hasStringPredicate(expr, " IN ") {
		parts := e.splitByOperator(expr, " IN ")
		if len(parts) == 2 {
			value := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(parts[1], nodes, rels)

			// Convert to []interface{} if needed
			var list []interface{}
			switch v := listVal.(type) {
			case []interface{}:
				list = v
			case []string:
				list = make([]interface{}, len(v))
				for i, s := range v {
					list[i] = s
				}
			case []int64:
				list = make([]interface{}, len(v))
				for i, n := range v {
					list[i] = n
				}
			default:
				// Not a list, return false
				return false
			}

			// Check if value is in the list
			for _, item := range list {
				if e.compareEqual(value, item) {
					return true
				}
			}
			return false
		}
	}

	// ========================================
	// Property Access: n.property
	// ========================================
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node, ok := nodes[varName]; ok {
			// Handle has_embedding specially - check both property and native embedding field
			if propName == "has_embedding" {
				if val, ok := node.Properties["has_embedding"]; ok {
					return val
				}
				return len(node.Embedding) > 0
			}
			// Don't return internal properties like embeddings (except has_embedding handled above)
			if e.isInternalProperty(propName) {
				return nil
			}
			if val, ok := node.Properties[propName]; ok {
				return val
			}
			return nil
		}
		if rel, ok := rels[varName]; ok {
			if val, ok := rel.Properties[propName]; ok {
				return val
			}
			return nil
		}
	}

	// ========================================
	// Variable Reference - return whole node/rel/path
	// ========================================
	if node, ok := nodes[expr]; ok {
		// Check if this is a scalar wrapper (pseudo-node created for YIELD variables)
		// If it only has a "value" property, return that value directly
		if len(node.Properties) == 1 {
			if val, hasValue := node.Properties["value"]; hasValue {
				return val
			}
		}
		return e.nodeToMap(node)
	}
	if rel, ok := rels[expr]; ok {
		return map[string]interface{}{
			"_edgeId":    string(rel.ID),
			"type":       rel.Type,
			"properties": rel.Properties,
		}
	}
	// Check if this is a path variable - return the PathResult as a map
	// This allows path functions like length(path), relationships(path) to work after WITH
	if paths != nil {
		if pathResult, ok := paths[expr]; ok && pathResult != nil {
			// Return path as a serializable structure that can be used later
			return map[string]interface{}{
				"_pathResult": pathResult, // Keep the PathResult for later use
				"length":      pathResult.Length,
				"nodes":       pathResult.Nodes,
				"rels":        pathResult.Relationships,
			}
		}
	}

	// ========================================
	// Literals
	// ========================================

	// null
	if lowerExpr == "null" {
		return nil
	}

	// Boolean
	if lowerExpr == "true" {
		return true
	}
	if lowerExpr == "false" {
		return false
	}

	// String literal (single or double quotes)
	if len(expr) >= 2 {
		if expr[0] == '\'' && expr[len(expr)-1] == '\'' {
			// Unescape doubled single quotes
			inner := expr[1 : len(expr)-1]
			inner = strings.ReplaceAll(inner, "''", "'")
			inner = strings.ReplaceAll(inner, "\\\\", "\\")
			return inner
		}
		if expr[0] == '"' && expr[len(expr)-1] == '"' {
			// Unescape doubled double quotes
			inner := expr[1 : len(expr)-1]
			inner = strings.ReplaceAll(inner, "\"\"", "\"")
			inner = strings.ReplaceAll(inner, "\\\\", "\\")
			return inner
		}
	}

	// Number literal
	if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return num
	}
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num
	}

	// Array literal [a, b, c]
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return e.parseArrayValue(expr)
	}

	// Map literal {key: value}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.parseProperties(expr)
	}

	// Check if this looks like a variable reference (identifier pattern)
	// If it's a valid identifier and not found in nodes/rels, it should be null
	// This handles cases like OPTIONAL MATCH where the variable might not exist
	isValidIdentifier := true
	for i, ch := range expr {
		if i == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
				isValidIdentifier = false
				break
			}
		} else {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
				isValidIdentifier = false
				break
			}
		}
	}
	if isValidIdentifier && len(expr) > 0 {
		// This looks like an unresolved variable reference - return null
		return nil
	}

	// Check if this is an aggregation function - they should not be evaluated in expression context
	exprLower := strings.ToLower(expr)
	if strings.HasPrefix(exprLower, "count(") ||
		strings.HasPrefix(exprLower, "sum(") ||
		strings.HasPrefix(exprLower, "avg(") ||
		strings.HasPrefix(exprLower, "min(") ||
		strings.HasPrefix(exprLower, "max(") ||
		strings.HasPrefix(exprLower, "collect(") {
		// Aggregation functions must be handled by aggregation logic, not per-row evaluation
		return nil
	}

	// Unknown - return as string (for string literals without quotes, etc.)
	return expr
}

// evaluateStringConcatWithContext handles string concatenation with + operator.
func (e *StorageExecutor) evaluateStringConcatWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) string {
	var result strings.Builder

	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)

	for _, part := range parts {
		val := e.evaluateExpressionWithContext(part, nodes, rels)
		result.WriteString(fmt.Sprintf("%v", val))
	}

	return result.String()
}

// tryCallPluginFunction attempts to call a function from the plugin system.
// Returns the result and true if the function was handled, or nil and false if not found.
// This is GENERIC - works for any plugin function (not specific to any plugin).
func (e *StorageExecutor) tryCallPluginFunction(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, bool) {
	// Plugin lookup must be configured
	if PluginFunctionLookup == nil {
		return nil, false
	}

	// Extract function name from expression (e.g., "myplugin.func([1,2,3])" -> "myplugin.func")
	lowerExpr := strings.ToLower(expr)
	parenIdx := strings.Index(lowerExpr, "(")
	if parenIdx == -1 {
		return nil, false
	}
	funcName := lowerExpr[:parenIdx]

	// Look up in plugin registry
	handler, found := PluginFunctionLookup(funcName)
	if !found {
		return nil, false
	}

	// Parse arguments
	argsStr := strings.TrimSpace(expr[parenIdx+1 : len(expr)-1])
	args := e.splitFunctionArgs(argsStr)

	// Evaluate each argument
	var evalArgs []interface{}
	for _, arg := range args {
		evalArgs = append(evalArgs, e.evaluateExpressionWithContext(arg, nodes, rels))
	}

	// Call the plugin function
	result, err := callPluginHandler(handler, evalArgs)
	if err != nil {
		// Log error but don't fail - fall back to built-in if available
		return nil, false
	}

	return result, true
}

// callPluginHandler invokes a plugin function handler with the given arguments.
func callPluginHandler(handler interface{}, args []interface{}) (interface{}, error) {
	if handler == nil {
		return nil, fmt.Errorf("nil handler")
	}

	// Type-based dispatch for common function signatures
	switch fn := handler.(type) {
	// No args
	case func() interface{}:
		return fn(), nil
	case func() string:
		return fn(), nil
	case func() int64:
		return fn(), nil
	case func() float64:
		return fn(), nil

	// Single arg functions
	case func(interface{}) interface{}:
		if len(args) >= 1 {
			return fn(args[0]), nil
		}
	case func([]interface{}) interface{}:
		if len(args) >= 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func([]interface{}) float64:
		if len(args) >= 1 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list), nil
			}
		}
	case func(string) string:
		if len(args) >= 1 {
			if s, ok := args[0].(string); ok {
				return fn(s), nil
			}
		}
	case func(float64) float64:
		if len(args) >= 1 {
			if f, ok := toFloat64(args[0]); ok {
				return fn(f), nil
			}
		}
	case func([]float64) []float64:
		if len(args) >= 1 {
			if list, ok := toFloat64Slice(args[0]); ok {
				return fn(list), nil
			}
		}

	// Two arg functions
	case func(interface{}, interface{}) interface{}:
		if len(args) >= 2 {
			return fn(args[0], args[1]), nil
		}
	case func([]interface{}, interface{}) bool:
		if len(args) >= 2 {
			if list, ok := args[0].([]interface{}); ok {
				return fn(list, args[1]), nil
			}
		}
	case func([]interface{}, []interface{}) []interface{}:
		if len(args) >= 2 {
			list1, ok1 := args[0].([]interface{})
			list2, ok2 := args[1].([]interface{})
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}
	case func(string, string) string:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) int:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func(string, string) float64:
		if len(args) >= 2 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			if ok1 && ok2 {
				return fn(s1, s2), nil
			}
		}
	case func([]float64, []float64) float64:
		if len(args) >= 2 {
			list1, ok1 := toFloat64Slice(args[0])
			list2, ok2 := toFloat64Slice(args[1])
			if ok1 && ok2 {
				return fn(list1, list2), nil
			}
		}

	// Three arg functions
	case func(string, string, string) string:
		if len(args) >= 3 {
			s1, ok1 := args[0].(string)
			s2, ok2 := args[1].(string)
			s3, ok3 := args[2].(string)
			if ok1 && ok2 && ok3 {
				return fn(s1, s2, s3), nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported function signature")
}

// NOTE: Logical, Comparison, and Arithmetic operators moved to operators.go

// evaluateMapLiteral parses and evaluates a map literal like { key: value, ... }
// where values can be expressions that reference variables in context.
func (e *StorageExecutor) evaluateMapLiteral(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) map[string]interface{} {
	return e.evaluateMapLiteralFull(expr, nodes, rels, nil, nil, nil, 0)
}

// evaluateMapLiteralFull parses and evaluates a map literal with full path context
func (e *StorageExecutor) evaluateMapLiteralFull(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) map[string]interface{} {
	result := make(map[string]interface{})

	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "{") || !strings.HasSuffix(expr, "}") {
		return result
	}

	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	if inner == "" {
		return result
	}

	// Split by commas, respecting nesting
	pairs := e.splitMapPairsRespectingNesting(inner)

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Find the first colon (key: value)
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])

		// Evaluate the value expression in context with full path info
		value := e.evaluateExpressionWithContextFull(valueExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		result[key] = value
	}

	return result
}

// splitMapPairsRespectingNesting splits map literal pairs by commas, respecting brackets and quotes.
func (e *StorageExecutor) splitMapPairsRespectingNesting(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range s {
		if inQuote {
			current.WriteRune(c)
			if c == quoteChar {
				inQuote = false
			}
			continue
		}

		switch c {
		case '"', '\'':
			inQuote = true
			quoteChar = c
			current.WriteRune(c)
		case '{', '[', '(':
			depth++
			current.WriteRune(c)
		case '}', ']', ')':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}
