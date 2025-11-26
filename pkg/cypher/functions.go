// Cypher function implementations for NornicDB.
// This file contains evaluateExpressionWithContext and all Cypher functions.

package cypher

import (
	"crypto/rand"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateExpression evaluates an expression for a single node context.
func (e *StorageExecutor) evaluateExpression(expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(expr, map[string]*storage.Node{varName: node}, nil)
}
func (e *StorageExecutor) evaluateExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
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
	if strings.HasPrefix(lowerExpr, "id(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[3 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return string(node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return string(rel.ID)
		}
		return nil
	}

	// elementId(n) - same as id() for compatibility
	if strings.HasPrefix(lowerExpr, "elementid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return fmt.Sprintf("4:nornicdb:%s", node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return fmt.Sprintf("5:nornicdb:%s", rel.ID)
		}
		return nil
	}

	// labels(n) - return list of labels for a node
	if strings.HasPrefix(lowerExpr, "labels(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			// Return labels as a list of strings
			result := make([]interface{}, len(node.Labels))
			for i, label := range node.Labels {
				result[i] = label
			}
			return result
		}
		return nil
	}

	// type(r) - return relationship type
	if strings.HasPrefix(lowerExpr, "type(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			return rel.Type
		}
		return nil
	}

	// keys(n) - return list of property keys
	if strings.HasPrefix(lowerExpr, "keys(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "properties(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
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

	// count(*) or count(n) - simplified aggregation (returns 1 for single row context)
	if strings.HasPrefix(lowerExpr, "count(") && strings.HasSuffix(expr, ")") {
		return int64(1)
	}

	// size(list) or size(string) - return length
	if strings.HasPrefix(lowerExpr, "size(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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

	// length(path) - same as size for compatibility
	if strings.HasPrefix(lowerExpr, "length(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		}
		return int64(0)
	}

	// exists(n.prop) - check if property exists
	if strings.HasPrefix(lowerExpr, "exists(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "coalesce(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "head(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[0]
		}
		return nil
	}

	// last(list) - return last element
	if strings.HasPrefix(lowerExpr, "last(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[len(list)-1]
		}
		return nil
	}

	// tail(list) - return list without first element
	if strings.HasPrefix(lowerExpr, "tail(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}

	// reverse(list) - return reversed list
	if strings.HasPrefix(lowerExpr, "reverse(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "range(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "slice(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "indexof(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "degree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			inDegree := e.storage.GetInDegree(node.ID)
			outDegree := e.storage.GetOutDegree(node.ID)
			return int64(inDegree + outDegree)
		}
		return int64(0)
	}

	// inDegree(node) - incoming edges count
	if strings.HasPrefix(lowerExpr, "indegree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetInDegree(node.ID))
		}
		return int64(0)
	}

	// outDegree(node) - outgoing edges count
	if strings.HasPrefix(lowerExpr, "outdegree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetOutDegree(node.ID))
		}
		return int64(0)
	}

	// hasLabels(node, labels) - check if node has all specified labels
	if strings.HasPrefix(lowerExpr, "haslabels(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "apoc.map.frompairs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "apoc.map.merge(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "apoc.map.removekey(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "apoc.map.setkey(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "apoc.map.clean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tostring(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fmt.Sprintf("%v", val)
	}

	// toInteger(value) / toInt(value)
	if (strings.HasPrefix(lowerExpr, "tointeger(") || strings.HasPrefix(lowerExpr, "toint(")) && strings.HasSuffix(expr, ")") {
		startIdx := 10
		if strings.HasPrefix(lowerExpr, "toint(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tofloat(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "toboolean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return strings.ToLower(v) == "true"
		}
		return nil
	}

	// ========================================
	// OrNull Variants (return null instead of error)
	// ========================================

	// toIntegerOrNull(value)
	if strings.HasPrefix(lowerExpr, "tointegerornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tofloatornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tobooleanornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tostringornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tointegerlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tofloatlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[12 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tobooleanlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "tostringlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[13 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "valuetype(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "sum(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// avg(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "avg(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// min(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "min(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// max(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "max(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// collect(expr) - in single row context, returns single-element list
	if strings.HasPrefix(lowerExpr, "collect(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if val == nil {
			return []interface{}{}
		}
		return []interface{}{val}
	}

	// toLower(string) / lower(string)
	if (strings.HasPrefix(lowerExpr, "tolower(") || strings.HasPrefix(lowerExpr, "lower(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "lower(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
		return nil
	}

	// toUpper(string) / upper(string)
	if (strings.HasPrefix(lowerExpr, "toupper(") || strings.HasPrefix(lowerExpr, "upper(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "upper(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
		return nil
	}

	// trim(string) / ltrim(string) / rtrim(string)
	if strings.HasPrefix(lowerExpr, "trim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimSpace(str)
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "ltrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimLeft(str, " \t\n\r")
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "rtrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimRight(str, " \t\n\r")
		}
		return nil
	}

	// replace(string, search, replacement)
	if strings.HasPrefix(lowerExpr, "replace(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "split(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "substring(") && strings.HasSuffix(expr, ")") {
		return e.evaluateSubstring(expr)
	}

	// left(string, n) - return first n characters
	if strings.HasPrefix(lowerExpr, "left(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "right(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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

	// ========================================
	// Date/Time Functions
	// ========================================

	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return e.idCounter() // Use counter as pseudo-timestamp for consistency
	}

	// datetime() - current datetime as string
	if lowerExpr == "datetime()" {
		return fmt.Sprintf("%d", e.idCounter())
	}

	// date() - current date
	if lowerExpr == "date()" {
		return fmt.Sprintf("%d", e.idCounter())
	}

	// time() - current time
	if lowerExpr == "time()" {
		return fmt.Sprintf("%d", e.idCounter())
	}

	// ========================================
	// Math Functions
	// ========================================

	// abs(number)
	if strings.HasPrefix(lowerExpr, "abs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "ceil(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}

	// floor(number)
	if strings.HasPrefix(lowerExpr, "floor(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}

	// round(number)
	if strings.HasPrefix(lowerExpr, "round(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}

	// sign(number)
	if strings.HasPrefix(lowerExpr, "sign(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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
	// Trigonometric Functions
	// ========================================

	// sin(x) - sine of x (radians)
	if strings.HasPrefix(lowerExpr, "sin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sin(f)
		}
		return nil
	}

	// cos(x) - cosine of x (radians)
	if strings.HasPrefix(lowerExpr, "cos(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cos(f)
		}
		return nil
	}

	// tan(x) - tangent of x (radians)
	if strings.HasPrefix(lowerExpr, "tan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tan(f)
		}
		return nil
	}

	// cot(x) - cotangent of x (radians)
	if strings.HasPrefix(lowerExpr, "cot(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return 1.0 / math.Tan(f)
		}
		return nil
	}

	// asin(x) - arc sine
	if strings.HasPrefix(lowerExpr, "asin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Asin(f)
		}
		return nil
	}

	// acos(x) - arc cosine
	if strings.HasPrefix(lowerExpr, "acos(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Acos(f)
		}
		return nil
	}

	// atan(x) - arc tangent
	if strings.HasPrefix(lowerExpr, "atan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Atan(f)
		}
		return nil
	}

	// atan2(y, x) - arc tangent of y/x
	if strings.HasPrefix(lowerExpr, "atan2(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "exp(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Exp(f)
		}
		return nil
	}

	// log(x) - natural logarithm
	if strings.HasPrefix(lowerExpr, "log(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log(f)
		}
		return nil
	}

	// log10(x) - base-10 logarithm
	if strings.HasPrefix(lowerExpr, "log10(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log10(f)
		}
		return nil
	}

	// sqrt(x) - square root
	if strings.HasPrefix(lowerExpr, "sqrt(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "radians(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * math.Pi / 180.0
		}
		return nil
	}

	// degrees(radians) - convert radians to degrees
	if strings.HasPrefix(lowerExpr, "degrees(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * 180.0 / math.Pi
		}
		return nil
	}

	// haversin(x) - half of versine = (1 - cos(x))/2
	if strings.HasPrefix(lowerExpr, "haversin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return (1 - math.Cos(f)) / 2
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
	if strings.HasPrefix(lowerExpr, "startnode(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.StartNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// endNode(r) - return end node of relationship
	if strings.HasPrefix(lowerExpr, "endnode(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.EndNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// nodes(path) - return list of nodes in a path
	if strings.HasPrefix(lowerExpr, "nodes(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// For now, return nodes from node context
		if node, ok := nodes[inner]; ok {
			return []interface{}{e.nodeToMap(node)}
		}
		return []interface{}{}
	}

	// relationships(path) - return list of relationships in a path
	if strings.HasPrefix(lowerExpr, "relationships(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		// For now, return rels from rel context
		if rel, ok := rels[inner]; ok {
			return []interface{}{map[string]interface{}{
				"id":         string(rel.ID),
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
	if strings.HasPrefix(lowerExpr, "isempty(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "isnan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.IsNaN(f)
		}
		return false
	}

	// nullIf(val1, val2) - return null if val1 = val2
	if strings.HasPrefix(lowerExpr, "nullif(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "btrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
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

	// char_length(string) / character_length(string)
	if (strings.HasPrefix(lowerExpr, "char_length(") || strings.HasPrefix(lowerExpr, "character_length(")) && strings.HasSuffix(expr, ")") {
		startIdx := 12
		if strings.HasPrefix(lowerExpr, "character_length(") {
			startIdx = 17
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// normalize(string) - Unicode normalization
	if strings.HasPrefix(lowerExpr, "normalize(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "percentilecont(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// percentileDisc(expr, percentile) - discrete percentile
	if strings.HasPrefix(lowerExpr, "percentiledisc(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// stDev(expr) - standard deviation
	if strings.HasPrefix(lowerExpr, "stdev(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// stDevP(expr) - population standard deviation
	if strings.HasPrefix(lowerExpr, "stdevp(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// ========================================
	// Reduce Function
	// ========================================

	// reduce(acc = initial, x IN list | expr) - reduce a list
	if strings.HasPrefix(lowerExpr, "reduce(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])

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
	// Vector Functions
	// ========================================

	// vector.similarity.cosine(v1, v2)
	if strings.HasPrefix(lowerExpr, "vector.similarity.cosine(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[25 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return cosineSimilarity(vec1, vec2)
			}
		}
		return nil
	}

	// vector.similarity.euclidean(v1, v2)
	if strings.HasPrefix(lowerExpr, "vector.similarity.euclidean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[28 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return euclideanSimilarity(vec1, vec2)
			}
		}
		return nil
	}

	// ========================================
	// Point/Spatial Functions (basic support)
	// ========================================

	// point({x: val, y: val}) or point({latitude: val, longitude: val})
	if strings.HasPrefix(lowerExpr, "point(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// Return the point as a map
		if strings.HasPrefix(inner, "{") && strings.HasSuffix(inner, "}") {
			props := e.parseProperties(inner)
			return props
		}
		return nil
	}

	// distance(p1, p2) - Euclidean distance between two points
	if strings.HasPrefix(lowerExpr, "distance(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "withinbbox(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
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

	// ========================================
	// List Predicate Functions
	// ========================================

	// all(variable IN list WHERE predicate) - check if all elements match
	if strings.HasPrefix(lowerExpr, "all(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "any(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "none(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "single(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "filter(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
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
	if strings.HasPrefix(lowerExpr, "extract(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
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

	// [x IN list | expression] - list comprehension
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

				list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
				listVal, ok := list.([]interface{})
				if !ok {
					return []interface{}{}
				}

				result := make([]interface{}, len(listVal))
				for i, item := range listVal {
					transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
					result[i] = e.evaluateExpressionWithContext(transformWithVal, nodes, rels)
				}
				return result
			}
		}
	}

	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// to avoid infinite recursion when property values contain " + "
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(expr, nodes, rels)
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

	// Arithmetic operators (*, /, %, -)
	// Note: + is handled separately for string concatenation
	if e.hasArithmeticOperator(expr) {
		return e.evaluateArithmeticExpr(expr, nodes, rels)
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
	if e.hasStringPredicate(expr, " IN ") {
		parts := e.splitByOperator(expr, " IN ")
		if len(parts) == 2 {
			value := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			if list, ok := listVal.([]interface{}); ok {
				for _, item := range list {
					if e.compareEqual(value, item) {
						return true
					}
				}
				return false
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
			// Don't return internal properties like embeddings
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
	// Variable Reference - return whole node/rel
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
			"id":         string(rel.ID),
			"type":       rel.Type,
			"properties": rel.Properties,
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
		if (expr[0] == '\'' && expr[len(expr)-1] == '\'') ||
			(expr[0] == '"' && expr[len(expr)-1] == '"') {
			return expr[1 : len(expr)-1]
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

// ========================================
// Logical Operators
// ========================================

// hasLogicalOperator checks if the expression has a logical operator outside of quotes/parentheses
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

// evaluateLogicalAnd evaluates expr1 AND expr2
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

// evaluateLogicalOr evaluates expr1 OR expr2
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

// evaluateLogicalXor evaluates expr1 XOR expr2
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

// hasComparisonOperator checks if the expression has a comparison operator
func (e *StorageExecutor) hasComparisonOperator(expr string) bool {
	ops := []string{"<>", "<=", ">=", "=~", "!=", "=", "<", ">"}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// hasOperatorOutsideQuotes checks if operator exists outside quotes and parentheses
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

// evaluateComparisonExpr evaluates comparison expressions
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

// hasArithmeticOperator checks if the expression has arithmetic operators (*, /, %, -)
func (e *StorageExecutor) hasArithmeticOperator(expr string) bool {
	// Note: + is handled by string concatenation
	ops := []string{"*", "/", "%", " - "}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// evaluateArithmeticExpr evaluates arithmetic expressions
func (e *StorageExecutor) evaluateArithmeticExpr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
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

// splitByOperator splits expression by operator respecting quotes and parentheses
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

// Arithmetic helper functions
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

func (e *StorageExecutor) divide(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return l / r
}

func (e *StorageExecutor) modulo(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return int64(l) % int64(r)
}

func (e *StorageExecutor) subtract(left, right interface{}) interface{} {
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

// hasStringPredicate checks if expression has a string predicate (case-insensitive)
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
