package cypher

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullFunctions(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
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
			return e.evaluateExpressionWithContextFull(ctx, expr[1:len(expr)-1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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

			// Avoid misclassifying list literals in larger expressions as indexing.
			// Example: "file_tags + ['hello']" should parse as list concatenation,
			// not "(file_tags +)['hello']".
			lastNonSpace := byte(0)
			for i := len(baseExpr) - 1; i >= 0; i-- {
				if baseExpr[i] != ' ' && baseExpr[i] != '\t' && baseExpr[i] != '\n' && baseExpr[i] != '\r' {
					lastNonSpace = baseExpr[i]
					break
				}
			}
			switch lastNonSpace {
			case '+', '-', '*', '/', '%', '(', ',', '=', '<', '>', '|':
				goto skipArrayIndexing
			}

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
				baseVal := e.evaluateExpressionWithContextFull(ctx, baseExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			baseVal := e.evaluateExpressionWithContextFull(ctx, baseExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if baseVal == nil {
				return nil
			}

			// Evaluate the index
			index := e.evaluateExpressionWithContextFull(ctx, indexExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if key, ok := index.(string); ok {
				switch value := baseVal.(type) {
				case map[string]interface{}:
					return value[key]
				case *storage.Node:
					if value != nil {
						return value.Properties[key]
					}
					return nil
				case *storage.Edge:
					if value != nil {
						return value.Properties[key]
					}
					return nil
				}
			}
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
				if character, ok := cyphertext.At(list, int(idx)); ok {
					return character
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
		return e.evaluateMapLiteralFull(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// ========================================
	// CASE Expressions (must be checked first)
	// ========================================
	if isCaseExpression(expr) {
		// The path bindings reach the CASE's WHEN / THEN / ELSE expressions
		// (CASE WHEN … THEN length(p) … END in a path WHERE, #699).
		return e.evaluateCaseExpression(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	lowerExpr := strings.ToLower(expr)

	// ========================================
	// Registered Function Dispatch (Phase B: pkg/cypher/fn)
	// ========================================
	if name, inner, ok := parseFunctionCallWS(expr); ok {
		args := e.splitFunctionArgs(inner)
		fnCtx := cypherfn.Context{
			Nodes:    nodes,
			Rels:     rels,
			Database: e.databaseName(),
			Eval: func(argExpr string) (interface{}, error) {
				return e.evaluateExpressionWithContextFull(ctx, argExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength), nil
			},
			Now: time.Now,
		}

		if v, found, err := cypherfn.EvaluateFunction(name, args, fnCtx); found {
			if err != nil {
				functionEvaluationFailure(ctx, err)
				return nil
			}
			return v
		}
	}

	// ========================================
	// Cypher Functions (Neo4j compatible)
	// ========================================
	// Core scalar functions like id(), labels(), type(), keys(), properties(),
	// size(), coalesce(), toLower(), and toUpper() are dispatched above via
	// pkg/cypher/fn. Keep only evaluator-specific behavior here to avoid
	// duplicate implementations drifting apart.

	// Note: count(), sum(), avg(), etc. are aggregation functions and should NOT be
	// evaluated here. They must be handled by executeAggregation() in match.go or
	// executeMatchWithRelationships() in traversal.go. If we reach here with count(),
	// it means the query wasn't properly detected as an aggregation query - that's a bug
	// in the query router, not something we should handle here.

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

		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		switch v := innerVal.(type) {
		case string:
			return int64(cyphertext.Length(v))
		case []interface{}:
			return int64(len(v))
		case map[string]interface{}:
			// A path carried as a computed-row map (WITH p AS path): read the
			// recorded length, then the node list as a fallback.
			if length, ok := v["length"]; ok {
				switch distance := length.(type) {
				case int:
					return int64(distance)
				case int64:
					return distance
				case float64:
					return int64(distance)
				}
			}
			if nodesList, _, hasNodes, _ := pathValueParts(v); hasNodes {
				return int64(len(nodesList))
			}
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

	// head(list) - return first element
	if matchFuncStartAndSuffix(expr, "head") {
		inner := extractFuncArgs(expr, "head")
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[0]
		}
		return nil
	}

	// last(list) - return last element
	if matchFuncStartAndSuffix(expr, "last") {
		inner := extractFuncArgs(expr, "last")
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[len(list)-1]
		}
		return nil
	}

	// tail(list) - return list without first element
	if matchFuncStartAndSuffix(expr, "tail") {
		inner := extractFuncArgs(expr, "tail")
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}

	// reverse(list) - return reversed list
	if matchFuncStartAndSuffix(expr, "reverse") {
		inner := extractFuncArgs(expr, "reverse")
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		arguments := make([]interface{}, len(args))
		for index, argument := range args {
			arguments[index] = e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(argument), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		result, err := evaluateCypherRange(arguments)
		if err != nil {
			return nil
		}
		return result
	}

	// slice(list, start, end) - get sublist from start to end (exclusive)
	if matchFuncStartAndSuffix(expr, "slice") {
		inner := extractFuncArgs(expr, "slice")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			listVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			searchVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
				labelsVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		pairsVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			map1 := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			map2 := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			mapVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			keyVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			mapVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			keyVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[2]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			mapVal := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			var keysToRemove []string
			var valuesToRemove []interface{}

			if len(args) >= 2 {
				if keys, ok := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).([]interface{}); ok {
					for _, k := range keys {
						if ks, ok := k.(string); ok {
							keysToRemove = append(keysToRemove, ks)
						}
					}
				}
			}
			if len(args) >= 3 {
				if vals, ok := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[2]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength).([]interface{}); ok {
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
						if (v == nil && vr == nil) || e.compareEqual(v, vr) {
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return fmt.Sprintf("%v", val)
	}

	// toInteger(value)
	if matchFuncStartAndSuffix(expr, "tointeger") {
		inner := extractFuncArgs(expr, "tointeger")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if val == nil {
			return nil
		}
		return fmt.Sprintf("%v", val)
	}

	// ========================================
	// List Conversion Functions
	// ========================================

	// ========================================
	// Additional Utility Functions
	// ========================================

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// sum(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "sum") {
		inner := extractFuncArgs(expr, "sum")
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// avg(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "avg") {
		inner := extractFuncArgs(expr, "avg")
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// min(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "min") {
		inner := extractFuncArgs(expr, "min")
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// max(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "max") {
		inner := extractFuncArgs(expr, "max")
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// collect(expr) - in single row context, returns single-element list
	if matchFuncStartAndSuffix(expr, "collect") {
		inner := extractFuncArgs(expr, "collect")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if val == nil {
			return []interface{}{}
		}
		return []interface{}{val}
	}

	// replace(string, search, replacement)
	if matchFuncStartAndSuffix(expr, "replace") {
		inner := extractFuncArgs(expr, "replace")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 3 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			search := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			repl := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[2]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return strings.ReplaceAll(str, search, repl)
		}
		return nil
	}

	// split(string, delimiter)
	if matchFuncStartAndSuffix(expr, "split") {
		inner := extractFuncArgs(expr, "split")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			delim := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
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
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			start, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if len(args) >= 3 {
				length, _ := strconv.Atoi(strings.TrimSpace(args[2]))
				return cyphertext.Substring(str, start, length)
			}
			return cyphertext.From(str, start)
		}
		return nil
	}

	// left(string, n) - return first n characters
	if matchFuncStartAndSuffix(expr, "left") {
		inner := extractFuncArgs(expr, "left")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			return cyphertext.Left(str, n)
		}
		return nil
	}

	// right(string, n) - return last n characters
	if matchFuncStartAndSuffix(expr, "right") {
		inner := extractFuncArgs(expr, "right")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			return cyphertext.Right(str, n)
		}
		return nil
	}

	// lpad(string, length, padString) - left-pad string to specified length
	if matchFuncStartAndSuffix(expr, "lpad") {
		inner := extractFuncArgs(expr, "lpad")
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[2]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
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
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[2]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
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
			template := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			// Remove quotes from template
			template = strings.Trim(template, "'\"")

			// Evaluate remaining arguments
			formatArgs := make([]interface{}, 0, len(args)-1)
			for i := 1; i < len(args); i++ {
				val := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[i]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
	if value, handled := e.evaluateTemporalConstructor(func(argument string) interface{} {
		return e.evaluateExpressionWithContextFull(ctx, argument, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}, expr); handled {
		if value == nil {
			if function, argument, ok := parseFunctionCallWS(expr); ok && isTemporalConstructor(function) && strings.TrimSpace(argument) != "" {
				input := e.evaluateExpressionWithContextFull(ctx, argument, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				// An argument the evaluator can't resolve comes back as its own
				// text; that is not a value to parse.
				if text, isText := input.(string); isText && text == strings.TrimSpace(argument) && !isWholeCypherQuotedString(text) {
					input = nil
				}
				if err := temporalConstructorError(function, input); err != nil {
					recordExpressionFailure(ctx, err)
				}
			}
		}
		return value
	}

	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return time.Now().UnixMilli()
	}

	// date.year(date), date.month(date), date.day(date) - extract components
	if matchFuncStartAndSuffix(expr, "date.year") {
		inner := extractFuncArgs(expr, "date.year")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			val := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			val := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			val := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if d, ok := val.(*CypherDuration); ok {
			return d.Years*12 + d.Months
		}
		return nil
	}

	// duration() - create duration from ISO 8601 string (P1Y2M3DT4H5M6S)
	// Returns a CypherDuration struct that can be used in arithmetic
	if isFunctionCall(expr, "duration") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			d1 := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[0]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			d2 := e.evaluateExpressionWithContextFull(ctx, strings.TrimSpace(args[1]), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return durationBetween(d1, d2)
		}
		return nil
	}

	// duration.inDays(duration) - convert duration to days
	if matchFuncStartAndSuffix(expr, "duration.indays") {
		inner := extractFuncArgs(expr, "duration.indays")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalDays()
		}
		return nil
	}

	// duration.inSeconds(duration) - convert duration to seconds
	if matchFuncStartAndSuffix(expr, "duration.inseconds") {
		inner := extractFuncArgs(expr, "duration.inseconds")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}

	// floor(number)
	if matchFuncStartAndSuffix(expr, "floor") {
		inner := extractFuncArgs(expr, "floor")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}

	// round(number)
	if matchFuncStartAndSuffix(expr, "round") {
		inner := extractFuncArgs(expr, "round")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}

	// sign(number)
	if matchFuncStartAndSuffix(expr, "sign") {
		inner := extractFuncArgs(expr, "sign")
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		return randomCypherFloat()
	}

	// ========================================

	// Plugin Functions (loaded dynamically from .so files)
	// ========================================

	// Try plugin functions for any namespaced function call (contains dots)
	// This is GENERIC - works for any plugin, not just a specific one
	if strings.Contains(lowerExpr, ".") && looksLikeFunctionCall(lowerExpr) {
		if result, handled := e.tryCallPluginFunction(ctx, expr, nodes, rels); handled {
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
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			sepVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return flattenList(listVal)
	}

	// apoc.coll.toSet(list) - Remove duplicates from list
	if isFunctionCall(expr, "apoc.coll.toset") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return toSet(listVal)
	}

	// apoc.coll.sum(list) - Sum numeric values in list
	if isFunctionCall(expr, "apoc.coll.sum") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollSum(listVal)
	}

	// apoc.coll.avg(list) - Average of numeric values in list
	if isFunctionCall(expr, "apoc.coll.avg") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollAvg(listVal)
	}

	// apoc.coll.min(list) - Minimum value in list
	if isFunctionCall(expr, "apoc.coll.min") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollMin(listVal)
	}

	// apoc.coll.max(list) - Maximum value in list
	if isFunctionCall(expr, "apoc.coll.max") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollMax(listVal)
	}

	// apoc.coll.sort(list) - Sort list in ascending order
	if isFunctionCall(expr, "apoc.coll.sort") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollSort(listVal)
	}

	// apoc.coll.sortNodes(nodes, property) - Sort nodes by property
	if isFunctionCall(expr, "apoc.coll.sortnodes") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			propName := strings.Trim(args[1], "'\"")
			return apocCollSortNodes(listVal, propName)
		}
		return nil
	}

	// apoc.coll.reverse(list) - Reverse a list
	if isFunctionCall(expr, "apoc.coll.reverse") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollReverse(listVal)
	}

	// apoc.coll.union(list1, list2) - Union of two lists (removes duplicates)
	if isFunctionCall(expr, "apoc.coll.union") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollUnion(list1, list2)
		}
		return nil
	}

	// apoc.coll.unionAll(list1, list2) - Union of two lists (keeps duplicates)
	if isFunctionCall(expr, "apoc.coll.unionall") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollUnionAll(list1, list2)
		}
		return nil
	}

	// apoc.coll.intersection(list1, list2) - Intersection of two lists
	if isFunctionCall(expr, "apoc.coll.intersection") {
		inner := strings.TrimSpace(expr[23 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollIntersection(list1, list2)
		}
		return nil
	}

	// apoc.coll.subtract(list1, list2) - Elements in list1 but not in list2
	if isFunctionCall(expr, "apoc.coll.subtract") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollSubtract(list1, list2)
		}
		return nil
	}

	// apoc.coll.contains(list, value) - Check if list contains value
	if isFunctionCall(expr, "apoc.coll.contains") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContains(listVal, value)
		}
		return false
	}

	// apoc.coll.containsAll(list1, list2) - Check if list1 contains all elements of list2
	if isFunctionCall(expr, "apoc.coll.containsall") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContainsAll(list1, list2)
		}
		return false
	}

	// apoc.coll.containsAny(list1, list2) - Check if list1 contains any element of list2
	if isFunctionCall(expr, "apoc.coll.containsany") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContainsAny(list1, list2)
		}
		return false
	}

	// apoc.coll.indexOf(list, value) - Find index of value in list (-1 if not found)
	if isFunctionCall(expr, "apoc.coll.indexof") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollIndexOf(listVal, value)
		}
		return int64(-1)
	}

	// apoc.coll.split(list, value) - Split list at occurrences of value
	if isFunctionCall(expr, "apoc.coll.split") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollSplit(listVal, value)
		}
		return nil
	}

	// apoc.coll.partition(list, size) - Partition list into sublists of given size
	if isFunctionCall(expr, "apoc.coll.partition") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			sizeVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollPartition(listVal, sizeVal)
		}
		return nil
	}

	// apoc.coll.pairs(list) - Create pairs from consecutive elements [[a,b], [b,c], ...]
	if isFunctionCall(expr, "apoc.coll.pairs") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollPairs(listVal)
	}

	// apoc.coll.zip(list1, list2) - Zip two lists into pairs [[a1,b1], [a2,b2], ...]
	if isFunctionCall(expr, "apoc.coll.zip") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollZip(list1, list2)
		}
		return nil
	}

	// apoc.coll.frequencies(list) - Count frequency of each element
	if isFunctionCall(expr, "apoc.coll.frequencies") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollFrequencies(listVal)
	}

	// apoc.coll.occurrences(list, value) - Count occurrences of value in list
	if isFunctionCall(expr, "apoc.coll.occurrences") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollOccurrences(listVal, value)
		}
		return int64(0)
	}

	// apoc.convert.toJson(value) - Convert value to JSON string
	if isFunctionCall(expr, "apoc.convert.tojson") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return string(jsonBytes)
	}

	// apoc.convert.fromJsonMap(json) - Parse JSON string to map
	if isFunctionCall(expr, "apoc.convert.fromjsonmap") {
		inner := strings.TrimSpace(extractFuncArgs(expr, "apoc.convert.fromjsonmap"))
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		inner := strings.TrimSpace(extractFuncArgs(expr, "apoc.convert.fromjsonlist"))
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return getCypherType(val)
	}

	// apoc.meta.isType(value, typeName) - Check if value is of given type
	if isFunctionCall(expr, "apoc.meta.istype") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			typeVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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
			map1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			map2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return mergeMaps(map1, map2)
		}
		return nil
	}

	// apoc.map.fromPairs(list) - Create map from list of [key, value] pairs
	if isFunctionCall(expr, "apoc.map.frompairs") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return fromPairs(listVal)
	}

	// apoc.map.fromLists(keys, values) - Create map from parallel lists
	if isFunctionCall(expr, "apoc.map.fromlists") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			keys := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			values := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return fromLists(keys, values)
		}
		return nil
	}

	if v, ok := e.evaluateKnowledgePolicyFunction(ctx, expr, lowerExpr, nodes, rels); ok {
		return v
	}

	// Fall through to remaining evaluation logic.
	return e.evaluateExpressionWithContextFullMath(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func randomCypherFloat() float64 {
	var bytes [8]byte
	_, _ = rand.Read(bytes[:])
	return float64(bytes[0]^bytes[1]^bytes[2]^bytes[3]) / 256.0
}
