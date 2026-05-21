package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeAggregation(ctx context.Context, nodes []*storage.Node, variable string, items []returnItem, result *ExecuteResult) (*ExecuteResult, error) {
	// Use pre-compiled case-insensitive regex patterns for aggregation functions

	// Pre-compute upper-case expressions ONCE for all subsequent use
	upperExprs := make([]string, len(items))
	for i, item := range items {
		upperExprs[i] = strings.ToUpper(item.expr)
	}
	upperVariable := strings.ToUpper(variable)

	// Identify which columns are aggregations and which are grouping keys
	type colInfo struct {
		isAggregation bool
		propName      string // For grouping columns: the property being accessed
	}
	colInfos := make([]colInfo, len(items))

	for i, item := range items {
		// Use whitespace-tolerant function check
		// Also check for expressions that contain aggregates (e.g., SUM(a) + SUM(b))
		if isAggregateFunc(item.expr) || containsAggregateFunc(item.expr) {
			colInfos[i] = colInfo{isAggregation: true}
		} else {
			// Non-aggregation - this becomes an implicit GROUP BY key
			propName := ""
			if strings.HasPrefix(item.expr, variable+".") {
				propName = item.expr[len(variable)+1:]
			}
			colInfos[i] = colInfo{isAggregation: false, propName: propName}
		}
	}

	// Check if there are any grouping columns
	// A non-aggregation column is a grouping column, even if propName is empty
	// (e.g., labels(n)[0] is a grouping key even though it's not a simple property)
	hasGrouping := false
	for _, ci := range colInfos {
		if !ci.isAggregation {
			hasGrouping = true
			break
		}
	}

	// If no grouping columns OR no nodes, return single aggregated row (old behavior)
	if !hasGrouping || len(nodes) == 0 {
		return e.executeAggregationSingleGroup(ctx, nodes, variable, items, result)
	}

	// Group nodes by the non-aggregated column values
	groups := make(map[string][]*storage.Node)
	groupKeys := make(map[string][]interface{}) // Store the actual key values

	for _, node := range nodes {
		// Build group key from all non-aggregated columns
		keyParts := make([]interface{}, 0)
		for i, ci := range colInfos {
			if !ci.isAggregation {
				var val interface{}
				if ci.propName != "" {
					val = node.Properties[ci.propName]
				} else {
					val = e.resolveReturnItem(ctx, items[i], variable, node)
				}
				keyParts = append(keyParts, val)
			}
		}
		key := fmt.Sprintf("%v", keyParts)
		groups[key] = append(groups[key], node)
		if _, exists := groupKeys[key]; !exists {
			groupKeys[key] = keyParts
		}
	}

	// Build result rows - one per group
	for key, groupNodes := range groups {
		row := make([]interface{}, len(items))
		keyIdx := 0 // Track position in keyParts

		for i, item := range items {
			upperExpr := upperExprs[i] // Use pre-computed upper-case expression

			if !colInfos[i].isAggregation {
				// Non-aggregated column - use the group key value
				row[i] = groupKeys[key][keyIdx]
				keyIdx++
				continue
			}

			switch {
			case strings.HasPrefix(upperExpr, "COUNT("):
				// COUNT(*) or COUNT(n)
				if strings.Contains(upperExpr, "*") || strings.Contains(upperExpr, "("+upperVariable+")") {
					row[i] = int64(len(groupNodes))
				} else {
					// COUNT(n.property) - count non-null values
					propMatch := countPropPattern.FindStringSubmatch(item.expr)
					if len(propMatch) == 3 {
						count := int64(0)
						for _, node := range groupNodes {
							if _, exists := node.Properties[propMatch[2]]; exists {
								count++
							}
						}
						row[i] = count
					} else {
						row[i] = int64(len(groupNodes))
					}
				}

			case strings.HasPrefix(upperExpr, "SUM("):
				propMatch := sumPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					var sumInt int64
					var sumFloat float64
					hasFloat := false
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							switch v := val.(type) {
							case int64:
								sumInt += v
								sumFloat += float64(v)
							case int:
								sumInt += int64(v)
								sumFloat += float64(v)
							case float64:
								hasFloat = true
								sumFloat += v
							}
						}
					}
					// Return float64 if any input was float, otherwise int64
					// This is more predictable and prevents type assertion panics
					if hasFloat {
						row[i] = sumFloat
					} else {
						row[i] = sumInt
					}
				} else {
					row[i] = int64(0)
				}

			case strings.HasPrefix(upperExpr, "AVG("):
				propMatch := avgPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					sum := float64(0)
					count := 0
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							if num, ok := toFloat64(val); ok {
								sum += num
								count++
							}
						}
					}
					if count > 0 {
						row[i] = sum / float64(count)
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "MIN("):
				propMatch := minPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					var minInt *int64
					var minFloat *float64
					hasFloat := false
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							switch v := val.(type) {
							case int64:
								if minInt == nil || v < *minInt {
									minInt = &v
								}
							case int:
								iv := int64(v)
								if minInt == nil || iv < *minInt {
									minInt = &iv
								}
							case float64:
								hasFloat = true
								if minFloat == nil || v < *minFloat {
									minFloat = &v
								}
							}
						}
					}
					if hasFloat && minFloat != nil {
						row[i] = *minFloat
					} else if minInt != nil {
						row[i] = *minInt
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "MAX("):
				propMatch := maxPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					var maxInt *int64
					var maxFloat *float64
					hasFloat := false
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							switch v := val.(type) {
							case int64:
								if maxInt == nil || v > *maxInt {
									maxInt = &v
								}
							case int:
								iv := int64(v)
								if maxInt == nil || iv > *maxInt {
									maxInt = &iv
								}
							case float64:
								hasFloat = true
								if maxFloat == nil || v > *maxFloat {
									maxFloat = &v
								}
							}
						}
					}
					if hasFloat && maxFloat != nil {
						row[i] = *maxFloat
					} else if maxInt != nil {
						row[i] = *maxInt
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "COLLECT("):
				propMatch := collectPropPattern.FindStringSubmatch(item.expr)
				collected := make([]interface{}, 0)
				if len(propMatch) >= 2 {
					for _, node := range groupNodes {
						if len(propMatch) == 3 && propMatch[2] != "" {
							// COLLECT(n.property)
							if val, exists := node.Properties[propMatch[2]]; exists {
								collected = append(collected, val)
							}
						} else {
							// COLLECT(n)
							collected = append(collected, map[string]interface{}{
								"id":         string(node.ID),
								"labels":     node.Labels,
								"properties": node.Properties,
							})
						}
					}
				}
				row[i] = collected
			}
		}

		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// executeAggregationSingleGroup handles aggregation without grouping (original behavior)
func (e *StorageExecutor) executeAggregationSingleGroup(ctx context.Context, nodes []*storage.Node, variable string, items []returnItem, result *ExecuteResult) (*ExecuteResult, error) {
	row := make([]interface{}, len(items))

	// Pre-compute upper-case expressions ONCE to avoid repeated ToUpper calls in loop
	upperExprs := make([]string, len(items))
	for i, item := range items {
		upperExprs[i] = strings.ToUpper(item.expr)
	}

	// Use pre-compiled regex patterns from regex_patterns.go

	for i, item := range items {
		upperExpr := upperExprs[i]

		switch {
		// Handle SUM() + SUM() arithmetic expressions first
		case strings.Contains(upperExpr, "+") && strings.Contains(upperExpr, "SUM("):
			row[i] = e.evaluateSumArithmetic(item.expr, nodes, variable)

		// Handle COUNT(DISTINCT n.property)
		case strings.HasPrefix(upperExpr, "COUNT(") && strings.Contains(upperExpr, "DISTINCT"):
			propMatch := countDistinctPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				seen := make(map[interface{}]bool)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists && val != nil {
						seen[val] = true
					}
				}
				row[i] = int64(len(seen))
			} else {
				// COUNT(DISTINCT n) - count distinct nodes
				row[i] = int64(len(nodes))
			}

		case strings.HasPrefix(upperExpr, "COUNT("):
			inner := item.expr[6 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			if inner == "*" || strings.EqualFold(inner, variable) {
				row[i] = int64(len(nodes))
			} else if isCaseExpression(inner) {
				// COUNT(CASE WHEN condition THEN 1 END) - count only non-NULL results
				count := int64(0)
				for _, node := range nodes {
					nodeMap := map[string]*storage.Node{variable: node}
					result := e.evaluateCaseExpression(ctx, inner, nodeMap, nil)
					// count() only counts non-NULL values
					if result != nil {
						count++
					}
				}
				row[i] = count
			} else {
				propMatch := countPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					count := int64(0)
					for _, node := range nodes {
						if _, exists := node.Properties[propMatch[2]]; exists {
							count++
						}
					}
					row[i] = count
				} else {
					row[i] = int64(len(nodes))
				}
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			inner := item.expr[4 : len(item.expr)-1] // Extract inner expression
			propMatch := sumPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				// SUM(n.property) - preserve integer type if all values are integers
				var sumInt int64
				var sumFloat float64
				hasFloat := false
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						switch v := val.(type) {
						case int64:
							sumInt += v
							sumFloat += float64(v)
						case int:
							sumInt += int64(v)
							sumFloat += float64(v)
						case float64:
							hasFloat = true
							sumFloat += v
						}
					}
				}
				if hasFloat {
					row[i] = sumFloat
				} else {
					row[i] = sumInt
				}
			} else if isCaseExpression(inner) {
				// SUM(CASE WHEN ... END)
				sum := float64(0)
				for _, node := range nodes {
					nodeMap := map[string]*storage.Node{variable: node}
					val := e.evaluateCaseExpression(ctx, inner, nodeMap, nil)
					if num, ok := toFloat64(val); ok {
						sum += num
					}
				}
				row[i] = sum
			} else if num, ok := toFloat64(e.parseValue(ctx, inner)); ok {
				// SUM(literal) like SUM(1)
				row[i] = num * float64(len(nodes))
			} else {
				row[i] = int64(0)
			}

		case strings.HasPrefix(upperExpr, "AVG("):
			propMatch := avgPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				sum := float64(0)
				count := 0
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
				}
				if count > 0 {
					row[i] = sum / float64(count)
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MIN("):
			propMatch := minPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var minInt *int64
				var minFloat *float64
				hasFloat := false
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						switch v := val.(type) {
						case int64:
							if minInt == nil || v < *minInt {
								minInt = &v
							}
						case int:
							iv := int64(v)
							if minInt == nil || iv < *minInt {
								minInt = &iv
							}
						case float64:
							hasFloat = true
							if minFloat == nil || v < *minFloat {
								minFloat = &v
							}
						}
					}
				}
				if hasFloat && minFloat != nil {
					row[i] = *minFloat
				} else if minInt != nil {
					row[i] = *minInt
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MAX("):
			propMatch := maxPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var maxInt *int64
				var maxFloat *float64
				hasFloat := false
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						switch v := val.(type) {
						case int64:
							if maxInt == nil || v > *maxInt {
								maxInt = &v
							}
						case int:
							iv := int64(v)
							if maxInt == nil || iv > *maxInt {
								maxInt = &iv
							}
						case float64:
							hasFloat = true
							if maxFloat == nil || v > *maxFloat {
								maxFloat = &v
							}
						}
					}
				}
				if hasFloat && maxFloat != nil {
					row[i] = *maxFloat
				} else if maxInt != nil {
					row[i] = *maxInt
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		// Handle COLLECT(DISTINCT expression)
		case strings.HasPrefix(upperExpr, "COLLECT(") && strings.Contains(upperExpr, "DISTINCT"):
			// Extract the inner expression after "COLLECT(DISTINCT "
			inner := item.expr[17 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			propMatch := collectDistinctPropPattern.FindStringSubmatch(item.expr)
			seen := make(map[string]bool) // Use string key for map comparison
			collected := make([]interface{}, 0)

			if len(propMatch) == 3 {
				// Simple property access: COLLECT(DISTINCT n.property)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists && val != nil {
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT(DISTINCT { key: value })
				for _, node := range nodes {
					nodeCtx := map[string]*storage.Node{variable: node}
					// Add connected node if this is a relationship traversal result
					// The variable from the matched row should be accessible
					val := e.evaluateExpressionWithContext(ctx, inner, nodeCtx, nil)
					if val != nil {
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
				}
			}
			row[i] = collected

		case strings.HasPrefix(upperExpr, "COLLECT("):
			// Use extractFuncArgsWithSuffix to properly handle cases like collect({...})[..10]
			inner, suffix, ok := extractFuncArgsWithSuffix(item.expr, "collect")
			if !ok {
				// Fallback to old method
				inner = item.expr[8 : len(item.expr)-1]
				inner = strings.TrimSpace(inner)
				suffix = ""
			}

			propMatch := collectPropPattern.FindStringSubmatch(item.expr)
			collected := make([]interface{}, 0)

			if len(propMatch) >= 2 && (len(propMatch) < 3 || propMatch[2] != "") {
				// Simple property or variable access: COLLECT(n) or COLLECT(n.property)
				for _, node := range nodes {
					if len(propMatch) == 3 && propMatch[2] != "" {
						if val, exists := node.Properties[propMatch[2]]; exists {
							collected = append(collected, val)
						}
					} else {
						collected = append(collected, map[string]interface{}{
							"id":         string(node.ID),
							"labels":     node.Labels,
							"properties": node.Properties,
						})
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT({ key: value })
				for _, node := range nodes {
					nodeCtx := map[string]*storage.Node{variable: node}
					val := e.evaluateExpressionWithContext(ctx, inner, nodeCtx, nil)
					if val != nil {
						collected = append(collected, val)
					}
				}
			}

			// Apply suffix (e.g., [..10] for slicing) if present
			var result interface{} = collected
			if suffix != "" {
				// Wrap collected in a temporary expression for slicing
				// The suffix is something like "[..10]"
				result = e.applyArraySuffix(collected, suffix)
			}
			row[i] = result

		default:
			// Non-aggregate in aggregation query - return first value
			if len(nodes) > 0 {
				row[i] = e.resolveReturnItem(ctx, item, variable, nodes[0])
			} else {
				row[i] = nil
			}
		}
	}

	result.Rows = [][]interface{}{row}
	return result, nil
}

// nodeOrderSpec represents a single ORDER BY specification for nodes
type nodeOrderSpec struct {
	propName   string
	descending bool
}

// orderNodes sorts nodes by the given expression, supporting multiple columns
