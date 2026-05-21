package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullOperators(
	ctx context.Context,
	expr string,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	paths map[string]*PathResult,
	allPathEdges []*storage.Edge,
	allPathNodes []*storage.Node,
	pathLength int,
) interface{} {
	// Boolean/Comparison Operators (must be before property access)
	// ========================================

	// NOT expr
	if strings.HasPrefix(lowerExpr, "not ") {
		inner := strings.TrimSpace(expr[4:])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if b, ok := result.(bool); ok {
			return !b
		}
		return nil
	}

	// BETWEEN must be checked before AND (because BETWEEN x AND y uses AND)
	if e.hasStringPredicate(expr, " BETWEEN ") {
		betweenParts := e.splitByOperator(expr, " BETWEEN ")
		if len(betweenParts) == 2 {
			value := e.evaluateExpressionWithContext(ctx, betweenParts[0], nodes, rels)
			// For BETWEEN, we need to split by AND but only in the range part
			rangeParts := strings.SplitN(strings.ToUpper(betweenParts[1]), " AND ", 2)
			if len(rangeParts) == 2 {
				// Get the actual case-preserved parts
				andIdx := strings.Index(strings.ToUpper(betweenParts[1]), " AND ")
				minPart := strings.TrimSpace(betweenParts[1][:andIdx])
				maxPart := strings.TrimSpace(betweenParts[1][andIdx+5:])
				minVal := e.evaluateExpressionWithContext(ctx, minPart, nodes, rels)
				maxVal := e.evaluateExpressionWithContext(ctx, maxPart, nodes, rels)
				return (e.compareGreater(value, minVal) || e.compareEqual(value, minVal)) &&
					(e.compareLess(value, maxVal) || e.compareEqual(value, maxVal))
			}
		}
	}

	// AND operator
	if e.hasLogicalOperator(expr, " AND ") {
		return e.evaluateLogicalAnd(ctx, expr, nodes, rels)
	}

	// OR operator
	if e.hasLogicalOperator(expr, " OR ") {
		return e.evaluateLogicalOr(ctx, expr, nodes, rels)
	}

	// XOR operator
	if e.hasLogicalOperator(expr, " XOR ") {
		return e.evaluateLogicalXor(ctx, expr, nodes, rels)
	}

	// Comparison operators (=, <>, <, >, <=, >=)
	if e.hasComparisonOperator(expr) {
		return e.evaluateComparisonExpr(ctx, expr, nodes, rels)
	}

	// Arithmetic operators (*, /, %, -, +)
	// NOTE: Arithmetic is checked BEFORE string concatenation to support date/duration arithmetic
	if e.hasArithmeticOperator(expr) {
		result := e.evaluateArithmeticExpr(ctx, expr, nodes, rels)
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
		return e.evaluateStringConcatWithContext(ctx, expr, nodes, rels)
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
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
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		return result == nil
	}
	if strings.HasSuffix(lowerExpr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		return result != nil
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	if e.hasStringPredicate(expr, " STARTS WITH ") {
		parts := e.splitByOperator(expr, " STARTS WITH ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(ctx, parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(ctx, parts[1], nodes, rels)
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
			left := e.evaluateExpressionWithContext(ctx, parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(ctx, parts[1], nodes, rels)
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
			left := e.evaluateExpressionWithContext(ctx, parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(ctx, parts[1], nodes, rels)
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
			value := e.evaluateExpressionWithContext(ctx, parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(ctx, parts[1], nodes, rels)

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
			value := e.evaluateExpressionWithContext(ctx, parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(ctx, parts[1], nodes, rels)

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

	return e.evaluateExpressionWithContextFullPropsLiterals(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}
