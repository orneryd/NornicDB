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
	if hasPrefixFoldASCII(expr, "not ") {
		inner := strings.TrimSpace(expr[4:])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if b, ok := result.(bool); ok {
			return !b
		}
		return nil
	}

	// BETWEEN must be checked before AND (because BETWEEN x AND y uses AND)
	if betweenLeft, betweenRight, ok := splitByOperatorWithOptions(expr, " BETWEEN ", true, true); ok {
		value := e.evaluateExpressionWithContextFull(ctx, betweenLeft, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if minPart, maxPart, ok := splitByOperatorWithOptions(betweenRight, " AND ", true, true); ok {
			minVal := e.evaluateExpressionWithContextFull(ctx, minPart, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			maxVal := e.evaluateExpressionWithContextFull(ctx, maxPart, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return (e.compareGreater(value, minVal) || e.compareEqual(value, minVal)) &&
				(e.compareLess(value, maxVal) || e.compareEqual(value, maxVal))
		}
	}

	// AND operator
	if left, right, ok := splitByOperatorWithOptions(expr, " AND ", true, false); ok {
		leftValue := e.evaluateExpressionWithContextFull(ctx, left, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if leftValue == false {
			return false
		}
		rightValue := e.evaluateExpressionWithContextFull(ctx, right, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if rightValue == false {
			return false
		}
		if leftValue == nil || rightValue == nil {
			return nil
		}
		return leftValue == true && rightValue == true
	}

	// OR operator
	if left, right, ok := splitByOperatorWithOptions(expr, " OR ", true, false); ok {
		leftValue := e.evaluateExpressionWithContextFull(ctx, left, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if leftValue == true {
			return true
		}
		rightValue := e.evaluateExpressionWithContextFull(ctx, right, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if rightValue == true {
			return true
		}
		if leftValue == nil || rightValue == nil {
			return nil
		}
		return false
	}

	// XOR operator
	if left, right, ok := splitByOperatorWithOptions(expr, " XOR ", true, false); ok {
		leftValue := e.evaluateExpressionWithContextFull(ctx, left, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		rightValue := e.evaluateExpressionWithContextFull(ctx, right, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if leftValue == nil || rightValue == nil {
			return nil
		}
		return (leftValue == true) != (rightValue == true)
	}

	// ========================================
	// Null Predicates (IS NULL, IS NOT NULL)
	// ========================================
	if hasSuffixFoldASCII(expr, " is null") {
		inner := strings.TrimSpace(expr[:len(expr)-8])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return result == nil
	}
	if hasSuffixFoldASCII(expr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return result != nil
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " STARTS WITH ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if ok1 && ok2 {
			return strings.HasPrefix(leftStr, rightStr)
		}
		return false
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " ENDS WITH ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if ok1 && ok2 {
			return strings.HasSuffix(leftStr, rightStr)
		}
		return false
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " CONTAINS ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if ok1 && ok2 {
			return strings.Contains(leftStr, rightStr)
		}
		return false
	}

	// ========================================
	// IN Operator (value IN list)
	// ========================================
	// NOT IN must be checked before IN (because "NOT IN" contains " IN ")
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " NOT IN ", true, true); ok {
		result, validList := e.evaluateInOperator(ctx, leftExpr, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if !validList {
			return true
		}
		if result == nil {
			return nil
		}
		return !result.(bool)
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " IN ", true, true); ok {
		result, validList := e.evaluateInOperator(ctx, leftExpr, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if !validList {
			return false
		}
		return result
	}

	// Comparison operators (=, <>, <, >, <=, >=)
	if result, matched := e.evaluateComparisonExpr(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); matched {
		return result
	}

	// Arithmetic operators (*, /, %, -, +)
	// NOTE: Arithmetic is checked BEFORE string concatenation to support date/duration arithmetic
	if result := e.evaluateArithmeticExpr(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); result != nil {
		return result
		// If arithmetic returned nil, fall through to string concatenation for + operator
	}

	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// This is a fallback when arithmetic didn't apply (e.g., string + string)
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
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

	if lowerExpr == "" {
		lowerExpr = strings.ToLower(expr)
	}
	return e.evaluateExpressionWithContextFullPropsLiterals(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func (e *StorageExecutor) evaluateInOperator(ctx context.Context, leftExpr, rightExpr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (interface{}, bool) {
	value := e.evaluateExpressionWithContextFull(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	listValue := e.evaluateExpressionWithContextFull(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	if listValue == nil {
		return nil, true
	}
	list, ok := toInterfaceSlice(listValue)
	if !ok {
		return nil, false
	}
	if len(list) == 0 {
		return false, true
	}
	if value == nil {
		return nil, true
	}
	containsUnknown := false
	for _, item := range list {
		equal := cypherEquality(value, item)
		if equal == nil {
			containsUnknown = true
			continue
		}
		if equal.(bool) {
			return true, true
		}
	}
	if containsUnknown {
		return nil, true
	}
	return false, true
}

func (e *StorageExecutor) evaluateStringPredicateOperand(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (string, bool) {
	if isWholeCypherQuotedString(expr) {
		return decodeCypherQuotedString(expr)
	}
	value := e.evaluateExpressionWithContextFull(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	str, ok := value.(string)
	return str, ok
}
