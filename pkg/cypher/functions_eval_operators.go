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

	// OR, XOR, AND and NOT (evaluateLogicalExpression).
	if value, logical, _, err := evaluateLogicalExpression(expr, func(operand string) (interface{}, bool, error) {
		return deferredExpressionValue(ctx, func(ctx context.Context) interface{} {
			return e.evaluateExpressionWithContextFull(ctx, operand, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		})
	}); logical {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return value
	}

	// ========================================
	// Null Predicates (IS NULL, IS NOT NULL)
	// ========================================
	if hasSuffixFoldASCII(expr, " is null") {
		inner := strings.TrimSpace(expr[:len(expr)-8])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return nullEvaluationHandler(false).evaluate(result)
	}
	if hasSuffixFoldASCII(expr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return nullEvaluationHandler(true).evaluate(result)
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	for _, p := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{" STARTS WITH ", strings.HasPrefix}, {" ENDS WITH ", strings.HasSuffix}, {" CONTAINS ", strings.Contains},
	} {
		if left, right, ok := splitByOperatorOutsideCase(expr, p.operator, true, true); ok {
			l := e.evaluateExpressionWithContextFull(ctx, left, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			r := e.evaluateExpressionWithContextFull(ctx, right, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return rowStringPredicateValue(l, r, p.match)
		}
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

	// Arithmetic operators (*, /, %, -, +), including string and list +.
	// A null result (null + 1, [1] + null) is the expression's value.
	if result, handled := e.evaluateArithmeticExpr(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); handled {
		return result
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if result == nil {
			return nil
		}
		if negated, handled, err := numericArithmetic('-', int64(0), result); handled {
			recordExpressionFailure(ctx, err)
			return negated
		}
	}

	// ========================================

	if lowerExpr == "" {
		lowerExpr = lowerASCII(expr)
	}
	return e.evaluateExpressionWithContextFullPropsLiterals(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func (e *StorageExecutor) evaluateInOperator(ctx context.Context, leftExpr, rightExpr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (interface{}, bool) {
	value := e.evaluateExpressionWithContextFull(ctx, leftExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	listValue := e.evaluateExpressionWithContextFull(ctx, rightExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	return cypherMembership(value, listValue)
}

// cypherMembership is Cypher's three-valued `value IN list`: nil for a null
// list, false for an empty list, nil for a null value, true on a match, and
// nil when the list holds an element whose comparison is unknown (null) and
// nothing matched. ok is false when listValue is not a list.
func cypherMembership(value, listValue interface{}) (interface{}, bool) {
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
