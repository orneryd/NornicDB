package cypher

import (
	"context"
	"strconv"
	"strings"
	"sync"
)

type bindingWherePredicate func(binding, map[string]interface{}) bool

var compiledBindingWhereCache sync.Map // map[string]bindingWherePredicate

func (e *StorageExecutor) getCompiledBindingWhere(ctx context.Context, whereClause string) bindingWherePredicate {
	key := normalizeBindingWhereClause(whereClause)
	if cached, ok := compiledBindingWhereCache.Load(key); ok {
		if predicate, ok := cached.(bindingWherePredicate); ok {
			return predicate
		}
	}
	predicate := e.compileBindingWhere(ctx, key)
	compiledBindingWhereCache.Store(key, predicate)
	return predicate
}

func normalizeBindingWhereClause(whereClause string) string {
	clause := strings.TrimSpace(whereClause)
	clause = strings.ReplaceAll(clause, "\r", " ")
	clause = strings.ReplaceAll(clause, "\n", " ")
	clause = strings.ReplaceAll(clause, "\t", " ")
	return clause
}

func (e *StorageExecutor) compileBindingWhere(ctx context.Context, whereClause string) bindingWherePredicate {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return func(binding, map[string]interface{}) bool { return true }
	}

	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left := e.getCompiledBindingWhere(ctx, clause[:andIdx])
		right := e.getCompiledBindingWhere(ctx, clause[andIdx+5:])
		return func(b binding, params map[string]interface{}) bool {
			return left(b, params) && right(b, params)
		}
	}
	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left := e.getCompiledBindingWhere(ctx, clause[:orIdx])
		right := e.getCompiledBindingWhere(ctx, clause[orIdx+4:])
		return func(b binding, params map[string]interface{}) bool {
			return left(b, params) || right(b, params)
		}
	}
	if hasPrefixFold(clause, "NOT ") {
		inner := e.getCompiledBindingWhere(ctx, clause[4:])
		return func(b binding, params map[string]interface{}) bool {
			return !inner(b, params)
		}
	}

	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NOT NULL ", true); ok {
		return predicate
	}
	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NULL ", false); ok {
		return predicate
	}

	if predicate, ok := e.compileBindingStringPredicate(clause, " STARTS WITH "); ok {
		return predicate
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " ENDS WITH "); ok {
		return predicate
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " CONTAINS "); ok {
		return predicate
	}
	if predicate, ok := e.compileBindingInPredicate(clause, " IN ", false); ok {
		return predicate
	}
	if predicate, ok := e.compileBindingInPredicate(clause, " NOT IN ", true); ok {
		return predicate
	}

	if predicate, ok := e.compileBindingComparisonPredicate(clause); ok {
		return predicate
	}

	return func(b binding, params map[string]interface{}) bool {
		return e.evaluateBindingWhereGeneric(ctx, b, clause, params)
	}
}

func (e *StorageExecutor) compileBindingStringPredicate(clause, op string) (bindingWherePredicate, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	leftExpr := strings.TrimSpace(clause[:idx])
	rightExpr := strings.TrimSpace(clause[idx+len(op):])
	leftResolver, ok := e.compileBindingValueResolver(leftExpr)
	if !ok {
		return nil, false
	}
	rightResolver, ok := e.compileBindingValueResolver(rightExpr)
	if !ok {
		return nil, false
	}
	return func(b binding, params map[string]interface{}) bool {
		leftValue, ok := leftResolver(b, params)
		if !ok {
			return false
		}
		rightValue, ok := rightResolver(b, params)
		if !ok {
			return false
		}
		leftStr, ok := leftValue.(string)
		if !ok {
			return false
		}
		rightStr, ok := rightValue.(string)
		if !ok {
			return false
		}
		switch op {
		case " STARTS WITH ":
			return strings.HasPrefix(leftStr, rightStr)
		case " ENDS WITH ":
			return strings.HasSuffix(leftStr, rightStr)
		case " CONTAINS ":
			return strings.Contains(leftStr, rightStr)
		default:
			return false
		}
	}, true
}

func (e *StorageExecutor) compileBindingNullPredicate(clause, op string, expectNotNull bool) (bindingWherePredicate, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	expr := strings.TrimSpace(clause[:idx])
	if expr == "" {
		return nil, false
	}
	resolver, ok := e.compileBindingValueResolver(expr)
	if !ok {
		return nil, false
	}
	return func(b binding, params map[string]interface{}) bool {
		value, ok := resolver(b, params)
		if !ok {
			return !expectNotNull
		}
		if expectNotNull {
			return value != nil
		}
		return value == nil
	}, true
}

func (e *StorageExecutor) compileBindingComparisonPredicate(clause string) (bindingWherePredicate, bool) {
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		idx := findTopLevelKeyword(clause, op)
		if idx <= 0 {
			continue
		}
		leftExpr := strings.TrimSpace(clause[:idx])
		rightExpr := strings.TrimSpace(clause[idx+len(op):])

		if leftExpr == "" || rightExpr == "" {
			return nil, false
		}

		leftIsNodeRef := isValidIdentifier(leftExpr)
		rightIsNodeRef := isValidIdentifier(rightExpr)
		if leftIsNodeRef && rightIsNodeRef {
			leftKey := leftExpr
			rightKey := rightExpr
			return func(b binding, params map[string]interface{}) bool {
				_ = params
				leftNode := b[leftKey]
				rightNode := b[rightKey]
				if leftNode == nil || rightNode == nil {
					return false
				}
				switch op {
				case "=":
					return leftNode.ID == rightNode.ID
				case "<>", "!=":
					return leftNode.ID != rightNode.ID
				default:
					return e.compareNodeIDs(string(leftNode.ID), string(rightNode.ID), op)
				}
			}, true
		}

		leftResolver, ok := e.compileBindingValueResolver(leftExpr)
		if !ok {
			return nil, false
		}
		rightResolver, ok := e.compileBindingValueResolver(rightExpr)
		if !ok {
			return nil, false
		}

		return func(b binding, params map[string]interface{}) bool {
			leftValue, ok := leftResolver(b, params)
			if !ok {
				return false
			}
			rightValue, ok := rightResolver(b, params)
			if !ok {
				return false
			}
			switch op {
			case "=":
				return e.compareEqual(leftValue, rightValue)
			case "<>", "!=":
				return !e.compareEqual(leftValue, rightValue)
			case ">":
				return e.compareGreater(leftValue, rightValue)
			case ">=":
				return e.compareGreater(leftValue, rightValue) || e.compareEqual(leftValue, rightValue)
			case "<":
				return e.compareLess(leftValue, rightValue)
			case "<=":
				return e.compareLess(leftValue, rightValue) || e.compareEqual(leftValue, rightValue)
			default:
				return false
			}
		}, true
	}
	return nil, false
}

func (e *StorageExecutor) compileBindingInPredicate(clause, op string, negate bool) (bindingWherePredicate, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	leftExpr := strings.TrimSpace(clause[:idx])
	rightExpr := strings.TrimSpace(clause[idx+len(op):])
	leftResolver, ok := e.compileBindingValueResolver(leftExpr)
	if !ok {
		return nil, false
	}

	if listValues, ok := parseBindingLiteralList(rightExpr); ok {
		return e.makeCompiledBindingMembershipPredicate(leftResolver, listValues, negate), true
	}

	rightResolver, ok := e.compileBindingValueResolver(rightExpr)
	if !ok {
		return nil, false
	}
	return func(b binding, params map[string]interface{}) bool {
		leftValue, ok := leftResolver(b, params)
		if !ok {
			return false
		}
		rightValue, ok := rightResolver(b, params)
		if !ok {
			return false
		}
		items, ok := toInterfaceSlice(rightValue)
		if !ok {
			return false
		}
		comparableSet, nonComparable := buildComparableMembershipIndex(items)
		matched := evaluateComparableMembership(leftValue, comparableSet, nonComparable, e.compareEqual)
		if negate {
			return !matched
		}
		return matched
	}, true
}

func (e *StorageExecutor) makeCompiledBindingMembershipPredicate(leftResolver bindingValueResolver, items []interface{}, negate bool) bindingWherePredicate {
	comparableSet := make(map[interface{}]struct{}, len(items))
	nonComparable := make([]interface{}, 0)
	for _, item := range items {
		if item == nil {
			continue
		}
		if isComparableValue(item) {
			comparableSet[item] = struct{}{}
		} else {
			nonComparable = append(nonComparable, item)
		}
	}
	return func(b binding, params map[string]interface{}) bool {
		leftValue, ok := leftResolver(b, params)
		if !ok {
			return false
		}
		matched := false
		if isComparableValue(leftValue) {
			if _, hit := comparableSet[leftValue]; hit {
				matched = true
			}
		}
		if !matched {
			for _, item := range nonComparable {
				if e.compareEqual(leftValue, item) {
					matched = true
					break
				}
			}
		}
		if negate {
			return !matched
		}
		return matched
	}
}

func parseBindingLiteralList(raw string) ([]interface{}, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	if inner == "" {
		return []interface{}{}, true
	}
	parts := splitTopLevelCommaKeepEmpty(inner)
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := parseLiteralValue(part)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

type bindingValueResolver func(binding, map[string]interface{}) (interface{}, bool)

func (e *StorageExecutor) compileBindingValueResolver(expr string) (bindingValueResolver, bool) {
	clause := strings.TrimSpace(expr)
	if clause == "" {
		return func(binding, map[string]interface{}) (interface{}, bool) { return nil, false }, false
	}
	if literal, ok := parseLiteralValue(clause); ok {
		return func(binding, map[string]interface{}) (interface{}, bool) { return literal, true }, true
	}
	if strings.HasPrefix(clause, "$") {
		paramName := strings.TrimSpace(strings.TrimPrefix(clause, "$"))
		if paramName == "" {
			return nil, false
		}
		return func(_ binding, params map[string]interface{}) (interface{}, bool) {
			if params == nil {
				return nil, false
			}
			value, ok := params[paramName]
			return value, ok
		}, true
	}
	if dotIdx := strings.Index(clause, "."); dotIdx > 0 {
		varName := strings.TrimSpace(clause[:dotIdx])
		propName := strings.TrimSpace(clause[dotIdx+1:])
		if varName == "" || propName == "" || strings.ContainsAny(varName, " \t\r\n") || strings.ContainsAny(propName, " \t\r\n") {
			return nil, false
		}
		return func(b binding, params map[string]interface{}) (interface{}, bool) {
			_ = params
			node := b[varName]
			if node == nil {
				return nil, false
			}
			return getBindingNodeValue(node, propName)
		}, true
	}
	if isValidIdentifier(clause) {
		return func(b binding, params map[string]interface{}) (interface{}, bool) {
			_ = params
			node := b[clause]
			if node == nil {
				return nil, false
			}
			return node, true
		}, true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateBindingWhereGeneric(ctx context.Context, b binding, whereClause string, params map[string]interface{}) bool {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return true
	}
	clause = strings.ReplaceAll(clause, "\n", " ")
	clause = strings.ReplaceAll(clause, "\r", " ")
	clause = strings.ReplaceAll(clause, "\t", " ")
	upper := strings.ToUpper(clause)

	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(clause[:andIdx])
		right := strings.TrimSpace(clause[andIdx+5:])
		return e.evaluateBindingWhere(ctx, b, left, params) && e.evaluateBindingWhere(ctx, b, right, params)
	}
	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(clause[:orIdx])
		right := strings.TrimSpace(clause[orIdx+4:])
		return e.evaluateBindingWhere(ctx, b, left, params) || e.evaluateBindingWhere(ctx, b, right, params)
	}
	if strings.HasPrefix(upper, "NOT ") {
		return !e.evaluateBindingWhere(ctx, b, clause[4:], params)
	}

	for _, pred := range []string{" STARTS WITH ", " ENDS WITH ", " CONTAINS "} {
		if idx := findTopLevelKeyword(clause, pred); idx > 0 {
			left := strings.TrimSpace(clause[:idx])
			right := strings.TrimSpace(clause[idx+len(pred):])
			if dotIdx := strings.Index(left, "."); dotIdx > 0 {
				varName := left[:dotIdx]
				propName := left[dotIdx+1:]
				if node := b[varName]; node != nil {
					actual, _ := node.Properties[propName].(string)
					expectedRaw := e.resolveBindingFallbackValue(ctx, right, b, params)
					expected, _ := expectedRaw.(string)
					switch strings.TrimSpace(strings.ToUpper(pred)) {
					case "STARTS WITH":
						return strings.HasPrefix(actual, expected)
					case "ENDS WITH":
						return strings.HasSuffix(actual, expected)
					case "CONTAINS":
						return strings.Contains(actual, expected)
					}
				}
			}
			return false
		}
	}

	if strings.Contains(clause, "<>") || strings.Contains(clause, "!=") {
		op := "<>"
		opIdx := strings.Index(clause, "<>")
		if opIdx == -1 {
			op = "!="
			opIdx = strings.Index(clause, "!=")
		}
		left := strings.TrimSpace(clause[:opIdx])
		right := strings.TrimSpace(clause[opIdx+len(op):])
		if !strings.Contains(left, ".") && !strings.Contains(right, ".") {
			leftNode := b[left]
			rightNode := b[right]
			if leftNode != nil && rightNode != nil {
				return leftNode.ID != rightNode.ID
			}
		}
	}

	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := strings.Index(clause, op); idx > 0 {
			left := strings.TrimSpace(clause[:idx])
			right := strings.TrimSpace(clause[idx+len(op):])

			if dotIdx := strings.Index(left, "."); dotIdx > 0 {
				varName := left[:dotIdx]
				propName := left[dotIdx+1:]

				if node := b[varName]; node != nil {
					actualVal := node.Properties[propName]
					expectedVal := e.resolveBindingFallbackValue(ctx, right, b, params)

					switch op {
					case "=":
						return e.compareEqual(actualVal, expectedVal)
					case "<>", "!=":
						return !e.compareEqual(actualVal, expectedVal)
					case ">":
						return e.compareGreater(actualVal, expectedVal)
					case ">=":
						return e.compareGreater(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					case "<":
						return e.compareLess(actualVal, expectedVal)
					case "<=":
						return e.compareLess(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					}
				}
			} else {
				leftNode := b[left]
				rightNode := b[right]
				if leftNode != nil && rightNode != nil {
					switch op {
					case "=":
						return leftNode.ID == rightNode.ID
					case "<>", "!=":
						return leftNode.ID != rightNode.ID
					}
				}
			}
		}
	}

	return e.evaluateBindingExpressionAsBoolean(ctx, b, clause, params)
}

func (e *StorageExecutor) resolveBindingFallbackValue(ctx context.Context, expr string, b binding, params map[string]interface{}) interface{} {
	value, ok := e.resolveBindingFallbackValueWithOk(ctx, expr, b, params)
	if !ok {
		return nil
	}
	return value
}

func (e *StorageExecutor) resolveBindingFallbackValueWithOk(ctx context.Context, expr string, b binding, params map[string]interface{}) (interface{}, bool) {
	resolver, ok := e.compileBindingValueResolver(expr)
	if ok {
		return resolver(b, params)
	}
	if params != nil {
		if value, exists := params[strings.TrimSpace(expr)]; exists {
			return value, true
		}
	}
	return e.evaluateExpressionWithContext(ctx, e.substituteParams(expr, params), b, nil), true
}

func (e *StorageExecutor) evaluateBindingExpressionAsBoolean(ctx context.Context, b binding, expr string, params map[string]interface{}) bool {
	resolved := e.substituteParams(expr, params)
	result := e.evaluateExpressionWithContext(ctx, resolved, b, nil)
	if boolResult, ok := result.(bool); ok {
		return boolResult
	}
	return false
}

func (e *StorageExecutor) compareNodeIDs(leftID, rightID string, op string) bool {
	leftNum, leftErr := strconv.ParseInt(leftID, 10, 64)
	rightNum, rightErr := strconv.ParseInt(rightID, 10, 64)
	if leftErr == nil && rightErr == nil {
		switch op {
		case ">":
			return leftNum > rightNum
		case ">=":
			return leftNum >= rightNum
		case "<":
			return leftNum < rightNum
		case "<=":
			return leftNum <= rightNum
		default:
			return false
		}
	}

	switch op {
	case ">":
		return leftID > rightID
	case ">=":
		return leftID >= rightID
	case "<":
		return leftID < rightID
	case "<=":
		return leftID <= rightID
	default:
		return false
	}
}
