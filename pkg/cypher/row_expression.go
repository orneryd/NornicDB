package cypher

import (
	"context"
	"reflect"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateRowExpression resolves an expression against a heterogeneous Cypher
// row. Unlike the graph-only evaluator, a row may also contain scalar, map,
// and list bindings introduced by WITH or UNWIND.
func (e *StorageExecutor) evaluateRowExpression(expr string, values map[string]interface{}) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if inner, ok := stripEnclosingExpressionParentheses(expr); ok {
		return e.evaluateRowExpression(inner, values)
	}
	if value, ok := values[expr]; ok {
		return value, true
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		return value, true
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		result := make(map[string]interface{})
		if inner == "" {
			return result, true
		}
		for _, pair := range splitTopLevelComma(inner) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				return nil, false
			}
			key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
			value, ok := e.evaluateRowExpression(strings.TrimSpace(pair[separator+1:]), values)
			if !ok {
				return nil, false
			}
			result[key] = value
		}
		return result, true
	}

	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}, true
		}
		items := splitTopLevelComma(inner)
		result := make([]interface{}, 0, len(items))
		for _, item := range items {
			value, ok := e.evaluateRowExpression(item, values)
			if !ok {
				return nil, false
			}
			result = append(result, value)
		}
		return result, true
	}

	if value, matched, ok := e.evaluateRowQuantifier(expr, values); matched {
		return value, ok
	}

	if function, argument, ok := parseFunctionCallWS(expr); ok {
		switch strings.ToLower(function) {
		case "abs":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			switch number := value.(type) {
			case int64:
				if number < 0 {
					return -number, true
				}
				return number, true
			case float64:
				if number < 0 {
					return -number, true
				}
				return number, true
			default:
				return nil, false
			}
		case "head", "last", "tail", "reverse", "size":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if text, isString := value.(string); isString {
				switch strings.ToLower(function) {
				case "size":
					return int64(len([]rune(text))), true
				case "reverse":
					runes := []rune(text)
					for left, right := 0, len(runes)-1; left < right; left, right = left+1, right-1 {
						runes[left], runes[right] = runes[right], runes[left]
					}
					return string(runes), true
				default:
					return nil, false
				}
			}
			valueType := reflect.TypeOf(value)
			if valueType == nil || (valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array) {
				return nil, false
			}
			items := toAnySlice(value)
			switch strings.ToLower(function) {
			case "head":
				if len(items) == 0 {
					return nil, true
				}
				return items[0], true
			case "last":
				if len(items) == 0 {
					return nil, true
				}
				return items[len(items)-1], true
			case "tail":
				if len(items) <= 1 {
					return []interface{}{}, true
				}
				return append([]interface{}(nil), items[1:]...), true
			case "reverse":
				reversed := make([]interface{}, len(items))
				for index := range items {
					reversed[len(items)-1-index] = items[index]
				}
				return reversed, true
			default:
				return int64(len(items)), true
			}
		case "keys":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			object, isMap := toStringAnyMap(value)
			if !isMap {
				switch entity := value.(type) {
				case *storage.Node:
					if entity != nil {
						object = entity.Properties
						isMap = true
					}
				case *storage.Edge:
					if entity != nil {
						object = entity.Properties
						isMap = true
					}
				}
			}
			if !isMap {
				return nil, false
			}
			keys := make([]string, 0, len(object))
			for key := range object {
				if key != "_nodeId" && key != "_edgeId" && key != "labels" && key != "type" {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			result := make([]interface{}, len(keys))
			for index, key := range keys {
				result[index] = key
			}
			return result, true
		case "labels":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			var labels []string
			switch entity := value.(type) {
			case *storage.Node:
				if entity != nil {
					labels = entity.Labels
				}
			default:
				if object, isMap := toStringAnyMap(value); isMap {
					labels = toStringSlice(object["labels"])
				}
			}
			result := make([]interface{}, len(labels))
			for index, label := range labels {
				result[index] = label
			}
			return result, true
		}
	}

	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		if left, right, ok := splitByOperatorWithOptions(expr, operator, true, false); ok {
			leftValue, leftOK := e.evaluateRowExpression(left, values)
			rightValue, rightOK := e.evaluateRowExpression(right, values)
			if !leftOK || !rightOK {
				return nil, false
			}
			return evaluateRowBooleanOperator(strings.TrimSpace(operator), leftValue, rightValue)
		}
	}

	for _, operator := range []string{"<=", ">=", "<>", "!=", "=", "<", ">"} {
		if left, right, ok := splitByOperatorWithOptions(expr, operator, true, true); ok {
			leftValue, leftOK := e.evaluateRowExpression(left, values)
			rightValue, rightOK := e.evaluateRowExpression(right, values)
			if !leftOK || !rightOK {
				return nil, false
			}
			if leftValue == nil || rightValue == nil {
				return nil, true
			}
			if operator == "!=" {
				operator = "<>"
			}
			return compareWithOperator(leftValue, rightValue, operator), true
		}
	}

	for _, predicate := range []struct {
		suffix  string
		notNull bool
	}{
		{suffix: " IS NOT NULL", notNull: true},
		{suffix: " IS NULL"},
	} {
		if hasSuffixFoldASCII(expr, strings.ToLower(predicate.suffix)) {
			value, ok := e.evaluateRowExpression(strings.TrimSpace(expr[:len(expr)-len(predicate.suffix)]), values)
			if !ok {
				return nil, false
			}
			if predicate.notNull {
				return value != nil, true
			}
			return value == nil, true
		}
	}

	if hasPrefixFoldASCII(expr, "NOT ") {
		value, ok := e.evaluateRowExpression(strings.TrimSpace(expr[len("NOT "):]), values)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		boolean, ok := value.(bool)
		if !ok {
			return nil, false
		}
		return !boolean, true
	}

	if open := strings.LastIndex(expr, "["); open > 0 && strings.HasSuffix(expr, "]") {
		base, ok := e.evaluateRowExpression(expr[:open], values)
		if !ok {
			return nil, false
		}
		indexValue, ok := e.evaluateRowExpression(expr[open+1:len(expr)-1], values)
		if !ok {
			return nil, false
		}
		index, ok := rowSubscriptIndex(indexValue)
		if !ok {
			return nil, false
		}
		items := toAnySlice(base)
		if index < 0 {
			index += len(items)
		}
		if index < 0 || index >= len(items) {
			return nil, true
		}
		return items[index], true
	}

	if left, right, ok := splitByOperatorWithOptions(expr, "+", true, false); ok {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		if leftText, ok := leftValue.(string); ok {
			if rightText, ok := rightValue.(string); ok {
				return leftText + rightText, true
			}
		}
		return e.add(leftValue, rightValue), true
	}
	if left, right, ok := splitByOperatorWithOptions(expr, "-", true, false); ok && isBinaryRowSubtraction(left) {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		return e.subtract(leftValue, rightValue), true
	}

	for _, arithmetic := range []struct {
		operator string
		apply    func(interface{}, interface{}) interface{}
	}{
		{operator: "%", apply: e.modulo},
		{operator: "*", apply: e.multiply},
		{operator: "/", apply: e.divide},
	} {
		if left, right, ok := splitByOperatorWithOptions(expr, arithmetic.operator, false, true); ok {
			leftValue, leftOK := e.evaluateRowExpression(left, values)
			rightValue, rightOK := e.evaluateRowExpression(right, values)
			if !leftOK || !rightOK {
				return nil, false
			}
			return arithmetic.apply(leftValue, rightValue), true
		}
	}

	if dot := strings.Index(expr, "."); dot > 0 {
		base, ok := e.evaluateRowExpression(strings.TrimSpace(expr[:dot]), values)
		if ok {
			return evaluateRowPropertyChain(base, strings.TrimSpace(expr[dot+1:]))
		}
	}
	value := e.evaluateExpressionFromValues(expr, values)
	if text, ok := value.(string); ok && text == expr && !isWholeCypherQuotedString(expr) {
		return nil, false
	}
	return value, true
}

func isBinaryRowSubtraction(left string) bool {
	left = strings.TrimSpace(left)
	if left == "" {
		return false
	}
	last := left[len(left)-1]
	return !strings.ContainsRune("+-*/%(<>=,", rune(last))
}

func (e *StorageExecutor) evaluateRowQuantifier(expr string, values map[string]interface{}) (interface{}, bool, bool) {
	function, inner, isFunction := parseFunctionCallWS(expr)
	function = strings.ToLower(function)
	if !isFunction || (function != "all" && function != "any" && function != "none" && function != "single") {
		return nil, false, false
	}
	lowerInner := strings.ToLower(inner)
	inIndex := strings.Index(lowerInner, " in ")
	if inIndex <= 0 {
		return nil, true, false
	}
	rest := inner[inIndex+len(" in "):]
	whereIndex := strings.Index(strings.ToLower(rest), " where ")
	if whereIndex < 0 {
		return nil, true, false
	}
	variable := strings.TrimSpace(inner[:inIndex])
	listExpression := strings.TrimSpace(rest[:whereIndex])
	predicate := strings.TrimSpace(rest[whereIndex+len(" where "):])
	if !isValidIdentifier(variable) || listExpression == "" || predicate == "" {
		return nil, true, false
	}
	listValue, ok := e.evaluateRowExpression(listExpression, values)
	if !ok {
		return nil, true, false
	}
	valueType := reflect.TypeOf(listValue)
	if valueType == nil || (valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array) {
		return nil, true, false
	}
	items := toAnySlice(listValue)
	trueCount := 0
	sawNull := false
	for _, item := range items {
		scope := make(map[string]interface{}, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[variable] = item
		result, evaluated := e.evaluateRowExpression(predicate, scope)
		if !evaluated || result == nil {
			sawNull = true
			continue
		}
		boolean, booleanOK := result.(bool)
		if !booleanOK {
			return nil, true, false
		}
		if boolean {
			trueCount++
		}
		switch function {
		case "all":
			if !boolean {
				return false, true, true
			}
		case "any":
			if boolean {
				return true, true, true
			}
		case "none":
			if boolean {
				return false, true, true
			}
		case "single":
			if trueCount > 1 {
				return false, true, true
			}
		}
	}
	if sawNull {
		return nil, true, true
	}
	switch function {
	case "all", "none":
		return true, true, true
	case "any":
		return false, true, true
	default:
		return trueCount == 1, true, true
	}
}

func evaluateRowPropertyChain(value interface{}, chain string) (interface{}, bool) {
	for _, property := range strings.Split(chain, ".") {
		property = strings.TrimSpace(property)
		if property == "" {
			return nil, false
		}
		switch typed := value.(type) {
		case *storage.Node:
			if typed == nil {
				return nil, true
			}
			value = typed.Properties[property]
		case *storage.Edge:
			if typed == nil {
				return nil, true
			}
			value = typed.Properties[property]
		default:
			object, ok := toStringAnyMap(value)
			if !ok {
				return nil, false
			}
			value = object[property]
		}
	}
	return value, true
}

func stripEnclosingExpressionParentheses(expr string) (string, bool) {
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return "", false
	}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(expr); i++ {
		character := expr[i]
		if quote != 0 {
			if character == quote && (i == 0 || expr[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		switch character {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(expr)-1 {
				return "", false
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	if depth != 0 || quote != 0 {
		return "", false
	}
	return strings.TrimSpace(expr[1 : len(expr)-1]), true
}

func evaluateRowBooleanOperator(operator string, left, right interface{}) (interface{}, bool) {
	leftBool, leftIsBool := left.(bool)
	rightBool, rightIsBool := right.(bool)
	if (left != nil && !leftIsBool) || (right != nil && !rightIsBool) {
		return nil, false
	}
	switch operator {
	case "AND":
		if (leftIsBool && !leftBool) || (rightIsBool && !rightBool) {
			return false, true
		}
		if left == nil || right == nil {
			return nil, true
		}
		return leftBool && rightBool, true
	case "OR":
		if (leftIsBool && leftBool) || (rightIsBool && rightBool) {
			return true, true
		}
		if left == nil || right == nil {
			return nil, true
		}
		return false, true
	case "XOR":
		if left == nil || right == nil {
			return nil, true
		}
		return leftBool != rightBool, true
	default:
		return nil, false
	}
}

func rowSubscriptIndex(value interface{}) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, true
	case int64:
		return int(number), true
	case float64:
		integer := int(number)
		return integer, float64(integer) == number
	default:
		return 0, false
	}
}

func (e *StorageExecutor) evaluateRowPredicate(ctx context.Context, expression string, values map[string]interface{}) bool {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false
	}
	if variable, labels, ok := parseWithWhereLabelTest(expression); ok {
		return withWhereNodeHasAllLabels(values[variable], labels)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " AND ", true, false); ok {
		return e.evaluateRowPredicate(ctx, left, values) && e.evaluateRowPredicate(ctx, right, values)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " OR ", true, false); ok {
		return e.evaluateRowPredicate(ctx, left, values) || e.evaluateRowPredicate(ctx, right, values)
	}
	if hasPrefixFoldASCII(expression, "NOT ") {
		return !e.evaluateRowPredicate(ctx, strings.TrimSpace(expression[4:]), values)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " STARTS WITH ", true, true); ok {
		return e.evaluateRowStringPredicate(left, right, values, strings.HasPrefix)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " ENDS WITH ", true, true); ok {
		return e.evaluateRowStringPredicate(left, right, values, strings.HasSuffix)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " CONTAINS ", true, true); ok {
		return e.evaluateRowStringPredicate(left, right, values, strings.Contains)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " IN ", true, true); ok {
		needle, leftOK := e.evaluateRowExpression(left, values)
		haystack, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return false
		}
		for _, item := range toAnySlice(haystack) {
			if e.compareEqual(needle, item) {
				return true
			}
		}
		return false
	}
	for _, operator := range []string{" IS NOT NULL", " IS NULL"} {
		if hasSuffixFoldASCII(expression, strings.ToLower(operator)) {
			left := strings.TrimSpace(expression[:len(expression)-len(operator)])
			value, ok := e.evaluateRowExpression(left, values)
			if operator == " IS NULL" {
				return !ok || value == nil
			}
			return ok && value != nil
		}
	}
	for _, operator := range []string{"<=", ">=", "<>", "!=", "=", "<", ">"} {
		if left, right, ok := splitByOperatorWithOptions(expression, operator, true, true); ok {
			leftValue, leftOK := e.evaluateRowExpression(left, values)
			rightValue, rightOK := e.evaluateRowExpression(right, values)
			if !leftOK || !rightOK {
				return false
			}
			if operator == "!=" {
				operator = "<>"
			}
			return compareWithOperator(leftValue, rightValue, operator)
		}
	}
	value, ok := e.evaluateRowExpression(expression, values)
	return ok && isTruthy(value)
}

func (e *StorageExecutor) evaluateRowStringPredicate(left, right string, values map[string]interface{}, predicate func(string, string) bool) bool {
	leftValue, leftOK := e.evaluateRowExpression(left, values)
	rightValue, rightOK := e.evaluateRowExpression(right, values)
	leftText, leftString := leftValue.(string)
	rightText, rightString := rightValue.(string)
	return leftOK && rightOK && leftString && rightString && predicate(leftText, rightText)
}
