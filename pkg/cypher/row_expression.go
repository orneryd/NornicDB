package cypher

import (
	"context"
	"math"
	"reflect"
	"regexp"
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
	if caseEnd := leadingCaseExpressionEnd(expr); caseEnd > 0 && strings.TrimSpace(expr[caseEnd:]) != "" {
		caseValue, ok := e.evaluateRowCaseExpression(strings.TrimSpace(expr[:caseEnd]), values)
		if !ok {
			return nil, false
		}
		const caseBinding = "__nornic_case_value"
		scope := make(map[string]interface{}, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[caseBinding] = caseValue
		return e.evaluateRowExpression(caseBinding+expr[caseEnd:], scope)
	}
	if value, ok := values[expr]; ok {
		return value, true
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		return value, true
	}
	if variable, labels, labelPredicate := parseWithWhereLabelTest(expr); labelPredicate {
		return entityHasAllLabelsOrTypes(values[variable], labels), true
	}
	if isCaseExpression(expr) {
		return e.evaluateRowCaseExpression(expr, values)
	}
	if value, matched, resolved := e.evaluateRowMapProjection(expr, values); matched {
		return value, resolved
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
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
	if value, matched, ok := e.evaluateRowListComprehension(expr, values); matched {
		return value, ok
	}

	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
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
		if value, handled := e.evaluateTemporalConstructor(func(inner string) interface{} {
			value, evaluated := e.evaluateRowExpression(inner, values)
			if !evaluated {
				return nil
			}
			return value
		}, expr); handled {
			if value == nil && strings.EqualFold(function, "date") && strings.TrimSpace(argument) != "" {
				input, resolved := e.evaluateRowExpression(argument, values)
				if resolved && input != nil {
					return nil, false
				}
			}
			return value, true
		}
		if value, matched, resolved := e.evaluateRowMathFunction(function, argument, values); matched {
			return value, resolved
		}
		if value, matched, resolved := e.evaluateRowExtensionFunction(function, argument, values); matched {
			return value, resolved
		}
		switch strings.ToLower(function) {
		case "reduce":
			return e.evaluateRowReduce(argument, values)
		case "coalesce":
			for _, expression := range splitTopLevelComma(argument) {
				value, resolved := e.evaluateRowExpression(strings.TrimSpace(expression), values)
				if !resolved {
					return nil, false
				}
				if value != nil {
					return value, true
				}
			}
			return nil, true
		case "tostring":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			return formatCypherValueString(value), true
		case "length":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			if path, isPath := toStringAnyMap(value); isPath {
				if length, exists := path["length"]; exists {
					switch distance := length.(type) {
					case int:
						return int64(distance), true
					case int64:
						return distance, true
					}
				}
				if relationships, exists := path["relationships"]; exists {
					return int64(len(toAnySlice(relationships))), true
				}
			}
			return nil, false
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
		case "sign":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			number, numeric := toFloat64(value)
			if !numeric {
				return nil, false
			}
			switch {
			case number < 0:
				return int64(-1), true
			case number > 0:
				return int64(1), true
			default:
				return int64(0), true
			}
		case "range":
			parts := e.splitFunctionArgs(argument)
			arguments := make([]interface{}, len(parts))
			for index, part := range parts {
				value, resolved := e.evaluateRowExpression(strings.TrimSpace(part), values)
				if !resolved {
					return nil, false
				}
				arguments[index] = value
			}
			result, err := evaluateCypherRange(arguments)
			return result, err == nil
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
		case "nodes", "relationships":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			path, isPath := toStringAnyMap(value)
			if !isPath {
				return nil, false
			}
			key := "nodes"
			if strings.EqualFold(function, "relationships") {
				key = "rels"
			}
			items, found := path[key]
			if !found {
				return nil, false
			}
			return toAnySlice(items), true
		case "properties":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			switch entity := value.(type) {
			case *storage.Node:
				if entity == nil {
					return nil, true
				}
				return entity.Properties, true
			case *storage.Edge:
				if entity == nil {
					return nil, true
				}
				return entity.Properties, true
			default:
				object, isMap := toStringAnyMap(value)
				return object, isMap
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
			if value == nil {
				return nil, true
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
				} else {
					return nil, false
				}
			}
			result := make([]interface{}, len(labels))
			for index, label := range labels {
				result[index] = label
			}
			return result, true
		case "type":
			value, resolved := e.evaluateRowExpression(argument, values)
			if !resolved {
				return nil, false
			}
			if value == nil {
				return nil, true
			}
			switch relationship := value.(type) {
			case *storage.Edge:
				if relationship == nil {
					return nil, true
				}
				return relationship.Type, true
			default:
				if object, isMap := toStringAnyMap(value); isMap {
					if relationshipType, exists := object["type"].(string); exists {
						return relationshipType, true
					}
				}
				return nil, false
			}
		}
	}

	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		if left, right, ok := splitByOperatorWithOptions(expr, operator, true, true); ok {
			leftValue, leftOK := e.evaluateRowExpression(left, values)
			rightValue, rightOK := e.evaluateRowExpression(right, values)
			if !leftOK || !rightOK {
				return nil, false
			}
			return evaluateRowBooleanOperator(strings.TrimSpace(operator), leftValue, rightValue)
		}
	}

	// NOT binds less tightly than comparisons and postfix predicates. Parsing
	// it before those operators makes the complete remainder its operand, so
	// `NOT a = b`, `NOT a IS NULL`, and `NOT a IN xs` follow Cypher's grammar.
	if len(expr) > len("NOT") && strings.EqualFold(expr[:len("NOT")], "NOT") &&
		(isASCIISpace(expr[len("NOT")]) || expr[len("NOT")] == '(') {
		value, ok := e.evaluateRowExpression(strings.TrimSpace(expr[len("NOT"):]), values)
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

	resolved := true
	result, comparison := evaluateComparisonChain(expr, func(operand string) interface{} {
		value, ok := e.evaluateRowExpression(operand, values)
		if !ok {
			resolved = false
		}
		if isRowIdentityExpression(operand) {
			value = rowIdentityPayload(value)
		}
		return value
	}, compareCypherPredicateValue)
	if comparison {
		if !resolved {
			return nil, false
		}
		return result, true
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

	if left, right, ok := splitByOperatorWithOptions(expr, " NOT IN ", true, true); ok {
		value, evaluated := e.evaluateRowMembershipValue(left, right, values)
		if !evaluated || value == nil {
			return value, evaluated
		}
		return !value.(bool), true
	}
	if left, right, ok := splitByOperatorWithOptions(expr, " IN ", true, true); ok {
		return e.evaluateRowMembershipValue(left, right, values)
	}

	for _, predicate := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{operator: " STARTS WITH ", match: strings.HasPrefix},
		{operator: " ENDS WITH ", match: strings.HasSuffix},
		{operator: " CONTAINS ", match: strings.Contains},
	} {
		left, right, matched := splitByOperatorWithOptions(expr, predicate.operator, true, true)
		if !matched {
			continue
		}
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		if leftValue == nil || rightValue == nil {
			return nil, true
		}
		leftText, leftIsText := leftValue.(string)
		rightText, rightIsText := rightValue.(string)
		if !leftIsText || !rightIsText {
			return nil, true
		}
		return predicate.match(leftText, rightText), true
	}

	if open := strings.LastIndex(expr, "["); open > 0 && strings.HasSuffix(expr, "]") && rowSubscriptReceiverStart(expr, open) == 0 {
		base, ok := e.evaluateRowExpression(expr[:open], values)
		if !ok {
			return nil, false
		}
		subscript := strings.TrimSpace(expr[open+1 : len(expr)-1])
		if rangeIndex := strings.Index(subscript, ".."); rangeIndex >= 0 {
			return e.evaluateRowListSlice(base, strings.TrimSpace(subscript[:rangeIndex]), strings.TrimSpace(subscript[rangeIndex+2:]), values)
		}
		indexValue, ok := e.evaluateRowExpression(subscript, values)
		if !ok {
			return nil, false
		}
		if base == nil || indexValue == nil {
			return nil, true
		}
		if object, isObject := toStringAnyMap(base); isObject {
			key, isString := indexValue.(string)
			if !isString {
				return nil, false
			}
			return object[key], true
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

	if left, right, operator, ok := splitRowArithmeticTier(expr, "+-"); ok {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		if operator == '+' {
			if leftText, ok := leftValue.(string); ok {
				if rightText, ok := rightValue.(string); ok {
					return leftText + rightText, true
				}
			}
			value := e.add(leftValue, rightValue)
			return value, value != nil || leftValue == nil || rightValue == nil
		}
		value := e.subtract(leftValue, rightValue)
		return value, value != nil || leftValue == nil || rightValue == nil
	}

	if left, right, operator, ok := splitRowArithmeticTier(expr, "*/%"); ok {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		switch operator {
		case '*':
			value := e.multiply(leftValue, rightValue)
			return value, value != nil || leftValue == nil || rightValue == nil
		case '/':
			value := e.divide(leftValue, rightValue)
			return value, value != nil || leftValue == nil || rightValue == nil
		default:
			value := e.modulo(leftValue, rightValue)
			return value, value != nil || leftValue == nil || rightValue == nil
		}
	}

	if left, right, _, ok := splitRowArithmeticTier(expr, "^"); ok {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !leftOK || !rightOK {
			return nil, false
		}
		if leftValue == nil || rightValue == nil {
			return nil, true
		}
		base, baseOK := toFloat64(leftValue)
		exponent, exponentOK := toFloat64(rightValue)
		if !baseOK || !exponentOK {
			return nil, false
		}
		return math.Pow(base, exponent), true
	}

	if len(expr) > 1 && (expr[0] == '-' || expr[0] == '+') {
		value, ok := e.evaluateRowExpression(strings.TrimSpace(expr[1:]), values)
		if !ok {
			return nil, false
		}
		if expr[0] == '+' {
			if _, numeric := toFloat64(value); !numeric && value != nil {
				return nil, false
			}
			return value, true
		}
		if value == nil {
			return nil, true
		}
		if _, numeric := toFloat64(value); !numeric {
			return nil, false
		}
		return e.subtract(int64(0), value), true
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

// splitRowArithmeticTier splits at the rightmost top-level operator in one
// precedence tier. Cypher's binary arithmetic operators are left-associative,
// so evaluating the left side recursively preserves source order even when a
// tier contains different operators.
func splitRowArithmeticTier(expr, operators string) (left, right string, operator byte, ok bool) {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	quote := byte(0)
	operatorIndex := -1
	for index := 0; index < len(expr); index++ {
		current := expr[index]
		if quote != 0 {
			if current == quote {
				if index+1 < len(expr) && expr[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"', '`':
			quote = current
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			parenDepth--
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			bracketDepth--
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			braceDepth--
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || !strings.ContainsRune(operators, rune(current)) {
			continue
		}
		if (current == '+' || current == '-') && rowArithmeticSignIsUnary(expr, index) {
			continue
		}
		operatorIndex = index
		operator = current
	}
	if operatorIndex < 0 {
		return "", "", 0, false
	}
	return strings.TrimSpace(expr[:operatorIndex]), strings.TrimSpace(expr[operatorIndex+1:]), operator, true
}

func rowArithmeticSignIsUnary(expr string, index int) bool {
	previous := index - 1
	for previous >= 0 && isASCIIWhitespace(expr[previous]) {
		previous--
	}
	if previous < 0 {
		return true
	}
	if (expr[previous] == 'e' || expr[previous] == 'E') && previous > 0 && index+1 < len(expr) &&
		expr[previous-1] >= '0' && expr[previous-1] <= '9' && expr[index+1] >= '0' && expr[index+1] <= '9' {
		return true
	}
	return strings.ContainsRune("([{,:+-*/%^=<>|", rune(expr[previous]))
}

func compareCypherOrderedValues(left, right interface{}) (int, bool) {
	if comparison, temporal := compareTemporalOrdering(left, right); temporal {
		return comparison, true
	}
	leftNumber, leftIsNumber := strictNumericValue(left)
	rightNumber, rightIsNumber := strictNumericValue(right)
	if leftIsNumber || rightIsNumber {
		if !leftIsNumber || !rightIsNumber {
			return 0, false
		}
		switch {
		case leftNumber < rightNumber:
			return -1, true
		case leftNumber > rightNumber:
			return 1, true
		default:
			return 0, true
		}
	}
	leftText, leftIsText := left.(string)
	rightText, rightIsText := right.(string)
	if leftIsText || rightIsText {
		if !leftIsText || !rightIsText {
			return 0, false
		}
		switch {
		case leftText < rightText:
			return -1, true
		case leftText > rightText:
			return 1, true
		default:
			return 0, true
		}
	}
	leftBoolean, leftIsBoolean := left.(bool)
	rightBoolean, rightIsBoolean := right.(bool)
	if leftIsBoolean || rightIsBoolean {
		if !leftIsBoolean || !rightIsBoolean {
			return 0, false
		}
		switch {
		case leftBoolean == rightBoolean:
			return 0, true
		case !leftBoolean && rightBoolean:
			return -1, true
		default:
			return 1, true
		}
	}
	leftList, leftIsList := cypherListValue(left)
	rightList, rightIsList := cypherListValue(right)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return 0, false
		}
		sharedLength := len(leftList)
		if len(rightList) < sharedLength {
			sharedLength = len(rightList)
		}
		for index := 0; index < sharedLength; index++ {
			equal := cypherEquality(leftList[index], rightList[index])
			if equal == nil {
				return 0, false
			}
			if equal.(bool) {
				continue
			}
			comparison, comparable := compareCypherOrderedValues(leftList[index], rightList[index])
			if !comparable {
				return 0, false
			}
			return comparison, true
		}
		switch {
		case len(leftList) < len(rightList):
			return -1, true
		case len(leftList) > len(rightList):
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

func compareCypherPredicateValue(left, right interface{}, operator string) interface{} {
	if left == nil || right == nil {
		return nil
	}
	if leftNode, ok := left.(*storage.Node); ok {
		rightNode, rightIsNode := right.(*storage.Node)
		if !rightIsNode || rightNode == nil {
			return false
		}
		switch operator {
		case "=":
			return leftNode.ID == rightNode.ID
		case "<>", "!=":
			return leftNode.ID != rightNode.ID
		default:
			return nil
		}
	}
	if leftEdge, ok := left.(*storage.Edge); ok {
		rightEdge, rightIsEdge := right.(*storage.Edge)
		if !rightIsEdge || rightEdge == nil {
			return false
		}
		switch operator {
		case "=":
			return leftEdge.ID == rightEdge.ID
		case "<>", "!=":
			return leftEdge.ID != rightEdge.ID
		default:
			return nil
		}
	}
	if operator != "=" && operator != "<>" && operator != "!=" {
		leftNumber, leftIsNumber := strictNumericValue(left)
		rightNumber, rightIsNumber := strictNumericValue(right)
		if (leftIsNumber && math.IsNaN(leftNumber)) || (rightIsNumber && math.IsNaN(rightNumber)) {
			if leftIsNumber && rightIsNumber {
				return false
			}
			return nil
		}
	}
	if operator == "=" || operator == "<>" || operator == "!=" {
		equal := cypherEquality(left, right)
		matched, known := equal.(bool)
		if !known {
			return nil
		}
		if operator == "=" {
			return matched
		}
		return !matched
	}
	comparison, comparable := compareCypherOrderedValues(left, right)
	if !comparable {
		return nil
	}
	switch operator {
	case "<":
		return comparison < 0
	case ">":
		return comparison > 0
	case "<=":
		return comparison <= 0
	case ">=":
		return comparison >= 0
	default:
		return nil
	}
}

func compareCypherPredicateValues(left, right interface{}, operator string) bool {
	matched, known := compareCypherPredicateValue(left, right, operator).(bool)
	return known && matched
}

func (e *StorageExecutor) evaluateRowListSlice(base interface{}, lowerExpression, upperExpression string, values map[string]interface{}) (interface{}, bool) {
	if base == nil {
		return nil, true
	}
	baseType := reflect.TypeOf(base)
	if baseType == nil || (baseType.Kind() != reflect.Slice && baseType.Kind() != reflect.Array) {
		return nil, false
	}
	items := toAnySlice(base)
	length := len(items)
	lower, upper := 0, length
	if lowerExpression != "" {
		value, ok := e.evaluateRowExpression(lowerExpression, values)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		lower, ok = rowSubscriptIndex(value)
		if !ok {
			return nil, false
		}
	}
	if upperExpression != "" {
		value, ok := e.evaluateRowExpression(upperExpression, values)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		upper, ok = rowSubscriptIndex(value)
		if !ok {
			return nil, false
		}
	}
	if lower < 0 {
		lower += length
	}
	if upper < 0 {
		upper += length
	}
	if lower < 0 {
		lower = 0
	}
	if lower > length {
		lower = length
	}
	if upper < 0 {
		upper = 0
	}
	if upper > length {
		upper = length
	}
	if lower >= upper {
		return []interface{}{}, true
	}
	return append([]interface{}(nil), items[lower:upper]...), true
}

// evaluateRowCaseExpression evaluates CASE against the complete heterogeneous
// row. This keeps CASE semantics available after WITH/UNWIND and in the
// converged pipeline, where bindings are not limited to graph entities.
func (e *StorageExecutor) evaluateRowCaseExpression(expr string, values map[string]interface{}) (interface{}, bool) {
	parsed, err := parseCaseExpression(expr)
	if err != nil {
		return nil, false
	}
	if parsed.isSimple {
		testValue, ok := e.evaluateRowExpression(parsed.testExpression, values)
		if !ok {
			return nil, false
		}
		for _, clause := range parsed.whenClauses {
			whenValue, ok := e.evaluateRowExpression(clause.value, values)
			if !ok {
				return nil, false
			}
			if compareValues(testValue, whenValue) {
				return e.evaluateRowExpression(clause.result, values)
			}
		}
	} else {
		for _, clause := range parsed.whenClauses {
			if e.evaluateRowPredicate(context.Background(), clause.condition, values) {
				return e.evaluateRowExpression(clause.result, values)
			}
		}
	}
	if parsed.elseResult == "" {
		return nil, true
	}
	return e.evaluateRowExpression(parsed.elseResult, values)
}

// evaluateRowListComprehension evaluates a comprehension against typed row
// bindings. The loop value remains a node, relationship, map, path, or scalar;
// it is never converted to query text.
func (e *StorageExecutor) evaluateRowListComprehension(expr string, values map[string]interface{}) (interface{}, bool, bool) {
	if len(expr) < 2 || expr[0] != '[' || expr[len(expr)-1] != ']' {
		return nil, false, false
	}
	variable, listExpression, predicate, projection, matched := parseListComprehension(expr[1 : len(expr)-1])
	if !matched {
		return nil, false, false
	}
	listValue, ok := e.evaluateRowExpression(listExpression, values)
	if !ok {
		return nil, true, false
	}
	if listValue == nil {
		return nil, true, true
	}
	valueType := reflect.TypeOf(listValue)
	if valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array {
		return nil, true, false
	}
	items := toAnySlice(listValue)
	result := make([]interface{}, 0, len(items))
	for _, item := range items {
		scope := make(map[string]interface{}, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[variable] = item
		if predicate != "" {
			condition, evaluated := e.evaluateRowExpression(predicate, scope)
			if !evaluated {
				return nil, true, false
			}
			matches, isBoolean := condition.(bool)
			if condition == nil || (isBoolean && !matches) {
				continue
			}
			if !isBoolean {
				return nil, true, false
			}
		}
		value := item
		if projection != "" {
			var evaluated bool
			value, evaluated = e.evaluateRowExpression(projection, scope)
			if !evaluated {
				return nil, true, false
			}
		}
		result = append(result, value)
	}
	return result, true, true
}

func parseListComprehension(inner string) (variable, listExpression, predicate, projection string, ok bool) {
	inIndex := findListComprehensionToken(inner, " IN ")
	if inIndex <= 0 {
		return "", "", "", "", false
	}
	variable = strings.TrimSpace(inner[:inIndex])
	if !isValidIdentifier(variable) {
		return "", "", "", "", false
	}
	rest := inner[inIndex+len(" IN "):]
	whereIndex := findListComprehensionToken(rest, " WHERE ")
	pipeIndex := findListComprehensionToken(rest, "|")
	endList := len(rest)
	if whereIndex >= 0 && whereIndex < endList {
		endList = whereIndex
	}
	if pipeIndex >= 0 && pipeIndex < endList {
		endList = pipeIndex
	}
	listExpression = strings.TrimSpace(rest[:endList])
	if listExpression == "" {
		return "", "", "", "", false
	}
	if whereIndex >= 0 {
		conditionEnd := len(rest)
		if pipeIndex > whereIndex {
			conditionEnd = pipeIndex
		}
		predicate = strings.TrimSpace(rest[whereIndex+len(" WHERE ") : conditionEnd])
		if predicate == "" {
			return "", "", "", "", false
		}
	}
	if pipeIndex >= 0 {
		projection = strings.TrimSpace(rest[pipeIndex+1:])
		if projection == "" {
			return "", "", "", "", false
		}
	}
	return variable, listExpression, predicate, projection, true
}

func findListComprehensionToken(expression, token string) int {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	var quote rune
	escaped := false
	for index, current := range expression {
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if current == '\\' {
				escaped = true
				continue
			}
			if current == quote {
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"', '`':
			quote = current
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		}
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 && index+len(token) <= len(expression) && strings.EqualFold(expression[index:index+len(token)], token) {
			return index
		}
	}
	return -1
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
	for start := 0; start < len(chain); {
		end := nextRowPropertySeparator(chain, start)
		property := normalizePropertyKey(strings.TrimSpace(chain[start:end]))
		if property == "" {
			return nil, false
		}
		// Cypher property access is null-propagating. OPTIONAL MATCH stores an
		// unmatched variable as an untyped nil interface, so handle it before
		// the entity/map type switch just as we handle typed nil entities below.
		if value == nil {
			return nil, true
		}
		if propertyValue, temporal, supported := evaluateTemporalProperty(value, property); temporal {
			if !supported {
				return nil, false
			}
			value = propertyValue
		} else {
			switch typed := value.(type) {
			case *storage.Node:
				if typed == nil {
					return nil, true
				}
				propertyValue, _ := getNodePropertyValue(typed, property)
				if _, isStringList := propertyValue.([]string); isStringList {
					// Parsed node-property literals can remain []string in an in-memory
					// streaming row while persisted reads expose the same Cypher list
					// as []interface{}. Keep both physical sources observationally equal.
					propertyValue = toAnySlice(propertyValue)
				}
				value = propertyValue
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
		if end == len(chain) {
			break
		}
		start = end + 1
	}
	return value, true
}

// nextRowPropertySeparator finds the next chain dot outside a backtick-
// delimited identifier. Doubled backticks escape a literal backtick and do not
// close the identifier. Scanning avoids splitting and reallocating the entire
// chain for each row in the streaming executor.
func nextRowPropertySeparator(chain string, start int) int {
	delimited := false
	for index := start; index < len(chain); index++ {
		switch chain[index] {
		case '`':
			if delimited && index+1 < len(chain) && chain[index+1] == '`' {
				index++
				continue
			}
			delimited = !delimited
		case '.':
			if !delimited {
				return index
			}
		}
	}
	return len(chain)
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
			if character == quote && !isBackslashEscaped(expr, i) {
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

func stripEnclosingRowDelimiter(expr string, open, close byte) (string, bool) {
	if len(expr) < 2 || expr[0] != open || expr[len(expr)-1] != close {
		return "", false
	}
	depth := 0
	quote := byte(0)
	for index := 0; index < len(expr); index++ {
		current := expr[index]
		if quote != 0 {
			if current == quote {
				if index+1 < len(expr) && expr[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' || current == '`' {
			quote = current
			continue
		}
		switch current {
		case open:
			depth++
		case close:
			depth--
			if depth == 0 && index != len(expr)-1 {
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
		return entityHasAllLabelsOrTypesPredicate(values[variable], labels)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " OR ", true, true); ok {
		return e.evaluateRowPredicate(ctx, left, values) || e.evaluateRowPredicate(ctx, right, values)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " AND ", true, true); ok {
		return e.evaluateRowPredicate(ctx, left, values) && e.evaluateRowPredicate(ctx, right, values)
	}
	// EXISTS and NOT EXISTS are complete predicates. Resolve both before the
	// generic NOT operator so a subquery is evaluated against its correlated
	// typed row bindings rather than being treated as a scalar expression.
	if matched, recognized := e.evaluateRowExistsPredicate(ctx, expression, values); recognized {
		return matched
	}
	if hasPrefixFoldASCII(expression, "NOT ") {
		inner := strings.TrimSpace(expression[4:])
		if value, resolved := e.evaluateRowExpression(inner, values); resolved {
			if value == nil {
				return false
			}
			boolean, isBoolean := value.(bool)
			return isBoolean && !boolean
		}
		return !e.evaluateRowPredicate(ctx, inner, values)
	}
	if matched, recognized := e.evaluateRowCountSubqueryPredicate(expression, values); recognized {
		return matched
	}
	if nodeCtx, _ := withWhereValueContext(values); len(nodeCtx) > 0 && looksLikeRowRelationshipPattern(expression) {
		if matches, recognized := e.evaluateBoundRelationshipPattern(ctx, expression, nodeCtx); recognized {
			return matches
		}
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
	if left, right, ok := splitByOperatorWithOptions(expression, "=~", false, true); ok {
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		text, textOK := leftValue.(string)
		pattern, patternOK := rightValue.(string)
		if !leftOK || !rightOK || !textOK || !patternOK {
			return false
		}
		matched, err := regexp.MatchString(pattern, text)
		return err == nil && matched
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " NOT IN ", true, true); ok {
		return !e.evaluateRowMembership(left, right, values)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " IN ", true, true); ok {
		return e.evaluateRowMembership(left, right, values)
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
	resolved := true
	comparisonResult, comparison := evaluateComparisonChain(expression, func(operand string) interface{} {
		value, ok := e.evaluateRowExpression(operand, values)
		if !ok {
			resolved = false
		}
		if isRowIdentityExpression(operand) {
			value = rowIdentityPayload(value)
		}
		return value
	}, compareCypherPredicateValue)
	if comparison {
		matched, known := comparisonResult.(bool)
		return resolved && known && matched
	}
	value, ok := e.evaluateRowExpression(expression, values)
	return ok && isTruthy(value)
}

func normalizeRowIdentityComparison(leftExpr, rightExpr string, leftValue, rightValue interface{}) (interface{}, interface{}) {
	if !isRowIdentityExpression(leftExpr) && !isRowIdentityExpression(rightExpr) {
		return leftValue, rightValue
	}
	return rowIdentityPayload(leftValue), rowIdentityPayload(rightValue)
}

func isRowIdentityExpression(expression string) bool {
	name, _, ok := parseFunctionCallWS(strings.TrimSpace(expression))
	return ok && (strings.EqualFold(name, "id") || strings.EqualFold(name, "elementId"))
}

func rowIdentityPayload(value interface{}) interface{} {
	text, ok := value.(string)
	if !ok {
		return value
	}
	parts := strings.SplitN(text, ":", 3)
	if len(parts) == 3 && (parts[0] == "4" || parts[0] == "5") {
		return parts[2]
	}
	return text
}

func looksLikeRowRelationshipPattern(expression string) bool {
	return strings.Contains(expression, "-[") || strings.Contains(expression, "]-") ||
		strings.Contains(expression, "--") || strings.Contains(expression, "<-") || strings.Contains(expression, "->")
}

func (e *StorageExecutor) evaluateRowMembership(left, right string, values map[string]interface{}) bool {
	value, ok := e.evaluateRowMembershipValue(left, right, values)
	return ok && value == true
}

func (e *StorageExecutor) evaluateRowMembershipValue(left, right string, values map[string]interface{}) (interface{}, bool) {
	needle, leftOK := e.evaluateRowExpression(left, values)
	haystack, rightOK := e.evaluateRowExpression(right, values)
	if !leftOK || !rightOK {
		return nil, false
	}
	if haystack == nil {
		return nil, true
	}
	haystackType := reflect.TypeOf(haystack)
	if haystackType == nil || (haystackType.Kind() != reflect.Slice && haystackType.Kind() != reflect.Array) {
		return nil, false
	}
	items := toAnySlice(haystack)
	if len(items) == 0 {
		return false, true
	}
	if needle == nil {
		return nil, true
	}
	identityMembership := isRowIdentityExpression(left)
	if identityMembership {
		needle = rowIdentityPayload(needle)
	}
	containsUnknown := false
	for _, item := range items {
		if identityMembership {
			item = rowIdentityPayload(item)
		}
		equal := cypherEquality(needle, item)
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

func (e *StorageExecutor) evaluateRowExistsPredicate(ctx context.Context, expression string, values map[string]interface{}) (bool, bool) {
	trimmed := strings.TrimSpace(expression)
	negated := hasPrefixFold(trimmed, "NOT EXISTS")
	if !negated && !hasPrefixFold(trimmed, "EXISTS") {
		return false, false
	}
	prefix := "EXISTS"
	if negated {
		prefix = "NOT EXISTS"
	}
	subquery := e.extractSubquery(trimmed, prefix)
	if subquery == "" {
		return false, false
	}
	if clauses, ok := splitPipelineClauses(subquery); ok && len(clauses) > 1 {
		correlated := e.cloneWithStorage(e.getStorage(ctx))
		correlated.fabricRecordBindings = make(map[string]interface{}, len(e.fabricRecordBindings)+len(values))
		for name, value := range e.fabricRecordBindings {
			correlated.fabricRecordBindings[name] = value
		}
		for name, value := range values {
			correlated.fabricRecordBindings[name] = value
		}
		result, handled, err := correlated.executePipeline(ctx, subquery)
		matched := err == nil && handled && result != nil && len(result.Rows) > 0
		if negated {
			matched = !matched
		}
		return matched, true
	}
	if !hasPrefixFold(strings.TrimSpace(subquery), "MATCH ") {
		subquery = "MATCH " + strings.TrimSpace(subquery)
	}
	path := PathContext{nodes: make(map[string]*storage.Node), rels: make(map[string]*storage.Edge)}
	for name, value := range values {
		switch entity := value.(type) {
		case *storage.Node:
			if entity != nil {
				path.nodes[name] = entity
			}
		case *storage.Edge:
			if entity != nil {
				path.rels[name] = entity
			}
		}
	}
	matched := e.pathSubqueryMatches(ctx, path, subquery)
	if negated {
		matched = !matched
	}
	return matched, true
}

func (e *StorageExecutor) evaluateRowCountSubqueryPredicate(expression string, values map[string]interface{}) (bool, bool) {
	if !hasPrefixFold(strings.TrimSpace(expression), "COUNT") || !hasSubqueryPattern(expression, countSubqueryRe) {
		return false, false
	}
	for variable, value := range values {
		node, ok := value.(*storage.Node)
		if !ok || node == nil {
			continue
		}
		subquery := e.extractSubquery(expression, "COUNT")
		if strings.Contains(subquery, "("+variable+")") || strings.Contains(subquery, "("+variable+":") {
			return e.evaluateCountSubqueryComparison(node, variable, expression), true
		}
	}
	return false, true
}

func (e *StorageExecutor) evaluateRowStringPredicate(left, right string, values map[string]interface{}, predicate func(string, string) bool) bool {
	leftValue, leftOK := e.evaluateRowExpression(left, values)
	rightValue, rightOK := e.evaluateRowExpression(right, values)
	leftText, leftString := leftValue.(string)
	rightText, rightString := rightValue.(string)
	return leftOK && rightOK && leftString && rightString && predicate(leftText, rightText)
}
