package cypher

import (
	"context"
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
	if value, ok := values[expr]; ok {
		return value, true
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		return value, true
	}
	if isCaseExpression(expr) {
		return e.evaluateRowCaseExpression(expr, values)
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
	if value, matched, ok := e.evaluateRowListComprehension(expr, values); matched {
		return value, ok
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
		if left, right, ok := splitByOperatorWithOptions(expr, operator, true, true); ok {
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
			leftValue, rightValue = normalizeRowIdentityComparison(left, right, leftValue, rightValue)
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
	for _, property := range strings.Split(chain, ".") {
		property = strings.TrimSpace(property)
		if property == "" {
			return nil, false
		}
		// Cypher property access is null-propagating. OPTIONAL MATCH stores an
		// unmatched variable as an untyped nil interface, so handle it before
		// the entity/map type switch just as we handle typed nil entities below.
		if value == nil {
			return nil, true
		}
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
	if left, right, ok := splitByOperatorWithOptions(expression, " OR ", true, true); ok {
		return e.evaluateRowPredicate(ctx, left, values) || e.evaluateRowPredicate(ctx, right, values)
	}
	if left, right, ok := splitByOperatorWithOptions(expression, " AND ", true, true); ok {
		return e.evaluateRowPredicate(ctx, left, values) && e.evaluateRowPredicate(ctx, right, values)
	}
	if hasPrefixFoldASCII(expression, "NOT ") {
		return !e.evaluateRowPredicate(ctx, strings.TrimSpace(expression[4:]), values)
	}
	if matched, recognized := e.evaluateRowExistsPredicate(ctx, expression, values); recognized {
		return matched
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
			leftValue, rightValue = normalizeRowIdentityComparison(left, right, leftValue, rightValue)
			return compareWithOperator(leftValue, rightValue, operator)
		}
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
	needle, leftOK := e.evaluateRowExpression(left, values)
	haystack, rightOK := e.evaluateRowExpression(right, values)
	if !leftOK || !rightOK {
		return false
	}
	identityMembership := isRowIdentityExpression(left)
	if identityMembership {
		needle = rowIdentityPayload(needle)
	}
	for _, item := range toAnySlice(haystack) {
		if identityMembership {
			item = rowIdentityPayload(item)
		}
		if e.compareEqual(needle, item) {
			return true
		}
	}
	return false
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
