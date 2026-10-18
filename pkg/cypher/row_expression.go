package cypher

import (
	"context"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateRowExpression resolves an expression against a heterogeneous Cypher
// row. Unlike the graph-only evaluator, a row may also contain scalar, map,
// and list bindings introduced by WITH or UNWIND.

// rowArithmeticResult is the row evaluator's result of left op right, given
// the value helper's result: the value, null for a null operand, the
// statement error (INTEGER division by zero, INTEGER overflow, an operand
// type the operator doesn't take), or unresolved when none applies.
func rowArithmeticResult(op byte, value, left, right interface{}) (interface{}, bool, error) {
	if value != nil {
		return value, true, nil
	}
	if divisionByZero(op, left, right) {
		return nil, false, divisionByZeroError()
	}
	if left == nil || right == nil {
		return nil, true, nil
	}
	if err := arithmeticError(op, left, right); err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

// rowTypeOf adapts the shared type owner to the row evaluator's error contract.
func rowTypeOf(value interface{}) (interface{}, bool, error) {
	result, err := cypherfn.RelationshipType(value)
	if err != nil {
		return nil, false, functionEvaluationError(err)
	}
	return result, true, nil
}

func (e *StorageExecutor) evaluateRowArithmeticValues(operator byte, left, right interface{}) (interface{}, bool, error) {
	return e.evaluateRowArithmeticValuesWithCancellation(operator, left, right, nil)
}

func (e *StorageExecutor) evaluateRowArithmeticValuesWithCancellation(operator byte, left, right interface{}, probe *pipelineCancellation) (interface{}, bool, error) {
	switch operator {
	case '+':
		value := e.addWithCancellation(left, right, probe)
		if probe != nil && probe.err != nil {
			return nil, false, probe.err
		}
		return rowArithmeticResult(operator, value, left, right)
	case '-':
		return rowArithmeticResult(operator, e.subtract(left, right), left, right)
	case '*':
		return rowArithmeticResult(operator, e.multiply(left, right), left, right)
	case '/':
		return rowArithmeticResult(operator, e.divide(left, right), left, right)
	case '%':
		return rowArithmeticResult(operator, e.modulo(left, right), left, right)
	case '^':
		if left == nil || right == nil {
			return nil, true, nil
		}
		base, baseOK := toFloat64(left)
		exponent, exponentOK := toFloat64(right)
		if !baseOK || !exponentOK {
			if err := arithmeticError(operator, left, right); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
		return math.Pow(base, exponent), true, nil
	}
	return nil, false, nil
}

// evaluateRowValue evaluates expr against a row's values. It has three
// outcomes: a value (ok); ok false with a nil error when the row evaluator
// doesn't handle the expression; and an error, the statement's error for an
// expression it handles (a division by zero, a type error, a function's
// argument error). Nothing in between turns an error into a value.
//
// evaluateRowValueWithCancellation is evaluateRowValue (the compiled
// rowValueProgram, or the text) checked against probe: a canceled statement
// stops before the expression and wins over its result.
func (e *StorageExecutor) evaluateRowValueWithCancellation(expr string, values map[string]interface{}, probe *pipelineCancellation) (interface{}, bool, error) {
	if probe == nil {
		return e.evaluateRowValueProbed(expr, values, probe)
	}
	if !probe.check() {
		return nil, false, probe.err
	}
	value, resolved, err := e.evaluateRowValueProbed(expr, values, probe)
	if probe.err != nil {
		return nil, false, probe.err
	}
	return value, resolved, err
}

// evaluateRowValueText is evaluateRowValue read from the text: one level of
// the expression, its parts evaluated through evaluateRowValue. A
// rowValueProgram keeps the decisions it makes about a text and calls it
// for the forms it doesn't compile.
func (e *StorageExecutor) evaluateRowValueText(expr string, values map[string]interface{}) (interface{}, bool, error) {
	return e.evaluateRowValueTextWithCancellation(expr, values, rowPipelineCancellation(values))
}

func (e *StorageExecutor) evaluateRowValueTextWithCancellation(expr string, values map[string]interface{}, probe *pipelineCancellation) (resultValue interface{}, resultResolved bool, resultErr error) {
	if probe != nil {
		if !probe.check() {
			return nil, false, probe.err
		}
		defer func() {
			if probe.err != nil {
				resultValue, resultResolved, resultErr = nil, false, probe.err
			}
		}()
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false, nil
	}
	if inner, ok := stripEnclosingExpressionParentheses(expr); ok {
		return e.evaluateRowValueWithCancellation(inner, values, probe)
	}
	if caseEnd := leadingCaseExpressionEnd(expr); caseEnd > 0 && strings.TrimSpace(expr[caseEnd:]) != "" && !hasTopLevelLogicalOperator(expr) {
		caseValue, ok, err := e.evaluateRowCaseExpressionWithCancellation(strings.TrimSpace(expr[:caseEnd]), values, probe)
		if err != nil || !ok {
			return nil, false, err
		}
		const caseBinding = "__nornic_case_value"
		scope := make(map[string]interface{}, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[caseBinding] = caseValue
		return e.evaluateRowValueWithCancellation(caseBinding+expr[caseEnd:], scope, probe)
	}
	if value, ok := values[expr]; ok && !isLiteralKeyword(expr) {
		return value, true, nil
	}
	// Relationship arrows contain comparison and subtraction characters.
	// Resolve the complete pattern before scalar operator parsing sees them.
	if isRelationshipPatternOperand(expr) {
		if matched, recognized := e.rowPatternPredicate(rowStatementContextOrBackground(values), expr, values); recognized {
			return matched, true, nil
		}
	}
	// A plain property chain on a row variable (e.uuid, n.a.b) can't match
	// any branch below but the property access at the end: resolve it there
	// directly instead of scanning for literals, operators and subscripts.
	if variable, chain, ok := rowPropertyChainShape(expr); ok {
		if base, bound := values[variable]; bound {
			value, ok := evaluateRowPropertyChain(e.queryVisibleValue(base), chain)
			if !ok {
				return nil, false, rowPropertyChainTypeError(base, chain)
			}
			return value, true, nil
		}
	}
	// A backtick-quoted variable (`my x`) names the same binding as its
	// unquoted form, which is how projection aliases are keyed.
	if name := normalizeProjectionColumnName(expr); name != expr {
		if value, ok := values[name]; ok {
			return value, true, nil
		}
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		return value, true, nil
	}
	if value, matched, err := e.evaluateRowSubqueryAtom(expr, values); matched {
		return value, err == nil, err
	}
	if value, matched, err := e.evaluateRowPatternComprehensionAtom(expr, values); matched {
		return value, err == nil, err
	}
	if variable, labels, labelPredicate := parseWithWhereLabelTest(expr); labelPredicate {
		return entityHasAllLabelsOrTypes(values[variable], labels), true, nil
	}
	if isCaseExpression(expr) {
		return e.evaluateRowCaseExpressionWithCancellation(expr, values, probe)
	}
	if value, matched, resolved, err := e.evaluateRowMapProjectionWithCancellation(expr, values, probe); matched {
		return value, resolved, err
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		result := make(map[string]interface{})
		keys := make([]string, 0)
		if inner == "" {
			return result, true, nil
		}
		for _, pair := range splitTopLevelComma(inner) {
			if probe != nil && !probe.check() {
				return nil, false, probe.err
			}
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				return nil, false, nil
			}
			key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
			value, ok, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(pair[separator+1:]), values, probe)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			if _, exists := result[key]; !exists {
				keys = appendPipelineItem(keys, key, probe)
			}
			result[key] = value
		}
		if failure, ok := values["\x00mapKeyOrders"].(*expressionFailure); ok {
			failure.recordMapKeyOrder(result, keys)
		}
		return result, true, nil
	}
	if value, matched, ok, err := e.evaluateRowListComprehension(expr, values, probe); matched {
		return value, ok, err
	}

	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true, nil
		}
		items := splitTopLevelComma(inner)
		if probe != nil && probe.err != nil {
			return nil, false, probe.err
		}
		return rowListLiteral(len(items), func(i int) (interface{}, bool, error) {
			if probe != nil && !probe.check() {
				return nil, false, probe.err
			}
			return e.evaluateRowValueWithCancellation(items[i], values, probe)
		})
	}

	if value, matched, ok, err := e.evaluateRowQuantifier(expr, values, func(text string, scope map[string]interface{}) (interface{}, bool, error) {
		return e.evaluateRowValueWithCancellation(text, scope, probe)
	}, probe); matched {
		return value, ok, err
	}

	if function, argument, ok := parseFunctionCallWS(expr); ok {
		var argumentErr error
		temporalContext, _ := values[rowStatementContextKey].(context.Context)
		if temporalContext == nil {
			temporalContext = probeStatementContext(probe)
		}
		if value, handled, callErr := e.evaluateTemporalConstructor(temporalContext, func(inner string) interface{} {
			value, evaluated, err := e.evaluateRowValueWithCancellation(inner, values, probe)
			if err != nil && argumentErr == nil {
				argumentErr = err
			}
			if err != nil || !evaluated {
				return nil
			}
			return value
		}, expr); handled {
			if argumentErr != nil {
				return nil, false, argumentErr
			}
			if callErr != nil {
				return nil, false, callErr
			}
			if value == nil && isTemporalConstructor(function) && strings.TrimSpace(argument) != "" {
				input, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
				if err != nil {
					return nil, false, err
				}
				if resolved && input != nil {
					return nil, false, temporalConstructorError(function, input)
				}
			}
			return value, true, nil
		}
		if value, matched, resolved, err := e.evaluateRowMathFunction(function, argument, values); matched {
			return value, resolved, err
		}
		if value, matched, resolved, err := e.evaluateRowExtensionFunction(function, argument, values); matched {
			return value, resolved, err
		}
		if isGDSFunctionName(lowerASCII(function)) {
			return e.evaluateRowGraphFunction(function, argument, values)
		}
		switch lowerASCII(function) {
		case "exists":
			if isRelationshipPatternOperand(strings.TrimSpace(argument)) {
				matched, recognized := e.rowPatternPredicate(rowStatementContextOrBackground(values), argument, values)
				return matched, recognized, nil
			}
		case "graph.names", "graph.propertiesbyname", "graph.byname", "graph.byelementid":
			return e.evaluateRowGraphFunction(function, argument, values)
		case "shortestpath", "allshortestpaths":
			nodes := make(map[string]*storage.Node, len(values))
			for name, value := range values {
				if node, ok := value.(*storage.Node); ok {
					nodes[name] = node
				}
			}
			search, _ := values[rowStatementContextKey].(context.Context)
			if search == nil {
				search = probeStatementContext(probe)
			}
			value, handled, err := e.evaluateShortestPathValue(search, function, argument, nodes)
			return value, handled, err
		case "startnode", "endnode":
			if value, handled, err := e.rowRelationshipEndpoint(function, argument, values); handled || err != nil {
				return value, handled, err
			}
		case "reduce", "allreduce":
			return e.evaluateRowReduce(function, argument, values, e.evaluateRowValue)
		case "coalesce":
			// EXISTS subqueries are non-null booleans and end coalesce
			// immediately. An argument is evaluated only when reached, its
			// subqueries too (evaluateRowLocalSubqueries).
			operands := splitTopLevelComma(argument)
			if probe != nil && probe.err != nil {
				return nil, false, probe.err
			}
			return cypherfn.Coalesce(len(operands), func(index int) (interface{}, bool, error) {
				if probe != nil && !probe.check() {
					return nil, false, probe.err
				}
				trimmed := strings.TrimSpace(operands[index])
				return evaluateRowLocalSubqueries(values, trimmed, func(values map[string]interface{}) (interface{}, bool, error) {
					if matched, recognized := e.evaluateRowExistsPredicate(rowStatementContextOrBackground(values), trimmed, values); recognized {
						return matched, true, nil
					}
					return e.evaluateRowValueWithCancellation(trimmed, values, probe)
				})
			})
		case "length":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			if value == nil || nilRemotePath(value) {
				return nil, true, nil
			}
			if remote, ok := value.(storage.RemotePathValue); ok {
				result, err := remotePathFunctionValue("length", remote)
				return result, err == nil, err
			}
			object, isMap := toStringAnyMapWithCancellation(value, probe)
			if path, ok := cypherPathResultOf(value, object, isMap); ok {
				return int64(path.Length), true, nil
			}
			return nil, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
				localization.CypherCoreFunctionArgumentInvalid("length", "a Path", neo4jValueRepr(value)))
		case "range":
			parts := e.splitFunctionArgs(argument)
			arguments := make([]interface{}, len(parts))
			for index, part := range parts {
				value, resolved, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(part), values, probe)
				if err != nil {
					return nil, false, err
				}
				if !resolved {
					return nil, false, nil
				}
				arguments[index] = value
			}
			budget := rowExpressionBudget(values)
			result, err := evaluateCypherRangeWithCancellation(&budget, arguments, probe)
			if err != nil {
				return nil, false, err
			}
			return result, true, nil
		case "head", "last", "reverse", "size":
			if equalFoldASCII(function, "size") {
				argument = sizeArgumentExpression(argument)
			}
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			if strings.EqualFold(function, "size") {
				return evaluateCypherSizeWithCancellation(value, probe)
			}
			result, err := evaluateListAccessFunctionWithCancellation(lowerASCII(function), value, probe)
			if err != nil {
				return nil, false, err
			}
			return result, true, nil
		case "nodes", "relationships":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			if value == nil || nilRemotePath(value) {
				return nil, true, nil
			}
			if remote, ok := value.(storage.RemotePathValue); ok {
				result, err := remotePathFunctionValue(function, remote)
				return result, err == nil, err
			}
			path, isPath := toStringAnyMapWithCancellation(value, probe)
			if !isPath {
				return nil, false, nil
			}
			nodes, relationships, hasNodes, hasRelationships := pathValueParts(path)
			if strings.EqualFold(function, "relationships") {
				return relationships, hasRelationships, nil
			}
			return nodes, hasNodes, nil
		case "properties":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			if value == nil {
				return nil, true, nil
			}
			switch entity := value.(type) {
			case *storage.Node:
				if entity == nil {
					return nil, true, nil
				}
				return cypherfn.EntityProperties(e.queryEntityProperties(entity)), true, nil
			case *storage.Edge:
				if entity == nil {
					return nil, true, nil
				}
				return cypherfn.EntityProperties(e.queryEntityProperties(entity)), true, nil
			case storage.RemoteEntityValue:
				return entity["properties"], true, nil
			default:
				object, isMap := toStringAnyMapWithCancellation(value, probe)
				return object, isMap, nil
			}
		case "keys":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			switch entity := value.(type) {
			case nil:
				return nil, true, nil
			case *storage.Node:
				if entity == nil {
					return nil, true, nil
				}
			case *storage.Edge:
				if entity == nil {
					return nil, true, nil
				}
			}
			if object, isMap := toStringAnyMapWithCancellation(value, probe); isMap {
				value = object
			}
			object, isMap := toStringAnyMapWithCancellation(value, probe)
			if _, isPath := cypherPathResultOf(value, object, isMap); isPath {
				return nil, false, invalidFunctionArgument("keys", value)
			}
			keys, ok := cypherfn.PropertyKeysWithCancellation(e.queryVisibleValue(value), functionCancellation(probe))
			if probe != nil && probe.err != nil {
				return nil, false, probe.err
			}
			if !ok {
				return nil, false, invalidFunctionArgument("keys", value)
			}
			return keys, true, nil
		case "labels":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			result, err := cypherfn.Labels(value)
			if err != nil {
				return nil, false, functionEvaluationError(err)
			}
			return result, true, nil
		case "type":
			value, resolved, err := e.evaluateRowValueWithCancellation(argument, values, probe)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			return rowTypeOf(value)
		}
	}

	// OR, XOR, AND and NOT (evaluateLogicalExpression). NOT binds less
	// tightly than comparisons and postfix predicates, so NOT a = b, NOT a
	// IS NULL and NOT a IN xs negate the whole remainder.
	if value, logical, ok, err := evaluateLogicalExpressionInContext(rowStatementContextOrBackground(values), expr, func(operand string, position foldPosition) (interface{}, bool, error) {
		return e.rowTextFoldOperand(rowStatementContextOrBackground(values), operand, position, values, false, probe)
	}); logical {
		if err != nil || !ok {
			return nil, false, err
		}
		return value, true, nil
	}

	// =~ raises a type or pattern error, which the comparison chain can't.
	if left, right, regex := splitByOperatorOutsideCase(expr, "=~", false, true); regex {
		leftValue, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		if leftValue == nil || rightValue == nil {
			return nil, true, nil
		}
		matched, err := cypherRegexMatch(leftValue, rightValue)
		if err != nil {
			return nil, false, err
		}
		return matched, true, nil
	}
	result, comparison, resolved, err := evaluateComparisonChainInContext(rowStatementContextOrBackground(values), expr, func(operand string, position foldPosition) (interface{}, bool, error) {
		return e.rowTextFoldOperand(rowStatementContextOrBackground(values), operand, position, values, false, probe)
	}, func(left, right interface{}, operator string) interface{} {
		return compareCypherPredicateValueWithCancellation(left, right, operator, probe)
	}, rowIsCypher25(values))
	if comparison {
		if probe != nil && probe.err != nil {
			return nil, false, probe.err
		}
		return result, resolved, err
	}
	if operand, negated, spec, isTypePredicate, err := splitTypePredicate(expr); isTypePredicate {
		if err != nil {
			return nil, false, err
		}
		value, ok, err := e.evaluateRowValueWithCancellation(operand, values, probe)
		if err != nil || !ok {
			return nil, false, err
		}
		return evaluateTypePredicate(value, negated, spec), true, nil
	}
	if operand, negated, form, isNormalization := splitNormalizationPredicate(expr); isNormalization {
		value, ok, err := e.evaluateRowValueWithCancellation(operand, values, probe)
		if err != nil || !ok {
			return nil, false, err
		}
		return evaluateNormalizationPredicate(value, negated, form), true, nil
	}
	for _, predicate := range []struct {
		suffix  string
		notNull bool
	}{
		{suffix: " IS NOT NULL", notNull: true},
		{suffix: " IS NULL"},
	} {
		if hasSuffixFoldASCII(expr, predicate.suffix) {
			value, ok, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(expr[:len(expr)-len(predicate.suffix)]), values, probe)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			return nullEvaluationHandler(predicate.notNull).evaluate(value), true, nil
		}
	}

	if left, right, ok := splitByOperatorOutsideCase(expr, " NOT IN ", true, true); ok {
		value, evaluated, err := e.evaluateRowMembershipValueWithCancellation(left, right, values, probe)
		if err != nil || !evaluated || value == nil {
			return value, evaluated, err
		}
		return !value.(bool), true, nil
	}
	if left, right, ok := splitByOperatorOutsideCase(expr, " IN ", true, true); ok {
		return e.evaluateRowMembershipValueWithCancellation(left, right, values, probe)
	}

	for _, predicate := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{operator: " STARTS WITH ", match: strings.HasPrefix},
		{operator: " ENDS WITH ", match: strings.HasSuffix},
		{operator: " CONTAINS ", match: strings.Contains},
	} {
		left, right, matched := splitByOperatorOutsideCase(expr, predicate.operator, true, true)
		if !matched {
			continue
		}
		leftValue, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return rowStringPredicateValue(leftValue, rightValue, predicate.match), true, nil
	}

	// The subscript is the bracket group that closes expr (rowSubscriptStart):
	// its key may hold brackets of its own (m[r[k]], m[l[0].id]).
	if open := rowSubscriptStart(expr); open > 0 {
		return e.evaluateCypherSubscript(expr[:open], strings.TrimSpace(expr[open+1:len(expr)-1]), func(part string) (interface{}, bool, error) {
			return e.evaluateRowValueWithCancellation(part, values, probe)
		}, probe)
	}

	if left, right, operator, ok := splitRowArithmeticTier(expr, "+-"); ok {
		leftValue, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return e.evaluateRowArithmeticValuesWithCancellation(operator, leftValue, rightValue, probe)
	}

	if left, right, operator, ok := splitRowArithmeticTier(expr, "*/%"); ok {
		leftValue, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		if operator == '/' {
			if value, folded := foldedDivisionByZero(left, right, leftValue, rightValue); folded {
				return value, true, nil
			}
		}
		return e.evaluateRowArithmeticValuesWithCancellation(operator, leftValue, rightValue, probe)
	}

	if left, right, _, ok := splitRowArithmeticTier(expr, "^"); ok {
		leftValue, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		if leftValue == nil || rightValue == nil {
			return nil, true, nil
		}
		base, baseOK := toFloat64(leftValue)
		exponent, exponentOK := toFloat64(rightValue)
		if !baseOK || !exponentOK {
			if err := arithmeticError('^', leftValue, rightValue); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
		return math.Pow(base, exponent), true, nil
	}

	if len(expr) > 1 && (expr[0] == '-' || expr[0] == '+') {
		value, ok, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(expr[1:]), values, probe)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		if expr[0] == '+' {
			// Unary plus returns its operand unchanged, as in Neo4j: an
			// operand typed other than a number, temporal value or duration
			// is rejected before running (static_operator_types.go), and at
			// run time any value passes through (UNWIND [3, '3'] AS x
			// RETURN +x is 3 and '3').
			return value, true, nil
		}
		if value == nil {
			return nil, true, nil
		}
		if _, numeric := toFloat64(value); !numeric {
			// -'a' is Neo4j's TypeError; a duration is negated elsewhere.
			if err := unaryMinusTypeError(value); err != nil {
				return nil, false, err
			}
			return nil, false, nil
		}
		// -n of the most negative integer is Neo4j's long overflow, not null
		// (#893).
		negated, handled, err := numericArithmetic('-', int64(0), value)
		if err != nil {
			return nil, false, err
		}
		if handled {
			return negated, true, nil
		}
		return e.subtract(int64(0), value), true, nil
	}

	// Use the last top-level dot so qualified calls remain intact as the base.
	// A whole qualified call (text.locate(…)) is a function call, not a
	// property of its first name, even when a variable has that name, as in
	// Neo4j.
	if dot := topLevelSymbolIndex(expr, "."); dot > 0 && !isQualifiedFunctionCall(expr) {
		for {
			next := topLevelSymbolIndex(expr[dot+1:], ".")
			if next < 0 {
				break
			}
			dot += next + 1
		}
		base, ok, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(expr[:dot]), values, probe)
		if err != nil {
			return nil, false, err
		}
		if ok {
			chain := strings.TrimSpace(expr[dot+1:])
			value, ok := evaluateRowPropertyChain(e.queryVisibleValue(base), chain)
			if !ok {
				if err := rowPropertyChainTypeError(base, chain); err != nil {
					return nil, false, err
				}
			}
			return value, ok, nil
		}
	}
	return e.rowValueFallbackProbed(expr, values, probe)
}

// splitRowArithmeticTier splits at the rightmost top-level operator in one
// precedence tier. Cypher's binary arithmetic operators are left-associative,
// so evaluating the left side recursively preserves source order even when a
// tier contains different operators.
func splitRowArithmeticTier(expr, operators string) (left, right string, operator byte, ok bool) {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	var operatorMask [4]uint64
	for _, candidate := range operators {
		if candidate < 256 {
			operatorMask[candidate>>6] |= uint64(1) << (candidate & 63)
		}
	}
	operatorIndex := -1
	for index := 0; index < len(expr); index++ {
		current := expr[index]
		if current == '/' {
			if end := queryCommentEnd(expr, index); end >= 0 {
				index = end - 1
				continue
			}
		}
		switch current {
		case '\'', '"', '`':
			index = skipCypherQuotedText(expr, index, current) - 1
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
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || operatorMask[current>>6]&(uint64(1)<<(current&63)) == 0 {
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

// compareCypherOrderedValues orders two values for the comparison operators
// (comparability): comparable is false for values that have no order between
// them (different kinds, durations, points, null inside a list or map).
func compareCypherOrderedValues(left, right interface{}) (int, bool) {
	return compareCypherOrderedValuesWithCancellation(left, right, nil)
}

func compareCypherOrderedValuesWithCancellation(left, right interface{}, probe *pipelineCancellation) (int, bool) {
	if comparison, temporal := compareTemporalOrdering(left, right); temporal {
		return comparison, true
	}
	if comparison, exact := compareCypherNumbersExactly(left, right); exact {
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
		return comparePipelineStrings(leftText, rightText, probe), true
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
	leftList, leftIsList := cypherListValueWithCancellation(left, probe)
	rightList, rightIsList := cypherListValueWithCancellation(right, probe)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return 0, false
		}
		sharedLength := len(leftList)
		if len(rightList) < sharedLength {
			sharedLength = len(rightList)
		}
		for index := 0; index < sharedLength; index++ {
			comparison, comparable := compareCypherComparableValuesWithCancellation(leftList[index], rightList[index], probe)
			if probe != nil && probe.err != nil {
				return 0, false
			}
			if !comparable || comparison != 0 {
				return comparison, comparable
			}
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
	// ORDER BY distinguishes optional identity metadata for a total order;
	// relational predicates still recognize the same remote entity. Element
	// identity remains authoritative even when optional numeric metadata differs.
	if leftRemote, leftIsRemote := left.(storage.RemoteEntityValue); leftIsRemote {
		if rightRemote, rightIsRemote := right.(storage.RemoteEntityValue); rightIsRemote && cypherEquality(leftRemote, rightRemote) == true {
			return 0, true
		}
	}
	// Maps, nodes, relationships and paths, after the scalar kinds above.
	if comparison, comparable, structured := compareCypherStructuredValuesWithCancellation(left, right, func(a, b interface{}) (int, bool) { return compareCypherComparableValuesWithCancellation(a, b, probe) }, probe); structured {
		return comparison, comparable
	}
	return 0, false
}

// compareCypherPredicateValue accepts typed integers without boxing them in
// planned arithmetic predicates. All other values use the general value path.
func compareCypherPredicateValue[L, R any](left L, right R, operator string) interface{} {
	if operator == "=" || operator == "<>" || operator == "!=" {
		equal := cypherEquality(left, right)
		if equal == nil {
			return nil
		}
		return equal.(bool) == (operator == "=")
	}
	if l, ok := any(left).(int64); ok {
		if r, ok := any(right).(int64); ok {
			return cypherComparisonResult(compareInt64(l, r), operator)
		}
	}
	return compareCypherPredicateGeneralValue(any(left), any(right), operator)
}

func compareCypherPredicateGeneralValue(left, right interface{}, operator string) interface{} {
	return compareCypherPredicateValueWithCancellation(left, right, operator, nil)
}

// compareCypherPredicateValueWithCancellation is compareCypherPredicateValue
// on boxed values, its structural equality and ordering checked against probe.
func compareCypherPredicateValueWithCancellation(left, right interface{}, operator string, probe *pipelineCancellation) interface{} {
	if operator == "=" || operator == "<>" || operator == "!=" {
		equal, known := cypherEqualityWithCancellation(left, right, probe).(bool)
		if !known {
			return nil
		}
		return equal == (operator == "=")
	}
	if left == nil || right == nil {
		return nil
	}
	if operator == "=~" {
		// A type or pattern error is null here; the row evaluator's =~ branch
		// and the predicate evaluator raise it.
		matched, _ := cypherRegexMatch(left, right)
		return matched
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
	comparison, comparable := compareCypherOrderedValuesWithCancellation(left, right, probe)
	if !comparable {
		// Values with no order between them (two durations, two points) are
		// still <= and >= when equal, as Neo4j reads a <= b as a < b OR a = b.
		if operator == "<=" || operator == ">=" {
			if equal, known := cypherEqualityWithCancellation(left, right, probe).(bool); known && equal {
				return true
			}
		}
		return nil
	}
	return cypherComparisonResult(comparison, operator)
}

func cypherComparisonResult(comparison int, operator string) interface{} {
	switch op := comparisonOperatorOf(operator); op {
	case comparisonLess, comparisonGreater, comparisonLessOrEqual, comparisonGreaterOrEqual:
		held, _ := op.orders(comparison)
		return held
	}
	return nil
}

// comparisonOperator is a comparison operator read once from its text, for a
// plan that compares many rows with it (rowPredicatePart.op).
type comparisonOperator uint8

const (
	// comparisonOther is =~ or any other operator that doesn't compare order.
	comparisonOther comparisonOperator = iota
	comparisonEqual
	comparisonNotEqual
	comparisonLess
	comparisonGreater
	comparisonLessOrEqual
	comparisonGreaterOrEqual
)

func comparisonOperatorOf(operator string) comparisonOperator {
	switch operator {
	case "=":
		return comparisonEqual
	case "<>", "!=":
		return comparisonNotEqual
	case "<":
		return comparisonLess
	case ">":
		return comparisonGreater
	case "<=":
		return comparisonLessOrEqual
	case ">=":
		return comparisonGreaterOrEqual
	}
	return comparisonOther
}

// orders reads an order between two values (-1, 0 or 1, as compareInt64
// gives it) as the operator's answer, the one reading of an order for every
// comparison (cypherComparisonResult); ok is false for comparisonOther.
func (op comparisonOperator) orders(comparison int) (held, ok bool) {
	switch op {
	case comparisonEqual:
		return comparison == 0, true
	case comparisonNotEqual:
		return comparison != 0, true
	case comparisonLess:
		return comparison < 0, true
	case comparisonGreater:
		return comparison > 0, true
	case comparisonLessOrEqual:
		return comparison <= 0, true
	case comparisonGreaterOrEqual:
		return comparison >= 0, true
	}
	return false, false
}

func compareCypherPredicateValues(left, right interface{}, operator string) bool {
	matched, known := compareCypherPredicateValue(left, right, operator).(bool)
	return known && matched
}

// cypherListSlice is base[lower..upper], the one list slice of every
// evaluator (#907). A bound that is absent (hasLower / hasUpper false) is
// the list's start or end; a negative one counts from the end; both are
// clamped to the list. A null base or bound gives null. A base that isn't a
// list is a list of that one value, as Neo4j slices a value whose type it
// doesn't know before the statement runs (n.p[..1] is [n.p]); a type known
// before is checked statically. ok is false for a bound that isn't an
// integer.
func cypherListSlice(base, lower, upper interface{}, hasLower, hasUpper bool) (interface{}, bool) {
	if base == nil || hasLower && lower == nil || hasUpper && upper == nil {
		return nil, true
	}
	var items []interface{}
	if list, isList := cypherListValue(base); isList {
		items = list
	} else {
		items = []interface{}{base}
	}
	length := len(items)
	from, to := 0, length
	if hasLower {
		index, isIndex := rowSubscriptIndex(lower)
		if !isIndex {
			return nil, false
		}
		from = index
	}
	if hasUpper {
		index, isIndex := rowSubscriptIndex(upper)
		if !isIndex {
			return nil, false
		}
		to = index
	}
	if from < 0 {
		from += length
	}
	if to < 0 {
		to += length
	}
	from = min(max(from, 0), length)
	to = min(max(to, 0), length)
	if from >= to {
		return []interface{}{}, true
	}
	return append([]interface{}(nil), items[from:to]...), true
}

// evaluateRowCaseExpression evaluates CASE against the complete heterogeneous
// row. This keeps CASE semantics available after WITH/UNWIND and in the
// converged pipeline, where bindings are not limited to graph entities.
func (e *StorageExecutor) evaluateRowCaseExpression(expr string, values map[string]interface{}) (interface{}, bool, error) {
	return e.evaluateRowCaseExpressionWithCancellation(expr, values, rowPipelineCancellation(values))
}

func (e *StorageExecutor) evaluateRowCaseExpressionWithCancellation(expr string, values map[string]interface{}, probe *pipelineCancellation) (interface{}, bool, error) {
	parsed, err := parseCaseExpression(expr)
	if err != nil {
		return nil, false, nil
	}
	return runCaseExpression(parsed, rowIsCypher25(values), func(part caseValuePart, _ int, text string) (interface{}, bool, error) {
		if probe != nil && !probe.check() {
			return nil, false, probe.err
		}
		evaluate := func(scope map[string]interface{}) (interface{}, bool, error) {
			value, ok, err := e.evaluateRowValueWithCancellation(text, scope, probe)
			if part == caseWhen && !parsed.isSimple && err == nil && !ok {
				ctx := rowStatementContextOrBackground(scope)
				return e.evaluateRowPredicate(ctx, text, scope), true, getExpressionFailure(ctx)
			}
			return value, ok, err
		}
		return evaluateRowLocalSubqueries(values, text, evaluate)
	})
}

// evaluateRowListComprehension evaluates a comprehension against typed row
// bindings. The loop value remains a node, relationship, map, path, or scalar;
// it is never converted to query text.
func (e *StorageExecutor) evaluateRowListComprehension(expr string, values map[string]interface{}, budgets ...*pipelineCancellation) (interface{}, bool, bool, error) {
	probe := rowPipelineCancellation(values)
	if len(budgets) > 0 {
		probe = budgets[0]
	}
	inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']')
	if !enclosed {
		return nil, false, false, nil
	}
	variable, listExpression, predicate, projection, matched := parseListComprehension(inner)
	if !matched {
		return nil, false, false, nil
	}
	listValue, ok, err := e.evaluateRowValueWithCancellation(listExpression, values, probe)
	if err != nil {
		return nil, true, false, err
	}
	if !ok {
		return nil, true, false, nil
	}
	if listValue == nil {
		return nil, true, true, nil
	}
	if predicate == "" && projection == "" {
		// [x IN v] is v itself, list or not, as in Neo4j.
		return listValue, true, true, nil
	}
	items := coerceToUnwindItemsWithCancellation(listValue, probe)
	if probe != nil && probe.err != nil {
		return nil, true, false, probe.err
	}
	// One scope for every element, as in evaluateRowQuantifier.
	scope := rowElementScope(values, 1, mayContainSubqueryExpression(predicate) || mayContainSubqueryExpression(projection))
	var holds func() (bool, bool, error)
	switch {
	case predicate != "" && containsRelationshipPattern(predicate):
		// A pattern predicate reads the graph: the row predicate evaluator
		// tests it as WHERE does (#907).
		holds = func() (bool, bool, error) {
			if probe != nil && !probe.check() {
				return false, false, probe.err
			}
			ctx := withExpressionFailureSlot(rowStatementContextOrBackground(scope))
			held := e.evaluateRowPredicateText(ctx, predicate, scope)
			return held, true, getExpressionFailure(ctx)
		}
	case predicate != "":
		holds = func() (bool, bool, error) {
			if probe != nil && !probe.check() {
				return false, false, probe.err
			}
			return rowConditionHolds(e.evaluateRowValueWithCancellation(predicate, scope, probe))
		}
	}
	var project func() (interface{}, bool, error)
	if projection != "" {
		project = func() (interface{}, bool, error) {
			if holds == nil && probe != nil && !probe.check() {
				return nil, false, probe.err
			}
			return e.evaluateRowValueWithCancellation(projection, scope, probe)
		}
	}
	budget := rowExpressionBudget(scope)
	value, resolved, err := rowComprehensionLoop(&budget, items, func(item interface{}) { scope[variable] = item }, holds, project)
	if probe != nil && probe.err != nil {
		return nil, true, false, probe.err
	}
	return value, true, resolved, err
}

func parseListComprehension(inner string) (variable, listExpression, predicate, projection string, ok bool) {
	// The declaration as every reader reads it (bindingDeclaration): x IN[1]
	// and x /* … */ IN l too.
	variable, _, listStart, declared := bindingDeclaration(inner, 0)
	if !declared {
		return "", "", "", "", false
	}
	rest := inner[listStart:]
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
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 && index+len(token) <= len(expression) && strings.EqualFold(expression[index:index+len(token)], token) &&
			!insideCaseBlock(expression, index) {
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

// rowValueEvaluator evaluates an expression over a row: the row evaluator
// (evaluateRowValue), or, for a scope whose expressions hold subqueries, the
// context evaluator (rowSubqueryValue).
type rowValueEvaluator func(expr string, values map[string]interface{}) (interface{}, bool, error)

// evaluateRowQuantifier is all / any / none / single(x IN list WHERE p) for a
// row, each predicate evaluated by evaluate with x bound; matched is false for
// any other expression.
func (e *StorageExecutor) evaluateRowQuantifier(expr string, values map[string]interface{}, evaluate rowValueEvaluator, budgets ...*pipelineCancellation) (interface{}, bool, bool, error) {
	probe := rowPipelineCancellation(values)
	if len(budgets) > 0 {
		probe = budgets[0]
	}
	function, inner, isFunction := parseFunctionCallWS(expr)
	function = lowerASCII(function)
	if !isFunction || !isQuantifierFunction(function) {
		return nil, false, false, nil
	}
	variable, listExpression, predicate, parsed := parseQuantifierArguments(inner)
	if !parsed {
		return nil, true, false, nil
	}
	listValue, ok, err := evaluate(listExpression, values)
	if err != nil {
		return nil, true, false, err
	}
	if !ok {
		return nil, true, false, nil
	}
	if listValue == nil || nilRemotePath(listValue) {
		return nil, true, true, nil
	}
	if cypherValueKindOf(listValue) == valueKindPath {
		return nil, true, false, runtimeTypeError("Type mismatch: expected List<T> but was Path")
	}
	items := coerceToUnwindItemsWithCancellation(listValue, probe)
	if probe != nil && probe.err != nil {
		return nil, true, false, probe.err
	}
	// One scope for every element, the variable rebound per element, as
	// reduce does: copying the row per element cost an allocation of the
	// whole row for each one (#1214).
	scope := rowElementScope(values, 1, mayContainSubqueryExpression(predicate))
	value, resolved, err := rowQuantifierLoop(function, items, func(item interface{}) { scope[variable] = item }, func() (interface{}, bool, error) {
		if probe != nil && !probe.check() {
			return nil, false, probe.err
		}
		return evaluate(predicate, scope)
	})
	if probe != nil && probe.err != nil {
		return nil, true, false, probe.err
	}
	return value, true, resolved, err
}

func evaluateRowPropertyChain(value interface{}, chain string) (interface{}, bool) {
	for start := 0; start < len(chain); {
		end := nextRowPropertySeparator(chain, start)
		property := normalizePropertyKey(strings.TrimSpace(chain[start:end]))
		if property == "" {
			return nil, false
		}
		next, ok := rowPropertyValue(value, property)
		if !ok {
			return nil, false
		}
		value = next
		if end == len(chain) {
			break
		}
		start = end + 1
	}
	return value, true
}

// isQualifiedFunctionCall reports whether expr is one call of a qualified
// function name: name.name… followed by an argument list that ends expr.
func isQualifiedFunctionCall(expr string) bool {
	_, end, ok := scanSymbolicName(expr, 0)
	if !ok {
		return false
	}
	segments := 0
	for end < len(expr) && expr[end] == '.' {
		_, next, ok := scanSymbolicName(expr, end+1)
		if !ok {
			return false
		}
		end, segments = next, segments+1
	}
	for end < len(expr) && isASCIIWhitespace(expr[end]) {
		end++
	}
	return segments > 0 && end < len(expr) && expr[end] == '(' && findMatchingParen(expr, end) == len(expr)-1
}

// rowPropertyChainTypeError is the TypeError of a property chain that
// reached a value with no properties: a number, string, boolean, list or path
// ("Type mismatch: expected a map but was Long(1)", propertyAccessTypeError),
// as Neo4j reports it at run time. It is nil when every step has properties.
func rowPropertyChainTypeError(value interface{}, chain string) error {
	for start := 0; start < len(chain); {
		end := nextRowPropertySeparator(chain, start)
		property := normalizePropertyKey(strings.TrimSpace(chain[start:end]))
		next, ok := rowPropertyValue(value, property)
		if !ok {
			if _, isPoint, err := evaluatePointProperty(value, property); isPoint && err != nil {
				return err
			}
			if isRuntimeTemporal(value) || isRuntimeDuration(value) {
				return temporalNoSuchFieldError(property)
			}
			return propertyAccessTypeError(value)
		}
		value = next
		if end == len(chain) {
			break
		}
		start = end + 1
	}
	return nil
}

// rowPropertyValue is value's property: a node's or relationship's
// property, a map's key or a temporal value's component. Property access is
// null-propagating: a null value (an unmatched OPTIONAL MATCH variable) or a
// missing property is null. ok is false when value has no properties.
func rowPropertyValue(value interface{}, property string) (interface{}, bool) {
	if value == nil {
		return nil, true
	}
	switch typed := value.(type) {
	case storage.RemoteEntityValue:
		properties, _ := rowProjectionProperties(typed)
		return properties[property], true
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
		return propertyValue, true
	case *storage.Edge:
		if typed == nil {
			return nil, true
		}
		return typed.Properties[property], true
	}
	if propertyValue, temporal, supported := evaluateTemporalProperty(value, property); temporal {
		return propertyValue, supported
	}
	if propertyValue, isPoint, err := evaluatePointProperty(value, property); isPoint {
		return propertyValue, err == nil
	}
	object, ok := toStringAnyMap(value)
	if !ok {
		return nil, false
	}
	return object[property], true
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

// stripEnclosingRowDelimiter returns the inside of expr when expr is one
// open … close group: its first delimiter closes at its end, matched by
// findMatchingDelimiter (quoted text, escaped quotes and comments skipped).
// For '[' it is isListLiteral.
func stripEnclosingRowDelimiter(expr string, open, close byte) (string, bool) {
	if len(expr) < 2 || expr[0] != open || expr[len(expr)-1] != close || findMatchingDelimiter(expr, 0, rune(open), rune(close)) != len(expr)-1 {
		return "", false
	}
	return strings.TrimSpace(expr[1 : len(expr)-1]), true
}

func rowSubscriptIndex(value interface{}) (int, bool) {
	if number, ok := cypherSignedInteger(value); ok {
		return int(number), true
	}
	if number, ok := cypherUnsignedInteger(value); ok {
		return int(number), true
	}
	return 0, false
}

// evaluateRowPredicate evaluates a WHERE predicate against a row.
//
// The operator scanners of evaluateRowPredicateParts are CASE-unaware (keeping
// the hot scan cheap), so a CASE block would have the AND / > of its WHEN
// conditions read as the predicate's own (#699). The blocks' values are bound
// first, once per predicate, as the context evaluator substitutes them
// (evaluateExpressionWithCASESubstituted).
func (e *StorageExecutor) evaluateRowPredicate(ctx context.Context, expression string, values map[string]interface{}) bool {
	if plan := wherePredicatePlanFor(ctx, expression); plan != nil {
		return e.evaluateWherePredicatePlan(ctx, plan, expression, values)
	}
	return e.evaluateRowPredicateMode(ctx, expression, values)
}

// evaluateWherePredicatePlan evaluates a WHERE predicate as Neo4j plans its
// subqueries (wherePredicatePlan): the positive subquery conjuncts first
// (Apply), then the fold, with the plan's local operands evaluated in a
// local subquery scope.
func (e *StorageExecutor) evaluateWherePredicatePlan(ctx context.Context, plan *wherePredicatePlan, expression string, values map[string]interface{}) bool {
	values = rowValuesWithStatementContext(ctx, expression, values)
	position := logicalFoldPosition(plan.logical.kind)
	value, ok, err := plan.logical.evaluateInContext(ctx, func(index int) (interface{}, bool, error) {
		operand := plan.logical.operands[index]
		if plan.local[index] {
			local := context.WithValue(ctx, localSubqueryScopeKey{}, true)
			return e.rowTextFoldOperand(local, operand, foldPositionInContext(local, position), values, true, nil)
		}
		return e.rowTextFoldOperand(ctx, operand, position, values, true, nil)
	})
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	return ok && predicateValueIsTrue(ctx, value, expression)
}

// evaluateMatchRowPredicate is the MATCH-clause entry into the same shared
// evaluator: null drops the row, while predicate conversion and runtime
// type errors are shared across clause positions.
func (e *StorageExecutor) evaluateMatchRowPredicate(ctx context.Context, expression string, values map[string]interface{}) bool {
	return e.evaluateRowPredicate(ctx, expression, values)
}

func (e *StorageExecutor) evaluateRowPredicateMode(ctx context.Context, expression string, values map[string]interface{}) bool {
	// A predicate already planned as complete is evaluated from its plan:
	// the scans below look for forms it doesn't have, and run once per row.
	if plan, cached := rowPredicatePlans.get(expression); cached && plan != nil && plan.complete {
		return e.evaluateRowPredicatePlan(ctx, plan, values)
	}
	// A predicate evaluated before is evaluated from its program: the
	// decisions below, read from its text once (row_predicate_program.go).
	if program := rowPredicateProgramFor(expression); program != nil {
		return program.holds(e, ctx, values)
	}
	// CASE stays in the value program at its original operand position,
	// including on the first row.
	if mayContainCaseKeyword(expression) && len(caseBlockSpans(expression)) > 0 {
		program := compileRowPredicateProgram(expression)
		rowPredicatePrograms.put(expression, program)
		return program.holds(e, ctx, values)
	}
	values = rowValuesWithStatementContext(ctx, expression, values)
	return e.evaluateRowPredicateParts(ctx, expression, values)
}

// rowValuesWithStatementContext returns a row predicate's values with the
// statement context bound (rowStatementContextKey) when the predicate reads the
// statement's clock, as evaluateRowExpressionWithContext binds it for a
// projection: every clock call of one statement then reads one instant
// (#872). values itself is not modified.
func rowValuesWithStatementContext(ctx context.Context, expression string, values map[string]interface{}) map[string]interface{} {
	if _, bound := values[rowStatementContextKey]; bound {
		return values
	}
	// A predicate that builds values (range(), reduce, a comprehension)
	// carries the context too while a memory limit is configured: its
	// builds count against the limit (#1343).
	if !rowPredicateNeedsStatementContext(expression) && !(transactionMemoryTracking() && rowContextShapeOf(expression).buildsValues) {
		return values
	}
	return rowValuesBindingStatementContext(ctx, values)
}

// rowValuesBindingStatementContext is values with the statement context
// bound, for a predicate that needs it (rowValuesWithStatementContext).
func rowValuesBindingStatementContext(ctx context.Context, values map[string]interface{}) map[string]interface{} {
	probe := contextPipelineProbe(ctx)
	if _, bound := values[rowStatementContextKey]; bound {
		return values
	}
	extended := make(map[string]interface{}, len(values)+1)
	for key, value := range values {
		if probe.stopped() {
			return nil
		}
		extended[key] = value
	}
	extended[rowStatementContextKey] = withPipelineCancellation(ctx)
	return extended
}

// rowPredicateStatementContextNeeds caches rowPredicateNeedsStatementContext
// by predicate text: it is decided once, not once per row.
var rowPredicateStatementContextNeeds = newBoundedCache[string, bool](1024, boundedCacheStringShardIndex)

// rowPredicateNeedsStatementContext reports a predicate whose row values need
// the statement context (needsRowStatementContext), except a whole [NOT]
// EXISTS { … } predicate, which the predicate evaluator runs with its context
// directly (evaluateRowExistsPredicate): binding it would copy every row.
func rowPredicateNeedsStatementContext(expression string) bool {
	if needs, cached := rowPredicateStatementContextNeeds.get(expression); cached {
		return needs
	}
	needs := needsRowStatementContext(expression)
	if needs && !containsStatementClockCall(expression) {
		if _, whole := wholeExistsPredicate(expression); whole {
			needs = false
		}
	}
	rowPredicateStatementContextNeeds.put(expression, needs)
	return needs
}

// evaluateRowPredicateParts is evaluateRowPredicate for a predicate whose CASE
// blocks are bound; it recurses into its OR / AND / NOT parts.
func (e *StorageExecutor) evaluateRowPredicateParts(ctx context.Context, expression string, values map[string]interface{}) bool {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false
	}
	// A predicate wholly wrapped in one matching outer paren pair — e.g.
	// `(EXISTS { ... })`, `(a.x = 1 OR EXISTS { ... })` — must be unwrapped
	// before the OR/AND/EXISTS/COUNT recognizers below, which only look for
	// their marker at the top level of the expression. Without this, the
	// wrapping parens hide a top-level " OR "/" AND " or an EXISTS/COUNT
	// prefix from those recognizers, silently falling through to the
	// generic comparison/expression evaluators and returning a wrong
	// (false) result instead of evaluating the subquery.
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return e.evaluateRowPredicateParts(ctx, inner, values)
	}
	strictLogical := rowPredicateIsStrictLogical(expression)
	if mayContainArithmetic(expression) && strictLogical && !containsRelationshipPattern(expression) {
		value, evaluated, err := e.evaluateRowValueProbed(expression, values, contextPipelineProbe(ctx))
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		if evaluated {
			return predicateValueIsTrue(ctx, value, expression)
		}
	}
	return e.evaluateRowPredicatePartsFromSubqueries(ctx, expression, values)
}

// rowPredicateIsStrictLogical reports a predicate evaluateRowPredicateParts
// evaluates whole with the row value evaluator first: one with arithmetic
// that is a NOT or an XOR, or an AND / OR with an operand of a type the
// static checker can't tell.
func rowPredicateIsStrictLogical(expression string) bool {
	strictLogical := startsWithKeywordFold(expression, "NOT") || topLevelKeywordIndex(expression, "XOR") >= 0
	if mayContainArithmetic(expression) && !strictLogical {
		checker := staticOperatorChecker{}
		for _, operator := range []string{" AND ", " OR "} {
			if left, right, found := splitByOperatorOutsideCase(expression, operator, true, true); found {
				for _, operand := range []string{left, right} {
					if mayContainArithmetic(operand) {
						continue
					}
					typeInfo, _ := checker.check(operand)
					if !typeInfo.known() {
						typeInfo, _ = checker.checkAtom(operand)
					}
					if !typeInfo.known() {
						strictLogical = true
					}
				}
			}
		}
	}
	return strictLogical
}

// evaluateRowPredicatePartsFromSubqueries is evaluateRowPredicateParts
// after the strict logical evaluation: subqueries, label tests, the
// predicate plan, then the text.
func (e *StorageExecutor) evaluateRowPredicatePartsFromSubqueries(ctx context.Context, expression string, values map[string]interface{}) bool {
	// Subquery expressions inside a larger predicate ([EXISTS { … }] = [true],
	// COUNT { … } + 1 > 1, …) are evaluated where they stand by the row
	// evaluator (#1055), unless AND / OR / XOR split the predicate: then each
	// side evaluates its own.
	if plan := planRowSubqueries(expression); plan != nil && !plan.logical && !plan.wholeExists {
		if plan.integerComparison != nil {
			// COUNT { … } <op> <integer>: compare the count directly; a null
			// or non-integer value takes the general comparison.
			value, _ := e.evaluateRowSubqueryValue(ctx, plan.found[0].kind, plan.found[0].body, values)
			if count, ok := value.(int64); ok {
				return plan.integerComparison.holds(count)
			}
		}
		if holds, evaluated := e.rowPredicateWithSubqueries(ctx, expression, values); evaluated {
			return holds
		}
	}
	if variable, labels, ok := parseWithWhereLabelTest(expression); ok {
		return entityHasAllLabelsOrTypesPredicate(values[variable], labels)
	}
	// AND parts that compare or null-test simple operands are parsed once per
	// predicate text (planRowPredicate), not once per row.
	if plan := planRowPredicate(expression); plan != nil {
		return e.evaluateRowPredicatePlan(ctx, plan, values)
	}
	return e.evaluateRowPredicateText(ctx, expression, values)
}

// rowPredicateWithSubqueries evaluates a predicate holding subquery
// expressions with the row evaluator, which evaluates each subquery where it
// stands; evaluated is false when the row evaluator doesn't handle the
// predicate. An error is recorded on ctx and the predicate doesn't hold.
func (e *StorageExecutor) rowPredicateWithSubqueries(ctx context.Context, expression string, values map[string]interface{}) (bool, bool) {
	value, evaluated, err := e.evaluateRowValueProbed(expression, rowValuesWithStatementContext(ctx, expression, values), contextPipelineProbe(ctx))
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false, true
	}
	if !evaluated {
		return false, false
	}
	return predicateValueIsTrue(ctx, value, expression), true
}

// evaluateRowPredicateText is evaluateRowPredicate without the predicate plan:
// the predicate is evaluated from its text. A planned part whose operand the
// plan can't resolve directly is evaluated here.
func (e *StorageExecutor) evaluateRowPredicateText(ctx context.Context, expression string, values map[string]interface{}) bool {
	// EXISTS and NOT EXISTS are complete predicates, evaluated against the
	// row's correlated typed bindings rather than as a scalar NOT.
	if matched, recognized := e.evaluateRowExistsPredicate(ctx, expression, values); recognized {
		return matched
	}
	// OR, XOR, AND and NOT (evaluateLogicalExpression) over the operands'
	// values, or whether an operand only the predicate evaluator reads holds
	// (rowTextFoldOperand); the result is read as a predicate.
	if value, logical, ok, err := evaluateLogicalExpressionInContext(ctx, expression, func(operand string, position foldPosition) (interface{}, bool, error) {
		return e.rowTextFoldOperand(ctx, operand, position, values, true, nil)
	}); logical {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		return ok && predicateValueIsTrue(ctx, value, expression)
	}
	return e.evaluateRowPredicateTextFromSubqueries(ctx, expression, values)
}

// evaluateRowPredicateTextFromSubqueries is evaluateRowPredicateText for a
// predicate that is neither a whole EXISTS nor a logical operator.
func (e *StorageExecutor) evaluateRowPredicateTextFromSubqueries(ctx context.Context, expression string, values map[string]interface{}) bool {
	if plan := planRowSubqueries(expression); plan != nil && !plan.wholeExists {
		if holds, evaluated := e.rowPredicateWithSubqueries(ctx, expression, values); evaluated {
			return holds
		}
	}
	if isRelationshipPatternOperand(expression) {
		if value, resolved := e.rowPredicateOperand(ctx, expression, values); resolved {
			return predicateValueIsTrue(ctx, value, expression)
		}
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, " STARTS WITH ", true, true); ok {
		return e.evaluateRowStringPredicate(ctx, left, right, values, strings.HasPrefix)
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, " ENDS WITH ", true, true); ok {
		return e.evaluateRowStringPredicate(ctx, left, right, values, strings.HasSuffix)
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, " CONTAINS ", true, true); ok {
		return e.evaluateRowStringPredicate(ctx, left, right, values, strings.Contains)
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, "=~", false, true); ok {
		leftValue, leftOK := e.rowPredicateOperand(ctx, left, values)
		rightValue, rightOK := e.rowPredicateOperand(ctx, right, values)
		if !leftOK || !rightOK {
			return false
		}
		matched, err := cypherRegexMatch(leftValue, rightValue)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		return matched == true
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, " NOT IN ", true, true); ok {
		// x NOT IN list holds only when the membership is known false; a null
		// membership (null x, null list, or a null element without a match)
		// drops the row.
		membership, known := e.rowPredicateMembership(ctx, left, right, values)
		return known && membership == false
	}
	if left, right, ok := splitByOperatorOutsideCase(expression, " IN ", true, true); ok {
		membership, known := e.rowPredicateMembership(ctx, left, right, values)
		return known && membership == true
	}
	if operand, negated, spec, isTypePredicate, err := splitTypePredicate(expression); isTypePredicate {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		value, ok := e.rowPredicateOperand(ctx, operand, values)
		return ok && evaluateTypePredicate(value, negated, spec)
	}
	if operand, negated, form, isNormalization := splitNormalizationPredicate(expression); isNormalization {
		value, ok := e.rowPredicateOperand(ctx, operand, values)
		matched, known := evaluateNormalizationPredicate(value, negated, form).(bool)
		return ok && known && matched
	}
	for _, operator := range []string{" IS NOT NULL", " IS NULL"} {
		if hasSuffixFoldASCII(expression, operator) {
			left := strings.TrimSpace(expression[:len(expression)-len(operator)])
			value, ok := e.rowPredicateOperand(ctx, left, values)
			if !ok {
				value = nil
			}
			return nullEvaluationHandler(operator == " IS NOT NULL").evaluate(value)
		}
	}
	comparisonResult, comparison, resolved, err := evaluateComparisonChainInContext(ctx, expression, func(operand string, position foldPosition) (interface{}, bool, error) {
		return e.rowTextFoldOperand(ctx, operand, position, values, false, nil)
	}, compareCypherPredicateValue, cypherVersionFromContext(ctx) == "25")
	if comparison {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return false
		}
		matched, known := comparisonResult.(bool)
		return resolved && known && matched
	}
	value, ok := e.rowPredicateOperand(ctx, expression, values)
	if !ok {
		return false
	}
	return predicateValueIsTrue(ctx, value, expression)
}

// evaluateCypherSize computes size(value) for the shared row evaluator and
// the compiled binding resolver (the compiled fast path is a compiled form
// of the same evaluator, #728): null stays null, a string sizes in runes, a
// list or array in items, and anything else is the argument type error.
func evaluateCypherSize(value interface{}) (interface{}, bool, error) {
	return evaluateCypherSizeWithCancellation(value, nil)
}

func evaluateCypherSizeWithCancellation(value interface{}, probe *pipelineCancellation) (interface{}, bool, error) {
	result, err := cypherfn.SizeWithCancellation(value, functionCancellation(probe))
	if err != nil {
		if probe != nil && probe.err != nil {
			return nil, false, probe.err
		}
		return nil, false, functionEvaluationError(err)
	}
	return result, true, nil
}

func evaluateCypherSizeInteger(value interface{}) (int64, bool, error) {
	return evaluateCypherSizeIntegerWithCancellation(value, nil)
}

func evaluateCypherSizeIntegerWithCancellation(value interface{}, probe *pipelineCancellation) (int64, bool, error) {
	// A string, the usual argument of a planned size(), counts its code
	// points (cyphertext.Length, as cypherfn.SizeInteger counts them)
	// without the call into the function package for every row.
	if text, ok := value.(string); ok {
		length, err := cypherfn.CharacterCount(text, functionCancellation(probe))
		return int64(length), err == nil, err
	}
	length, nonNull, err := cypherfn.SizeInteger(value)
	if err != nil {
		return 0, false, functionEvaluationError(err)
	}
	return length, nonNull, nil
}

// rowTextFoldOperand is an operand of a text predicate's logical or
// comparison fold at position: its value, or (predicateFallback) whether a
// form only the predicate evaluator reads (a pattern, EXISTS) holds, with
// the error slots the policy asks for (isolateOperandErrors). A value is
// returned with its error; the predicate evaluator records its own.
func (e *StorageExecutor) rowTextFoldOperand(ctx context.Context, operand string, position foldPosition, values map[string]interface{}, predicateFallback bool, probe *pipelineCancellation) (interface{}, bool, error) {
	// The operand's facts are read from its text once per statement.
	facts := rowOperandFactsFor(ctx, operand)
	isolation := facts.isolationAt(position, false)
	var fallback func(context.Context, map[string]interface{}) interface{}
	if predicateFallback {
		fallback = func(ctx context.Context, scope map[string]interface{}) interface{} {
			return e.evaluateRowPredicateParts(ctx, operand, scope)
		}
	}
	resolve := func(_ context.Context, scope map[string]interface{}) (interface{}, bool, error) {
		return e.evaluateRowValueWithCancellation(operand, scope, probe)
	}
	if predicateFallback && facts.pattern {
		// A pattern has no value: only the predicate evaluator reads it.
		resolve = nil
	}
	return isolation.evaluate(ctx, values, resolve, fallback)
}

// rowPredicateOperand evaluates an operand of a row predicate. An error is
// the statement's: it is recorded on ctx, and the operand is unresolved, so
// the predicate doesn't hold.
func (e *StorageExecutor) rowPredicateOperand(ctx context.Context, expr string, values map[string]interface{}) (interface{}, bool) {
	if isRelationshipPatternOperand(expr) {
		if matched, recognized := e.rowPatternPredicate(ctx, expr, values); recognized {
			return matched, true
		}
	}
	value, ok, err := e.evaluateRowValueProbed(expr, values, contextPipelineProbe(ctx))
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	return value, ok
}

// rowPredicateMembership is left IN right for a row predicate, recording an
// operand's error on ctx.
func (e *StorageExecutor) rowPredicateMembership(ctx context.Context, left, right string, values map[string]interface{}) (interface{}, bool) {
	value, ok, err := e.evaluateRowMembershipValue(left, right, values)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	return value, ok
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

// rowRelationshipEndpoint evaluates startNode(r) / endNode(r), reading bound
// variables directly and sharing argument validation with the context evaluator.
// Non-variable arguments use the regular row expression evaluator.
func (e *StorageExecutor) rowRelationshipEndpoint(function, argument string, values map[string]interface{}) (interface{}, bool, error) {
	value, resolved := values[strings.TrimSpace(argument)]
	if !resolved {
		var err error
		value, resolved, err = e.evaluateRowValue(strings.TrimSpace(argument), values)
		if err != nil || !resolved {
			return nil, resolved, err
		}
	}
	end := equalFoldASCII(function, "endNode")
	memo, memoized := values[rowEndpointMemoKey].(map[rowEndpointMemoEntry]interface{})
	if !memoized {
		result, err := e.relationshipEndpointValue(value, end)
		return result, true, err
	}
	edge, err := relationshipEndpointArgument(value, end)
	if err != nil {
		return nil, true, err
	}
	if edge == nil {
		return nil, true, nil
	}
	entry := rowEndpointMemoEntry{id: edge.ID, end: end}
	if result, read := memo[entry]; read {
		return result, true, nil
	}
	result := e.relationshipEndpoint(edge, end)
	memo[entry] = result
	return result, true, nil
}

// rowEndpointMemoKey binds, in a row whose predicate reads a relationship's
// endpoints many times (an EXISTS body's WHERE, boundExpandCount), the
// endpoints read so far: startNode(r) / endNode(r) is read from storage once
// per relationship and end. Nothing writes while a predicate is evaluated,
// so a second read would give the same node.
const rowEndpointMemoKey = "\x00endpoint_memo"

// rowEndpointMemoEntry is one relationship's start (end false) or end node
// in a rowEndpointMemoKey memo.
type rowEndpointMemoEntry struct {
	id  storage.EdgeID
	end bool
}

func (e *StorageExecutor) relationshipEndpointValue(value interface{}, end bool) (interface{}, error) {
	edge, err := relationshipEndpointArgument(value, end)
	if err != nil {
		return nil, err
	}
	return e.relationshipEndpoint(edge, end), nil
}

// relationshipEndpoint is startNode(edge) / endNode(edge) (end): the
// endpoint read by its id, null when it is gone. The one implementation, for
// the row evaluator (rowRelationshipEndpoint) and the context evaluator
// (evaluateExpressionWithContextFullMath).
func (e *StorageExecutor) relationshipEndpoint(edge *storage.Edge, end bool) interface{} {
	if edge == nil {
		return nil
	}
	// A deleted relationship has no endpoints. Only a value shaped like its
	// view (deletedEntityViews.edge) can be one, and storage confirms it: a
	// relationship carrying its properties costs no second read.
	if mayBeDeletedRelationshipView(edge) {
		if current, err := e.storage.GetEdge(edge.ID); err != nil || current == nil {
			return nil
		}
	}
	id := edge.StartNode
	if end {
		id = edge.EndNode
	}
	node, err := e.getQueryNode(e.storage, id)
	if err != nil {
		return nil
	}
	return node
}

// isRelationshipPatternOperand reports whether expression is one whole
// relationship pattern ((a)-[:R]->(b), (a)<--()), read by
// relationshipChainEnd. A parenthesised expression whose operands are
// patterns, (NOT (a)<--() OR (a)-->()), is not one: it is evaluated as the
// predicate it is (#907).
func isRelationshipPatternOperand(expression string) bool {
	expression = strings.TrimSpace(expression)
	end, chain := relationshipChainEnd(expression, 0, len(expression))
	return chain && end == len(expression)
}

// containsRelationshipPattern reports whether a relationship pattern
// (relationshipChainEnd) starts anywhere in expression outside a string
// literal.
func containsRelationshipPattern(expression string) bool {
	for i := 0; i < len(expression); i++ {
		switch c := expression[i]; c {
		case '\'', '"', '`':
			j := i + 1
			for j < len(expression) && (expression[j] != c || isBackslashEscaped(expression, j)) {
				j++
			}
			i = j
		case '(':
			if _, chain := relationshipChainEnd(expression, i, len(expression)); chain {
				return true
			}
		}
	}
	return false
}

func (e *StorageExecutor) evaluateRowMembershipValue(left, right string, values map[string]interface{}) (interface{}, bool, error) {
	return e.evaluateRowMembershipValueWithCancellation(left, right, values, rowPipelineCancellation(values))
}

func (e *StorageExecutor) evaluateRowMembershipValueWithCancellation(left, right string, values map[string]interface{}, probe *pipelineCancellation) (interface{}, bool, error) {
	needle, leftOK, err := e.evaluateRowValueWithCancellation(left, values, probe)
	if err != nil {
		return nil, false, err
	}
	haystack, rightOK, err := e.evaluateRowValueWithCancellation(right, values, probe)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	value, ok := rowMembershipOfValuesWithCancellation(needle, haystack, isRowIdentityExpression(left), probe)
	if probe != nil && probe.err != nil {
		return nil, false, probe.err
	}
	return value, ok, nil
}

// rowMembershipOfValues is `needle IN haystack` for evaluated operands, with
// Cypher's three-valued rules. identity is set when the needle is an id() /
// elementId() expression, compared by its identity payload.
func rowMembershipOfValues(needle, haystack interface{}, identity bool) (interface{}, bool) {
	return rowMembershipOfValuesWithCancellation(needle, haystack, identity, nil)
}

func rowMembershipOfValuesWithCancellation(needle, haystack interface{}, identity bool, probe *pipelineCancellation) (interface{}, bool) {
	if haystack == nil {
		return cypherMembership(needle, nil)
	}
	items := coerceToUnwindItemsWithCancellation(haystack, probe)
	if probe != nil && probe.err != nil {
		return nil, false
	}
	equal := func(left, right interface{}) interface{} {
		if probe != nil && !probe.check() {
			return nil
		}
		return cypherEqualityWithCancellation(left, right, probe)
	}
	if !identity {
		value := cypherMembershipValues(needle, len(items), func(i int) interface{} {
			return equal(needle, items[i])
		})
		return value, probe == nil || probe.err == nil
	}
	needle = rowIdentityPayload(needle)
	return cypherMembershipValues(needle, len(items), func(i int) interface{} {
		return equal(needle, rowIdentityPayload(items[i]))
	}), probe == nil || probe.err == nil
}

// [NOT] EXISTS { } predicate; recognized is false otherwise.
func (e *StorageExecutor) evaluateRowExistsPredicate(ctx context.Context, expression string, values map[string]interface{}) (matched bool, recognized bool) {
	trimmed := strings.TrimSpace(expression)
	// Only a whole [NOT] EXISTS { } is this predicate: EXISTS { … } = false
	// is a comparison, evaluated with the subquery as one of its values.
	negated, whole := wholeExistsPredicate(trimmed)
	if !whole {
		return false, false
	}
	exists := trimmed
	if negated {
		exists = strings.TrimSpace(trimmed[len("NOT"):])
	}
	matched, recognized = e.rowExistsSubqueryMatches(ctx, exists, values)
	return matched != negated, recognized
}

// rowPatternPredicate is a relationship pattern used as a predicate (in
// WHERE, bare, or as exists(pattern)): whether it matches, decided by the
// EXISTS { pattern } owner with the whole row, so bound relationships, row
// values in property maps and rows without a node binding are all seen.
func (e *StorageExecutor) rowPatternPredicate(ctx context.Context, pattern string, values map[string]interface{}) (matched bool, recognized bool) {
	return e.evaluateRowExistsPredicate(ctx, "EXISTS { "+strings.TrimSpace(pattern)+" }", values)
}

// rowExistsSubqueryMatches is EXISTS { subquery } for a row: exists is the
// whole EXISTS { } text; recognized is false for an empty body.
func (e *StorageExecutor) rowExistsSubqueryMatches(ctx context.Context, exists string, values map[string]interface{}) (matched bool, recognized bool) {
	subquery := strings.TrimSpace(exists[strings.IndexByte(exists, '{')+1 : len(exists)-1])
	if subquery == "" {
		return false, false
	}
	// The subquery evaluator EXISTS shares with COUNT and COLLECT; its error
	// fails the statement, it doesn't read as "no rows".
	value, ok, err := e.rowSubqueryValue(ctx, "EXISTS", subquery, values)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false, true
	}
	matched, _ = value.(bool)
	return matched, ok
}

func (e *StorageExecutor) evaluateRowStringPredicate(ctx context.Context, left, right string, values map[string]interface{}, predicate func(string, string) bool) bool {
	leftValue, leftOK := e.rowPredicateOperand(ctx, left, values)
	rightValue, rightOK := e.rowPredicateOperand(ctx, right, values)
	return leftOK && rightOK && evaluateRowStringPredicateValues(leftValue, rightValue, predicate)
}

func evaluateRowStringPredicateValues(left, right interface{}, predicate func(string, string) bool) bool {
	matched, _ := rowStringPredicateValue(left, right, predicate).(bool)
	return matched
}
