package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"reflect"
	"strings"
	"unicode/utf8"

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
	switch operator {
	case '+':
		return rowArithmeticResult(operator, e.add(left, right), left, right)
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
// evaluateRowValueText is evaluateRowValue read from the text: one level of
// the expression, its parts evaluated through evaluateRowValue. A
// rowValueProgram keeps the decisions it makes about a text and calls it
// for the forms it doesn't compile.
func (e *StorageExecutor) evaluateRowValueText(expr string, values map[string]interface{}) (interface{}, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false, nil
	}
	if inner, ok := stripEnclosingExpressionParentheses(expr); ok {
		return e.evaluateRowValue(inner, values)
	}
	if caseEnd := leadingCaseExpressionEnd(expr); caseEnd > 0 && strings.TrimSpace(expr[caseEnd:]) != "" && !hasTopLevelLogicalOperator(expr) {
		caseValue, ok, err := e.evaluateRowCaseExpression(strings.TrimSpace(expr[:caseEnd]), values)
		if err != nil || !ok {
			return nil, false, err
		}
		const caseBinding = "__nornic_case_value"
		scope := make(map[string]interface{}, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[caseBinding] = caseValue
		return e.evaluateRowValue(caseBinding+expr[caseEnd:], scope)
	}
	if value, ok := values[expr]; ok && !isLiteralKeyword(expr) {
		return value, true, nil
	}
	// Relationship arrows contain comparison and subtraction characters.
	// Resolve the complete pattern before scalar operator parsing sees them.
	if isRelationshipPatternOperand(expr) {
		ctx := rowStatementContextOrBackground(values)
		if matched, recognized := e.evaluateRowRelationshipPattern(ctx, expr, values); recognized {
			return matched, true, getExpressionFailure(ctx)
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
		value, bound := values[variable]
		if !bound {
			return nil, false, nil
		}
		return entityHasAllLabelsOrTypes(value, labels), true, nil
	}
	if isCaseExpression(expr) {
		return e.evaluateRowCaseExpression(expr, values)
	}
	if value, matched, resolved, err := e.evaluateRowMapProjection(expr, values); matched {
		return value, resolved, err
	}
	if _, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		result, err := e.materializePropertyMap(rowMapContext(values), expr, values, true, true)
		return result, err == nil, err
	}
	if value, matched, ok, err := e.evaluateRowListComprehension(expr, values); matched {
		return value, ok, err
	}

	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true, nil
		}
		items := splitTopLevelComma(inner)
		return rowListLiteral(len(items), func(i int) (interface{}, bool, error) {
			return e.evaluateRowValue(items[i], values)
		})
	}

	if value, matched, ok, err := e.evaluateRowQuantifier(expr, values, e.evaluateRowValue); matched {
		return value, ok, err
	}

	var callFacts1 functionCallFacts
	if ok := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1); ok {
		function, argument := callFacts1.name, callFacts1.inner

		temporalContext, _ := values[rowStatementContextKey].(context.Context)
		if temporalContext == nil {
			temporalContext = context.Background()
		}
		if value, handled, callErr := e.evaluateTemporalConstructor(temporalContext, func(inner string) (interface{}, bool, error) {
			return e.evaluateRowValue(inner, values)
		}, expr); handled {
			return value, callErr == nil, callErr
		}
		if value, matched, resolved, err := e.evaluateRowMathFunction(function, argument, values); matched {
			return value, resolved, err
		}
		if value, matched, resolved, err := e.evaluateRowExtensionFunction(function, argument, values); matched {
			return value, resolved, err
		}
		if isGDSFunctionName(lowerASCII(function)) {
			value, _, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
			return value, resolved, err
		}
		switch lowerASCII(function) {
		case "exists":
			if isRelationshipPatternOperand(strings.TrimSpace(argument)) {
				ctx := rowStatementContextOrBackground(values)
				matched, recognized := e.evaluateRowRelationshipPattern(ctx, argument, values)
				return matched, recognized, getExpressionFailure(ctx)
			}
		case "graph.names", "graph.propertiesbyname", "graph.byname", "graph.byelementid":
			value, _, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
			return value, resolved, err
		case "shortestpath", "allshortestpaths":
			nodes := make(map[string]*storage.Node, len(values))
			for name, value := range values {
				if node, ok := value.(*storage.Node); ok {
					nodes[name] = node
				}
			}
			search, _ := values[rowStatementContextKey].(context.Context)
			if search == nil {
				search = context.Background()
			}
			value, handled, err := e.evaluateShortestPathValue(withValueBindings(search, values), function, argument, nodes)
			return value, handled, err
		case "startnode", "endnode":
			if value, handled, err := e.rowRelationshipEndpoint(function, argument, values); handled || err != nil {
				return value, handled, err
			}
		case "reduce", "allreduce":
			return e.evaluateRowReduce(function, argument, values, e.evaluateRowValue)
		case "coalesce":
			// EXISTS subqueries are non-null booleans and end coalesce
			// immediately.
			operands := splitTopLevelComma(argument)
			return cypherfn.Coalesce(len(operands), func(index int) (interface{}, bool, error) {
				trimmed := strings.TrimSpace(operands[index])
				return e.evaluateRowValue(trimmed, values)
			})
		case "length":
			value, resolved, err := e.evaluateRowValue(argument, values)
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
			object, isMap := toStringAnyMap(value)
			if path, ok := cypherPathResultOf(value, object, isMap); ok {
				return int64(path.Length), true, nil
			}
			return nil, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
				localization.CypherCoreFunctionArgumentInvalid("length", "a Path", neo4jValueRepr(value)))
		case "range":
			parts := e.splitFunctionArgs(argument)
			arguments := make([]interface{}, len(parts))
			for index, part := range parts {
				value, resolved, err := e.evaluateRowValue(strings.TrimSpace(part), values)
				if err != nil {
					return nil, false, err
				}
				if !resolved {
					return nil, false, nil
				}
				arguments[index] = value
			}
			result, err := evaluateCypherRange(arguments)
			if err != nil {
				return nil, false, err
			}
			return result, true, nil
		case "head", "last", "reverse", "size":
			value, resolved, err := e.evaluateRowValue(argument, values)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			if strings.EqualFold(function, "size") {
				return evaluateCypherSize(value)
			}
			result, err := evaluateListAccessFunction(lowerASCII(function), value)
			if err != nil {
				return nil, false, err
			}
			return result, true, nil
		case "nodes", "relationships":
			value, resolved, err := e.evaluateRowValue(argument, values)
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
			path, isPath := toStringAnyMap(value)
			if !isPath {
				return nil, false, nil
			}
			nodes, relationships, hasNodes, hasRelationships := pathValueParts(path)
			if strings.EqualFold(function, "relationships") {
				return relationships, hasRelationships, nil
			}
			return nodes, hasNodes, nil
		case "properties":
			value, resolved, err := e.evaluateRowValue(argument, values)
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
				properties, _ := entity["properties"].(map[string]interface{})
				return cypherfn.EntityProperties(properties), true, nil
			default:
				object, isMap := toStringAnyMap(value)
				return object, isMap, nil
			}
		case "keys":
			value, resolved, err := e.evaluateRowValue(argument, values)
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
			if object, isMap := toStringAnyMap(value); isMap {
				value = object
			}
			object, isMap := toStringAnyMap(value)
			if _, isPath := cypherPathResultOf(value, object, isMap); isPath {
				return nil, false, invalidFunctionArgument("keys", value)
			}
			keys, ok := cypherfn.PropertyKeys(e.queryVisibleValue(value))
			if !ok {
				return nil, false, invalidFunctionArgument("keys", value)
			}
			return keys, true, nil
		case "labels":
			value, resolved, err := e.evaluateRowValue(argument, values)
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
			value, resolved, err := e.evaluateRowValue(argument, values)
			if err != nil {
				return nil, false, err
			}
			if !resolved {
				return nil, false, nil
			}
			return rowTypeOf(value)
		}
		// Registered functions evaluate arguments in this row and statement's
		// context, including nested graph expressions, just like specialized calls.
		if value, handled, resolved, err := e.evaluateRowGraphFunction(function, argument, values); handled {
			return value, resolved, err
		}
	}

	// OR, XOR, AND and NOT (evaluateLogicalExpression). NOT binds less
	// tightly than comparisons and postfix predicates, so NOT a = b, NOT a
	// IS NULL and NOT a IN xs negate the whole remainder.
	_, _, isOr := splitByOperatorOutsideCase(expr, " OR ", true, true)
	if value, logical, ok, err := evaluateLogicalExpression(expr, func(operand string) (interface{}, bool, error) {
		if isOr && rowOperandRecordsFailure(operand) {
			return deferredRowOperand(values, func(scope map[string]interface{}) (interface{}, bool, error) {
				return e.evaluateRowValue(operand, scope)
			})
		}
		return e.evaluateRowValue(operand, values)
	}); logical {
		if err != nil || !ok {
			return nil, false, err
		}
		return value, true, nil
	}

	// =~ raises a type or pattern error, which the comparison chain can't.
	if left, right, regex := splitByOperatorOutsideCase(expr, "=~", false, true); regex {
		leftValue, leftOK, err := e.evaluateRowValue(left, values)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValue(right, values)
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
	result, comparison, resolved, err := evaluateComparisonChain(expr, func(operand string) (interface{}, bool, error) {
		return e.evaluateRowValue(operand, values)
	}, compareCypherPredicateValue, rowIsCypher25(values), e.graphIdentityDomain())
	if comparison {
		return result, resolved, err
	}
	if operand, negated, spec, isTypePredicate, err := splitTypePredicate(expr); isTypePredicate {
		if err != nil {
			return nil, false, err
		}
		value, ok, err := e.evaluateRowValue(operand, values)
		if err != nil || !ok {
			return nil, false, err
		}
		return evaluateTypePredicate(value, negated, spec), true, nil
	}
	if operand, negated, form, isNormalization := splitNormalizationPredicate(expr); isNormalization {
		value, ok, err := e.evaluateRowValue(operand, values)
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
		if operand, nullTest := nullPredicateOperand(expr, strings.TrimSpace(predicate.suffix)); nullTest {
			value, ok, err := e.evaluateRowValue(operand, values)
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
		value, evaluated, err := e.evaluateRowMembershipValue(left, right, values)
		if err != nil || !evaluated || value == nil {
			return value, evaluated, err
		}
		return !value.(bool), true, nil
	}
	if left, right, ok := splitByOperatorOutsideCase(expr, " IN ", true, true); ok {
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
		left, right, matched := splitByOperatorOutsideCase(expr, predicate.operator, true, true)
		if !matched {
			continue
		}
		leftValue, leftOK, err := e.evaluateRowValue(left, values)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValue(right, values)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return rowStringPredicateValue(leftValue, rightValue, predicate.match), true, nil
	}

	if open := rowSubscriptStart(expr); open > 0 {
		return e.evaluateCypherSubscript(expr[:open], strings.TrimSpace(expr[open+1:len(expr)-1]), func(part string) (interface{}, bool, error) {
			return e.evaluateRowValue(part, values)
		})
	}

	if left, right, operator, ok := splitRowArithmeticTier(expr, "+-"); ok {
		leftValue, leftOK, err := e.evaluateRowValue(left, values)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValue(right, values)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return e.evaluateRowArithmeticValues(operator, leftValue, rightValue)
	}

	if left, right, operator, ok := splitRowArithmeticTier(expr, "*/%"); ok {
		leftValue, leftOK, err := e.evaluateRowValue(left, values)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValue(right, values)
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
		return e.evaluateRowArithmeticValues(operator, leftValue, rightValue)
	}

	if left, right, _, ok := splitRowArithmeticTier(expr, "^"); ok {
		leftValue, leftOK, err := e.evaluateRowValue(left, values)
		if err != nil {
			return nil, false, err
		}
		rightValue, rightOK, err := e.evaluateRowValue(right, values)
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
		value, ok, err := e.evaluateRowValue(strings.TrimSpace(expr[1:]), values)
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
		if endpoint := endpointPropertyNode(strings.TrimSpace(expr[:dot]), strings.TrimSpace(expr[dot+1:])); endpoint != nil {
			return endpoint.eval(e, values, nil)
		}
		base, ok, err := e.evaluateRowValue(strings.TrimSpace(expr[:dot]), values)
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
	return e.evaluateRowFallback(expr, values, rowFallbackValue)
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
		switch cypherLexicalByte(current) {
		case cypherQuoteClass:
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
	for previous >= 0 && cyphertext.IsWhitespaceAt(expr, previous) {
		previous--
	}
	if previous < 0 {
		return true
	}
	if (expr[previous] == 'e' || expr[previous] == 'E') && previous > 0 && index+1 < len(expr) &&
		cyphertext.IsDigit(expr[previous-1]) && cyphertext.IsDigit(expr[index+1]) {
		return true
	}
	return strings.ContainsRune("([{,:+-*/%^=<>|", rune(expr[previous]))
}

// compareCypherOrderedValues orders two values for the comparison operators
// (comparability): comparable is false for values that have no order between
// them (different kinds, durations, points, null inside a list or map).
func compareCypherOrderedValues(left, right interface{}) (int, bool) {
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
			comparison, comparable := compareCypherComparableValues(leftList[index], rightList[index])
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
	if comparison, comparable, structured := compareCypherStructuredValues(left, right, compareCypherComparableValues); structured {
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
	comparison, comparable := compareCypherOrderedValues(left, right)
	if !comparable {
		// Values with no order between them (two durations, two points) are
		// still <= and >= when equal, as Neo4j reads a <= b as a < b OR a = b.
		if operator == "<=" || operator == ">=" {
			if equal, known := cypherEquality(left, right).(bool); known && equal {
				return true
			}
		}
		return nil
	}
	return cypherComparisonResult(comparison, operator)
}

func cypherComparisonResult(comparison int, operator string) interface{} {
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
	parsed, err := parseCaseExpression(expr)
	if err != nil {
		return nil, false, nil
	}
	return runCaseExpression(parsed, rowIsCypher25(values), func(part caseValuePart, _ int, text string) (interface{}, bool, error) {
		value, ok, err := e.evaluateRowValue(text, values)
		if part == caseWhen && !parsed.isSimple && err == nil && !ok {
			ctx := rowStatementContextOrBackground(values)
			return e.evaluateRowPredicate(ctx, text, values), true, getExpressionFailure(ctx)
		}
		return value, ok, err
	})
}

// evaluateRowListComprehension evaluates a comprehension against typed row
// bindings. The loop value remains a node, relationship, map, path, or scalar;
// it is never converted to query text.
func (e *StorageExecutor) evaluateRowListComprehension(expr string, values map[string]interface{}) (interface{}, bool, bool, error) {
	inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']')
	if !enclosed {
		return nil, false, false, nil
	}
	variable, listExpression, predicate, projection, matched := parseListComprehension(inner)
	if !matched {
		return nil, false, false, nil
	}
	listValue, ok, err := e.evaluateRowValue(listExpression, values)
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
	// One scope for every element, as in evaluateRowQuantifier.
	scope := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		scope[name] = value
	}
	var holds func() (bool, bool, error)
	switch {
	case predicate != "" && containsRelationshipPattern(predicate):
		// A pattern predicate reads the graph: the row predicate evaluator
		// tests it as WHERE does (#907).
		holds = func() (bool, bool, error) {
			ctx := withExpressionFailureSlot(rowStatementContextOrBackground(scope))
			held := e.evaluateRowPredicateText(ctx, predicate, scope)
			return held, true, getExpressionFailure(ctx)
		}
	case predicate != "":
		holds = func() (bool, bool, error) {
			return rowConditionHolds(e.evaluateRowValue(predicate, scope))
		}
	}
	var project func() (interface{}, bool, error)
	if projection != "" {
		project = func() (interface{}, bool, error) { return e.evaluateRowValue(projection, scope) }
	}
	value, resolved, err := rowComprehensionLoop(coerceToUnwindItems(listValue), func(item interface{}) { scope[variable] = item }, holds, project)
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
	for index := 0; index < len(expression); index++ {
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			index = end - 1
			continue
		}
		current := expression[index]
		switch current {
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
func (e *StorageExecutor) evaluateRowQuantifier(expr string, values map[string]interface{}, evaluate rowValueEvaluator) (interface{}, bool, bool, error) {
	var callFacts2 functionCallFacts
	isFunction := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts2)
	function, inner := callFacts2.name, callFacts2.inner
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
	// One scope for every element, the variable rebound per element, as
	// reduce does: copying the row per element cost an allocation of the
	// whole row for each one (#1214).
	scope := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		scope[name] = value
	}
	value, resolved, err := rowQuantifierLoop(function, coerceToUnwindItems(listValue), func(item interface{}) { scope[variable] = item }, func() (interface{}, bool, error) {
		return evaluate(predicate, scope)
	})
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
	for end < len(expr) && cyphertext.IsWhitespaceAt(expr, end) {
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
	for index := start; index < len(chain); index++ {
		if end, quoted := scanCypherQuotedText(chain, index, chain[index]); quoted {
			index = end - 1
			continue
		}
		switch chain[index] {
		case '.':
			return index
		}
	}
	return len(chain)
}

func stripEnclosingExpressionParentheses(expr string) (string, bool) {
	return stripEnclosingRowDelimiter(expr, '(', ')')
}

// stripEnclosingRowDelimiter returns the inside of expr when expr is one
// open … close group: its first delimiter closes at its end, matched by
// cyphertext.FindMatchingDelimiter (quoted text, escaped quotes and comments skipped).
// For '[' it is isListLiteral.
func stripEnclosingRowDelimiter(expr string, open, close byte) (string, bool) {
	if len(expr) < 2 || expr[0] != open || expr[len(expr)-1] != close || cyphertext.FindMatchingDelimiter(expr, 0, rune(open), rune(close), cyphertext.OpeningDelimiter) != len(expr)-1 {
		return "", false
	}
	return strings.TrimSpace(expr[1 : len(expr)-1]), true
}
func rowSubscriptIndex(value interface{}) (int, bool) {
	// Out-of-bounds integer magnitudes clamp before a native-int conversion;
	// the list owner then returns null or clips a slice, never a wrapped index.
	return valueconvert.ToInt(value, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat|valueconvert.IntegerClampBounds)
}

// evaluateRowPredicate evaluates a WHERE predicate against a row.
//
// The operator scanners of evaluateRowPredicateParts are CASE-unaware (keeping
// the hot scan cheap), so a CASE block would have the AND / > of its WHEN
// conditions read as the predicate's own (#699). The blocks' values are bound
// first, once per predicate, as the context evaluator substitutes them
// (evaluateExpressionWithCASESubstituted).
func (e *StorageExecutor) evaluateRowPredicate(ctx context.Context, expression string, values map[string]interface{}) bool {
	return e.evaluateRowPredicateMode(ctx, expression, values)
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
	values, contextBound := rowValuesWithStatementContext(ctx, expression, values)
	if contextBound {
		defer delete(values, rowStatementContextKey)
	}
	return e.evaluateRowPredicateParts(ctx, expression, values)
}

// rowValuesWithStatementContext returns a row predicate's values with the
// statement context bound (rowStatementContextKey) when its functions or graph
// expressions need execution state, as evaluateRowExpressionWithContext binds
// it for a projection. The caller removes a newly bound context on return.
func rowValuesWithStatementContext(ctx context.Context, expression string, values map[string]interface{}) (map[string]interface{}, bool) {
	if _, bound := values[rowStatementContextKey]; bound || !rowPredicateNeedsStatementContext(expression) {
		return values, false
	}
	return rowValuesBindingStatementContext(ctx, values)
}

// rowValuesBindingStatementContext is values with the statement context
// bound for this synchronous evaluation. Rows belong to their evaluator; bind
// the private key in place rather than copying every row. The caller must defer
// deleting a newly bound key, including on errors or panics. An enclosing
// evaluation's existing binding is preserved and remains its owner's to remove.
func rowValuesBindingStatementContext(ctx context.Context, values map[string]interface{}) (map[string]interface{}, bool) {
	if _, bound := values[rowStatementContextKey]; bound {
		return values, false
	}
	if values == nil {
		values = make(map[string]interface{}, 1)
	}
	values[rowStatementContextKey] = ctx
	return values, true
}

// rowPredicateStatementContextNeeds caches rowPredicateNeedsStatementContext
// by predicate text: it is decided once, not once per row.
var rowPredicateStatementContextNeeds = newBoundedCache[string, bool](1024, boundedCacheStringShardIndex)

// rowPredicateNeedsStatementContext keeps the statement context available to
// every value operand, including a whole NOT EXISTS. Logical folds evaluate
// subqueries through the same row value owner as projections and CALL bodies.
func rowPredicateNeedsStatementContext(expression string) bool {
	if needs, cached := rowPredicateStatementContextNeeds.get(expression); cached {
		return needs
	}
	needs := needsRowStatementContext(expression)
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
		value, evaluated, err := e.evaluateRowValue(expression, values)
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
	if plan := planRowSubqueries(expression); plan != nil && !plan.logical {
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
	values, contextBound := rowValuesWithStatementContext(ctx, expression, values)
	if contextBound {
		defer delete(values, rowStatementContextKey)
	}
	value, evaluated, err := e.evaluateRowValue(expression, values)
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
	// OR, XOR, AND and NOT (evaluateLogicalExpression) over the operands'
	// values (rowPredicateValue); the result is read as a predicate.
	if value, logical, ok, err := evaluateLogicalExpression(expression, func(operand string) (interface{}, bool, error) {
		return e.evaluateRowLogicalPredicateOperand(ctx, operand, values, rowOperandRecordsFailure(operand) || isRelationshipPatternOperand(operand), func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
			return e.evaluateRowValue(operand, scope)
		}, true)
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
// predicate without a top-level logical operator.
func (e *StorageExecutor) evaluateRowPredicateTextFromSubqueries(ctx context.Context, expression string, values map[string]interface{}) bool {
	if plan := planRowSubqueries(expression); plan != nil {
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
		if left, nullTest := nullPredicateOperand(expression, strings.TrimSpace(operator)); nullTest {
			value, ok := e.rowPredicateOperand(ctx, left, values)
			if !ok {
				value = nil
			}
			return nullEvaluationHandler(operator == " IS NOT NULL").evaluate(value)
		}
	}
	comparisonResult, comparison, resolved, err := evaluateComparisonChain(expression, func(operand string) (interface{}, bool, error) {
		return e.evaluateRowLogicalPredicateOperand(ctx, operand, values, rowOperandRecordsFailure(operand) || isRelationshipPatternOperand(operand), func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
			return e.evaluateRowValue(operand, scope)
		}, false)
	}, compareCypherPredicateValue, cypherVersionFromContext(ctx) == "25", e.graphIdentityDomain())
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
	if nilRemotePath(value) {
		return nil, true, nil
	}
	if value == nil {
		return nil, true, nil
	}
	length, ok, err := evaluateCypherSizeInteger(value)
	if !ok || err != nil {
		return nil, ok, err
	}
	return length, true, nil
}

func evaluateCypherSizeInteger(value interface{}) (int64, bool, error) {
	if _, path := value.(storage.RemotePathValue); path {
		return 0, false, sizeArgumentError(value)
	}
	if text, isString := value.(string); isString {
		// RuneCountInString counts code points the way len([]rune(s)) does
		// (invalid UTF-8 bytes become U+FFFD), without the allocation.
		return int64(utf8.RuneCountInString(text)), true, nil
	}
	if sized, ok := value.(cypherfn.Sized); ok {
		return sized.CypherSize(), true, nil
	}
	valueType := reflect.TypeOf(value)
	if valueType == nil || (valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array) {
		if err := sizeArgumentError(value); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	return int64(reflect.ValueOf(value).Len()), true, nil
}

// rowPredicateValue is the value of an operand of a row predicate's AND,
// OR, XOR or NOT: its expression value (rowPredicateOperand), or, for a
// predicate form only the predicate evaluator reads (a pattern, EXISTS),
// whether it holds.
func (e *StorageExecutor) rowPredicateValue(ctx context.Context, operand string, values map[string]interface{}) interface{} {
	if value, resolved := e.rowPredicateOperand(ctx, operand, values); resolved {
		return value
	}
	return e.evaluateRowPredicateParts(ctx, operand, values)
}

// rowPredicateOperand evaluates an operand of a row predicate. An error is
// the statement's: it is recorded on ctx, and the operand is unresolved, so
// the predicate doesn't hold.
func (e *StorageExecutor) rowPredicateOperand(ctx context.Context, expr string, values map[string]interface{}) (interface{}, bool) {
	if isRelationshipPatternOperand(expr) {
		if matched, recognized := e.evaluateRowRelationshipPattern(ctx, expr, values); recognized {
			return matched, getExpressionFailure(ctx) == nil
		}
	}
	value, ok, err := e.evaluateRowValue(expr, values)
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
	return normalizeRowIdentityValues(leftValue, rightValue)
}

func isRowIdentityExpression(expression string) bool {
	var callFacts3 functionCallFacts
	ok := inspectFunctionCall(strings.TrimSpace(expression), qualifiedFunctionCall, "", &callFacts3)
	name := callFacts3.name
	return ok && (strings.EqualFold(name, "id") || strings.EqualFold(name, "elementId"))
}

// normalizeRowIdentityValues preserves complete element identities. The legacy
// raw-ID comparison extension applies only when one operand is unqualified;
// stripping two database-qualified IDs would collapse different constituents.
func normalizeRowIdentityValues(left, right interface{}, domains ...graphIdentityDomain) (interface{}, interface{}) {
	leftPayload, leftElement := rowIdentityPayload(left)
	rightPayload, rightElement := rowIdentityPayload(right)
	if leftElement && rightElement && (len(domains) == 0 || domains[0].preserveQualifiers(left, right)) {
		return left, right
	}
	return leftPayload, rightPayload
}

func rowIdentityPayload(value interface{}) (interface{}, bool) {
	text, ok := value.(string)
	if !ok {
		return value, false
	}
	if _, _, payload, found := storage.ParseElementID(text); found {
		return payload, true
	}
	return text, false
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
	result, err := e.relationshipEndpointValue(value, equalFoldASCII(function, "endNode"))
	return result, true, err
}

func (e *StorageExecutor) relationshipEndpointValue(value interface{}, end bool) (interface{}, error) {
	edge, err := relationshipEndpointArgument(value, end)
	if err != nil {
		return nil, err
	}
	store, err := e.FunctionStorage(e.databaseName())
	if err != nil {
		return nil, err
	}
	return e.relationshipEndpoint(edge, end, store), nil
}

// relationshipEndpoint is startNode(edge) / endNode(edge) (end): the
// endpoint read by its id, null when it is gone. The one implementation, for
// the row evaluator (rowRelationshipEndpoint) and the context evaluator
// (evaluateExpressionWithContextFullMath).
func (e *StorageExecutor) relationshipEndpoint(edge *storage.Edge, end bool, stores ...storage.Engine) interface{} {
	return e.relationshipEndpointWithProperties(edge, end, nil, stores...)
}

func (e *StorageExecutor) relationshipEndpointWithProperties(edge *storage.Edge, end bool, properties []string, stores ...storage.Engine) interface{} {
	store := e.storage
	if len(stores) > 0 {
		store = stores[0]
	}
	id, exists := e.relationshipEndpointID(edge, end, store)
	if !exists {
		return nil
	}
	return e.readRelationshipEndpoint(id, properties, store)
}

func (e *StorageExecutor) relationshipEndpointID(edge *storage.Edge, end bool, stores ...storage.Engine) (storage.NodeID, bool) {
	if edge == nil {
		return "", false
	}
	store := e.storage
	if len(stores) > 0 {
		store = stores[0]
	}
	id := edge.StartNode
	if end {
		id = edge.EndNode
	}
	edgeID, edgeSelected := edge.Identity().LookupID(store, "5")
	nodeID, nodeSelected := (storage.EntityIdentity{Database: edge.Database, LocalID: string(id)}).NodeLookupID(store)
	if !edgeSelected || !nodeSelected {
		return "", false
	}
	if current, err := store.GetEdge(storage.EdgeID(edgeID)); err != nil || current == nil {
		return "", false
	}
	return nodeID, true
}

func (e *StorageExecutor) readRelationshipEndpoint(id storage.NodeID, properties []string, stores ...storage.Engine) interface{} {
	store := e.storage
	if len(stores) > 0 {
		store = stores[0]
	}
	node, err := e.newRelationshipEndpointReader(store).read(id, properties)
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
		if end, quoted := skipCypherLexicalToken(expression, i); quoted {
			i = end - 1
			continue
		}
		switch c := expression[i]; c {
		case '(':
			if _, chain := relationshipChainEnd(expression, i, len(expression)); chain {
				return true
			}
		}
	}
	return false
}

func (e *StorageExecutor) evaluateRowMembershipValue(left, right string, values map[string]interface{}) (interface{}, bool, error) {
	needle, leftOK, err := e.evaluateRowValue(left, values)
	if err != nil {
		return nil, false, err
	}
	haystack, rightOK, err := e.evaluateRowValue(right, values)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	value, ok := rowMembershipOfValues(needle, haystack, isRowIdentityExpression(left), e.graphIdentityDomain())
	return value, ok, nil
}

// rowMembershipOfValues is `needle IN haystack` for evaluated operands, with
// Cypher's three-valued rules. identity is set when the needle is an id() /
// elementId() expression, compared by its identity payload.
func rowMembershipOfValues(needle, haystack interface{}, identity bool, domains ...graphIdentityDomain) (interface{}, bool) {
	if haystack == nil {
		return nil, true
	}
	items := coerceToUnwindItems(haystack)
	if len(items) == 0 {
		return false, true
	}
	if needle == nil {
		return nil, true
	}
	containsUnknown := false
	for _, item := range items {
		comparisonNeedle := needle
		if identity {
			comparisonNeedle, item = normalizeRowIdentityValues(needle, item, domains...)
		}
		equal := cypherEquality(comparisonNeedle, item)
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

func (e *StorageExecutor) evaluateRowStringPredicate(ctx context.Context, left, right string, values map[string]interface{}, predicate func(string, string) bool) bool {
	leftValue, leftOK := e.rowPredicateOperand(ctx, left, values)
	rightValue, rightOK := e.rowPredicateOperand(ctx, right, values)
	return leftOK && rightOK && evaluateRowStringPredicateValues(leftValue, rightValue, predicate)
}

func evaluateRowStringPredicateValues(left, right interface{}, predicate func(string, string) bool) bool {
	matched, _ := rowStringPredicateValue(left, right, predicate).(bool)
	return matched
}
