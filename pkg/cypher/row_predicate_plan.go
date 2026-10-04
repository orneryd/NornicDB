package cypher

import (
	"context"
	"strings"
)

// A row predicate plan is a WHERE predicate parsed once per text instead of
// once per row: its top-level AND parts, each either a comparison of two
// simple operands, an IS [NOT] NULL test of one, or text evaluated by
// evaluateRowPredicate. It is a planner over the row evaluator, not a second
// evaluator: operands resolve as evaluateRowExpression resolves them,
// comparisons go through compareCypherPredicateValue with the same null
// rules as evaluateComparisonChain, and a row whose operand the plan can't
// resolve directly (an unbound name) evaluates that part as text.

type rowOperandKind uint8

const (
	rowOperandLiteral rowOperandKind = iota
	rowOperandVariable
	rowOperandParameter
	rowOperandPropertyChain
)

// rowOperand is a simple comparison operand: a literal, a row variable, a
// $parameter (bound in the row as "$name"), or a property chain on a row
// variable (n.a.b).
type rowOperand struct {
	kind           rowOperandKind
	literal        interface{}
	variable       string
	chain          string
	directProperty bool
	compiled       *compiledRowOperand
}

type compiledRowOperand struct {
	left, right rowOperand
	operator    byte
}

type plannedRowValue struct {
	value     interface{}
	integer   int64
	isInteger bool
}

type compiledRowScope struct {
	values     map[string]interface{}
	nodes      binding
	rels       relationshipBinding
	parameters map[string]interface{}
}

func (scope compiledRowScope) lookup(name string) (interface{}, bool) {
	if len(name) > 1 && name[0] == '$' {
		if value, ok := scope.parameters[name[1:]]; ok {
			return value, true
		}
	}
	if scope.values != nil {
		if value, ok := scope.values[name]; ok {
			return value, true
		}
	}
	if len(name) > 1 && name[0] == '$' {
		value, ok := scope.parameters[name[1:]]
		return value, ok
	}
	if node, ok := scope.nodes[name]; ok {
		if node == nil {
			return nil, true
		}
		return node, true
	}
	if edge, ok := scope.rels[name]; ok {
		if edge == nil {
			return nil, true
		}
		return edge, true
	}
	return nil, false
}

func (scope compiledRowScope) materialize() map[string]interface{} {
	if scope.nodes == nil && scope.rels == nil && scope.parameters == nil {
		return scope.values
	}
	values := make(map[string]interface{}, len(scope.values)+len(scope.nodes)+len(scope.rels)+len(scope.parameters))
	for name, value := range scope.values {
		values[name] = value
	}
	for name, value := range scope.nodes {
		values[name] = value
	}
	for name, value := range scope.rels {
		values[name] = value
	}
	for name, value := range scope.parameters {
		values["$"+name] = value
	}
	return values
}

func (value plannedRowValue) materialize() interface{} {
	if value.isInteger {
		return value.integer
	}
	return value.value
}

func (operand *rowOperand) evaluate(e *StorageExecutor, scope compiledRowScope) (plannedRowValue, bool, error) {
	if operand.compiled == nil {
		value, ok := operand.resolveScope(scope)
		if !ok && operand.kind == rowOperandPropertyChain {
			if base, bound := scope.lookup(operand.variable); bound {
				return plannedRowValue{}, false, rowPropertyChainTypeError(base, operand.chain)
			}
		}
		integer, isInteger := cypherIntegerOperand(value)
		return plannedRowValue{value: value, integer: integer, isInteger: isInteger}, ok, nil
	}
	left, leftOK, err := operand.compiled.left.evaluate(e, scope)
	if err != nil || !leftOK {
		return plannedRowValue{}, leftOK, err
	}
	if operand.compiled.operator == 's' {
		if left.value == nil && !left.isInteger {
			return plannedRowValue{}, true, nil
		}
		integer, ok, err := evaluateCypherSizeInteger(left.materialize())
		return plannedRowValue{integer: integer, isInteger: ok}, ok, err
	}
	right, rightOK, err := operand.compiled.right.evaluate(e, scope)
	if err != nil || !rightOK {
		return plannedRowValue{}, rightOK, err
	}
	if left.isInteger && right.isInteger {
		result, ok, err := exactIntegerArithmetic(operand.compiled.operator, left.integer, right.integer)
		if err != nil {
			return plannedRowValue{}, false, err
		}
		if ok {
			return plannedRowValue{integer: result, isInteger: true}, true, nil
		}
		if operand.compiled.operator == '/' || operand.compiled.operator == '%' {
			return plannedRowValue{}, false, divisionByZeroError()
		}
	}
	value, ok, err := e.evaluateRowArithmeticValues(operand.compiled.operator, left.materialize(), right.materialize())
	integer, isInteger := cypherIntegerOperand(value)
	return plannedRowValue{value: value, integer: integer, isInteger: isInteger}, ok, err
}

type comparisonEvaluationHandler string

func (handler comparisonEvaluationHandler) evaluate(left, right interface{}) interface{} {
	return compareCypherPredicateValue(left, right, string(handler))
}

type nullEvaluationHandler bool

func (handler nullEvaluationHandler) evaluate(value interface{}) bool {
	return (value != nil) == bool(handler)
}

// resolve returns the operand's value for the row. ok is false when the row
// doesn't bind the operand's variable or parameter; the caller then evaluates
// the part as text, as evaluateRowExpression would resolve it further.
func (o *rowOperand) resolve(values map[string]interface{}) (interface{}, bool) {
	return o.resolveScope(compiledRowScope{values: values})
}

func (o *rowOperand) resolveScope(scope compiledRowScope) (interface{}, bool) {
	switch o.kind {
	case rowOperandLiteral:
		return o.literal, true
	case rowOperandPropertyChain:
		base, bound := scope.lookup(o.variable)
		if !bound {
			return nil, false
		}
		if o.directProperty {
			return rowPropertyValue(base, o.chain)
		}
		return evaluateRowPropertyChain(base, o.chain)
	default:
		return scope.lookup(o.variable)
	}
}

// parseRowOperand classifies text as a simple operand.
func parseRowOperand(text string) (rowOperand, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return rowOperand{}, false
	}
	if value, ok := parseLiteralValueFromComputedRow(text); ok {
		switch value.(type) {
		case nil, bool, string, int64, float64:
			return rowOperand{kind: rowOperandLiteral, literal: value}, true
		}
		return rowOperand{}, false
	}
	if text[0] == '$' {
		if isValidIdentifier(text[1:]) {
			return rowOperand{kind: rowOperandParameter, variable: text}, true
		}
		return rowOperand{}, false
	}
	if variable, chain, ok := rowPropertyChainShape(text); ok {
		return rowOperand{kind: rowOperandPropertyChain, variable: variable, chain: chain, directProperty: isValidIdentifier(chain)}, true
	}
	if isValidIdentifier(text) {
		return rowOperand{kind: rowOperandVariable, variable: text}, true
	}
	return rowOperand{}, false
}

func parseCompiledRowOperand(text string) (rowOperand, bool) {
	text = strings.TrimSpace(text)
	if operand, ok := parseRowOperand(text); ok {
		return operand, true
	}
	if inner, enclosed := stripEnclosingExpressionParentheses(text); enclosed {
		return parseCompiledRowOperand(inner)
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		if left, right, operator, split := splitRowArithmeticTier(text, tier); split {
			leftOperand, leftOK := parseCompiledRowOperand(left)
			rightOperand, rightOK := parseCompiledRowOperand(right)
			if !leftOK || !rightOK {
				return rowOperand{}, false
			}
			return rowOperand{compiled: &compiledRowOperand{left: leftOperand, right: rightOperand, operator: operator}}, true
		}
	}
	if function, argument, call := parseFunctionCallWS(text); call && equalFoldASCII(function, "size") {
		operand, ok := parseCompiledRowOperand(argument)
		if ok {
			return rowOperand{compiled: &compiledRowOperand{left: operand, operator: 's'}}, true
		}
	}
	return rowOperand{}, false
}

type rowPredicatePartKind uint8

const (
	rowPredicateText rowPredicatePartKind = iota
	rowPredicateComparison
	rowPredicateIsNull
	rowPredicateIsNotNull
	rowPredicateAnd
	rowPredicateOr
	rowPredicateIn
)

// rowPredicatePart is a node of a planned predicate: an AND or OR of parts, a
// comparison or null test of simple operands, or text.
type rowPredicatePart struct {
	kind       rowPredicatePartKind
	text       string
	left       rowOperand
	right      rowOperand
	operator   comparisonEvaluationHandler
	parts      []rowPredicatePart
	membership *bindingParamMembershipCache
}

// rowPredicatePlan is a planned predicate.
type rowPredicatePlan struct {
	root rowPredicatePart
	// complete: no part of the plan is text. Such a predicate is only
	// comparisons, IN and null tests of simple operands under AND / OR, so
	// none of evaluateRowPredicate's other forms (CASE, arithmetic,
	// subqueries, label tests) applies to it, and it is evaluated from the
	// plan without scanning its text for them on every row.
	complete bool
}

// complete reports whether part and every part below it is planned.
func (part *rowPredicatePart) complete() bool {
	if part.kind == rowPredicateText {
		return false
	}
	for i := range part.parts {
		if !part.parts[i].complete() {
			return false
		}
	}
	return true
}

// rowPredicatePlans caches plans by predicate text; a nil plan (a predicate
// with nothing to plan) is cached too.
var rowPredicatePlans = newBoundedCache[string, *rowPredicatePlan](1024)

// planRowPredicate returns the plan of a predicate that has at least one
// comparison or null test of simple operands, combined with AND / OR, and nil
// for any other predicate (one with a subquery or braces, or nothing to plan).
func planRowPredicate(expression string) *rowPredicatePlan {
	if plan, cached := rowPredicatePlans.get(expression); cached {
		return plan
	}
	var plan *rowPredicatePlan
	if !strings.ContainsAny(expression, "{}") {
		if root, planned := planRowPredicatePart(expression); planned {
			plan = &rowPredicatePlan{root: root, complete: root.complete()}
		}
	}
	rowPredicatePlans.put(expression, plan)
	return plan
}

// planRowPredicatePart plans text in the order evaluateRowPredicate evaluates
// it: enclosing parentheses, then a label test (kept as text), then a
// top-level OR, then a top-level AND, then a comparison or null test of
// simple operands. Anything else is text. planned reports whether the part,
// or one below it, is not text.
func planRowPredicatePart(text string) (rowPredicatePart, bool) {
	text = strings.TrimSpace(text)
	textPart := rowPredicatePart{kind: rowPredicateText, text: text}
	if text == "" {
		return textPart, false
	}
	if inner, ok := stripEnclosingExpressionParentheses(text); ok {
		part, planned := planRowPredicatePart(inner)
		if !planned {
			return textPart, false
		}
		return part, true
	}
	if _, _, ok := parseWithWhereLabelTest(text); ok {
		return textPart, false
	}
	if _, _, split := splitByOperatorWithOptions(text, " XOR ", true, true); split {
		return textPart, false
	}
	for _, logical := range []struct {
		operator string
		kind     rowPredicatePartKind
	}{{" OR ", rowPredicateOr}, {" AND ", rowPredicateAnd}} {
		if _, _, split := splitByOperatorWithOptions(text, logical.operator, true, true); !split {
			continue
		}
		node := rowPredicatePart{kind: logical.kind, text: text}
		planned := false
		rest := text
		for {
			left, right, split := splitByOperatorWithOptions(rest, logical.operator, true, true)
			if !split {
				left = rest
			}
			part, partPlanned := planRowPredicatePart(left)
			planned = planned || partPlanned
			node.parts = append(node.parts, part)
			if !split {
				break
			}
			rest = right
		}
		if !planned {
			return textPart, false
		}
		return node, true
	}
	if leaf, ok := planRowPredicateLeaf(text); ok {
		return leaf, true
	}
	return textPart, false
}

// planRowPredicateLeaf plans a comparison of two simple operands or a null test
// of one. A shape one of evaluateRowPredicate's earlier branches handles (NOT,
// EXISTS, IN, string operators, =~, labels, calls, lists, strings) isn't one.
func planRowPredicateLeaf(text string) (rowPredicatePart, bool) {
	if !hasPrefixFoldASCII(text, "NOT ") && mayContainArithmetic(text) {
		if scan, ok := scanComparisonChain(text); ok && scan.count == 1 {
			span := scan.operator(0)
			operator := text[span.offset : span.offset+span.length]
			if operator != "=~" {
				left, leftOK := parseCompiledRowOperand(scan.operand(text, 0))
				right, rightOK := parseCompiledRowOperand(scan.operand(text, 1))
				if leftOK && rightOK {
					return rowPredicatePart{kind: rowPredicateComparison, text: text, left: left, right: right, operator: comparisonEvaluationHandler(operator)}, true
				}
			}
		}
	}
	if part, ok := planRowLiteralListMembership(text); ok {
		return part, true
	}
	if strings.ContainsAny(text, "'\"") {
		return planRowStringComparison(text)
	}
	if strings.ContainsAny(text, "()[]:`") || hasPrefixFoldASCII(text, "NOT ") {
		return rowPredicatePart{}, false
	}
	upper := upperASCII(text)
	for _, keyword := range []string{" NOT IN ", " STARTS WITH ", " ENDS WITH ", " CONTAINS ", "=~", "EXISTS", "COUNT", "COLLECT"} {
		if strings.Contains(upper, keyword) {
			return rowPredicatePart{}, false
		}
	}
	if left, right, ok := splitByOperatorWithOptions(text, " IN ", true, true); ok {
		needle, needleOK := parseRowOperand(left)
		haystack, haystackOK := parseRowOperand(right)
		if !needleOK || !haystackOK || needle.kind == rowOperandLiteral || haystack.kind == rowOperandLiteral {
			return rowPredicatePart{}, false
		}
		return rowPredicatePart{kind: rowPredicateIn, text: text, left: needle, right: haystack, membership: &bindingParamMembershipCache{length: -1}}, true
	}
	for _, test := range []struct {
		suffix string
		kind   rowPredicatePartKind
	}{{" IS NOT NULL", rowPredicateIsNotNull}, {" IS NULL", rowPredicateIsNull}} {
		if hasSuffixFoldASCII(text, test.suffix) {
			operand, ok := parseRowOperand(text[:len(text)-len(test.suffix)])
			if !ok || operand.kind == rowOperandLiteral {
				return rowPredicatePart{}, false
			}
			return rowPredicatePart{kind: test.kind, text: text, left: operand}, true
		}
	}
	operands, operators, ok := splitComparisonChain(text)
	if !ok || len(operands) != 2 || len(operators) != 1 {
		return rowPredicatePart{}, false
	}
	left, leftOK := parseRowOperand(operands[0])
	right, rightOK := parseRowOperand(operands[1])
	if !leftOK || !rightOK {
		return rowPredicatePart{}, false
	}
	operator := operators[0]
	if operator == "!=" {
		operator = "<>"
	}
	return rowPredicatePart{kind: rowPredicateComparison, text: text, left: left, right: right, operator: comparisonEvaluationHandler(operator)}, true
}

// planRowLiteralListMembership plans <operand> IN [<literal>, …]: a variable,
// parameter or property chain tested against a list of scalar literals (the
// form a list parameter takes once the statement's parameters are written
// into its text). The list is parsed once, into its values; evaluated as
// text, it was scanned and split again for every row. A list with anything
// but scalar literals in it isn't planned.
func planRowLiteralListMembership(text string) (rowPredicatePart, bool) {
	if len(text) == 0 || text[len(text)-1] != ']' {
		return rowPredicatePart{}, false
	}
	in := strings.Index(upperASCII(text), " IN ")
	if in <= 0 {
		return rowPredicatePart{}, false
	}
	left := strings.TrimSpace(text[:in])
	right := strings.TrimSpace(text[in+len(" IN "):])
	if strings.ContainsAny(left, "()[]:`'\"") || hasPrefixFoldASCII(left, "NOT ") || hasSuffixFoldASCII(left, " NOT") {
		return rowPredicatePart{}, false
	}
	needle, ok := parseRowOperand(left)
	if !ok || needle.kind == rowOperandLiteral {
		return rowPredicatePart{}, false
	}
	if len(right) < 2 || right[0] != '[' {
		return rowPredicatePart{}, false
	}
	values := []interface{}{}
	if inner := strings.TrimSpace(right[1 : len(right)-1]); inner != "" {
		items := splitTopLevelComma(inner)
		values = make([]interface{}, 0, len(items))
		for _, item := range items {
			item = strings.TrimSpace(item)
			if strings.ContainsAny(item, "'\"") && !isWholeCypherQuotedString(item) {
				return rowPredicatePart{}, false
			}
			value, ok := parseLiteralScalarForPipeline(item)
			if !ok {
				return rowPredicatePart{}, false
			}
			values = append(values, value)
		}
	}
	return rowPredicatePart{kind: rowPredicateIn, text: text, left: needle, right: rowOperand{kind: rowOperandLiteral, literal: values}, membership: &bindingParamMembershipCache{length: -1}}, true
}

// planRowStringComparison plans a comparison of a simple operand with a
// string literal (n.name = 'Ada', 'a' < n.k). Each quoted operand is one
// whole string literal, so nothing in it is predicate syntax, whatever it
// contains; the other operand is a variable, parameter, property chain or
// literal. Any other text with a quote in it isn't planned.
func planRowStringComparison(text string) (rowPredicatePart, bool) {
	operands, operators, ok := splitComparisonChain(text)
	if !ok || len(operands) != 2 || len(operators) != 1 {
		return rowPredicatePart{}, false
	}
	var sides [2]rowOperand
	for index, operand := range operands {
		operand = strings.TrimSpace(operand)
		if strings.ContainsAny(operand, "'\"") {
			if !isWholeCypherQuotedString(operand) {
				return rowPredicatePart{}, false
			}
		} else if strings.ContainsAny(operand, "()[]:`") || hasPrefixFoldASCII(operand, "NOT ") {
			return rowPredicatePart{}, false
		}
		side, ok := parseRowOperand(operand)
		if !ok {
			return rowPredicatePart{}, false
		}
		sides[index] = side
	}
	operator := operators[0]
	switch operator {
	case "!=":
		operator = "<>"
	case "=", "<>", "<", ">", "<=", ">=":
	default:
		// =~ and any other operator the chain scanner knows is evaluated as
		// text.
		return rowPredicatePart{}, false
	}
	return rowPredicatePart{kind: rowPredicateComparison, text: text, left: sides[0], right: sides[1], operator: comparisonEvaluationHandler(operator)}, true
}

// evaluateRowPredicatePlan evaluates a planned predicate for a row.
type compiledRowMembership struct {
	part          *rowPredicatePart
	comparable    map[interface{}]struct{}
	nonComparable []interface{}
	hasNull       bool
}

type compiledRowMemberships struct {
	inline   [4]compiledRowMembership
	overflow []compiledRowMembership
	count    int
}

func (memberships *compiledRowMemberships) prepare(part *rowPredicatePart, scope compiledRowScope) {
	if part.membership != nil {
		if value, ok := part.right.resolveScope(scope); ok {
			if items, ok := toInterfaceSlice(value); ok {
				comparable, nonComparable, hasNull := part.membership.getValidated(items)
				entry := compiledRowMembership{part: part, comparable: comparable, nonComparable: nonComparable, hasNull: hasNull}
				if memberships.count < len(memberships.inline) {
					memberships.inline[memberships.count] = entry
				} else {
					memberships.overflow = append(memberships.overflow, entry)
				}
				memberships.count++
			}
		}
	}
	for index := range part.parts {
		memberships.prepare(&part.parts[index], scope)
	}
}

func (memberships *compiledRowMemberships) find(part *rowPredicatePart) *compiledRowMembership {
	if memberships == nil {
		return nil
	}
	for index := 0; index < memberships.count && index < len(memberships.inline); index++ {
		if memberships.inline[index].part == part {
			return &memberships.inline[index]
		}
	}
	for index := range memberships.overflow {
		if memberships.overflow[index].part == part {
			return &memberships.overflow[index]
		}
	}
	return nil
}

func (e *StorageExecutor) evaluateRowPredicatePlan(ctx context.Context, plan *rowPredicatePlan, values map[string]interface{}) bool {
	return e.evaluateRowPredicatePart(ctx, &plan.root, values)
}

func (e *StorageExecutor) evaluateRowPredicatePart(ctx context.Context, part *rowPredicatePart, values map[string]interface{}) bool {
	return e.evaluateRowPredicatePartScope(ctx, part, compiledRowScope{values: values}, nil)
}

func (e *StorageExecutor) evaluateRowPredicatePartScope(ctx context.Context, part *rowPredicatePart, scope compiledRowScope, memberships *compiledRowMemberships) bool {
	switch part.kind {
	case rowPredicateAnd:
		for i := range part.parts {
			if !e.evaluateRowPredicatePartScope(ctx, &part.parts[i], scope, memberships) {
				return false
			}
		}
		return true
	case rowPredicateOr:
		for i := range part.parts {
			if e.evaluateRowPredicatePartScope(ctx, &part.parts[i], scope, memberships) {
				return true
			}
		}
		return false
	case rowPredicateComparison:
		if part.left.compiled != nil || part.right.compiled != nil {
			left, leftOK, err := part.left.evaluate(e, scope)
			if err != nil {
				recordExpressionFailure(ctx, err)
				return false
			}
			right, rightOK, err := part.right.evaluate(e, scope)
			if err != nil {
				recordExpressionFailure(ctx, err)
				return false
			}
			if !leftOK || !rightOK {
				return e.evaluateRowPredicateText(ctx, part.text, scope.materialize())
			}
			if left.isInteger && right.isInteger {
				switch part.operator {
				case "=":
					return left.integer == right.integer
				case "<>", "!=":
					return left.integer != right.integer
				case "<":
					return left.integer < right.integer
				case ">":
					return left.integer > right.integer
				case "<=":
					return left.integer <= right.integer
				case ">=":
					return left.integer >= right.integer
				}
			}
			matched, known := part.operator.evaluate(left.materialize(), right.materialize()).(bool)
			return known && matched
		}
		left, leftOK := part.left.resolveScope(scope)
		right, rightOK := part.right.resolveScope(scope)
		if !leftOK || !rightOK {
			return e.evaluateRowPredicateText(ctx, part.text, scope.materialize())
		}
		if left == nil || right == nil {
			return false
		}
		matched, known := part.operator.evaluate(left, right).(bool)
		return known && matched
	case rowPredicateIn:
		needle, needleOK := part.left.resolveScope(scope)
		haystack, haystackOK := part.right.resolveScope(scope)
		if !needleOK || !haystackOK {
			return e.evaluateRowPredicateText(ctx, part.text, scope.materialize())
		}
		if part.membership != nil {
			switch needle.(type) {
			case string, bool:
				if prepared := memberships.find(part); prepared != nil && len(prepared.nonComparable) == 0 {
					return membershipTruth(needle, prepared.comparable, nil, prepared.hasNull, e.compareBindingValuesEqual) == truthTrue
				}
				if items, ok := toInterfaceSlice(haystack); ok {
					comparable, nonComparable, hasNull := part.membership.get(items, firstInterfaceElement(items))
					if len(nonComparable) == 0 {
						return membershipTruth(needle, comparable, nil, hasNull, e.compareBindingValuesEqual) == truthTrue
					}
				}
			}
		}
		member, ok := rowMembershipOfValues(needle, haystack, false)
		return ok && member == true
	case rowPredicateIsNull, rowPredicateIsNotNull:
		value, ok := part.left.resolveScope(scope)
		if !ok {
			return e.evaluateRowPredicateText(ctx, part.text, scope.materialize())
		}
		return nullEvaluationHandler(part.kind == rowPredicateIsNotNull).evaluate(value)
	default:
		// Text parts go through the whole row predicate evaluator: they have
		// no plan of their own, so this doesn't come back here.
		return e.evaluateRowPredicateMode(ctx, part.text, scope.materialize())
	}
}
