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
	kind     rowOperandKind
	literal  interface{}
	variable string
	chain    string
}

// resolve returns the operand's value for the row. ok is false when the row
// doesn't bind the operand's variable or parameter; the caller then evaluates
// the part as text, as evaluateRowExpression would resolve it further.
func (o rowOperand) resolve(values map[string]interface{}) (interface{}, bool) {
	switch o.kind {
	case rowOperandLiteral:
		return o.literal, true
	case rowOperandPropertyChain:
		base, bound := values[o.variable]
		if !bound {
			return nil, false
		}
		return evaluateRowPropertyChain(base, o.chain)
	default:
		value, bound := values[o.variable]
		return value, bound
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
		return rowOperand{kind: rowOperandPropertyChain, variable: variable, chain: chain}, true
	}
	if isValidIdentifier(text) {
		return rowOperand{kind: rowOperandVariable, variable: text}, true
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
	kind     rowPredicatePartKind
	text     string
	left     rowOperand
	right    rowOperand
	operator string
	parts    []rowPredicatePart
}

// rowPredicatePlan is a planned predicate.
type rowPredicatePlan struct {
	root rowPredicatePart
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
			plan = &rowPredicatePlan{root: root}
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
	if strings.ContainsAny(text, "()[]:`'\"") || hasPrefixFoldASCII(text, "NOT ") {
		return rowPredicatePart{}, false
	}
	upper := strings.ToUpper(text)
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
		return rowPredicatePart{kind: rowPredicateIn, text: text, left: needle, right: haystack}, true
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
	return rowPredicatePart{kind: rowPredicateComparison, text: text, left: left, right: right, operator: operator}, true
}

// evaluateRowPredicatePlan evaluates a planned predicate for a row.
func (e *StorageExecutor) evaluateRowPredicatePlan(ctx context.Context, plan *rowPredicatePlan, values map[string]interface{}) bool {
	return e.evaluateRowPredicatePart(ctx, &plan.root, values)
}

func (e *StorageExecutor) evaluateRowPredicatePart(ctx context.Context, part *rowPredicatePart, values map[string]interface{}) bool {
	switch part.kind {
	case rowPredicateAnd:
		for i := range part.parts {
			if !e.evaluateRowPredicatePart(ctx, &part.parts[i], values) {
				return false
			}
		}
		return true
	case rowPredicateOr:
		for i := range part.parts {
			if e.evaluateRowPredicatePart(ctx, &part.parts[i], values) {
				return true
			}
		}
		return false
	case rowPredicateComparison:
		left, leftOK := part.left.resolve(values)
		right, rightOK := part.right.resolve(values)
		if !leftOK || !rightOK {
			return e.evaluateRowPredicateText(ctx, part.text, values)
		}
		// A null operand makes the comparison null, which doesn't hold, as in
		// evaluateComparisonChain.
		if left == nil || right == nil {
			return false
		}
		matched, known := compareCypherPredicateValue(left, right, part.operator).(bool)
		return known && matched
	case rowPredicateIn:
		needle, needleOK := part.left.resolve(values)
		haystack, haystackOK := part.right.resolve(values)
		if !needleOK || !haystackOK {
			return e.evaluateRowPredicateText(ctx, part.text, values)
		}
		member, ok := rowMembershipOfValues(needle, haystack, false)
		return ok && member == true
	case rowPredicateIsNull, rowPredicateIsNotNull:
		value, ok := part.left.resolve(values)
		if !ok {
			return e.evaluateRowPredicateText(ctx, part.text, values)
		}
		if part.kind == rowPredicateIsNull {
			return value == nil
		}
		return value != nil
	default:
		// Text parts go through the whole row predicate evaluator: they have
		// no plan of their own, so this doesn't come back here.
		return e.evaluateRowPredicate(ctx, part.text, values)
	}
}
