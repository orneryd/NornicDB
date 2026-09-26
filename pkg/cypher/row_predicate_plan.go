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
)

// rowPredicatePart is one top-level AND part of a planned predicate.
type rowPredicatePart struct {
	kind     rowPredicatePartKind
	text     string
	left     rowOperand
	right    rowOperand
	operator string
}

// rowPredicatePlan is a planned predicate: its AND parts, in order.
type rowPredicatePlan struct {
	parts []rowPredicatePart
}

// rowPredicatePlans caches plans by predicate text; a nil plan (a predicate
// with nothing to plan) is cached too.
var rowPredicatePlans = newBoundedCache[string, *rowPredicatePlan](1024)

// planRowPredicate returns the plan of a predicate that is AND parts at least
// one of which is a comparison or null test of simple operands, and nil for
// any other predicate: one with a top-level OR / XOR, a subquery or braces,
// or no part the plan can evaluate without the text.
func planRowPredicate(expression string) *rowPredicatePlan {
	if plan, cached := rowPredicatePlans.get(expression); cached {
		return plan
	}
	plan := buildRowPredicatePlan(expression)
	rowPredicatePlans.put(expression, plan)
	return plan
}

func buildRowPredicatePlan(expression string) *rowPredicatePlan {
	if strings.ContainsAny(expression, "{}") {
		return nil
	}
	for _, operator := range []string{" OR ", " XOR "} {
		if _, _, split := splitByOperatorWithOptions(expression, operator, true, true); split {
			return nil
		}
	}
	var conjuncts []string
	rest := expression
	for {
		left, right, split := splitByOperatorWithOptions(rest, " AND ", true, true)
		if !split {
			conjuncts = append(conjuncts, strings.TrimSpace(rest))
			break
		}
		conjuncts = append(conjuncts, strings.TrimSpace(left))
		rest = right
	}
	plan := &rowPredicatePlan{parts: make([]rowPredicatePart, 0, len(conjuncts))}
	planned := false
	for _, conjunct := range conjuncts {
		part := planRowPredicatePart(conjunct)
		if part.kind != rowPredicateText {
			planned = true
		}
		plan.parts = append(plan.parts, part)
	}
	if !planned {
		return nil
	}
	return plan
}

// planRowPredicatePart classifies one AND part. Only shapes evaluateRowPredicate
// would evaluate as a plain comparison or null test are planned; anything
// that one of its earlier branches handles (parentheses, labels, NOT, EXISTS,
// IN, string operators, =~) stays text.
func planRowPredicatePart(conjunct string) rowPredicatePart {
	text := rowPredicatePart{kind: rowPredicateText, text: conjunct}
	if conjunct == "" || strings.ContainsAny(conjunct, "()[]:`'\"") || hasPrefixFoldASCII(conjunct, "NOT ") {
		return text
	}
	upper := strings.ToUpper(conjunct)
	for _, keyword := range []string{" IN ", " STARTS WITH ", " ENDS WITH ", " CONTAINS ", "=~", "EXISTS", "COUNT", "COLLECT"} {
		if strings.Contains(upper, keyword) {
			return text
		}
	}
	for _, test := range []struct {
		suffix string
		kind   rowPredicatePartKind
	}{{" IS NOT NULL", rowPredicateIsNotNull}, {" IS NULL", rowPredicateIsNull}} {
		if hasSuffixFoldASCII(conjunct, test.suffix) {
			operand, ok := parseRowOperand(conjunct[:len(conjunct)-len(test.suffix)])
			if !ok || operand.kind == rowOperandLiteral {
				return text
			}
			return rowPredicatePart{kind: test.kind, text: conjunct, left: operand}
		}
	}
	operands, operators, ok := splitComparisonChain(conjunct)
	if !ok || len(operands) != 2 || len(operators) != 1 {
		return text
	}
	left, leftOK := parseRowOperand(operands[0])
	right, rightOK := parseRowOperand(operands[1])
	if !leftOK || !rightOK {
		return text
	}
	operator := operators[0]
	if operator == "!=" {
		operator = "<>"
	}
	return rowPredicatePart{kind: rowPredicateComparison, text: conjunct, left: left, right: right, operator: operator}
}

// evaluateRowPredicatePlan evaluates a planned predicate for a row: its AND
// parts in order, stopping at the first that doesn't hold.
func (e *StorageExecutor) evaluateRowPredicatePlan(ctx context.Context, plan *rowPredicatePlan, values map[string]interface{}) bool {
	for i := range plan.parts {
		if !e.evaluateRowPredicatePart(ctx, &plan.parts[i], values) {
			return false
		}
	}
	return true
}

func (e *StorageExecutor) evaluateRowPredicatePart(ctx context.Context, part *rowPredicatePart, values map[string]interface{}) bool {
	switch part.kind {
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
		return e.evaluateRowPredicateText(ctx, part.text, values)
	}
}
