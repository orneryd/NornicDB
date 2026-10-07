package cypher

import (
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// impliedBound is one side of the range a WHERE clause leaves possible for a
// property: every row the clause keeps has a value at or beyond it.
type impliedBound struct {
	value     interface{}
	inclusive bool
	set       bool
}

// impliedPropertyBounds returns the range of variable.property values that
// where leaves possible: a row whose value falls outside it can't satisfy
// where. A comparison of the property with a number or string literal or
// parameter bounds it (n.t > $last, 5 <= n.t < 9, n.t = 'x'); AND keeps the
// tightest bound, and OR the loosest, present on every side, so the keyset
// predicate n.t > $t OR (n.t = $t AND n.id > $id) gives n.t >= $t. Anything
// else (NOT, XOR, functions, other properties) bounds nothing. The bounds
// only let an ordered index scan skip values that can't match
// (VisitPropertyIndexGroupsInRange); the caller still evaluates where on
// every row (#939).
func impliedPropertyBounds(variable, property, where string, params map[string]interface{}) storage.PropertyIndexBounds {
	lower, upper := impliedBounds(variable, property, where, params)
	return storage.PropertyIndexBounds{
		Lower: lower.value, HasLower: lower.set, LowerInclusive: lower.inclusive,
		Upper: upper.value, HasUpper: upper.set, UpperInclusive: upper.inclusive,
	}
}

func impliedBounds(variable, property, expression string, params map[string]interface{}) (lower, upper impliedBound) {
	expression = unwrapOuterParens(strings.TrimSpace(expression))
	if expression == "" {
		return lower, upper
	}
	if disjuncts := splitTopLevelOrTerms(expression); len(disjuncts) > 1 {
		for index, disjunct := range disjuncts {
			disjunctLower, disjunctUpper := impliedBounds(variable, property, disjunct, params)
			if index == 0 {
				lower, upper = disjunctLower, disjunctUpper
				continue
			}
			lower = looserBound(lower, disjunctLower, false)
			upper = looserBound(upper, disjunctUpper, true)
		}
		return lower, upper
	}
	if conjuncts := splitTopLevelAndConjuncts(expression); len(conjuncts) > 1 {
		for _, conjunct := range conjuncts {
			conjunctLower, conjunctUpper := impliedBounds(variable, property, conjunct, params)
			lower = tighterBound(lower, conjunctLower, false)
			upper = tighterBound(upper, conjunctUpper, true)
		}
		return lower, upper
	}
	operands, operators, isChain := splitComparisonChain(expression)
	if !isChain {
		return lower, upper
	}
	for index, operator := range operators {
		left, right := strings.TrimSpace(operands[index]), strings.TrimSpace(operands[index+1])
		operand := right
		if leftProperty, isProperty := parseVariableProperty(left, variable); !isProperty || leftProperty != property {
			rightProperty, isProperty := parseVariableProperty(right, variable)
			if !isProperty || rightProperty != property {
				continue
			}
			operand, operator = left, mirroredComparison(operator)
		}
		value, resolved := rangeBoundValue(operand, params)
		if !resolved || !seekableBoundValue(value) {
			continue
		}
		bound := impliedBound{value: value, inclusive: operator != ">" && operator != "<", set: true}
		switch operator {
		case ">", ">=":
			lower = tighterBound(lower, bound, false)
		case "<", "<=":
			upper = tighterBound(upper, bound, true)
		case "=":
			lower = tighterBound(lower, bound, false)
			upper = tighterBound(upper, bound, true)
		}
	}
	return lower, upper
}

// seekableBoundValue reports whether value can bound an index range: a
// string or a number other than NaN.
func seekableBoundValue(value interface{}) bool {
	switch typed := value.(type) {
	case string:
		return true
	case bool:
		return false
	case float64:
		return !math.IsNaN(typed)
	}
	_, isNumber := toFloat64(value)
	return isNumber
}

// compareBoundValues orders two bound values; comparable is false for a
// string against a number.
func compareBoundValues(left, right interface{}) (order int, comparable bool) {
	leftString, leftIsString := left.(string)
	rightString, rightIsString := right.(string)
	if leftIsString || rightIsString {
		if !leftIsString || !rightIsString {
			return 0, false
		}
		return strings.Compare(leftString, rightString), true
	}
	return compareCypherNumbersExactly(left, right)
}

// tighterBound is the bound both a and b imply: the larger lower bound or the
// smaller upper bound. Either is sound when they can't be compared.
func tighterBound(a, b impliedBound, upper bool) impliedBound {
	if !a.set {
		return b
	}
	if !b.set {
		return a
	}
	order, comparable := compareBoundValues(a.value, b.value)
	if !comparable {
		return a
	}
	if upper {
		order = -order
	}
	switch {
	case order > 0:
		return a
	case order < 0:
		return b
	}
	a.inclusive = a.inclusive && b.inclusive
	return a
}

// looserBound is the bound that holds when either a or b does: the smaller
// lower bound or the larger upper bound, and none when either side has none.
func looserBound(a, b impliedBound, upper bool) impliedBound {
	if !a.set || !b.set {
		return impliedBound{}
	}
	order, comparable := compareBoundValues(a.value, b.value)
	if !comparable {
		return impliedBound{}
	}
	if upper {
		order = -order
	}
	switch {
	case order < 0:
		return a
	case order > 0:
		return b
	}
	a.inclusive = a.inclusive || b.inclusive
	return a
}
