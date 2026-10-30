package cypher

import "strings"

// inPredicateTruth evaluates a WHERE operand that is exactly `x IN list` or
// `x NOT IN list` (optionally parenthesised) with Cypher's three-valued logic,
// using the caller's own expression evaluator for both sides and
// cypherMembership - the same membership rule as the expression evaluator's IN.
// ok is false when the operand is not a single IN predicate; callers then keep
// their existing handling.
//
// Boolean WHERE evaluators use it for NOT: `NOT (x IN list)` holds only when the
// membership is known false. Negating a two-valued membership instead turned
// null (x IN null, a null x, or a list holding null without a match) into true.
func inPredicateTruth(view braceView, eval func(braceView) interface{}) (cypherTruth, bool) {
	view = view.trim()
	clause := view.text()
	for {
		_, enclosed := stripEnclosingExpressionParentheses(clause)
		if !enclosed {
			break
		}
		view = view.window(1, len(clause)-1).trim()
		clause = view.text()
	}
	upper := upperASCII(clause)
	if strings.HasPrefix(upper, "EXISTS") || strings.HasPrefix(upper, "COUNT") || strings.HasPrefix(upper, "NOT ") {
		return truthUnknown, false
	}
	for _, operator := range []string{" AND ", " OR ", " XOR "} {
		if _, _, found := splitByOperatorWithOptions(view, operator, true, true); found {
			return truthUnknown, false
		}
	}
	negate := false
	left, right, found := splitByOperatorWithOptions(view, " NOT IN ", true, true)
	if found {
		negate = true
	} else if left, right, found = splitByOperatorWithOptions(view, " IN ", true, true); !found {
		return truthUnknown, false
	}
	truth := truthUnknown
	if membership, ok := cypherMembership(eval(view.window(0, len(left))), eval(view.window(len(clause)-len(right), len(clause)))); ok {
		if matched, known := membership.(bool); known {
			truth = truthOf(matched)
		}
	}
	if negate {
		return truth.not(), true
	}
	return truth, true
}
