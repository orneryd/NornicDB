package cypher

import (
	"strings"
)

// Logical operators: OR, XOR, AND and NOT, as Neo4j 5.26 evaluates them
// (#907). Every evaluator reads them through evaluateLogicalExpression, so
// their meaning has one owner; an evaluator supplies only how it evaluates
// an operand.
//
// Precedence, loosest first: OR, XOR, AND, NOT. A chain of AND or OR is one
// operation over all its operands (a AND (b AND c) is a AND b AND c):
//
//   - AND is false when any operand is false, whatever the others are;
//     otherwise an operand that failed to evaluate fails it, then an
//     operand that is not a predicate (cypherPredicateTruth) is a
//     TypeError; otherwise null when any operand is null; else true.
//     Operands are evaluated in order up to the first false, so
//     x > 0 AND 1 / x > 0 never divides by zero.
//   - OR is the same with true in place of false.
//
// An evaluator that records an operand's error on the statement
// (recordExpressionFailure) rather than returning it can't take it back: in
// it, an error before the deciding operand still fails the statement.
//   - XOR and NOT take predicates: a non-predicate operand is a TypeError,
//     and null gives null.
//
// Before evaluating, the operators are simplified as Neo4j's planner does:
// true drops out of an AND, false out of an OR and out of an XOR, an operand
// repeated in an AND or OR chain counts once, and NOT NOT x is x. What is
// left of a chain of one operand is that operand's value as it is, not a
// predicate: RETURN n.count AND true returns n.count. Operand types known
// when the statement is compiled are checked before it runs
// (validateStaticOperatorTypes).

// logicalOperandEvaluator evaluates one operand of a logical operator. ok is
// false when the evaluator can't evaluate it; the caller then doesn't
// handle the expression.
type logicalOperandEvaluator func(operand string) (value interface{}, ok bool, err error)

// evaluateLogicalExpression evaluates expr when its top level is OR, XOR,
// AND or NOT (logical reports whether it is), each operand through eval.
func evaluateLogicalExpression(expr string, eval logicalOperandEvaluator) (value interface{}, logical, ok bool, err error) {
	if !mayBeLogicalExpression(expr) {
		return nil, false, false, nil
	}
	var buffer [8]string
	for _, operator := range [...]string{" OR ", " XOR ", " AND "} {
		operands := appendLogicalOperands(buffer[:0], expr, operator)
		if len(operands) == 0 {
			continue
		}
		switch operator {
		case " OR ":
			value, ok, err = evaluateLogicalChain(simplifyLogicalChain(operands, "false"), true, eval)
		case " AND ":
			value, ok, err = evaluateLogicalChain(simplifyLogicalChain(operands, "true"), false, eval)
		default:
			value, ok, err = evaluateLogicalXor(operands, eval)
		}
		return value, true, ok, err
	}
	operand, negated := logicalNotOperand(expr)
	if !negated {
		return nil, false, false, nil
	}
	if inner, doubled := logicalNotOperand(operand); doubled {
		value, ok, err = eval(inner)
		return value, true, ok, err
	}
	value, ok, err = eval(operand)
	if !ok || err != nil {
		return nil, true, ok, err
	}
	truth, err := cypherPredicateTruth(value)
	if err != nil {
		return nil, true, true, err
	}
	return truth.not().value(), true, true, nil
}

// mayBeLogicalExpression reports, without allocating, whether expr may be
// an OR, XOR, AND or NOT: whether it has one of those words after a space,
// or starts with NOT. It never answers false for one.
func mayBeLogicalExpression(expr string) bool {
	if len(expr) > len("NOT") && equalFoldASCII(expr[:len("NOT")], "NOT") {
		return true
	}
	for index := 0; index+3 < len(expr); index++ {
		if expr[index] != ' ' {
			continue
		}
		switch rest := expr[index+1:]; asciiLowerByte(rest[0]) {
		case 'a':
			if len(rest) > len("AND") && equalFoldASCII(rest[:len("AND")], "AND") {
				return true
			}
		case 'o':
			if equalFoldASCII(rest[:len("OR")], "OR") {
				return true
			}
		case 'x':
			if len(rest) > len("XOR") && equalFoldASCII(rest[:len("XOR")], "XOR") {
				return true
			}
		}
	}
	return false
}

// appendLogicalOperands appends the operands of expr's top-level chain of
// operator to operands, in order, none when it has none. An operand that is
// a parenthesized chain of the same operator joins the chain. It works
// through a stack of the parts still to split rather than recursing, so a
// caller's buffer (operands) stays where the caller put it.
func appendLogicalOperands(operands []string, expr, operator string) []string {
	first, rest, found := splitByOperatorOutsideCase(expr, operator, true, true)
	if !found {
		return operands
	}
	var stackBuffer [8]string
	pending := append(stackBuffer[:0], rest, first)
	for len(pending) > 0 {
		text := strings.TrimSpace(pending[len(pending)-1])
		pending = pending[:len(pending)-1]
		if left, right, split := splitByOperatorOutsideCase(text, operator, true, true); split {
			pending = append(pending, right, left)
			continue
		}
		if inner, enclosed := stripEnclosingExpressionParentheses(text); enclosed {
			inner = strings.TrimSpace(inner)
			if left, right, split := splitByOperatorOutsideCase(inner, operator, true, true); split {
				pending = append(pending, right, left)
				continue
			}
		}
		operands = append(operands, text)
	}
	return operands
}

// simplifyLogicalChain drops the operands that are the literal neutral
// (true in an AND, false in an OR) and the repeats of an operand, in place.
func simplifyLogicalChain(operands []string, neutral string) []string {
	kept := operands[:0]
	for _, operand := range operands {
		key := logicalOperandKey(operand)
		if strings.EqualFold(key, neutral) {
			continue
		}
		repeated := false
		for _, earlier := range kept {
			if logicalOperandKey(earlier) == key {
				repeated = true
				break
			}
		}
		if !repeated {
			kept = append(kept, operand)
		}
	}
	if len(kept) == 0 {
		return append(kept, neutral)
	}
	return kept
}

// logicalOperandKey is an operand's text without enclosing parentheses: the
// same operand however it is parenthesized.
func logicalOperandKey(operand string) string {
	operand = strings.TrimSpace(operand)
	for {
		inner, enclosed := stripEnclosingExpressionParentheses(operand)
		if !enclosed {
			return operand
		}
		operand = strings.TrimSpace(inner)
	}
}

// evaluateLogicalChain evaluates an AND chain (decisive false) or an OR
// chain (decisive true). A chain of one operand is its value as it is.
func evaluateLogicalChain(operands []string, decisive bool, eval logicalOperandEvaluator) (interface{}, bool, error) {
	if len(operands) == 1 {
		if strings.EqualFold(operands[0], "true") || strings.EqualFold(operands[0], "false") {
			return strings.EqualFold(operands[0], "true"), true, nil
		}
		return eval(operands[0])
	}
	var evaluationErr, typeErr error
	unknown := false
	for _, operand := range operands {
		value, ok, err := eval(operand)
		if !ok {
			return nil, false, err
		}
		if err != nil {
			if evaluationErr == nil {
				evaluationErr = err
			}
			continue
		}
		truth, err := cypherPredicateTruth(value)
		switch {
		case err != nil:
			if typeErr == nil {
				typeErr = err
			}
		case truth == truthUnknown:
			unknown = true
		case (truth == truthTrue) == decisive:
			// The deciding operand: the rest isn't evaluated (x > 0 AND
			// 1 / x > 0 guards the division), and the errors of the ones
			// before it don't count.
			return decisive, true, nil
		}
	}
	if evaluationErr != nil {
		return nil, true, evaluationErr
	}
	if typeErr != nil {
		return nil, true, typeErr
	}
	if unknown {
		return nil, true, nil
	}
	return !decisive, true, nil
}

// evaluateLogicalXor evaluates an XOR chain left to right. false drops out
// of it; what is left of one operand is that operand's value as it is.
func evaluateLogicalXor(operands []string, eval logicalOperandEvaluator) (interface{}, bool, error) {
	kept := operands[:0]
	for _, operand := range operands {
		if !strings.EqualFold(logicalOperandKey(operand), "false") {
			kept = append(kept, operand)
		}
	}
	switch len(kept) {
	case 0:
		return false, true, nil
	case 1:
		return eval(kept[0])
	}
	result := truthFalse
	for _, operand := range kept {
		value, ok, err := eval(operand)
		if !ok || err != nil {
			return nil, ok, err
		}
		truth, err := cypherPredicateTruth(value)
		if err != nil {
			return nil, true, err
		}
		result = result.xor(truth)
	}
	return result.value(), true, nil
}

// logicalNotOperand returns the operand of expr when expr is NOT <operand>.
// NOT is also a valid variable name: when what follows can't start an
// expression (not = 1, not.p, not IN l, not AS x), it is the variable (#907).
func logicalNotOperand(expr string) (string, bool) {
	expr = logicalOperandKey(expr)
	if len(expr) <= len("NOT") || !strings.EqualFold(expr[:len("NOT")], "NOT") ||
		!(isASCIISpace(expr[len("NOT")]) || expr[len("NOT")] == '(') {
		return "", false
	}
	operand := strings.TrimSpace(expr[len("NOT"):])
	if operand == "" || strings.IndexByte("=<>!*/%^.|,)]}", operand[0]) >= 0 {
		return "", false
	}
	word, _, isWord := scanIdentifierToken(operand, 0)
	if isWord {
		switch upperASCII(word) {
		case "AS", "AND", "OR", "XOR", "IN", "IS", "STARTS", "ENDS", "CONTAINS":
			return "", false
		}
	}
	return operand, true
}
