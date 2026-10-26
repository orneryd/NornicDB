package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
// Context evaluators capture operand errors until the chain decides whether
// they count (deferredExpressionValue).
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
func evaluateLogicalExpression(view braceView, eval func(braceView) (interface{}, bool, error)) (value interface{}, logical, ok bool, err error) {
	var buffer [8]string
	plan := planLogicalExpression(view, buffer[:0])
	if plan.kind == logicalNone {
		return nil, false, false, nil
	}
	value, ok, err = plan.evaluate(func(index int) (interface{}, bool, error) {
		return eval(plan.operandView(view, index))
	})
	return value, true, ok, err
}

// logicalKind is the top-level logical operator of an expression.
type logicalKind uint8

const (
	logicalNone logicalKind = iota
	logicalOr
	logicalXor
	logicalAnd
	logicalNot
	// logicalNotNot is NOT NOT x: x's value as it is.
	logicalNotNot
)

// logicalPlan is an expression's top-level logical operator and the
// operands it evaluates, read from the text once: an evaluator that keeps
// the plan (rowValueProgram) evaluates the operator without reading the
// text again, through the same evaluate as evaluateLogicalExpression.
type logicalPlan struct {
	kind     logicalKind
	operands []string
	spans    []cyphertext.Span
}

// planLogicalExpression reads expr's top-level OR, XOR, AND or NOT,
// simplified as Neo4j's planner does (see above); kind is logicalNone for
// any other expression. operands are appended to buffer.
func planLogicalExpression(view braceView, buffer []string) logicalPlan {
	expr := view.text()
	if !mayBeLogicalExpression(expr) {
		return logicalPlan{}
	}
	for _, operator := range [...]string{" OR ", " XOR ", " AND "} {
		spans := appendLogicalOperandSpans(nil, view, operator)
		if len(spans) == 0 {
			continue
		}
		operands := buffer[:0]
		for _, span := range spans {
			operands = append(operands, expr[span.Start:span.End])
		}
		kind, neutral := logicalXor, "false"
		switch operator {
		case " OR ":
			kind = logicalOr
		case " AND ":
			kind, neutral = logicalAnd, "true"
		}
		kept, selected := operands[:0], spans[:0]
		for index, operand := range operands {
			key := logicalOperandKey(operand)
			if cyphertext.KeywordEnd(key, 0, neutral, cyphertext.CypherWord) == len(key) {
				continue
			}
			repeated := false
			if kind != logicalXor {
				for _, earlier := range kept {
					if logicalOperandKey(earlier) == key {
						repeated = true
						break
					}
				}
			}
			if !repeated {
				kept = append(kept, operand)
				selected = append(selected, spans[index])
			}
		}
		if len(kept) == 0 && kind != logicalXor {
			kept = append(kept, neutral)
			selected = append(selected, cyphertext.Span{Start: -1, End: -1})
		}
		return logicalPlan{kind: kind, operands: kept, spans: selected}
	}
	operand, negated := logicalNotOperandSpan(expr)
	if !negated {
		return logicalPlan{}
	}
	text := expr[operand.Start:operand.End]
	if inner, doubled := logicalNotOperandSpan(text); doubled && !hasTopLevelLogicalOperator(logicalOperandView(view.spanWindow(operand))) {
		return logicalPlan{kind: logicalNotNot, operands: append(buffer[:0], text[inner.Start:inner.End]), spans: []cyphertext.Span{{Start: operand.Start + inner.Start, End: operand.Start + inner.End}}}
	}
	return logicalPlan{kind: logicalNot, operands: append(buffer[:0], text), spans: []cyphertext.Span{operand}}
}

func (plan logicalPlan) operandView(view braceView, index int) braceView {
	span := plan.spans[index]
	if span.Start < 0 {
		return view.generated(plan.operands[index])
	}
	return view.spanWindow(span)
}

// evaluate evaluates the plan, operand i through eval(i); operands are
// evaluated in order, only as far as the result needs them.
func (p logicalPlan) evaluate(eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	switch p.kind {
	case logicalOr:
		return evaluateLogicalChain(p.operands, true, eval)
	case logicalAnd:
		return evaluateLogicalChain(p.operands, false, eval)
	case logicalXor:
		return evaluateLogicalXor(p.operands, eval)
	case logicalNotNot:
		return eval(0)
	}
	value, ok, err := eval(0)
	if !ok || err != nil {
		return nil, ok, err
	}
	truth, err := cypherPredicateTruth(value)
	if err != nil {
		return nil, true, err
	}
	return truth.not().value(), true, nil
}

// hasTopLevelLogicalOperator reports whether expr is an OR, XOR or AND
// chain at its top level.
func hasTopLevelLogicalOperator(view braceView) bool {
	var buffer [8]string
	for _, operator := range [...]string{" OR ", " XOR ", " AND "} {
		if len(appendLogicalOperands(buffer[:0], view, operator)) > 0 {
			return true
		}
	}
	return false
}

// mayBeLogicalExpression reports, without allocating, whether expr may be
// an OR, XOR, AND or NOT: whether it has one of those words after a space,
// or starts with NOT. It never answers false for one.
func mayBeLogicalExpression(expr string) bool {
	if len(expr) > len("NOT") && equalFoldASCII(expr[:len("NOT")], "NOT") {
		return true
	}
	for index := 0; index+3 < len(expr); index++ {
		if !cyphertext.IsWhitespaceAt(expr, index) {
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
func appendLogicalOperands(operands []string, view braceView, operator string) []string {
	expr := view.text()
	for _, span := range appendLogicalOperandSpans(nil, view, operator) {
		operands = append(operands, expr[span.Start:span.End])
	}
	return operands
}

// The existing logical reader retains its split extents, including flattened
// parenthesized chains, before a consumer creates any Source windows.
func appendLogicalOperandSpans(operands []cyphertext.Span, view braceView, operator string) []cyphertext.Span {
	expr := view.text()
	first, rest, found := splitByOperatorOutsideCase(view, operator, true, true)
	if !found {
		return operands
	}
	pending := []cyphertext.Span{{Start: len(expr) - len(rest), End: len(expr)}, {End: len(first)}}
	for len(pending) > 0 {
		span := cyphertext.TrimWhitespaceSpan(expr, pending[len(pending)-1])
		pending = pending[:len(pending)-1]
		text := expr[span.Start:span.End]
		if left, right, split := splitByOperatorOutsideCase(view.spanWindow(span), operator, true, true); split {
			pending = append(pending, cyphertext.Span{Start: span.End - len(right), End: span.End}, cyphertext.Span{Start: span.Start, End: span.Start + len(left)})
			continue
		}
		if inner, enclosed := stripEnclosingExpressionParentheses(text); enclosed {
			innerSpan := cyphertext.TrimWhitespaceSpan(expr, cyphertext.Span{Start: span.Start + 1, End: span.End - 1})
			inner = strings.TrimSpace(inner)
			if left, right, split := splitByOperatorOutsideCase(view.spanWindow(innerSpan), operator, true, true); split {
				pending = append(pending, cyphertext.Span{Start: innerSpan.End - len(right), End: innerSpan.End}, cyphertext.Span{Start: innerSpan.Start, End: innerSpan.Start + len(left)})
				continue
			}
		}
		operands = append(operands, span)
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

// logicalOperandView keeps the same parenthesis normalization as the key
// reader while retaining the selected occurrence. Author: #1663 lane.
func logicalOperandView(view braceView) braceView {
	view = view.trim()
	for {
		if _, enclosed := stripEnclosingExpressionParentheses(view.text()); !enclosed {
			return view
		}
		view = view.window(1, len(view.text())-1).trim()
	}
}

// evaluateLogicalChain evaluates an AND chain (decisive false) or an OR
// chain (decisive true). A chain of one operand is its value as it is.
func evaluateLogicalChain(operands []string, decisive bool, eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	if len(operands) == 1 {
		if strings.EqualFold(operands[0], "true") || strings.EqualFold(operands[0], "false") {
			return strings.EqualFold(operands[0], "true"), true, nil
		}
		return eval(0)
	}
	return evaluateLogicalOperands(len(operands), decisive, eval)
}

func evaluateLogicalOperands(count int, decisive bool, eval func(int) (interface{}, bool, error)) (interface{}, bool, error) {
	var evaluationErr, typeErr error
	unknown := false
	for index := 0; index < count; index++ {
		value, ok, err := eval(index)
		if err != nil {
			if evaluationErr == nil {
				evaluationErr = err
			}
			continue
		}
		if !ok {
			return nil, false, nil
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

// evaluateLogicalXor evaluates an XOR chain left to right, its false
// operands already dropped (planLogicalExpression); what is left of one
// operand is that operand's value as it is.
func evaluateLogicalXor(operands []string, eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	switch len(operands) {
	case 0:
		return false, true, nil
	case 1:
		return eval(0)
	}
	result := truthFalse
	for index := range operands {
		value, ok, err := eval(index)
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
	span, ok := logicalNotOperandSpan(expr)
	if !ok {
		return "", false
	}
	return expr[span.Start:span.End], true
}

func logicalNotOperandSpan(expr string) (cyphertext.Span, bool) {
	span := cyphertext.TrimWhitespaceSpan(expr, cyphertext.Span{End: len(expr)})
	for {
		text := expr[span.Start:span.End]
		if _, enclosed := stripEnclosingExpressionParentheses(text); !enclosed {
			break
		}
		span = cyphertext.TrimWhitespaceSpan(expr, cyphertext.Span{Start: span.Start + 1, End: span.End - 1})
	}
	text := expr[span.Start:span.End]
	if len(text) <= len("NOT") || !(cyphertext.KeywordEnd(text, 0, "NOT", cyphertext.CypherWord) == len("NOT")) || !(cyphertext.IsWhitespaceAt(text, len("NOT")) || text[len("NOT")] == '(') {
		return cyphertext.Span{}, false
	}
	span = cyphertext.TrimWhitespaceSpan(expr, cyphertext.Span{Start: span.Start + len("NOT"), End: span.End})
	operand := expr[span.Start:span.End]
	if operand == "" || syntaxByteInSet("=<>!*/%^.|,)]}", operand[0]) {
		return cyphertext.Span{}, false
	}
	word, isWord := cyphertext.ScanIdentifier(operand, 0)
	if isWord {
		next := word.End
		for _, keyword := range []string{"AS", "AND", "OR", "XOR", "IN", "IS", "STARTS", "ENDS", "CONTAINS"} {
			if cyphertext.KeywordEnd(operand, 0, keyword, cyphertext.CypherWord) == next {
				return cyphertext.Span{}, false
			}
		}
	}
	return span, true
}
