package cypher

import (
	"context"
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
//   - Which operand errors a deciding operand discards is one policy,
//     isolateOperandErrors; every evaluator asks it for each operand.
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

// logicalOperandEvaluator evaluates one operand of a logical operator at
// position (isolateOperandErrors). ok is
// false when the evaluator can't evaluate it; the caller then doesn't
// handle the expression.
type logicalOperandEvaluator func(operand string, position foldPosition) (value interface{}, ok bool, err error)

// evaluateLogicalExpression evaluates expr when its top level is OR, XOR,
// AND or NOT (logical reports whether it is), each operand through eval.
func evaluateLogicalExpression(expr string, eval logicalOperandEvaluator) (value interface{}, logical, ok bool, err error) {
	return evaluateLogicalExpressionInContext(context.Background(), expr, eval)
}
func evaluateLogicalExpressionInContext(ctx context.Context, expr string, eval logicalOperandEvaluator) (value interface{}, logical, ok bool, err error) {
	var buffer [8]string
	plan := planLogicalExpression(expr, buffer[:0])
	if plan.kind == logicalNone {
		return nil, false, false, nil
	}
	position := logicalFoldPosition(plan.kind)
	value, ok, err = plan.evaluateInContext(ctx, func(index int) (interface{}, bool, error) {
		return eval(plan.operands[index], position)
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
	// logicalNotAnd is NOT (a AND b) with a subquery, evaluated as
	// NOT a OR NOT b, as Neo4j normalizes it: its operands are a and b, and
	// a subquery among them runs only when the others don't decide.
	logicalNotAnd
	// logicalNotOr is NOT (a OR b) with a subquery, evaluated as
	// NOT a AND NOT b: its operands are a and b, whose subqueries run before
	// the fold, as any AND's (applyOperand).
	logicalNotOr
)

// logicalPlan is an expression's top-level logical operator and the
// operands it evaluates, read from the text once: an evaluator that keeps
// the plan (rowValueProgram) evaluates the operator without reading the
// text again, through the same evaluate as evaluateLogicalExpression.
type logicalPlan struct {
	kind logicalKind
	// The cache is shared across versions. Bits 1/2 mark a deciding constant
	// in Cypher 5/25 that must be resolved from context before Apply runs.
	decidingVersions uint8
	operands         []string
	apply            []int
}

// planLogicalExpression reads expr's top-level OR, XOR, AND or NOT,
// simplified as Neo4j's planner does (see above); kind is logicalNone for
// any other expression. operands are appended to buffer.
func planLogicalExpression(expr string, buffer []string) logicalPlan {
	if !mayBeLogicalExpression(expr) {
		return logicalPlan{}
	}
	for _, operator := range [...]string{" OR ", " XOR ", " AND "} {
		operands := appendLogicalOperands(buffer[:0], expr, operator)
		if len(operands) == 0 {
			continue
		}
		subqueries := mayContainSubqueryExpression(expr)
		switch operator {
		case " OR ":
			plan := logicalPlan{kind: logicalOr, operands: simplifyLogicalChain(operands, "false", subqueries)}
			if subqueries {
				plan.decidingVersions = constantDecidingVersions(plan.operands, truthTrue)
				plan.apply = disjunctionApplyOperandIndexes(plan.operands)
			}
			return plan
		case " AND ":
			plan := logicalPlan{kind: logicalAnd, operands: simplifyLogicalChain(operands, "true", subqueries)}
			if subqueries {
				plan.decidingVersions = constantDecidingVersions(plan.operands, truthFalse)
				plan.apply = applyOperandIndexes(len(plan.operands), func(i int) string { return plan.operands[i] })
			}
			return plan
		default:
			// false drops out of an XOR.
			kept := operands[:0]
			for _, operand := range operands {
				if !strings.EqualFold(logicalOperandKey(operand), "false") {
					kept = append(kept, operand)
				}
			}
			return logicalPlan{kind: logicalXor, operands: kept}
		}
	}
	operand, negated := logicalNotOperand(expr)
	if !negated {
		return logicalPlan{}
	}
	// NOT (NOT x) is x only when the inner NOT covers its whole operand:
	// NOT (NOT a OR b) negates the OR, whose left operand is NOT a.
	if inner, doubled := logicalNotOperand(operand); doubled && !hasTopLevelLogicalOperator(logicalOperandKey(operand)) {
		return logicalPlan{kind: logicalNotNot, operands: append(buffer[:0], inner)}
	}
	// NOT over an AND or an OR with a subquery is normalized as Neo4j does
	// (De Morgan), so that its subqueries are planned as the result's.
	if mayContainSubqueryExpression(operand) {
		switch inner := planLogicalExpression(logicalOperandKey(operand), buffer[:0]); inner.kind {
		case logicalAnd:
			return logicalPlan{kind: logicalNotAnd, decidingVersions: inner.decidingVersions, operands: inner.operands, apply: disjunctionApplyOperandIndexes(inner.operands)}
		case logicalOr:
			plan := logicalPlan{kind: logicalNotOr, decidingVersions: inner.decidingVersions, operands: inner.operands}
			plan.apply = applyOperandIndexes(len(plan.operands), func(i int) string { return plan.operands[i] })
			return plan
		}
	}
	return logicalPlan{kind: logicalNot, operands: append(buffer[:0], operand)}
}

// evaluate evaluates the plan, operand i through eval(i); operands are
// evaluated in order, only as far as the result needs them.
func (p logicalPlan) evaluate(eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	return p.evaluateInContext(context.Background(), eval)
}
func (p logicalPlan) evaluateInContext(ctx context.Context, eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	version := uint8(0)
	if p.decidingVersions != 0 {
		version = 1
		if cypherVersionFromContext(ctx) == "25" {
			version = 2
		}
		if p.decidingVersions&version != 0 {
			return p.kind == logicalOr || p.kind == logicalNotAnd, true, nil
		}
	}
	if len(p.apply) > 0 {
		cached, err := evaluateApplyOperands(ctx, p.apply, eval)
		if err != nil {
			return nil, false, err
		}
		p.apply = nil
		return p.evaluateInContext(ctx, func(index int) (interface{}, bool, error) { return resolveApplyOperand(cached, index, eval) })
	}
	switch p.kind {
	case logicalOr:
		return evaluateLogicalChain(p.operands, true, eval, version)
	case logicalAnd:
		return evaluateLogicalChain(p.operands, false, eval, version)
	case logicalXor:
		return evaluateLogicalXor(p.operands, eval)
	case logicalNotNot:
		return eval(0)
	case logicalNotAnd, logicalNotOr:
		return evaluateLogicalOperands(len(p.operands), p.kind == logicalNotAnd, func(index int) (interface{}, bool, error) {
			value, ok, err := eval(index)
			if !ok || err != nil {
				return value, ok, err
			}
			truth, err := foldPredicateTruth(value)
			if err != nil {
				return nil, true, err
			}
			return truth.not().value(), true, nil
		})
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
func hasTopLevelLogicalOperator(expr string) bool {
	var buffer [8]string
	for _, operator := range [...]string{" OR ", " XOR ", " AND "} {
		if len(appendLogicalOperands(buffer[:0], expr, operator)) > 0 {
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
//
// A deciding literal (false in an AND, true in an OR) is the chain's value
// wherever it stands: Neo4j folds it before it plans subqueries. With
// constants (the chain has a subquery) so is any predicate Neo4j folds to a
// constant (constantPredicateTruth: NOT true, 1 = 2). Static type validation
// precedes us.
func simplifyLogicalChain(operands []string, neutral string, constants bool) []string {
	decisive, decisiveText := truthFalse, "false"
	if neutral == "false" {
		decisive, decisiveText = truthTrue, "true"
	}
	for _, operand := range operands {
		if truth, constant := logicalOperandTruth(operand, constants); constant && truth == decisive {
			operands[0] = decisiveText
			return operands[:1]
		}
	}
	kept := operands[:0]
	for _, operand := range operands {
		key := logicalOperandKey(operand)
		if truth, constant := logicalOperandTruth(operand, constants); constant && truth != truthUnknown {
			// The neutral value: the deciding one returned above.
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

// logicalOperandTruth is an AND / OR operand's constant truth: a boolean
// literal, or with constants any constant predicate.
func logicalOperandTruth(operand string, constants bool) (cypherTruth, bool) {
	key := logicalOperandKey(operand)
	switch {
	case equalFoldASCII(key, "true"):
		return truthTrue, true
	case equalFoldASCII(key, "false"):
		return truthFalse, true
	case constants:
		truth5, known5 := constantPredicateTruth(key, false)
		truth25, known25 := constantPredicateTruth(key, true)
		// A versionless plan may discard only predicates that agree in both
		// supported policies. Keep differing ones for the statement context.
		if known5 && known25 && truth5 == truth25 {
			return truth5, true
		}
	}
	return truthUnknown, false
}

// constantPredicateTruth reads a predicate Neo4j folds to a constant before
// it plans subqueries: a boolean literal or null, NOT of a constant, or a
// comparison of literals (1 = 2, 1 < 0 < 2).
func constantPredicateTruth(expression string, cypher25 bool) (cypherTruth, bool) {
	key := logicalOperandKey(expression)
	switch {
	case equalFoldASCII(key, "true"):
		return truthTrue, true
	case equalFoldASCII(key, "false"):
		return truthFalse, true
	case equalFoldASCII(key, "null"):
		return truthUnknown, true
	}
	if inner, negated := logicalNotOperand(key); negated {
		truth, constant := constantPredicateTruth(inner, cypher25)
		return truth.not(), constant
	}
	scan, comparison := scanComparisonChain(key)
	if !comparison {
		return truthUnknown, false
	}
	for i := 0; i <= scan.count; i++ {
		if operand, ok := parseRowOperand(scan.operand(key, i)); !ok || operand.kind != rowOperandLiteral {
			return truthUnknown, false
		}
	}
	value, _, resolved, err := evaluateComparisonChain(key, func(text string, _ foldPosition) (interface{}, bool, error) {
		operand, _ := parseRowOperand(text)
		return operand.literal, true, nil
	}, compareCypherPredicateValue, cypher25)
	if err != nil || !resolved {
		return truthUnknown, false
	}
	return cypherBooleanTruth(value)
}

// constantDecidingVersions records only version-sensitive decisions. The
// ordinary simplifier already handles constants that agree in both versions;
// these remaining decisions must precede subquery Apply at evaluation time.
func constantDecidingVersions(operands []string, decisive cypherTruth) uint8 {
	var versions uint8
	for _, operand := range operands {
		truth5, known5 := constantPredicateTruth(operand, false)
		truth25, known25 := constantPredicateTruth(operand, true)
		if !known5 || !known25 || truth5 == truth25 {
			continue
		}
		if truth5 == decisive {
			versions |= 1
		}
		if truth25 == decisive {
			versions |= 2
		}
	}
	return versions
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
func evaluateLogicalChain(operands []string, decisive bool, eval func(index int) (interface{}, bool, error), version uint8) (interface{}, bool, error) {
	count, single := len(operands), 0
	if version != 0 {
		neutral := truthTrue
		if decisive {
			neutral = truthFalse
		}
		count = 0
		for index, operand := range operands {
			if truth, known := constantPredicateTruth(operand, version == 2); known && truth == neutral {
				continue
			}
			count++
			single = index
			if count > 1 {
				break
			}
		}
	}
	if count == 0 {
		return !decisive, true, nil
	}
	if count == 1 {
		if strings.EqualFold(operands[single], "true") || strings.EqualFold(operands[single], "false") {
			return strings.EqualFold(operands[single], "true"), true, nil
		}
		return eval(single)
	}
	// Neutral constants cannot change a genuine multi-operand predicate fold.
	// Keep original indexes so compiled operands and Apply results stay aligned.
	return evaluateLogicalOperands(len(operands), decisive, eval)
}

func evaluateLogicalOperands(count int, decisive bool, eval func(int) (interface{}, bool, error)) (interface{}, bool, error) {
	fold := logicalFold{decisive: decisive}
	for index := 0; index < count && !fold.add(eval(index)); index++ {
	}
	return fold.result()
}

// logicalFold is the one AND (decisive false) / OR (decisive true) fold
// (evaluateLogicalOperands): operands are added in order until add reports
// the fold done. An evaluator that walks its own operands (the predicate
// plan's junctions) adds them directly instead of through a callback.
type logicalFold struct {
	decisive      bool
	unknown       bool
	decided       bool
	unresolved    bool
	evaluationErr error
	typeErr       error
}

// add folds one operand's evaluation, and reports whether the fold is done:
// the operand decided it, or couldn't be evaluated (unresolved).
func (fold *logicalFold) add(value interface{}, ok bool, err error) bool {
	if err == nil && ok {
		// A boolean or a null, the usual operand, as cypherPredicateTruth
		// reads it, without the call.
		switch truth := value.(type) {
		case bool:
			return fold.addKnown(truthOf(truth))
		case nil:
			return fold.addKnown(truthUnknown)
		}
	}
	return fold.addValue(value, ok, err)
}

// logicalPredicateTypeError keeps predicate-conversion errors below operand
// evaluation errors when a planned fold carries typed truth values.
type logicalPredicateTypeError struct{ error }

func (err *logicalPredicateTypeError) Unwrap() error { return err.error }

func foldPredicateTruth(value interface{}) (cypherTruth, error) {
	truth, err := cypherPredicateTruth(value)
	if err != nil {
		return truth, &logicalPredicateTypeError{err}
	}
	return truth, nil
}

func (fold *logicalFold) addValue(value interface{}, ok bool, err error) bool {
	if err != nil {
		return fold.addError(err)
	}
	if !ok {
		fold.unresolved = true
		return true
	}
	truth, err := foldPredicateTruth(value)
	return fold.addTruth(truth, err)
}

// result is the fold's value: the deciding value; else an operand's
// evaluation error, then a non-predicate's TypeError; else null when an
// operand was null; else the non-deciding value.
func (fold *logicalFold) result() (interface{}, bool, error) {
	truth, err := fold.truthResult()
	if fold.unresolved {
		return nil, false, err
	}
	return truth.value(), true, err
}

// addTruth accepts planned predicates without boxing each intermediate
// boolean/null.
func (fold *logicalFold) addTruth(truth cypherTruth, err error) bool {
	if err != nil {
		return fold.addError(err)
	}
	return fold.addKnown(truth)
}

// addKnown folds an operand's truth value. It is small enough to inline, so
// an evaluator that walks its own operands (the predicate plan's junctions)
// folds each without a call.
func (fold *logicalFold) addKnown(truth cypherTruth) bool {
	if truth == truthUnknown {
		fold.unknown = true
		return false
	}
	fold.decided = (truth == truthTrue) == fold.decisive
	return fold.decided
}

// addError folds an operand's error: an evaluation error ranks above a
// non-predicate's TypeError (foldPredicateTruth); a later deciding operand
// still discards both.
func (fold *logicalFold) addError(err error) bool {
	if conversion, typed := err.(*logicalPredicateTypeError); typed {
		if fold.typeErr == nil {
			fold.typeErr = conversion.error
		}
	} else if fold.evaluationErr == nil {
		fold.evaluationErr = err
	}
	return false
}

func (fold *logicalFold) truthResult() (cypherTruth, error) {
	switch {
	case fold.unresolved:
		return truthUnknown, nil
	case fold.decided:
		return truthOf(fold.decisive), nil
	case fold.evaluationErr != nil:
		return truthUnknown, fold.evaluationErr
	case fold.typeErr != nil:
		return truthUnknown, fold.typeErr
	case fold.unknown:
		return truthUnknown, nil
	}
	return truthOf(!fold.decisive), nil
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
