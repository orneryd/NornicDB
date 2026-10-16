package cypher

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type expressionFailureKey struct{}

// expressionOperandFailureKey isolates only errors while preserving the
// statement's map ordering and REVEAL metadata in expressionFailureKey.
type expressionOperandFailureKey struct{}

// deferredExpressionValue adapts a context evaluator to the logical fold's
// explicit error contract. A later deciding operand can discard this error.
func deferredExpressionValue(ctx context.Context, evaluate func(context.Context) interface{}) (interface{}, bool, error) {
	return deferredExpressionResult(ctx, func(ctx context.Context) (interface{}, bool, error) {
		return evaluate(ctx), true, nil
	})
}

func deferredExpressionResult(ctx context.Context, evaluate func(context.Context) (interface{}, bool, error)) (interface{}, bool, error) {
	ctx = context.WithValue(ctx, expressionOperandFailureKey{}, &expressionFailure{})
	value, ok, err := evaluate(ctx)
	if err == nil {
		err = getExpressionFailure(ctx)
	}
	return value, ok, err
}

// withExpressionFailureSlot gives ctx a slot that collects the first
// expression error of the statement (recordExpressionFailure), unless it
// has one: the running statement's own slot when there is one (no
// allocation), else a new one.
func withExpressionFailureSlot(ctx context.Context) context.Context {
	if ctx.Value(expressionFailureKey{}) != nil {
		return ctx
	}
	if statement, ok := ctx.Value(statementContextKey{}).(*statementContext); ok {
		statement.failureActive.Store(true)
		return ctx
	}
	return context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
}

type expressionFailure struct {
	mu               sync.Mutex
	err              error
	readScopeEngine  *storage.BadgerEngine
	readScopeContext context.Context
	mapKeyOrders     map[uintptr][]string

	// rowFallback is the expression the row evaluator handed the shared
	// evaluator (evaluateRowFallback), "" for any other slot.
	rowFallback string
}

// handedBackByRowEvaluator reports whether expr is the expression the row
// evaluator handed this evaluation (evaluateRowFallback). The shared
// evaluator evaluates such an expression itself and hands the row evaluator
// only its parts: handing it back whole would make the two evaluators call
// each other until the stack overflows. Every hand-back to the row
// evaluator asks this.
func handedBackByRowEvaluator(ctx context.Context, expr string) bool {
	failure, _ := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	return failure != nil && failure.rowFallback != "" && logicalOperandKey(failure.rowFallback) == logicalOperandKey(expr)
}

// contextFoldOperand evaluates an operand of a context evaluator's logical
// or comparison fold. That evaluator records errors on ctx; the operand gets
// its own error slot where the policy asks for one (isolateOperandErrors).
func contextFoldOperand(ctx context.Context, operand string, position foldPosition, evaluate func(context.Context) interface{}) (interface{}, bool, error) {
	if isolateOperandErrors(operand, foldPositionInContext(ctx, position), true) {
		return deferredExpressionValue(ctx, evaluate)
	}
	return evaluate(ctx), true, nil
}

func (failure *expressionFailure) recordMapKeyOrder(value map[string]interface{}, keys []string) {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if failure.mapKeyOrders == nil {
		failure.mapKeyOrders = make(map[uintptr][]string)
	}
	failure.mapKeyOrders[uintptr(reflect.ValueOf(value).UnsafePointer())] = keys
}

func snapshotMapKeyOrders(ctx context.Context) map[uintptr][]string {
	failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	if !ok {
		return nil
	}
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if len(failure.mapKeyOrders) == 0 {
		return nil
	}
	orders := make(map[uintptr][]string, len(failure.mapKeyOrders))
	for pointer, keys := range failure.mapKeyOrders {
		orders[pointer] = append([]string(nil), keys...)
	}
	return orders
}

func recordExpressionFailure(ctx context.Context, err error) {
	if failure := expressionErrorSlot(ctx); failure != nil {
		failure.mu.Lock()
		if failure.err == nil {
			failure.err = err
		}
		failure.mu.Unlock()
	}
}

func getExpressionFailure(ctx context.Context) error {
	if failure := expressionErrorSlot(ctx); failure != nil {
		failure.mu.Lock()
		defer failure.mu.Unlock()
		return failure.err
	}
	return nil
}

func expressionErrorSlot(ctx context.Context) *expressionFailure {
	if failure, ok := ctx.Value(expressionOperandFailureKey{}).(*expressionFailure); ok {
		return failure
	}
	failure, _ := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	return failure
}

// rowValuesInExpressionContext overrides the statement context for a deferred
// operand; nested context evaluators must record their errors in that operand.
func rowValuesInExpressionContext(ctx context.Context, values map[string]interface{}) map[string]interface{} {
	scope := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		scope[name] = value
	}
	scope[rowStatementContextKey] = ctx
	return scope
}

// isolatedRowOperand evaluates a row operand with its own error slot and a
// row scope carrying it, so that the fold around it decides whether the
// operand's statement errors count (isolateOperandErrors). The one
// isolate-and-copy step of the row evaluators.
func isolatedRowOperand(ctx context.Context, values map[string]interface{}, evaluate func(context.Context, map[string]interface{}) (interface{}, bool, error)) (interface{}, bool, error) {
	return deferredExpressionResult(ctx, func(ctx context.Context) (interface{}, bool, error) {
		return evaluate(ctx, rowValuesInExpressionContext(ctx, values))
	})
}

// foldPosition is where a logical or comparison fold evaluates an operand,
// for isolateOperandErrors.
type foldPosition uint8

const (
	// foldUndecided is NOT, XOR, a single comparison or a lone operand:
	// no other operand decides the result, so every error counts.
	foldUndecided foldPosition = iota
	// foldAnd is an AND, or a link of a comparison chain: a false decides.
	foldAnd
	// foldOr is an OR: a true decides.
	foldOr
	// CASE conditions evaluate subqueries as expression operands, not Apply.
	foldCaseAnd
)

type caseConditionScopeKey struct{}

func foldPositionInContext(ctx context.Context, position foldPosition) foldPosition {
	if position == foldAnd && ctx.Value(caseConditionScopeKey{}) != nil {
		return foldCaseAnd
	}
	return position
}

// caseConditionScope changes planning scope only for conditions containing
// subqueries. Ordinary CASE expressions keep their allocation-free path.
func caseConditionScope(ctx context.Context, expression string, evaluate func(context.Context) (interface{}, bool, error)) (interface{}, bool, error) {
	if !mayContainSubqueryExpression(expression) {
		return evaluate(ctx)
	}
	ctx = context.WithValue(ctx, caseConditionScopeKey{}, true)
	return deferredExpressionResult(ctx, evaluate)
}

// applyOperand identifies a subquery lifted outside a conjunction or chain.
// A negated existence test and a CASE are expression-local instead.
func applyOperand(expression string) bool {
	if !mayContainSubqueryExpression(expression) || mayContainCaseKeyword(expression) {
		return false
	}
	if inner, negated := logicalNotOperand(expression); negated {
		if isStandaloneExistsSubquery(logicalOperandKey(inner)) {
			return false
		}
	}
	return true
}

func applyOperandIndexes(count int, operand func(int) string) []int {
	var indexes []int
	for i := 0; i < count; i++ {
		if applyOperand(operand(i)) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

type applyOperandResult struct {
	index int
	value interface{}
	ok    bool
	err   error
}

// evaluateApplyOperands owns eager Apply evaluation. Cached results are
// reused when the fold reaches each operand; callbacks never escape.
func evaluateApplyOperands(ctx context.Context, indexes []int, evaluate func(int) (interface{}, bool, error)) ([]applyOperandResult, error) {
	if len(indexes) == 0 || ctx.Value(caseConditionScopeKey{}) != nil {
		return nil, nil
	}
	results := make([]applyOperandResult, len(indexes))
	for i, index := range indexes {
		value, ok, err := evaluate(index)
		if err == nil {
			err = getExpressionFailure(ctx)
		}
		if err != nil {
			return nil, err
		}
		results[i] = applyOperandResult{index, value, ok, err}
	}
	return results, nil
}

func resolveApplyOperand(results []applyOperandResult, index int, evaluate func(int) (interface{}, bool, error)) (interface{}, bool, error) {
	for _, result := range results {
		if result.index == index {
			return result.value, result.ok, result.err
		}
	}
	return evaluate(index)
}

// logicalFoldPosition is the position of the operands of a logical plan.
func logicalFoldPosition(kind logicalKind) foldPosition {
	switch kind {
	case logicalAnd:
		return foldAnd
	case logicalOr:
		return foldOr
	}
	return foldUndecided
}

// comparisonFoldPosition is the position of the operands of a scanned
// comparison: a chain of two or more comparisons can be decided by a false
// link; a single comparison cannot.
func comparisonFoldPosition(scan *comparisonChainScan) foldPosition {
	if scan.count > 1 {
		return foldAnd
	}
	return foldUndecided
}

// isolateOperandErrors is the one policy (#1214) for which operand errors a
// logical or comparison fold may discard; every evaluator's fold asks it. It
// reports whether operand must be evaluated with its own error slot
// (isolatedRowOperand, deferredExpressionResult), so that the fold, not the
// statement, decides whether the operand's error counts:
//
//   - A subquery (EXISTS, COUNT, COLLECT) or a relationship pattern
//     predicate that can fail (rowOperandRecordsFailure) is planned by
//     Neo4j outside the expression (SemiApply, Apply) and runs for the row,
//     so its error counts, even when another AND operand is false. Only an
//     OR, which Neo4j plans to test the other operand first
//     (SelectOrSemiApply), lets a deciding operand discard it.
//   - Any other operand's error is discarded by a deciding operand, as
//     Neo4j's Ands / Ors do. It needs a slot only when its evaluator records
//     errors on the statement context (contextRecorded) instead of
//     returning them, and only when it can fail at all (operandCannotFail).
//   - CASE conditions use expression-local subqueries; a deciding AND can
//     discard their errors. Positive subqueries outside CASE are Apply
//     operands evaluated before the fold, including after a false operand.
//   - Nothing discards an error in an undecided position.
//
// A pattern comprehension is an expression operand in this sense: its atom
// returns its errors (evaluateRowPatternComprehensionAtom).
func isolateOperandErrors(operand string, position foldPosition, contextRecorded bool) bool {
	if position == foldUndecided {
		return false
	}
	if rowOperandRecordsFailure(operand) {
		return position == foldOr || position == foldCaseAnd || !applyOperand(operand)
	}
	return contextRecorded && !operandCannotFail(operand)
}

// operandIsolation is isolateOperandErrors decided once for an operand: for
// its value (contextRecorded as its evaluator is), and for the predicate
// fallback that runs when that evaluator leaves it unresolved, which records
// its errors on the context.
type operandIsolation struct{ value, fallback, caseValue bool }

func operandIsolationAt(operand string, position foldPosition, contextRecorded bool) operandIsolation {
	return operandIsolation{
		value:     isolateOperandErrors(operand, position, contextRecorded),
		fallback:  isolateOperandErrors(operand, position, true),
		caseValue: position == foldAnd && isolateOperandErrors(operand, foldCaseAnd, contextRecorded),
	}
}

// evaluate evaluates an operand of a fold through resolve (nil for an
// operand only the predicate evaluator reads), then, when resolve leaves it
// unresolved, through fallback (nil for none), each with its own error slot
// where isolation asks for one (isolatedRowOperand).
func (isolation operandIsolation) evaluate(ctx context.Context, values map[string]interface{},
	resolve func(context.Context, map[string]interface{}) (interface{}, bool, error),
	fallback func(context.Context, map[string]interface{}) interface{}) (interface{}, bool, error) {
	if isolation.caseValue && ctx.Value(caseConditionScopeKey{}) != nil {
		isolation.value, isolation.fallback = true, true
	}
	run := func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
		if resolve != nil {
			value, ok, err := resolve(ctx, scope)
			if ok || err != nil || fallback == nil {
				return value, ok, err
			}
		}
		return fallback(ctx, scope), true, nil
	}
	if isolation.value || (resolve == nil && isolation.fallback) {
		return isolatedRowOperand(ctx, values, run)
	}
	if resolve == nil {
		return run(ctx, values)
	}
	value, ok, err := resolve(ctx, values)
	if ok || err != nil || fallback == nil {
		return value, ok, err
	}
	if !isolation.fallback {
		return fallback(ctx, values), true, nil
	}
	return isolatedRowOperand(ctx, values, func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
		return fallback(ctx, scope), true, nil
	})
}

// rowOperandRecordsFailure identifies the operands planned outside the
// expression whose errors their evaluators record through the statement
// context: subqueries, and relationship pattern predicates with an
// expression that can fail (a property map or an inline WHERE).
func rowOperandRecordsFailure(expression string) bool {
	if mayContainSubqueryExpression(expression) {
		return true
	}
	return relationshipPatternMayFail(expression) && isRelationshipPatternOperand(expression)
}

// relationshipPatternMayFail reports a relationship pattern with an
// expression its match evaluates: a property map or an inline WHERE. Labels,
// types, directions and bound variables raise no error.
func relationshipPatternMayFail(pattern string) bool {
	return strings.IndexByte(pattern, '{') >= 0 || containsFold(pattern, "WHERE")
}

// operandCannotFail reports an operand whose evaluation records no error: a
// literal, a variable, a parameter or a property of a variable, a
// comparison or a null test of those, or a relationship pattern predicate
// that can't fail (relationshipPatternMayFail). Anything else may.
func operandCannotFail(operand string) bool {
	operand = logicalOperandKey(operand)
	if simpleRowOperand(operand) {
		return true
	}
	for _, suffix := range [...]string{" IS NOT NULL", " IS NULL"} {
		if hasSuffixFoldASCII(operand, suffix) {
			return simpleRowOperand(strings.TrimSpace(operand[:len(operand)-len(suffix)]))
		}
	}
	if isRelationshipPatternOperand(operand) {
		return !relationshipPatternMayFail(operand)
	}
	scan, comparison := scanComparisonChain(operand)
	if !comparison {
		return false
	}
	for index := 0; index <= scan.count; index++ {
		if !simpleRowOperand(scan.operand(operand, index)) {
			return false
		}
	}
	return true
}

// simpleRowOperand reports a literal (number, string, boolean, null), a
// variable, a parameter or a property chain of a variable (n.a.b).
func simpleRowOperand(operand string) bool {
	switch {
	case operand == "":
		return false
	case operand[0] == '\'' || operand[0] == '"':
		return isWholeCypherQuotedString(operand)
	case operand[0] == '$':
		return isValidIdentifier(operand[1:])
	case operand[0] == '-' || operand[0] == '.' || (operand[0] >= '0' && operand[0] <= '9'):
		return isNumericLiteral(operand)
	}
	for {
		dot := strings.IndexByte(operand, '.')
		if dot < 0 {
			return isValidIdentifier(operand)
		}
		if !isValidIdentifier(operand[:dot]) {
			return false
		}
		operand = operand[dot+1:]
	}
}
