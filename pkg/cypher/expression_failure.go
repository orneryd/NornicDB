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
	// foldLocalAnd is an AND in a local subquery scope
	// (localSubqueryScope): a false decides, and it discards a subquery's
	// error too, as the subquery runs only where the fold reaches it.
	foldLocalAnd
)

// Neo4j runs a subquery expression (EXISTS, COUNT, COLLECT) of a projection
// or of a WHERE's conjunct for the row before the expression (Apply), so its
// error counts even when another AND operand is false (applyOperand). Some
// subqueries its planner keeps inside the expression instead, where only
// reaching them runs them: those of a CASE condition, those of a WHERE's
// negated or disjunctive conjunct and its disjuncts (anti-semi-apply and
// select-or-semi-apply after the WHERE's filters, wherePredicatePlan), and
// the disjuncts NOT (a AND b) is normalized to (logicalNotAnd). Their
// operands are evaluated in a local subquery scope: no Apply, and a
// deciding operand discards a subquery's error (foldLocalAnd).
type localSubqueryScopeKey struct{}

func foldPositionInContext(ctx context.Context, position foldPosition) foldPosition {
	if position == foldAnd && inLocalSubqueryScope(ctx) {
		return foldLocalAnd
	}
	return position
}

func inLocalSubqueryScope(ctx context.Context) bool {
	return ctx.Value(localSubqueryScopeKey{}) != nil
}

// localSubqueryScope evaluates expression in a local subquery scope, with
// its own error slot. An expression without a subquery keeps its context and
// its allocation-free path.
func localSubqueryScope(ctx context.Context, expression string, evaluate func(context.Context) (interface{}, bool, error)) (interface{}, bool, error) {
	if !mayContainSubqueryExpression(expression) {
		return evaluate(ctx)
	}
	ctx = context.WithValue(ctx, localSubqueryScopeKey{}, true)
	return deferredExpressionResult(ctx, evaluate)
}

// withoutLocalSubqueryScope is ctx for running a subquery's body: Neo4j plans
// the body as its own query, so the body's WHERE plans its subqueries again,
// whatever scope the subquery expression stands in.
func withoutLocalSubqueryScope(ctx context.Context) context.Context {
	if inLocalSubqueryScope(ctx) {
		return context.WithValue(ctx, localSubqueryScopeKey{}, nil)
	}
	return ctx
}

// applyOperand reports an operand of an AND or a comparison chain whose
// subqueries Neo4j runs for the row before the fold (Apply), negated or not
// (RETURN x <> 0 AND NOT EXISTS {...} raises the subquery's error): one with
// a subquery outside a CASE and outside a list scope (liftableSubquery).
func applyOperand(expression string) bool {
	return liftableSubquery(expression)
}

// semiApplyPredicate reports a predicate Neo4j plans as a semi-apply, or as
// an anti-semi-apply when negated: a whole EXISTS { … }, or a COUNT { … }
// compared with 0 in the forms Neo4j rewrites to one (COUNT { … } > 0 and
// 0 < COUNT { … } are EXISTS, COUNT { … } = 0 and 0 = COUNT { … } are
// NOT EXISTS), under any number of NOTs. Only such a subquery can wait for
// an OR's other disjuncts, or, negated, for a WHERE's filters; any other
// subquery (COUNT { … } >= 1, COLLECT { … } = [], EXISTS { … } = true) is a
// value an Apply computes for the row first (liftsValueSubquery).
func semiApplyPredicate(expression string) (negated, ok bool) {
	operand := logicalOperandKey(expression)
	for {
		inner, not := logicalNotOperand(operand)
		if !not || hasTopLevelLogicalOperator(logicalOperandKey(inner)) {
			break
		}
		negated, operand = !negated, logicalOperandKey(inner)
	}
	if isStandaloneExistsSubquery(operand) {
		return negated, true
	}
	scan, comparison := scanComparisonChain(operand)
	if !comparison || scan.count != 1 {
		return false, false
	}
	span := scan.operator(0)
	operator := operand[span.offset : span.offset+span.length]
	left, right := strings.TrimSpace(scan.operand(operand, 0)), strings.TrimSpace(scan.operand(operand, 1))
	var count string
	switch {
	case operator == ">" && right == "0":
		count = left
	case operator == "<" && left == "0":
		count = right
	case operator == "=" && right == "0":
		count, negated = left, !negated
	case operator == "=" && left == "0":
		count, negated = right, !negated
	default:
		return false, false
	}
	if found, whole := standaloneSubqueryExpression(count); whole && found.kind == "COUNT" {
		return negated, true
	}
	return false, false
}

// liftsValueSubquery reports an operand whose subqueries Neo4j computes for
// the row before the expression even as an OR's disjunct: one with a
// liftable subquery (liftableSubquery) that isn't a semi-apply predicate of
// the operand's AND / OR / NOT structure (semiApplyPredicate).
func liftsValueSubquery(expression string) bool {
	if !liftableSubquery(expression) {
		return false
	}
	if _, semi := semiApplyPredicate(expression); semi {
		return false
	}
	var buffer [8]string
	plan := planLogicalExpression(logicalOperandKey(expression), buffer[:0])
	if plan.kind == logicalNone {
		return true
	}
	for _, operand := range plan.operands {
		if liftsValueSubquery(operand) {
			return true
		}
	}
	return false
}

// disjunctionApplyOperandIndexes is the Apply set of an OR (and of
// NOT (a AND b), its disjunction of negations): the operands that compute a
// value subquery (liftsValueSubquery). A semi-apply disjunct waits for the
// others (SelectOrSemiApply).
func disjunctionApplyOperandIndexes(operands []string) []int {
	var indexes []int
	for i, operand := range operands {
		if liftsValueSubquery(operand) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

// whereConjunctApplies reports a WHERE conjunct whose subqueries Neo4j runs
// for the row before the WHERE's filters. A negated semi-apply runs after
// them (anti-semi-apply), and so does a disjunction of semi-apply
// predicates (select-or-semi-apply); a positive semi-apply or a value
// subquery runs first, unless a filter is selective (selective).
func whereConjunctApplies(conjunct string, selective bool) bool {
	if selective {
		return false
	}
	if negated, semi := semiApplyPredicate(conjunct); semi {
		return !negated
	}
	var buffer [8]string
	if len(appendLogicalOperands(buffer[:0], logicalOperandKey(conjunct), " OR ")) > 0 {
		return liftsValueSubquery(conjunct)
	}
	return liftableSubquery(conjunct)
}

// selectiveComparison reports a WHERE filter Neo4j's planner runs before the
// WHERE's subqueries: a comparison link of two subquery-free operands with =,
// <, >, <= or >=, whose estimated selectivity (0.1, 0.3) makes filtering
// first cheaper. <>, IS [NOT] NULL, IN and the string predicates run after
// the subqueries (EXPLAIN on Neo4j 5.26.30).
func selectiveComparison(expression string) bool {
	key := logicalOperandKey(expression)
	if _, negated := logicalNotOperand(key); negated || hasTopLevelLogicalOperator(key) {
		return false
	}
	scan, comparison := scanComparisonChain(key)
	if !comparison {
		return false
	}
	for i := 0; i < scan.count; i++ {
		span := scan.operator(i)
		switch key[span.offset : span.offset+span.length] {
		case "=", "<", ">", "<=", ">=":
			if !mayContainSubqueryExpression(scan.operand(key, i)) && !mayContainSubqueryExpression(scan.operand(key, i+1)) {
				return true
			}
		}
	}
	return false
}

// wherePredicatePlan is a WHERE predicate whose top-level AND or OR Neo4j
// plans differently from the same expression's value: the conjuncts whose
// subqueries run after the WHERE's filters (whereConjunctApplies), and the
// disjuncts of semi-apply predicates, are evaluated in a local subquery
// scope (local), so a subquery in them runs only when the fold reaches it.
type wherePredicatePlan struct {
	logical logicalPlan
	local   []bool
}

var wherePredicatePlans = newBoundedCache[string, *wherePredicatePlan](4096, boundedCacheStringShardIndex)

// wherePredicatePlanFor returns expression's WHERE plan, or nil when the
// expression's own evaluation already runs its subqueries as Neo4j's WHERE
// does (as for every predicate without a subquery) or ctx is a local
// subquery scope (a CASE condition), where nothing is lifted.
func wherePredicatePlanFor(ctx context.Context, expression string) *wherePredicatePlan {
	if !mayContainSubqueryExpression(expression) || inLocalSubqueryScope(ctx) {
		return nil
	}
	if plan, cached := wherePredicatePlans.get(expression); cached {
		return plan
	}
	plan := planWherePredicate(expression)
	wherePredicatePlans.put(expression, plan)
	return plan
}

func planWherePredicate(expression string) *wherePredicatePlan {
	logical := planLogicalExpression(expression, nil)
	var conjunction bool
	switch logical.kind {
	case logicalAnd, logicalNotOr:
		conjunction = true
	case logicalOr, logicalNotAnd:
	case logicalNone:
		// A comparison chain is the conjunction of its links: a selective
		// link filters before the chain's subqueries (x < -1 < COUNT {…}).
		if scan, chain := scanComparisonChain(expression); chain && scan.count > 1 && liftableSubquery(expression) && selectiveComparison(expression) {
			return &wherePredicatePlan{logical: logicalPlan{kind: logicalAnd, operands: []string{expression}}, local: []bool{true}}
		}
		return nil
	default:
		return nil
	}
	selective := false
	if logical.kind == logicalAnd {
		for _, operand := range logical.operands {
			if selectiveComparison(operand) {
				selective = true
				break
			}
		}
	}
	plan := &wherePredicatePlan{logical: logical, local: make([]bool, len(logical.operands))}
	plan.logical.apply = nil
	differs := false
	for index, operand := range logical.operands {
		if !mayContainSubqueryExpression(operand) {
			continue
		}
		var applies, valueApplies bool
		if conjunction {
			conjunct := operand
			if logical.kind == logicalNotOr {
				// NOT (a OR b) is the conjunction NOT a AND NOT b.
				conjunct = "NOT (" + operand + ")"
			}
			applies, valueApplies = whereConjunctApplies(conjunct, selective), applyOperand(operand)
		} else {
			applies = liftsValueSubquery(operand)
			valueApplies = applies
		}
		if applies {
			plan.logical.apply = append(plan.logical.apply, index)
			continue
		}
		plan.local[index] = true
		// The expression's value would run this operand's subqueries before
		// the fold, or an AND inside it would run its own.
		differs = differs || valueApplies || hasApplyingConjunction(operand)
	}
	if !differs {
		return nil
	}
	return plan
}

// hasApplyingConjunction reports a fold in expression, outside a CASE, that
// runs a subquery operand before it (an AND's or a chain's applyOperand, an
// OR's value subquery): in a local subquery scope it waits instead.
func hasApplyingConjunction(expression string) bool {
	if !mayContainSubqueryExpression(expression) {
		return false
	}
	key := logicalOperandKey(expression)
	var buffer [8]string
	plan := planLogicalExpression(key, buffer[:0])
	if len(plan.apply) > 0 {
		return true
	}
	if plan.kind == logicalNone {
		scan, chain := scanComparisonChain(key)
		return chain && scan.count > 1 && liftableSubquery(key)
	}
	for _, operand := range plan.operands {
		if hasApplyingConjunction(operand) {
			return true
		}
	}
	return false
}

// liftableSubquery reports whether expression has a subquery Neo4j can run
// before the expression: one outside a CASE, whose branches it evaluates
// only when reached, coalesce's lazy arguments, and list-lambda bodies.
// A list input or reduce initializer remains in the containing row scope.
func liftableSubquery(expression string) bool {
	if !mayContainSubqueryExpression(expression) {
		return false
	}
	for _, found := range findSubqueryExpressions(expression) {
		// Only an enclosing CASE makes this subquery local. A sibling CASE
		// or one inside the subquery body does not change its Apply scope.
		if !subqueryInsideCase(expression, found.start) && !inLocalExpressionScope(expression, found.start) {
			return true
		}
	}
	return false
}

func subqueryInsideCase(expression string, offset int) bool {
	if mayContainCaseKeyword(expression) {
		for _, span := range caseBlockSpans(expression) {
			if span.start < offset && offset < span.end {
				return true
			}
		}
	}
	return false
}

// inLocalExpressionScope reads lazy argument and lambda-body boundaries.
// localBindingAt owns each lambda's input/body split; an input is not local
// merely because it is textually inside the function's parentheses.
func inLocalExpressionScope(expression string, offset int) bool {
	for i := 0; i < offset; i++ {
		switch expression[i] {
		case '\'', '"', '`':
			i = skipCypherQuotedText(expression, i, expression[i]) - 1
		case '/':
			if end := queryCommentEnd(expression, i); end >= 0 {
				i = end - 1
			}
		case '[', '(':
			if binding, ok := localBindingAt(expression, i); ok {
				if binding.close < offset {
					i = binding.close
				} else if offset >= binding.scope {
					return true
				} else if subqueryInsideCase(expression[i+1:binding.scope], offset-i-1) {
					// The CASE reader skips whole binding forms for substitution.
					// Its input still has its own enclosing CASE boundaries.
					return true
				}
				continue
			}
			closeBy := ')'
			if expression[i] == '[' {
				closeBy = ']'
			}
			closing := findMatchingDelimiter(expression, i, rune(expression[i]), closeBy)
			if closing < 0 || closing < offset {
				if closing > i {
					i = closing
				}
				continue
			}
			if expression[i] == '[' {
				if _, _, pattern := splitPatternComprehension(expression[i : closing+1]); pattern {
					return true
				}
				continue
			}
			end := i
			for end > 0 && isASCIISpace(expression[end-1]) {
				end--
			}
			word := end
			for word > 0 && isRowReceiverIdentifierByte(expression[word-1]) {
				word--
			}
			if function, ok := canonicalCallableName(expression[word:end]); ok && equalFoldASCII(function, "coalesce") && functionNameStartsAt(expression, word) {
				return true
			}
		}
	}
	return false
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
	if len(indexes) == 0 || inLocalSubqueryScope(ctx) {
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
	case logicalAnd, logicalNotOr:
		return foldAnd
	case logicalOr, logicalNotAnd:
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
//     Neo4j outside the expression (SemiApply, Apply, applyOperand) and runs
//     for the row, so its error counts, even when another AND operand is
//     false. An OR, which Neo4j plans to test the other operand first
//     (SelectOrSemiApply), and a local subquery scope (foldLocalAnd), where
//     the subquery runs only when reached, let a deciding operand discard it.
//   - Any other operand's error is discarded by a deciding operand, as
//     Neo4j's Ands / Ors do. It needs a slot only when its evaluator records
//     errors on the statement context (contextRecorded) instead of
//     returning them, and only when it can fail at all (operandCannotFail).
//   - Nothing discards an error in an undecided position.
//
// A pattern comprehension is an expression operand in this sense: its atom
// returns its errors (evaluateRowPatternComprehensionAtom).
func isolateOperandErrors(operand string, position foldPosition, contextRecorded bool) bool {
	if position == foldUndecided {
		return false
	}
	if rowOperandRecordsFailure(operand) {
		return position == foldOr || position == foldLocalAnd || !applyOperand(operand)
	}
	return contextRecorded && !operandCannotFail(operand)
}

// operandIsolation is isolateOperandErrors decided once for an operand: for
// its value (contextRecorded as its evaluator is), for the predicate
// fallback that runs when that evaluator leaves it unresolved, which records
// its errors on the context, and (local) for an AND operand evaluated in a
// local subquery scope.
type operandIsolation struct{ value, fallback, local bool }

func operandIsolationAt(operand string, position foldPosition, contextRecorded bool) operandIsolation {
	return operandIsolation{
		value:    isolateOperandErrors(operand, position, contextRecorded),
		fallback: isolateOperandErrors(operand, position, true),
		local:    position == foldAnd && isolateOperandErrors(operand, foldLocalAnd, contextRecorded),
	}
}

// evaluate evaluates an operand of a fold through resolve (nil for an
// operand only the predicate evaluator reads), then, when resolve leaves it
// unresolved, through fallback (nil for none), each with its own error slot
// where isolation asks for one (isolatedRowOperand).
func (isolation operandIsolation) evaluate(ctx context.Context, values map[string]interface{},
	resolve func(context.Context, map[string]interface{}) (interface{}, bool, error),
	fallback func(context.Context, map[string]interface{}) interface{}) (interface{}, bool, error) {
	if isolation.local && inLocalSubqueryScope(ctx) {
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
