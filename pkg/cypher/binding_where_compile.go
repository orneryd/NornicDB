package cypher

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type bindingWherePredicate func(binding, map[string]interface{}) bool

// cypherTruth is a Cypher three-valued boolean. A WHERE keeps a row only when
// its predicate is truthTrue; truthUnknown is Cypher's null (for example
// `x IN null`, or `2 IN [1, null]`). Combining predicates with NOT / AND / OR
// must keep unknown distinct from false: NOT unknown is unknown, not true.
type cypherTruth uint8

const (
	truthFalse cypherTruth = iota
	truthTrue
	truthUnknown
)

func truthOf(value bool) cypherTruth {
	if value {
		return truthTrue
	}
	return truthFalse
}

func (t cypherTruth) not() cypherTruth {
	switch t {
	case truthTrue:
		return truthFalse
	case truthFalse:
		return truthTrue
	default:
		return truthUnknown
	}
}

// xor is Kleene XOR: unknown when either side is unknown.
func (t cypherTruth) xor(other cypherTruth) cypherTruth {
	if t == truthUnknown || other == truthUnknown {
		return truthUnknown
	}
	return truthOf(t != other)
}

// value is the truth as a Cypher value: true, false or null.
func (t cypherTruth) value() interface{} {
	if t == truthUnknown {
		return nil
	}
	return t == truthTrue
}

// truthAndLazy is Kleene AND; right is evaluated only when left is not false.
func truthAndLazy(left cypherTruth, right func() cypherTruth) cypherTruth {
	if left == truthFalse {
		return truthFalse
	}
	r := right()
	if r == truthFalse {
		return truthFalse
	}
	if left == truthTrue && r == truthTrue {
		return truthTrue
	}
	return truthUnknown
}

// truthOrLazy is Kleene OR; right is evaluated only when left is not true.
func truthOrLazy(left cypherTruth, right func() cypherTruth) cypherTruth {
	if left == truthTrue {
		return truthTrue
	}
	r := right()
	if r == truthTrue {
		return truthTrue
	}
	if left == truthFalse && r == truthFalse {
		return truthFalse
	}
	return truthUnknown
}

// bindingWhereTruth is the three-valued form of a compiled binding predicate.
// Leaf predicates that can meet null (IN / NOT IN) return truthUnknown; the
// others report their boolean result as known.
type bindingWhereTruth func(binding, map[string]interface{}) cypherTruth

type bindingWherePlan struct {
	predicate bindingWherePredicate
	truth     bindingWhereTruth
}

func (p bindingWherePlan) truthValue(b binding, params map[string]interface{}) cypherTruth {
	if p.truth != nil {
		return p.truth(b, params)
	}
	return truthOf(p.predicate(b, params))
}

func (p bindingWherePlan) asPredicate() bindingWherePredicate {
	if p.predicate != nil {
		return p.predicate
	}
	truth := p.truth
	return func(b binding, params map[string]interface{}) bool {
		return truth(b, params) == truthTrue
	}
}

func (t bindingWhereTruth) predicate() bindingWherePredicate {
	return func(b binding, params map[string]interface{}) bool {
		return t(b, params) == truthTrue
	}
}

func liftBindingPredicate(predicate bindingWherePredicate) bindingWhereTruth {
	return func(b binding, params map[string]interface{}) cypherTruth {
		return truthOf(predicate(b, params))
	}
}

func notTruth(inner bindingWhereTruth) bindingWhereTruth {
	return func(b binding, params map[string]interface{}) cypherTruth {
		return inner(b, params).not()
	}
}

func andTruth(left, right bindingWhereTruth) bindingWhereTruth {
	return func(b binding, params map[string]interface{}) cypherTruth {
		return truthAndLazy(left(b, params), func() cypherTruth { return right(b, params) })
	}
}

func orTruth(left, right bindingWhereTruth) bindingWhereTruth {
	return func(b binding, params map[string]interface{}) cypherTruth {
		return truthOrLazy(left(b, params), func() cypherTruth { return right(b, params) })
	}
}

// Compiled binding WHERE predicates, cached by clause text (boundedCache).
var (
	compiledBindingWhereCache               = newBoundedCache[string, bindingWherePredicate](4096)
	compiledSupportedBindingWhereCache      = newBoundedCache[string, bindingWherePredicate](4096)
	compiledSupportedBindingWhereTruthCache = newBoundedCache[string, bindingWhereTruth](4096)
)

type bindingFilterPredicate struct {
	plan            *rowPredicatePlan
	membership      compiledRowMemberships
	queryParameters map[string]interface{}
	executor        *StorageExecutor
	ctx             context.Context
	clause          string
	parameters      map[string]interface{}
	values          map[string]interface{}
}

func (predicate *bindingFilterPredicate) matches(row binding, params map[string]interface{}) bool {
	if predicate.plan != nil {
		return predicate.executor.evaluateRowPredicatePartScope(predicate.ctx, &predicate.plan.root, compiledRowScope{nodes: row, parameters: predicate.queryParameters}, &predicate.membership)
	}
	for name, node := range row {
		predicate.values[name] = node
	}
	for name, value := range predicate.parameters {
		predicate.values[name] = value
	}
	accepted := predicate.executor.evaluateMatchRowPredicate(predicate.ctx, predicate.clause, predicate.values)
	for name := range row {
		delete(predicate.values, name)
	}
	return accepted
}

func (predicate *bindingFilterPredicate) matchesRelationships(row binding, rels relationshipBinding, params map[string]interface{}) bool {
	if predicate.plan != nil {
		return predicate.executor.evaluateRowPredicatePartScope(predicate.ctx, &predicate.plan.root, compiledRowScope{nodes: row, rels: rels, parameters: predicate.queryParameters}, &predicate.membership)
	}
	for name, value := range rels {
		predicate.values[name] = value
	}
	accepted := predicate.matches(row, params)
	for name := range rels {
		delete(predicate.values, name)
	}
	return accepted
}

// newBindingFilterPredicate owns scratch for one synchronous filter invocation.
// Its returned frame must not be cached or shared across goroutines.
func (e *StorageExecutor) newBindingFilterPredicate(ctx context.Context, whereClause string, params map[string]interface{}) bindingFilterPredicate {
	whereClause = normalizeBindingWhereClause(whereClause)
	if plan := planRowPredicate(whereClause); plan != nil && plan.complete {
		if params == nil {
			params = getParamsFromContext(ctx)
		}
		predicate := bindingFilterPredicate{executor: e, ctx: ctx, clause: whereClause, plan: plan, queryParameters: params}
		predicate.membership.prepare(&plan.root, compiledRowScope{parameters: params})
		return predicate
	}
	if params != nil && !sameParameterMap(getParamsFromContext(ctx), params) {
		ctx = withQueryParams(ctx, params)
	}
	parameters := parameterRowValues(ctx)
	plan := planRowPredicate(whereClause)
	if plan != nil && !plan.complete {
		plan = nil
	}
	return bindingFilterPredicate{
		executor: e, ctx: ctx, clause: whereClause, parameters: parameters, plan: plan,
		values: make(map[string]interface{}, len(parameters)+4),
	}
}

func (e *StorageExecutor) getCompiledBindingWhere(ctx context.Context, whereClause string) bindingWherePredicate {
	key := normalizeBindingWhereClause(whereClause)
	if predicate, ok := compiledBindingWhereCache.get(key); ok {
		return predicate
	}
	if predicate, ok := e.tryCompileBindingWhere(ctx, key); ok {
		compiledBindingWhereCache.put(key, predicate)
		return predicate
	}
	// Generic predicates may consult this executor's graph (for example, a
	// bound relationship-pattern predicate). They must not enter the global
	// cache, where the closure would retain one database and leak it into a
	// later executor using the same predicate text.
	return e.compileBindingWhere(ctx, key)
}

func normalizeBindingWhereClause(whereClause string) string {
	return normalizePipelineWhitespace(whereClause)
}

func (e *StorageExecutor) compileBindingWhere(ctx context.Context, whereClause string) bindingWherePredicate {
	if predicate, ok := e.tryCompileBindingWhere(ctx, whereClause); ok {
		return predicate
	}
	if predicate, ok := e.tryCompileExecutorBindingWhere(ctx, whereClause); ok {
		return predicate
	}
	clause := strings.TrimSpace(whereClause)
	return func(b binding, params map[string]interface{}) bool {
		return e.evaluateBindingWhereGeneric(ctx, b, clause, params)
	}
}

// tryCompileExecutorBindingWhere compiles predicates that depend on this
// executor's graph. These closures intentionally remain executor-local and
// therefore never enter compiledBindingWhereCache.
func (e *StorageExecutor) tryCompileExecutorBindingWhere(ctx context.Context, whereClause string) (bindingWherePredicate, bool) {
	truth, ok := e.tryCompileExecutorBindingWhereTruth(ctx, whereClause)
	if !ok {
		return nil, false
	}
	return truth.predicate(), true
}

func (e *StorageExecutor) tryCompileExecutorBindingWhereTruth(ctx context.Context, whereClause string) (bindingWhereTruth, bool) {
	clause := strings.TrimSpace(whereClause)
	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left, leftOK := e.compileExecutorBindingWhereBranch(ctx, clause[:orIdx])
		right, rightOK := e.compileExecutorBindingWhereBranch(ctx, clause[orIdx+4:])
		if !leftOK || !rightOK {
			return nil, false
		}
		return orTruth(left, right), true
	}
	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left, leftOK := e.compileExecutorBindingWhereBranch(ctx, clause[:andIdx])
		right, rightOK := e.compileExecutorBindingWhereBranch(ctx, clause[andIdx+5:])
		if !leftOK || !rightOK {
			return nil, false
		}
		return andTruth(left, right), true
	}
	if hasPrefixFold(clause, "NOT ") {
		inner, ok := e.compileExecutorBindingWhereBranch(ctx, clause[4:])
		if !ok {
			return nil, false
		}
		return notTruth(inner), true
	}
	match, ok := e.parseBoundRelationshipPattern(ctx, clause)
	if !ok {
		return nil, false
	}
	return func(b binding, params map[string]interface{}) cypherTruth {
		_ = params
		return truthOf(e.evaluateParsedBoundRelationshipPattern(ctx, match, map[string]*storage.Node(b)))
	}, true
}

func (e *StorageExecutor) compileExecutorBindingWhereBranch(ctx context.Context, clause string) (bindingWhereTruth, bool) {
	clause = strings.TrimSpace(clause)
	if truth, ok := e.tryCompileBindingWhereTruth(ctx, clause); ok {
		return truth, true
	}
	return e.tryCompileExecutorBindingWhereTruth(ctx, clause)
}

func (e *StorageExecutor) getCompiledBindingWhereIfSupported(ctx context.Context, whereClause string) (bindingWherePredicate, bool) {
	key := normalizeBindingWhereClause(whereClause)
	if predicate, ok := compiledSupportedBindingWhereCache.get(key); ok {
		return predicate, true
	}
	predicate, ok := e.tryCompileBindingWhere(ctx, key)
	if ok {
		compiledSupportedBindingWhereCache.put(key, predicate)
	}
	return predicate, ok
}

func (e *StorageExecutor) getCompiledBindingWhereTruthIfSupported(ctx context.Context, whereClause string) (bindingWhereTruth, bool) {
	key := normalizeBindingWhereClause(whereClause)
	if truth, ok := compiledSupportedBindingWhereTruthCache.get(key); ok {
		return truth, true
	}
	truth, ok := e.tryCompileBindingWhereTruth(ctx, key)
	if ok {
		compiledSupportedBindingWhereTruthCache.put(key, truth)
	}
	return truth, ok
}

// tryCompileBindingWhere compiles a WHERE clause into a row predicate that is
// true only when the clause is known true (Cypher three-valued logic: null and
// false both drop the row).
func (e *StorageExecutor) tryCompileBindingWhere(ctx context.Context, whereClause string) (bindingWherePredicate, bool) {
	plan, ok := e.compileBindingWherePlan(ctx, whereClause)
	if !ok {
		return nil, false
	}
	return plan.asPredicate(), true
}

func (e *StorageExecutor) compileBindingWherePlan(ctx context.Context, whereClause string) (bindingWherePlan, bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return bindingWherePlan{predicate: func(binding, map[string]interface{}) bool { return true }}, true
	}
	if inner, ok := stripEnclosingExpressionParentheses(clause); ok {
		return e.compileBindingWherePlan(ctx, inner)
	}
	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left, leftOK := e.compileBindingWherePlan(ctx, clause[:orIdx])
		right, rightOK := e.compileBindingWherePlan(ctx, clause[orIdx+4:])
		if !leftOK || !rightOK {
			return bindingWherePlan{}, false
		}
		if left.truth == nil && right.truth == nil {
			leftPredicate, rightPredicate := left.predicate, right.predicate
			return bindingWherePlan{predicate: func(b binding, params map[string]interface{}) bool {
				return leftPredicate(b, params) || rightPredicate(b, params)
			}}, true
		}
		leftPredicate, rightPredicate := left.asPredicate(), right.asPredicate()
		return bindingWherePlan{predicate: func(b binding, params map[string]interface{}) bool {
			return leftPredicate(b, params) || rightPredicate(b, params)
		}, truth: func(b binding, params map[string]interface{}) cypherTruth {
			return truthOrLazy(left.truthValue(b, params), func() cypherTruth {
				return right.truthValue(b, params)
			})
		}}, true
	}
	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left, leftOK := e.compileBindingWherePlan(ctx, clause[:andIdx])
		right, rightOK := e.compileBindingWherePlan(ctx, clause[andIdx+5:])
		if !leftOK || !rightOK {
			return bindingWherePlan{}, false
		}
		if left.truth == nil && right.truth == nil {
			leftPredicate, rightPredicate := left.predicate, right.predicate
			return bindingWherePlan{predicate: func(b binding, params map[string]interface{}) bool {
				return leftPredicate(b, params) && rightPredicate(b, params)
			}}, true
		}
		leftPredicate, rightPredicate := left.asPredicate(), right.asPredicate()
		return bindingWherePlan{predicate: func(b binding, params map[string]interface{}) bool {
			return leftPredicate(b, params) && rightPredicate(b, params)
		}, truth: func(b binding, params map[string]interface{}) cypherTruth {
			return truthAndLazy(left.truthValue(b, params), func() cypherTruth {
				return right.truthValue(b, params)
			})
		}}, true
	}
	if hasPrefixFold(clause, "NOT ") {
		inner, ok := e.compileBindingWherePlan(ctx, clause[len("NOT "):])
		if !ok {
			return bindingWherePlan{}, false
		}
		if inner.truth == nil {
			predicate := inner.predicate
			return bindingWherePlan{predicate: func(b binding, params map[string]interface{}) bool {
				return !predicate(b, params)
			}}, true
		}
		truth := inner.truth
		return bindingWherePlan{truth: func(b binding, params map[string]interface{}) cypherTruth {
			return truth(b, params).not()
		}}, true
	}

	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NOT NULL", true); ok {
		return bindingWherePlan{predicate: predicate}, true
	}
	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NULL", false); ok {
		return bindingWherePlan{predicate: predicate}, true
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " STARTS WITH "); ok {
		return bindingWherePlan{predicate: predicate}, true
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " ENDS WITH "); ok {
		return bindingWherePlan{predicate: predicate}, true
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " CONTAINS "); ok {
		return bindingWherePlan{predicate: predicate}, true
	}
	if truth, ok := e.compileBindingInPredicate(clause, " IN ", false); ok {
		return bindingWherePlan{truth: truth}, true
	}
	if truth, ok := e.compileBindingInPredicate(clause, " NOT IN ", true); ok {
		return bindingWherePlan{truth: truth}, true
	}
	if truth, ok := e.compileBindingComparisonTruth(clause); ok {
		return bindingWherePlan{truth: truth}, true
	}
	return bindingWherePlan{}, false
}

func (e *StorageExecutor) tryCompileBindingWhereTruth(ctx context.Context, whereClause string) (bindingWhereTruth, bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return func(binding, map[string]interface{}) cypherTruth { return truthTrue }, true
	}

	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left, okLeft := e.getCompiledBindingWhereTruthIfSupported(ctx, clause[:orIdx])
		right, okRight := e.getCompiledBindingWhereTruthIfSupported(ctx, clause[orIdx+4:])
		if !okLeft || !okRight {
			return nil, false
		}
		return orTruth(left, right), true
	}
	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left, okLeft := e.getCompiledBindingWhereTruthIfSupported(ctx, clause[:andIdx])
		right, okRight := e.getCompiledBindingWhereTruthIfSupported(ctx, clause[andIdx+5:])
		if !okLeft || !okRight {
			return nil, false
		}
		return andTruth(left, right), true
	}
	if hasPrefixFold(clause, "NOT ") {
		inner, ok := e.getCompiledBindingWhereTruthIfSupported(ctx, clause[4:])
		if !ok {
			return nil, false
		}
		return notTruth(inner), true
	}

	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NOT NULL", true); ok {
		return liftBindingPredicate(predicate), true
	}
	if predicate, ok := e.compileBindingNullPredicate(clause, " IS NULL", false); ok {
		return liftBindingPredicate(predicate), true
	}

	if predicate, ok := e.compileBindingStringPredicate(clause, " STARTS WITH "); ok {
		return liftBindingPredicate(predicate), true
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " ENDS WITH "); ok {
		return liftBindingPredicate(predicate), true
	}
	if predicate, ok := e.compileBindingStringPredicate(clause, " CONTAINS "); ok {
		return liftBindingPredicate(predicate), true
	}
	if truth, ok := e.compileBindingInPredicate(clause, " IN ", false); ok {
		return truth, true
	}
	if truth, ok := e.compileBindingInPredicate(clause, " NOT IN ", true); ok {
		return truth, true
	}

	if truth, ok := e.compileBindingComparisonTruth(clause); ok {
		return truth, true
	}

	return nil, false
}

func (e *StorageExecutor) compileBindingStringPredicate(clause, op string) (bindingWherePredicate, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	leftExpr := strings.TrimSpace(clause[:idx])
	rightExpr := strings.TrimSpace(clause[idx+len(op):])
	leftResolver, ok := e.compileBindingValueResolver(leftExpr)
	if !ok {
		return nil, false
	}
	rightResolver, ok := e.compileBindingValueResolver(rightExpr)
	if !ok {
		return nil, false
	}
	return func(b binding, params map[string]interface{}) bool {
		leftValue, ok := leftResolver(b, params)
		if !ok {
			return false
		}
		rightValue, ok := rightResolver(b, params)
		if !ok {
			return false
		}
		leftStr, ok := leftValue.(string)
		if !ok {
			return false
		}
		rightStr, ok := rightValue.(string)
		if !ok {
			return false
		}
		switch op {
		case " STARTS WITH ":
			return strings.HasPrefix(leftStr, rightStr)
		case " ENDS WITH ":
			return strings.HasSuffix(leftStr, rightStr)
		case " CONTAINS ":
			return strings.Contains(leftStr, rightStr)
		default:
			return false
		}
	}, true
}

func (e *StorageExecutor) compileBindingNullPredicate(clause, op string, expectNotNull bool) (bindingWherePredicate, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	expr := strings.TrimSpace(clause[:idx])
	if expr == "" {
		return nil, false
	}
	resolver, ok := e.compileBindingValueResolver(expr)
	if !ok {
		return nil, false
	}
	handler := nullEvaluationHandler(expectNotNull)
	return func(b binding, params map[string]interface{}) bool {
		value, _ := resolver(b, params)
		return handler.evaluate(value)
	}, true
}

func (e *StorageExecutor) compileBindingComparisonTruth(clause string) (bindingWhereTruth, bool) {
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		idx := findTopLevelKeyword(clause, op)
		if idx <= 0 {
			continue
		}
		leftExpr := strings.TrimSpace(clause[:idx])
		rightExpr := strings.TrimSpace(clause[idx+len(op):])

		if leftExpr == "" || rightExpr == "" {
			return nil, false
		}

		leftResolver, ok := e.compileBindingValueResolver(leftExpr)
		if !ok {
			return nil, false
		}
		rightResolver, ok := e.compileBindingValueResolver(rightExpr)
		if !ok {
			return nil, false
		}

		handler := comparisonEvaluationHandler(op)
		constantNumbers := constantNumericComparison(op, leftExpr, rightExpr)
		return func(b binding, params map[string]interface{}) cypherTruth {
			leftValue, ok := leftResolver(b, params)
			if !ok {
				return truthUnknown
			}
			rightValue, ok := rightResolver(b, params)
			if !ok {
				return truthUnknown
			}
			if constantNumbers {
				leftValue, rightValue = promoteIntegerToFloat(leftValue, rightValue)
			}
			matched, known := handler.evaluate(leftValue, rightValue).(bool)
			if !known {
				return truthUnknown
			}
			return truthOf(matched)
		}, true
	}
	return nil, false
}

// compileBindingInPredicate compiles `x IN list` (negate=false) and
// `x NOT IN list` (negate=true) with Cypher's null rules: a null or unresolved
// x, a null or non-list right-hand side, or a list that contains null and no
// match all give truthUnknown, which negation leaves unknown.
func (e *StorageExecutor) compileBindingInPredicate(clause, op string, negate bool) (bindingWhereTruth, bool) {
	truth, ok := compileMembershipTruth(e, clause, op, negate, func(expression string) (func(binding, map[string]interface{}) (interface{}, bool), bool) {
		return e.compileBindingValueResolver(expression)
	})
	if !ok {
		return nil, false
	}
	return bindingWhereTruth(truth), true
}

// compileMembershipTruth is the one compiled `x [NOT] IN list` test, for any
// row type R: the MATCH WHERE compiler (R = binding) and the CALL-tail WHERE
// compiler (R = the yielded values) share it (#547). resolve compiles an
// operand into a resolver over R. The list is a literal (indexed once), a
// $parameter (indexed once per distinct list value) or any other resolvable
// expression. Three-valued: an unresolved or null x, a null or non-list
// right side, or a list holding null without a match give truthUnknown, which
// negate leaves unknown.
func compileMembershipTruth[R any](e *StorageExecutor, clause, op string, negate bool, resolve func(string) (func(R, map[string]interface{}) (interface{}, bool), bool)) (func(R, map[string]interface{}) cypherTruth, bool) {
	idx := findTopLevelKeyword(clause, op)
	if idx <= 0 {
		return nil, false
	}
	leftExpr := strings.TrimSpace(clause[:idx])
	rightExpr := strings.TrimSpace(clause[idx+len(op):])
	left, ok := resolve(leftExpr)
	if !ok {
		return nil, false
	}
	var truth func(R, map[string]interface{}) cypherTruth
	if listValues, ok := parseBindingLiteralList(rightExpr); ok {
		truth = staticMembershipTruth(e, left, listValues)
	} else if strings.HasPrefix(rightExpr, "$") {
		paramName := strings.TrimSpace(strings.TrimPrefix(rightExpr, "$"))
		if paramName == "" {
			return nil, false
		}
		truth = paramMembershipTruth(e, left, paramName)
	} else {
		right, ok := resolve(rightExpr)
		if !ok {
			return nil, false
		}
		truth = func(row R, params map[string]interface{}) cypherTruth {
			leftValue, ok := left(row, params)
			if !ok {
				return truthUnknown
			}
			rightValue, ok := right(row, params)
			if !ok || rightValue == nil {
				return truthUnknown
			}
			items, ok := toInterfaceSlice(rightValue)
			if !ok {
				return truthUnknown
			}
			comparableSet, nonComparable := buildComparableMembershipIndex(items)
			return membershipTruth(leftValue, comparableSet, nonComparable, listHasNull(items), e.compareBindingValuesEqual)
		}
	}
	if negate {
		return func(row R, params map[string]interface{}) cypherTruth {
			return truth(row, params).not()
		}, true
	}
	return truth, true
}

// membershipTruth is the three-valued result of `actual IN list` for a list
// indexed by buildComparableMembershipIndex (which leaves out null items).
func membershipTruth(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, hasNull bool, equals func(interface{}, interface{}) bool) cypherTruth {
	if len(comparableSet) == 0 && len(nonComparable) == 0 && !hasNull {
		return truthFalse // x IN [] is false, even for a null x
	}
	if actual == nil {
		return truthUnknown
	}
	if evaluateComparableMembership(actual, comparableSet, nonComparable, equals) {
		return truthTrue
	}
	if hasNull {
		return truthUnknown
	}
	return truthFalse
}

func listHasNull(items []interface{}) bool {
	for _, item := range items {
		if item == nil {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) makeCompiledBindingMembershipPredicate(leftResolver bindingValueResolver, items []interface{}) bindingWhereTruth {
	return bindingWhereTruth(staticMembershipTruth(e, leftResolver, items))
}

// staticMembershipTruth is membership in a literal list, indexed once.
func staticMembershipTruth[R any, L ~func(R, map[string]interface{}) (interface{}, bool)](e *StorageExecutor, left L, items []interface{}) func(R, map[string]interface{}) cypherTruth {
	comparableSet, nonComparable := buildComparableMembershipIndex(items)
	hasNull := listHasNull(items)
	return func(row R, params map[string]interface{}) cypherTruth {
		leftValue, ok := left(row, params)
		if !ok {
			return truthUnknown
		}
		return membershipTruth(leftValue, comparableSet, nonComparable, hasNull, e.compareBindingValuesEqual)
	}
}

type bindingParamMembershipCache struct {
	sync.RWMutex
	firstElement  *interface{}
	length        int
	snapshot      []interface{}
	comparable    map[interface{}]struct{}
	nonComparable []interface{}
	hasNull       bool
}

func (e *StorageExecutor) makeCompiledParamMembershipPredicate(leftResolver bindingValueResolver, paramName string) bindingWhereTruth {
	return bindingWhereTruth(paramMembershipTruth(e, leftResolver, paramName))
}

// paramMembershipTruth is membership in a $parameter list, indexed once per
// distinct list value (bindingParamMembershipCache).
func paramMembershipTruth[R any, L ~func(R, map[string]interface{}) (interface{}, bool)](e *StorageExecutor, left L, paramName string) func(R, map[string]interface{}) cypherTruth {
	cache := &bindingParamMembershipCache{length: -1}
	return func(row R, params map[string]interface{}) cypherTruth {
		rightValue, ok := params[paramName]
		if !ok || rightValue == nil {
			return truthUnknown
		}
		items, ok := toInterfaceSlice(rightValue)
		if !ok {
			return truthUnknown
		}
		leftValue, ok := left(row, params)
		if !ok {
			return truthUnknown
		}
		firstElement := firstInterfaceElement(items)
		comparableSet, nonComparable, hasNull := cache.get(items, firstElement)
		return membershipTruth(leftValue, comparableSet, nonComparable, hasNull, e.compareBindingValuesEqual)
	}
}

func (cache *bindingParamMembershipCache) get(items []interface{}, firstElement *interface{}) (map[interface{}]struct{}, []interface{}, bool) {
	return cache.getValidated(items)
}

func (cache *bindingParamMembershipCache) getValidated(items []interface{}) (map[interface{}]struct{}, []interface{}, bool) {
	cache.Lock()
	defer cache.Unlock()
	unchanged := cache.comparable != nil && len(cache.snapshot) == len(items)
	if unchanged {
		for index, item := range items {
			previous := cache.snapshot[index]
			if item == nil && previous == nil {
				continue
			}
			if !isComparableValue(item) || !isComparableValue(previous) || previous != item {
				unchanged = false
				break
			}
		}
	}
	if !unchanged {
		cache.comparable, cache.nonComparable = buildComparableMembershipIndex(items)
		cache.hasNull = listHasNull(items)
		cache.snapshot = append(cache.snapshot[:0], items...)
	}
	return cache.comparable, cache.nonComparable, cache.hasNull
}

func firstInterfaceElement(items []interface{}) *interface{} {
	if len(items) == 0 {
		return nil
	}
	return &items[0]
}

func (e *StorageExecutor) compareBindingValuesEqual(leftValue, rightValue interface{}) bool {
	if leftValue == nil || rightValue == nil {
		return leftValue == nil && rightValue == nil
	}
	if reflect.TypeOf(leftValue) == reflect.TypeOf(rightValue) && isComparableValue(leftValue) {
		return leftValue == rightValue
	}
	return e.compareEqual(leftValue, rightValue)
}

func parseBindingLiteralList(raw string) ([]interface{}, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	if inner == "" {
		return []interface{}{}, true
	}
	parts := splitTopLevelCommaKeepEmpty(inner)
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := parseLiteralValue(part)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

type bindingValueResolver func(binding, map[string]interface{}) (interface{}, bool)

func (e *StorageExecutor) compileBindingValueResolver(expr string) (bindingValueResolver, bool) {
	clause := strings.TrimSpace(expr)
	if clause == "" {
		return func(binding, map[string]interface{}) (interface{}, bool) { return nil, false }, false
	}
	if literal, ok := parseLiteralValue(clause); ok {
		return func(binding, map[string]interface{}) (interface{}, bool) { return literal, true }, true
	}
	if strings.HasPrefix(clause, "$") {
		paramName := strings.TrimSpace(strings.TrimPrefix(clause, "$"))
		if paramName == "" {
			return nil, false
		}
		return func(_ binding, params map[string]interface{}) (interface{}, bool) {
			if params == nil {
				return nil, false
			}
			value, ok := params[paramName]
			return value, ok
		}, true
	}
	// A supported function call compiles through the same shared helper the
	// row evaluator applies (#728): the compiled fast path is a compiled form
	// of the one evaluator, with identical results.
	if function, argument, ok := parseFunctionCallWS(clause); ok {
		if inner, ok := e.compileBindingValueResolver(argument); ok {
			switch lowerASCII(function) {
			case "size":
				return func(b binding, params map[string]interface{}) (interface{}, bool) {
					value, ok := inner(b, params)
					if !ok {
						return nil, false
					}
					result, resolved, err := evaluateCypherSize(value)
					if err != nil || !resolved {
						return nil, false
					}
					return result, true
				}, true
			}
		}
	}
	if dotIdx := strings.Index(clause, "."); dotIdx > 0 {
		varName := strings.TrimSpace(clause[:dotIdx])
		propName := strings.TrimSpace(clause[dotIdx+1:])
		// Only a symbolic property name resolves here: an expression tail
		// (b.id+2) must not compile into a property lookup named "id+2" —
		// the caller falls back to the shared evaluator, which computes the
		// arithmetic exactly (#692).
		property, validProperty := isOneSymbolicName(propName)
		if !validProperty || varName == "" || strings.ContainsAny(varName, " \t\r\n") {
			return nil, false
		}
		return func(b binding, params map[string]interface{}) (interface{}, bool) {
			_ = params
			node := b[varName]
			if node == nil {
				return nil, false
			}
			return getBindingNodeValue(node, property)
		}, true
	}
	if isValidIdentifier(clause) {
		return func(b binding, params map[string]interface{}) (interface{}, bool) {
			_ = params
			node := b[clause]
			if node == nil {
				return nil, false
			}
			return node, true
		}, true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateBindingWhereGeneric(ctx context.Context, b binding, whereClause string, params map[string]interface{}) bool {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return true
	}
	return e.evaluateBindingExpressionAsBoolean(ctx, b, clause, params)
}

func (e *StorageExecutor) resolveBindingFallbackValue(ctx context.Context, expr string, b binding, params map[string]interface{}) interface{} {
	value, ok := e.resolveBindingFallbackValueWithOk(ctx, expr, b, params)
	if !ok {
		return nil
	}
	return value
}

func (e *StorageExecutor) resolveBindingFallbackValueWithOk(ctx context.Context, expr string, b binding, params map[string]interface{}) (interface{}, bool) {
	resolver, ok := e.compileBindingValueResolver(expr)
	if ok {
		return resolver(b, params)
	}
	if params != nil {
		if value, exists := params[strings.TrimSpace(expr)]; exists {
			return value, true
		}
	}
	return e.evaluateExpressionWithContext(ctx, e.substituteParams(expr, params), b, nil), true
}

func (e *StorageExecutor) evaluateBindingExpressionAsBoolean(ctx context.Context, b binding, expr string, params map[string]interface{}) bool {
	if params != nil && !sameParameterMap(getParamsFromContext(ctx), params) {
		ctx = withQueryParams(ctx, params)
	}
	values := make(map[string]interface{}, len(b)+len(params))
	for name, node := range b {
		values[name] = node
	}
	bindParameterRow(ctx, pipelineRow(values))
	return e.evaluateMatchRowPredicate(ctx, expr, values)
}

func (e *StorageExecutor) compareNodeIDs(leftID, rightID string, op string) bool {
	leftNum, leftErr := strconv.ParseInt(leftID, 10, 64)
	rightNum, rightErr := strconv.ParseInt(rightID, 10, 64)
	if leftErr == nil && rightErr == nil {
		switch op {
		case ">":
			return leftNum > rightNum
		case ">=":
			return leftNum >= rightNum
		case "<":
			return leftNum < rightNum
		case "<=":
			return leftNum <= rightNum
		default:
			return false
		}
	}

	switch op {
	case ">":
		return leftID > rightID
	case ">=":
		return leftID >= rightID
	case "<":
		return leftID < rightID
	case "<=":
		return leftID <= rightID
	default:
		return false
	}
}
