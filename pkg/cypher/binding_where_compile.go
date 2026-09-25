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
	clause := strings.TrimSpace(whereClause)
	clause = strings.ReplaceAll(clause, "\r", " ")
	clause = strings.ReplaceAll(clause, "\n", " ")
	clause = strings.ReplaceAll(clause, "\t", " ")
	return clause
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
	truth, ok := e.tryCompileBindingWhereTruth(ctx, whereClause)
	if !ok {
		return nil, false
	}
	return truth.predicate(), true
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

	if predicate, ok := e.compileBindingComparisonPredicate(clause); ok {
		return liftBindingPredicate(predicate), true
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
	return func(b binding, params map[string]interface{}) bool {
		value, ok := resolver(b, params)
		if !ok {
			return !expectNotNull
		}
		if expectNotNull {
			return value != nil
		}
		return value == nil
	}, true
}

func (e *StorageExecutor) compileBindingComparisonPredicate(clause string) (bindingWherePredicate, bool) {
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

		leftIsNodeRef := isValidIdentifier(leftExpr)
		rightIsNodeRef := isValidIdentifier(rightExpr)
		if leftIsNodeRef && rightIsNodeRef {
			leftKey := leftExpr
			rightKey := rightExpr
			return func(b binding, params map[string]interface{}) bool {
				_ = params
				leftNode := b[leftKey]
				rightNode := b[rightKey]
				if leftNode == nil || rightNode == nil {
					return false
				}
				switch op {
				case "=":
					return leftNode.ID == rightNode.ID
				case "<>", "!=":
					return leftNode.ID != rightNode.ID
				default:
					return e.compareNodeIDs(string(leftNode.ID), string(rightNode.ID), op)
				}
			}, true
		}

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
			switch op {
			case "=":
				return e.compareBindingValuesEqual(leftValue, rightValue)
			case "<>", "!=":
				return !e.compareBindingValuesEqual(leftValue, rightValue)
			case ">":
				return e.compareGreater(leftValue, rightValue)
			case ">=":
				return e.compareGreater(leftValue, rightValue) || e.compareBindingValuesEqual(leftValue, rightValue)
			case "<":
				return e.compareLess(leftValue, rightValue)
			case "<=":
				return e.compareLess(leftValue, rightValue) || e.compareBindingValuesEqual(leftValue, rightValue)
			default:
				return false
			}
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
	cache.RLock()
	if cache.length == len(items) && cache.firstElement == firstElement && cache.comparable != nil {
		comparableSet := cache.comparable
		nonComparable := cache.nonComparable
		hasNull := cache.hasNull
		cache.RUnlock()
		return comparableSet, nonComparable, hasNull
	}
	cache.RUnlock()

	comparableSet, nonComparable := buildComparableMembershipIndex(items)
	hasNull := listHasNull(items)
	cache.Lock()
	cache.length = len(items)
	cache.firstElement = firstElement
	cache.comparable = comparableSet
	cache.nonComparable = nonComparable
	cache.hasNull = hasNull
	cache.Unlock()
	return comparableSet, nonComparable, hasNull
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
	if dotIdx := strings.Index(clause, "."); dotIdx > 0 {
		varName := strings.TrimSpace(clause[:dotIdx])
		propName := strings.TrimSpace(clause[dotIdx+1:])
		if varName == "" || propName == "" || strings.ContainsAny(varName, " \t\r\n") || strings.ContainsAny(propName, " \t\r\n") {
			return nil, false
		}
		return func(b binding, params map[string]interface{}) (interface{}, bool) {
			_ = params
			node := b[varName]
			if node == nil {
				return nil, false
			}
			return getBindingNodeValue(node, propName)
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
	clause = strings.ReplaceAll(clause, "\n", " ")
	clause = strings.ReplaceAll(clause, "\r", " ")
	clause = strings.ReplaceAll(clause, "\t", " ")
	upper := strings.ToUpper(clause)

	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(clause[:orIdx])
		right := strings.TrimSpace(clause[orIdx+4:])
		return e.evaluateBindingWhere(ctx, b, left, params) || e.evaluateBindingWhere(ctx, b, right, params)
	}
	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(clause[:andIdx])
		right := strings.TrimSpace(clause[andIdx+5:])
		return e.evaluateBindingWhere(ctx, b, left, params) && e.evaluateBindingWhere(ctx, b, right, params)
	}
	if strings.HasPrefix(upper, "NOT ") {
		if truth, ok := inPredicateTruth(clause[4:], func(expr string) interface{} {
			return e.evaluateExpressionWithContext(ctx, expr, map[string]*storage.Node(b), nil)
		}); ok {
			return truth == truthFalse
		}
		return !e.evaluateBindingWhere(ctx, b, clause[4:], params)
	}
	if matches, recognized := e.evaluateBoundRelationshipPattern(ctx, clause, map[string]*storage.Node(b)); recognized {
		return matches
	}

	for _, pred := range []string{" STARTS WITH ", " ENDS WITH ", " CONTAINS "} {
		if idx := findTopLevelKeyword(clause, pred); idx > 0 {
			left := strings.TrimSpace(clause[:idx])
			right := strings.TrimSpace(clause[idx+len(pred):])
			if dotIdx := strings.Index(left, "."); dotIdx > 0 {
				varName := left[:dotIdx]
				propName := left[dotIdx+1:]
				if node := b[varName]; node != nil {
					actual, _ := node.Properties[propName].(string)
					expectedRaw := e.resolveBindingFallbackValue(ctx, right, b, params)
					expected, _ := expectedRaw.(string)
					switch strings.TrimSpace(strings.ToUpper(pred)) {
					case "STARTS WITH":
						return strings.HasPrefix(actual, expected)
					case "ENDS WITH":
						return strings.HasSuffix(actual, expected)
					case "CONTAINS":
						return strings.Contains(actual, expected)
					}
				}
			}
			return false
		}
	}

	if strings.Contains(clause, "<>") || strings.Contains(clause, "!=") {
		op := "<>"
		opIdx := strings.Index(clause, "<>")
		if opIdx == -1 {
			op = "!="
			opIdx = strings.Index(clause, "!=")
		}
		left := strings.TrimSpace(clause[:opIdx])
		right := strings.TrimSpace(clause[opIdx+len(op):])
		if !strings.Contains(left, ".") && !strings.Contains(right, ".") {
			leftNode := b[left]
			rightNode := b[right]
			if leftNode != nil && rightNode != nil {
				return leftNode.ID != rightNode.ID
			}
		}
	}

	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := strings.Index(clause, op); idx > 0 {
			left := strings.TrimSpace(clause[:idx])
			right := strings.TrimSpace(clause[idx+len(op):])

			if dotIdx := strings.Index(left, "."); dotIdx > 0 {
				varName := left[:dotIdx]
				propName := left[dotIdx+1:]

				if node := b[varName]; node != nil {
					actualVal := node.Properties[propName]
					expectedVal := e.resolveBindingFallbackValue(ctx, right, b, params)

					switch op {
					case "=":
						return e.compareEqual(actualVal, expectedVal)
					case "<>", "!=":
						return !e.compareEqual(actualVal, expectedVal)
					case ">":
						return e.compareGreater(actualVal, expectedVal)
					case ">=":
						return e.compareGreater(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					case "<":
						return e.compareLess(actualVal, expectedVal)
					case "<=":
						return e.compareLess(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					}
				}
			} else {
				leftNode := b[left]
				rightNode := b[right]
				if leftNode != nil && rightNode != nil {
					switch op {
					case "=":
						return leftNode.ID == rightNode.ID
					case "<>", "!=":
						return leftNode.ID != rightNode.ID
					}
				}
			}
		}
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
	resolved := e.substituteParams(expr, params)
	result := e.evaluateExpressionWithContext(ctx, resolved, b, nil)
	if boolResult, ok := result.(bool); ok {
		return boolResult
	}
	return false
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
