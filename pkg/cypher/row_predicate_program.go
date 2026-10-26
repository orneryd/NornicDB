package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"sync/atomic"
)

// Row predicate programs retain parsing decisions across rows. Operands are
// value programs, including CASE expressions, so skipped logical operands and
// lazy function arguments do not evaluate their CASE branches. Unsupported
// values continue through the text evaluator from that point.

// rowPredicateProgramsDisabled turns predicate programs off, for tests.
var rowPredicateProgramsDisabled atomic.Bool

var rowPredicateSeen = &rowPredicateProgram{}

var rowPredicatePrograms = newBoundedCache[*cyphertext.Source, *rowPredicateProgram](4096, func(source *cyphertext.Source) int { return boundedCacheStringShardIndex(source.Text) })

// rowPredicateProgramFor returns expression's program, or nil when it is to
// be read from the text this time.
func rowPredicateProgramFor(view braceView) *rowPredicateProgram {
	if rowPredicateProgramsDisabled.Load() || rowValueProgramsDisabled.Load() {
		return nil
	}
	program, cached := rowPredicatePrograms.get(view.source)
	if !cached {
		rowPredicatePrograms.put(view.source, rowPredicateSeen)
		return nil
	}
	if program != rowPredicateSeen {
		return program
	}
	program = compileRowPredicateProgram(view)
	rowPredicatePrograms.put(view.source, program)
	return program
}

// rowPredicateProgram evaluates CASE at its position in the value program.
type rowPredicateProgram struct {
	expression       string
	statementContext bool
	root             rowPredicateNode
}
type rowPredicateNode interface {
	holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool
}

func compileRowPredicateProgram(view braceView) *rowPredicateProgram {
	expression := view.text()
	p := &rowPredicateProgram{expression: expression, statementContext: rowPredicateNeedsStatementContext(expression)}
	if mayContainCaseKeyword(expression) && len(caseBlockSpans(view)) > 0 {
		p.root = &rowPredicateStrictNode{text: expression, view: view, value: compileRowPredicateValue(view), arithmetic: predicateIsArithmetic(expression)}
	} else {
		p.root = compileRowPredicateParts(view)
	}
	return p
}
func (p *rowPredicateProgram) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	if p.statementContext {
		var contextBound bool
		values, contextBound = rowValuesBindingStatementContext(ctx, values)
		if contextBound {
			defer delete(values, rowStatementContextKey)
		}
	}
	return p.root.holds(e, ctx, values)
}

// preparedRowPredicate evaluates one predicate for many rows (a WHERE over
// the paths or rows of one clause) as evaluateRowPredicate evaluates it for
// each, without looking the predicate's plan or program up by its text for
// every row: that lookup hashes the whole text, which for a long predicate
// cost more than evaluating it.
type preparedRowPredicate struct {
	expression braceView
	plan       *rowPredicatePlan
	program    *rowPredicateProgram
}

func prepareRowPredicate(expression braceView) *preparedRowPredicate {
	return &preparedRowPredicate{expression: expression}
}

func (p *preparedRowPredicate) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	if p.plan != nil {
		return e.evaluateRowPredicatePlan(ctx, p.plan, values)
	}
	if p.program != nil {
		return p.program.holds(e, ctx, values)
	}
	held := e.evaluateRowPredicate(ctx, p.expression, values)
	// The evaluation decided how the next row is evaluated: from the
	// predicate's complete plan, or once it is compiled, from its program.
	if plan, cached := rowPredicatePlans.get(p.expression.source); cached && plan != nil && plan.complete {
		p.plan = plan
	} else if program, cached := rowPredicatePrograms.get(p.expression.source); cached && program != rowPredicateSeen && !rowPredicateProgramsDisabled.Load() && !rowValueProgramsDisabled.Load() {
		p.program = program
	}
	return held
}

func compileRowPredicateValue(view braceView) rowValueNode {
	compiler := rowValueCompiler{}
	node, _ := compiler.compile(view)
	return node
}

// compileRowPredicateParts compiles a predicate as evaluateRowPredicateParts
// evaluates it.
func compileRowPredicateParts(view braceView) rowPredicateNode {
	view = view.trim()
	expr := view.text()
	if expr == "" {
		return rowPredicateConstNode(false)
	}
	if _, ok := stripEnclosingExpressionParentheses(expr); ok {
		return compileRowPredicateParts(view.window(1, len(expr)-1))
	}
	if mayContainArithmetic(expr) && rowPredicateIsStrictLogical(view) && !containsRelationshipPattern(expr) {
		return &rowPredicateStrictNode{text: expr, view: view, value: compileRowPredicateValue(view), arithmetic: predicateIsArithmetic(expr)}
	}
	return compileRowPredicateFromSubqueries(view)
}

// compileRowPredicateFromSubqueries compiles a predicate as
// evaluateRowPredicatePartsFromSubqueries evaluates it.
func compileRowPredicateFromSubqueries(view braceView) rowPredicateNode {
	view = view.trim()
	expr := view.text()
	if plan := planRowSubqueries(view); plan != nil && !plan.logical {
		return &rowPredicatePartsTextNode{view: view}
	}
	if variable, labels, ok := parseWithWhereLabelTest(expr); ok {
		return &rowPredicateLabelNode{variable: variable, labels: labels}
	}
	if plan := planRowPredicate(view); plan != nil {
		return &rowPredicatePlanNode{plan: plan}
	}
	var buffer [8]string
	if plan := planLogicalExpression(view, buffer[:0]); plan.kind != logicalNone {
		plan.operands = append([]string(nil), plan.operands...)
		node := &rowPredicateLogicalNode{plan: plan, arithmetic: predicateIsArithmetic(expr)}
		for index := range plan.operands {
			node.operands = append(node.operands, compileRowPredicateOperand(plan.operandView(view, index)))
		}
		return node
	}
	return compileRowPredicateLeaf(view)
}

// compileRowPredicateLeaf compiles a predicate without a top-level logical
// operator, as evaluateRowPredicateTextFromSubqueries evaluates it.
func compileRowPredicateLeaf(view braceView) rowPredicateNode {
	view = view.trim()
	expr := view.text()
	if plan := planRowSubqueries(view); plan != nil {
		return &rowPredicateLeafTextNode{view: view}
	}
	if isRelationshipPatternOperand(expr) {
		return &rowPredicateLeafTextNode{view: view}
	}
	for _, predicate := range []struct {
		operator string
		match    func(string, string) bool
	}{{" STARTS WITH ", strings.HasPrefix}, {" ENDS WITH ", strings.HasSuffix}, {" CONTAINS ", strings.Contains}} {
		if left, right, ok := splitByOperatorOutsideCase(view, predicate.operator, true, true); ok {
			return &rowPredicateStringNode{left: compileRowPredicateOperand(view.window(0, len(left))), right: compileRowPredicateOperand(view.window(len(expr)-len(right), len(expr))), match: predicate.match}
		}
	}
	if _, _, ok := splitByOperatorOutsideCase(view, "=~", false, true); ok {
		return &rowPredicateLeafTextNode{view: view}
	}
	for _, membership := range []struct {
		operator string
		negated  bool
	}{{" NOT IN ", true}, {" IN ", false}} {
		if left, right, ok := splitByOperatorOutsideCase(view, membership.operator, true, true); ok {
			return &rowPredicateMembershipNode{needle: compileRowPredicateValue(view.window(0, len(left))), haystack: compileRowPredicateValue(view.window(len(expr)-len(right), len(expr))), identity: isRowIdentityExpression(left), negated: membership.negated}
		}
	}
	if _, _, _, isTypePredicate, _ := splitTypePredicate(view); isTypePredicate {
		return &rowPredicateLeafTextNode{view: view}
	}
	if _, _, _, isNormalization := splitNormalizationPredicate(expr); isNormalization {
		return &rowPredicateLeafTextNode{view: view}
	}
	for _, operator := range []string{" IS NOT NULL", " IS NULL"} {
		if hasSuffixFoldASCII(expr, operator) {
			return &rowPredicateNullNode{operand: compileRowPredicateOperand(view.window(0, len(expr)-len(operator))), isNull: operator == " IS NULL"}
		}
	}
	if scan, comparison := scanComparisonChain(view); comparison {
		node := &rowPredicateComparisonNode{text: expr, scan: scan, normalizeIdentity: comparisonChainHasIdentity(expr, &scan)}
		for i := 0; i <= scan.count; i++ {
			node.operands = append(node.operands, compileRowPredicateOperand(view.spanWindow(scan.operandSpan(len(expr), i))))
		}
		return node
	}
	return &rowPredicateValueNode{operand: compileRowPredicateOperand(view), arithmetic: predicateIsArithmetic(expr)}
}

// rowPredicateOperand is an operand of a predicate, resolved as
// rowPredicateOperand resolves it.
type rowPredicateOperand struct {
	text           string
	view           braceView
	pattern        bool
	recordsFailure bool
	value          rowValueNode
}

func compileRowPredicateOperand(view braceView) rowPredicateOperand {
	text := view.text()
	pattern := isRelationshipPatternOperand(text)
	o := rowPredicateOperand{text: text, view: view, pattern: pattern, recordsFailure: pattern || rowOperandRecordsFailure(text)}
	if !pattern {
		o.value = compileRowPredicateValue(view)
	}
	return o
}

func (o *rowPredicateOperand) evaluate(e *StorageExecutor, ctx context.Context, values map[string]interface{}) (interface{}, bool, error) {
	if o.pattern {
		value, ok := e.rowPredicateOperand(ctx, o.view, values)
		return value, ok, getExpressionFailure(ctx)
	}
	return o.value.eval(e, values, nil)
}

func (o *rowPredicateOperand) resolve(e *StorageExecutor, ctx context.Context, values map[string]interface{}) (interface{}, bool) {
	value, ok, err := o.evaluate(e, ctx, values)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	return value, ok
}

// Only context-writing operands need an isolated error slot and row scope.
// Value programs return errors explicitly, so the deciding common operand
// can go directly to the logical/comparison fold without these allocations.
func (e *StorageExecutor) evaluateRowLogicalPredicateOperand(ctx context.Context, view braceView, values map[string]interface{}, recordsFailure bool,
	resolve func(context.Context, map[string]interface{}) (interface{}, bool, error), predicateFallback bool) (interface{}, bool, error) {
	if !recordsFailure {
		value, ok, err := resolve(ctx, values)
		if ok || err != nil || !predicateFallback {
			return value, ok, err
		}
	}
	return deferredExpressionResult(ctx, func(ctx context.Context) (interface{}, bool, error) {
		scope := rowValuesInExpressionContext(ctx, values)
		value, ok, err := resolve(ctx, scope)
		if ok || err != nil || !predicateFallback {
			return value, ok, err
		}
		return e.evaluateRowPredicateParts(ctx, view, scope), true, nil
	})
}

type rowPredicateConstNode bool

func (n rowPredicateConstNode) holds(*StorageExecutor, context.Context, map[string]interface{}) bool {
	return bool(n)
}

// rowPredicateStrictNode is a predicate evaluated whole by the row value
// evaluator first (rowPredicateIsStrictLogical); one it can't evaluate
// continues on the text path.
type rowPredicateStrictNode struct {
	text       string
	view       braceView
	value      rowValueNode
	arithmetic bool
}

func (n *rowPredicateStrictNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	value, evaluated, err := n.value.eval(e, values, nil)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	if evaluated {
		return predicateValueHolds(ctx, value, n.arithmetic)
	}
	return e.evaluateRowPredicatePartsFromSubqueries(ctx, n.view, values)
}

type rowPredicatePartsTextNode struct{ view braceView }

func (n *rowPredicatePartsTextNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	return e.evaluateRowPredicatePartsFromSubqueries(ctx, n.view, values)
}

type rowPredicateLeafTextNode struct{ view braceView }

func (n *rowPredicateLeafTextNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	return e.evaluateRowPredicateTextFromSubqueries(ctx, n.view, values)
}

type rowPredicateLabelNode struct {
	variable string
	labels   *labelExpression
}

func (n *rowPredicateLabelNode) holds(_ *StorageExecutor, _ context.Context, values map[string]interface{}) bool {
	return entityHasAllLabelsOrTypesPredicate(values[n.variable], n.labels)
}

type rowPredicatePlanNode struct {
	plan *rowPredicatePlan
}

func (n *rowPredicatePlanNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	return e.evaluateRowPredicatePlan(ctx, n.plan, values)
}

// rowPredicateLogicalNode is a top-level OR / XOR / AND / NOT, its operands
// evaluated as far as the result needs them (logicalPlan.evaluate).
type rowPredicateLogicalNode struct {
	plan       logicalPlan
	operands   []rowPredicateOperand
	arithmetic bool
}

func (n *rowPredicateLogicalNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	value, ok, err := n.plan.evaluate(func(index int) (interface{}, bool, error) {
		o := &n.operands[index]
		return e.evaluateRowLogicalPredicateOperand(ctx, o.view, values, o.recordsFailure, func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
			return o.evaluate(e, ctx, scope)
		}, true)
	})
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	return ok && predicateValueHolds(ctx, value, n.arithmetic)
}

type rowPredicateStringNode struct {
	left, right rowPredicateOperand
	match       func(string, string) bool
}

func (n *rowPredicateStringNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	leftValue, leftOK := n.left.resolve(e, ctx, values)
	rightValue, rightOK := n.right.resolve(e, ctx, values)
	return leftOK && rightOK && evaluateRowStringPredicateValues(leftValue, rightValue, n.match)
}

type rowPredicateMembershipNode struct {
	needle, haystack rowValueNode
	identity         bool
	negated          bool
}

func (n *rowPredicateMembershipNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	needle, leftOK, err := n.needle.eval(e, values, nil)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	haystack, rightOK, err := n.haystack.eval(e, values, nil)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	if !leftOK || !rightOK {
		return false
	}
	membership, known := rowMembershipOfValues(needle, haystack, n.identity, e.graphIdentityDomain())
	return known && membership == !n.negated
}

type rowPredicateNullNode struct {
	operand rowPredicateOperand
	isNull  bool
}

func (n *rowPredicateNullNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	value, _ := n.operand.resolve(e, ctx, values)
	return nullEvaluationHandler(!n.isNull).evaluate(value)
}

type rowPredicateComparisonNode struct {
	text              string
	scan              comparisonChainScan
	operands          []rowPredicateOperand
	normalizeIdentity bool
}

func (n *rowPredicateComparisonNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	result, resolved, err := evaluateScannedComparisonChain(n.text, &n.scan, n.normalizeIdentity, func(index int, _ string) (interface{}, bool, error) {
		o := &n.operands[index]
		return e.evaluateRowLogicalPredicateOperand(ctx, o.view, values, o.recordsFailure, func(ctx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
			return o.evaluate(e, ctx, scope)
		}, false)
	}, compareCypherPredicateValue, cypherVersionFromContext(ctx) == "25", e.graphIdentityDomain())
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	matched, known := result.(bool)
	return resolved && known && matched
}

type rowPredicateValueNode struct {
	operand    rowPredicateOperand
	arithmetic bool
}

func (n *rowPredicateValueNode) holds(e *StorageExecutor, ctx context.Context, values map[string]interface{}) bool {
	value, ok := n.operand.resolve(e, ctx, values)
	if !ok {
		return false
	}
	return predicateValueHolds(ctx, value, n.arithmetic)
}
