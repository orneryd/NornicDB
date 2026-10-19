package cypher

import (
	"strings"
	"sync/atomic"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// Row value programs (#1214).
//
// The row evaluator (evaluateRowValueText) reads an expression's form from
// its text on every evaluation: it tests the text for each form in turn
// (parentheses, literals, comprehensions, function calls, logical and
// comparison operators, arithmetic, property access) and splits it at the
// operator it finds. For an expression evaluated once per row, or once per
// list element inside a list predicate, that reading was most of the cost:
// a few microseconds for each operator, and a cost that grew with the
// expression's text.
//
// A rowValueProgram keeps those decisions. It is compiled from the text the
// first time the text is evaluated a second time (rowValueProgramFor), by
// running the same tests in the same order as evaluateRowValueText, and it
// evaluates each form through the same functions evaluateRowValueText calls:
// it is a cache of the row evaluator's parse, not a second evaluator. A form
// it doesn't compile is a rowTextNode, which evaluates its text with
// evaluateRowValueText; that function's own parts come back through
// evaluateRowValue, and so through their programs.
//
// What a form's text alone doesn't decide, the program decides at run time
// as evaluateRowValueText does: a name bound in the row (values[expr]) wins
// over the expression's form; a property chain whose variable isn't bound
// takes the text path.
//
// Inside a list predicate, list comprehension or other per-element
// evaluation, a part that doesn't read the element variable and reads
// nothing but the row (no function the program doesn't know, no subquery,
// no clock) has the same value for every element: the program evaluates it
// once per list (rowMemoNode), the first time it is needed, as the bound
// key of WITH r, <key> AS key WHERE any(k IN $keys WHERE k = key) is
// evaluated once.

// rowValueNode is one compiled form. eval returns what evaluateRowValueText
// returns for its text: the value, whether it is resolved, and the
// statement error. frames holds the memo slots of the enclosing list
// evaluations of this program.
type rowValueNode interface {
	eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error)
}

// rowValueMemo is a part's value, kept for one list evaluation.
type rowValueMemo struct {
	value interface{}
	ok    bool
	err   error
	done  bool
}

// rowValueProgramsDisabled turns programs off: evaluateRowValue then always
// reads the text. Tests use it to compare the two.
var rowValueProgramsDisabled atomic.Bool

// rowValueSeen marks a text evaluated once: it is compiled the next time.
// A text built per row (a value written into it) is never compiled, so it
// costs a cache entry, not a compilation.
var rowValueSeen = &rowValueProgram{}

// rowValuePrograms caches programs by expression text.
var rowValuePrograms = newBoundedCache[string, *rowValueProgram](8192, boundedCacheStringShardIndex)

// rowValueProgram is a compiled expression.
type rowValueProgram struct {
	root rowValueNode
}

// rowValueProgramFor returns expr's program, or nil when expr is to be
// read from the text this time.
func rowValueProgramFor(expr string) *rowValueProgram {
	if rowValueProgramsDisabled.Load() {
		return nil
	}
	program, cached := rowValuePrograms.get(expr)
	if !cached {
		rowValuePrograms.put(expr, rowValueSeen)
		return nil
	}
	if program != rowValueSeen {
		return program
	}
	compiler := rowValueCompiler{}
	root, _ := compiler.compile(expr)
	program = &rowValueProgram{root: root}
	rowValuePrograms.put(expr, program)
	return program
}

// evaluateRowValue evaluates expr against a row: the value, whether it is
// resolved (an expression this evaluator can't evaluate is not), and the
// statement error.
func (e *StorageExecutor) evaluateRowValue(expr string, values map[string]interface{}) (interface{}, bool, error) {
	if program := rowValueProgramFor(expr); program != nil {
		return program.root.eval(e, values, nil)
	}
	return e.evaluateRowValueText(expr, values)
}

// rowValueInfo describes a compiled part for the per-element memo: pure
// when its value depends only on the row (every part compiled, nothing that
// reads the clock, the graph through a subquery, or randomness), trivial
// when it is cheaper to evaluate than to memo, and the row names it reads.
type rowValueInfo struct {
	pure    bool
	trivial bool
	refs    []string
}

func impureRowValue() rowValueInfo { return rowValueInfo{} }

// combineRowValueInfo is the info of a part made of parts.
func combineRowValueInfo(parts ...rowValueInfo) rowValueInfo {
	info := rowValueInfo{pure: true}
	for _, part := range parts {
		if !part.pure {
			return impureRowValue()
		}
		for _, name := range part.refs {
			info.refs = appendUniqueString(info.refs, name)
		}
	}
	return info
}

func appendUniqueString(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}

func (info rowValueInfo) reads(name string) bool {
	for _, ref := range info.refs {
		if ref == name {
			return true
		}
	}
	return false
}

func (info rowValueInfo) readsAny(names []string) bool {
	for _, name := range names {
		if info.reads(name) {
			return true
		}
	}
	return false
}

// rowValueLoop is a list evaluation being compiled: the names it binds for
// each element (the element variable; reduce's accumulator too) and the
// memo slots its body uses.
type rowValueLoop struct {
	variables []string
	slots     int
}

type rowValueCompiler struct {
	loops []*rowValueLoop
}

// hoist returns part memoized for the innermost list evaluation when its
// value is the same for every element: pure, not trivial, and not reading
// the element variable.
func (c *rowValueCompiler) hoist(node rowValueNode, info rowValueInfo) rowValueNode {
	if len(c.loops) == 0 || !info.pure || info.trivial {
		return node
	}
	loop := c.loops[len(c.loops)-1]
	if info.readsAny(loop.variables) {
		return node
	}
	slot := loop.slots
	loop.slots++
	return &rowMemoNode{depth: len(c.loops) - 1, slot: slot, inner: node}
}

// compose compiles the parts of a composite form: each part is memoized
// when the whole isn't the same for every element but the part is (hoist).
func (c *rowValueCompiler) compose(whole rowValueInfo, parts []rowValueNode, infos []rowValueInfo) {
	if len(c.loops) > 0 && whole.pure && !whole.readsAny(c.loops[len(c.loops)-1].variables) {
		return
	}
	for i := range parts {
		parts[i] = c.hoist(parts[i], infos[i])
	}
}

// part compiles one part of a composite form.
func (c *rowValueCompiler) part(expr string) (rowValueNode, rowValueInfo) {
	return c.compile(expr)
}

func rowTextValue(expr string) (rowValueNode, rowValueInfo) {
	return &rowTextNode{text: expr}, impureRowValue()
}

// bound wraps a form evaluateRowValueText reaches only when expr isn't a
// name bound in the row: that binding wins (values[expr]).
func bound(expr string, node rowValueNode, info rowValueInfo) (rowValueNode, rowValueInfo) {
	if isLiteralKeyword(expr) {
		return node, info
	}
	return &rowBoundNode{text: expr, inner: node}, info
}

// compile compiles expr, running evaluateRowValueText's tests in its order.
func (c *rowValueCompiler) compile(expr string) (rowValueNode, rowValueInfo) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return rowUnresolvedNode{}, rowValueInfo{pure: true, trivial: true}
	}
	if inner, ok := stripEnclosingExpressionParentheses(expr); ok {
		return c.compile(inner)
	}
	if caseEnd := leadingCaseExpressionEnd(expr); caseEnd > 0 && strings.TrimSpace(expr[caseEnd:]) != "" && !hasTopLevelLogicalOperator(expr) {
		return rowTextValue(expr)
	}
	if isRelationshipPatternOperand(expr) {
		return rowTextValue(expr)
	}
	if variable, chain, ok := rowPropertyChainShape(expr); ok {
		return &rowPropertyChainNode{text: expr, literal: isLiteralKeyword(expr), variable: variable, chain: chain},
			rowValueInfo{pure: true, trivial: true, refs: []string{variable}}
	}
	if normalizeProjectionColumnName(expr) != expr {
		return rowTextValue(expr)
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		switch value.(type) {
		case nil, bool, int64, float64, string:
			return bound(expr, rowConstNode{value: value}, rowValueInfo{pure: true, trivial: true})
		}
		return bound(expr, &rowLiteralNode{text: expr}, rowValueInfo{pure: true, trivial: true})
	}
	if _, _, subquery := rowSubqueryAtom(expr); subquery {
		return rowTextValue(expr)
	}
	if _, _, comprehension := rowPatternComprehensionAtom(expr); comprehension {
		return rowTextValue(expr)
	}
	if _, _, labelTest := parseWithWhereLabelTest(expr); labelTest {
		return rowTextValue(expr)
	}
	if isCaseExpression(expr) {
		return bound(expr, c.compileCase(expr), impureRowValue())
	}
	if _, _, projection := rowMapProjectionSplit(expr); projection {
		return rowTextValue(expr)
	}
	if _, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		return rowTextValue(expr)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if variable, listExpression, predicate, projection, comprehension := parseListComprehension(inner); comprehension {
			return c.compileComprehension(expr, variable, listExpression, predicate, projection)
		}
		if inner == "" {
			return bound(expr, rowEmptyListNode{}, rowValueInfo{pure: true, trivial: true})
		}
		items := splitTopLevelComma(inner)
		parts := make([]rowValueNode, len(items))
		infos := make([]rowValueInfo, len(items))
		for i, item := range items {
			parts[i], infos[i] = c.part(item)
		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowListNode{items: parts}, info)
	}
	if function, inner, isFunction := parseFunctionCallWS(expr); isFunction {
		name := lowerASCII(function)
		if isQuantifierFunction(name) {
			variable, listExpression, predicate, parsed := parseQuantifierArguments(inner)
			if !parsed {
				return rowTextValue(expr)
			}
			return c.compileQuantifier(expr, name, variable, listExpression, predicate)
		}
		return c.compileFunction(expr, name, inner)
	}
	var buffer [8]string
	if plan := planLogicalExpression(expr, buffer[:0]); plan.kind != logicalNone {
		plan.operands = append([]string(nil), plan.operands...)
		parts := make([]rowValueNode, len(plan.operands))
		infos := make([]rowValueInfo, len(plan.operands))
		for i, operand := range plan.operands {
			parts[i], infos[i] = c.part(operand)
			if plan.kind == logicalOr && rowOperandRecordsFailure(operand) {
				parts[i] = &rowDeferredOperandNode{operand: parts[i]}
			}

		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowLogicalNode{plan: plan, operands: parts}, info)
	}
	if _, _, regex := splitByOperatorOutsideCase(expr, "=~", false, true); regex {
		return rowTextValue(expr)
	}
	if scan, comparison := scanComparisonChain(expr); comparison {
		parts := make([]rowValueNode, scan.count+1)
		infos := make([]rowValueInfo, scan.count+1)
		for i := 0; i <= scan.count; i++ {
			operand := scan.operand(expr, i)
			parts[i], infos[i] = c.part(operand)

		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowComparisonNode{text: expr, scan: scan, operands: parts, normalizeIdentity: comparisonChainHasIdentity(expr, &scan)}, info)
	}
	if _, _, _, isTypePredicate, _ := splitTypePredicate(expr); isTypePredicate {
		return rowTextValue(expr)
	}
	if _, _, _, isNormalization := splitNormalizationPredicate(expr); isNormalization {
		return rowTextValue(expr)
	}
	for _, predicate := range []struct {
		suffix  string
		notNull bool
	}{{suffix: " IS NOT NULL", notNull: true}, {suffix: " IS NULL"}} {
		if hasSuffixFoldASCII(expr, lowerASCII(predicate.suffix)) {
			operand, info := c.part(strings.TrimSpace(expr[:len(expr)-len(predicate.suffix)]))
			parts, infos := []rowValueNode{operand}, []rowValueInfo{info}
			c.compose(info, parts, infos)
			return bound(expr, &rowNullTestNode{operand: parts[0], notNull: predicate.notNull}, info)
		}
	}
	for _, membership := range []struct {
		operator string
		negated  bool
	}{{operator: " NOT IN ", negated: true}, {operator: " IN "}} {
		if left, right, ok := splitByOperatorOutsideCase(expr, membership.operator, true, true); ok {
			leftNode, leftInfo := c.part(left)
			rightNode, rightInfo := c.part(right)
			parts, infos := []rowValueNode{leftNode, rightNode}, []rowValueInfo{leftInfo, rightInfo}
			info := combineRowValueInfo(infos...)
			c.compose(info, parts, infos)
			return bound(expr, &rowMembershipNode{needle: parts[0], haystack: parts[1], identity: isRowIdentityExpression(left), negated: membership.negated}, info)
		}
	}
	for _, predicate := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{operator: " STARTS WITH ", match: strings.HasPrefix},
		{operator: " ENDS WITH ", match: strings.HasSuffix},
		{operator: " CONTAINS ", match: strings.Contains},
	} {
		if left, right, ok := splitByOperatorOutsideCase(expr, predicate.operator, true, true); ok {
			leftNode, leftInfo := c.part(left)
			rightNode, rightInfo := c.part(right)
			parts, infos := []rowValueNode{leftNode, rightNode}, []rowValueInfo{leftInfo, rightInfo}
			info := combineRowValueInfo(infos...)
			c.compose(info, parts, infos)
			return bound(expr, &rowStringPredicateNode{left: parts[0], right: parts[1], match: predicate.match}, info)
		}
	}
	if rowSubscriptStart(expr) > 0 {
		return rowTextValue(expr)
	}
	for _, tier := range []string{"+-", "*/%"} {
		if left, right, operator, ok := splitRowArithmeticTier(expr, tier); ok {
			leftNode, leftInfo := c.part(left)
			rightNode, rightInfo := c.part(right)
			parts, infos := []rowValueNode{leftNode, rightNode}, []rowValueInfo{leftInfo, rightInfo}
			info := combineRowValueInfo(infos...)
			c.compose(info, parts, infos)
			return bound(expr, &rowArithmeticNode{left: parts[0], right: parts[1], leftText: left, rightText: right, operator: operator}, info)
		}
	}
	if _, _, _, ok := splitRowArithmeticTier(expr, "^"); ok {
		return rowTextValue(expr)
	}
	if len(expr) > 1 && (expr[0] == '-' || expr[0] == '+') {
		return rowTextValue(expr)
	}
	if dot := topLevelSymbolIndex(expr, "."); dot > 0 && !isQualifiedFunctionCall(expr) {
		for {
			next := topLevelSymbolIndex(expr[dot+1:], ".")
			if next < 0 {
				break
			}
			dot += next + 1
		}
		base, info := c.part(strings.TrimSpace(expr[:dot]))
		parts, infos := []rowValueNode{base}, []rowValueInfo{info}
		c.compose(info, parts, infos)
		return bound(expr, &rowPropertyNode{text: expr, base: parts[0], chain: strings.TrimSpace(expr[dot+1:])}, info)
	}
	if isValidIdentifier(expr) || len(expr) > 1 && expr[0] == '$' && isValidIdentifier(expr[1:]) {
		// A row variable or parameter: bound in the row, or read by the
		// fallback as evaluateRowValueText reads it.
		return bound(expr, &rowFallbackNode{text: expr}, rowValueInfo{pure: true, trivial: true, refs: []string{expr}})
	}
	return bound(expr, &rowFallbackNode{text: expr}, impureRowValue())
}

// compileFunction compiles a function call evaluateRowValueText evaluates
// itself; any other is read from the text. None of these names is a
// temporal, math, extension, plugin (those have a dot) or GDS function,
// which evaluateRowValueText tries first.
func (c *rowValueCompiler) compileFunction(expr, name, argument string) (rowValueNode, rowValueInfo) {
	switch name {
	case "startnode", "endnode":
		variable := strings.TrimSpace(argument)
		if !isValidIdentifier(variable) {
			return rowTextValue(expr)
		}
		return bound(expr, &rowEndpointNode{text: expr, function: name, variable: variable}, rowValueInfo{pure: true, refs: []string{variable}})
	case "size", "head", "last", "reverse", "type":
		operand, operandInfo := c.part(argument)
		info := combineRowValueInfo(operandInfo)
		parts, infos := []rowValueNode{operand}, []rowValueInfo{operandInfo}
		c.compose(info, parts, infos)
		return bound(expr, &rowUnaryFunctionNode{function: name, operand: parts[0]}, info)
	case "coalesce":
		operands := splitTopLevelComma(argument)
		parts := make([]rowValueNode, len(operands))
		infos := make([]rowValueInfo, len(operands))
		for i, operand := range operands {
			if _, whole := wholeExistsPredicate(strings.TrimSpace(operand)); whole {
				return rowTextValue(expr)
			}
			parts[i], infos[i] = c.part(operand)

		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowCoalesceNode{operands: parts}, info)
	case "reduce", "allreduce":
		form, ok := parseReduceForm(name, argument)
		if !ok {
			return rowTextValue(expr)
		}
		return c.compileReduce(expr, form)
	}
	return rowTextValue(expr)
}

// compileLoopBody compiles a per-element part of a list evaluation over
// variable.
func (c *rowValueCompiler) compileLoopBody(loop *rowValueLoop, expr string) rowValueNode {
	if expr == "" {
		return nil
	}
	c.loops = append(c.loops, loop)
	node, info := c.compile(expr)
	node = c.hoist(node, info)
	c.loops = c.loops[:len(c.loops)-1]
	return node
}

func (c *rowValueCompiler) compileQuantifier(expr, function, variable, listExpression, predicate string) (rowValueNode, rowValueInfo) {
	list, listInfo := c.part(listExpression)
	parts, infos := []rowValueNode{list}, []rowValueInfo{listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{variable}}
	body := c.compileLoopBody(loop, predicate)
	return bound(expr, &rowQuantifierNode{function: function, variable: variable, list: parts[0], predicate: body, loop: loop}, impureRowValue())
}

func (c *rowValueCompiler) compileComprehension(expr, variable, listExpression, predicate, projection string) (rowValueNode, rowValueInfo) {
	if predicate != "" && containsRelationshipPattern(predicate) {
		return rowTextValue(expr)
	}
	list, listInfo := c.part(listExpression)
	parts, infos := []rowValueNode{list}, []rowValueInfo{listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{variable}}
	node := &rowComprehensionNode{variable: variable, list: parts[0], loop: loop}
	node.predicate = c.compileLoopBody(loop, predicate)
	node.projection = c.compileLoopBody(loop, projection)
	return bound(expr, node, impureRowValue())
}

// compileReduce compiles reduce / allReduce as evaluateRowReduce evaluates
// it: a list evaluation whose step and predicate read the accumulator and
// the variable.
func (c *rowValueCompiler) compileReduce(expr string, form reduceForm) (rowValueNode, rowValueInfo) {
	initial, initialInfo := c.part(form.initial)
	list, listInfo := c.part(form.list)
	parts, infos := []rowValueNode{initial, list}, []rowValueInfo{initialInfo, listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{form.accumulator, form.variable}}
	node := &rowReduceNode{form: form, initial: parts[0], list: parts[1], loop: loop}
	node.step = c.compileLoopBody(loop, form.step)
	if form.all {
		if node.predicate = c.compileLoopBody(loop, form.predicate); node.predicate == nil {
			node.predicate = rowUnresolvedNode{}
		}
	}
	return bound(expr, node, impureRowValue())
}

// rowReduceNode is reduce / allReduce(accumulator = initial, variable IN
// list | step[, predicate]), folded by runReduceForm.
type rowReduceNode struct {
	form            reduceForm
	initial, list   rowValueNode
	step, predicate rowValueNode
	loop            *rowValueLoop
}

func (n *rowReduceNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	accumulator, resolved, err := n.initial.eval(e, values, frames)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := n.list.eval(e, values, frames)
	if err != nil || !resolved {
		return nil, false, err
	}
	if listValue == nil {
		return nil, true, nil
	}
	scope, inner := rowLoopScope(values, frames, n.loop)
	return rowReduceLoop(n.form, accumulator, listValue, scope,
		func() (interface{}, bool, error) { return n.step.eval(e, scope, inner) },
		func() (interface{}, bool, error) { return n.predicate.eval(e, scope, inner) })
}

// compileCase compiles a CASE expression as evaluateRowCaseExpression
// evaluates it.
func (c *rowValueCompiler) compileCase(expr string) *rowCaseNode {
	parsed, err := parseCaseExpression(expr)
	if err != nil {
		return &rowCaseNode{}
	}
	node := &rowCaseNode{parsed: parsed}
	if parsed.isSimple {
		node.test, _ = c.compile(parsed.testExpression)
	}
	node.whens = make([]rowValueNode, len(parsed.whenClauses))
	node.results = make([]rowValueNode, len(parsed.whenClauses))
	for i, clause := range parsed.whenClauses {
		if parsed.isSimple {
			node.whens[i], _ = c.compile(clause.value)
		} else {
			node.whens[i], _ = c.compile(clause.condition)
		}
		node.results[i], _ = c.compile(clause.result)
	}
	if parsed.elseResult != "" {
		node.elseResult, _ = c.compile(parsed.elseResult)
	}
	return node
}

// rowCaseNode is a CASE expression, evaluated as evaluateRowCaseExpression
// evaluates it; parsed is nil for text that isn't one (unresolved).
type rowCaseNode struct {
	parsed     *caseExpression
	test       rowValueNode
	whens      []rowValueNode
	results    []rowValueNode
	elseResult rowValueNode
}

func (n *rowCaseNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return runCaseExpression(n.parsed, rowIsCypher25(values), func(part caseValuePart, i int, text string) (interface{}, bool, error) {
		var node rowValueNode
		switch part {
		case caseTest:
			node = n.test
		case caseWhen:
			node = n.whens[i]
		case caseResult:
			node = n.results[i]
		case caseElse:
			node = n.elseResult
		}
		value, ok, err := node.eval(e, values, frames)
		if part == caseWhen && !n.parsed.isSimple && err == nil && !ok {
			ctx := rowStatementContextOrBackground(values)
			return e.evaluateRowPredicate(ctx, text, values), true, getExpressionFailure(ctx)
		}
		return value, ok, err
	})
}

// rowTextNode evaluates its text with evaluateRowValueText.
type rowTextNode struct{ text string }

func (n *rowTextNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return e.evaluateRowValueText(n.text, values)
}

// rowBoundNode is values[text] when the row binds the text, else inner.
type rowBoundNode struct {
	text  string
	inner rowValueNode
}

func (n *rowBoundNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	if value, ok := values[n.text]; ok {
		return value, true, nil
	}
	return n.inner.eval(e, values, frames)
}

type rowUnresolvedNode struct{}

func (rowUnresolvedNode) eval(*StorageExecutor, map[string]interface{}, [][]rowValueMemo) (interface{}, bool, error) {
	return nil, false, nil
}

type rowConstNode struct{ value interface{} }

func (n rowConstNode) eval(*StorageExecutor, map[string]interface{}, [][]rowValueMemo) (interface{}, bool, error) {
	return n.value, true, nil
}

// rowLiteralNode is a list or map literal, parsed per evaluation: the value
// is the caller's to change.
type rowLiteralNode struct{ text string }

func (n *rowLiteralNode) eval(*StorageExecutor, map[string]interface{}, [][]rowValueMemo) (interface{}, bool, error) {
	value, _ := parseLiteralValueFromComputedRow(n.text)
	return value, true, nil
}

type rowEmptyListNode struct{}

func (rowEmptyListNode) eval(*StorageExecutor, map[string]interface{}, [][]rowValueMemo) (interface{}, bool, error) {
	return []interface{}{}, true, nil
}

// rowPropertyChainNode is a property chain on a row variable (n.a.b); a
// variable the row doesn't bind takes the text path.
type rowPropertyChainNode struct {
	text, variable, chain string
	literal               bool
}

func (n *rowPropertyChainNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	if value, ok := values[n.text]; ok && !n.literal {
		return value, true, nil
	}
	base, bound := values[n.variable]
	if !bound {
		return e.evaluateRowValueText(n.text, values)
	}
	value, ok := evaluateRowPropertyChain(e.queryVisibleValue(base), n.chain)
	if !ok {
		return nil, false, rowPropertyChainTypeError(base, n.chain)
	}
	return value, true, nil
}

// rowFallbackNode is an expression no form of the row evaluator matches:
// the context evaluator's value (evaluateRowFallback).
type rowFallbackNode struct{ text string }

func (n *rowFallbackNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return e.rowValueFallback(n.text, values)
}

// rowValueFallback is evaluateRowValueText's last step for expr.
func (e *StorageExecutor) rowValueFallback(expr string, values map[string]interface{}) (interface{}, bool, error) {
	// The context evaluator sends expressions containing subqueries back to
	// this evaluator. Resolve a containing function here instead of returning
	// the unchanged expression to that dispatcher indefinitely.
	if mayContainSubqueryExpression(expr) {
		if function, argument, call := parseFunctionCallWS(expr); call {
			return e.evaluateRowGraphFunction(function, argument, values)
		}
	}
	value, err := e.evaluateRowFallback(expr, values)
	if err != nil {
		return nil, false, err
	}
	if text, ok := value.(string); ok && text == expr && !isWholeCypherQuotedString(expr) {
		return nil, false, nil
	}
	return value, true, nil
}

type rowListNode struct{ items []rowValueNode }

func (n *rowListNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return rowListLiteral(len(n.items), func(i int) (interface{}, bool, error) {
		return n.items[i].eval(e, values, frames)
	})
}

type rowLogicalNode struct {
	plan     logicalPlan
	operands []rowValueNode
}

func (n *rowLogicalNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, ok, err := n.plan.evaluate(func(index int) (interface{}, bool, error) {
		return n.operands[index].eval(e, values, frames)
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return value, true, nil
}

type rowDeferredOperandNode struct{ operand rowValueNode }

func (n *rowDeferredOperandNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return deferredRowOperand(values, func(scope map[string]interface{}) (interface{}, bool, error) {
		return n.operand.eval(e, scope, frames)
	})
}

type rowComparisonNode struct {
	text              string
	scan              comparisonChainScan
	operands          []rowValueNode
	normalizeIdentity bool
}

func (n *rowComparisonNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return evaluateScannedComparisonChain(n.text, &n.scan, n.normalizeIdentity, func(index int, _ string) (interface{}, bool, error) {
		return n.operands[index].eval(e, values, frames)
	}, compareCypherPredicateValue, rowIsCypher25(values), e.graphIdentityDomain())
}

type rowNullTestNode struct {
	operand rowValueNode
	notNull bool
}

func (n *rowNullTestNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, ok, err := n.operand.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return nullEvaluationHandler(n.notNull).evaluate(value), true, nil
}

type rowMembershipNode struct {
	needle, haystack rowValueNode
	identity         bool
	negated          bool
}

func (n *rowMembershipNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	needle, leftOK, err := n.needle.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	haystack, rightOK, err := n.haystack.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	value, ok := rowMembershipOfValues(needle, haystack, n.identity, e.graphIdentityDomain())
	if !n.negated || !ok || value == nil {
		return value, ok, nil
	}
	return !value.(bool), true, nil
}

type rowStringPredicateNode struct {
	left, right rowValueNode
	match       func(string, string) bool
}

func (n *rowStringPredicateNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	leftValue, leftOK, err := n.left.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	rightValue, rightOK, err := n.right.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	return rowStringPredicateValue(leftValue, rightValue, n.match), true, nil
}

type rowArithmeticNode struct {
	left, right         rowValueNode
	leftText, rightText string
	operator            byte
}

func (n *rowArithmeticNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	leftValue, leftOK, err := n.left.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	rightValue, rightOK, err := n.right.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	if n.operator == '/' {
		if value, folded := foldedDivisionByZero(n.leftText, n.rightText, leftValue, rightValue); folded {
			return value, true, nil
		}
	}
	return e.evaluateRowArithmeticValues(n.operator, leftValue, rightValue)
}

// rowPropertyNode is base.chain for a base that isn't a row variable
// (startNode(r).id, m.a[0].b); an unresolved base is the fallback's.
type rowPropertyNode struct {
	text  string
	base  rowValueNode
	chain string
}

func (n *rowPropertyNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	base, ok, err := n.base.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return e.rowValueFallback(n.text, values)
	}
	value, ok := evaluateRowPropertyChain(e.queryVisibleValue(base), n.chain)
	if !ok {
		if err := rowPropertyChainTypeError(base, n.chain); err != nil {
			return nil, false, err
		}
	}
	return value, ok, nil
}

// rowEndpointNode is startNode(variable) / endNode(variable).
type rowEndpointNode struct {
	text, function, variable string
}

func (n *rowEndpointNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	if value, handled, err := e.rowRelationshipEndpoint(n.function, n.variable, values); handled || err != nil {
		return value, handled, err
	}
	return e.evaluateRowValueText(n.text, values)
}

// rowUnaryFunctionNode is size / head / last / reverse / type of one
// argument, as evaluateRowValueText evaluates them.
type rowUnaryFunctionNode struct {
	function string
	operand  rowValueNode
}

func (n *rowUnaryFunctionNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, resolved, err := n.operand.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !resolved {
		return nil, false, nil
	}
	switch n.function {
	case "size":
		return evaluateCypherSize(value)
	case "type":
		return rowTypeOf(value)
	}
	result, err := evaluateListAccessFunction(n.function, value)
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}

type rowCoalesceNode struct{ operands []rowValueNode }

func (n *rowCoalesceNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return cypherfn.Coalesce(len(n.operands), func(index int) (interface{}, bool, error) {
		return n.operands[index].eval(e, values, frames)
	})
}

// rowMemoNode is a part with one value for every element of the list
// evaluation at depth: evaluated once, when first needed.
type rowMemoNode struct {
	depth, slot int
	inner       rowValueNode
}

func (n *rowMemoNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	memo := &frames[n.depth][n.slot]
	if !memo.done {
		memo.value, memo.ok, memo.err = n.inner.eval(e, values, frames[:n.depth])
		memo.done = true
	}
	return memo.value, memo.ok, memo.err
}

// rowLoopScope is the row a list evaluation's element parts see: the row
// with the element variable bound, one map for every element (as reduce
// keeps one), and the list's memo frame.
func rowLoopScope(values map[string]interface{}, frames [][]rowValueMemo, loop *rowValueLoop) (map[string]interface{}, [][]rowValueMemo) {
	scope := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		scope[name] = value
	}
	if loop.slots > 0 {
		frames = append(frames[:len(frames):len(frames)], make([]rowValueMemo, loop.slots))
	} else {
		frames = append(frames[:len(frames):len(frames)], nil)
	}
	return scope, frames
}

// rowQuantifierNode is all / any / none / single(variable IN list WHERE
// predicate), folded as evaluateRowQuantifier folds it.
type rowQuantifierNode struct {
	function, variable string
	list, predicate    rowValueNode
	loop               *rowValueLoop
}

func (n *rowQuantifierNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	listValue, ok, err := n.list.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if listValue == nil || nilRemotePath(listValue) {
		return nil, true, nil
	}
	if cypherValueKindOf(listValue) == valueKindPath {
		return nil, false, runtimeTypeError("Type mismatch: expected List<T> but was Path")
	}
	scope, inner := rowLoopScope(values, frames, n.loop)
	return rowQuantifierLoop(n.function, coerceToUnwindItems(listValue), func(item interface{}) { scope[n.variable] = item }, func() (interface{}, bool, error) {
		return n.predicate.eval(e, scope, inner)
	})
}

// rowComprehensionNode is [variable IN list WHERE predicate | projection],
// as evaluateRowListComprehension evaluates it.
type rowComprehensionNode struct {
	variable              string
	list                  rowValueNode
	predicate, projection rowValueNode
	loop                  *rowValueLoop
}

func (n *rowComprehensionNode) eval(e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	listValue, ok, err := n.list.eval(e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if listValue == nil {
		return nil, true, nil
	}
	if n.predicate == nil && n.projection == nil {
		return listValue, true, nil
	}
	scope, inner := rowLoopScope(values, frames, n.loop)
	var holds func() (bool, bool, error)
	if n.predicate != nil {
		holds = func() (bool, bool, error) { return rowConditionHolds(n.predicate.eval(e, scope, inner)) }
	}
	var project func() (interface{}, bool, error)
	if n.projection != nil {
		project = func() (interface{}, bool, error) { return n.projection.eval(e, scope, inner) }
	}
	return rowComprehensionLoop(coerceToUnwindItems(listValue), func(item interface{}) { scope[n.variable] = item }, holds, project)
}
