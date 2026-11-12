package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error)
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
// read from the text this time. values says whether the row carries a bound
// expression origin: a statement profiling its rows then reads expr from its
// own collector's source-bearing program, which reports where every part
// starts and is discarded with the statement. A normal program is compiled
// exactly as before and holds no positions at all.
func rowValueProgramFor(ctx context.Context, expr string, values map[string]interface{}) *rowValueProgram {
	if ctx.Err() != nil {
		return nil
	}
	if rowValueProgramsDisabled.Load() {
		return nil
	}
	if rowExpressionOrigin(values).had {
		if collector := profileRuntimeFromContext(ctx); collector != nil {
			return collector.programs.program(ctx, expr, compileProfileRowValue)
		}
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
	root, _ := compiler.compile(ctx, expressionPart(expr))
	if ctx.Err() != nil {
		return nil
	}
	program = &rowValueProgram{root: root}
	rowValuePrograms.put(expr, program)
	return program
}

// evaluateRowValue evaluates expr against a row: the value, whether it is
// resolved (an expression this evaluator can't evaluate is not), and the
// statement error.
func (e *StorageExecutor) evaluateRowValue(ctx context.Context, expr string, values map[string]interface{}) (interface{}, bool, error) {
	program := rowValueProgramFor(ctx, expr, values)
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if program != nil {
		return program.root.eval(ctx, e, values, nil)
	}
	return runtimeRowResult(e.evaluateRowValueText(ctx, expr, values))
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
	// origins compiles a PROFILE statement's source-bearing program: every
	// position the canonical readers do report is kept, so a scalar subquery
	// reports the source occurrence it was written at. A form whose parts'
	// positions the canonical readers don't report is one text node read at
	// that origin instead, never a guessed position. A normal program is
	// compiled with origins false: it carries no positions and no wrapper.
	origins bool
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

// textValue compiles a part the canonical evaluator reads from its text: the
// text node, wrapped with where the part starts when this program reports
// source occurrences. A normal program's text node is the text alone.
func (c *rowValueCompiler) textValue(part expressionFragment) (rowValueNode, rowValueInfo) {
	node, info := rowTextValue(part.text)
	return c.sourceValue(part.start, node), info
}

func rowTextValue(expr string) (rowValueNode, rowValueInfo) {
	return &rowTextNode{text: expr}, impureRowValue()
}

// rowValuePartList is one top-level comma split of an expression part: the
// texts an ordinary reader evaluates, or the text-and-start spans a
// source-bearing (PROFILE) reader reports. A reader fills only the container
// its own evaluation needs, from the one comma iterator
// (topLevelCommaIterator), and reads its parts through count and at, so the
// text reader and the span reader share one loop rather than splitting the
// part twice.
type rowValuePartList struct {
	texts []string
	spans []expressionFragment
}

// count returns how many parts the split found.
func (parts rowValuePartList) count() int {
	return len(parts.texts) + len(parts.spans)
}

// at returns part index's text and where that text starts in the split input.
// A text split reports no positions, so its parts start at zero: the only
// position a reader that reports none moves an origin by.
func (parts rowValuePartList) at(index int) (string, int) {
	if len(parts.spans) > 0 {
		span := parts.spans[index]
		return span.text, span.start
	}
	return parts.texts[index], 0
}

// sourceValue attaches where a form starts in this program's text when the
// program reports source occurrences: the wrapper carries that position, so
// an ordinary form keeps its own layout and an ordinary program carries
// neither a position nor a wrapper.
func (c *rowValueCompiler) sourceValue(start int, node rowValueNode) rowValueNode {
	if !c.origins {
		return node
	}
	return &rowSourceValueNode{start: start, inner: node}
}

func bound(expr string, node rowValueNode, info rowValueInfo) (rowValueNode, rowValueInfo) {
	if isLiteralKeyword(expr) {
		return node, info
	}
	return &rowBoundNode{text: expr, inner: node}, info
}

// compile compiles a part, running evaluateRowValueText's tests in its order.
func (c *rowValueCompiler) compile(ctx context.Context, part expressionFragment) (rowValueNode, rowValueInfo) {
	node, info := c.compileValue(ctx, part)
	return normalizeRowValueNode(node), info
}

// normalizeRowValueNode decides once, at compilation, which results need the
// runtime boundary. Scalar/constructed results and normalized child results do
// not need another check. Dynamic producers (including future forms) do. A row
// binding can override any form, so rowBoundNode checks that read separately.
func normalizeRowValueNode(node rowValueNode) rowValueNode {
	switch n := node.(type) {
	case *rowBoundNode:
		n.inner = normalizeRowValueNode(n.inner)
		return n
	case *rowRuntimeValueNode, rowUnresolvedNode, rowConstNode, *rowLiteralNode, rowEmptyListNode,
		*rowListNode, *rowLogicalNode, *rowComparisonNode, *rowNullTestNode,
		*rowMembershipNode, *rowStringPredicateNode, *rowArithmeticNode,
		*rowCaseNode, *rowCoalesceNode, *rowReduceNode, *rowComprehensionNode,
		*rowMemoNode, *rowDeferredOperandNode:
		return node
	default:
		return &rowRuntimeValueNode{inner: node}
	}
}

type rowRuntimeValueNode struct{ inner rowValueNode }

// rowValueShape exposes the compiled form to structural planners. Runtime
// result checks apply during evaluation and do not change that form.
func rowValueShape(node rowValueNode) rowValueNode {
	for {
		runtime, wrapped := node.(*rowRuntimeValueNode)
		if !wrapped {
			return node
		}
		node = runtime.inner
	}
}

func (n *rowRuntimeValueNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return runtimeRowResult(n.inner.eval(ctx, e, values, frames))
}

func (c *rowValueCompiler) compileValue(ctx context.Context, part expressionFragment) (rowValueNode, rowValueInfo) {
	expr := strings.TrimSpace(part.text)
	// The part the tests below read is the trimmed text, so where it starts is
	// where the given part starts plus what was trimmed from it.
	part = expressionFragment{text: expr, start: part.start + leadingExpressionSpace(part.text)}
	if expr == "" {
		return rowUnresolvedNode{}, rowValueInfo{pure: true, trivial: true}
	}
	if inner, ok := stripEnclosingExpressionParentheses(expr); ok {
		return c.compile(ctx, expressionFragment{text: inner, start: part.start + len(expr) - 1 - len(inner)})
	}
	if caseEnd := leadingCaseExpressionEnd(expr); caseEnd > 0 && strings.TrimSpace(expr[caseEnd:]) != "" && !hasTopLevelLogicalOperator(expr) {
		return c.textValue(part)
	}
	if isRelationshipPatternOperand(expr) {
		return c.textValue(part)
	}
	if variable, chain, ok := rowPropertyChainShape(expr); ok {
		return &rowPropertyChainNode{text: expr, literal: isLiteralKeyword(expr), variable: variable, chain: chain},
			rowValueInfo{pure: true, trivial: true, refs: []string{variable}}
	}
	if normalizeProjectionColumnName(expr) != expr {
		return c.textValue(part)
	}
	if value, ok := parseLiteralValueFromComputedRow(expr); ok {
		switch value.(type) {
		case nil, bool, int64, float64, string:
			return bound(expr, rowConstNode{value: value}, rowValueInfo{pure: true, trivial: true})
		}
		return bound(expr, &rowLiteralNode{text: expr}, rowValueInfo{pure: true, trivial: true})
	}
	if kind, open, subquery := rowSubqueryAtom(expr); subquery {
		body := strings.TrimSpace(expr[open+1 : len(expr)-1])
		return bound(expr, c.sourceValue(part.start, &rowSubqueryNode{text: expr, kind: kind, body: body}), impureRowValue())
	}
	if _, _, comprehension := rowPatternComprehensionAtom(expr); comprehension {
		return c.textValue(part)
	}
	if _, _, labelTest := parseWithWhereLabelTest(expr); labelTest {
		return c.textValue(part)
	}
	if isCaseExpression(expr) {
		if c.origins {
			return c.textValue(part)
		}
		return bound(expr, c.compileCase(ctx, expr), impureRowValue())
	}
	if _, _, projection := mapProjectionSplit(expr, mapProjectionRuntime, nil); projection {
		return c.textValue(part)
	}
	if _, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		return c.textValue(part)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		// The inside starts one delimiter in, and its items are parts of the
		// inside: each starts there plus where it starts in it.
		inside := expressionFragment{text: inner, start: part.start + len(expr) - 1 - len(inner)}
		if variable, listExpression, predicate, projection, comprehension := parseListComprehension(inner, nil); comprehension {
			if c.origins {
				return c.textValue(part)
			}
			return c.compileComprehension(ctx, expr, variable, listExpression, predicate, projection)
		}
		if inner == "" {
			return bound(expr, rowEmptyListNode{}, rowValueInfo{pure: true, trivial: true})
		}
		// The split a compilation reads is this caller's own buffer: the texts
		// an ordinary compilation evaluates, or the spans a source-bearing one
		// reports, never both.
		var items rowValuePartList
		if c.origins {
			var spanBuffer [8]expressionFragment
			items = rowValuePartList{spans: offsetFragments(appendExpressionParts(spanBuffer[:0], inside.text, false), inside.start)}
		} else {
			var textBuffer [8]string
			items = rowValuePartList{texts: appendTopLevelComma(textBuffer[:0], inside.text)}
		}
		count := items.count()
		parts := make([]rowValueNode, count)
		infos := make([]rowValueInfo, count)
		for i := 0; i < count; i++ {
			text, start := items.at(i)
			parts[i], infos[i] = c.compile(ctx, expressionFragment{text: text, start: start})
		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowListNode{items: parts}, info)
	}
	var callFacts1 functionCallFacts
	if isFunction := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1); isFunction {
		function, inner := callFacts1.name, callFacts1.inner

		name := lowerASCII(function)
		if isQuantifierFunction(name) {
			if c.origins {
				return c.textValue(part)
			}
			variable, listExpression, predicate, parsed := parseQuantifierArguments(ctx, inner)
			if !parsed {
				return c.textValue(part)
			}
			return c.compileQuantifier(ctx, expr, name, variable, listExpression, predicate)
		}
		return c.compileFunction(ctx, part, name, expressionFragment{text: inner, start: part.start + callFacts1.argumentStart})
	}
	var buffer [8]string
	if plan := planLogicalExpression(expr, buffer[:0]); plan.kind != logicalNone {
		if c.origins {
			return c.textValue(part)
		}
		plan.operands = append([]string(nil), plan.operands...)
		parts := make([]rowValueNode, len(plan.operands))
		infos := make([]rowValueInfo, len(plan.operands))
		for i, operand := range plan.operands {
			parts[i], infos[i] = c.compile(ctx, expressionPart(operand))
			if plan.kind == logicalOr && rowOperandRecordsFailure(operand) {
				parts[i] = &rowDeferredOperandNode{operand: parts[i]}
			}

		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowLogicalNode{plan: plan, operands: parts}, info)
	}
	if _, _, regex := splitByOperatorOutsideCase(expr, "=~", false, true); regex {
		return c.textValue(part)
	}
	if scan, comparison := scanComparisonChain(expr); comparison {
		if c.origins {
			return c.textValue(part)
		}
		parts := make([]rowValueNode, scan.count+1)
		infos := make([]rowValueInfo, scan.count+1)
		for i := 0; i <= scan.count; i++ {
			operand := scan.operand(expr, i)
			parts[i], infos[i] = c.compile(ctx, expressionPart(operand))

		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowComparisonNode{text: expr, scan: scan, operands: parts, normalizeIdentity: comparisonChainHasIdentity(expr, &scan)}, info)
	}
	if _, _, _, isTypePredicate, _ := splitTypePredicate(expr); isTypePredicate {
		return c.textValue(part)
	}
	if _, _, _, isNormalization := splitNormalizationPredicate(expr); isNormalization {
		return c.textValue(part)
	}
	for _, predicate := range []struct {
		suffix  string
		notNull bool
	}{{suffix: " IS NOT NULL", notNull: true}, {suffix: " IS NULL"}} {
		if operand, nullTest := nullPredicateOperand(ctx, expr, strings.TrimSpace(predicate.suffix)); nullTest {
			if c.origins {
				return c.textValue(part)
			}
			operand, info := c.compile(ctx, expressionPart(operand))
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
			leftNode, leftInfo := c.compile(ctx, expressionFragment{text: left, start: part.start})
			rightNode, rightInfo := c.compile(ctx, expressionFragment{text: right, start: part.start + len(expr) - len(right)})
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
			leftNode, leftInfo := c.compile(ctx, expressionFragment{text: left, start: part.start})
			rightNode, rightInfo := c.compile(ctx, expressionFragment{text: right, start: part.start + len(expr) - len(right)})
			parts, infos := []rowValueNode{leftNode, rightNode}, []rowValueInfo{leftInfo, rightInfo}
			info := combineRowValueInfo(infos...)
			c.compose(info, parts, infos)
			return bound(expr, &rowStringPredicateNode{left: parts[0], right: parts[1], match: predicate.match}, info)
		}
	}
	if rowSubscriptStart(expr) > 0 {
		return c.textValue(part)
	}
	for _, tier := range []string{"+-", "*/%"} {
		if left, right, operator, ok := splitRowArithmeticTier(expr, tier); ok {
			leftNode, leftInfo := c.compile(ctx, expressionFragment{text: left, start: part.start})
			rightNode, rightInfo := c.compile(ctx, expressionFragment{text: right, start: part.start + len(expr) - len(right)})
			parts, infos := []rowValueNode{leftNode, rightNode}, []rowValueInfo{leftInfo, rightInfo}
			info := combineRowValueInfo(infos...)
			c.compose(info, parts, infos)
			return bound(expr, &rowArithmeticNode{left: parts[0], right: parts[1], leftText: left, rightText: right, operator: operator}, info)
		}
	}
	if _, _, _, ok := splitRowArithmeticTier(expr, "^"); ok {
		return c.textValue(part)
	}
	if len(expr) > 1 && (expr[0] == '-' || expr[0] == '+') {
		return c.textValue(part)
	}
	if dot := topLevelSymbolIndex(expr, "."); dot > 0 && !isQualifiedFunctionCall(expr) {
		for {
			next := topLevelSymbolIndex(expr[dot+1:], ".")
			if next < 0 {
				break
			}
			dot += next + 1
		}
		if endpoint := endpointPropertyNode(strings.TrimSpace(expr[:dot]), strings.TrimSpace(expr[dot+1:])); endpoint != nil {
			// A fixed endpoint of a row relationship is stable throughout a
			// list predicate. Hoist its property read when the loop variable
			// is unrelated; this also preserves the per-row endpoint cache.
			if cyphertext.IsIdentifier(endpoint.argument) {
				return bound(expr, endpoint, rowValueInfo{pure: true, refs: []string{endpoint.argument}})
			}
			return bound(expr, endpoint, impureRowValue())
		}
		base, info := c.compile(ctx, expressionFragment{text: strings.TrimSpace(expr[:dot]), start: part.start})
		parts, infos := []rowValueNode{base}, []rowValueInfo{info}
		c.compose(info, parts, infos)
		return bound(expr, &rowPropertyNode{text: expr, base: parts[0], chain: strings.TrimSpace(expr[dot+1:])}, info)
	}
	if cyphertext.IsIdentifier(expr) || len(expr) > 1 && expr[0] == '$' && cyphertext.IsIdentifier(expr[1:]) {
		// A row variable or parameter: bound in the row, or read by the
		// fallback as evaluateRowValueText reads it.
		return bound(expr, c.sourceValue(part.start, &rowFallbackNode{text: expr}), rowValueInfo{pure: true, trivial: true, refs: []string{expr}})
	}
	return bound(expr, c.sourceValue(part.start, &rowFallbackNode{text: expr}), impureRowValue())
}

// compileFunction compiles a function call evaluateRowValueText evaluates
// itself; any other is read from the text. A temporal constructor
// (isTemporalConstructor: date, time, localtime, localdatetime, datetime and
// duration) keeps the canonical evaluator, evaluateTemporalConstructor, and
// compiles its arguments. None of the other names is a math, extension,
// plugin (those have a dot) or GDS function, which evaluateRowValueText
// tries first.
func (c *rowValueCompiler) compileFunction(ctx context.Context, part expressionFragment, name string, argument expressionFragment) (rowValueNode, rowValueInfo) {
	expr := part.text
	if isTemporalConstructor(name) {
		if c.origins {
			// The constructor asks for its arguments by text, so their own
			// positions are not this reader's to report: the call is read at
			// its own origin instead of guessing inside it.
			return c.textValue(part)
		}
		return c.compileTemporalFunction(ctx, expr, name, argument.text)
	}
	switch name {
	case "startnode", "endnode":
		variable := strings.TrimSpace(argument.text)
		if !cyphertext.IsIdentifier(variable) {
			return c.textValue(part)
		}
		return bound(expr, &rowEndpointNode{text: expr, function: name, variable: variable}, rowValueInfo{pure: true, refs: []string{variable}})
	case "size", "head", "last", "reverse", "type":
		operand, operandInfo := c.compile(ctx, argument)
		info := combineRowValueInfo(operandInfo)
		parts, infos := []rowValueNode{operand}, []rowValueInfo{operandInfo}
		c.compose(info, parts, infos)
		return bound(expr, &rowUnaryFunctionNode{function: name, operand: parts[0]}, info)
	case "coalesce":
		// The operands are parts of the argument: each starts where the
		// argument starts plus where it starts in it. The split a compilation
		// reads is this caller's own buffer: the texts an ordinary compilation
		// evaluates, or the spans a source-bearing one reports, never both.
		var operands rowValuePartList
		if c.origins {
			var spanBuffer [8]expressionFragment
			operands = rowValuePartList{spans: offsetFragments(appendExpressionParts(spanBuffer[:0], argument.text, false), argument.start)}
		} else {
			var textBuffer [8]string
			operands = rowValuePartList{texts: appendTopLevelComma(textBuffer[:0], argument.text)}
		}
		count := operands.count()
		parts := make([]rowValueNode, count)
		infos := make([]rowValueInfo, count)
		for i := 0; i < count; i++ {
			text, start := operands.at(i)
			parts[i], infos[i] = c.compile(ctx, expressionFragment{text: text, start: start})
		}
		info := combineRowValueInfo(infos...)
		c.compose(info, parts, infos)
		return bound(expr, &rowCoalesceNode{operands: parts}, info)
	case "reduce", "allreduce":
		if c.origins {
			return c.textValue(part)
		}
		form, ok := parseReduceForm(name, argument.text)
		if !ok {
			return c.textValue(part)
		}
		return c.compileReduce(ctx, expr, form)
	}
	return c.textValue(part)
}

// compileTemporalFunction compiles a temporal constructor's arguments for
// evaluateTemporalConstructor. The evaluator evaluates an argument by its
// text: the trimmed whole argument, or one of the arguments splitFunctionArgs
// returns. The node therefore keeps those texts with the parts compiled from
// them and answers the evaluator from that slice, which is at most the call's
// arguments plus its whole argument. A qualified temporal function
// (X.truncate, the duration.between family, datetime.fromepoch*) is not a
// temporal constructor and keeps the text path.
func (c *rowValueCompiler) compileTemporalFunction(ctx context.Context, expr, name, argument string) (rowValueNode, rowValueInfo) {
	arguments := splitTopLevelComma(argument)
	texts := make([]string, 0, len(arguments)+1)
	parts := make([]rowValueNode, 0, len(arguments)+1)
	retain := func(text string) {
		// An empty argument is unresolved however it is read, so it needs no
		// part: the node answers an unmatched text as unresolved.
		if text == "" {
			return
		}
		for _, retained := range texts {
			if retained == text {
				return
			}
		}
		part, _ := c.compile(ctx, expressionPart(text))
		texts, parts = append(texts, text), append(parts, part)
	}
	for _, text := range arguments {
		retain(text)
	}
	retain(strings.TrimSpace(argument))
	// The constructors' part arguments are impure: the result may read the
	// statement's clock, and neither a clock nor an argument's error may be
	// hoisted out of a list evaluation or kept across rows.
	return bound(expr, &rowTemporalFunctionNode{function: name, argument: argument, texts: texts, parts: parts}, impureRowValue())
}

// compileLoopBody compiles a per-element part of a list evaluation over
// variable.
func (c *rowValueCompiler) compileLoopBody(ctx context.Context, loop *rowValueLoop, expr string) rowValueNode {
	if expr == "" {
		return nil
	}
	c.loops = append(c.loops, loop)
	node, info := c.compile(ctx, expressionPart(expr))
	node = c.hoist(node, info)
	c.loops = c.loops[:len(c.loops)-1]
	return node
}

func (c *rowValueCompiler) compileQuantifier(ctx context.Context, expr, function, variable, listExpression, predicate string) (rowValueNode, rowValueInfo) {
	list, listInfo := c.compile(ctx, expressionPart(listExpression))
	parts, infos := []rowValueNode{list}, []rowValueInfo{listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{variable}}
	body := c.compileLoopBody(ctx, loop, predicate)
	return bound(expr, &rowQuantifierNode{function: function, variable: variable, list: parts[0], predicate: body, loop: loop}, impureRowValue())
}

func (c *rowValueCompiler) compileComprehension(ctx context.Context, expr, variable, listExpression, predicate, projection string) (rowValueNode, rowValueInfo) {
	if predicate != "" && containsRelationshipPattern(predicate) {
		return rowTextValue(expr)
	}
	list, listInfo := c.compile(ctx, expressionPart(listExpression))
	parts, infos := []rowValueNode{list}, []rowValueInfo{listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{variable}}
	node := &rowComprehensionNode{variable: variable, list: parts[0], loop: loop}
	node.predicate = c.compileLoopBody(ctx, loop, predicate)
	node.projection = c.compileLoopBody(ctx, loop, projection)
	return bound(expr, node, impureRowValue())
}

// compileReduce compiles reduce / allReduce as evaluateRowReduce evaluates
// it: a list evaluation whose step and predicate read the accumulator and
// the variable.
func (c *rowValueCompiler) compileReduce(ctx context.Context, expr string, form reduceForm) (rowValueNode, rowValueInfo) {
	initial, initialInfo := c.compile(ctx, expressionPart(form.initial))
	list, listInfo := c.compile(ctx, expressionPart(form.list))
	parts, infos := []rowValueNode{initial, list}, []rowValueInfo{initialInfo, listInfo}
	c.compose(impureRowValue(), parts, infos)
	loop := &rowValueLoop{variables: []string{form.accumulator, form.variable}}
	node := &rowReduceNode{form: form, initial: parts[0], list: parts[1], loop: loop}
	node.step = c.compileLoopBody(ctx, loop, form.step)
	if form.all {
		if node.predicate = c.compileLoopBody(ctx, loop, form.predicate); node.predicate == nil {
			node.predicate = rowUnresolvedNode{}
		}
	}
	if plan := form.appendPlan(ctx); plan != nil {
		for _, term := range plan.terms {
			node.terms = append(node.terms, c.compileLoopBody(ctx, loop, term))
		}
	}
	return bound(expr, node, impureRowValue())
}

// rowReduceNode is reduce / allReduce(accumulator = initial, variable IN
// list | step[, predicate]), folded by runReduceForm; terms are the step's
// operands when it has an append plan.
type rowReduceNode struct {
	form            reduceForm
	initial, list   rowValueNode
	step, predicate rowValueNode
	terms           []rowValueNode
	loop            *rowValueLoop
}

func (n *rowReduceNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	accumulator, resolved, err := n.initial.eval(ctx, e, values, frames)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := n.list.eval(ctx, e, values, frames)
	if err != nil || !resolved {
		return nil, false, err
	}
	if listValue == nil {
		return nil, true, nil
	}
	scope, inner := rowLoopScope(values, frames, n.loop)
	var term func(index int) (interface{}, bool, error)
	if len(n.terms) > 0 {
		term = func(index int) (interface{}, bool, error) { return n.terms[index].eval(ctx, e, scope, inner) }
	}
	return rowReduceLoop(ctx, n.form, accumulator, listValue, scope,
		func() (interface{}, bool, error) { return n.step.eval(ctx, e, scope, inner) },
		func() (interface{}, bool, error) { return n.predicate.eval(ctx, e, scope, inner) }, term)
}

// compileCase compiles a CASE expression as evaluateRowCaseExpression
// evaluates it.
func (c *rowValueCompiler) compileCase(ctx context.Context, expr string) *rowCaseNode {
	parsed, err := parseCaseExpression(expr, nil)
	if err != nil {
		return &rowCaseNode{}
	}
	node := &rowCaseNode{parsed: parsed}
	if parsed.isSimple {
		node.test, _ = c.compile(ctx, expressionPart(parsed.testExpression))
	}
	node.whens = make([]rowValueNode, len(parsed.whenClauses))
	node.results = make([]rowValueNode, len(parsed.whenClauses))
	for i, clause := range parsed.whenClauses {
		if parsed.isSimple {
			node.whens[i], _ = c.compile(ctx, expressionPart(clause.value))
		} else {
			node.whens[i], _ = c.compile(ctx, expressionPart(clause.condition))
		}
		node.results[i], _ = c.compile(ctx, expressionPart(clause.result))
	}
	if parsed.elseResult != "" {
		node.elseResult, _ = c.compile(ctx, expressionPart(parsed.elseResult))
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

func (n *rowCaseNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
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
		value, ok, err := node.eval(ctx, e, values, frames)
		if part == caseWhen && !n.parsed.isSimple && err == nil && !ok {
			return e.evaluateRowPredicate(ctx, text, values), true, getExpressionFailure(ctx)
		}
		return value, ok, err
	})
}

// rowSubqueryNode is an EXISTS / COUNT / COLLECT { … } subquery read once:
// its keyword and body. It runs the body for the row in the statement
// context the row carries, as evaluateRowSubqueryAtom does, without reading
// the text again. A row without a statement context, or a body the
// subquery evaluator doesn't resolve, takes the text route. Where the
// subquery starts, when its program reports source occurrences, is the
// wrapper's (rowSourceValueNode).
type rowSubqueryNode struct {
	text string
	kind string
	body string
}

func (n *rowSubqueryNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	value, resolved, err := e.evaluateRowSubqueryBody(n.kind, n.body, values)
	if err != nil {
		return nil, false, err
	}
	if !resolved {
		return e.evaluateRowValueText(ctx, n.text, values)
	}
	return value, true, nil
}

// rowTextNode evaluates its text with evaluateRowValueText. Where the text
// starts, when its program reports source occurrences, is the wrapper's
// (rowSourceValueNode), so an ordinary text node is its text alone.
type rowTextNode struct{ text string }

func (n *rowTextNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return e.evaluateRowValueText(ctx, n.text, values)
}

// rowTemporalFunctionNode is a temporal constructor (date, time, localtime,
// localdatetime, datetime, duration): evaluateTemporalConstructor owns its
// semantics, and the node answers the argument texts that evaluator asks for
// with the parts compiled for them (texts, parts). A text the evaluator asks
// for that wasn't compiled is unresolved; it is never read from the text here.
type rowTemporalFunctionNode struct {
	function string
	argument string
	texts    []string
	parts    []rowValueNode
}

func (n *rowTemporalFunctionNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	// The constructors' clock and the arguments' evaluation use the row's
	// statement context and the operation context, as evaluateRowValueText
	// reads them.
	temporalContext := ctx
	if inherited := rowStatementContext(values); inherited != nil {
		temporalContext = inherited
	}
	evaluate := func(argument string) (interface{}, bool, error) {
		for index, text := range n.texts {
			if text == argument {
				return n.parts[index].eval(ctx, e, values, frames)
			}
		}
		return nil, false, nil
	}
	value, handled, err := e.evaluateTemporalConstructor(temporalContext, evaluate, functionCallFacts{name: n.function, inner: n.argument})
	return value, handled && err == nil, err
}

// rowBoundNode is values[text] when the row binds the text, else inner.
type rowBoundNode struct {
	text  string
	inner rowValueNode
}

func (n *rowBoundNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	if value, ok := values[n.text]; ok {
		return runtimeRowResult(value, true, nil)
	}
	return n.inner.eval(ctx, e, values, frames)
}

type rowUnresolvedNode struct{}

func (rowUnresolvedNode) eval(ctx context.Context, _ *StorageExecutor, _ map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return nil, false, nil
}

type rowConstNode struct{ value interface{} }

func (n rowConstNode) eval(ctx context.Context, _ *StorageExecutor, _ map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return n.value, true, nil
}

// rowLiteralNode is a list or map literal, parsed per evaluation: the value
// is the caller's to change.
type rowLiteralNode struct{ text string }

func (n *rowLiteralNode) eval(ctx context.Context, _ *StorageExecutor, _ map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	value, _ := parseLiteralValueFromComputedRow(n.text)
	return value, true, nil
}

type rowEmptyListNode struct{}

func (rowEmptyListNode) eval(ctx context.Context, _ *StorageExecutor, _ map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return []interface{}{}, true, nil
}

// rowPropertyChainNode is a property chain on a row variable (n.a.b); a
// variable the row doesn't bind takes the text path.
type rowPropertyChainNode struct {
	text, variable, chain string
	literal               bool
}

func (n *rowPropertyChainNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	if value, ok := values[n.text]; ok && !n.literal {
		return value, true, nil
	}
	base, bound := values[n.variable]
	if !bound {
		return e.evaluateRowValueText(ctx, n.text, values)
	}
	return evaluateRowPropertyResult(base, n.chain, e)
}

// rowFallbackNode is an expression no form of the row evaluator matches:
// the context evaluator's value (evaluateRowFallback). Where it starts, when
// its program reports source occurrences, is the wrapper's
// (rowSourceValueNode).
type rowFallbackNode struct{ text string }

func (n *rowFallbackNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	return e.evaluateRowFallback(ctx, n.text, values, rowFallbackValue)
}

type rowListNode struct{ items []rowValueNode }

func (n *rowListNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return rowListLiteral(len(n.items), func(i int) (interface{}, bool, error) {
		return n.items[i].eval(ctx, e, values, frames)
	})
}

type rowLogicalNode struct {
	plan     logicalPlan
	operands []rowValueNode
}

func (n *rowLogicalNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, ok, err := n.plan.evaluate(func(index int) (interface{}, bool, error) {
		return n.operands[index].eval(ctx, e, values, frames)
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return value, true, nil
}

type rowDeferredOperandNode struct{ operand rowValueNode }

func (n *rowDeferredOperandNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return deferredRowOperand(ctx, values, func(operandCtx context.Context, scope map[string]interface{}) (interface{}, bool, error) {
		return n.operand.eval(operandCtx, e, scope, frames)
	})
}

type rowComparisonNode struct {
	text              string
	scan              comparisonChainScan
	operands          []rowValueNode
	normalizeIdentity bool
}

func (n *rowComparisonNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return evaluateScannedComparisonChain(n.text, &n.scan, n.normalizeIdentity, func(index int, _ string) (interface{}, bool, error) {
		return n.operands[index].eval(ctx, e, values, frames)
	}, compareCypherPredicateValue, rowIsCypher25(values))
}

type rowNullTestNode struct {
	operand rowValueNode
	notNull bool
}

func (n *rowNullTestNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, ok, err := n.operand.eval(ctx, e, values, frames)
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

func (n *rowMembershipNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	needle, leftOK, err := n.needle.eval(ctx, e, values, frames)
	if err != nil {
		return nil, false, err
	}
	haystack, rightOK, err := n.haystack.eval(ctx, e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	value, ok := rowMembershipOfValues(needle, haystack, n.identity)
	if !n.negated || !ok || value == nil {
		return value, ok, nil
	}
	return !value.(bool), true, nil
}

type rowStringPredicateNode struct {
	left, right rowValueNode
	match       func(string, string) bool
}

func (n *rowStringPredicateNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	leftValue, leftOK, err := n.left.eval(ctx, e, values, frames)
	if err != nil {
		return nil, false, err
	}
	rightValue, rightOK, err := n.right.eval(ctx, e, values, frames)
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

func (n *rowArithmeticNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	leftValue, leftOK, err := n.left.eval(ctx, e, values, frames)
	if err != nil {
		return nil, false, err
	}
	rightValue, rightOK, err := n.right.eval(ctx, e, values, frames)
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

func (n *rowPropertyNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	base, ok, err := n.base.eval(ctx, e, values, frames)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return e.evaluateRowFallback(ctx, n.text, values, rowFallbackValue)
	}
	return evaluateRowPropertyResult(base, n.chain, e)
}

// evaluateRowPropertyResult owns the value/error contract after either a bound
// variable or a computed expression has supplied the base of a property chain.
func evaluateRowPropertyResult(base interface{}, chain string, e *StorageExecutor) (interface{}, bool, error) {
	value, ok := evaluateRowPropertyChain(base, chain, e, false)
	if !ok {
		return nil, false, rowPropertyChainTypeError(base, chain)
	}
	return value, true, nil
}

// rowEndpointNode is startNode(variable) / endNode(variable).
type rowEndpointNode struct {
	text, function, variable string
}

func (n *rowEndpointNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	if value, handled, err := e.rowRelationshipEndpoint(ctx, n.function, n.variable, values); handled || err != nil {
		return value, handled, err
	}
	return e.evaluateRowValueText(ctx, n.text, values)
}

// rowUnaryFunctionNode is size / head / last / reverse / type of one
// argument, as evaluateRowValueText evaluates them.
type rowUnaryFunctionNode struct {
	function string
	operand  rowValueNode
}

func (n *rowUnaryFunctionNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	value, resolved, err := n.operand.eval(ctx, e, values, frames)
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

func (n *rowCoalesceNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	return cypherfn.Coalesce(len(n.operands), func(index int) (interface{}, bool, error) {
		return n.operands[index].eval(ctx, e, values, frames)
	})
}

// rowMemoNode is a part with one value for every element of the list
// evaluation at depth: evaluated once, when first needed.
type rowMemoNode struct {
	depth, slot int
	inner       rowValueNode
}

func (n *rowMemoNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	memo := &frames[n.depth][n.slot]
	if !memo.done {
		memo.value, memo.ok, memo.err = n.inner.eval(ctx, e, values, frames[:n.depth])
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

func (n *rowQuantifierNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	listValue, ok, err := n.list.eval(ctx, e, values, frames)
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
		return n.predicate.eval(ctx, e, scope, inner)
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

func (n *rowComprehensionNode) eval(ctx context.Context, e *StorageExecutor, values map[string]interface{}, frames [][]rowValueMemo) (interface{}, bool, error) {
	listValue, ok, err := n.list.eval(ctx, e, values, frames)
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
		holds = func() (bool, bool, error) { return rowConditionHolds(n.predicate.eval(ctx, e, scope, inner)) }
	}
	var project func() (interface{}, bool, error)
	if n.projection != nil {
		project = func() (interface{}, bool, error) { return n.projection.eval(ctx, e, scope, inner) }
	}
	return rowComprehensionLoop(coerceToUnwindItems(listValue), func(item interface{}) { scope[n.variable] = item }, holds, project)
}
