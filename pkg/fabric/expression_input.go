package fabric

import (
	"sort"
	"sync"

	antlr4 "github.com/antlr4-go/antlr/v4"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// ExpressionInput is a source-span view of one native statement. Slicing a view
// never parses its text or consults a text cache. The admission walk owns the
// statement and passes views to its clause, operand and local-scope consumers.
type ExpressionInput struct {
	facts      *statementExpressionFacts
	start, end int
}

type nativeReference struct {
	ExpressionReference
	query, localSpan ExpressionSpan
}

// QueryOperand is an expression selected by native clause ownership. Predicate
// and Projected retain the input phase needed by the shared semantic walk.
type QueryOperand struct {
	Input                ExpressionInput
	Predicate, Projected bool
	Pagination           string
}

type nativeOperand struct {
	span, query ExpressionSpan
	predicate   bool
	pagination  string
}

type nativeBindingEvent struct {
	name        string
	at, end     int
	binding     bool
	patternRead bool
	local       ExpressionSpan
}

type statementExpressionFacts struct {
	positions      *structureReader
	source         string
	tree           antlr4.Tree
	queryOnce      sync.Once
	query          *QueryStructure
	lazy           bool
	once           sync.Once
	scopes         []ExpressionScope
	scopeIndex     *nativeScopeIndex
	references     []nativeReference
	operands       []nativeOperand
	bindings       []nativeBindingEvent
	bindingsBySpan sync.Map
	clauses        sync.Map
	queriesOnce    sync.Once
	queries        map[ExpressionSpan]*QueryStructure
	children       map[ExpressionSpan][]ExpressionSpan
	arguments      map[int][]ExpressionSpan
}

// BindingRoles selects first native declarations/references by source span.
// Interval keys belong to this immutable statement, never to source text.
func (input ExpressionInput) BindingRoles() map[string]bool {
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	key := ExpressionSpan{input.start, input.end}
	if cached, ok := input.facts.bindingsBySpan.Load(key); ok {
		return cached.(map[string]bool)
	}
	roles := make(map[string]bool)
	start := sort.Search(len(input.facts.bindings), func(index int) bool { return input.facts.bindings[index].at >= input.start })
	for _, event := range input.facts.bindings[start:] {
		if event.at >= input.end {
			break
		}
		if event.local.End > event.local.Start && event.local.Start >= input.start && event.local.End <= input.end {
			continue
		}
		if _, found := roles[event.name]; !found {
			roles[event.name] = event.binding
		}
	}
	cached, _ := input.facts.bindingsBySpan.LoadOrStore(key, roles)
	return cached.(map[string]bool)
}

// NewStatementInput retains provenance without eagerly acquiring a native
// tree. Its first structural request supplies the same owner to every view.
func NewStatementInput(text string) ExpressionInput {
	return ExpressionInput{facts: &statementExpressionFacts{source: text, lazy: true}, end: len(text)}
}

func (input ExpressionInput) resolved() ExpressionInput {
	if !input.Valid() || !input.facts.lazy {
		return input
	}
	input.facts.queryOnce.Do(func() { input.facts.query = ReadQueryStructure(input.facts.source) })
	query := input.facts.query
	if query.Err != nil || !query.Input.Valid() {
		return ExpressionInput{}
	}
	input.facts = query.Input.facts
	return input
}

// NativeClause selects an existing native clause by its source interval.
func (input ExpressionInput) NativeClause() *QueryClause {
	input = input.resolved()
	if !input.Valid() || input.facts.query == nil {
		return nil
	}
	key := ExpressionSpan{input.start, input.end}
	if clause, found := input.facts.clauses.Load(key); found {
		return clause.(*QueryClause)
	}
	input.QueryScopes()
	if clause, found := input.facts.clauses.Load(key); found {
		return clause.(*QueryClause)
	}
	return nil
}

// Operands supplies top-level clause expressions without parsing them again.
// Nested CALL and expression-query bodies have their own clause admission.
func (clause *QueryClause) Operands() []QueryOperand { return clause.operands(false) }

// PaginationOperands transports native SKIP/LIMIT operand spans with their
// grammar-owned position, through the same statement fact index.
func (clause *QueryClause) PaginationOperands() []QueryOperand { return clause.operands(true) }

func (clause *QueryClause) operands(pagination bool) []QueryOperand {
	input := clause.Input
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	start := sort.Search(len(input.facts.operands), func(index int) bool { return input.facts.operands[index].span.Start >= input.start })
	var result []QueryOperand
	for _, operand := range input.facts.operands[start:] {
		if operand.span.Start >= input.end {
			break
		}
		if (operand.pagination != "") != pagination || operand.span.End > input.end || operand.query.Start > input.start || operand.query.End < input.end {
			continue
		}
		result = append(result, QueryOperand{Input: ExpressionInput{input.facts, operand.span.Start, operand.span.End}, Pagination: operand.pagination, Predicate: operand.predicate, Projected: clause.Projection != nil && operand.span.Start >= clause.Projection.itemsEnd})
	}
	return result
}

// RootScope selects only a binding whose native span owns this whole operand.
func (input ExpressionInput) RootScope() (ExpressionScope, bool) {
	if input.Valid() && !QuerySyntaxDemand(input.Text()).Has(NativeLocalScopes) {
		return ExpressionScope{}, false
	}
	input = input.resolved()
	if !input.Valid() {
		return ExpressionScope{}, false
	}
	input.facts.load()
	start := sort.Search(len(input.facts.scopes), func(index int) bool { return input.facts.scopes[index].Span.Start >= input.start })
	for _, scope := range input.facts.scopes[start:] {
		if scope.Span.Start != input.start {
			break
		}
		if scope.Span.End == input.end {
			return input.relativeScopes([]ExpressionScope{scope})[0], true
		}
	}
	return ExpressionScope{}, false
}

// nativeScopeIndex indexes native spans, so a point lookup visits enclosing
// scopes instead of rescanning every declaration elsewhere in the statement.
type nativeScopeIndex struct {
	scope       ExpressionScope
	end         int
	left, right *nativeScopeIndex
}

func indexNativeScopes(scopes []ExpressionScope) *nativeScopeIndex {
	if len(scopes) == 0 {
		return nil
	}
	index := len(scopes) / 2
	node := &nativeScopeIndex{scope: scopes[index], end: scopes[index].Span.End}
	node.left, node.right = indexNativeScopes(scopes[:index]), indexNativeScopes(scopes[index+1:])
	for _, child := range []*nativeScopeIndex{node.left, node.right} {
		if child != nil {
			node.end = max(node.end, child.end)
		}
	}
	return node
}

func (index *nativeScopeIndex) at(at int, visit func(ExpressionScope)) {
	if index == nil || index.end <= at {
		return
	}
	index.left.at(at, visit)
	if index.scope.Span.Start > at {
		return
	}
	if at < index.scope.Span.End {
		visit(index.scope)
	}
	index.right.at(at, visit)
}

func (input ExpressionInput) Valid() bool { return input.facts != nil }

func (input ExpressionInput) Text() string {
	if !input.Valid() {
		return ""
	}
	return input.facts.source[input.start:input.end]
}

// Slice retains native provenance, including for identical text at different
// positions. Offsets are relative to this view, not discovered from its text.
func (input ExpressionInput) Slice(start, end int) ExpressionInput {
	if !input.Valid() || start < 0 || end < start || end > input.end-input.start {
		return ExpressionInput{}
	}
	return ExpressionInput{input.facts, input.start + start, input.start + end}
}

// RangeIn returns this view's offsets in its native parent view.
func (input ExpressionInput) RangeIn(parent ExpressionInput) (int, int, bool) {
	if !input.Valid() || input.facts != parent.facts || input.start < parent.start || input.end > parent.end {
		return 0, 0, false
	}
	return input.start - parent.start, input.end - parent.start, true
}

// Scopes projects declarations contained in this source view. The statement's
// native tree is visited once, independently of how many consumers use it.
func (input ExpressionInput) Scopes() []ExpressionScope {
	if input.Valid() && !QuerySyntaxDemand(input.Text()).Has(NativeLocalScopes) {
		return nil
	}
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	if input.facts.scopeIndex == nil {
		return nil
	}
	var selected []ExpressionScope
	start := sort.Search(len(input.facts.scopes), func(index int) bool { return input.facts.scopes[index].Span.Start >= input.start })
	for _, scope := range input.facts.scopes[start:] {
		if scope.Span.Start >= input.end {
			break
		}
		if scope.Span.End <= input.end {
			selected = append(selected, scope)
		}
	}
	return input.relativeScopes(selected)
}

// Branches retains each UNION branch's original native clauses and spans.
func (query *QueryStructure) Branches() []*QueryStructure {
	var branches []*QueryStructure
	start := 0
	appendBranch := func(end int) {
		if end == start {
			return
		}
		clauses := query.Clauses[start:end]
		input := query.Input
		if input.Valid() {
			input.start, input.end = clauses[0].Start, clauses[len(clauses)-1].End
		}
		branches = append(branches, &QueryStructure{Clauses: clauses, Input: input, parsed: query.parsed, tree: query.tree, Err: query.Err})
	}
	for index, clause := range query.Clauses {
		if clause.Kind == "UNION" {
			appendBranch(index)
			start = index + 1
		}
	}
	appendBranch(len(query.Clauses))
	return branches
}

// ScopesAt selects only scopes enclosing an occurrence in this view.
func (input ExpressionInput) ScopesAt(at int) []ExpressionScope {
	if input.Valid() && !QuerySyntaxDemand(input.Text()).Has(NativeLocalScopes) {
		return nil
	}
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	if input.facts.scopeIndex == nil {
		return nil
	}
	var selected []ExpressionScope
	input.facts.scopeIndex.at(input.start+at, func(scope ExpressionScope) {
		if scope.Span.Start >= input.start && scope.Span.End <= input.end {
			selected = append(selected, scope)
		}
	})
	return input.relativeScopes(selected)
}

func (input ExpressionInput) relativeScopes(scopes []ExpressionScope) []ExpressionScope {
	depth := 0
	input.facts.scopeIndex.at(input.start, func(scope ExpressionScope) {
		if scope.Kind == ExpressionQueryScope && scope.Open < input.start && scope.Close >= input.end {
			depth++
		}
	})
	result := relativeExpressionScopes(scopes, input.start)
	for index := range result {
		result[index].QueryDepth -= depth
	}
	return result
}

// References selects native occurrences in the current expression-query scope.
// A declaration outside the view is an input binding, rather than a local
// declaration of that view. CALL footprints remain visible; expression-query
// bodies are admitted by their own walk.
func (input ExpressionInput) References() []ExpressionReference {
	return input.references(false)
}

// ReadReferences retains captures and pattern reuse for physical scheduling.
// A pattern declaration can read an already-bound node, relationship or path;
// projection must retain that value. Admission keeps its declaration-only view.
func (input ExpressionInput) ReadReferences() []ExpressionReference {
	return input.references(true)
}

func (input ExpressionInput) references(includeQueries bool) []ExpressionReference {
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	var selected []ExpressionReference
	pattern := 0
	if includeQueries {
		pattern = sort.Search(len(input.facts.bindings), func(index int) bool { return input.facts.bindings[index].at >= input.start })
	}
	start := sort.Search(len(input.facts.references), func(index int) bool { return input.facts.references[index].Start >= input.start })
	for _, native := range input.facts.references[start:] {
		if native.Start >= input.end {
			break
		}
		if includeQueries {
			selected, pattern = input.appendPatternReads(selected, pattern, native.Start)
		}
		if native.End > input.end || !includeQueries && (native.query.Start > input.start || native.query.End < input.end) {
			continue
		}
		reference := native.ExpressionReference
		reference.Local = reference.Local && native.localSpan.Start >= input.start && native.localSpan.End <= input.end
		reference.Start -= input.start
		reference.End -= input.start
		selected = append(selected, reference)
	}
	if includeQueries {
		selected, _ = input.appendPatternReads(selected, pattern, input.end)
	}
	return selected
}

// Merge physical pattern uses from the existing ordered binding index. Views
// borrow statement-owned roles/spans; no tree walk, reparse or per-view sort.
func (input ExpressionInput) appendPatternReads(selected []ExpressionReference, cursor, before int) ([]ExpressionReference, int) {
	for cursor < len(input.facts.bindings) {
		event := input.facts.bindings[cursor]
		if event.at >= before {
			break
		}
		cursor++
		if !event.patternRead || event.end > input.end {
			continue
		}
		selected = append(selected, ExpressionReference{Name: event.name, Start: event.at - input.start, End: event.end - input.start,
			Local: event.local.End > event.local.Start && event.local.Start >= input.start && event.local.End <= input.end})
	}
	return selected, cursor
}

func (facts *statementExpressionFacts) load() {
	facts.once.Do(func() {
		if facts.tree == nil {
			return
		}
		positions := facts.positions
		facts.scopes = nativeExpressionScopes(facts.tree, positions)
		sort.SliceStable(facts.scopes, func(i, j int) bool { return facts.scopes[i].Span.Start < facts.scopes[j].Span.Start })
		facts.scopeIndex = indexNativeScopes(facts.scopes)
		facts.children = make(map[ExpressionSpan][]ExpressionSpan)
		facts.arguments = make(map[int][]ExpressionSpan)
		var visit func(antlr4.Tree, antlr4.Tree, ExpressionSpan, ExpressionSpan, bool, ExpressionSpan)
		visit = func(tree, parent antlr4.Tree, query, operandQuery ExpressionSpan, inExpression bool, expressionParent ExpressionSpan) {
			switch node := tree.(type) {
			case grammar.ISubqueryExistContext:
				query = ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}
				operandQuery, inExpression, expressionParent = query, false, ExpressionSpan{}
			case grammar.ICountSubqueryContext:
				query = ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}
				operandQuery, inExpression, expressionParent = query, false, ExpressionSpan{}
			case grammar.ICollectSubqueryContext:
				query = ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}
				operandQuery, inExpression, expressionParent = query, false, ExpressionSpan{}
			case grammar.IFabricCallSubqueryContext:
				operandQuery, inExpression, expressionParent = ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}, false, ExpressionSpan{}
			case grammar.ICallSubqueryContext:
				operandQuery, inExpression, expressionParent = ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}, false, ExpressionSpan{}
			}
			if call, ok := tree.(grammar.IFunctionInvocationContext); ok {
				arguments := call.AllExpression()
				if chain := call.ExpressionChain(); chain != nil {
					arguments = chain.AllExpression()
				}
				open := positions.start(call.LPAREN().GetSymbol())
				spans := make([]ExpressionSpan, 0, len(arguments))
				for _, argument := range arguments {
					spans = append(spans, ExpressionSpan{positions.start(argument.GetStart()), positions.end(argument.GetStop())})
				}
				facts.arguments[open] = spans
			}
			if expression, ok := tree.(grammar.IExpressionContext); ok {
				span := ExpressionSpan{positions.start(expression.GetStart()), positions.end(expression.GetStop())}
				if expressionParent.End > expressionParent.Start && span != expressionParent {
					facts.children[expressionParent] = append(facts.children[expressionParent], span)
				}
				expressionParent = span
				if !inExpression {
					operand := nativeOperand{span: ExpressionSpan{positions.start(expression.GetStart()), positions.end(expression.GetStop())}, query: operandQuery}
					switch parent.(type) {
					case grammar.IWhereContext, grammar.IFilterStContext:
						operand.predicate = true
					case grammar.ISkipStContext:
						operand.pagination = "SKIP"
					case grammar.ILimitStContext:
						operand.pagination = "LIMIT"
					}
					facts.operands = append(facts.operands, operand)
				}
				inExpression = true
			}
			if symbol, ok := tree.(grammar.ISymbolContext); ok {
				role := symbolRole(symbol, parent)
				expressionPattern := role.patternBinding && expressionPatternReference(symbol)
				if role.queryBinding || role.variable && !role.procedureColumn {
					event := nativeBindingEvent{name: unquoteSyntaxName(symbol.GetText()), at: positions.start(symbol.GetStart()), end: positions.end(symbol.GetStop()), binding: role.queryBinding, patternRead: role.patternBinding && !expressionPattern}
					facts.scopeIndex.at(event.at, func(scope ExpressionScope) {
						if scope.Binds(event.name, event.at) {
							event.local = scope.Span
						}
					})
					facts.bindings = append(facts.bindings, event)
				}
				if role.variable && !role.binding || expressionPattern {
					reference := nativeReference{ExpressionReference: ExpressionReference{Name: unquoteSyntaxName(symbol.GetText()), Start: positions.start(symbol.GetStart()), End: positions.end(symbol.GetStop())}, query: query}
					facts.scopeIndex.at(reference.Start, func(scope ExpressionScope) {
						if scope.Binds(reference.Name, reference.Start) {
							reference.Local, reference.localSpan = true, scope.Span
						}
					})
					if atom, ok := parent.(grammar.IAtomContext); ok {
						if property, ok := atom.GetParent().(grammar.IPropertyExpressionContext); ok && len(property.AllName()) > 0 {
							reference.Property = unquoteSyntaxName(property.Name(0).GetText())
						}
					}
					facts.references = append(facts.references, reference)
				}
			}
			for index := 0; index < tree.GetChildCount(); index++ {
				visit(tree.GetChild(index), tree, query, operandQuery, inExpression, expressionParent)
			}
		}
		root := ExpressionSpan{0, len(facts.source)}
		visit(facts.tree, nil, root, root, false, ExpressionSpan{})
		sort.SliceStable(facts.references, func(i, j int) bool { return facts.references[i].Start < facts.references[j].Start })
		sort.SliceStable(facts.operands, func(i, j int) bool { return facts.operands[i].span.Start < facts.operands[j].span.Start })
		sort.SliceStable(facts.bindings, func(i, j int) bool { return facts.bindings[i].at < facts.bindings[j].at })
	})
}

// ChildExpressions follows native expression-parent edges. It supplies the
// spans of list/map items and function arguments to recursive operand readers.
func (input ExpressionInput) ChildExpressions() []ExpressionInput {
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	input.facts.load()
	spans := input.facts.children[ExpressionSpan{input.start, input.end}]
	result := make([]ExpressionInput, 0, len(spans))
	for _, span := range spans {
		result = append(result, ExpressionInput{input.facts, span.Start, span.End})
	}
	return result
}

// FunctionArguments projects argument spans from the invocation grammar node.
// Separators, DISTINCT and TRIM syntax are never mistaken for operand text.
func (input ExpressionInput) FunctionArguments(open int) ([]ExpressionInput, bool) {
	input = input.resolved()
	if !input.Valid() {
		return nil, false
	}
	input.facts.load()
	spans, found := input.facts.arguments[input.start+open]
	result := make([]ExpressionInput, 0, len(spans))
	for _, span := range spans {
		result = append(result, ExpressionInput{input.facts, span.Start, span.End})
	}
	return result, found
}
