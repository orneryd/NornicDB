package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryExecutePipelineSimpleRelationshipCountPlan is the fused physical form of
// the pipeline's MATCH -> count reduction for relationship patterns (issue
// #638). Without it, the row pipeline binds the relationship variable for
// every match and counts the materialized rows — so
//
//	MATCH ()-[r:T]->() RETURN count(r)
//
// costs O(store size) instead of an O(1) counter read, even though the
// traversal-layer fast path exists. It delegates to relationshipCountFromStore,
// the single owner of counts answered from the counters (count_store.go).
func (e *StorageExecutor) tryExecutePipelineSimpleRelationshipCountPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) pipelineDispatchOutcome {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	if topLevelKeywordIndex(ctx, matchBody, "WHERE") >= 0 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// This counter plan owns a relationship pattern, never a path assignment
	// or shortestPath call. Decline their syntax before reading any values.
	if !strings.HasPrefix(matchBody, "(") || !containsRelationshipPattern(matchBody) {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if len(splitTopLevelComma(matchBody)) != 1 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// A counter owns one projected item. Decline other projections before
	// reading a traversal that this plan cannot consume.
	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) != 1 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if !patternMapsAreConstant(matchBody) {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matches := e.parseTraversalPatternStructure(ctx, matchBody)
	if matches == nil {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	count, ok, err := e.relationshipCountFromStore(ctx, matches, items[0])
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if !ok.available() {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	column := items[0].column()
	return newCountReductionOutcome(count, column, ok)
}

// tryExecutePipelineSimpleNodeReadPlan applies cardinality and property-index
// operators before row materialization for a single node MATCH. The result is
// still projected by the pipeline, so indexed and scanned inputs share the
// same expression semantics.
func (e *StorageExecutor) tryExecutePipelineSimpleNodeReadPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) pipelineDispatchOutcome {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(ctx, matchBody, "WHERE"); whereIndex >= 0 {
		whereClause = strings.TrimSpace(matchBody[whereIndex+len("WHERE"):])
		matchBody = strings.TrimSpace(matchBody[:whereIndex])
	}
	// A path assignment (p = (n:L)) is the general route's, which binds the
	// path: read as a node, its variable would be "p = (n".
	if !e.isSingleNodePattern(matchBody) || extractPathAssignmentVariable(matchBody) != "" {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if !patternMapsAreConstant(matchBody) || !patternMapsAreConstant(whereClause) || stream.forClause(ctx, &clauses[1]) != nil {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	nodePattern, parseErr := e.parseNodePattern(ctx, matchBody)
	if parseErr != nil {
		return newPipelineDispatchOutcome(nil, true, parseErr)
	}
	if nodePattern.variable == "" {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}

	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) > 1 && items[0].expr == "*" {
		// RETURN *, items: the general projection writes the * out (#883).
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	hint := e.pipelineMatchHint(ctx, clauses[1:])
	boundedSimpleProjection := whereClause == "" && hint.earlyLimit > 0 && pipelineSimpleNodeProjections(items, nodePattern.variable)
	countColumn, filteredCount := pipelineSingleNodeCountProjection(items, nodePattern.variable)
	if whereClause == "" && len(items) == 1 {
		if count, ok, err := e.nodeCountFromStore(ctx, nodePattern, items[0]); ok.available() {
			if err != nil {
				return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
			}
			return newCountReductionOutcome(count, items[0].column(), ok)
		}
	}

	// A WHERE that is an OR of indexed IN lists is read here, as the shortcut
	// for batched key lookups. Its shape admits the statement to this read;
	// which index seeds it is the one node-pattern access decision's
	// (collectPipelineIndexedNodeCandidates, NOR-414), which may prefer the
	// pattern's own unique lookup. Every candidate is held to the whole
	// pattern and to the WHERE.
	if _, orIn, err := e.planPropertyIndexInOrParam(ctx, nodePattern, whereClause, params); err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	} else if orIn {
		candidates, whereApplied, used, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{})
		if err != nil {
			return newPipelineDispatchOutcome(nil, true, err)
		}
		if used {
			var projection *profileRuntimeRecord
			ctx, projection = profileRuntimeInvocation(ctx, "NodeProjection", clauses[1].text, 0)
			rows := make([]pipelineRow, 0, len(candidates))
			for _, node := range candidates {
				if node == nil || !e.pipelineNodeMatchesPattern(ctx, node, nodePattern) {
					continue
				}
				row := pipelineNodeRow(ctx, nodePattern.variable, node)
				if whereApplied || e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
					rows = append(rows, row)
				}
			}
			result, projected := e.pipelineApplyReturn(ctx, rows, clauses[1].text)
			if !projected {
				return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
			}
			projection.invocationRows(len(result.Rows))
			return pipelineDispatchOutcome{state: pipelineDispatchHandledObserved, result: result}
		}
	}
	// The store counters answer only the reads they describe, so this
	// reduction also serves a plain labeled count: it counts the label's
	// stream through the same admission and WHERE test as the rows, and
	// never materializes them. It seeds from the same access decision.
	if filteredCount {
		if outcome := e.tryStreamPipelineFilteredNodeCount(ctx, nodePattern, whereClause, countColumn); outcome.terminal() {
			return outcome
		}
	}
	if !boundedSimpleProjection {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// boundedSimpleProjection requires an empty WHERE, so no seek can
	// have applied one here.
	var projection *profileRuntimeRecord
	ctx, projection = profileRuntimeInvocation(ctx, "NodeProjection", clauses[1].text, 0)
	candidates, _, err := e.collectPipelineInitialNodeCandidates(ctx, nodePattern, whereClause, hint)
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if len(candidates) > hint.earlyLimit {
		candidates = candidates[:hint.earlyLimit]
	}
	result := e.projectVisiblePipelineNodeRead(candidates, items, nodePattern.variable)
	projection.invocationRows(len(result.Rows))
	return pipelineDispatchOutcome{state: pipelineDispatchHandledObserved, result: result}
}

// pipelineNodeRow is the binding row of a single-node pipeline read: the node
// under its variable and every parameter under "$name", as row values
// (parameterRowValues). The row loop and the fused filtered count evaluate
// WHERE on it.
func pipelineNodeRow(ctx context.Context, variable string, node *storage.Node) pipelineRow {
	paramRows := parameterRowValues(ctx)
	row := make(pipelineRow, len(paramRows)+1)
	for name, value := range paramRows {
		row[name] = value
	}
	row[variable] = node
	return row
}

func pipelineSingleNodeCountProjection(items []returnItem, variable string) (string, bool) {
	if len(items) != 1 || variable == "" || !countsRows(items[0], variable) {
		return "", false
	}
	return items[0].column(), true
}

// tryStreamPipelineFilteredNodeCount is the fused physical form of the
// pipeline's MATCH -> filter -> count reduction. It consumes the same
// snapshot-aware label stream as the general row operator but reduces each
// qualifying binding immediately, avoiding a node slice and one map-backed
// pipelineRow per match.
func (e *StorageExecutor) tryStreamPipelineFilteredNodeCount(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	column string,
) pipelineDispatchOutcome {
	if len(nodePattern.labels) == 0 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	store := e.getStorage(ctx)
	reader, ok := store.(storage.ProjectedLabelNodeReader)
	if !ok {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}

	var reduction *profileRuntimeRecord
	ctx, reduction = profileRuntimeInvocation(ctx, "FilteredNodeCount", whereClause, 0)
	predicateRow := pipelineNodeRow(ctx, nodePattern.variable, nil)
	predicatePlan := planRowPredicate(whereClause)
	hasWhere := strings.TrimSpace(whereClause) != ""
	whereFilter := func(node *storage.Node) bool {
		predicateRow[nodePattern.variable] = node
		if !hasWhere {
			return true
		}
		if predicatePlan != nil && predicatePlan.complete {
			return e.evaluateRowPredicatePlan(ctx, predicatePlan, predicateRow)
		}
		return e.evaluateRowPredicate(ctx, whereClause, predicateRow)
	}
	hideSystemNodes := shouldHideSystemNodes(store)
	viewport, hasViewport := TemporalViewportFromContext(ctx)
	checker, canCheckViewport := store.(temporalCurrentNodeChecker)
	var count int64
	countNode := func(node *storage.Node, whereApplied bool) error {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			return nil
		}
		if !mergeNodeHasLabels(node, nodePattern.labels) || !e.nodeMatchesProps(node, nodePattern.properties) || (!whereApplied && !whereFilter(node)) {
			return nil
		}
		if hasViewport && canCheckViewport {
			visible, visibleErr := checker.IsCurrentTemporalNode(node, viewport.AsOf)
			if visibleErr != nil {
				return visibleErr
			}
			if !visible {
				return nil
			}
		}
		count++
		return nil
	}
	// A lookup an index can narrow is counted from the same seed the row
	// read uses (#820); only otherwise is the label streamed.
	candidates, whereApplied, indexed, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{})
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if indexed {
		for _, node := range candidates {
			if err := countNode(node, whereApplied); err != nil {
				return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
			}
		}
		reduction.invocationRows(1)
		return newCountReductionOutcome(count, column, countReductionObservedScan)
	}
	projectedProperties := pipelineNodePredicateProperties(nodePattern.variable, whereClause, nodePattern.properties)
	label := nodeScanLabels(ctx, store, nodePattern.labels)[0]
	visit := queryprofile.EntityVisitor(ctx, profileKindNodeByLabelScan, label, func(node *storage.Node) error {
		return countNode(node, false)
	})
	err = reader.StreamNodesByLabelProjected(ctx, label, projectedProperties, visit)
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
	}
	reduction.invocationRows(1)
	return newCountReductionOutcome(count, column, countReductionObservedScan)
}

// pipelineNodeObservation is how much of a node image one consumer reads. Every
// stored image carries identity and labels; a property body is decoded only
// when the row reads it.
type pipelineNodeObservation uint8

const (
	// pipelineNodeUnobserved reads nothing: the value is not evaluated.
	pipelineNodeUnobserved pipelineNodeObservation = iota
	// pipelineNodeCardinality reads only how many elements a list value has.
	pipelineNodeCardinality
	// pipelineNodeHeader reads identity, labels and existence, which the stored
	// image carries without a property body.
	pipelineNodeHeader
	// pipelineNodeBody reads every stored property body.
	pipelineNodeBody
)

// pipelineNodeRead is what one expression position reads from the value it
// evaluates: the value itself at an observation level, one legible member of it
// (n.key), or, through a list literal or a list function, one of its elements.
type pipelineNodeRead struct {
	level   pipelineNodeObservation
	member  string
	element *pipelineNodeRead
}

// pipelineNodePredicateProperties returns the stored properties a row reads
// from variable through expression and the pattern's inline map. A nil result
// means the row reads every property body: the variable escapes as a value into
// a result, a list, a map or native code, or the expression has a form this
// analysis does not read. An empty non-nil result means the row reads nothing
// beyond the image's identity and labels, so the scan decodes no property body.
//
// The forms narrower than the whole node are the ones that own the read: a
// property access reads that property, an existence test reads the header, and
// a call reads the input image its registration declares (pipelineCallInputOf).
// Every other form keeps the conservative walk of
// forEachSemanticExpressionReference, so an operation this analysis does not
// read keeps the whole demand.
// pipelineCallInput is how much of the row's visible bindings a call may read:
// the promise of the input-image demand its registration declares, or, for a
// name no registration owns, the promise of the owner that implements it.
type pipelineCallInput uint8

const (
	// pipelineCallArguments reads only the values of its own argument
	// expressions: an unregistered call (a native plugin function, an aggregate
	// or an evaluator built-in) receives its arguments' values and never the
	// row, and a registration that declares ArgumentInputImage says the same.
	pipelineCallArguments pipelineCallInput = iota
	// pipelineCallHeader reads identity, labels and existence from its argument
	// values.
	pipelineCallHeader
	// pipelineCallStructural reads an argument through the built-in structural
	// contract the planner owns for the name.
	pipelineCallStructural
	// pipelineCallFullVisible may read every visible row binding completely,
	// including bindings its arguments never name.
	pipelineCallFullVisible
)

// pipelineStructuralRead is what a built-in's canonical structural contract
// reads from the argument a call hands it.
type pipelineStructuralRead uint8

const (
	// pipelineStructuralUnknown owns no structural contract: a structural
	// declaration of the name keeps its arguments' whole values.
	pipelineStructuralUnknown pipelineStructuralRead = iota
	// pipelineStructuralCardinality reads a value's number of elements.
	pipelineStructuralCardinality
	// pipelineStructuralElement reads one element of a list as the caller reads
	// it (head, last).
	pipelineStructuralElement
	// pipelineStructuralElements reads a list's elements at the shallowest level
	// a list building reads them (tail, reverse, collect).
	pipelineStructuralElements
)

// pipelineStructuralReads is the canonical structural contract of the built-in
// functions whose read of an argument is narrower than that argument's value.
// A name here is read through this contract only when its registration declares
// StructuralInputImage (size, tail) or when no scalar registration exists and
// this planner's own implementation owns the name (length, head, last,
// reverse, collect): a lexical name alone narrows nothing, so a custom
// implementation registered under one of these names declares its own demand
// and keeps whatever that demand promises.
var pipelineStructuralReads = map[string]pipelineStructuralRead{
	"size":    pipelineStructuralCardinality,
	"length":  pipelineStructuralCardinality,
	"head":    pipelineStructuralElement,
	"last":    pipelineStructuralElement,
	"tail":    pipelineStructuralElements,
	"reverse": pipelineStructuralElements,
	"collect": pipelineStructuralElements,
}

// pipelineCallInputOf reports how much of the row a call of name may read. A
// registered function states it: FullInputImage is the conservative custom
// default, and the narrowed promises are the reviewed declarations. A name the
// registry doesn't own is an aggregate, an evaluator built-in or a native
// plugin function: none of them receives the row, so they read their arguments'
// values, except the structural built-ins this planner implements itself.
//
// The declaration is read from the live registration when the statement is
// planned; a replacement completed before planning supplies its own demand.
func pipelineCallInputOf(name string) pipelineCallInput {
	if image, registered := cypherfn.InputImageOf(name); registered {
		switch image {
		case cypherfn.ArgumentInputImage, cypherfn.MetadataArgumentInputImage:
			return pipelineCallArguments
		case cypherfn.HeaderInputImage:
			return pipelineCallHeader
		case cypherfn.StructuralInputImage:
			return pipelineCallStructural
		}
		return pipelineCallFullVisible
	}
	if _, owned := pipelineStructuralReads[name]; owned {
		return pipelineCallStructural
	}
	return pipelineCallArguments
}

// pipelineConsumersReadWholeVisibleScope reports whether any consumer
// expression a MATCH's rows are read through calls a function that may read
// every visible row binding completely. Such a registration receives the call
// context, not only its arguments' values, so a binding its arguments never
// name is still part of the image it reads: no variable's demand is complete
// while one of the clauses that read the MATCH's rows holds such a call.
//
// The calls are the semantic scanner's, so a call nested inside a form the
// per-variable analysis cannot read (an unknown function's argument, an
// operator over the variable) is found too. The scan reads retained syntax and
// performs no statement work, so it has no caller context of its own to
// observe.
func pipelineConsumersReadWholeVisibleScope(expressions []string) bool {
	for _, expression := range expressions {
		if strings.TrimSpace(expression) == "" {
			continue
		}
		reads := false
		_ = forEachFunctionCall(context.Background(), expression, func(_, normalized string) error {
			if pipelineCallInputOf(normalized) == pipelineCallFullVisible {
				reads = true
			}
			return nil
		})
		if reads {
			return true
		}
	}
	return false
}

func pipelineNodePredicateProperties(variable, expression string, inline map[string]interface{}) []string {
	properties := make([]string, 0, len(inline)+2)
	seen := make(map[string]struct{}, len(inline)+2)
	for property := range inline {
		seen[property] = struct{}{}
		properties = append(properties, property)
	}
	variable = normalizeProjectionColumnName(variable)
	wholeNode := false
	add := func(property string) {
		if _, exists := seen[property]; property == "" || exists {
			return
		}
		seen[property] = struct{}{}
		properties = append(properties, property)
	}
	conservative := func(text string) {
		forEachSemanticExpressionReference(text, func(base, property string, hasProperty bool, _ int) bool {
			if base != variable {
				return true
			}
			if !hasProperty || property == "" {
				wholeNode = true
				return false
			}
			add(property)
			return true
		})
	}
	// A list's elements are evaluated when the list is built, so the shallowest
	// read of one is its header; only a whole-body read of a list reads every
	// element's body.
	elementLevel := func(level pipelineNodeObservation) pipelineNodeObservation {
		if level == pipelineNodeBody {
			return pipelineNodeBody
		}
		return pipelineNodeHeader
	}
	var observe func(expression string, read pipelineNodeRead)
	observe = func(expression string, read pipelineNodeRead) {
		if wholeNode || (read.element == nil && read.member == "" && read.level == pipelineNodeUnobserved) {
			return
		}
		expression = strings.TrimSpace(expression)
		for {
			inner, enclosed := stripEnclosingExpressionParentheses(expression)
			if !enclosed {
				break
			}
			expression = strings.TrimSpace(inner)
		}
		if expression == "" || expression == "*" {
			return
		}
		// n IS NULL reads whether the value exists, never its properties.
		for _, suffix := range [...]string{" is not null", " is null"} {
			if hasSuffixFoldASCII(expression, suffix) {
				observe(strings.TrimSpace(expression[:len(expression)-len(suffix)]), pipelineNodeRead{level: pipelineNodeHeader})
				return
			}
		}
		if name := simpleSemanticIdentifier(expression); name != "" {
			if name != variable {
				return
			}
			switch {
			case read.element != nil:
				// A stored node is not a list, so a read of one of its
				// elements reads the node as a value.
				wholeNode = true
			case read.member != "":
				add(read.member)
			case read.level == pipelineNodeBody, read.level == pipelineNodeCardinality:
				wholeNode = true
			}
			return
		}
		// n['key'] reads one property. A computed key can name any of them.
		if open := rowSubscriptStart(expression); open > 0 {
			key := strings.TrimSpace(expression[open+1 : len(expression)-1])
			if property := decodeCypherStringLiteral(key); property != key {
				observe(expression[:open], pipelineNodeRead{member: property})
				return
			}
			conservative(expression)
			return
		}
		if base, member, ok := splitPostfixPropertyAccess(expression); ok {
			observe(base, pipelineNodeRead{member: member})
			return
		}
		if isListLiteral(expression) {
			items := splitTopLevelComma(expression[1 : len(expression)-1])
			switch {
			case read.element != nil:
				for _, item := range items {
					observe(item, *read.element)
				}
			case read.member != "":
				// A list has no member to read.
				conservative(expression)
			default:
				element := pipelineNodeRead{level: elementLevel(read.level)}
				for _, item := range items {
					observe(item, element)
				}
			}
			return
		}
		var call functionCallFacts
		if inspectFunctionCall(expression, qualifiedFunctionCall, "", &call) {
			name := strings.ToLower(call.name)
			arguments := splitTopLevelComma(call.inner)
			switch pipelineCallInputOf(name) {
			case pipelineCallFullVisible:
				// The implementation receives the row, not only its
				// arguments: every visible binding keeps its whole image,
				// including the bindings this expression never names.
				wholeNode = true
			case pipelineCallHeader:
				for _, argument := range arguments {
					observe(argument, pipelineNodeRead{level: pipelineNodeHeader})
				}
			case pipelineCallStructural:
				switch pipelineStructuralReads[name] {
				case pipelineStructuralCardinality:
					for _, argument := range arguments {
						observe(argument, pipelineNodeRead{level: pipelineNodeCardinality})
					}
				case pipelineStructuralElement:
					for _, argument := range arguments {
						element := read
						observe(argument, pipelineNodeRead{element: &element})
					}
				case pipelineStructuralElements:
					for _, argument := range arguments {
						switch {
						case read.element != nil:
							observe(argument, *read.element)
						case read.member != "":
							conservative(expression)
						default:
							observe(argument, pipelineNodeRead{level: elementLevel(read.level)})
						}
					}
				default:
					// A structural promise this planner has no contract
					// for keeps the argument values whole.
					for _, argument := range arguments {
						conservative(argument)
					}
				}
			default:
				// The callee owns its arguments' values: a property access
				// reads that property, a bare variable its whole value.
				for _, argument := range arguments {
					conservative(argument)
				}
			}
			return
		}
		conservative(expression)
	}
	observe(expression, pipelineNodeRead{level: pipelineNodeBody})
	if wholeNode {
		return nil
	}
	sort.Strings(properties)
	return properties
}

func projectPipelineSimpleNodeRead(nodes []*storage.Node, items []returnItem, variable string) *ExecuteResult {
	result := &ExecuteResult{
		Columns: make([]string, len(items)),
		Rows:    make([][]interface{}, 0, len(nodes)),
		Stats:   &QueryStats{},
	}
	for index, item := range items {
		result.Columns[index] = item.column()
	}
	prefix := variable + "."
	for _, node := range nodes {
		row := make([]interface{}, len(items))
		for index, item := range items {
			expression := strings.TrimSpace(item.expr)
			if expression == variable {
				row[index] = node
				continue
			}
			property := normalizeProjectionColumnName(strings.TrimSpace(expression[len(prefix):]))
			row[index] = node.Properties[property]
		}
		result.Rows = append(result.Rows, row)
	}
	return result
}

func pipelineSimpleNodeProjections(items []returnItem, variable string) bool {
	if len(items) == 0 || variable == "" {
		return false
	}
	for _, item := range items {
		expression := strings.TrimSpace(item.expr)
		if expression == variable {
			continue
		}
		prefix := variable + "."
		if !strings.HasPrefix(expression, prefix) {
			return false
		}
		property := strings.TrimSpace(expression[len(prefix):])
		if property == "" || strings.ContainsAny(property, ".()[]{}+-*/%^<>=, ") {
			return false
		}
	}
	return true
}
