package cypher

import (
	"context"
	"slices"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// boundCompilation schedules scope transfers on the canonical CST. Binding and
// graph effects observe the same non-pruning traversal. Structure is never read
// again from rewritten fragments, and an effect-cache hit cannot bypass binding.
type boundCompilation struct {
	owner       *projectionCompilation
	events      map[int]*boundEvent
	current     *boundBranch
	clause      *boundClause
	failure     *projectedScopeFacts
	product     *boundStatementProgram
	locals      map[localIdentityKey]statementBindingID
	clauses     map[int]queryBodyClause
	localSyntax map[int][]syntaxLocalBinding
}
type boundEvent struct{ enter, leave []func() }
type boundBody struct {
	start, end             int
	incoming, retained     resultmetadata.Names
	graph                  projectionGraph
	branches               []*boundBranch
	program                *projectedScopeFacts
	incomingIDs, outputIDs map[string]statementBindingID
}
type boundBranch struct {
	body      *boundBody
	shape     queryBodyBranch
	scope     map[string]struct{}
	ids       map[string]statementBindingID
	outputs   resultmetadata.Names
	columns   resultmetadata.Columns
	graph     projectionGraph
	statement resultmetadata.Statement
}
type boundClause struct {
	branch              *boundBranch
	source              queryBodyClause
	step                resultmetadata.Clause
	before, after       map[string]struct{}
	beforeIDs, afterIDs map[string]statementBindingID
	outputs             resultmetadata.Names
	plan                *returnProjectionPlan
	projectionSyntax    []cypherantlr.SyntaxNode
	body                *boundBody
	expressionDepth     int
}

func (compiler *projectionCompilation) compileBound(incoming, retained resultmetadata.Names) *projectedScopeFacts {
	syntax, err := cypherantlr.ParseSyntax(context.Background(), compiler.source, cypherantlr.StatementEntry, compiler.cypher25)
	if err != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, syntaxAdmissionError(compiler.source, err))
	}
	compiler.syntax = syntax
	query := statementSyntaxQuery(syntax)
	if !query.Valid() {
		return programFailure(resultmetadata.ReasonParserDecline, 0, nil)
	}
	start, end := query.Span()
	if _, command, failure := parseProcedurePrivilegeCommand(compiler.source[start:end]); command {
		if failure != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, failure)
		}
		scope := projectionScope(incoming)
		statement := resultmetadata.Statement{Start: start, End: end, Incoming: scope, Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(nil), Clauses: []resultmetadata.Clause{{Kind: resultmetadata.ClauseWrite, Start: start, End: end, Before: scope, After: scope}}}
		return &projectedScopeFacts{after: incoming, outputs: resultmetadata.Known(nil), statement: &statement}
	}
	compilation := &boundCompilation{owner: compiler, events: map[int]*boundEvent{}, product: &boundStatementProgram{syntax: syntax, projections: map[int]*returnProjectionPlan{}, effects: map[int]pipelineClauseEffects{}, scalars: map[int]scalarExpressionSyntax{}}, locals: map[localIdentityKey]statementBindingID{}, clauses: map[int]queryBodyClause{}, localSyntax: map[int][]syntaxLocalBinding{}}
	body := compilation.prepareBody(queryBodyFromSyntax(query), query, start, end)
	body.incoming, body.retained = incoming, retained
	body.incomingIDs = map[string]statementBindingID{}
	input := projectedNameMap(incoming)
	var inputNames []string
	for name := range input {
		inputNames = append(inputNames, name)
	}
	slices.Sort(inputNames)
	for _, name := range inputNames {
		body.incomingIDs[name] = compilation.define(name, -1, -1)
	}
	var effects pipelineClauseEffects
	syntax.Walk(compilation.visitor(), compilation.metadataVisitor(&effects))
	if compilation.failure != nil {
		return compilation.failure
	}
	if body.program == nil {
		return programFailure(resultmetadata.ReasonParserDecline, start, nil)
	}
	pipelineClauseEffectCache.put("\x00"+compiler.source, effects)
	body.program.bound = compilation.product
	return body.program
}

func (compilation *boundCompilation) event(node cypherantlr.SyntaxNode) *boundEvent {
	event := compilation.events[node.ID()]
	if event == nil {
		event = &boundEvent{}
		compilation.events[node.ID()] = event
	}
	return event
}
func (compilation *boundCompilation) fail(reason resultmetadata.UnresolvedReason, position int, err error) {
	if compilation.failure == nil {
		compilation.failure = programFailure(reason, position, err)
	}
}
func (compilation *boundCompilation) prepareBody(shape queryBody, root cypherantlr.SyntaxNode, start, end int) *boundBody {
	body := &boundBody{start: start, end: end}
	if shape.status != queryBodyRead || len(shape.branches) == 0 {
		err := error(nil)
		reason := resultmetadata.ReasonParserDecline
		if shape.status == queryBodyMixedUnion {
			reason = resultmetadata.ReasonUnsupportedSyntax
			err = unionMixedError()
		}
		compilation.event(root).enter = append(compilation.event(root).enter, func() { compilation.fail(reason, start, err) })
		return body
	}
	// Registered first: reverse-order leaves finish branches before their body.
	compilation.event(root).leave = append(compilation.event(root).leave, func() { compilation.finishBody(body) })
	for _, branchShape := range shape.branches {
		branch := &boundBranch{body: body, shape: branchShape}
		body.branches = append(body.branches, branch)
		first, last := root, root
		if len(branchShape.clauses) > 0 {
			first = branchShape.clauses[0].syntax
			last = branchShape.clauses[len(branchShape.clauses)-1].syntax
		}
		if branchShape.useSyntax.Valid() {
			if len(branchShape.clauses) == 0 || branchShape.useStart < branchShape.clauses[0].start {
				first = branchShape.useSyntax
			}
			if len(branchShape.clauses) == 0 || branchShape.useStart > branchShape.clauses[len(branchShape.clauses)-1].start {
				last = branchShape.useSyntax
			}
		}
		if branchShape.finished {
			last = branchShape.finishSyntax
			if len(branchShape.clauses) == 0 && !branchShape.useSyntax.Valid() {
				first = last
			}
		}
		var previousBranch *boundBranch
		var previousClause *boundClause
		compilation.event(first).enter = append(compilation.event(first).enter, func() {
			previousBranch, previousClause = compilation.current, compilation.clause
			compilation.current, compilation.clause = branch, nil
			branch.scope = projectedNameMap(body.incoming)
			branch.ids = cloneBindingIDs(body.incomingIDs)
			branch.graph = body.graph
			branch.outputs = resultmetadata.Known(nil)
			branch.columns = resultmetadata.KnownColumns(nil)
			branch.statement = resultmetadata.Statement{Start: branchShape.start, End: branchShape.end, Incoming: projectionScope(body.incoming), Retained: projectionScope(body.retained)}
			if branch.scope == nil || projectedNameMap(body.retained) == nil {
				compilation.fail(resultmetadata.ReasonDynamicSchema, branchShape.start, nil)
			}
		})
		compilation.event(last).leave = append(compilation.event(last).leave, func() {
			branch.statement.After = projectionScope(projectedMapNames(branch.scope))
			branch.statement.Columns = branch.columns
			compilation.current, compilation.clause = previousBranch, previousClause
		})
		if branchShape.useSyntax.Valid() {
			compilation.prepareUse(branch, branchShape.useSyntax)
		}
		if branchShape.finished {
			compilation.event(branchShape.finishSyntax).enter = append(compilation.event(branchShape.finishSyntax).enter, func() {
				scope := projectionScope(projectedMapNames(branch.scope))
				branch.statement.Clauses = append(branch.statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseFinish, Start: branchShape.finishStart, End: branchShape.finishEnd, Before: scope, After: scope})
				branch.outputs = resultmetadata.Known(nil)
				branch.columns = resultmetadata.KnownColumns(nil)
			})
		}
		for _, clause := range branchShape.clauses {
			compilation.prepareClause(branch, clause)
		}
	}
	return body
}

func (compilation *boundCompilation) prepareUse(branch *boundBranch, node cypherantlr.SyntaxNode) {
	var prior *boundClause
	compilation.event(node).enter = append(compilation.event(node).enter, func() {
		prior = compilation.clause
		start, end := node.Span()
		branch.graph = projectionGraph{start: start, end: end, selected: true, outer: branch.body.graph.outer, outerIDs: branch.body.graph.outerIDs}
		if parent, ok := node.Parent(); ok && parent.Rule() == cypherantlr.CypherParserRULE_query {
			branch.body.graph = branch.graph
		}
		input := copyBindingNames(branch.scope)
		for name := range projectedNameMap(branch.body.graph.outer) {
			input[name] = struct{}{}
		}
		ids := cloneBindingIDs(branch.ids)
		for name, id := range branch.body.graph.outerIDs {
			ids[name] = id
		}
		compilation.clause = &boundClause{branch: branch, source: queryBodyClause{syntax: node}, before: input, after: input, beforeIDs: ids, afterIDs: ids}
		scope := projectionScope(projectedMapNames(branch.scope))
		branch.statement.Clauses = append(branch.statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseUse, Start: start, End: end, Before: scope, After: scope})
	})
	compilation.event(node).leave = append(compilation.event(node).leave, func() { compilation.clause = prior })
}

func (compilation *boundCompilation) prepareClause(branch *boundBranch, source queryBodyClause) {
	compilation.clauses[source.syntax.ID()] = source
	clause := &boundClause{branch: branch, source: source}
	var prior *boundClause
	compilation.event(source.syntax).enter = append(compilation.event(source.syntax).enter, func() {
		prior = compilation.clause
		compilation.clause = clause
		if compilation.failure != nil {
			return
		}
		if source.kind == pipelineClauseReturn && strings.EqualFold(strings.TrimSpace(pipelineClauseBody(source.text, "RETURN")), "distinct") {
			tail := strings.TrimSpace(compilation.owner.source[source.end:])
			if startsWithKeywordFold(tail, "UNION") && !startsWithKeywordFold(strings.TrimSpace(tail[len("UNION"):]), "ALL") {
				// DISTINCT consumes UNION as its operand in Neo4j; it is
				// a variable reference here, rather than a composition boundary.
				compilation.fail(resultmetadata.ReasonUndefinedVariable, source.end, createUndefinedVariableError("UNION"))
				return
			}
		}
		compilation.enterClause(clause)
		compilation.defineClause(clause)
	})
	compilation.event(source.syntax).leave = append(compilation.event(source.syntax).leave, func() {
		if compilation.failure == nil {
			compilation.leaveClause(clause)
		}
		compilation.clause = prior
	})
	if source.kind == pipelineClauseCallSubquery {
		node := source.syntax.ChildRule(cypherantlr.CypherParserRULE_subqueryBody)
		start, end, _ := callClauseBody(source)
		clause.body = compilation.prepareBody(queryBodyFromSyntax(node), node, start, end)
	}
	if source.kind == pipelineClauseForeach {
		nestedShape := queryBodyBranch{start: source.start, end: source.end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
		for i := 0; i < source.syntax.ChildCount(); i++ {
			child := source.syntax.Child(i)
			if child.Rule() == cypherantlr.CypherParserRULE_updatingStatement {
				appendQuerySyntax(&nestedShape, child)
			}
		}
		clause.body = compilation.prepareBody(queryBody{branches: []queryBodyBranch{nestedShape}, status: queryBodyRead}, source.syntax, source.start, source.end)
	}
	// Expression subqueries have an isolated projected scope. Their names do
	// not leak out of the expression, but every input read must still bind.
	if source.kind == pipelineClauseCallSubquery {
		return
	}
	source.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.ID() == source.syntax.ID() {
			return nil
		}
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
		default:
			return nil
		}
		shape, bodyNode, found := queryExpressionBody(node)
		if !found {
			return nil
		}
		// Only the expression bodies owned by this clause. A nested CALL's
		// compiler registers its own expression bodies.
		for parent, ok := node.Parent(); ok && parent.ID() != source.syntax.ID(); parent, ok = parent.Parent() {
			if parent.Rule() == cypherantlr.CypherParserRULE_callSubquery || parent.Rule() == cypherantlr.CypherParserRULE_collectSubquery || parent.Rule() == cypherantlr.CypherParserRULE_countSubquery || parent.Rule() == cypherantlr.CypherParserRULE_existsSubquery || parent.Rule() == cypherantlr.CypherParserRULE_subqueryExist {
				return nil
			}
		}
		start, end := bodyNode.Span()
		nested := compilation.prepareBody(shape, bodyNode, start, end)
		initialize := func() {
			scope, ids, _ := compilation.expressionBindings(clause, node)
			nested.incoming = projectedMapNames(scope)
			nested.incomingIDs = cloneBindingIDs(ids)
			nested.retained = projectedMapNames(scope)
			nested.graph = branch.graph
		}
		event := compilation.event(node)
		if bodyNode.ID() == node.ID() {
			event.enter = append([]func(){initialize}, event.enter...)
		} else {
			event.enter = append(event.enter, initialize)
		}
		return nil
	})
}

func copyBindingNames(scope map[string]struct{}) map[string]struct{} {
	copy := make(map[string]struct{}, len(scope))
	for name := range scope {
		copy[name] = struct{}{}
	}
	return copy
}

func (compilation *boundCompilation) enterClause(clause *boundClause) {
	branch, source := clause.branch, clause.source
	clause.before = copyBindingNames(branch.scope)
	clause.after = copyBindingNames(branch.scope)
	clause.outputs = resultmetadata.Known(nil)
	clause.step = resultmetadata.Clause{Start: source.start, End: source.end, Before: projectionScope(projectedMapNames(clause.before))}
	switch source.kind {
	case pipelineClauseWith, pipelineClauseReturn:
		clause.step.Kind = resultmetadata.ClauseReturn
		if source.kind == pipelineClauseWith {
			clause.step.Kind = resultmetadata.ClauseWith
		}
		plan := projectionPlanFromSyntax(source.syntax, func(expression cypherantlr.SyntaxNode) {
			clause.projectionSyntax = append(clause.projectionSyntax, expression)
		})
		if !plan.valid {
			compilation.fail(resultmetadata.ReasonParserDecline, source.start, nil)
			return
		}
		if plan.star {
			if len(clause.before) == 0 && source.kind == pipelineClauseReturn {
				compilation.fail(resultmetadata.ReasonDynamicSchema, source.start, nil)
				return
			}
			plan = plan.withStarExpanded(pipelineScopeColumns(clause.before))
		}
		clause.plan = plan
		clause.outputs = resultmetadata.Known(plan.columns)
		clause.after = projectionPlanScope(plan, clause.before)
		if source.kind == pipelineClauseWith {
			for name := range projectedNameMap(branch.body.retained) {
				clause.after[name] = struct{}{}
			}
			clause.outputs = resultmetadata.Known(nil)
		}
		projection := projectionDescriptor(plan)
		projection.Standalone = len(branch.shape.clauses) == 1 && source.kind == pipelineClauseReturn
		clause.step.Projection = &projection
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
		clause.step.Kind = resultmetadata.ClauseWrite
		if source.kind == pipelineClauseMatch {
			clause.step.Kind = resultmetadata.ClauseMatch
		}
		if source.kind == pipelineClauseOptionalMatch {
			clause.step.Kind = resultmetadata.ClauseOptionalMatch
		}
		addSyntaxPatternBindings(clause.after, source.syntax)
	case pipelineClauseUnwind:
		clause.step.Kind = resultmetadata.ClauseUnwind
		symbol := source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
		if !symbol.Valid() {
			compilation.fail(resultmetadata.ReasonParserDecline, source.start, nil)
			return
		}
		name := syntaxSymbolName(symbol)
		clause.after[name] = struct{}{}
	case pipelineClauseLet:
		clause.step.Kind = resultmetadata.ClauseLet
	case pipelineClauseFilter:
		clause.step.Kind = resultmetadata.ClauseFilter
	case pipelineClauseCall:
		compilation.procedure(clause)
	case pipelineClauseCallSubquery:
		clause.step.Kind = resultmetadata.ClauseCallSubquery
		compilation.callImports(clause)
	case pipelineClauseForeach:
		clause.step.Kind = resultmetadata.ClauseWrite
		inner := copyBindingNames(clause.before)
		inner[syntaxSymbolName(source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol))] = struct{}{}
		clause.body.incomingIDs = cloneBindingIDs(branch.ids)
		iterator := source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
		from, to := iterator.Span()
		clause.body.incomingIDs[syntaxSymbolName(iterator)] = compilation.define(syntaxSymbolName(iterator), from, to)
		clause.body.incoming = projectedMapNames(inner)
		clause.body.retained = resultmetadata.Known(nil)
		clause.body.graph = branch.graph
	case pipelineClauseDelete, pipelineClauseSet, pipelineClauseRemove:
		clause.step.Kind = resultmetadata.ClauseWrite
	default:
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, nil)
	}
}

func (compilation *boundCompilation) leaveClause(clause *boundClause) {
	if clause.plan != nil {
		compilation.product.projections[clause.source.syntax.ID()] = clause.plan
	}
	if clause.source.kind == pipelineClauseCallSubquery {
		program := clause.body.program
		if program == nil || program.statement == nil {
			compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, nil)
			return
		}
		call := clause.step.Call
		call.Statement = program.statement
		call.Kind = resultmetadata.CallUnit
		clause.outputs = resultmetadata.Known(nil)
		if statementHasReturningOperator(*program.statement) {
			call.Kind = resultmetadata.CallReturning
			clause.outputs = program.outputs
		}
		call.Body = projectionColumns(clause.outputs)
		for name := range projectedNameMap(clause.outputs) {
			clause.after[name] = struct{}{}
			clause.afterIDs[name] = clause.body.outputIDs[name]
		}
		call.After = projectionScope(projectedMapNames(clause.after))
		if statementUsesParticipant(*program.statement) {
			call.Resolution = resultmetadata.ResolvedByParticipant
		}
	}
	clause.step.After = projectionScope(projectedMapNames(clause.after))
	clause.branch.statement.Clauses = append(clause.branch.statement.Clauses, clause.step)
	clause.branch.scope, clause.branch.outputs = clause.after, clause.outputs
	clause.branch.columns = projectionColumns(clause.outputs)
	if clause.source.kind == pipelineClauseCall {
		node := clause.source.syntax
		proc, found := globalProcedureRegistry.Get(node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName())
		yield := yieldFromSyntax(node)
		if found && proc.Spec.ResultSchemas != nil && (yield == nil || yield.yieldAll) {
			clause.branch.columns = resultmetadata.KnownColumnAlternatives(proc.Spec.ResultSchemas())
		}
	}
	clause.branch.ids = clause.afterIDs
}

func (compilation *boundCompilation) finishBody(body *boundBody) {
	if compilation.failure != nil {
		return
	}
	if len(body.branches) == 1 {
		branch := body.branches[0]
		body.program = &projectedScopeFacts{after: projectedMapNames(branch.scope), outputs: branch.outputs, statement: &branch.statement}
		body.outputIDs = cloneBindingIDs(branch.ids)
		return
	}
	statement := resultmetadata.Statement{Start: body.start, End: body.end, Incoming: projectionScope(body.incoming), Retained: projectionScope(body.retained)}
	var columns []string
	for index, branch := range body.branches {
		var next []string
		branch.outputs.Visit(func(known []string) { next = known }, func() { compilation.fail(resultmetadata.ReasonDynamicSchema, branch.shape.start, nil) })
		if compilation.failure != nil {
			return
		}
		if index == 0 {
			columns = next
		} else {
			if _, same := unionColumnOrder(columns, next); !same {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, branch.shape.start, unionColumnsError(columns, next))
				return
			}
			statement.UnionAll = append(statement.UnionAll, branch.shape.unionAll)
		}
		statement.Branches = append(statement.Branches, branch.statement)
	}
	names := resultmetadata.Known(columns)
	statement.After = projectionScope(names)
	statement.Columns = projectionColumns(names)
	body.outputIDs = map[string]statementBindingID{}
	for _, name := range columns {
		body.outputIDs[name] = compilation.define(name, body.start, body.end)
	}
	body.program = &projectedScopeFacts{after: names, outputs: names, statement: &statement}
}

func (compilation *boundCompilation) callImports(clause *boundClause) {
	descriptor := scopeClauseFromSyntax(clause.source.syntax)
	before := clause.before
	var outer []string
	for name := range before {
		outer = append(outer, name)
	}
	slices.Sort(outer)
	mode := resultmetadata.ImportNone
	var imports, implicit []string
	if descriptor.scoped {
		mode = resultmetadata.ImportListed
		imports = append([]string{}, descriptor.names...)
		if descriptor.names == nil {
			mode = resultmetadata.ImportAll
			imports = outer
		}
	} else {
		for _, branch := range clause.body.branches {
			_, _, err := branchSyntaxImports(branch.shape, func(name string) bool { _, found := before[name]; return found }, false)
			if err != nil {
				compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
				return
			}
			for _, name := range importingWithSyntax(branch.shape) {
				if name == "*" {
					imports = append(imports, outer...)
				} else {
					imports = append(imports, name)
				}
			}
		}
		for _, branch := range clause.body.branches {
			var nodes []cypherantlr.SyntaxNode
			for _, clause := range branch.shape.clauses {
				nodes = append(nodes, clause.syntax)
			}
			implicit = append(implicit, implicitSyntaxImports(before, nodes...)...)
		}
		imports = append(imports, implicit...)
	}
	initial := map[string]struct{}{}
	for _, name := range imports {
		if _, exists := before[name]; !exists {
			compilation.fail(resultmetadata.ReasonUndefinedVariable, clause.source.start, createUndefinedVariableError(name))
			return
		}
		initial[name] = struct{}{}
	}
	retained := resultmetadata.Known(implicit)
	if descriptor.scoped {
		retained = projectedMapNames(initial)
	}
	clause.body.incomingIDs = map[string]statementBindingID{}
	for name := range initial {
		clause.body.incomingIDs[name] = clause.branch.ids[name]
	}
	clause.body.incoming = projectedMapNames(initial)
	clause.body.retained = retained
	clause.body.graph = clause.branch.graph
	clause.body.graph.outer = projectedMapNames(before)
	clause.body.graph.outerIDs = cloneBindingIDs(clause.branch.ids)
	clause.step.Call = &resultmetadata.Call{Mode: mode, Imports: imports, Retained: projectionScope(retained), Optional: clause.source.optional}
}

// Implicit imports are an existing extension. The canonical expression slots
// supply candidate dependencies; grammar-owned declarations never import a row
// value under a name that this body declares for itself.
func implicitSyntaxImports(outer map[string]struct{}, bodies ...cypherantlr.SyntaxNode) []string {
	referenced, declared := map[string]bool{}, map[string]bool{}
	for _, body := range bodies {
		body.Walk(func(node cypherantlr.SyntaxNode) func() {
			if variableSyntaxRead(node) {
				selected := false
				for parent, ok := node.Parent(); ok && parent.ID() != body.ID(); parent, ok = parent.Parent() {
					if parent.Rule() == cypherantlr.CypherParserRULE_useClause {
						selected = true
						break
					}
				}
				if !selected {
					name, _ := node.Name()
					if !syntaxLocalAt(node, body, name, outer) {
						referenced[name] = true
					}
				}
			}
			switch node.Rule() {
			case cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail, cypherantlr.CypherParserRULE_patternPart, cypherantlr.CypherParserRULE_projectionItem, cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt, cypherantlr.CypherParserRULE_letItem, cypherantlr.CypherParserRULE_yieldItem:
				if symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol); symbol.Valid() {
					declared[syntaxSymbolName(symbol)] = true
				}
			}
			return nil
		})
	}
	var imports []string
	for name := range outer {
		if referenced[name] && !declared[name] {
			imports = append(imports, name)
		}
	}
	slices.Sort(imports)
	return imports
}

func (compilation *boundCompilation) procedure(clause *boundClause) {
	clause.step.Kind = resultmetadata.ClauseProcedure
	node := clause.source.syntax
	name := node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
	_, found := globalProcedureRegistry.Get(name)
	yield := yieldFromSyntax(node)
	outputs := projectedProcedureNodeNames(node)
	if !found {
		if !clause.branch.graph.selected {
			compilation.fail(resultmetadata.ReasonUnknownProcedure, clause.source.start, unknownProcedureError(name))
			return
		}
		clause.step.ParticipantResolved = true
		if yield == nil || yield.yieldAll {
			if compilation.owner.schema == nil {
				compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, nil)
				return
			}
			graph := clause.branch.graph
			columns, err := compilation.owner.schema(resultmetadata.SchemaObligation{Procedure: name, Start: clause.source.start, End: clause.source.end, UseStart: graph.start, UseEnd: graph.end, Incoming: projectionScope(projectedMapNames(clause.before))})
			if err != nil {
				compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, err)
				return
			}
			columns.Visit(func(names []string) { outputs = resultmetadata.Known(names) }, func() { compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, nil) })
		}
	}
	if projectedNameMap(outputs) == nil {
		compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, nil)
		return
	}
	clause.outputs = outputs
	for name := range projectedNameMap(outputs) {
		clause.after[name] = struct{}{}
	}
	clause.step.Projection = &resultmetadata.Projection{Names: outputs}
}

func (compilation *boundCompilation) visitor() cypherantlr.SyntaxVisitor {
	return func(node cypherantlr.SyntaxNode) func() {
		event := compilation.events[node.ID()]
		if event != nil {
			for _, enter := range event.enter {
				enter()
			}
		}
		clause := compilation.clause
		if compilation.failure == nil && clause != nil {
			compilation.bindSymbol(clause, node)
		}
		return func() {
			if compilation.failure == nil && clause != nil && node.Rule() == cypherantlr.CypherParserRULE_letItem {
				symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
				name := syntaxSymbolName(symbol)
				start, end := symbol.Span()
				clause.after[name] = struct{}{}
				clause.afterIDs[name] = compilation.define(name, start, end)
			}
			if event != nil {
				for i := len(event.leave) - 1; i >= 0; i-- {
					event.leave[i]()
				}
			}
		}
	}
}

func (compilation *boundCompilation) failUndefined(position int, name string) {
	compilation.fail(resultmetadata.ReasonUndefinedVariable, position, createUndefinedVariableError(name))
}

func (compilation *boundCompilation) expressionBindings(clause *boundClause, node cypherantlr.SyntaxNode) (map[string]struct{}, map[string]statementBindingID, bool) {
	input, ids := clause.before, clause.beforeIDs
	restricted := false
	switch clause.source.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge, pipelineClauseLet:
		input, ids = clause.after, clause.afterIDs
	}
	for parent, ok := node.Parent(); ok && parent.ID() != clause.source.syntax.ID(); parent, ok = parent.Parent() {
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_yieldItems:
			input, ids = clause.after, clause.afterIDs
		case cypherantlr.CypherParserRULE_where:
			owner, direct := parent.Parent()
			if !direct || owner.ID() != clause.source.syntax.ID() {
				continue // A comprehension predicate reads its expression's input.
			}
			if clause.source.kind == pipelineClauseWith {
				input, ids, restricted = clause.projectionTailBindings(node)
			} else if clause.source.kind == pipelineClauseUnwind {
				input, ids = clause.after, clause.afterIDs
			}
		case cypherantlr.CypherParserRULE_orderSt:
			input, ids, restricted = clause.projectionTailBindings(node)
		case cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
			input, ids = clause.combinedBindings()
		}
	}
	start, _ := node.Span()
	var ancestors []cypherantlr.SyntaxNode
	for parent, ok := node.Parent(); ok && parent.ID() != clause.source.syntax.ID(); parent, ok = parent.Parent() {
		ancestors = append(ancestors, parent)
	}
	copied := false
	for index := len(ancestors) - 1; index >= 0; index-- {
		parent := ancestors[index]
		bindings, found := compilation.localSyntax[parent.ID()]
		if !found {
			bindings = expressionLocalSyntax(parent)
			compilation.localSyntax[parent.ID()] = bindings
		}
		for _, binding := range bindings {
			if !binding.visible(start) {
				continue
			}
			name := syntaxSymbolName(binding.symbol)
			if binding.reuse {
				if _, visible := input[name]; visible {
					continue
				}
			}
			if !copied {
				input = copyBindingNames(input)
				ids = cloneBindingIDs(ids)
				copied = true
			}
			declaration, _ := binding.symbol.Span()
			input[name] = struct{}{}
			ids[name] = compilation.localBindingIdentity(name, declaration)
		}
	}
	return input, ids, restricted
}
