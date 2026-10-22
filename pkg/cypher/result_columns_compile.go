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
	bindings               *semanticBodyScope
	dependencies           *semanticDependencyScope
	start, end             int
	incoming, retained     resultmetadata.Names
	graph                  projectionGraph
	branches               []*boundBranch
	program                *projectedScopeFacts
	incomingIDs, outputIDs map[string]statementBindingID
}
type boundBranch struct {
	bindings     *semanticBodyScope
	dependencies *semanticDependencyScope

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
	fresh               map[string]bool
	branch              *boundBranch
	source              queryBodyClause
	step                resultmetadata.Clause
	before, after       map[string]struct{}
	localProjection     map[string]struct{}
	beforeIDs, afterIDs map[string]statementBindingID
	outputs             resultmetadata.Names
	plan                *returnProjectionPlan
	projectionSyntax    []cypherantlr.SyntaxNode
	body                *boundBody
	expressionDepth     int
}

func (compiler *projectionCompilation) compileBound(ctx context.Context, incoming, retained resultmetadata.Names) *projectedScopeFacts {
	if compiler.cypher25 {
		ctx = context.WithValue(ctx, cypherVersionKey{}, "25")
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, compiler.source, cypherantlr.StatementEntry, compiler.cypher25)
	if ctx.Err() != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, ctx.Err())
	}
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
	// These commands already declare their fixed schema in the typed SHOW
	// owner. EXPLAIN reads that declaration without enumerating users.
	show := query.ChildRule(cypherantlr.CypherParserRULE_showCommand)
	if show.Valid() && !show.ChildRule(cypherantlr.CypherParserRULE_showTail).Valid() &&
		(startsWithKeywords(show.Code(), "SHOW", "USERS") || startsWithKeywords(show.Code(), "SHOW", "CURRENT USER")) {
		names := resultmetadata.Known(showUsersColumns)
		scope := projectionScope(names)
		statement := resultmetadata.Statement{Start: start, End: end, Incoming: projectionScope(incoming), Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(showUsersColumns), Clauses: []resultmetadata.Clause{{Kind: resultmetadata.ClauseOther, Start: start, End: end, Before: projectionScope(incoming), After: scope}}}
		return &projectedScopeFacts{after: names, outputs: names, statement: &statement}
	}
	compilation := &boundCompilation{owner: compiler, events: map[int]*boundEvent{}, product: &boundStatementProgram{syntax: syntax, projections: map[int]*returnProjectionPlan{}, scopes: map[int]resultmetadata.Clause{}, bodies: map[int]*semanticBodyScope{}, effects: map[int]pipelineClauseEffects{}, scalars: map[int]scalarExpressionSyntax{}}, locals: map[localIdentityKey]statementBindingID{}, clauses: map[int]queryBodyClause{}, localSyntax: map[int][]syntaxLocalBinding{}}
	body := compilation.prepareBody(ctx, queryBodyFromSyntax(query), query, start, end)
	body.incoming, body.retained = incoming, retained
	body.bindings = compiler.bindings
	if body.bindings != nil {
		body.dependencies = newSemanticDependencies(body.bindings.visible(body.bindings.entry))
	}
	body.incomingIDs = map[string]statementBindingID{}
	input := projectedNameMap(incoming)
	if input != nil && body.bindings != nil {
		for name := range body.bindings.visible(body.bindings.entry) {
			input[name] = struct{}{}
		}
	}
	var inputNames []string
	for name := range input {
		inputNames = append(inputNames, name)
	}
	slices.Sort(inputNames)
	for _, name := range inputNames {
		body.incomingIDs[name] = compilation.define(name, -1, -1)
	}
	var effects pipelineClauseEffects
	syntax.Walk(compilation.visitor(ctx), compilation.metadataVisitor(&effects))
	if compilation.failure != nil {
		return compilation.failure
	}
	if body.program == nil {
		return programFailure(resultmetadata.ReasonParserDecline, start, nil)
	}
	if ctx.Err() != nil {
		return programFailure(resultmetadata.ReasonParserDecline, start, ctx.Err())
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
func (compilation *boundCompilation) prepareBody(ctx context.Context, shape queryBody, root cypherantlr.SyntaxNode, start, end int) *boundBody {
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
	for branchIndex, branchShape := range shape.branches {
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
			if err := compilation.enterBodyScope(ctx, branch, branchIndex); err != nil {
				compilation.fail(resultmetadata.ReasonInvalidImport, branchShape.start, err)
				return
			}
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
			compilation.prepareUse(ctx, branch, branchShape.useSyntax)
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
			compilation.prepareClause(ctx, branch, clause)
		}
	}
	return body
}

func (compilation *boundCompilation) prepareUse(ctx context.Context, branch *boundBranch, node cypherantlr.SyntaxNode) {
	var prior *boundClause
	compilation.event(node).enter = append(compilation.event(node).enter, func() {
		prior = compilation.clause
		start, end := node.Span()
		if !node.ChildRule(cypherantlr.CypherParserRULE_functionInvocation).Valid() && strings.EqualFold(node.QualifiedName(), "system") {
			if err := systemDatabaseStatementError(ctx, strings.TrimSpace(compilation.owner.source[end:branch.shape.end])); err != nil {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, start, err)
				return
			}
		}
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
		compilation.clause = &boundClause{branch: branch, source: queryBodyClause{pipelineClause: pipelineClause{syntax: node}}, before: input, after: input, beforeIDs: ids, afterIDs: ids}
		scope := projectionScope(projectedMapNames(branch.scope))
		branch.statement.Clauses = append(branch.statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseUse, Start: start, End: end, Before: scope, After: scope})
	})
	compilation.event(node).leave = append(compilation.event(node).leave, func() { compilation.clause = prior })
}

func (compilation *boundCompilation) prepareClause(ctx context.Context, branch *boundBranch, source queryBodyClause) {
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
		compilation.enterClause(ctx, clause)
		compilation.defineClause(clause)
	})
	compilation.event(source.syntax).leave = append(compilation.event(source.syntax).leave, func() {
		if compilation.failure == nil {
			compilation.leaveClause(ctx, clause)
		}
		compilation.clause = prior
	})
	if source.kind == pipelineClauseCallSubquery {
		node := source.syntax.ChildRule(cypherantlr.CypherParserRULE_subqueryBody)
		start, end, _ := callClauseBody(ctx, source)
		clause.body = compilation.prepareBody(ctx, queryBodyFromSyntax(node), node, start, end)
	}
	if source.kind == pipelineClauseForeach {
		clause.body = compilation.prepareBody(ctx, queryBodyFromSyntax(source.syntax), source.syntax, source.start, source.end)
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
		nested := compilation.prepareBody(ctx, shape, bodyNode, start, end)
		initialize := func() {
			scope, ids, _ := compilation.expressionBindings(clause, node)
			nested.incoming = projectedMapNames(scope)
			nested.incomingIDs = cloneBindingIDs(ids)
			nested.retained = projectedMapNames(scope)
			nested.bindings = branch.bindings.scalarChild(bindingTypes(scope))
			compilation.scopeFrame(nested.start, nested.bindings)
			nested.dependencies = newSemanticDependencies(bindingTypes(scope))
			nested.dependencies.parent = branch.dependencies
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

func (compilation *boundCompilation) enterClause(ctx context.Context, clause *boundClause) {
	branch, source := clause.branch, clause.source
	compilation.beginBodyClause(ctx, clause)
	if body := branch.bindings; body != nil {
		compilation.product.bodies[source.syntax.ID()] = body.clone(body.entry)
	}
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
		if branch.bindings != nil {
			clause.localProjection = projectionPlanScope(plan, bindingNames(branch.bindings.entry))
		}
		if plan.star {
			current := clause.before
			ordinary := current
			if branch.bindings != nil {
				ordinary = bindingNames(branch.bindings.entry)
				current = bindingNames(branch.bindings.projection(branch.bindings.entry))
			}
			if len(ordinary) == 0 && source.kind == pipelineClauseReturn {
				compilation.fail(resultmetadata.ReasonDynamicSchema, source.start, returnStarScopeError(true, len(ordinary)))
				return
			}
			for name := range current {
				branch.dependencies.read(name)
			}
			plan = plan.withStarExpanded(pipelineScopeColumns(current))
		}
		clause.plan = plan
		clause.outputs = resultmetadata.Known(plan.columns)
		clause.after = projectionPlanScope(plan, clause.before)
		if source.kind == pipelineClauseWith {
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
		if branch.bindings != nil {
			clause.body.bindings = branch.bindings.localChild(syntaxSymbolName(iterator))
			clause.body.dependencies = branch.dependencies.nested()
		}
	case pipelineClauseDelete, pipelineClauseSet, pipelineClauseRemove:
		clause.step.Kind = resultmetadata.ClauseWrite
	default:
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, nil)
	}
}

func (compilation *boundCompilation) leaveClause(ctx context.Context, clause *boundClause) {
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
		if clause.body.bindings != nil && clause.body.bindings.bare {
			call.Imports = nil
			for name := range clause.body.dependencies.reads {
				call.Imports = append(call.Imports, name)
			}
			slices.Sort(call.Imports)
		}
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
		if compilation.product.calls == nil {
			compilation.product.calls = make(map[int]*resultmetadata.Call)
		}
		compilation.product.calls[clause.source.syntax.ID()] = call
	}
	compilation.finishBodyClause(ctx, clause)
	clause.step.After = projectionScope(projectedMapNames(clause.after))
	compilation.product.scopes[clause.source.syntax.ID()] = clause.step
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
		identity := body.branches[0].ids[name]
		for _, branch := range body.branches[1:] {
			if branch.ids[name] != identity {
				identity = 0
				break
			}
		}
		if identity == 0 {
			identity = compilation.define(name, body.start, body.end)
		}
		body.outputIDs[name] = identity
	}
	body.program = &projectedScopeFacts{after: names, outputs: names, statement: &statement}
}

func (compilation *boundCompilation) callImports(clause *boundClause) {
	descriptor := scopeClauseFromSyntax(clause.source.syntax)
	local := bindingTypes(clause.before)
	parent := clause.branch.bindings
	if parent != nil {
		local = parent.entry
	}
	imports, bindings, err := callBodyScope(descriptor.names, descriptor.scoped, local, parent, parent != nil && parent.scalar)
	if err != nil {
		compilation.fail(resultmetadata.ReasonUndefinedVariable, clause.source.start, err)
		return
	}
	clause.body.bindings = bindings
	compilation.scopeFrame(clause.body.start, bindings)
	clause.body.dependencies = newSemanticDependencies(imports)
	clause.body.dependencies.parent = clause.branch.dependencies
	initial := bindingNames(bindings.visible(bindings.entry))
	mode := resultmetadata.ImportNone
	var names []string
	if descriptor.scoped {
		mode = resultmetadata.ImportListed
		if descriptor.names == nil {
			mode = resultmetadata.ImportAll
		}
		for name := range imports {
			names = append(names, name)
			clause.branch.dependencies.read(name)
		}
		slices.Sort(names)
	}
	retained := projectedMapNames(bindingNames(bindings.retained))
	clause.body.incomingIDs = map[string]statementBindingID{}
	for name := range imports {
		clause.body.incomingIDs[name] = clause.branch.ids[name]
	}
	clause.body.incoming = projectedMapNames(initial)
	clause.body.retained = retained
	clause.body.graph = clause.branch.graph
	clause.body.graph.outer = projectedMapNames(clause.before)
	clause.body.graph.outerIDs = cloneBindingIDs(clause.branch.ids)
	clause.step.Call = &resultmetadata.Call{Mode: mode, Imports: names, Retained: projectionScope(retained), Optional: clause.source.optional}
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

func (compilation *boundCompilation) visitor(ctx context.Context) cypherantlr.SyntaxVisitor {
	return func(node cypherantlr.SyntaxNode) func() {
		event := compilation.events[node.ID()]
		if event != nil {
			for _, enter := range event.enter {
				enter()
			}
		}
		clause := compilation.clause
		if compilation.failure == nil && clause != nil {
			if clause.source.kind == pipelineClauseLet && node.Rule() == cypherantlr.CypherParserRULE_letItem {
				frame := clause.branch.bindings
				if frame == nil {
					frame = &semanticBodyScope{entry: bindingTypes(clause.after)}
				} else {
					frame = frame.clone(frame.entry)
				}
				start, _ := node.Span()
				compilation.scopeFrame(start, frame)
			}
			compilation.bindSymbol(ctx, clause, node)
		}
		return func() {
			if compilation.failure == nil && clause != nil && node.Rule() == cypherantlr.CypherParserRULE_letItem {
				symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
				name := syntaxSymbolName(symbol)
				start, end := symbol.Span()
				clause.after[name] = struct{}{}
				clause.afterIDs[name] = compilation.define(name, start, end)
				if body := clause.branch.bindings; body != nil {
					body.declare(name, body.entry, clause.branch.dependencies, nil, false)
					body.bindCurrent(name, staticOperand{})
				}
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
