package cypher

import (
	"context"
	"errors"
	"slices"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	readViews   map[string]resultmetadata.Scope
	localSyntax map[int][]syntaxLocalBinding
}
type boundEvent struct{ enter, leave []func() }

// A body composes branch outputs, never their binding contexts. Keeping every
// input/retained/identity snapshot on its branch prevents a sibling's imports
// from becoming an admission exception in another branch.
type boundBody struct {
	expressionKind string
	bindings       *semanticBodyScope
	dependencies   *semanticDependencyScope
	call           bool // CALL bodies retain their established undefined-read diagnostics.
	start, end     int
	graph          projectionGraph
	branches       []*boundBranch
	program        *projectedScopeFacts
	outputIDs      map[string]statementBindingID
}

// boundBranchSeed supplies candidate names/identities to the transition owner.
// It is not an admitted branch input.
type boundBranchSeed struct {
	incoming, retained resultmetadata.Names
	ids                map[string]statementBindingID
}

// boundBranchInput is a frozen result of semanticBodyScope entry and reads.
// Candidate inputs are supplied separately and never used as admitted facts.
type boundBranchInput struct {
	frame              *semanticBodyScope
	incoming, retained resultmetadata.Names
	ids                map[string]statementBindingID
}
type boundBranch struct {
	pendingUpdate string
	bindings      *semanticBodyScope
	dependencies  *semanticDependencyScope

	body      *boundBody
	shape     queryBodyBranch
	input     boundBranchInput
	seed      boundBranchSeed
	scope     map[string]struct{}
	ids       map[string]statementBindingID
	outputs   resultmetadata.Names
	columns   resultmetadata.Columns
	graph     projectionGraph
	statement resultmetadata.Statement
}

func (branch *boundBranch) setInput(incoming, retained resultmetadata.Names, identities map[string]statementBindingID) {
	ids := cloneBindingIDs(identities)
	branch.seed = boundBranchSeed{incoming: incoming, retained: retained, ids: ids}
}

// Shared enclosing scope is seeded independently; subsequent projection
// transfers and CALL import rules can only change the selected branch.
func (body *boundBody) setInputs(incoming, retained resultmetadata.Names, identities map[string]statementBindingID) {
	for _, branch := range body.branches {
		branch.setInput(incoming, retained, identities)
	}
}

type boundClause struct {
	procedureArity      *boundProcedureArity
	procedureSpec       *ProcedureSpec
	procedure           *registeredProcedure // the admitted local procedure execution invokes
	fresh               map[string]bool
	branch              *boundBranch
	source              queryBodyClause
	step                resultmetadata.Clause
	before, after       map[string]struct{}
	localProjection     map[string]struct{}
	beforeIDs, afterIDs map[string]statementBindingID
	outputs             resultmetadata.Names
	plan                *returnProjectionPlan
	writtenPlan         *returnProjectionPlan
	projectionSyntax    []cypherantlr.SyntaxNode
	body                *boundBody
	expressionDepth     int
	transparent         bool // an unbound WITH EMBEDDING candidate
}

func (compiler *projectionCompilation) compileBound(ctx context.Context, incoming, retained resultmetadata.Names) *projectedScopeFacts {
	if compiler.cypher25 {
		ctx = context.WithValue(ctx, cypherVersionKey{}, "25")
	}
	if command, supported, err := compiler.executor.prepareCompositeCommand(ctx, compiler.source); supported {
		if err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, 0, err)
		}
		names := resultmetadata.Known(command.columns())
		scope := projectionScope(names)
		statement := resultmetadata.Statement{Start: 0, End: len(compiler.source), Incoming: projectionScope(incoming), Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(command.columns()), Clauses: []resultmetadata.Clause{{Kind: resultmetadata.ClauseWrite, Start: 0, End: len(compiler.source), Before: projectionScope(incoming), After: scope}}}
		return &projectedScopeFacts{after: names, outputs: names, statement: &statement}
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, compiler.source, cypherantlr.StatementEntry, compiler.cypher25, cypherantlr.AcceptedSource)
	if ctx.Err() != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, ctx.Err())
	}
	if err != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, syntaxAdmissionError(ctx, compiler.source, AcceptedSource, err, syntax))
	}
	compiler.syntax = syntax
	compiler.names = quotedVariableNamesFor(ctx, compiler.source)
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
		statement := resultmetadata.Statement{Start: start, End: end, Incoming: scope, Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(nil), Command: &resultmetadata.Command{Text: compiler.source[start:end]}}
		return &projectedScopeFacts{after: incoming, outputs: resultmetadata.Known(nil), statement: &statement}
	}
	// These commands already declare their fixed schema in the typed SHOW
	// owner. EXPLAIN reads that declaration without enumerating users.
	show := query.ChildRule(cypherantlr.CypherParserRULE_showCommand)
	if show.Valid() && !show.ChildRule(cypherantlr.CypherParserRULE_showTail).Valid() &&
		(startsWithKeywords(show.Code(), "SHOW", "USERS") || startsWithKeywords(show.Code(), "SHOW", "CURRENT USER")) {
		names := resultmetadata.Known(showUsersColumns)
		scope := projectionScope(names)
		statement := resultmetadata.Statement{Start: start, End: end, Incoming: projectionScope(incoming), Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(showUsersColumns), Command: &resultmetadata.Command{Text: compiler.source[start:end]}}
		return &projectedScopeFacts{after: names, outputs: names, statement: &statement}
	}
	compilation := &boundCompilation{owner: compiler, events: map[int]*boundEvent{}, product: &boundStatementProgram{syntax: syntax, projections: map[int]*returnProjectionPlan{}, scopes: map[int]resultmetadata.Clause{}, bodies: map[int]*semanticBodyScope{}, effects: map[int]pipelineClauseEffects{}, scalars: map[int]scalarExpressionSyntax{}, calls: map[int]*resultmetadata.Call{}}, locals: map[localIdentityKey]statementBindingID{}, clauses: map[int]queryBodyClause{}, localSyntax: map[int][]syntaxLocalBinding{}}
	compilation.product.clauses = make(map[int]*boundClause)
	compilation.product.expressions = make(map[int]resultmetadata.Scope)
	compilation.product.nested = make(map[int]*boundBody)
	compilation.product.nodes = make(map[int]cypherantlr.SyntaxNode)
	compilation.readViews = make(map[string]resultmetadata.Scope)
	body := compilation.prepareBody(ctx, queryBodyFromSyntax(query), query, start, end)
	compilation.product.root = body
	body.bindings = compiler.bindings
	if compiler.scalarKind != "" {
		body.expressionKind = compiler.scalarKind
		body.bindings = body.bindings.scalarChild(bindingTypes(projectedNameMap(incoming)))
	}
	if body.bindings == nil {
		body.bindings = &semanticBodyScope{entry: bindingTypes(projectedNameMap(incoming)), carried: bindingTypes(projectedNameMap(retained)), retained: bindingTypes(projectedNameMap(retained)), original: bindingTypes(projectedNameMap(retained))}
	}
	body.dependencies = newSemanticDependencies(body.bindings.visible(body.bindings.entry))
	incomingIDs := map[string]statementBindingID{}
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
		incomingIDs[name] = compilation.define(name, -1, -1)
	}
	body.setInputs(incoming, retained, incomingIDs)
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
		for _, use := range branchShape.useNodes() {
			useStart, _ := use.Span()
			firstStart, _ := first.Span()
			lastStart, _ := last.Span()
			if useStart < firstStart || len(branchShape.clauses) == 0 && first.ID() == root.ID() {
				first = use
			}
			if useStart > lastStart || len(branchShape.clauses) == 0 && last.ID() == root.ID() {
				last = use
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
			branch.scope = projectedNameMap(branch.seed.incoming)
			if err := compilation.enterBodyScope(ctx, branch, branchIndex); err != nil {
				compilation.fail(resultmetadata.ReasonInvalidImport, branchShape.start, err)
				return
			}
			// Initial identities are immutable. Each clause's transfer creates
			// new before/after maps rather than modifying this snapshot.
			branch.ids = cloneBindingIDs(branch.input.ids)
			branch.graph = body.graph
			branch.outputs = resultmetadata.Known(nil)
			branch.columns = resultmetadata.KnownColumns(nil)
			branch.statement = resultmetadata.Statement{Start: branchShape.start, End: branchShape.end, Incoming: boundBindingScope(branch.input.incoming, branch.input.ids), Retained: boundBindingScope(branch.input.retained, branch.input.ids)}
			if branch.scope == nil || projectedNameMap(branch.input.retained) == nil {
				compilation.fail(resultmetadata.ReasonDynamicSchema, branchShape.start, nil)
			}
		})
		compilation.event(last).leave = append(compilation.event(last).leave, func() {
			if compilation.failure != nil {
				compilation.current, compilation.clause = previousBranch, previousClause
				return
			}
			branch.freezeInput()
			branch.statement.Entry = branch.input.frame.horizon(branch.input.ids, branch.input.ids, branch.dependencies)
			branch.statement.Exit = branch.bindings.horizon(branch.ids, branch.input.ids, branch.dependencies)
			branch.statement.After = boundBindingScope(projectedMapNames(branch.scope), branch.ids)
			branch.statement.Columns = branch.columns
			compileFragmentHorizons(&branch.statement)
			compilation.current, compilation.clause = previousBranch, previousClause
		})
		for _, use := range branchShape.useNodes() {
			compilation.prepareUse(ctx, branch, use)
		}
		if branchShape.finished {
			compilation.event(branchShape.finishSyntax).enter = append(compilation.event(branchShape.finishSyntax).enter, func() {
				scope := boundBindingScope(projectedMapNames(branch.scope), branch.ids)
				branch.statement.Clauses = append(branch.statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseFinish, Start: branchShape.finishStart, End: branchShape.finishEnd, Before: scope, After: scope, Entry: branch.bindings.horizon(branch.ids, branch.input.ids, branch.dependencies), Exit: branch.bindings.horizon(branch.ids, branch.input.ids, branch.dependencies)})
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
		reference := node.UseReference(nil)
		if reference.Function == "" && strings.EqualFold(reference.Name, "system") {
			if err := systemDatabaseStatementError(ctx, strings.TrimSpace(compilation.owner.source[end:branch.shape.end])); err != nil {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, start, err)
				return
			}
		}
		branch.graph = projectionGraph{start: start, end: end, selected: true, outer: branch.body.graph.outer, outerIDs: branch.body.graph.outerIDs}
		input := copyBindingNames(branch.scope)
		for name := range projectedNameMap(branch.body.graph.outer) {
			input[name] = struct{}{}
		}
		ids := cloneBindingIDs(branch.ids)
		for name, id := range branch.body.graph.outerIDs {
			if _, current := ids[name]; !current {
				ids[name] = id
			}
		}
		compilation.clause = &boundClause{branch: branch, source: queryBodyClause{pipelineClause: pipelineClause{syntax: node}}, before: input, after: input, beforeIDs: ids, afterIDs: ids}
		scope := boundBindingScope(projectedMapNames(branch.scope), branch.ids)
		step := resultmetadata.Clause{Kind: resultmetadata.ClauseUse, Start: start, End: end, Before: scope, After: scope, Routing: boundBindingScope(projectedMapNames(input), ids), Graph: boundGraphSelection(reference), Entry: branch.bindings.horizon(branch.ids, branch.input.ids, branch.dependencies), Exit: branch.bindings.horizon(branch.ids, branch.input.ids, branch.dependencies)}
		branch.statement.Clauses = append(branch.statement.Clauses, step)
		branch.graph.routing, branch.graph.selection = step.Routing, step.Graph
		if parent, ok := node.Parent(); ok && parent.Rule() == cypherantlr.CypherParserRULE_query {
			branch.body.graph = branch.graph
		}
	})
	compilation.event(node).leave = append(compilation.event(node).leave, func() { compilation.clause = prior })
}

func (compilation *boundCompilation) prepareClause(ctx context.Context, branch *boundBranch, source queryBodyClause) {
	compilation.clauses[source.syntax.ID()] = source
	clause := &boundClause{branch: branch, source: source}
	// Decode projection grammar once, before branch entry can inspect a
	// leading importing WITH. Admission and executable wildcard expansion
	// below consume this same retained plan; compatibility text caches do not.
	if source.kind == pipelineClauseWith || source.kind == pipelineClauseReturn {
		clause.writtenPlan = projectionPlanFromSemantics(projectionSemanticsForSyntax(source.syntax), func(expression cypherantlr.SyntaxNode) {
			clause.projectionSyntax = append(clause.projectionSyntax, expression)
		})
	}
	compilation.product.clauses[source.syntax.ID()] = clause
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
		clause.body.call = true
	}
	if source.kind == pipelineClauseForeach {
		clause.body = compilation.prepareBody(ctx, queryBodyFromSyntax(source.syntax), source.syntax, source.start, source.end)
	}
	// Expression subqueries have an isolated projected scope. Their names do
	// not leak out of the expression, but every input read must still bind.
	source.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.ID() == source.syntax.ID() {
			return nil
		}
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
		default:
			return nil
		}
		// Nested clause owners registered their bodies recursively. Header
		// expressions outside a CALL body (transaction batch/parameters) are
		// still owned here and must not disappear with the child-body cut.
		if compilation.product.nested[node.ID()] != nil {
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
		compilation.product.nested[node.ID()] = nested
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist:
			nested.expressionKind = "EXISTS"
		case cypherantlr.CypherParserRULE_countSubquery:
			nested.expressionKind = "COUNT"
		case cypherantlr.CypherParserRULE_collectSubquery:
			nested.expressionKind = "COLLECT"
		}
		initialize := func() {
			scope, ids, _ := compilation.expressionBindings(clause, node)
			nested.setInputs(projectedMapNames(scope), projectedMapNames(scope), ids)
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
	if previous := branch.statement.Clauses; len(previous) > 0 && previous[len(previous)-1].Kind == resultmetadata.ClauseReturn {
		token := strings.Fields(source.text)[0]
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput(token)))
		return
	}
	if err := compilation.admitClauseTransition(clause); err != nil {
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, err)
		return
	}
	if class, _ := clauseAdmissionClass(clause); branch.body.expressionKind != "" && class.updating {
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, &classifiedCypherError{cause: localizedError(expressionBodyUpdatesMessages[branch.body.expressionKind](), nil), code: "Neo.ClientError.Statement.SyntaxError", detail: "InvalidClauseComposition"})
		return
	}
	if source.kind == pipelineClauseMatch || source.kind == pipelineClauseOptionalMatch {
		if err := branch.bindings.admitPatternDeclarations(ctx, source.pipelineClause); err != nil {
			compilation.fail(resultmetadata.ReasonInvalidImport, source.start, err)
			return
		}
	}
	if err := compilation.admitMutationClause(ctx, clause); err != nil {
		compilation.fail(resultmetadata.ReasonInvalidImport, source.start, err)
		return
	}
	// The transition owner decides an execution option's role before any
	// binding-read event observes its spelling as a projection operand.
	if name := branchEmbeddingCandidate(branch.shape.clauses, source); name != "" {
		_, bound := branch.scope[name]
		clause.transparent = !bound
	}
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
		if clause.transparent {
			clause.step.Kind = resultmetadata.ClauseWrite
			return
		}
		clause.step.Kind = resultmetadata.ClauseReturn
		if source.kind == pipelineClauseWith {
			clause.step.Kind = resultmetadata.ClauseWith
		}
		projectionRule := projectionSemanticsForSyntax(source.syntax)
		if err := branch.bindings.admitProjectionItems(projectionRule, source.kind, compilation.owner.names); err != nil {
			compilation.fail(resultmetadata.ReasonInvalidImport, source.start, err)
			return
		}
		plan := clause.writtenPlan
		if plan == nil || !plan.valid {
			compilation.fail(resultmetadata.ReasonParserDecline, source.start, nil)
			return
		}
		if branch.bindings != nil {
			clause.localProjection = projectionPlanScope(plan, bindingNames(branch.bindings.projection(branch.bindings.entry)))
		}
		writtenWildcard := plan.star
		if plan.star {
			current := bindingNames(branch.bindings.projection(branch.bindings.entry))
			if len(current) == 0 && source.kind == pipelineClauseReturn {
				compilation.fail(resultmetadata.ReasonUndefinedVariable, source.start, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "NoVariablesInScope", localization.CypherMatchingReturnStarNoVariables()))
				return
			}
			for name := range current {
				branch.dependencies.readIdentity(name, branch.ids[name])
			}
			plan = plan.withStarExpanded(pipelineScopeColumns(current))
		}
		for _, item := range plan.projections {
			if !item.direct || item.variable != item.alias {
				if err := branch.bindings.admitDeclaration(item.alias, "WITH"); err != nil {
					compilation.fail(resultmetadata.ReasonInvalidImport, source.start, err)
					return
				}
			}

		}
		clause.plan = plan
		clause.outputs = resultmetadata.Known(plan.columns)
		clause.after = projectionPlanScope(plan, clause.before)
		if source.kind == pipelineClauseWith {

			clause.outputs = resultmetadata.Known(nil)
		}
		projection := projectionDescriptor(plan)
		projection.WrittenWildcard = writtenWildcard
		var writtenNames []string
		projectionRule.eachItem(func(item projectionSemanticItem) {
			writtenNames = append(writtenNames, item.writtenName(compilation.owner.names))
		})
		projection.WrittenNames = resultmetadata.Known(writtenNames)
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
		symbol := source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
		form := "UNWIND"
		if source.syntax.Rule() == cypherantlr.CypherParserRULE_forSt {
			form = "FOR"
		}
		if err := branch.bindings.admitDeclaration(syntaxSymbolName(symbol), form); err != nil {
			compilation.fail(resultmetadata.ReasonInvalidImport, source.start, err)
			return
		}
		clause.step.Kind = resultmetadata.ClauseUnwind
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
		compilation.procedure(ctx, clause)
	case pipelineClauseCallSubquery:
		clause.step.Kind = resultmetadata.ClauseCallSubquery
		compilation.callImports(clause)
	case pipelineClauseForeach:
		clause.step.Kind = resultmetadata.ClauseWrite
		inner := copyBindingNames(clause.before)
		inner[syntaxSymbolName(source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol))] = struct{}{}
		identities := cloneBindingIDs(branch.ids)
		iterator := source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
		from, to := iterator.Span()
		identities[syntaxSymbolName(iterator)] = compilation.define(syntaxSymbolName(iterator), from, to)
		clause.body.setInputs(projectedMapNames(inner), resultmetadata.Known(nil), identities)
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

// branchEmbeddingCandidate locates source among its branch's clauses.
func branchEmbeddingCandidate(clauses []queryBodyClause, source queryBodyClause) string {
	for index := range clauses {
		if clauses[index].syntax.ID() == source.syntax.ID() {
			return embeddingCandidate(clauses, index)
		}
	}
	return ""
}

func (compilation *boundCompilation) leaveClause(ctx context.Context, clause *boundClause) {
	// Reads are admitted first, preserving their release diagnostic precedence.
	if err := compilation.admitClauseSyntax(clause); err != nil {
		compilation.fail(resultmetadata.ReasonUnsupportedSyntax, clause.source.start, err)
		return
	}
	// Structural projection rules still run in this constructor, before any
	// clause facts or executable headers can be published.
	if clause.writtenPlan != nil {
		if err := clause.branch.bindings.admitScalarAggregation(clause.writtenPlan, compilation.owner.cypher25); err != nil {
			compilation.fail(resultmetadata.ReasonUnsupportedSyntax, clause.source.start, err)
			return
		}
		if err := validateProjectionAggregatePlan(clause.writtenPlan, clause.branch.bindings.aggregateConstants()); err != nil {
			compilation.fail(resultmetadata.ReasonUnsupportedSyntax, clause.source.start, err)
			return
		}
		projection := projectionSemanticsForSyntax(clause.source.syntax)
		order := projection.body.ChildRule(cypherantlr.CypherParserRULE_orderSt)
		if order.Valid() {
			if err := validateReturnOrderPlan(ctx, clause.writtenPlan, order.Code(), clause.branch.bindings.orderConstants()); err != nil {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, clause.source.start, err)
				return
			}
		}
	}
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
		call.Routing = boundCallRouting(program.statement, boundBindingScope(projectedMapNames(clause.before), clause.beforeIDs))
		call.Kind = resultmetadata.CallUnit
		clause.outputs = resultmetadata.Known(nil)
		if statementHasReturningOperator(*program.statement) {
			call.Kind = resultmetadata.CallReturning
			clause.outputs = program.outputs
		}
		columns := scopeValues(projectionScope(clause.outputs))
		if err := clause.branch.bindings.admitExports(clause.branch.bindings.entry, columns, func(name string) bool {
			input := clause.beforeIDs[name]
			if input == 0 {
				return false
			}
			for _, branch := range clause.body.branches {
				if branch.ids[name] != input {
					return false
				}
			}
			return true
		}); err != nil {
			compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
			return
		}
		call.Body = projectionColumns(clause.outputs)
		for name := range projectedNameMap(clause.outputs) {
			clause.after[name] = struct{}{}
			clause.afterIDs[name] = clause.body.outputIDs[name]
		}
		call.After = boundBindingScope(projectedMapNames(clause.after), clause.afterIDs)
		if statementUsesParticipant(*program.statement) {
			call.Resolution = resultmetadata.ResolvedByParticipant
		}
		compilation.product.calls[clause.source.syntax.ID()] = call
	}
	clause.step.Entry = compilation.product.bodies[clause.source.syntax.ID()].horizon(clause.beforeIDs, clause.branch.input.ids, clause.branch.dependencies)
	compilation.finishBodyClause(ctx, clause)
	for name := range clause.after {
		if clause.afterIDs[name] == 0 {
			clause.afterIDs[name] = clause.beforeIDs[name]
		}
	}
	// Result membership can omit a readable retained binding (RETURN x).
	// Preserve its current value identity for every declared horizon.
	readableIDs := cloneBindingIDs(clause.beforeIDs)
	for name, id := range clause.afterIDs {
		readableIDs[name] = id
	}
	clause.step.Exit = clause.branch.bindings.horizon(readableIDs, clause.branch.input.ids, clause.branch.dependencies)
	clause.step.Before = boundBindingScope(projectedMapNames(clause.before), clause.beforeIDs)
	clause.step.After = boundBindingScope(projectedMapNames(clause.after), clause.afterIDs)
	if clause.step.Kind == resultmetadata.ClauseWith {
		_, clause.step.Projection.ItemsEnd = projectionSemanticsForSyntax(clause.source.syntax).items.Span()
		projected := projectedNameMap(clause.step.Projection.Names)
		for _, name := range scopeValues(clause.branch.statement.Retained) {
			entry, _ := clause.branch.statement.Retained.Binding(name)
			current := clause.afterIDs[name]
			if _, output := projected[name]; !output && current != 0 && current != entry {
				clause.step.Projection.Retention = append(clause.step.Projection.Retention, resultmetadata.RetainedProjection{Name: name, Entry: entry, Aggregate: clause.plan.hasAggregate || clause.plan.distinct})
			}
		}
	}
	compilation.product.scopes[clause.source.syntax.ID()] = clause.step
	clause.branch.statement.Clauses = append(clause.branch.statement.Clauses, clause.step)
	compilation.advanceClauseAdmission(clause)
	clause.branch.scope, clause.branch.outputs = clause.after, clause.outputs
	clause.branch.columns = projectionColumns(clause.outputs)
	if clause.source.kind == pipelineClauseCall {
		yield := yieldFromSyntax(clause.source.syntax)
		if spec := clause.procedureSpec; spec != nil && spec.ResultSchemas != nil && (yield == nil || yield.yieldAll) {
			clause.branch.columns = resultmetadata.KnownColumnAlternatives(spec.ResultSchemas())
		}
	}
	clause.branch.ids = readableIDs
}

func (compilation *boundCompilation) finishBody(body *boundBody) {
	if compilation.failure != nil {
		return
	}
	for _, branch := range body.branches {
		// NornicDB extension: a written terminal RETURN * is accepted when its
		// expanded result has exactly one column, including empty input. The
		// spelling alone is not a rejection; Cypher's one-column
		// COLLECT contract is admitted here once for RUN/EXPLAIN/PROFILE.
		if body.expressionKind == "COLLECT" && !body.call {

			if len(canonicalColumns(branch.columns)) != 1 || branch.shape.finished || len(branch.shape.clauses) == 0 || branch.shape.clauses[len(branch.shape.clauses)-1].kind != pipelineClauseReturn {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, branch.shape.start, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery", localization.CypherSubqueriesCollectSingleColumn(len(canonicalColumns(branch.columns)))))
				return
			}
		}
		if !body.call || branch.shape.finished || len(branch.shape.clauses) == 0 {
			continue
		}
		last := branch.shape.clauses[len(branch.shape.clauses)-1]
		keyword := queryBodyEndings[last.kind]
		if last.kind == pipelineClauseCallSubquery {
			if call := compilation.product.calls[last.syntax.ID()]; call != nil && call.Kind == resultmetadata.CallUnit {
				keyword = ""
			}
		}
		if keyword != "" {
			compilation.fail(resultmetadata.ReasonUnsupportedSyntax, last.start, &classifiedCypherError{cause: localizedError(localization.CypherMatchingQueryCannotConclude(keyword), nil), code: "Neo.ClientError.Statement.SyntaxError", detail: "InvalidClauseComposition"})
			return
		}
	}
	if len(body.branches) == 1 {
		branch := body.branches[0]
		body.program = &projectedScopeFacts{after: projectedMapNames(branch.scope), outputs: branch.outputs, statement: &branch.statement}
		body.outputIDs = cloneBindingIDs(branch.ids)
		return
	}
	// UNION's envelope owns only the joined output. The complete input and
	// retained scopes are on each Statement.Branches entry, never unioned here.
	statement := resultmetadata.Statement{Start: body.start, End: body.end, Incoming: resultmetadata.KnownScope(nil), Retained: resultmetadata.KnownScope(nil)}
	var columns []string
	hasFinish := false
	for _, branch := range body.branches {
		hasFinish = hasFinish || branch.shape.finished
	}
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
				failure := unionColumnsError(columns, next)
				if hasFinish {
					// Preserve release's FINISH/header diagnostic, with both
					// column sets supplied by this construction rather than a
					// framing-time CALL/procedure output inference.
					failure = newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidProjection", "All sub queries in an UNION must have the same return column names")
				}
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, branch.shape.start, failure)
				return
			}
			statement.UnionAll = append(statement.UnionAll, branch.shape.unionAll)
		}
		statement.Branches = append(statement.Branches, branch.statement)
	}
	names := resultmetadata.Known(columns)
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
	statement.After = boundBindingScope(names, body.outputIDs)
	empty := resultmetadata.KnownScope(nil)
	statement.Entry = &resultmetadata.BindingHorizon{Readable: empty, Current: empty, Retained: empty, Weak: empty, Dependencies: empty}
	statement.Exit = &resultmetadata.BindingHorizon{Readable: statement.After, Current: statement.After, Retained: empty, Weak: empty, Dependencies: empty}
	body.program = &projectedScopeFacts{after: names, outputs: names, statement: &statement}
}

func (compilation *boundCompilation) callImports(clause *boundClause) {
	descriptor := scopeClauseFromSyntax(clause.source.syntax)
	parent := clause.branch.bindings
	imports, bindings, err := callBodyScope(descriptor.names, descriptor.scoped, parent.entry, parent, parent.scalar)
	if err != nil {
		compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
		return
	}
	clause.body.expressionKind = clause.branch.body.expressionKind
	clause.body.bindings = bindings
	clause.body.dependencies = newSemanticDependencies(imports)
	clause.body.dependencies.parent = clause.branch.dependencies
	var names []string
	for name := range imports {
		names = append(names, name)
		if descriptor.scoped {
			clause.branch.dependencies.readIdentity(name, clause.branch.ids[name])
		}
	}
	slices.Sort(names)
	clause.body.setInputs(resultmetadata.Known(names), projectedMapNames(bindingNames(bindings.retained)), clause.branch.ids)
	mode := resultmetadata.ImportNone
	if descriptor.scoped {
		mode = resultmetadata.ImportListed
		if descriptor.names == nil {
			mode = resultmetadata.ImportAll
		}
	}
	clause.body.graph = clause.branch.graph
	clause.body.graph.outer = projectedMapNames(clause.before)
	clause.body.graph.outerIDs = cloneBindingIDs(clause.branch.ids)
	clause.step.Call = &resultmetadata.Call{Mode: mode, Imports: names, Retained: boundBindingScope(projectedMapNames(bindingNames(bindings.retained)), clause.branch.ids), Optional: clause.source.optional}
	compilation.scopeFrame(clause.body.start, bindings)
}

func (compilation *boundCompilation) procedure(ctx context.Context, clause *boundClause) {
	clause.step.Kind = resultmetadata.ClauseProcedure
	node := clause.source.syntax
	name := node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
	procedure, found, err := compilation.procedureDeclaration(ctx, clause, name)
	if err != nil {
		compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, err)
		return
	}
	if found {
		admitted := procedure
		clause.procedure = &admitted
		clause.procedureSpec = &admitted.Spec
	}
	yield := yieldFromSyntax(node)
	if err := compilation.admitProcedureInvocation(ctx, clause, procedure, found, yield); err != nil {
		compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
		return
	}
	if found {
		if arguments := procedureArgumentTextsFromSyntax(node); arguments != nil {
			proof, err := constructProcedureArity(procedure.Spec, len(arguments))
			if err != nil {
				compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
				return
			}
			clause.procedureArity = proof
		}
	}
	if err := clause.branch.bindings.admitYieldItems(yield, len(clause.branch.shape.clauses) > 1); err != nil {
		compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
		return
	}
	if !found {
		if clause.branch.graph.selected {
			compilation.fail(resultmetadata.ReasonDynamicSchema, clause.source.start, nil)
		} else {
			compilation.fail(resultmetadata.ReasonUnknownProcedure, clause.source.start, unknownProcedureError(name))
		}
		return
	}
	outputs := projectedProcedureNodeNames(node, procedure.Spec)
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
			for _, local := range expressionLocalSyntax(node) {
				if !local.reuse {
					if err := clause.branch.bindings.admitDeclaration(syntaxSymbolName(local.symbol), "LEXICAL"); err != nil {
						compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
					}
				}
			}
			if boundOperandHandle(node) {
				scope, ids, _ := compilation.expressionBindings(clause, node)
				compilation.product.expressions[node.ID()] = compilation.freezeReadView(scope, ids)
				compilation.product.nodes[node.ID()] = node
			}
		}
		if compilation.failure == nil && clause != nil {
			if clause.source.kind == pipelineClauseLet && node.Rule() == cypherantlr.CypherParserRULE_letItem {
				symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
				if err := clause.branch.bindings.admitDeclaration(syntaxSymbolName(symbol), "LET"); err != nil {
					compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
					return nil
				}
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
			if compilation.failure == nil {
				if err := admitLexicalSyntax(node); err != nil {
					start, _ := node.Span()
					compilation.fail(resultmetadata.ReasonUnsupportedSyntax, start, err)
				}
			}
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
	// Membership was decided above from the admitted identity view. Preserve
	// DELETE's existing target diagnostic instead of replacing it with the
	// generic variable renderer when admission runs before mutation checks.
	if clause := compilation.clause; clause != nil && clause.source.kind == pipelineClauseDelete {
		targets := clause.source.syntax.ChildRule(cypherantlr.CypherParserRULE_expressionChain)
		for index := 0; index < targets.ChildCount(); index++ {
			target := targets.Child(index)
			if target.Rule() != cypherantlr.CypherParserRULE_expression {
				continue
			}
			first, last := target.Span()
			if first <= position && position < last && deleteExpressionRootIdentifier(target.Code()) == name {
				compilation.fail(resultmetadata.ReasonUndefinedVariable, position, deleteUndefinedVariableError(target.Text()))
				return
			}
		}
	}
	compilation.fail(resultmetadata.ReasonUndefinedVariable, position, createUndefinedVariableError(name))
}

func (compilation *boundCompilation) expressionBindings(clause *boundClause, node cypherantlr.SyntaxNode) (map[string]struct{}, map[string]statementBindingID, bool) {
	input, ids := clause.before, clause.beforeIDs
	restricted := false
	switch clause.source.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge, pipelineClauseLet:
		input, ids = clause.after, clause.afterIDs
	}
	if clause.plan != nil {
		switch (projectionSemantics{clause: clause.source.syntax}).readPhase(node) {
		case projectionProjected, projectionOrdered:
			input, ids, restricted = clause.projectionTailBindings(node)
		case projectionWindow:
			input, ids = clause.combinedBindings()
		}
	} else {
		for parent, ok := node.Parent(); ok && parent.ID() != clause.source.syntax.ID(); parent, ok = parent.Parent() {
			switch parent.Rule() {
			case cypherantlr.CypherParserRULE_yieldItems:
				input, ids = clause.after, clause.afterIDs
			case cypherantlr.CypherParserRULE_where:
				owner, direct := parent.Parent()
				if !direct || owner.ID() != clause.source.syntax.ID() {
					continue // A comprehension predicate reads its expression's input.
				}
				if clause.source.kind == pipelineClauseUnwind {
					input, ids = clause.after, clause.afterIDs
				}
			case cypherantlr.CypherParserRULE_orderSt:
				input, ids, restricted = clause.projectionTailBindings(node)
			case cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
				input, ids = clause.combinedBindings()
			}
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

// Commands own their signature and continuation admission separately from
// graph Facts. This syntax adapter asks the same constructor for nested CALL
// composition; it does not infer a command schema or publish partial scopes.
// Unavailable command inputs remain with the command owner; only its body's
// constructor-owned composition diagnostic is observed here.
func validateCommandBodyComposition(ctx context.Context, source string) error {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return syntaxAdmissionError(ctx, source, AcceptedSource, err, syntax)
	}
	var failure error
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure != nil || node.Rule() != cypherantlr.CypherParserRULE_callSubquery {
			return nil
		}
		compiler := projectionCompilation{executor: &StorageExecutor{}, source: node.Text(), cypher25: cypherVersionFromContext(ctx) == "25"}
		program := compiler.root(ctx, resultmetadata.Known(nil), resultmetadata.Known(nil))
		var detail interface{ BoltErrorDetail() string }
		if errors.As(program.err, &detail) && detail.BoltErrorDetail() == "InvalidClauseComposition" {
			failure = program.err
		}
		return nil
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return failure
}
