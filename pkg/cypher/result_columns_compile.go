package cypher

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"

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
	// commands holds the declaration of every catalog command clause this
	// compilation composed from retained syntax, by that command's node ID.
	commands map[int]*boundCommand
}
type boundEvent struct{ enter, leave []func() }
type boundBodyRole uint8

const (
	boundOrdinaryBody boundBodyRole = iota
	boundScopedCallBody
	boundImplicitCallBody
	boundScalarBody
)

type boundBody struct {
	// Weak implicit candidates can be replaced by a local declaration. Scalar
	// descendants inherit that provenance; explicit CALL imports are strong.
	weakInputs             map[statementBindingID]struct{}
	legacyInput            resultmetadata.Names
	scalarKind             int
	role                   boundBodyRole
	start, end             int
	incoming, retained     resultmetadata.Names
	graph                  projectionGraph
	branches               []*boundBranch
	program                *projectedScopeFacts
	incomingIDs, outputIDs map[string]statementBindingID
}
type boundBranch struct {
	firstUse    int
	usedInputs  map[string]struct{}
	body        *boundBody
	shape       queryBodyBranch
	scope       map[string]struct{}
	ids         map[string]statementBindingID
	retainedIDs map[string]statementBindingID
	outputs     resultmetadata.Names
	columns     resultmetadata.Columns
	graph       projectionGraph
	statement   resultmetadata.Statement
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
	transparent         bool // an unbound WITH EMBEDDING candidate
}

type commandScopeTransfer uint8

const (
	commandResultScope commandScopeTransfer = iota
	commandRetainInputScope
)

// fixedCommandScopeFacts binds a typed command's declared result schema without
// executing the command. Composite writes, administration creation and fixed
// SHOW results use one metadata constructor; each command declares its effect
// and whether its existing scope contract retains incoming execution names.
func fixedCommandScopeFacts(start, end int, incoming, retained resultmetadata.Names, columns []string, kind resultmetadata.ClauseKind, transfer commandScopeTransfer) *projectedScopeFacts {
	names := resultmetadata.Known(columns)
	before := projectionScope(incoming)
	after, afterNames := before, incoming
	if transfer == commandResultScope {
		after, afterNames = projectionScope(names), names
	}
	statement := resultmetadata.Statement{Start: start, End: end, Incoming: before, Retained: projectionScope(retained), After: after, Columns: resultmetadata.KnownColumns(columns), Clauses: []resultmetadata.Clause{{Kind: kind, Start: start, End: end, Before: before, After: after}}}
	return &projectedScopeFacts{after: afterNames, outputs: names, statement: &statement}
}

func (compiler *projectionCompilation) compileBound(ctx context.Context, incoming, retained resultmetadata.Names) *projectedScopeFacts {
	if compiler.cypher25 {
		ctx = context.WithValue(ctx, cypherVersionKey{}, "25")
	}
	if command, supported, err := compiler.executor.prepareCompositeCommand(ctx, compiler.source); supported {
		if err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, 0, err)
		}
		return fixedCommandScopeFacts(0, len(compiler.source), incoming, retained, command.columns(), resultmetadata.ClauseWrite, commandResultScope)
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, compiler.source, cypherantlr.StatementEntry, compiler.cypher25, cypherantlr.AcceptedSource)
	if ctx.Err() != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, ctx.Err())
	}
	if err != nil {
		return programFailure(resultmetadata.ReasonParserDecline, 0, syntaxAdmissionError(ctx, compiler.source, AcceptedSource, err))
	}
	compiler.syntax = syntax
	query := statementSyntaxQuery(syntax)
	if !query.Valid() {
		return programFailure(resultmetadata.ReasonParserDecline, 0, nil)
	}
	start, end := query.Span()
	if _, command, failure := parseProcedurePrivilegeCommand(ctx, compiler.source[start:end]); command {
		if failure != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, failure)
		}
		return fixedCommandScopeFacts(start, end, incoming, retained, nil, resultmetadata.ClauseWrite, commandRetainInputScope)
	}
	// A SHOW command declares its result schema in the typed owner that builds
	// the listing: the kind it names selects rows, never columns, so EXPLAIN and
	// PROFILE read that declaration without running the listing. A retained YIELD
	// / WHERE / RETURN tail is the statement's own projection over those columns
	// and composes with them as any query composes with its input.
	if show := query.ChildRule(cypherantlr.CypherParserRULE_showCommand); show.Valid() {
		if compiled, handled := compiler.compileShowCommand(ctx, syntax, show, start, end, incoming, retained); handled {
			return compiled
		}
	}
	// A database creation command declares an empty result schema in its execute
	// owner: as in Neo4j, it returns no columns and no records. The command's
	// native syntax has no graph body to compose, so EXPLAIN reads that
	// declaration instead.
	if administration := query.ChildRule(cypherantlr.CypherParserRULE_administrationCommand); administration.Valid() &&
		(startsWithKeywords(ctx, administration.Code(), "CREATE", "DATABASE") || isCreateOrReplaceDatabaseQuery(ctx, administration.Code())) {
		return fixedCommandScopeFacts(start, end, incoming, retained, nil, resultmetadata.ClauseWrite, commandResultScope)
	}
	// Native schema DDL (CREATE / DROP CONSTRAINT and INDEX) declares an empty
	// result schema in its own execute owner: as in Neo4j it returns no columns
	// and no records. Its native syntax has no graph body to compose, so EXPLAIN
	// and PROFILE read that declaration instead of projecting graph variables.
	if schema := query.ChildRule(cypherantlr.CypherParserRULE_schemaCommand); schema.Valid() {
		if isCreateConstraintStatement(ctx, schema.Code()) {
			if _, err := compiler.executor.canonicalConstraintRequirement(ctx, schema.Code()); err != nil {
				return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, err)
			}
		}
		return fixedCommandScopeFacts(start, end, incoming, retained, nil, resultmetadata.ClauseWrite, commandResultScope)
	}
	compilation := newBoundCompilation(compiler, syntax)
	body := compilation.prepareBody(ctx, queryBodyFromSyntax(query), query, start, end)
	return compilation.completeBody(ctx, body, syntax, incoming, retained)
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
			branch.firstUse = len(compilation.product.uses)
			branch.scope = projectedNameMap(body.incoming)
			branch.ids = map[string]statementBindingID{}
			branch.retainedIDs = map[string]statementBindingID{}
			for name := range branch.scope {
				branch.ids[name] = body.incomingIDs[name]
			}
			for name := range projectedNameMap(body.retained) {
				branch.retainedIDs[name] = body.incomingIDs[name]
			}
			if body.role == boundImplicitCallBody {
				// A legacy importing WITH selects ordinary input members per branch.
				// Other outer inputs are readable candidates until an actual binding
				// use proves the implicit dependency.
				branch.scope = map[string]struct{}{}
				branch.ids = map[string]statementBindingID{}
				for _, name := range importingWithSyntax(branch.shape) {
					if name == "*" {
						for name := range projectedNameMap(body.legacyInput) {
							branch.scope[name] = struct{}{}
							branch.ids[name] = body.incomingIDs[name]
						}
						branch.retainedIDs = map[string]statementBindingID{}
					} else if id := body.incomingIDs[name]; id != 0 {
						branch.scope[name] = struct{}{}
						branch.ids[name] = id
						delete(branch.retainedIDs, name)
					}
				}
			}
			branch.graph = body.graph
			branch.outputs = resultmetadata.Known(nil)
			branch.columns = resultmetadata.KnownColumns(nil)
			branch.statement = resultmetadata.Statement{Start: branchShape.start, End: branchShape.end, Incoming: projectionScope(projectedMapNames(branch.scope)), Retained: projectionScope(body.retained)}
			if branch.scope == nil || projectedNameMap(body.retained) == nil {
				compilation.fail(resultmetadata.ReasonDynamicSchema, branchShape.start, nil)
			}
		})
		compilation.event(last).leave = append(compilation.event(last).leave, func() {
			if body.role == boundImplicitCallBody {
				branch.usedInputs = compilation.usedInputs(body, branch.firstUse)
				retained := copyBindingNames(branch.usedInputs)
				for _, name := range importingWithSyntax(branch.shape) {
					if name == "*" {
						retained = map[string]struct{}{}
						break
					}
					delete(retained, name)
				}
				branch.statement.Retained = projectionScope(projectedMapNames(retained))
			}
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
		reference := node.UseReference(nil)
		if reference.Function == "" && strings.EqualFold(reference.Name, "system") {
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
		compilation.clause = &boundClause{branch: branch, source: queryBodyClause{syntax: node}, before: input, after: input, beforeIDs: ids, afterIDs: ids}
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
			compilation.leaveClause(clause)
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
		case cypherantlr.CypherParserRULE_subqueryExist, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
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
			if parent.Rule() == cypherantlr.CypherParserRULE_callSubquery || parent.Rule() == cypherantlr.CypherParserRULE_collectSubquery || parent.Rule() == cypherantlr.CypherParserRULE_countSubquery || parent.Rule() == cypherantlr.CypherParserRULE_subqueryExist {
				return nil
			}
		}
		start, end := bodyNode.Span()
		nested := compilation.prepareBody(ctx, shape, bodyNode, start, end)
		initialize := func() {
			readable, ids, _ := compilation.expressionBindings(clause, node)
			nested.role = boundScalarBody
			nested.weakInputs = branch.body.weakInputs
			nested.scalarKind = node.Rule()
			nested.incoming = resultmetadata.Known(nil)
			nested.incomingIDs = cloneBindingIDs(ids)
			nested.retained = projectedMapNames(readable)
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
	clause.before = copyBindingNames(branch.scope)
	clause.after = copyBindingNames(branch.scope)
	clause.beforeIDs, clause.afterIDs = branch.ids, branch.ids
	compilation.promoteClauseInputs(ctx, clause)
	clause.outputs = resultmetadata.Known(nil)
	clause.step = resultmetadata.Clause{Start: source.start, End: source.end, Before: projectionScope(projectedMapNames(clause.before))}
	if command, declared := compilation.commands[source.syntax.ID()]; declared {
		compilation.enterCommand(clause, command)
		return
	}
	switch source.kind {
	case pipelineClauseWith, pipelineClauseReturn:
		if name := branchEmbeddingCandidate(branch.shape.clauses, source); name != "" {
			if _, bound := clause.before[name]; !bound {
				// The same decision as semantic validation and the pipeline
				// frame: an unbound candidate is the execution option, and
				// the scope passes through unchanged.
				clause.transparent = true
				clause.step.Kind = resultmetadata.ClauseWrite
				return
			}
		}
		clause.step.Kind = resultmetadata.ClauseReturn
		if source.kind == pipelineClauseWith {
			clause.step.Kind = resultmetadata.ClauseWith
		}
		plan := projectionPlanFromSyntax(source.syntax, func(expression cypherantlr.SyntaxNode) {
			clause.projectionSyntax = append(clause.projectionSyntax, expression)
		})
		if !plan.valid {
			compilation.fail(resultmetadata.ReasonParserDecline, source.start, plan.err)
			return
		}
		// Binding uses canonical identifiers; an unaliased expression's public
		// name retains the client's spelling, including quoted occurrences.
		var labels []string
		if names := quotedVariableNamesFor(ctx, compilation.owner.source); names != nil {
			projections := plan.projections
			if plan.star {
				projections = plan.starProjections
			}
			labels = make([]string, len(projections))
			for index, projection := range projections {
				labels[index] = projection.alias
				expression := clause.projectionSyntax[index]
				item, _ := expression.Parent()
				_, aliased := item.DirectToken(cypherantlr.CypherLexerAS)
				_, variable, grouped := projectionVariableFromSyntax(expression)
				if aliased || variable && !grouped {
					continue
				}
				start, end := expression.Span()
				original, mapped := names.OriginalText(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExactBoundaries, nil)
				if !mapped {
					compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, unresolvedStatementError())
					return
				}
				labels[index] = strings.TrimSpace(original)
			}
		}
		if err := validateDuplicateProjectionNames(plan, labels); err != nil {
			compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, err)
			return
		}
		if source.kind == pipelineClauseReturn && branch.body.scalarKind == cypherantlr.CypherParserRULE_collectSubquery {
			if err := collectProjectionError(len(plan.projections), plan.star, declaredCollectProjection); err != nil {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, err)
				return
			}
		}
		if plan.star {
			if len(clause.before) == 0 && source.kind == pipelineClauseReturn {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, source.start, returnStarWithoutVariablesError())
				return
			}
			columns := pipelineScopeColumns(clause.before)
			star, _ := source.syntax.ChildRule(cypherantlr.CypherParserRULE_projectionBody).ChildRule(cypherantlr.CypherParserRULE_projectionItems).DirectToken(cypherantlr.CypherLexerMULT)
			from, to := star.Span()
			for _, name := range columns {
				compilation.product.uses = append(compilation.product.uses, statementBindingUse{binding: branch.ids[name], name: name, start: from, end: to, clauseStart: source.start, clauseEnd: source.end})
			}
			plan = plan.withStarExpanded(columns)
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
	case pipelineClauseLoadCSV:
		// LOAD CSV binds one record variable per row of the resource it names;
		// the resource expression reads only names the clause already has.
		clause.step.Kind = resultmetadata.ClauseLoadCSV
		symbol := source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)
		if !symbol.Valid() {
			compilation.fail(resultmetadata.ReasonParserDecline, source.start, nil)
			return
		}
		clause.after[syntaxSymbolName(symbol)] = struct{}{}
	case pipelineClauseLet:
		clause.step.Kind = resultmetadata.ClauseLet
	case pipelineClauseFilter:
		clause.step.Kind = resultmetadata.ClauseFilter
	case pipelineClauseCall:
		compilation.procedure(clause)
	case pipelineClauseCallSubquery:
		clause.step.Kind = resultmetadata.ClauseCallSubquery
		compilation.callImports(ctx, clause)
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

// branchEmbeddingCandidate locates source among its branch's clauses.
func branchEmbeddingCandidate(clauses []queryBodyClause, source queryBodyClause) string {
	for index := range clauses {
		if clauses[index].syntax.ID() == source.syntax.ID() {
			return embeddingCandidate(clauses, index)
		}
	}
	return ""
}

func (compilation *boundCompilation) leaveClause(clause *boundClause) {
	if _, declared := compilation.commands[clause.source.syntax.ID()]; declared {
		// A command clause completes on entry (enterCommand): the clauses it
		// composes with are its own descendants, so their leaves have run long
		// before its own and must not be undone or reordered here.
		return
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
		if compilation.product.calls == nil {
			compilation.product.calls = map[int]boundCallProgram{}
		}
		reserved := clause.before
		if clause.branch.body.role != boundImplicitCallBody {
			reserved, _ = clause.readableBindings(reserved, clause.beforeIDs)
		}
		compilation.product.calls[clause.source.syntax.ID()] = boundCallProgram{call: call, reserved: projectedMapNames(reserved)}
		call.Statement = program.statement
		if clause.body.role == boundImplicitCallBody {
			inputs := map[string]struct{}{}
			for _, branch := range clause.body.branches {
				for name := range branch.usedInputs {
					inputs[name] = struct{}{}
				}
			}
			call.Imports = pipelineScopeColumns(inputs)
			retained := map[string]struct{}{}
			for _, branch := range clause.body.branches {
				branch.statement.Retained.Visit(func(names []string) {
					for _, name := range names {
						retained[name] = struct{}{}
					}
				}, func() {})
			}
			call.Retained = projectionScope(projectedMapNames(retained))
		}
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
	// The compiled columns of every clause are the one declared result schema a
	// runtime consumer may read: CALL's empty-input metadata, for instance, is
	// this clause's own header rather than a second compile of the CALL text.
	compilation.product.columns[clause.source.syntax.ID()] = clause.branch.columns
	clause.branch.ids = clause.afterIDs
	for name := range clause.branch.retainedIDs {
		if id, projected := clause.afterIDs[name]; projected {
			clause.branch.retainedIDs[name] = id
		}
	}
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

func (compilation *boundCompilation) callImports(ctx context.Context, clause *boundClause) {
	descriptor := scopeClauseFromSyntax(clause.source.syntax)
	before, beforeIDs := clause.readableBindings(clause.before, clause.branch.ids)
	mode := resultmetadata.ImportNone
	var imports []string
	if descriptor.scoped {
		mode = resultmetadata.ImportListed
		imports = append([]string{}, descriptor.names...)
		if descriptor.names == nil {
			mode = resultmetadata.ImportAll
			// Imports are execution bindings, including generated selectors.
			// Public wildcard column filtering must not remove those bindings.
			imports = slices.Sorted(maps.Keys(clause.importableBindings()))
		}
	} else {
		shape := queryBodyFromSyntax(clause.source.syntax.ChildRule(cypherantlr.CypherParserRULE_subqueryBody))
		if err := compilation.owner.executor.validateUnscopedImportForms(ctx, shape, func(name string) bool { _, found := before[name]; return found }); err != nil {
			compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
			return
		}
		for _, branch := range clause.body.branches {
			_, _, err := branchSyntaxImports(ctx, branch.shape, func(name string) bool { _, found := before[name]; return found }, false)
			if err != nil {
				compilation.fail(resultmetadata.ReasonInvalidImport, clause.source.start, err)
				return
			}
			for _, name := range importingWithSyntax(branch.shape) {
				if name == "*" {
					imports = append(imports, pipelineScopeColumns(clause.before)...)
				} else {
					imports = append(imports, name)
					compilation.promoteInput(clause, name, beforeIDs[name])
				}
			}
		}
	}
	initial := map[string]struct{}{}
	for _, name := range imports {
		if _, exists := before[name]; !exists {
			compilation.fail(resultmetadata.ReasonUndefinedVariable, clause.source.start, createUndefinedVariableError(name))
			return
		}
		initial[name] = struct{}{}
	}
	retained := resultmetadata.Known(nil)
	if descriptor.scoped {
		retained = projectedMapNames(initial)
	}
	clause.body.incomingIDs = map[string]statementBindingID{}
	for name := range initial {
		clause.body.incomingIDs[name] = beforeIDs[name]
	}
	clause.body.incoming = projectedMapNames(initial)
	if descriptor.scoped {
		clause.body.incoming = resultmetadata.Known(nil)
		clause.body.role = boundScopedCallBody
	}
	if !descriptor.scoped {
		clause.body.role = boundImplicitCallBody
		clause.body.incomingIDs = map[string]statementBindingID{}
		legacyInput := clause.before
		if clause.branch.body.role == boundImplicitCallBody {
			legacyInput = clause.importableBindings()
		}
		clause.body.legacyInput = projectedMapNames(legacyInput)
		clause.body.weakInputs = map[statementBindingID]struct{}{}
		candidates := copyBindingNames(before)
		for name := range initial {
			candidates[name] = struct{}{}
		}
		for name := range candidates {
			id := beforeIDs[name]
			clause.body.incomingIDs[name] = id
			if _, explicit := initial[name]; !explicit {
				clause.body.weakInputs[id] = struct{}{}
			}
		}
		retained = projectedMapNames(candidates)
	}
	clause.body.retained = retained
	clause.body.graph = clause.branch.graph
	clause.body.graph.outer = projectedMapNames(before)
	clause.body.graph.outerIDs = cloneBindingIDs(beforeIDs)
	if descriptor.scoped {
		star, all := clause.source.syntax.DirectToken(cypherantlr.CypherLexerMULT)
		for _, name := range imports {
			from, to := 0, 0
			if all {
				from, to = star.Span()
			}
			if !all {
				for index := 0; index < clause.source.syntax.ChildCount(); index++ {
					node := clause.source.syntax.Child(index)
					if node.Rule() == cypherantlr.CypherParserRULE_symbol && syntaxSymbolName(node) == name {
						from, to = node.Span()
						break
					}
				}
			}
			id := beforeIDs[name]
			compilation.product.uses = append(compilation.product.uses, statementBindingUse{binding: id, name: name, start: from, end: to, clauseStart: clause.source.start, clauseEnd: clause.source.end})
			if clause.branch.body.role == boundImplicitCallBody {
				compilation.promoteInput(clause, name, id)
			}
		}
	}
	clause.step.Call = &resultmetadata.Call{Mode: mode, Imports: imports, Retained: projectionScope(retained), Optional: clause.source.optional}
}

func (compilation *boundCompilation) procedure(clause *boundClause) {
	clause.step.Kind = resultmetadata.ClauseProcedure
	node := clause.source.syntax
	name := node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
	procedure, found := globalProcedureRegistry.Get(name)
	if found {
		if arguments := procedureArgumentTextsFromSyntax(node); arguments != nil {
			if err := validateProcedureArgumentCount(procedure.Spec, len(arguments)); err != nil {
				compilation.fail(resultmetadata.ReasonUnsupportedSyntax, clause.source.start, err)
				return
			}
		}
	}
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
			compilation.bindSymbol(ctx, clause, node)
		}
		if event == nil && (clause == nil || node.Rule() != cypherantlr.CypherParserRULE_letItem) {
			return nil
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

// readableBindings adds CALL's retained inputs without making them wildcard
// members. Current declarations override retained identities in the same frame.
func (clause *boundClause) readableBindings(names map[string]struct{}, ids map[string]statementBindingID) (map[string]struct{}, map[string]statementBindingID) {
	copied := false
	for name, id := range clause.branch.retainedIDs {
		if _, current := names[name]; current && ids[name] != 0 {
			continue
		}
		if !copied {
			names, ids = copyBindingNames(names), cloneBindingIDs(ids)
			copied = true
		}
		names[name], ids[name] = struct{}{}, id
	}
	return names, ids
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
			if _, command := compilation.commands[clause.source.syntax.ID()]; command {
				// A catalog command's own tail predicates filter the columns it
				// declares and the aliases a YIELD adds: that is the command
				// clause's after-scope. A predicate of a body nested in the tail
				// binds in that body's clause, which has entered by then.
				input, ids = clause.after, clause.afterIDs
				break
			}
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
	input, ids = clause.readableBindings(input, ids)
	start, _ := node.Span()
	// Ordinary expression ancestry fits on the stack. Deeper expressions
	// still grow normally, without imposing a grammar depth limit.
	var nearby [16]cypherantlr.SyntaxNode
	ancestors := nearby[:0]
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

// completeBody binds input identities and publishes one completed statement product.
// Graph and declared-command bodies share the same completion and cancellation rules.
func (compilation *boundCompilation) completeBody(ctx context.Context, body *boundBody, syntax cypherantlr.StatementSyntax, incoming, retained resultmetadata.Names) *projectedScopeFacts {
	body.role = compilation.owner.role
	if body.role == boundScalarBody || body.role == boundScopedCallBody {
		retained = addProjectedNames(incoming, retained)
		incoming = resultmetadata.Known(nil)
	}
	body.incoming, body.retained = incoming, retained
	body.incomingIDs = map[string]statementBindingID{}
	input := projectedNameMap(addProjectedNames(incoming, retained))
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
		return programFailure(resultmetadata.ReasonParserDecline, body.start, nil)
	}
	if ctx.Err() != nil {
		return programFailure(resultmetadata.ReasonParserDecline, body.start, ctx.Err())
	}
	pipelineClauseEffectCache.put("\x00"+compilation.owner.source, effects)
	body.program.bound = compilation.product
	return body.program
}
