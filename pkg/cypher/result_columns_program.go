package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
)

var _ resultmetadata.FactsProvider = (*StorageExecutor)(nil)

// StatementFacts exposes the one cached execution projection program. Scope,
// imports, classification and ordered columns are established together. The
// owner's answer is complete or a terminal error; it never declines execution.
func (e *StorageExecutor) StatementFacts(query string, incoming resultmetadata.Scope, cypher25 bool) resultmetadata.Facts {
	return e.StatementFactsWith(query, incoming, cypher25, nil)
}

// StatementFactsWith completes participant-owned signatures during compilation.
// Signature answers are not cached under query text: a participant may change
// its procedure registry independently of this executor's registry generation.
func (e *StorageExecutor) StatementFactsWith(query string, incoming resultmetadata.Scope, cypher25 bool, schema resultmetadata.ParticipantSchema) resultmetadata.Facts {
	if schema == nil {
		program := e.statementProjectionProgram(query, incoming, cypher25)
		if program.complete {
			return program.facts
		}
		return resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: program.reason, Position: program.position, Err: program.err})
	}
	ensureBuiltInProceduresRegistered()
	names := resultmetadata.Unknown()
	incoming.Visit(func(known []string) { names = resultmetadata.Known(known) }, func() { names = resultmetadata.Unknown() })
	compiler := projectionCompilation{executor: e, source: query, cypher25: cypher25, schema: schema}
	program := compiler.root(names, resultmetadata.Known(nil))
	if program.err == nil && program.statement != nil {
		facts, err := resultmetadata.KnownFacts(*program.statement)
		if err == nil {
			return facts
		}
	}
	return resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: program.reason, Position: program.position, Err: program.err})
}

func (e *StorageExecutor) statementProjectionProgram(query string, incoming resultmetadata.Scope, cypher25 bool) *projectedScopeFacts {
	ensureBuiltInProceduresRegistered()
	names := resultmetadata.Unknown()
	incoming.Visit(func(values []string) { names = resultmetadata.Known(values) }, func() { names = resultmetadata.Unknown() })
	return e.projectedStatementScope(query, names, resultmetadata.Known(nil), cypher25)
}

func projectionScope(names resultmetadata.Names) resultmetadata.Scope {
	scope := resultmetadata.UnknownScope()
	names.Visit(func(values []string) { scope = resultmetadata.KnownScope(values) }, func() { scope = resultmetadata.UnknownScope() })
	return scope
}

func projectionColumns(names resultmetadata.Names) resultmetadata.Columns {
	columns := resultmetadata.UnknownColumns()
	names.Visit(func(values []string) { columns = resultmetadata.KnownColumns(values) }, func() { columns = resultmetadata.UnknownColumns() })
	return columns
}

func programFailure(reason resultmetadata.UnresolvedReason, position int, err error) *projectedScopeFacts {
	return &projectedScopeFacts{reason: reason, position: position, err: err}
}

// compileStatementProgram compiles the execution clause operators and their
// scope transfers. WITH and RETURN use the same cached projection plans that
// execute rows; CALL stores its nested program rather than re-inferring exports.
func (e *StorageExecutor) compileStatementProgram(source string, incoming, retained resultmetadata.Names, cypher25 bool) *projectedScopeFacts {
	compiler := projectionCompilation{executor: e, source: source, cypher25: cypher25}
	return compiler.root(incoming, retained)
}

type projectionCompilation struct {
	executor *StorageExecutor
	source   string
	cypher25 bool
	schema   resultmetadata.ParticipantSchema
}

type projectionGraph struct {
	start, end int
	selected   bool
}

func (compiler *projectionCompilation) root(incoming, retained resultmetadata.Names) *projectedScopeFacts {
	start, end, _, ok := queryStatementWindow(compiler.source)
	if !ok {
		return programFailure(resultmetadata.ReasonParserDecline, 0, nil)
	}
	// Privilege administration has an executable command rather than graph
	// clauses. Its canonical parser establishes this unit operation's schema;
	// an unsupported or unresolved query never reaches this known outcome.
	if _, command, err := parseProcedurePrivilegeCommand(compiler.source[start:end]); command {
		if err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, err)
		}
		scope := projectionScope(incoming)
		statement := resultmetadata.Statement{Start: start, End: end, Incoming: scope, Retained: projectionScope(retained), After: scope, Columns: resultmetadata.KnownColumns(nil)}
		statement.Clauses = []resultmetadata.Clause{{Kind: resultmetadata.ClauseWrite, Start: start, End: end, Before: scope, After: scope}}
		return &projectedScopeFacts{after: incoming, outputs: resultmetadata.Known(nil), statement: &statement}
	}
	return compiler.body(readQueryBody(compiler.source, start, end), start, end, incoming, retained, projectionGraph{})
}

// body consumes the shared structure reader's original-source windows. It does
// not split UNIONs, remove USE/FINISH, locate clauses, or rewrite query text.
func (compiler *projectionCompilation) body(body queryBody, start, end int, incoming, retained resultmetadata.Names, graph projectionGraph) *projectedScopeFacts {
	if projectedNameMap(incoming) == nil || projectedNameMap(retained) == nil {
		return programFailure(resultmetadata.ReasonDynamicSchema, start, nil)
	}
	if body.status == queryBodyMixedUnion {
		return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, unionMixedError())
	}
	if body.status != queryBodyRead || len(body.branches) == 0 {
		return programFailure(resultmetadata.ReasonParserDecline, start, nil)
	}
	if len(body.branches) == 1 {
		return compiler.branch(body.branches[0], incoming, retained, graph)
	}
	statement := resultmetadata.Statement{Start: start, End: end, Incoming: projectionScope(incoming), Retained: projectionScope(retained)}
	var columns []string
	for index, branch := range body.branches {
		program := compiler.branch(branch, incoming, retained, graph)
		if program.err != nil || program.statement == nil {
			return program
		}
		statement.Branches = append(statement.Branches, *program.statement)
		var next []string
		program.outputs.Visit(func(names []string) { next = names }, func() { next = nil })
		if next == nil {
			return programFailure(resultmetadata.ReasonDynamicSchema, branch.start, nil)
		}
		if index == 0 {
			columns = next
		} else {
			if _, same := unionColumnOrder(columns, next); !same {
				return programFailure(resultmetadata.ReasonUnsupportedSyntax, branch.start, unionColumnsError(columns, next))
			}
			statement.UnionAll = append(statement.UnionAll, branch.unionAll)
		}
	}
	names := resultmetadata.Known(columns)
	statement.After, statement.Columns = projectionScope(names), projectionColumns(names)
	return &projectedScopeFacts{after: names, outputs: names, statement: &statement}
}

func (compiler *projectionCompilation) branch(branch queryBodyBranch, incoming, retained resultmetadata.Names, graph projectionGraph) *projectedScopeFacts {
	if branch.declined {
		return programFailure(resultmetadata.ReasonParserDecline, branch.start, nil)
	}
	statement := resultmetadata.Statement{Start: branch.start, End: branch.end, Incoming: projectionScope(incoming), Retained: projectionScope(retained)}
	scope, outputs := incoming, resultmetadata.Known(nil)
	usePending := branch.useStart >= 0
	selectGraph := func() {
		statement.Clauses = append(statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseUse, Start: branch.useStart, End: branch.useEnd, Before: projectionScope(scope), After: projectionScope(scope)})
		graph = projectionGraph{start: branch.useStart, end: branch.useEnd, selected: true}
		usePending = false
	}
	for _, clause := range branch.clauses {
		if usePending && branch.useStart < clause.start {
			selectGraph()
		}
		step, transferred, declared, failure := compiler.step(clause, scope, retained, graph)
		if failure != nil {
			return failure
		}
		if step.Projection != nil {
			step.Projection.Standalone = len(branch.clauses) == 1 && clause.kind == pipelineClauseReturn
		}
		statement.Clauses = append(statement.Clauses, step)
		scope, outputs = transferred, declared
	}
	if usePending {
		selectGraph()
	}
	if branch.finished {
		statement.Clauses = append(statement.Clauses, resultmetadata.Clause{Kind: resultmetadata.ClauseFinish, Start: branch.finishStart, End: branch.finishEnd, Before: projectionScope(scope), After: projectionScope(scope)})
		outputs = resultmetadata.Known(nil)
	}
	statement.After, statement.Columns = projectionScope(scope), projectionColumns(outputs)
	return &projectedScopeFacts{after: scope, outputs: outputs, statement: &statement}
}

func (compiler *projectionCompilation) call(clause queryBodyClause, outer resultmetadata.Names, graph projectionGraph) *projectedScopeFacts {
	origin := clause.start
	declaration := readCallSubqueryScope(clause.text)
	if declaration.invalid {
		_, err := callScopeClauseVariables(declaration.text)
		return programFailure(resultmetadata.ReasonInvalidImport, origin, err)
	}
	start, end, ok := callClauseBody(clause)
	if !ok {
		return programFailure(resultmetadata.ReasonParserDecline, origin, nil)
	}
	body := readQueryBody(compiler.source, start, end)
	if body.status != queryBodyRead {
		return compiler.body(body, start, end, outer, resultmetadata.Known(nil), graph)
	}
	outerNames := projectedNameMap(outer)
	if outerNames == nil {
		return programFailure(resultmetadata.ReasonDynamicSchema, origin, nil)
	}
	var outerIdentities []string
	projectionScope(outer).Visit(func(names []string) { outerIdentities = names }, func() { outerIdentities = nil })
	mode := resultmetadata.ImportNone
	var imports []string
	initial := make(map[string]struct{})
	if declaration.scoped {
		mode = resultmetadata.ImportListed
		imports = append([]string{}, declaration.names...)
		if declaration.names == nil {
			mode = resultmetadata.ImportAll
			imports = outerIdentities
		}
	} else {
		// Importing WITH's grammar remains owned by execution's import reader,
		// including its requirement for a following query clause.
		if _, _, err := unscopedCallImports(compiler.source[start:end], func(name string) bool { _, found := outerNames[name]; return found }, false); err != nil {
			return programFailure(resultmetadata.ReasonInvalidImport, origin, err)
		}
		// The canonical import reader sees the first execution clause, whose
		// window excludes USE even for the legacy WITH-before-USE grammar.
		for _, branch := range body.branches {
			if len(branch.clauses) == 0 || branch.clauses[0].kind != pipelineClauseWith {
				continue
			}
			declared, err := callSubqueryImportedVariables("", false, branch.clauses[0].text)
			if err != nil {
				return programFailure(resultmetadata.ReasonInvalidImport, branch.start, err)
			}
			for _, name := range declared {
				if name == "*" {
					imports = append(imports, outerIdentities...)
				} else {
					imports = append(imports, name)
				}
			}
		}
		if len(imports) == 0 {
			// Preserve the existing implicit-import extension through its
			// declaration owner. No row or column position supplies a name.
			text := compiler.source[start:end]
			for _, name := range outerIdentities {
				if isIdentifierReferenced(text, name) {
					initial[name] = struct{}{}
				}
			}
			for _, name := range unscopedBranchLocals(text, outerIdentities) {
				delete(initial, name)
			}
			for _, name := range outerIdentities {
				if _, imported := initial[name]; imported {
					imports = append(imports, name)
				}
			}
		}
	}
	for _, name := range imports {
		if _, exists := outerNames[name]; !exists {
			return programFailure(resultmetadata.ReasonUndefinedVariable, origin, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UndefinedVariable", localization.CypherAdminVariableNotDefined(name)))
		}
		initial[name] = struct{}{}
	}
	retained := resultmetadata.Known(nil)
	if declaration.scoped {
		retained = projectedMapNames(initial)
	}
	program := compiler.body(body, start, end, projectedMapNames(initial), retained, graph)
	if program.err != nil || program.statement == nil {
		return program
	}
	statement := *program.statement
	kind, outputs := resultmetadata.CallUnit, resultmetadata.Known(nil)
	if statementHasReturningOperator(statement) {
		kind, outputs = resultmetadata.CallReturning, program.outputs
	}
	after := addProjectedNames(outer, outputs)
	if mode == resultmetadata.ImportNone && len(imports) == 0 {
		imports = nil
	}
	call := &resultmetadata.Call{Mode: mode, Imports: imports, Retained: projectionScope(retained), Body: projectionColumns(outputs), Kind: kind, After: projectionScope(after), Statement: &statement}
	if statementUsesParticipant(statement) {
		call.Resolution = resultmetadata.ResolvedByParticipant
	}
	return &projectedScopeFacts{after: after, outputs: outputs, statement: &statement, call: call}
}
func statementHasReturningOperator(statement resultmetadata.Statement) bool {
	if len(statement.Branches) > 0 {
		for _, branch := range statement.Branches {
			if statementHasReturningOperator(branch) {
				return true
			}
		}
		return false
	}
	return len(statement.Clauses) > 0 && statement.Clauses[len(statement.Clauses)-1].Kind == resultmetadata.ClauseReturn
}

// compileProjectionStep is the single scope-transfer operation. Its projection
// plans are also consumed by pipelineProjectionScope and row execution.
func (compiler *projectionCompilation) step(clause queryBodyClause, scope, retained resultmetadata.Names, graph projectionGraph) (resultmetadata.Clause, resultmetadata.Names, resultmetadata.Names, *projectedScopeFacts) {
	origin := clause.start
	step := resultmetadata.Clause{Start: clause.start, End: clause.end, Before: projectionScope(scope)}
	outputs := resultmetadata.Known(nil)
	switch clause.kind {
	case pipelineClauseReturn, pipelineClauseWith:
		step.Kind = resultmetadata.ClauseReturn
		text := clause.text
		if clause.kind == pipelineClauseWith {
			step.Kind = resultmetadata.ClauseWith
			projection, _ := splitWithProjection(text)
			text = "RETURN " + pipelineClauseBody(projection, "WITH")
		}
		plan := returnProjectionPlanFor(text)
		if !plan.valid {
			return step, scope, outputs, programFailure(resultmetadata.ReasonParserDecline, origin, nil)
		}
		input := projectedNameMap(scope)
		if input == nil || plan.star && len(input) == 0 {
			return step, scope, outputs, programFailure(resultmetadata.ReasonDynamicSchema, origin, nil)
		}
		if plan.star {
			plan = plan.withStarExpanded(pipelineScopeColumns(input))
		}
		if err := plan.bindInputs(input); err != nil {
			return step, scope, outputs, programFailure(resultmetadata.ReasonUndefinedVariable, origin, err)
		}
		outputs = resultmetadata.Known(plan.columns)
		projection := projectionDescriptor(plan)
		step.Projection = &projection
		if clause.kind == pipelineClauseWith {
			scope = mergeProjectedNames(projectedMapNames(projectionPlanScope(plan, input)), retained)
			outputs = resultmetadata.Known(nil)
		} else {
			scope = outputs
		}
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
		step.Kind = resultmetadata.ClauseWrite
		keyword := "CREATE"
		switch clause.kind {
		case pipelineClauseMatch:
			step.Kind, keyword = resultmetadata.ClauseMatch, "MATCH"
		case pipelineClauseOptionalMatch:
			step.Kind, keyword = resultmetadata.ClauseOptionalMatch, "OPTIONAL MATCH"
		case pipelineClauseMerge:
			keyword = "MERGE"
		}
		bindings := projectedNameMap(scope)
		if bindings == nil {
			return step, scope, outputs, programFailure(resultmetadata.ReasonDynamicSchema, origin, nil)
		}
		addPipelinePatternBindings(context.Background(), compiler.executor, bindings, clause.text, keyword)
		scope = projectedMapNames(bindings)
	case pipelineClauseUnwind:
		step.Kind = resultmetadata.ClauseUnwind
		alias := unwindBindingName(clause.text)
		if alias == "" {
			return step, scope, outputs, programFailure(resultmetadata.ReasonParserDecline, origin, nil)
		}
		scope = addProjectedNames(scope, resultmetadata.Known([]string{alias}))
	case pipelineClauseLet:
		step.Kind = resultmetadata.ClauseLet
		bindings, err := parsePipelineLet(clause.text)
		if err != nil {
			return step, scope, outputs, programFailure(resultmetadata.ReasonUnsupportedSyntax, origin, err)
		}
		for _, binding := range bindings {
			scope = addProjectedNames(scope, resultmetadata.Known([]string{binding.alias}))
		}
	case pipelineClauseCall:
		step.Kind = resultmetadata.ClauseProcedure
		name := extractProcedureName(clause.text)
		if _, found := globalProcedureRegistry.Get(name); !found {
			if !graph.selected {
				return step, scope, outputs, programFailure(resultmetadata.ReasonUnknownProcedure, origin, localizedStatusError("Neo.ClientError.Procedure.ProcedureNotFound", "ProcedureNotFound", localization.CypherCommandRoutingUnknownProcedure(name)))
			}
			step.ParticipantResolved = true
			yield := parseYieldClause(clause.text)
			if yield == nil || yield.yieldAll {
				if compiler.schema == nil {
					return step, scope, outputs, programFailure(resultmetadata.ReasonDynamicSchema, origin, nil)
				}
				obligation := resultmetadata.SchemaObligation{Procedure: name, Start: origin, End: origin + len(clause.text), UseStart: graph.start, UseEnd: graph.end, Incoming: projectionScope(scope)}
				columns, err := compiler.schema(obligation)
				if err != nil {
					return step, scope, outputs, programFailure(resultmetadata.ReasonDynamicSchema, origin, err)
				}
				outputs = resultmetadata.Unknown()
				columns.Visit(func(known []string) { outputs = resultmetadata.Known(known) }, func() { outputs = resultmetadata.Unknown() })
				if projectedNameMap(outputs) == nil {
					return step, scope, outputs, programFailure(resultmetadata.ReasonDynamicSchema, origin, nil)
				}
			} else {
				outputs = projectedProcedureNames(clause.text)
			}
		} else {
			outputs = projectedProcedureNames(clause.text)
		}
		scope = addProjectedNames(scope, outputs)
		step.Projection = &resultmetadata.Projection{Names: outputs}
	case pipelineClauseCallSubquery:
		step.Kind = resultmetadata.ClauseCallSubquery
		program := compiler.call(clause, scope, graph)
		if program.err != nil || program.call == nil {
			return step, scope, outputs, program
		}
		call := *program.call
		call.Optional = clause.optional
		outputs = program.outputs
		scope = addProjectedNames(scope, outputs)
		call.After = projectionScope(scope)
		step.Call = &call
	case pipelineClauseDelete, pipelineClauseSet, pipelineClauseRemove, pipelineClauseForeach:
		step.Kind = resultmetadata.ClauseWrite
	case pipelineClauseFilter:
		step.Kind = resultmetadata.ClauseFilter
	default:
		return step, scope, outputs, programFailure(resultmetadata.ReasonUnsupportedSyntax, origin, nil)
	}
	step.After = projectionScope(scope)
	return step, scope, outputs, nil
}

func projectionDescriptor(plan *returnProjectionPlan) resultmetadata.Projection {
	expressions := make([]string, 0, len(plan.projections))
	for _, item := range plan.projections {
		expressions = append(expressions, item.expr)
	}
	return resultmetadata.Projection{Expressions: expressions, Names: resultmetadata.Known(plan.columns), Distinct: plan.distinct, Modifiers: plan.modifiers}
}

func statementUsesParticipant(statement resultmetadata.Statement) bool {
	for _, clause := range statement.Clauses {
		if clause.ParticipantResolved || clause.Call != nil && clause.Call.Resolution == resultmetadata.ResolvedByParticipant {
			return true
		}
	}
	for _, branch := range statement.Branches {
		if statementUsesParticipant(branch) {
			return true
		}
	}
	return false
}
