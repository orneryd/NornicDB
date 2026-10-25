package cypher

import (
	"context"
	"fmt"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// boundTypeValues enriches an already admitted program. The only mutable
// namespace here is BindingID -> operand. Name maps passed to existing operand
// rules are detached selections of frozen horizons, never transfer state.
type boundTypeValues struct {
	executor          *StorageExecutor
	program           *boundStatementProgram
	values            map[statementBindingID]staticOperand
	cypher25          bool
	params            map[string]interface{}
	unwound           map[statementBindingID]bool
	declarations      map[boundTypeDeclaration]statementBindingID
	done              map[*boundBody]bool
}

func (e *StorageExecutor) enrichBoundTypes(ctx context.Context, source string, initial typeScope, parent *semanticBodyScope, scalar string, cypher25 bool) (*boundTypeValues, *semanticQueryScope, error) {
	source = statementAdmissionSource(ctx, source)
	ensureBuiltInProceduresRegistered()
	var facts *projectedScopeFacts
	if scalar == "" {
		if admitted, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); admitted != nil && admitted.syntax.Source() == source {
			facts = admitted.root.program
		} else if schema := e.participantSchemaForStatement(ctx, source); schema != nil {
			compiler := projectionCompilation{executor: e, source: source, cypher25: cypher25, bindings: parent, schema: schema}
			facts = compiler.root(ctx, projectedMapNames(bindingNames(initial)), resultmetadata.Known(nil))
		} else {
			var retained []string
			for name := range pipelineImports(ctx) {
				retained = append(retained, name)
			}
			facts = e.projectedStatementScope(ctx, source, projectedMapNames(bindingNames(initial)), resultmetadata.Known(retained), cypher25, parent)
		}
	} else {
		compiler := projectionCompilation{executor: e, source: source, cypher25: cypher25, bindings: parent, scalarKind: scalar}
		facts = compiler.root(ctx, projectedMapNames(bindingNames(initial)), resultmetadata.Known(nil))
	}
	if facts.err != nil {
		return nil, nil, facts.err
	}
	if facts.statement != nil && facts.statement.Command != nil {
		return nil, &semanticQueryScope{complete: true, columns: canonicalColumns(facts.statement.Columns)}, nil
	}
	if facts.bound == nil || facts.statement == nil {
		return nil, nil, unresolvedStatementError()
	}
	// Parameter operands are typed by the request's values exactly when the
	// context carries them; no caller selects a mode.
	state := &boundTypeValues{params: getParamsFromContext(ctx), executor: e, program: facts.bound, values: make(map[statementBindingID]staticOperand), cypher25: cypher25, unwound: make(map[statementBindingID]bool), done: make(map[*boundBody]bool)}
	state.declarations = make(map[boundTypeDeclaration]statementBindingID, len(facts.bound.bindings))
	for _, binding := range facts.bound.bindings {
		state.declarations[boundTypeDeclaration{binding.name, binding.start, binding.end}] = binding.id
	}
	for _, branch := range facts.bound.root.branches {
		for name, identity := range branch.input.ids {
			state.values[identity] = initial[name]
		}
	}
	if err := state.body(ctx, facts.bound.root); err != nil {
		return state, nil, err
	}
	return state, state.model(facts.bound.root), nil
}

func (types *boundTypeValues) selectValues(scope resultmetadata.Scope) typeScope {
	values := make(typeScope)
	scope.Visit(func(names []string) {
		for _, name := range names {
			if identity, known := scope.Binding(name); known {
				values[name] = types.values[identity]
			}
		}
	}, func() {})
	return values
}

func (types *boundTypeValues) view(scope resultmetadata.Scope, frame *semanticBodyScope) staticTypeScope {
	view := types.selectValues(scope).static(types.cypher25)
	view.params = types.params
	view.admitted = types
	// These membership maps are immutable constructor snapshots. Type rules
	// may observe their retained/scalar flags, but cannot transition them.
	view.bindings = frame
	return view
}

func (types *boundTypeValues) model(body *boundBody) *semanticQueryScope {
	statement := body.program.statement
	model := &semanticQueryScope{query: types.program.syntax.Source()[statement.Start:statement.End], complete: true, stableNames: true, proof: body.dependencies, returned: types.columnValues(statement), imports: types.selectValues(statement.Incoming)}
	statement.Columns.Visit(func(names []string) { model.columns = names }, func() { model.complete = false })
	for _, branch := range body.branches {
		part := &semanticQueryScope{query: types.program.syntax.Source()[branch.statement.Start:branch.statement.End], complete: true, stableNames: true, bindings: branch.input.frame, proof: branch.dependencies, returned: types.columnValues(&branch.statement), imports: types.selectValues(branch.statement.Incoming)}
		branch.statement.Columns.Visit(func(names []string) { part.columns = names }, func() { part.complete = false })
		for _, source := range branch.shape.clauses {
			clause := source.pipelineClause
			clause.bindings = types.program.bodies[source.syntax.ID()]
			part.clauses = append(part.clauses, clause)

		}
		model.branches = append(model.branches, part)
	}
	if body.expressionKind != "" {
		model.imports = types.selectValues(statement.Entry.Dependencies)
	}
	if len(model.branches) == 1 {
		model.clauses = model.branches[0].clauses
		model.bindings = model.branches[0].bindings
	}
	return model
}

func (types *boundTypeValues) body(ctx context.Context, body *boundBody) error {
	if types.done[body] {
		return nil
	}
	types.done[body] = true
	for _, branch := range body.branches {
		for _, source := range branch.shape.clauses {
			if err := ctx.Err(); err != nil {
				return err
			}
			clause := types.program.clauses[source.syntax.ID()]
			if err := types.clause(ctx, clause); err != nil {
				return err
			}
		}
	}
	// UNION owns the output IDs. Join operand values at those declared IDs;
	// never derive another import context or output header from the branches.
	if len(body.branches) > 1 {
		for _, name := range scopeValues(body.program.statement.After) {
			var joined staticOperand
			for index, branch := range body.branches {
				identity, _ := branch.statement.After.Binding(name)
				if index == 0 {
					joined = types.values[identity]
				} else {
					joined = joinedOperand(joined, types.values[identity])
				}
			}
			identity, _ := body.program.statement.After.Binding(name)
			types.values[identity] = joined
		}
	}
	return nil
}

func (types *boundTypeValues) clause(ctx context.Context, clause *boundClause) error {
	source, step := clause.source.pipelineClause, clause.step
	frame := types.program.bodies[clause.source.syntax.ID()]
	before := types.view(step.Before, frame)
	if source.kind == pipelineClauseMatch || source.kind == pipelineClauseOptionalMatch || source.kind == pipelineClauseCreate || source.kind == pipelineClauseMerge {
		// Pattern kinds are operand constraints. The set and identity of every
		// name have already been decided, including fresh weak shadows.
		// A fresh weak shadow has a different admitted ID from its input.
		// Reused outer/lexical bindings retain their ID and operand constraint.
		kinds := types.operandView(source.syntax, step.After).kinds
		if source.kind == pipelineClauseMatch || source.kind == pipelineClauseOptionalMatch {
			if err := types.executor.validateMatchClauseBindings(kinds, source.text); err != nil {
				return err
			}
		}
		// Enrich only the identities selected at retained pattern symbols. A
		// second clause parser loses range/group cardinality and can turn an
		// admitted relationship-list binding into a scalar relationship.
		if err := types.patternRoles(source.syntax); err != nil {
			return err
		}
	}
	switch source.kind {
	case pipelineClauseCallSubquery:
		if err := types.body(ctx, clause.body); err != nil {
			return err
		}
	case pipelineClauseForeach:
		list := source.syntax.ChildRule(cypherantlr.CypherParserRULE_expression)
		operand, err := types.expression(ctx, list)
		if err != nil {
			return err
		}
		name := syntaxSymbolName(source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol))
		for _, branch := range clause.body.branches {
			if identity, known := branch.statement.Incoming.Binding(name); known {
				types.values[identity] = operand.elementOperand()
			}
		}
		return types.body(ctx, clause.body)
	case pipelineClauseLet:
		for index := 0; index < source.syntax.ChildCount(); index++ {
			item := source.syntax.Child(index)
			if item.Rule() != cypherantlr.CypherParserRULE_letItem {
				continue
			}
			operand, err := types.expression(ctx, item.ChildRule(cypherantlr.CypherParserRULE_expression))
			if err != nil {
				return err
			}
			identity := types.declaration(item.ChildRule(cypherantlr.CypherParserRULE_symbol))
			types.values[identity] = bindingOperand(operand)
		}
	case pipelineClauseUnwind:
		operand, err := types.expression(ctx, source.syntax.ChildRule(cypherantlr.CypherParserRULE_expression))
		if err != nil {
			return err
		}
		name := syntaxSymbolName(source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol))
		identity, _ := step.After.Binding(name)
		if operand.union || !operand.known() || operand.list() {
			types.values[identity] = bindingOperand(operand.elementOperand())
		} else {
			types.values[identity] = bindingOperand(operand)
		}
		types.unwound[identity] = types.values[identity].typeName() != "" && !operand.excludes(isListTypeName)
	case pipelineClauseCall:
		if clause.procedureSpec == nil {
			return unresolvedStatementError()
		}
		var unwound map[string]struct{}
		for _, name := range scopeValues(step.Before) {
			identity, _ := step.Before.Binding(name)
			if types.unwound[identity] {
				if unwound == nil {
					unwound = make(map[string]struct{})
				}
				unwound[name] = struct{}{}
			}
		}
		// Arguments are compared through the same type-only atoms as every
		// other clause consumer: an inline comprehension, quantifier, reduce
		// or subquery carries the operand this typed program produced.
		arguments, atoms, err := types.expressionText(ctx, source.syntax)
		if err != nil {
			return err
		}
		before.typeParameters = atoms
		if err := staticProcedureArgumentTypeError(ctx, arguments, before, unwound, *clause.procedureSpec); err != nil {
			return err
		}
		outputs := procedureOutputTypes(*clause.procedureSpec)
		if yield := yieldFromSyntax(source.syntax); yield != nil && !yield.yieldAll {
			for _, item := range yield.items {
				identity, _ := step.After.Binding(item.column())
				if value := outputs[item.name]; value != "" {
					types.values[identity] = provenOperand(value)
				}
			}
		}
	case pipelineClauseWith, pipelineClauseReturn:
		// A written WITH can be an admitted execution option. It has no
		// projection operands; consume the constructor's operator role.
		if step.Kind != resultmetadata.ClauseWith && step.Kind != resultmetadata.ClauseReturn {
			return nil
		}
		for _, item := range clause.plan.projections {
			var operand staticOperand
			if item.direct {
				identity, _ := step.Before.Binding(item.variable)
				operand = types.values[identity]
			} else {
				var err error
				operand, err = types.expression(ctx, item.syntax)
				if err != nil {
					return err
				}
			}
			identity, _ := step.After.Binding(item.alias)
			types.values[identity] = bindingOperand(operand)
		}
	}
	// Lower lexical/scalar constructs to type-only parameter atoms. Existing
	// operator/function rules see no query-local namespace to reconstruct.
	text, atoms, err := types.expressionText(ctx, source.syntax)
	if err != nil {
		return err
	}
	checking := source
	checking.text = text
	current := step.Entry.Readable
	if source.kind == pipelineClauseMatch || source.kind == pipelineClauseOptionalMatch || source.kind == pipelineClauseCreate || source.kind == pipelineClauseMerge || source.kind == pipelineClauseCall {
		current = step.After
	}
	view := types.view(current, frame)
	view.typeParameters = atoms
	projected := types.view(step.Exit.Readable, frame)
	projected.typeParameters = atoms
	view.projected = &projected
	if source.kind == pipelineClauseDelete {
		if err := deleteTargetTypeError(checking.text, view.kinds); err != nil {
			return err
		}
	}
	if err := types.executor.validateStaticClauseTypes(ctx, checking, view); err != nil {
		return err
	}
	return nil
}

type boundTypeDeclaration struct {
	name       string
	start, end int
}

func (types *boundTypeValues) declaration(symbol cypherantlr.SyntaxNode) statementBindingID {
	start, end := symbol.Span()
	return types.declarations[boundTypeDeclaration{syntaxSymbolName(symbol), start, end}]
}

func (types *boundTypeValues) expression(ctx context.Context, node cypherantlr.SyntaxNode) (staticOperand, error) {
	if !node.Valid() {
		return staticOperand{}, nil
	}
	if types.local(node) {
		return types.localOperand(ctx, node)
	}
	text, atoms, err := types.expressionText(ctx, node)
	if err != nil {
		return staticOperand{}, err
	}
	view := types.expressionView(node)
	view.typeParameters = atoms
	if err := types.patternRoles(node); err != nil {
		return staticOperand{}, err
	}
	if err := validateShortestCallBindings(text, view.kinds); err != nil {
		return staticOperand{}, err
	}
	if err := checkedWithoutUnaryPlus(ctx, text, types.cypher25, func(literalText string) error {
		return validateStaticFunctionArguments(ctx, literalText, types.cypher25)
	}); err != nil {
		return staticOperand{}, err
	}
	checker := staticOperatorChecker{scope: view, params: types.params}
	operand, err := checker.checkScoped(ctx, text)
	if err != nil {
		return staticOperand{}, err
	}
	// Keep operator diagnostics first, as on release. The remaining list-
	// position predicate still sees only this retained expression and its IDs.
	if err := staticListOperandTypeError(text, view); err != nil {
		return staticOperand{}, err
	}
	// The retained grammar's calls, read by the one call reader.
	return operand, syntaxFunctionCalls(node, func(call syntaxFunctionCall) error {
		if call.reduce || call.trimFrom {
			return nil
		}
		return knownFunctionCallError(call.name, call.normalized)
	})
}

func (types *boundTypeValues) local(node cypherantlr.SyntaxNode) bool {
	return types.program.nested[node.ID()] != nil || len(expressionLocalSyntax(node)) != 0
}

func (types *boundTypeValues) expressionText(ctx context.Context, node cypherantlr.SyntaxNode) (string, map[string]staticOperand, error) {
	text := node.Text()
	start, _ := node.Span()
	atoms := make(map[string]staticOperand)
	type edit struct {
		first, last int
		value       string
	}
	var edits []edit
	var visit func(cypherantlr.SyntaxNode) error
	visit = func(child cypherantlr.SyntaxNode) error {
		if child.ID() != node.ID() && types.local(child) {
			operand, err := types.localOperand(ctx, child)
			if err != nil {
				return err
			}
			name := fmt.Sprintf("__nornic_type_%d", child.ID())
			for strings.Contains(text, "$"+name) {
				name += "_"
			}
			atoms["$"+name] = operand
			first, last := child.Span()
			edits = append(edits, edit{first - start, last - start, "$" + name})
			return nil
		}
		for index := 0; index < child.ChildCount(); index++ {
			if err := visit(child.Child(index)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(node); err != nil {
		return "", nil, err
	}
	for index := len(edits) - 1; index >= 0; index-- {
		change := edits[index]
		text = text[:change.first] + change.value + text[change.last:]
	}
	return text, atoms, nil
}

func syntaxExpressions(node cypherantlr.SyntaxNode) []cypherantlr.SyntaxNode {
	var result []cypherantlr.SyntaxNode
	for index := 0; index < node.ChildCount(); index++ {
		child := node.Child(index)
		if child.Rule() == cypherantlr.CypherParserRULE_expression {
			result = append(result, child)
		}
	}
	return result
}

func (types *boundTypeValues) localOperand(ctx context.Context, node cypherantlr.SyntaxNode) (staticOperand, error) {
	if body := types.program.nested[node.ID()]; body != nil {
		if err := types.body(ctx, body); err != nil {
			return staticOperand{}, err
		}
		switch body.expressionKind {
		case "EXISTS":
			return provenOperand("Boolean"), nil
		case "COUNT":
			return provenOperand("Integer"), nil
		case "COLLECT":
			names := canonicalColumns(body.program.statement.Columns)
			identity, _ := body.program.statement.After.Binding(names[0])
			element := types.values[identity]
			if element.typeName() != "" && composable(element) {
				return typedOperand(listOfType(element.typeName())).proved(), nil
			}
			return provenOperand("List<T>"), nil
		}
	}
	locals := expressionLocalSyntax(node)
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_listComprehension, cypherantlr.CypherParserRULE_filterWith:
		filter := node.ChildRule(cypherantlr.CypherParserRULE_filterExpression)
		source, err := types.expression(ctx, filter.ChildRule(cypherantlr.CypherParserRULE_expression))
		if err != nil {
			return staticOperand{}, err
		}
		if source.excludes(isListTypeName) {
			return staticOperand{}, operandMismatch(source, "List<T>")
		}
		types.values[types.declaration(locals[0].symbol)] = source.elementOperand()
		if where := filter.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
			predicate, err := types.expression(ctx, where.ChildRule(cypherantlr.CypherParserRULE_expression))
			if err != nil {
				return staticOperand{}, err
			}
			if err := requireBooleanOperand(predicate); err != nil {
				return staticOperand{}, err
			}
		}
		if node.Rule() == cypherantlr.CypherParserRULE_filterWith {
			return provenOperand("Boolean"), nil
		}
		projection := node.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !projection.Valid() {
			return source, nil
		}
		element, err := types.expression(ctx, projection)
		if err != nil {
			return staticOperand{}, err
		}
		if element.typeName() != "" && composable(element) {
			return typedOperand(listOfType(element.typeName())).proved(), nil
		}
		return provenOperand("List<T>"), nil
	case cypherantlr.CypherParserRULE_reduceExpression, cypherantlr.CypherParserRULE_allReduceExpression:
		expressions := syntaxExpressions(node)
		initial, err := types.expression(ctx, expressions[0])
		if err != nil {
			return staticOperand{}, err
		}
		source, err := types.expression(ctx, expressions[1])
		if err != nil {
			return staticOperand{}, err
		}
		if source.excludes(isListTypeName) {
			return staticOperand{}, operandMismatch(source, "List<T>")
		}
		types.values[types.declaration(locals[0].symbol)] = reduceAccumulatorOperand(initial)
		types.values[types.declaration(locals[1].symbol)] = source.elementOperand()
		step, err := types.expression(ctx, expressions[2])
		if err != nil {
			return staticOperand{}, err
		}
		if len(expressions) > 3 {
			predicate, err := types.expression(ctx, expressions[3])
			if err != nil {
				return staticOperand{}, err
			}
			if err := requireBooleanOperand(predicate); err != nil {
				return staticOperand{}, err
			}
		}
		if node.Rule() == cypherantlr.CypherParserRULE_allReduceExpression {
			return provenOperand("Boolean"), nil
		}
		if initial.typeName() != "" && step.typeName() != "" && composable(initial) && step.excludes(accumulatorAccepts(initial)) {
			return staticOperand{}, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", fmt.Sprintf("Type mismatch: accumulator is %s but expression has type %s", initial.typeName(), step.typeName()))
		}
		return reduceAccumulatorOperand(initial), nil
	case cypherantlr.CypherParserRULE_patternComprehension:
		if err := types.patternRoles(node.ChildRule(cypherantlr.CypherParserRULE_relationshipsChainPattern)); err != nil {
			return staticOperand{}, err
		}
		for _, local := range locals {
			if !local.reuse {
				types.values[types.declaration(local.symbol)] = operandOfKind(matchBindingPath)
			}
		}
		if where := node.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
			predicate, err := types.expression(ctx, where.ChildRule(cypherantlr.CypherParserRULE_expression))
			if err != nil {
				return staticOperand{}, err
			}
			if err := requireBooleanOperand(predicate); err != nil {
				return staticOperand{}, err
			}
		}
		element, err := types.expression(ctx, node.ChildRule(cypherantlr.CypherParserRULE_expression))
		if err != nil {
			return staticOperand{}, err
		}
		if element.typeName() != "" && composable(element) {
			return typedOperand(listOfType(element.typeName())).proved(), nil
		}
		return provenOperand("List<T>"), nil
	}
	return staticOperand{}, nil
}

func canonicalColumns(columns resultmetadata.Columns) (names []string) {
	columns.Visit(func(values []string) { names = values }, func() {})
	return names
}

// columnValues selects the declared result, not the body's full readable scope.
func (types *boundTypeValues) columnValues(statement *resultmetadata.Statement) typeScope {
	selected := make(typeScope)
	for _, name := range canonicalColumns(statement.Columns) {
		if identity, ok := statement.After.Binding(name); ok {
			selected[name] = types.values[identity]
		}
	}
	return selected
}

// Standalone operand callers also enter the constructor; they do not install
// lexical names themselves. The offset selects an original retained syntax span.
func typeExpressionSource(source string) string {
	trimmed := strings.TrimSpace(source)
	for _, keyword := range []string{"RETURN", "WITH", "MATCH", "OPTIONAL MATCH", "UNWIND", "FOR", "LET", "CALL", "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "FOREACH", "FILTER", "USE", "CYPHER"} {
		if startsWithKeywordFold(trimmed, keyword) {
			return source
		}
	}
	return "RETURN " + source
}
func boundExpressionTypeView(ctx context.Context, scope staticTypeScope, text string, at int) staticTypeScope {
	if scope.admitted != nil {
		return scope
	}
	source := typeExpressionSource(text)
	offset := len(source) - len(text) + at
	executor := &StorageExecutor{}
	input := typeScopeOf(scope)
	if !scope.complete {
		for _, name := range expressionFreeVariables(ctx, text) {
			if _, known := input[name]; !known {
				input[name] = staticOperand{}
			}
		}
	}
	values, _, err := executor.enrichBoundTypes(ctx, source, input, scope.bindings, "", scope.cypher25)
	if values == nil {
		scope.typeFailure = err
		return scope
	}
	values.params = scope.params
	width := len(source) + 1
	var selected resultmetadata.Scope
	found := false
	for id, node := range values.program.nodes {
		first, last := node.Span()
		if first <= offset && offset < last && last-first < width {
			if frozen, ok := values.program.expressions[id]; ok {
				width = last - first
				selected = frozen
				found = true
			}
		}
	}
	if !found {
		return scope
	}
	view := values.view(selected, scope.bindings)
	view.complete = scope.complete
	view.params = scope.params
	view.typeFailure = err
	return view
}

// checkClauseOperandHandles visits retained expression operands at their exact
// admitted lexical/projection horizon. No text scan chooses their namespace.
func (types *boundTypeValues) checkClauseOperandHandles(ctx context.Context, source pipelineClause, view staticTypeScope) error {
	var failure error
	depth := 0
	source.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure != nil {
			return nil
		}
		if node.ID() != source.syntax.ID() {
			if nested := types.program.clauses[node.ID()]; nested != nil {
				return nil
			}
		}
		if node.Rule() != cypherantlr.CypherParserRULE_expression {
			return nil
		}
		depth++
		if depth == 1 {
			operand, err := types.expression(ctx, node)
			if err != nil {
				failure = err
			} else if parent, ok := node.Parent(); ok {
				if parent.Rule() == cypherantlr.CypherParserRULE_filterSt {
					failure = requireBooleanOperand(operand)
				} else if parent.Rule() == cypherantlr.CypherParserRULE_where {
					if owner, ok := parent.Parent(); ok && owner.ID() == source.syntax.ID() {
						failure = requireBooleanOperand(operand)
					}
				}
			}
		}
		return func() { depth-- }
	})
	if failure != nil {
		return failure
	}
	if source.kind == pipelineClauseDelete {
		return staticDeleteTargetTypeError(ctx, source.text, view)
	}
	return staticWriteTokenError(ctx, source, view)
}

func (types *boundTypeValues) expressionView(node cypherantlr.SyntaxNode) staticTypeScope {
	return types.operandView(node, types.program.expressions[node.ID()])
}

func (types *boundTypeValues) operandView(node cypherantlr.SyntaxNode, frozen resultmetadata.Scope) staticTypeScope {
	selected := types.selectValues(frozen)
	// A predicate inside a quantified pattern reads one iteration. Select
	// the same admitted IDs at retained pattern symbols; do not recover group
	// membership from rewritten clause text or add a name to the frozen view.
	refine := func(pattern cypherantlr.SyntaxNode) {
		syntaxPatternDefinitions(pattern, func(rule int, symbol cypherantlr.SyntaxNode) {
			name := syntaxSymbolName(symbol)
			identity, known := frozen.Binding(name)
			roleID, admitted := types.program.expressions[symbol.ID()].Binding(name)
			if !known || !admitted || identity != roleID {
				return
			}
			switch rule {
			case cypherantlr.CypherParserRULE_nodePattern:
				selected[name] = operandOfKind(matchBindingNode)
			case cypherantlr.CypherParserRULE_relationDetail:
				selected[name] = operandOfKind(matchBindingRelationship)
			}
		})
	}
	for parent, ok := node.Parent(); ok; parent, ok = parent.Parent() {
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_quantifiedPath:
			refine(parent)
		case cypherantlr.CypherParserRULE_relationDetail:
			// An inline relationship predicate reads its current relationship,
			// including when the completed pattern exports a relationship list.
			refine(parent)
		}
	}
	view := selected.static(types.cypher25)
	view.admitted = types
	view.params = types.params
	return view
}

// A declared variable retains its operand constraints, not a parameter's
// diagnostic name. Direct parameter operands continue to name the parameter.
func bindingOperand(operand staticOperand) staticOperand { operand.parameter = ""; return operand }
