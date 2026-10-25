package cypher

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Mutation declaration predicates run inside construction, with its incoming
// membership. Their temporary maps check ordered declarations within ONE clause;
// no mutation validator schedules another WITH/CALL/FOREACH scope walk.
func (compilation *boundCompilation) admitMutationClause(ctx context.Context, clause *boundClause) error {
	source := clause.source.pipelineClause
	scope := &semanticBindingScope{names: copyBindingNames(clause.branch.scope)}
	if source.kind == pipelineClauseCreate || source.kind == pipelineClauseMerge {
		body := clause.branch.bindings
		var current typeScope
		if body != nil {
			current = body.entry
		}
		scope.declared = bindingNames(body.strong(current))
	}
	executor := compilation.owner.executor
	switch source.kind {
	case pipelineClauseCreate:
		return executor.validateCreateClauseBindings(ctx, scope, source.text, compilation.owner.cypher25)
	case pipelineClauseMerge:
		return executor.validateMergeClause(ctx, scope, source.text, compilation.owner.cypher25)
	case pipelineClauseSet:
		return executor.validateSetClauseScope(ctx, scope, source.text)
	case pipelineClauseRemove:
		return validateRemoveClauseScope(ctx, scope, source.text)
	case pipelineClauseDelete:
		return validateDeleteClauseScope(scope, source.text)
	}
	return nil
}

// Procedure syntax/signature legality is admitted before output names exist.
// Registry return declarations are schema, distinct from a handler's row values.
func (compilation *boundCompilation) admitProcedureInvocation(ctx context.Context, clause *boundClause, procedure registeredProcedure, found bool, yield *yieldClause) error {
	inQuery := len(clause.branch.shape.clauses) > 1 || clause.branch.shape.finished
	if err := admitYieldModifiers(yield, inQuery); err != nil {
		return err
	}
	for _, argument := range procedureArgumentTextsFromSyntax(clause.source.syntax) {
		if containsAggregateFunc(argument) {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidAggregation", "procedure arguments cannot contain aggregate expressions")
		}
	}
	if !found {
		return nil // participant lookup belongs to procedure(), never a row loop.
	}
	if len(procedure.Spec.Params) > 0 && procedureArgumentTextsFromSyntax(clause.source.syntax) == nil && (yield != nil || inQuery) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentPassingMode", fmt.Sprintf("procedure %s arguments must be passed explicitly inside a query", procedure.Spec.Name))
	}
	// An opaque user procedure may declare no schema and keep its existing
	// explicit-YIELD extension. A registered built-in's empty schema is closed.
	if yield != nil && (!procedure.User || len(procedureDeclaredReturns(procedure.Spec)) > 0) {
		var columns []string
		for _, column := range procedureDeclaredReturns(procedure.Spec) {
			columns = append(columns, column.Name)
		}
		if err := validateYieldColumnsExist(columns, yield); err != nil {
			return err
		}
	}
	if yield == nil && inQuery && len(procedure.Spec.Returns) > 0 {
		if clause.source.start != clause.branch.shape.clauses[0].start {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "procedure calls inside a query must name results explicitly using YIELD")
		}
		// Preserve the release extension: a leading call may discard outputs.
		// It may not read an output without declaring it through YIELD.
		for _, column := range procedure.Spec.Returns {
			for _, tail := range clause.branch.shape.clauses[1:] {
				if referencesVariable(tail.text, column.Name) {
					return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UndefinedVariable", fmt.Sprintf("procedure output %s must be introduced with YIELD", column.Name))
				}
			}
		}
	}
	return nil
}

// admitYieldModifiers owns modifier placement, not pagination VALUE typing.
func admitYieldModifiers(yield *yieldClause, hasTail bool) error {
	if yield == nil || !yield.hasModifiers() {
		return nil
	}
	if !hasTail {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreStandaloneCallModifiers())
	}
	if yield.misplacedWhere {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreYieldWhereMisplaced())
	}
	return nil
}

// Retained clause predicates are structurally admitted after their reads. Type
// enrichment only constrains their operands; it cannot admit clause composition.
func (compilation *boundCompilation) admitClauseSyntax(clause *boundClause) error {
	source := clause.source
	if source.kind == pipelineClauseFilter {
		expression := pipelineFilterExpression(source.text)
		if expression == "" {
			return sharedClauseSyntaxError("FILTER requires a predicate")
		}
		if pipelineExpressionContainsAggregate(expression) {
			return sharedClauseSyntaxError("Aggregation is not allowed in FILTER")
		}
	}
	var failure error
	source.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure != nil || node.Rule() != cypherantlr.CypherParserRULE_where {
			return nil
		}
		for parent, ok := node.Parent(); ok && parent.ID() != source.syntax.ID(); parent, ok = parent.Parent() {
			if compilation.product.clauses[parent.ID()] != nil || len(expressionLocalSyntax(parent)) > 0 {
				return nil // nested clause or lexical predicate has its own owner.
			}
		}
		expression := strings.TrimSpace(maskSubqueryBodies(node.ChildRule(cypherantlr.CypherParserRULE_expression).Code()))
		if expression == "" {
			return nil
		}
		if containsAggregateFunc(expression) {
			failure = newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidAggregation", "aggregate expressions are not allowed in WHERE")
			return nil
		}
		switch expression[0] {
		case '=', '<', '>', '~', '!':
			failure = localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInputExpectedExpression(string(expression[0])))
			return nil
		}
		failure = projectionItemTermError(expression)
		return nil
	})
	return failure
}

func admitLexicalSyntax(node cypherantlr.SyntaxNode) error {
	if node.Rule() != cypherantlr.CypherParserRULE_listComprehension {
		return nil
	}
	filter := node.ChildRule(cypherantlr.CypherParserRULE_filterExpression)
	where := filter.ChildRule(cypherantlr.CypherParserRULE_where)
	projection := node.ChildRule(cypherantlr.CypherParserRULE_expression)
	if where.Valid() && containsAggregateFunc(where.Code()) || projection.Valid() && containsAggregateFunc(projection.Code()) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidAggregation", "aggregate expressions are not allowed inside list-comprehension predicates or projections")
	}
	return nil
}

// admitProjectionItems observes ordered decoded declarations before any name
// map or executable wildcard expansion can erase a sibling declaration.
func (body *semanticBodyScope) admitProjectionItems(projection projectionSemantics, kind pipelineClauseKind, names *quotedVariableNames) error {
	seen := make(map[string]bool)
	var failure error
	projection.eachItem(func(item projectionSemanticItem) {
		if failure != nil {
			return
		}
		name := item.writtenName(names)
		if kind == pipelineClauseWith && !item.alias.Valid() && item.expression.Code() != "{}" {
			if _, variable := projectionVariableFromSyntax(item.expression); !variable {
				failure = newSemanticError("Neo.ClientError.Statement.SyntaxError", "NoExpressionAlias", "expressions in WITH must be explicitly aliased")
				return
			}
		}
		if seen[name] {
			message := "Multiple RETURN items project the same column name: " + name
			if kind == pipelineClauseWith {
				message = "multiple WITH items project the same column name: " + name
			}
			failure = newSemanticError("Neo.ClientError.Statement.SyntaxError", "ColumnNameConflict", message)
		}
		seen[name] = true
	})
	return failure
}

func (body *semanticBodyScope) admitYieldItems(yield *yieldClause, inQuery bool) error {
	if yield == nil {
		return nil
	}
	if yield.yieldAll && inQuery {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "YIELD * is only valid for a standalone procedure call")
	}
	seen := make(map[string]bool, len(yield.items))
	for _, item := range yield.items {
		name := normalizeProjectionColumnName(item.column())
		if seen[name] {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherCoreProcedureOutputShadowsVariable(name))
		}
		if err := body.admitDeclaration(name, "YIELD"); err != nil {
			return err
		}
		seen[name] = true
	}
	return nil
}

// Cypher 5's imported-value grouping rule is declaration-time admission. The
// scalar frame supplies declared imports; executed row keys cannot admit it.
// Cypher 25 treats those same enclosing bindings as constants instead.
func (body *semanticBodyScope) admitScalarAggregation(plan *returnProjectionPlan, cypher25 bool) error {
	if body == nil || !body.scalar || cypher25 {
		return nil
	}
	for _, item := range writtenProjectionItems(plan) {
		spans := findAggregateSpans(item.expr)
		if len(spans) == 0 {
			continue
		}
		outside := item.expr
		for i := len(spans) - 1; i >= 0; i-- {
			outside = outside[:spans[i].start] + " " + outside[spans[i].end:]
		}
		for _, name := range expressionVariables(outside) {
			if _, imported := body.original[name]; imported {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreGroupByImplicitGroupingKey(name))
			}
		}
	}
	return nil
}

// patternRoles checks operands of IDs already selected by construction. A
// pattern role cannot introduce a name or choose whether it reuses an input.
func (types *boundTypeValues) patternRoles(root cypherantlr.SyntaxNode) error {
	var visit func(cypherantlr.SyntaxNode) error
	visit = func(node cypherantlr.SyntaxNode) error {
		if node.ID() != root.ID() && types.local(node) {
			return nil
		}
		kind := patternOperandKind(node)
		if kind != matchBindingUnknown {
			symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
			name := syntaxSymbolName(symbol)
			if symbol.Valid() {
				identity, known := types.program.expressions[symbol.ID()].Binding(name)
				if !known || identity == 0 {
					return unresolvedStatementError()
				}
				operand := gateProven(types.values[identity])
				if types.declaration(symbol) != identity {
					operand = gateProven(typeScopeOf(types.expressionView(symbol))[name])
				}
				if err := patternBindingKindError(name, operand.bindingKind(), kind); err != nil {
					return err
				}
				// Only a new declaration is enriched. Null/unknown reused operands retain
				// their uncertainty and their existing runtime checks.
				if types.declaration(symbol) == identity {
					types.values[identity] = operandOfKind(kind)
				}
			}
		}
		for i := 0; i < node.ChildCount(); i++ {
			if err := visit(node.Child(i)); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

// patternOperandKind is a type constraint at a retained grammar handle. The
// constructor has already chosen its binding ID; this function cannot declare,
// import, export or select membership. Group outputs retain list cardinality.
func patternOperandKind(node cypherantlr.SyntaxNode) matchBindingKind {
	kind := matchBindingUnknown
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_nodePattern:
		kind = matchBindingNode
	case cypherantlr.CypherParserRULE_relationDetail:
		kind = matchBindingRelationship
		if node.ChildRule(cypherantlr.CypherParserRULE_rangeLit).Valid() {
			return matchBindingRelationshipList
		}
	case cypherantlr.CypherParserRULE_patternPart:
		return matchBindingPath
	default:
		return kind
	}
	for parent, ok := node.Parent(); ok; parent, ok = parent.Parent() {
		if parent.Rule() == cypherantlr.CypherParserRULE_quantifiedPath {
			if kind == matchBindingNode {
				return matchBindingNodeList
			}
			return matchBindingRelationshipList
		}
		if kind == matchBindingRelationship && parent.Rule() == cypherantlr.CypherParserRULE_patternElemChain && parent.ChildRule(cypherantlr.CypherParserRULE_relationshipQuantifier).Valid() {
			return matchBindingRelationshipList
		}
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_patternPart, cypherantlr.CypherParserRULE_relationshipsChainPattern, cypherantlr.CypherParserRULE_expression:
			// A nested pattern has its own operand role. An enclosing
			// quantified pattern refines a reused input, not this pattern's
			// declaration cardinality.
			return kind
		}
	}
	return kind
}

// Only operands consumed by typing retain a read view. Pattern symbols keep
// their exact admitted role identity; clause facts already retain clause views.
func boundOperandHandle(node cypherantlr.SyntaxNode) bool {
	if node.Rule() == cypherantlr.CypherParserRULE_expression || len(expressionLocalSyntax(node)) != 0 {
		return true
	}
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist, cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
		return true
	}
	if node.Rule() == cypherantlr.CypherParserRULE_symbol {
		if parent, ok := node.Parent(); ok {
			return parent.Rule() == cypherantlr.CypherParserRULE_nodePattern || parent.Rule() == cypherantlr.CypherParserRULE_relationDetail || parent.Rule() == cypherantlr.CypherParserRULE_patternPart
		}
	}
	return false
}

func (compilation *boundCompilation) freezeReadView(names map[string]struct{}, ids map[string]statementBindingID) resultmetadata.Scope {
	identities := make([]uint64, 0, len(names))
	for name := range names {
		identities = append(identities, uint64(ids[name]))
	}
	slices.Sort(identities)
	key := make([]byte, 8*len(identities))
	for i, id := range identities {
		binary.LittleEndian.PutUint64(key[i*8:], id)
	}
	if view, known := compilation.readViews[string(key)]; known {
		return view
	}
	view := boundBindingScope(projectedMapNames(names), ids)
	compilation.readViews[string(key)] = view
	return view
}

// boundProcedureArity is a frozen signature decision, independent of rows.
// Only constructProcedureArity creates it. Runtime argument materialization
// consumes the admitted decision, including when a call receives no rows.
type boundProcedureArity struct {
	name                    string
	count, minimum, maximum int
}

func constructProcedureArity(spec ProcedureSpec, count int) (*boundProcedureArity, error) {
	if spec.MinArgs > 0 && count < spec.MinArgs {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments", fmt.Sprintf("procedure %s requires at least %d arguments, got %d", spec.Name, spec.MinArgs, count))
	}
	if spec.MaxArgs >= 0 && count > spec.MaxArgs {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments", fmt.Sprintf("procedure %s accepts at most %d arguments, got %d", spec.Name, spec.MaxArgs, count))
	}
	return &boundProcedureArity{name: spec.Name, count: count, minimum: spec.MinArgs, maximum: spec.MaxArgs}, nil
}
