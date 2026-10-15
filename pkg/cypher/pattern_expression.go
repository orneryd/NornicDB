package cypher

import (
	"context"
	"maps"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// splitPatternComprehension splits a pattern comprehension
// [p = (a)-->(b) WHERE … | projection] into its pattern (with WHERE) and its
// projection. A pattern comprehension starts with a node pattern, or a path
// variable and = before one; a list comprehension [x IN list WHERE … | …]
// starts with its variable, so an IN in the WHERE doesn't make one (#1109).
func splitPatternComprehension(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if len(expr) < 3 || expr[0] != '[' || expr[len(expr)-1] != ']' {
		return "", "", false
	}

	parenDepth, bracketDepth, braceDepth := 0, 1, 0
	var quote byte
	for i := 1; i < len(expr)-1; i++ {
		char := expr[i]
		if quote != 0 {
			if char == '\\' {
				i++
				continue
			}
			if char == quote {
				quote = 0
			}
			continue
		}
		switch char {
		case '\'', '"':
			quote = char
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		case '|':
			if labelExpressionBarAt(expr, 1, i) {
				continue // m:A|B in the WHERE (#860)
			}
			if parenDepth == 0 && bracketDepth == 1 && braceDepth == 0 {
				pattern := strings.TrimSpace(expr[1:i])
				projection := strings.TrimSpace(expr[i+1 : len(expr)-1])
				// Classify on the leading token, not on the presence of " IN ".
				// A pattern comprehension begins (after an optional "p = " path
				// assignment) with a node pattern "("; a list comprehension
				// begins with an identifier followed by IN. Rejecting any
				// pattern containing " IN " wrongly breaks pattern
				// comprehensions whose WHERE uses IN (#1109).
				if patternComprehensionHead(pattern) && projection != "" {
					return pattern, projection, true
				}
			}
		}
	}
	return "", "", false
}

// evaluateRowPatternComprehensionAtom evaluates an expression that is one
// pattern comprehension where it stands, as evaluateRowSubqueryAtom does a
// subquery: inside a comparison, a list, a CASE or a function argument, with
// the row's variables and the statement's context (#1109). matched is false
// for any other expression, or when the row carries no statement context.
func (e *StorageExecutor) evaluateRowPatternComprehensionAtom(expr string, values map[string]interface{}) (interface{}, bool) {
	if len(expr) < 3 || expr[0] != '[' || !patternComprehensionHead(expr[1:]) {
		return nil, false
	}
	pattern, projection, comprehension := splitPatternComprehension(expr)
	if !comprehension {
		return nil, false
	}
	ctx := rowStatementContext(values)
	if ctx == nil {
		return nil, false
	}
	return e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, values), true
}

// patternComprehensionHead reports whether pattern is the head of a pattern
// comprehension: a node pattern "(", optionally prefixed by a path-variable
// assignment ("p = "). A list comprehension head is an identifier followed by
// IN, which this rejects.
func patternComprehensionHead(pattern string) bool {
	head := strings.TrimSpace(pattern)
	if strings.HasPrefix(head, "(") {
		return true
	}
	// p = (…) or `p p` = (…), read as every reader reads it.
	_, _, _, named := namedPathDeclaration(head, 0)
	return named
}

// isStandaloneExistsSubquery reports whether expr is exactly one
// EXISTS { ... } subquery expression, with nothing before EXISTS and nothing
// after its closing brace. Such an expression is a boolean value wherever an
// expression is allowed (RETURN, WITH, list elements, function arguments), not
// only in WHERE; compound expressions reach it through their operands.
func isStandaloneExistsSubquery(expr string) bool {
	if len(expr) < len("EXISTS{}") || (expr[0] != 'E' && expr[0] != 'e') || !matchKeywordAt(expr, 0, "EXISTS") {
		return false
	}
	open := skipSpaces(expr, len("EXISTS"))
	if open >= len(expr) || expr[open] != '{' {
		return false
	}
	return findMatchingDelimiter(expr, open, '{', '}') == len(expr)-1
}

// evaluateExistsSubqueryValue evaluates a standalone EXISTS { ... } expression
// against the current row: the value scope of ctx (a quantifier's or
// comprehension's variable, the row's scalars) and the entities bound in the
// row, which shadow it. It shares the WHERE predicate's correlated evaluation
// (evaluateRowExistsPredicate), so an EXISTS value and an EXISTS filter
// always agree.
func (e *StorageExecutor) evaluateExistsSubqueryValue(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) bool {
	matched, _ := e.evaluateRowExistsPredicate(ctx, expr, entityRow(ctx, nodes, rels))
	return matched
}

// evaluateBoundPatternRows matches pattern (with its WHERE) from the bound
// nodes and relationships; the row's other values are in scope for the
// WHERE, as a pattern comprehension's or a subquery's outer variables are
// (#1055).
func (e *StorageExecutor) evaluateBoundPatternRows(ctx context.Context, pattern string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, values map[string]interface{}) []traversalOptRow {
	rows, _ := e.boundPatternRows(ctx, pattern, nodes, rels, values)
	return rows
}

// boundPatternRows is evaluateBoundPatternRows reporting whether the kernel
// matched the pattern: ok is false for a pattern it doesn't take or an error,
// which isn't "no rows" (#1055).
func (e *StorageExecutor) boundPatternRows(ctx context.Context, pattern string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, values map[string]interface{}) ([]traversalOptRow, bool) {
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(upperASCII(pattern), "MATCH ") {
		pattern = strings.TrimSpace(pattern[len("MATCH "):])
	}
	clauses := splitOptionalMatchClauses(pattern)
	if len(clauses) != 1 {
		return nil, false
	}
	seed := traversalOptRow{
		nodes: make(map[string]*storage.Node, len(nodes)),
		rels:  make(map[string]*storage.Edge, len(rels)),
	}
	for name, node := range nodes {
		seed.nodes[name] = node
	}
	for name, relationship := range rels {
		seed.rels[name] = relationship
	}
	for name, value := range values {
		switch value.(type) {
		case *storage.Node, *storage.Edge:
			continue
		}
		if seed.values == nil {
			seed.values = make(map[string]interface{}, len(values))
		}
		seed.values[name] = value
	}

	expanded, err := e.applyTraversalOptionalClause(ctx, []traversalOptRow{seed}, clauses[0])
	if err != nil {
		return nil, false
	}
	matches := expanded[:0]
	for _, row := range expanded {
		if row.optionalMatched {
			matches = append(matches, row)
		}
	}
	return matches, true
}

func (e *StorageExecutor) evaluatePatternComprehension(ctx context.Context, pattern, projection string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) []interface{} {
	scope := make(pipelineRow, len(nodes)+len(rels))
	for variable, node := range nodes {
		scope[variable] = node
	}
	for variable, relationship := range rels {
		scope[variable] = relationship
	}
	return e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, scope)
}

// evaluatePatternComprehensionFromRow evaluates a correlated pattern against
// the entity bindings in a heterogeneous pipeline row and evaluates its
// projection against the complete outer scope. This keeps WITH/RETURN
// horizons on the same row-expression path while preserving scalar aliases
// that are visible inside the comprehension projection.
func (e *StorageExecutor) evaluatePatternComprehensionFromRow(ctx context.Context, pattern, projection string, outer pipelineRow) []interface{} {
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	// The comprehension's named path (p = …) is its own variable
	// (localBindingAt), shadowing an outer one of the same name rather than
	// constraining the match.
	if path := extractPathAssignmentVariable(pattern); path != "" {
		if _, bound := outer[path]; bound {
			outer = maps.Clone(outer)
			delete(outer, path)
		}
	}
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	for variable, value := range outer {
		switch entity := value.(type) {
		case *storage.Node:
			nodes[variable] = entity
		case *storage.Edge:
			rels[variable] = entity
		}
	}
	rows := e.evaluateBoundPatternRows(ctx, pattern, nodes, rels, outer)
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		if !probe.check() {
			return nil
		}
		scope := make(pipelineRow, len(outer)+len(row.nodes)+len(row.rels)+len(row.values))
		for variable, value := range outer {
			scope[variable] = value
		}
		for variable, node := range row.nodes {
			if node == nil {
				scope[variable] = nil
			} else {
				scope[variable] = node
			}
		}
		for variable, relationship := range row.rels {
			if relationship == nil {
				scope[variable] = nil
			} else {
				scope[variable] = relationship
			}
		}
		for variable, value := range row.values {
			scope[variable] = value
		}
		value, evaluated := e.evaluateRowExpressionWithContext(ctx, projection, scope)
		if getExpressionFailure(ctx) != nil {
			return nil
		}
		if evaluated {
			values = append(values, value)
			continue
		}
		values = append(values, e.evaluateExpressionWithContext(ctx, projection, row.nodes, row.rels))
	}
	return values
}

// evaluateRowExpressionWithContext extends the allocation-conscious row
// evaluator with graph expressions that require storage access. Callers with
// an execution context use this as the converged expression entry point.
func evaluateBoundRowValue(expr string, values pipelineRow) (interface{}, bool) {
	if value, bound := values[expr]; bound && !isLiteralKeyword(expr) {
		return value, true
	}
	if variable, chain, ok := rowPropertyChainShape(expr); ok {
		if base, bound := values[variable]; bound {
			if value, resolved := evaluateRowPropertyChain(base, chain); resolved {
				return value, true
			}
		}
	}
	return nil, false
}

func (e *StorageExecutor) evaluateRowExpressionWithContext(ctx context.Context, expr string, values pipelineRow, cancellation ...*pipelineCancellation) (interface{}, bool) {
	if value, bound := evaluateBoundRowValue(expr, values); bound {
		return value, true
	}
	if containsRelationshipPattern(expr) {
		if value, ok, handled := e.evaluatePatternPredicateValue(ctx, expr, values); handled {
			return value, ok
		}
	}
	var extended pipelineRow
	bind := func(name string, value interface{}) {
		if _, exists := values[name]; exists {
			return
		}
		if extended == nil {
			extended = make(pipelineRow, len(values)+1)
			for key, existing := range values {
				extended[key] = existing
			}
		}
		extended[name] = value
	}
	for name, value := range valueBindingsFromContext(ctx) {
		bind(name, value)
	}
	for name, value := range parameterRowValues(ctx) {
		bind(name, value)
	}
	if needsRowStatementContext(expr) || strings.ContainsAny(expr, "([{") {
		ctx = withPipelineCancellation(ctx)
		bind(rowStatementContextKey, ctx)
	}
	if extended != nil {
		values = extended
	}
	// A row variable, or a plain property chain on one (e.uuid), resolves
	// without the graph-expression checks below, which can't match it.
	if value, bound := evaluateBoundRowValue(expr, values); bound {
		return value, true
	}
	if subquery, ok := standaloneSubqueryExpression(expr); ok {
		return e.evaluateRowSubqueryValue(ctx, subquery.kind, subquery.body, values)
	}
	// id() / elementId() resolve the entity identity at this context-aware
	// boundary: the allocation-conscious row evaluator below carries no
	// context and would build the id with the executor's default database,
	// losing a composite subquery's constituent identity (#745 §3).
	if function, argument, ok := parseFunctionCallWS(strings.TrimSpace(expr)); ok &&
		(strings.EqualFold(function, "id") || strings.EqualFold(function, "elementId")) {
		if value, resolved := e.evaluateRowEntityIdentity(ctx, function, argument, values); resolved {
			return value, true
		}
	}
	if pattern, projection, ok := splitPatternComprehension(expr); ok {
		value := e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, values)
		return value, getExpressionFailure(ctx) == nil
	}
	if trimmed := strings.TrimSpace(expr); isStandaloneExistsSubquery(trimmed) {
		matched, _ := e.evaluateRowExistsPredicate(ctx, trimmed, values)
		return matched, true
	}
	// The row evaluator's error is the statement's: it is recorded, and the
	// expression is unresolved.
	if strings.Contains(expr, "{") {
		if failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure); ok {
			scope := make(pipelineRow, len(values)+1)
			for name, value := range values {
				scope[name] = value
			}
			scope["\x00mapKeyOrders"] = failure
			values = scope
		}
	}
	// Simple arithmetic/comparisons borrow a probe without cloning the row to
	// carry context metadata. Recursive calls keep this same budget.
	probe, _ := ctx.Value(pipelineCancellationKey{}).(*pipelineCancellation)
	local := pipelineCancellation{ctx: ctx}
	if len(cancellation) > 0 {
		probe = cancellation[0]
	}
	if probe == nil {
		probe = &local
	}
	value, resolved, err := e.evaluateRowValueWithCancellation(expr, values, probe)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	if getExpressionFailure(ctx) != nil {
		return nil, false
	}
	if !resolved && containsRelationshipPattern(expr) {
		// The row evaluator doesn't read the graph: an expression with a
		// relationship pattern inside an operator or comprehension
		// (size([(n)--() | 1]) > 0, [x IN l | size([(x)-->() | 1])]) is
		// evaluated whole by the shared evaluator against the row's entities
		// and values, recording its errors on ctx. Text it doesn't recognize
		// comes back unchanged: unresolved. Other expressions the row
		// evaluator leaves unresolved stay unresolved (a malformed literal
		// such as [, ] is a syntax error, not a value). Subquery expressions
		// were evaluated above.
		trimmed := strings.TrimSpace(expr)
		nodes, rels := entityScopesFromValues(values)
		value, resolved = e.evaluateExpressionWithContextDefined(withValueBindings(ctx, values), trimmed, nodes, rels)
		if getExpressionFailure(ctx) != nil {
			return nil, false
		}
		if text, ok := value.(string); ok && text == trimmed && !isWholeCypherQuotedString(trimmed) {
			return nil, false
		}
	}
	if function, arguments, functionCall := parseFunctionCallWS(strings.TrimSpace(expr)); functionCall && strings.EqualFold(function, "substring") {
		parts := splitTopLevelComma(arguments)
		if len(parts) == 2 || len(parts) == 3 {
			for _, argument := range parts[1:] {
				position, valid, _ := e.evaluateRowValue(argument, values)
				if numeric, ok := toInt(position); valid && ok && numeric < 0 {
					recordExpressionFailure(ctx, stringOperationOutOfRange(cypherVersionFromContext(ctx) == "25", "substring"))
					break
				}
			}
		}
	}
	return value, resolved
}

// evaluateRowEntityIdentity resolves id(entity) and elementId(entity) at the
// context-aware row boundary. The element id names the database the statement
// currently executes on — a composite subquery's constituent after
// CALL { USE … } — so the identity survives the row projection (#745 §3).
func (e *StorageExecutor) evaluateRowEntityIdentity(ctx context.Context, function, argument string, values pipelineRow) (interface{}, bool) {
	inner := strings.TrimSpace(argument)
	var node *storage.Node
	var edge *storage.Edge
	if value, bound := values[inner]; bound {
		node, _ = value.(*storage.Node)
		edge, _ = value.(*storage.Edge)
	} else if value, resolved, err := e.evaluateRowValue(inner, values); err == nil && resolved {
		node, _ = value.(*storage.Node)
		edge, _ = value.(*storage.Edge)
	} else {
		return nil, false
	}
	switch {
	case node != nil:
		if strings.EqualFold(function, "id") {
			return string(node.ID), true
		}
		return storage.NodeElementID(e.entityIdentityDatabase(ctx, node.ID), node.ID), true
	case edge != nil:
		if strings.EqualFold(function, "id") {
			return string(edge.ID), true
		}
		return storage.RelationshipElementID(e.entityIdentityDatabase(ctx, storage.NodeID(edge.StartNode)), edge.ID), true
	}
	return nil, false
}

// entityIdentityDatabase is the database an entity identity should name: the
// statement's execution database, or — on a composite coordinator — the
// constituent that actually holds the entity (#745 §3).
func (e *StorageExecutor) entityIdentityDatabase(ctx context.Context, anchor storage.NodeID) string {
	db := e.executionDatabaseName(ctx)
	if e.dbManager != nil && e.dbManager.IsCompositeDatabase(db) {
		if composite, ok := e.storage.(*storage.CompositeEngine); ok {
			if constituent := composite.ConstituentDatabaseForNode(anchor); constituent != "" {
				return constituent
			}
		}
	}
	return db
}

// evaluatePatternPredicateValue is the value of a pattern predicate in a
// projection: a bare pattern (in grouping parentheses or not) is whether it
// matches, as EXISTS { pattern }; NOT, AND, OR and XOR over operands that
// hold one combine their values with Cypher's null logic (#907). handled is
// false for any other expression.
func (e *StorageExecutor) evaluatePatternPredicateValue(ctx context.Context, expr string, values pipelineRow) (value interface{}, ok, handled bool) {
	trimmed := strings.TrimSpace(expr)
	// A chain's first node closes before it ends, so these parentheses
	// only ever group.
	for len(trimmed) > 1 && trimmed[0] == '(' && findMatchingParen(trimmed, 0) == len(trimmed)-1 {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	if chainEnd, chain := relationshipChainEnd(trimmed, 0, len(trimmed)); chain && chainEnd == len(trimmed) {
		matched, _ := e.evaluateRowExistsPredicate(ctx, "EXISTS { "+trimmed+" }", values)
		return matched, true, true
	}
	logicalValue, logical, logicalOK, err := evaluateLogicalExpression(trimmed, func(operand string) (interface{}, bool, error) {
		operandValue, resolved := e.evaluateRowExpressionWithContext(ctx, operand, values)
		return operandValue, resolved, nil
	})
	if !logical {
		return nil, false, false
	}
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false, true
	}
	return logicalValue, logicalOK, true
}
