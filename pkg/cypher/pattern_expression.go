package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

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
				if !strings.Contains(upperASCII(pattern), " IN ") &&
					strings.Contains(pattern, "(") && strings.Contains(pattern, ")") && projection != "" {
					return pattern, projection, true
				}
			}
		}
	}
	return "", "", false
}

func standaloneCountSubquery(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if !hasSubqueryPattern(expr, countSubqueryRe) {
		return "", false
	}
	open := strings.Index(expr, "{")
	close := strings.LastIndex(expr, "}")
	if open < 0 || close <= open || strings.TrimSpace(expr[close+1:]) != "" {
		return "", false
	}
	return strings.TrimSpace(expr[open+1 : close]), true
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
// against the entities bound in the current row. It shares the WHERE
// predicate's correlated evaluation (evaluateRowExistsPredicate), so an EXISTS
// value and an EXISTS filter always agree.
func (e *StorageExecutor) evaluateExistsSubqueryValue(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) bool {
	values := make(map[string]interface{}, len(nodes)+len(rels))
	for name, node := range nodes {
		if node != nil {
			values[name] = node
		}
	}
	for name, relationship := range rels {
		if relationship != nil {
			values[name] = relationship
		}
	}
	matched, _ := e.evaluateRowExistsPredicate(ctx, expr, values)
	return matched
}

func (e *StorageExecutor) evaluateBoundPatternRows(ctx context.Context, pattern string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) []traversalOptRow {
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(upperASCII(pattern), "MATCH ") {
		pattern = strings.TrimSpace(pattern[len("MATCH "):])
	}
	clauses := splitOptionalMatchClauses(pattern)
	if len(clauses) != 1 {
		return nil
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

	expanded, err := e.applyTraversalOptionalClause(ctx, []traversalOptRow{seed}, clauses[0])
	if err != nil {
		return nil
	}
	matches := expanded[:0]
	for _, row := range expanded {
		if row.optionalMatched {
			matches = append(matches, row)
		}
	}
	return matches
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
	rows := e.evaluateBoundPatternRows(ctx, pattern, nodes, rels)
	values := make([]interface{}, 0, len(rows))
	for _, row := range rows {
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
		value, evaluated, err := e.evaluateRowValue(projection, scope)
		if err != nil {
			recordExpressionFailure(ctx, err)
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

func (e *StorageExecutor) evaluateRowExpressionWithContext(ctx context.Context, expr string, values pipelineRow) (interface{}, bool) {
	if value, bound := evaluateBoundRowValue(expr, values); bound {
		return value, true
	}
	if containsRelExistencePattern(expr) {
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
	if containsStatementClockCall(expr) {
		bind(temporalRowContextKey, ctx)
	}
	if extended != nil {
		values = extended
	}
	// A row variable, or a plain property chain on one (e.uuid), resolves
	// without the graph-expression checks below, which can't match it.
	if value, bound := evaluateBoundRowValue(expr, values); bound {
		return value, true
	}
	if plan := planRowSubqueries(strings.TrimSpace(expr)); plan != nil {
		rewritten, extended := e.materializeRowSubqueries(ctx, plan, values)
		return e.evaluateRowExpressionWithContext(ctx, rewritten, extended)
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
		return e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, values), true
	}
	if trimmed := strings.TrimSpace(expr); isStandaloneExistsSubquery(trimmed) {
		matched, _ := e.evaluateRowExistsPredicate(ctx, trimmed, values)
		return matched, true
	}
	// Pattern comprehensions can be nested in scalar functions. Resolve the
	// graph-producing argument here, at the shared context-aware expression
	// boundary, before the allocation-conscious scalar evaluator takes over.
	if function, argument, ok := parseFunctionCallWS(strings.TrimSpace(expr)); ok && strings.EqualFold(function, "size") {
		if pattern, projection, comprehension := splitPatternComprehension(argument); comprehension {
			items := e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, values)
			return int64(len(items)), true
		}
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
	value, resolved, err := e.evaluateRowValue(expr, values)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	if !resolved && containsRelExistencePattern(expr) {
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
					recordExpressionFailure(ctx, newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidSubstringIndex", "Cannot handle negative start index nor negative length"))
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
		nodes, rels := entityScopesFromValues(values)
		return e.evaluateExistsSubqueryValue(ctx, "EXISTS { "+trimmed+" }", nodes, rels), true, true
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
