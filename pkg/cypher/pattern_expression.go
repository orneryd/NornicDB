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
			if parenDepth == 0 && bracketDepth == 1 && braceDepth == 0 {
				pattern := strings.TrimSpace(expr[1:i])
				projection := strings.TrimSpace(expr[i+1 : len(expr)-1])
				if !strings.Contains(strings.ToUpper(pattern), " IN ") &&
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
	if strings.HasPrefix(strings.ToUpper(pattern), "MATCH ") {
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
		if value, evaluated := e.evaluateRowExpression(projection, scope); evaluated {
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
func (e *StorageExecutor) evaluateRowExpressionWithContext(ctx context.Context, expr string, values pipelineRow) (interface{}, bool) {
	if plan := planRowSubqueries(strings.TrimSpace(expr)); plan != nil {
		rewritten, extended := e.materializeRowSubqueries(ctx, plan, values)
		return e.evaluateRowExpressionWithContext(ctx, rewritten, extended)
	}
	if subquery, ok := standaloneSubqueryExpression(expr); ok {
		return e.evaluateRowSubqueryValue(ctx, subquery.kind, subquery.body, values)
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
	value, resolved := e.evaluateRowExpression(expr, values)
	// A =~ with a non-string operand or an invalid pattern evaluates to null
	// in the row evaluator; report it as the statement error.
	if strings.Contains(expr, "=~") {
		e.recordRowRegexFailure(ctx, expr, values)
	}
	if !resolved {
		if e.recordRowSizeArgumentFailure(ctx, expr, values) {
			return nil, false
		}
		// The row evaluator reports an operator error (division by zero,
		// INTEGER overflow, an operand of the wrong type) as "unresolved";
		// record the statement error of the operator that failed.
		e.recordRowOperatorFailure(ctx, expr, values)
	}
	if function, arguments, functionCall := parseFunctionCallWS(strings.TrimSpace(expr)); functionCall && strings.EqualFold(function, "substring") {
		parts := splitTopLevelComma(arguments)
		if len(parts) == 2 || len(parts) == 3 {
			for _, argument := range parts[1:] {
				position, valid := e.evaluateRowExpression(argument, values)
				if numeric, ok := toInt(position); valid && ok && numeric < 0 {
					recordExpressionFailure(ctx, newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidSubstringIndex", "Cannot handle negative start index nor negative length"))
					break
				}
			}
		}
	}
	return value, resolved
}

// recordRowRegexFailure records the error of a top-level text =~ pattern
// whose operands are not strings or whose pattern is invalid.
func (e *StorageExecutor) recordRowRegexFailure(ctx context.Context, expr string, values pipelineRow) {
	expression := strings.TrimSpace(expr)
	for {
		inner, enclosed := stripEnclosingExpressionParentheses(expression)
		if !enclosed {
			break
		}
		expression = inner
	}
	left, right, regex := splitByOperatorWithOptions(expression, "=~", false, true)
	if !regex {
		return
	}
	leftValue, leftOK := e.evaluateRowExpression(left, values)
	rightValue, rightOK := e.evaluateRowExpression(right, values)
	if !leftOK || !rightOK {
		return
	}
	if _, err := cypherRegexMatch(leftValue, rightValue); err != nil {
		recordExpressionFailure(ctx, err)
	}
}
