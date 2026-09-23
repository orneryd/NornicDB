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
	if pattern, projection, ok := splitPatternComprehension(expr); ok {
		return e.evaluatePatternComprehensionFromRow(ctx, pattern, projection, values), true
	}
	return e.evaluateRowExpression(expr, values)
}
