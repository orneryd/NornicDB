package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A subquery expression (EXISTS { … }, COUNT { … }, COLLECT { … }) is a value
// like any other: it may appear inside list and map literals, arithmetic,
// comparisons, CASE, list-comprehension filters and function arguments, and
// as a SET / CREATE / MERGE property value. The expression evaluators replace
// each nested subquery expression with a row variable holding its value for
// the current row (materializeRowSubqueries), then evaluate the rest of the
// expression normally. A subquery expression that is the whole expression is
// evaluated directly by the evaluators.

// subqueryExpression is one EXISTS / COUNT / COLLECT { body } occurrence, or
// a list comprehension whose filter or projection contains one (kind
// COMPREHENSION, body the text between the brackets): its subqueries may use
// the comprehension variable, so it is evaluated element by element.
type subqueryExpression struct {
	kind       string // EXISTS, COUNT, COLLECT or COMPREHENSION
	start, end int    // expression text [start, end)
	body       string
}

var subqueryExpressionKeywords = [...]string{"EXISTS", "COUNT", "COLLECT"}

// mayContainSubqueryExpression is the cheap precheck the evaluators run on
// every expression: a subquery expression needs a brace and one of the
// keywords.
func mayContainSubqueryExpression(expr string) bool {
	if strings.IndexByte(expr, '{') < 0 {
		return false
	}
	for _, keyword := range subqueryExpressionKeywords {
		if containsFold(expr, keyword) {
			return true
		}
	}
	return false
}

// findSubqueryExpressions returns the outermost subquery expressions in expr,
// outside string literals and quoted names.
func findSubqueryExpressions(expr string) []subqueryExpression {
	var found []subqueryExpression
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '\'', '"':
			i = skipQuotedSemanticText(expr, i)
			continue
		case '`':
			if end := strings.IndexByte(expr[i+1:], '`'); end >= 0 {
				i += end + 2
				continue
			}
			return found
		}
		if expr[i] == '[' {
			if closing := findMatchingDelimiter(expr, i, '[', ']'); closing > i {
				inner := expr[i+1 : closing]
				if _, _, _, _, comprehension := parseListComprehension(inner); comprehension && mayContainSubqueryExpression(inner) {
					found = append(found, subqueryExpression{kind: "COMPREHENSION", start: i, end: closing + 1, body: inner})
					i = closing + 1
					continue
				}
			}
		}
		matched := false
		for _, keyword := range subqueryExpressionKeywords {
			if !matchKeywordAt(expr, i, keyword) {
				continue
			}
			open := skipSpaces(expr, i+len(keyword))
			if open >= len(expr) || expr[open] != '{' {
				continue
			}
			closing := findMatchingDelimiter(expr, open, '{', '}')
			if closing < 0 {
				return found
			}
			found = append(found, subqueryExpression{
				kind:  keyword,
				start: i,
				end:   closing + 1,
				body:  strings.TrimSpace(expr[open+1 : closing]),
			})
			i = closing + 1
			matched = true
			break
		}
		if !matched {
			i++
		}
	}
	return found
}

// nestedSubqueryExpressions returns the subquery expressions of expr when
// there is at least one and expr isn't a single subquery expression.
func nestedSubqueryExpressions(expr string) []subqueryExpression {
	if !mayContainSubqueryExpression(expr) {
		return nil
	}
	found := findSubqueryExpressions(expr)
	if len(found) == 0 || (len(found) == 1 && found[0].kind != "COMPREHENSION" && found[0].start == 0 && found[0].end == len(expr)) {
		return nil
	}
	return found
}

// standaloneSubqueryExpression returns expr's subquery expression when expr is
// exactly one EXISTS / COUNT / COLLECT { … }.
func standaloneSubqueryExpression(expr string) (subqueryExpression, bool) {
	expr = strings.TrimSpace(expr)
	if !mayContainSubqueryExpression(expr) {
		return subqueryExpression{}, false
	}
	found := findSubqueryExpressions(expr)
	if len(found) != 1 || found[0].kind == "COMPREHENSION" || found[0].start != 0 || found[0].end != len(expr) {
		return subqueryExpression{}, false
	}
	return found[0], true
}

// materializeRowSubqueries replaces each subquery expression in expr with a
// row variable bound to its value for the row, and returns the rewritten
// expression and the extended row.
func (e *StorageExecutor) materializeRowSubqueries(ctx context.Context, expr string, values pipelineRow, found []subqueryExpression) (string, pipelineRow) {
	extended := make(pipelineRow, len(values)+len(found))
	for name, value := range values {
		extended[name] = value
	}
	var rewritten strings.Builder
	cursor := 0
	for index, subquery := range found {
		name := "__subquery_value_" + strconv.Itoa(index)
		value, _ := e.evaluateRowSubqueryValue(ctx, subquery.kind, subquery.body, values)
		extended[name] = value
		rewritten.WriteString(expr[cursor:subquery.start])
		rewritten.WriteString(name)
		cursor = subquery.end
	}
	rewritten.WriteString(expr[cursor:])
	return rewritten.String(), extended
}

// correlatedSubqueryExecutor returns an executor whose pipeline sees the row's
// values as outer bindings (fabricRecordBindings), for a subquery body that
// refers to them.
func (e *StorageExecutor) correlatedSubqueryExecutor(ctx context.Context, values map[string]interface{}) *StorageExecutor {
	correlated := e.cloneWithStorage(e.getStorage(ctx))
	correlated.fabricRecordBindings = make(map[string]interface{}, len(e.fabricRecordBindings)+len(values))
	for name, value := range e.fabricRecordBindings {
		correlated.fabricRecordBindings[name] = value
	}
	for name, value := range values {
		correlated.fabricRecordBindings[name] = value
	}
	return correlated
}

// evaluateRowSubqueryValue evaluates EXISTS / COUNT / COLLECT { body } for a
// row, correlated with the row's values: EXISTS is whether the body has a row,
// COUNT how many rows it has, COLLECT the list of its single returned column.
// A body that is only a pattern is matched as MATCH <pattern>.
func (e *StorageExecutor) evaluateRowSubqueryValue(ctx context.Context, kind, body string, values map[string]interface{}) (interface{}, bool) {
	if kind == "COMPREHENSION" {
		return e.evaluateRowComprehensionWithSubqueries(ctx, body, values)
	}
	if kind == "EXISTS" {
		return e.evaluateRowExistsPredicate(ctx, "EXISTS {"+body+"}", values)
	}
	query := strings.TrimSpace(body)
	patternOnly := strings.HasPrefix(query, "(")
	if patternOnly {
		query = "MATCH " + query
	}
	if kind == "COUNT" && topLevelKeywordIndex(query, "RETURN") < 0 {
		query += " RETURN 1 AS __count"
	}
	result, handled, err := e.correlatedSubqueryExecutor(ctx, values).executePipeline(ctx, query)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	if !handled || result == nil {
		if kind == "COUNT" && (patternOnly || hasPrefixFold(strings.TrimSpace(body), "MATCH ")) {
			nodes, rels := entityBindings(values)
			return int64(len(e.evaluateBoundPatternRows(ctx, body, nodes, rels))), true
		}
		return nil, false
	}
	if kind == "COUNT" {
		return int64(len(result.Rows)), true
	}
	collected := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			collected = append(collected, row[0])
		}
	}
	return collected, true
}

// evaluateRowComprehensionWithSubqueries evaluates [x IN list WHERE p | f]
// whose filter or projection contains subquery expressions: each element
// binds x, so a subquery may use it (EXISTS { MATCH (i)-->(o) WHERE o.id = x }).
func (e *StorageExecutor) evaluateRowComprehensionWithSubqueries(ctx context.Context, inner string, values map[string]interface{}) (interface{}, bool) {
	variable, listExpression, predicate, projection, comprehension := parseListComprehension(inner)
	if !comprehension {
		return nil, false
	}
	listValue, ok := e.evaluateRowExpressionWithContext(ctx, listExpression, pipelineRow(values))
	if !ok {
		return nil, false
	}
	if listValue == nil {
		return nil, true
	}
	items := toAnySlice(listValue)
	out := make([]interface{}, 0, len(items))
	for _, item := range items {
		scope := make(pipelineRow, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[variable] = item
		if strings.TrimSpace(predicate) != "" && !e.evaluateRowPredicate(ctx, predicate, scope) {
			continue
		}
		if strings.TrimSpace(projection) == "" {
			out = append(out, item)
			continue
		}
		value, _ := e.evaluateRowExpressionWithContext(ctx, projection, scope)
		out = append(out, value)
	}
	return out, true
}

// subqueryReadsScalarRowValue reports whether a subquery body names a row
// value that isn't a node or relationship.
func subqueryReadsScalarRowValue(body string, values map[string]interface{}) bool {
	for name, value := range values {
		switch value.(type) {
		case *storage.Node, *storage.Edge:
			continue
		}
		if containsIdentifierWord(body, name) {
			return true
		}
	}
	return false
}

// entityRow is the row of an expression evaluated over node and relationship
// bindings.
func entityRow(nodes map[string]*storage.Node, rels map[string]*storage.Edge) pipelineRow {
	values := make(pipelineRow, len(nodes)+len(rels))
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
	return values
}

// entityBindings splits a row's node and relationship values out of it.
func entityBindings(values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge) {
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	for name, value := range values {
		switch entity := value.(type) {
		case *storage.Node:
			if entity != nil {
				nodes[name] = entity
			}
		case *storage.Edge:
			if entity != nil {
				rels[name] = entity
			}
		}
	}
	return nodes, rels
}

// containsIdentifierWord reports whether name occurs in text as a whole word.
func containsIdentifierWord(text, name string) bool {
	if name == "" {
		return false
	}
	for from := 0; from < len(text); {
		index := strings.Index(text[from:], name)
		if index < 0 {
			return false
		}
		start := from + index
		end := start + len(name)
		if (start == 0 || !isWordChar(text[start-1])) && (end == len(text) || !isWordChar(text[end])) {
			return true
		}
		from = start + 1
	}
	return false
}
