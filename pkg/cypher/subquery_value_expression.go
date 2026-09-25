package cypher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

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

// wholeExistsPredicate reports whether a WHERE predicate is exactly
// EXISTS { … } or NOT EXISTS { … }, and which. Only such a predicate is a
// subquery test on its own; one that compares or combines the subquery
// (EXISTS { … } = false) is an expression with the subquery as one of its
// values.
func wholeExistsPredicate(predicate string) (negated bool, ok bool) {
	predicate = strings.TrimSpace(predicate)
	if len(predicate) > len("NOT") && matchKeywordAt(predicate, 0, "NOT") {
		return true, isStandaloneExistsSubquery(strings.TrimSpace(predicate[len("NOT"):]))
	}
	return false, isStandaloneExistsSubquery(predicate)
}

// isWholeCollectItem reports whether a projection item is exactly one
// COLLECT { … } subquery, rather than an expression containing one.
func isWholeCollectItem(expr string) bool {
	collect, ok := standaloneSubqueryExpression(expr)
	return ok && collect.kind == "COLLECT"
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
	value, ok, err := e.rowSubqueryValue(ctx, kind, body, values)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil, false
	}
	return value, ok
}

// rowSubqueryValue is evaluateRowSubqueryValue returning the subquery's error
// instead of recording it. It is the one evaluator of EXISTS / COUNT /
// COLLECT subqueries: the body runs as a pipeline, so its WHERE, ORDER BY,
// SKIP / LIMIT and RETURN all apply.
func (e *StorageExecutor) rowSubqueryValue(ctx context.Context, kind, body string, values map[string]interface{}) (interface{}, bool, error) {
	if kind == "COMPREHENSION" {
		value, ok := e.evaluateRowComprehensionWithSubqueries(ctx, body, values)
		return value, ok, nil
	}
	if kind == "EXISTS" {
		value, ok := e.evaluateRowExistsPredicate(ctx, "EXISTS {"+body+"}", values)
		return value, ok, nil
	}
	if kind == "COUNT" {
		if count, ok := e.boundDegreeCount(ctx, body, values); ok {
			return count, true, nil
		}
		if nodes, rels, ok := boundPatternCountBindings(body, values); ok {
			return int64(len(e.evaluateBoundPatternRows(ctx, body, nodes, rels))), true, nil
		}
	}
	query := strings.TrimSpace(body)
	patternOnly := strings.HasPrefix(query, "(")
	if patternOnly {
		query = "MATCH " + query
	}
	if kind == "COUNT" && topLevelKeywordIndex(query, "RETURN") < 0 {
		query += " RETURN 1 AS __count"
	}
	result, err := e.runCorrelatedSubquery(ctx, query, values)
	if err != nil {
		return nil, false, err
	}
	if result == nil {
		return nil, false, nil
	}
	if kind == "COUNT" {
		return int64(len(result.Rows)), true, nil
	}
	collected := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			collected = append(collected, row[0])
		}
	}
	return collected, true, nil
}

// runCorrelatedSubquery runs a subquery body for a row: as a pipeline
// correlated with the row's values, and through the full executor with those
// values bound when the pipeline declines the body (a CALL subquery or a
// procedure call in it, …). EXISTS, COUNT and COLLECT all run their bodies
// here, so a body gives the same rows whichever of them wraps it.
func (e *StorageExecutor) runCorrelatedSubquery(ctx context.Context, query string, values map[string]interface{}) (*ExecuteResult, error) {
	result, handled, err := e.correlatedSubqueryExecutor(ctx, values).executePipeline(ctx, query)
	if err != nil {
		return nil, err
	}
	if handled && result != nil {
		return result, nil
	}
	return e.executeCorrelatedSubqueryBody(ctx, query, values)
}

// executeCorrelatedSubqueryBody runs a subquery body through the full
// executor with the row's values it uses bound the way MATCH … CALL binds a
// correlated subquery's imports: a node as MATCH (v) WHERE id(v) = $p, a
// relationship as MATCH ()-[v]->() WHERE id(v) = $p, any other value as
// WITH $p AS v. Row parameters stay available.
func (e *StorageExecutor) executeCorrelatedSubqueryBody(ctx context.Context, query string, values map[string]interface{}) (*ExecuteResult, error) {
	params := make(map[string]interface{}, len(values))
	for name, value := range getParamsFromContext(ctx) {
		params[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		if !strings.HasPrefix(name, "$") && containsIdentifierWord(query, name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var prefix strings.Builder
	carried := make([]string, 0, len(names))
	for index, name := range names {
		param := fmt.Sprintf("__subquery_row_%d", index)
		switch value := values[name].(type) {
		case *storage.Node:
			if value == nil {
				params[param] = nil
				carried = append(carried, "$"+param+" AS "+name)
				continue
			}
			params[param] = string(value.ID)
			fmt.Fprintf(&prefix, "MATCH (%s) WHERE id(%s) = $%s ", name, name, param)
			carried = append(carried, name)
		case *storage.Edge:
			if value == nil {
				params[param] = nil
				carried = append(carried, "$"+param+" AS "+name)
				continue
			}
			params[param] = string(value.ID)
			fmt.Fprintf(&prefix, "MATCH ()-[%s]->() WHERE id(%s) = $%s ", name, name, param)
			carried = append(carried, name)
		default:
			params[param] = value
			carried = append(carried, "$"+param+" AS "+name)
		}
	}
	if len(carried) > 0 {
		prefix.WriteString("WITH " + strings.Join(carried, ", ") + " ")
	}
	return e.executeInternal(ctx, prefix.String()+query, params)
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

// boundPatternCountBindings reports whether a COUNT { body } is one pattern
// with an optional WHERE ((a)-->(b) WHERE b.x > 1, with or without MATCH)
// whose outer variables are all nodes or relationships, and returns them. Such
// a count is the number of rows the traversal kernel expands from the bound
// entities (evaluateBoundPatternRows, as for COUNT predicates in WHERE),
// without running the body as a correlated pipeline per row.
func boundPatternCountBindings(body string, values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge, bool) {
	query := strings.TrimSpace(body)
	if !hasPrefixFold(query, "MATCH") {
		query = "MATCH " + query
	}
	clauses, ok := splitPipelineClauses(query)
	if !ok || len(clauses) != 1 || clauses[0].kind != pipelineClauseMatch {
		return nil, nil, false
	}
	for name, value := range values {
		switch value.(type) {
		case *storage.Node, *storage.Edge:
			continue
		}
		if !strings.HasPrefix(name, "$") && containsIdentifierWord(body, name) {
			return nil, nil, false
		}
	}
	nodes, rels := entityBindings(values)
	return nodes, rels, true
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

// boundDegreePattern is a COUNT body that is one directed hop from a bound
// node to an anonymous, unlabelled node with no properties and no WHERE:
// [MATCH] (v)-[:T|U]->(), (v)<-[:T]-(), ()-[:T]->(v), with the arrows in
// either spelling (-->, -[r]->). Its count is the node's degree.
var boundDegreePattern = regexp.MustCompile(`^(?i:MATCH\s+)?(?:\(\s*([A-Za-z_]\w*)\s*\)\s*(<?)-(?:\[\s*(?:[A-Za-z_]\w*)?\s*(?::\s*([A-Za-z_\w|\s` + "`" + `]+?))?\s*\])?-(>?)\s*\(\s*\)|\(\s*\)\s*(<?)-(?:\[\s*(?:[A-Za-z_]\w*)?\s*(?::\s*([A-Za-z_\w|\s` + "`" + `]+?))?\s*\])?-(>?)\s*\(\s*([A-Za-z_]\w*)\s*\))\s*$`)

// boundDegreeShape is a parsed boundDegreePattern body: the bound variable,
// the hop's direction from it and its relationship types. ok is false for any
// other body.
type boundDegreeShape struct {
	variable string
	outgoing bool
	types    []string
	ok       bool
}

// boundDegreeShapes caches parsed COUNT bodies; a body is parsed once, not
// once per row. The cache stops growing at boundDegreeShapeCacheLimit bodies.
var (
	boundDegreeShapesMu sync.RWMutex
	boundDegreeShapes   = make(map[string]boundDegreeShape)
)

const boundDegreeShapeCacheLimit = 1024

// parseBoundDegreeShape parses body as a boundDegreePattern (cached).
func parseBoundDegreeShape(body string) boundDegreeShape {
	boundDegreeShapesMu.RLock()
	shape, cached := boundDegreeShapes[body]
	boundDegreeShapesMu.RUnlock()
	if cached {
		return shape
	}
	shape = parseBoundDegreeShapeText(body)
	boundDegreeShapesMu.Lock()
	if len(boundDegreeShapes) < boundDegreeShapeCacheLimit {
		boundDegreeShapes[body] = shape
	}
	boundDegreeShapesMu.Unlock()
	return shape
}

func parseBoundDegreeShapeText(body string) boundDegreeShape {
	trimmed := strings.TrimSpace(body)
	if len(trimmed) == 0 || strings.ContainsAny(trimmed, "{*,") {
		return boundDegreeShape{}
	}
	match := boundDegreePattern.FindStringSubmatch(trimmed)
	if match == nil {
		return boundDegreeShape{}
	}
	variable, types := match[1], match[3]
	outgoing := match[2] == "" && match[4] == ">"
	incoming := match[2] == "<" && match[4] == ""
	if variable == "" {
		// ()-[:T]->(v) is an incoming hop of v.
		variable, types = match[8], match[6]
		outgoing = match[5] == "<" && match[7] == ""
		incoming = match[5] == "" && match[7] == ">"
	}
	if outgoing == incoming {
		return boundDegreeShape{}
	}
	var relTypes []string
	for _, relType := range strings.Split(types, "|") {
		if relType = strings.Trim(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(relType), ":")), "`"); relType != "" {
			relTypes = append(relTypes, relType)
		}
	}
	return boundDegreeShape{variable: variable, outgoing: outgoing, types: relTypes, ok: true}
}

// boundDegreeCount counts COUNT { (v)-[:T]->() } for a node bound in values
// from its adjacency, without the traversal kernel. It takes only the exact
// boundDegreePattern shape: a far end with a label, properties or a WHERE,
// more hops, or an undirected hop go through the kernel, which applies them.
func (e *StorageExecutor) boundDegreeCount(ctx context.Context, body string, values map[string]interface{}) (int64, bool) {
	shape := parseBoundDegreeShape(body)
	if !shape.ok {
		return 0, false
	}
	node, ok := values[shape.variable].(*storage.Node)
	if !ok || node == nil {
		return 0, false
	}
	return e.nodeDegreeCount(ctx, node, shape), true
}

// nodeDegreeCount is the degree of node along shape's direction and types.
func (e *StorageExecutor) nodeDegreeCount(ctx context.Context, node *storage.Node, shape boundDegreeShape) int64 {
	store := e.getStorage(ctx)
	var edges []*storage.Edge
	if shape.outgoing {
		edges, _ = store.GetOutgoingEdges(node.ID)
	} else {
		edges, _ = store.GetIncomingEdges(node.ID)
	}
	if len(shape.types) == 0 {
		return int64(len(edges))
	}
	var count int64
	for _, edge := range edges {
		if e.edgeTypeMatches(edge.Type, shape.types) {
			count++
		}
	}
	return count
}
