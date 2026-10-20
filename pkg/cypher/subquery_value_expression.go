package cypher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// A subquery expression (EXISTS { … }, COUNT { … }, COLLECT { … }) is a value
// like any other: it may appear inside list and map literals, arithmetic,
// comparisons, CASE, list-comprehension filters and function arguments, and
// as a SET / CREATE / MERGE property value. The row evaluator evaluates
// each one where it stands (evaluateRowSubqueryAtom), with the variables
// bound at that point and the statement context the row carries (#1055).

// subqueryExpression is one EXISTS / COUNT / COLLECT { body } occurrence.
type subqueryExpression struct {
	kind       string // EXISTS, COUNT or COLLECT
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
		keyword, open, isSubquery := subqueryExpressionKeywordAt(expr, i)
		if !isSubquery {
			i++
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
	}
	return found
}

// subqueryExpressionKeywordAt reports whether an EXISTS / COUNT / COLLECT
// subquery expression starts at expr[i]: the keyword, then its body's '{'
// (at open). It is the one reader of where a subquery expression starts.
func subqueryExpressionKeywordAt(expr string, i int) (keyword string, open int, ok bool) {
	if i < 0 || i > len(expr) || i > 0 && isIdentByte(expr[i-1]) {
		return "", 0, false
	}
	for _, keyword := range subqueryExpressionKeywords {
		end, matched := keywordMatchAt(expr, i, keyword, 0, len(keyword))
		if !matched {
			continue
		}
		if open := queryGapEnd(expr, end); open < len(expr) && expr[open] == '{' && subqueryExpressionKeywordBeforeBrace(expr, i, end, open) {
			return keyword, open, true
		}
	}
	return "", 0, false
}

// nestedSubqueryExpressions returns the subquery expressions of expr when
// there is at least one and expr isn't a single subquery expression.
func nestedSubqueryExpressions(expr string) []subqueryExpression {
	if !mayContainSubqueryExpression(expr) {
		return nil
	}
	found := findSubqueryExpressions(expr)
	if len(found) == 0 || (len(found) == 1 && found[0].start == 0 && found[0].end == len(expr)) {
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
	if len(found) != 1 || found[0].start != 0 || found[0].end != len(expr) {
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

// rowSubqueryPlan is the shape of a predicate holding subquery expressions,
// read once per predicate text: the subqueries, and the predicate with each
// one masked by a name (__subquery_value_N), on which logical and
// integerComparison are read. logical is true when the predicate has a
// top-level AND / OR / XOR, which splits it before its subqueries run, so each
// side's subquery only runs when its value is needed.
type rowSubqueryPlan struct {
	found     []subqueryExpression
	names     []string
	rewritten string
	logical   bool
	// wholeExists is set for a predicate that is exactly NOT EXISTS { … },
	// which the predicate evaluator tests directly (evaluateRowExistsPredicate).
	wholeExists bool
	// integerComparison is set when the rewritten predicate is exactly
	// __subquery_value_0 <op> <integer literal> (COUNT { … } > 1): the
	// comparison is parsed once and applied to an integer value directly.
	integerComparison *integerComparison
}

// integerComparison is a comparison with an integer literal, parsed once.
type integerComparison struct {
	operator string
	literal  int64
}

// parseIntegerComparison parses "<op> <integer literal>".
func parseIntegerComparison(comparison string) (*integerComparison, bool) {
	comparison = strings.TrimSpace(comparison)
	for _, operator := range [...]string{"<>", "!=", "<=", ">=", "=", "<", ">"} {
		if !strings.HasPrefix(comparison, operator) {
			continue
		}
		literal, err := strconv.ParseInt(strings.TrimSpace(comparison[len(operator):]), 10, 64)
		if err != nil {
			return nil, false
		}
		return &integerComparison{operator: operator, literal: literal}, true
	}
	return nil, false
}

// holds compares value with the literal, as the comparison evaluator does for
// two integers.
func (c *integerComparison) holds(value int64) bool {
	matched, _ := compareCypherPredicateValue(value, c.literal, c.operator).(bool)
	return matched
}

// rowSubqueryPlans caches row subquery plans by expression text: an
// expression is planned once, not once per row. A nil plan (no subquery in a
// larger expression) is cached too.
var rowSubqueryPlans = newBoundedCache[string, *rowSubqueryPlan](1024, boundedCacheStringShardIndex)

// planRowSubqueries returns expr's plan when it holds subquery expressions
// inside a larger expression (nestedSubqueryExpressions), and nil otherwise.
func planRowSubqueries(expr string) *rowSubqueryPlan {
	if !mayContainSubqueryExpression(expr) {
		return nil
	}
	plan, cached := rowSubqueryPlans.get(expr)
	if cached {
		return plan
	}
	if found := nestedSubqueryExpressions(expr); found != nil {
		plan = &rowSubqueryPlan{found: found, names: make([]string, len(found))}
		var rewritten strings.Builder
		cursor := 0
		for index, subquery := range found {
			plan.names[index] = "__subquery_value_" + strconv.Itoa(index)
			rewritten.WriteString(expr[cursor:subquery.start])
			rewritten.WriteString(plan.names[index])
			cursor = subquery.end
		}
		rewritten.WriteString(expr[cursor:])
		plan.rewritten = rewritten.String()
		for _, operator := range []string{" AND ", " OR ", " XOR "} {
			if _, _, split := splitByOperatorWithOptions(plan.rewritten, operator, true, true); split {
				plan.logical = true
				break
			}
		}
		_, plan.wholeExists = wholeExistsPredicate(expr)
		if !plan.logical && len(found) == 1 && strings.HasPrefix(plan.rewritten, plan.names[0]) {
			plan.integerComparison, _ = parseIntegerComparison(plan.rewritten[len(plan.names[0]):])
		}
	}
	rowSubqueryPlans.put(expr, plan)
	return plan
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
		if strings.HasPrefix(name, "\x00") {
			continue // the evaluator's own keys (rowStatementContextKey) aren't variables
		}
		correlated.fabricRecordBindings[name] = value
	}
	return correlated
}

// rowSubqueryAtom reads an expression that is one EXISTS / COUNT / COLLECT
// { … } subquery: its keyword and the position of its {. The one reader of
// the form, for evaluateRowSubqueryAtom and the compiled row programs.
func rowSubqueryAtom(expr string) (kind string, open int, ok bool) {
	if expr == "" {
		return "", 0, false
	}
	switch expr[0] {
	case 'E', 'e', 'C', 'c':
	default:
		return "", 0, false
	}
	kind, open, ok = subqueryExpressionKeywordAt(expr, 0)
	if !ok || findMatchingDelimiter(expr, open, '{', '}') != len(expr)-1 {
		return "", 0, false
	}
	return kind, open, true
}

// evaluateRowSubqueryAtom evaluates an expression that is one EXISTS / COUNT /
// COLLECT { … } subquery for the row evaluator, with the row's variables, in
// the statement context the row carries (rowStatementContextKey). Nested in
// a larger expression, the subquery is reached by the row evaluator's own
// recursion, so it sees the variables bound at that point (a quantifier's, a
// comprehension's, reduce's). matched is false for any other expression and
// when the row carries no statement context.
func (e *StorageExecutor) evaluateRowSubqueryAtom(expr string, values map[string]interface{}) (interface{}, bool, error) {
	kind, open, ok := rowSubqueryAtom(expr)
	if !ok {
		return nil, false, nil
	}
	ctx := rowStatementContext(values)
	if ctx == nil {
		return nil, false, nil
	}
	value, resolved, err := e.rowSubqueryValue(ctx, kind, strings.TrimSpace(expr[open+1:len(expr)-1]), values)
	if err != nil {
		return nil, true, err
	}
	if !resolved {
		return nil, false, nil
	}
	return value, true, nil
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
	ctx = withoutLocalSubqueryScope(ctx)
	// EXISTS is whether the body has a row and COUNT how many: one
	// evaluation, so a body gives EXISTS and COUNT the same rows.
	if kind == "EXISTS" || kind == "COUNT" {
		if count, ok := e.boundDegreeCount(ctx, body, values); ok {
			return rowCountValue(kind, count), true, nil
		}
		if count, ok := e.boundChainCount(ctx, body, values, kind == "EXISTS"); ok {
			return rowCountValue(kind, count), true, nil
		}
		if count, ok := e.boundNodeLookupCount(ctx, body, values, kind == "EXISTS"); ok {
			return rowCountValue(kind, count), true, nil
		}
		if nodes, rels, ok := boundPatternCountBindings(body, values); ok {
			// A pattern the kernel can't match goes to the pipeline below:
			// its failure isn't a count of 0.
			if rows, matched := e.boundPatternRows(ctx, body, nodes, rels, values); matched {
				return rowCountValue(kind, int64(len(rows))), true, nil
			}
		}
	}
	if cypherVersionFromContext(ctx) != "25" {
		grouped, err := cypher5ImportGrouping(body, values)
		if err != nil {
			return nil, false, err
		}
		body = grouped
	}
	query := rowSubqueryQueryFor(ctx, kind, body)
	result, err := e.runCorrelatedSubquery(ctx, query, values)
	if err != nil {
		return nil, false, err
	}
	if result == nil {
		return nil, false, nil
	}
	if kind == "EXISTS" || kind == "COUNT" {
		return rowCountValue(kind, int64(len(result.Rows))), true, nil
	}
	// A COLLECT subquery collects one value per row: its RETURN has
	// exactly one column (Neo4j 5.26 rejects any other RETURN, #907).
	if len(result.Columns) != 1 {
		return nil, false, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery",
			localization.CypherSubqueriesCollectSingleColumn(len(result.Columns)))
	}
	collected := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			collected = append(collected, row[0])
		}
	}
	return collected, true, nil
}

// rowCountValue is a subquery's value from its number of rows: EXISTS is
// whether it has one, COUNT the number.
func rowCountValue(kind string, rows int64) interface{} {
	if kind == "EXISTS" {
		return rows > 0
	}
	return rows
}

// runCorrelatedSubquery runs a subquery body for a row: as a pipeline
// correlated with the row's values, and through the full executor with those
// values bound when the pipeline declines the body (a CALL subquery or a
// procedure call in it, …). EXISTS, COUNT and COLLECT all run their bodies
// here, so a body gives the same rows whichever of them wraps it.
func (e *StorageExecutor) runCorrelatedSubquery(ctx context.Context, query string, values map[string]interface{}) (*ExecuteResult, error) {
	// The current expression row owns this invocation. Enclosing value maps
	// must not overwrite a local binding while the child pipeline is seeded.
	ctx = withValueBindings(ctx, values)
	// A UNION body: each branch runs for the row, and the shared UNION
	// combiner (executeUnionBranches) joins them, as for a top-level UNION.
	if _, unionAll, _, union := parseTopLevelUnionBranchesFor(ctx, query); union {
		return e.executeUnionBranches(ctx, query, unionAll, func(branch string) (*ExecuteResult, error) {
			return e.runCorrelatedSubquery(ctx, branch, values)
		})
	}
	outcome := e.correlatedSubqueryExecutor(ctx, values).executePipeline(ctx, query)
	if outcome.terminal() {
		return outcome.result, outcome.err
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
	used := fabric.BranchBindings(query)
	names := make([]string, 0, len(values))
	for name := range values {
		if !strings.HasPrefix(name, "$") && used[name] {
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

// boundChainCount counts the rows of a body that is one relationship chain,
// with an optional WHERE, starting at a node bound in values: it traverses
// from that node and keeps the paths that agree with the row's other nodes
// and relationships and pass the WHERE. With first it stops at the first row
// (EXISTS). ok is false for any other body, which the traversal kernel or
// the correlated pipeline evaluates: a pattern list, a body reading a value
// of the row that isn't a node or relationship, a property map reading the
// row, a shortest path, or no bound start node.
func (e *StorageExecutor) boundChainCount(ctx context.Context, body string, values map[string]interface{}, first bool) (int64, bool) {
	shape := boundChainShapeFor(ctx, body)
	if !shape.ok {
		return 0, false
	}
	pattern, where := shape.pattern, shape.where
	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil || !chainVariablesBindEntities(matches, values) {
		return 0, false
	}
	seed, _ := values[matches.StartNode.variable].(*storage.Node)
	if seed == nil {
		return 0, false
	}
	if count, expanded := e.boundExpandCount(ctx, matches, seed, where, values, first); expanded {
		return count, true
	}
	return e.boundTraversalCount(ctx, matches, seed, where, values, first), true
}

// boundNodeLookupCount counts the rows of a body that is one labelled node
// pattern with a property map, MATCH (v:L {k: <expression>}) [WHERE …],
// whose variable the row doesn't bind, as Neo4j plans [NOT] EXISTS over it:
// SemiApply over a node index seek. The map's values are evaluated against
// the row, the candidates are the pipeline's own (its index seek, or its
// label scan), each is checked as the pipeline's node match checks it, and
// the WHERE runs on the row with the node, through the predicate plan
// already built for its text. EXISTS stops at the first match. It replaces
// a correlated pipeline run per row, which plans the WHERE's text again for
// every row; ok is false for any other body, a value of the map the row
// can't resolve, and under a temporal viewport.
func (e *StorageExecutor) boundNodeLookupCount(ctx context.Context, body string, values map[string]interface{}, first bool) (int64, bool) {
	shape := clauseAnalysisFor(ctx, clauseAnalysisBoundNode, body, parseBoundNodeShape)
	if !shape.ok {
		return 0, false
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return 0, false
	}
	if shape.variable != "" {
		if _, bound := values[shape.variable]; bound {
			return 0, false
		}
	}
	properties := make(map[string]interface{}, len(shape.keys))
	for index, key := range shape.keys {
		value, resolved, err := e.evaluateRowValue(shape.values[index], values)
		if err != nil || !resolved {
			return 0, false
		}
		properties[key] = normalizePropValue(value)
	}
	pattern := nodePatternInfo{variable: shape.variable, labels: shape.labels, properties: properties}
	nodes, _, err := e.collectPipelineInitialNodeCandidates(ctx, pattern, "", pipelineMatchPhysicalHint{})
	if err != nil {
		return 0, false
	}
	// The candidates and the set that drops duplicates count while the
	// semi-join runs (#1343).
	memory := trackedRetention(ctx)
	defer memory.release()
	if memory.reserveSlots(len(nodes), entityReferenceBytes+mapEntryBytes+stringHeaderBytes) != nil {
		return 0, true
	}
	hideSystemNodes := shouldHideSystemNodes(e.storage)
	seen := make(map[storage.NodeID]struct{}, len(nodes))
	var row map[string]interface{}
	var count int64
	for _, node := range nodes {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			continue
		}
		if _, duplicate := seen[node.ID]; duplicate {
			continue
		}
		seen[node.ID] = struct{}{}
		if !mergeNodeHasLabels(node, pattern.labels) || !e.nodeMatchesProps(node, pattern.properties) {
			continue
		}
		if shape.where != "" {
			if row == nil {
				row = make(map[string]interface{}, len(values)+1)
				for name, value := range values {
					row[name] = value
				}
			}
			if shape.variable != "" {
				row[shape.variable] = node
			}
			if !e.evaluateRowPredicate(ctx, shape.where, row) {
				continue
			}
		}
		count++
		if first {
			return count, true
		}
	}
	return count, true
}

// boundNodeShape is the text part of boundNodeLookupCount: the node
// pattern's variable, labels and map (keys and value expressions), and the
// body's WHERE.
type boundNodeShape struct {
	variable string
	labels   []string
	keys     []string
	values   []string
	where    string
	ok       bool
}

func parseBoundNodeShape(body string) boundNodeShape {
	pattern := strings.TrimSpace(body)
	if hasPrefixFold(pattern, "MATCH ") {
		pattern = strings.TrimSpace(pattern[len("MATCH"):])
	}
	if clauses, ok := splitPipelineClauses("MATCH " + pattern); !ok || len(clauses) != 1 || clauses[0].kind != pipelineClauseMatch {
		return boundNodeShape{}
	}
	where := ""
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = strings.TrimSpace(pattern[index+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:index])
	}
	if !strings.HasPrefix(pattern, "(") || findMatchingParen(pattern, 0) != len(pattern)-1 || containsRelationshipPattern(pattern) ||
		extractPathAssignmentVariable(pattern) != "" || indexASCIIFold(body, "shortestpath") >= 0 || indexASCIIFold(body, repeatableElementsFunction) >= 0 {
		return boundNodeShape{}
	}
	head, props := splitNodePatternProperties(pattern)
	// Plain labels only: a label expression, a dynamic label or an inline
	// WHERE is the pipeline's.
	if props == "" || strings.ContainsAny(head, "|&!%$()") || indexASCIIFold(head, "WHERE") >= 0 || indexASCIIFold(head, " IS ") >= 0 {
		return boundNodeShape{}
	}
	variable, labels, labelErr := parseNodeHead(head)
	if labelErr != nil || len(labels) == 0 {
		return boundNodeShape{}
	}
	inner, enclosed := stripEnclosingRowDelimiter(strings.TrimSpace(props), '{', '}')
	if !enclosed || strings.TrimSpace(inner) == "" {
		return boundNodeShape{}
	}
	shape := boundNodeShape{variable: variable, labels: labels, where: where, ok: true}
	seen := map[string]bool{}
	for _, pair := range splitTopLevelComma(inner) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			return boundNodeShape{}
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
		value := strings.TrimSpace(pair[separator+1:])
		if key == "" || value == "" || seen[key] {
			return boundNodeShape{}
		}
		seen[key] = true
		shape.keys = append(shape.keys, key)
		shape.values = append(shape.values, value)
	}
	return shape
}

// boundTraversalCount is boundChainCount for any chain: the paths the
// traversal finds from seed that agree with the row's nodes and
// relationships and pass where.
func (e *StorageExecutor) boundTraversalCount(ctx context.Context, matches *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) int64 {
	var paths []PathResult
	if matches.IsChained && len(matches.Segments) > 1 {
		paths = e.traverseChainedGraph(ctx, matches, []*storage.Node{seed})
	} else {
		paths = e.traverseFromNode(ctx, seed, matches)
	}
	var count int64
	for _, path := range paths {
		inner := e.buildPathContext(path, matches)
		correlated := true
		for name, node := range inner.nodes {
			if bound, isNode := values[name].(*storage.Node); isNode && (bound == nil || node == nil || bound.ID != node.ID) {
				correlated = false
				break
			}
		}
		for name, relationship := range inner.rels {
			if !correlated {
				break
			}
			if bound, isRel := values[name].(*storage.Edge); isRel && (bound == nil || relationship == nil || bound.ID != relationship.ID) {
				correlated = false
				break
			}
		}
		if !correlated {
			continue
		}
		if where != "" {
			// The WHERE sees the row's values with the path's bindings.
			row := make(map[string]interface{}, len(values)+len(inner.nodes)+len(inner.rels))
			for name, value := range values {
				row[name] = value
			}
			for name, node := range inner.nodes {
				row[name] = node
			}
			for name, relationship := range inner.rels {
				row[name] = relationship
			}
			if !e.evaluateRowPredicate(ctx, where, row) {
				continue
			}
		}
		count++
		if first {
			return count
		}
	}
	return count
}

// boundExpandCount is boundChainCount for one relationship of length one,
// (a)-[r]-(b), as Neo4j plans [NOT] EXISTS over it: SemiApply over Expand
// (Into when b is bound too). The relationships are read once from the end
// that has fewer of them, a candidate's far end is compared by ID before
// any node is read, a bound end is the row's own node (no node read), the
// WHERE's row is built once for every candidate, and EXISTS stops at the
// first match. It gives the rows the traversal below gives the same body;
// expanded is false for any other chain, and under a temporal viewport,
// which the traversal handles.
func (e *StorageExecutor) boundExpandCount(ctx context.Context, matches *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) (count int64, expanded bool) {
	rel := matches.Relationship
	if matches.IsChained || rel.VariableLength || rel.MinHops != 1 || rel.MaxHops != 1 {
		return 0, false
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return 0, false
	}
	if !e.traversalStartMatches(seed, matches.StartNode) {
		return 0, true
	}
	// chainVariablesBindEntities admitted only entities of the right kind:
	// a bound null matches no path.
	var end *storage.Node
	if name := matches.EndNode.variable; name != "" {
		if value, bound := values[name]; bound {
			if end, _ = value.(*storage.Node); end == nil {
				return 0, true
			}
		}
	}
	var boundRel *storage.Edge
	if name := rel.Variable; name != "" {
		if value, bound := values[name]; bound {
			if boundRel, _ = value.(*storage.Edge); boundRel == nil {
				return 0, true
			}
		}
	}
	// With both ends bound, expand from the end with fewer relationships in
	// the direction it sees them: the same relationships either way.
	from, target := seed, storage.NodeID("")
	if end != nil {
		target = end.ID
		reversed := reverseRelationshipDirection(rel.Direction)
		if end.ID != seed.ID && e.relationshipDegree(end.ID, reversed) < e.relationshipDegree(seed.ID, rel.Direction) {
			from, target, rel.Direction = end, seed.ID, reversed
		}
	}
	traversal := e.newMatchTraversalContext(ctx, from, matches, &rel)
	var edges []*storage.Edge
	switch rel.Direction {
	case "outgoing":
		edges = e.traversalEdges(traversal, from.ID, true)
	case "incoming":
		edges = e.traversalEdges(traversal, from.ID, false)
	default:
		var err error
		if edges, err = undirectedIncidentEdges(e.storage, from.ID); err != nil {
			recordExpressionFailure(ctx, err)
			return 0, true
		}
	}
	// The relationships it reads count while the semi-join runs (#1343).
	memory := trackedRetention(ctx)
	defer memory.release()
	if memory.reserveSlots(len(edges), entityReferenceBytes) != nil {
		return 0, true
	}
	var row map[string]interface{}
	for index, edge := range edges {
		if index&bfsCancelCheckMask == 0 && traversal.traversalCancelled() {
			return count, true
		}
		if len(rel.Types) > 0 {
			if len(rel.Types) == 1 {
				if edge.Type != rel.Types[0] {
					continue
				}
			} else if _, ok := traversal.relTypeSet[edge.Type]; !ok {
				continue
			}
		}
		if boundRel != nil && boundRel.ID != edge.ID {
			continue
		}
		if len(rel.Properties) > 0 && !e.edgeMatchesProps(edge, rel.Properties) {
			continue
		}
		far := edge.StartNode
		if rel.Direction == "outgoing" || (rel.Direction == "both" && edge.StartNode == from.ID) {
			far = edge.EndNode
		}
		endNode := end
		if target != "" {
			if far != target {
				continue
			}
		} else {
			loaded, ok := e.loadTraversalEndpointNode(traversal, far)
			if !ok {
				continue
			}
			endNode = loaded
		}
		if !e.matchesEndPattern(endNode, &matches.EndNode) {
			continue
		}
		if where != "" {
			// The WHERE sees the row's values with the relationship's bindings.
			if row == nil {
				row = make(map[string]interface{}, len(values)+4)
				for name, value := range values {
					row[name] = value
				}
				row[matches.StartNode.variable] = seed
				row[rowEndpointMemoKey] = make(map[rowEndpointMemoEntry]interface{})
			}
			if name := matches.EndNode.variable; name != "" {
				row[name] = endNode
			}
			if rel.Variable != "" {
				row[rel.Variable] = edge
			}
			if !e.evaluateRowPredicate(ctx, where, row) {
				continue
			}
		}
		count++
		if first {
			return count, true
		}
	}
	return count, true
}

// reverseRelationshipDirection is a relationship pattern's direction read
// from its other end.
func reverseRelationshipDirection(direction string) string {
	switch direction {
	case "outgoing":
		return "incoming"
	case "incoming":
		return "outgoing"
	}
	return direction
}

// relationshipDegree is how many relationships a node has in direction: an
// estimate to choose the cheaper end of an expansion by, never a result.
func (e *StorageExecutor) relationshipDegree(id storage.NodeID, direction string) int {
	switch direction {
	case "outgoing":
		return e.storage.GetOutDegree(id)
	case "incoming":
		return e.storage.GetInDegree(id)
	}
	return e.storage.GetOutDegree(id) + e.storage.GetInDegree(id)
}

// boundChainShape is the text part of boundChainCount: a body that is one
// relationship chain with an optional WHERE (ok), split into the two.
type boundChainShape struct {
	pattern string
	where   string
	ok      bool
}

func parseBoundChainShape(body string) boundChainShape {
	pattern := strings.TrimSpace(body)
	if hasPrefixFold(pattern, "MATCH ") {
		pattern = strings.TrimSpace(pattern[len("MATCH"):])
	}
	// One MATCH clause, its WHERE included: a RETURN or any other clause is
	// the kernel's or the pipeline's.
	if clauses, ok := splitPipelineClauses("MATCH " + pattern); !ok || len(clauses) != 1 || clauses[0].kind != pipelineClauseMatch {
		return boundChainShape{}
	}
	where := ""
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = strings.TrimSpace(pattern[index+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:index])
	}
	// A match mode or path selector (REPEATABLE ELEMENTS, ANY SHORTEST)
	// precedes the pattern; only the pipeline's MATCH runs those.
	if !strings.HasPrefix(pattern, "(") || len(splitTopLevelComma(pattern)) != 1 || !containsRelationshipPattern(pattern) ||
		extractPathAssignmentVariable(pattern) != "" || !patternMapsAreConstant(pattern) ||
		indexASCIIFold(body, "shortestpath") >= 0 || indexASCIIFold(body, repeatableElementsFunction) >= 0 {
		return boundChainShape{}
	}
	return boundChainShape{pattern: pattern, where: where, ok: true}
}

func boundChainShapeFor(ctx context.Context, body string) boundChainShape {
	return clauseAnalysisFor(ctx, clauseAnalysisBoundChain, body, parseBoundChainShape)
}

// patternMapsAreConstant reports whether every property map in a node or
// relationship pattern of pattern holds only literals and parameters
// ((n {id: 1}), ()-[:R {k: $k}]->()): boundChainCount's traversal matches
// those maps only. A map value that reads a variable ((n {id: r.prop}),
// (n {id: i + 1})) goes to the kernel or the pipeline, which evaluate it.
func patternMapsAreConstant(pattern string) bool {
	if constant, cached := patternMapsConstant.get(pattern); cached {
		return constant
	}
	constant := patternMapsAreConstantText(pattern)
	patternMapsConstant.put(pattern, constant)
	return constant
}

// patternMapsConstant caches patternMapsAreConstant by pattern text: a
// pattern is read once, not once per row.
var patternMapsConstant = newBoundedCache[string, bool](1024, boundedCacheStringShardIndex)

func patternMapsAreConstantText(pattern string) bool {
	depth := 0
	for index := 0; index < len(pattern); index++ {
		switch c := pattern[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(pattern, index, c) - 1
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '{':
			if depth <= 0 {
				continue
			}
			close := findMatchingDelimiter(pattern, index, '{', '}')
			if close < 0 {
				return false
			}
			for _, pair := range splitTopLevelComma(pattern[index+1 : close]) {
				separator := findTopLevelMapKeyValueSeparator(pair)
				if separator <= 0 || !isConstantMapValue(pair[separator+1:]) {
					return false
				}
			}
			index = close
		}
	}
	return true
}

// isConstantMapValue reports a property map value that is a literal or a
// parameter.
func isConstantMapValue(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) > 1 && value[0] == '$' && isValidIdentifier(value[1:]) {
		return true
	}
	if inner, list := stripEnclosingRowDelimiter(value, '[', ']'); list {
		if strings.TrimSpace(inner) == "" {
			return true
		}
		for _, item := range splitTopLevelComma(inner) {
			if !isConstantMapValue(item) {
				return false
			}
		}
		return true
	}
	_, literal := parseLiteralValueFromComputedRow(value)
	return literal
}

// chainVariablesBindEntities reports whether every node and relationship
// variable of a parsed chain that the row binds holds an entity of that kind:
// boundChainCount compares the traversal with the row's nodes and
// relationships only.
func chainVariablesBindEntities(matches *TraversalMatch, values map[string]interface{}) bool {
	node := func(info nodePatternInfo) bool {
		if info.variable == "" {
			return true
		}
		value, bound := values[info.variable]
		if !bound {
			return true
		}
		_, isNode := value.(*storage.Node)
		return isNode
	}
	relationship := func(pattern RelationshipPattern) bool {
		if pattern.Variable == "" {
			return true
		}
		value, bound := values[pattern.Variable]
		if !bound {
			return true
		}
		_, isEdge := value.(*storage.Edge)
		return isEdge
	}
	if !node(matches.StartNode) || !node(matches.EndNode) || !relationship(matches.Relationship) {
		return false
	}
	for _, info := range matches.IntermediateNodes {
		if !node(info) {
			return false
		}
	}
	for _, segment := range matches.Segments {
		if !node(segment.FromNode) || !node(segment.ToNode) || !relationship(segment.Relationship) {
			return false
		}
	}
	return true
}

// entityRow is the row an expression evaluated over node and relationship
// bindings sees, as its subqueries see it: the value scope of ctx (the row's
// other values, a comprehension's or quantifier's variable) with the nodes
// and relationships, which shadow it. Every subquery the node and
// relationship evaluator runs reads its row from here.
func entityRow(ctx context.Context, nodes map[string]*storage.Node, rels map[string]*storage.Edge) pipelineRow {
	values := pipelineRow(valueBindingsLayer(ctx, len(nodes)+len(rels)))
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

// entityRowWithPaths is entityRow with the path variables bound too, as a
// path is in a pipeline row.
func (e *StorageExecutor) entityRowWithPaths(ctx context.Context, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult) pipelineRow {
	values := entityRow(ctx, nodes, rels)
	for name, path := range paths {
		if path != nil {
			values[name] = e.pathContextRowValue(path)
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
// patternHasInlineWhere reports a WHERE inside a node or relationship
// pattern of text ((n WHERE n.x > 1), -[r WHERE r.w = 2]->), outside quoted
// text.
func patternHasInlineWhere(text string) bool {
	depth := 0
	for index := 0; index < len(text); index++ {
		switch character := text[index]; character {
		case '\'', '"', '`':
			index = skipCypherQuotedText(text, index, character) - 1
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case 'W', 'w':
			if depth > 0 && matchKeywordAt(text, index, "WHERE") && (index == 0 || !isIdentByte(text[index-1])) {
				return true
			}
		}
	}
	return false
}

func boundPatternCountBindings(body string, values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge, bool) {
	query := strings.TrimSpace(body)
	if !hasPrefixFold(query, "MATCH") {
		query = "MATCH " + query
	}
	clauses, ok := splitPipelineClauses(query)
	if !ok || len(clauses) != 1 || clauses[0].kind != pipelineClauseMatch {
		return nil, nil, false
	}
	// The bound-pattern matcher doesn't read a node's or relationship's own
	// WHERE ((m:Coll WHERE m.id IN $ids)); the pipeline does.
	if patternHasInlineWhere(query) {
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
		if (start == 0 || !isIdentByte(text[start-1])) && (end == len(text) || !isIdentByte(text[end])) {
			return true
		}
		from = start + 1
	}
	return false
}

// boundDegreePattern is a COUNT body that is one directed hop from a bound
// node to an anonymous, unlabelled node with no properties and no WHERE:
// [MATCH] (v)-[:T|U]->(), (v)<-[:T]-(), ()-[:T]->(v), with the arrows in
// either spelling (-->, -[r]->). Its count is the node's degree, unless the
// relationship variable is bound in the row (boundDegreeCount).
var boundDegreePattern = regexp.MustCompile(`^(?i:MATCH\s+)?(?:\(\s*([A-Za-z_]\w*)\s*\)\s*(<?)-(?:\[\s*([A-Za-z_]\w*)?\s*(?::\s*([A-Za-z_\w|\s` + "`" + `]+?))?\s*\])?-(>?)\s*\(\s*\)|\(\s*\)\s*(<?)-(?:\[\s*([A-Za-z_]\w*)?\s*(?::\s*([A-Za-z_\w|\s` + "`" + `]+?))?\s*\])?-(>?)\s*\(\s*([A-Za-z_]\w*)\s*\))\s*$`)

// boundDegreeShape is a parsed boundDegreePattern body: the bound variable,
// the hop's direction from it and its relationship types. ok is false for any
// other body.
type boundDegreeShape struct {
	variable     string
	relationship string // the hop's relationship variable, or ""
	outgoing     bool
	types        []string
	ok           bool
}

// boundDegreeShapes caches parsed COUNT bodies; a body is parsed once, not
// once per row.
var boundDegreeShapes = newBoundedCache[string, boundDegreeShape](1024, boundedCacheStringShardIndex)

// parseBoundDegreeShape parses body as a boundDegreePattern (cached).
func parseBoundDegreeShape(body string) boundDegreeShape {
	if shape, cached := boundDegreeShapes.get(body); cached {
		return shape
	}
	shape := parseBoundDegreeShapeText(body)
	boundDegreeShapes.put(body, shape)
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
	variable, relationship, types := match[1], match[3], match[4]
	outgoing := match[2] == "" && match[5] == ">"
	incoming := match[2] == "<" && match[5] == ""
	if variable == "" {
		// ()-[:T]->(v) is an incoming hop of v.
		variable, relationship, types = match[10], match[7], match[8]
		outgoing = match[6] == "<" && match[9] == ""
		incoming = match[6] == "" && match[9] == ">"
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
	return boundDegreeShape{variable: variable, relationship: relationship, outgoing: outgoing, types: relTypes, ok: true}
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
	if shape.relationship != "" {
		// A relationship variable bound in the row restricts the hop to
		// that relationship: not a degree (#1055).
		if _, bound := values[shape.relationship]; bound {
			return 0, false
		}
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

// rowSubqueryQuery is the query a subquery body runs as: a body that is
// only a pattern is matched as MATCH <pattern>, and a body without RETURN
// returns a row per match (one row for EXISTS).
func rowSubqueryQuery(kind, body string) string {
	query := strings.TrimSpace(body)
	if strings.HasPrefix(query, "(") {
		query = "MATCH " + query
	}
	if topLevelKeywordIndex(query, "RETURN") < 0 {
		switch kind {
		case "COUNT":
			query += " RETURN 1 AS __count"
		case "EXISTS":
			query += " RETURN 1 AS __exists LIMIT 1"
		}
	}
	return query
}

// rowSubqueryQueryFor is rowSubqueryQuery from body's plan in ctx.
func rowSubqueryQueryFor(ctx context.Context, kind, body string) string {
	analysis := clauseAnalysisSubqueryQueryCollect
	switch kind {
	case "EXISTS":
		analysis = clauseAnalysisSubqueryQueryExists
	case "COUNT":
		analysis = clauseAnalysisSubqueryQueryCount
	case "COLLECT":
	default:
		return rowSubqueryQuery(kind, body)
	}
	plan := pipelineClausePlanFor(ctx, body)
	if plan == nil {
		return rowSubqueryQuery(kind, body)
	}
	return plannedAnalysis(plan, analysis, body, func(body string) string { return rowSubqueryQuery(kind, body) })
}
