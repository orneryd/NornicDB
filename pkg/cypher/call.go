// CALL procedure implementations for NornicDB.
// This file contains all CALL procedures for Neo4j compatibility and NornicDB extensions.
//
// Core procedure implementation.
// =======================================
//
// Critical Neo4j-compatible procedures:
//   - db.index.vector.queryNodes - Vector similarity search with cosine/euclidean
//   - db.index.fulltext.queryNodes - Full-text search with BM25-like scoring
//   - apoc.path.subgraphNodes - Graph traversal with depth/filter control
//   - apoc.path.expand - Path expansion with relationship filters
//
// These procedures are essential for:
//   - Semantic search (vector similarity)
//   - Text search (full-text indexing)
//   - Knowledge graph traversal
//   - Memory relationship discovery

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// toFloat32Slice is a package-level alias to convert.ToFloat32Slice for internal use.
func toFloat32Slice(v interface{}) []float32 {
	return convert.ToFloat32Slice(v)
}

// yieldClause is a procedure call's YIELD: the yielded columns with their
// aliases, and the WHERE, ORDER BY, SKIP and LIMIT that may follow them, in
// that order (YIELD items [WHERE p] [ORDER BY o] [SKIP s] [LIMIT l]). They
// see the aliases, as a WITH's do. A RETURN after the YIELD isn't part of it:
// it starts the query's tail.
type yieldClause struct {
	items    []yieldItem // List of yielded items (possibly with aliases)
	yieldAll bool        // YIELD * - return all columns
	where    string      // WHERE predicate, "" when absent
	orderBy  string      // ORDER BY items, "" when absent
	skip     string      // SKIP expression, "" when absent
	limit    string      // LIMIT expression, "" when absent
}

// hasModifiers reports whether the YIELD filters, orders or pages its rows.
func (y *yieldClause) hasModifiers() bool {
	return y.where != "" || y.orderBy != "" || y.skip != "" || y.limit != ""
}

// yieldItem represents a single item in a YIELD clause
type yieldItem struct {
	name  string // Original column name from procedure
	alias string // Alias (empty if no AS clause)
}

type callSplit struct {
	callOnly string
	tail     string
}

// parseYieldClause projects the native declaration descriptor after the shared
// semantic owner has admitted the statement. Invalid syntax has no declaration
// facts; its error belongs to admission, not a guessed empty YIELD descriptor.
func parseYieldClause(cypher string) *yieldClause {
	y := fabric.ReadQueryStructure(cypher).YieldClause()
	if y == nil {
		return nil
	}
	result := &yieldClause{yieldAll: y.Star, where: y.Where, orderBy: y.OrderBy, skip: y.Skip, limit: y.Limit, items: []yieldItem{}}
	for _, item := range y.Items {
		result.items = append(result.items, yieldItem{name: item.Name, alias: item.Alias})
	}
	return result
}

func splitCallAndTail(cypher string) callSplit {
	if c := fabric.ReadQueryStructure(cypher).First("CALL"); c != nil {
		return callSplit{callOnly: c.Text, tail: c.Remaining}
	}
	return callSplit{callOnly: strings.TrimSpace(cypher)}
}

func buildCallTailPredicateInjection(tail string, predicates []string) string {
	if len(predicates) == 0 {
		return strings.TrimSpace(tail)
	}
	injected := strings.Join(predicates, " AND ")
	trimmed := strings.TrimSpace(tail)
	if q := fabric.ReadQueryStructure(tail); q.Starts("MATCH") || q.Starts("OPTIONAL MATCH") {
		c := q.Clauses[0]
		predicate := c.Predicate
		if predicate != "" {
			predicate += " AND "
		}
		return strings.TrimSpace(c.Kind + " " + c.Pattern + " WHERE " + predicate + injected + " " + c.Remaining)
	}

	whereIdx := findKeywordIndexInContext(trimmed, "WHERE")
	if whereIdx != -1 {
		endIdx := len(trimmed)
		for _, kw := range []string{"WITH", "RETURN", "ORDER", "SKIP", "LIMIT", "UNWIND", "SET", "REMOVE", "DELETE", "DETACH", "MERGE", "CREATE"} {
			if idx := findKeywordIndexInContext(trimmed, kw); idx != -1 && idx > whereIdx && idx < endIdx {
				endIdx = idx
			}
		}
		left := strings.TrimSpace(trimmed[:endIdx])
		right := strings.TrimSpace(trimmed[endIdx:])
		if right == "" {
			return left + " AND " + injected
		}
		return left + " AND " + injected + " " + right
	}

	insertIdx := len(trimmed)
	for _, kw := range []string{"WITH", "RETURN", "ORDER", "SKIP", "LIMIT", "UNWIND", "SET", "REMOVE", "DELETE", "DETACH", "MERGE", "CREATE"} {
		if idx := findKeywordIndexInContext(trimmed, kw); idx != -1 && idx < insertIdx {
			insertIdx = idx
		}
	}
	if insertIdx >= len(trimmed) {
		return trimmed + " WHERE " + injected
	}
	left := strings.TrimSpace(trimmed[:insertIdx])
	right := strings.TrimSpace(trimmed[insertIdx:])
	return left + " WHERE " + injected + " " + right
}

func tailStartsWithMatchClause(tail string) bool {
	if q := fabric.ReadQueryStructure(tail); q.Err == nil {
		return q.Starts("MATCH") || q.Starts("OPTIONAL MATCH")
	}
	trimmed := upperASCII(strings.TrimSpace(tail))
	return strings.HasPrefix(trimmed, "MATCH ") || strings.HasPrefix(trimmed, "OPTIONAL MATCH ")
}

// expectedReturnColumnsFromTail is the column names of the tail's RETURN:
// the one outside any CALL { } subquery, whose own RETURN is inside braces.
func expectedReturnColumnsFromTail(tail string) []string {
	q := fabric.ReadQueryStructure(tail)
	if c := q.ResultClause(); c != nil && c.Projection != nil && c.Projection.Star {
		return []string{"*"}
	}
	return q.ResultColumns()
}

func (e *StorageExecutor) executeCallTail(ctx context.Context, seed *ExecuteResult, tail string) (*ExecuteResult, error) {
	if seed == nil {
		return nil, localizedError(localization.CypherCommandRoutingCallTailSeedRequired(), nil)
	}
	if strings.TrimSpace(tail) == "" {
		return seed, nil
	}
	if projected, ok, err := e.projectCallTailReturnAll(seed, tail); ok || err != nil {
		return projected, err
	}
	// A tail that doesn't start with MATCH (WITH, UNWIND, RETURN, a chained
	// CALL, …) runs as one pipeline over every yielded row, so aggregation,
	// ORDER BY and SKIP / LIMIT see all of them. MATCH tails keep their fused
	// operators below and reach the pipeline before the per-row fallback.
	if !tailStartsWithMatchClause(tail) && !isPotentialWriteTail(tail) {
		// WITH … [WHERE] RETURN … projections keep their compiled row
		// projection, which declines aggregation, DISTINCT and SKIP / LIMIT
		// expressions.
		if projected, ok, err := e.tryExecuteCallTailProjectionFilter(ctx, seed, tail, expectedReturnColumnsFromTail(tail)); ok || err != nil {
			return projected, err
		}
		if pipelined, ok, err := e.executeCallTailPipeline(ctx, seed, tail); ok || err != nil {
			return pipelined, err
		}
	}
	if pipelined, ok, err := e.tryExecuteCallTailProcedurePipeline(ctx, seed, tail); ok || err != nil {
		return pipelined, err
	}
	if len(seed.Rows) == 0 {
		cols := expectedReturnColumnsFromTail(tail)
		if len(cols) == 0 {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		return &ExecuteResult{Columns: cols, Rows: [][]interface{}{}}, nil
	}

	expectedCols := expectedReturnColumnsFromTail(tail)
	usedCols := make([]int, 0, len(seed.Columns))
	for i, col := range seed.Columns {
		if isIdentifierReferenced(tail, col) {
			usedCols = append(usedCols, i)
		}
	}

	// Fast path: execute the full tail once with all seed rows bound via UNWIND
	// or batched IN. This avoids per-row tail re-execution and preserves global
	// ORDER/LIMIT scope. Write tails (SET/CREATE/DELETE/MERGE/REMOVE) must use
	// the per-row path to preserve transactional write-per-row semantics.
	if !isPotentialWriteTail(tail) {
		if projected, ok, err := e.tryExecuteCallTailRelationshipMatchProjection(ctx, seed, tail, expectedCols); ok || err != nil {
			return projected, err
		}
		if projected, ok, err := e.tryExecuteCallTailProjectionFilter(ctx, seed, tail, expectedCols); ok || err != nil {
			return projected, err
		}
	}
	if !isPotentialWriteTail(tail) {
		if pipelined, ok, err := e.executeCallTailPipeline(ctx, seed, tail); ok || err != nil {
			return pipelined, err
		}
	}
	// Fallback fast path for read-only tails the pipeline declines: execute
	// per-row tails concurrently. It runs the tail once per yielded row, so it
	// can't aggregate across rows.
	if len(seed.Rows) > 1 && !isPotentialWriteTail(tail) {
		if parallel, ok, err := e.executeCallTailParallel(ctx, seed, tail, usedCols, expectedCols); ok || err != nil {
			return parallel, err
		}
	}

	results := make([]callTailRowResult, len(seed.Rows))
	for i, row := range seed.Rows {
		res, err := e.executeCallTailSingleRow(ctx, seed.Columns, row, tail, usedCols, expectedCols)
		results[i] = callTailRowResult{idx: i, res: res, err: err}
		if err != nil {
			break
		}
	}
	return combineCallTailRowResults(results, expectedCols)
}

// executeCallTailPipeline runs the clauses after CALL … YIELD as one pipeline
// (runPipelineClauses) whose input rows are the procedure's yielded rows. It
// declines (ok false) when a tail clause isn't a pipeline clause.
func (e *StorageExecutor) executeCallTailPipeline(ctx context.Context, seed *ExecuteResult, tail string) (*ExecuteResult, bool, error) {
	clauses, ok := pipelineClausesFor(tail)
	if !ok || len(clauses) == 0 {
		return nil, false, nil
	}
	rows := make([]pipelineRow, 0, len(seed.Rows))
	for _, row := range seed.Rows {
		rows = append(rows, callTailRow(ctx, seed, row))
	}
	scope := make(map[string]struct{}, len(seed.Columns))
	for _, column := range seed.Columns {
		scope[column] = struct{}{}
	}
	result, ok, err := e.runPipelineClauses(ctx, rows, scope, clauses, clauses)
	if ok {
		e.markCallTailPipelineUsed()
	}
	return result, ok, err
}

func (e *StorageExecutor) tryExecuteCallTailProcedurePipeline(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
) (*ExecuteResult, bool, error) {
	callIndex := -1
	if q := fabric.ReadQueryStructure(tail); q.Err == nil {
		if c := q.First("CALL"); c != nil {
			callIndex = c.Start
		}
	} else {
		callIndex = findKeywordIndexInContext(tail, "CALL")
	}
	if callIndex <= 0 {
		return nil, false, nil
	}
	prefix := strings.TrimSpace(tail[:callIndex])
	if !hasPrefixFoldASCII(prefix, "WITH ") {
		return nil, false, nil
	}

	rows := make([]pipelineRow, 0, len(seed.Rows))
	for _, row := range seed.Rows {
		rows = append(rows, callTailRow(ctx, seed, row))
	}
	projected, ok := e.pipelineApplyWith(ctx, rows, prefix)
	if !ok {
		return nil, false, nil
	}
	prefixColumns, ok := callTailWithProjectionColumns(prefix)
	if !ok {
		return nil, false, nil
	}

	callParts := splitChainedProcedureCall(strings.TrimSpace(tail[callIndex:]))
	if strings.TrimSpace(callParts.tail) == "" {
		return nil, false, nil
	}
	combined := &ExecuteResult{Columns: append([]string(nil), prefixColumns...)}
	for _, bindings := range projected {
		procedureResult, err := e.executeProcedureCall(ctx, callParts.callOnly, true)
		if err != nil {
			return nil, true, err
		}
		if len(combined.Columns) == len(prefixColumns) {
			combined.Columns = append(combined.Columns, procedureResult.Columns...)
		}
		for _, procedureRow := range procedureResult.Rows {
			row := make([]interface{}, 0, len(prefixColumns)+len(procedureRow))
			for _, column := range prefixColumns {
				row = append(row, bindings[column])
			}
			row = append(row, procedureRow...)
			combined.Rows = append(combined.Rows, row)
		}
	}

	result, err := e.executeCallTail(ctx, combined, callParts.tail)
	return result, true, err
}

func splitChainedProcedureCall(cypher string) callSplit {
	return splitCallAndTail(cypher)
}

func callTailWithProjectionColumns(withClause string) ([]string, bool) {
	q := fabric.ReadQueryStructure(withClause)
	if !q.Starts("WITH") {
		return nil, false
	}
	columns := fabric.ProjectionColumns(q.Clauses[0].Projection)
	return columns, len(columns) > 0
}

func (e *StorageExecutor) projectCallTailReturnAll(seed *ExecuteResult, tail string) (*ExecuteResult, bool, error) {
	q := fabric.ReadQueryStructure(tail)
	if !q.Starts("RETURN") || len(q.Clauses) != 1 || q.Clauses[0].Projection == nil || !q.Clauses[0].Projection.Star {
		return nil, false, nil
	}
	modifiers := q.Clauses[0].Projection.Modifiers
	result := &ExecuteResult{
		Columns: append([]string(nil), seed.Columns...),
		Rows:    make([][]interface{}, len(seed.Rows)),
		Stats:   seed.Stats,
	}
	for index, row := range seed.Rows {
		result.Rows[index] = append([]interface{}(nil), row...)
	}

	if modifiers == "" {
		return result, true, nil
	}
	result, err := e.applyResultModifiers(context.Background(), result, modifiers)
	return result, true, err
}

type callTailRowResult struct {
	idx int
	res *ExecuteResult
	err error
}

func (e *StorageExecutor) executeCallTailParallel(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	usedCols []int,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	if len(usedCols) == 0 || len(seed.Rows) <= 1 {
		return nil, false, nil
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	if workers > len(seed.Rows) {
		workers = len(seed.Rows)
	}
	type job struct {
		idx int
		row []interface{}
	}
	jobs := make(chan job, len(seed.Rows))
	results := make(chan callTailRowResult, len(seed.Rows))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				res, err := e.executeCallTailSingleRow(ctx, seed.Columns, j.row, tail, usedCols, expectedCols)
				results <- callTailRowResult{idx: j.idx, res: res, err: err}
			}
		}()
	}
	for i, row := range seed.Rows {
		jobs <- job{idx: i, row: row}
	}
	close(jobs)
	wg.Wait()
	close(results)

	ordered := make([]callTailRowResult, len(seed.Rows))
	for rr := range results {
		ordered[rr.idx] = rr
	}
	combined, err := combineCallTailRowResults(ordered, expectedCols)
	return combined, true, err
}

// combineCallTailRowResults concatenates per-row CALL-tail results in row
// order, for the sequential and the parallel per-row paths alike (#547): the
// first failing row's error (in row order, so the parallel path reports the
// same one), and the tail's columns when no row produced any.
func combineCallTailRowResults(results []callTailRowResult, expectedCols []string) (*ExecuteResult, error) {
	var combined *ExecuteResult
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		if result.res == nil {
			continue
		}
		if combined == nil {
			combined = &ExecuteResult{
				Columns: append([]string{}, result.res.Columns...),
				Rows:    make([][]interface{}, 0, len(result.res.Rows)),
			}
		}
		combined.Rows = append(combined.Rows, result.res.Rows...)
	}
	if combined == nil {
		return &ExecuteResult{Columns: append([]string{}, expectedCols...), Rows: [][]interface{}{}}, nil
	}
	return combined, nil
}

func (e *StorageExecutor) executeCallTailSingleRow(
	ctx context.Context,
	seedCols []string,
	row []interface{},
	tail string,
	usedCols []int,
	expectedCols []string,
) (*ExecuteResult, error) {
	params := map[string]interface{}{}
	prefix := make([]string, 0, len(usedCols)+2)
	withBindings := make([]string, 0, len(usedCols))
	predicates := make([]string, 0, len(usedCols))
	tailIsMatch := tailStartsWithMatchClause(tail)

	for _, i := range usedCols {
		if i >= len(row) {
			continue
		}
		col := seedCols[i]
		val := row[i]
		if node, ok := val.(*storage.Node); ok {
			pname := "seed_id_" + col
			if node != nil {
				params[pname] = string(node.ID)
			} else {
				params[pname] = nil
			}
			if tailIsMatch {
				predicates = append(predicates, fmt.Sprintf("id(%s) = $%s", col, pname))
			} else {
				prefix = append(prefix, fmt.Sprintf("MATCH (%s) WHERE id(%s) = $%s", col, col, pname))
				withBindings = append(withBindings, col)
			}
			continue
		}
		pname := "seed_" + col
		params[pname] = val
		withBindings = append(withBindings, fmt.Sprintf("$%s AS %s", pname, col))
	}

	query := buildCallTailPredicateInjection(tail, predicates)
	if len(withBindings) > 0 {
		prefix = append(prefix, "WITH "+strings.Join(withBindings, ", "))
	}
	if len(prefix) > 0 {
		query = strings.Join(prefix, " ") + " " + query
	}

	inner, err := e.executeInternal(ctx, query, params)
	if err != nil {
		return nil, err
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(inner.Columns) {
		inner.Columns = append([]string{}, expectedCols...)
	}
	return inner, nil
}

func isPotentialWriteTail(tail string) bool {
	if q := fabric.ReadQueryStructure(tail); q.Err == nil {
		return q.Writes()
	}
	t := upperASCII(strings.TrimSpace(tail))
	return findKeywordIndexInContext(t, "CREATE") >= 0 ||
		findKeywordIndexInContext(t, "MERGE") >= 0 ||
		findKeywordIndexInContext(t, "DELETE") >= 0 ||
		findKeywordIndexInContext(t, "SET") >= 0 ||
		findKeywordIndexInContext(t, "REMOVE") >= 0
}

// callTailProjectionPlan is a CALL tail that is exactly WITH … [WHERE …]
// RETURN …, run over the yielded rows by the pipeline's WITH and RETURN
// appliers without the general pipeline's routing.
type callTailProjectionPlan struct {
	withClause   string
	returnClause string
	// limitToken / skipToken are the RETURN's LIMIT / SKIP, a literal or a
	// parameter, checked per execution (the parameter's value can change).
	limitToken string
	skipToken  string
	// passThroughWith is a WITH whose items are all row variables under
	// their own names, with no ORDER BY / SKIP / LIMIT / DISTINCT: it only
	// filters (withWhere). The plan then filters the rows with the pipeline's
	// row filter instead of rebuilding each one; the RETURN (never *) reads
	// only the variables the WITH keeps.
	passThroughWith bool
	withWhere       string
}

// Parsed CALL-tail plans, cached by tail text: a nil plan (a tail the plan
// doesn't cover) is cached too.
var (
	callTailProjectionPlans        = newBoundedCache[string, *callTailProjectionPlan](1024, boundedCacheStringShardIndex)
	callTailRelationshipMatchPlans = newBoundedCache[string, *callTailRelationshipMatchPlan](1024, boundedCacheStringShardIndex)
)

// callTailRelationshipMatchPlan is a CALL tail MATCH (a)-[r:T {key: y.p}]->(b)
// [WHERE …] WITH … RETURN … over a yielded relationship y: r is bound to y
// itself when its type and key property match, and a and b to its end nodes.
type callTailRelationshipMatchPlan struct {
	startVar       string
	startLabel     string
	relVar         string
	relType        string
	propertyKey    string
	propertyExpr   string
	propertySource string
	endVar         string
	endLabel       string
	whereClause    string
	projection     *callTailProjectionPlan
}

type callTailRelationshipPatternParts struct {
	startVar     string
	startLabel   string
	relVar       string
	relType      string
	propertyKey  string
	propertyExpr string
	endVar       string
	endLabel     string
	rest         string
}

func (e *StorageExecutor) tryExecuteCallTailProjectionFilter(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailProjectionPlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	rows := make([]pipelineRow, 0, len(seed.Rows))
	for _, seedRow := range seed.Rows {
		rows = append(rows, callTailRow(ctx, seed, seedRow))
	}
	result, ok := e.executeCallTailProjectionPlan(ctx, plan, rows, expectedCols)
	if !ok {
		return nil, false, nil
	}
	e.markCallTailProjectionFastPathUsed()
	return result, true, nil
}

// executeCallTailProjectionPlan applies the plan's WITH (and its WHERE), then
// its RETURN with ORDER BY / SKIP / LIMIT, with the pipeline's appliers. ok is
// false when an applier declines; nothing has been written then.
func (e *StorageExecutor) executeCallTailProjectionPlan(
	ctx context.Context,
	plan *callTailProjectionPlan,
	rows []pipelineRow,
	expectedCols []string,
) (*ExecuteResult, bool) {
	var projected []pipelineRow
	if plan.passThroughWith {
		projected = e.filterPipelineRows(ctx, rows, plan.withWhere)
	} else {
		var ok bool
		projected, ok = e.pipelineApplyWith(ctx, rows, plan.withClause)
		if !ok {
			return nil, false
		}
	}
	result, ok := e.pipelineApplyReturn(ctx, projected, plan.returnClause)
	if !ok {
		return nil, false
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(result.Columns) {
		result.Columns = append([]string{}, expectedCols...)
	}
	return result, true
}

func (e *StorageExecutor) tryExecuteCallTailRelationshipMatchProjection(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailRelationshipMatchPlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	rows := make([]pipelineRow, 0, len(seed.Rows))
	nodes := make(map[storage.NodeID]*storage.Node)
	getNode := func(id storage.NodeID) (*storage.Node, error) {
		if node := nodes[id]; node != nil {
			return node, nil
		}
		node, err := e.getQueryNode(e.storage, id)
		if err == nil && node != nil {
			nodes[id] = node
		}
		return node, err
	}
	for _, seedRow := range seed.Rows {
		values := callTailRow(ctx, seed, seedRow)
		relationship, ok := values[plan.propertySource].(*storage.Edge)
		if !ok || relationship == nil {
			continue
		}
		if plan.relType != "" && relationship.Type != plan.relType {
			continue
		}
		expected, _ := e.evaluateRowExpressionWithContext(ctx, plan.propertyExpr, values)
		if expected == nil || !e.compareEqual(relationship.Properties[plan.propertyKey], expected) {
			continue
		}
		startNode, err := getNode(relationship.StartNode)
		if err != nil || startNode == nil {
			continue
		}
		endNode, err := getNode(relationship.EndNode)
		if err != nil || endNode == nil {
			continue
		}
		if plan.startLabel != "" && !containsString(startNode.Labels, plan.startLabel) {
			continue
		}
		if plan.endLabel != "" && !containsString(endNode.Labels, plan.endLabel) {
			continue
		}
		matched := make(pipelineRow, util.SafePreallocSum(len(values), 3))
		for key, value := range values {
			matched[key] = value
		}
		matched[plan.startVar] = startNode
		matched[plan.relVar] = relationship
		matched[plan.endVar] = endNode
		if plan.whereClause != "" && !e.evaluateRowPredicate(ctx, plan.whereClause, matched) {
			continue
		}
		rows = append(rows, matched)
	}
	result, ok := e.executeCallTailProjectionPlan(ctx, plan.projection, rows, expectedCols)
	if !ok {
		return nil, false, nil
	}
	e.markCallTailProjectionFastPathUsed()
	return result, true, nil
}

// parseCallTailRelationshipMatchPlan returns the tail's relationship MATCH
// plan, parsed once per tail text, when its projection's LIMIT / SKIP resolve
// for this execution.
func (e *StorageExecutor) parseCallTailRelationshipMatchPlan(ctx context.Context, tail string) (*callTailRelationshipMatchPlan, bool) {
	plan, cached := callTailRelationshipMatchPlans.get(tail)
	if !cached {
		plan = e.planCallTailRelationshipMatch(tail)
		callTailRelationshipMatchPlans.put(tail, plan)
	}
	if plan == nil || !callTailPlanTokensResolve(ctx, plan.projection) {
		return nil, false
	}
	return plan, true
}

// planCallTailRelationshipMatch parses a relationship MATCH tail, or returns nil.
func (e *StorageExecutor) planCallTailRelationshipMatch(tail string) *callTailRelationshipMatchPlan {
	q := fabric.ReadQueryStructure(tail)
	if !q.Starts("MATCH") || q.Writes() {
		return nil
	}
	parts, ok := parseCallTailRelationshipPattern(q.Clauses[0].Text)
	if !ok {
		return nil
	}
	whereClause := q.Clauses[0].Predicate
	projection := e.planCallTailProjection(q.Clauses[0].Remaining)
	if projection == nil {
		return nil
	}
	propertyExpr := strings.TrimSpace(parts.propertyExpr)
	propertySource := ""
	if dotIdx := strings.Index(propertyExpr, "."); dotIdx > 0 {
		propertySource = strings.TrimSpace(propertyExpr[:dotIdx])
	}
	if parts.propertyKey == "" || propertySource == "" {
		return nil
	}
	return &callTailRelationshipMatchPlan{
		startVar:       parts.startVar,
		startLabel:     parts.startLabel,
		relVar:         parts.relVar,
		relType:        parts.relType,
		propertyKey:    parts.propertyKey,
		propertyExpr:   propertyExpr,
		propertySource: propertySource,
		endVar:         parts.endVar,
		endLabel:       parts.endLabel,
		whereClause:    whereClause,
		projection:     projection,
	}
}

func parseCallTailRelationshipPattern(tail string) (callTailRelationshipPatternParts, bool) {
	var parts callTailRelationshipPatternParts
	q := fabric.ReadQueryStructure(tail)
	if !q.Starts("MATCH") {
		return parts, false
	}
	rest := q.Clauses[0].Pattern
	startVar, startLabel, rest, ok := parseCallTailNodeToken(rest)
	if !ok {
		return parts, false
	}
	rest = strings.TrimLeftFunc(rest, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
	if !strings.HasPrefix(rest, "-") {
		return parts, false
	}
	rest = strings.TrimSpace(rest[1:])
	relInside, rest, ok := parseCallTailDelimited(rest, '[', ']')
	if !ok {
		return parts, false
	}
	relVar, relType, propertyKey, propertyExpr, ok := parseCallTailRelationshipToken(relInside)
	if !ok {
		return parts, false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "->") {
		return parts, false
	}
	rest = strings.TrimSpace(rest[2:])
	endVar, endLabel, rest, ok := parseCallTailNodeToken(rest)
	if !ok {
		return parts, false
	}
	parts.startVar = startVar
	parts.startLabel = startLabel
	parts.relVar = relVar
	parts.relType = relType
	parts.propertyKey = propertyKey
	parts.propertyExpr = propertyExpr
	parts.endVar = endVar
	parts.endLabel = endLabel
	if strings.TrimSpace(rest) != "" {
		return parts, false
	}
	parts.rest = q.Clauses[0].Remaining
	if q.Clauses[0].Predicate != "" {
		parts.rest = "WHERE " + q.Clauses[0].Predicate + " " + parts.rest
	}
	return parts, true
}

func parseCallTailNodeToken(input string) (variable, label, rest string, ok bool) {
	inside, rest, ok := parseCallTailDelimited(strings.TrimSpace(input), '(', ')')
	if !ok {
		return "", "", "", false
	}
	variable, label, ok = parseCallTailIdentifierAndOptionalType(inside)
	if !ok || variable == "" {
		return "", "", "", false
	}
	return variable, label, rest, true
}

func parseCallTailRelationshipToken(input string) (variable, relType, propertyKey, propertyExpr string, ok bool) {
	rest := strings.TrimSpace(input)
	variable, rest, ok = parseCallTailIdentifier(rest)
	if !ok || variable == "" {
		return "", "", "", "", false
	}
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, ":") {
		rest = strings.TrimSpace(rest[1:])
		relType, rest, ok = parseCallTailIdentifier(rest)
		if !ok || relType == "" {
			return "", "", "", "", false
		}
		rest = strings.TrimSpace(rest)
	}
	if rest == "" {
		return variable, relType, "", "", true
	}
	if !strings.HasPrefix(rest, "{") {
		return "", "", "", "", false
	}
	propertyKey, propertyExpr, rest, ok = parseCallTailSinglePropertyMap(rest)
	if !ok || strings.TrimSpace(rest) != "" {
		return "", "", "", "", false
	}
	return variable, relType, propertyKey, propertyExpr, true
}

func parseCallTailIdentifierAndOptionalType(input string) (variable, label string, ok bool) {
	rest := strings.TrimSpace(input)
	variable, rest, ok = parseCallTailIdentifier(rest)
	if !ok || variable == "" {
		return "", "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return variable, "", true
	}
	if !strings.HasPrefix(rest, ":") {
		return "", "", false
	}
	rest = strings.TrimSpace(rest[1:])
	label, rest, ok = parseCallTailIdentifier(rest)
	if !ok || label == "" || strings.TrimSpace(rest) != "" {
		return "", "", false
	}
	return variable, label, true
}

func parseCallTailIdentifier(input string) (string, string, bool) {
	return parseIdentifierToken(input)
}

func parseCallTailDelimited(input string, open, close byte) (inside, rest string, ok bool) {
	return extractDelimitedSection(strings.TrimSpace(input), rune(open), rune(close))
}

func parseCallTailSinglePropertyMap(input string) (key, expr, rest string, ok bool) {
	inside, rest, ok := parseCallTailDelimited(input, '{', '}')
	if !ok {
		return "", "", "", false
	}
	commaIdx := findTopLevelByte(inside, ',')
	if commaIdx >= 0 {
		return "", "", "", false
	}
	colonIdx := findTopLevelByte(inside, ':')
	if colonIdx <= 0 {
		return "", "", "", false
	}
	key = strings.TrimSpace(inside[:colonIdx])
	expr = strings.TrimSpace(inside[colonIdx+1:])
	if !isValidIdentifier(key) || expr == "" {
		return "", "", "", false
	}
	return key, expr, rest, true
}

func findTopLevelByte(input string, target byte) int {
	inSingle := false
	inDouble := false
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0
	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			parenDepth++
		case ch == ')':
			parenDepth--
		case ch == '[':
			bracketDepth++
		case ch == ']':
			bracketDepth--
		case ch == '{':
			braceDepth++
		case ch == '}':
			braceDepth--
		case ch == target && parenDepth == 0 && bracketDepth == 0 && braceDepth == 0:
			return i
		}
	}
	return -1
}

func resolveOptionalIntLiteralOrParam(ctx context.Context, raw string) (int, bool) {
	if strings.TrimSpace(raw) == "" {
		return -1, true
	}
	return resolveIntLiteralOrParam(ctx, raw)
}

func resolveIntLiteralOrParam(ctx context.Context, raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if strings.HasPrefix(raw, "$") {
		params := getParamsFromContext(ctx)
		if params == nil {
			return 0, false
		}
		value, ok := params[strings.TrimPrefix(raw, "$")]
		if !ok {
			return 0, false
		}
		switch typed := value.(type) {
		case int:
			return typed, true
		case int64:
			return int(typed), true
		case float64:
			return int(typed), true
		}
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return n, true
}

// callTailPlanClauseKeywords start the clauses that can follow a CALL's (or
// a SHOW command's) YIELD, ending its YIELD items and segments; a compiled
// CALL-tail projection plan can't hold them between its WITH and RETURN.
var callTailPlanClauseKeywords = []string{
	"MATCH", "OPTIONAL", "UNWIND", "CALL", "WITH", "CREATE", "MERGE",
	"SET", "DELETE", "DETACH", "REMOVE", "FOREACH", "LOAD", "UNION",
	// Cypher 25's clauses (#907).
	"FILTER", "LET", "FOR", "FINISH", "INSERT",
}

// parseCallTailProjectionPlan returns the tail's projection plan, parsed once
// per tail text, when its LIMIT / SKIP resolve to integers for this execution.
func (e *StorageExecutor) parseCallTailProjectionPlan(ctx context.Context, tail string) (*callTailProjectionPlan, bool) {
	plan, cached := callTailProjectionPlans.get(tail)
	if !cached {
		plan = e.planCallTailProjection(tail)
		callTailProjectionPlans.put(tail, plan)
	}
	if plan == nil || !callTailPlanTokensResolve(ctx, plan) {
		return nil, false
	}
	return plan, true
}

// callTailPlanTokensResolve reports whether the plan's LIMIT and SKIP are a
// literal or a bound integer parameter; other forms are the pipeline's.
func callTailPlanTokensResolve(ctx context.Context, plan *callTailProjectionPlan) bool {
	if _, ok := resolveOptionalIntLiteralOrParam(ctx, plan.limitToken); !ok {
		return false
	}
	_, ok := resolveOptionalIntLiteralOrParam(ctx, plan.skipToken)
	return ok
}

// planCallTailProjection parses a tail that is exactly WITH … [WHERE …]
// RETURN …, or returns nil.
func (e *StorageExecutor) planCallTailProjection(tail string) *callTailProjectionPlan {
	q := fabric.ReadQueryStructure(tail)
	if !q.Starts("WITH") || len(q.Clauses) != 2 || q.Clauses[1].Kind != "RETURN" {
		return nil
	}
	with, ret := q.Clauses[0].Projection, q.Clauses[1].Projection
	if with == nil || ret == nil || with.Star || ret.Star || with.Distinct || ret.Distinct || with.Aggregates || ret.Aggregates {
		return nil
	}
	for _, item := range ret.Order {
		if p := fabric.ProjectionSyntax(item.Expression); p != nil && p.Aggregates {
			return nil
		}
	}
	plan := &callTailProjectionPlan{withClause: q.Clauses[0].Text, returnClause: q.Clauses[1].Text, limitToken: ret.Limit, skipToken: ret.Skip, withWhere: q.Clauses[0].Predicate, passThroughWith: with.Modifiers == ""}
	for _, item := range with.Items {
		if item.Name == "" || (item.Alias != "" && item.Alias != item.Name) {
			plan.passThroughWith = false
		}
	}
	return plan
}

func cloneStringInterfaceMap(values map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func hasStarReturnItem(items []returnItem) bool {
	for _, item := range items {
		if strings.TrimSpace(item.expr) == "*" {
			return true
		}
	}
	return false
}

func splitCallTailProjectionModifiers(returnAndModifiers string) (projection, orderBy, limitToken, skipToken string) {
	if p := fabric.ProjectionSyntax(returnAndModifiers); p != nil {
		order := ""
		if p.OrderText != "" {
			order = "ORDER BY " + p.OrderText
		}
		projection = p.Text
		if p.Distinct {
			projection = "DISTINCT " + projection
		}
		return projection, order, p.Limit, p.Skip
	}
	projection = strings.TrimSpace(returnAndModifiers)
	modifierStart := len(projection)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(projection, kw); idx >= 0 && idx < modifierStart {
			modifierStart = idx
		}
	}
	modifiers := ""
	if modifierStart < len(projection) {
		modifiers = strings.TrimSpace(projection[modifierStart:])
		projection = strings.TrimSpace(projection[:modifierStart])
	}
	if modifiers == "" {
		return projection, "", "", ""
	}
	if orderIdx := topLevelKeywordIndex(modifiers, "ORDER BY"); orderIdx >= 0 {
		end := len(modifiers)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := topLevelKeywordIndex(modifiers[orderIdx:], kw); idx >= 0 {
				abs := orderIdx + idx
				if abs > orderIdx && abs < end {
					end = abs
				}
			}
		}
		orderBy = strings.TrimSpace(modifiers[orderIdx:end])
	}
	limitToken = extractCallTailModifierToken(modifiers, "LIMIT")
	skipToken = extractCallTailModifierToken(modifiers, "SKIP")
	return projection, orderBy, limitToken, skipToken
}

// extractCallTailModifierToken returns the whole value of keyword (SKIP or
// LIMIT) in modifiers, up to the next modifier: an expression such as
// LIMIT 0 + 1 stays whole, so the projection plan declines it instead of
// reading LIMIT 0 (#572).
func extractCallTailModifierToken(modifiers, keyword string) string {
	if p := fabric.ResultModifierSyntax(modifiers); p != nil {
		switch keyword {
		case "SKIP":
			return p.Skip
		case "LIMIT":
			return p.Limit
		}
		return ""
	}
	idx := topLevelKeywordIndex(modifiers, keyword)
	if idx < 0 {
		return ""
	}
	rest := modifiers[idx+len(keyword):]
	end := len(rest)
	for _, next := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if nextIdx := topLevelKeywordIndex(rest, next); nextIdx >= 0 && nextIdx < end {
			end = nextIdx
		}
	}
	return strings.TrimSpace(rest[:end])
}

func callTailHasRelationshipTypeConstraint(tail string) bool {
	inSingle := false
	inDouble := false
	for i := 0; i < len(tail); i++ {
		ch := tail[i]
		if inSingle {
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if ch == '\'' {
			inSingle = true
			continue
		}
		if ch == '"' {
			inDouble = true
			continue
		}
		if ch != '[' {
			continue
		}
		end := strings.IndexByte(tail[i+1:], ']')
		if end < 0 {
			return false
		}
		segment := tail[i+1 : i+1+end]
		if strings.Contains(segment, ":") {
			return true
		}
		i += end
	}
	return false
}

// callTailRow returns a yielded row as a pipeline row, with the statement's
// parameters bound as "$name" the way the statement pipeline binds them, so a
// typed parameter (a list or map the text substitution keeps typed) resolves
// in a CALL tail's WHERE and projections.
func callTailRow(ctx context.Context, seed *ExecuteResult, row []interface{}) pipelineRow {
	params := getParamsFromContext(ctx)
	values := make(pipelineRow, len(seed.Columns)+len(params))
	for i, col := range seed.Columns {
		if i < len(row) {
			values[col] = row[i]
		}
	}
	bindParameterRow(ctx, values)
	return values
}

func splitPathAssignment(patternPart string) (string, string, bool) {
	eqIdx := strings.Index(patternPart, "=")
	if eqIdx == -1 {
		return "", "", false
	}
	left := strings.TrimSpace(patternPart[:eqIdx])
	right := strings.TrimSpace(patternPart[eqIdx+1:])
	if left == "" || right == "" {
		return "", "", false
	}
	return left, right, true
}

// isIdentifierReferenced consumes native variable-role facts, never token presence.
func isIdentifierReferenced(query, identifier string) bool {
	return fabric.QueryMentionsVariable(query, identifier)
}

// findKeywordIndexInContext returns the index of keyword's first occurrence
// at the top level of s, or -1: topLevelKeywordIndex, the one scanner. A
// keyword inside parentheses, a list or braces (a function call's or list
// comprehension's WHERE, a subquery body) isn't the clause's; nor is one in a
// string, a quoted name or a comment, the WITH of STARTS WITH / ENDS WITH, or
// a clause keyword used as a name.
func findKeywordIndexInContext(s, keyword string) int {
	return topLevelKeywordIndex(s, keyword)
}

// applyYieldFilter applies a procedure call's YIELD to the procedure's rows.
// It selects and aliases the yielded columns, then filters the rows with the
// YIELD's WHERE and orders / pages them with its ORDER BY / SKIP / LIMIT the
// way a WITH does: through the pipeline, over the aliased rows, so the
// predicate sees the aliases and evaluates like any other WHERE (#530).
func (e *StorageExecutor) applyYieldFilter(ctx context.Context, result *ExecuteResult, yield *yieldClause) (*ExecuteResult, error) {
	if yield == nil {
		return result, nil
	}
	if err := validateYieldColumnsExist(result.Columns, yield); err != nil {
		return nil, err
	}

	// Apply column selection and aliasing (if not YIELD *)
	if !yield.yieldAll && len(yield.items) > 0 {
		colIndex := make(map[string]int, len(result.Columns))
		for i, col := range result.Columns {
			colIndex[col] = i
		}
		newColumns := make([]string, 0, len(yield.items))
		for _, item := range yield.items {
			if item.alias != "" {
				newColumns = append(newColumns, item.alias)
			} else {
				newColumns = append(newColumns, item.name)
			}
		}
		newRows := make([][]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			newRow := make([]interface{}, len(yield.items))
			for i, item := range yield.items {
				if idx, ok := colIndex[item.name]; ok && idx < len(row) {
					newRow[i] = row[idx]
				}
			}
			newRows = append(newRows, newRow)
		}
		result.Columns = newColumns
		result.Rows = newRows
	}
	if !yield.hasModifiers() {
		return result, nil
	}

	rows := make([]pipelineRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, callTailRow(ctx, result, row))
	}
	rows = e.filterPipelineRows(ctx, rows, yield.where)
	if yield.orderBy != "" || yield.skip != "" || yield.limit != "" {
		var with strings.Builder
		with.WriteString("WITH *")
		if yield.orderBy != "" {
			with.WriteString(" ORDER BY " + yield.orderBy)
		}
		if yield.skip != "" {
			with.WriteString(" SKIP " + yield.skip)
		}
		if yield.limit != "" {
			with.WriteString(" LIMIT " + yield.limit)
		}
		ordered, ok := e.pipelineApplyWith(ctx, rows, with.String())
		if !ok {
			// SKIP / LIMIT passed the compile-time checks (validateYieldModifiers),
			// so only a parameter can make them invalid here.
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentType",
				localization.CypherCoreYieldPaginationInvalid())
		}
		rows = ordered
	}
	result.Rows = make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			values[i] = row[column]
		}
		result.Rows = append(result.Rows, values)
	}
	return result, nil
}

func (e *StorageExecutor) executeCall(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return e.executeProcedureCall(ctx, cypher, false)
}

// executeProcedureCall runs a procedure call and the tail after it. inQuery is
// true when the call is part of a larger query whose other clauses the caller
// runs (MATCH … CALL, a CALL in a tail): its YIELD may then filter, order and
// page, which a standalone call can't (validateYieldModifiers).
func (e *StorageExecutor) executeProcedureCall(ctx context.Context, cypher string, inQuery bool) (*ExecuteResult, error) {
	// A RETURN right after YIELD is the start of the tail, like any other
	// clause, so it runs over all yielded rows (executeCallTail): an aggregate
	// in it groups the rows, and ORDER BY / SKIP / LIMIT apply to all of them.
	parts := splitChainedProcedureCall(cypher)
	callCypher := parts.callOnly
	tailCypher := parts.tail

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(callCypher)
	if err := e.validateYieldModifiers(yield, inQuery || strings.TrimSpace(tailCypher) != "" || endsInFinish(ctx, cypher)); err != nil {
		return nil, err
	}

	// Registry-first path: canonical procedure contract for built-ins and UDFs.
	ensureBuiltInProceduresRegistered()
	procName := extractProcedureName(callCypher)
	if proc, found := globalProcedureRegistry.Get(procName); found {
		hasTail := strings.TrimSpace(tailCypher) != ""
		if err := validateProcedureArgumentPassingMode(proc.Spec, callCypher, hasTail); err != nil {
			return nil, err
		}
		if err := validateProcedureYieldBindings(yield, hasTail); err != nil {
			return nil, err
		}
		args, err := e.extractBoundProcedureInvocationArguments(ctx, proc.Spec, callCypher)
		if err != nil {
			return nil, err
		}
		if yield == nil && strings.TrimSpace(tailCypher) != "" && len(proc.Spec.Returns) > 0 {
			for _, column := range proc.Spec.Returns {
				if referencesVariable(tailCypher, column.Name) {
					return nil, newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"UndefinedVariable",
						fmt.Sprintf("procedure output %s must be introduced with YIELD", column.Name),
					)
				}
			}
		}
		procedureCtx, procedureExec := e.systemProcedureScope(ctx)
		result, err := proc.Handler(procedureCtx, procedureExec, callCypher, args)
		if err != nil {
			return nil, procedureRuntimeError(procName, err)
		}
		if yield != nil {
			result, err = e.applyYieldFilter(ctx, result, yield)
			if err != nil {
				return nil, err
			}
		}
		if strings.TrimSpace(tailCypher) != "" {
			if yield == nil && len(proc.Spec.Returns) == 0 {
				// A void procedure leaves the row it was called for as it
				// is: the clauses after it run once, as after any other
				// leading clause.
				result = &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{{}}}
			}
			return e.executeCallTail(ctx, result, tailCypher)
		}
		return result, nil
	}

	// Terminal chokepoint of the converged procedure router: every built-in
	// procedure is registered, so an unrecognized name is rejected here.
	return nil, newSemanticError(
		"Neo.ClientError.Procedure.ProcedureNotFound",
		"ProcedureNotFound",
		fmt.Sprintf("There is no procedure with the name `%s` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.", procName),
	)
}

func (e *StorageExecutor) callDbLabels() (*ExecuteResult, error) {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"label"},
		Rows:    make([][]interface{}, 0, len(labelSet)),
	}
	for _, label := range e.storage.GetSchema().OrderTokens(sortedStringSet(labelSet), false) {
		result.Rows = append(result.Rows, []interface{}{label})
	}
	return result, nil
}

// sortedStringSet returns a set's members in sorted order, so procedures
// listing schema tokens (db.labels, db.relationshipTypes, …) return the same
// order on every call.
func sortedStringSet(set map[string]bool) []string {
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	sort.Strings(members)
	return members
}

func (e *StorageExecutor) callDbRelationshipTypes() (*ExecuteResult, error) {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}

	result := &ExecuteResult{
		Columns: []string{"relationshipType"},
		Rows:    make([][]interface{}, 0, len(typeSet)),
	}
	for _, relType := range e.storage.GetSchema().OrderTokens(sortedStringSet(typeSet), true) {
		result.Rows = append(result.Rows, []interface{}{relType})
	}
	return result, nil
}

func (e *StorageExecutor) callDbIndexes() (*ExecuteResult, error) {
	// Get indexes from schema manager
	schema := e.storage.GetSchema()
	indexes := schema.GetIndexes()

	rows := make([][]interface{}, 0, len(indexes))
	for _, idx := range indexes {
		idxMap := idx.(map[string]interface{})
		name := idxMap["name"]
		idxType := idxMap["type"]

		// Get labels/properties based on index type
		var labels interface{}
		var properties interface{}

		if l, ok := idxMap["label"]; ok {
			labels = []string{l.(string)}
		} else if ls, ok := idxMap["labels"]; ok {
			labels = ls
		}

		if p, ok := idxMap["property"]; ok {
			properties = []string{p.(string)}
		} else if ps, ok := idxMap["properties"]; ok {
			properties = ps
		}

		rows = append(rows, []interface{}{name, idxType, labels, properties, "ONLINE"})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties", "state"},
		Rows:    rows,
	}, nil
}

// callDbIndexStats returns statistics for all indexes.
// Syntax: CALL db.index.stats() YIELD name, type, totalEntries, uniqueValues, selectivity
func (e *StorageExecutor) callDbIndexStats() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	stats := schema.GetIndexStats()

	rows := make([][]interface{}, 0, len(stats))
	for _, s := range stats {
		rows = append(rows, []interface{}{
			s.Name,
			s.Type,
			s.Label,
			s.Property,
			s.TotalEntries,
			s.UniqueValues,
			s.Selectivity,
		})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "label", "property", "totalEntries", "uniqueValues", "selectivity"},
		Rows:    rows,
	}, nil
}

// callDbConstraints returns all constraints in the database.
// Syntax: CALL db.constraints() YIELD name, type, labelsOrTypes, properties
// Returns constraints in Neo4j-compatible format.
func (e *StorageExecutor) callDbConstraints() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return &ExecuteResult{
			Columns: []string{"name", "type", "labelsOrTypes", "properties", "propertyType"},
			Rows:    [][]interface{}{},
		}, nil
	}

	// Get all constraints from schema
	allConstraints := schema.GetAllConstraints()

	rows := make([][]interface{}, 0, len(allConstraints))
	for _, constraint := range allConstraints {
		// Format labelsOrTypes as []string (single label for node constraints)
		labelsOrTypes := []string{constraint.Label}

		// Format properties as []string
		properties := constraint.Properties

		// Convert constraint type to string
		constraintType := string(constraint.Type)

		rows = append(rows, []interface{}{
			constraint.Name,
			constraintType,
			labelsOrTypes,
			properties,
			nil,
		})
	}

	for _, constraint := range schema.GetAllPropertyTypeConstraints() {
		rows = append(rows, []interface{}{
			constraint.Name,
			string(storage.ConstraintPropertyType),
			[]string{constraint.Label},
			[]string{constraint.Property},
			string(constraint.ExpectedType),
		})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties", "propertyType"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) callDbmsComponents() (*ExecuteResult, error) {
	// Wired to buildinfo so the reported version reflects the actual
	// running binary. The previous hard-coded "1.0.0" caused
	// CALL dbms.components() to under-report version on every release
	// past 1.0.0 (bug reported May 2026 alongside the mcp-neo4j-memory
	// regressions).
	return &ExecuteResult{
		Columns: []string{"name", "versions", "edition"},
		Rows: [][]interface{}{
			{"NornicDB", []string{buildinfo.Version()}, "community"},
		},
	}, nil
}

// NornicDB-specific procedures

func (e *StorageExecutor) callNornicDbVersion() (*ExecuteResult, error) {
	build := buildinfo.ShortCommit()
	if build == "" {
		build = "dev"
	}
	return &ExecuteResult{
		Columns: []string{"version", "build", "edition"},
		Rows: [][]interface{}{
			{buildinfo.Version(), build, "community"},
		},
	}, nil
}

func (e *StorageExecutor) callNornicDbStats() (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships", "labels", "relationshipTypes"},
		Rows: [][]interface{}{
			{nodeCount, edgeCount, e.countLabels(), e.countRelTypes()},
		},
	}, nil
}

func (e *StorageExecutor) countLabels() int {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return 0
	}
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}
	return len(labelSet)
}

func (e *StorageExecutor) countRelTypes() int {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return 0
	}
	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}
	return len(typeSet)
}

func (e *StorageExecutor) callNornicDbDecayInfo() (*ExecuteResult, error) {
	enabled := false
	if be := unwrapBadgerEngine(e.storage); be != nil {
		enabled = be.IsDecayEnabled()
	}

	return &ExecuteResult{
		Columns: []string{"enabled", "system", "configuredVia"},
		Rows: [][]interface{}{
			{enabled, "knowledge-layer scoring (decay profile bundles + bindings)", "CREATE DECAY PROFILE ... OPTIONS / CREATE DECAY PROFILE ... FOR ... APPLY DDL"},
		},
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyInfo() (*ExecuteResult, error) {
	enabled := false
	if be := unwrapBadgerEngine(e.storage); be != nil {
		enabled = be.IsDecayEnabled()
	}

	var decayProfiles, decayBindings int
	var promotionProfiles, promotionPolicies int
	schema, err := e.knowledgePolicySchema()
	if err != nil {
		return nil, err
	}
	if schema != nil {
		bundles, bindings := schema.ShowDecayProfiles()
		decayProfiles = len(bundles)
		decayBindings = len(bindings)
		promotionProfiles = len(schema.ShowPromotionProfiles())
		promotionPolicies = len(schema.ShowPromotionPolicies())
	}

	return &ExecuteResult{
		Columns: []string{"enabled", "system", "decayProfiles", "decayBindings", "promotionProfiles", "promotionPolicies", "configuredVia"},
		Rows: [][]interface{}{
			{
				enabled,
				"knowledge-layer scoring and promotion policy system",
				decayProfiles,
				decayBindings,
				promotionProfiles,
				promotionPolicies,
				"CREATE DECAY PROFILE ... OPTIONS / CREATE DECAY PROFILE ... FOR ... APPLY / CREATE PROMOTION PROFILE ... OPTIONS / CREATE PROMOTION POLICY ... APPLY DDL",
			},
		},
	}, nil
}

// Neo4j schema procedures

func (e *StorageExecutor) callDbSchemaVisualization() (*ExecuteResult, error) {
	return e.callDbSchemaVisualizationWithContext(context.Background())
}

func (e *StorageExecutor) callDbSchemaNodeProperties() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()

	// Collect properties per label
	labelProps := make(map[string]map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			if _, ok := labelProps[label]; !ok {
				labelProps[label] = make(map[string]bool)
			}
			for prop := range node.Properties {
				labelProps[label][prop] = true
			}
		}
	}

	result := &ExecuteResult{
		Columns: []string{"nodeLabel", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for label, props := range labelProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{label, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbSchemaRelProperties() (*ExecuteResult, error) {
	edges, _ := e.storage.AllEdges()

	// Collect properties per relationship type
	typeProps := make(map[string]map[string]bool)
	for _, edge := range edges {
		if _, ok := typeProps[edge.Type]; !ok {
			typeProps[edge.Type] = make(map[string]bool)
		}
		for prop := range edge.Properties {
			typeProps[edge.Type][prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"relType", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for relType, props := range typeProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{relType, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbPropertyKeys() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	propSet := make(map[string]bool)
	for _, node := range nodes {
		for prop := range node.Properties {
			propSet[prop] = true
		}
	}
	for _, edge := range edges {
		for prop := range edge.Properties {
			propSet[prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"propertyKey"},
		Rows:    make([][]interface{}, 0, len(propSet)),
	}
	for _, prop := range sortedStringSet(propSet) {
		result.Rows = append(result.Rows, []interface{}{prop})
	}

	return result, nil
}

func (e *StorageExecutor) callDbmsProcedures() (*ExecuteResult, error) {
	ensureBuiltInProceduresRegistered()
	registered := ListRegisteredProcedures()
	procedures := make([][]interface{}, 0, len(registered))
	for _, p := range registered {
		procedures = append(procedures, []interface{}{p.Name, p.Description, string(p.Mode), p.Signature})
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "mode", "signature"},
		Rows:    procedures,
	}, nil
}

func (e *StorageExecutor) callDbmsFunctions() (*ExecuteResult, error) {
	functions := [][]interface{}{
		{"count", "Counts items", "Aggregating"},
		{"sum", "Sums numeric values", "Aggregating"},
		{"avg", "Averages numeric values", "Aggregating"},
		{"min", "Returns minimum value", "Aggregating"},
		{"max", "Returns maximum value", "Aggregating"},
		{"collect", "Collects values into a list", "Aggregating"},
		{"id", "Returns internal ID", "Scalar"},
		{"labels", "Returns labels of a node", "Scalar"},
		{"type", "Returns type of relationship", "Scalar"},
		{"properties", "Returns properties map", "Scalar"},
		{"keys", "Returns property keys", "Scalar"},
		{"coalesce", "Returns first non-null value", "Scalar"},
		{"toString", "Converts to string", "Scalar"},
		{"toInteger", "Converts to integer", "Scalar"},
		{"toFloat", "Converts to float", "Scalar"},
		{"toBoolean", "Converts to boolean", "Scalar"},
		{"size", "Returns size of list/string", "Scalar"},
		{"length", "Returns path length", "Scalar"},
		{"head", "Returns first list element", "List"},
		{"tail", "Returns list without first element", "List"},
		{"last", "Returns last list element", "List"},
		{"range", "Creates a range list", "List"},
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "category"},
		Rows:    functions,
	}, nil
}
