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
	// misplacedWhere is a WHERE after ORDER BY / SKIP / LIMIT, which Cypher
	// rejects.
	misplacedWhere bool
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

// parseYieldClause extracts YIELD information from a CALL statement.
// Handles: YIELD *, YIELD a, b, YIELD a AS x, b AS y, YIELD a WHERE a.score > 0.5,
// YIELD a ORDER BY a SKIP 1 LIMIT 2. Keywords are found at the top level, so
// a WHERE / ORDER inside a subquery, a list or a string, and the WITH of
// STARTS WITH, belong to the expression they are in.
func parseYieldClause(cypher string) *yieldClause {
	// Normalize whitespace: replace newlines/tabs with spaces for keyword detection
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")
	yieldIdx := findKeywordIndexInContext(normalized, "YIELD")
	if yieldIdx == -1 {
		return nil
	}
	result := &yieldClause{items: []yieldItem{}}

	afterYield := strings.TrimSpace(normalized[yieldIdx+len("YIELD"):])
	if len(afterYield) > 0 && afterYield[0] == '*' {
		result.yieldAll = true
		afterYield = strings.TrimSpace(afterYield[1:])
	}

	// Limit YIELD parsing to the CALL-clause scope only. Anything after the first
	// outer clause boundary (and a RETURN) belongs to the subsequent query.
	yieldScope := scopeYieldToCallClause(afterYield)
	if returnIdx := topLevelKeywordIndex(yieldScope, "RETURN"); returnIdx >= 0 {
		yieldScope = strings.TrimSpace(yieldScope[:returnIdx])
	}
	whereIdx := topLevelKeywordIndex(yieldScope, "WHERE")
	orderIdx := topLevelKeywordIndex(yieldScope, "ORDER")
	skipIdx := topLevelKeywordIndex(yieldScope, "SKIP")
	limitIdx := topLevelKeywordIndex(yieldScope, "LIMIT")
	positions := []int{whereIdx, orderIdx, skipIdx, limitIdx}

	// part returns the text of the keyword at start, up to the next keyword.
	part := func(start, keywordLen int) string {
		end := len(yieldScope)
		for _, idx := range positions {
			if idx > start && idx < end {
				end = idx
			}
		}
		return strings.TrimSpace(yieldScope[start+keywordLen : end])
	}
	if whereIdx >= 0 {
		result.where = part(whereIdx, len("WHERE"))
		for _, idx := range []int{orderIdx, skipIdx, limitIdx} {
			if idx >= 0 && idx < whereIdx {
				result.misplacedWhere = true
			}
		}
	}
	if orderIdx >= 0 {
		orderBy := part(orderIdx, len("ORDER"))
		if len(orderBy) >= len("BY") && strings.EqualFold(orderBy[:len("BY")], "BY") {
			orderBy = strings.TrimSpace(orderBy[len("BY"):])
		}
		result.orderBy = orderBy
	}
	if skipIdx >= 0 {
		result.skip = part(skipIdx, len("SKIP"))
	}
	if limitIdx >= 0 {
		result.limit = part(limitIdx, len("LIMIT"))
	}

	// Parse yield items (if not YIELD *)
	if !result.yieldAll {
		itemsEnd := len(yieldScope)
		for _, idx := range positions {
			if idx != -1 && idx < itemsEnd {
				itemsEnd = idx
			}
		}
		itemsStr := strings.TrimSpace(yieldScope[:itemsEnd])
		if itemsStr != "" {
			// Split by comma, respecting AS keyword
			for _, item := range strings.Split(itemsStr, ",") {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				yi := yieldItem{}
				upperItem := strings.ToUpper(item)
				if asIdx := strings.Index(upperItem, " AS "); asIdx != -1 {
					yi.name = strings.TrimSpace(item[:asIdx])
					yi.alias = strings.TrimSpace(item[asIdx+4:])
				} else {
					yi.name = item
				}
				result.items = append(result.items, yi)
			}
		}
	}

	return result
}

// scopeYieldToCallClause trims text after YIELD to the first outer query-clause
// boundary so YIELD item parsing does not accidentally consume later clauses.
func scopeYieldToCallClause(afterYield string) string {
	scopeEnd := findYieldOuterBoundary(afterYield)
	if scopeEnd == -1 {
		scopeEnd = len(afterYield)
	}
	return strings.TrimSpace(afterYield[:scopeEnd])
}

func findYieldOuterBoundary(afterYield string) int {
	scopeEnd := len(afterYield)
	// Keep this list conservative and clause-oriented; ORDER/RETURN/WHERE/LIMIT/SKIP
	// are intentionally excluded here because they are valid within the YIELD scope.
	for _, kw := range []string{
		"WITH", "MATCH", "OPTIONAL", "UNWIND", "CALL",
		"CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "FOREACH", "LOAD",
	} {
		if idx := topLevelKeywordIndex(afterYield, kw); idx != -1 && idx < scopeEnd {
			scopeEnd = idx
		}
	}
	if scopeEnd >= len(afterYield) {
		return -1
	}
	return scopeEnd
}

func splitCallAndTail(cypher string) callSplit {
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")
	yieldIdx := findKeywordIndexInContext(normalized, "YIELD")
	if yieldIdx == -1 {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(normalized)), "CALL ") {
			return callSplit{callOnly: strings.TrimSpace(cypher)}
		}
		open := strings.Index(normalized, "(")
		if open < 0 {
			return callSplit{callOnly: strings.TrimSpace(cypher)}
		}
		close := findMatchingCallParen(normalized, open)
		if close < 0 {
			return callSplit{callOnly: strings.TrimSpace(cypher)}
		}
		searchStart := close + 1
		boundary := len(normalized)
		for _, keyword := range []string{"WITH", "MATCH", "OPTIONAL MATCH", "UNWIND", "CALL", "CREATE", "MERGE", "SET", "DELETE", "DETACH DELETE", "REMOVE", "FOREACH", "LOAD CSV", "RETURN"} {
			if index := findKeywordIndexInContext(normalized[searchStart:], keyword); index >= 0 {
				index += searchStart
				if index < boundary {
					boundary = index
				}
			}
		}
		if boundary < len(normalized) {
			return callSplit{
				callOnly: strings.TrimSpace(normalized[:boundary]),
				tail:     strings.TrimSpace(normalized[boundary:]),
			}
		}
		return callSplit{callOnly: strings.TrimSpace(cypher)}
	}

	afterYieldStart := yieldIdx + len("YIELD")
	if afterYieldStart >= len(normalized) {
		return callSplit{callOnly: strings.TrimSpace(cypher)}
	}
	afterYield := normalized[afterYieldStart:]
	boundary := findYieldOuterBoundary(afterYield)
	if boundary == -1 {
		return callSplit{callOnly: strings.TrimSpace(normalized)}
	}

	callOnly := strings.TrimSpace(normalized[:afterYieldStart+boundary])
	tail := strings.TrimSpace(afterYield[boundary:])
	return callSplit{callOnly: callOnly, tail: tail}
}

func buildCallTailPredicateInjection(tail string, predicates []string) string {
	if len(predicates) == 0 {
		return strings.TrimSpace(tail)
	}
	injected := strings.Join(predicates, " AND ")
	trimmed := strings.TrimSpace(tail)

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
	trimmed := strings.ToUpper(strings.TrimSpace(tail))
	return strings.HasPrefix(trimmed, "MATCH ") || strings.HasPrefix(trimmed, "OPTIONAL MATCH ")
}

func expectedReturnColumnsFromTail(tail string) []string {
	trimmed := strings.TrimSpace(tail)
	retIdx := findKeywordIndexInContext(trimmed, "RETURN")
	if retIdx == -1 {
		return nil
	}
	returnPart := strings.TrimSpace(trimmed[retIdx+len("RETURN"):])
	if returnPart == "" {
		return nil
	}
	end := len(returnPart)
	for _, kw := range []string{"ORDER", "SKIP", "LIMIT"} {
		if idx := findKeywordIndexInContext(returnPart, kw); idx != -1 && idx < end {
			end = idx
		}
	}
	returnExpr := strings.TrimSpace(returnPart[:end])
	if returnExpr == "" {
		return nil
	}
	items := splitReturnExpressions(returnExpr)
	cols := make([]string, 0, len(items))
	for _, item := range items {
		expr := strings.TrimSpace(item)
		if expr == "" {
			continue
		}
		upperExpr := strings.ToUpper(expr)
		if asIdx := strings.Index(upperExpr, " AS "); asIdx >= 0 {
			alias := normalizeProjectionColumnName(expr[asIdx+4:])
			if alias != "" {
				cols = append(cols, alias)
				continue
			}
		}
		cols = append(cols, expr)
	}
	return cols
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
		if setBased, ok := e.executeCallTailSetBased(ctx, seed, tail, usedCols, expectedCols); ok {
			return setBased, nil
		}
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
	return e.runPipelineClauses(ctx, rows, scope, clauses, clauses)
}

func (e *StorageExecutor) tryExecuteCallTailProcedurePipeline(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
) (*ExecuteResult, bool, error) {
	callIndex := findKeywordIndexInContext(tail, "CALL")
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
	parts := splitCallAndTail(cypher)
	if strings.TrimSpace(parts.tail) != "" {
		return parts
	}
	yieldIndex := findKeywordIndexInContext(cypher, "YIELD")
	if yieldIndex < 0 {
		return parts
	}
	searchStart := yieldIndex + len("YIELD")
	returnIndex := findKeywordIndexInContext(cypher[searchStart:], "RETURN")
	if returnIndex < 0 {
		return parts
	}
	returnIndex += searchStart
	return callSplit{
		callOnly: strings.TrimSpace(cypher[:returnIndex]),
		tail:     strings.TrimSpace(cypher[returnIndex:]),
	}
}

func callTailWithProjectionColumns(withClause string) ([]string, bool) {
	body := strings.TrimSpace(withClause[len("WITH "):])
	if whereIndex := findKeywordIndexInContext(body, "WHERE"); whereIndex >= 0 {
		body = strings.TrimSpace(body[:whereIndex])
	}
	items := splitTopLevelComma(body)
	columns := make([]string, 0, len(items))
	for _, item := range items {
		expr, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		if expr == "" || alias == "" {
			return nil, false
		}
		columns = append(columns, normalizeProjectionColumnName(alias))
	}
	return columns, len(columns) > 0
}

func (e *StorageExecutor) projectCallTailReturnAll(seed *ExecuteResult, tail string) (*ExecuteResult, bool, error) {
	trimmed := strings.TrimSpace(tail)
	if !hasPrefixFoldASCII(trimmed, "RETURN ") {
		return nil, false, nil
	}
	body := strings.TrimSpace(trimmed[len("RETURN "):])
	modifierStart := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := findKeywordIndexInContext(body, keyword); index >= 0 && index < modifierStart {
			modifierStart = index
		}
	}
	if strings.TrimSpace(body[:modifierStart]) != "*" {
		return nil, false, nil
	}

	result := &ExecuteResult{
		Columns: append([]string(nil), seed.Columns...),
		Rows:    make([][]interface{}, len(seed.Rows)),
		Stats:   seed.Stats,
	}
	for index, row := range seed.Rows {
		result.Rows[index] = append([]interface{}(nil), row...)
	}
	modifiers := strings.TrimSpace(body[modifierStart:])
	if modifiers == "" {
		return result, true, nil
	}
	result, err := e.applyResultModifiers(result, modifiers)
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
	t := strings.ToUpper(strings.TrimSpace(tail))
	return findKeywordIndexInContext(t, "CREATE") >= 0 ||
		findKeywordIndexInContext(t, "MERGE") >= 0 ||
		findKeywordIndexInContext(t, "DELETE") >= 0 ||
		findKeywordIndexInContext(t, "SET") >= 0 ||
		findKeywordIndexInContext(t, "REMOVE") >= 0
}

func (e *StorageExecutor) executeCallTailSetBased(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	usedCols []int,
	expectedCols []string,
) (*ExecuteResult, bool) {
	if len(usedCols) == 0 {
		return nil, false
	}
	if !tailStartsWithMatchClause(tail) {
		return nil, false
	}
	if res, ok, err := e.tryExecuteCallTailVariableLengthMaxLengthFastPath(ctx, seed, tail, expectedCols); ok {
		if err != nil {
			return nil, false
		}
		return res, true
	}
	if res, ok, err := e.tryExecuteCallTailBranchingPathCountFastPath(ctx, seed, tail, expectedCols); ok {
		if err != nil {
			return nil, false
		}
		return res, true
	}
	if res, ok, err := e.tryExecuteCallTailFrontierReachableFastPath(ctx, seed, tail, expectedCols); ok {
		if err != nil {
			return nil, false
		}
		return res, true
	}
	if res, ok, err := e.tryExecuteCallTailConstrainedMaxDepthFastPath(ctx, seed, tail, expectedCols); ok {
		if err != nil {
			return nil, false
		}
		return res, true
	}
	// Preserve typed relationship semantics by delegating non-fast-path typed
	// tails to executeCallTail's per-row fallback.
	if callTailHasRelationshipTypeConstraint(tail) {
		return nil, false
	}
	relationshipTail := strings.Contains(tail, "-[") || strings.Contains(tail, "]-")
	upperTail := strings.ToUpper(tail)
	// Relationship tails that aggregate over path length still benefit from a
	// single batched query, but the MATCH ... WITH aggregate executor preserves
	// scalar seed bindings more reliably when they stay in normal query scope via
	// the UNWIND-based route below rather than being rewritten as CASE id(node)
	// expressions inside the tail.

	params := map[string]interface{}{}
	rowsParam := make([]map[string]interface{}, 0, len(seed.Rows))

	nodeCols := make([]string, 0, len(usedCols))
	scalarCols := make([]string, 0, len(usedCols))

	for _, idx := range usedCols {
		col := seed.Columns[idx]
		isNode := false
		for _, row := range seed.Rows {
			if idx >= len(row) {
				continue
			}
			if _, ok := row[idx].(*storage.Node); ok {
				isNode = true
				break
			}
		}
		if isNode {
			nodeCols = append(nodeCols, col)
		} else {
			scalarCols = append(scalarCols, col)
		}
	}

	for _, row := range seed.Rows {
		seedMap := make(map[string]interface{}, len(usedCols))
		for _, idx := range usedCols {
			if idx >= len(row) {
				continue
			}
			col := seed.Columns[idx]
			val := row[idx]
			if node, ok := val.(*storage.Node); ok {
				key := "seed_id_" + col
				if node != nil {
					seedMap[key] = string(node.ID)
				} else {
					seedMap[key] = nil
				}
				continue
			}
			seedMap["seed_"+col] = val
		}
		rowsParam = append(rowsParam, seedMap)
	}
	params["__seed_rows"] = rowsParam

	if relationshipTail && !strings.Contains(upperTail, "MAX(LENGTH(") {
		// Relationship-safe batched path:
		// Inject id(nodeVar) IN $__seed_ids into the existing tail MATCH's WHERE clause
		// instead of prepending a separate bare MATCH (node). This preserves label
		// constraints from the original pattern (e.g. MATCH (node:OriginalText)-[...]->(...))
		// so the engine can use label-index seeks instead of full node scan.
		// Scalar vars (like score) are projected via CASE id(nodeVar) ... in the first WITH.
		if len(nodeCols) != 1 {
			return nil, false
		}
		nodeVar := nodeCols[0]
		seedIDs := make([]string, 0, len(rowsParam))
		for _, r := range rowsParam {
			if idRaw, ok := r["seed_id_"+nodeVar]; ok {
				if s, ok := idRaw.(string); ok && s != "" {
					seedIDs = append(seedIDs, s)
				}
			}
		}
		if len(seedIDs) == 0 {
			if len(expectedCols) > 0 {
				return &ExecuteResult{Columns: append([]string{}, expectedCols...), Rows: [][]interface{}{}}, true
			}
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true
		}
		paramsRel := map[string]interface{}{
			"__seed_ids": seedIDs,
		}
		rewritten := strings.TrimSpace(tail)
		for _, scol := range scalarCols {
			// Build CASE id(nodeVar) WHEN '<id>' THEN <value> ... END AS <scol>
			m := make(map[string]interface{}, len(rowsParam))
			for _, r := range rowsParam {
				idv, okID := r["seed_id_"+nodeVar].(string)
				if !okID || idv == "" {
					continue
				}
				m[idv] = r["seed_"+scol]
			}
			caseExpr := buildIDCaseExpression(nodeVar, m)
			var ok bool
			rewritten, ok = rewriteFirstWithScalar(rewritten, scol, caseExpr)
			if !ok {
				return nil, false
			}
		}
		// Inject the IN predicate into the tail's existing MATCH WHERE clause
		// so label constraints are preserved. Previous approach prepended a bare
		// MATCH (node) which caused full node scans on real datasets.
		inPredicate := fmt.Sprintf("id(%s) IN $__seed_ids", nodeVar)
		query := buildCallTailPredicateInjection(rewritten, []string{inPredicate})
		res, err := e.executeInternal(ctx, query, paramsRel)
		if err != nil {
			return nil, false
		}
		if len(expectedCols) > 0 && len(expectedCols) == len(res.Columns) {
			res.Columns = append([]string{}, expectedCols...)
		}
		return res, true
	}

	prefix := make([]string, 0, util.SafePreallocSum(3, len(scalarCols)))
	prefix = append(prefix, "WITH $__seed_rows AS __seed_rows")
	prefix = append(prefix, "UNWIND __seed_rows AS __seed")
	withBindings := make([]string, 0, util.SafePreallocSum(len(scalarCols), 1))
	withBindings = append(withBindings, "__seed")
	for _, col := range scalarCols {
		withBindings = append(withBindings, fmt.Sprintf("__seed.seed_%s AS %s", col, col))
	}
	if len(withBindings) > 0 {
		prefix = append(prefix, "WITH "+strings.Join(withBindings, ", "))
	}

	predicates := make([]string, 0, len(nodeCols))
	for _, col := range nodeCols {
		predicates = append(predicates, fmt.Sprintf("id(%s) = __seed.seed_id_%s", col, col))
	}
	tailWithPredicates := buildCallTailPredicateInjection(strings.TrimSpace(tail), predicates)
	query := strings.Join(prefix, " ") + " " + tailWithPredicates
	res, err := e.executeInternal(ctx, query, params)
	if err != nil {
		return nil, false
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(res.Columns) {
		res.Columns = append([]string{}, expectedCols...)
	}
	return res, true
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
	callTailProjectionPlans        = newBoundedCache[string, *callTailProjectionPlan](1024)
	callTailRelationshipMatchPlans = newBoundedCache[string, *callTailRelationshipMatchPlan](1024)
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
	for _, seedRow := range seed.Rows {
		values := callTailRow(ctx, seed, seedRow)
		relationship, ok := values[plan.propertySource].(*storage.Edge)
		if !ok || relationship == nil {
			continue
		}
		if plan.relType != "" && !strings.EqualFold(relationship.Type, plan.relType) {
			continue
		}
		expected, _ := e.evaluateRowExpressionWithContext(ctx, plan.propertyExpr, values)
		if expected == nil || !e.compareEqual(relationship.Properties[plan.propertyKey], expected) {
			continue
		}
		startNode, err := e.storage.GetNode(relationship.StartNode)
		if err != nil || startNode == nil {
			continue
		}
		endNode, err := e.storage.GetNode(relationship.EndNode)
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
	trimmed := strings.TrimSpace(tail)
	if !hasPrefixFoldASCII(trimmed, "MATCH ") || isPotentialWriteTail(trimmed) {
		return nil
	}
	parts, ok := parseCallTailRelationshipPattern(trimmed)
	if !ok {
		return nil
	}
	rest := strings.TrimSpace(parts.rest)
	withIdx := topLevelKeywordIndex(rest, "WITH")
	if withIdx < 0 {
		return nil
	}
	whereClause := strings.TrimSpace(rest[:withIdx])
	if whereClause != "" {
		if !hasPrefixFoldASCII(whereClause, "WHERE ") {
			return nil
		}
		whereClause = strings.TrimSpace(whereClause[len("WHERE "):])
	}
	projection := e.planCallTailProjection(strings.TrimSpace(rest[withIdx:]))
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
	rest := strings.TrimSpace(tail)
	if !hasPrefixFoldASCII(rest, "MATCH") {
		return parts, false
	}
	rest = strings.TrimSpace(rest[len("MATCH"):])
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
	parts.rest = rest
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
	text := strings.TrimSpace(input)
	if text == "" || !isIdentifierStart(text[0]) {
		return "", "", false
	}
	i := 1
	for i < len(text) && isIdentifierPart(text[i]) {
		i++
	}
	return text[:i], text[i:], true
}

func parseCallTailDelimited(input string, open, close byte) (inside, rest string, ok bool) {
	text := strings.TrimSpace(input)
	if text == "" || text[0] != open {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
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
		case ch == open:
			depth++
		case ch == close:
			depth--
			if depth == 0 {
				return text[1:i], text[i+1:], true
			}
		}
	}
	return "", "", false
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

// callTailPlanClauseKeywords start the clauses a compiled CALL-tail
// projection plan can't hold between its WITH and RETURN.
var callTailPlanClauseKeywords = []string{
	"MATCH", "OPTIONAL", "UNWIND", "CALL", "WITH", "CREATE", "MERGE",
	"SET", "DELETE", "DETACH", "REMOVE", "FOREACH", "LOAD", "UNION",
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
	trimmed := strings.TrimSpace(tail)
	if !hasPrefixFoldASCII(trimmed, "WITH ") || isPotentialWriteTail(trimmed) {
		return nil
	}
	withIdx := len("WITH ")
	returnIdx := topLevelKeywordIndex(trimmed, "RETURN")
	if returnIdx < 0 {
		return nil
	}
	beforeReturn := strings.TrimSpace(trimmed[withIdx:returnIdx])
	returnAndModifiers := strings.TrimSpace(trimmed[returnIdx+len("RETURN"):])
	if beforeReturn == "" || returnAndModifiers == "" {
		return nil
	}
	// The plan covers exactly WITH … [WHERE …] RETURN …; a tail with another
	// clause between them (WITH label MATCH (n) RETURN …) is the pipeline's.
	for _, keyword := range callTailPlanClauseKeywords {
		if topLevelKeywordIndex(beforeReturn, keyword) >= 0 {
			return nil
		}
	}

	withProjection := beforeReturn
	if whereIdx := topLevelKeywordIndex(beforeReturn, "WHERE"); whereIdx >= 0 {
		withProjection = strings.TrimSpace(beforeReturn[:whereIdx])
	}
	if withProjection == "" {
		return nil
	}
	returnProjection, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(returnAndModifiers)
	if returnProjection == "" {
		return nil
	}
	// The plan projects row by row: aggregation, DISTINCT and SKIP / LIMIT
	// expressions (LIMIT 0 + 1) are the pipeline's (executeCallTailPipeline).
	if hasPrefixFoldASCII(withProjection, "DISTINCT") || hasPrefixFoldASCII(returnProjection, "DISTINCT") ||
		containsAggregateFunc(withProjection) || containsAggregateFunc(returnProjection) || containsAggregateFunc(orderBy) {
		return nil
	}
	withItems := e.parseReturnItems(withProjection)
	returnItems := e.parseReturnItems(returnProjection)
	if len(withItems) == 0 || len(returnItems) == 0 || hasStarReturnItem(withItems) || hasStarReturnItem(returnItems) {
		return nil
	}
	plan := &callTailProjectionPlan{
		withClause:   "WITH " + beforeReturn,
		returnClause: "RETURN " + returnAndModifiers,
		limitToken:   limitToken,
		skipToken:    skipToken,
	}
	plan.passThroughWith = true
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if topLevelKeywordIndex(beforeReturn, keyword) >= 0 {
			plan.passThroughWith = false
		}
	}
	for _, item := range withItems {
		if !isValidIdentifier(item.expr) || (item.alias != "" && item.alias != item.expr) {
			plan.passThroughWith = false
		}
	}
	if whereIdx := topLevelKeywordIndex(beforeReturn, "WHERE"); whereIdx >= 0 {
		plan.withWhere = strings.TrimSpace(beforeReturn[whereIdx+len("WHERE"):])
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
func (e *StorageExecutor) tryExecuteCallTailVariableLengthMaxLengthFastPath(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailVariableLengthMaxLengthPlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	limit, ok := resolveOptionalIntLiteralOrParam(ctx, plan.limitToken)
	if !ok {
		return nil, false, nil
	}
	skip, ok := resolveOptionalIntLiteralOrParam(ctx, plan.skipToken)
	if !ok {
		return nil, false, nil
	}

	result := &ExecuteResult{
		Columns: make([]string, len(plan.returnItems)),
		Rows:    make([][]interface{}, 0, len(seed.Rows)),
	}
	for i, item := range plan.returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, row := range seed.Rows {
		values := make(map[string]interface{}, util.SafePreallocSum(len(seed.Columns), 1))
		for i, col := range seed.Columns {
			if i < len(row) {
				values[col] = row[i]
			}
		}

		startRaw, ok := values[plan.nodeVar]
		if !ok {
			return nil, false, nil
		}
		startNode, ok := startRaw.(*storage.Node)
		if !ok || startNode == nil {
			continue
		}

		maxDepth, err := e.maxDepthForTraversalMatch(startNode, plan.match)
		if err != nil {
			return nil, true, err
		}
		if maxDepth < plan.match.Relationship.MinHops {
			continue
		}
		values[plan.aggregateAlias] = int64(maxDepth)

		projected := make([]interface{}, len(plan.returnItems))
		for i, item := range plan.returnItems {
			projected[i] = e.evaluateExpressionFromValues(item.expr, values)
		}
		result.Rows = append(result.Rows, projected)
	}

	if plan.orderBy != "" {
		result = e.applyOrderByToResult(result, plan.orderBy)
	}
	if skip > 0 {
		if skip >= len(result.Rows) {
			result.Rows = [][]interface{}{}
		} else {
			result.Rows = result.Rows[skip:]
		}
	}
	if limit >= 0 && limit < len(result.Rows) {
		result.Rows = result.Rows[:limit]
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(result.Columns) {
		result.Columns = append([]string{}, expectedCols...)
	}
	e.markCallTailTraversalFastPathUsed()
	return result, true, nil
}

func (e *StorageExecutor) tryExecuteCallTailBranchingPathCountFastPath(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailBranchingPathCountPlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	pathCap, ok := resolveIntLiteralOrParam(ctx, plan.pathCapToken)
	if !ok || pathCap < 0 {
		return nil, false, nil
	}
	limit, ok := resolveOptionalIntLiteralOrParam(ctx, plan.limitToken)
	if !ok {
		return nil, false, nil
	}

	result := &ExecuteResult{Columns: plan.resultColumns(), Rows: make([][]interface{}, 0, len(seed.Rows))}
	for _, row := range seed.Rows {
		values := seedValuesForRow(seed, row)
		startNode, ok := values[plan.nodeVar].(*storage.Node)
		if !ok || startNode == nil {
			continue
		}
		pathCount, err := e.countTraversalPathsWithCap(startNode, plan.match, pathCap, callTailPathPredicate{requireAllNodesLabeled: true})
		if err != nil {
			return nil, true, err
		}
		values[plan.pathsAlias] = make([]interface{}, pathCount)
		result.Rows = append(result.Rows, projectReturnItemsFromValues(e, plan.returnItems, values))
	}
	if limit >= 0 && limit < len(result.Rows) {
		result.Rows = result.Rows[:limit]
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(result.Columns) {
		result.Columns = append([]string{}, expectedCols...)
	}
	e.markCallTailTraversalFastPathUsed()
	return result, true, nil
}

func (e *StorageExecutor) tryExecuteCallTailFrontierReachableFastPath(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailFrontierReachablePlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	limit, ok := resolveOptionalIntLiteralOrParam(ctx, plan.limitToken)
	if !ok {
		return nil, false, nil
	}
	result := &ExecuteResult{Columns: plan.resultColumns(), Rows: make([][]interface{}, 0, len(seed.Rows))}
	for _, row := range seed.Rows {
		values := seedValuesForRow(seed, row)
		startNode, ok := values[plan.nodeVar].(*storage.Node)
		if !ok || startNode == nil {
			continue
		}
		nearest, reachable, err := e.shortestReachableStats(startNode, plan.match)
		if err != nil {
			return nil, true, err
		}
		if reachable == 0 {
			continue
		}
		values[plan.nearestAlias] = int64(nearest)
		values[plan.reachableAlias] = int64(reachable)
		result.Rows = append(result.Rows, projectReturnItemsFromValues(e, plan.returnItems, values))
	}
	if limit >= 0 && limit < len(result.Rows) {
		result.Rows = result.Rows[:limit]
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(result.Columns) {
		result.Columns = append([]string{}, expectedCols...)
	}
	e.markCallTailTraversalFastPathUsed()
	return result, true, nil
}

func (e *StorageExecutor) tryExecuteCallTailConstrainedMaxDepthFastPath(
	ctx context.Context,
	seed *ExecuteResult,
	tail string,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	plan, ok := e.parseCallTailConstrainedMaxDepthPlan(ctx, tail)
	if !ok {
		return nil, false, nil
	}
	minWeight, ok := resolveFloatLiteralOrParam(ctx, plan.minWeightToken)
	if !ok {
		return nil, false, nil
	}
	categories, ok := resolveStringSliceLiteralOrParam(ctx, plan.categoriesToken)
	if !ok {
		return nil, false, nil
	}
	limit, ok := resolveOptionalIntLiteralOrParam(ctx, plan.limitToken)
	if !ok {
		return nil, false, nil
	}
	allowedCategories := make(map[string]struct{}, len(categories))
	for _, category := range categories {
		allowedCategories[category] = struct{}{}
	}
	result := &ExecuteResult{Columns: plan.resultColumns(), Rows: make([][]interface{}, 0, len(seed.Rows))}
	for _, row := range seed.Rows {
		values := seedValuesForRow(seed, row)
		startNode, ok := values[plan.nodeVar].(*storage.Node)
		if !ok || startNode == nil {
			continue
		}
		maxDepth, found, err := e.maxDepthForPredicateTraversal(startNode, plan.match, callTailPathPredicate{
			minWeight:       &minWeight,
			allowedCategory: allowedCategories,
		})
		if err != nil {
			return nil, true, err
		}
		if !found {
			continue
		}
		values[plan.aggregateExpr] = int64(maxDepth)
		if plan.aggregateAlias != "" {
			values[plan.aggregateAlias] = int64(maxDepth)
		}
		result.Rows = append(result.Rows, projectReturnItemsFromValues(e, plan.returnItems, values))
	}
	if limit >= 0 && limit < len(result.Rows) {
		result.Rows = result.Rows[:limit]
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(result.Columns) {
		result.Columns = append([]string{}, expectedCols...)
	}
	e.markCallTailTraversalFastPathUsed()
	return result, true, nil
}

type callTailVariableLengthMaxLengthPlan struct {
	match          *TraversalMatch
	nodeVar        string
	aggregateAlias string
	returnItems    []returnItem
	orderBy        string
	// limitToken / skipToken are the RETURN's whole LIMIT / SKIP values,
	// resolved when the plan runs; an expression other than a literal or a
	// parameter makes the plan decline, so the pipeline evaluates it (#572).
	limitToken string
	skipToken  string
}

func (e *StorageExecutor) parseCallTailVariableLengthMaxLengthPlan(ctx context.Context, tail string) (*callTailVariableLengthMaxLengthPlan, bool) {
	trimmed := strings.TrimSpace(tail)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "MATCH ") {
		return nil, false
	}

	withIdx := findKeywordIndexInContext(trimmed, "WITH")
	returnIdx := findKeywordIndexInContext(trimmed, "RETURN")
	if withIdx == -1 || returnIdx == -1 || returnIdx <= withIdx {
		return nil, false
	}

	matchClause := strings.TrimSpace(trimmed[len("MATCH"):withIdx])
	if matchClause == "" || findKeywordIndexInContext(matchClause, "WHERE") != -1 {
		return nil, false
	}
	withClause := strings.TrimSpace(trimmed[withIdx+len("WITH") : returnIdx])
	if withClause == "" {
		return nil, false
	}
	returnClause, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(strings.TrimSpace(trimmed[returnIdx+len("RETURN"):]))
	if returnClause == "" {
		return nil, false
	}
	orderBy = strings.TrimSpace(strings.TrimPrefix(orderBy, "ORDER BY"))

	pattern := matchClause
	pathVar := ""
	if eqIdx := strings.Index(matchClause, "="); eqIdx != -1 {
		pathVar = strings.TrimSpace(matchClause[:eqIdx])
		pattern = strings.TrimSpace(matchClause[eqIdx+1:])
	}
	if pathVar == "" || pattern == "" {
		return nil, false
	}

	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.IsChained {
		return nil, false
	}
	if match.Relationship.Direction != "outgoing" && match.Relationship.Direction != "incoming" && match.Relationship.Direction != "both" {
		return nil, false
	}

	withItems := splitReturnExpressions(withClause)
	if len(withItems) < 2 {
		return nil, false
	}
	nodeVar := match.StartNode.variable
	if nodeVar == "" {
		return nil, false
	}
	aggAlias := ""
	seenNodeVar := false
	for _, item := range withItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		expr, alias := parseProjectionExprAlias(item)
		if alias, ok := parseAggregateExprAlias(expr, alias, "max", "length("+pathVar+")"); ok {
			if aggAlias != "" {
				return nil, false
			}
			aggAlias = alias
			continue
		}
		if !isSimpleIdentifier(item) {
			return nil, false
		}
		if item == nodeVar {
			seenNodeVar = true
		}
	}
	if aggAlias == "" || !seenNodeVar {
		return nil, false
	}

	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) == 0 {
		return nil, false
	}

	return &callTailVariableLengthMaxLengthPlan{
		match:          match,
		nodeVar:        nodeVar,
		aggregateAlias: aggAlias,
		returnItems:    returnItems,
		orderBy:        orderBy,
		limitToken:     limitToken,
		skipToken:      skipToken,
	}, true
}

type callTailBranchingPathCountPlan struct {
	match        *TraversalMatch
	nodeVar      string
	pathsAlias   string
	pathCapToken string
	returnItems  []returnItem
	limitToken   string
}

func (p *callTailBranchingPathCountPlan) resultColumns() []string {
	cols := make([]string, len(p.returnItems))
	for i, item := range p.returnItems {
		if item.alias != "" {
			cols[i] = item.alias
		} else {
			cols[i] = item.expr
		}
	}
	return cols
}

type callTailFrontierReachablePlan struct {
	match          *TraversalMatch
	nodeVar        string
	nearestAlias   string
	reachableAlias string
	returnItems    []returnItem
	limitToken     string
}

func (p *callTailFrontierReachablePlan) resultColumns() []string {
	cols := make([]string, len(p.returnItems))
	for i, item := range p.returnItems {
		if item.alias != "" {
			cols[i] = item.alias
		} else {
			cols[i] = item.expr
		}
	}
	return cols
}

type callTailConstrainedMaxDepthPlan struct {
	match           *TraversalMatch
	nodeVar         string
	aggregateExpr   string
	aggregateAlias  string
	minWeightToken  string
	categoriesToken string
	returnItems     []returnItem
	limitToken      string
}

func (p *callTailConstrainedMaxDepthPlan) resultColumns() []string {
	cols := make([]string, len(p.returnItems))
	for i, item := range p.returnItems {
		if item.alias != "" {
			cols[i] = item.alias
		} else {
			cols[i] = item.expr
		}
	}
	return cols
}

func (e *StorageExecutor) parseCallTailBranchingPathCountPlan(ctx context.Context, tail string) (*callTailBranchingPathCountPlan, bool) {
	normalized := normalizeCallTailShape(tail)
	firstWithIdx := findKeywordIndexInContext(normalized, "WITH")
	if firstWithIdx == -1 {
		return nil, false
	}
	matchSection := strings.TrimSpace(normalized[:firstWithIdx])
	afterFirstWith := strings.TrimSpace(normalized[firstWithIdx+len("WITH"):])
	secondWithIdx := findKeywordIndexInContext(afterFirstWith, "WITH")
	if secondWithIdx == -1 {
		return nil, false
	}
	firstWith := strings.TrimSpace(afterFirstWith[:secondWithIdx])
	afterSecondWith := strings.TrimSpace(afterFirstWith[secondWithIdx+len("WITH"):])
	returnIdx := findKeywordIndexInContext(afterSecondWith, "RETURN")
	if returnIdx == -1 {
		return nil, false
	}
	secondWith := strings.TrimSpace(afterSecondWith[:returnIdx])
	returnClause, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(strings.TrimSpace(afterSecondWith[returnIdx+len("RETURN"):]))
	// The traversal fast paths implement RETURN … [LIMIT n] only; ORDER BY
	// or SKIP goes to the pipeline, which applies them (#547).
	if orderBy != "" || skipToken != "" {
		return nil, false
	}
	if !strings.HasPrefix(strings.ToUpper(matchSection), "MATCH ") {
		return nil, false
	}
	matchBody := strings.TrimSpace(matchSection[len("MATCH"):])
	whereIdx := findKeywordIndexInContext(matchBody, "WHERE")
	if whereIdx == -1 {
		return nil, false
	}
	patternPart := strings.TrimSpace(matchBody[:whereIdx])
	whereClause := strings.TrimSpace(matchBody[whereIdx+len("WHERE"):])
	pathVar, pattern, ok := splitPathAssignment(patternPart)
	if !ok {
		return nil, false
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.StartNode.variable == "" {
		return nil, false
	}
	compactWhere := compactCypherFragment(whereClause)
	expectedWhere := compactCypherFragment("ALL(n IN nodes(" + pathVar + ") WHERE size(labels(n)) > 0)")
	if compactWhere != expectedWhere {
		return nil, false
	}
	firstWithItems := splitReturnExpressions(strings.TrimSpace(stripOrderByFromClause(firstWith)))
	if len(firstWithItems) != 4 || strings.TrimSpace(firstWithItems[0]) != match.StartNode.variable || strings.TrimSpace(firstWithItems[1]) != "score" || strings.TrimSpace(firstWithItems[2]) != pathVar {
		return nil, false
	}
	lengthCompact := compactCypherFragment(firstWithItems[3])
	if !strings.HasPrefix(lengthCompact, compactCypherFragment("length("+pathVar+") as ")) {
		return nil, false
	}
	dAlias := strings.TrimSpace(firstWithItems[3][strings.LastIndex(strings.ToUpper(firstWithItems[3]), " AS ")+4:])
	if !strings.EqualFold(strings.TrimSpace(extractOrderByClause(firstWith)), dAlias+" ASC") {
		return nil, false
	}
	secondItems := splitReturnExpressions(secondWith)
	if len(secondItems) != 3 || strings.TrimSpace(secondItems[0]) != match.StartNode.variable || strings.TrimSpace(secondItems[1]) != "score" {
		return nil, false
	}
	collectCompact := compactCypherFragment(secondItems[2])
	prefix := compactCypherFragment("collect(" + pathVar + ")[0..")
	if !strings.HasPrefix(collectCompact, prefix) || !strings.Contains(strings.ToUpper(secondItems[2]), " AS ") {
		return nil, false
	}
	asIdx := strings.LastIndex(strings.ToUpper(secondItems[2]), " AS ")
	pathsAlias := strings.TrimSpace(secondItems[2][asIdx+4:])
	sliceStart := strings.Index(collectCompact, "[0..")
	sliceEnd := strings.Index(collectCompact[sliceStart:], "]AS")
	if sliceStart == -1 || sliceEnd == -1 {
		return nil, false
	}
	pathCapToken := strings.TrimSpace(secondItems[2][strings.Index(secondItems[2], "[0..")+4 : strings.LastIndex(secondItems[2], "]")])
	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 3 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" || compactCypherFragment(returnItems[2].expr) != compactCypherFragment("size("+pathsAlias+")") {
		return nil, false
	}
	return &callTailBranchingPathCountPlan{
		match:        match,
		nodeVar:      match.StartNode.variable,
		pathsAlias:   pathsAlias,
		pathCapToken: pathCapToken,
		returnItems:  returnItems,
		limitToken:   limitToken,
	}, true
}

func (e *StorageExecutor) parseCallTailFrontierReachablePlan(ctx context.Context, tail string) (*callTailFrontierReachablePlan, bool) {
	normalized := normalizeCallTailShape(tail)
	firstWithIdx := findKeywordIndexInContext(normalized, "WITH")
	if firstWithIdx == -1 || !strings.HasPrefix(strings.ToUpper(normalized), "MATCH ") {
		return nil, false
	}
	matchPart := strings.TrimSpace(normalized[len("MATCH"):firstWithIdx])
	afterFirstWith := strings.TrimSpace(normalized[firstWithIdx+len("WITH"):])
	secondWithIdx := findKeywordIndexInContext(afterFirstWith, "WITH")
	if secondWithIdx == -1 {
		return nil, false
	}
	firstWith := strings.TrimSpace(afterFirstWith[:secondWithIdx])
	afterSecondWith := strings.TrimSpace(afterFirstWith[secondWithIdx+len("WITH"):])
	returnIdx := findKeywordIndexInContext(afterSecondWith, "RETURN")
	if returnIdx == -1 {
		return nil, false
	}
	secondWith := strings.TrimSpace(afterSecondWith[:returnIdx])
	returnClause, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(strings.TrimSpace(afterSecondWith[returnIdx+len("RETURN"):]))
	// The traversal fast paths implement RETURN … [LIMIT n] only; ORDER BY
	// or SKIP goes to the pipeline, which applies them (#547).
	if orderBy != "" || skipToken != "" {
		return nil, false
	}
	match := e.parseTraversalPattern(ctx, matchPart)
	if match == nil || match.StartNode.variable == "" {
		return nil, false
	}
	firstItems := splitReturnExpressions(firstWith)
	if len(firstItems) != 3 || strings.TrimSpace(firstItems[0]) != match.StartNode.variable || strings.TrimSpace(firstItems[1]) != "score" || !strings.Contains(strings.ToUpper(firstItems[2]), " AS ") {
		return nil, false
	}
	asIdx := strings.LastIndex(strings.ToUpper(firstItems[2]), " AS ")
	dAlias := strings.TrimSpace(firstItems[2][asIdx+4:])
	expectedShortest := compactCypherFragment("length(shortestPath((" + match.StartNode.variable + ")-[:" + strings.Join(match.Relationship.Types, "|") + "*1.." + strconv.Itoa(match.Relationship.MaxHops) + "]->(" + match.EndNode.variable + ")))")
	if compactCypherFragment(strings.TrimSpace(firstItems[2][:asIdx])) != expectedShortest {
		return nil, false
	}
	secondItems := splitReturnExpressions(secondWith)
	if len(secondItems) != 4 || strings.TrimSpace(secondItems[0]) != match.StartNode.variable || strings.TrimSpace(secondItems[1]) != "score" {
		return nil, false
	}
	nearestExpr, nearestAliasRaw := parseProjectionExprAlias(secondItems[2])
	nearestAlias, ok1 := parseAggregateExprAlias(nearestExpr, nearestAliasRaw, "min", dAlias)
	reachableAlias, ok2 := parseCountStarAlias(secondItems[3])
	if !ok1 || !ok2 {
		return nil, false
	}
	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 4 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" || returnItems[2].expr != nearestAlias || returnItems[3].expr != reachableAlias {
		return nil, false
	}
	return &callTailFrontierReachablePlan{match: match, nodeVar: match.StartNode.variable, nearestAlias: nearestAlias, reachableAlias: reachableAlias, returnItems: returnItems, limitToken: limitToken}, true
}

func (e *StorageExecutor) parseCallTailConstrainedMaxDepthPlan(ctx context.Context, tail string) (*callTailConstrainedMaxDepthPlan, bool) {
	normalized := normalizeCallTailShape(tail)
	if !strings.HasPrefix(strings.ToUpper(normalized), "MATCH ") {
		return nil, false
	}
	returnIdx := findKeywordIndexInContext(normalized, "RETURN")
	if returnIdx == -1 {
		return nil, false
	}
	matchWhere := strings.TrimSpace(normalized[len("MATCH"):returnIdx])
	returnClause, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(strings.TrimSpace(normalized[returnIdx+len("RETURN"):]))
	// The traversal fast paths implement RETURN … [LIMIT n] only; ORDER BY
	// or SKIP goes to the pipeline, which applies them (#547).
	if orderBy != "" || skipToken != "" {
		return nil, false
	}
	whereIdx := findKeywordIndexInContext(matchWhere, "WHERE")
	if whereIdx == -1 {
		return nil, false
	}
	patternPart := strings.TrimSpace(matchWhere[:whereIdx])
	whereClause := strings.TrimSpace(matchWhere[whereIdx+len("WHERE"):])
	pathVar, pattern, ok := splitPathAssignment(patternPart)
	if !ok {
		return nil, false
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.StartNode.variable == "" {
		return nil, false
	}
	parts := splitConjunction(whereClause)
	if len(parts) != 2 {
		return nil, false
	}
	minWeightToken, categoriesToken, ok := parseConstrainedTraversalPredicates(parts, pathVar)
	if !ok {
		return nil, false
	}
	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 3 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" {
		return nil, false
	}
	alias, ok := parseAggregateExprAlias(returnItems[2].expr, returnItems[2].alias, "max", "length("+pathVar+")")
	if !ok {
		return nil, false
	}
	return &callTailConstrainedMaxDepthPlan{match: match, nodeVar: match.StartNode.variable, aggregateExpr: returnItems[2].expr, aggregateAlias: alias, minWeightToken: minWeightToken, categoriesToken: categoriesToken, returnItems: returnItems, limitToken: limitToken}, true
}

func (e *StorageExecutor) maxDepthForTraversalMatch(startNode *storage.Node, match *TraversalMatch) (int, error) {
	if startNode == nil || match == nil {
		return 0, nil
	}
	ctx := &callTailMaxDepthContext{
		nodeCache:  map[storage.NodeID]*storage.Node{startNode.ID: startNode},
		visited:    make(map[storage.EdgeID]bool),
		relTypeSet: buildRelTypeSet(match.Relationship.Types),
	}
	if err := e.maxDepthForTraversalMatchFromNode(startNode, 0, match, ctx); err != nil {
		return 0, err
	}
	return ctx.best, nil
}

type callTailPathPredicate struct {
	requireAllNodesLabeled bool
	minWeight              *float64
	allowedCategory        map[string]struct{}
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
	for name, value := range params {
		values["$"+name] = value
	}
	return values
}

func seedValuesForRow(seed *ExecuteResult, row []interface{}) map[string]interface{} {
	values := make(map[string]interface{}, len(seed.Columns))
	for i, col := range seed.Columns {
		if i < len(row) {
			values[col] = row[i]
		}
	}
	return values
}

func projectReturnItemsFromValues(e *StorageExecutor, items []returnItem, values map[string]interface{}) []interface{} {
	row := make([]interface{}, len(items))
	for i, item := range items {
		row[i] = e.evaluateExpressionFromValues(item.expr, values)
	}
	return row
}

func normalizeCallTailShape(tail string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(tail)), " ")
}

func compactCypherFragment(value string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), " ", ""))
}

func stripOrderByFromClause(clause string) string {
	idx := findKeywordIndexInContext(clause, "ORDER")
	if idx == -1 {
		return clause
	}
	return strings.TrimSpace(clause[:idx])
}

func extractOrderByClause(clause string) string {
	idx := findKeywordIndexInContext(clause, "ORDER")
	if idx == -1 {
		return ""
	}
	part := strings.TrimSpace(clause[idx:])
	if strings.HasPrefix(strings.ToUpper(part), "ORDER BY") {
		return strings.TrimSpace(part[len("ORDER BY"):])
	}
	return ""
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

func parseAggregateExprAlias(expr, alias, funcName, inner string) (string, bool) {
	if !isSimpleIdentifier(alias) {
		return "", false
	}
	if compactCypherFragment(expr) != compactCypherFragment(funcName+"("+inner+")") {
		return "", false
	}
	return alias, true
}

func parseCountStarAlias(item string) (string, bool) {
	upper := strings.ToUpper(item)
	asIdx := strings.LastIndex(upper, " AS ")
	if asIdx == -1 || compactCypherFragment(item[:asIdx]) != "COUNT(*)" {
		return "", false
	}
	return strings.TrimSpace(item[asIdx+4:]), true
}

func splitConjunction(whereClause string) []string {
	parts := strings.Split(whereClause, " AND ")
	if len(parts) != 2 {
		parts = strings.Split(strings.ToUpper(whereClause), " AND ")
	}
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		result = append(result, strings.TrimSpace(part))
	}
	return result
}

func parseConstrainedTraversalPredicates(parts []string, pathVar string) (string, string, bool) {
	var minWeightToken string
	var categoriesToken string
	for _, part := range parts {
		compact := compactCypherFragment(part)
		prefixRel := compactCypherFragment("any(r IN relationships(" + pathVar + ") WHERE r.weight >=")
		prefixNode := compactCypherFragment("any(n IN nodes(" + pathVar + ") WHERE n.category IN")
		switch {
		case strings.HasPrefix(compact, prefixRel) && strings.HasSuffix(compact, ")"):
			start := strings.Index(strings.ToUpper(part), ">=")
			if start == -1 {
				return "", "", false
			}
			minWeightToken = strings.TrimSpace(strings.TrimSuffix(part[start+2:], ")"))
		case strings.HasPrefix(compact, prefixNode) && strings.HasSuffix(compact, ")"):
			needle := "CATEGORY IN "
			upperPart := strings.ToUpper(part)
			inIdx := strings.Index(upperPart, needle)
			if inIdx == -1 {
				return "", "", false
			}
			categoriesToken = strings.TrimSpace(strings.TrimSuffix(part[inIdx+len(needle):], ")"))
		}
	}
	return minWeightToken, categoriesToken, minWeightToken != "" && categoriesToken != ""
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

func resolveFloatLiteralOrParam(ctx context.Context, raw string) (float64, bool) {
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
		return toFloat64(value)
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func resolveStringSliceLiteralOrParam(ctx context.Context, raw string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(raw, "$") {
		params := getParamsFromContext(ctx)
		if params == nil {
			return nil, false
		}
		value, ok := params[strings.TrimPrefix(raw, "$")]
		if !ok {
			return nil, false
		}
		switch typed := value.(type) {
		case []string:
			return append([]string{}, typed...), true
		case []interface{}:
			out := make([]string, 0, len(typed))
			for _, item := range typed {
				text, ok := item.(string)
				if !ok {
					return nil, false
				}
				out = append(out, text)
			}
			return out, true
		}
		return nil, false
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(raw[1 : len(raw)-1])
		if inner == "" {
			return []string{}, true
		}
		parts := strings.Split(inner, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.Trim(strings.TrimSpace(part), "'\"")
			out = append(out, trimmed)
		}
		return out, true
	}
	return nil, false
}

func (e *StorageExecutor) countTraversalPathsWithCap(startNode *storage.Node, match *TraversalMatch, cap int, predicate callTailPathPredicate) (int, error) {
	if startNode == nil || match == nil || cap == 0 {
		return 0, nil
	}
	ctx := &callTailTraversalContext{nodeCache: map[storage.NodeID]*storage.Node{startNode.ID: startNode}, visitedEdges: make(map[storage.EdgeID]bool), relTypeSet: buildRelTypeSet(match.Relationship.Types)}
	count, _, err := e.walkCallTailTraversal(startNode, 0, match, ctx, predicate, false, cap, predicateMatchesCategory(startNode, predicate.allowedCategory), false)
	return count, err
}

func (e *StorageExecutor) maxDepthForPredicateTraversal(startNode *storage.Node, match *TraversalMatch, predicate callTailPathPredicate) (int, bool, error) {
	if startNode == nil || match == nil {
		return 0, false, nil
	}
	ctx := &callTailTraversalContext{nodeCache: map[storage.NodeID]*storage.Node{startNode.ID: startNode}, visitedEdges: make(map[storage.EdgeID]bool), relTypeSet: buildRelTypeSet(match.Relationship.Types)}
	_, maxDepth, err := e.walkCallTailTraversal(startNode, 0, match, ctx, predicate, false, -1, predicateMatchesCategory(startNode, predicate.allowedCategory), true)
	if err != nil {
		return 0, false, err
	}
	return maxDepth, maxDepth > 0, nil
}

type callTailTraversalContext struct {
	nodeCache    map[storage.NodeID]*storage.Node
	visitedEdges map[storage.EdgeID]bool
	relTypeSet   map[string]struct{}
}

func (e *StorageExecutor) walkCallTailTraversal(current *storage.Node, depth int, match *TraversalMatch, ctx *callTailTraversalContext, predicate callTailPathPredicate, hasWeight bool, cap int, hasCategory bool, trackMax bool) (int, int, error) {
	count := 0
	maxDepth := 0
	if depth >= match.Relationship.MinHops && e.matchesEndPattern(current, &match.EndNode) {
		qualifies := true
		if predicate.requireAllNodesLabeled && len(current.Labels) == 0 {
			qualifies = false
		}
		if predicate.minWeight != nil && !hasWeight {
			qualifies = false
		}
		if len(predicate.allowedCategory) > 0 && !hasCategory {
			qualifies = false
		}
		if qualifies {
			count = 1
			maxDepth = depth
			if cap > 0 && count >= cap && !trackMax {
				return count, maxDepth, nil
			}
		}
	}
	if depth >= match.Relationship.MaxHops {
		return count, maxDepth, nil
	}
	edges, err := e.callTailTraversalEdges(current, match)
	if err != nil {
		return 0, 0, err
	}
	for _, edge := range edges {
		if edge == nil || ctx.visitedEdges[edge.ID] || !callTailEdgeMatchesTypes(edge, match.Relationship.Types, ctx.relTypeSet) {
			continue
		}
		nextNodeID := callTailNextNodeID(current.ID, edge, match.Relationship.Direction)
		nextNode, err := e.callTailLoadNode(nextNodeID, ctx)
		if err != nil || nextNode == nil {
			if err != nil {
				return 0, 0, err
			}
			continue
		}
		if predicate.requireAllNodesLabeled && len(nextNode.Labels) == 0 {
			continue
		}
		nextHasWeight := hasWeight
		if predicate.minWeight != nil {
			if weight, ok := toFloat64(edge.Properties["weight"]); ok && weight >= *predicate.minWeight {
				nextHasWeight = true
			}
		}
		nextHasCategory := hasCategory || predicateMatchesCategory(nextNode, predicate.allowedCategory)
		ctx.visitedEdges[edge.ID] = true
		subCount, subMaxDepth, err := e.walkCallTailTraversal(nextNode, depth+1, match, ctx, predicate, nextHasWeight, cap-count, nextHasCategory, trackMax)
		delete(ctx.visitedEdges, edge.ID)
		if err != nil {
			return 0, 0, err
		}
		count += subCount
		if subMaxDepth > maxDepth {
			maxDepth = subMaxDepth
		}
		if cap > 0 && count >= cap && !trackMax {
			break
		}
	}
	return count, maxDepth, nil
}

func predicateMatchesCategory(node *storage.Node, allowed map[string]struct{}) bool {
	if node == nil || len(allowed) == 0 || node.Properties == nil {
		return false
	}
	category, ok := node.Properties["category"].(string)
	if !ok {
		return false
	}
	_, found := allowed[category]
	return found
}

func (e *StorageExecutor) shortestReachableStats(startNode *storage.Node, match *TraversalMatch) (int, int, error) {
	if startNode == nil || match == nil {
		return 0, 0, nil
	}
	type queueItem struct {
		node  *storage.Node
		depth int
	}
	ctx := &callTailTraversalContext{nodeCache: map[storage.NodeID]*storage.Node{startNode.ID: startNode}, relTypeSet: buildRelTypeSet(match.Relationship.Types)}
	visitedNodes := map[storage.NodeID]bool{startNode.ID: true}
	queue := []queueItem{{node: startNode, depth: 0}}
	nearest := 0
	reachable := 0
	for head := 0; head < len(queue); head++ {
		current := queue[head]
		if current.depth >= match.Relationship.MaxHops {
			continue
		}
		edges, err := e.callTailTraversalEdges(current.node, match)
		if err != nil {
			return 0, 0, err
		}
		for _, edge := range edges {
			if edge == nil || !callTailEdgeMatchesTypes(edge, match.Relationship.Types, ctx.relTypeSet) {
				continue
			}
			nextNodeID := callTailNextNodeID(current.node.ID, edge, match.Relationship.Direction)
			if visitedNodes[nextNodeID] {
				continue
			}
			nextNode, err := e.callTailLoadNode(nextNodeID, ctx)
			if err != nil {
				return 0, 0, err
			}
			if nextNode == nil {
				continue
			}
			visitedNodes[nextNodeID] = true
			nextDepth := current.depth + 1
			if nextDepth >= match.Relationship.MinHops && e.matchesEndPattern(nextNode, &match.EndNode) {
				reachable++
				if nearest == 0 || nextDepth < nearest {
					nearest = nextDepth
				}
			}
			queue = append(queue, queueItem{node: nextNode, depth: nextDepth})
		}
	}
	return nearest, reachable, nil
}

func (e *StorageExecutor) callTailTraversalEdges(current *storage.Node, match *TraversalMatch) ([]*storage.Edge, error) {
	switch match.Relationship.Direction {
	case "outgoing":
		return e.storage.GetOutgoingEdges(current.ID)
	case "incoming":
		return e.storage.GetIncomingEdges(current.ID)
	default:
		return undirectedIncidentEdges(e.storage, current.ID)
	}
}

func callTailEdgeMatchesTypes(edge *storage.Edge, relTypes []string, relTypeSet map[string]struct{}) bool {
	if len(relTypes) == 0 {
		return true
	}
	if len(relTypes) == 1 {
		return edge.Type == relTypes[0]
	}
	_, ok := relTypeSet[edge.Type]
	return ok
}

func callTailNextNodeID(currentID storage.NodeID, edge *storage.Edge, direction string) storage.NodeID {
	if direction == "outgoing" || (direction == "both" && edge.StartNode == currentID) {
		return edge.EndNode
	}
	return edge.StartNode
}

func (e *StorageExecutor) callTailLoadNode(nodeID storage.NodeID, ctx *callTailTraversalContext) (*storage.Node, error) {
	if node, ok := ctx.nodeCache[nodeID]; ok {
		return node, nil
	}
	node, err := e.storage.GetNode(nodeID)
	if err != nil || node == nil {
		return node, err
	}
	ctx.nodeCache[nodeID] = node
	return node, nil
}

type callTailMaxDepthContext struct {
	nodeCache  map[storage.NodeID]*storage.Node
	visited    map[storage.EdgeID]bool
	relTypeSet map[string]struct{}
	best       int
}

func (e *StorageExecutor) maxDepthForTraversalMatchFromNode(
	current *storage.Node,
	depth int,
	match *TraversalMatch,
	ctx *callTailMaxDepthContext,
) error {
	if current == nil || match == nil || ctx == nil {
		return nil
	}
	if depth >= match.Relationship.MinHops && e.matchesEndPattern(current, &match.EndNode) && depth > ctx.best {
		ctx.best = depth
	}
	if depth >= match.Relationship.MaxHops {
		return nil
	}

	var edges []*storage.Edge
	switch match.Relationship.Direction {
	case "outgoing":
		outgoing, err := e.storage.GetOutgoingEdges(current.ID)
		if err != nil {
			return err
		}
		edges = outgoing
	case "incoming":
		incoming, err := e.storage.GetIncomingEdges(current.ID)
		if err != nil {
			return err
		}
		edges = incoming
	default:
		incident, err := undirectedIncidentEdges(e.storage, current.ID)
		if err != nil {
			return err
		}
		edges = incident
	}

	for _, edge := range edges {
		if edge == nil || ctx.visited[edge.ID] {
			continue
		}
		if len(match.Relationship.Types) > 0 {
			if len(match.Relationship.Types) == 1 {
				if edge.Type != match.Relationship.Types[0] {
					continue
				}
			} else {
				if _, ok := ctx.relTypeSet[edge.Type]; !ok {
					continue
				}
			}
		}

		var nextNodeID storage.NodeID
		if match.Relationship.Direction == "outgoing" || (match.Relationship.Direction == "both" && edge.StartNode == current.ID) {
			nextNodeID = edge.EndNode
		} else {
			nextNodeID = edge.StartNode
		}

		nextNode := ctx.nodeCache[nextNodeID]
		if nextNode == nil {
			loaded, err := e.storage.GetNode(nextNodeID)
			if err != nil {
				return err
			}
			if loaded == nil {
				continue
			}
			nextNode = loaded
			ctx.nodeCache[nextNodeID] = nextNode
		}

		ctx.visited[edge.ID] = true
		if err := e.maxDepthForTraversalMatchFromNode(nextNode, depth+1, match, ctx); err != nil {
			return err
		}
		delete(ctx.visited, edge.ID)
	}
	return nil
}

func buildIDCaseExpression(nodeVar string, valueByID map[string]interface{}) string {
	// Deterministic order for stable query text/testing
	ids := make([]string, 0, len(valueByID))
	for k := range valueByID {
		ids = append(ids, k)
	}
	// simple insertion sort avoids extra imports
	for i := 1; i < len(ids); i++ {
		j := i
		for j > 0 && ids[j] < ids[j-1] {
			ids[j], ids[j-1] = ids[j-1], ids[j]
			j--
		}
	}
	var b strings.Builder
	b.WriteString("CASE id(")
	b.WriteString(nodeVar)
	b.WriteString(")")
	for _, id := range ids {
		b.WriteString(" WHEN '")
		b.WriteString(strings.ReplaceAll(id, "'", "\\'"))
		b.WriteString("' THEN ")
		b.WriteString(cypherLiteral(valueByID[id]))
	}
	b.WriteString(" ELSE null END")
	return b.String()
}

func rewriteFirstWithScalar(tail, scalarVar, caseExpr string) (string, bool) {
	withIdx := findKeywordIndexInContext(tail, "WITH")
	if withIdx == -1 {
		return "", false
	}
	afterWith := strings.TrimSpace(tail[withIdx+len("WITH"):])
	if afterWith == "" {
		return "", false
	}
	end := len(afterWith)
	for _, kw := range []string{"WHERE", "RETURN", "ORDER", "SKIP", "LIMIT", "UNWIND", "MATCH", "OPTIONAL", "CALL", "SET", "REMOVE", "DELETE", "DETACH", "MERGE", "CREATE"} {
		if idx := findKeywordIndexInContext(afterWith, kw); idx != -1 && idx < end {
			end = idx
		}
	}
	withClause := strings.TrimSpace(afterWith[:end])
	items := splitReturnExpressions(withClause)
	if len(items) == 0 {
		return "", false
	}
	replaced := false
	for i := range items {
		item := strings.TrimSpace(items[i])
		if item == scalarVar {
			items[i] = caseExpr + " AS " + scalarVar
			replaced = true
		}
	}
	if !replaced {
		return "", false
	}
	newWith := "WITH " + strings.Join(items, ", ")
	rest := strings.TrimSpace(afterWith[end:])
	prefix := strings.TrimSpace(tail[:withIdx])
	if prefix == "" {
		if rest == "" {
			return newWith, true
		}
		return newWith + " " + rest, true
	}
	if rest == "" {
		return prefix + " " + newWith, true
	}
	return prefix + " " + newWith + " " + rest, true
}

func cypherLiteral(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return "'" + strings.ReplaceAll(t, "'", "\\'") + "'"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "\\'") + "'"
	}
}

func isIdentifierReferenced(query, identifier string) bool {
	if strings.TrimSpace(identifier) == "" {
		return false
	}
	q := query
	id := identifier
	idLen := len(id)
	if idLen == 0 || len(q) < idLen {
		return false
	}
	for i := 0; i <= len(q)-idLen; i++ {
		if q[i:i+idLen] != id {
			continue
		}
		if i > 0 && isIdentChar(q[i-1]) {
			continue
		}
		end := i + idLen
		if end < len(q) && isIdentChar(q[end]) {
			continue
		}
		return true
	}
	return false
}

func isIdentChar(b byte) bool {
	return (b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z') ||
		(b >= '0' && b <= '9') ||
		b == '_'
}

// findKeywordIndexInContext finds a keyword in context, avoiding matches inside quotes
func findKeywordIndexInContext(s, keyword string) int {
	upper := strings.ToUpper(s)
	keyword = strings.ToUpper(keyword)

	inQuote := false
	quoteChar := rune(0)

	for i := 0; i <= len(s)-len(keyword); i++ {
		c := rune(s[i])

		// Track quote state
		if c == '\'' || c == '"' {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			continue
		}

		if inQuote {
			continue
		}

		// Check for keyword match with word boundary
		if strings.HasPrefix(upper[i:], keyword) {
			// Check left boundary (must be start or non-alphanumeric)
			if i > 0 {
				prev := s[i-1]
				if (prev >= 'A' && prev <= 'Z') || (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' {
					continue
				}
			}
			// Check right boundary
			end := i + len(keyword)
			if end < len(s) {
				next := s[end]
				if (next >= 'A' && next <= 'Z') || (next >= 'a' && next <= 'z') || (next >= '0' && next <= '9') || next == '_' {
					continue
				}
			}
			if isWithKeyword(keyword) && isOperatorWith(s, i) {
				continue
			}
			return i
		}
	}
	return -1
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
			return nil, newSemanticError(
				"Neo.ClientError.Statement.ArgumentError",
				"InvalidArgumentType",
				"SKIP and LIMIT require a non-negative INTEGER",
			)
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
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}
	// A RETURN right after YIELD is the start of the tail, like any other
	// clause, so it runs over all yielded rows (executeCallTail): an aggregate
	// in it groups the rows, and ORDER BY / SKIP / LIMIT apply to all of them.
	parts := splitChainedProcedureCall(cypher)
	callCypher := parts.callOnly
	tailCypher := parts.tail

	upper := strings.ToUpper(callCypher)

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(callCypher)
	if err := e.validateYieldModifiers(yield, inQuery || strings.TrimSpace(tailCypher) != ""); err != nil {
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
		args, err := extractProcedureInvocationArguments(ctx, proc.Spec, callCypher)
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
		result, err := proc.Handler(ctx, e, callCypher, args)
		if err != nil {
			return nil, err
		}
		if yield != nil {
			result, err = e.applyYieldFilter(ctx, result, yield)
			if err != nil {
				return nil, err
			}
		}
		if strings.TrimSpace(tailCypher) != "" {
			return e.executeCallTail(ctx, result, tailCypher)
		}
		return result, nil
	}

	var result *ExecuteResult
	var err error

	switch {
	// Neo4j Vector Index Procedures
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYNODES"):
		result, err = e.callDbIndexVectorQueryNodes(ctx, callCypher)
	// Neo4j Fulltext Index Procedures
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYNODES"):
		result, err = e.callDbIndexFulltextQueryNodes(callCypher)
	// APOC Procedures (graph traversal)
	case strings.Contains(upper, "APOC.PATH.SUBGRAPHNODES"):
		result, err = e.callApocPathSubgraphNodes(callCypher)
	case strings.Contains(upper, "APOC.PATH.EXPAND"):
		result, err = e.callApocPathExpand(ctx, callCypher)
	case strings.Contains(upper, "APOC.PATH.SPANNINGTREE"):
		result, err = e.callApocPathSpanningTree(callCypher)
	// APOC Graph Algorithms
	case strings.Contains(upper, "APOC.ALGO.DIJKSTRA"):
		result, err = e.callApocAlgoDijkstra(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.ASTAR"):
		result, err = e.callApocAlgoAStar(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.ALLSIMPLEPATHS"):
		result, err = e.callApocAlgoAllSimplePaths(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.PAGERANK"):
		result, err = e.callApocAlgoPageRank(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.BETWEENNESS"):
		result, err = e.callApocAlgoBetweenness(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.CLOSENESS"):
		result, err = e.callApocAlgoCloseness(ctx, callCypher)
	// APOC Community Detection
	case strings.Contains(upper, "APOC.ALGO.LOUVAIN"):
		result, err = e.callApocAlgoLouvain(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.LABELPROPAGATION"):
		result, err = e.callApocAlgoLabelPropagation(ctx, callCypher)
	case strings.Contains(upper, "APOC.ALGO.WCC"):
		result, err = e.callApocAlgoWCC(ctx, callCypher)
	// APOC Neighbor Traversal
	case strings.Contains(upper, "APOC.NEIGHBORS.TOHOP"):
		result, err = e.callApocNeighborsTohop(ctx, callCypher)
	case strings.Contains(upper, "APOC.NEIGHBORS.BYHOP"):
		result, err = e.callApocNeighborsByhop(ctx, callCypher)
	// APOC Load/Export Procedures
	case strings.Contains(upper, "APOC.LOAD.JSONARRAY"):
		result, err = e.callApocLoadJsonArray(ctx, callCypher)
	case strings.Contains(upper, "APOC.LOAD.JSON"):
		result, err = e.callApocLoadJson(ctx, callCypher)
	case strings.Contains(upper, "APOC.LOAD.CSV"):
		result, err = e.callApocLoadCsv(ctx, callCypher)
	case strings.Contains(upper, "APOC.EXPORT.JSON.ALL"):
		result, err = e.callApocExportJsonAll(ctx, callCypher)
	case strings.Contains(upper, "APOC.EXPORT.JSON.QUERY"):
		result, err = e.callApocExportJsonQuery(ctx, callCypher)
	case strings.Contains(upper, "APOC.EXPORT.CSV.ALL"):
		result, err = e.callApocExportCsvAll(ctx, callCypher)
	case strings.Contains(upper, "APOC.EXPORT.CSV.QUERY"):
		result, err = e.callApocExportCsvQuery(ctx, callCypher)
	case strings.Contains(upper, "APOC.IMPORT.JSON"):
		result, err = e.callApocImportJson(ctx, callCypher)
	// NornicDB Extensions
	case strings.Contains(upper, "NORNICDB.VERSION"):
		result, err = e.callNornicDbVersion()
	case strings.Contains(upper, "NORNICDB.STATS"):
		result, err = e.callNornicDbStats()
	case strings.Contains(upper, "NORNICDB.DECAY.INFO"):
		result, err = e.callNornicDbDecayInfo()
	case strings.Contains(upper, "NORNICDB.KNOWLEDGEPOLICY.INFO"):
		result, err = e.callNornicDbKnowledgePolicyInfo()
	// Seam-aligned RAG procedures
	case strings.Contains(upper, "DB.RETRIEVE"):
		result, err = e.callDbRetrieve(ctx, callCypher)
	case strings.Contains(upper, "DB.RRETRIEVE"):
		result, err = e.callDbRRetrieve(ctx, callCypher)
	case strings.Contains(upper, "DB.RERANK"):
		result, err = e.callDbRerank(ctx, callCypher)
	case strings.Contains(upper, "DB.INFER"):
		result, err = e.callDbInfer(ctx, callCypher)
	// Neo4j Schema/Metadata Procedures
	case strings.Contains(upper, "DB.SCHEMA.VISUALIZATION"):
		result, err = e.callDbSchemaVisualization()
	case strings.Contains(upper, "DB.SCHEMA.NODEPROPERTIES"):
		result, err = e.callDbSchemaNodeProperties()
	case strings.Contains(upper, "DB.SCHEMA.RELPROPERTIES"):
		result, err = e.callDbSchemaRelProperties()
	case strings.Contains(upper, "DB.LABELS"):
		result, err = e.callDbLabels()
	case strings.Contains(upper, "DB.RELATIONSHIPTYPES"):
		result, err = e.callDbRelationshipTypes()
	case strings.Contains(upper, "DB.INDEXES"):
		result, err = e.callDbIndexes()
	case strings.Contains(upper, "DB.INDEX.STATS"):
		result, err = e.callDbIndexStats()
	case strings.Contains(upper, "DB.CONSTRAINTS"):
		result, err = e.callDbConstraints()
	case strings.Contains(upper, "DB.PROPERTYKEYS"):
		result, err = e.callDbPropertyKeys()
	// Neo4j GDS Link Prediction Procedures (topological)
	case strings.Contains(upper, "GDS.LINKPREDICTION.ADAMICADAR.STREAM"):
		result, err = e.callGdsLinkPredictionAdamicAdar(ctx, callCypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.COMMONNEIGHBORS.STREAM"):
		result, err = e.callGdsLinkPredictionCommonNeighbors(ctx, callCypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.RESOURCEALLOCATION.STREAM"):
		result, err = e.callGdsLinkPredictionResourceAllocation(ctx, callCypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.PREFERENTIALATTACHMENT.STREAM"):
		result, err = e.callGdsLinkPredictionPreferentialAttachment(ctx, callCypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.JACCARD.STREAM"):
		result, err = e.callGdsLinkPredictionJaccard(ctx, callCypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.PREDICT.STREAM"):
		result, err = e.callGdsLinkPredictionPredict(ctx, callCypher)
	// GDS Graph Management and FastRP
	case strings.Contains(upper, "GDS.VERSION"):
		result, err = e.callGdsVersion()
	case strings.Contains(upper, "GDS.GRAPH.LIST"):
		result, err = e.callGdsGraphList()
	case strings.Contains(upper, "GDS.GRAPH.DROP"):
		result, err = e.callGdsGraphDrop(callCypher)
	case strings.Contains(upper, "GDS.GRAPH.PROJECT"):
		result, err = e.callGdsGraphProject(callCypher)
	case strings.Contains(upper, "GDS.FASTRP.STREAM"):
		result, err = e.callGdsFastRPStream(callCypher)
	case strings.Contains(upper, "GDS.FASTRP.STATS"):
		result, err = e.callGdsFastRPStats(callCypher)
	// Additional Neo4j procedures for compatibility
	case strings.Contains(upper, "DB.INFO"):
		result, err = e.callDbInfo()
	case strings.Contains(upper, "DB.PING"):
		result, err = e.callDbPing()
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexFulltextQueryRelationships(callCypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexVectorQueryRelationships(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.EMBED"):
		result, err = e.callDbIndexVectorEmbed(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.CREATENODEINDEX"):
		result, err = e.callDbIndexVectorCreateNodeIndex(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.CREATERELATIONSHIPINDEX"):
		result, err = e.callDbIndexVectorCreateRelationshipIndex(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.CREATENODEINDEX"):
		result, err = e.callDbIndexFulltextCreateNodeIndex(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.CREATERELATIONSHIPINDEX"):
		result, err = e.callDbIndexFulltextCreateRelationshipIndex(ctx, callCypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.DROP"):
		result, err = e.callDbIndexFulltextDrop(callCypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.DROP"):
		result, err = e.callDbIndexVectorDrop(callCypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.LISTAVAILABLEANALYZERS"):
		result, err = e.callDbIndexFulltextListAvailableAnalyzers()
	case strings.Contains(upper, "DB.CREATE.SETNODEVECTORPROPERTY"):
		result, err = e.callDbCreateSetNodeVectorProperty(ctx, callCypher)
	case strings.Contains(upper, "DB.CREATE.SETRELATIONSHIPVECTORPROPERTY"):
		result, err = e.callDbCreateSetRelationshipVectorProperty(ctx, callCypher)
	case strings.Contains(upper, "DBMS.INFO"):
		result, err = e.callDbmsInfo()
	case strings.Contains(upper, "DBMS.LISTCONFIG"):
		result, err = e.callDbmsListConfig()
	case strings.Contains(upper, "DBMS.CLIENTCONFIG"):
		result, err = e.callDbmsClientConfig()
	case strings.Contains(upper, "DBMS.LISTCONNECTIONS"):
		result, err = e.callDbmsListConnections()
	case strings.Contains(upper, "DBMS.COMPONENTS"):
		result, err = e.callDbmsComponents()
	case strings.Contains(upper, "DBMS.PROCEDURES"):
		result, err = e.callDbmsProcedures()
	case strings.Contains(upper, "DBMS.FUNCTIONS"):
		result, err = e.callDbmsFunctions()
	// Transaction log query procedures (NornicDB extension for Idea #7)
	case strings.Contains(upper, "DB.TXLOG.ENTRIES"):
		result, err = e.callDbTxlogEntries(ctx, callCypher)
	case strings.Contains(upper, "DB.TXLOG.BYTXID"):
		result, err = e.callDbTxlogByTxID(ctx, callCypher)
	// Temporal helper procedures (NornicDB extension for Idea #7)
	case strings.Contains(upper, "DB.TEMPORAL.ASSERTNOOVERLAP"):
		result, err = e.callDbTemporalAssertNoOverlap(ctx, callCypher)
	case strings.Contains(upper, "DB.TEMPORAL.ASOF"):
		result, err = e.callDbTemporalAsOf(ctx, callCypher)
	// Transaction metadata (Neo4j tx.setMetaData)
	case strings.Contains(upper, "TX.SETMETADATA"):
		result, err = e.callTxSetMetadata(ctx, callCypher)
	// Index management procedures
	case strings.Contains(upper, "DB.AWAITINDEXES"):
		result, err = e.callDbAwaitIndexes(callCypher)
	case strings.Contains(upper, "DB.AWAITINDEX"):
		result, err = e.callDbAwaitIndex(callCypher)
	case strings.Contains(upper, "DB.RESAMPLEINDEX"):
		result, err = e.callDbResampleIndex(callCypher)
	// Query statistics procedures (longer matches first)
	case strings.Contains(upper, "DB.STATS.RETRIEVEALLANTHESTATS"):
		result, err = e.callDbStatsRetrieveAllAnTheStats()
	case strings.Contains(upper, "DB.STATS.RETRIEVE"):
		result, err = e.callDbStatsRetrieve(callCypher)
	case strings.Contains(upper, "DB.STATS.COLLECT"):
		result, err = e.callDbStatsCollect(callCypher)
	case strings.Contains(upper, "DB.STATS.CLEAR"):
		result, err = e.callDbStatsClear()
	case strings.Contains(upper, "DB.STATS.STATUS"):
		result, err = e.callDbStatsStatus()
	case strings.Contains(upper, "DB.STATS.STOP"):
		result, err = e.callDbStatsStop()
	// Database cleardown procedures (for testing)
	case strings.Contains(upper, "DB.CLEARQUERYCACHES"):
		result, err = e.callDbClearQueryCaches()
	// APOC Dynamic Cypher Execution
	case strings.Contains(upper, "APOC.CYPHER.RUNMANY"):
		result, err = e.callApocCypherRunMany(ctx, callCypher)
	case strings.Contains(upper, "APOC.CYPHER.RUN"):
		result, err = e.callApocCypherRun(ctx, callCypher)
	case strings.Contains(upper, "APOC.CYPHER.DOITALL"):
		result, err = e.callApocCypherRun(ctx, callCypher) // Alias
	// APOC Periodic/Batch Operations
	case strings.Contains(upper, "APOC.PERIODIC.ITERATE"):
		result, err = e.callApocPeriodicIterate(ctx, callCypher)
	case strings.Contains(upper, "APOC.PERIODIC.COMMIT"):
		result, err = e.callApocPeriodicCommit(ctx, callCypher)
	case strings.Contains(upper, "APOC.PERIODIC.ROCK_N_ROLL"):
		result, err = e.callApocPeriodicIterate(ctx, callCypher) // Alias
	default:
		// Extract procedure name for clearer error
		procName := extractProcedureName(callCypher)
		return nil, newSemanticError(
			"Neo.ClientError.Procedure.ProcedureError",
			"ProcedureNotFound",
			fmt.Sprintf("unknown procedure %s", procName),
		)
	}

	// Return error if procedure failed
	if err != nil {
		return nil, err
	}

	// Apply YIELD clause filtering (WHERE, column selection, aliasing)
	if yield != nil {
		result, err = e.applyYieldFilter(ctx, result, yield)
		if err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(tailCypher) != "" {
		return e.executeCallTail(ctx, result, tailCypher)
	}
	return result, nil
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
	for _, label := range sortedStringSet(labelSet) {
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
	for _, relType := range sortedStringSet(typeSet) {
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
	if schema := e.storage.GetSchema(); schema != nil {
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
	// Return a simplified schema visualization
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	// Collect unique labels and relationship types
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	relTypeSet := make(map[string]bool)
	for _, edge := range edges {
		relTypeSet[edge.Type] = true
	}

	// Build schema nodes (one per label)
	var schemaNodes []map[string]interface{}
	for label := range labelSet {
		schemaNodes = append(schemaNodes, map[string]interface{}{
			"label": label,
		})
	}

	// Build schema relationships
	var schemaRels []map[string]interface{}
	for relType := range relTypeSet {
		schemaRels = append(schemaRels, map[string]interface{}{
			"type": relType,
		})
	}

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships"},
		Rows: [][]interface{}{
			{schemaNodes, schemaRels},
		},
	}, nil
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
