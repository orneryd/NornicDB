// CALL procedure implementations for NornicDB.
// This file contains all CALL procedures for Neo4j compatibility and NornicDB extensions.
//
// Phase 3: Core Procedures Implementation
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
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// toFloat32Slice is a package-level alias to convert.ToFloat32Slice for internal use.
func toFloat32Slice(v interface{}) []float32 {
	return convert.ToFloat32Slice(v)
}

// yieldClause represents parsed YIELD information from a CALL statement.
// Syntax: CALL procedure() YIELD var1, var2 AS alias WHERE condition RETURN ... ORDER BY ... LIMIT n SKIP m
type yieldClause struct {
	items      []yieldItem // List of yielded items (possibly with aliases)
	yieldAll   bool        // YIELD * - return all columns
	where      string      // Optional WHERE condition after YIELD
	hasReturn  bool        // Whether there's a RETURN clause after
	returnExpr string      // The RETURN expression if present
	orderBy    string      // ORDER BY clause (e.g., "score DESC")
	limit      int         // LIMIT value (-1 if not specified)
	skip       int         // SKIP value (-1 if not specified)
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
// Handles: YIELD *, YIELD a, b, YIELD a AS x, b AS y, YIELD a WHERE a.score > 0.5
func parseYieldClause(cypher string) *yieldClause {
	// Normalize whitespace: replace newlines/tabs with spaces for keyword detection
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")
	yieldIdx := findKeywordIndexInContext(normalized, "YIELD")
	if yieldIdx == -1 {
		return nil
	}

	result := &yieldClause{
		items: []yieldItem{},
		limit: -1,
		skip:  -1,
	}

	// Get everything after YIELD
	afterYield := strings.TrimSpace(normalized[yieldIdx+len("YIELD"):])

	// Check for YIELD *
	trimmedYield := strings.TrimSpace(afterYield)
	if len(trimmedYield) > 0 && trimmedYield[0] == '*' {
		result.yieldAll = true
		afterYield = strings.TrimSpace(afterYield[1:])
	}

	// Limit YIELD parsing to the CALL-clause scope only. Anything after the first
	// outer clause boundary belongs to the subsequent query pipeline.
	yieldScope := scopeYieldToCallClause(afterYield)
	whereIdx := findKeywordIndexInContext(yieldScope, "WHERE")
	returnIdx := findKeywordIndexInContext(yieldScope, "RETURN")
	orderIdx := findKeywordIndexInContext(yieldScope, "ORDER")
	limitIdx := findKeywordIndexInContext(yieldScope, "LIMIT")
	skipIdx := findKeywordIndexInContext(yieldScope, "SKIP")

	// Extract WHERE clause if present
	if whereIdx != -1 {
		whereEnd := len(yieldScope)
		for _, idx := range []int{returnIdx, orderIdx, limitIdx, skipIdx} {
			if idx != -1 && idx > whereIdx && idx < whereEnd {
				whereEnd = idx
			}
		}
		if whereEnd > whereIdx+5 {
			result.where = strings.TrimSpace(yieldScope[whereIdx+5 : whereEnd])
		} else {
			result.where = strings.TrimSpace(yieldScope[whereIdx+5:])
		}
	}

	// Extract RETURN clause if present (strip and parse ORDER BY, LIMIT, SKIP)
	if returnIdx != -1 {
		result.hasReturn = true
		returnPart := strings.TrimSpace(yieldScope[returnIdx+6:])

		// Find ORDER BY, LIMIT, SKIP positions
		orderIdx := findKeywordIndexInContext(returnPart, "ORDER")
		limitIdx := findKeywordIndexInContext(returnPart, "LIMIT")
		skipIdx := findKeywordIndexInContext(returnPart, "SKIP")

		// Find where RETURN items end
		endIdx := len(returnPart)
		if orderIdx != -1 {
			endIdx = min(endIdx, orderIdx)
		}
		if limitIdx != -1 {
			endIdx = min(endIdx, limitIdx)
		}
		if skipIdx != -1 {
			endIdx = min(endIdx, skipIdx)
		}

		result.returnExpr = strings.TrimSpace(returnPart[:endIdx])

		// Parse ORDER BY clause
		if orderIdx != -1 {
			// Find end of ORDER BY (at LIMIT, SKIP, or end of string)
			orderEnd := len(returnPart)
			if limitIdx != -1 && limitIdx > orderIdx {
				orderEnd = min(orderEnd, limitIdx)
			}
			if skipIdx != -1 && skipIdx > orderIdx {
				orderEnd = min(orderEnd, skipIdx)
			}
			orderPart := strings.TrimSpace(returnPart[orderIdx:orderEnd])
			// Strip "ORDER BY" prefix
			if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER BY") {
				result.orderBy = strings.TrimSpace(orderPart[8:])
			} else if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER") {
				result.orderBy = strings.TrimSpace(orderPart[5:])
			}
		}

		// Parse LIMIT value
		if limitIdx != -1 {
			limitEnd := len(returnPart)
			if skipIdx != -1 && skipIdx > limitIdx {
				limitEnd = skipIdx
			}
			limitPart := strings.TrimSpace(returnPart[limitIdx+5 : limitEnd])
			// Extract just the number
			limitPart = strings.TrimSpace(strings.Split(limitPart, " ")[0])
			if n, err := strconv.Atoi(limitPart); err == nil {
				result.limit = n
			}
		}

		// Parse SKIP value
		if skipIdx != -1 {
			skipEnd := len(returnPart)
			if limitIdx != -1 && limitIdx > skipIdx {
				skipEnd = limitIdx
			}
			skipPart := strings.TrimSpace(returnPart[skipIdx+4 : skipEnd])
			// Extract just the number
			skipPart = strings.TrimSpace(strings.Split(skipPart, " ")[0])
			if n, err := strconv.Atoi(skipPart); err == nil {
				result.skip = n
			}
		}
	} else {
		// No RETURN clause - parse ORDER BY, LIMIT, SKIP directly from afterYield
		// Parse ORDER BY clause
		if orderIdx != -1 {
			// Find end of ORDER BY (at LIMIT, SKIP, or end of string)
			orderEnd := len(yieldScope)
			if limitIdx != -1 && limitIdx > orderIdx {
				orderEnd = min(orderEnd, limitIdx)
			}
			if skipIdx != -1 && skipIdx > orderIdx {
				orderEnd = min(orderEnd, skipIdx)
			}
			orderPart := strings.TrimSpace(yieldScope[orderIdx:orderEnd])
			// Strip "ORDER BY" prefix
			if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER BY") {
				result.orderBy = strings.TrimSpace(orderPart[8:])
			} else if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER") {
				result.orderBy = strings.TrimSpace(orderPart[5:])
			}
		}

		// Parse LIMIT value
		if limitIdx != -1 {
			limitEnd := len(yieldScope)
			if skipIdx != -1 && skipIdx > limitIdx {
				limitEnd = skipIdx
			}
			if orderIdx != -1 && orderIdx > limitIdx {
				limitEnd = min(limitEnd, orderIdx)
			}
			limitPart := strings.TrimSpace(yieldScope[limitIdx+5 : limitEnd])
			// Extract just the number
			limitPart = strings.TrimSpace(strings.Split(limitPart, " ")[0])
			if n, err := strconv.Atoi(limitPart); err == nil {
				result.limit = n
			}
		}

		// Parse SKIP value
		if skipIdx != -1 {
			skipEnd := len(yieldScope)
			if limitIdx != -1 && limitIdx > skipIdx {
				skipEnd = limitIdx
			}
			if orderIdx != -1 && orderIdx > skipIdx {
				skipEnd = min(skipEnd, orderIdx)
			}
			skipPart := strings.TrimSpace(yieldScope[skipIdx+4 : skipEnd])
			// Extract just the number
			skipPart = strings.TrimSpace(strings.Split(skipPart, " ")[0])
			if n, err := strconv.Atoi(skipPart); err == nil {
				result.skip = n
			}
		}
	}

	// Parse yield items (if not YIELD *)
	if !result.yieldAll {
		// Get the items part (before WHERE, RETURN, ORDER, LIMIT, SKIP)
		itemsEnd := len(yieldScope)
		for _, idx := range []int{whereIdx, returnIdx, orderIdx, limitIdx, skipIdx} {
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
				// Check for AS alias
				upperItem := strings.ToUpper(item)
				asIdx := strings.Index(upperItem, " AS ")
				if asIdx != -1 {
					yi.name = strings.TrimSpace(item[:asIdx])
					yi.alias = strings.TrimSpace(item[asIdx+4:])
				} else {
					yi.name = item
					yi.alias = ""
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
		if idx := findKeywordIndexInContext(afterYield, kw); idx != -1 && idx < scopeEnd {
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
		// No YIELD clause; treat full query as call-only.
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
			alias := strings.TrimSpace(expr[asIdx+4:])
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
		return nil, fmt.Errorf("CALL tail execution requires seed result")
	}
	if strings.TrimSpace(tail) == "" {
		return seed, nil
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
	}
	// Fallback fast path for read-only tails: execute per-row tails concurrently.
	// This preserves current semantics while reducing wall-clock fixed cost for
	// CALL ... YIELD ... MATCH/RETURN tails that cannot use the set-based route.
	if len(seed.Rows) > 1 && !isPotentialWriteTail(tail) {
		if parallel, ok, err := e.executeCallTailParallel(ctx, seed, tail, usedCols, expectedCols); ok || err != nil {
			return parallel, err
		}
	}

	var combined *ExecuteResult
	for _, row := range seed.Rows {
		params := map[string]interface{}{}
		prefix := make([]string, 0, len(usedCols)+2)
		withBindings := make([]string, 0, len(usedCols))
		predicates := make([]string, 0, len(usedCols))
		tailIsMatch := tailStartsWithMatchClause(tail)

		for _, i := range usedCols {
			if i >= len(row) {
				continue
			}
			col := seed.Columns[i]
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
		if combined == nil {
			combined = &ExecuteResult{
				Columns: append([]string{}, inner.Columns...),
				Rows:    make([][]interface{}, 0, len(inner.Rows)),
			}
		}
		combined.Rows = append(combined.Rows, inner.Rows...)
	}

	if combined == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	return combined, nil
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

	ordered := make([]*ExecuteResult, len(seed.Rows))
	for rr := range results {
		if rr.err != nil {
			return nil, true, rr.err
		}
		ordered[rr.idx] = rr.res
	}
	var combined *ExecuteResult
	for _, inner := range ordered {
		if inner == nil {
			continue
		}
		if combined == nil {
			combined = &ExecuteResult{
				Columns: append([]string{}, inner.Columns...),
				Rows:    make([][]interface{}, 0, len(inner.Rows)),
			}
		}
		combined.Rows = append(combined.Rows, inner.Rows...)
	}
	if combined == nil {
		if len(expectedCols) > 0 {
			return &ExecuteResult{Columns: append([]string{}, expectedCols...), Rows: [][]interface{}{}}, true, nil
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
	}
	return combined, true, nil
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

	prefix := make([]string, 0, 3+len(scalarCols))
	prefix = append(prefix, "WITH $__seed_rows AS __seed_rows")
	prefix = append(prefix, "UNWIND __seed_rows AS __seed")
	withBindings := make([]string, 0, len(scalarCols)+1)
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
		values := make(map[string]interface{}, len(seed.Columns)+1)
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
	if plan.skip > 0 && plan.skip < len(result.Rows) {
		result.Rows = result.Rows[plan.skip:]
	} else if plan.skip >= len(result.Rows) {
		result.Rows = [][]interface{}{}
	}
	if plan.limit >= 0 {
		if plan.limit == 0 {
			result.Rows = [][]interface{}{}
		} else if plan.limit < len(result.Rows) {
			result.Rows = result.Rows[:plan.limit]
		}
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
	limit          int
	skip           int
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
	returnClause, orderBy, limit, skip := splitCallTailReturnOptions(strings.TrimSpace(trimmed[returnIdx+len("RETURN"):]))
	if returnClause == "" {
		return nil, false
	}

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
		limit:          limit,
		skip:           skip,
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
	returnOptions := splitCallTailReturnOptionsRaw(strings.TrimSpace(afterSecondWith[returnIdx+len("RETURN"):]))
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
	returnItems := e.parseReturnItems(returnOptions.returnClause)
	if len(returnItems) != 3 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" || compactCypherFragment(returnItems[2].expr) != compactCypherFragment("size("+pathsAlias+")") {
		return nil, false
	}
	return &callTailBranchingPathCountPlan{
		match:        match,
		nodeVar:      match.StartNode.variable,
		pathsAlias:   pathsAlias,
		pathCapToken: pathCapToken,
		returnItems:  returnItems,
		limitToken:   returnOptions.limitRaw,
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
	returnOptions := splitCallTailReturnOptionsRaw(strings.TrimSpace(afterSecondWith[returnIdx+len("RETURN"):]))
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
	returnItems := e.parseReturnItems(returnOptions.returnClause)
	if len(returnItems) != 4 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" || returnItems[2].expr != nearestAlias || returnItems[3].expr != reachableAlias {
		return nil, false
	}
	return &callTailFrontierReachablePlan{match: match, nodeVar: match.StartNode.variable, nearestAlias: nearestAlias, reachableAlias: reachableAlias, returnItems: returnItems, limitToken: returnOptions.limitRaw}, true
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
	returnOptions := splitCallTailReturnOptionsRaw(strings.TrimSpace(normalized[returnIdx+len("RETURN"):]))
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
	returnItems := e.parseReturnItems(returnOptions.returnClause)
	if len(returnItems) != 3 || compactCypherFragment(returnItems[0].expr) != compactCypherFragment("elementId("+match.StartNode.variable+")") || returnItems[1].expr != "score" {
		return nil, false
	}
	alias, ok := parseAggregateExprAlias(returnItems[2].expr, returnItems[2].alias, "max", "length("+pathVar+")")
	if !ok {
		return nil, false
	}
	return &callTailConstrainedMaxDepthPlan{match: match, nodeVar: match.StartNode.variable, aggregateExpr: returnItems[2].expr, aggregateAlias: alias, minWeightToken: minWeightToken, categoriesToken: categoriesToken, returnItems: returnItems, limitToken: returnOptions.limitRaw}, true
}

type callTailReturnOptionsRaw struct {
	returnClause string
	orderBy      string
	limitRaw     string
	skipRaw      string
}

func splitCallTailReturnOptionsRaw(returnAndTail string) callTailReturnOptionsRaw {
	trimmed := strings.TrimSpace(returnAndTail)
	if trimmed == "" {
		return callTailReturnOptionsRaw{}
	}
	orderIdx := findKeywordIndexInContext(trimmed, "ORDER")
	limitIdx := findKeywordIndexInContext(trimmed, "LIMIT")
	skipIdx := findKeywordIndexInContext(trimmed, "SKIP")
	end := len(trimmed)
	for _, idx := range []int{orderIdx, limitIdx, skipIdx} {
		if idx != -1 && idx < end {
			end = idx
		}
	}
	out := callTailReturnOptionsRaw{returnClause: strings.TrimSpace(trimmed[:end])}
	if orderIdx != -1 {
		orderEnd := len(trimmed)
		for _, idx := range []int{limitIdx, skipIdx} {
			if idx != -1 && idx > orderIdx && idx < orderEnd {
				orderEnd = idx
			}
		}
		orderPart := strings.TrimSpace(trimmed[orderIdx:orderEnd])
		if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER BY") {
			out.orderBy = strings.TrimSpace(orderPart[len("ORDER BY"):])
		}
	}
	if limitIdx != -1 {
		limitEnd := len(trimmed)
		if skipIdx != -1 && skipIdx > limitIdx {
			limitEnd = skipIdx
		}
		out.limitRaw = strings.TrimSpace(strings.Split(strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):limitEnd]), " ")[0])
	}
	if skipIdx != -1 {
		skipEnd := len(trimmed)
		if limitIdx != -1 && limitIdx > skipIdx {
			skipEnd = limitIdx
		}
		out.skipRaw = strings.TrimSpace(strings.Split(strings.TrimSpace(trimmed[skipIdx+len("SKIP"):skipEnd]), " ")[0])
	}
	return out
}

func splitCallTailReturnOptions(returnAndTail string) (string, string, int, int) {
	raw := splitCallTailReturnOptionsRaw(returnAndTail)
	limit := -1
	if raw.limitRaw != "" {
		if n, err := strconv.Atoi(raw.limitRaw); err == nil {
			limit = n
		}
	}

	skip := -1
	if raw.skipRaw != "" {
		if n, err := strconv.Atoi(raw.skipRaw); err == nil {
			skip = n
		}
	}

	return raw.returnClause, raw.orderBy, limit, skip
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
		outgoing, err := e.storage.GetOutgoingEdges(current.ID)
		if err != nil {
			return nil, err
		}
		incoming, err := e.storage.GetIncomingEdges(current.ID)
		if err != nil {
			return nil, err
		}
		return append(outgoing, incoming...), nil
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
		outgoing, err := e.storage.GetOutgoingEdges(current.ID)
		if err != nil {
			return err
		}
		incoming, err := e.storage.GetIncomingEdges(current.ID)
		if err != nil {
			return err
		}
		edges = append(outgoing, incoming...)
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
			return i
		}
	}
	return -1
}

// applyYieldFilter applies YIELD clause filtering to procedure results.
// This handles column selection, aliasing, and WHERE filtering.
func (e *StorageExecutor) applyYieldFilter(ctx context.Context, result *ExecuteResult, yield *yieldClause) (*ExecuteResult, error) {
	if yield == nil {
		return result, nil
	}
	if err := validateYieldColumnsExist(result.Columns, yield); err != nil {
		return nil, err
	}

	// Apply WHERE filter first
	if yield.where != "" {
		filteredRows := make([][]interface{}, 0)
		for _, row := range result.Rows {
			// Create a yield-context with the row values mapped to column names
			yieldCtx := make(map[string]interface{})
			for i, col := range result.Columns {
				if i < len(row) {
					yieldCtx[col] = row[i]
				}
			}

			// Evaluate the WHERE condition
			passes, err := e.evaluateYieldWhere(ctx, yield.where, yieldCtx)
			if err != nil {
				// If evaluation fails, include the row (conservative)
				passes = true
			}
			if passes {
				filteredRows = append(filteredRows, row)
			}
		}
		result.Rows = filteredRows
	}

	// Apply column selection and aliasing (if not YIELD *)
	if !yield.yieldAll && len(yield.items) > 0 {
		// Build column index map
		colIndex := make(map[string]int)
		for i, col := range result.Columns {
			colIndex[col] = i
		}

		// Build new columns and project rows
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
				} else {
					newRow[i] = nil
				}
			}
			newRows = append(newRows, newRow)
		}

		result.Columns = newColumns
		result.Rows = newRows
	}

	// Apply RETURN clause transformation if present
	// RETURN allows projecting properties from yielded values and renaming columns
	// Example: YIELD node, score RETURN node.id as id, node.type, score
	if yield.hasReturn && yield.returnExpr != "" {
		var err error
		result, err = e.applyReturnToYieldResult(ctx, result, yield.returnExpr)
		if err != nil {
			return nil, err
		}
	}

	// Apply ORDER BY if present
	if yield.orderBy != "" {
		result = e.applyOrderByToResult(result, yield.orderBy)
	}

	// Apply SKIP if present
	if yield.skip > 0 && yield.skip < len(result.Rows) {
		result.Rows = result.Rows[yield.skip:]
	} else if yield.skip >= len(result.Rows) {
		result.Rows = [][]interface{}{}
	}

	// Apply LIMIT if present
	if yield.limit >= 0 && yield.limit < len(result.Rows) {
		result.Rows = result.Rows[:yield.limit]
	}

	return result, nil
}

// applyReturnToYieldResult transforms procedure results based on a RETURN clause.
// This handles property access (node.id), aliasing (AS), and expression evaluation.
func (e *StorageExecutor) applyReturnToYieldResult(ctx context.Context, result *ExecuteResult, returnExpr string) (*ExecuteResult, error) {
	// Parse RETURN items
	returnItems := splitReturnExpressions(returnExpr)
	if len(returnItems) == 0 {
		return result, nil
	}

	// Build column index map for current result
	colIndex := make(map[string]int)
	for i, col := range result.Columns {
		colIndex[col] = i
	}

	// Parse each return item to determine new columns and how to compute values
	type returnItem struct {
		expr  string // Original expression (e.g., "node.id", "score")
		alias string // Column name in output (e.g., "id", "score")
	}
	var items []returnItem

	for _, item := range returnItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		ri := returnItem{expr: item, alias: item}

		// Check for AS alias
		upperItem := strings.ToUpper(item)
		if asIdx := strings.Index(upperItem, " AS "); asIdx != -1 {
			ri.expr = strings.TrimSpace(item[:asIdx])
			ri.alias = strings.TrimSpace(item[asIdx+4:])
		}

		items = append(items, ri)
	}

	// Build new columns
	newColumns := make([]string, len(items))
	for i, item := range items {
		newColumns[i] = item.alias
	}

	// Transform each row
	newRows := make([][]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		// Build yield-context with current row values
		yieldCtx := make(map[string]interface{})
		for i, col := range result.Columns {
			if i < len(row) {
				yieldCtx[col] = row[i]
			}
		}

		// Evaluate each return expression
		newRow := make([]interface{}, len(items))
		for i, item := range items {
			newRow[i] = e.evaluateReturnExprInContext(ctx, item.expr, yieldCtx)
		}
		newRows = append(newRows, newRow)
	}

	return &ExecuteResult{
		Columns: newColumns,
		Rows:    newRows,
		Stats:   result.Stats,
	}, nil
}

// evaluateReturnExprInContext evaluates a RETURN expression in the context of yielded values.
// Handles: direct references (score), property access (node.id), and functions.
func (e *StorageExecutor) evaluateReturnExprInContext(ctx context.Context, expr string, yieldCtx map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)

	// Literal handling
	if strings.EqualFold(expr, "null") {
		return nil
	}
	if strings.EqualFold(expr, "true") {
		return true
	}
	if strings.EqualFold(expr, "false") {
		return false
	}
	if len(expr) >= 2 {
		if (expr[0] == '\'' && expr[len(expr)-1] == '\'') || (expr[0] == '"' && expr[len(expr)-1] == '"') {
			return expr[1 : len(expr)-1]
		}
	}
	if i, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}

	// Direct reference to a yielded value
	if val, ok := yieldCtx[expr]; ok {
		return val
	}

	// Build nodes and rels maps from context for function evaluation
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	for key, val := range yieldCtx {
		if node, ok := val.(*storage.Node); ok && node != nil {
			nodes[key] = node
		}
		if edge, ok := val.(*storage.Edge); ok && edge != nil {
			rels[key] = edge
		}
	}

	// Handle function calls (e.g., id(a), elementId(a), labels(a))
	if strings.Contains(expr, "(") {
		return e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
	}

	// Property access: node.property
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		if len(parts) == 2 {
			varName := strings.TrimSpace(parts[0])
			propName := strings.TrimSpace(parts[1])

			if val, ok := yieldCtx[varName]; ok {
				// Handle *storage.Node (Neo4j compatible)
				if node, ok := val.(*storage.Node); ok && node != nil {
					// Handle special "id" property
					if propName == "id" {
						if propVal, ok := node.Properties["id"]; ok {
							return propVal
						}
						return string(node.ID)
					}
					// Regular property access
					if propVal, ok := node.Properties[propName]; ok {
						return propVal
					}
					return nil
				}
				// If the value is a map (legacy node representation), extract property
				if mapVal, ok := val.(map[string]interface{}); ok {
					// Try direct property access
					if propVal, ok := mapVal[propName]; ok {
						return propVal
					}
					// Try in "properties" sub-map (Neo4j style)
					if props, ok := mapVal["properties"].(map[string]interface{}); ok {
						if propVal, ok := props[propName]; ok {
							return propVal
						}
					}
				}
			}
		}
	}

	// Return nil for unresolved expressions
	return nil
}

// evaluateYieldWhere evaluates a WHERE condition in the context of YIELD variables.
func (e *StorageExecutor) evaluateYieldWhere(ctx context.Context, whereExpr string, yieldCtx map[string]interface{}) (bool, error) {
	// Simple evaluation for common patterns
	whereExpr = strings.TrimSpace(whereExpr)
	if whereExpr == "" {
		return true, nil
	}

	// Convert context for the expression evaluator.
	// Preserve real yielded node/relationship values so functions like elementId(node),
	// id(node), labels(node), and type(relationship) evaluate correctly.
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)

	for name, val := range yieldCtx {
		// Preserve real graph entities.
		if nodeVal, ok := val.(*storage.Node); ok && nodeVal != nil {
			nodes[name] = nodeVal
			continue
		}
		if relVal, ok := val.(*storage.Edge); ok && relVal != nil {
			rels[name] = relVal
			continue
		}

		// If the value is a map (legacy node-like result), wrap it as pseudo-node.
		if mapVal, ok := val.(map[string]interface{}); ok {
			props := make(map[string]interface{})
			for k, v := range mapVal {
				props[k] = v
			}
			nodes[name] = &storage.Node{
				ID:         storage.NodeID(name),
				Properties: props,
			}
		} else {
			// For scalar values, create a node with that value as a property
			nodes[name] = &storage.Node{
				ID: storage.NodeID(name),
				Properties: map[string]interface{}{
					"value": val,
				},
			}
			// Also add the scalar value directly to enable direct comparisons like "score > 0.5"
			yieldCtx[name] = val
		}
	}

	// Try to evaluate using the expression evaluator with context
	result := e.evaluateExpressionWithContext(ctx, whereExpr, nodes, rels)

	// Convert result to boolean
	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("WHERE expression did not evaluate to boolean: %v", result)
	}
}

func (e *StorageExecutor) executeCall(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}
	parts := splitCallAndTail(cypher)
	callCypher := parts.callOnly
	tailCypher := parts.tail

	upper := strings.ToUpper(callCypher)

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(callCypher)

	// Registry-first path: canonical procedure contract for built-ins and UDFs.
	ensureBuiltInProceduresRegistered()
	procName := extractProcedureName(callCypher)
	if proc, found := globalProcedureRegistry.Get(procName); found {
		args, err := extractCallArguments(callCypher)
		if err != nil {
			return nil, err
		}
		if err := validateProcedureArgCount(proc.Spec, args); err != nil {
			return nil, err
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
	case strings.Contains(upper, "APOC.CYPHER.RUN"):
		result, err = e.callApocCypherRun(ctx, callCypher)
	case strings.Contains(upper, "APOC.CYPHER.DOITALL"):
		result, err = e.callApocCypherRun(ctx, callCypher) // Alias
	case strings.Contains(upper, "APOC.CYPHER.RUNMANY"):
		result, err = e.callApocCypherRunMany(ctx, callCypher)
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
		return nil, fmt.Errorf("unknown procedure: %s (try SHOW PROCEDURES for available procedures)", procName)
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
	for label := range labelSet {
		result.Rows = append(result.Rows, []interface{}{label})
	}
	return result, nil
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
	for relType := range typeSet {
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
	return &ExecuteResult{
		Columns: []string{"name", "versions", "edition"},
		Rows: [][]interface{}{
			{"NornicDB", []string{"1.0.0"}, "community"},
		},
	}, nil
}

// NornicDB-specific procedures

func (e *StorageExecutor) callNornicDbVersion() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"version", "build", "edition"},
		Rows: [][]interface{}{
			{"1.0.0", "development", "community"},
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
	for prop := range propSet {
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
