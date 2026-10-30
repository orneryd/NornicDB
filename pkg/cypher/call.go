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
	"github.com/orneryd/nornicdb/internal/collection"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
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
	view, whereView, orderView, skipView, limitView braceView
	items                                           []yieldItem // List of yielded items (possibly with aliases)
	yieldAll                                        bool        // YIELD * - return all columns
	where                                           string      // WHERE predicate, "" when absent
	orderBy                                         string      // ORDER BY items, "" when absent
	skip                                            string      // SKIP expression, "" when absent
	limit                                           string      // LIMIT expression, "" when absent
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
	name    string // Original column name from procedure
	alias   string // Decoded alias, which may be empty
	aliased bool   // An AS clause is present, independently of its value
}

type callSplit struct {
	callOnly braceView
	tail     braceView
}

func splitCallAndTail(ctx context.Context, source braceView) callSplit {
	clause, ok := canonicalQueryClause(ctx, source)
	if !ok {
		return callSplit{callOnly: source, tail: source.window(0, 0)}
	}
	if clause.Rule() != cypherantlr.CypherParserRULE_queryCallSt && clause.Rule() != cypherantlr.CypherParserRULE_standaloneCall && clause.Rule() != cypherantlr.CypherParserRULE_callSubquery {
		return callSplit{callOnly: source, tail: source.window(0, 0)}
	}
	start, end := clause.Span()
	return callSplit{callOnly: source.window(start, end), tail: source.window(end, len(source.text())).trim()}
}
func buildCallTailPredicateInjection(tail braceView, predicates []string) braceView {
	tail = tail.trim()
	if len(predicates) == 0 {
		return tail
	}
	text := tail.text()
	where := findKeywordIndexInContext(tail, "WHERE")
	at := len(text)
	for _, keyword := range []string{"WITH", "RETURN", "ORDER", "SKIP", "LIMIT", "UNWIND", "SET", "REMOVE", "DELETE", "DETACH", "MERGE", "CREATE"} {
		if index := findKeywordIndexInContext(tail, keyword); index >= 0 && (where < 0 || index > where) && index < at {
			at = index
		}
	}
	word := " WHERE "
	if where >= 0 {
		word = " AND "
	}
	left, right := tail.window(0, at).trim(), tail.window(at, len(text)).trim()
	pieces := []braceView{left, tail.generated(word + strings.Join(predicates, " AND "))}
	if !right.empty() {
		pieces = append(pieces, tail.generated(" "), right)
	}
	return tail.assembleCode(pieces, "")
}

func tailStartsWithMatchClause(tail string) bool {
	trimmed := upperASCII(strings.TrimSpace(tail))
	return strings.HasPrefix(trimmed, "MATCH ") || strings.HasPrefix(trimmed, "OPTIONAL MATCH ")
}

func (e *StorageExecutor) executeCallTail(ctx context.Context, seed *ExecuteResult, tailView braceView) (result *ExecuteResult, err error) {
	tail := tailView.text()
	ctx = withBraceView(ctx, tailView)
	// Every tail route consumes the same seed once, including projections,
	// empty row sets and write tails. Pass-through results already own its stats.
	defer func() {
		if err == nil && result != nil && seed != nil && result != seed && result.Stats != seed.Stats {
			if result.Stats == nil || *result.Stats == (QueryStats{}) {
				result.Stats = seed.Stats
			} else {
				result.Stats = mergeQueryStats(result.Stats, seed.Stats)
			}
		}
	}()
	if seed == nil {
		return nil, localizedError(localization.CypherCommandRoutingCallTailSeedRequired(), nil)
	}
	if strings.TrimSpace(tail) == "" {
		return seed, nil
	}
	if _, all, _, union := parseTopLevelUnionBranches(ctx, tailView); union {
		first := true
		return e.executeUnion(ctx, tailView, all, func(branchCtx context.Context, branch string) (*ExecuteResult, error) {
			if first {
				first = false
				return e.executeCallTail(branchCtx, seed, statementBraceView(branchCtx, branch))
			}
			return e.executeInternal(branchCtx, branch, nil)
		})
	}
	if projected, ok, err := e.projectCallTailReturnAll(ctx, seed, tailView); ok || err != nil {
		return projected, err
	}
	// A tail that doesn't start with MATCH (WITH, UNWIND, RETURN, a chained
	// CALL, …) runs as one pipeline over every yielded row, so aggregation,
	// ORDER BY and SKIP / LIMIT see all of them. MATCH tails keep their fused
	// operators below and reach the pipeline before the per-row fallback.
	if !tailStartsWithMatchClause(tail) && !isPotentialWriteTail(tailView) {
		// WITH … [WHERE] RETURN … projections keep their compiled row
		// projection, which declines aggregation, DISTINCT and SKIP / LIMIT
		// expressions.
		if projected, ok, err := e.tryExecuteCallTailProjectionFilter(ctx, seed, tailView, nil); ok || err != nil {
			return projected, err
		}
		if pipelined, ok, err := e.executeCallTailPipeline(ctx, seed, tailView); ok || err != nil {
			return pipelined, err
		}
	}
	if pipelined, ok, err := e.tryExecuteCallTailProcedurePipeline(ctx, seed, tailView); ok || err != nil {
		return pipelined, err
	}
	if len(seed.Rows) == 0 {
		// Empty input still runs projection operators: their declared fields
		// and aggregate rows are available without a metadata analysis pass.
		if projected, ok, err := e.executeCallTailPipeline(ctx, seed, tailView); ok || err != nil {
			return projected, err
		}
		columns, err := e.resultColumns(ctx, tailView, resultmetadata.KnownScope(seed.Columns), cypherVersionFromContext(ctx) == "25")
		if err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}}, nil
	}

	expectedCols := knownResultColumns(e.cachedResultColumns(ctx, tailView, seed.Columns))
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
	if !isPotentialWriteTail(tailView) {
		if projected, ok, err := e.tryExecuteCallTailRelationshipMatchProjection(ctx, seed, tailView, expectedCols); ok || err != nil {
			return projected, err
		}
		if projected, ok, err := e.tryExecuteCallTailProjectionFilter(ctx, seed, tailView, expectedCols); ok || err != nil {
			return projected, err
		}
	}
	if !isPotentialWriteTail(tailView) {
		if pipelined, ok, err := e.executeCallTailPipeline(ctx, seed, tailView); ok || err != nil {
			return pipelined, err
		}
	}
	// Fallback fast path for read-only tails the pipeline declines: execute
	// per-row tails concurrently. It runs the tail once per yielded row, so it
	// can't aggregate across rows.
	if len(seed.Rows) > 1 && !isPotentialWriteTail(tailView) {
		if parallel, ok, err := e.executeCallTailParallel(ctx, seed, tailView, usedCols, expectedCols); ok || err != nil {
			return parallel, err
		}
	}

	results := make([]callTailRowResult, len(seed.Rows))
	for i, row := range seed.Rows {
		res, err := e.executeCallTailSingleRow(ctx, seed.Columns, row, tailView, usedCols, expectedCols)
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
func (e *StorageExecutor) executeCallTailPipeline(ctx context.Context, seed *ExecuteResult, tailView braceView) (*ExecuteResult, bool, error) {
	tail := tailView.text()
	ctx = withBraceView(ctx, tailView)
	clauses, ok := pipelineClausesFor(ctx, tail)
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
	result, ok, err := e.runPipelineClauseRows(ctx, rows, scope, clauses, clauses, nil)
	if ok {
		e.markCallTailPipelineUsed()
	}
	return result, ok, err
}

func (e *StorageExecutor) tryExecuteCallTailProcedurePipeline(
	ctx context.Context,
	seed *ExecuteResult,
	tailView braceView,
) (*ExecuteResult, bool, error) {
	tail := tailView.text()
	ctx = withBraceView(ctx, tailView)

	callIndex := findKeywordIndexInContext(tailView, "CALL")
	if callIndex <= 0 {
		return nil, false, nil
	}
	prefix := tailView.window(0, callIndex).trim()
	if !hasPrefixFoldASCII(prefix.text(), "WITH ") {
		return nil, false, nil
	}

	rows := make([]pipelineRow, 0, len(seed.Rows))
	for _, row := range seed.Rows {
		rows = append(rows, callTailRow(ctx, seed, row))
	}
	projected, ok := e.pipelineApplyWith(ctx, rows, prefix, nil, false)
	if !ok {
		return nil, false, nil
	}
	prefixColumns, ok := callTailWithProjectionColumns(ctx, prefix)
	if !ok {
		return nil, false, nil
	}

	callParts := splitChainedProcedureCall(ctx, tailView.window(callIndex, len(tail)).trim())
	if callParts.tail.empty() {
		return nil, false, nil
	}
	combined := &ExecuteResult{Columns: append([]string(nil), prefixColumns...)}
	for _, bindings := range projected {
		procedureResult, err := e.executeProcedureCall(withBraceView(ctx, callParts.callOnly), callParts.callOnly.text(), true)
		if err != nil {
			return nil, true, err
		}
		combined.Stats = mergeQueryStats(combined.Stats, procedureResult.Stats)
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

func splitChainedProcedureCall(ctx context.Context, source braceView) callSplit {
	return splitCallAndTail(ctx, source)
}
func (e *StorageExecutor) projectCallTailReturnAll(ctx context.Context, seed *ExecuteResult, tailView braceView) (*ExecuteResult, bool, error) {
	tail := tailView.text()
	ctx = withBraceView(ctx, tailView)

	node, valid := canonicalQueryClause(ctx, tailView)
	if !valid || node.Rule() != cypherantlr.CypherParserRULE_returnSt {
		return nil, false, nil
	}
	_, end := node.Span()
	if strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(tail[end:]), ";")) != "" {
		return nil, false, nil
	}
	plan := projectionPlanFromSyntax(tailView, node)
	if !plan.valid || !plan.star || plan.distinct || len(plan.starProjections) != 0 {
		return nil, false, nil
	}
	columns := append([]string{}, seed.Columns...)
	sort.Strings(columns)
	expanded := plan.withStarExpanded(columns)
	positions := make(map[string]int, len(seed.Columns))
	for index, name := range seed.Columns {
		positions[name] = index
	}
	result := &ExecuteResult{Columns: expanded.columns, Rows: make([][]interface{}, len(seed.Rows)), Stats: seed.Stats}
	for index, row := range seed.Rows {
		projected := make([]interface{}, len(columns))
		for target, name := range columns {
			if position := positions[name]; position < len(row) {
				projected[target] = row[position]
			}
		}
		result.Rows[index] = projected
	}
	if plan.modifiers == "" {
		return result, true, nil
	}
	result, err := e.applyResultModifiers(ctx, result, plan.modifiersView)
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
	tailView braceView,
	usedCols []int,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	ctx = withBraceView(ctx, tailView)

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
				res, err := e.executeCallTailSingleRow(ctx, seed.Columns, j.row, tailView, usedCols, expectedCols)
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
		combined.Stats = mergeQueryStats(combined.Stats, result.res.Stats)
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
	tailView braceView,
	usedCols []int,
	expectedCols []string,
) (*ExecuteResult, error) {
	tail := tailView.text()
	ctx = withBraceView(ctx, tailView)

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

	query := buildCallTailPredicateInjection(tailView, predicates)
	if len(withBindings) > 0 {
		prefix = append(prefix, "WITH "+strings.Join(withBindings, ", "))
	}
	if len(prefix) > 0 {
		query = query.wrap(strings.Join(prefix, " ")+" ", "")
	}

	inner, err := e.executeInternal(withBraceView(ctx, query), query.text(), params)
	if err != nil {
		return nil, err
	}
	if len(expectedCols) > 0 && len(expectedCols) == len(inner.Columns) {
		inner.Columns = append([]string{}, expectedCols...)
	}
	return inner, nil
}

func isPotentialWriteTail(tail braceView) bool {
	t := tail.trim()
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
	// relationship is the optional MATCH admission that precedes WITH.
	relationship *callTailRelationshipMatchPlan
	withClause   braceView
	returnClause braceView
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
	withWhere       braceView
}

// Parsed CALL-tail plans, cached by tail text: a nil plan (a tail the plan
// doesn't cover) is cached too.
var (
	callTailProjectionPlans        = newBoundedCache[*cyphertext.Source, *callTailProjectionPlan](1024, func(source *cyphertext.Source) int { return boundedCacheStringShardIndex(source.Text) })
	callTailRelationshipMatchPlans = newBoundedCache[*cyphertext.Source, *callTailProjectionPlan](1024, func(source *cyphertext.Source) int { return boundedCacheStringShardIndex(source.Text) })
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
	propertyExpr   braceView
	propertySource string
	endVar         string
	endLabel       string
	whereClause    braceView
}

type callTailRelationshipPatternParts struct {
	startVar     string
	startLabel   string
	relVar       string
	relType      string
	propertyKey  string
	propertyExpr braceView
	endVar       string
	endLabel     string
	rest         braceView
}

func (e *StorageExecutor) tryExecuteCallTailProjectionFilter(
	ctx context.Context,
	seed *ExecuteResult,
	tailView braceView,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	ctx = withBraceView(ctx, tailView)

	plan, ok := e.parseCallTailProjectionPlan(ctx, tailView)
	if !ok || plan.relationship != nil {
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
		projected, ok = e.pipelineApplyWith(ctx, rows, plan.withClause, nil, false)
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
	tailView braceView,
	expectedCols []string,
) (*ExecuteResult, bool, error) {
	ctx = withBraceView(ctx, tailView)

	projection, ok := e.parseCallTailProjectionPlan(ctx, tailView)
	if !ok || projection.relationship == nil {
		return nil, false, nil
	}
	plan := projection.relationship
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
		if !plan.whereClause.empty() && !e.evaluateRowPredicate(ctx, plan.whereClause, matched) {
			continue
		}
		rows = append(rows, matched)
	}
	result, ok := e.executeCallTailProjectionPlan(ctx, projection, rows, expectedCols)
	if !ok {
		return nil, false, nil
	}
	e.markCallTailProjectionFastPathUsed()
	return result, true, nil
}

func parseCallTailRelationshipPattern(tail braceView) (callTailRelationshipPatternParts, bool) {
	var parts callTailRelationshipPatternParts
	rest := tail.trim()
	if !hasPrefixFoldASCII(rest.text(), "MATCH") {
		return parts, false
	}
	rest = rest.clauseBody("MATCH")
	startVar, startLabel, rest, ok := parseCallTailNodeToken(rest)
	if !ok {
		return parts, false
	}
	rest = rest.trim()
	if !strings.HasPrefix(rest.text(), "-") {
		return parts, false
	}
	relInside, rest, ok := parseCallTailDelimited(rest.window(1, len(rest.text())).trim(), '[', ']')
	if !ok {
		return parts, false
	}
	relVar, relType, propertyKey, propertyExpr, ok := parseCallTailRelationshipToken(relInside)
	if !ok {
		return parts, false
	}
	rest = rest.trim()
	if !strings.HasPrefix(rest.text(), "->") {
		return parts, false
	}
	endVar, endLabel, rest, ok := parseCallTailNodeToken(rest.window(2, len(rest.text())).trim())
	if !ok {
		return parts, false
	}
	parts.startVar, parts.startLabel = startVar, startLabel
	parts.relVar, parts.relType = relVar, relType
	parts.propertyKey, parts.propertyExpr = propertyKey, propertyExpr
	parts.endVar, parts.endLabel, parts.rest = endVar, endLabel, rest
	return parts, true
}

func parseCallTailNodeToken(input braceView) (variable, label string, rest braceView, ok bool) {
	inside, rest, ok := parseCallTailDelimited(input.trim(), '(', ')')
	if !ok {
		return "", "", braceView{}, false
	}
	variable, label, ok = parseCallTailIdentifierAndOptionalType(inside.text())
	if !ok || variable == "" {
		return "", "", braceView{}, false
	}
	return variable, label, rest, true
}

func parseCallTailRelationshipToken(input braceView) (variable, relType, propertyKey string, propertyExpr braceView, ok bool) {
	rest := input.trim()
	variable, suffix, ok := parseCallTailIdentifier(rest.text())
	if !ok || variable == "" {
		return "", "", "", braceView{}, false
	}
	rest = rest.window(len(rest.text())-len(suffix), len(rest.text())).trim()
	if strings.HasPrefix(rest.text(), ":") {
		rest = rest.window(1, len(rest.text())).trim()
		relType, suffix, ok = parseCallTailIdentifier(rest.text())
		if !ok || relType == "" {
			return "", "", "", braceView{}, false
		}
		rest = rest.window(len(rest.text())-len(suffix), len(rest.text())).trim()
	}
	if rest.empty() {
		return variable, relType, "", rest, true
	}
	if !startsBrace(rest.text()) {
		return "", "", "", braceView{}, false
	}
	propertyKey, propertyExpr, rest, ok = parseCallTailSinglePropertyMap(rest)
	if !ok || !rest.empty() {
		return "", "", "", braceView{}, false
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

func parseCallTailDelimited(input braceView, open, close byte) (inside, rest braceView, ok bool) {
	input = input.trim()
	if len(input.text()) == 0 || input.text()[0] != open {
		return braceView{}, braceView{}, false
	}
	end := -1
	if open == '{' {
		if retained, complete := input.close(0); complete {
			end = retained
		}
	} else {
		end = cyphertext.FindMatchingDelimiter(input.text(), 0, rune(open), rune(close), cyphertext.OpeningDelimiter)
	}
	if end < 0 {
		return braceView{}, braceView{}, false
	}
	return input.window(1, end), input.window(end+1, len(input.text())), true
}

func parseCallTailSinglePropertyMap(input braceView) (key string, expr, rest braceView, ok bool) {
	input = input.trim()
	close, complete := input.close(0)
	if !complete {
		return "", braceView{}, braceView{}, false
	}
	entries, valid := input.window(0, close+1).keyedEntries()
	if !valid || len(entries) != 1 {
		return "", braceView{}, braceView{}, false
	}
	return entries[0].key(), entries[0].value, input.window(close+1, len(input.text())), true
}

func resolveOptionalIntLiteralOrParam(ctx context.Context, raw string) (int, bool) {
	if strings.TrimSpace(raw) == "" {
		return -1, true
	}
	return resolveIntLiteralOrParam(ctx, raw)
}

func resolveIntLiteralOrParam(ctx context.Context, raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "$") {
		value, ok := getParamsFromContext(ctx)[strings.TrimPrefix(raw, "$")]
		if !ok {
			return 0, false
		}
		return convert.ToInt(value, convert.IntegerFloat)
	}
	integer, err := strconv.Atoi(raw)
	return integer, err == nil
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
func (e *StorageExecutor) parseCallTailProjectionPlan(ctx context.Context, tail braceView) (*callTailProjectionPlan, bool) {
	cache := callTailProjectionPlans
	if hasPrefixFoldASCII(tail.trim().text(), "MATCH ") {
		cache = callTailRelationshipMatchPlans
	}
	plan, cached := cache.get(tail.source)
	if !cached {
		plan = e.planCallTailProjection(ctx, tail)
		cache.put(tail.source, plan)
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

// planCallTailProjection parses WITH … [WHERE …] RETURN …, optionally
// preceded by a relationship MATCH over a yielded relationship, or returns nil.
func (e *StorageExecutor) planCallTailProjection(ctx context.Context, tail braceView) *callTailProjectionPlan {
	tail = tail.trim()
	trimmed := tail.text()
	if hasPrefixFoldASCII(trimmed, "MATCH ") && !isPotentialWriteTail(tail) {
		parts, ok := parseCallTailRelationshipPattern(tail)
		if !ok {
			return nil
		}
		restView := parts.rest.trim()
		rest := restView.text()
		withIdx := topLevelKeywordIndex(restView, "WITH")
		if withIdx < 0 {
			return nil
		}
		whereClause := restView.window(0, withIdx).trim()
		if !whereClause.empty() {
			if !hasPrefixFoldASCII(whereClause.text(), "WHERE ") {
				return nil
			}
			whereClause = whereClause.clauseBody("WHERE")
		}
		projection := e.planCallTailProjection(ctx, restView.window(withIdx, len(rest)).trim())
		if projection == nil {
			return nil
		}
		propertyExpr := parts.propertyExpr.trim()
		propertySource := ""
		if dotIdx := strings.Index(propertyExpr.text(), "."); dotIdx > 0 {
			propertySource = strings.TrimSpace(propertyExpr.text()[:dotIdx])
		}
		if parts.propertyKey == "" || propertySource == "" {
			return nil
		}
		projection.relationship = &callTailRelationshipMatchPlan{
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
		}
		return projection
	}
	if !hasPrefixFoldASCII(trimmed, "WITH ") || isPotentialWriteTail(tail) {
		return nil
	}
	withIdx := len("WITH ")
	returnIdx := topLevelKeywordIndex(tail, "RETURN")
	if returnIdx < 0 {
		return nil
	}
	beforeReturnView := tail.window(withIdx, returnIdx).trim()
	beforeReturn := beforeReturnView.text()
	returnAndModifiersView := tail.window(returnIdx+len("RETURN"), len(trimmed)).trim()
	returnAndModifiers := returnAndModifiersView.text()
	if beforeReturn == "" || returnAndModifiers == "" {
		return nil
	}
	// The plan covers exactly WITH … [WHERE …] RETURN …; a tail with another
	// clause between them (WITH label MATCH (n) RETURN …) is the pipeline's.
	for _, keyword := range callTailPlanClauseKeywords {
		if topLevelKeywordIndex(beforeReturnView, keyword) >= 0 {
			return nil
		}
	}

	withProjection := beforeReturnView
	if whereIdx := topLevelKeywordIndex(beforeReturnView, "WHERE"); whereIdx >= 0 {
		withProjection = beforeReturnView.window(0, whereIdx).trim()
	}
	if withProjection.empty() {
		return nil
	}
	returnProjection, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers(returnAndModifiersView)
	if returnProjection.text() == "" {
		return nil
	}
	// The plan projects row by row: aggregation, DISTINCT and SKIP / LIMIT
	// expressions (LIMIT 0 + 1) are the pipeline's (executeCallTailPipeline).
	if startsWithDistinct(withProjection) || startsWithDistinct(returnProjection) ||
		containsAggregateFunc(withProjection) || containsAggregateFunc(returnProjection) || containsAggregateFunc(orderBy) {
		return nil
	}
	withItems := e.parseReturnItems(ctx, withProjection)
	returnItems := e.parseReturnItems(ctx, returnProjection)
	if len(withItems) == 0 || len(returnItems) == 0 || hasStarReturnItem(withItems) || hasStarReturnItem(returnItems) {
		return nil
	}
	plan := &callTailProjectionPlan{
		withClause:   beforeReturnView.wrap("WITH ", ""),
		returnClause: returnAndModifiersView.wrap("RETURN ", ""),
		limitToken:   limitToken,
		skipToken:    skipToken,
		withWhere:    beforeReturnView.window(0, 0),
	}
	plan.passThroughWith = true
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if topLevelKeywordIndex(beforeReturnView, keyword) >= 0 {
			plan.passThroughWith = false
		}
	}
	for _, item := range withItems {
		if !cyphertext.IsIdentifier(item.expr) || item.alias != item.expr {
			plan.passThroughWith = false
		}
	}
	if whereIdx := topLevelKeywordIndex(beforeReturnView, "WHERE"); whereIdx >= 0 {
		plan.withWhere = beforeReturnView.window(whereIdx+len("WHERE"), len(beforeReturn)).trim()
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

func splitCallTailProjectionModifiers(input braceView) (projectionView, orderBy braceView, limitToken, skipToken string) {
	input = input.trim()
	projection := input.text()
	projectionView, orderBy = input, input.window(0, 0)
	modifierStart := len(projection)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(input, kw); idx >= 0 && idx < modifierStart {
			modifierStart = idx
		}
	}
	modifierView := input.window(len(projection), len(projection))
	modifiers := ""
	if modifierStart < len(projection) {
		modifierView = input.window(modifierStart, len(projection)).trim()
		modifiers = modifierView.text()
		projectionView = input.window(0, modifierStart).trim()
	}
	if modifiers == "" {
		return projectionView, orderBy, "", ""
	}
	if orderIdx := topLevelKeywordIndex(modifierView, "ORDER BY"); orderIdx >= 0 {
		end := len(modifiers)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := topLevelKeywordIndex(modifierView.window(orderIdx, len(modifiers)), kw); idx >= 0 {
				abs := orderIdx + idx
				if abs > orderIdx && abs < end {
					end = abs
				}
			}
		}
		orderBy = modifierView.window(orderIdx, end).trim()
	}
	limitToken = extractCallTailModifierToken(modifierView, "LIMIT")
	skipToken = extractCallTailModifierToken(modifierView, "SKIP")
	return projectionView, orderBy, limitToken, skipToken
}

// extractCallTailModifierToken returns the whole value of keyword (SKIP or
// LIMIT) in modifiers, up to the next modifier: an expression such as
// LIMIT 0 + 1 stays whole, so the projection plan declines it instead of
// reading LIMIT 0 (#572).
func extractCallTailModifierToken(view braceView, keyword string) string {
	modifiers := view.text()
	idx := topLevelKeywordIndex(view, keyword)
	if idx < 0 {
		return ""
	}
	restView := view.window(idx+len(keyword), len(modifiers))
	rest := restView.text()
	end := len(rest)
	for _, next := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if nextIdx := topLevelKeywordIndex(restView, next); nextIdx >= 0 && nextIdx < end {
			end = nextIdx
		}
	}
	return strings.TrimSpace(rest[:end])
}

func callTailHasRelationshipTypeConstraint(tail braceView) bool {
	for from := 0; from < len(tail.text()); {
		open, close := readRelationshipBracket(tail, from, requireClosedRelationship)
		if open < 0 {
			return false
		}
		if findTopLevelSymbol(tail.text()[open+1:close], ":", false, true) >= 0 {
			return true
		}
		from = close + 1
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
		} else {
			values[col] = nil
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

// isIdentifierReferenced compares identifier tokens with a binding's decoded
// name. cyphertext.ScanIdentifier owns identifier decoding, including doubled
// backticks; matching raw query bytes loses those imported bindings.
// This is a conservative name check, not a scope or variable-role analysis.
func isIdentifierReferenced(query, identifier string) bool {
	if strings.TrimSpace(identifier) == "" {
		return false
	}
	for index := 0; index < len(query); {
		if isCypherStringQuoteByte(query[index]) {
			index = skipCypherQuotedText(query, index, query[index])
			continue
		}
		if end := queryCommentEnd(query, index); end > index {
			index = end
			continue
		}
		name, ok := cyphertext.ScanIdentifier(query, index)
		if !ok || (!isCypherBacktickByte(query[index]) && index > 0 && cyphertext.NamePartAt(query, index-1)) {
			index++
			continue
		}
		if name.Value == identifier {
			return true
		}
		index = name.End
	}
	return false
}

// findKeywordIndexInContext returns the index of keyword's first occurrence
// at the top level of s, or -1: topLevelKeywordIndex, the one scanner. A
// keyword inside parentheses, a list or braces (a function call's or list
// comprehension's WHERE, a subquery body) isn't the clause's; nor is one in a
// string, a quoted name or a comment, the WITH of STARTS WITH / ENDS WITH, or
// a clause keyword used as a name.
func findKeywordIndexInContext(view braceView, keyword string) int {
	return topLevelKeywordIndex(view, keyword)
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
			newColumns = append(newColumns, item.column())
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
	rows = e.filterPipelineRows(ctx, rows, yield.whereView)
	if yield.orderBy != "" || yield.skip != "" || yield.limit != "" {
		pieces := []braceView{yield.view.generated("WITH *")}
		if yield.orderBy != "" {
			pieces = append(pieces, yield.view.generated(" ORDER BY "), yield.orderView)
		}
		if yield.skip != "" {
			pieces = append(pieces, yield.view.generated(" SKIP "), yield.skipView)
		}
		if yield.limit != "" {
			pieces = append(pieces, yield.view.generated(" LIMIT "), yield.limitView)
		}
		ordered, ok := e.pipelineApplyWith(ctx, rows, yield.view.assembleCode(pieces, ""), nil, false)
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
	parts := splitChainedProcedureCall(ctx, statementBraceView(ctx, cypher))
	callCypher := parts.callOnly.text()
	ctx = withBraceView(ctx, parts.callOnly)
	tailCypher := parts.tail.text()

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(ctx, parts.callOnly)
	if err := e.validateYieldModifiers(ctx, yield, inQuery || strings.TrimSpace(tailCypher) != "" || endsInFinish(ctx, cypher)); err != nil {
		return nil, err
	}

	// Registry-first path: canonical procedure contract for built-ins and UDFs.
	ensureBuiltInProceduresRegistered()
	procName := extractProcedureName(ctx, callCypher)
	if proc, found := globalProcedureRegistry.Get(procName); found {
		hasTail := strings.TrimSpace(tailCypher) != ""
		if err := validateProcedureArgumentPassingMode(ctx, proc.Spec, parts.callOnly, hasTail); err != nil {
			return nil, err
		}
		if err := validateProcedureYieldBindings(yield, hasTail); err != nil {
			return nil, err
		}
		args, err := e.extractBoundProcedureInvocationArguments(ctx, proc.Spec, parts.callOnly)
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
		if proc.Spec.OwnsTransactions {
			if err := procedureExec.checkPeriodicTransactionScope(procedureCtx, procName); err != nil {
				return nil, err
			}
		}
		result, err := procedureExec.executeProcedureHandler(procedureCtx, proc, callCypher, args)
		if err != nil {
			return nil, procedureRuntimeError(procName, err)
		}
		if proc.User {
			result = runtimeProcedureResult(result)
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
				seed := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{{}}}
				if result != nil {
					seed.Stats = result.Stats
				}
				result = seed
			}
			return e.executeCallTail(ctx, result, parts.tail)
		}
		return result, nil
	}

	// Terminal chokepoint of the converged procedure router: every built-in
	// procedure is registered, so an unrecognized name is rejected here.
	return nil, unknownProcedureError(procName)
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
	for _, label := range e.storage.GetSchema().OrderTokens(collection.SortedKeys(labelSet), false) {
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
	for _, relType := range e.storage.GetSchema().OrderTokens(collection.SortedKeys(typeSet), true) {
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
	_, schema, err := e.knowledgePolicyContext("")
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
	for _, prop := range collection.SortedKeys(propSet) {
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
