package cypher

// Pipeline executor for composite queries of the form
//
//	MATCH ... [WHERE ...]
//	CREATE ... (any number)
//	WITH ... (projection / pass-through)
//	UNWIND <list-expression> AS <var>
//	MATCH ...
//	CREATE ...
//	[RETURN ...]
//
// The existing executeMatchWithClause / executeMatchWithUnwind handlers assume
// the segment between MATCH and WITH is a single node pattern. That makes
// them corrupt any query where CREATE clauses live between MATCH and WITH
// (the classic "invalid property value" error where a generated property key
// swallows the rest of the query, e.g. key="{}UNWIND[{productID").
//
// This file walks the clauses in order and threads a binding context through
// each step so arbitrary compositions work. It reuses the existing primitives
// (executeMatchForContext, executeCreateWithRefs, executeInternal) rather
// than reparsing patterns from scratch.

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// pipelineClauseKind enumerates the clause types the pipeline executor
// understands. Anything else causes us to bail and return false so callers
// fall back to the legacy handlers.
type pipelineClauseKind int

const (
	pipelineClauseMatch pipelineClauseKind = iota
	pipelineClauseCreate
	pipelineClauseWith
	pipelineClauseUnwind
	pipelineClauseReturn
)

// pipelineClause is one segment of the pipeline. `text` includes the leading
// keyword (MATCH/CREATE/WITH/UNWIND/RETURN) and the clause body — exactly
// what you would pass to the legacy handlers.
type pipelineClause struct {
	kind pipelineClauseKind
	text string
}

// pipelineRow carries bindings across clauses. Values may be *storage.Node,
// *storage.Edge, or scalars (for WITH projections and UNWIND variables).
type pipelineRow map[string]interface{}

// canExecuteAsPipeline returns true when the query is decomposable into the
// clause kinds this executor understands. Any unsupported clause (OPTIONAL
// MATCH, MERGE, FOREACH, CALL subquery, etc.) causes a false return so the
// caller can fall back to the legacy path.
func canExecuteAsPipeline(cypher string) ([]pipelineClause, bool) {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil, false
	}
	// Must contain at least one of the clause kinds that distinguishes this
	// from a single-clause query the legacy handler already covers.
	if len(clauses) < 2 {
		return nil, false
	}
	// Require at least one WITH *or* UNWIND in the middle — otherwise the
	// existing MATCH/CREATE compound path is perfectly fine.
	hasWithOrUnwind := false
	for _, c := range clauses {
		if c.kind == pipelineClauseWith || c.kind == pipelineClauseUnwind {
			hasWithOrUnwind = true
			break
		}
	}
	if !hasWithOrUnwind {
		return nil, false
	}
	return clauses, true
}

// splitPipelineClauses walks the query from left to right and slices it on
// top-level MATCH/CREATE/WITH/UNWIND/RETURN keywords. Returns (clauses, true)
// on success. On anything unsupported (e.g. nested MERGE, OPTIONAL MATCH)
// returns (nil, false) so the caller falls back.
func splitPipelineClauses(cypher string) ([]pipelineClause, bool) {
	type kw struct {
		name string
		kind pipelineClauseKind
	}
	// Order matters for multi-word lookups but we only care about single-word
	// keywords here; OPTIONAL MATCH and MERGE kick us out via detection below.
	keywords := []kw{
		{"MATCH", pipelineClauseMatch},
		{"CREATE", pipelineClauseCreate},
		{"WITH", pipelineClauseWith},
		{"UNWIND", pipelineClauseUnwind},
		{"RETURN", pipelineClauseReturn},
	}
	// Clauses we don't yet model as their own kind force a fallback. Anything
	// else — including $param references and arbitrary WHERE on bindings —
	// is handled by the per-clause appliers below, which substitute params
	// from context and respect node bindings supplied by the caller.
	upper := strings.ToUpper(cypher)
	for _, bad := range []string{"OPTIONAL MATCH", "MERGE ", "FOREACH", "CALL ", "DELETE", "REMOVE ", "SET "} {
		if findKeywordIndex(upper, strings.TrimRight(bad, " ")) >= 0 {
			return nil, false
		}
	}

	// Collect boundary positions for each supported keyword.
	var boundaries []pipelineBoundary
	for _, k := range keywords {
		for _, p := range findAllKeywordPositions(cypher, k.name) {
			// Skip "STARTS WITH" / "ENDS WITH".
			if k.name == "WITH" {
				preceding := strings.TrimRight(strings.ToUpper(cypher[:p]), " \t\n\r")
				if strings.HasSuffix(preceding, "STARTS") || strings.HasSuffix(preceding, "ENDS") {
					continue
				}
			}
			boundaries = append(boundaries, pipelineBoundary{pos: p, kind: k.kind, name: k.name})
		}
	}
	if len(boundaries) == 0 {
		return nil, false
	}
	// Sort ascending by pos.
	sortBoundariesByPos(boundaries)

	// Cut the query on each boundary. The first clause must begin at the
	// first boundary (i.e. the query should begin with one of these keywords
	// after trimming).
	trimmedLeft := len(cypher) - len(strings.TrimLeft(cypher, " \t\n\r"))
	if boundaries[0].pos != trimmedLeft {
		return nil, false
	}

	var out []pipelineClause
	for i, b := range boundaries {
		end := len(cypher)
		if i+1 < len(boundaries) {
			end = boundaries[i+1].pos
		}
		text := strings.TrimSpace(cypher[b.pos:end])
		if text == "" {
			continue
		}
		out = append(out, pipelineClause{kind: b.kind, text: text})
	}
	return out, true
}

type pipelineBoundary struct {
	pos  int
	kind pipelineClauseKind
	name string
}

func sortBoundariesByPos(bs []pipelineBoundary) {
	// Insertion sort — boundary count is small (usually < 20).
	for i := 1; i < len(bs); i++ {
		j := i
		for j > 0 && bs[j-1].pos > bs[j].pos {
			bs[j-1], bs[j] = bs[j], bs[j-1]
			j--
		}
	}
}

// executePipeline walks the clauses, threading the binding rows through each
// step. Returns (*ExecuteResult, true, nil) on success, (nil, false, nil) if
// the shape proves unsupported mid-execution (caller should fall back), or
// (nil, true, err) on a hard error.
func (e *StorageExecutor) executePipeline(ctx context.Context, cypher string) (*ExecuteResult, bool, error) {
	clauses, ok := canExecuteAsPipeline(cypher)
	if !ok {
		return nil, false, nil
	}

	// Substitute $param placeholders up-front — this is the same pass the
	// other top-level handlers perform. After this step the clause texts are
	// self-contained and our per-clause appliers only have to worry about
	// pipeline-bound names (from WITH/UNWIND/MATCH), not caller parameters.
	params := getParamsFromContext(ctx)
	if params != nil {
		cypher = e.substituteParams(cypher, params)
		clauses, ok = canExecuteAsPipeline(cypher)
		if !ok {
			return nil, false, nil
		}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Start with a single empty binding row — the first MATCH populates it.
	rows := []pipelineRow{{}}

	for idx, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch:
			newRows, ok, err := e.pipelineApplyMatch(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			rows = newRows
		case pipelineClauseCreate:
			newRows, stats, ok, err := e.pipelineApplyCreate(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			if stats != nil {
				result.Stats.NodesCreated += stats.NodesCreated
				result.Stats.RelationshipsCreated += stats.RelationshipsCreated
			}
		case pipelineClauseWith:
			newRows, ok := e.pipelineApplyWith(rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
		case pipelineClauseUnwind:
			newRows, ok := e.pipelineApplyUnwind(rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
		case pipelineClauseReturn:
			final, ok := e.pipelineApplyReturn(rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			result.Columns = final.Columns
			result.Rows = final.Rows
			// RETURN is always last.
			return result, true, nil
		}
		_ = idx
	}

	return result, true, nil
}

// ---- clause appliers ----

// pipelineApplyMatch runs MATCH for each current binding row and expands rows
// by the matched combinations. Returns (newRows, true, nil) on success. If a
// MATCH in the middle of a pipeline binds zero rows, it does NOT fail — it
// just zeros out the pipeline (matches Neo4j semantics for chained MATCH).
func (e *StorageExecutor) pipelineApplyMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	// If the MATCH has scalar references to already-bound variables (e.g.
	// `MATCH (p:Product {productID: prodRef.productID})`), substitute them
	// per-row before invoking executeMatchForContext.
	var out []pipelineRow
	for _, row := range rows {
		substituted := clause
		for name, val := range row {
			if _, isNode := val.(*storage.Node); isNode {
				continue
			}
			if _, isEdge := val.(*storage.Edge); isEdge {
				continue
			}
			// Substitute `name.prop` references and bare `name` references.
			if asMap, ok := toStringAnyMap(val); ok {
				for k, v := range asMap {
					pattern := name + "." + k
					substituted = strings.ReplaceAll(substituted, pattern, e.valueToLiteral(v))
				}
				continue
			}
			substituted = replaceIdentifierOutsideQuotes(substituted, name, e.valueToLiteral(val))
		}

		combos, edges, err := e.executeMatchForContext(ctx, substituted)
		if err != nil {
			return nil, true, err
		}
		for _, combo := range combos {
			newRow := make(pipelineRow, len(row)+len(combo)+len(edges))
			for k, v := range row {
				newRow[k] = v
			}
			for k, n := range combo {
				newRow[k] = n
			}
			for k, ed := range edges {
				newRow[k] = ed
			}
			out = append(out, newRow)
		}
	}
	// No matches → empty pipeline (legal).
	return out, true, nil
}

// pipelineApplyCreate runs CREATE for each binding row, threading pre-bound
// nodes into the CREATE handler via a synthetic MATCH prefix. Newly-created
// nodes (by variable name) are captured and added to the output row so later
// pipeline steps can reference them.
func (e *StorageExecutor) pipelineApplyCreate(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, bool, error) {
	stats := &QueryStats{}
	var out []pipelineRow

	for _, row := range rows {
		// Substitute scalar bindings (e.g. prodRef.productID → literal) up
		// front so the CREATE pattern parser sees a concrete value.
		substituted := clause
		for name, val := range row {
			if _, isNode := val.(*storage.Node); isNode {
				continue
			}
			if _, isEdge := val.(*storage.Edge); isEdge {
				continue
			}
			if asMap, ok := toStringAnyMap(val); ok {
				for k, v := range asMap {
					pattern := name + "." + k
					substituted = strings.ReplaceAll(substituted, pattern, e.valueToLiteral(v))
				}
				continue
			}
			substituted = replaceIdentifierOutsideQuotes(substituted, name, e.valueToLiteral(val))
		}

		// Package node bindings into a MATCH prefix so the CREATE handler
		// sees the already-bound variables. We emit a chained
		// `MATCH (a) WHERE id(a) = "..." MATCH (b) WHERE id(b) = "..."` which
		// executeInternal resolves via executeCompoundMatchCreate.
		var matchPieces []string
		for name, val := range row {
			node, isNode := val.(*storage.Node)
			if !isNode || node == nil {
				continue
			}
			if !referencesVariable(substituted, name) {
				continue
			}
			var label string
			if len(node.Labels) > 0 {
				label = ":" + node.Labels[0]
			}
			matchPieces = append(matchPieces,
				fmt.Sprintf("(%s%s) WHERE id(%s) = %q", name, label, name, string(node.ID)))
		}

		var queryToRun string
		if len(matchPieces) == 0 {
			queryToRun = substituted
		} else {
			queryToRun = strings.Join(matchPieces, " MATCH ")
			queryToRun = "MATCH " + queryToRun + " " + substituted
		}

		subResult, refsNodes, _, err := e.executeCreateWithRefsOrCompound(ctx, queryToRun)
		if err != nil {
			return nil, nil, true, fmt.Errorf("pipeline CREATE failed: %w", err)
		}
		if subResult != nil && subResult.Stats != nil {
			stats.NodesCreated += subResult.Stats.NodesCreated
			stats.RelationshipsCreated += subResult.Stats.RelationshipsCreated
		}

		// Merge newly-created node bindings into the row so subsequent
		// pipeline steps can reference them (e.g. CREATE (c)-[:REL]->(o)
		// where `o` was created by an earlier CREATE in the same pipeline).
		newRow := make(pipelineRow, len(row)+len(refsNodes))
		for k, v := range row {
			newRow[k] = v
		}
		for k, n := range refsNodes {
			newRow[k] = n
		}
		out = append(out, newRow)
	}

	return out, stats, true, nil
}

// executeCreateWithRefsOrCompound runs a CREATE or MATCH...CREATE query and
// returns the created-node refs. Handles both the standalone CREATE case and
// the synthetic `MATCH (x) WHERE id(x) = "..." MATCH (y) ... CREATE ...`
// prefix we prepend for pre-bound variables.
func (e *StorageExecutor) executeCreateWithRefsOrCompound(ctx context.Context, query string) (*ExecuteResult, map[string]*storage.Node, map[string]*storage.Edge, error) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "CREATE") {
		return e.executeCreateWithRefs(ctx, query)
	}
	// MATCH ... CREATE ... — use the compound handler and try to derive the
	// created-node refs by parsing the CREATE part. Simpler: run the query
	// via executeInternal and re-scan the storage for nodes referenced by
	// bare variable patterns. For now we fall back to running it directly
	// and capturing stats only; binding propagation of newly-created nodes
	// through compound MATCH+CREATE is not needed by the seeder (the only
	// newly-bound name inside a single block is captured by the subsequent
	// CREATE in the same block when they share the pipeline).
	//
	// Concretely: in our pipeline the second CREATE `CREATE (c)-[:REL]->(o)`
	// references `o` which was bound by the previous CREATE clause. Because
	// we split each CREATE into its own pipeline clause, that earlier CREATE
	// is its own standalone CREATE and goes through executeCreateWithRefs
	// above, populating refsNodes. The MATCH+CREATE branch here handles
	// compound shapes only for completeness; populated refs empty.
	result, err := e.executeInternal(ctx, query, nil)
	return result, nil, nil, err
}

// pipelineApplyWith drops / renames binding keys according to a WITH clause.
// Supports:
//   - plain variables:     `WITH a, b`            carries each forward
//   - map placeholder:     `WITH o, {}`           drops the {} projection
//   - variable → alias:    `WITH a AS b`          renames
//   - property → alias:    `WITH a.name AS n`     projects node property
//   - literal → alias:     `WITH 42 AS x`         binds scalar literal
//   - aggregate pass-thru: `WITH count(*) AS c`   counts current rows
//
// Anything else returns ok=false so the caller can fall back.
func (e *StorageExecutor) pipelineApplyWith(rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "WITH"))
	body = strings.TrimPrefix(body, "with")
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return rows, true
	}

	// Detect a simple `count(*)` / `count(var)` aggregation — in that case
	// WITH collapses the whole row stream into one row carrying the count.
	aggregateOnly := true
	countAlias := ""
	for _, rawItem := range items {
		item := strings.TrimSpace(rawItem)
		if item == "{}" {
			continue
		}
		upper := strings.ToUpper(item)
		asIdx := strings.Index(upper, " AS ")
		expr := item
		alias := ""
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+4:])
		}
		exprUpper := strings.ToUpper(expr)
		if strings.HasPrefix(exprUpper, "COUNT(") && strings.HasSuffix(expr, ")") {
			if countAlias == "" && alias != "" {
				countAlias = alias
			}
			continue
		}
		aggregateOnly = false
	}
	if aggregateOnly && countAlias != "" {
		return []pipelineRow{{countAlias: int64(len(rows))}}, true
	}

	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		newRow := pipelineRow{}
		ok := true
		for _, rawItem := range items {
			item := strings.TrimSpace(rawItem)
			if item == "" || item == "{}" {
				continue
			}
			upper := strings.ToUpper(item)
			var expr, alias string
			if asIdx := strings.Index(upper, " AS "); asIdx > 0 {
				expr = strings.TrimSpace(item[:asIdx])
				alias = strings.TrimSpace(item[asIdx+4:])
			} else {
				expr = item
				alias = item
			}

			// 1. Exact binding match.
			if val, found := row[expr]; found {
				newRow[alias] = val
				continue
			}

			// 2. Property access (var.prop).
			if dot := strings.Index(expr, "."); dot > 0 {
				base := strings.TrimSpace(expr[:dot])
				field := strings.TrimSpace(expr[dot+1:])
				if baseVal, found := row[base]; found {
					if node, isNode := baseVal.(*storage.Node); isNode && node != nil {
						newRow[alias] = node.Properties[field]
						continue
					}
					if m, isMap := toStringAnyMap(baseVal); isMap {
						newRow[alias] = m[field]
						continue
					}
				}
			}

			// 3. Literal scalar (number, quoted string, bool, null). Covers
			// `WITH 42 AS x` and the post-$param-substitution form like
			// `WITH 'hi' AS note`.
			if val, lit := parseLiteralScalarForPipeline(expr); lit {
				newRow[alias] = val
				continue
			}

			// Anything else — fall back.
			ok = false
			break
		}
		if !ok {
			return nil, false
		}
		out = append(out, newRow)
	}
	return out, true
}

// pipelineApplyUnwind evaluates the list expression (which may be a literal,
// a reference to a bound variable, or a bare property access) and produces
// one row per element.
func (e *StorageExecutor) pipelineApplyUnwind(rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "UNWIND"))
	body = strings.TrimPrefix(body, "unwind")
	upper := strings.ToUpper(body)
	asIdx := strings.Index(upper, " AS ")
	if asIdx <= 0 {
		return nil, false
	}
	listExpr := strings.TrimSpace(body[:asIdx])
	alias := strings.TrimSpace(body[asIdx+4:])

	out := make([]pipelineRow, 0)
	for _, row := range rows {
		items := evaluateListForPipeline(listExpr, row)
		if items == nil {
			// Couldn't evaluate — fall back.
			return nil, false
		}
		for _, item := range items {
			newRow := make(pipelineRow, len(row)+1)
			for k, v := range row {
				newRow[k] = v
			}
			newRow[alias] = item
			out = append(out, newRow)
		}
	}
	return out, true
}

// pipelineApplyReturn projects each binding row through the RETURN list.
// Supports:
//   - `count(*)` / `count(var)` (aggregate — collapses all rows to one)
//   - bare variable              (`RETURN node`)
//   - property access            (`RETURN m.name`)
//   - aliased forms              (`RETURN m.name AS probeName`)
//   - literal scalar             (`RETURN 42 AS answer`)
//
// Returns (nil, false) if any item can't be projected, so the caller falls
// back to the legacy RETURN pipeline.
func (e *StorageExecutor) pipelineApplyReturn(rows []pipelineRow, clause string) (*ExecuteResult, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "RETURN"))
	body = strings.TrimPrefix(body, "return")
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(len(rows))}}}, true
	}

	type proj struct {
		expr   string
		alias  string
		isAggr bool // count(*) / count(var)
	}
	var projs []proj
	aggregateOnly := true
	for _, rawItem := range items {
		item := strings.TrimSpace(rawItem)
		if item == "" {
			continue
		}
		upper := strings.ToUpper(item)
		asIdx := strings.Index(upper, " AS ")
		expr := item
		alias := item
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+4:])
		}
		exprUpper := strings.ToUpper(expr)
		isAggr := strings.HasPrefix(exprUpper, "COUNT(") && strings.HasSuffix(expr, ")")
		if !isAggr {
			aggregateOnly = false
		}
		projs = append(projs, proj{expr: expr, alias: alias, isAggr: isAggr})
	}

	result := &ExecuteResult{}
	for _, p := range projs {
		result.Columns = append(result.Columns, p.alias)
	}

	// All aggregates → collapse to a single result row.
	if aggregateOnly {
		row := make([]interface{}, 0, len(projs))
		for range projs {
			row = append(row, int64(len(rows)))
		}
		result.Rows = [][]interface{}{row}
		return result, true
	}

	for _, row := range rows {
		outRow := make([]interface{}, 0, len(projs))
		for _, p := range projs {
			if p.isAggr {
				// Mixed aggregate + row projection is unusual; emit row count.
				outRow = append(outRow, int64(len(rows)))
				continue
			}
			val, ok := projectFromRow(row, p.expr)
			if !ok {
				return nil, false
			}
			outRow = append(outRow, val)
		}
		result.Rows = append(result.Rows, outRow)
	}
	return result, true
}

// projectFromRow resolves a RETURN / WITH expression against a single
// binding row. Returns (value, true) on success, (nil, false) otherwise.
func projectFromRow(row pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if val, ok := row[expr]; ok {
		return val, true
	}
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if node, isNode := baseVal.(*storage.Node); isNode && node != nil {
				return node.Properties[field], true
			}
			if m, isMap := toStringAnyMap(baseVal); isMap {
				return m[field], true
			}
		}
	}
	if v, ok := parseLiteralScalarForPipeline(expr); ok {
		return v, true
	}
	return nil, false
}

// ---- helpers ----

// referencesVariable returns true if the query text refers to the variable
// `name` outside string literals and outside property-access positions.
func referencesVariable(query, name string) bool {
	if name == "" {
		return false
	}
	// Use the existing identifier-aware scanner by replacing with a sentinel
	// and checking for a diff.
	const sentinel = "\x00"
	replaced := replaceIdentifierOutsideQuotes(query, name, sentinel)
	return strings.Contains(replaced, sentinel)
}

// evaluateListForPipeline evaluates a list expression against a binding row.
// Supports three forms:
//  1. Bare variable:  UNWIND items AS x
//  2. Property access: UNWIND row.products AS prodRef
//  3. Literal list:   UNWIND [{...}, {...}] AS x  (already a literal)
//
// Returns nil if the expression can't be evaluated.
func evaluateListForPipeline(expr string, row pipelineRow) []interface{} {
	expr = strings.TrimSpace(expr)
	// Bare variable.
	if val, ok := row[expr]; ok {
		return toAnySlice(val)
	}
	// Property access (a.b).
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if asMap, ok := toStringAnyMap(baseVal); ok {
				if v, ok := asMap[field]; ok {
					return toAnySlice(v)
				}
			}
			if node, ok := baseVal.(*storage.Node); ok && node != nil {
				return toAnySlice(node.Properties[field])
			}
		}
	}
	// Literal list — parse via an ad-hoc evaluator. The simplest reliable
	// thing to do is wrap the literal and let the storage executor parse it
	// as a value. Without plumbing a full parser here we only accept the
	// `[...]` form and split top-level items.
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}
		}
		parts := splitTopLevelComma(inner)
		out := make([]interface{}, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
				m := parseLiteralMapForPipeline(p)
				if m == nil {
					return nil
				}
				out = append(out, m)
				continue
			}
			// Scalar — defer to scalar parser (ints / strings / floats).
			if v, ok := parseLiteralScalarForPipeline(p); ok {
				out = append(out, v)
				continue
			}
			return nil
		}
		return out
	}
	return nil
}

func toAnySlice(v interface{}) []interface{} {
	switch s := v.(type) {
	case []interface{}:
		return s
	case []map[string]interface{}:
		out := make([]interface{}, len(s))
		for i, m := range s {
			out[i] = m
		}
		return out
	}
	return nil
}

func parseLiteralMapForPipeline(s string) map[string]interface{} {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]interface{}{}
	}
	pairs := splitTopLevelComma(inner)
	out := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		colon := strings.Index(pair, ":")
		if colon <= 0 {
			return nil
		}
		k := strings.TrimSpace(pair[:colon])
		vRaw := strings.TrimSpace(pair[colon+1:])
		if v, ok := parseLiteralScalarForPipeline(vRaw); ok {
			out[k] = v
			continue
		}
		if strings.HasPrefix(vRaw, "{") {
			m := parseLiteralMapForPipeline(vRaw)
			if m == nil {
				return nil
			}
			out[k] = m
			continue
		}
		return nil
	}
	return out
}

func parseLiteralScalarForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	// Quoted string.
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		return s[1 : len(s)-1], true
	}
	// Bool.
	switch strings.ToLower(s) {
	case "true":
		return true, true
	case "false":
		return false, true
	case "null":
		return nil, true
	}
	// Int.
	if i, ok := parseIntFast(s); ok {
		return i, true
	}
	// Float.
	if f, ok := parseFloatFast(s); ok {
		return f, true
	}
	return nil, false
}

func parseIntFast(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	var sign int64 = 1
	i := 0
	if s[0] == '-' {
		sign = -1
		i = 1
	} else if s[0] == '+' {
		i = 1
	}
	if i == len(s) {
		return 0, false
	}
	var n int64
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n * sign, true
}

func parseFloatFast(s string) (float64, bool) {
	if !strings.ContainsAny(s, ".eE") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
