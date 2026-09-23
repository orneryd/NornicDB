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
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// pipelineClauseKind enumerates the clause types the pipeline executor
// understands. Anything else causes us to bail and return false so callers
// delegate to specialized executors. OPTIONAL MATCH is supported for bounded,
// single-hop clauses after a WITH horizon.
type pipelineClauseKind int

const (
	pipelineClauseMatch pipelineClauseKind = iota
	pipelineClauseOptionalMatch
	pipelineClauseCreate
	pipelineClauseMerge
	pipelineClauseDelete
	pipelineClauseSet
	pipelineClauseRemove
	pipelineClauseWith
	pipelineClauseUnwind
	pipelineClauseReturn
)

// pipelineClause is one segment of the pipeline. `text` includes the leading
// keyword (MATCH/CREATE/WITH/UNWIND/RETURN) and the clause body — exactly
// what you would pass to the clause implementation.
type pipelineClause struct {
	kind pipelineClauseKind
	text string
}

// pipelineMatchPhysicalHint describes downstream row requirements that a
// MATCH operator may safely push into candidate collection or traversal. The
// logical pipeline remains N-ary; this is an operator property derived by
// walking every remaining clause, not a separate query-shape handler.
type pipelineMatchPhysicalHint struct {
	orderExpr  string
	limit      int
	earlyLimit int
}

// pipelineRow carries bindings across clauses. Values may be *storage.Node,
// *storage.Edge, or scalars (for WITH projections and UNWIND variables).
type pipelineRow map[string]interface{}

// canExecuteAsPipeline returns true when the query is decomposable into the
// clause kinds this executor understands. Any unsupported clause (FOREACH,
// CALL subquery, etc.) causes a false return so the
// caller can select a specialized physical plan.
func canExecuteAsPipeline(cypher string) ([]pipelineClause, bool) {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil, false
	}
	upper := strings.ToUpper(cypher)
	// Inline MERGE actions and dynamic-label assignments are atomic operators
	// owned by their parse-once mutation plans. The row pipeline must not split
	// them into independent MERGE/SET clauses until it can retain that atomicity.
	if strings.Contains(upper, "ON CREATE SET") || strings.Contains(upper, "ON MATCH SET") || strings.Contains(cypher, "$(") {
		return nil, false
	}
	// Top-level UNWIND mutation plans use the batch executor. WITH-bearing
	// pipelines still require row projection here; direct UNWIND plans do not.
	if clauses[0].kind == pipelineClauseUnwind {
		hasWith := false
		requiresBatchPlan := false
		mergeCount := 0
		for _, clause := range clauses {
			hasWith = hasWith || clause.kind == pipelineClauseWith
			if clause.kind == pipelineClauseMerge {
				mergeCount++
			}
			switch clause.kind {
			case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate,
				pipelineClauseDelete, pipelineClauseSet, pipelineClauseRemove:
				requiresBatchPlan = true
			}
		}
		if mergeCount > 0 && (!hasWith || requiresBatchPlan || mergeCount > 1) {
			return nil, false
		}
	}
	hasMergeAction, hasSetAction := false, false
	for _, clause := range clauses {
		hasMergeAction = hasMergeAction || clause.kind == pipelineClauseMerge
		hasSetAction = hasSetAction || clause.kind == pipelineClauseSet
	}
	if hasMergeAction && hasSetAction {
		return nil, false
	}
	for _, clause := range clauses {
		if clause.kind == pipelineClauseOptionalMatch && strings.Contains(clause.text, "*") {
			return nil, false
		}
	}
	// Must contain at least two clauses.
	if len(clauses) < 2 {
		return nil, false
	}
	// Standalone CREATE ... RETURN remains one atomic write operator. CREATE
	// participates in the row pipeline as soon as another clause establishes
	// or consumes a row horizon.
	if len(clauses) == 2 && clauses[0].kind == pipelineClauseCreate && clauses[1].kind == pipelineClauseReturn {
		returnBody := strings.TrimSpace(clauses[1].text[len("RETURN"):])
		if firstTopLevelModifierIndex(returnBody) < 0 {
			return nil, false
		}
	}
	return clauses, true
}

// splitPipelineClauses walks the query from left to right and slices it on
// top-level MATCH/CREATE/WITH/UNWIND/RETURN keywords. Returns (clauses, true)
// on success. On anything unsupported (e.g. nested MERGE or CALL subquery)
// returns (nil, false) so the caller falls back.
func splitPipelineClauses(cypher string) ([]pipelineClause, bool) {
	type kw struct {
		name string
		kind pipelineClauseKind
	}
	// Order matters for multi-word lookups but we only care about single-word
	// keywords here; OPTIONAL MATCH and MERGE kick us out via detection below.
	keywords := []kw{
		{"OPTIONAL MATCH", pipelineClauseOptionalMatch},
		{"MATCH", pipelineClauseMatch},
		{"CREATE", pipelineClauseCreate},
		{"MERGE", pipelineClauseMerge},
		{"DETACH DELETE", pipelineClauseDelete},
		{"DELETE", pipelineClauseDelete},
		{"SET", pipelineClauseSet},
		{"REMOVE", pipelineClauseRemove},
		{"WITH", pipelineClauseWith},
		{"UNWIND", pipelineClauseUnwind},
		{"RETURN", pipelineClauseReturn},
	}
	// Clauses we don't yet model as their own kind force a fallback. Anything
	// else — including $param references and arbitrary WHERE on bindings —
	// is handled by the per-clause appliers below, which substitute params
	// from context and respect node bindings supplied by the caller.
	upper := strings.ToUpper(cypher)
	for _, bad := range []string{"FOREACH", "CALL "} {
		if findKeywordIndex(upper, strings.TrimRight(bad, " ")) >= 0 {
			return nil, false
		}
	}

	// Collect boundary positions for each supported keyword.
	var boundaries []pipelineBoundary
	for _, k := range keywords {
		for _, p := range findAllTopLevelPipelineKeywordPositions(cypher, k.name) {
			if k.kind == pipelineClauseMatch {
				preceding := strings.TrimSpace(strings.ToUpper(cypher[:p]))
				if strings.HasSuffix(preceding, "OPTIONAL") {
					continue
				}
				if strings.HasSuffix(preceding, "ON") {
					continue
				}
			}
			if k.kind == pipelineClauseSet {
				preceding := strings.TrimSpace(strings.ToUpper(cypher[:p]))
				if strings.HasSuffix(preceding, "ON CREATE") || strings.HasSuffix(preceding, "ON MATCH") {
					continue
				}
			}
			if k.kind == pipelineClauseReturn {
				preceding := strings.TrimRight(cypher[:p], " \t\n\r")
				if strings.HasSuffix(preceding, ":") {
					continue
				}
			}
			if k.name == "DELETE" {
				preceding := strings.TrimSpace(strings.ToUpper(cypher[:p]))
				if strings.HasSuffix(preceding, "DETACH") {
					continue
				}
			}
			if k.kind == pipelineClauseCreate {
				preceding := strings.TrimSpace(strings.ToUpper(cypher[:p]))
				if strings.HasSuffix(preceding, "ON") {
					continue
				}
			}
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

// findAllTopLevelPipelineKeywordPositions returns clause boundaries outside
// strings and every bracketed construct. In particular, MATCH inside
// EXISTS { MATCH ... } belongs to the predicate and must never become a new
// outer pipeline clause.
func findAllTopLevelPipelineKeywordPositions(query, keyword string) []int {
	positions := make([]int, 0, 4)
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	inSingle, inDouble := false, false
	for i := 0; i < len(query); i++ {
		character := query[i]
		if character == '\\' && (inSingle || inDouble) {
			i++
			continue
		}
		switch character {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		}
		if inSingle || inDouble {
			continue
		}
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 &&
			i+len(keyword) <= len(query) && strings.EqualFold(query[i:i+len(keyword)], keyword) &&
			(i == 0 || !isAlphaNumericByte(query[i-1])) &&
			(i+len(keyword) == len(query) || !isAlphaNumericByte(query[i+len(keyword)])) {
			positions = append(positions, i)
			i += len(keyword) - 1
			continue
		}
		switch character {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}
	}
	return positions
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
	if pipelineHasClauseKind(clauses, pipelineClauseSet) {
		cypher = normalizePipelineWhitespace(cypher)
		clauses, ok = canExecuteAsPipeline(cypher)
		if !ok {
			return nil, false, nil
		}
	}
	if clauses[0].kind == pipelineClauseUnwind {
		plan, err := e.prepareTopLevelUnwind(ctx, cypher)
		if err != nil {
			return nil, true, err
		}
		if result, handled, err := e.executeUnwindBatchOperator(ctx, plan); handled || err != nil {
			return result, true, err
		}
	}
	originalClauses := clauses

	// Substitute $param placeholders up-front — this is the same pass the
	// other top-level handlers perform. After this step the clause texts are
	// self-contained and our per-clause appliers only have to worry about
	// pipeline-bound names (from WITH/UNWIND/MATCH), not caller parameters.
	params := getParamsFromContext(ctx)
	if result, handled, err := e.tryExecutePipelineSimpleNodeReadPlan(ctx, clauses, params); handled || err != nil {
		return result, true, err
	}
	if params != nil {
		cypher = e.substituteParams(cypher, params)
		clauses, ok = canExecuteAsPipeline(cypher)
		if !ok {
			return nil, false, nil
		}
	}
	if result, handled, err := e.tryExecutePipelineOptionalMatchPlan(ctx, cypher, clauses); handled || err != nil {
		return result, true, err
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Start with one binding row. Parameters retain their typed values under
	// their `$name` expression keys so list/map inputs are not stringified while
	// crossing WITH and UNWIND horizons.
	initialRow := pipelineRow{}
	for name, value := range e.fabricRecordBindings {
		initialRow[name] = value
	}
	for name, value := range params {
		initialRow["$"+name] = value
	}
	rows := []pipelineRow{initialRow}
	scope := make(map[string]struct{})
	for name := range e.fabricRecordBindings {
		scope[name] = struct{}{}
	}

	for idx, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch:
			hint := e.pipelineMatchHint(clauses[idx+1:])
			newRows, ok, err := e.pipelineApplyMatchWithHint(ctx, rows, clause.text, hint)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "MATCH")
		case pipelineClauseOptionalMatch:
			newRows, err := e.pipelineApplyOptionalMatch(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "OPTIONAL MATCH")
		case pipelineClauseCreate:
			newRows, stats, ok, err := e.pipelineApplyCreate(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "CREATE")
			if stats != nil {
				result.Stats.NodesCreated += stats.NodesCreated
				result.Stats.RelationshipsCreated += stats.RelationshipsCreated
			}
		case pipelineClauseMerge:
			newRows, stats, err := e.pipelineApplyMerge(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			rows = newRows
			addPipelinePatternBindings(e, scope, clause.text, "MERGE")
			if stats != nil {
				result.Stats.NodesCreated += stats.NodesCreated
				result.Stats.RelationshipsCreated += stats.RelationshipsCreated
				result.Stats.PropertiesSet += stats.PropertiesSet
			}
		case pipelineClauseDelete:
			stats, ok, err := e.pipelineApplyDelete(ctx, rows, scope, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			result.Stats.NodesDeleted += stats.NodesDeleted
			result.Stats.RelationshipsDeleted += stats.RelationshipsDeleted
		case pipelineClauseSet:
			stats, ok, err := e.pipelineApplySet(ctx, rows, clause.text)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			result.Stats.PropertiesSet += stats.PropertiesSet
			result.Stats.LabelsAdded += stats.LabelsAdded
		case pipelineClauseRemove:
			if err := e.pipelineApplyRemove(ctx, rows, clause.text, result); err != nil {
				return nil, true, err
			}
		case pipelineClauseWith:
			if err := e.validatePipelineRangeArguments(rows, clause.text, "WITH"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineConversionArguments(rows, clause.text, "WITH"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineGraphFunctionArguments(rows, clause.text, "WITH"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineProjectionSubscripts(rows, clause.text, "WITH"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineSizeArguments(rows, clause.text, "WITH"); err != nil {
				return nil, true, err
			}
			newRows, ok := e.pipelineApplyWith(ctx, rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			scope = pipelineProjectionScope(scope, clause.text)
		case pipelineClauseUnwind:
			if err := e.validatePipelineRangeArguments(rows, clause.text, "UNWIND"); err != nil {
				return nil, true, err
			}
			newRows, ok := e.pipelineApplyUnwind(ctx, rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			if alias := pipelineUnwindAlias(clause.text); alias != "" {
				scope[alias] = struct{}{}
			}
		case pipelineClauseReturn:
			if err := validateDeletedEntityProjection(rows, clause.text); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineRangeArguments(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineConversionArguments(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineGraphFunctionArguments(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineProjectionSubscripts(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			if err := e.validatePipelineSizeArguments(rows, clause.text, "RETURN"); err != nil {
				return nil, true, err
			}
			final, ok := e.pipelineApplyReturn(ctx, rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			if len(final.Columns) == 0 && strings.TrimSpace(strings.TrimPrefix(clause.text, "RETURN")) == "*" {
				final.Columns = pipelineScopeColumns(scope)
			}
			if idx < len(originalClauses) && originalClauses[idx].kind == pipelineClauseReturn {
				if columns := pipelineReturnSourceColumns(originalClauses[idx].text); len(columns) == len(final.Columns) {
					final.Columns = columns
				}
			}
			result.Columns = final.Columns
			result.Rows = final.Rows
			// RETURN is always last.
			return result, true, nil
		}
		_ = idx
	}
	if len(clauses) > 0 && clauses[len(clauses)-1].kind == pipelineClauseSet {
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{len(rows)}}
	}

	return result, true, nil
}

// tryExecutePipelineSimpleNodeReadPlan applies cardinality and property-index
// operators before row materialization for a single node MATCH. The result is
// still projected by the pipeline, so indexed and scanned inputs share the
// same expression semantics.
func (e *StorageExecutor) tryExecutePipelineSimpleNodeReadPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) (*ExecuteResult, bool, error) {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	matchBody := strings.TrimSpace(clauses[0].text[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(matchBody, "WHERE"); whereIndex >= 0 {
		whereClause = strings.TrimSpace(matchBody[whereIndex+len("WHERE"):])
		matchBody = strings.TrimSpace(matchBody[:whereIndex])
	}
	if strings.Contains(matchBody, "-[") || strings.Contains(matchBody, "]-") || len(e.splitNodePatterns(matchBody)) != 1 {
		return nil, false, nil
	}
	nodePattern := e.parseNodePattern(ctx, matchBody)
	if nodePattern.variable == "" {
		return nil, false, nil
	}

	items := e.parseReturnItems(strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if whereClause == "" && len(items) == 1 && len(nodePattern.properties) == 0 && isAggregateFuncName(items[0].expr, "count") {
		inner := strings.TrimSpace(extractFuncInner(items[0].expr))
		if inner == "*" || inner == nodePattern.variable {
			store := e.getStorage(ctx)
			var count int64
			var err error
			if len(nodePattern.labels) == 1 && !storageHasDecayFiltering(store) {
				if viewport, ok := TemporalViewportFromContext(ctx); !ok || !viewport.Enabled() {
					if counter, ok := store.(interface{ NodeCountByLabel(string) (int64, error) }); ok {
						count, err = counter.NodeCountByLabel(nodePattern.labels[0])
						if err != nil {
							return nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err)
						}
						column := items[0].expr
						if items[0].alias != "" {
							column = items[0].alias
						}
						return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{count}}, Stats: &QueryStats{}}, true, nil
					}
				}
			}
		}
	}

	candidates, usedIndex, err := e.tryCollectNodesFromPropertyIndexInOrParam(nodePattern, whereClause, params)
	if err != nil {
		return nil, true, err
	}
	if !usedIndex {
		return nil, false, nil
	}
	rows := make([]pipelineRow, 0, len(candidates))
	for _, node := range candidates {
		row := pipelineRow{nodePattern.variable: node}
		for name, value := range params {
			row["$"+name] = value
		}
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
			rows = append(rows, row)
		}
	}
	result, projected := e.pipelineApplyReturn(ctx, rows, clauses[1].text)
	if !projected {
		return nil, false, nil
	}
	return result, true, nil
}

// tryExecutePipelineOptionalMatchPlan selects the optimized physical operator
// for a read-only MATCH followed by one or more OPTIONAL MATCH clauses. The
// query still enters through the row pipeline; clause count never changes the
// logical handler. The traversal operator performs the same N-ary left-outer
// join while avoiding repeated row materialization for graph-only queries.
func (e *StorageExecutor) tryExecutePipelineOptionalMatchPlan(ctx context.Context, cypher string, clauses []pipelineClause) (*ExecuteResult, bool, error) {
	if len(clauses) < 3 || clauses[0].kind != pipelineClauseMatch || clauses[len(clauses)-1].kind != pipelineClauseReturn {
		return nil, false, nil
	}
	seenOptional := false
	for index, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch:
			if seenOptional {
				return nil, false, nil
			}
		case pipelineClauseOptionalMatch:
			seenOptional = true
		case pipelineClauseReturn:
			if index != len(clauses)-1 {
				return nil, false, nil
			}
		default:
			return nil, false, nil
		}
	}
	if !seenOptional {
		return nil, false, nil
	}

	optionalIndex := findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	returnIndex := findKeywordIndexInContext(cypher, "RETURN")
	if optionalIndex <= len("MATCH") || returnIndex <= optionalIndex {
		return nil, false, nil
	}
	initialSection := strings.TrimSpace(cypher[len("MATCH"):optionalIndex])
	optionalSection := strings.TrimSpace(cypher[optionalIndex+len("OPTIONAL MATCH") : returnIndex])
	restOfQuery := strings.TrimSpace(cypher[returnIndex:])
	if initialSection == "" || optionalSection == "" {
		return nil, false, nil
	}

	result, err := e.executeTraversalSeededOptionalMatch(ctx, initialSection, initialSection, optionalSection, restOfQuery)
	return result, true, err
}

func pipelineHasClauseKind(clauses []pipelineClause, kind pipelineClauseKind) bool {
	for _, clause := range clauses {
		if clause.kind == kind {
			return true
		}
	}
	return false
}

func normalizePipelineWhitespace(query string) string {
	if !strings.ContainsAny(query, "\t\n\r") {
		return strings.TrimSpace(query)
	}
	var normalized strings.Builder
	normalized.Grow(len(query))
	quote := byte(0)
	spacePending := false
	for index := 0; index < len(query); index++ {
		character := query[index]
		if quote != 0 {
			normalized.WriteByte(character)
			if character == '\\' && quote != '`' && index+1 < len(query) {
				index++
				normalized.WriteByte(query[index])
				continue
			}
			if character == quote {
				if quote == '`' && index+1 < len(query) && query[index+1] == '`' {
					index++
					normalized.WriteByte(query[index])
					continue
				}
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			if spacePending && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			spacePending = false
			quote = character
			normalized.WriteByte(character)
			continue
		}
		if isWhitespace(character) {
			spacePending = normalized.Len() > 0
			continue
		}
		if spacePending {
			normalized.WriteByte(' ')
			spacePending = false
		}
		normalized.WriteByte(character)
	}
	return strings.TrimSpace(normalized.String())
}

// pipelineApplyDelete collects every entity target before validation and
// mutation, preserving statement atomicity while retaining input rows for
// subsequent WITH and RETURN clauses.
func (e *StorageExecutor) pipelineApplyDelete(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clause string) (*QueryStats, bool, error) {
	body := strings.TrimSpace(clause)
	detach := startsWithKeywordFold(body, "DETACH DELETE")
	if detach {
		body = strings.TrimSpace(body[len("DETACH DELETE"):])
	} else if startsWithKeywordFold(body, "DELETE") {
		body = strings.TrimSpace(body[len("DELETE"):])
	} else {
		return nil, false, nil
	}
	targets := splitTopLevelComma(body)
	if len(targets) == 0 {
		return nil, false, nil
	}
	for _, expression := range targets {
		if hasTopLevelDeleteLabelQualifier(expression) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidDelete",
				"DELETE accepts nodes, relationships, and paths, not labels or relationship types",
			)
		}
		if root := deleteExpressionRootIdentifier(expression); root != "" {
			if _, bound := scope[root]; !bound {
				return nil, true, newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"UndefinedVariable",
					fmt.Sprintf("DELETE expression %q refers to an undefined variable", strings.TrimSpace(expression)),
				)
			}
		} else if value, evaluated := e.evaluateRowExpression(strings.TrimSpace(expression), pipelineRow{}); evaluated && !isDeleteTargetValue(value) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
			)
		}
	}
	projected := &ExecuteResult{Columns: targets, Rows: make([][]interface{}, 0, len(rows))}
	for _, row := range rows {
		values := make([]interface{}, 0, len(targets))
		for _, expression := range targets {
			value, ok := e.evaluateRowExpression(strings.TrimSpace(expression), row)
			if !ok {
				return nil, true, newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"UndefinedVariable",
					fmt.Sprintf("DELETE expression %q refers to an undefined variable", strings.TrimSpace(expression)),
				)
			}
			if !isDeleteTargetValue(value) {
				return nil, true, newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"InvalidArgumentType",
					fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
				)
			}
			values = append(values, value)
		}
		projected.Rows = append(projected.Rows, values)
	}
	nodeIDs, edgeIDs := collectDeleteMutationTargets(projected)
	store := e.getStorage(ctx)
	if !detach {
		if err := validateNoResidualRelationships(store, nodeIDs, edgeIDs); err != nil {
			return nil, true, err
		}
	}

	deletedEdges := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		deletedEdges[edgeID] = struct{}{}
	}
	if detach {
		for _, nodeID := range nodeIDs {
			incident, err := undirectedIncidentEdges(store, nodeID)
			if err != nil {
				return nil, true, err
			}
			for _, edge := range incident {
				deletedEdges[edge.ID] = struct{}{}
			}
		}
	}
	if len(edgeIDs) > 0 {
		if err := store.BulkDeleteEdges(edgeIDs); err != nil {
			return nil, true, err
		}
	}
	stats := &QueryStats{RelationshipsDeleted: len(deletedEdges)}
	for _, nodeID := range nodeIDs {
		if err := store.DeleteNode(nodeID); err != nil {
			return nil, true, err
		}
		stats.NodesDeleted++
		e.removeNodeFromSearch(string(nodeID))
	}
	markPipelineRowsDeletedEntities(rows, nodeIDs, deletedEdges)
	return stats, true, nil
}

func (e *StorageExecutor) pipelineApplyRemove(ctx context.Context, rows []pipelineRow, clause string, result *ExecuteResult) error {
	body := strings.TrimSpace(clause[len("REMOVE"):])
	store := e.getStorage(ctx)
	for _, bindings := range rows {
		columns := make([]string, 0, len(bindings))
		row := make([]interface{}, 0, len(bindings))
		for name, value := range bindings {
			columns = append(columns, name)
			row = append(row, value)
		}
		matched := &ExecuteResult{Columns: columns, Rows: [][]interface{}{row}}
		if err := e.applyRemoveToMatchedRows(store, matched, body, result); err != nil {
			return err
		}
	}
	return nil
}

// pipelineApplySet mutates entities already bound in each pipeline row. Scalar
// and map bindings are attached as typed context values so assignments such as
// SET target = row retain their original Go/Cypher types.
func (e *StorageExecutor) pipelineApplySet(ctx context.Context, rows []pipelineRow, clause string) (*QueryStats, bool, error) {
	body := strings.TrimSpace(clause[len("SET"):])
	assignments := e.splitSetAssignments(body)
	if body == "" || len(assignments) == 0 {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	stats := &QueryStats{}
	if err := validatePipelineSetAssignments(assignments); err != nil {
		return nil, true, err
	}
	simpleTarget, simpleProperty, simpleExpression, simplePropertyAssignment := pipelineSimplePropertyAssignment(assignments)
	for _, row := range rows {
		nodes := make(map[string]*storage.Node)
		evalNodes := nodes
		evalNodesShared := true
		rels := make(map[string]*storage.Edge)
		params := make(map[string]interface{})
		for name, value := range getParamsFromContext(ctx) {
			params[name] = value
		}
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				nodes[name] = entity
				if !evalNodesShared {
					evalNodes[name] = entity
				}
			case *storage.Edge:
				rels[name] = entity
			default:
				params[name] = value
				if evalNodesShared {
					evalNodes = make(map[string]*storage.Node, util.SafePreallocSum(len(nodes), 1))
					for nodeName, node := range nodes {
						evalNodes[nodeName] = node
					}
					evalNodesShared = false
				}
				evalNodes[name] = &storage.Node{
					ID: storage.NodeID(name),
					Properties: map[string]interface{}{
						"value": value,
					},
				}
			}
		}
		rowCtx := withParams(ctx, params)
		targets := pipelineSetTargetVariables(assignments)
		if len(targets) == 0 {
			return nil, false, nil
		}
		for _, variable := range targets {
			if node := nodes[variable]; node != nil {
				beforeProperties := cloneStringAnyMap(node.Properties)
				beforeLabels := append([]string(nil), node.Labels...)
				if simplePropertyAssignment && simpleTarget == variable {
					value := e.evaluatePipelineSetValue(rowCtx, simpleExpression, evalNodes, rels)
					if err := validateSetPropertyValue(value); err != nil {
						return nil, true, err
					}
					setNodeProperty(node, simpleProperty, value)
				} else {
					e.applySetToNodeWithContext(rowCtx, node, variable, body, evalNodes, rels)
				}
				if !reflect.DeepEqual(beforeLabels, node.Labels) {
					if err := validatePolicyOnLabelChange(store, node, beforeLabels); err != nil {
						node.Properties = beforeProperties
						node.Labels = beforeLabels
						return nil, true, err
					}
					embeddingutil.InvalidateManagedEmbeddings(node)
				}
				if err := store.UpdateNode(node); err != nil {
					node.Properties = beforeProperties
					node.Labels = beforeLabels
					return nil, true, fmt.Errorf("SET %s: %w", pipelineSetOperation(variable, assignments), err)
				}
				stats.PropertiesSet += changedPropertyCount(beforeProperties, node.Properties)
				stats.LabelsAdded += addedLabelCount(beforeLabels, node.Labels)
				e.notifyNodeMutated(string(node.ID))
				continue
			}
			if relationship := rels[variable]; relationship != nil {
				beforeProperties := cloneStringAnyMap(relationship.Properties)
				if simplePropertyAssignment && simpleTarget == variable {
					value := e.evaluatePipelineSetValue(rowCtx, simpleExpression, evalNodes, rels)
					if err := validateSetPropertyValue(value); err != nil {
						return nil, true, err
					}
					setRelationshipProperty(relationship, simpleProperty, value)
				} else {
					e.applySetToRelationshipWithContext(rowCtx, relationship, variable, body, evalNodes, rels)
				}
				if err := store.UpdateEdge(relationship); err != nil {
					relationship.Properties = beforeProperties
					return nil, true, fmt.Errorf("SET %s: %w", pipelineSetOperation(variable, assignments), err)
				}
				stats.PropertiesSet += changedPropertyCount(beforeProperties, relationship.Properties)
				e.notifyEdgeMutated(string(relationship.ID))
				continue
			}
			return nil, false, nil
		}
	}
	return stats, true, nil
}

func pipelineSimplePropertyAssignment(assignments []string) (target, property, expression string, ok bool) {
	if len(assignments) != 1 {
		return "", "", "", false
	}
	assignment := strings.TrimSpace(assignments[0])
	if strings.Contains(assignment, "+=") {
		return "", "", "", false
	}
	equalIndex := strings.Index(assignment, "=")
	if equalIndex <= 0 {
		return "", "", "", false
	}
	target, property, hasProperty := parseSetAssignmentTarget(strings.TrimSpace(assignment[:equalIndex]))
	if !hasProperty {
		return "", "", "", false
	}
	expression = strings.TrimSpace(assignment[equalIndex+1:])
	return target, property, expression, expression != ""
}

func (e *StorageExecutor) evaluatePipelineSetValue(ctx context.Context, expression string, nodes map[string]*storage.Node, relationships map[string]*storage.Edge) interface{} {
	if value, ok := resolveDirectParamRef(ctx, expression); ok {
		return normalizePropValue(value)
	}
	return e.evaluateSetExpressionWithContext(ctx, expression, nodes, relationships)
}

func validatePipelineSetAssignments(assignments []string) error {
	for _, raw := range assignments {
		assignment := strings.TrimSpace(raw)
		if assignment == "" {
			return localizedError(localization.CypherMutationsSetAssignmentRequired(), nil)
		}
		if plusIndex := strings.Index(assignment, "+="); plusIndex >= 0 {
			target := strings.TrimSpace(assignment[:plusIndex])
			right := strings.TrimSpace(assignment[plusIndex+2:])
			if !isValidIdentifier(target) || right == "" {
				return localizedError(localization.CypherResidualSetAssignmentInvalid(assignment), nil)
			}
			if strings.HasPrefix(right, "{") {
				if _, err := parseSetMergeMapExpressionsStrict(right); err != nil {
					return localizedError(localization.CypherMutationsSetMergeParseFailed(err), err)
				}
			} else if _, scalar := parseLiteralScalarForPipeline(right); scalar {
				return localizedError(localization.CypherResidualSetAssignmentInvalid(assignment), nil)
			}
			continue
		}
		if equalIndex := strings.Index(assignment, "="); equalIndex >= 0 {
			target := strings.TrimSpace(assignment[:equalIndex])
			right := strings.TrimSpace(assignment[equalIndex+1:])
			variable, _, _ := parseSetAssignmentTarget(target)
			if !isValidIdentifier(variable) || right == "" {
				return localizedError(localization.CypherResidualSetAssignmentInvalid(assignment), nil)
			}
			continue
		}
		colonIndex := strings.Index(assignment, ":")
		if colonIndex <= 0 || !isValidIdentifier(strings.TrimSpace(assignment[:colonIndex])) {
			return localizedError(localization.CypherResidualSetAssignmentInvalid(assignment), nil)
		}
		labels := splitSetLabelChain(strings.TrimSpace(assignment[colonIndex+1:]))
		if len(labels) == 0 {
			return localizedError(localization.CypherResidualSetAssignmentInvalid(assignment), nil)
		}
		for _, label := range labels {
			if !isValidIdentifier(label) {
				return localizedError(localization.CypherMutationsInvalidLabelName(label), nil)
			}
		}
	}
	return nil
}

func pipelineSetOperation(variable string, assignments []string) string {
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if strings.HasPrefix(assignment, variable+" +=") || strings.HasPrefix(assignment, variable+"+=") {
			return variable + " +="
		}
		if strings.HasPrefix(assignment, variable+" =") || strings.HasPrefix(assignment, variable+"=") {
			return variable + " ="
		}
		if strings.HasPrefix(assignment, variable+".") {
			return variable + ".property ="
		}
	}
	return variable
}

func addedLabelCount(before, after []string) int {
	known := make(map[string]struct{}, len(before))
	for _, label := range before {
		known[label] = struct{}{}
	}
	added := 0
	for _, label := range after {
		if _, exists := known[label]; !exists {
			added++
		}
	}
	return added
}

func pipelineSetTargetVariables(assignments []string) []string {
	seen := make(map[string]struct{})
	var targets []string
	for _, assignment := range assignments {
		left := strings.TrimSpace(assignment)
		if idx := strings.Index(left, "+="); idx >= 0 {
			left = strings.TrimSpace(left[:idx])
		} else if idx := strings.Index(left, "="); idx >= 0 {
			left = strings.TrimSpace(left[:idx])
		}
		if idx := strings.IndexAny(left, ".:"); idx >= 0 {
			left = strings.TrimSpace(left[:idx])
		}
		if !isValidIdentifier(left) {
			continue
		}
		if _, exists := seen[left]; exists {
			continue
		}
		seen[left] = struct{}{}
		targets = append(targets, left)
	}
	return targets
}

func changedPropertyCount(before, after map[string]interface{}) int {
	changed := 0
	for key, value := range after {
		if old, exists := before[key]; !exists || !reflect.DeepEqual(old, value) {
			changed++
		}
	}
	for key := range before {
		if _, exists := after[key]; !exists {
			changed++
		}
	}
	return changed
}

// ---- clause appliers ----

func (e *StorageExecutor) pipelineApplyOptionalMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, error) {
	optionalClause := splitOptionalMatchClauses(strings.TrimSpace(clause[len("OPTIONAL MATCH"):]))
	if len(optionalClause) != 1 {
		return nil, localizedError(localization.CypherCoreOptionalMatchRequired(), nil)
	}
	nodeVariables := make(map[string]struct{})
	for _, variable := range extractNodeVariables(clause) {
		nodeVariables[variable] = struct{}{}
	}
	relationshipVariables := make(map[string]struct{})
	for _, variable := range extractRelationshipVariables(clause) {
		relationshipVariables[variable] = struct{}{}
	}

	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		traversalRow := traversalOptRow{
			nodes: make(map[string]*storage.Node),
			rels:  make(map[string]*storage.Edge),
		}
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				traversalRow.nodes[name] = entity
			case *storage.Edge:
				traversalRow.rels[name] = entity
			default:
				if value == nil {
					if _, isNodeBinding := nodeVariables[name]; isNodeBinding {
						traversalRow.nodes[name] = nil
						continue
					}
					if _, isRelationshipBinding := relationshipVariables[name]; isRelationshipBinding {
						traversalRow.rels[name] = nil
						continue
					}
				}
				if traversalRow.values == nil {
					traversalRow.values = make(map[string]interface{})
				}
				traversalRow.values[name] = value
			}
		}

		expanded, err := e.applyTraversalOptionalClause(ctx, []traversalOptRow{traversalRow}, optionalClause[0])
		if err != nil {
			return nil, err
		}
		for _, expandedRow := range expanded {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(expandedRow.nodes)+len(expandedRow.rels)))
			for name, value := range row {
				joined[name] = value
			}
			for name, node := range expandedRow.nodes {
				if node == nil {
					joined[name] = nil
				} else {
					joined[name] = node
				}
			}
			for name, relationship := range expandedRow.rels {
				if relationship == nil {
					joined[name] = nil
				} else {
					joined[name] = relationship
				}
			}
			for name, value := range expandedRow.values {
				joined[name] = value
			}
			out = append(out, joined)
		}
	}
	return out, nil
}

// pipelineApplyMatch runs MATCH for each current binding row and expands rows
// by the matched combinations. Returns (newRows, true, nil) on success. If a
// MATCH in the middle of a pipeline binds zero rows, it does NOT fail — it
// just zeros out the pipeline (matches Neo4j semantics for chained MATCH).
func (e *StorageExecutor) pipelineApplyMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	return e.pipelineApplyMatchWithHint(ctx, rows, clause, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
}

func (e *StorageExecutor) pipelineApplyMatchWithHint(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if expanded, ok, err := e.pipelineApplyBoundRelationshipListMatch(ctx, rows, clause); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok, err := e.pipelineApplyBoundTraversalMatch(ctx, rows, clause); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok, err := e.pipelineApplyInitialTraversalMatch(ctx, rows, clause, hint); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok, err := e.pipelineApplyInitialNodeMatch(ctx, rows, clause, hint); ok || err != nil {
		return expanded, ok, err
	}
	if expanded, ok := e.pipelineApplyChainedMatch(ctx, rows, clause); ok {
		return expanded, true, nil
	}

	// If the MATCH has scalar references to already-bound variables (e.g.
	// `MATCH (p:Product {productID: prodRef.productID})`), substitute them
	// per-row and re-seed referenced node variables by ID before invoking the
	// normal MATCH executor with a synthetic RETURN of the clause bindings.
	var out []pipelineRow
	store := e.getStorage(ctx)
	patternVariables := append(extractNodeVariables(clause), extractRelationshipVariables(clause)...)
	for _, row := range rows {
		hasNullPatternBinding := false
		for _, variable := range patternVariables {
			if value, bound := row[variable]; bound && value == nil {
				hasNullPatternBinding = true
				break
			}
		}
		if hasNullPatternBinding {
			continue
		}
		substituted := clause
		var matchPieces []string
		for name, val := range row {
			if node, isNode := val.(*storage.Node); isNode {
				if node != nil {
					for k, v := range node.Properties {
						pattern := name + "." + k
						substituted = strings.ReplaceAll(substituted, pattern, e.valueToLiteral(v))
					}
					if referencesVariable(substituted, name) {
						var label string
						if len(node.Labels) > 0 {
							label = ":" + node.Labels[0]
						}
						matchPieces = append(matchPieces,
							fmt.Sprintf("MATCH (%s%s) WHERE id(%s) = %q", name, label, name, string(node.ID)))
					}
				}
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

		patternPart := strings.TrimSpace(strings.TrimPrefix(substituted, "MATCH"))
		if whereIdx := findKeywordIndex(substituted, "WHERE"); whereIdx > 0 {
			patternPart = strings.TrimSpace(substituted[len("MATCH"):whereIdx])
		}
		returnVars := e.extractVariableNamesFromPattern(patternPart)
		if pathVariable := extractPathAssignmentVariable(patternPart); pathVariable != "" {
			returnVars = appendUniquePipelineBinding(returnVars, pathVariable)
		}
		for _, relVar := range extractRelationshipVariables(patternPart) {
			returnVars = appendUniquePipelineBinding(returnVars, relVar)
		}
		if len(returnVars) == 0 {
			trimmedPattern := strings.TrimSpace(patternPart)
			if strings.Contains(trimmedPattern, "-[") || strings.Contains(trimmedPattern, "]-") || !strings.HasPrefix(trimmedPattern, "(") {
				return nil, false, nil
			}
			const anonymousBinding = "__nornic_pipeline_anonymous"
			open := strings.Index(substituted, "(")
			if open < 0 {
				return nil, false, nil
			}
			substituted = substituted[:open+1] + anonymousBinding + substituted[open+1:]
			returnVars = []string{anonymousBinding}
		}

		queryToRun := substituted
		if len(matchPieces) > 0 {
			queryToRun = strings.Join(matchPieces, " ") + " " + substituted
		}
		queryToRun = normalizeMultiMatchWhereClauses(queryToRun)

		result, err := e.executeMatch(ctx, queryToRun+" RETURN "+strings.Join(returnVars, ", "))
		if err != nil {
			return nil, true, err
		}
		e.normalizeSetMatchRowsToNodes(result, store)
		e.normalizeSetMatchRowsToEdges(result, store)
		for _, resultRow := range result.Rows {
			newRow := make(pipelineRow, util.SafePreallocSum(len(row), len(result.Columns)))
			for k, v := range row {
				newRow[k] = v
			}
			for i, col := range result.Columns {
				if col == "__nornic_pipeline_anonymous" {
					continue
				}
				if i >= len(resultRow) {
					continue
				}
				newRow[col] = resultRow[i]
			}
			out = append(out, newRow)
		}
	}
	// No matches → empty pipeline (legal).
	return out, true, nil
}

func (e *StorageExecutor) pipelineMatchHint(remaining []pipelineClause) pipelineMatchPhysicalHint {
	hint := pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1}
	var terminalReturn string
	for index, clause := range remaining {
		if clause.kind != pipelineClauseReturn || index != len(remaining)-1 {
			return hint
		}
		terminalReturn = clause.text
	}
	if terminalReturn == "" {
		return hint
	}
	body := strings.TrimSpace(terminalReturn[len("RETURN"):])
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		return hint
	}
	for _, item := range e.parseReturnItems(body) {
		if pipelineExpressionContainsAggregate(item.expr) {
			return hint
		}
	}
	skip, hasSkip := parseIntModifier(body, "SKIP")
	if hasSkip && skip != 0 {
		return hint
	}
	limit, hasLimit := parseIntModifier(body, "LIMIT")
	if !hasLimit || limit < 0 {
		return hint
	}
	hint.limit = limit
	if orderIndex := topLevelKeywordIndex(body, "ORDER BY"); orderIndex >= 0 {
		orderExpr := strings.TrimSpace(body[orderIndex+len("ORDER BY"):])
		end := len(orderExpr)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(orderExpr, keyword); index >= 0 && index < end {
				end = index
			}
		}
		hint.orderExpr = strings.TrimSpace(orderExpr[:end])
	} else {
		hint.earlyLimit = limit
	}
	return hint
}

// pipelineApplyInitialTraversalMatch adapts the shared streaming traversal
// operators to pipeline rows. It returns graph entities as bindings so every
// later WITH, mutation, and RETURN clause continues through the same executor.
func (e *StorageExecutor) pipelineApplyInitialTraversalMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	if !containsRelExistencePattern(pattern) {
		return nil, false, nil
	}
	// A mixed comma-separated MATCH is a product of independent pattern
	// components. The single traversal operator cannot consume only one
	// component without losing rows; leave the complete product to the shared
	// multi-pattern operator.
	if len(splitTopLevelComma(pattern)) != 1 {
		return nil, false, nil
	}

	variables := make([]string, 0)
	for _, variable := range extractNodeVariables(pattern) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		variables = appendUniquePipelineBinding(variables, pathVariable)
	}
	const anonymousBinding = "__nornic_pipeline_traversal"
	returnItems := make([]returnItem, 0, len(variables)+1)
	for _, variable := range variables {
		returnItems = append(returnItems, returnItem{expr: variable, alias: variable})
	}
	if len(returnItems) == 0 {
		returnItems = append(returnItems, returnItem{expr: "1", alias: anonymousBinding})
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		materializedPattern := e.materializePipelinePropertyExpressions(pattern, row)
		materializedWhere := e.materializePipelinePredicateExpressions(whereClause, row)
		physicalWhere := pipelineTraversalPushdownPredicate(materializedWhere, row, variables)
		var result *ExecuteResult
		var handled bool
		var err error
		if hint.limit > 0 && hint.orderExpr != "" {
			result, handled, err = e.tryExecuteTraversalStartSeedOrderLimit(ctx, materializedPattern, physicalWhere, returnItems, pathVariable, hint.orderExpr, hint.limit)
			if err == nil && !handled {
				result, handled, err = e.tryExecuteTraversalEndSeedOrderLimit(ctx, materializedPattern, physicalWhere, returnItems, pathVariable, hint.orderExpr, hint.limit)
			}
		}
		if err != nil {
			return nil, true, err
		}
		if !handled {
			result, err = e.executeMatchWithRelationshipsWithPath(ctx, materializedPattern, physicalWhere, returnItems, nil, pathVariable, hint.earlyLimit)
			if err != nil {
				return nil, true, err
			}
		}
		e.normalizeSetMatchRowsToNodes(result, store)
		e.normalizeSetMatchRowsToEdges(result, store)
		for _, resultRow := range result.Rows {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(result.Columns)))
			for name, value := range row {
				joined[name] = value
			}
			compatible := true
			for index, column := range result.Columns {
				if column == anonymousBinding || index >= len(resultRow) {
					continue
				}
				value := resultRow[index]
				if existing, bound := joined[column]; bound && !pipelineBindingValuesEqual(existing, value) {
					compatible = false
					break
				}
				joined[column] = value
			}
			if compatible && (materializedWhere == "" || e.evaluateWithWhereCondition(ctx, materializedWhere, map[string]interface{}(joined))) {
				out = append(out, joined)
			}
		}
	}
	return out, true, nil
}

func pipelineTraversalPushdownPredicate(whereClause string, row pipelineRow, localVariables []string) string {
	if strings.TrimSpace(whereClause) == "" {
		return ""
	}
	local := make(map[string]struct{}, len(localVariables))
	for _, variable := range localVariables {
		local[variable] = struct{}{}
	}
	terms := splitTopLevelAndConjuncts(whereClause)
	pushable := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		dependsOnOuterBinding := false
		for name := range row {
			if _, isLocal := local[name]; isLocal || strings.HasPrefix(name, "$") {
				continue
			}
			if referencesVariable(term, name) {
				dependsOnOuterBinding = true
				break
			}
		}
		if !dependsOnOuterBinding {
			pushable = append(pushable, term)
		}
	}
	return strings.Join(pushable, " AND ")
}

func pipelineBindingValuesEqual(left, right interface{}) bool {
	switch typed := left.(type) {
	case *storage.Node:
		other, ok := right.(*storage.Node)
		return ok && typed != nil && other != nil && typed.ID == other.ID
	case *storage.Edge:
		other, ok := right.(*storage.Edge)
		return ok && typed != nil && other != nil && typed.ID == other.ID
	default:
		return reflect.DeepEqual(left, right)
	}
}

func (e *StorageExecutor) pipelineApplyInitialNodeMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") || len(e.splitNodePatterns(pattern)) != 1 {
		return nil, false, nil
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	basePattern := e.parseNodePattern(ctx, pattern)
	if basePattern.variable == "" {
		return nil, false, nil
	}
	candidateCache := make(map[string][]*storage.Node)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		materializedPattern := e.materializePipelinePropertyExpressions(pattern, row)
		materializedWhere := e.materializePipelinePredicateExpressions(whereClause, row)
		nodePattern := e.parseNodePattern(ctx, materializedPattern)
		if bound, exists := row[nodePattern.variable]; exists {
			node, isNode := bound.(*storage.Node)
			if !isNode || node == nil || !pipelineNodeMatchesPattern(node, nodePattern) {
				continue
			}
			if materializedWhere == "" || e.evaluateWithWhereCondition(ctx, materializedWhere, map[string]interface{}(row)) {
				out = append(out, e.pipelineBindZeroLengthPath(row, pathVariable, node))
			}
			continue
		}
		cacheKey := materializedPattern + "\x00" + materializedWhere
		nodes, cached := candidateCache[cacheKey]
		if !cached {
			var err error
			nodes, err = e.collectPipelineInitialNodeCandidates(ctx, nodePattern, materializedWhere, hint)
			if err != nil {
				return nil, true, err
			}
			candidateCache[cacheKey] = nodes
		}
		for _, node := range nodes {
			joined := make(pipelineRow, len(row)+2)
			for name, value := range row {
				joined[name] = value
			}
			joined[nodePattern.variable] = node
			if pathVariable != "" {
				joined[pathVariable] = e.pathToMap(PathResult{Nodes: []*storage.Node{node}})
			}
			if materializedWhere == "" || e.evaluateWithWhereCondition(ctx, materializedWhere, map[string]interface{}(joined)) {
				out = append(out, joined)
			}
		}
	}
	return out, true, nil
}

func (e *StorageExecutor) pipelineBindZeroLengthPath(row pipelineRow, variable string, node *storage.Node) pipelineRow {
	if variable == "" {
		return row
	}
	joined := make(pipelineRow, len(row)+1)
	for name, value := range row {
		joined[name] = value
	}
	joined[variable] = e.pathToMap(PathResult{Nodes: []*storage.Node{node}})
	return joined
}

func (e *StorageExecutor) materializePipelinePredicateExpressions(expression string, row pipelineRow) string {
	materialized := expression
	for name, value := range row {
		if strings.HasPrefix(name, "$") {
			continue
		}
		if object, ok := toStringAnyMap(value); ok {
			for property, propertyValue := range object {
				materialized = replaceQualifiedReferenceOutsideQuotes(materialized, name+"."+property, e.valueToLiteral(propertyValue))
			}
			continue
		}
		switch entity := value.(type) {
		case *storage.Node:
			if entity != nil {
				for property, propertyValue := range entity.Properties {
					materialized = replaceQualifiedReferenceOutsideQuotes(materialized, name+"."+property, e.valueToLiteral(propertyValue))
				}
			}
			continue
		case *storage.Edge:
			if entity != nil {
				for property, propertyValue := range entity.Properties {
					materialized = replaceQualifiedReferenceOutsideQuotes(materialized, name+"."+property, e.valueToLiteral(propertyValue))
				}
			}
			continue
		}
		materialized = replaceIdentifierOutsideQuotes(materialized, name, e.valueToLiteral(value))
	}
	return materialized
}

// replaceQualifiedReferenceOutsideQuotes replaces a complete dotted row
// reference without rewriting string literals, longer identifiers, or a
// property access rooted at the reference. replaceIdentifierOutsideQuotes is
// intentionally token-oriented and therefore cannot match a dotted name.
func replaceQualifiedReferenceOutsideQuotes(input, reference, replacement string) string {
	if reference == "" || !strings.Contains(input, reference) {
		return input
	}
	var output strings.Builder
	output.Grow(len(input) + len(replacement))
	quote := byte(0)
	for index := 0; index < len(input); {
		character := input[index]
		if quote != 0 {
			output.WriteByte(character)
			index++
			if character == '\\' && quote != '`' && index < len(input) {
				output.WriteByte(input[index])
				index++
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			quote = character
			output.WriteByte(character)
			index++
			continue
		}
		end := index + len(reference)
		if end <= len(input) && input[index:end] == reference &&
			(index == 0 || (!isIdentByte(input[index-1]) && input[index-1] != '.')) &&
			(end == len(input) || (!isIdentByte(input[end]) && input[end] != '.')) {
			output.WriteString(replacement)
			index = end
			continue
		}
		output.WriteByte(character)
		index++
	}
	return output.String()
}

// collectPipelineInitialNodeCandidates chooses an indexed seed whenever one
// of the shared property-index operators can safely narrow the MATCH. The
// complete predicate is still evaluated after the join, so these operators
// only affect the physical seed source and never the logical result.
func (e *StorageExecutor) collectPipelineInitialNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) ([]*storage.Node, error) {
	params := getParamsFromContext(ctx)
	streamingWhere := ""
	if hint.earlyLimit > 0 {
		streamingWhere = whereClause
	}
	if hint.limit > 0 && hint.orderExpr != "" {
		orderedPlans := []func() ([]*storage.Node, bool, error){
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexNotNullOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit)
			},
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit)
			},
		}
		for _, plan := range orderedPlans {
			nodes, used, err := plan()
			if err != nil {
				return nil, err
			}
			if used {
				if len(nodes) == 0 {
					e.markOuterScanFallbackUsed()
					return e.collectNodesWithStreaming(ctx, nodePattern.labels, nodePattern.properties, nodePattern.variable, "", -1)
				}
				e.markOuterIndexTopKUsed()
				return nodes, nil
			}
		}
	}
	identifierPlans := []func() ([]*storage.Node, bool, error){
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromIDEqualityCompound(ctx, nodePattern, whereClause, params)
		},
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromIDInParam(nodePattern, whereClause, params)
		},
	}
	for _, plan := range identifierPlans {
		nodes, used, err := plan()
		if err != nil {
			return nil, err
		}
		if used {
			return nodes, nil
		}
	}
	indexedPlans := []func() ([]*storage.Node, bool, error){
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexInOrParam(nodePattern, whereClause, params)
		},
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexOrEquality(ctx, nodePattern, whereClause, params)
		},
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern, whereClause, params)
		},
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexEqualityCompound(ctx, nodePattern, whereClause)
		},
		func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexNotNull(nodePattern, whereClause)
		},
	}
	for _, plan := range indexedPlans {
		nodes, used, err := plan()
		if err != nil {
			return nil, err
		}
		if used {
			// A schema can transiently advertise an index whose entries have not
			// caught up with existing data. Preserve correctness by streaming the
			// scan fallback only for an empty property-index seed.
			if len(nodes) == 0 {
				return e.collectNodesWithStreaming(ctx, nodePattern.labels, nodePattern.properties, nodePattern.variable, streamingWhere, hint.earlyLimit)
			}
			if len(nodePattern.properties) > 0 {
				nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
			}
			return nodes, nil
		}
	}
	return e.collectNodesWithStreaming(ctx, nodePattern.labels, nodePattern.properties, nodePattern.variable, streamingWhere, hint.earlyLimit)
}

// pipelineApplyChainedMatch expands a MATCH against graph bindings already in
// each row. It is the pipeline adapter around the shared traversal operator:
// node and relationship identity conflicts are rejected during the join, and
// WHERE is evaluated once against the complete joined row.
func (e *StorageExecutor) pipelineApplyChainedMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	if len(rows) == 0 || extractPathAssignmentVariable(clause) != "" {
		return nil, false
	}
	patternBindings := make(map[string]struct{})
	for _, variable := range extractNodeVariables(clause) {
		patternBindings[variable] = struct{}{}
	}
	for _, variable := range extractRelationshipVariables(clause) {
		patternBindings[variable] = struct{}{}
	}
	hasGraphBinding := false
	for _, row := range rows {
		for name, value := range row {
			if _, referenced := patternBindings[name]; !referenced {
				continue
			}
			switch value.(type) {
			case *storage.Node, *storage.Edge:
				hasGraphBinding = true
			}
		}
	}
	if !hasGraphBinding {
		return nil, false
	}

	pattern := strings.TrimSpace(clause[len("MATCH"):])
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	for _, component := range splitTopLevelComma(pattern) {
		if !containsRelExistencePattern(component) {
			return nil, false
		}
	}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		nodes := make(binding)
		relationships := make(relationshipBinding)
		for name, value := range row {
			switch entity := value.(type) {
			case *storage.Node:
				if entity != nil {
					nodes[name] = entity
				}
			case *storage.Edge:
				if entity != nil {
					relationships[name] = entity
				}
			}
		}
		joinedNodes, joinedRelationships := e.executeChainedMatch(ctx, pattern, []binding{nodes}, []relationshipBinding{relationships})
		for index, nodeBindings := range joinedNodes {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeBindings)))
			for name, value := range row {
				joined[name] = value
			}
			for name, node := range nodeBindings {
				joined[name] = node
			}
			if index < len(joinedRelationships) {
				for name, relationship := range joinedRelationships[index] {
					joined[name] = relationship
				}
			}
			if whereClause == "" || e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(joined)) {
				out = append(out, joined)
			}
		}
	}
	return out, true
}

func (e *StorageExecutor) pipelineApplyBoundRelationshipListMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.IsChained || !match.Relationship.VariableLength || match.Relationship.Variable == "" {
		return nil, false, nil
	}
	for _, row := range rows {
		if _, exists := row[match.Relationship.Variable]; !exists {
			return nil, false, nil
		}
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		relationships, ok := pipelineRelationshipList(row[match.Relationship.Variable])
		if !ok || len(relationships) == 0 || relationshipListReusesEdge(relationships) {
			continue
		}
		for _, endpoints := range traceRelationshipList(relationships, match.Relationship.Direction) {
			start, startErr := store.GetNode(endpoints[0])
			end, endErr := store.GetNode(endpoints[1])
			if startErr != nil || endErr != nil || start == nil || end == nil ||
				!pipelineNodeMatchesPattern(start, match.StartNode) || !pipelineNodeMatchesPattern(end, match.EndNode) {
				continue
			}
			if bound, exists := row[match.StartNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.ID != start.ID {
					continue
				}
			}
			if bound, exists := row[match.EndNode.variable]; exists {
				boundNode, isNode := bound.(*storage.Node)
				if !isNode || boundNode == nil || boundNode.ID != end.ID {
					continue
				}
			}
			expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
			for name, value := range row {
				expanded[name] = value
			}
			if match.StartNode.variable != "" {
				expanded[match.StartNode.variable] = start
			}
			if match.EndNode.variable != "" {
				expanded[match.EndNode.variable] = end
			}
			out = append(out, expanded)
		}
	}
	return out, true, nil
}

func pipelineRelationshipList(value interface{}) ([]*storage.Edge, bool) {
	switch relationships := value.(type) {
	case []*storage.Edge:
		return relationships, true
	case []interface{}:
		result := make([]*storage.Edge, len(relationships))
		for index, value := range relationships {
			relationship, ok := value.(*storage.Edge)
			if !ok || relationship == nil {
				return nil, false
			}
			result[index] = relationship
		}
		return result, true
	default:
		return nil, false
	}
}

func relationshipListReusesEdge(relationships []*storage.Edge) bool {
	seen := make(map[storage.EdgeID]struct{}, len(relationships))
	for _, relationship := range relationships {
		if relationship == nil {
			return true
		}
		if _, exists := seen[relationship.ID]; exists {
			return true
		}
		seen[relationship.ID] = struct{}{}
	}
	return false
}

func traceRelationshipList(relationships []*storage.Edge, direction string) [][2]storage.NodeID {
	if len(relationships) == 0 {
		return nil
	}
	starts := [][2]storage.NodeID{{relationships[0].StartNode, relationships[0].EndNode}}
	if direction == "incoming" {
		starts[0] = [2]storage.NodeID{relationships[0].EndNode, relationships[0].StartNode}
	} else if direction == "both" && relationships[0].StartNode != relationships[0].EndNode {
		starts = append(starts, [2]storage.NodeID{relationships[0].EndNode, relationships[0].StartNode})
	}
	for _, relationship := range relationships[1:] {
		next := starts[:0]
		for _, endpoints := range starts {
			switch direction {
			case "outgoing":
				if endpoints[1] == relationship.StartNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.EndNode})
				}
			case "incoming":
				if endpoints[1] == relationship.EndNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.StartNode})
				}
			default:
				if endpoints[1] == relationship.StartNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.EndNode})
				}
				if endpoints[1] == relationship.EndNode && relationship.StartNode != relationship.EndNode {
					next = append(next, [2]storage.NodeID{endpoints[0], relationship.StartNode})
				}
			}
		}
		starts = next
	}
	return starts
}

func (e *StorageExecutor) pipelineApplyBoundTraversalMatch(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	if findKeywordIndexInContext(pattern, "WHERE") >= 0 || strings.Contains(pattern, "*") || strings.Contains(pattern, "{") {
		return nil, false, nil
	}
	nodeGroups, brackets := scanOptionalPatternShape(pattern)
	if nodeGroups != 2 || brackets > 1 {
		return nil, false, nil
	}
	endpoints, err := e.parseOptionalClauseEndpoints(ctx, pattern)
	if err != nil || endpoints.source.variable == "" || endpoints.target.variable == "" {
		return nil, false, nil
	}
	if _, sourceBound := rows[0][endpoints.source.variable]; !sourceBound {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		source, ok := row[endpoints.source.variable].(*storage.Node)
		if !ok || source == nil || !pipelineNodeMatchesPattern(source, endpoints.source) {
			continue
		}
		boundTarget, targetBound := row[endpoints.target.variable]
		if targetBound && boundTarget == nil {
			continue
		}
		expectedTarget, expectedTargetIsNode := boundTarget.(*storage.Node)
		if targetBound && (!expectedTargetIsNode || expectedTarget == nil) {
			continue
		}
		var expectedRelationship *storage.Edge
		relationshipBound := false
		if endpoints.relVar != "" {
			if boundRelationship, bound := row[endpoints.relVar]; bound {
				relationshipBound = true
				var relationshipIsEdge bool
				expectedRelationship, relationshipIsEdge = boundRelationship.(*storage.Edge)
				if !relationshipIsEdge || expectedRelationship == nil {
					continue
				}
			}
		}

		var edges []*storage.Edge
		switch endpoints.direction {
		case "out":
			edges, err = store.GetOutgoingEdges(source.ID)
		case "in":
			edges, err = store.GetIncomingEdges(source.ID)
		default:
			edges, err = undirectedIncidentEdges(store, source.ID)
		}
		if err != nil {
			return nil, true, err
		}

		for _, edge := range edges {
			if relationshipBound && edge.ID != expectedRelationship.ID {
				continue
			}
			if endpoints.relType != "" && edge.Type != endpoints.relType {
				continue
			}
			targetID := edge.EndNode
			if edge.StartNode != source.ID {
				targetID = edge.StartNode
			}
			target, getErr := store.GetNode(targetID)
			if getErr != nil {
				return nil, true, getErr
			}
			if target == nil || !pipelineNodeMatchesPattern(target, endpoints.target) {
				continue
			}
			if targetBound && target.ID != expectedTarget.ID {
				continue
			}

			expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
			for name, value := range row {
				expanded[name] = value
			}
			expanded[endpoints.target.variable] = target
			if endpoints.relVar != "" {
				expanded[endpoints.relVar] = edge
			}
			out = append(out, expanded)
		}
	}
	return out, true, nil
}

func pipelineNodeMatchesPattern(node *storage.Node, pattern nodePatternInfo) bool {
	if !mergeNodeHasLabels(node, pattern.labels) {
		return false
	}
	for property, expected := range pattern.properties {
		if actual, exists := node.Properties[property]; !exists || !reflect.DeepEqual(actual, expected) {
			return false
		}
	}
	return true
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
		substituted := e.materializePipelinePropertyExpressions(clause, row)
		for name, val := range row {
			if node, isNode := val.(*storage.Node); isNode {
				if node != nil {
					for property, propertyValue := range node.Properties {
						substituted = strings.ReplaceAll(substituted, name+"."+property, e.valueToLiteral(propertyValue))
					}
				}
				continue
			}
			if edge, isEdge := val.(*storage.Edge); isEdge {
				if edge != nil {
					for property, propertyValue := range edge.Properties {
						substituted = strings.ReplaceAll(substituted, name+"."+property, e.valueToLiteral(propertyValue))
					}
				}
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

		subResult, refsNodes, refsEdges, err := e.executeCreateWithRefsOrCompound(ctx, queryToRun)
		if err != nil {
			return nil, nil, true, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
		}
		if subResult != nil && subResult.Stats != nil {
			stats.NodesCreated += subResult.Stats.NodesCreated
			stats.RelationshipsCreated += subResult.Stats.RelationshipsCreated
		}

		// Merge newly-created node bindings into the row so subsequent
		// pipeline steps can reference them (e.g. CREATE (c)-[:REL]->(o)
		// where `o` was created by an earlier CREATE in the same pipeline).
		newRow := make(pipelineRow, util.SafePreallocSum(len(row), len(refsNodes), len(refsEdges)))
		for k, v := range row {
			newRow[k] = v
		}
		for k, n := range refsNodes {
			newRow[k] = n
		}
		for k, relationship := range refsEdges {
			newRow[k] = relationship
		}
		out = append(out, newRow)
	}

	return out, stats, true, nil
}

// pipelineApplyMerge executes one MERGE per input row while retaining the row
// bindings for subsequent clauses. This preserves Cypher's row-at-a-time
// mutation semantics after UNWIND/WITH without duplicating MERGE behavior.
func (e *StorageExecutor) pipelineApplyMerge(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, error) {
	stats := &QueryStats{}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		substituted := e.materializePipelinePropertyExpressions(clause, row)
		nodeContext := make(map[string]*storage.Node)
		relContext := make(map[string]*storage.Edge)
		for name, value := range row {
			switch typed := value.(type) {
			case *storage.Node:
				nodeContext[name] = typed
			case *storage.Edge:
				relContext[name] = typed
			default:
				substituted = replaceIdentifierOutsideQuotes(substituted, name, e.valueToLiteral(value))
			}
		}
		var relationshipPattern *mergeRelationshipPattern
		var nodePathVariable string
		var nodePathBinding string
		mergeBody := strings.TrimSpace(substituted)
		if startsWithKeywordFold(mergeBody, "MERGE") {
			mergeBody = strings.TrimSpace(mergeBody[len("MERGE"):])
			if strings.Contains(mergeBody, "[") {
				var parseErr error
				relationshipPattern, parseErr = e.parseMergeRelationshipPattern(ctx, mergeBody, nodeContext, relContext)
				if parseErr != nil {
					return nil, nil, parseErr
				}
			} else if nodePathVariable = extractPathAssignmentVariable(mergeBody); nodePathVariable != "" {
				mergeBody = strings.TrimSpace(mergeBody[strings.Index(mergeBody, "=")+1:])
				nodePathBinding = e.extractVarName(mergeBody)
				substituted = "MERGE " + mergeBody
			}
		}
		if relationshipPattern == nil {
			nodePattern := mergeBody
			variable, labels, properties, parseErr := e.parseMergePattern(ctx, nodePattern)
			if parseErr == nil {
				properties = e.resolveMergePropsWithContext(ctx, properties, nodeContext, relContext)
				_, alreadyBound := nodeContext[variable]
				if variable == "" || !alreadyBound {
					matches, findErr := e.findMergeNodes(e.getStorage(ctx), labels, properties)
					if findErr != nil {
						return nil, nil, findErr
					}
					if len(matches) > 0 {
						for _, node := range matches {
							expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
							for name, value := range row {
								expanded[name] = value
							}
							if variable != "" {
								expanded[variable] = node
							}
							if nodePathVariable != "" {
								path := PathResult{Nodes: []*storage.Node{node}}
								expanded[nodePathVariable] = e.pathToMap(path)
							}
							out = append(out, expanded)
						}
						continue
					}
				}
			}
		}
		merged, err := e.executeMergeWithContext(ctx, substituted, nodeContext, relContext)
		if err != nil {
			return nil, nil, err
		}
		if merged != nil && merged.Stats != nil {
			stats.NodesCreated += merged.Stats.NodesCreated
			stats.RelationshipsCreated += merged.Stats.RelationshipsCreated
			stats.PropertiesSet += merged.Stats.PropertiesSet
		}
		newRow := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeContext), len(relContext)))
		for name, value := range row {
			newRow[name] = value
		}
		for name, node := range nodeContext {
			newRow[name] = node
		}
		for name, relationship := range relContext {
			newRow[name] = relationship
		}
		if nodePathVariable != "" {
			if node := nodeContext[nodePathBinding]; node != nil {
				path := PathResult{Nodes: []*storage.Node{node}}
				newRow[nodePathVariable] = e.pathToMap(path)
			}
		}
		if relationshipPattern != nil {
			startNode := nodeContext[relationshipPattern.startVariable]
			endNode := nodeContext[relationshipPattern.endVariable]
			if startNode != nil && endNode != nil {
				matches, findErr := findParsedMergeRelationships(e.getStorage(ctx), relationshipPattern, startNode, endNode)
				if findErr != nil {
					return nil, nil, findErr
				}
				if len(matches) > 0 {
					for _, relationship := range matches {
						expanded := make(pipelineRow, util.SafePreallocSum(len(newRow), 2))
						for name, value := range newRow {
							expanded[name] = value
						}
						if relationshipPattern.relVariable != "" {
							expanded[relationshipPattern.relVariable] = relationship
						}
						if relationshipPattern.pathVariable != "" {
							path := PathResult{Nodes: []*storage.Node{startNode, endNode}, Relationships: []*storage.Edge{relationship}, Length: 1}
							expanded[relationshipPattern.pathVariable] = e.pathToMap(path)
						}
						out = append(out, expanded)
					}
					continue
				}
			}
		}
		out = append(out, newRow)
	}
	return out, stats, nil
}

// materializePipelinePropertyExpressions evaluates property-map values using
// the current row before the CREATE/MERGE parsers consume them. Substituting a
// variable token alone is insufficient for expressions such as row.parts[0]
// or row.value + '!': it can turn valid expressions into quoted source text.
func (e *StorageExecutor) materializePipelinePropertyExpressions(clause string, row pipelineRow) string {
	var output strings.Builder
	output.Grow(len(clause))
	for cursor := 0; cursor < len(clause); {
		if clause[cursor] != '{' {
			output.WriteByte(clause[cursor])
			cursor++
			continue
		}
		end := e.findMatchingBrace(clause, cursor)
		if end < 0 {
			output.WriteString(clause[cursor:])
			break
		}
		body := clause[cursor+1 : end]
		pairs := e.splitPropertyPairs(body)
		materialized := make([]string, 0, len(pairs))
		for _, pair := range pairs {
			colon := findTopLevelMapKeyValueSeparator(pair)
			if colon <= 0 {
				materialized = append(materialized, pair)
				continue
			}
			key := strings.TrimSpace(pair[:colon])
			expression := strings.TrimSpace(pair[colon+1:])
			if value, ok := e.evaluateRowExpression(expression, row); ok {
				expression = e.valueToLiteral(value)
			}
			materialized = append(materialized, key+": "+expression)
		}
		output.WriteByte('{')
		output.WriteString(strings.Join(materialized, ", "))
		output.WriteByte('}')
		cursor = end + 1
	}
	return output.String()
}

// executeCreateWithRefsOrCompound runs a CREATE or MATCH...CREATE query and
// returns the created entity refs. Handles both the standalone CREATE case and
// the synthetic `MATCH (x) WHERE id(x) = "..." MATCH (y) ... CREATE ...`
// prefix we prepend for pre-bound variables.
func (e *StorageExecutor) executeCreateWithRefsOrCompound(ctx context.Context, query string) (*ExecuteResult, map[string]*storage.Node, map[string]*storage.Edge, error) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "CREATE") {
		return e.executeCreateWithRefs(ctx, query)
	}
	// MATCH ... CREATE ... — retain both the matched node bindings and newly
	// created relationship bindings. Later pipeline clauses (for example SET
	// on a relationship created here) must continue from this exact mutation,
	// never fall back and execute the CREATE a second time.
	return e.executeCompoundMatchCreateWithRefs(ctx, query)
}

// pipelineApplyWith drops / renames binding keys according to a WITH clause.
// Supports:
//   - plain variables:     `WITH a, b`            carries each forward
//   - map placeholder:     `WITH o, {}`           drops the {} projection
//   - variable → alias:    `WITH a AS b`          renames
//   - expression → alias:  `WITH a.name AS n`     evaluates a row expression
//   - aggregate pass-thru: `WITH count(*) AS c`   counts current rows
//
// Projection expressions use the same row-expression operator as WHERE,
// RETURN, and ORDER BY so list, map, property, and postfix operations cannot
// diverge between pipeline clauses.
func (e *StorageExecutor) pipelineApplyWith(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "WITH"))
	body = strings.TrimPrefix(body, "with")
	orderTerms := parseOrderByTerms(body)
	withSkip, withLimit := 0, -1
	if skipIndex := topLevelKeywordIndex(body, "SKIP"); skipIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "SKIP"), rows)
		if !ok {
			return nil, false
		}
		withSkip = value
	}
	if limitIndex := topLevelKeywordIndex(body, "LIMIT"); limitIndex >= 0 {
		value, ok := e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(body, "LIMIT"), rows)
		if !ok {
			return nil, false
		}
		withLimit = value
	}
	postWithWhere := ""
	if whereIdx := topLevelKeywordIndex(body, "WHERE"); whereIdx >= 0 {
		postWithWhere = strings.TrimSpace(body[whereIdx+len("WHERE"):])
		body = strings.TrimSpace(body[:whereIdx])
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	withDistinct := false
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		withDistinct = true
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	if strings.TrimSpace(body) == "*" {
		out := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			projected := make(pipelineRow, len(row))
			for name, value := range row {
				projected[name] = value
			}
			out = append(out, projected)
		}
		out = e.filterPipelineRows(ctx, out, postWithWhere)
		if !e.orderPipelineRows(out, orderTerms) {
			return nil, false
		}
		return applyPipelineWindow(out, withSkip, withLimit), true
	}
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return rows, true
	}

	type withProjection struct {
		expr          string
		alias         string
		aggregate     bool
		aggregateName string
		aggregateExpr string
		distinct      bool
	}
	projections := make([]withProjection, 0, len(items))
	projectionAliases := make([]string, 0, len(items))
	hasAggregate := false
	for _, rawItem := range items {
		item := strings.TrimSpace(rawItem)
		if item == "" || item == "{}" {
			continue
		}
		expr, alias := parseProjectionExprAlias(item)
		if expr == "" || alias == "" {
			return nil, false
		}
		projection := withProjection{expr: expr, alias: alias}
		if aggregateName, aggregateExpr, distinct, aggregate := parsePipelineAggregate(expr); aggregate {
			if !strings.Contains(strings.ToUpper(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateName = aggregateName
			projection.aggregateExpr = aggregateExpr
			projection.distinct = distinct
			hasAggregate = true
		} else if pipelineExpressionContainsAggregate(expr) {
			if !strings.Contains(strings.ToUpper(item), " AS ") {
				return nil, false
			}
			projection.aggregate = true
			projection.aggregateExpr = expr
			hasAggregate = true
		}
		projections = append(projections, projection)
		projectionAliases = append(projectionAliases, alias)
	}
	if hasAggregate {
		type aggregateGroup struct {
			first pipelineRow
			rows  []pipelineRow
		}
		groups := make(map[string]*aggregateGroup)
		groupOrder := make([]string, 0)
		for _, row := range rows {
			keyParts := make([]string, 0, len(projections))
			for _, projection := range projections {
				if projection.aggregate {
					continue
				}
				value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, row)
				if !ok {
					return nil, false
				}
				keyParts = append(keyParts, pipelineValueKey(value))
			}
			key := strings.Join(keyParts, "\x1f")
			group, exists := groups[key]
			if !exists {
				group = &aggregateGroup{first: row}
				groups[key] = group
				groupOrder = append(groupOrder, key)
			}
			group.rows = append(group.rows, row)
		}
		if len(rows) == 0 {
			allAggregates := len(projections) > 0
			for _, projection := range projections {
				allAggregates = allAggregates && projection.aggregate
			}
			if allAggregates {
				groups[""] = &aggregateGroup{}
				groupOrder = append(groupOrder, "")
			}
		}

		out := make([]pipelineRow, 0, len(groupOrder))
		orderScopes := make([]pipelineRow, 0, len(groupOrder))
		for _, key := range groupOrder {
			group := groups[key]
			newRow := pipelineRow{}
			projectedExpressions := make(pipelineRow, len(projections))
			for name, value := range group.first {
				if strings.HasPrefix(name, "$") {
					newRow[name] = value
				}
			}
			for _, projection := range projections {
				if !projection.aggregate {
					value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, group.first)
					if !ok {
						return nil, false
					}
					newRow[projection.alias] = value
					projectedExpressions[projection.expr] = value
					continue
				}
				var value interface{}
				var ok bool
				if projection.aggregateName == "" {
					value, ok = e.evaluatePipelineAggregateExpression(group.rows, projection.aggregateExpr)
				} else {
					value, ok = e.evaluatePipelineAggregate(group.rows, projection.aggregateName, projection.aggregateExpr, projection.distinct)
				}
				if !ok {
					return nil, false
				}
				newRow[projection.alias] = value
				projectedExpressions[projection.expr] = value
			}
			orderScope := make(pipelineRow, len(group.first)+len(projectedExpressions)+len(newRow))
			for name, value := range group.first {
				orderScope[name] = value
			}
			for expression, value := range projectedExpressions {
				orderScope[expression] = value
			}
			for name, value := range newRow {
				orderScope[name] = value
			}
			if postWithWhere != "" && !e.evaluateWithWhereCondition(ctx, postWithWhere, orderScope) {
				continue
			}
			out = append(out, newRow)
			orderScopes = append(orderScopes, orderScope)
		}
		if withDistinct {
			out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
		}
		if !e.orderPipelineRowsWithScopes(out, orderScopes, orderTerms) {
			return nil, false
		}
		return applyPipelineWindow(out, withSkip, withLimit), true
	}

	out := make([]pipelineRow, 0, len(rows))
	orderScopes := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		newRow := pipelineRow{}
		for name, value := range row {
			if strings.HasPrefix(name, "$") {
				newRow[name] = value
			}
		}
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

			// All non-binding projections are evaluated by the converged row
			// expression operator. Keep this as the only expression path so
			// nested collection literals and postfix operations are parsed as a
			// whole expression rather than mistaken for specialized shapes.
			if value, projected := e.evaluateRowExpressionWithContext(ctx, expr, row); projected {
				newRow[alias] = value
				continue
			}

			// Anything else — fall back.
			ok = false
			break
		}
		if !ok {
			return nil, false
		}
		if postWithWhere != "" {
			predicateScope := make(map[string]interface{}, len(row)+len(newRow))
			for name, value := range row {
				predicateScope[name] = value
			}
			for name, value := range newRow {
				predicateScope[name] = value
			}
			if !e.evaluateWithWhereCondition(ctx, postWithWhere, predicateScope) {
				continue
			}
		}
		out = append(out, newRow)
		orderScope := make(pipelineRow, len(row)+len(newRow))
		for name, value := range row {
			orderScope[name] = value
		}
		for name, value := range newRow {
			orderScope[name] = value
		}
		orderScopes = append(orderScopes, orderScope)
	}
	if withDistinct {
		out, orderScopes = deduplicatePipelineRowsWithScopes(out, orderScopes, projectionAliases)
	}
	if !e.orderPipelineRowsWithScopes(out, orderScopes, orderTerms) {
		return nil, false
	}
	return applyPipelineWindow(out, withSkip, withLimit), true
}

func pipelinePaginationExpression(body, keyword string) string {
	index := topLevelKeywordIndex(body, keyword)
	if index < 0 {
		return ""
	}
	expression := strings.TrimSpace(body[index+len(keyword):])
	end := len(expression)
	for _, nextKeyword := range []string{"SKIP", "LIMIT"} {
		if nextIndex := topLevelKeywordIndex(expression, nextKeyword); nextIndex >= 0 && nextIndex < end {
			end = nextIndex
		}
	}
	return strings.TrimSpace(expression[:end])
}

func (e *StorageExecutor) evaluatePipelinePagination(ctx context.Context, expression string, rows []pipelineRow) (int, bool) {
	values := make(pipelineRow)
	if len(rows) > 0 {
		values = rows[0]
	}
	value, evaluated := e.evaluateRowExpressionWithContext(ctx, expression, values)
	if !evaluated {
		return 0, false
	}
	integer, valid := cypherIntegerValue(value)
	if !valid || integer < 0 || int64(int(integer)) != integer {
		return 0, false
	}
	return int(integer), true
}

// orderPipelineRows applies every ORDER BY term lexicographically. WITH has
// already materialized its projection at this point, so aliases and retained
// entity properties resolve from the same scope exposed to the next clause.
func (e *StorageExecutor) orderPipelineRows(rows []pipelineRow, terms []orderByTerm) bool {
	return e.orderPipelineRowsWithScopes(rows, rows, terms)
}

func (e *StorageExecutor) orderPipelineRowsWithScopes(rows, scopes []pipelineRow, terms []orderByTerm) bool {
	if len(terms) == 0 || len(rows) < 2 {
		return true
	}
	if len(rows) != len(scopes) {
		return false
	}
	type orderValue struct {
		values []interface{}
		row    pipelineRow
	}
	ordered := make([]orderValue, 0, len(rows))
	for index, row := range rows {
		values := make([]interface{}, len(terms))
		for termIndex, term := range terms {
			value, ok := e.evaluateRowExpression(term.column, scopes[index])
			if !ok {
				return false
			}
			values[termIndex] = value
		}
		ordered = append(ordered, orderValue{values: values, row: row})
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		for index, term := range terms {
			comparison := compareValuesForSort(ordered[left].values[index], ordered[right].values[index])
			if comparison == 0 {
				continue
			}
			if term.descending {
				return comparison > 0
			}
			return comparison < 0
		}
		return false
	})
	for index := range rows {
		rows[index] = ordered[index].row
	}
	return true
}

func applyPipelineWindow(rows []pipelineRow, skip, limit int) []pipelineRow {
	if skip >= len(rows) {
		return []pipelineRow{}
	}
	if skip > 0 {
		rows = rows[skip:]
	}
	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func (e *StorageExecutor) filterPipelineRows(ctx context.Context, rows []pipelineRow, whereClause string) []pipelineRow {
	if whereClause == "" {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		if e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func deduplicatePipelineRows(rows []pipelineRow, columns []string) []pipelineRow {
	unique, _ := deduplicatePipelineRowsWithScopes(rows, rows, columns)
	return unique
}

func deduplicatePipelineRowsWithScopes(rows, scopes []pipelineRow, columns []string) ([]pipelineRow, []pipelineRow) {
	seen := make(map[string]struct{}, len(rows))
	unique := make([]pipelineRow, 0, len(rows))
	uniqueScopes := make([]pipelineRow, 0, len(rows))
	keys := make([]string, len(columns))
	for rowIndex, row := range rows {
		for i, column := range columns {
			keys[i] = pipelineValueKey(row[column])
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
		uniqueScopes = append(uniqueScopes, scopes[rowIndex])
	}
	return unique, uniqueScopes
}

// pipelineApplyUnwind evaluates the list expression (which may be a literal,
// a reference to a bound variable, or a bare property access) and produces
// one row per element.
func (e *StorageExecutor) pipelineApplyUnwind(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
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
		items, ok := e.evaluateListForPipelineWithContext(ctx, listExpr, row)
		if !ok {
			// Couldn't evaluate — fall back.
			return nil, false
		}
		for _, item := range items {
			newRow := make(pipelineRow, util.SafePreallocSum(len(row), 1))
			for k, v := range row {
				newRow[k] = v
			}
			newRow[alias] = item
			out = append(out, newRow)
		}
	}
	return out, true
}

// parsePipelineAggregate recognizes the standard Cypher aggregate functions
// and separates their input expression from an optional DISTINCT modifier.
func parsePipelineAggregate(expr string) (name, inner string, distinct, ok bool) {
	if !isAggregateFunc(expr) {
		return "", "", false, false
	}
	open := strings.Index(expr, "(")
	if open < 0 {
		return "", "", false, false
	}
	name = strings.ToLower(strings.TrimSpace(expr[:open]))
	inner = strings.TrimSpace(extractFuncInner(expr))
	if strings.HasPrefix(strings.ToUpper(inner), "DISTINCT ") {
		distinct = true
		inner = strings.TrimSpace(inner[len("DISTINCT "):])
	}
	if inner == "" {
		return "", "", false, false
	}
	return name, inner, distinct, true
}

func pipelineExpressionContainsAggregate(expr string) bool {
	return len(findAggregateSpans(strings.TrimSpace(expr))) > 0
}

func (e *StorageExecutor) evaluatePipelineAggregateExpression(rows []pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if name, inner, distinct, ok := parsePipelineAggregate(expr); ok {
		return e.evaluatePipelineAggregate(rows, name, inner, distinct)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '{', '}'); enclosed {
		result := make(map[string]interface{})
		if inner == "" {
			return result, true
		}
		for _, pair := range splitTopLevelComma(inner) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				return nil, false
			}
			key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
			value, ok := e.evaluatePipelineAggregateExpression(rows, pair[separator+1:])
			if !ok {
				return nil, false
			}
			result[key] = value
		}
		return result, true
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expr, '[', ']'); enclosed {
		if inner == "" {
			return []interface{}{}, true
		}
		if _, _, _, _, comprehension := parseListComprehension(inner); !comprehension {
			result := make([]interface{}, 0)
			for _, item := range splitTopLevelComma(inner) {
				value, ok := e.evaluatePipelineAggregateExpression(rows, item)
				if !ok {
					return nil, false
				}
				result = append(result, value)
			}
			return result, true
		}
	}
	if spans := findAggregateSpans(expr); len(spans) > 0 {
		values := make(pipelineRow, len(spans))
		// Mixed aggregate expressions are evaluated after isolating aggregate
		// calls, but their non-aggregate terms still resolve against the group's
		// grouping row. Preserve that scope in the shared row evaluator instead
		// of adding operator-specific aggregate paths.
		if len(rows) > 0 {
			values = make(pipelineRow, len(rows[0])+len(spans))
			for name, value := range rows[0] {
				values[name] = value
			}
		}
		var rewritten strings.Builder
		last := 0
		for index, span := range spans {
			name, inner, distinct, ok := parsePipelineAggregate(expr[span.start:span.end])
			if !ok {
				return nil, false
			}
			value, ok := e.evaluatePipelineAggregate(rows, name, inner, distinct)
			if !ok {
				return nil, false
			}
			placeholder := traversalAggPlaceholder(index)
			rewritten.WriteString(expr[last:span.start])
			rewritten.WriteString(placeholder)
			values[placeholder] = value
			last = span.end
		}
		rewritten.WriteString(expr[last:])
		return e.evaluateRowExpression(rewritten.String(), values)
	}
	if len(rows) == 0 {
		return e.evaluateRowExpression(expr, pipelineRow{})
	}
	return e.evaluateRowExpression(expr, rows[0])
}

// evaluatePipelineAggregate applies an aggregate to one logical group. Null
// inputs are ignored by every standard aggregate, including collect().
func (e *StorageExecutor) evaluatePipelineAggregate(rows []pipelineRow, name, expr string, distinct bool) (interface{}, bool) {
	if name == "count" && expr == "*" {
		return int64(len(rows)), true
	}
	values := make([]interface{}, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		value, ok := e.evaluateRowExpression(expr, row)
		if !ok {
			return nil, false
		}
		if value == nil {
			continue
		}
		if distinct {
			key := pipelineValueKey(value)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		values = append(values, value)
	}

	switch name {
	case "count":
		return int64(len(values)), true
	case "collect":
		return values, true
	case "sum":
		var integerTotal int64
		var floatingTotal float64
		hasFloat := false
		for _, value := range values {
			numeric, integer, ok := pipelineAggregateNumber(value)
			if !ok {
				continue
			}
			if integer && !hasFloat {
				integerTotal += int64(numeric)
				continue
			}
			if !hasFloat {
				floatingTotal = float64(integerTotal)
				hasFloat = true
			}
			floatingTotal += numeric
		}
		if hasFloat {
			return floatingTotal, true
		}
		return integerTotal, true
	case "avg":
		var total float64
		var count int
		for _, value := range values {
			numeric, _, ok := pipelineAggregateNumber(value)
			if !ok {
				continue
			}
			total += numeric
			count++
		}
		if count == 0 {
			return nil, true
		}
		return total / float64(count), true
	case "min", "max":
		if len(values) == 0 {
			return nil, true
		}
		selected := values[0]
		for _, value := range values[1:] {
			less := compareForSort(value, selected)
			if (name == "min" && less) || (name == "max" && compareForSort(selected, value)) {
				selected = value
			}
		}
		return selected, true
	case "stdev", "stdevp":
		return stdevTraversalAggregateValues(values, name == "stdevp"), true
	default:
		return nil, false
	}
}

func pipelineAggregateNumber(value interface{}) (float64, bool, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true, true
	case int8:
		return float64(number), true, true
	case int16:
		return float64(number), true, true
	case int32:
		return float64(number), true, true
	case int64:
		return float64(number), true, true
	case uint:
		return float64(number), true, true
	case uint8:
		return float64(number), true, true
	case uint16:
		return float64(number), true, true
	case uint32:
		return float64(number), true, true
	case uint64:
		return float64(number), true, true
	case float32:
		return float64(number), false, true
	case float64:
		return number, false, true
	default:
		return 0, false, false
	}
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
// back to the established RETURN projection.
func (e *StorageExecutor) pipelineApplyReturn(ctx context.Context, rows []pipelineRow, clause string) (*ExecuteResult, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "RETURN"))
	body = strings.TrimPrefix(body, "return")
	modifierStart := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(body, keyword); idx >= 0 && idx < modifierStart {
			modifierStart = idx
		}
	}
	modifiers := strings.TrimSpace(body[modifierStart:])
	body = strings.TrimSpace(body[:modifierStart])
	returnDistinct := false
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		returnDistinct = true
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	if body == "*" {
		columns := pipelineWildcardColumns(rows)
		result := &ExecuteResult{Columns: columns, Rows: make([][]interface{}, 0, len(rows))}
		for _, row := range rows {
			projected := make([]interface{}, len(columns))
			for index, column := range columns {
				projected[index] = row[column]
			}
			result.Rows = append(result.Rows, projected)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(result, modifiers)
		return result, err == nil
	}
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		return nil, false
	}

	type proj struct {
		expr          string
		alias         string
		isAggr        bool
		aggregateName string
		aggregateExpr string
		distinct      bool
	}
	var projs []proj
	hasAggregate := false
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
			alias = normalizeProjectionColumnName(item[asIdx+4:])
		}
		aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregate(expr)
		if !isAggr && pipelineExpressionContainsAggregate(expr) {
			isAggr = true
			aggregateExpr = expr
		}
		if isAggr {
			hasAggregate = true
		}
		projection := proj{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct}
		if isAggr {
			projection.aggregateExpr = aggregateExpr
		}
		projs = append(projs, projection)
	}
	if len(projs) == 0 {
		return nil, false
	}

	result := &ExecuteResult{}
	for _, p := range projs {
		result.Columns = append(result.Columns, p.alias)
	}

	if hasAggregate {
		type returnGroup struct {
			first pipelineRow
			rows  []pipelineRow
		}
		groups := make(map[string]*returnGroup)
		groupOrder := make([]string, 0)
		for _, inputRow := range rows {
			keyParts := make([]string, 0, len(projs))
			for _, projection := range projs {
				if projection.isAggr {
					continue
				}
				value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, inputRow)
				if !ok {
					return nil, false
				}
				keyParts = append(keyParts, pipelineValueKey(value))
			}
			key := strings.Join(keyParts, "\x1f")
			group, exists := groups[key]
			if !exists {
				group = &returnGroup{first: inputRow}
				groups[key] = group
				groupOrder = append(groupOrder, key)
			}
			group.rows = append(group.rows, inputRow)
		}
		if len(rows) == 0 && len(projs) > 0 {
			allAggregates := true
			for _, projection := range projs {
				allAggregates = allAggregates && projection.isAggr
			}
			if allAggregates {
				groups[""] = &returnGroup{}
				groupOrder = append(groupOrder, "")
			}
		}

		for _, key := range groupOrder {
			group := groups[key]
			outRow := make([]interface{}, 0, len(projs))
			for _, projection := range projs {
				if !projection.isAggr {
					value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, group.first)
					if !ok {
						return nil, false
					}
					outRow = append(outRow, value)
					continue
				}
				var value interface{}
				var ok bool
				if projection.aggregateName == "" {
					value, ok = e.evaluatePipelineAggregateExpression(group.rows, projection.aggregateExpr)
				} else {
					value, ok = e.evaluatePipelineAggregate(group.rows, projection.aggregateName, projection.aggregateExpr, projection.distinct)
				}
				if !ok {
					return nil, false
				}
				outRow = append(outRow, value)
			}
			result.Rows = append(result.Rows, outRow)
		}
		if returnDistinct {
			result.Rows = deduplicatePipelineResultRows(result.Rows)
		}
		result, err := e.applyResultModifiers(result, modifiers)
		return result, err == nil
	}

	projectedRows := make([]pipelineRow, 0, len(rows))
	orderScopes := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		projected := make(pipelineRow, len(projs))
		for _, p := range projs {
			val, ok := e.evaluateRowExpressionWithContext(ctx, p.expr, row)
			if !ok {
				return nil, false
			}
			projected[p.alias] = val
		}
		scope := make(pipelineRow, len(row)+len(projected))
		for name, value := range row {
			scope[name] = value
		}
		for name, value := range projected {
			scope[name] = value
		}
		projectedRows = append(projectedRows, projected)
		orderScopes = append(orderScopes, scope)
	}
	if returnDistinct {
		projectedRows, orderScopes = deduplicatePipelineRowsWithScopes(projectedRows, orderScopes, result.Columns)
	}
	if !e.orderPipelineRowsWithScopes(projectedRows, orderScopes, parseOrderByTerms(modifiers)) {
		return nil, false
	}
	skip := 0
	if value, ok := parseIntModifier(modifiers, "SKIP"); ok {
		skip = value
	}
	limit := -1
	if value, ok := parseIntModifier(modifiers, "LIMIT"); ok {
		limit = value
	}
	projectedRows = applyPipelineWindow(projectedRows, skip, limit)
	result.Rows = make([][]interface{}, 0, len(projectedRows))
	for _, projected := range projectedRows {
		outRow := make([]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			outRow[index] = projected[column]
		}
		result.Rows = append(result.Rows, outRow)
	}
	return result, true
}

func deduplicatePipelineResultRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]struct{}, len(rows))
	unique := make([][]interface{}, 0, len(rows))
	keys := make([]string, 0)
	for _, row := range rows {
		if cap(keys) < len(row) {
			keys = make([]string, len(row))
		} else {
			keys = keys[:len(row)]
		}
		for index, value := range row {
			keys[index] = pipelineValueKey(value)
		}
		key := strings.Join(keys, "\x1f")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, row)
	}
	return unique
}

func pipelineWildcardColumns(rows []pipelineRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for column := range row {
			if strings.HasPrefix(column, "$") {
				continue
			}
			seen[column] = struct{}{}
		}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func pipelineScopeColumns(scope map[string]struct{}) []string {
	columns := make([]string, 0, len(scope))
	for column := range scope {
		if !strings.HasPrefix(column, "$") {
			columns = append(columns, column)
		}
	}
	sort.Strings(columns)
	return columns
}

// projectFromRow resolves a RETURN / WITH expression against a single
// binding row. Returns (value, true) on success, (nil, false) otherwise.
func projectFromRow(row pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if val, ok := row[expr]; ok {
		return val, true
	}
	upperExpr := strings.ToUpper(expr)
	if strings.HasPrefix(upperExpr, "SIZE(") && strings.HasSuffix(expr, ")") {
		value, ok := projectFromRow(row, strings.TrimSpace(expr[len("size("):len(expr)-1]))
		if !ok {
			return nil, false
		}
		return int64(len(toAnySlice(value))), true
	}
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if node, isNode := baseVal.(*storage.Node); isNode && node != nil {
				return node.Properties[field], true
			}
			if edge, isEdge := baseVal.(*storage.Edge); isEdge && edge != nil {
				return edge.Properties[field], true
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

func pipelineValueKey(value interface{}) string {
	switch entity := value.(type) {
	case *storage.Node:
		if entity != nil {
			return "node:" + string(entity.ID)
		}
	case *storage.Edge:
		if entity != nil {
			return "edge:" + string(entity.ID)
		}
	}
	return fmt.Sprintf("%T:%#v", value, value)
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
	items, _ := evaluateStaticListForPipeline(expr, row)
	return items
}

func evaluateStaticListForPipeline(expr string, row pipelineRow) ([]interface{}, bool) {
	expr = strings.TrimSpace(expr)
	// Bare variable.
	if val, ok := row[expr]; ok {
		return toAnySlice(val), true
	}
	// Property access (a.b).
	if dot := strings.Index(expr, "."); dot > 0 {
		base := strings.TrimSpace(expr[:dot])
		field := strings.TrimSpace(expr[dot+1:])
		if baseVal, ok := row[base]; ok {
			if asMap, ok := toStringAnyMap(baseVal); ok {
				if v, ok := asMap[field]; ok {
					return toAnySlice(v), true
				}
			}
			if node, ok := baseVal.(*storage.Node); ok && node != nil {
				return toAnySlice(node.Properties[field]), true
			}
		}
	}
	// Literal list — parse via an ad-hoc evaluator. The simplest reliable
	// thing to do is wrap the literal and let the storage executor parse it
	// as a value. Without plumbing a full parser here we only accept the
	// `[...]` form and split top-level items.
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		parsed, ok := parseLiteralValueForPipeline(expr)
		if !ok {
			return nil, false
		}
		return toAnySlice(parsed), true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateListForPipelineWithContext(ctx context.Context, expr string, row pipelineRow) ([]interface{}, bool) {
	if inner, wrapped := stripEnclosingExpressionParentheses(strings.TrimSpace(expr)); wrapped {
		expr = inner
	}
	if items, ok := evaluateStaticListForPipeline(expr, row); ok {
		return items, true
	}
	if matchFuncStartAndSuffix(expr, "range") {
		args := e.splitFunctionArgs(extractFuncArgs(expr, "range"))
		if len(args) < 2 || len(args) > 3 {
			return nil, false
		}
		arguments := make([]interface{}, len(args))
		for index, argument := range args {
			value, resolved := e.evaluateRowExpression(strings.TrimSpace(argument), row)
			if !resolved {
				return nil, false
			}
			arguments[index] = value
		}
		items, err := evaluateCypherRange(arguments)
		return items, err == nil
	}
	if value, ok := e.evaluateRowExpression(expr, row); ok {
		return toAnySlice(value), true
	}

	materialized := expr
	for name, value := range row {
		materialized = replaceIdentifierOutsideQuotes(materialized, name, e.valueToLiteral(value))
	}
	value := e.evaluateExpressionWithContext(ctx, materialized, nil, nil)
	if text, unresolved := value.(string); unresolved && text == materialized && !isWholeCypherQuotedString(materialized) {
		return nil, false
	}
	if value == nil && !strings.EqualFold(strings.TrimSpace(materialized), "null") && !looksLikeFunctionCall(materialized) {
		return nil, false
	}
	return toAnySlice(value), true
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
	case []string:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []int:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = int64(s[i])
		}
		return out
	case []int64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []float64:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	case []float32:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = float64(s[i])
		}
		return out
	case []bool:
		out := make([]interface{}, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil
	}
	out := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
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
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			return nil
		}
		k := normalizePropertyKey(strings.TrimSpace(pair[:colon]))
		vRaw := strings.TrimSpace(pair[colon+1:])
		v, ok := parseLiteralValueForPipeline(vRaw)
		if !ok {
			return nil
		}
		out[k] = v
	}
	return out
}

func parseLiteralListForPipeline(s string) ([]interface{}, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return nil, false
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return []interface{}{}, true
	}
	parts := splitTopLevelComma(inner)
	out := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		v, ok := parseLiteralValueForPipeline(part)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

func parseLiteralValueForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		m := parseLiteralMapForPipeline(s)
		if m == nil {
			return nil, false
		}
		return m, true
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return parseLiteralListForPipeline(s)
	}
	return parseLiteralScalarForPipeline(s)
}

func parseLiteralScalarForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	// Quoted string.
	if isWholeCypherQuotedString(s) {
		return decodeCypherQuotedString(s)
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
	negative := false
	digits := 0
	if s[0] == '-' {
		negative = true
		digits = 1
	} else if s[0] == '+' {
		digits = 1
	}
	if digits == len(s) {
		return 0, false
	}
	base := 10
	if digits+2 <= len(s) && s[digits] == '0' {
		switch s[digits+1] {
		case 'x', 'X':
			base = 16
			digits += 2
		case 'o', 'O':
			base = 8
			digits += 2
		}
	}
	if base == 10 {
		value, err := strconv.ParseInt(s, 10, 64)
		return value, err == nil
	}
	if digits == len(s) {
		return 0, false
	}
	magnitude, err := strconv.ParseUint(s[digits:], base, 64)
	if err != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
		return 0, false
	}
	if !negative {
		return int64(magnitude), true
	}
	if magnitude == uint64(math.MaxInt64)+1 {
		return math.MinInt64, true
	}
	return -int64(magnitude), true
}

func parseFloatFast(s string) (float64, bool) {
	if !strings.ContainsAny(s, ".eE") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	// Cypher canonicalizes every floating-point zero to positive zero.
	if f == 0 {
		return 0, true
	}
	return f, true
}
