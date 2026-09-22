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
	"reflect"
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

// pipelineRow carries bindings across clauses. Values may be *storage.Node,
// *storage.Edge, or scalars (for WITH projections and UNWIND variables).
type pipelineRow map[string]interface{}

// canExecuteAsPipeline returns true when the query is decomposable into the
// clause kinds this executor understands. Any unsupported clause (FOREACH,
// CALL subquery, etc.) causes a false return so the
// caller can select a specialized physical plan.
func canExecuteAsPipeline(cypher string) ([]pipelineClause, bool) {
	if optionalIdx := findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH"); optionalIdx >= 0 {
		if !startsWithKeywordFold(strings.TrimSpace(cypher), "OPTIONAL MATCH") {
			withIdx := findKeywordIndex(cypher, "WITH")
			if withIdx < 0 || findMultiWordKeywordIndex(cypher[withIdx+len("WITH"):], "OPTIONAL", "MATCH") < 0 {
				return nil, false
			}
		}
	}
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
	hasWithOrUnwind := false
	hasRemove := false
	hasDelete := false
	hasSet := false
	hasMerge := false
	mutationClauseCount := 0
	for _, clause := range clauses {
		if clause.kind == pipelineClauseWith || clause.kind == pipelineClauseUnwind {
			hasWithOrUnwind = true
		}
		if clause.kind == pipelineClauseRemove {
			hasRemove = true
		}
		if clause.kind == pipelineClauseDelete {
			hasDelete = true
		}
		if clause.kind == pipelineClauseSet {
			hasSet = true
		}
		if clause.kind == pipelineClauseMerge {
			hasMerge = true
		}
		switch clause.kind {
		case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseDelete, pipelineClauseSet, pipelineClauseRemove:
			mutationClauseCount++
		}
	}
	stringPredicateMutation := strings.Contains(upper, " CREATE ") &&
		(strings.Contains(upper, " STARTS WITH ") || strings.Contains(upper, " ENDS WITH "))
	startsWithCreateProjection := clauses[0].kind == pipelineClauseCreate &&
		clauses[len(clauses)-1].kind == pipelineClauseReturn &&
		firstTopLevelModifierIndex(strings.TrimSpace(clauses[len(clauses)-1].text[len("RETURN"):])) >= 0
	if !hasWithOrUnwind && !hasRemove && !hasDelete && !hasSet && !hasMerge && mutationClauseCount < 2 && !stringPredicateMutation && !startsWithCreateProjection {
		return nil, false
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
	scope := make(map[string]struct{})

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
			newRows, ok := e.pipelineApplyWith(ctx, rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			scope = pipelineProjectionScope(scope, clause.text)
		case pipelineClauseUnwind:
			newRows, ok := e.pipelineApplyUnwind(ctx, rows, clause.text)
			if !ok {
				return nil, false, nil
			}
			rows = newRows
			if alias := pipelineUnwindAlias(clause.text); alias != "" {
				scope[alias] = struct{}{}
			}
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
	if len(clauses) > 0 && clauses[len(clauses)-1].kind == pipelineClauseSet {
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{len(rows)}}
	}

	return result, true, nil
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
	if expanded, ok, err := e.pipelineApplyBoundTraversalMatch(ctx, rows, clause); ok || err != nil {
		return expanded, ok, err
	}

	// If the MATCH has scalar references to already-bound variables (e.g.
	// `MATCH (p:Product {productID: prodRef.productID})`), substitute them
	// per-row and re-seed referenced node variables by ID before invoking the
	// normal MATCH executor with a synthetic RETURN of the clause bindings.
	var out []pipelineRow
	store := e.getStorage(ctx)
	for _, row := range rows {
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
//   - property → alias:    `WITH a.name AS n`     projects node property
//   - literal → alias:     `WITH 42 AS x`         binds scalar literal
//   - aggregate pass-thru: `WITH count(*) AS c`   counts current rows
//
// Anything else returns ok=false so the caller can fall back.
func (e *StorageExecutor) pipelineApplyWith(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(clause, "WITH"))
	body = strings.TrimPrefix(body, "with")
	postWithWhere := ""
	if whereIdx := findKeywordIndexInContext(body, "WHERE"); whereIdx >= 0 {
		postWithWhere = strings.TrimSpace(body[whereIdx+len("WHERE"):])
		body = strings.TrimSpace(body[:whereIdx])
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
		return e.filterPipelineRows(ctx, out, postWithWhere), true
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
				value, ok := e.evaluateRowExpression(projection.expr, row)
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
		for _, key := range groupOrder {
			group := groups[key]
			newRow := pipelineRow{}
			for _, projection := range projections {
				if !projection.aggregate {
					value, ok := e.evaluateRowExpression(projection.expr, group.first)
					if !ok {
						return nil, false
					}
					newRow[projection.alias] = value
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
			}
			out = append(out, newRow)
		}
		if withDistinct {
			out = deduplicatePipelineRows(out, projectionAliases)
		}
		return e.filterPipelineRows(ctx, out, postWithWhere), true
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

			// 2. List subscript (values[0]).
			if value, matched, ok := pipelineListSubscript(row, expr); matched {
				if !ok {
					return nil, false
				}
				newRow[alias] = value
				continue
			}

			if value, projected := e.evaluateRowExpression(expr, row); projected {
				newRow[alias] = value
				continue
			}

			// 3. Property access (var.prop).
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

			// 4. Literal scalar (number, quoted string, bool, null). Covers
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
	}
	if withDistinct {
		out = deduplicatePipelineRows(out, projectionAliases)
	}
	return out, true
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
	seen := make(map[string]struct{}, len(rows))
	unique := make([]pipelineRow, 0, len(rows))
	keys := make([]string, len(columns))
	for _, row := range rows {
		for i, column := range columns {
			keys[i] = pipelineValueKey(row[column])
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
	expr = strings.TrimSpace(expr)
	if _, _, _, ok := parsePipelineAggregate(expr); ok {
		return true
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		for _, pair := range splitTopLevelComma(strings.TrimSpace(expr[1 : len(expr)-1])) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator > 0 && pipelineExpressionContainsAggregate(pair[separator+1:]) {
				return true
			}
		}
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		for _, item := range splitTopLevelComma(strings.TrimSpace(expr[1 : len(expr)-1])) {
			if pipelineExpressionContainsAggregate(item) {
				return true
			}
		}
	}
	return false
}

func (e *StorageExecutor) evaluatePipelineAggregateExpression(rows []pipelineRow, expr string) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if name, inner, distinct, ok := parsePipelineAggregate(expr); ok {
		return e.evaluatePipelineAggregate(rows, name, inner, distinct)
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
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
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}, true
		}
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
		total := interface{}(int64(0))
		for _, value := range values {
			total = e.add(total, value)
			if total == nil {
				return nil, false
			}
		}
		return total, true
	case "avg":
		if len(values) == 0 {
			return nil, true
		}
		var total float64
		for _, value := range values {
			numeric, ok := toFloat64(value)
			if !ok {
				return nil, false
			}
			total += numeric
		}
		return total / float64(len(values)), true
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
	default:
		return nil, false
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
func (e *StorageExecutor) pipelineApplyReturn(rows []pipelineRow, clause string) (*ExecuteResult, bool) {
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
	items := splitTopLevelComma(body)
	if len(items) == 0 {
		result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(len(rows))}}}
		result, err := e.applyResultModifiers(result, modifiers)
		return result, err == nil
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
		if isAggr {
			hasAggregate = true
		}
		projection := proj{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct}
		if isAggr {
			projection.aggregateExpr = aggregateExpr
		}
		projs = append(projs, projection)
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
				value, ok := e.evaluateRowExpression(projection.expr, inputRow)
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
					value, ok := e.evaluateRowExpression(projection.expr, group.first)
					if !ok {
						return nil, false
					}
					outRow = append(outRow, value)
					continue
				}
				value, ok := e.evaluatePipelineAggregate(group.rows, projection.aggregateName, projection.aggregateExpr, projection.distinct)
				if !ok {
					return nil, false
				}
				outRow = append(outRow, value)
			}
			result.Rows = append(result.Rows, outRow)
		}
		result, err := e.applyResultModifiers(result, modifiers)
		return result, err == nil
	}

	for _, row := range rows {
		outRow := make([]interface{}, 0, len(projs))
		for _, p := range projs {
			val, ok := e.evaluateRowExpression(p.expr, row)
			if !ok {
				return nil, false
			}
			outRow = append(outRow, val)
		}
		result.Rows = append(result.Rows, outRow)
	}
	result, err := e.applyResultModifiers(result, modifiers)
	return result, err == nil
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

func pipelineListSubscript(row pipelineRow, expr string) (interface{}, bool, bool) {
	open := strings.LastIndex(expr, "[")
	if open <= 0 || !strings.HasSuffix(expr, "]") {
		return nil, false, false
	}
	base := strings.TrimSpace(expr[:open])
	indexText := strings.TrimSpace(expr[open+1 : len(expr)-1])
	index, err := strconv.Atoi(indexText)
	if err != nil || index < 0 {
		return nil, true, false
	}
	value, exists := row[base]
	if !exists {
		return nil, true, false
	}
	items := toAnySlice(value)
	if index >= len(items) {
		return nil, true, true
	}
	return items[index], true, true
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
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
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
