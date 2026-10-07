package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const deleteStreamingBatchSize = 500

// ========================================
// WITH Clause
// ========================================

// executeMatch handles MATCH queries.
func (e *StorageExecutor) parseMergePattern(ctx context.Context, pattern string) (string, []string, map[string]interface{}, error) {
	return e.parseMergeNodePattern(ctx, pattern, nil, nil)
}

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.

// tryExecuteBoundStandaloneDelete executes DELETE / DETACH DELETE <var> where
// <var> resolves through the value scope (FOREACH over entity lists, §6.2
// bound child contexts). It reports ok=false when the target is not a single
// bound variable, so the caller keeps its historical match-required error.
// Non-DETACH deletion of a connected node goes through the same residual-
// relationship guard as the MATCH path.
func (e *StorageExecutor) tryExecuteBoundStandaloneDelete(ctx context.Context, target string, detach bool) (*ExecuteResult, bool, error) {
	target = strings.TrimSpace(target)
	if target == "" || !isValidIdentifier(target) {
		return nil, false, nil
	}
	value, bound := e.boundValue(ctx, target)
	if !bound {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	deleteEntity := func(entityID string, isNode bool) error {
		if isNode {
			if !detach {
				if err := validateNoResidualRelationships(store, []storage.NodeID{storage.NodeID(entityID)}, nil); err != nil {
					return err
				}
			}
			edgesCount := 0
			if detach {
				outgoing, _ := store.GetOutgoingEdges(storage.NodeID(entityID))
				incoming, _ := store.GetIncomingEdges(storage.NodeID(entityID))
				edgesCount = len(outgoing) + len(incoming)
			}
			if err := store.DeleteNode(storage.NodeID(entityID)); err != nil {
				return localizedError(localization.CypherMutationsDeleteFailed(err), err)
			}
			result.Stats.NodesDeleted++
			result.Stats.RelationshipsDeleted += edgesCount
			e.removeNodeFromSearch(entityID)
			return nil
		}
		if err := store.DeleteEdge(storage.EdgeID(entityID)); err != nil {
			return localizedError(localization.CypherMutationsDeleteFailed(err), err)
		}
		result.Stats.RelationshipsDeleted++
		return nil
	}

	switch entity := value.(type) {
	case *storage.Node:
		if entity == nil {
			return result, true, nil
		}
		if err := deleteEntity(string(entity.ID), true); err != nil {
			return nil, true, err
		}
		return result, true, nil
	case *storage.Edge:
		if entity == nil {
			return result, true, nil
		}
		if err := deleteEntity(string(entity.ID), false); err != nil {
			return nil, true, err
		}
		return result, true, nil
	case string:
		if _, err := store.GetNode(storage.NodeID(entity)); err == nil {
			if err := deleteEntity(entity, true); err != nil {
				return nil, true, err
			}
			return result, true, nil
		}
		if _, err := store.GetEdge(storage.EdgeID(entity)); err == nil {
			if err := deleteEntity(entity, false); err != nil {
				return nil, true, err
			}
			return result, true, nil
		}
		return nil, true, localizedError(localization.CypherMutationsDeleteFailed(storage.ErrNotFound), storage.ErrNotFound)
	default:
		// Bound non-entity values are not deletable targets.
		return nil, true, localizedError(localization.CypherMutationsDeleteFailed(storage.ErrInvalidData), storage.ErrInvalidData)
	}
}

// tryExecuteDeleteWithWithLimitHotPath executes:
//
//	MATCH ... [WHERE ...]
//	WITH <var> LIMIT <n>
//	DETACH DELETE <var>
//	[RETURN ...]
//
// with a streamlined path that avoids generic WITH parsing in delete execution.
func (e *StorageExecutor) tryExecuteDeleteWithWithLimitHotPath(ctx context.Context, cypher string, matchIdx, deleteIdx int, deleteVars string, detach bool, needEdgeStats bool) (*ExecuteResult, bool, error) {
	if !detach || matchIdx < 0 || deleteIdx <= matchIdx {
		return nil, false, nil
	}
	if strings.Contains(deleteVars, ",") {
		return nil, false, nil
	}
	deleteVar := strings.TrimSpace(deleteVars)
	if deleteVar == "" {
		return nil, false, nil
	}

	matchSegment := strings.TrimSpace(cypher[matchIdx:deleteIdx])
	withIdx := findKeywordIndex(matchSegment, "WITH")
	if withIdx <= 0 {
		return nil, false, nil
	}

	// Keep this hot path strict and deterministic.
	for _, blocked := range []string{"ORDER BY", "SKIP", "UNWIND", "CALL", "OPTIONAL MATCH"} {
		if containsKeywordOutsideStrings(matchSegment, blocked) {
			return nil, false, nil
		}
	}

	baseMatch := strings.TrimSpace(matchSegment[:withIdx])
	withPart := strings.TrimSpace(matchSegment[withIdx+4:]) // skip "WITH"
	limitIdx := findKeywordIndex(withPart, "LIMIT")
	if limitIdx <= 0 {
		return nil, false, nil
	}

	withVar := strings.TrimSpace(withPart[:limitIdx])
	if withVar != deleteVar {
		return nil, false, nil
	}
	limitLiteral := strings.TrimSpace(withPart[limitIdx+5:]) // skip "LIMIT"
	if limitLiteral == "" {
		return nil, false, nil
	}
	limitFields := strings.Fields(limitLiteral)
	if len(limitFields) == 0 {
		return nil, false, nil
	}
	limitN, err := strconv.Atoi(limitFields[0])
	if err != nil || limitN <= 0 {
		return nil, false, nil
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	candidates, ok, err := e.collectDeleteWithLimitCandidates(ctx, baseMatch, deleteVar, limitN, getParamsFromContext(ctx))
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}

	deletedNodeIDs := make(map[string]struct{}, len(candidates))
	for _, node := range candidates {
		if node == nil {
			continue
		}
		nodeID := string(node.ID)
		if _, seen := deletedNodeIDs[nodeID]; seen {
			continue
		}
		edgesCount := 0
		if needEdgeStats {
			outgoingEdges, _ := store.GetOutgoingEdges(storage.NodeID(nodeID))
			incomingEdges, _ := store.GetIncomingEdges(storage.NodeID(nodeID))
			edgesCount = len(outgoingEdges) + len(incomingEdges)
		}
		if err := store.DeleteNode(storage.NodeID(nodeID)); err == nil {
			result.Stats.NodesDeleted++
			result.Stats.RelationshipsDeleted += edgesCount
			deletedNodeIDs[nodeID] = struct{}{}
			e.removeNodeFromSearch(nodeID)
		}
	}

	input := &ExecuteResult{Columns: []string{deleteVar}, Rows: make([][]interface{}, 0, len(candidates))}
	for _, node := range candidates {
		input.Rows = append(input.Rows, []interface{}{node})
	}
	e.applyDeleteReturnProjection(result, cypher, deleteVar, deleteProjectionInfo{ctx: ctx, input: input})
	return result, true, getExpressionFailure(ctx)
}

func (e *StorageExecutor) collectDeleteWithLimitCandidates(ctx context.Context, baseMatch, deleteVar string, limitN int, params map[string]interface{}) ([]*storage.Node, bool, error) {
	matchIdx := findKeywordIndex(baseMatch, "MATCH")
	if matchIdx < 0 {
		return nil, false, nil
	}
	whereIdx := findKeywordIndex(baseMatch, "WHERE")
	patternPart := strings.TrimSpace(baseMatch[matchIdx+5:])
	wherePart := ""
	if whereIdx > 0 {
		patternPart = strings.TrimSpace(baseMatch[matchIdx+5 : whereIdx])
		wherePart = strings.TrimSpace(baseMatch[whereIdx+5:])
	}
	nodePat := e.parseNodePattern(ctx, patternPart)
	if strings.TrimSpace(nodePat.variable) != deleteVar {
		return nil, false, nil
	}

	var nodes []*storage.Node
	var err error
	usedIndex := false
	if wherePart != "" {
		if candidates, used, idxErr := e.tryCollectNodesFromIDIn(ctx, nodePat, wherePart, params); idxErr == nil && used {
			nodes = candidates
			usedIndex = true
		}
		if !usedIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexIn(wherePartNodePattern(nodePat, deleteVar), wherePart, params); idxErr == nil && used {
				nodes = candidates
				usedIndex = true
			}
		}
		if !usedIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexInLiteral(ctx, wherePartNodePattern(nodePat, deleteVar), wherePart); idxErr == nil && used {
				nodes = candidates
				usedIndex = true
			}
		}
		if !usedIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndex(ctx, wherePartNodePattern(nodePat, deleteVar), wherePart); idxErr == nil && used {
				nodes = candidates
				usedIndex = true
			}
		}
	}
	if !usedIndex {
		if len(nodePat.labels) > 0 {
			nodes, err = e.storage.GetNodesByLabel(nodePat.labels[0])
		} else {
			nodes, err = e.storage.AllNodes()
		}
		if err != nil {
			return nil, true, err
		}
	}
	if len(nodePat.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePat.properties)
	}

	if wherePart != "" {
		// Supported hot-path predicates:
		//   var.prop = $param | 'literal'
		//   var.prop IN $param
		if m := deleteWherePropertyEquals.FindStringSubmatch(wherePart); len(m) == 4 && strings.EqualFold(m[1], deleteVar) {
			prop := m[2]
			rhs := strings.TrimSpace(m[3])
			var expected interface{}
			if strings.HasPrefix(rhs, "$") {
				key := strings.TrimSpace(strings.TrimPrefix(rhs, "$"))
				val, ok := params[key]
				if !ok {
					return []*storage.Node{}, true, nil
				}
				expected = val
			} else {
				expected = strings.Trim(rhs, "'\"")
			}
			filtered := make([]*storage.Node, 0, len(nodes))
			for _, n := range nodes {
				if n == nil {
					continue
				}
				if e.compareEqual(n.Properties[prop], expected) {
					filtered = append(filtered, n)
				}
			}
			nodes = filtered
		} else if m := deleteWherePropertyInParameter.FindStringSubmatch(wherePart); len(m) == 4 && strings.EqualFold(m[1], deleteVar) {
			prop := m[2]
			paramName := m[3]
			raw, ok := params[paramName]
			if !ok {
				return []*storage.Node{}, true, nil
			}
			allowed := map[string]struct{}{}
			switch v := raw.(type) {
			case []interface{}:
				for _, item := range v {
					allowed[fmt.Sprintf("%v", item)] = struct{}{}
				}
			case []string:
				for _, item := range v {
					allowed[item] = struct{}{}
				}
			default:
				return nil, false, nil
			}
			filtered := make([]*storage.Node, 0, len(nodes))
			for _, n := range nodes {
				if n == nil {
					continue
				}
				if _, ok := allowed[fmt.Sprintf("%v", n.Properties[prop])]; ok {
					filtered = append(filtered, n)
				}
			}
			nodes = filtered
		} else {
			return nil, false, nil
		}
	}

	if limitN < len(nodes) {
		nodes = nodes[:limitN]
	}
	return nodes, true, nil
}

func wherePartNodePattern(nodePat nodePatternInfo, variable string) nodePatternInfo {
	if strings.TrimSpace(nodePat.variable) != "" {
		return nodePat
	}
	nodePat.variable = variable
	return nodePat
}

type deleteProjectionKind uint8

const (
	deleteProjectionUnknown deleteProjectionKind = iota
	deleteProjectionNode
	deleteProjectionRelationship
)

type deleteProjectionInfo struct {
	ctx   context.Context
	input *ExecuteResult
}

type deleteTargetValue struct {
	kind   deleteProjectionKind
	nodeID storage.NodeID
	edgeID storage.EdgeID
}

func singleDeleteProjectionInfo(_ string, _ deleteProjectionKind) deleteProjectionInfo {
	return deleteProjectionInfo{}
}

func classifyDeleteTargetValue(val interface{}) deleteTargetValue {
	switch v := val.(type) {
	case map[string]interface{}:
		if id, ok := v["_edgeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionRelationship, edgeID: storage.EdgeID(id)}
		}
		if id, ok := v["_nodeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: storage.NodeID(id)}
		}
	case *storage.Node:
		if v != nil {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: v.ID}
		}
	case *storage.Edge:
		if v != nil {
			return deleteTargetValue{kind: deleteProjectionRelationship, edgeID: v.ID}
		}
	case string:
		if v != "" {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: storage.NodeID(v)}
		}
	}
	return deleteTargetValue{}
}

func (e *StorageExecutor) applyDeleteReturnProjection(result *ExecuteResult, cypher, deleteVars string, info deleteProjectionInfo) {
	if result == nil {
		return
	}
	returnIdx := topLevelKeywordIndex(cypher, "RETURN")
	if returnIdx <= 0 {
		return
	}
	ctx := info.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	rows := []pipelineRow{}
	nodeIDs := []storage.NodeID{}
	edgeIDs := make(map[storage.EdgeID]struct{})
	if info.input != nil {
		store := e.getStorage(ctx)
		for _, values := range info.input.Rows {
			row := pipelineRow(buildRowValueMap(info.input.Columns, values))
			rows = append(rows, row)
			for _, value := range row {
				switch entity := value.(type) {
				case *storage.Node:
					if entity != nil {
						if _, err := store.GetNode(entity.ID); errors.Is(err, storage.ErrNotFound) {
							nodeIDs = append(nodeIDs, entity.ID)
						}
					}
				case *storage.Edge:
					if entity != nil {
						if _, err := store.GetEdge(entity.ID); errors.Is(err, storage.ErrNotFound) {
							edgeIDs[entity.ID] = struct{}{}
						}
					}
				}
			}
			for _, target := range splitTopLevelComma(deleteVars) {
				value, ok := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(target), row)
				if !ok {
					pipelineItemUnevaluable(ctx, target)
					return
				}
				entity := classifyDeleteTargetValue(value)
				if entity.kind == deleteProjectionNode {
					nodeIDs = append(nodeIDs, entity.nodeID)
				} else if entity.kind == deleteProjectionRelationship {
					edgeIDs[entity.edgeID] = struct{}{}
				}
			}
		}
	}
	markPipelineRowsDeletedEntities(rows, nodeIDs, edgeIDs)
	if err := validateDeletedEntityProjection(rows, cypher[returnIdx:]); err != nil {
		recordExpressionFailure(ctx, err)
		return
	}
	projected, err := e.projectMergeReturn(ctx, rows, cypher[returnIdx:])
	if err != nil {
		return
	}
	result.Columns = projected.Columns
	result.Rows = projected.Rows
}

func (e *StorageExecutor) isDeleteStreamingEligible(matchSegment, deleteVars string, detach bool) bool {
	if !detach {
		return false
	}
	if strings.TrimSpace(matchSegment) == "" {
		return false
	}
	// Keep streaming path conservative to avoid semantic drift.
	for _, blocked := range []string{"WITH", "LIMIT", "SKIP", "ORDER", "CALL", "UNWIND"} {
		if containsKeywordOutsideStrings(matchSegment, blocked) {
			return false
		}
	}
	// Keep variable parsing simple and deterministic for now.
	if strings.Contains(deleteVars, ",") {
		return false
	}
	deleteVars = strings.TrimSpace(deleteVars)
	if deleteVars == "" {
		return false
	}
	// Basic identifier check
	for i, r := range deleteVars {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func (e *StorageExecutor) normalizeSetMatchRowsToNodes(matchResult *ExecuteResult, store storage.Engine) {
	if matchResult == nil {
		return
	}
	for rowIdx := range matchResult.Rows {
		row := matchResult.Rows[rowIdx]
		for colIdx, val := range row {
			m, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			rawID, ok := m["_nodeId"]
			if !ok {
				continue
			}
			nodeID, ok := rawID.(string)
			if !ok || nodeID == "" {
				continue
			}
			node, err := store.GetNode(storage.NodeID(nodeID))
			if err != nil || node == nil {
				continue
			}
			row[colIdx] = node
		}
	}
}

func (e *StorageExecutor) normalizeSetMatchRowsToEdges(matchResult *ExecuteResult, store storage.Engine) {
	if matchResult == nil {
		return
	}
	for rowIdx := range matchResult.Rows {
		row := matchResult.Rows[rowIdx]
		for colIdx, val := range row {
			m, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			rawID, ok := m["_edgeId"]
			if !ok {
				continue
			}
			edgeID, ok := rawID.(string)
			if !ok || edgeID == "" {
				continue
			}
			edge, err := store.GetEdge(storage.EdgeID(edgeID))
			if err != nil || edge == nil {
				continue
			}
			row[colIdx] = edge
		}
	}
}

var setScopeVarPattern = regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:\.|\+=|=)`)

// extractScopeVariablesFromSetAndReturn returns variable names referenced in
// SET assignments and RETURN items so the pre-SET MATCH query includes all
// variables needed for mutation and projection.
func extractScopeVariablesFromSetAndReturn(setPart, returnPart string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 4)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if !isValidIdentifier(v) {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	for _, m := range setScopeVarPattern.FindAllStringSubmatch(setPart, -1) {
		if len(m) > 1 {
			add(m[1])
		}
	}

	if strings.TrimSpace(returnPart) != "" {
		for _, raw := range splitTopLevelCommaKeepEmpty(returnPart) {
			expr := strings.TrimSpace(raw)
			if expr == "" {
				continue
			}
			if asIdx := findKeywordIndex(expr, "AS"); asIdx > 0 {
				expr = strings.TrimSpace(expr[:asIdx])
			}
			add(extractVariableNameFromReturnItem(expr))
		}
	}
	return out
}

// collapseChainedSetClauses rewrites chained SET keywords into comma-separated assignments.
// Example: "n += $props SET n.x = 1 SET n.y = 2" -> "n += $props, n.x = 1, n.y = 2".
// Counting what a SET writes needs the clause boundaries, so the SET
// applicators walk the clauses with nextChainedSetClause instead.
func collapseChainedSetClauses(setPart string) string {
	setPart = strings.TrimSpace(setPart)
	first, next, ok := nextChainedSetClause(setPart, 0)
	if !ok {
		return setPart
	}
	clause, next, ok := nextChainedSetClause(setPart, next)
	if !ok {
		return first
	}
	clauses := []string{first}
	for ; ok; clause, next, ok = nextChainedSetClause(setPart, next) {
		clauses = append(clauses, clause)
	}
	return strings.Join(clauses, ", ")
}

// nextChainedSetClause returns the next non-empty clause at or after start in
// setPart, a SET assignment list that may chain further SET clauses
// ("n += $props SET n.x = 1"), and the index where the clause after it
// starts. ok is false when no clause is left.
func nextChainedSetClause(setPart string, start int) (clause string, next int, ok bool) {
	for start < len(setPart) {
		end, after := len(setPart), len(setPart)
		if idx := keywordIndexFrom(setPart, "SET", start, defaultKeywordScanOpts()); idx >= 0 {
			end, after = idx, idx+len("SET")
		}
		if clause = strings.TrimSpace(setPart[start:end]); clause != "" {
			return clause, after, true
		}
		start = after
	}
	return "", len(setPart), false
}

// firstPostSetClauseIndex returns the first index of a clause keyword that can
// legally follow a SET assignment list. Returns -1 when none are present.
func firstPostSetClauseIndex(setTail string) int {
	opts := defaultKeywordScanOpts()
	first := -1
	for _, kw := range []string{
		"REMOVE", "UNWIND", "WITH", "RETURN",
		"CREATE", "MATCH", "MERGE", "DELETE", "CALL",
		"ORDER BY", "LIMIT", "SKIP",
	} {
		if idx := keywordIndexFrom(setTail, kw, 0, opts); idx >= 0 {
			if first == -1 || idx < first {
				first = idx
			}
		}
	}
	return first
}

func (e *StorageExecutor) resolveUnwindValueFromExpr(ctx context.Context, unwindExpr string, nodeVars map[string]*storage.Node) interface{} {
	expr := normalizeUnwindExpression(unwindExpr)
	if strings.HasPrefix(expr, "$") {
		paramName := strings.TrimSpace(expr[1:])
		if paramName != "" {
			if params := getParamsFromContext(ctx); params != nil {
				if v, ok := params[paramName]; ok {
					return v
				}
			}
		}
	}
	return e.evaluateExpressionWithContext(ctx, expr, nodeVars, nil)
}

// normalizeUnwindExpression removes syntactic wrapper parentheses around a valid
// UNWIND expression, e.g. "($vals)" -> "$vals", while preserving inner
// expression content for evaluation.
func normalizeUnwindExpression(expr string) string {
	trimmed := strings.TrimSpace(expr)
	for hasOuterParens(trimmed) {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	return trimmed
}

func hasOuterParens(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 && i < len(s)-1 {
					return false
				}
			}
		}
	}
	return depth == 0 && !inSingle && !inDouble
}

// coerceToUnwindItems is the one UNWIND coercion, as in Neo4j (#693): null
// unwinds to no rows, a list to its elements (cypherListValue), and any other
// value to one row holding it.
func coerceToUnwindItems(value interface{}) []interface{} {
	if value == nil {
		return nil
	}
	if items, isList := cypherListValue(value); isList {
		return items
	}
	return []interface{}{value}
}

func extractWithAliases(querySegment string) []string {
	matches := withAliasPattern.FindAllStringSubmatch(querySegment, -1)
	aliases := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) > 1 {
			aliases = append(aliases, m[1])
		}
	}
	return aliases
}

func dedupeNonEmpty(groups ...[]string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, group := range groups {
		for _, item := range group {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	return out
}

func normalizePropsMap(value interface{}, source string) (map[string]interface{}, error) {
	propsMap, ok := value.(map[string]interface{})
	if ok {
		for k, v := range propsMap {
			propsMap[k] = normalizePropValue(v)
		}
		return propsMap, nil
	}
	if genericMap, ok := value.(map[interface{}]interface{}); ok {
		propsMap = make(map[string]interface{}, len(genericMap))
		for k, v := range genericMap {
			keyStr, ok := k.(string)
			if !ok {
				return nil, localizedError(localization.CypherMutationsSetMergeStringKeysRequired(source, fmt.Sprintf("%T", k)), nil)
			}
			propsMap[keyStr] = normalizePropValue(v)
		}
		return propsMap, nil
	}
	return nil, localizedError(localization.CypherMutationsSetMergeMapRequired(source, fmt.Sprintf("%T", value)), nil)
}

func normalizePropValue(value interface{}) interface{} {
	switch v := value.(type) {
	case time.Time:
		return CypherDateTime{Time: v}
	case *time.Time:
		if v == nil {
			return nil
		}
		return CypherDateTime{Time: *v}
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > math.MaxInt64 {
			return float64(v)
		}
		return int64(v)
	case float32:
		return float64(v)
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = normalizePropValue(item)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, item := range v {
			out[k] = normalizePropValue(item)
		}
		return out
	default:
		return value
	}
}

// parseRemoveItems parses "n.prop1, n:LabelA:LabelB, m.prop3" into
// property names and label names.
func (e *StorageExecutor) parseRemoveItems(removePart string) ([]string, []string) {
	var props []string
	var labels []string
	parts := strings.Split(removePart, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if dotIdx := strings.Index(part, "."); dotIdx >= 0 {
			propName := strings.TrimSpace(part[dotIdx+1:])
			if propName != "" {
				props = append(props, propName)
			}
			continue
		}
		if _, chain, hasLabels := splitNodeHead(part); hasLabels {
			labels = append(labels, labelChainNames(chain)...)
		}
	}
	return props, labels
}

// parseRemoveProperties is kept for test and call-site compatibility.
func (e *StorageExecutor) parseRemoveProperties(removePart string) []string {
	props, _ := e.parseRemoveItems(removePart)
	return props
}

func removeNodeLabels(existing []string, labelsToRemove []string) ([]string, int64) {
	if len(existing) == 0 || len(labelsToRemove) == 0 {
		return existing, 0
	}
	removeSet := make(map[string]struct{}, len(labelsToRemove))
	for _, label := range labelsToRemove {
		removeSet[label] = struct{}{}
	}
	next := make([]string, 0, len(existing))
	var removed int64
	for _, label := range existing {
		if _, ok := removeSet[label]; ok {
			removed++
			continue
		}
		next = append(next, label)
	}
	return next, removed
}

func (e *StorageExecutor) applyRemoveToMatchedRows(
	store storage.Engine,
	matchResult *ExecuteResult,
	removePart string,
	result *ExecuteResult,
) error {
	removeTargets := parseRemoveTargetBindings(removePart)
	for _, row := range matchResult.Rows {
		for colIdx, val := range row {
			if colIdx >= len(matchResult.Columns) {
				continue
			}
			varName := matchResult.Columns[colIdx]
			propTargets := removeTargets.propertyNames(varName)
			labelTargets := removeTargets.labelNames(varName)
			if len(propTargets) == 0 && len(labelTargets) == 0 {
				continue
			}
			switch entity := val.(type) {
			case *storage.Node:
				if entity == nil {
					continue
				}
				invalidated := false
				for _, prop := range propTargets {
					if _, exists := entity.Properties[prop]; exists {
						delete(entity.Properties, prop)
						result.Stats.PropertiesSet++
						if !embeddingutil.IsMetadataPropertyKey(prop) {
							invalidated = true
						}
					}
				}
				if len(labelTargets) > 0 {
					oldLabels := make([]string, len(entity.Labels))
					copy(oldLabels, entity.Labels)
					next, removed := removeNodeLabels(entity.Labels, labelTargets)
					if removed > 0 {
						entity.Labels = next
						if err := validatePolicyOnLabelChange(store, entity, oldLabels); err != nil {
							entity.Labels = oldLabels
							return err
						}
						result.Stats.LabelsRemoved += int(removed)
					}
				}
				if invalidated {
					embeddingutil.InvalidateManagedEmbeddings(entity)
				}
				if err := store.UpdateNode(entity); err != nil {
					return err
				}
				e.notifyNodeMutated(string(entity.ID))
			case *storage.Edge:
				if entity == nil {
					continue
				}
				for _, prop := range propTargets {
					if _, exists := entity.Properties[prop]; exists {
						delete(entity.Properties, prop)
						result.Stats.PropertiesSet++
					}
				}
				if err := store.UpdateEdge(entity); err != nil {
					return err
				}
				e.notifyEdgeMutated(string(entity.ID))
			}
		}
	}
	return nil
}

// executeCall handles CALL procedure queries.

// smartSplitReturnItems adapts callers to canonical top-level comma splitting.
func (e *StorageExecutor) smartSplitReturnItems(returnPart string) []string {
	return splitTopLevelComma(returnPart)
}

// isAlphaNum checks if a character is alphanumeric or underscore
func isAlphaNum(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func (e *StorageExecutor) parseReturnItems(returnPart string) []returnItem {
	if strings.TrimSpace(returnPart) == "" {
		returnPart = "*"
	}
	plan := returnProjectionPlanFor("RETURN " + returnPart)
	if plan.star {
		items := []returnItem{{expr: "*"}}
		for _, item := range plan.starItems {
			expr, alias := parseProjectionExprAlias(item)
			items = append(items, returnItem{expr: expr, alias: alias})
		}
		return items
	}
	items := make([]returnItem, 0, len(plan.projections))
	for _, projection := range plan.projections {
		items = append(items, returnItem{expr: projection.expr, alias: projection.alias})
	}
	return items
}

func normalizeProjectionColumnName(raw string) string {
	return symbolicNameValue(strings.TrimSpace(raw))
}

func (e *StorageExecutor) generateID() string {
	// Use UUID for globally unique IDs
	// This prevents ID collisions across server restarts which caused
	// the race condition where CREATE would cancel pending DELETEs
	return uuid.New().String()
}

// Deprecated: Sequential counter replaced with UUID generation
var idCounter int64

func (e *StorageExecutor) idCounter() int64 {
	// Keep for backward compatibility but not used in generateID anymore
	atomic.AddInt64(&idCounter, 1)
	return atomic.LoadInt64(&idCounter)
}

// extractSubquery extracts the MATCH pattern from EXISTS { MATCH ... } or NOT EXISTS { MATCH ... }
func (e *StorageExecutor) extractSubquery(whereClause, prefix string) string {
	upperClause := upperASCII(whereClause)
	prefixUpper := upperASCII(prefix)

	// Find the prefix position
	prefixIdx := strings.Index(upperClause, prefixUpper)
	if prefixIdx < 0 {
		return ""
	}

	// Find the opening brace
	rest := whereClause[prefixIdx+len(prefix):]
	braceStart := strings.Index(rest, "{")
	if braceStart < 0 {
		return ""
	}

	// Find matching closing brace
	depth := 0
	for i := braceStart; i < len(rest); i++ {
		if rest[i] == '{' {
			depth++
		} else if rest[i] == '}' {
			depth--
			if depth == 0 {
				return strings.TrimSpace(rest[braceStart+1 : i])
			}
		}
	}

	return ""
}

// evaluateCollectSubquery evaluates a COLLECT { … } projection item for one
// node bound to variable, through the shared subquery evaluator
// (rowSubqueryValue), so the body's ORDER BY and SKIP / LIMIT apply.
func (e *StorageExecutor) evaluateCollectSubquery(ctx context.Context, node *storage.Node, variable, subquery string) ([]interface{}, error) {
	collect, ok := standaloneSubqueryExpression(subquery)
	if !ok || collect.kind != "COLLECT" {
		return nil, localizedError(localization.CypherResidualCollectSubquerySyntaxInvalid(), nil)
	}
	if topLevelKeywordIndex(collect.body, "RETURN") < 0 {
		return nil, localizedError(localization.CypherResidualCollectSubqueryReturnRequired(), nil)
	}
	// A COLLECT body is always evaluated (rowSubqueryValue); only its error
	// can stop it.
	value, _, err := e.rowSubqueryValue(ctx, collect.kind, collect.body, map[string]interface{}{variable: node})
	if err != nil {
		return nil, localizedError(localization.CypherResidualCollectSubqueryExecutionFailed(err), err)
	}
	collected, _ := value.([]interface{})
	return collected, nil
}

// evaluateRelationshipPatternInWhere evaluates a WHERE clause relationship pattern
// like "(n)-[:SUPERSEDED_BY]->()" and returns true if the node has a matching edge.
// Used when NOT (n)-[:TYPE]->() is evaluated after stripping outer parens to "n)-[:TYPE]->()".
func (e *StorageExecutor) evaluateRelationshipPatternInWhere(node *storage.Node, variable, pattern string) bool {
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return false
	}
	relationshipCount := strings.Count(pattern, "-[")
	if relationshipCount > 1 {
		return e.checkChainedPattern(node, variable, pattern, "")
	}
	_ = e.extractTargetVariable(pattern, variable) // not needed for simple (var)-[]->() pattern
	relPattern := e.parseOptionalRelPattern(context.Background(), pattern)
	targetMatches := func(targetNodeID storage.NodeID) bool {
		if len(relPattern.targetLabels) == 0 && len(relPattern.targetProps) == 0 {
			return true
		}
		targetNode, err := e.storage.GetNode(targetNodeID)
		if err != nil || targetNode == nil {
			return false
		}
		if len(relPattern.targetLabels) > 0 && !mergeNodeHasLabels(targetNode, relPattern.targetLabels) {
			return false
		}
		for key, expected := range relPattern.targetProps {
			actual, ok := targetNode.Properties[key]
			if !ok || !e.compareEqual(actual, expected) {
				return false
			}
		}
		return true
	}
	var checkIncoming, checkOutgoing bool
	var relTypes []string
	checkIncoming, checkOutgoing, relTypes = e.relationshipExistencePatternDirections(pattern, variable)
	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if (len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes)) && targetMatches(edge.StartNode) {
				return true
			}
		}
	}
	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if (len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes)) && targetMatches(edge.EndNode) {
				return true
			}
		}
	}
	if !checkIncoming && !checkOutgoing {
		incoming, _ := e.storage.GetIncomingEdges(node.ID)
		outgoing, _ := e.storage.GetOutgoingEdges(node.ID)
		return len(incoming) > 0 || len(outgoing) > 0
	}
	return false
}

// checkChainedPattern handles chained relationship patterns like (p)-[:KNOWS]->()-[:KNOWS]->()
func (e *StorageExecutor) checkChainedPattern(node *storage.Node, variable, pattern, innerWhere string) bool {
	// Parse the pattern to extract relationship hops
	// E.g., (p)-[:KNOWS]->()-[:KNOWS]->() has two hops

	// Find the first relationship part
	// Pattern looks like: (variable)-[rel1]->(intermediate)-[rel2]->...

	// Find the start of the first relationship (after the variable node)
	varPattern := "(" + variable + ")"
	if !strings.Contains(pattern, varPattern) {
		// Try with label: (variable:Label)
		idx := strings.Index(pattern, "("+variable+":")
		if idx < 0 {
			return false
		}
	}

	// Extract relationship hops
	hops := e.parseRelationshipHops(pattern, variable)
	if len(hops) == 0 {
		return false
	}

	// Traverse the chain starting from the given node
	return e.traverseChain(node, hops, 0)
}

// relationshipHop represents one step in a chained relationship pattern
type relationshipHop struct {
	relTypes []string
	outgoing bool
}

// parseRelationshipHops extracts relationship hops from a pattern
func (e *StorageExecutor) parseRelationshipHops(pattern, variable string) []relationshipHop {
	var hops []relationshipHop

	// Find all relationship patterns: -[...]->  or  <-[...]-
	remaining := pattern

	for len(remaining) > 0 {
		// Look for outgoing: -[...]->(
		outIdx := strings.Index(remaining, "-[")
		inIdx := strings.Index(remaining, "<-[")

		if outIdx >= 0 && (inIdx < 0 || outIdx < inIdx) {
			// Found outgoing pattern
			relStart := outIdx + 2
			relEnd := strings.Index(remaining[relStart:], "]")
			if relEnd < 0 {
				break
			}
			relEnd += relStart

			relPart := remaining[relStart:relEnd]
			// Extract relationship types
			var relTypes []string
			if strings.HasPrefix(relPart, ":") {
				typePart := relPart[1:]
				// Handle multiple types separated by |
				for _, t := range strings.Split(typePart, "|") {
					if t = strings.TrimSpace(t); t != "" {
						relTypes = append(relTypes, t)
					}
				}
			}

			hops = append(hops, relationshipHop{
				relTypes: relTypes,
				outgoing: true,
			})

			remaining = remaining[relEnd+1:]
		} else if inIdx >= 0 {
			// Found incoming pattern
			relStart := inIdx + 3
			relEnd := strings.Index(remaining[relStart:], "]")
			if relEnd < 0 {
				break
			}
			relEnd += relStart

			relPart := remaining[relStart:relEnd]
			// Extract relationship types
			var relTypes []string
			if strings.HasPrefix(relPart, ":") {
				typePart := relPart[1:]
				for _, t := range strings.Split(typePart, "|") {
					if t = strings.TrimSpace(t); t != "" {
						relTypes = append(relTypes, t)
					}
				}
			}

			hops = append(hops, relationshipHop{
				relTypes: relTypes,
				outgoing: false,
			})

			remaining = remaining[relEnd+1:]
		} else {
			break
		}
	}

	return hops
}

// traverseChain recursively checks if a chain of relationships exists
func (e *StorageExecutor) traverseChain(node *storage.Node, hops []relationshipHop, hopIndex int) bool {
	if hopIndex >= len(hops) {
		return true // All hops matched
	}

	hop := hops[hopIndex]

	if hop.outgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(hop.relTypes) == 0 || e.edgeTypeMatches(edge.Type, hop.relTypes) {
				// Get the target node and recurse
				nextNode, err := e.storage.GetNode(edge.EndNode)
				if err != nil {
					continue
				}
				if e.traverseChain(nextNode, hops, hopIndex+1) {
					return true
				}
			}
		}
	} else {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(hop.relTypes) == 0 || e.edgeTypeMatches(edge.Type, hop.relTypes) {
				// Get the source node and recurse
				nextNode, err := e.storage.GetNode(edge.StartNode)
				if err != nil {
					continue
				}
				if e.traverseChain(nextNode, hops, hopIndex+1) {
					return true
				}
			}
		}
	}

	return false
}

// extractVariableNameFromReturnItem extracts the variable name from a return item expression.
// Examples:
//   - "n" -> "n"
//   - "n.name" -> "n"
//   - "id(n)" -> "n"
//   - "n.age + 1" -> "n"
func extractVariableNameFromReturnItem(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}

	// Handle function calls like id(n), elementId(n), etc.
	if strings.Contains(expr, "(") {
		// Extract variable from function call: id(n) -> n
		openParen := strings.Index(expr, "(")
		closeParen := strings.LastIndex(expr, ")")
		if openParen > 0 && closeParen > openParen {
			inner := strings.TrimSpace(expr[openParen+1 : closeParen])
			// If inner contains a dot, it's property access: id(n.name) -> n
			if dotIdx := strings.Index(inner, "."); dotIdx > 0 {
				return strings.TrimSpace(inner[:dotIdx])
			}
			return inner
		}
	}

	// Handle property access: n.name -> n
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		return strings.TrimSpace(parts[0])
	}

	// Simple variable name
	return expr
}

// extractTargetVariable extracts the target variable name from a relationship pattern
// e.g., from "(m)-[:MANAGES]->(report)" it extracts "report"
func (e *StorageExecutor) extractTargetVariable(pattern, sourceVar string) string {
	// Look for outgoing pattern: (source)-[...]->(target)
	if arrowIdx := strings.Index(pattern, "]->"); arrowIdx >= 0 {
		rest := pattern[arrowIdx+3:]
		if parenIdx := strings.Index(rest, "("); parenIdx >= 0 {
			rest = rest[parenIdx+1:]
			// Extract variable name (before : or ))
			endIdx := strings.IndexAny(rest, ":)")
			if endIdx > 0 {
				return strings.TrimSpace(rest[:endIdx])
			}
		}
	}

	// Look for incoming pattern: (target)<-[...]-(source)
	if arrowIdx := strings.Index(pattern, "<-["); arrowIdx >= 0 {
		// Target is before the arrow
		before := pattern[:arrowIdx]
		if parenIdx := strings.LastIndex(before, "("); parenIdx >= 0 {
			inner := before[parenIdx+1:]
			endIdx := strings.IndexAny(inner, ":)")
			if endIdx > 0 {
				return strings.TrimSpace(inner[:endIdx])
			}
		}
	}

	return ""
}

// extractRelTypesFromPattern extracts relationship types from a pattern
func (e *StorageExecutor) extractRelTypesFromPattern(pattern, prefix string) []string {
	var types []string

	idx := strings.Index(pattern, prefix)
	if idx < 0 {
		return types
	}

	rest := pattern[idx+len(prefix):]
	endIdx := strings.Index(rest, "]")
	if endIdx < 0 {
		return types
	}

	relPart := rest[:endIdx]

	// Extract type after colon
	if colonIdx := strings.Index(relPart, ":"); colonIdx >= 0 {
		typePart := relPart[colonIdx+1:]
		// Handle multiple types (TYPE1|TYPE2)
		for _, t := range strings.Split(typePart, "|") {
			t = strings.TrimSpace(t)
			if t != "" {
				types = append(types, t)
			}
		}
	}

	return types
}

func (e *StorageExecutor) relationshipExistencePatternDirections(pattern, variable string) (incoming, outgoing bool, relTypes []string) {
	if groupStart := strings.Index(pattern, "("+variable+")"); groupStart >= 0 || strings.Contains(pattern, "("+variable+":") {
		if groupStart < 0 {
			groupStart = strings.Index(pattern, "("+variable+":")
		}
		groupEnd := groupStart + len(variable) + 2
		if strings.HasPrefix(pattern[groupStart:], "("+variable+":") {
			closeRel := strings.Index(pattern[groupStart:], ")")
			if closeRel >= 0 {
				groupEnd = groupStart + closeRel + 1
			}
		}
		before := pattern[:groupStart]
		after := pattern[groupEnd:]

		switch {
		case strings.HasPrefix(after, "<-["):
			incoming = true
			relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
		case strings.HasPrefix(after, "-["):
			relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			if strings.Contains(after, "]->") {
				outgoing = true
			} else if strings.Contains(after, "]-") {
				incoming = true
				outgoing = true
			}
		}

		switch {
		case strings.HasSuffix(before, "]->"):
			incoming = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			}
		case strings.Contains(before, "<-[") && strings.HasSuffix(before, "]-"):
			outgoing = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
			}
		case strings.HasSuffix(before, "]-"):
			incoming = true
			outgoing = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			}
		}
	}

	if !incoming && !outgoing {
		if in, out, ok := bareRelDirection(pattern, variable); ok {
			return in, out, nil
		}
	}
	return incoming, outgoing, relTypes
}

// edgeTypeMatches checks if an edge type matches any of the allowed types
func (e *StorageExecutor) edgeTypeMatches(edgeType string, allowedTypes []string) bool {
	for _, t := range allowedTypes {
		if edgeType == t {
			return true
		}
	}
	return false
}

// validatePolicyOnLabelChange checks RELATIONSHIP_POLICY constraints when a node's labels
// change. It validates all adjacent edges (outgoing and incoming) against the current
// policy constraints to ensure no DISALLOWED pair is formed and any ALLOWED whitelist
// is still satisfied.
func validatePolicyOnLabelChange(store storage.Engine, node *storage.Node, oldLabels []string) error {
	schema := store.GetSchema()
	if schema == nil {
		return nil
	}

	// Collect all policy constraints.
	constraints := schema.GetAllConstraints()
	var policies []storage.Constraint
	for _, c := range constraints {
		if c.Type == storage.ConstraintPolicy {
			policies = append(policies, c)
		}
	}
	if len(policies) == 0 {
		return nil
	}

	// Check outgoing edges (node is the source).
	outgoing, _ := store.GetOutgoingEdges(node.ID)
	for _, edge := range outgoing {
		targetNode, err := store.GetNode(storage.NodeID(edge.EndNode))
		if err != nil || targetNode == nil {
			continue
		}
		if err := checkPolicyForEdge(edge.Type, node.Labels, targetNode.Labels, policies); err != nil {
			return err
		}
	}

	// Check incoming edges (node is the target).
	incoming, _ := store.GetIncomingEdges(node.ID)
	for _, edge := range incoming {
		sourceNode, err := store.GetNode(storage.NodeID(edge.StartNode))
		if err != nil || sourceNode == nil {
			continue
		}
		if err := checkPolicyForEdge(edge.Type, sourceNode.Labels, node.Labels, policies); err != nil {
			return err
		}
	}

	return nil
}

// checkPolicyForEdge validates a single edge against all policy constraints for its type.
// DISALLOWED policies are checked first (they take precedence).
func checkPolicyForEdge(edgeType string, sourceLabels, targetLabels []string, policies []storage.Constraint) error {
	// Gather policies for this edge type.
	var relevantAllowed []storage.Constraint
	for _, p := range policies {
		if p.Label != edgeType {
			continue
		}
		if p.PolicyMode == "DISALLOWED" {
			if sliceContains(sourceLabels, p.SourceLabel) && sliceContains(targetLabels, p.TargetLabel) {
				return localizedError(localization.CypherResidualPolicyDisallowed(p.Name, p.SourceLabel, edgeType, p.TargetLabel), nil)
			}
		} else if p.PolicyMode == "ALLOWED" {
			relevantAllowed = append(relevantAllowed, p)
		}
	}

	// If ALLOWED policies exist for this edge type, at least one must match.
	if len(relevantAllowed) > 0 {
		matched := false
		for _, p := range relevantAllowed {
			if sliceContains(sourceLabels, p.SourceLabel) && sliceContains(targetLabels, p.TargetLabel) {
				matched = true
				break
			}
		}
		if !matched {
			return localizedError(localization.CypherResidualPolicyAllowedRequired(edgeType), nil)
		}
	}

	return nil
}

func sliceContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Patterns compiled once (#591): these helpers run per statement or per row.
var (
	// deleteWherePropertyEquals is the DELETE hot path's
	// "variable.property = value" predicate (the variable is compared by the
	// caller).
	deleteWherePropertyEquals = regexp.MustCompile(`(?i)^\s*([^.\s]+)\.(\w+)\s*=\s*(.+?)\s*$`)
	// deleteWherePropertyInParameter is "variable.property IN $param".
	deleteWherePropertyInParameter = regexp.MustCompile(`(?i)^\s*([^.\s]+)\.(\w+)\s+IN\s+\$(\w+)\s*$`)
	withAliasPattern               = regexp.MustCompile(`(?i)\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b`)
	subqueryWherePattern           = regexp.MustCompile(`(?i)\s+WHERE\s+`)
)
