package cypher

import (
	"context"
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
	"github.com/orneryd/nornicdb/pkg/util"
)

const deleteStreamingBatchSize = 500

// ========================================
// WITH Clause
// ========================================

// executeMatch handles MATCH queries.
func (e *StorageExecutor) parseMergePattern(ctx context.Context, pattern string) (string, []string, map[string]interface{}, error) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", nil, nil, localizedError(localization.CypherResidualMergePatternInvalid(pattern), nil)
	}
	info := e.parseNodePattern(ctx, pattern)
	if info.labelErr != nil {
		return "", nil, nil, info.labelErr
	}
	return info.variable, info.labels, info.properties, nil
}

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.

// executeDelete handles DELETE queries.
func (e *StorageExecutor) executeDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Parse: MATCH (n) WHERE ... DELETE n or DETACH DELETE n
	upper := strings.ToUpper(cypher)
	detach := strings.Contains(upper, "DETACH")

	// Get MATCH part - use word boundary detection
	matchIdx := findKeywordIndex(cypher, "MATCH")

	// Find the delete clause - could be "DELETE" or "DETACH DELETE"
	// IMPORTANT: Search for "DETACH DELETE" first (longer string) to avoid matching just "DETACH"
	var deleteIdx int
	if detach {
		// Try "DETACH DELETE" first (longer, more specific)
		deleteIdx = findKeywordIndex(cypher, "DETACH DELETE")
		if deleteIdx == -1 {
			// Fallback to just "DETACH" if "DETACH DELETE" not found
			deleteIdx = findKeywordIndex(cypher, "DETACH")
		}
	} else {
		deleteIdx = findKeywordIndex(cypher, "DELETE")
	}

	if matchIdx == -1 || deleteIdx == -1 {
		// Standalone DELETE <var> with the variable bound through the value
		// scope (§6.2 bound child contexts, e.g. FOREACH over an entity list).
		if matchIdx == -1 && deleteIdx >= 0 {
			target := strings.TrimSpace(cypher[deleteIdx:])
			upperTarget := strings.ToUpper(target)
			switch {
			case strings.HasPrefix(upperTarget, "DETACH DELETE "):
				target = target[14:] // len("DETACH DELETE ")
			case strings.HasPrefix(upperTarget, "DELETE "):
				target = target[7:] // len("DELETE ")
			}
			if ret := findKeywordIndex(target, "RETURN"); ret > 0 {
				target = strings.TrimSpace(target[:ret])
			}
			if boundResult, ok, err := e.tryExecuteBoundStandaloneDelete(ctx, target, detach); ok || err != nil {
				return boundResult, err
			}
		}
		return nil, localizedError(localization.CypherMutationsDeleteMatchRequired(), nil)
	}

	// Parse the delete target variable(s) - e.g., "DELETE n" or "DETACH DELETE n"
	// Preserve original case of variable names
	deleteClause := strings.TrimSpace(cypher[deleteIdx:])
	upperDeleteClause := strings.ToUpper(deleteClause)

	// Handle DETACH DELETE - must check for "DETACH DELETE " first (longer string)
	if detach {
		if strings.HasPrefix(upperDeleteClause, "DETACH DELETE ") {
			// Found "DETACH DELETE " - remove it to get variable name
			deleteClause = deleteClause[14:] // len("DETACH DELETE ")
		} else if strings.HasPrefix(upperDeleteClause, "DETACH ") {
			// Found just "DETACH " - this shouldn't happen if we found "DETACH DELETE" above
			// but handle it for safety
			deleteClause = deleteClause[7:] // len("DETACH ")
		}
	}

	// After handling DETACH, check for remaining "DELETE " prefix
	upperDeleteClause = strings.ToUpper(deleteClause)
	if strings.HasPrefix(upperDeleteClause, "DELETE ") {
		deleteClause = deleteClause[7:] // len("DELETE ")
	}

	// Strip RETURN clause from deleteVars if present
	returnInDelete := findKeywordIndex(deleteClause, "RETURN")
	if returnInDelete > 0 {
		deleteClause = strings.TrimSpace(deleteClause[:returnInDelete])
	}
	deleteVars := strings.TrimSpace(deleteClause)

	if deleteVars == "" {
		return nil, localizedError(localization.CypherMutationsDeleteVariablesRequired(), nil)
	}

	// For DETACH DELETE, ensure deleteIdx points to "DETACH DELETE", not bare "DETACH".
	if detach && deleteIdx > 0 {
		checkSubstring := strings.ToUpper(strings.TrimSpace(cypher[deleteIdx:]))
		if strings.HasPrefix(checkSubstring, "DETACH ") && !strings.HasPrefix(checkSubstring, "DETACH DELETE ") {
			return nil, localizedError(localization.CypherMutationsDetachDeleteKeywordsRequired(), nil)
		}
	}

	returnIdx := topLevelKeywordIndex(cypher, "RETURN")
	needEdgeStats := returnIdx > 0 || detach // always track for DETACH so stats are correct

	// Streaming batched delete hot path for large DETACH DELETE scans.
	// This preserves semantics by only engaging on simple shapes without
	// LIMIT/SKIP/WITH/ORDER BY/CALL/UNWIND in the MATCH segment.
	matchSegment := strings.TrimSpace(cypher[matchIdx:deleteIdx])
	_, inTransactionWrapper := e.getStorage(ctx).(*transactionStorageWrapper)
	if hot, ok, err := e.tryExecuteBoundRelationshipDelete(ctx, matchSegment, cypher, deleteVars, detach); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return hot, nil
	}
	if !inTransactionWrapper && e.isDeleteStreamingEligible(matchSegment, deleteVars, detach) {
		result, err := e.executeDeleteStreaming(ctx, matchSegment, deleteVars, needEdgeStats)
		if err != nil {
			return nil, err
		}
		e.applyDeleteReturnProjection(result, cypher, deleteVars, singleDeleteProjectionInfo(deleteVars, deleteProjectionNode))
		return result, nil
	}

	// Execute the match first - return the specific variables being deleted
	// Can't use RETURN * because it returns literal "*" instead of expanding
	if hot, ok, err := e.tryExecuteDeleteWithWithLimitHotPath(ctx, cypher, matchIdx, deleteIdx, deleteVars, detach, needEdgeStats); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return hot, nil
	}

	matchQuery := cypher[matchIdx:deleteIdx] + " RETURN " + deleteVars
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}
	// MATCH ... RETURN can surface nodes as maps (e.g. via nodeToMap), so normalize
	// to live nodes before delete processing.
	e.normalizeSetMatchRowsToNodes(matchResult, store)

	// Delete matched nodes and/or relationships.
	//
	// This runs as collect -> validate -> apply (see
	// executor_mutations_delete_guard.go) so a non-DETACH DELETE is judged
	// as a whole statement before anything is mutated: a multi-row DELETE
	// must not partially apply before a later row is found to still have
	// relationships, and a plain "DELETE n" on a connected node must error
	// instead of silently cascading its edges away.
	nodeIDs, edgeIDsToDelete := collectDeleteMutationTargets(matchResult)

	if !detach {
		if err := validateNoResidualRelationships(store, nodeIDs, edgeIDsToDelete); err != nil {
			return nil, err
		}
	}

	for _, nodeID := range nodeIDs {
		if detach {
			// Count edges that will be deleted with the node (for stats).
			// Combine outgoing + incoming in a single stats tally.
			edgesCount := 0
			if needEdgeStats {
				outgoingEdges, _ := store.GetOutgoingEdges(nodeID)
				incomingEdges, _ := store.GetIncomingEdges(nodeID)
				edgesCount = len(outgoingEdges) + len(incomingEdges)
			}

			if err := store.DeleteNode(nodeID); err == nil {
				result.Stats.NodesDeleted++
				result.Stats.RelationshipsDeleted += edgesCount
				e.removeNodeFromSearch(string(nodeID))
			}
		} else {
			if err := store.DeleteNode(nodeID); err == nil {
				result.Stats.NodesDeleted++
				e.removeNodeFromSearch(string(nodeID))
			}
		}
	}

	if len(edgeIDsToDelete) > 0 {
		if err := store.BulkDeleteEdges(edgeIDsToDelete); err != nil {
			return nil, err
		}
		result.Stats.RelationshipsDeleted += len(edgeIDsToDelete)
	}

	e.applyDeleteReturnProjection(result, cypher, deleteVars, inferDeleteProjectionInfo(matchResult.Rows, deleteVars))

	return result, nil
}

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

	e.applyDeleteReturnProjection(result, cypher, deleteVar, singleDeleteProjectionInfo(deleteVar, deleteProjectionNode))
	return result, true, nil
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
		if candidates, used, idxErr := e.tryCollectNodesFromIDInParam(nodePat, wherePart, params); idxErr == nil && used {
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
		eqRE := regexp.MustCompile(`(?i)^\s*` + regexp.QuoteMeta(deleteVar) + `\.(\w+)\s*=\s*(.+?)\s*$`)
		inRE := regexp.MustCompile(`(?i)^\s*` + regexp.QuoteMeta(deleteVar) + `\.(\w+)\s+IN\s+\$(\w+)\s*$`)

		if m := eqRE.FindStringSubmatch(wherePart); len(m) == 3 {
			prop := m[1]
			rhs := strings.TrimSpace(m[2])
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
		} else if m := inRE.FindStringSubmatch(wherePart); len(m) == 3 {
			prop := m[1]
			paramName := m[2]
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
	kinds map[string]deleteProjectionKind
}

type deleteTargetValue struct {
	kind   deleteProjectionKind
	nodeID storage.NodeID
	edgeID storage.EdgeID
}

func singleDeleteProjectionInfo(deleteVar string, kind deleteProjectionKind) deleteProjectionInfo {
	deleteVar = strings.TrimSpace(deleteVar)
	if deleteVar == "" {
		return deleteProjectionInfo{}
	}
	return deleteProjectionInfo{kindMap(deleteVar, kind)}
}

func kindMap(deleteVar string, kind deleteProjectionKind) map[string]deleteProjectionKind {
	return map[string]deleteProjectionKind{deleteVar: kind}
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

func inferDeleteProjectionInfo(rows [][]interface{}, deleteVars string) deleteProjectionInfo {
	vars := strings.Split(deleteVars, ",")
	if len(vars) == 0 {
		return deleteProjectionInfo{}
	}
	for i := range vars {
		vars[i] = strings.TrimSpace(vars[i])
	}
	info := deleteProjectionInfo{kinds: make(map[string]deleteProjectionKind, len(vars))}
	for _, row := range rows {
		for idx, val := range row {
			if idx >= len(vars) {
				break
			}
			name := vars[idx]
			if name == "" || info.kindFor(name) != deleteProjectionUnknown {
				continue
			}
			target := classifyDeleteTargetValue(val)
			if target.kind != deleteProjectionUnknown {
				info.kinds[name] = target.kind
			}
		}
	}
	return info
}

func (i deleteProjectionInfo) kindFor(name string) deleteProjectionKind {
	if i.kinds == nil {
		return deleteProjectionUnknown
	}
	return i.kinds[strings.TrimSpace(name)]
}

func deletedCountForProjection(result *ExecuteResult, inner string, info deleteProjectionInfo) int64 {
	if strings.EqualFold(strings.TrimSpace(inner), "*") {
		return int64(result.Stats.NodesDeleted + result.Stats.RelationshipsDeleted)
	}
	switch info.kindFor(inner) {
	case deleteProjectionNode:
		return int64(result.Stats.NodesDeleted)
	case deleteProjectionRelationship:
		return int64(result.Stats.RelationshipsDeleted)
	}
	if result.Stats.NodesDeleted > 0 && result.Stats.RelationshipsDeleted == 0 {
		return int64(result.Stats.NodesDeleted)
	}
	if result.Stats.RelationshipsDeleted > 0 && result.Stats.NodesDeleted == 0 {
		return int64(result.Stats.RelationshipsDeleted)
	}
	return int64(result.Stats.NodesDeleted)
}

func (e *StorageExecutor) applyDeleteReturnProjection(result *ExecuteResult, cypher, deleteVars string, info deleteProjectionInfo) {
	if result == nil {
		return
	}
	returnIdx := topLevelKeywordIndex(cypher, "RETURN")
	if returnIdx <= 0 {
		return
	}
	returnPart := strings.TrimSpace(cypher[returnIdx+6:])
	returnItems := e.parseReturnItems(returnPart)
	result.Columns = make([]string, len(returnItems))
	row := make([]interface{}, len(returnItems))

	// Build a set of deleted variable names for nil-resolution below.
	deletedVarSet := make(map[string]struct{})
	for _, v := range strings.Split(deleteVars, ",") {
		deletedVarSet[strings.TrimSpace(v)] = struct{}{}
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
		upperExpr := strings.ToUpper(item.expr)

		// COUNT() aggregation over deleted nodes/relationships.
		if strings.HasPrefix(upperExpr, "COUNT(") {
			inner := strings.TrimSpace(item.expr[6 : len(item.expr)-1])
			row[i] = deletedCountForProjection(result, inner, info)
			continue
		}

		// Property access on a deleted variable (e.g. s.title after DELETE s)
		// yields nil — the node no longer exists.
		if dotIdx := strings.Index(item.expr, "."); dotIdx > 0 {
			varName := item.expr[:dotIdx]
			if _, deleted := deletedVarSet[varName]; deleted {
				row[i] = nil
				continue
			}
		}
		// Bare reference to a deleted variable yields nil.
		if _, deleted := deletedVarSet[item.expr]; deleted {
			row[i] = nil
			continue
		}

		// Evaluate non-deleted expressions: string literals, numeric
		// literals, function calls, etc.
		row[i] = e.parseValue(context.Background(), item.expr)
	}
	result.Rows = [][]interface{}{row}
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

func (e *StorageExecutor) executeDeleteStreaming(ctx context.Context, matchSegment, deleteVars string, needEdgeStats bool) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	limitLiteral := strconv.Itoa(deleteStreamingBatchSize)
	usedFallback := false

	for {
		matchQuery := matchSegment + " RETURN " + deleteVars + " LIMIT " + limitLiteral
		matchResult, err := e.executeMatch(ctx, matchQuery)
		if err != nil {
			return nil, err
		}
		if len(matchResult.Rows) == 0 {
			break
		}

		e.normalizeSetMatchRowsToNodes(matchResult, store)
		deletedNodeIDs := make(map[string]struct{}, len(matchResult.Rows))
		deletedEdgeIDs := make(map[string]struct{}, len(matchResult.Rows))
		batchDeletes := 0
		for _, row := range matchResult.Rows {
			for _, val := range row {
				var nodeID string
				var edgeID string
				switch v := val.(type) {
				case map[string]interface{}:
					if id, ok := v["_edgeId"].(string); ok {
						edgeID = id
					} else if id, ok := v["_nodeId"].(string); ok {
						nodeID = id
					}
				case *storage.Node:
					nodeID = string(v.ID)
				case *storage.Edge:
					edgeID = string(v.ID)
				case string:
					nodeID = v
				}

				if edgeID != "" {
					if _, seen := deletedEdgeIDs[edgeID]; seen {
						continue
					}
					if err := store.DeleteEdge(storage.EdgeID(edgeID)); err == nil {
						result.Stats.RelationshipsDeleted++
						deletedEdgeIDs[edgeID] = struct{}{}
						batchDeletes++
					}
					continue
				}
				if nodeID == "" {
					continue
				}
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
					batchDeletes++
				}
			}
		}

		// Safety valve: if a batch yields rows but no successful deletes, stop to avoid loops.
		if batchDeletes == 0 {
			// Fallback once to the generic non-limited match to recover when an indexed
			// candidate source is temporarily stale after deletes.
			if !usedFallback {
				usedFallback = true
				fullMatchResult, fullErr := e.executeMatch(ctx, matchSegment+" RETURN "+deleteVars)
				if fullErr != nil {
					return nil, fullErr
				}
				e.normalizeSetMatchRowsToNodes(fullMatchResult, store)
				if len(fullMatchResult.Rows) == 0 {
					break
				}
				for _, row := range fullMatchResult.Rows {
					for _, val := range row {
						deleteTarget := classifyDeleteTargetValue(val)
						if deleteTarget.kind == deleteProjectionRelationship {
							if err := store.DeleteEdge(deleteTarget.edgeID); err == nil {
								result.Stats.RelationshipsDeleted++
								batchDeletes++
							}
							continue
						}
						if deleteTarget.kind != deleteProjectionNode {
							continue
						}
						nodeID := string(deleteTarget.nodeID)
						edgesCount := 0
						if needEdgeStats {
							outgoingEdges, _ := store.GetOutgoingEdges(storage.NodeID(nodeID))
							incomingEdges, _ := store.GetIncomingEdges(storage.NodeID(nodeID))
							edgesCount = len(outgoingEdges) + len(incomingEdges)
						}
						if err := store.DeleteNode(storage.NodeID(nodeID)); err == nil {
							result.Stats.NodesDeleted++
							result.Stats.RelationshipsDeleted += edgesCount
							e.removeNodeFromSearch(nodeID)
							batchDeletes++
						}
					}
				}
				if batchDeletes > 0 {
					continue
				}
			}
			break
		}
		// If fewer than the batch size rows were returned, we drained the candidate set.
		if len(matchResult.Rows) < deleteStreamingBatchSize {
			break
		}
	}

	return result, nil
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

// executeSet handles MATCH ... SET queries.
func (e *StorageExecutor) executeSet(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Normalize whitespace for index finding (newlines/tabs become spaces)
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Use word boundary detection to avoid matching substrings
	matchIdx := topLevelKeywordIndex(normalized, "MATCH")
	setIdx := topLevelKeywordIndex(normalized, "SET")
	returnIdx := topLevelKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || setIdx == -1 {
		return nil, localizedError(localization.CypherMutationsSetMatchRequired(), nil)
	}

	// Execute MATCH/WITH pipeline first and retain row scope for SET expression
	// evaluation, including aliases introduced by WITH ... AS.
	matchSegment := normalized[matchIdx:setIdx]
	matchPartEnd := len(matchSegment)
	if withIdx := findKeywordIndex(matchSegment, "WITH"); withIdx >= 0 {
		matchPartEnd = withIdx
	}
	matchPattern := strings.TrimSpace(matchSegment[len("MATCH"):matchPartEnd])
	matchVars := e.extractVariableNamesFromPattern(matchPattern)
	withAliases := extractWithAliases(matchSegment)
	allVars := dedupeNonEmpty(matchVars, withAliases)

	// Include variables referenced in SET/RETURN expressions so multi-MATCH SET
	// pipelines keep all required bindings in scope (e.g. t from:
	// MATCH ... MATCH ... SET t.x=... RETURN t.x).
	setTailForScope := strings.TrimSpace(normalized[setIdx+4:])
	setScopePart := setTailForScope
	if splitIdx := firstPostSetClauseIndex(setTailForScope); splitIdx >= 0 {
		setScopePart = strings.TrimSpace(setTailForScope[:splitIdx])
	}
	returnScopePart := ""
	if returnIdx > setIdx {
		returnScopePart = strings.TrimSpace(normalized[returnIdx+6:])
	}
	scopeVars := extractScopeVariablesFromSetAndReturn(setScopePart, returnScopePart)
	allVars = dedupeNonEmpty(allVars, scopeVars)

	matchQuery := matchSegment + " RETURN *"
	if len(allVars) > 0 {
		matchQuery = matchSegment + " RETURN " + strings.Join(allVars, ", ")
	}
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}
	// MATCH ... RETURN can surface nodes as maps (e.g. via nodeToMap). SET/RETURN
	// pipelines need live node pointers to preserve Cypher property semantics.
	e.normalizeSetMatchRowsToNodes(matchResult, store)
	e.normalizeSetMatchRowsToEdges(matchResult, store)

	// Parse SET clause: SET n.property = value or SET n += $properties.
	// If additional clauses follow SET (e.g., UNWIND/WITH/RETURN), split them out
	// so they are not consumed as part of assignment expressions.
	setTail := strings.TrimSpace(normalized[setIdx+4:]) // Skip "SET "
	postSetIdx := firstPostSetClauseIndex(setTail)
	setPart := setTail
	trailingPart := ""
	if postSetIdx >= 0 {
		setPart = strings.TrimSpace(setTail[:postSetIdx])
		trailingPart = strings.TrimSpace(setTail[postSetIdx:])
	}
	// Neo4j-compatible chained SET support:
	// MATCH ... SET n += $props SET n.foo = 1
	// pipelineApplySet gets the clauses as written (their boundaries count for
	// properties_set); the checks below see one assignment list.
	assignments := splitSetAssignments(collapseChainedSetClauses(setPart))
	if len(assignments) == 0 || (len(assignments) == 1 && strings.TrimSpace(assignments[0]) == "") {
		return nil, localizedError(localization.CypherMutationsSetAssignmentRequired(), nil)
	}

	// Apply SET through the shared per-entity applicator (pipelineApplySet ->
	// applySetToNodeWithContext / applySetToRelationshipWithContext), exactly
	// as the pipeline, MERGE and CREATE ... SET do. Only the targeted
	// variable changes; other row bindings are evaluation scope.
	targets := pipelineSetTargetVariables(assignments)
	variable := ""
	if len(targets) > 0 {
		variable = targets[0]
	}
	colIndex := make(map[string]int, len(matchResult.Columns))
	for i, col := range matchResult.Columns {
		colIndex[col] = i
	}
	rows := make([]pipelineRow, 0, len(matchResult.Rows))
	for _, row := range matchResult.Rows {
		bindings := make(pipelineRow, len(matchResult.Columns))
		for i, col := range matchResult.Columns {
			if i < len(row) {
				bindings[col] = row[i]
			}
		}
		for _, target := range targets {
			if _, bound := colIndex[target]; !bound {
				return nil, localizedError(localization.CypherMutationsUnknownSetVariable(target), nil)
			}
			switch bindings[target].(type) {
			case *storage.Node, *storage.Edge:
			default:
				return nil, localizedError(localization.CypherMutationsSetEntityRequired(target), nil)
			}
		}
		rows = append(rows, bindings)
	}
	if len(rows) > 0 {
		setStats, _, err := e.pipelineApplySet(ctx, rows, "SET "+setPart)
		if err != nil {
			return nil, err
		}
		if setStats != nil {
			addQueryStats(result.Stats, setStats)
		}
	}

	// If SET is followed by additional pipeline clauses (e.g. UNWIND/WITH), rerun
	// the post-mutation read pipeline as MATCH ... <trailing clauses>.
	if trailingPart != "" && !strings.HasPrefix(strings.ToUpper(trailingPart), "RETURN ") {
		if strings.HasPrefix(strings.ToUpper(trailingPart), "REMOVE ") {
			removeTail := strings.TrimSpace(trailingPart[len("REMOVE "):])
			removePart := removeTail
			nextTrailing := ""
			if retIdx := findKeywordIndex(removeTail, "RETURN"); retIdx >= 0 {
				removePart = strings.TrimSpace(removeTail[:retIdx])
				nextTrailing = strings.TrimSpace(removeTail[retIdx:])
			}
			if err := e.applyRemoveToMatchedRows(store, matchResult, removePart, result); err != nil {
				return nil, err
			}
			trailingPart = nextTrailing
		}

		if trailingPart == "" || strings.HasPrefix(strings.ToUpper(trailingPart), "RETURN ") {
			// Defer to common RETURN/default handling below.
		} else if strings.HasPrefix(strings.ToUpper(trailingPart), "UNWIND ") {
			return e.executeSetTrailingUnwind(ctx, trailingPart, matchResult, result)
		} else if withResult, handled, err := e.executeSetTrailingWithReturn(ctx, trailingPart, matchResult, result); handled {
			if err != nil {
				return nil, err
			}
			return withResult, nil
		} else {
			followQuery := strings.TrimSpace(matchSegment + " " + trailingPart)
			followResult, err := e.executeMatch(ctx, followQuery)
			if err != nil {
				return nil, err
			}
			result.Columns = followResult.Columns
			result.Rows = followResult.Rows
			return result, nil
		}
	}

	// Handle RETURN
	if returnIdx > 0 || strings.HasPrefix(strings.ToUpper(trailingPart), "RETURN ") {
		returnPart := trailingPart
		if returnPart == "" {
			returnPart = strings.TrimSpace(cypher[returnIdx+6:])
		} else {
			returnPart = strings.TrimSpace(returnPart[len("RETURN "):])
		}
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}

		// Aggregation in SET RETURN should produce aggregated rows, not one row per match.
		// Example: MATCH ... SET ... RETURN count(t) AS updated
		hasAggregation := false
		for _, item := range returnItems {
			if isAggregateFunc(item.expr) {
				hasAggregation = true
				break
			}
		}
		if hasAggregation {
			aggRow := make([]interface{}, len(returnItems))
			for j, item := range returnItems {
				exprUpper := strings.ToUpper(strings.TrimSpace(item.expr))
				switch {
				case strings.HasPrefix(exprUpper, "COUNT(") && strings.HasSuffix(exprUpper, ")"):
					inner := strings.TrimSpace(item.expr[len("COUNT(") : len(item.expr)-1])
					innerUpper := strings.ToUpper(inner)
					if innerUpper == "*" || inner == "" {
						aggRow[j] = int64(len(matchResult.Rows))
						continue
					}
					count := int64(0)
					parts := strings.SplitN(inner, ".", 2)
					varName := strings.TrimSpace(parts[0])
					propName := ""
					if len(parts) == 2 {
						propName = strings.TrimSpace(parts[1])
					}
					for _, row := range matchResult.Rows {
						varMap := make(map[string]*storage.Node, len(matchResult.Columns))
						relMap := make(map[string]*storage.Edge, len(matchResult.Columns))
						for i, colName := range matchResult.Columns {
							if i >= len(row) {
								continue
							}
							switch entity := row[i].(type) {
							case *storage.Node:
								if entity != nil {
									varMap[colName] = entity
								}
							case *storage.Edge:
								if entity != nil {
									relMap[colName] = entity
								}
							}
						}
						if node := varMap[varName]; node != nil {
							if propName == "" {
								count++
								continue
							}
							if v, ok := node.Properties[propName]; ok && v != nil {
								count++
							}
							continue
						}
						if rel := relMap[varName]; rel != nil {
							if propName == "" {
								count++
								continue
							}
							if v, ok := rel.Properties[propName]; ok && v != nil {
								count++
							}
						}
					}
					aggRow[j] = count
				default:
					// Keep behavior deterministic for mixed projections by evaluating against first row.
					if len(matchResult.Rows) == 0 {
						aggRow[j] = nil
						continue
					}
					varMap := make(map[string]*storage.Node, len(matchResult.Columns))
					relMap := make(map[string]*storage.Edge, len(matchResult.Columns))
					first := matchResult.Rows[0]
					for i, colName := range matchResult.Columns {
						if i < len(first) {
							switch entity := first[i].(type) {
							case *storage.Node:
								if entity != nil {
									varMap[colName] = entity
								}
							case *storage.Edge:
								if entity != nil {
									relMap[colName] = entity
								}
							}
						}
					}
					if variable != "" {
						if node, ok := varMap[variable]; ok {
							aggRow[j] = e.resolveReturnItem(ctx, item, variable, node)
							continue
						}
						if _, ok := relMap[variable]; ok {
							aggRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
							continue
						}
					}
					aggRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
				}
			}
			result.Rows = [][]interface{}{aggRow}
			return result, nil
		}

		// Return updated nodes
		// Build a map of variable names to nodes from match result columns
		// This handles multiple variables (e.g., n, m) correctly
		for _, row := range matchResult.Rows {
			// Map column names to values in this row
			varMap := make(map[string]*storage.Node)
			relMap := make(map[string]*storage.Edge)
			for i, colName := range matchResult.Columns {
				if i < len(row) {
					switch entity := row[i].(type) {
					case *storage.Node:
						if entity != nil {
							varMap[colName] = entity
						}
					case *storage.Edge:
						if entity != nil {
							relMap[colName] = entity
						}
					}
				}
			}

			// Build a single row with all return items
			newRow := make([]interface{}, len(returnItems))
			for j, item := range returnItems {
				// Extract variable name from return item expression
				// Handle cases like: "n", "n.name", "id(n)", etc.
				varName := extractVariableNameFromReturnItem(item.expr)
				if varName != "" {
					if node, ok := varMap[varName]; ok {
						newRow[j] = e.resolveReturnItem(ctx, item, varName, node)
						continue
					}
					if _, ok := relMap[varName]; ok {
						newRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
						continue
					}
				}
				// Fallback: try to resolve with the first variable (for backward compatibility)
				if variable != "" {
					if node, ok := varMap[variable]; ok {
						newRow[j] = e.resolveReturnItem(ctx, item, variable, node)
						continue
					}
					if _, ok := relMap[variable]; ok {
						newRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
						continue
					}
				}
				// If no variable matches, try to evaluate expression with all variables
				newRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
			}
			result.Rows = append(result.Rows, newRow)
		}
	} else {
		// SET without RETURN has no columns and no rows, as in Neo4j (#676).
		result.Columns = []string{}
		result.Rows = [][]interface{}{}
	}

	return result, nil
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

// executeSetTrailingUnwind handles MATCH ... SET ... UNWIND ... RETURN by applying
// UNWIND and RETURN projection directly on mutated MATCH rows, so SET changes are
// visible within the same query.
func (e *StorageExecutor) executeSetTrailingUnwind(ctx context.Context, trailingPart string, matchResult *ExecuteResult, result *ExecuteResult) (*ExecuteResult, error) {
	unwindPart := strings.TrimSpace(trailingPart)
	if !strings.HasPrefix(strings.ToUpper(unwindPart), "UNWIND ") {
		return nil, localizedError(localization.CypherMutationsUnwindClauseExpected(), nil)
	}
	unwindPart = strings.TrimSpace(unwindPart[len("UNWIND "):])

	asIdx := findKeywordIndex(unwindPart, "AS")
	if asIdx <= 0 {
		return nil, localizedError(localization.CypherMutationsUnwindASRequired(), nil)
	}

	unwindExpr := strings.TrimSpace(unwindPart[:asIdx])
	afterAs := strings.TrimSpace(unwindPart[asIdx+2:])
	if afterAs == "" {
		return nil, localizedError(localization.CypherMutationsUnwindVariableRequired(), nil)
	}

	returnIdx := findKeywordIndex(afterAs, "RETURN")
	if returnIdx <= 0 {
		return nil, localizedError(localization.CypherMutationsUnwindSetReturnRequired(), nil)
	}

	unwindVar := strings.TrimSpace(afterAs[:returnIdx])
	if fields := strings.Fields(unwindVar); len(fields) > 0 {
		unwindVar = fields[0]
	}
	if unwindVar == "" {
		return nil, localizedError(localization.CypherMutationsUnwindASVariableNonEmpty(), nil)
	}

	returnClause := strings.TrimSpace(afterAs[returnIdx+6:])
	returnItems := e.parseReturnItems(returnClause)
	result.Columns = make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	colIndex := make(map[string]int, len(matchResult.Columns))
	for i, col := range matchResult.Columns {
		colIndex[col] = i
	}

	for _, row := range matchResult.Rows {
		nodeVars := make(map[string]*storage.Node, len(matchResult.Columns))
		for i, col := range matchResult.Columns {
			if i < len(row) {
				if node, ok := row[i].(*storage.Node); ok && node != nil {
					nodeVars[col] = node
				}
			}
		}

		listVal := e.resolveUnwindValueFromExpr(ctx, unwindExpr, nodeVars)
		items := coerceToUnwindItems(listVal)
		for _, itemVal := range items {
			newRow := make([]interface{}, len(returnItems))
			for i, ret := range returnItems {
				expr := strings.TrimSpace(ret.expr)
				switch {
				case expr == unwindVar:
					newRow[i] = itemVal
				case strings.Contains(expr, "."):
					parts := strings.SplitN(expr, ".", 2)
					if len(parts) == 2 {
						if node, ok := nodeVars[parts[0]]; ok && node != nil {
							newRow[i] = node.Properties[parts[1]]
							break
						}
					}
					newRow[i] = e.evaluateExpressionWithContext(ctx, expr, nodeVars, make(map[string]*storage.Edge))
				default:
					if idx, ok := colIndex[expr]; ok && idx < len(row) {
						newRow[i] = row[idx]
						break
					}
					if node, ok := nodeVars[expr]; ok {
						newRow[i] = node
						break
					}
					newRow[i] = e.evaluateExpressionWithContext(ctx, expr, nodeVars, make(map[string]*storage.Edge))
				}
			}
			result.Rows = append(result.Rows, newRow)
		}
	}

	return result, nil
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

// executeSetTrailingWithReturn handles MATCH ... SET ... WITH ... RETURN by
// evaluating WITH/RETURN directly over the mutated MATCH rows.
func (e *StorageExecutor) executeSetTrailingWithReturn(ctx context.Context, trailingPart string, matchResult *ExecuteResult, result *ExecuteResult) (*ExecuteResult, bool, error) {
	upper := strings.ToUpper(strings.TrimSpace(trailingPart))
	if !strings.HasPrefix(upper, "WITH ") {
		return nil, false, nil
	}

	returnIdx := findKeywordIndex(trailingPart, "RETURN")
	if returnIdx <= 0 {
		return nil, false, nil
	}
	withClause := strings.TrimSpace(trailingPart[len("WITH "):returnIdx])
	if withClause == "" {
		return nil, true, localizedError(localization.CypherMutationsWithExpressionRequired(), nil)
	}
	for _, kw := range []string{"ORDER BY", "LIMIT", "SKIP", "UNWIND", "OPTIONAL MATCH", "MATCH", "CALL"} {
		if findKeywordIndex(withClause, kw) >= 0 {
			return nil, false, nil
		}
	}

	withItems := e.splitWithItems(withClause)
	type withExpr struct {
		expr  string
		alias string
	}
	parsedWith := make([]withExpr, 0, len(withItems))
	for _, item := range withItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		asIdx := findKeywordIndex(item, "AS")
		if asIdx > 0 {
			expr := strings.TrimSpace(item[:asIdx])
			alias := strings.TrimSpace(item[asIdx+2:])
			if expr == "" || alias == "" {
				return nil, true, localizedError(localization.CypherResidualWithItemInvalid(item), nil)
			}
			parsedWith = append(parsedWith, withExpr{expr: expr, alias: alias})
			continue
		}
		parsedWith = append(parsedWith, withExpr{expr: item, alias: item})
	}
	if len(parsedWith) == 0 {
		return nil, true, localizedError(localization.CypherMutationsWithExpressionRequired(), nil)
	}

	returnClause := strings.TrimSpace(trailingPart[returnIdx+len("RETURN"):])
	returnItems := e.parseReturnItems(returnClause)
	result.Columns = make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	colIndex := make(map[string]int, len(matchResult.Columns))
	for i, col := range matchResult.Columns {
		colIndex[col] = i
	}

	for _, row := range matchResult.Rows {
		rowScope := make(map[string]interface{}, len(parsedWith))
		nodeScope := make(map[string]*storage.Node, util.SafePreallocSum(len(parsedWith), len(matchResult.Columns)))
		for i, col := range matchResult.Columns {
			if i >= len(row) {
				continue
			}
			if node, ok := row[i].(*storage.Node); ok && node != nil {
				nodeScope[col] = node
			}
		}

		for _, wi := range parsedWith {
			val, resolved := resolveSetTrailingValue(wi.expr, row, colIndex, nodeScope)
			if !resolved {
				val = e.evaluateExpressionWithContext(ctx, wi.expr, nodeScope, nil)
			}
			rowScope[wi.alias] = val
			if node, ok := val.(*storage.Node); ok && node != nil {
				nodeScope[wi.alias] = node
			}
		}

		out := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			expr := strings.TrimSpace(item.expr)
			if val, ok := rowScope[expr]; ok {
				out[i] = val
				continue
			}
			if strings.Contains(expr, ".") {
				parts := strings.SplitN(expr, ".", 2)
				base := strings.TrimSpace(parts[0])
				prop := strings.TrimSpace(parts[1])
				if node, ok := nodeScope[base]; ok && node != nil {
					out[i] = node.Properties[prop]
					continue
				}
				if m, ok := rowScope[base].(map[string]interface{}); ok {
					out[i] = m[prop]
					continue
				}
			}
			out[i] = e.evaluateExpressionWithContext(ctx, expr, nodeScope, nil)
		}
		result.Rows = append(result.Rows, out)
	}

	return result, true, nil
}

func resolveSetTrailingValue(expr string, row []interface{}, colIndex map[string]int, nodeScope map[string]*storage.Node) (interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if idx, ok := colIndex[expr]; ok && idx < len(row) {
		return row[idx], true
	}
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		base := strings.TrimSpace(parts[0])
		prop := strings.TrimSpace(parts[1])
		if node, ok := nodeScope[base]; ok && node != nil {
			return node.Properties[prop], true
		}
	}
	return nil, false
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

func coerceToUnwindItems(listVal interface{}) []interface{} {
	switch v := listVal.(type) {
	case nil:
		return nil
	case []interface{}:
		return v
	case []string:
		out := make([]interface{}, len(v))
		for i, s := range v {
			out[i] = s
		}
		return out
	case []int:
		out := make([]interface{}, len(v))
		for i, n := range v {
			out[i] = n
		}
		return out
	case []int64:
		out := make([]interface{}, len(v))
		for i, n := range v {
			out[i] = n
		}
		return out
	default:
		return []interface{}{listVal}
	}
}

func extractWithAliases(querySegment string) []string {
	re := regexp.MustCompile(`(?i)\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b`)
	matches := re.FindAllStringSubmatch(querySegment, -1)
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

// executeRemove handles MATCH ... REMOVE queries for property removal.
// Syntax: MATCH (n:Label) REMOVE n.property [, n.property2] [RETURN ...]
func (e *StorageExecutor) executeRemove(ctx context.Context, cypher string) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Normalize whitespace
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(normalized, "MATCH")
	removeIdx := findKeywordIndex(normalized, "REMOVE")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || removeIdx == -1 {
		return nil, localizedError(localization.CypherMutationsRemoveMatchRequired(), nil)
	}

	// Execute the MATCH/WITH pipeline while preserving every entity that can
	// remain in scope after WITH. Returning explicit variables avoids the
	// legacy MATCH/WITH projection treating RETURN * as an unbound expression.
	matchSegment := normalized[matchIdx:removeIdx]
	matchPartEnd := len(matchSegment)
	if withIdx := findKeywordIndex(matchSegment, "WITH"); withIdx >= 0 {
		matchPartEnd = withIdx
	}
	matchPattern := strings.TrimSpace(matchSegment[len("MATCH"):matchPartEnd])
	matchVars := e.extractVariableNamesFromPattern(matchPattern)
	withAliases := extractWithAliases(matchSegment)
	allVars := dedupeNonEmpty(matchVars, withAliases)
	removeLen := len("REMOVE")
	var removePart string
	if returnIdx > 0 && returnIdx > removeIdx {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen : returnIdx])
	} else {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen:])
	}
	returnScopePart := ""
	if returnIdx > removeIdx {
		returnScopePart = strings.TrimSpace(normalized[returnIdx+6:])
	}
	scopeVars := extractScopeVariablesFromRemoveAndReturn(removePart, returnScopePart)
	allVars = dedupeNonEmpty(allVars, scopeVars)

	// Include variables referenced only in the REMOVE/RETURN clauses so a
	// relationship variable bound inside a bracketed pattern (e.g. the "r" in
	// "OPTIONAL MATCH (n)-[r:TYPE]->(m)") stays in the probe's projection.
	// extractVariableNamesFromPattern above only scans node groups outside
	// "[...]", so a bare "REMOVE r.prop" previously vanished from matchQuery's
	// RETURN list entirely.
	matchQuery := matchSegment + " RETURN *"
	if len(allVars) > 0 {
		matchQuery = matchSegment + " RETURN " + strings.Join(allVars, ", ")
	}
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}
	// MATCH ... WITH projections can surface nodes as maps. REMOVE needs the
	// live storage entities so property and label mutations reach the graph.
	e.normalizeSetMatchRowsToNodes(matchResult, store)
	e.normalizeSetMatchRowsToEdges(matchResult, store)

	// Update matched nodes and relationships for the variables explicitly named
	// in the REMOVE clause, as the pipeline's REMOVE does.
	if err := e.applyRemoveToMatchedRows(store, matchResult, removePart, result); err != nil {
		return nil, err
	}

	// Handle RETURN. Build exactly one result row per matchResult row
	// (mirroring executeSet's RETURN handling below via varMap/relMap +
	// extractVariableNameFromReturnItem) instead of emitting one row per
	// *storage.Node encountered in the row -- the latter both dropped every
	// relationship variable and duplicated rows whenever a row bound more
	// than one node variable.
	if returnIdx > 0 && returnIdx > removeIdx {
		returnPart := strings.TrimSpace(normalized[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}
		for _, row := range matchResult.Rows {
			varMap := make(map[string]*storage.Node, len(matchResult.Columns))
			relMap := make(map[string]*storage.Edge, len(matchResult.Columns))
			for i, colName := range matchResult.Columns {
				if i >= len(row) {
					continue
				}
				switch entity := row[i].(type) {
				case *storage.Node:
					if entity != nil {
						varMap[colName] = entity
					}
				case *storage.Edge:
					if entity != nil {
						relMap[colName] = entity
					}
				}
			}

			resultRow := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				varName := extractVariableNameFromReturnItem(item.expr)
				if varName != "" {
					if node, ok := varMap[varName]; ok {
						resultRow[i] = e.resolveReturnItem(ctx, item, varName, node)
						continue
					}
					if _, ok := relMap[varName]; ok {
						resultRow[i] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
						continue
					}
				}
				resultRow[i] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, relMap)
			}
			result.Rows = append(result.Rows, resultRow)
		}
	}

	return result, nil
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

// smartSplitReturnItems splits a RETURN clause by commas, but respects:
// - CASE/END boundaries
// - Parentheses (function calls)
// - Curly braces (map projections like n { .*, key: value })
// - Square brackets (list literals)
// - String literals
// smartSplitReturnItems splits RETURN items by comma, respecting strings, parentheses, and CASE/END.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func (e *StorageExecutor) smartSplitReturnItems(returnPart string) []string {
	var result []string
	var current strings.Builder
	var inString bool
	var stringChar rune
	var parenDepth int
	var braceDepth int
	var bracketDepth int
	var caseDepth int

	runes := []rune(returnPart)
	runeLen := len(runes)

	// Build rune-to-byte index mapping for keyword checking
	runeToByteIndex := make([]int, util.SafePreallocSum(runeLen, 1))
	byteIdx := 0
	for ri, r := range runes {
		runeToByteIndex[ri] = byteIdx
		byteIdx += len(string(r))
	}
	runeToByteIndex[runeLen] = byteIdx

	upper := strings.ToUpper(returnPart)

	for ri := 0; ri < runeLen; ri++ {
		ch := runes[ri]
		bytePos := runeToByteIndex[ri]

		// Track string literals
		if ch == '\'' || ch == '"' {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			current.WriteRune(ch)
			continue
		}

		if inString {
			current.WriteRune(ch)
			continue
		}

		// Track parentheses
		if ch == '(' {
			parenDepth++
			current.WriteRune(ch)
			continue
		}
		if ch == ')' {
			parenDepth--
			current.WriteRune(ch)
			continue
		}

		// Track curly braces (map projections)
		if ch == '{' {
			braceDepth++
			current.WriteRune(ch)
			continue
		}
		if ch == '}' {
			braceDepth--
			current.WriteRune(ch)
			continue
		}

		// Track square brackets (list literals)
		if ch == '[' {
			bracketDepth++
			current.WriteRune(ch)
			continue
		}
		if ch == ']' {
			bracketDepth--
			current.WriteRune(ch)
			continue
		}

		// Track CASE/END keywords (using byte positions for substring comparison)
		if bytePos+4 <= len(returnPart) && upper[bytePos:bytePos+4] == "CASE" {
			// Check if CASE is a word boundary
			prevOk := ri == 0 || !isAlphaNum(runes[ri-1])
			nextRuneIdx := ri + 4 // Skip 4 runes for "CASE"
			// Need to find which rune corresponds to bytePos+4
			for nextRuneIdx < runeLen && runeToByteIndex[nextRuneIdx] < bytePos+4 {
				nextRuneIdx++
			}
			nextOk := nextRuneIdx >= runeLen || !isAlphaNum(runes[nextRuneIdx])
			if prevOk && nextOk {
				caseDepth++
			}
		}
		if bytePos+3 <= len(returnPart) && upper[bytePos:bytePos+3] == "END" {
			// Check if END is a word boundary
			prevOk := ri == 0 || !isAlphaNum(runes[ri-1])
			nextRuneIdx := ri + 3 // Skip 3 runes for "END"
			for nextRuneIdx < runeLen && runeToByteIndex[nextRuneIdx] < bytePos+3 {
				nextRuneIdx++
			}
			nextOk := nextRuneIdx >= runeLen || !isAlphaNum(runes[nextRuneIdx])
			if prevOk && nextOk && caseDepth > 0 {
				caseDepth--
			}
		}

		// Split on comma only if we're not inside parens, braces, brackets, CASE, or strings
		if ch == ',' && parenDepth == 0 && braceDepth == 0 && bracketDepth == 0 && caseDepth == 0 {
			result = append(result, current.String())
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	// Add remaining content
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// isAlphaNum checks if a character is alphanumeric or underscore
func isAlphaNum(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func (e *StorageExecutor) parseReturnItems(returnPart string) []returnItem {
	items := []returnItem{}

	// Strip top-level trailing clauses from RETURN projection.
	// Use keyword scanning to avoid false matches in identifiers like "order_count".
	end := len(returnPart)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(returnPart, kw); idx >= 0 && idx < end {
			end = idx
		}
	}
	if end < len(returnPart) {
		returnPart = strings.TrimSpace(returnPart[:end])
	}

	// Split by comma, but respect CASE/END boundaries and parentheses
	parts := e.smartSplitReturnItems(returnPart)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "*" {
			continue
		}

		item := returnItem{expr: part}

		// Check for AS alias
		asIdx := projectionAliasIndex(part)
		if asIdx > 0 {
			item.expr = strings.TrimSpace(part[:asIdx])
			item.alias = normalizeProjectionColumnName(part[asIdx+len("AS"):])
		} else {
			// Handle map projection without AS alias: n { .*, key: value } -> column name is "n"
			// Neo4j infers the column name from the variable before the map projection
			if braceIdx := strings.Index(part, " {"); braceIdx > 0 {
				varName := strings.TrimSpace(part[:braceIdx])
				if varName != "" && !strings.Contains(varName, "(") {
					item.alias = varName
				}
			}
		}

		items = append(items, item)
	}

	// If empty or *, return all
	if len(items) == 0 {
		items = append(items, returnItem{expr: "*"})
	}

	return items
}

func normalizeProjectionColumnName(raw string) string {
	name := strings.TrimSpace(raw)
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		return strings.ReplaceAll(name[1:len(name)-1], "``", "`")
	}
	return name
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
	upperClause := strings.ToUpper(whereClause)
	prefixUpper := strings.ToUpper(prefix)

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
