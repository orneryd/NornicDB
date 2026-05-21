package cypher

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
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
		return "", nil, nil, fmt.Errorf("invalid pattern: %s", pattern)
	}
	pattern = pattern[1 : len(pattern)-1]

	// Extract variable name and labels
	varName := ""
	labels := []string{}
	props := make(map[string]interface{})

	// Find properties block
	propsStart := strings.Index(pattern, "{")
	labelPart := pattern
	if propsStart > 0 {
		labelPart = pattern[:propsStart]
		propsEnd := strings.LastIndex(pattern, "}")
		if propsEnd > propsStart {
			propsStr := pattern[propsStart+1 : propsEnd]
			props = e.parseProperties(ctx, propsStr)
		}
	}

	// Parse variable and labels
	parts := strings.Split(labelPart, ":")
	if len(parts) > 0 {
		varName = strings.TrimSpace(parts[0])
	}
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		if label != "" {
			labels = append(labels, label)
		}
	}

	return varName, labels, props, nil
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
		return nil, fmt.Errorf("DELETE requires a MATCH clause first (e.g., MATCH (n) DELETE n)")
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
		return nil, fmt.Errorf("DELETE clause must specify variable(s) to delete (e.g., DELETE n)")
	}

	// For DETACH DELETE, ensure deleteIdx points to "DETACH DELETE", not bare "DETACH".
	if detach && deleteIdx > 0 {
		checkSubstring := strings.ToUpper(strings.TrimSpace(cypher[deleteIdx:]))
		if strings.HasPrefix(checkSubstring, "DETACH ") && !strings.HasPrefix(checkSubstring, "DETACH DELETE ") {
			return nil, fmt.Errorf("DETACH DELETE requires both DETACH and DELETE keywords together")
		}
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	needEdgeStats := returnIdx > 0 || detach // always track for DETACH so stats are correct

	// Streaming batched delete hot path for large DETACH DELETE scans.
	// This preserves semantics by only engaging on simple shapes without
	// LIMIT/SKIP/WITH/ORDER BY/CALL/UNWIND in the MATCH segment.
	matchSegment := strings.TrimSpace(cypher[matchIdx:deleteIdx])
	_, inTransactionWrapper := e.getStorage(ctx).(*transactionStorageWrapper)
	if !inTransactionWrapper && e.isDeleteStreamingEligible(matchSegment, deleteVars, detach) {
		result, err := e.executeDeleteStreaming(ctx, matchSegment, deleteVars, needEdgeStats)
		if err != nil {
			return nil, err
		}
		e.applyDeleteReturnProjection(result, cypher, deleteVars)
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
	// Pre-size dedup maps based on match result to reduce rehashing.
	rowCount := len(matchResult.Rows)
	deletedNodeIDs := make(map[string]struct{}, rowCount)
	deletedEdgeIDs := make(map[string]struct{}, rowCount)

	for _, row := range matchResult.Rows {
		for _, val := range row {
			// Try to extract node ID or edge ID
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

			// Handle relationship deletion
			if edgeID != "" {
				if _, seen := deletedEdgeIDs[edgeID]; seen {
					continue
				}
				if err := store.DeleteEdge(storage.EdgeID(edgeID)); err == nil {
					result.Stats.RelationshipsDeleted++
					deletedEdgeIDs[edgeID] = struct{}{}
				}
				continue
			}

			// Handle node deletion
			if nodeID == "" {
				continue
			}
			if _, seen := deletedNodeIDs[nodeID]; seen {
				continue
			}

			if detach {
				// Count edges that will be deleted with the node (for stats).
				// Combine outgoing + incoming in a single stats tally.
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
			} else {
				if err := store.DeleteNode(storage.NodeID(nodeID)); err == nil {
					result.Stats.NodesDeleted++
					deletedNodeIDs[nodeID] = struct{}{}
					e.removeNodeFromSearch(nodeID)
				}
			}
		}
	}

	e.applyDeleteReturnProjection(result, cypher, deleteVars)

	return result, nil
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

	e.applyDeleteReturnProjection(result, cypher, deleteVar)
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
	if usedIndex && len(nodes) == 0 && wherePart != "" {
		// Fail-open on potential stale index candidates.
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

func (e *StorageExecutor) applyDeleteReturnProjection(result *ExecuteResult, cypher, deleteVars string) {
	if result == nil {
		return
	}
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx <= 0 {
		return
	}
	returnPart := strings.TrimSpace(cypher[returnIdx+6:])
	returnItems := e.parseReturnItems(returnPart)
	result.Columns = make([]string, len(returnItems))
	row := make([]interface{}, len(returnItems))

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
		upperExpr := strings.ToUpper(item.expr)
		if !strings.HasPrefix(upperExpr, "COUNT(") {
			continue
		}
		inner := strings.TrimSpace(item.expr[6 : len(item.expr)-1]) // Remove "count(" and ")"
		upperInner := strings.ToUpper(inner)
		if upperInner == "*" {
			row[i] = int64(result.Stats.NodesDeleted + result.Stats.RelationshipsDeleted)
			continue
		}
		if strings.Contains(upperInner, strings.ToUpper(deleteVars)) {
			row[i] = int64(result.Stats.NodesDeleted)
		} else {
			row[i] = int64(result.Stats.NodesDeleted)
		}
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
							if err := store.DeleteEdge(storage.EdgeID(edgeID)); err == nil {
								result.Stats.RelationshipsDeleted++
								batchDeletes++
							}
							continue
						}
						if nodeID == "" {
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
	matchIdx := findKeywordIndex(normalized, "MATCH")
	setIdx := findKeywordIndex(normalized, "SET")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || setIdx == -1 {
		return nil, fmt.Errorf("SET requires a MATCH clause first (e.g., MATCH (n) SET n.property = value)")
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
	// Collapse additional SET keywords into a single assignment list.
	setPart = collapseChainedSetClauses(setPart)
	setPartForAssignments := setPart

	// Substitute params for simple SET assignments (e.g., n.prop = $value).
	// For SET += $props, defer to executeSetMerge which reads params directly.
	if params := getParamsFromContext(ctx); params != nil && !strings.Contains(setPart, "+=") {
		setPartForAssignments = e.substituteParams(setPart, params)
	}

	// Split SET clause into individual assignments, respecting brackets
	// e.g., "n.embedding = [0.1, 0.2], n.dim = 4" -> ["n.embedding = [0.1, 0.2]", "n.dim = 4"]
	assignments := e.splitSetAssignmentsRespectingBrackets(setPartForAssignments)

	if len(assignments) == 0 || (len(assignments) == 1 && strings.TrimSpace(assignments[0]) == "") {
		return nil, fmt.Errorf("SET clause requires at least one assignment")
	}

	colIndex := make(map[string]int, len(matchResult.Columns))
	for i, col := range matchResult.Columns {
		colIndex[col] = i
	}

	var variable string
	validAssignments := 0
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Support SET property merge in mixed assignment lists:
		// SET n += $props, n.updated_at = $ts
		plusEqIdx := strings.Index(assignment, "+=")
		if plusEqIdx >= 0 {
			leftVar := strings.TrimSpace(assignment[:plusEqIdx])
			right := strings.TrimSpace(assignment[plusEqIdx+2:])
			if leftVar == "" {
				return nil, fmt.Errorf("SET += requires a variable target")
			}
			validAssignments++
			variable = leftVar

			var propsToMerge map[string]interface{}
			mapVarName := ""
			paramMapUsed := false
			if strings.HasPrefix(right, "{") {
				parsedProps, err := e.parseSetMergeMapLiteralStrict(ctx, right)
				if err != nil {
					return nil, fmt.Errorf("failed to parse properties in SET +=: %w", err)
				}
				propsToMerge = parsedProps
			} else if strings.HasPrefix(right, "$") {
				paramName := strings.TrimSpace(right[1:])
				if paramName == "" {
					return nil, fmt.Errorf("SET += requires a valid parameter name after $")
				}
				params := getParamsFromContext(ctx)
				if params == nil {
					return nil, fmt.Errorf("SET += parameter $%s requires parameters to be provided", paramName)
				}
				paramValue, exists := params[paramName]
				if !exists {
					return nil, fmt.Errorf("SET += parameter $%s not found in provided parameters", paramName)
				}
				propsMap, err := normalizePropsMap(paramValue, fmt.Sprintf("parameter $%s", paramName))
				if err != nil {
					return nil, err
				}
				propsToMerge = propsMap
				paramMapUsed = true
			} else if isValidIdentifier(right) {
				mapVarName = right
			} else {
				return nil, fmt.Errorf("SET += requires a map or parameter (got: %q)", right)
			}

			targetIdx, hasTargetIdx := colIndex[leftVar]
			mapIdx, hasMapIdx := colIndex[mapVarName]
			for _, row := range matchResult.Rows {
				propsForRow := propsToMerge
				if mapVarName != "" && !paramMapUsed {
					if !hasMapIdx || mapIdx >= len(row) {
						return nil, fmt.Errorf("SET += requires a map variable in scope (missing %q)", mapVarName)
					}
					propsMap, err := normalizePropsMap(row[mapIdx], fmt.Sprintf("variable %s", mapVarName))
					if err != nil {
						return nil, err
					}
					propsForRow = propsMap
				}

				updated := false
				if hasTargetIdx && targetIdx < len(row) {
					if node, ok := row[targetIdx].(*storage.Node); ok && node != nil {
						for k, v := range propsForRow {
							setNodeProperty(node, k, v)
							result.Stats.PropertiesSet++
						}
						if err := store.UpdateNode(node); err == nil {
							e.notifyNodeMutated(string(node.ID))
						}
						updated = true
					}
				}
				if updated {
					continue
				}

				for _, val := range row {
					node, ok := val.(*storage.Node)
					if !ok || node == nil {
						continue
					}
					for k, v := range propsForRow {
						setNodeProperty(node, k, v)
						result.Stats.PropertiesSet++
					}
					if err := store.UpdateNode(node); err == nil {
						e.notifyNodeMutated(string(node.ID))
					}
				}
			}
			continue
		}

		// Check for label assignment: n:Label (no = sign, has : for label)
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			// Could be a label assignment like "n:Label"
			colonIdx := strings.Index(assignment, ":")
			if colonIdx > 0 {
				// This is a label assignment
				labelVar := strings.TrimSpace(assignment[:colonIdx])
				labelName := strings.TrimSpace(assignment[colonIdx+1:])
				// Normalize escaped label identifiers (e.g. `MyLabel`) before validation/storage.
				if len(labelName) >= 2 && strings.HasPrefix(labelName, "`") && strings.HasSuffix(labelName, "`") {
					labelName = strings.ReplaceAll(labelName[1:len(labelName)-1], "``", "`")
				}
				if labelVar != "" && labelName != "" {
					if !isValidIdentifier(labelName) {
						return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", labelName)
					}
					if containsReservedKeyword(labelName) {
						return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", labelName)
					}
					validAssignments++
					variable = labelVar
					// Add label to matched nodes
					for _, row := range matchResult.Rows {
						for _, val := range row {
							node, ok := val.(*storage.Node)
							if !ok || node == nil {
								continue
							}
							// Add label if not already present
							hasLabel := false
							for _, l := range node.Labels {
								if l == labelName {
									hasLabel = true
									break
								}
							}
							if !hasLabel {
								oldLabels := make([]string, len(node.Labels))
								copy(oldLabels, node.Labels)
								node.Labels = append(node.Labels, labelName)
								// Validate policy constraints before committing the label change.
								if err := validatePolicyOnLabelChange(store, node, oldLabels); err != nil {
									node.Labels = oldLabels // restore
									return nil, err
								}
								// Labels are part of the embedding text; invalidate managed embeddings so they regenerate.
								embeddingutil.InvalidateManagedEmbeddings(node)
								if err := store.UpdateNode(node); err != nil {
									node.Labels = oldLabels // restore
									return nil, err
								}
								result.Stats.LabelsAdded++
								e.notifyNodeMutated(string(node.ID))
							}
						}
					}
					continue
				}
			}
			return nil, fmt.Errorf("invalid SET assignment: %q (expected n.property = value or n:Label)", assignment)
		}

		left := strings.TrimSpace(assignment[:eqIdx])
		right := strings.TrimSpace(assignment[eqIdx+1:])

		buildEvalNodes := func(row []interface{}) map[string]*storage.Node {
			evalNodes := make(map[string]*storage.Node, len(matchResult.Columns))
			for i, col := range matchResult.Columns {
				if i >= len(row) {
					continue
				}
				switch v := row[i].(type) {
				case *storage.Node:
					if v != nil {
						evalNodes[col] = v
					}
				case map[string]interface{}:
					evalNodes[col] = &storage.Node{
						ID:         storage.NodeID(col),
						Properties: v,
					}
				default:
					evalNodes[col] = &storage.Node{
						ID: storage.NodeID(col),
						Properties: map[string]interface{}{
							"value": v,
						},
					}
				}
			}
			return evalNodes
		}

		buildEvalEdges := func(row []interface{}) map[string]*storage.Edge {
			evalEdges := make(map[string]*storage.Edge, len(matchResult.Columns))
			for i, col := range matchResult.Columns {
				if i >= len(row) {
					continue
				}
				if edge, ok := row[i].(*storage.Edge); ok && edge != nil {
					evalEdges[col] = edge
				}
			}
			return evalEdges
		}

		resolvePropValue := func(row []interface{}) (interface{}, error) {
			if strings.HasPrefix(right, "$") {
				paramName := strings.TrimSpace(right[1:])
				if paramName == "" {
					return nil, fmt.Errorf("SET assignment requires a valid parameter name after $")
				}
				params := getParamsFromContext(ctx)
				if params == nil {
					return nil, fmt.Errorf("SET assignment parameter $%s requires parameters to be provided", paramName)
				}
				paramValue, exists := params[paramName]
				if !exists {
					return nil, fmt.Errorf("SET assignment parameter $%s not found in provided parameters", paramName)
				}
				return normalizePropValue(paramValue), nil
			}
			return e.evaluateExpressionWithContext(ctx, right, buildEvalNodes(row), buildEvalEdges(row)), nil
		}

		// Extract variable and property (or whole-variable map replacement)
		parts := strings.SplitN(left, ".", 2)
		validAssignments++
		if len(parts) != 2 {
			variable = strings.TrimSpace(left)
			// Replace properties on matched entities: SET n = { ... }
			for _, row := range matchResult.Rows {
				propValue, err := resolvePropValue(row)
				if err != nil {
					return nil, err
				}
				props, err := normalizePropsMap(propValue, "SET assignment")
				if err != nil {
					return nil, fmt.Errorf("invalid SET assignment: %q (expected variable.property = value or variable = {property: value}): %w", assignment, err)
				}
				for _, val := range row {
					switch entity := val.(type) {
					case *storage.Node:
						if entity == nil {
							continue
						}
						entity.Properties = cloneStringAnyMap(props)
						if err := store.UpdateNode(entity); err == nil {
							result.Stats.PropertiesSet++
							e.notifyNodeMutated(string(entity.ID))
						}
					case *storage.Edge:
						if entity == nil {
							continue
						}
						entity.Properties = cloneStringAnyMap(props)
						if err := store.UpdateEdge(entity); err == nil {
							result.Stats.PropertiesSet++
						}
					}
				}
			}
			continue
		}
		variable = parts[0]
		propName := parts[1]

		// Update matched nodes
		for _, row := range matchResult.Rows {
			propValue, err := resolvePropValue(row)
			if err != nil {
				return nil, err
			}
			for _, val := range row {
				switch entity := val.(type) {
				case *storage.Node:
					if entity == nil {
						continue
					}
					setNodeProperty(entity, propName, propValue)
					if err := store.UpdateNode(entity); err != nil {
						return nil, fmt.Errorf("SET %s.%s: %w", variable, propName, err)
					}
					result.Stats.PropertiesSet++
					e.notifyNodeMutated(string(entity.ID))
				case *storage.Edge:
					if entity == nil {
						continue
					}
					if entity.Properties == nil {
						entity.Properties = make(map[string]interface{})
					}
					entity.Properties[propName] = propValue
					if err := store.UpdateEdge(entity); err != nil {
						return nil, fmt.Errorf("SET %s.%s: %w", variable, propName, err)
					}
					result.Stats.PropertiesSet++
				}
			}
		}
	}
	_ = variable // silence unused warning

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
						for i, colName := range matchResult.Columns {
							if i < len(row) {
								if node, ok := row[i].(*storage.Node); ok && node != nil {
									varMap[colName] = node
								}
							}
						}
						node := varMap[varName]
						if node == nil {
							continue
						}
						if propName == "" {
							count++
							continue
						}
						if v, ok := node.Properties[propName]; ok && v != nil {
							count++
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
					first := matchResult.Rows[0]
					for i, colName := range matchResult.Columns {
						if i < len(first) {
							if node, ok := first[i].(*storage.Node); ok && node != nil {
								varMap[colName] = node
							}
						}
					}
					if variable != "" {
						if node, ok := varMap[variable]; ok {
							aggRow[j] = e.resolveReturnItem(ctx, item, variable, node)
							continue
						}
					}
					aggRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, make(map[string]*storage.Edge))
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
			for i, colName := range matchResult.Columns {
				if i < len(row) {
					if node, ok := row[i].(*storage.Node); ok && node != nil {
						varMap[colName] = node
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
				}
				// Fallback: try to resolve with the first variable (for backward compatibility)
				if variable != "" {
					if node, ok := varMap[variable]; ok {
						newRow[j] = e.resolveReturnItem(ctx, item, variable, node)
						continue
					}
				}
				// If no variable matches, try to evaluate expression with all variables
				newRow[j] = e.evaluateExpressionWithContext(ctx, item.expr, varMap, make(map[string]*storage.Edge))
			}
			result.Rows = append(result.Rows, newRow)
		}
	} else {
		// Neo4j-compatible default for SET without RETURN: matched row count.
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{len(matchResult.Rows)}}
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
func collapseChainedSetClauses(setPart string) string {
	setPart = strings.TrimSpace(setPart)
	if setPart == "" {
		return setPart
	}

	opts := defaultKeywordScanOpts()
	segments := make([]string, 0, 2)
	start := 0
	for {
		nextSet := keywordIndexFrom(setPart, "SET", start, opts)
		if nextSet < 0 {
			break
		}
		segment := strings.TrimSpace(setPart[start:nextSet])
		if segment != "" {
			segments = append(segments, segment)
		}
		start = nextSet + len("SET")
		for start < len(setPart) && isASCIISpace(setPart[start]) {
			start++
		}
	}

	tail := strings.TrimSpace(setPart[start:])
	if tail != "" {
		segments = append(segments, tail)
	}
	if len(segments) == 0 {
		return setPart
	}
	return strings.Join(segments, ", ")
}

// firstPostSetClauseIndex returns the first index of a clause keyword that can
// legally follow a SET assignment list. Returns -1 when none are present.
func firstPostSetClauseIndex(setTail string) int {
	opts := defaultKeywordScanOpts()
	first := -1
	for _, kw := range []string{"REMOVE", "UNWIND", "WITH", "RETURN", "ORDER BY", "LIMIT", "SKIP"} {
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
		return nil, fmt.Errorf("UNWIND clause expected")
	}
	unwindPart = strings.TrimSpace(unwindPart[len("UNWIND "):])

	asIdx := findKeywordIndex(unwindPart, "AS")
	if asIdx <= 0 {
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
	}

	unwindExpr := strings.TrimSpace(unwindPart[:asIdx])
	afterAs := strings.TrimSpace(unwindPart[asIdx+2:])
	if afterAs == "" {
		return nil, fmt.Errorf("UNWIND requires a variable after AS")
	}

	returnIdx := findKeywordIndex(afterAs, "RETURN")
	if returnIdx <= 0 {
		return nil, fmt.Errorf("UNWIND in SET query requires RETURN clause")
	}

	unwindVar := strings.TrimSpace(afterAs[:returnIdx])
	if fields := strings.Fields(unwindVar); len(fields) > 0 {
		unwindVar = fields[0]
	}
	if unwindVar == "" {
		return nil, fmt.Errorf("UNWIND requires a non-empty AS variable")
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
		return nil, true, fmt.Errorf("WITH clause requires at least one expression")
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
				return nil, true, fmt.Errorf("invalid WITH item: %q", item)
			}
			parsedWith = append(parsedWith, withExpr{expr: expr, alias: alias})
			continue
		}
		parsedWith = append(parsedWith, withExpr{expr: item, alias: item})
	}
	if len(parsedWith) == 0 {
		return nil, true, fmt.Errorf("WITH clause requires at least one expression")
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
		nodeScope := make(map[string]*storage.Node, len(parsedWith)+len(matchResult.Columns))
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

// executeSetMerge handles SET n += $properties for property merging.
// This implements the Cypher property merge operator which merges properties from a map
// or parameter into existing node properties.
//
// Example:
//
//	MATCH (n:Person) SET n += {age: 30, city: 'NYC'}  // Inline map
//	MATCH (n:Person) SET n += $props                  // Parameter map
//
// Parameters are retrieved from context (stored during query execution).
func (e *StorageExecutor) executeSetMerge(ctx context.Context, matchResult *ExecuteResult, setPart string, result *ExecuteResult, cypher string, returnIdx int) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	// Parse: n += $properties or n += {key: value}
	plusEqIdx := strings.Index(setPart, "+=")
	if plusEqIdx == -1 {
		return nil, fmt.Errorf("expected += operator")
	}

	variable := strings.TrimSpace(setPart[:plusEqIdx])
	right := strings.TrimSpace(setPart[plusEqIdx+2:])

	// Parse the properties to merge
	var propsToMerge map[string]interface{}
	mapVarName := ""
	paramMapUsed := false

	if strings.HasPrefix(right, "{") {
		// Inline properties: {key: value, ...}
		parsedProps, err := e.parseSetMergeMapLiteralStrict(ctx, right)
		if err != nil {
			return nil, fmt.Errorf("failed to parse properties in SET +=: %w", err)
		}
		propsToMerge = parsedProps
	} else if strings.HasPrefix(right, "$") {
		// Parameter reference: $properties
		// Extract parameter name (remove $ prefix)
		paramName := strings.TrimSpace(right[1:])
		if paramName == "" {
			return nil, fmt.Errorf("SET += requires a valid parameter name after $")
		}

		// Retrieve parameters from context
		params := getParamsFromContext(ctx)
		if params == nil {
			return nil, fmt.Errorf("SET += parameter $%s requires parameters to be provided", paramName)
		}

		// Look up the parameter value
		paramValue, exists := params[paramName]
		if !exists {
			return nil, fmt.Errorf("SET += parameter $%s not found in provided parameters", paramName)
		}

		propsMap, err := normalizePropsMap(paramValue, fmt.Sprintf("parameter $%s", paramName))
		if err != nil {
			return nil, err
		}
		propsToMerge = propsMap
		paramMapUsed = true
	} else if isValidIdentifier(right) {
		// Map variable: SET n += props
		mapVarName = right
	} else {
		return nil, fmt.Errorf("SET += requires a map or parameter (got: %q)", right)
	}

	// Collect updated nodes for RETURN
	var updatedNodes []*storage.Node
	colIndex := make(map[string]int, len(matchResult.Columns))
	for i, col := range matchResult.Columns {
		colIndex[col] = i
	}
	targetIdx, hasTargetIdx := colIndex[variable]
	mapIdx, hasMapIdx := colIndex[mapVarName]

	// Update matched nodes
	for _, row := range matchResult.Rows {
		propsForRow := propsToMerge
		if mapVarName != "" && !paramMapUsed {
			if !hasMapIdx || mapIdx >= len(row) {
				return nil, fmt.Errorf("SET += requires a map variable in scope (missing %q)", mapVarName)
			}
			propsMap, err := normalizePropsMap(row[mapIdx], fmt.Sprintf("variable %s", mapVarName))
			if err != nil {
				return nil, err
			}
			propsForRow = propsMap
		}

		// Prefer updating only the requested variable, fall back to scanning row.
		if hasTargetIdx && targetIdx < len(row) {
			node, ok := row[targetIdx].(*storage.Node)
			if ok && node != nil {
				for k, v := range propsForRow {
					setNodeProperty(node, k, v)
					result.Stats.PropertiesSet++
				}
				_ = store.UpdateNode(node)
				e.notifyNodeMutated(string(node.ID))
				updatedNodes = append(updatedNodes, node)
				continue
			}
		}

		for _, val := range row {
			node, ok := val.(*storage.Node)
			if !ok || node == nil {
				continue
			}

			// Merge properties (new values override existing)
			for k, v := range propsForRow {
				setNodeProperty(node, k, v)
				result.Stats.PropertiesSet++
			}
			_ = store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
			updatedNodes = append(updatedNodes, node)
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}

		// Return updated nodes (Neo4j compatible: return *storage.Node)
		for _, storageNode := range updatedNodes {
			newRow := make([]interface{}, len(returnItems))
			for j, item := range returnItems {
				newRow[j] = e.resolveReturnItem(ctx, item, variable, storageNode)
			}
			result.Rows = append(result.Rows, newRow)
		}
	} else {
		// No RETURN clause - return matched count
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{len(matchResult.Rows)}}
	}

	return result, nil
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
				return nil, fmt.Errorf("SET += %s must be a map with string keys, got key type %T", source, k)
			}
			propsMap[keyStr] = normalizePropValue(v)
		}
		return propsMap, nil
	}
	return nil, fmt.Errorf("SET += %s must be a map, got type %T", source, value)
}

func normalizePropValue(value interface{}) interface{} {
	switch v := value.(type) {
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
		return nil, fmt.Errorf("REMOVE requires a MATCH clause first (e.g., MATCH (n) REMOVE n.property)")
	}

	// Execute the match first
	matchQuery := normalized[matchIdx:removeIdx] + " RETURN *"
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Parse REMOVE clause: REMOVE n.prop1, n.prop2, n:Label
	var removePart string
	removeLen := len("REMOVE")
	if returnIdx > 0 && returnIdx > removeIdx {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen : returnIdx])
	} else {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen:])
	}

	// Split by comma and parse property and label removals.
	propsToRemove, labelsToRemove := e.parseRemoveItems(removePart)

	// Update matched nodes
	for _, row := range matchResult.Rows {
		for _, val := range row {
			node, ok := val.(*storage.Node)
			if !ok || node == nil {
				continue
			}
			invalidated := false
			// Remove specified properties
			for _, prop := range propsToRemove {
				if _, exists := node.Properties[prop]; exists {
					delete(node.Properties, prop)
					result.Stats.PropertiesSet++ // Neo4j counts removals as properties set
					if !embeddingutil.IsMetadataPropertyKey(prop) {
						invalidated = true
					}
				}
			}
			if invalidated {
				embeddingutil.InvalidateManagedEmbeddings(node)
			}
			if len(labelsToRemove) > 0 {
				oldLabels := make([]string, len(node.Labels))
				copy(oldLabels, node.Labels)
				next, removed := removeNodeLabels(node.Labels, labelsToRemove)
				if removed > 0 {
					node.Labels = next
					// Validate policy constraints before committing the label change.
					if err := validatePolicyOnLabelChange(store, node, oldLabels); err != nil {
						node.Labels = oldLabels // restore
						return nil, err
					}
				}
			}
			if err := store.UpdateNode(node); err != nil {
				return nil, err
			}
			e.notifyNodeMutated(string(node.ID))
		}
	}

	// Handle RETURN
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
		// Return updated nodes
		for _, row := range matchResult.Rows {
			for _, val := range row {
				node, ok := val.(*storage.Node)
				if !ok || node == nil {
					continue
				}
				resultRow := make([]interface{}, len(returnItems))
				for i, item := range returnItems {
					resultRow[i] = e.resolveReturnItem(ctx, item, "n", node)
				}
				result.Rows = append(result.Rows, resultRow)
			}
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
		if colonIdx := strings.Index(part, ":"); colonIdx >= 0 {
			labelExpr := strings.TrimSpace(part[colonIdx+1:])
			if labelExpr == "" {
				continue
			}
			for _, label := range strings.Split(labelExpr, ":") {
				label = strings.TrimSpace(label)
				if label != "" {
					labels = append(labels, label)
				}
			}
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
	propsToRemove, labelsToRemove := e.parseRemoveItems(removePart)
	for _, row := range matchResult.Rows {
		for _, val := range row {
			node, ok := val.(*storage.Node)
			if !ok || node == nil {
				continue
			}
			invalidated := false
			for _, prop := range propsToRemove {
				if _, exists := node.Properties[prop]; exists {
					delete(node.Properties, prop)
					result.Stats.PropertiesSet++
					if !embeddingutil.IsMetadataPropertyKey(prop) {
						invalidated = true
					}
				}
			}
			if len(labelsToRemove) > 0 {
				oldLabels := make([]string, len(node.Labels))
				copy(oldLabels, node.Labels)
				next, removed := removeNodeLabels(node.Labels, labelsToRemove)
				if removed > 0 {
					node.Labels = next
					if err := validatePolicyOnLabelChange(store, node, oldLabels); err != nil {
						node.Labels = oldLabels // restore
						return err
					}
				}
			}
			if invalidated {
				embeddingutil.InvalidateManagedEmbeddings(node)
			}
			if err := store.UpdateNode(node); err != nil {
				return err
			}
			e.notifyNodeMutated(string(node.ID))
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
	runeToByteIndex := make([]int, runeLen+1)
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
		upperPart := strings.ToUpper(part)
		asIdx := strings.Index(upperPart, " AS ")
		if asIdx > 0 {
			item.expr = strings.TrimSpace(part[:asIdx])
			item.alias = strings.TrimSpace(part[asIdx+4:])
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

// evaluateExistsSubquery checks if an EXISTS { } subquery returns any matches
// Syntax: EXISTS { MATCH (node)<-[:TYPE]-(other) }
func (e *StorageExecutor) evaluateExistsSubquery(ctx context.Context, node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from EXISTS { ... }
	subquery := e.extractSubquery(whereClause, "EXISTS")
	if subquery == "" {
		return true // No valid subquery, pass through
	}

	// Execute the subquery with the current node as context
	return e.checkSubqueryMatch(ctx, node, variable, subquery)
}

// evaluateNotExistsSubquery checks if a NOT EXISTS { } subquery returns no matches
func (e *StorageExecutor) evaluateNotExistsSubquery(ctx context.Context, node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from NOT EXISTS { ... }
	subquery := e.extractSubquery(whereClause, "NOT EXISTS")
	if subquery == "" {
		return true // No valid subquery, pass through
	}

	// Return true if no matches found
	return !e.checkSubqueryMatch(ctx, node, variable, subquery)
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

// extractCollectSubquery extracts the subquery body from COLLECT { ... }
func (e *StorageExecutor) extractCollectSubquery(expr string) string {
	// Find "collect" (case-insensitive)
	upperExpr := strings.ToUpper(expr)
	collectIdx := strings.Index(upperExpr, "COLLECT")
	if collectIdx < 0 {
		return ""
	}

	// Find the opening brace after COLLECT
	rest := expr[collectIdx+7:] // Skip "COLLECT"
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

// evaluateCollectSubquery executes a COLLECT { } subquery for a given node and returns collected values
func (e *StorageExecutor) evaluateCollectSubquery(ctx context.Context, node *storage.Node, variable, subquery string) ([]interface{}, error) {
	// Extract the subquery body from COLLECT { ... }
	subqueryBody := e.extractCollectSubquery(subquery)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid COLLECT subquery syntax")
	}

	// The subquery body should be a complete query like:
	// MATCH (p)-[:KNOWS]->(friend) RETURN friend.name
	// We need to execute it with the node bound to the variable.
	// We'll add a WHERE clause to bind the variable to the node ID.
	// Format: MATCH (p)-[:KNOWS]->(friend) WHERE id(p) = nodeID RETURN friend.name

	// Find WHERE clause position (if any)
	whereIdx := findKeywordIndex(subqueryBody, "WHERE")
	returnIdx := findKeywordIndex(subqueryBody, "RETURN")

	var substitutedQuery string
	if whereIdx > 0 && whereIdx < returnIdx {
		// WHERE clause exists - add id() check to it
		whereClause := strings.TrimSpace(subqueryBody[whereIdx+5 : returnIdx])
		beforeWhere := strings.TrimSpace(subqueryBody[:whereIdx])
		afterReturn := subqueryBody[returnIdx:]
		// Add id() check: WHERE id(variable) = nodeID AND existing_where_clause
		newWhere := fmt.Sprintf("WHERE id(%s) = '%s' AND %s", variable, string(node.ID), whereClause)
		substitutedQuery = strings.TrimSpace(beforeWhere + " " + newWhere + " " + afterReturn)
	} else if returnIdx > 0 {
		// No WHERE clause - add one before RETURN
		beforeReturn := subqueryBody[:returnIdx]
		afterReturn := subqueryBody[returnIdx:]
		// Add WHERE clause: WHERE id(variable) = nodeID
		newWhere := fmt.Sprintf(" WHERE id(%s) = '%s'", variable, string(node.ID))
		substitutedQuery = beforeReturn + newWhere + afterReturn
	} else {
		// No RETURN clause - this shouldn't happen, but handle it
		return nil, fmt.Errorf("COLLECT subquery must have a RETURN clause")
	}

	// Execute the subquery
	subqueryResult, err := e.executeInternal(ctx, substitutedQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("COLLECT subquery execution failed: %w", err)
	}

	// Collect all values from the first column of the subquery result
	collected := make([]interface{}, 0, len(subqueryResult.Rows))
	for _, row := range subqueryResult.Rows {
		if len(row) > 0 {
			collected = append(collected, row[0])
		}
	}

	return collected, nil
}

// substituteNodeInSubquery substitutes a node variable in a subquery with its actual ID
// Example: MATCH (p)-[:KNOWS]->(friend) RETURN friend.name
//
//	where p is bound to a node -> MATCH (nodeID)-[:KNOWS]->(friend) RETURN friend.name
func (e *StorageExecutor) substituteNodeInSubquery(subquery, variable string, node *storage.Node) string {
	// Replace (variable) or (variable:Label) patterns with the actual node ID
	// We need to be careful to only replace node patterns, not property accesses
	result := subquery

	// Pattern 1: (variable) -> (nodeID)
	// Use word boundaries to avoid matching variable names that are substrings
	pattern1 := regexp.MustCompile(`\(` + regexp.QuoteMeta(variable) + `\)`)
	replacement1 := "(" + string(node.ID) + ")"
	result = pattern1.ReplaceAllString(result, replacement1)

	// Pattern 2: (variable:Label) -> (nodeID:Label)
	// This preserves the label
	labelPattern := regexp.MustCompile(`\(` + regexp.QuoteMeta(variable) + `:([^)]+)\)`)
	result = labelPattern.ReplaceAllStringFunc(result, func(match string) string {
		// Extract the label part
		labelMatch := regexp.MustCompile(`:` + `([^)]+)`).FindStringSubmatch(match)
		if len(labelMatch) > 1 {
			return "(" + string(node.ID) + ":" + labelMatch[1] + ")"
		}
		return "(" + string(node.ID) + ")"
	})

	return result
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
	var checkIncoming, checkOutgoing bool
	var relTypes []string
	if strings.Contains(pattern, "<-[") {
		checkIncoming = true
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}
	if strings.Contains(pattern, "]->(") || strings.Contains(pattern, "]->") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}
	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				return true
			}
		}
	}
	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
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

// checkSubqueryMatch checks if the subquery matches for a given node
func (e *StorageExecutor) checkSubqueryMatch(ctx context.Context, node *storage.Node, variable, subquery string) bool {
	// Parse the MATCH pattern from the subquery
	// Format: MATCH (var)<-[:TYPE]-(other) WHERE ...
	upperSub := strings.ToUpper(subquery)

	if !strings.HasPrefix(upperSub, "MATCH ") {
		return false
	}

	// Split out any WHERE clause from the pattern
	pattern := strings.TrimSpace(subquery[6:])
	innerWhere := ""

	// Use regex to find WHERE with any whitespace before it (including newlines)
	whereRe := regexp.MustCompile(`(?i)\s+WHERE\s+`)
	if loc := whereRe.FindStringIndex(pattern); loc != nil {
		innerWhere = strings.TrimSpace(pattern[loc[1]:])
		pattern = strings.TrimSpace(pattern[:loc[0]])
	}

	// Check if pattern references our variable
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return false
	}

	// Check for chained relationship pattern (e.g., (p)-[:KNOWS]->()-[:KNOWS]->())
	// Count the number of relationship hops by counting relationship brackets [-
	// Each hop has one -[...]-
	relationshipCount := strings.Count(pattern, "-[")
	if relationshipCount > 1 {
		return e.checkChainedPattern(node, variable, pattern, innerWhere)
	}

	// Extract the target variable name from pattern (e.g., "report" from "(m)-[:MANAGES]->(report)")
	targetVar := e.extractTargetVariable(pattern, variable)

	// Parse relationship pattern
	// Simplified: check for incoming or outgoing relationships
	var checkIncoming, checkOutgoing bool
	var relTypes []string

	if strings.Contains(pattern, "<-[") {
		checkIncoming = true
		// Extract relationship type if specified
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}
	if strings.Contains(pattern, "]->(") || strings.Contains(pattern, "]->") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}

	// Check for matching edges
	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				// If there's an inner WHERE, check it against the connected node
				// Only evaluate WHERE if we have a target variable (otherwise we can't match properties)
				if innerWhere != "" && targetVar != "" {
					sourceNode, err := e.storage.GetNode(edge.StartNode)
					if err != nil || !e.evaluateInnerWhere(ctx, sourceNode, targetVar, innerWhere) {
						continue
					}
				} else if innerWhere != "" && targetVar == "" {
					// If we have a WHERE clause but no target variable, we can't evaluate it
					// This means the pattern doesn't have a named target, so skip this edge
					continue
				}
				return true
			}
		}
	}

	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				// If there's an inner WHERE, check it against the connected node
				// Only evaluate WHERE if we have a target variable (otherwise we can't match properties)
				if innerWhere != "" && targetVar != "" {
					targetNode, err := e.storage.GetNode(edge.EndNode)
					if err != nil || !e.evaluateInnerWhere(ctx, targetNode, targetVar, innerWhere) {
						continue
					}
				} else if innerWhere != "" && targetVar == "" {
					// If we have a WHERE clause but no target variable, we can't evaluate it
					// This means the pattern doesn't have a named target, so skip this edge
					continue
				}
				return true
			}
		}
	}

	// If no direction specified, check both
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

// evaluateInnerWhere evaluates an inner WHERE clause against a node
// Handles nested EXISTS subqueries and property comparisons
func (e *StorageExecutor) evaluateInnerWhere(ctx context.Context, node *storage.Node, variable, whereClause string) bool {
	whereClause = strings.TrimSpace(whereClause)
	upperWhere := strings.ToUpper(whereClause)

	// Handle parenthesized expressions - strip outer parens and recurse
	if strings.HasPrefix(whereClause, "(") && strings.HasSuffix(whereClause, ")") {
		// Verify these are matching outer parens, not separate groups
		depth := 0
		isOuterParen := true
		for i, ch := range whereClause {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			// If depth goes to 0 before the last char, these aren't outer parens
			if depth == 0 && i < len(whereClause)-1 {
				isOuterParen = false
				break
			}
		}
		if isOuterParen {
			return e.evaluateInnerWhere(ctx, node, variable, whereClause[1:len(whereClause)-1])
		}
	}

	// Handle AND/OR at top level
	if andIdx := findTopLevelKeyword(whereClause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateInnerWhere(ctx, node, variable, left) && e.evaluateInnerWhere(ctx, node, variable, right)
	}

	if orIdx := findTopLevelKeyword(whereClause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(whereClause[:orIdx])
		right := strings.TrimSpace(whereClause[orIdx+4:])
		return e.evaluateInnerWhere(ctx, node, variable, left) || e.evaluateInnerWhere(ctx, node, variable, right)
	}

	// Check for nested EXISTS subquery
	if hasSubqueryPattern(whereClause, existsSubqueryRe) {
		// Check for NOT EXISTS first
		if hasSubqueryPattern(whereClause, notExistsSubqueryRe) {
			return e.evaluateNotExistsSubquery(ctx, node, variable, whereClause)
		}
		return e.evaluateExistsSubquery(ctx, node, variable, whereClause)
	}

	// Check for nested COUNT subquery
	if hasSubqueryPattern(whereClause, countSubqueryRe) {
		return e.evaluateCountSubqueryComparison(node, variable, whereClause)
	}

	// Handle NOT prefix
	if strings.HasPrefix(upperWhere, "NOT ") {
		inner := strings.TrimSpace(whereClause[4:])
		return !e.evaluateInnerWhere(ctx, node, variable, inner)
	}

	// Handle string operators
	if strings.Contains(upperWhere, " CONTAINS ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "CONTAINS")
	}
	if strings.Contains(upperWhere, " STARTS WITH ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "STARTS WITH")
	}
	if strings.Contains(upperWhere, " ENDS WITH ") {
		return e.evaluateStringOp(ctx, node, variable, whereClause, "ENDS WITH")
	}
	if strings.Contains(upperWhere, " IN ") {
		return e.evaluateInOp(ctx, node, variable, whereClause)
	}

	// Handle IS NULL / IS NOT NULL
	if strings.Contains(upperWhere, " IS NULL") {
		return e.evaluateIsNull(ctx, node, variable, whereClause, false)
	}
	if strings.Contains(upperWhere, " IS NOT NULL") {
		return e.evaluateIsNull(ctx, node, variable, whereClause, true)
	}

	// Determine operator and split accordingly
	var op string
	var opIdx int

	// Check operators in order of length (longest first to avoid partial matches)
	operators := []string{"<>", "!=", ">=", "<=", "=~", ">", "<", "="}
	for _, testOp := range operators {
		idx := strings.Index(whereClause, testOp)
		if idx >= 0 {
			op = testOp
			opIdx = idx
			break
		}
	}

	if op == "" {
		// No valid operator found - check if clause is empty/whitespace
		trimmed := strings.TrimSpace(whereClause)
		if trimmed == "" {
			// Empty WHERE clause means no filter - include all
			return true
		}
		// Non-empty clause with no recognized operator - cannot evaluate properly
		// Return false (exclude) rather than true (include all) for safety
		// This prevents incorrect results from malformed or unsupported WHERE clauses
		return false
	}

	left := strings.TrimSpace(whereClause[:opIdx])
	right := strings.TrimSpace(whereClause[opIdx+len(op):])

	// Handle id(variable) = value comparisons
	lowerLeft := strings.ToLower(left)
	if strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")") {
		// Extract variable name from id(varName)
		idVar := strings.TrimSpace(left[3 : len(left)-1])
		if idVar == variable {
			// Compare node ID with expected value
			expectedVal := e.parseValue(ctx, right)
			actualId := string(node.ID)

			// Support both string and integer comparisons for Bolt protocol compatibility
			// If expected value is an integer (from Bolt Node structure), hash the string ID and compare
			if expectedInt, ok := expectedVal.(int64); ok {
				actualHash := util.HashStringToInt64(actualId)
				switch op {
				case "=":
					return actualHash == expectedInt
				case "<>", "!=":
					return actualHash != expectedInt
				default:
					return true
				}
			}

			// String comparison (original behavior)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

	// Handle elementId(variable) = value comparisons
	if strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")") {
		// Extract variable name from elementId(varName)
		idVar := strings.TrimSpace(left[10 : len(left)-1])
		if idVar == variable {
			// Compare node ID with expected value
			expectedVal := e.parseValue(ctx, right)
			actualId := string(node.ID)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

	// Extract property from left side (e.g., "n.name")
	// Use TrimSpace to handle whitespace around the dot
	varName := strings.TrimSpace(left)
	if !strings.HasPrefix(varName, variable+".") {
		// In EXISTS subqueries, if we can't evaluate the condition (variable doesn't match),
		// we should return false (exclude) rather than true (include all)
		// This ensures WHERE clauses in subqueries actually filter correctly
		return false // Not a property comparison we can handle for this variable
	}

	propName := strings.TrimSpace(varName[len(variable)+1:])

	// Get actual value
	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	// Parse the expected value from right side
	expectedVal := e.parseValue(ctx, right)

	// Perform comparison based on operator
	switch op {
	case "=":
		return e.compareEqual(actualVal, expectedVal)
	case "<>", "!=":
		return !e.compareEqual(actualVal, expectedVal)
	case ">":
		return e.compareGreater(actualVal, expectedVal)
	case ">=":
		return e.compareGreater(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
	case "<":
		return e.compareLess(actualVal, expectedVal)
	case "<=":
		return e.compareLess(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
	case "=~":
		return e.compareRegex(actualVal, expectedVal)
	default:
		return true
	}
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

// edgeTypeMatches checks if an edge type matches any of the allowed types
func (e *StorageExecutor) edgeTypeMatches(edgeType string, allowedTypes []string) bool {
	for _, t := range allowedTypes {
		if edgeType == t {
			return true
		}
	}
	return false
}

// evaluateCountSubqueryComparison evaluates COUNT { } subquery with comparison
// Syntax: COUNT { MATCH (node)-[:TYPE]->(other) } > 5
// Returns true if the comparison holds
func (e *StorageExecutor) evaluateCountSubqueryComparison(node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from COUNT { ... }
	subquery := e.extractSubquery(whereClause, "COUNT")
	if subquery == "" {
		return false // Malformed COUNT subquery
	}

	// Count matching relationships
	count := e.countSubqueryMatches(node, variable, subquery)

	// Extract and evaluate the comparison operator
	// Find the closing brace to get what comes after
	upperClause := strings.ToUpper(whereClause)
	countIdx := strings.Index(upperClause, "COUNT")
	if countIdx < 0 {
		return false
	}

	remaining := whereClause[countIdx:]
	braceDepth := 0
	closeIdx := -1
	for i := 0; i < len(remaining); i++ {
		if remaining[i] == '{' {
			braceDepth++
		} else if remaining[i] == '}' {
			braceDepth--
			if braceDepth == 0 {
				closeIdx = i
				break
			}
		}
	}

	if closeIdx == -1 {
		// No closing brace, invalid
		return false
	}

	// Get comparison part after COUNT { }
	comparison := strings.TrimSpace(remaining[closeIdx+1:])
	if comparison == "" {
		// No comparison, return true if count > 0
		return count > 0
	}

	// Parse comparison operator and value
	var op string
	var valueStr string

	if strings.HasPrefix(comparison, ">=") {
		op = ">="
		valueStr = strings.TrimSpace(comparison[2:])
	} else if strings.HasPrefix(comparison, "<=") {
		op = "<="
		valueStr = strings.TrimSpace(comparison[2:])
	} else if strings.HasPrefix(comparison, ">") {
		op = ">"
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "<") {
		op = "<"
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "=") {
		op = "="
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "!=") || strings.HasPrefix(comparison, "<>") {
		op = "!="
		if strings.HasPrefix(comparison, "!=") {
			valueStr = strings.TrimSpace(comparison[2:])
		} else {
			valueStr = strings.TrimSpace(comparison[2:])
		}
	} else {
		// No valid operator, treat as > 0
		return count > 0
	}

	// Parse the comparison value
	var compareValue int64
	_, err := fmt.Sscanf(valueStr, "%d", &compareValue)
	if err != nil {
		// Invalid number, treat as false
		return false
	}

	// Perform comparison
	switch op {
	case ">":
		return count > compareValue
	case ">=":
		return count >= compareValue
	case "<":
		return count < compareValue
	case "<=":
		return count <= compareValue
	case "=":
		return count == compareValue
	case "!=":
		return count != compareValue
	default:
		return false
	}
}

// countSubqueryMatches counts how many matches a subquery produces
func (e *StorageExecutor) countSubqueryMatches(node *storage.Node, variable, subquery string) int64 {
	// Parse the MATCH pattern from the subquery
	upperSub := strings.ToUpper(subquery)

	if !strings.HasPrefix(upperSub, "MATCH ") {
		return 0
	}

	pattern := strings.TrimSpace(subquery[6:])

	// Check if pattern references our variable
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return 0
	}

	// Parse relationship pattern
	var checkIncoming, checkOutgoing bool
	var relTypes []string

	// Variable on left side of "<-[" means incoming to variable.
	if strings.Contains(pattern, "("+variable+")<-[") || strings.Contains(pattern, "("+variable+":") && strings.Contains(pattern, "<-[") {
		checkIncoming = true
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}
	// Variable on left side of "-[" means outgoing from variable.
	if strings.Contains(pattern, "("+variable+")-[") || strings.Contains(pattern, "("+variable+":") && strings.Contains(pattern, ")-[") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}

	// Variable on right side of "]->" means incoming to variable.
	if strings.Contains(pattern, "]->("+variable+")") || strings.Contains(pattern, "]->("+variable+":") {
		checkIncoming = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}
	// Variable on right side of "]-(...)" in an incoming-arrow pattern means outgoing from variable:
	// e.g. ()<-[r]-(n)
	if (strings.Contains(pattern, "]-("+variable+")") || strings.Contains(pattern, "]-("+variable+":")) &&
		strings.Contains(pattern, "<-[") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}

	// Count matching edges
	var count int64

	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				count++
			}
		}
	}

	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				count++
			}
		}
	}

	// If no direction specified, count both
	if !checkIncoming && !checkOutgoing {
		incoming, _ := e.storage.GetIncomingEdges(node.ID)
		outgoing, _ := e.storage.GetOutgoingEdges(node.ID)
		count = int64(len(incoming) + len(outgoing))
	}

	return count
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
				return fmt.Errorf("policy constraint %q violated: (%s)-[:%s]->(%s) is DISALLOWED",
					p.Name, p.SourceLabel, edgeType, p.TargetLabel)
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
			return fmt.Errorf("policy constraint violated: no ALLOWED policy permits edge of type %s with these endpoint labels", edgeType)
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
