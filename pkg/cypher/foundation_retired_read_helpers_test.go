// Author: Lane #1644 (Codex). Historical helper-only tests retain these old
// algorithms. Production cannot call them: only the active error-carrying query
// owners are compiled into the product. The full production build proves it.
package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strconv"
	"strings"
)

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
		targetNode, err := e.getQueryNode(e.storage, targetNodeID)
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
				nextNode, err := e.getQueryNode(e.storage, edge.EndNode)
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
				nextNode, err := e.getQueryNode(e.storage, edge.StartNode)
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

func (e *StorageExecutor) getRelType(relID storage.EdgeID) string {
	edge, err := e.storage.GetEdge(relID)
	if err != nil || edge == nil {
		return ""
	}
	return edge.Type
}

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
		lookupID, validSource := e.identityLookup(store).NodeLookupID(node.Identity())
		if !validSource {
			continue
		}
		nodeID := string(lookupID)
		if _, seen := deletedNodeIDs[nodeID]; seen {
			continue
		}
		edgesCount := 0
		if needEdgeStats {
			outgoingEdges, err := store.GetOutgoingEdges(storage.NodeID(nodeID))
			if err != nil {
				return nil, true, err
			}
			incomingEdges, err := store.GetIncomingEdges(storage.NodeID(nodeID))
			if err != nil {
				return nil, true, err
			}
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
	if !patternMapsAreConstant(patternPart) || !patternMapsAreConstant(wherePart) {
		return nil, false, nil
	}
	nodePat, parseErr := readNodePattern(ctx, patternPart, patternPropertyKeys)
	if parseErr != nil {
		return nil, true, parseErr
	}
	if strings.TrimSpace(nodePat.variable) != deleteVar {
		return nil, false, nil
	}
	// Admit the WHERE shape and parameter type before reading property values.
	if wherePart != "" {
		equality := deleteWherePropertyEquals.FindStringSubmatch(wherePart)
		membership := deleteWherePropertyInParameter.FindStringSubmatch(wherePart)
		if len(equality) == 4 && strings.EqualFold(equality[1], deleteVar) {
			// The equality path accepts literal and parameter values.
		} else if len(membership) == 4 && strings.EqualFold(membership[1], deleteVar) {
			if raw, exists := params[membership[3]]; exists {
				switch raw.(type) {
				case []interface{}, []string:
				default:
					return nil, false, nil
				}
			}
		} else {
			return nil, false, nil
		}
	}
	rowCtx, prepared, prepareErr := e.bindPatternProperties(ctx, patternPart, nil)
	if prepareErr != nil {
		return nil, true, prepareErr
	}
	ctx = rowCtx
	nodePat, parseErr = e.parseNodePattern(ctx, prepared)
	if parseErr != nil {
		return nil, true, parseErr
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
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexIn(ctx, wherePartNodePattern(nodePat, deleteVar), wherePart, params); idxErr == nil && used {
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
			nodes, err = storage.GetNodesByLabelWithContext(ctx, e.storage, nodePat.labels[0])
		} else {
			nodes, err = storage.AllNodesWithContext(ctx, e.storage)
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
