package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) tryExecuteBoundRelationshipDelete(
	ctx context.Context,
	matchSegment string,
	cypher string,
	deleteVars string,
	detach bool,
) (*ExecuteResult, bool, error) {
	matchSegment = strings.TrimSpace(matchSegment)
	_, inTransactionWrapper := e.getStorage(ctx).(*transactionStorageWrapper)
	if detach || strings.Contains(deleteVars, ",") {
		return nil, false, nil
	}
	deleteVar := strings.TrimSpace(deleteVars)
	if deleteVar == "" {
		return nil, false, nil
	}
	limit := -1
	baseMatchSegment := matchSegment
	if !isSimpleBoundRelationshipDeleteMatchSegment(matchSegment) {
		limitedSegment, limitedRows, ok := parseBoundRelationshipDeleteWithLimitSegment(matchSegment, deleteVar)
		if !ok {
			return nil, false, nil
		}
		baseMatchSegment = limitedSegment
		limit = limitedRows
	}

	pseudo := baseMatchSegment + " RETURN __delete_probe__"
	returnIdx := findKeywordIndex(pseudo, "RETURN")
	if returnIdx <= 0 {
		return nil, false, nil
	}
	whereIdx := lastKeywordIndexBefore(pseudo, "WHERE", returnIdx)
	clauses := splitMatchClauses(pseudo, whereIdx, returnIdx)
	if len(clauses) != 2 {
		return nil, false, nil
	}

	sourcePattern := e.parseNodePattern(ctx, clauses[0])
	if sourcePattern.variable == "" {
		return nil, false, nil
	}
	relPatternText := strings.TrimSpace(clauses[1])
	relMatch := e.parseTraversalPattern(ctx, relPatternText)
	if relMatch == nil || relMatch.IsChained {
		return nil, false, nil
	}
	if relMatch.StartNode.variable != sourcePattern.variable {
		return nil, false, nil
	}
	if relMatch.Relationship.Variable != deleteVar {
		return nil, false, nil
	}
	if relMatch.Relationship.MinHops != 1 || relMatch.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	if relMatch.Relationship.Direction != "outgoing" && relMatch.Relationship.Direction != "incoming" && relMatch.Relationship.Direction != "both" {
		return nil, false, nil
	}

	whereClause := ""
	if whereIdx > 0 && whereIdx < returnIdx {
		whereClause = strings.TrimSpace(pseudo[whereIdx+len("WHERE") : returnIdx])
	}

	sources, err := e.collectBoundRelationshipDeleteSources(ctx, sourcePattern, inTransactionWrapper)
	if err != nil {
		return nil, true, err
	}
	if len(sources) == 0 {
		result := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
		e.applyDeleteReturnProjection(result, cypher, deleteVar)
		return result, true, nil
	}

	typeSet := buildRelTypeSet(relMatch.Relationship.Types)
	store := e.getStorage(ctx)
	edgeIDs := make([]storage.EdgeID, 0)
	seen := make(map[storage.EdgeID]struct{})
	for _, source := range sources {
		if source == nil {
			continue
		}
		candidates, err := e.boundRelationshipDeleteCandidateEdges(store, source.ID, relMatch.Relationship.Direction)
		if err != nil {
			return nil, true, err
		}
		for _, edge := range candidates {
			if edge == nil || !relationshipTypeAllowed(edge.Type, relMatch.Relationship.Types, typeSet) {
				continue
			}
			if len(relMatch.Relationship.Properties) > 0 && !e.edgeMatchesProps(edge, relMatch.Relationship.Properties) {
				continue
			}
			target, ok, err := e.boundRelationshipDeleteTargetNode(store, source.ID, edge, relMatch.Relationship.Direction)
			if err != nil {
				return nil, true, err
			}
			if !ok || !e.matchesEndPattern(target, &relMatch.EndNode) {
				continue
			}
			if whereClause != "" {
				pathCtx := PathContext{
					nodes: map[string]*storage.Node{
						relMatch.StartNode.variable: source,
					},
					rels: map[string]*storage.Edge{
						deleteVar: edge,
					},
				}
				if relMatch.EndNode.variable != "" {
					pathCtx.nodes[relMatch.EndNode.variable] = target
				}
				if !e.evaluateWhereOnPath(ctx, whereClause, pathCtx) {
					continue
				}
			}
			if _, exists := seen[edge.ID]; exists {
				continue
			}
			seen[edge.ID] = struct{}{}
			edgeIDs = append(edgeIDs, edge.ID)
			if limit > 0 && len(edgeIDs) >= limit {
				break
			}
		}
		if limit > 0 && len(edgeIDs) >= limit {
			break
		}
	}

	result := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	if len(edgeIDs) > 0 {
		if err := store.BulkDeleteEdges(edgeIDs); err != nil {
			return nil, true, err
		}
		result.Stats.RelationshipsDeleted = len(edgeIDs)
	}
	e.applyDeleteReturnProjection(result, cypher, deleteVar)
	return result, true, nil
}

func (e *StorageExecutor) collectBoundRelationshipDeleteSources(ctx context.Context, sourcePattern nodePatternInfo, inTransactionWrapper bool) ([]*storage.Node, error) {
	if !inTransactionWrapper {
		return e.collectNodesWithStreaming(ctx, sourcePattern.labels, sourcePattern.properties, sourcePattern.variable, "", -1)
	}

	store := e.getStorage(ctx)
	var nodes []*storage.Node
	var err error
	if len(sourcePattern.labels) > 0 {
		nodes, err = store.GetNodesByLabel(sourcePattern.labels[0])
	} else {
		nodes, err = store.AllNodes()
	}
	if err != nil {
		return nil, err
	}

	filtered := make([]*storage.Node, 0, len(nodes))
	hideSystemNodes := shouldHideSystemNodes(store)
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if hideSystemNodes && isSystemNode(node) {
			continue
		}
		if len(sourcePattern.labels) > 0 && !mergeNodeHasLabels(node, sourcePattern.labels) {
			continue
		}
		if len(sourcePattern.properties) > 0 && !e.nodeMatchesProps(node, sourcePattern.properties) {
			continue
		}
		filtered = append(filtered, node)
	}
	return filtered, nil
}

func (e *StorageExecutor) boundRelationshipDeleteCandidateEdges(store storage.Engine, nodeID storage.NodeID, direction string) ([]*storage.Edge, error) {
	var edges []*storage.Edge
	switch direction {
	case "outgoing":
		outgoing, err := store.GetOutgoingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		edges = append(edges, outgoing...)
	case "incoming":
		incoming, err := store.GetIncomingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		edges = append(edges, incoming...)
	case "both":
		outgoing, err := store.GetOutgoingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		incoming, err := store.GetIncomingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		edges = append(edges, outgoing...)
		edges = append(edges, incoming...)
	}
	return edges, nil
}

func parseBoundRelationshipDeleteWithLimitSegment(matchSegment, deleteVar string) (string, int, bool) {
	for _, blocked := range []string{"ORDER BY", "SKIP", "CALL", "UNWIND", "OPTIONAL MATCH"} {
		if containsKeywordOutsideStrings(matchSegment, blocked) {
			return "", 0, false
		}
	}
	withIdx := findKeywordIndex(matchSegment, "WITH")
	if withIdx <= 0 {
		return "", 0, false
	}
	baseMatch := strings.TrimSpace(matchSegment[:withIdx])
	if !isSimpleBoundRelationshipDeleteMatchSegment(baseMatch) {
		return "", 0, false
	}
	withPart := strings.TrimSpace(matchSegment[withIdx+len("WITH"):])
	limitIdx := findKeywordIndex(withPart, "LIMIT")
	if limitIdx <= 0 {
		return "", 0, false
	}
	withVar := strings.TrimSpace(withPart[:limitIdx])
	if withVar != deleteVar {
		return "", 0, false
	}
	limitLiteral := strings.TrimSpace(withPart[limitIdx+len("LIMIT"):])
	fields := strings.Fields(limitLiteral)
	if len(fields) != 1 {
		return "", 0, false
	}
	limitN, err := strconv.Atoi(fields[0])
	if err != nil || limitN <= 0 {
		return "", 0, false
	}
	return baseMatch, limitN, true
}

func isSimpleBoundRelationshipDeleteMatchSegment(matchSegment string) bool {
	if strings.TrimSpace(matchSegment) == "" {
		return false
	}
	for _, blocked := range []string{"WITH", "LIMIT", "SKIP", "ORDER BY", "CALL", "UNWIND", "OPTIONAL MATCH"} {
		if containsKeywordOutsideStrings(matchSegment, blocked) {
			return false
		}
	}
	return true
}

func (e *StorageExecutor) boundRelationshipDeleteTargetNode(
	store storage.Engine,
	sourceID storage.NodeID,
	edge *storage.Edge,
	direction string,
) (*storage.Node, bool, error) {
	var targetID storage.NodeID
	switch direction {
	case "outgoing":
		if edge.StartNode != sourceID {
			return nil, false, nil
		}
		targetID = edge.EndNode
	case "incoming":
		if edge.EndNode != sourceID {
			return nil, false, nil
		}
		targetID = edge.StartNode
	case "both":
		if edge.StartNode == sourceID {
			targetID = edge.EndNode
		} else if edge.EndNode == sourceID {
			targetID = edge.StartNode
		} else {
			return nil, false, nil
		}
	default:
		return nil, false, nil
	}
	target, err := store.GetNode(targetID)
	if err != nil {
		return nil, false, err
	}
	if target == nil {
		return nil, false, nil
	}
	return target, true, nil
}

func relationshipTypeAllowed(edgeType string, allowed []string, allowedSet map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	if len(allowed) == 1 {
		return edgeType == allowed[0]
	}
	_, ok := allowedSet[edgeType]
	return ok
}
