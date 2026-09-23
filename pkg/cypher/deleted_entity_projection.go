package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	pipelineDeletedNodesKey = "\x00nornic.deleted.nodes"
	pipelineDeletedEdgesKey = "\x00nornic.deleted.edges"
)

func markPipelineRowsDeletedEntities(rows []pipelineRow, nodeIDs []storage.NodeID, edgeIDs map[storage.EdgeID]struct{}) {
	deletedNodes := make(map[storage.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		deletedNodes[id] = struct{}{}
	}
	for _, row := range rows {
		row[pipelineDeletedNodesKey] = deletedNodes
		row[pipelineDeletedEdgesKey] = edgeIDs
	}
}

func validateDeletedEntityProjection(rows []pipelineRow, clause string) error {
	body := strings.TrimSpace(clause[len("RETURN"):])
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	for _, item := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		variable := deletedEntityAccessVariable(expression)
		if variable == "" {
			continue
		}
		for _, row := range rows {
			if rowReferencesDeletedEntity(row, variable) {
				return newSemanticError(
					"Neo.ClientError.Statement.EntityNotFound",
					"DeletedEntityAccess",
					"cannot access properties or labels of a deleted entity",
				)
			}
		}
	}
	return nil
}

func deletedEntityAccessVariable(expression string) string {
	if variable, _, property := parseVarPropertyRef(strings.TrimSpace(expression)); property {
		return normalizeProjectionColumnName(variable)
	}
	function, argument, functionCall := parseFunctionCallWS(expression)
	if !functionCall {
		return ""
	}
	switch strings.ToLower(function) {
	case "labels", "properties", "keys":
		return simpleSemanticIdentifier(argument)
	default:
		return ""
	}
}

func rowReferencesDeletedEntity(row pipelineRow, variable string) bool {
	switch entity := row[variable].(type) {
	case *storage.Node:
		deleted, _ := row[pipelineDeletedNodesKey].(map[storage.NodeID]struct{})
		_, found := deleted[entity.ID]
		return found
	case *storage.Edge:
		deleted, _ := row[pipelineDeletedEdgesKey].(map[storage.EdgeID]struct{})
		_, found := deleted[entity.ID]
		return found
	default:
		return false
	}
}
