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

// replaceDeletedEntityViews replaces, in rows, every node and relationship the
// statement deleted (the rows' deleted sets) with what Neo4j 5.26 returns for
// it after the DELETE: the same entity without labels or properties, so
// RETURN n is an empty node and n {.*}, properties(n) and keys(n) are empty.
// A relationship keeps its type and endpoints (type(r) still answers). Paths
// and lists holding a deleted entity are rebuilt with its view (#907).
// Reading a deleted entity's property or labels is an error instead
// (validateDeletedEntityProjection).
func (e *StorageExecutor) replaceDeletedEntityViews(rows []pipelineRow) {
	views := deletedEntityViews{executor: e}
	for _, row := range rows {
		views.nodes, _ = row[pipelineDeletedNodesKey].(map[storage.NodeID]struct{})
		views.edges, _ = row[pipelineDeletedEdgesKey].(map[storage.EdgeID]struct{})
		if len(views.nodes) == 0 && len(views.edges) == 0 {
			continue
		}
		for name, value := range row {
			if name == pipelineDeletedNodesKey || name == pipelineDeletedEdgesKey {
				continue
			}
			if replaced, changed := views.replace(value); changed {
				row[name] = replaced
			}
		}
	}
}

// deletedEntityViews builds the views of replaceDeletedEntityViews, one per
// deleted entity, shared by every row that holds it.
type deletedEntityViews struct {
	executor  *StorageExecutor
	nodes     map[storage.NodeID]struct{}
	edges     map[storage.EdgeID]struct{}
	nodeViews map[storage.NodeID]*storage.Node
	edgeViews map[storage.EdgeID]*storage.Edge
}

// replace returns value with its deleted entities replaced by their views,
// and whether anything was replaced.
func (v *deletedEntityViews) replace(value interface{}) (interface{}, bool) {
	switch typed := value.(type) {
	case *storage.Node:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.nodes[typed.ID]; deleted {
			return v.node(typed), true
		}
	case *storage.Edge:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.edges[typed.ID]; deleted {
			return v.edge(typed), true
		}
	case []interface{}:
		var replaced []interface{}
		for index, item := range typed {
			if view, changed := v.replace(item); changed {
				if replaced == nil {
					replaced = append([]interface{}(nil), typed...)
				}
				replaced[index] = view
			}
		}
		if replaced != nil {
			return replaced, true
		}
	case map[string]interface{}:
		var path PathResult
		switch result := typed["_pathResult"].(type) {
		case PathResult:
			path = result
		case *PathResult:
			if result == nil {
				return value, false
			}
			path = *result
		default:
			return value, false
		}
		changed := false
		nodes := make([]*storage.Node, len(path.Nodes))
		for index, node := range path.Nodes {
			nodes[index] = node
			if view, replaced := v.replace(node); replaced {
				nodes[index], changed = view.(*storage.Node), true
			}
		}
		relationships := make([]*storage.Edge, len(path.Relationships))
		for index, relationship := range path.Relationships {
			relationships[index] = relationship
			if view, replaced := v.replace(relationship); replaced {
				relationships[index], changed = view.(*storage.Edge), true
			}
		}
		if changed {
			path.Nodes, path.Relationships = nodes, relationships
			return v.executor.pathToMap(path), true
		}
	}
	return value, false
}

func (v *deletedEntityViews) node(node *storage.Node) *storage.Node {
	if view, built := v.nodeViews[node.ID]; built {
		return view
	}
	if v.nodeViews == nil {
		v.nodeViews = make(map[storage.NodeID]*storage.Node)
	}
	view := &storage.Node{ID: node.ID, Properties: map[string]interface{}{}}
	v.nodeViews[node.ID] = view
	return view
}

func (v *deletedEntityViews) edge(edge *storage.Edge) *storage.Edge {
	if view, built := v.edgeViews[edge.ID]; built {
		return view
	}
	if v.edgeViews == nil {
		v.edgeViews = make(map[storage.EdgeID]*storage.Edge)
	}
	view := &storage.Edge{ID: edge.ID, Type: edge.Type, StartNode: edge.StartNode, EndNode: edge.EndNode, Properties: map[string]interface{}{}}
	v.edgeViews[edge.ID] = view
	return view
}

// validateDeletedEntityProjection rejects a RETURN that reads a property or
// the labels of a node or relationship the statement deleted, or the keys of
// a deleted relationship, anywhere in an item (n.p + 1 too): Neo4j 5.26's
// EntityNotFound. keys and properties of a deleted node, and n {.*}, are
// empty instead (replaceDeletedEntityViews).
func validateDeletedEntityProjection(rows []pipelineRow, clause string) error {
	for _, expression := range projectionExpressions(clause, "RETURN") {
		reads := deletedEntityReadsIn(expression)
		if len(reads) == 0 {
			continue
		}
		for _, row := range rows {
			for _, read := range reads {
				if rowReferencesDeletedEntity(row, read.variable, read.keys) {
					return newSemanticError(
						"Neo.ClientError.Statement.EntityNotFound",
						"DeletedEntityAccess",
						"cannot access properties or labels of a deleted entity",
					)
				}
			}
		}
	}
	return nil
}

// deletedEntityRead is a variable an expression reads a property or the
// labels of (keys false), or the keys of (keys true).
type deletedEntityRead struct {
	variable string
	keys     bool
}

// deletedEntityReadsIn returns the variables expression reads a property, the
// labels or the keys of, outside string literals: v.p, labels(v), keys(v).
func deletedEntityReadsIn(expression string) []deletedEntityRead {
	var reads []deletedEntityRead
	for index := 0; index < len(expression); {
		switch character := expression[index]; {
		case character == '\'' || character == '"' || character == '`':
			end := strings.IndexByte(expression[index+1:], character)
			if end < 0 {
				return reads
			}
			index += end + 2
			continue
		case !isCypherIdentByte(character) || (index > 0 && (isCypherIdentByte(expression[index-1]) || expression[index-1] == '.')):
			index++
			continue
		}
		start := index
		for index < len(expression) && isCypherIdentByte(expression[index]) {
			index++
		}
		word := expression[start:index]
		next := skipSpaceIndex(expression, index)
		switch {
		case next < len(expression) && expression[next] == '.' && next+1 < len(expression) && expression[next+1] != '.':
			reads = append(reads, deletedEntityRead{variable: word})
		case next < len(expression) && expression[next] == '(' && (strings.EqualFold(word, "labels") || strings.EqualFold(word, "keys")):
			argumentStart := skipSpaceIndex(expression, next+1)
			argumentEnd := argumentStart
			for argumentEnd < len(expression) && isCypherIdentByte(expression[argumentEnd]) {
				argumentEnd++
			}
			if close := skipSpaceIndex(expression, argumentEnd); argumentEnd > argumentStart && close < len(expression) && expression[close] == ')' {
				reads = append(reads, deletedEntityRead{variable: expression[argumentStart:argumentEnd], keys: strings.EqualFold(word, "keys")})
			}
		}
	}
	return reads
}

func skipSpaceIndex(text string, index int) int {
	for index < len(text) && isASCIISpace(text[index]) {
		index++
	}
	return index
}

// rowReferencesDeletedEntity reports whether row binds variable to a node or
// relationship the statement deleted. For a keys() read (keys) only a
// relationship counts: the keys of a deleted node are empty.
func rowReferencesDeletedEntity(row pipelineRow, variable string, keys bool) bool {
	switch entity := row[variable].(type) {
	case *storage.Node:
		if keys {
			return false
		}
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
