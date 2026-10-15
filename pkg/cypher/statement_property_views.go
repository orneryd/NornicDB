package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"slices"
	"sync"
)

// Read-only statements can reuse immutable property projections. Write statements
// deliberately omit this memo: a mutation must immediately observe its new body.
// The executor clone, maps and scoring instant belong to one statement only.
type statementPropertyViews struct {
	mu         sync.Mutex
	projection storage.QueryResultProjection
	nodes      map[*storage.Node]*storage.Node
	edges      map[*storage.Edge]*storage.Edge
	nodeBodies map[storage.NodeID]nodePropertyViewBody
	edgeBodies map[storage.EdgeID]edgePropertyViewBody
}
type nodePropertyViewBody struct {
	source     *storage.Node
	properties map[string]interface{}
}
type edgePropertyViewBody struct {
	source     *storage.Edge
	properties map[string]interface{}
}

func (e *StorageExecutor) withStatementPropertyViews(query string) *StorageExecutor {
	if e.statementPropertyViews != nil || !e.queryPropertyDecayEnabled() {
		return nil
	}
	requirements := QueryPermissionRequirements(query)
	if requirements.Write || requirements.Schema || requirements.Admin || !isCacheableReadQuery(query) {
		return nil
	}
	scoped := e.cloneWithStorage(e.storage)
	scoped.statementPropertyViews = &statementPropertyViews{projection: storage.NewQueryResultProjection(e.storage)}
	return scoped
}

func (v *statementPropertyViews) node(source *storage.Node) *storage.Node {
	if source == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cached := v.nodes[source]; cached != nil {
		return cached
	}
	if v.nodes == nil {
		v.nodes = make(map[*storage.Node]*storage.Node)
		v.nodeBodies = make(map[storage.NodeID]nodePropertyViewBody)
	}
	body, ok := v.nodeBodies[source.ID]
	// A wrapper may return a fresh pointer on every index recheck. Reuse only
	// when its complete scoring input equals our detached snapshot: timestamps
	// alone are not a body version, and an ID alone is never sufficient.
	if !ok || !source.CreatedAt.Equal(body.source.CreatedAt) || !source.UpdatedAt.Equal(body.source.UpdatedAt) || !slices.Equal(source.Labels, body.source.Labels) || !reflect.DeepEqual(source.Properties, body.source.Properties) {
		snapshot := storage.CopyNode(&storage.Node{ID: source.ID, Labels: source.Labels, Properties: source.Properties, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt})
		body = nodePropertyViewBody{snapshot, v.projection.NodeProperties(source)}
		v.nodeBodies[source.ID] = body
	}
	out := *source
	out.Properties = body.properties
	v.nodes[source] = &out
	v.nodes[&out] = &out
	return &out
}

func (v *statementPropertyViews) edge(source *storage.Edge) *storage.Edge {
	if source == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cached := v.edges[source]; cached != nil {
		return cached
	}
	if v.edges == nil {
		v.edges = make(map[*storage.Edge]*storage.Edge)
		v.edgeBodies = make(map[storage.EdgeID]edgePropertyViewBody)
	}
	body, ok := v.edgeBodies[source.ID]
	if !ok || source.Type != body.source.Type || !source.CreatedAt.Equal(body.source.CreatedAt) || !source.UpdatedAt.Equal(body.source.UpdatedAt) || !reflect.DeepEqual(source.Properties, body.source.Properties) {
		snapshot := storage.CopyEdge(&storage.Edge{ID: source.ID, Type: source.Type, Properties: source.Properties, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt})
		body = edgePropertyViewBody{snapshot, v.projection.EdgeProperties(source)}
		v.edgeBodies[source.ID] = body
	}
	out := *source
	out.Properties = body.properties
	v.edges[source] = &out
	v.edges[&out] = &out
	return &out
}
