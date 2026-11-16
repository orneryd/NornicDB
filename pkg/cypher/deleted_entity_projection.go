package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/contextslot"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// deletedEntities is what a statement has deleted so far: the nodes and
// relationships its DELETE clauses removed (a DETACH DELETE's relationships
// and a deleted path's parts included). It is the statement's, not a row's,
// so it holds across WITH, aliases, collect() and the row-at-a-time runs of
// a write stretch: Neo4j 5.26 reads a deleted entity the same way wherever
// it is reached in the rest of the statement (#907).
type deletedEntities struct {
	nodes map[storage.NodeID]struct{}
	edges map[storage.EdgeID]struct{}
}

// pipelineClausesMayDelete reports whether clauses can delete: a DELETE, or
// a FOREACH or CALL subquery that may hold one.
func pipelineClausesMayDelete(clauses []pipelineClause) bool {
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseDelete, pipelineClauseForeach, pipelineClauseCallSubquery:
			return true
		}
	}
	return false
}

// add records deleted nodes and relationships.
func (d *deletedEntities) add(nodeIDs []storage.NodeID, edgeIDs map[storage.EdgeID]struct{}) {
	if len(nodeIDs) > 0 && d.nodes == nil {
		d.nodes = make(map[storage.NodeID]struct{}, len(nodeIDs))
	}
	for _, id := range nodeIDs {
		d.nodes[id] = struct{}{}
	}
	if len(edgeIDs) > 0 && d.edges == nil {
		d.edges = make(map[storage.EdgeID]struct{}, len(edgeIDs))
	}
	for id := range edgeIDs {
		d.edges[id] = struct{}{}
	}
}

// empty reports whether nothing is deleted (a nil d included).
func (d *deletedEntities) empty() bool {
	return d == nil || (len(d.nodes) == 0 && len(d.edges) == 0)
}

// replaceDeletedEntityViews replaces, in rows, every node and relationship in
// deleted with what Neo4j 5.26 returns for it after the DELETE: the same
// entity without labels or properties, so RETURN n is an empty node and
// n {.*}, properties(n) and keys(n) are empty. A relationship keeps its type
// and endpoints (type(r) still answers). Paths and lists holding a deleted
// entity are rebuilt with its view (#907). Reading a deleted entity's
// property or labels is an error instead (deletedEntityReadFailure).
func (e *StorageExecutor) replaceDeletedEntityViews(ctx context.Context, rows []pipelineRow, deleted *deletedEntities) {
	if deleted.empty() {
		return
	}
	views := deletedEntityViews{executor: e, nodes: deleted.nodes, edges: deleted.edges}
	for _, row := range rows {
		for name, value := range row {
			if replaced, changed := views.replace(ctx, value); changed {
				row[name] = replaced
			}
		}
	}
}

// deletedEntityViews builds the views of replaceDeletedEntityViews: a copy
// with the ID (a relationship's type and endpoints too) and no labels or
// properties. A view is equal to the entity by ID, as entities compare.
type deletedEntityViews struct {
	executor *StorageExecutor
	nodes    map[storage.NodeID]struct{}
	edges    map[storage.EdgeID]struct{}
}

// replace returns value with its deleted entities replaced by their views,
// and whether anything was replaced.
func (v *deletedEntityViews) replace(ctx context.Context, value interface{}) (interface{}, bool) {
	switch typed := value.(type) {
	case *storage.Node:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.nodes[storage.NodeID(typed.IdentityKey())]; deleted {
			return v.node(typed), true
		}
	case *storage.Edge:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.edges[storage.EdgeID(typed.IdentityKey())]; deleted {
			return v.edge(typed), true
		}
	case []interface{}:
		var replaced []interface{}
		for index, item := range typed {
			if view, changed := v.replace(ctx, item); changed {
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
			if view, replaced := v.replace(ctx, node); replaced {
				nodes[index], changed = view.(*storage.Node), true
			}
		}
		relationships := make([]*storage.Edge, len(path.Relationships))
		for index, relationship := range path.Relationships {
			relationships[index] = relationship
			if view, replaced := v.replace(ctx, relationship); replaced {
				relationships[index], changed = view.(*storage.Edge), true
			}
		}
		if changed {
			path.Nodes, path.Relationships = nodes, relationships
			return v.executor.pathToMapContext(ctx, path), true
		}
	}
	return value, false
}

func (v *deletedEntityViews) node(node *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	deleted.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: node.RuntimeMetadata().Database})
	return deleted
}

func (v *deletedEntityViews) edge(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Database: edge.Database, Type: edge.Type, StartNode: edge.StartNode, EndNode: edge.EndNode}
}

// deletedEntityReadKind is what a read asks of an entity value: the value of a
// property, the labels the entity carries, the property set keys() and
// properties() report, or whether one property is present.
type deletedEntityReadKind int

const (
	// deletedEntityPropertyValue reads one property value: n.p, (n).p, n[key],
	// head([n]).p, [x IN [n] | x.p], a map's value read on to its property
	// (m.entity.p) — and a relationship's keys(r) and properties(r), which
	// Neo4j answers from its property store.
	deletedEntityPropertyValue deletedEntityReadKind = iota
	// deletedEntityLabels reads labels(v).
	deletedEntityLabels
	// deletedEntityPropertySet reads keys(v) and properties(v) of a node: Neo4j
	// reports the empty set for a deleted node rather than an error.
	deletedEntityPropertySet
	// deletedEntityPropertyPresence asks whether one property is present: the
	// read a null test's own operand is. A deleted entity's property is absent,
	// and Neo4j answers n.p IS NULL for it without loading the entity, so this
	// read answers null instead of the statement error (#907).
	deletedEntityPropertyPresence
)

// deletedEntityReadFailure is the statement error reading value raises once the
// statement deleted it (#907): a deleted entity's property values and labels
// are gone, so Neo4j reports reading them as EntityNotFound wherever the read
// is reached — a projection, a filter, an ordering, a SET's value, a dynamic
// key, a list comprehension over a list holding the entity, through an alias.
// The decision is taken from the entity value the expression actually
// received, so every expression that hands a deleted entity to a read fails
// alike, whatever shape its text has.
//
// It is nil for a value that isn't an entity the statement deleted, for the
// empty property set a deleted node's keys() and properties() answer, and for
// the presence a null test asks of a property: that read is granted to the
// null test's own operand only, never to a computed or nested expression
// (deletedEntityPropertyPresence).
func deletedEntityReadFailure(ctx context.Context, value interface{}, read deletedEntityReadKind) error {
	switch value.(type) {
	case *storage.Node, *storage.Edge:
	default:
		return nil
	}
	deleted := contextslot.Get[deletedEntities](ctx)
	if deleted.empty() {
		return nil
	}
	if !deleted.readsDeleted(value, read) {
		return nil
	}
	return newSemanticError(
		"Neo.ClientError.Statement.EntityNotFound",
		"DeletedEntityAccess",
		"cannot access properties or labels of a deleted entity",
	)
}

// readsDeleted reports whether reading value the given way fails: a deleted
// node's property values and labels are gone, a deleted relationship's too
// (its type, endpoints and identity remain), a deleted node's property set is
// empty rather than missing, and a property's presence is what a null test
// answers without an error.
func (d *deletedEntities) readsDeleted(value interface{}, read deletedEntityReadKind) bool {
	switch entity := value.(type) {
	case *storage.Node:
		if entity == nil || read == deletedEntityPropertySet || read == deletedEntityPropertyPresence {
			return false
		}
		_, found := d.nodes[storage.NodeID(entity.IdentityKey())]
		return found
	case *storage.Edge:
		if entity == nil || read == deletedEntityPropertyPresence {
			return false
		}
		_, found := d.edges[storage.EdgeID(entity.IdentityKey())]
		return found
	}
	return false
}

// deleteEdgeLookupID uses the same identity translation as collected targets
// for relationships discovered while validating or detaching a node.
func deleteEdgeLookupID(store storage.Engine, edge *storage.Edge) storage.EdgeID {
	id, _ := edge.Identity().LookupID(store, "5")
	return storage.EdgeID(id)
}

// addFromStorage records identities rather than engine-relative lookup IDs.
// The statement registry can span constituent calls, aliases and later clauses.
func (d *deletedEntities) addFromStorage(store storage.Engine, nodes []storage.NodeID, edges map[storage.EdgeID]struct{}) {
	key := func(kind, value string) string {
		identity := storage.EntityIdentity{Database: storage.DatabaseOf(store), LocalID: value}
		if storage.CompositeDatabaseOf(store) != "" {
			actual, database, local, qualified := storage.ParseElementID(value)
			if qualified && actual == kind {
				identity = storage.EntityIdentity{Database: database, LocalID: local}
			}
		}
		return identity.Key()
	}
	nodeKeys := make([]storage.NodeID, len(nodes))
	for i, id := range nodes {
		nodeKeys[i] = storage.NodeID(key("4", string(id)))
	}
	edgeKeys := make(map[storage.EdgeID]struct{}, len(edges))
	for id := range edges {
		edgeKeys[storage.EdgeID(key("5", string(id)))] = struct{}{}
	}
	d.add(nodeKeys, edgeKeys)
}
