package cypher

import (
	"context"
	"reflect"

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
// and endpoints (type(r) still answers). Paths, lists and maps holding a
// deleted entity are rebuilt with its view (#907), whether the statement
// built them before or after the DELETE. Reading a deleted entity's property
// or labels is an error instead (deletedEntityReadFailure).
func (e *StorageExecutor) replaceDeletedEntityViews(ctx context.Context, rows []pipelineRow, deleted *deletedEntities) {
	if deleted.empty() {
		return
	}
	views := deletedEntityViews{executor: e, deleted: deleted}
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
	deleted  *deletedEntities
}

// replace returns value with its deleted entities replaced by their views,
// and whether anything was replaced. Lists and maps are walked to any depth,
// so a container a statement built before its DELETE — m {.entity: n}, a list
// of such maps, a map nested in a map — reads the deleted entity as its view
// too (#907). Only a container holding a deleted entity is copied; a list or
// map that holds none keeps its identity, and so do the entries of a copy the
// walk had no reason to change (a healthy sibling list or map, an alias, nil).
func (v *deletedEntityViews) replace(ctx context.Context, value interface{}) (interface{}, bool) {
	if view, replaced := v.entityView(value); replaced {
		return view, true
	}
	containers := &deletedEntityContainers{views: v}
	if !containers.mark(value) {
		return value, false
	}
	return containers.rewrite(ctx, value), true
}

// entityView returns value's view when value is a node or relationship the
// statement deleted.
func (v *deletedEntityViews) entityView(value interface{}) (interface{}, bool) {
	switch typed := value.(type) {
	case *storage.Node:
		if v.deleted.contains(typed) {
			return v.node(typed), true
		}
	case *storage.Edge:
		if v.deleted.contains(typed) {
			return v.edge(typed), true
		}
	}
	return value, false
}

// deletedEntityContainer identifies one list or map of a value by identity, so
// a walk that meets the same container twice — an alias, or a cycle a Go or
// native handler returned — copies it once and the copy keeps the value's
// topology.
type deletedEntityContainer struct {
	kind byte
	data uintptr
	size int
}

const (
	deletedEntityContainerList byte = 'l'
	deletedEntityContainerMap  byte = 'm'
)

// deletedEntityContainerOf identifies value as a list or map the walk has to
// consider: an empty list and a nil map have no entry to reach, so they are
// never a container that changes.
func deletedEntityContainerOf(value interface{}) (deletedEntityContainer, bool) {
	switch typed := value.(type) {
	case []interface{}:
		if len(typed) == 0 {
			return deletedEntityContainer{}, false
		}
		return deletedEntityContainer{
			kind: deletedEntityContainerList,
			data: uintptr(reflect.ValueOf(typed).UnsafePointer()),
			size: len(typed),
		}, true
	case map[string]interface{}:
		if typed == nil {
			return deletedEntityContainer{}, false
		}
		return deletedEntityContainer{kind: deletedEntityContainerMap, data: uintptr(reflect.ValueOf(typed).UnsafePointer())}, true
	}
	return deletedEntityContainer{}, false
}

// deletedEntityContainers rewrites the lists and maps of one value: it marks
// the containers a deleted entity is reachable from, then copies exactly those
// with the entity's view in place. A container nothing below changed keeps its
// identity, and a copy is recorded before its entries are walked, so a cycle
// ends at the copy instead of walking for ever.
type deletedEntityContainers struct {
	views  *deletedEntityViews
	marked map[deletedEntityContainer]struct{}
	clones map[deletedEntityContainer]interface{}
}

// mark records the containers that must be copied: those a deleted entity is
// reachable from, as an entry of theirs or of a container below them. It
// reports whether value itself is one of them.
func (c *deletedEntityContainers) mark(value interface{}) bool {
	root, isContainer := deletedEntityContainerOf(value)
	if !isContainer {
		return false
	}
	parents := make(map[deletedEntityContainer][]deletedEntityContainer)
	holding := make(map[deletedEntityContainer]struct{})
	seen := map[deletedEntityContainer]struct{}{root: {}}
	pending := []interface{}{value}
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		container, _ := deletedEntityContainerOf(current)
		// A path value is rebuilt from its PathResult, so its map entries are
		// not entries the walk reads: the deleted entity is the path's own.
		if path, isPath := deletedEntityPathResult(current); isPath {
			if c.views.pathNeedsRewrite(path) {
				holding[container] = struct{}{}
			}
			continue
		}
		forEachDeletedEntityContainerChild(current, func(child interface{}) {
			if c.views.deleted.contains(child) {
				holding[container] = struct{}{}
				return
			}
			childContainer, isChildContainer := deletedEntityContainerOf(child)
			if !isChildContainer {
				return
			}
			parents[childContainer] = append(parents[childContainer], container)
			if _, visited := seen[childContainer]; visited {
				return
			}
			seen[childContainer] = struct{}{}
			pending = append(pending, child)
		})
	}
	// A container holds a deleted entity either itself or through a container
	// below it, whichever way a cycle runs, so the marks come back up from the
	// containers that hold one.
	c.marked = make(map[deletedEntityContainer]struct{}, len(holding))
	c.clones = make(map[deletedEntityContainer]interface{}, len(holding))
	propagating := make([]deletedEntityContainer, 0, len(holding))
	for container := range holding {
		c.marked[container] = struct{}{}
		propagating = append(propagating, container)
	}
	for len(propagating) > 0 {
		container := propagating[len(propagating)-1]
		propagating = propagating[:len(propagating)-1]
		for _, parent := range parents[container] {
			if _, marked := c.marked[parent]; marked {
				continue
			}
			c.marked[parent] = struct{}{}
			propagating = append(propagating, parent)
		}
	}
	_, marked := c.marked[root]
	return marked
}

// rewrite returns value with the deleted entities the marked containers hold
// replaced by their views. A container that must change is copied, every other
// value stays as it is.
func (c *deletedEntityContainers) rewrite(ctx context.Context, value interface{}) interface{} {
	container, isContainer := deletedEntityContainerOf(value)
	if !isContainer {
		return value
	}
	if _, copyIt := c.marked[container]; !copyIt {
		return value
	}
	if clone, copied := c.clones[container]; copied {
		return clone
	}
	if path, isPath := deletedEntityPathResult(value); isPath {
		clone := c.views.viewPath(ctx, path)
		c.clones[container] = clone
		return clone
	}
	switch typed := value.(type) {
	case []interface{}:
		clone := make([]interface{}, len(typed))
		c.clones[container] = clone
		for index, child := range typed {
			clone[index] = c.entry(ctx, child)
		}
		return clone
	case map[string]interface{}:
		clone := make(map[string]interface{}, len(typed))
		c.clones[container] = clone
		for name, child := range typed {
			clone[name] = c.entry(ctx, child)
		}
		if failure := expressionErrorSlot(ctx); failure != nil {
			failure.copyMapKeyOrder(typed, clone)
		}
		return clone
	}
	return value
}

// entry returns one entry of a copied container: a deleted entity becomes its
// view, a container holding one becomes its copy, and every other value stays
// as it is (identity included).
func (c *deletedEntityContainers) entry(ctx context.Context, child interface{}) interface{} {
	if view, replaced := c.views.entityView(child); replaced {
		return view
	}
	return c.rewrite(ctx, child)
}

// deletedEntityPathResult returns the path a map value carries, whatever shape
// the path value has (pathToMap's or the path-context evaluator's); a map
// carrying no path, or a nil one, is not a path value.
func deletedEntityPathResult(value interface{}) (PathResult, bool) {
	object, isMap := value.(map[string]interface{})
	if !isMap {
		return PathResult{}, false
	}
	switch result := object["_pathResult"].(type) {
	case PathResult:
		return result, true
	case *PathResult:
		if result == nil {
			return PathResult{}, false
		}
		return *result, true
	}
	return PathResult{}, false
}

// forEachDeletedEntityContainerChild visits the entries a container's walk
// reads: a list's items or a map's values.
func forEachDeletedEntityContainerChild(value interface{}, visit func(interface{})) {
	switch typed := value.(type) {
	case []interface{}:
		for _, child := range typed {
			visit(child)
		}
	case map[string]interface{}:
		for _, child := range typed {
			visit(child)
		}
	}
}

// pathNeedsRewrite reports whether the path holds a deleted node or
// relationship.
func (v *deletedEntityViews) pathNeedsRewrite(path PathResult) bool {
	for _, node := range path.Nodes {
		if v.deleted.contains(node) {
			return true
		}
	}
	for _, relationship := range path.Relationships {
		if v.deleted.contains(relationship) {
			return true
		}
	}
	return false
}

// viewPath returns the path value with its deleted nodes and relationships
// replaced by their views (the map shape a path value is published as).
func (v *deletedEntityViews) viewPath(ctx context.Context, path PathResult) map[string]interface{} {
	nodes := make([]*storage.Node, len(path.Nodes))
	for index, node := range path.Nodes {
		nodes[index] = node
		if v.deleted.contains(node) {
			nodes[index] = v.node(node)
		}
	}
	relationships := make([]*storage.Edge, len(path.Relationships))
	for index, relationship := range path.Relationships {
		relationships[index] = relationship
		if v.deleted.contains(relationship) {
			relationships[index] = v.edge(relationship)
		}
	}
	path.Nodes, path.Relationships = nodes, relationships
	return v.executor.pathToMapContext(ctx, path)
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
	switch value.(type) {
	case *storage.Node:
		if read == deletedEntityPropertySet || read == deletedEntityPropertyPresence {
			return false
		}
	case *storage.Edge:
		if read == deletedEntityPropertyPresence {
			return false
		}
	default:
		return false
	}
	return d.contains(value)
}

// contains is the identity owner shared by runtime reads, row-image projection
// and SET publication. It recognizes only this statement's own deletions.
func (d *deletedEntities) contains(value interface{}) bool {
	if d == nil {
		return false
	}
	switch entity := value.(type) {
	case *storage.Node:
		if entity != nil {
			_, found := d.nodes[storage.NodeID(entity.IdentityKey())]
			return found
		}
	case *storage.Edge:
		if entity != nil {
			_, found := d.edges[storage.EdgeID(entity.IdentityKey())]
			return found
		}
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
